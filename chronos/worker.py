import asyncio
import copy
import gc
import hashlib
import hmac
import json
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import httpx
import logfire
import redis.exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from celery.app import Celery
from celery.signals import worker_process_init
from fastapi import APIRouter, FastAPI
from httpx import AsyncClient
from opentelemetry import context as otel_context
from redis import Redis
from sqlalchemy import delete, func
from sqlmodel import Session, select

from chronos.db import engine
from chronos.pydantic_schema import RequestData
from chronos.sql_models import WebhookEndpoint, WebhookLog, WebhookStatus
from chronos.tasks.queue import JobQueue
from chronos.utils import app_logger, settings

cronjob = APIRouter()

celery_app = Celery(__name__, broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    # Reliability: ack after completion, requeue on crash
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Worker tuning: fetch one task at a time for fair scheduling
    worker_prefetch_multiplier=1,
    # Task execution limits
    task_soft_time_limit=300,
    task_time_limit=600,
    # Route dispatcher to its own queue
    task_routes={
        'job_dispatcher': {'queue': 'dispatcher'},
    },
)

GLOBAL_BRANCH_ID = 0

cache = Redis.from_url(settings.redis_url)

# Initialize job queue with the same Redis client
job_queue = JobQueue(redis_client=cache)

# Large enough to make progress quickly without keeping delete logic in settings.
WEBHOOK_LOG_DELETE_BATCH_SIZE = 50_000


@worker_process_init.connect
def init_worker_process(**kwargs):
    """
    Dispose of the database connection pool when a worker process is forked.
    This ensures each worker gets fresh connections instead of inheriting
    stale connections from the parent process, preventing SSL SYSCALL errors.
    """
    app_logger.info('Disposing database engine pool for worker process')
    engine.dispose()

    from chronos.observability import instrument_worker

    if bool(settings.logfire_token):
        instrument_worker()


async def webhook_request(
    client: AsyncClient,
    url: str,
    endpoint_id: int,
    *,
    body: bytes,
    webhook_sig: str,
):
    """
    Send a POST request to the webhook endpoint the user configured (their integration URL).
    The body must be the exact bytes that were used to compute webhook_sig so the receiver
    can verify the HMAC.
    """
    headers = {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'webhook-signature': webhook_sig,
    }
    with logfire.span('{method=} {url!r}', url=url, method='POST'):
        r = None
        try:
            r = await client.post(url=url, content=body, headers=headers, timeout=settings.webhook_http_timeout_seconds)
        except httpx.TimeoutException as terr:
            app_logger.info('Timeout error sending webhook to %s: %s', url, terr)
        except httpx.HTTPError as httperr:
            app_logger.info('HTTP error sending webhook to %s: %s', url, httperr)
        except Exception as exc:
            # Malformed URLs, transport/assert failures from test doubles, etc. — still one delivery attempt.
            app_logger.info('Error sending webhook to %s: %s', url, exc)
    request_data = RequestData(endpoint_id=endpoint_id, request_headers=json.dumps(headers), request_body=body.decode())
    if r is not None:
        request_data.response_headers = json.dumps(dict(r.headers))
        request_data.response_body = json.dumps(r.content.decode())
        request_data.status_code = r.status_code
        request_data.successful_response = True
    return request_data


acceptable_url_schemes = ('http', 'https', 'ftp', 'ftps')
SUCCESS_STATUS_CODES = {200, 201, 202, 204}


def _split_payloads(loaded_payload: dict) -> list[dict]:
    """Split a multi-event payload into individual single-event payloads.

    Non-event payloads (e.g. TCPublicProfileWebhook) are returned as-is in a list.
    """
    events = loaded_payload.get('events')
    if events is None:
        return [copy.deepcopy(loaded_payload)]

    base = {k: v for k, v in loaded_payload.items() if k != 'events'}
    payloads_to_send = []
    for event in events:
        # Split each action event into individual requests for reverse compatibility
        # with Zapier and other client integrations.
        # TODO: remove the splitting logic here after Issue #119 fix is deployed
        single_event_payload = copy.deepcopy(base)
        single_event_payload['events'] = [copy.deepcopy(event)]
        payloads_to_send.append(single_event_payload)
    return payloads_to_send


def _build_signed_request(endpoint: WebhookEndpoint, payload_to_send: dict) -> tuple[bytes, str]:
    """Encode payload as JSON and sign it with the endpoint's api_key using HMAC-SHA256."""
    body = json.dumps(payload_to_send).encode()
    sig = hmac.new(endpoint.api_key.encode(), body, hashlib.sha256)
    return body, sig.hexdigest()


def _is_success(response: RequestData) -> bool:
    """True if the HTTP response indicates successful delivery (200, 201, 202, 204)."""
    return response.status_code in SUCCESS_STATUS_CODES


def _is_retryable(response: RequestData) -> bool:
    """True if the delivery failed in a way worth retrying.

    Retried:
    - No response / timeout: server unreachable or took too long — likely transient.
    - 429 Too Many Requests: server asked us to back off.
    - 5xx: server-side error — likely transient (restart, deploy, overload).

    Not retried:
    - 2xx: success.
    - 3xx: redirect — httpx does not follow redirects on POST; the endpoint URL has
      moved and the customer needs to update it.
    - 4xx (except 429): client error — the endpoint URL is wrong, auth failed, or the
      resource doesn't exist. Retrying with the same request won't help. Persistent 4xx
      failures will eventually trigger the auto-disable threshold instead.
    """
    if _is_success(response):
        return False
    if response.status_code == 429:
        return True
    if response.status_code is not None and response.status_code >= 500:
        return True
    if not response.successful_response:
        return True
    return False


def _get_webhook_status(response: RequestData) -> WebhookStatus:
    """Human-readable delivery status string written to WebhookLog."""
    if _is_success(response):
        return WebhookStatus.SUCCESS
    if not response.successful_response:
        return WebhookStatus.NO_RESPONSE
    return WebhookStatus.UNEXPECTED_RESPONSE


def _request_data_to_log(response: RequestData) -> WebhookLog:
    """Convert a completed RequestData into a WebhookLog ready for DB insertion."""
    return WebhookLog(
        webhook_endpoint_id=response.endpoint_id,
        request_headers=response.request_headers,
        request_body=response.request_body,
        response_headers=response.response_headers,
        response_body=response.response_body,
        status=_get_webhook_status(response),
        status_code=response.status_code,
    )


def _get_endpoint_url(endpoint: WebhookEndpoint, url_extension: str | None) -> str | None:
    """Return the delivery URL for an endpoint, appending url_extension if provided.

    Returns None (and logs an error) if the URL scheme is not in acceptable_url_schemes.
    """
    if not endpoint.webhook_url.startswith(acceptable_url_schemes):
        app_logger.error(
            'Webhook URL does not start with an acceptable url scheme: %s (%s)', endpoint.webhook_url, endpoint.id
        )
        return None

    url = endpoint.webhook_url
    if url_extension:
        url += f'/{url_extension}'
    return url


async def _send_single_webhook(
    client: AsyncClient, endpoint: WebhookEndpoint, payload_to_send: dict, url_extension: str | None
) -> RequestData | None:
    """Build a signed request and POST it. Returns None if the URL is invalid."""
    url = _get_endpoint_url(endpoint, url_extension)
    if url:
        body, webhook_sig = _build_signed_request(endpoint, payload_to_send)
        return await webhook_request(client, url, endpoint.id, body=body, webhook_sig=webhook_sig)


def _send_single_webhook_sync(
    endpoint: WebhookEndpoint, payload_to_send: dict, url_extension: str | None
) -> RequestData | None:
    """Synchronous wrapper around _send_single_webhook for use in Celery tasks."""

    async def _run():
        async with AsyncClient() as client:
            return await _send_single_webhook(client, endpoint, payload_to_send, url_extension)

    return asyncio.run(_run())


def _process_response(request_data: RequestData) -> tuple[WebhookLog, bool]:
    """Convert a RequestData into a WebhookLog and return whether it's retryable.

    Returns (log, retryable).
    """
    if not request_data.successful_response:
        app_logger.info('No response from endpoint %s', request_data.endpoint_id)
    return _request_data_to_log(request_data), _is_retryable(request_data)


async def _async_post_webhooks(endpoints, url_extension, payload):
    """Fan out a payload to all endpoints concurrently and return delivery results.

    Each multi-event payload is split into one request per event before sending
    (see `_split_payloads`). Returns (webhook_logs, total_success, total_failed,
    retry_list) where retry_list is a list of (endpoint_id, payload_str,
    url_extension) tuples for retryable failures.
    """
    webhook_logs = []
    total_success, total_failed = 0, 0
    retry_list = []  # (endpoint_id, payload_str, url_extension)
    limits = httpx.Limits(max_connections=settings.webhook_http_max_connections)
    loaded_payload = json.loads(payload)

    async with AsyncClient(limits=limits) as client:
        coros = [
            _send_single_webhook(client, endpoint, payload_to_send, url_extension)
            for endpoint in endpoints
            for payload_to_send in _split_payloads(loaded_payload)
        ]
        webhook_responses = await asyncio.gather(*coros, return_exceptions=True)
        for response in webhook_responses:
            if response is None:
                continue

            if not isinstance(response, RequestData):
                app_logger.info('Unexpected error sending webhook: %s', response)
                total_failed += 1
                continue

            log, retryable = _process_response(response)
            webhook_logs.append(log)
            if _is_success(response):
                total_success += 1
            else:
                total_failed += 1
            if retryable:
                retry_list.append((response.endpoint_id, response.request_body, url_extension))
    return webhook_logs, total_success, total_failed, retry_list


def _webhook_host_is_exempt_from_auto_disable(webhook_url: str) -> bool:
    """True if the webhook target is a TutorCruncher first-party host (never auto-disabled)."""
    parsed = urlparse(webhook_url)
    host = (parsed.hostname or '').lower()
    if not host:
        return False
    return host == 'tutorcruncher.com' or host.endswith('.tutorcruncher.com')


def _notify_endpoint_disabled(
    tc_id: int,
    branch_id: int,
    name: str,
    webhook_url: str,
    failure_count: int,
    total_attempts: int,
    window_minutes: int,
) -> None:
    """POST to TC2 when an endpoint is auto-disabled. Logs errors but does not raise."""
    url = settings.tc2_endpoint_disabled_url
    if not url:
        return
    payload = {
        'tc_id': tc_id,
        'branch_id': branch_id,
        'name': name,
        'webhook_url': webhook_url,
        'failure_count': failure_count,
        'total_attempts': total_attempts,
        'window_minutes': window_minutes,
        'reason': 'too_many_failures',
    }
    headers = {'Authorization': f'Bearer {settings.tc2_shared_key}', 'Content-Type': 'application/json'}
    try:
        r = httpx.post(url, json=payload, headers=headers, timeout=10.0)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        app_logger.warning('TC2 endpoint-disabled callback returned %s: %s', e.response.status_code, e.response.text)
    except Exception as e:
        app_logger.exception('Failed to notify TC2 of disabled endpoint: %s', e)


def _check_and_disable_endpoint_if_needed(db: Session, endpoint: WebhookEndpoint) -> None:
    """If endpoint has >threshold failure rate in the window with min attempts, set active=False and notify TC2."""
    if not endpoint.active:
        return
    if _webhook_host_is_exempt_from_auto_disable(endpoint.webhook_url):
        return
    window_start = datetime.now(UTC) - timedelta(minutes=settings.webhook_disable_failure_window_minutes)
    # Count total and failures in window
    total = db.exec(
        select(func.count())
        .select_from(WebhookLog)
        .where(WebhookLog.webhook_endpoint_id == endpoint.id, WebhookLog.timestamp >= window_start)
    ).one()
    failures = db.exec(
        select(func.count())
        .select_from(WebhookLog)
        .where(
            WebhookLog.webhook_endpoint_id == endpoint.id,
            WebhookLog.timestamp >= window_start,
            WebhookLog.status != WebhookStatus.SUCCESS,
        )
    ).one()
    if total < settings.webhook_disable_min_attempts:
        return
    failure_rate = failures / total
    if failure_rate <= settings.webhook_disable_failure_rate_threshold:
        return
    endpoint.active = False
    app_logger.info(
        'Auto-disabled endpoint %s (tc_id=%s): failure rate %.1f%% (%s/%s) in last %s minutes',
        endpoint.name,
        endpoint.tc_id,
        failure_rate * 100,
        failures,
        total,
        settings.webhook_disable_failure_window_minutes,
    )
    _notify_endpoint_disabled(
        endpoint.tc_id,
        endpoint.branch_id,
        endpoint.name,
        endpoint.webhook_url,
        failure_count=failures,
        total_attempts=total,
        window_minutes=settings.webhook_disable_failure_window_minutes,
    )


@celery_app.task(
    autoretry_for=(redis.exceptions.ConnectionError, redis.exceptions.TimeoutError),
    retry_kwargs={'max_retries': 3},
    retry_backoff=True,
)
def task_retry_single_webhook(
    endpoint_id: int,
    payload: str,
    url_extension: str | None = None,
    attempt: int = 1,
):
    """
    One-attempt retry for a single endpoint. Re-enqueues itself with backoff
    while the failure is retryable and we have retries remaining.
    """
    with Session(engine) as db:
        endpoint = db.get(WebhookEndpoint, endpoint_id)
        if not endpoint or not endpoint.active:
            return

        request_data = _send_single_webhook_sync(endpoint, json.loads(payload), url_extension)
        if request_data is None:
            return

        log, retryable = _process_response(request_data)
        db.add(log)
        db.commit()

        if not _is_success(request_data):
            _check_and_disable_endpoint_if_needed(db, endpoint)
            db.commit()

        should_retry = retryable and endpoint.active and attempt < settings.webhook_retry_max_attempts

        if should_retry:
            # here task_send_webhooks enqueues the first retry with countdown base,
            # so the exponent here starts at attempt to continue the series as base, base*10, base*100, ...
            next_delay = settings.webhook_retry_backoff_base_seconds * (
                settings.webhook_retry_backoff_multiplier**attempt
            )
            next_delay = max(0, int(next_delay))
            task_retry_single_webhook.apply_async(
                args=[endpoint_id, payload],
                kwargs={'url_extension': url_extension, 'attempt': attempt + 1},
                countdown=next_delay,
            )
            app_logger.info(
                'Re-queued retry for endpoint %s in %s s (attempt %s)', endpoint_id, next_delay, attempt + 1
            )


@celery_app.task(
    autoretry_for=(redis.exceptions.ConnectionError, redis.exceptions.TimeoutError),
    retry_kwargs={'max_retries': 3},
    retry_backoff=True,
)
def task_send_webhooks(payload: str, url_extension: str = None):
    """
    Send the webhook to the relevant endpoints
    """
    loaded_payload = json.loads(payload)
    loaded_payload['_request_time'] = loaded_payload.pop('request_time')

    if loaded_payload.get('events'):
        branch_id = loaded_payload['events'][0]['branch']
    else:
        branch_id = loaded_payload['branch_id']

    qlength = job_queue.get_celery_queue_length()
    if qlength > settings.dispatcher_max_celery_queue:
        app_logger.error('Queue is too long, qlength=%s. Check workers and speeds.', qlength)

    app_logger.info('Starting send webhook task for branch %s. qlength=%s.', branch_id, qlength)
    lf_span = 'Sending webhooks for branch: {branch_id=}'
    with logfire.span(lf_span, branch_id=branch_id):
        with Session(engine) as db:
            # Get all the endpoints for the branch
            endpoints_query = select(WebhookEndpoint).where(
                WebhookEndpoint.branch_id == branch_id, WebhookEndpoint.active
            )
            endpoints = db.exec(endpoints_query).all()

            webhook_logs, total_success, total_failed, retry_list = asyncio.run(
                _async_post_webhooks(endpoints, url_extension, payload)
            )
            for webhook_log in webhook_logs:
                db.add(webhook_log)
            db.commit()

            # Enqueue retries (non-blocking)
            for endpoint_id, payload_str, url_ext in retry_list:
                task_retry_single_webhook.apply_async(
                    args=[endpoint_id, payload_str],
                    kwargs={'url_extension': url_ext, 'attempt': 1},
                    countdown=settings.webhook_retry_backoff_base_seconds,
                )

            # Disable endpoints that exceed failure rate threshold
            endpoint_ids_with_failures = {
                log.webhook_endpoint_id for log in webhook_logs if log.status != WebhookStatus.SUCCESS
            }
            for endpoint in endpoints:
                if endpoint.id in endpoint_ids_with_failures:
                    _check_and_disable_endpoint_if_needed(db, endpoint)
            db.commit()
    app_logger.info(
        '%s Webhooks sent for branch %s. Total Sent: %s. Total failed: %s',
        total_success + total_failed,
        branch_id,
        total_success,
        total_failed,
    )


DELETE_JOBS_KEY = 'delete_old_logs_job'

scheduler = AsyncIOScheduler(timezone=UTC)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.shutdown()


@scheduler.scheduled_job('interval', hours=1)
async def delete_old_logs_job():
    """
    We run cron job at midnight every day that wipes all WebhookLogs older than 15 days
    """
    if cache.get(DELETE_JOBS_KEY):
        return
    else:
        cache.set(DELETE_JOBS_KEY, 'True', ex=1200)
        _delete_old_logs_job.delay()


def get_count(date_to_delete_before: datetime) -> int:
    """
    Get the count of all logs
    """
    with Session(engine) as db:
        count = db.exec(
            select(func.count()).select_from(WebhookLog).where(WebhookLog.timestamp < date_to_delete_before)
        ).one()
    return count


@celery_app.task
def task_delete_endpoint(endpoint_id: int) -> None:
    """Delete an endpoint and its logs asynchronously in safe batches."""
    with Session(engine) as db:
        endpoint = db.get(WebhookEndpoint, endpoint_id)
        if endpoint is None:
            app_logger.info('Endpoint %s already deleted before async cleanup started', endpoint_id)
            return

        total_deleted = 0
        delete_limit = WEBHOOK_LOG_DELETE_BATCH_SIZE
        while True:
            log_ids = db.exec(
                select(WebhookLog.id).where(WebhookLog.webhook_endpoint_id == endpoint_id).limit(delete_limit)
            ).all()
            if not log_ids:
                break

            delete_statement = delete(WebhookLog).where(WebhookLog.id.in_(log_id for log_id in log_ids))
            db.exec(delete_statement)
            db.commit()
            total_deleted += len(log_ids)

            app_logger.info('Deleted %s logs so far for endpoint %s', total_deleted, endpoint_id)

            del log_ids
            del delete_statement
            gc.collect()

        endpoint = db.get(WebhookEndpoint, endpoint_id)
        if endpoint is None:
            app_logger.info('Endpoint %s already deleted during async cleanup', endpoint_id)
            return

        db.delete(endpoint)
        db.commit()
        app_logger.info('Deleted endpoint %s after removing %s logs', endpoint_id, total_deleted)


@celery_app.task
def _delete_old_logs_job():
    # with logfire.span('Started to delete old logs'):
    with Session(engine) as db:
        # Get all logs older than 15 days
        date_to_delete_before = datetime.now(UTC) - timedelta(days=15)
        count = get_count(date_to_delete_before)
        delete_limit = WEBHOOK_LOG_DELETE_BATCH_SIZE
        while count > 0:
            app_logger.info(f'Deleting {count} logs')
            logs_to_delete = db.exec(
                select(WebhookLog.id).where(WebhookLog.timestamp < date_to_delete_before).limit(delete_limit)
            ).all()
            delete_statement = delete(WebhookLog).where(WebhookLog.id.in_(log_id for log_id in logs_to_delete))
            db.exec(delete_statement)
            db.commit()
            count -= delete_limit

            del logs_to_delete
            del delete_statement
            gc.collect()

    cache.delete(DELETE_JOBS_KEY)


def dispatch_branch_task(task, branch_id: int, **kwargs) -> None:
    """
    Dispatches a task to per branch queue for fair round robin processing.

    For payloads with N events, split such that:
    P({e1, e2, ..., eN}) -> [P({e1}), P({e2}), ..., P({eN})]
    """
    payload = kwargs.pop('payload', None)
    url_extension = kwargs.pop('url_extension', None)
    if not payload:
        return

    for single_payload in _split_payloads(payload):
        job_queue.enqueue(
            task.name, branch_id=branch_id, payload=json.dumps(single_payload), url_extension=url_extension
        )


@celery_app.task(name='job_dispatcher', acks_late=False)
def job_dispatcher_task(
    max_celery_queue: int = settings.dispatcher_max_celery_queue,
    cycle_delay: float = settings.dispatcher_cycle_delay_seconds,  # at most the webhooks need to wait for 10ms
    idle_delay: float = settings.dispatcher_idle_delay_seconds,  # this is for when no active branches, so doesn't need to be as frequent
) -> None:
    """
    Celery task that runs round robin dispatcher.

    Runs indefinitely, dispatching jobs from per-branch queues to Celery workers.
    Implements backpressure by pausing when the Celery queue is full.

    CRITICAL NOTE: acks_late=False overrides the global task_acks_late=True.
    The dispatcher runs forever and never completes. With acks_late=True,
    the Redis broker's visibility_timeout (default 1 hour) would redeliver
    the unacked task, spawning a DUPLICATE dispatcher. Setting acks_late=False
    acks the task immediately on receipt, preventing redelivery.
    """
    from billiard.exceptions import SoftTimeLimitExceeded

    from chronos.tasks.dispatcher import dispatch_cycle

    app_logger.info('Job dispatcher started')
    while True:
        try:
            if not job_queue.has_active_jobs():
                time.sleep(idle_delay)
                continue

            # LLEN celery: measures pending broker queue only, not in-flight tasks.
            celery_queue_len = job_queue.get_celery_queue_length()
            if celery_queue_len >= max_celery_queue:
                # We wait for then regular celery workers to process the present tasks first before
                # running the dispatch cycle again
                time.sleep(cycle_delay)
                continue

            cycle_token = otel_context.attach(otel_context.Context())
            try:
                with logfire.span('Dispatching jobs') as span:
                    dispatched = dispatch_cycle()
                    if dispatched > 0:
                        # without this gaurd the cycle will log every 10ms it finds nothing
                        # in the dispatcher queue which can be noisy
                        span.message = f'Dispatched {dispatched} jobs'
                        app_logger.info('Dispatched %d jobs', dispatched)
            finally:
                otel_context.detach(cycle_token)

            time.sleep(cycle_delay)
        except SoftTimeLimitExceeded:
            # Safety net for when if CLI flags (--soft-time-limit=0) aren't ever set properly,
            # catch the exception and continue rather than dying.
            app_logger.warning('Dispatcher caught SoftTimeLimitExceeded, continuing')
            # since this is meant to run indefinitely
            continue
        except Exception:
            # CRITICAL NOTE: Catch all other exceptions to prevent the dispatcher from
            # dying permanently. dispatch_cycle() or job_queue methods can raise
            # redis.ConnectionError, serialization errors, broker hiccups, etc.
            # The worker_ready signal only fires on worker process start, NOT on
            # task failure — so an uncaught exception here kills the dispatcher
            # with no automatic recovery until the worker process itself restarts.
            app_logger.exception('Dispatcher error, sleeping %s seconds before retry', idle_delay)
            time.sleep(idle_delay)
            continue


# Import worker_startup to register the signal handler.
# Must be at the bottom of the file because worker_startup imports from this module.
import chronos.tasks.worker_startup  # noqa: F401, E402
