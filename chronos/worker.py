import asyncio
import gc
import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import logfire
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from celery.app import Celery
from fastapi import APIRouter, FastAPI
from httpx import AsyncClient
from redis import Redis
from sqlalchemy import delete, func
from sqlmodel import Session, select

from chronos.db import engine
from chronos.pydantic_schema import RequestData
from chronos.sql_models import WebhookEndpoint, WebhookLog
from chronos.utils import app_logger, settings

cronjob = APIRouter()

celery_app = Celery(__name__, broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.broker_connection_retry_on_startup = True
cache = Redis.from_url(settings.redis_url)


async def webhook_request(client: AsyncClient, url: str, endpoint_id: int, *, webhook_sig: str, data: dict = None):
    """
    Send a request to TutorCruncher
    :param client
    :param url: The endpoint supplied by clients when creating an integration in TC2
    :param method: We should always be sending POST requests as we are sending data to the endpoints
    :param webhook_sig: The signature generated by hashing the payload with the shared key
    :param data: The Webhook data supplied by TC2
    :return: WebhookEndpoint response
    """
    from chronos.main import logfire

    headers = {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'webhook-signature': webhook_sig,
    }
    with logfire.span('{method=} {url!r}', url=url, method='POST'):
        r = None
        try:
            r = await client.post(url=url, json=data, headers=headers, timeout=8)
        except httpx.TimeoutException as terr:
            app_logger.info('Timeout error sending webhook to %s: %s', url, terr)
        except httpx.HTTPError as httperr:
            app_logger.info('HTTP error sending webhook to %s: %s', url, httperr)
    request_data = RequestData(
        endpoint_id=endpoint_id, request_headers=json.dumps(headers), request_body=json.dumps(data)
    )
    if r is not None:
        request_data.response_headers = json.dumps(dict(r.headers))
        request_data.response_body = json.dumps(r.content.decode())
        request_data.status_code = r.status_code
        request_data.successful_response = True
    return request_data


acceptable_url_schemes = ('http', 'https', 'ftp', 'ftps')


def get_qlength():
    """
    Get the length of the queue from celery. Celery returns a dictionary like so: {'queue_name': [task1, task2, ...]}
    so to get qlength we simply aggregate the length of all task lists
    """
    qlength = 0
    celery_inspector = celery_app.control.inspect()
    dict_of_queues = celery_inspector.reserved()
    if dict_of_queues and isinstance(dict_of_queues, dict):
        for k, v in dict_of_queues.items():
            qlength += len(v)

    return qlength


async def _async_post_webhooks(endpoints, url_extension, payload):
    webhook_logs = []
    total_success, total_failed = 0, 0
    # Temporary fix for the issue with the number of connections caused by a certain client
    limits = httpx.Limits(max_connections=250)

    async with AsyncClient(limits=limits) as client:
        tasks = []
        for endpoint in endpoints:
            # Check if the webhook URL is valid
            if not endpoint.webhook_url.startswith(acceptable_url_schemes):
                app_logger.error(
                    'Webhook URL does not start with an acceptable url scheme: %s (%s)',
                    endpoint.webhook_url,
                    endpoint.id,
                )
                continue
            # Create sig for the endpoint
            webhook_sig = hmac.new(endpoint.api_key.encode(), payload.encode(), hashlib.sha256)
            sig_hex = webhook_sig.hexdigest()

            url = endpoint.webhook_url
            if url_extension:
                url += f'/{url_extension}'
            # Send the Webhook to the endpoint

            loaded_payload = json.loads(payload)
            task = asyncio.ensure_future(
                webhook_request(client, url, endpoint.id, webhook_sig=sig_hex, data=loaded_payload)
            )
            tasks.append(task)
        webhook_responses = await asyncio.gather(*tasks, return_exceptions=True)
        for response in webhook_responses:
            if not isinstance(response, RequestData):
                app_logger.info('No response from endpoint %s: %s. %s', endpoint.id, endpoint.webhook_url, response)
                continue
            elif not response.successful_response:
                app_logger.info('No response from endpoint %s: %s', endpoint.id, endpoint.webhook_url)

            if response.status_code in {200, 201, 202, 204}:
                status = 'Success'
                total_success += 1
            else:
                status = 'Unexpected response'
                total_failed += 1

            # Log the response
            webhook_logs.append(
                WebhookLog(
                    webhook_endpoint_id=response.endpoint_id,
                    request_headers=response.request_headers,
                    request_body=response.request_body,
                    response_headers=response.response_headers,
                    response_body=response.response_body,
                    status=status,
                    status_code=response.status_code,
                )
            )
    return webhook_logs, total_success, total_failed


@celery_app.task
def task_send_webhooks(
    payload: str,
    url_extension: str = None,
):
    """
    Send the webhook to the relevant endpoints
    """
    loaded_payload = json.loads(payload)
    loaded_payload['_request_time'] = loaded_payload.pop('request_time')
    qlength = get_qlength()

    if loaded_payload.get('events'):
        branch_id = loaded_payload['events'][0]['branch']
    else:
        branch_id = loaded_payload['branch_id']

    if qlength > 100:
        app_logger.error('Queue is too long. Check workers and speeds.')

    app_logger.info('Starting send webhook task for branch %s. qlength=%s.', branch_id, qlength)
    lf_span = 'Sending webhooks for branch: {branch_id=}'
    with logfire.span(lf_span, branch_id=branch_id):
        with Session(engine) as db:
            # Get all the endpoints for the branch
            endpoints_query = select(WebhookEndpoint).where(
                WebhookEndpoint.branch_id == branch_id, WebhookEndpoint.active
            )
            endpoints = db.exec(endpoints_query).all()

            webhook_logs, total_success, total_failed = asyncio.run(
                _async_post_webhooks(endpoints, url_extension, payload)
            )
            for webhook_log in webhook_logs:
                db.add(webhook_log)
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
        count = (
            db.query(WebhookLog)
            .with_entities(func.count())
            .where(WebhookLog.timestamp < date_to_delete_before)
            .scalar()
        )
    return count


@celery_app.task
def _delete_old_logs_job():
    # with logfire.span('Started to delete old logs'):
    with Session(engine) as db:
        # Get all logs older than 15 days
        date_to_delete_before = datetime.now(UTC) - timedelta(days=15)
        count = get_count(date_to_delete_before)
        delete_limit = 4999
        while count > 0:
            app_logger.info(f'Deleting {count} logs')
            logs_to_delete = db.exec(
                select(WebhookLog).where(WebhookLog.timestamp < date_to_delete_before).limit(delete_limit)
            ).all()
            delete_statement = delete(WebhookLog).where(WebhookLog.id.in_(log.id for log in logs_to_delete))
            db.exec(delete_statement)
            db.commit()
            count -= delete_limit

            del logs_to_delete
            del delete_statement
            gc.collect()

    cache.delete(DELETE_JOBS_KEY)
