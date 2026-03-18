"""End-to-end tests for webhook retry logic. Start from TC2 request through to DB verification."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import httpx
import pytest
import respx
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlmodel import Session, select

from chronos.pydantic_schema import RequestData
from chronos.sql_models import WebhookEndpoint, WebhookLog, WebhookStatus
from chronos.tasks.dispatcher import dispatch_cycle
from chronos.utils import settings
from chronos.worker import job_queue, task_retry_single_webhook, task_send_webhooks
from tests.test_helpers import _get_webhook_headers, get_dft_webhook_data, send_webhook_url

# tc_id range used by retry tests; cleaned up after each test so other test files see a clean DB.
RETRY_TEST_TC_IDS = range(100, 112)


@pytest.fixture(autouse=True)
def cleanup_retry_data(app_db: Session):
    yield
    ids = app_db.exec(select(WebhookEndpoint.id).where(WebhookEndpoint.tc_id.in_(RETRY_TEST_TC_IDS))).all()
    if ids:
        app_db.exec(delete(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_(ids)))
        app_db.exec(delete(WebhookEndpoint).where(WebhookEndpoint.id.in_(ids)))
        app_db.commit()


def _create_endpoint(app_db: Session, branch_id: int = 99, active: bool = True, **kwargs) -> WebhookEndpoint:
    ep = WebhookEndpoint(
        tc_id=kwargs.get('tc_id', 1),
        name=kwargs.get('name', 'retry-test'),
        branch_id=branch_id,
        webhook_url=kwargs.get('webhook_url', 'https://retry-test.example.com/hook'),
        api_key=kwargs.get('api_key', 'secret'),
        active=active,
    )
    app_db.add(ep)
    app_db.commit()
    app_db.refresh(ep)
    return ep


@respx.mock
def test_retry_on_503_then_succeeds(client: TestClient, app_db: Session):
    """TC2 request -> endpoint returns 503 -> retry enqueued -> run retry with 200 -> success."""
    ep = _create_endpoint(app_db, tc_id=100)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))

    payload = get_dft_webhook_data()
    r = client.post(send_webhook_url, data=json.dumps(payload), headers=_get_webhook_headers())
    assert r.status_code == 200

    # Run the job from the queue in-process and capture retry enqueue
    with patch.object(task_retry_single_webhook, 'apply_async') as mock_retry_apply:
        with patch.object(task_send_webhooks, 'apply_async') as mock_send_apply:

            def run_task_inline(*args, **kwargs):
                kw = kwargs.get('kwargs', kwargs)
                task_send_webhooks(payload=kw['payload'], url_extension=kw.get('url_extension'))

            mock_send_apply.side_effect = run_task_inline
            dispatch_cycle(batch_limit=10)

    assert mock_retry_apply.called
    call_kw = mock_retry_apply.call_args.kwargs
    assert call_kw['kwargs']['attempt'] == 1
    assert call_kw['kwargs']['first_attempt_at']

    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 1
    assert logs[0].status != WebhookStatus.SUCCESS

    # Run retry with downstream now returning 200
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(200))
    endpoint_id = ep.id
    payload_str = call_kw['args'][1]
    task_retry_single_webhook(
        endpoint_id,
        payload_str,
        url_extension=call_kw['kwargs'].get('url_extension'),
        first_attempt_at=call_kw['kwargs']['first_attempt_at'],
        attempt=call_kw['kwargs']['attempt'],
    )

    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 2
    assert logs[1].status == WebhookStatus.SUCCESS
    assert logs[1].status_code == 200


@respx.mock
def test_retry_on_429(client: TestClient, app_db: Session):
    """TC2 request -> endpoint returns 429 -> retry enqueued."""
    ep = _create_endpoint(app_db, tc_id=101)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(429))

    payload = get_dft_webhook_data()
    with patch.object(task_retry_single_webhook, 'apply_async') as mock_retry:
        task_send_webhooks(payload=json.dumps(payload), url_extension=None)
    assert mock_retry.called


@respx.mock
def test_retry_on_timeout(client: TestClient, app_db: Session):
    """TC2 request -> endpoint times out -> WebhookLog has No response, retry enqueued."""
    ep = _create_endpoint(app_db, tc_id=102)
    respx.post(ep.webhook_url).mock(side_effect=httpx.TimeoutException('timeout'))

    payload = get_dft_webhook_data()
    with patch.object(task_retry_single_webhook, 'apply_async') as mock_retry:
        task_send_webhooks(payload=json.dumps(payload), url_extension=None)

    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 1
    assert logs[0].status == WebhookStatus.NO_RESPONSE
    assert mock_retry.called


@respx.mock
def test_no_retry_on_400(client: TestClient, app_db: Session):
    """TC2 request -> endpoint returns 400 -> WebhookLog written, retry NOT enqueued."""
    ep = _create_endpoint(app_db, tc_id=103)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(400))

    payload = get_dft_webhook_data()
    with patch.object(task_retry_single_webhook, 'apply_async') as mock_retry:
        task_send_webhooks(payload=json.dumps(payload), url_extension=None)

    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 1
    assert logs[0].status == WebhookStatus.UNEXPECTED_RESPONSE
    assert logs[0].status_code == 400
    assert not mock_retry.called


def test_retry_abandoned_past_window(app_db: Session):
    """Retry task with first_attempt_at 31 min ago -> no HTTP, no log, no re-enqueue."""
    ep = _create_endpoint(app_db, tc_id=104)
    first_attempt = (datetime.now(UTC) - timedelta(minutes=31)).timestamp()
    payload_str = json.dumps(get_dft_webhook_data())

    with patch('chronos.worker._send_single_webhook_sync') as mock_send:
        task_retry_single_webhook(
            ep.id,
            payload_str,
            url_extension=None,
            first_attempt_at=first_attempt,
            attempt=1,
        )
    assert not mock_send.called
    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 0


@respx.mock
def test_retry_skips_inactive_endpoint(app_db: Session):
    """Endpoint active=False -> task_retry_single_webhook does not send HTTP or add log."""
    ep = _create_endpoint(app_db, tc_id=105, active=False)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(200))
    payload_str = json.dumps(get_dft_webhook_data())
    first_attempt = datetime.now(UTC).timestamp()

    task_retry_single_webhook(
        ep.id,
        payload_str,
        url_extension=None,
        first_attempt_at=first_attempt,
        attempt=1,
    )

    assert not respx.calls
    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 0


def test_retry_skips_missing_endpoint(app_db: Session):
    """Non-existent endpoint_id -> no error, no log."""
    payload_str = json.dumps(get_dft_webhook_data())
    first_attempt = datetime.now(UTC).timestamp()
    task_retry_single_webhook(999999, payload_str, first_attempt_at=first_attempt, attempt=1)
    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == 999999)).all()
    assert len(logs) == 0


@respx.mock
def test_mixed_success_fail_across_endpoints(client: TestClient, app_db: Session):
    """Two endpoints: one 200, one 503 -> one Success log, one failure log, retry only for failing."""
    ep1 = _create_endpoint(app_db, webhook_url='https://ep1.example.com/hook', tc_id=106)
    ep2 = _create_endpoint(app_db, webhook_url='https://ep2.example.com/hook', tc_id=107)
    respx.post(ep1.webhook_url).mock(return_value=httpx.Response(200))
    respx.post(ep2.webhook_url).mock(return_value=httpx.Response(503))

    payload = get_dft_webhook_data()
    with patch.object(task_retry_single_webhook, 'apply_async') as mock_retry:
        task_send_webhooks(payload=json.dumps(payload), url_extension=None)

    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_([ep1.id, ep2.id]))).all()
    assert len(logs) == 2
    statuses = {log.webhook_endpoint_id: log.status for log in logs}
    assert statuses[ep1.id] == WebhookStatus.SUCCESS
    assert statuses[ep2.id] != WebhookStatus.SUCCESS
    # Retry enqueued only for the failing endpoint (ep2)
    ep2_retry_calls = [c for c in mock_retry.call_args_list if c.kwargs.get('args') and c.kwargs['args'][0] == ep2.id]
    assert len(ep2_retry_calls) == 1


def test_is_retryable_no_response():
    """_is_retryable returns True via line 174 when successful_response is falsy and status_code
    is neither 429 nor ≥500 (i.e. the connection was refused/closed mid-stream before a status code arrived)."""
    from chronos.worker import _is_retryable

    # Simulate a response where the server accepted the TCP connection but sent no HTTP status
    # (e.g. a connection reset). This creates a RequestData with a low status_code but no actual response.
    response = RequestData(endpoint_id=1, request_headers='{}', request_body='{}', status_code=0)
    assert not response.successful_response  # default False
    assert _is_retryable(response) is True  # hits line 174


@respx.mock
def test_retry_task_reenqueues_on_503(app_db: Session):
    """task_retry_single_webhook with 503 and recent first_attempt -> re-enqueues with attempt+1."""
    ep = _create_endpoint(app_db, tc_id=109)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))
    payload_str = json.dumps(get_dft_webhook_data())
    first_attempt = datetime.now(UTC).timestamp()

    with patch.object(task_retry_single_webhook, 'apply_async') as mock_apply:
        task_retry_single_webhook(
            ep.id,
            payload_str,
            url_extension=None,
            first_attempt_at=first_attempt,
            attempt=1,
        )

    assert mock_apply.called
    call_kw = mock_apply.call_args.kwargs
    assert call_kw['kwargs']['attempt'] == 2
    assert call_kw['kwargs']['first_attempt_at'] == first_attempt
    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 1
    assert logs[0].status != WebhookStatus.SUCCESS


@respx.mock
def test_retry_task_invalid_url_returns_early(app_db: Session):
    """task_retry_single_webhook with an endpoint whose URL has an invalid scheme returns without a log."""
    ep = _create_endpoint(app_db, tc_id=110, webhook_url='mailto:bad@example.com')
    payload_str = json.dumps(get_dft_webhook_data())
    first_attempt = datetime.now(UTC).timestamp()

    task_retry_single_webhook(
        ep.id,
        payload_str,
        url_extension=None,
        first_attempt_at=first_attempt,
        attempt=1,
    )

    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 0


@respx.mock
def test_retry_task_nonretryable_400_no_reenqueue(app_db: Session):
    """task_retry_single_webhook with a 400 response -> log written, no re-enqueue."""
    ep = _create_endpoint(app_db, tc_id=111)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(400))
    payload_str = json.dumps(get_dft_webhook_data())
    first_attempt = datetime.now(UTC).timestamp()

    with patch.object(task_retry_single_webhook, 'apply_async') as mock_apply:
        task_retry_single_webhook(
            ep.id,
            payload_str,
            url_extension=None,
            first_attempt_at=first_attempt,
            attempt=1,
        )

    assert not mock_apply.called
    app_db.expire_all()
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 1
    assert logs[0].status == WebhookStatus.UNEXPECTED_RESPONSE


@respx.mock
def test_queue_too_long_warning(client: TestClient, app_db: Session):
    """When Celery queue length exceeds threshold, app_logger.error is called."""
    ep = _create_endpoint(app_db, tc_id=108)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(200))
    payload = get_dft_webhook_data()
    threshold = settings.dispatcher_max_celery_queue

    with patch.object(job_queue, 'get_celery_queue_length', return_value=threshold + 50):
        with patch('chronos.worker.app_logger') as mock_logger:
            task_send_webhooks(payload=json.dumps(payload), url_extension=None)
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args[0]
    assert 'Queue is too long' in call_args[0]
    assert call_args[1] == threshold + 50
