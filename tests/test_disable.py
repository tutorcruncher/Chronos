"""End-to-end tests for webhook endpoint disable-on-failure and TC2 notification."""

import json
from datetime import datetime, timedelta
from unittest.mock import patch

import httpx
import pytest
import respx
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlmodel import Session, select

from chronos.sql_models import WebhookEndpoint, WebhookLog, WebhookStatus
from chronos.utils import settings
from chronos.worker import task_retry_single_webhook, task_send_webhooks
from tests.test_helpers import (
    create_webhook_log_from_dft_data,
    get_dft_webhook_data,
)

# tc_id range used by disable tests; cleaned up after each test so other test files see a clean DB.
DISABLE_TEST_TC_IDS = range(200, 230)


@pytest.fixture(autouse=True)
def no_retry_enqueue():
    """Suppress real Celery retry enqueuing so tasks don't fire during other tests."""
    with patch.object(task_retry_single_webhook, 'apply_async'):
        yield


@pytest.fixture(autouse=True)
def cleanup_disable_data(app_db: Session):
    yield
    ids = app_db.exec(select(WebhookEndpoint.id).where(WebhookEndpoint.tc_id.in_(DISABLE_TEST_TC_IDS))).all()
    if ids:
        app_db.exec(delete(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_(ids)))
        app_db.exec(delete(WebhookEndpoint).where(WebhookEndpoint.id.in_(ids)))
        app_db.commit()


# Use branch_id 199 so disable tests don't share branch 99 with retry tests (avoids cross-test disables).
def _create_endpoint(app_db: Session, branch_id: int = 199, active: bool = True, **kwargs) -> WebhookEndpoint:
    ep = WebhookEndpoint(
        tc_id=kwargs.get('tc_id', 200),
        name=kwargs.get('name', 'disable-test'),
        branch_id=branch_id,
        webhook_url=kwargs.get('webhook_url', 'https://disable-test.example.com/hook'),
        api_key=kwargs.get('api_key', 'secret'),
        active=active,
    )
    app_db.add(ep)
    app_db.commit()
    app_db.refresh(ep)
    return ep


def _add_logs(app_db: Session, endpoint_id: int, count: int, failure_count: int, minutes_ago: int = 0):
    """Add count logs, failure_count of them with status != Success, with timestamp in window."""
    base_ts = datetime.utcnow() - timedelta(minutes=minutes_ago or 1)
    for _ in range(failure_count):
        app_db.add(
            create_webhook_log_from_dft_data(
                webhook_endpoint_id=endpoint_id,
                status=WebhookStatus.UNEXPECTED_RESPONSE,
                status_code=500,
                timestamp=base_ts,
            )
        )
    for _ in range(count - failure_count):
        app_db.add(
            create_webhook_log_from_dft_data(
                webhook_endpoint_id=endpoint_id,
                status=WebhookStatus.SUCCESS,
                status_code=200,
                timestamp=base_ts,
            )
        )
    app_db.commit()


@respx.mock
def test_disable_after_repeated_failures(client: TestClient, app_db: Session):
    """Repeated failures exceed threshold -> endpoint disabled and TC2 notified."""
    ep = _create_endpoint(app_db, tc_id=200)
    # 10 logs, 3 failures = 30% > 20%, so disable triggers
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))

    notify_url = 'https://tc2.example.com/endpoint-disabled'
    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        mock_notify = respx.post(notify_url).mock(return_value=httpx.Response(200))
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False
    assert mock_notify.called
    body = json.loads(mock_notify.calls[0].request.content)
    headers = mock_notify.calls[0].request.headers
    assert body['tc_id'] == 200
    assert body['branch_id'] == 199
    assert body['name'] == ep.name
    assert body['webhook_url'] == ep.webhook_url
    assert body['failure_count'] == 4  # 3 + 1 from this send
    assert body['total_attempts'] == 11
    assert body['window_minutes'] == settings.webhook_disable_failure_window_minutes
    assert body['reason'] == 'too_many_failures'
    assert headers['authorization'] == f'Bearer {settings.tc2_shared_key}'
    assert headers['content-type'] == 'application/json'


@respx.mock
def test_timeout_failure_can_trigger_disable_and_notify(client: TestClient, app_db: Session):
    """Timeouts count as failures for disable calculations and send the normal TC2 payload."""
    ep = _create_endpoint(app_db, tc_id=207)
    # 9 logs, 2 failures. One timeout makes 3/10 = 30% and meets the min-attempts threshold.
    _add_logs(app_db, ep.id, count=9, failure_count=2)
    respx.post(ep.webhook_url).mock(side_effect=httpx.TimeoutException('timeout'))

    notify_url = 'https://tc2.example.com/endpoint-disabled'
    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        mock_notify = respx.post(notify_url).mock(return_value=httpx.Response(200))
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
    assert len(logs) == 10
    assert logs[-1].status == WebhookStatus.NO_RESPONSE
    assert ep.active is False
    assert mock_notify.called
    body = json.loads(mock_notify.calls[0].request.content)
    assert body['failure_count'] == 3
    assert body['total_attempts'] == 10
    assert body['reason'] == 'too_many_failures'


@respx.mock
def test_disable_not_triggered_below_min_attempts(client: TestClient, app_db: Session):
    """Below min_attempts (10) -> endpoint stays active."""
    ep = _create_endpoint(app_db, tc_id=201)
    _add_logs(app_db, ep.id, count=3, failure_count=3)  # only 3 attempts
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))

    with patch.object(settings, 'tc2_endpoint_disabled_url', 'https://tc2.example.com/disabled'):
        mock_notify = respx.post('https://tc2.example.com/disabled')
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is True
    assert not mock_notify.called


@respx.mock
def test_disable_not_triggered_below_threshold(client: TestClient, app_db: Session):
    """10 logs with 1 failure (10%); one more failure -> 2/11 < 20%, stays active."""
    ep = _create_endpoint(app_db, tc_id=202)
    _add_logs(app_db, ep.id, count=10, failure_count=1)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))

    with patch.object(settings, 'tc2_endpoint_disabled_url', 'https://tc2.example.com/disabled'):
        mock_notify = respx.post('https://tc2.example.com/disabled')
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is True
    assert not mock_notify.called


@respx.mock
def test_notify_url_not_set(client: TestClient, app_db: Session):
    """tc2_endpoint_disabled_url=None -> endpoint still disabled, no POST to TC2 notify."""
    ep = _create_endpoint(app_db, tc_id=203)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))
    notify_url = 'https://tc2-notify.example.com/disabled'
    mock_notify = respx.post(notify_url)

    with patch.object(settings, 'tc2_endpoint_disabled_url', None):
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False
    assert not mock_notify.called  # no notify when URL is None


@respx.mock
def test_notify_tc2_returns_error(client: TestClient, app_db: Session):
    """TC2 notify returns 400 -> warning logged, no exception, task completes."""
    ep = _create_endpoint(app_db, tc_id=204)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))
    notify_url = 'https://tc2.example.com/disabled'
    respx.post(notify_url).mock(return_value=httpx.Response(400, text='Bad request'))

    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False


@respx.mock
def test_notify_tc2_unreachable(client: TestClient, app_db: Session):
    """TC2 notify raises -> exception logged, task completes."""
    ep = _create_endpoint(app_db, tc_id=205)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(ep.webhook_url).mock(return_value=httpx.Response(503))
    notify_url = 'https://tc2.example.com/disabled'
    respx.post(notify_url).mock(side_effect=httpx.ConnectError('connection failed'))

    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False


@respx.mock
def test_disable_already_inactive_endpoint_skipped(client: TestClient, app_db: Session):
    """Endpoint already inactive when threshold check runs -> TC2 is NOT notified a second time."""
    ep = _create_endpoint(app_db, tc_id=206, active=False)
    _add_logs(app_db, ep.id, count=10, failure_count=4)
    notify_url = 'https://tc2.example.com/disabled'
    mock_notify = respx.post(notify_url)

    from sqlmodel import Session as DbSession

    from chronos.db import engine
    from chronos.worker import _check_and_disable_endpoint_if_needed

    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        with DbSession(engine) as db:
            ep_in_session = db.get(WebhookEndpoint, ep.id)
            _check_and_disable_endpoint_if_needed(db, ep_in_session)
            db.commit()

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False  # still inactive, unchanged
    assert not mock_notify.called  # no duplicate notification


@respx.mock
def test_disable_skipped_for_tutorcruncher_subdomain(client: TestClient, app_db: Session):
    """First-party *.tutorcruncher.com webhook URLs are never auto-disabled."""
    tc_url = 'https://hooks.tutorcruncher.com/inbound'
    ep = _create_endpoint(app_db, tc_id=210, webhook_url=tc_url)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(tc_url).mock(return_value=httpx.Response(503))
    notify_url = 'https://tc2.example.com/disabled'
    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        mock_notify = respx.post(notify_url).mock(return_value=httpx.Response(200))
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is True
    assert not mock_notify.called


@respx.mock
def test_disable_skipped_for_tutorcruncher_apex_domain(client: TestClient, app_db: Session):
    """Apex tutorcruncher.com host is also exempt from auto-disable."""
    tc_url = 'https://tutorcruncher.com/api/webhook'
    ep = _create_endpoint(app_db, tc_id=211, webhook_url=tc_url)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(tc_url).mock(return_value=httpx.Response(503))
    notify_url = 'https://tc2.example.com/disabled'
    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        mock_notify = respx.post(notify_url).mock(return_value=httpx.Response(200))
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is True
    assert not mock_notify.called


@respx.mock
def test_disable_still_applies_for_typosquat_tutorcruncher_domain(client: TestClient, app_db: Session):
    """Hosts that merely contain 'tutorcruncher' but are not under tutorcruncher.com still disable."""
    squatter_url = 'https://fake-tutorcruncher.com/hook'
    ep = _create_endpoint(app_db, tc_id=212, webhook_url=squatter_url)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(squatter_url).mock(return_value=httpx.Response(503))
    notify_url = 'https://tc2.example.com/disabled'
    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        mock_notify = respx.post(notify_url).mock(return_value=httpx.Response(200))
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False
    assert mock_notify.called


@respx.mock
def test_disable_applies_when_webhook_url_has_no_hostname_e2e(client: TestClient, app_db: Session):
    """https with empty netloc has no hostname -> not exempt; same path as normal auto-disable (TC2 -> worker -> DB)."""
    no_host_url = 'https:///webhook'
    ep = _create_endpoint(app_db, tc_id=213, webhook_url=no_host_url)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    # No respx route for this URL: outbound POST fails (respx AllMockedAssertionError and/or httpx URL handling);
    # worker still records a non-success log so this batch triggers _check_and_disable_endpoint_if_needed.
    notify_url = 'https://tc2.example.com/disabled'
    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        mock_notify = respx.post(notify_url).mock(return_value=httpx.Response(200))
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False
    assert mock_notify.called


@respx.mock
def test_disable_skipped_for_tutorcruncher_mixed_case_host_e2e(client: TestClient, app_db: Session):
    """Exemption uses hostname case-insensitively; full send path leaves endpoint active."""
    tc_url = 'https://HOOKS.TUTORCRUNCHER.COM/inbound'
    ep = _create_endpoint(app_db, tc_id=214, webhook_url=tc_url)
    _add_logs(app_db, ep.id, count=10, failure_count=3)
    respx.post(tc_url).mock(return_value=httpx.Response(503))
    notify_url = 'https://tc2.example.com/disabled'
    with patch.object(settings, 'tc2_endpoint_disabled_url', notify_url):
        mock_notify = respx.post(notify_url).mock(return_value=httpx.Response(200))
        task_send_webhooks(payload=json.dumps(get_dft_webhook_data(branch_id=199)), url_extension=None)

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is True
    assert not mock_notify.called


def test_disable_check_uses_naive_utc_timestamps(client: TestClient, app_db: Session):
    """Regression: _check_and_disable_endpoint_if_needed must use naive UTC datetimes.

    WebhookLog.timestamp is 'timestamp without time zone' (naive UTC). If the window
    query uses a timezone-aware datetime, PostgreSQL converts the naive column values
    via the session timezone (e.g. Europe/London = BST = UTC+1), shifting them and
    causing the count to return 0 — silently skipping the disable.
    """
    from sqlalchemy import text
    from sqlmodel import Session as DbSession

    from chronos.db import engine
    from chronos.worker import _check_and_disable_endpoint_if_needed

    ep = _create_endpoint(app_db, tc_id=220)
    now_naive = datetime.utcnow()
    for _ in range(10):
        app_db.add(
            create_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id,
                status=WebhookStatus.UNEXPECTED_RESPONSE,
                status_code=503,
                timestamp=now_naive - timedelta(minutes=1),
            )
        )
    app_db.commit()

    # Set a non-UTC session timezone so the bug surfaces if the code
    # uses timezone-aware datetimes in the window query.
    with DbSession(engine) as db:
        db.exec(text("SET LOCAL timezone = 'Pacific/Auckland'"))
        ep_in_session = db.get(WebhookEndpoint, ep.id)
        _check_and_disable_endpoint_if_needed(db, ep_in_session)
        db.commit()

    app_db.expire_all()
    app_db.refresh(ep)
    assert ep.active is False


def test_delete_old_logs_uses_naive_utc_timestamps(client: TestClient, app_db: Session):
    """Regression: _delete_old_logs_job must use naive UTC datetimes.

    Same root cause as the disable check: if the delete cutoff is timezone-aware,
    PostgreSQL interprets the naive column values in the session timezone and the
    comparison is off by the UTC offset.
    """
    from chronos.worker import _delete_old_logs_job

    ep = _create_endpoint(app_db, tc_id=221)
    now_naive = datetime.utcnow()
    # 5 logs from 16 days ago (should be deleted) and 5 from 1 day ago (should be kept)
    for _ in range(5):
        app_db.add(
            create_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id,
                timestamp=now_naive - timedelta(days=16),
            )
        )
    for _ in range(5):
        app_db.add(
            create_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id,
                timestamp=now_naive - timedelta(days=1),
            )
        )
    app_db.commit()

    _delete_old_logs_job()

    app_db.expire_all()
    remaining = app_db.exec(
        select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)
    ).all()
    assert len(remaining) == 5
