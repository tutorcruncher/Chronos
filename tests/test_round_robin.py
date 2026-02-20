"""Tests for the round-robin dispatch infrastructure."""

import asyncio
import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import httpx
import pytest
import redis.exceptions
import respx
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from chronos.sql_models import WebhookEndpoint, WebhookLog
from chronos.tasks.dispatcher import dispatch_cycle
from chronos.tasks.queue import ACTIVE_BRANCHES_KEY, BRANCH_KEY_TEMPLATE, JobQueue
from chronos.views import _extract_branch_id
from chronos.worker import _async_post_webhooks, cache, dispatch_branch_task, job_queue, task_send_webhooks
from tests.test_helpers import (
    _get_webhook_headers,
    get_dft_con_webhook_data,
    get_dft_webhook_data,
    send_webhook_url,
    send_webhook_with_extension_url,
)

REALISTIC_TC2_EVENTS = [
    {
        'branch': 3,
        'action': 'REMOVED_A_LABEL_FROM_A_SERVICE',
        'topic': 'SERVICES',
        'data': {'id': 1001, 'label': 'Premium'},
    },
    {
        'branch': 3,
        'action': 'ADDED_A_LABEL_TO_A_SERVICE',
        'topic': 'SERVICES',
        'data': {'id': 1002, 'label': 'Standard'},
    },
]


def test_send_webhooks_round_robin_enabled_uses_dispatch_branch_task(session: Session, client: TestClient):
    payload = get_dft_webhook_data()
    headers = _get_webhook_headers()

    r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 200

    assert job_queue.has_active_jobs()
    assert job_queue.get_queue_length(99) == 1


@patch.object(task_send_webhooks, 'delay')
def test_send_webhooks_round_robin_disabled_uses_direct_delay(
    mock_delay, session: Session, client: TestClient, monkeypatch
):
    from chronos.utils import settings

    monkeypatch.setattr(settings, 'use_round_robin', False)

    payload = get_dft_webhook_data()
    headers = _get_webhook_headers()

    r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 200

    mock_delay.assert_called_once()
    assert not job_queue.has_active_jobs()


def test_extract_branch_id():
    assert _extract_branch_id({'events': [{'branch': 123, 'action': 'test'}]}) == 123
    assert _extract_branch_id({'branch_id': 456, 'id': 1}) == 456
    assert _extract_branch_id({'events': [{'action': 'test'}]}) == 0
    assert _extract_branch_id({'id': 1}) == 0


def test_extract_branch_id_rejects_invalid_values():
    from fastapi import HTTPException

    for bad_payload in [
        {'events': [{'branch': 'not_a_number'}]},
        {'events': [{'branch': True}]},
        {'branch_id': 'abc'},
        {'branch_id': False},
        {'events': [{'branch': [1, 2]}]},
    ]:
        with pytest.raises(HTTPException) as exc_info:
            _extract_branch_id(bad_payload)
        assert exc_info.value.status_code == 422

    assert not job_queue.has_active_jobs()


def test_dispatch_branch_task_enqueues_with_task_name_and_kwargs():
    payload = {'branch_id': 42, 'id': 7}
    dispatch_branch_task(task_send_webhooks, branch_id=42, payload=payload, url_extension='ext')

    assert job_queue.has_active_jobs()
    peeked = job_queue.peek(42)
    assert peeked is not None
    assert peeked.task_name == task_send_webhooks.name
    assert peeked.kwargs == {'payload': json.dumps(payload), 'url_extension': 'ext'}
    assert peeked.branch_id == 42


def test_queue_enqueue_and_ack():
    """enqueue writes list + active set, ack removes correctly based on remaining items."""
    job_queue.enqueue('test_task', branch_id=88, payload='first')
    job_queue.enqueue('test_task', branch_id=88, payload='second')
    assert 88 in job_queue.get_active_branches()
    assert job_queue.get_queue_length(88) == 2

    peeked = job_queue.peek(88)
    assert peeked.task_name == 'test_task'
    assert peeked.kwargs == {'payload': 'first'}

    job_queue.ack(88)
    assert 88 in job_queue.get_active_branches()
    assert job_queue.get_queue_length(88) == 1
    assert job_queue.peek(88).kwargs == {'payload': 'second'}

    job_queue.ack(88)
    assert 88 not in job_queue.get_active_branches()
    assert job_queue.get_queue_length(88) == 0


def test_queue_ack_noscript_error_reloads_lua_and_retries():
    job_queue.enqueue('test_task', branch_id=55, payload='data')
    JobQueue._ack_script_sha = 'deadbeef_invalid_sha'

    job_queue.ack(55)

    assert job_queue.get_queue_length(55) == 0
    assert 55 not in job_queue.get_active_branches()
    assert JobQueue._ack_script_sha != 'deadbeef_invalid_sha'


@patch.object(task_send_webhooks, 'apply_async')
def test_dispatch_cycle_cursor_rotation_with_stale_cursor(mock_apply):
    """With cursor=7 and branches [5, 10], dispatch starts from 10 (bisect_right)."""
    job_queue.enqueue(task_send_webhooks.name, branch_id=5, payload='p5')
    job_queue.enqueue(task_send_webhooks.name, branch_id=10, payload='p10')
    job_queue.set_cursor(7)

    dispatched = dispatch_cycle(batch_limit=1)

    assert dispatched == 1
    assert job_queue.get_cursor() == 10
    assert job_queue.get_queue_length(10) == 0
    assert job_queue.get_queue_length(5) == 1


def test_dispatch_cycle_bad_jobs_are_acked():
    """Unknown tasks and poison payloads are acked and skipped."""
    job_queue.enqueue('nonexistent_task_xyz', branch_id=42, payload='data')
    dispatched = dispatch_cycle()
    assert dispatched == 0
    assert not job_queue.has_active_jobs()

    cache.rpush(BRANCH_KEY_TEMPLATE.format(43), 'not valid json{{{')
    cache.sadd(ACTIVE_BRANCHES_KEY, '43')
    dispatched = dispatch_cycle()
    assert dispatched == 0
    assert not job_queue.has_active_jobs()


def test_dispatch_cycle_apply_async_failure_not_acked():
    """When apply_async fails, the job remains in the queue."""
    job_queue.enqueue(task_send_webhooks.name, branch_id=42, payload='p')

    with patch.object(task_send_webhooks, 'apply_async', side_effect=RuntimeError('broker down')):
        dispatched = dispatch_cycle()

    assert dispatched == 0
    assert job_queue.has_active_jobs()
    assert job_queue.get_queue_length(42) == 1


@patch.object(task_send_webhooks, 'apply_async')
def test_dispatch_cycle_cursor_update_failure_does_not_fail_dispatch(mock_apply):
    """Cursor update failure after dispatch doesn't affect the dispatch count."""
    job_queue.enqueue(task_send_webhooks.name, branch_id=42, payload='p')

    with patch.object(job_queue, 'set_cursor', side_effect=RuntimeError('redis error')):
        dispatched = dispatch_cycle()

    assert dispatched == 1
    assert not job_queue.has_active_jobs()


def test_job_dispatcher_task_backpressure_and_idle():
    """Backpressure skips dispatch_cycle; idle path sleeps at idle_delay."""
    from chronos.worker import job_dispatcher_task

    sleep_calls = []

    def backpressure_sleep(s):
        sleep_calls.append(s)
        if len(sleep_calls) >= 2:
            raise SystemExit

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=True),
        patch.object(job_queue, 'get_celery_queue_length', return_value=100),
        patch('time.sleep', side_effect=backpressure_sleep),
        patch('chronos.tasks.dispatcher.dispatch_cycle') as mock_dispatch,
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()
        mock_dispatch.assert_not_called()
        assert sleep_calls[0] == 0.01

    sleep_calls.clear()

    def idle_sleep(s):
        sleep_calls.append(s)
        if len(sleep_calls) >= 2:
            raise SystemExit

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=False),
        patch('time.sleep', side_effect=idle_sleep),
        patch('chronos.tasks.dispatcher.dispatch_cycle') as mock_dispatch,
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()
        mock_dispatch.assert_not_called()
        assert sleep_calls[0] == 1.0


def test_job_dispatcher_task_catches_generic_exception_and_keeps_running():
    """The dispatcher catches generic exceptions and continues running."""
    from chronos.worker import job_dispatcher_task

    call_count = 0

    def dispatch_side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError('transient error')
        raise SystemExit

    sleep_calls = []

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=True),
        patch.object(job_queue, 'get_celery_queue_length', return_value=0),
        patch('chronos.tasks.dispatcher.dispatch_cycle', side_effect=dispatch_side_effect),
        patch('time.sleep', side_effect=lambda s: sleep_calls.append(s)),
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()

    assert call_count == 2
    assert 1.0 in sleep_calls


def test_worker_ready_starts_dispatcher_only_on_dispatcher_queue():
    from chronos.tasks.worker_startup import start_dispatcher_on_worker_ready

    mock_queue = MagicMock()
    mock_queue.name = 'dispatcher'
    mock_sender = MagicMock()
    mock_sender.app.amqp.queues.consume_from = [mock_queue]

    with patch('chronos.worker.job_dispatcher_task') as mock_task:
        start_dispatcher_on_worker_ready(sender=mock_sender)
        mock_task.delay.assert_called_once()

    mock_queue_regular = MagicMock()
    mock_queue_regular.name = 'celery'
    mock_sender_regular = MagicMock()
    mock_sender_regular.app.amqp.queues.consume_from = [mock_queue_regular]

    with patch('chronos.worker.job_dispatcher_task') as mock_task:
        start_dispatcher_on_worker_ready(sender=mock_sender_regular)
        mock_task.delay.assert_not_called()


def test_task_send_webhooks_missing_request_time_behavior(session: Session, client: TestClient):
    """API rejects missing request_time; task raises KeyError if it bypasses validation."""
    headers = _get_webhook_headers()
    payload_no_request_time = {'events': REALISTIC_TC2_EVENTS}

    r = client.post(send_webhook_url, data=json.dumps(payload_no_request_time), headers=headers)
    assert r.status_code == 422

    with pytest.raises(KeyError, match='request_time'):
        task_send_webhooks(json.dumps(payload_no_request_time))


def test_task_send_webhooks_autoretry_config_is_set():
    """Assert autoretry_for, retry_backoff, and max_retries are configured on the task."""
    assert task_send_webhooks.autoretry_for == (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)
    assert task_send_webhooks.retry_backoff is True
    assert task_send_webhooks.retry_kwargs == {'max_retries': 3}


def test_async_post_webhooks_empty_events_list():
    """events=[] produces zero outgoing requests even with active endpoints."""
    endpoint = WebhookEndpoint(
        id=1,
        tc_id=1,
        name='test-endpoint',
        branch_id=3,
        webhook_url='https://example.com/hook',
        api_key='secret',
        active=True,
    )
    payload = json.dumps({'events': [], 'request_time': 1771509452})

    logs, success, failed = asyncio.run(_async_post_webhooks([endpoint], None, payload))

    assert logs == []
    assert success == 0
    assert failed == 0


def test_async_post_webhooks_mixed_valid_invalid_endpoint_urls():
    """Invalid URL schemes are skipped; valid endpoints process all events."""
    valid_endpoint = WebhookEndpoint(
        id=1,
        tc_id=1,
        name='valid-hook',
        branch_id=3,
        webhook_url='https://valid.example.com/hook',
        api_key='key1',
        active=True,
    )
    invalid_endpoint = WebhookEndpoint(
        id=2,
        tc_id=2,
        name='invalid-hook',
        branch_id=3,
        webhook_url='foobar://not-a-real-url',
        api_key='key2',
        active=True,
    )
    payload = json.dumps(
        {
            'events': REALISTIC_TC2_EVENTS,
            'request_time': 1771509452,
        }
    )

    with respx.mock:
        respx.post('https://valid.example.com/hook').mock(return_value=httpx.Response(200))
        logs, success, failed = asyncio.run(_async_post_webhooks([valid_endpoint, invalid_endpoint], None, payload))

    assert len(logs) == 2
    assert success == 2
    assert failed == 0
    assert all(log.webhook_endpoint_id == 1 for log in logs)


def test_async_post_webhooks_response_exception_does_not_break_other_tasks():
    """An exception from one endpoint doesn't prevent logging of successful ones."""
    failing = WebhookEndpoint(
        id=1,
        tc_id=1,
        name='failing-hook',
        branch_id=3,
        webhook_url='https://failing.example.com/hook',
        api_key='key1',
        active=True,
    )
    healthy = WebhookEndpoint(
        id=2,
        tc_id=2,
        name='healthy-hook',
        branch_id=3,
        webhook_url='https://healthy.example.com/hook',
        api_key='key2',
        active=True,
    )
    payload = json.dumps(
        {
            'events': [REALISTIC_TC2_EVENTS[0]],
            'request_time': 1771509452,
        }
    )

    with respx.mock:
        respx.post('https://failing.example.com/hook').mock(side_effect=RuntimeError('connection exploded'))
        respx.post('https://healthy.example.com/hook').mock(return_value=httpx.Response(200))
        logs, success, failed = asyncio.run(_async_post_webhooks([failing, healthy], None, payload))

    assert len(logs) == 1
    assert success == 1
    assert failed == 0
    assert logs[0].webhook_endpoint_id == 2


@patch.object(task_send_webhooks, 'apply_async')
def test_round_robin_end_to_end_interleaving_two_branches(mock_apply, session: Session, client: TestClient):
    """Two branches enqueued via API are dispatched fairly: one from each per cycle."""
    headers = _get_webhook_headers()

    payload_branch_3 = {
        'events': [
            {'branch': 3, 'action': 'REMOVED_A_LABEL_FROM_A_SERVICE', 'topic': 'SERVICES', 'data': {'id': 1001}}
        ],
        'request_time': 1771509452,
    }
    payload_branch_7 = {
        'events': [{'branch': 7, 'action': 'ADDED_A_LABEL_TO_A_SERVICE', 'topic': 'SERVICES', 'data': {'id': 2001}}],
        'request_time': 1771509452,
    }

    for _ in range(2):
        r = client.post(send_webhook_url, data=json.dumps(payload_branch_3), headers=headers)
        assert r.status_code == 200
    r = client.post(send_webhook_url, data=json.dumps(payload_branch_7), headers=headers)
    assert r.status_code == 200

    assert job_queue.get_queue_length(3) == 2
    assert job_queue.get_queue_length(7) == 1

    dispatched = dispatch_cycle(batch_limit=2)
    assert dispatched == 2
    assert job_queue.get_queue_length(3) == 1
    assert job_queue.get_queue_length(7) == 0

    dispatched = dispatch_cycle(batch_limit=2)
    assert dispatched == 1
    assert not job_queue.has_active_jobs()
    assert mock_apply.call_count == 3


def test_queue_singleton_wiring_in_worker():
    """job_queue in worker.py uses the same Redis client as cache."""
    assert job_queue.redis_client is cache


def test_dispatch_cycle_no_active_branches():
    """dispatch_cycle returns 0 when no branches have pending jobs."""
    assert not job_queue.has_active_jobs()
    dispatched = dispatch_cycle()
    assert dispatched == 0


def test_dispatch_cycle_peek_returns_none_for_empty_branch():
    """A branch in the active set but with an empty LIST is skipped via peek returning None."""
    cache.sadd(ACTIVE_BRANCHES_KEY, '999')

    dispatched = dispatch_cycle()
    assert dispatched == 0


def test_dispatch_cycle_poison_payload_ack_failure():
    """When ack itself fails on a poison payload, the dispatcher continues without crashing."""
    cache.rpush(BRANCH_KEY_TEMPLATE.format(77), 'not valid json{{{')
    cache.sadd(ACTIVE_BRANCHES_KEY, '77')

    with patch.object(job_queue, 'ack', side_effect=RuntimeError('redis down')):
        dispatched = dispatch_cycle()

    assert dispatched == 0


def test_peek_returns_none_for_nonexistent_branch():
    """peek() returns None for a branch with no queue."""
    result = job_queue.peek(999999)
    assert result is None


def test_job_dispatcher_task_soft_time_limit_caught():
    """SoftTimeLimitExceeded is caught and the dispatcher continues."""
    from billiard.exceptions import SoftTimeLimitExceeded

    from chronos.worker import job_dispatcher_task

    call_count = 0

    def has_active_side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise SoftTimeLimitExceeded()
        raise SystemExit

    with (
        patch.object(job_queue, 'has_active_jobs', side_effect=has_active_side_effect),
        patch('time.sleep'),
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()

    assert call_count == 2


@patch.object(task_send_webhooks, 'apply_async')
def test_job_dispatcher_task_dispatches_and_logs(mock_apply):
    """The dispatcher runs dispatch_cycle and logs when dispatched > 0."""
    from chronos.worker import job_dispatcher_task

    job_queue.enqueue(task_send_webhooks.name, branch_id=42, payload='p')

    call_count = 0

    def sleep_side_effect(seconds):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise SystemExit

    with (
        patch('time.sleep', side_effect=sleep_side_effect),
        patch('chronos.worker.logfire') as mock_logfire,
    ):
        mock_span = MagicMock()
        mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
        with pytest.raises(SystemExit):
            job_dispatcher_task()

    mock_apply.assert_called_once()
    assert mock_span.message == 'Dispatched 1 jobs'


def test_job_dispatcher_task_dispatch_cycle_returns_zero():
    """When dispatch_cycle returns 0, the dispatcher does not set span.message."""
    from chronos.worker import job_dispatcher_task

    call_count = 0

    def sleep_side_effect(seconds):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise SystemExit

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=True),
        patch.object(job_queue, 'get_celery_queue_length', return_value=0),
        patch('chronos.tasks.dispatcher.dispatch_cycle', return_value=0),
        patch('time.sleep', side_effect=sleep_side_effect),
        patch('chronos.worker.logfire') as mock_logfire,
    ):
        mock_span = MagicMock(spec=['message'])
        del mock_span.message
        mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
        with pytest.raises(SystemExit):
            job_dispatcher_task()

    assert not hasattr(mock_span, 'message')


@pytest.fixture
def app_db(engine):
    """Yield a session on the test engine so data is visible to task_send_webhooks.

    Uses the conftest engine (test_pg_dsn) rather than chronos.db.engine to
    guarantee tests never touch the production database.  On CI (testing=true)
    both engines resolve to test_pg_dsn so the task's own session sees the
    same data.
    """
    with Session(engine) as db:
        yield db


@respx.mock
def test_e2e_tc2_multi_event_webhook_splits_and_delivers(session: Session, client: TestClient, app_db: Session):
    """
    A realistic TC2 webhook carrying 3 action events hits the API.

    Expected behaviour after the ingress-splitting change:
      1.  The view passes the raw dict to dispatch_branch_task.
      2.  dispatch_branch_task splits the 3-event payload into 3 single-event
          jobs and enqueues each into the per-branch Redis queue.
      3.  dispatch_cycle picks them up and hands them to Celery.
      4.  task_send_webhooks delivers each single-event payload to every
          active endpoint for the branch, computing a fresh HMAC per request.
      5.  One WebhookLog row is written per (event × endpoint) combination.
    """
    from sqlalchemy import delete as sa_delete

    ep1 = WebhookEndpoint(
        tc_id=501,
        name='alpha-hook',
        branch_id=99,
        webhook_url='https://alpha.example.com/hook',
        api_key='alpha_secret',
        active=True,
    )
    ep2 = WebhookEndpoint(
        tc_id=502,
        name='beta-hook',
        branch_id=99,
        webhook_url='https://beta.example.com/hook',
        api_key='beta_secret',
        active=True,
    )
    app_db.add_all([ep1, ep2])
    app_db.commit()
    ep1_id, ep2_id = ep1.id, ep2.id

    try:
        mock_alpha = respx.post('https://alpha.example.com/hook').mock(return_value=httpx.Response(200))
        mock_beta = respx.post('https://beta.example.com/hook').mock(return_value=httpx.Response(200))

        # -- 1. Simulate TC2 request ---------------------------------------
        tc2_payload = {
            'events': [
                {
                    'branch': 99,
                    'action': 'REMOVED_A_LABEL_FROM_A_SERVICE',
                    'topic': 'SERVICES',
                    'data': {'id': 1001, 'label': 'Premium'},
                },
                {
                    'branch': 99,
                    'action': 'ADDED_A_LABEL_TO_A_SERVICE',
                    'topic': 'SERVICES',
                    'data': {'id': 1002, 'label': 'Standard'},
                },
                {'branch': 99, 'action': 'UPDATED_A_SERVICE', 'topic': 'SERVICES', 'data': {'id': 1003}},
            ],
            'request_time': 1771509452,
        }
        headers = _get_webhook_headers()
        r = client.post(send_webhook_url, data=json.dumps(tc2_payload), headers=headers)
        assert r.status_code == 200

        # -- 2. Verify ingress splitting: 3 events → 3 individual jobs -----
        assert job_queue.get_queue_length(99) == 3

        queued_payloads = []
        for _ in range(3):
            peeked = job_queue.peek(99)
            parsed = json.loads(peeked.kwargs['payload'])
            queued_payloads.append(parsed)
            assert len(parsed['events']) == 1, 'Each queued job must carry exactly one event'
            assert parsed['request_time'] == 1771509452
            assert peeked.kwargs.get('url_extension') is None
            job_queue.ack(99)

        queued_actions = sorted(p['events'][0]['action'] for p in queued_payloads)
        assert queued_actions == sorted(e['action'] for e in tc2_payload['events'])

        # Re-enqueue so dispatch_cycle can process them
        for p in queued_payloads:
            job_queue.enqueue(task_send_webhooks.name, branch_id=99, payload=json.dumps(p), url_extension=None)

        # -- 3. dispatch_cycle → apply_async --------------------------------
        # dispatch_cycle processes one job per branch per cycle; all 3 jobs
        # sit on the same branch, so we need 3 cycles to drain the queue.
        with patch.object(task_send_webhooks, 'apply_async') as mock_apply:
            total_dispatched = 0
            for _ in range(3):
                total_dispatched += dispatch_cycle(batch_limit=10)
        assert total_dispatched == 3
        assert mock_apply.call_count == 3

        # -- 4. Execute each dispatched task --------------------------------
        for call in mock_apply.call_args_list:
            task_kwargs = call.kwargs['kwargs']
            task_send_webhooks(**task_kwargs)

        # -- 5. Verify downstream HTTP delivery -----------------------------
        assert mock_alpha.call_count == 3
        assert mock_beta.call_count == 3

        for mock_ep, api_key in [(mock_alpha, 'alpha_secret'), (mock_beta, 'beta_secret')]:
            delivered_actions = []
            for http_call in mock_ep.calls:
                body = json.loads(http_call.request.content)
                assert len(body['events']) == 1, 'Downstream must receive single-event payloads'
                assert body['request_time'] == 1771509452
                delivered_actions.append(body['events'][0]['action'])

                # Verify HMAC is computed on the individual payload, not the original
                expected_sig = hmac.new(
                    api_key.encode(),
                    http_call.request.content,
                    hashlib.sha256,
                ).hexdigest()
                assert http_call.request.headers['webhook-signature'] == expected_sig

            assert sorted(delivered_actions) == sorted(e['action'] for e in tc2_payload['events'])

        # -- 6. Verify WebhookLog rows -------------------------------------
        app_db.expire_all()
        logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_([ep1_id, ep2_id]))).all()
        assert len(logs) == 6  # 3 events × 2 endpoints

        for ep_id in [ep1_id, ep2_id]:
            ep_logs = [log for log in logs if log.webhook_endpoint_id == ep_id]
            assert len(ep_logs) == 3
            for log in ep_logs:
                assert log.status == 'Success'
                assert log.status_code == 200
                body = json.loads(log.request_body)
                assert len(body['events']) == 1

    finally:
        app_db.exec(sa_delete(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_([ep1_id, ep2_id])))
        app_db.exec(sa_delete(WebhookEndpoint).where(WebhookEndpoint.id.in_([ep1_id, ep2_id])))
        app_db.commit()


@respx.mock
def test_e2e_tc2_public_profile_webhook_no_split_with_url_extension(
    session: Session, client: TestClient, app_db: Session
):
    """
    A TCPublicProfileWebhook (no events key) hits the url_extension endpoint.

    Expected behaviour:
      1.  The view passes the raw dict to dispatch_branch_task.
      2.  dispatch_branch_task sees no 'events' key and enqueues the payload
          as-is (single job, no splitting), preserving url_extension.
      3.  dispatch_cycle hands it to Celery.
      4.  task_send_webhooks delivers the full profile payload to the endpoint
          with the url_extension appended to the URL.
      5.  One WebhookLog row is written containing the complete profile fields.
    """
    from sqlalchemy import delete as sa_delete

    # -- Setup: one active endpoint for branch 99 --------------------------
    ep = WebhookEndpoint(
        tc_id=601,
        name='profile-hook',
        branch_id=99,
        webhook_url='https://profile.example.com/hook',
        api_key='profile_secret',
        active=True,
    )
    app_db.add(ep)
    app_db.commit()
    ep_id = ep.id

    try:
        mock_endpoint = respx.post('https://profile.example.com/hook/test').mock(
            return_value=httpx.Response(200),
        )

        # -- 1. Simulate TC2 public profile request -------------------------
        profile_payload = get_dft_con_webhook_data()
        headers = _get_webhook_headers()
        r = client.post(send_webhook_with_extension_url, data=json.dumps(profile_payload), headers=headers)
        assert r.status_code == 200

        # -- 2. Verify no splitting: exactly 1 job --------------------------
        assert job_queue.get_queue_length(99) == 1

        peeked = job_queue.peek(99)
        assert peeked.task_name == task_send_webhooks.name
        assert peeked.kwargs['url_extension'] == 'test'

        queued_body = json.loads(peeked.kwargs['payload'])
        assert 'events' not in queued_body, 'Profile payloads must not acquire an events key'
        assert queued_body['branch_id'] == profile_payload['branch_id']
        assert queued_body['first_name'] == profile_payload['first_name']
        assert queued_body['request_time'] == profile_payload['request_time']

        # -- 3. dispatch_cycle → apply_async --------------------------------
        with patch.object(task_send_webhooks, 'apply_async') as mock_apply:
            dispatched = dispatch_cycle(batch_limit=10)
        assert dispatched == 1
        assert mock_apply.call_count == 1

        # -- 4. Execute the dispatched task ---------------------------------
        task_kwargs = mock_apply.call_args_list[0].kwargs['kwargs']
        assert task_kwargs['url_extension'] == 'test'
        task_send_webhooks(**task_kwargs)

        # -- 5. Verify downstream HTTP delivery -----------------------------
        assert mock_endpoint.call_count == 1

        http_call = mock_endpoint.calls[0]
        delivered_body = json.loads(http_call.request.content)
        assert 'events' not in delivered_body
        assert delivered_body['branch_id'] == profile_payload['branch_id']
        assert delivered_body['first_name'] == profile_payload['first_name']

        # Verify HMAC
        expected_sig = hmac.new(
            b'profile_secret',
            http_call.request.content,
            hashlib.sha256,
        ).hexdigest()
        assert http_call.request.headers['webhook-signature'] == expected_sig

        # -- 6. Verify WebhookLog -------------------------------------------
        app_db.expire_all()
        logs = app_db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep_id)).all()
        assert len(logs) == 1

        log = logs[0]
        assert log.status == 'Success'
        assert log.status_code == 200
        log_body = json.loads(log.request_body)
        assert 'events' not in log_body
        assert log_body['branch_id'] == profile_payload['branch_id']

    finally:
        app_db.exec(sa_delete(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep_id))
        app_db.exec(sa_delete(WebhookEndpoint).where(WebhookEndpoint.id == ep_id))
        app_db.commit()
