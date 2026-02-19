"""Tests for the round-robin dispatch infrastructure."""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from chronos.tasks.dispatcher import dispatch_cycle
from chronos.tasks.queue import ACTIVE_BRANCHES_KEY, BRANCH_KEY_TEMPLATE, JobQueue
from chronos.views import _extract_branch_id
from chronos.worker import cache, dispatch_branch_task, job_queue, task_send_webhooks
from tests.test_helpers import _get_webhook_headers, get_dft_webhook_data, send_webhook_url


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
    dispatch_branch_task(task_send_webhooks, branch_id=42, payload='test_payload', url_extension='ext')

    assert job_queue.has_active_jobs()
    peeked = job_queue.peek(42)
    assert peeked is not None
    assert peeked.task_name == task_send_webhooks.name
    assert peeked.kwargs == {'payload': 'test_payload', 'url_extension': 'ext'}
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

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=True),
        patch.object(job_queue, 'get_celery_queue_length', return_value=100),
        patch('time.sleep', side_effect=SystemExit) as mock_sleep,
        patch('chronos.tasks.dispatcher.dispatch_cycle') as mock_dispatch,
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()
        mock_dispatch.assert_not_called()
        mock_sleep.assert_called_with(0.01)

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=False),
        patch('time.sleep', side_effect=SystemExit) as mock_sleep,
        patch('chronos.tasks.dispatcher.dispatch_cycle') as mock_dispatch,
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()
        mock_dispatch.assert_not_called()
        mock_sleep.assert_called_with(1.0)


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
