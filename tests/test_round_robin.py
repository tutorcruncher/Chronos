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


def test_extract_branch_id_from_events_payload():
    payload = {'events': [{'branch': 123, 'action': 'test'}]}
    assert _extract_branch_id(payload) == 123
    payload = {'branch_id': 456, 'id': 1}
    assert _extract_branch_id(payload) == 456


def test_dispatch_branch_task_enqueues_with_task_name_and_kwargs():
    dispatch_branch_task(task_send_webhooks, branch_id=42, payload='test_payload', url_extension='ext')

    assert job_queue.has_active_jobs()
    peeked = job_queue.peek(42)
    assert peeked is not None
    assert peeked.task_name == task_send_webhooks.name
    assert peeked.kwargs == {'payload': 'test_payload', 'url_extension': 'ext'}
    assert peeked.branch_id == 42


def test_queue_enqueue_writes_list_and_active_set():
    job_queue.enqueue('test_task', branch_id=77, payload='data')

    assert 77 in job_queue.get_active_branches()
    assert job_queue.get_queue_length(77) == 1

    peeked = job_queue.peek(77)
    assert peeked.task_name == 'test_task'
    assert peeked.kwargs == {'payload': 'data'}


def test_queue_ack_single_item_removes_active_branch():
    job_queue.enqueue('test_task', branch_id=88, payload='data')
    assert 88 in job_queue.get_active_branches()
    job_queue.ack(88)
    assert 88 not in job_queue.get_active_branches()
    assert job_queue.get_queue_length(88) == 0


def test_queue_ack_multiple_items_keeps_branch_active():
    job_queue.enqueue('test_task', branch_id=88, payload='first')
    job_queue.enqueue('test_task', branch_id=88, payload='second')
    assert job_queue.get_queue_length(88) == 2

    job_queue.ack(88)
    assert 88 in job_queue.get_active_branches()
    assert job_queue.get_queue_length(88) == 1

    peeked = job_queue.peek(88)
    assert peeked.kwargs == {'payload': 'second'}


def test_queue_ack_noscript_error_reloads_lua_and_retries():
    job_queue.enqueue('test_task', branch_id=55, payload='data')

    JobQueue._ack_script_sha = 'deadbeef_invalid_sha'

    job_queue.ack(55)

    assert job_queue.get_queue_length(55) == 0
    assert 55 not in job_queue.get_active_branches()
    assert JobQueue._ack_script_sha != 'deadbeef_invalid_sha'


def test_dispatch_cycle_no_active_branches_returns_zero():
    assert dispatch_cycle() == 0


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


def test_dispatch_cycle_unknown_task_is_acked_and_skipped():
    """Jobs with unknown task names are acked and skipped."""
    job_queue.enqueue('nonexistent_task_xyz', branch_id=42, payload='data')

    dispatched = dispatch_cycle()
    assert dispatched == 0
    assert not job_queue.has_active_jobs()


def test_dispatch_cycle_poison_payload_is_acked():
    """Invalid JSON payloads are acked to prevent infinite retries."""
    cache.rpush(BRANCH_KEY_TEMPLATE.format(42), 'not valid json{{{')
    cache.sadd(ACTIVE_BRANCHES_KEY, '42')

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


# --- Tests 17-19: job_dispatcher_task loop behaviour ---


def test_job_dispatcher_task_backpressure_skips_dispatch_cycle():
    """When the Celery queue is full, the dispatcher skips dispatch_cycle."""
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


def test_job_dispatcher_task_idle_path():
    """When no active jobs, the dispatcher sleeps at idle_delay."""
    from chronos.worker import job_dispatcher_task

    sleep_calls = []

    def sleep_side_effect(seconds):
        sleep_calls.append(seconds)
        raise SystemExit

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=False),
        patch('time.sleep', side_effect=sleep_side_effect),
        patch('chronos.tasks.dispatcher.dispatch_cycle') as mock_dispatch,
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()
        assert sleep_calls == [1.0]
        mock_dispatch.assert_not_called()


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

    def sleep_side_effect(seconds):
        sleep_calls.append(seconds)

    with (
        patch.object(job_queue, 'has_active_jobs', return_value=True),
        patch.object(job_queue, 'get_celery_queue_length', return_value=0),
        patch('chronos.tasks.dispatcher.dispatch_cycle', side_effect=dispatch_side_effect),
        patch('time.sleep', side_effect=sleep_side_effect),
    ):
        with pytest.raises(SystemExit):
            job_dispatcher_task()

    assert call_count == 2
    assert 1.0 in sleep_calls


# --- Test 20: Worker startup signal ---


def test_worker_ready_starts_dispatcher_only_on_dispatcher_queue():
    """start_dispatcher_on_worker_ready only starts dispatcher on dispatcher queue workers."""
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
