import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import httpx
import pytest
import respx as respx
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, delete, select

from chronos.sql_models import WebhookEndpoint, WebhookLog
from chronos.worker import (
    _async_post_webhooks,
    _delete_old_logs_job,
    delete_old_logs_job,
    get_count,
    task_send_webhooks,
    webhook_request,
)
from tests.test_helpers import (
    _get_webhook_headers,
    create_endpoint_from_dft_data,
    create_webhook_log_from_dft_data,
    get_dft_webhook_data,
    get_failed_response,
    get_successful_response,
)


class TestWorkers:
    @pytest.fixture
    def create_tables(self, engine):
        SQLModel.metadata.create_all(engine)
        yield
        SQLModel.metadata.drop_all(engine)

    @pytest.fixture
    def db(self, engine, create_tables):
        with Session(engine) as session:
            yield session

    @respx.mock
    async def test_send_webhook_one(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()

        # Set up mock response for async request with pattern matching
        pattern = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, _get_webhook_headers()))

        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 1

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        # Test without extension
        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert pattern.call_count == 1

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200
        assert webhook.webhook_endpoint_id == ep.id

    @respx.mock
    async def test_send_many_endpoints(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data(count=10)
        payload = get_dft_webhook_data()

        # Set up mock responses for all endpoints with pattern matching
        patterns = []
        for ep in eps:
            pattern = respx.post(ep.webhook_url).mock(
                return_value=get_successful_response(payload, _get_webhook_headers())
            )
            patterns.append(pattern)
            db.add(ep)
        db.commit()

        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 10

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        # Test without extension
        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10

        for pattern in patterns:
            assert pattern.call_count == 1

        for webhook in webhooks:
            assert webhook.status == 'Success'
            assert webhook.status_code == 200

    @respx.mock
    def test_send_correct_branch(self, db: Session, client: TestClient, celery_session_worker):
        # Create endpoints for different branches
        branch_endpoints = {}
        for branch_id in [99, 199]:
            eps = create_endpoint_from_dft_data(tc_id=branch_id, branch_id=branch_id)
            branch_endpoints[branch_id] = eps
            for ep in eps:
                payload = get_dft_webhook_data(branch_id=branch_id)
                headers = _get_webhook_headers()
                headers['webhook-signature'] = hmac.new(
                    ep.api_key.encode(), json.dumps(payload).encode(), hashlib.sha256
                ).hexdigest()
                respx.post(ep.webhook_url).mock(
                    return_value=get_successful_response(payload, headers, status='success')
                )
                db.add(ep)
        db.commit()

        # Send webhooks for each branch
        for branch_id in [99, 199]:
            payload = get_dft_webhook_data(branch_id=branch_id)
            sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
            sending_webhooks.get(timeout=10)

        # Verify webhooks were sent correctly
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == len(branch_endpoints[99]) + len(branch_endpoints[199])

        for webhook in webhooks:
            assert webhook.status == 'Success'
            assert webhook.status_code == 200

    @respx.mock
    async def test_send_webhook_fail_to_send_only_one(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        for ep in eps:
            mock_request = respx.post(ep.webhook_url).mock(return_value=get_failed_response(payload, headers))
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert mock_request.called

        webhook = webhooks[0]
        assert webhook.status == 'Unexpected response'
        assert webhook.status_code == 409

    @respx.mock
    def test_webhook_not_send_if_active(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data(active=False)
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        for ep in eps:
            mock_request = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert not mock_request.called
        assert not len(webhooks)

    @respx.mock
    def test_webhook_not_send_if_url_incorrect(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data(webhook_url='htp://example.com')
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        for ep in eps:
            mock_request = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert not mock_request.called
        assert not len(webhooks)

    @patch('chronos.worker.app_logger')
    def test_webhook_not_send_if_not_url(self, mock_logger, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data(webhook_url='')
        payload = get_dft_webhook_data()
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0
        assert not mock_logger.error.called

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert mock_logger.error.called
        assert 'Webhook URL does not start with an acceptable url scheme:' in mock_logger.error.call_args[0][0]
        assert not len(webhooks)

    @patch('chronos.worker.app_logger')
    @respx.mock
    def test_webhook_not_send_errors(self, mock_logger, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        for ep in eps:
            pattern = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(
                    status_code=500,
                    json={'status': 'error', 'message': 'Internal Server Error'},
                    request=httpx.Request('POST', 'https://example.com', json=payload, headers=headers),
                    headers=headers,
                )
            )
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert pattern.call_count == 1

        webhook = webhooks[0]
        assert webhook.status == 'Unexpected response'
        assert webhook.status_code == 500

    def test_delete_old_logs(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        for ep in eps:
            db.add(ep)
        db.commit()

        for i in range(0, 30):
            whl = create_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id,
                timestamp=datetime.utcnow() - timedelta(days=i),
            )
            db.add(whl)
        db.commit()

        logs = db.exec(select(WebhookLog)).all()
        assert len(logs) == 30

        _delete_old_logs_job()
        logs = db.exec(select(WebhookLog)).all()
        # The log from 15 days ago is seconds older than the check and thus sdoesn't get deleted
        assert len(logs) == 15

    # Used for testing memory usage. Unnecessary for CI testing
    # @profile and install memory-profiler to use
    # def test_delete_old_logs_many(self, db: Session, client: TestClient, celery_session_worker):
    #     eps = create_endpoint_from_dft_data()
    #     for ep in eps:
    #         db.add(ep)
    #     db.commit()
    #
    #     for i in range(0, 30):
    #         whl = create_webhook_log_from_dft_data(
    #             webhook_endpoint_id=ep.id,
    #             timestamp=datetime.utcnow() - timedelta(days=i),
    #         )
    #         db.add(whl)
    #     db.commit()
    #
    #     for y in range(1000):
    #         for i in range(1000):
    #             whl = create_webhook_log_from_dft_data(
    #                 webhook_endpoint_id=ep.id,
    #                 timestamp=datetime.utcnow() - timedelta(days=20),
    #             )
    #             db.add(whl)
    #         db.commit()
    #
    #     logs = db.exec(select(WebhookLog)).all()
    #     assert len(logs) == 1000030
    #
    #     _delete_old_logs_job()
    #     logs = db.exec(select(WebhookLog)).all()
    #     # The log from 15 days ago is seconds older than the check and thus sdoesn't get deleted
    #     assert len(logs) == 15

    @patch('chronos.worker.app_logger')
    def test_send_webhook_malformed_json(self, mock_logger, db: Session, client: TestClient, celery_session_worker):
        """Test handling of malformed JSON payloads in webhook sending."""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Test with invalid JSON string
        invalid_json = "{'invalid': 'json'}"  # Using single quotes which is invalid JSON
        with pytest.raises(Exception) as exc_info:
            sending_webhooks = task_send_webhooks.delay(invalid_json)
            sending_webhooks.get(timeout=10)

        assert 'JSONDecodeError' in str(exc_info.value)

    @patch('chronos.worker.app_logger')
    def test_send_webhook_empty_payload(self, mock_logger, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = {'request_time': 1234567890, 'events': [{'branch': 99, 'event': 'test_event', 'data': {}}]}

        pattern = respx.post(ep.webhook_url).mock(
            return_value=httpx.Response(999, json={'status': 'error'}, request=httpx.Request('POST', ep.webhook_url))
        )

        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert webhooks[0].status_code == 999
        assert pattern.call_count == 0

    @respx.mock
    def test_send_webhook_large_payload(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        payload['events'][0]['data'] = {'large_data': 'x' * 1000000}
        headers = _get_webhook_headers()
        headers['webhook-signature'] = hmac.new(
            ep.api_key.encode(), json.dumps(payload).encode(), hashlib.sha256
        ).hexdigest()

        # Set up mock response for async request with pattern matching
        pattern = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))

        # Test without extension
        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert pattern.call_count == 1

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200
        assert webhook.webhook_endpoint_id == ep.id

    @respx.mock
    def test_concurrent_webhook_requests(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()

        pattern = respx.post(ep.webhook_url).mock(
            return_value=get_successful_response(payload, headers, status='success')
        )

        # Send multiple concurrent requests
        tasks = []
        for _ in range(5):
            task = task_send_webhooks.delay(json.dumps(payload))
            tasks.append(task)

        # Wait for all tasks to complete
        for task in tasks:
            task.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 5
        assert pattern.call_count == 5  # Each task should make its own request

    @respx.mock
    def test_queue_length_warning(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()

        pattern = respx.post(ep.webhook_url).mock(
            return_value=get_successful_response(payload, headers, status='success')
        )

        # Mock the queue length to be high
        with patch('chronos.worker.get_qlength', return_value=150):
            sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
            sending_webhooks.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert webhooks[0].status_code == 200
        assert pattern.call_count == 1

    @patch('chronos.worker.cache')
    async def test_delete_old_logs_job_scheduler(self, mock_cache):
        """Test the delete_old_logs_job scheduler function."""
        # Test when job is already running
        mock_cache.get.return_value = 'True'
        await delete_old_logs_job()
        mock_cache.get.assert_called_once_with('delete_old_logs_job')
        mock_cache.set.assert_not_called()

        # Reset mock
        mock_cache.reset_mock()

        # Test when job is not running
        mock_cache.get.return_value = None
        await delete_old_logs_job()
        mock_cache.get.assert_called_once_with('delete_old_logs_job')
        mock_cache.set.assert_called_once_with('delete_old_logs_job', 'True', ex=1200)

    def test_get_count(self, db: Session):
        """Test the get_count function for log deletion."""
        # Create some test logs with different timestamps
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        now = datetime.now(UTC)

        # Create 5 old logs (older than 5 days)
        for i in range(5):
            log = WebhookLog(
                webhook_endpoint_id=ep.id,
                timestamp=now - timedelta(days=6),
                request_headers='{}',
                request_body='{}',
                response_headers='{}',
                response_body='{}',
                status='Success',
                status_code=200,
            )
            db.add(log)

        # Create 5 recent logs (within 5 days)
        for i in range(5):
            log = WebhookLog(
                webhook_endpoint_id=ep.id,
                timestamp=now - timedelta(days=2),
                request_headers='{}',
                request_body='{}',
                response_headers='{}',
                response_body='{}',
                status='Success',
                status_code=200,
            )
            db.add(log)

        db.commit()

        # Check count of logs older than 5 days
        assert get_count(now - timedelta(days=5)) == 5

    @patch('chronos.worker.gc.collect')
    def test_delete_old_logs_batch_processing(self, mock_gc, db: Session):
        """Test batch processing in delete_old_logs_job."""
        # Create more logs than the delete limit
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Create 6000 logs (more than the 4999 delete limit)
        now = datetime.now(UTC)
        for i in range(6000):
            log = create_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id,
                timestamp=now - timedelta(days=16),  # All logs are older than 15 days
            )
            db.add(log)
        db.commit()

        # Run the deletion job
        _delete_old_logs_job()

        # Verify all logs were deleted
        remaining_logs = db.exec(select(WebhookLog)).all()
        assert len(remaining_logs) == 0

        # Verify garbage collection was called
        assert mock_gc.call_count > 0

    @respx.mock
    def test_async_post_webhooks_invalid_url(self, db: Session):
        """Test handling of webhooks with invalid URLs."""
        # Create endpoint with invalid URL
        ep = create_endpoint_from_dft_data(webhook_url='invalid://url')[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        webhook_logs, total_success, total_failed = asyncio.run(_async_post_webhooks([ep], None, json.dumps(payload)))

        assert len(webhook_logs) == 0
        assert total_success == 0
        assert total_failed == 0

    @respx.mock
    def test_async_post_webhooks_connection_error(self, db: Session):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()

        # Set up mock response for async request with pattern matching
        pattern = respx.post(ep.webhook_url).mock(side_effect=httpx.TimeoutException('Timeout'))

        webhook_logs, total_success, total_failed = asyncio.run(_async_post_webhooks([ep], None, json.dumps(payload)))

        assert len(webhook_logs) == 1
        assert pattern.call_count == 1
        assert total_success == 0
        assert total_failed == 1

        webhook = webhook_logs[0]
        assert webhook.status == 'Unexpected response'
        assert webhook.status_code == 999

    @respx.mock
    async def test_async_post_webhooks_mixed_responses(self, db: Session):
        # Create two endpoints
        ep1 = create_endpoint_from_dft_data(tc_id=1)[0]
        ep2 = create_endpoint_from_dft_data(tc_id=2)[0]
        db.add(ep1)
        db.add(ep2)
        db.commit()

        payload = get_dft_webhook_data()

        # Set up mock responses - one success, one failure
        pattern1 = respx.post(ep1.webhook_url).mock(
            return_value=get_successful_response(payload, _get_webhook_headers())
        )
        pattern2 = respx.post(ep2.webhook_url).mock(return_value=get_failed_response(payload, _get_webhook_headers()))

        webhook_logs, total_success, total_failed = await _async_post_webhooks([ep1, ep2], None, json.dumps(payload))

        assert len(webhook_logs) == 2
        assert pattern1.call_count == 1
        assert pattern2.call_count == 1
        assert total_success == 1
        assert total_failed == 1

        # Check the successful webhook
        success_webhook = next(log for log in webhook_logs if log.status == 'Success')
        assert success_webhook.status_code == 200

        # Check the failed webhook
        failed_webhook = next(log for log in webhook_logs if log.status == 'Unexpected response')
        assert failed_webhook.status_code == 409

    @respx.mock
    def test_webhook_request_direct(self, db: Session):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        async def test_timeout():
            pattern = respx.post(ep.webhook_url).mock(side_effect=httpx.TimeoutException('Timeout'))
            client = httpx.AsyncClient(timeout=0.1)
            try:
                with pytest.raises(httpx.TimeoutException):
                    await webhook_request(
                        client, ep.webhook_url, ep.id, webhook_sig='test', data=get_dft_webhook_data()
                    )
            finally:
                await client.aclose()
            assert pattern.call_count == 1

        asyncio.run(test_timeout())

    @respx.mock
    async def test_webhook_url_validation(self, db: Session, client: TestClient, celery_session_worker):
        # Create endpoints with different URL schemes
        valid_ep = create_endpoint_from_dft_data(tc_id=1, webhook_url='https://test_endpoint_1.com')[0]
        invalid_ep = create_endpoint_from_dft_data(tc_id=2, webhook_url='invalid://test_endpoint_5.com')[0]
        db.add(valid_ep)
        db.add(invalid_ep)
        db.commit()

        payload = get_dft_webhook_data()

        # Set up mock response for valid endpoint
        pattern = respx.post(valid_ep.webhook_url).mock(
            return_value=get_successful_response(payload, _get_webhook_headers())
        )

        # Test sending webhooks
        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert pattern.call_count == 1

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200
        assert webhook.webhook_endpoint_id == valid_ep.id

    @respx.mock
    def test_connection_pool_limits(self, db: Session):
        # Clean up any existing endpoints
        db.exec(delete(WebhookEndpoint))
        db.commit()

        # Create more endpoints than the connection limit
        num_endpoints = 300  # More than the 250 connection limit
        endpoints = []
        patterns = []
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()

        for i in range(num_endpoints):
            ep = create_endpoint_from_dft_data(tc_id=i + 1)[0]
            ep.webhook_url = f'https://test_endpoint_{i + 1}.com'
            endpoints.append(ep)
            db.add(ep)

            # Set up mock response
            pattern = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))
            patterns.append(pattern)
        db.commit()

        # Send webhooks
        webhook_logs, total_success, total_failed = asyncio.run(
            _async_post_webhooks(endpoints, None, json.dumps(payload))
        )

        # Verify all webhooks were processed
        assert len(webhook_logs) == num_endpoints
        assert total_success == num_endpoints
        assert total_failed == 0
        assert all(pattern.call_count == 1 for pattern in patterns)
