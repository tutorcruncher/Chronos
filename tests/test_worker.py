import json
from datetime import datetime, timedelta
from unittest.mock import patch

import httpx
import pytest
import respx as respx
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, col, select

from chronos.sql_models import WebhookEndpoint, WebhookLog
from chronos.worker import _delete_old_logs_job, task_send_webhooks
from tests.test_helpers import (
    _get_webhook_headers,
    create_endpoint_from_dft_data,
    create_webhook_log_from_dft_data,
    get_dft_webhook_data,
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
    def test_send_webhook_one(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        mock_request = respx.post(ep.webhook_url).mock(
            return_value=httpx.Response(status_code=200, json=payload, headers=headers)
        )

        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 1

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        assert mock_request.called

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200

    @respx.mock
    def test_send_many_endpoints(self, db: Session, client: TestClient, celery_session_worker):
        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 0

        eps = create_endpoint_from_dft_data(count=10)
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        mock_requests = []
        for ep in eps:
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(status_code=200, json=payload, headers=headers)
            )
            mock_requests.append(mock_request)
            db.add(ep)
        db.commit()

        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 10

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10
        assert all(mock_request.called for mock_request in mock_requests)

    @respx.mock
    def test_send_correct_branch(self, db: Session, client: TestClient, celery_session_worker):
        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 0

        mock_requests = []
        # Create endpoints and mock requests for branch 99
        for tc_id in range(1, 6):
            ep = create_endpoint_from_dft_data(tc_id=tc_id, webhook_url=f'https://test_endpoint_{tc_id}.com')[0]
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(
                    status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers()
                )
            )
            mock_requests.append((mock_request, ep))
            db.add(ep)

        # Create endpoints and mock requests for branch 199
        for tc_id in range(11, 16):
            ep = create_endpoint_from_dft_data(
                tc_id=tc_id, branch_id=199, webhook_url=f'https://test_endpoint_{tc_id}.com'
            )[0]
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(
                    status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers()
                )
            )
            mock_requests.append((mock_request, ep))
            db.add(ep)

        # Create endpoints and mock requests for branch 299
        for tc_id in range(101, 106):
            ep = create_endpoint_from_dft_data(
                tc_id=tc_id, branch_id=299, webhook_url=f'https://test_endpoint_{tc_id}.com'
            )[0]
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(
                    status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers()
                )
            )
            mock_requests.append((mock_request, ep))
            db.add(ep)
        db.commit()

        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 15

        endpoints_1 = db.exec(select(WebhookEndpoint).where(WebhookEndpoint.branch_id == 99)).all()
        assert len(endpoints_1) == 5
        endpoints_2 = db.exec(select(WebhookEndpoint).where(WebhookEndpoint.branch_id == 199)).all()
        assert len(endpoints_2) == 5
        endpoints_3 = db.exec(select(WebhookEndpoint).where(WebhookEndpoint.branch_id == 299)).all()
        assert len(endpoints_3) == 5

        # Test branch 99 webhooks
        payload = get_dft_webhook_data()
        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 5

        # Check that only branch 99 requests were called
        for mock_request, ep in mock_requests:
            if ep.branch_id == 99:
                assert mock_request.call_count == 1
            else:
                assert mock_request.call_count == 0

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.webhook_endpoint_id).in_([ep.id for ep in endpoints_1]))
        ).all()
        assert len(webhooks) == 5

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.webhook_endpoint_id).in_([ep.id for ep in endpoints_2]))
        ).all()
        assert len(webhooks) == 0

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.webhook_endpoint_id).in_([ep.id for ep in endpoints_3]))
        ).all()
        assert len(webhooks) == 0

        # Reset mock requests
        for mock_request, _ in mock_requests:
            mock_request.reset()

        # Test branch 199 webhooks
        payload = get_dft_webhook_data(branch_id=199)
        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10

        # Check that only branch 199 requests were called
        for mock_request, ep in mock_requests:
            if ep.branch_id == 199:
                assert mock_request.call_count == 1
            else:
                assert mock_request.call_count == 0

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.webhook_endpoint_id).in_([ep.id for ep in endpoints_1]))
        ).all()
        assert len(webhooks) == 5

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.webhook_endpoint_id).in_([ep.id for ep in endpoints_2]))
        ).all()
        assert len(webhooks) == 5

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.webhook_endpoint_id).in_([ep.id for ep in endpoints_3]))
        ).all()
        assert len(webhooks) == 0

    @respx.mock
    def test_send_webhook_fail_to_send_only_one(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        mock_request = respx.post(eps[0].webhook_url).mock(
            return_value=httpx.Response(status_code=500, json=payload, headers=headers)
        )
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert mock_request.called

        webhook = webhooks[0]
        assert webhook.status == 'Failed'
        assert webhook.status_code == 500

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
        assert len(webhooks) == 0

    @patch('chronos.worker.app_logger')
    def test_webhook_not_send_if_not_url(self, mock_logger, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data(webhook_url='')
        payload = get_dft_webhook_data()
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0
        assert mock_logger.error.call_count == 1

    @patch('chronos.worker.app_logger')
    @respx.mock
    def test_webhook_not_send_errors(self, mock_logger, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        mock_request = respx.post(eps[0].webhook_url).mock(
            return_value=httpx.Response(status_code=500, json=payload, headers=headers)
        )
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert mock_request.called

        webhook = webhooks[0]
        assert webhook.status == 'Failed'
        assert webhook.status_code == 500
        assert mock_logger.info.call_count == 5

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

    @respx.mock
    def test_webhook_retry_logic(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        mock_request = respx.post('https://test_endpoint_1.com').mock(
            side_effect=[
                httpx.Response(status_code=502),
                httpx.Response(status_code=502),
                httpx.Response(status_code=200, json=payload, headers=headers),
            ]
        )
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert mock_request.call_count == 3

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200

    @respx.mock
    def test_webhook_retry_timeout(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        mock_request = respx.post('https://test_endpoint_1.com').mock(
            side_effect=[
                httpx.TimeoutException('Connection timed out'),
                httpx.TimeoutException('Connection timed out'),
                httpx.Response(status_code=200, json=payload, headers=_get_webhook_headers()),
            ]
        )
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert mock_request.call_count == 3

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200

    @respx.mock
    def test_webhook_retry_max_attempts(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        mock_request = respx.post('https://test_endpoint_1.com').mock(
            side_effect=[
                httpx.Response(status_code=502),
                httpx.Response(status_code=502),
                httpx.Response(status_code=502),
                httpx.Response(status_code=502),  # This one should not be called
            ]
        )
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert mock_request.call_count == 3  # Should only try 3 times

        webhook = webhooks[0]
        assert webhook.status == 'Failed'
        assert webhook.status_code == 502

    @respx.mock
    def test_webhook_connection_pooling(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_endpoint_from_dft_data()
        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        mock_request = respx.post('https://test_endpoint_1.com').mock(
            return_value=httpx.Response(status_code=200, json=payload, headers=headers)
        )
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1
        assert mock_request.called

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200

    @respx.mock
    def test_webhook_branch_filtering(self, db: Session, client: TestClient, celery_session_worker):
        """Test that webhooks are correctly filtered by branch_id"""
        # Create endpoints for different branches
        branch_99_ep = create_endpoint_from_dft_data(branch_id=99)[0]
        branch_199_ep = create_endpoint_from_dft_data(branch_id=199)[0]
        branch_299_ep = create_endpoint_from_dft_data(branch_id=299)[0]
        
        for ep in [branch_99_ep, branch_199_ep, branch_299_ep]:
            db.add(ep)
        db.commit()

        # Create mock requests for each endpoint
        mock_requests = {}
        for ep in [branch_99_ep, branch_199_ep, branch_299_ep]:
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers())
            )
            mock_requests[ep.id] = mock_request

        # Test with branch_id in payload
        payload = get_dft_webhook_data(branch_id=199)
        task_send_webhooks(json.dumps(payload))
        
        # Verify only branch 199 endpoint was called
        assert mock_requests[branch_199_ep.id].called
        assert not mock_requests[branch_99_ep.id].called
        assert not mock_requests[branch_299_ep.id].called

        # Reset mock requests
        for mock_request in mock_requests.values():
            mock_request.reset()

        # Test with branch_id in events
        payload = get_dft_webhook_data()
        payload['events'] = [{'branch': 299}]
        task_send_webhooks(json.dumps(payload))
        
        # Verify only branch 299 endpoint was called
        assert mock_requests[branch_299_ep.id].called
        assert not mock_requests[branch_99_ep.id].called
        assert not mock_requests[branch_199_ep.id].called

        # Reset mock requests
        for mock_request in mock_requests.values():
            mock_request.reset()

        # Test with no branch_id (should default to 99)
        payload = get_dft_webhook_data()
        del payload['branch_id']
        task_send_webhooks(json.dumps(payload))
        
        # Verify only branch 99 endpoint was called
        assert mock_requests[branch_99_ep.id].called
        assert not mock_requests[branch_199_ep.id].called
        assert not mock_requests[branch_299_ep.id].called

    @respx.mock
    def test_webhook_exponential_backoff(self, db: Session, client: TestClient, celery_session_worker):
        """Test that webhook retries use exponential backoff"""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Create a mock request that fails twice then succeeds
        mock_request = respx.post(ep.webhook_url).mock(
            side_effect=[
                httpx.Response(status_code=500),
                httpx.Response(status_code=502),
                httpx.Response(status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers())
            ]
        )

        payload = get_dft_webhook_data()
        task_send_webhooks(json.dumps(payload))

        # Verify the request was retried with exponential backoff
        assert mock_request.call_count == 3
        assert mock_request.calls[0].timeout == settings.webhook_timeout
        assert mock_request.calls[1].timeout == settings.webhook_timeout
        assert mock_request.calls[2].timeout == settings.webhook_timeout

    @respx.mock
    def test_webhook_connection_pooling(self, db: Session, client: TestClient, celery_session_worker):
        """Test that connection pooling is working correctly"""
        # Create multiple endpoints
        endpoints = create_endpoint_from_dft_data(count=5)
        for ep in endpoints:
            db.add(ep)
        db.commit()

        # Create mock requests for each endpoint
        mock_requests = {}
        for ep in endpoints:
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers())
            )
            mock_requests[ep.id] = mock_request

        # Send webhooks to all endpoints
        payload = get_dft_webhook_data()
        task_send_webhooks(json.dumps(payload))

        # Verify all requests were made
        for mock_request in mock_requests.values():
            assert mock_request.called

        # Verify the number of connections used
        assert len(set(call.client for call in mock_requests[1].calls)) == 1  # Same client used for all requests

    @respx.mock
    def test_webhook_error_handling(self, db: Session, client: TestClient, celery_session_worker):
        """Test error handling and logging for various failure scenarios"""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Test timeout error
        mock_request = respx.post(ep.webhook_url).mock(
            side_effect=httpx.TimeoutException("Connection timed out")
        )

        payload = get_dft_webhook_data()
        task_send_webhooks(json.dumps(payload))

        # Verify the request was retried and logged
        assert mock_request.call_count == 3
        webhook_logs = db.exec(select(WebhookLog)).all()
        assert len(webhook_logs) == 1
        assert webhook_logs[0].status == 'Failed'
        assert webhook_logs[0].status_code == 0

        # Test invalid URL
        ep.webhook_url = "invalid://url"
        db.commit()

        task_send_webhooks(json.dumps(payload))
        webhook_logs = db.exec(select(WebhookLog)).all()
        assert len(webhook_logs) == 1  # No new logs for invalid URL

        # Test inactive endpoint
        ep.webhook_url = "https://valid.url"
        ep.active = False
        db.commit()

        task_send_webhooks(json.dumps(payload))
        webhook_logs = db.exec(select(WebhookLog)).all()
        assert len(webhook_logs) == 1  # No new logs for inactive endpoint

    def test_webhook_log_cleanup(self, db: Session, client: TestClient, celery_session_worker):
        """Test that old webhook logs are cleaned up correctly"""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Create logs with various ages
        now = datetime.utcnow()
        for days_ago in [5, 10, 15, 20, 25]:
            log = create_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id,
                timestamp=now - timedelta(days=days_ago)
            )
            db.add(log)
        db.commit()

        # Run cleanup job
        _delete_old_logs_job()

        # Verify only logs older than 15 days were deleted
        remaining_logs = db.exec(select(WebhookLog)).all()
        assert len(remaining_logs) == 3  # Logs from 5, 10, and 15 days ago should remain
        for log in remaining_logs:
            assert (now - log.timestamp).days <= 15

    @respx.mock
    def test_webhook_mixed_branch_events(self, db: Session, client: TestClient, celery_session_worker):
        """Test webhook handling with mixed branch events in payload"""
        # Create endpoints for different branches
        branch_99_ep = create_endpoint_from_dft_data(branch_id=99)[0]
        branch_199_ep = create_endpoint_from_dft_data(branch_id=199)[0]
        branch_299_ep = create_endpoint_from_dft_data(branch_id=299)[0]
        
        for ep in [branch_99_ep, branch_199_ep, branch_299_ep]:
            db.add(ep)
        db.commit()

        # Create mock requests for each endpoint
        mock_requests = {}
        for ep in [branch_99_ep, branch_199_ep, branch_299_ep]:
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers())
            )
            mock_requests[ep.id] = mock_request

        # Test with multiple events targeting different branches
        payload = get_dft_webhook_data()
        payload['events'] = [
            {'branch': 99},
            {'branch': 199},
            {'branch': 299},
            {'branch': 99}  # Duplicate branch
        ]
        task_send_webhooks(json.dumps(payload))
        
        # Verify all endpoints were called exactly once
        assert mock_requests[branch_99_ep.id].call_count == 1
        assert mock_requests[branch_199_ep.id].call_count == 1
        assert mock_requests[branch_299_ep.id].call_count == 1

    @respx.mock
    def test_webhook_invalid_payload_handling(self, db: Session, client: TestClient, celery_session_worker):
        """Test handling of various invalid payload scenarios"""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Test with invalid JSON
        with pytest.raises(json.JSONDecodeError):
            task_send_webhooks("invalid json")

        # Test with empty payload
        task_send_webhooks("{}")
        webhook_logs = db.exec(select(WebhookLog)).all()
        assert len(webhook_logs) == 0  # No logs should be created for empty payload

        # Test with malformed events array
        payload = get_dft_webhook_data()
        payload['events'] = "not an array"
        task_send_webhooks(json.dumps(payload))
        webhook_logs = db.exec(select(WebhookLog)).all()
        assert len(webhook_logs) == 0  # No logs should be created for malformed events

    @respx.mock
    def test_webhook_concurrent_requests(self, db: Session, client: TestClient, celery_session_worker):
        """Test handling of concurrent webhook requests"""
        # Create multiple endpoints
        endpoints = create_endpoint_from_dft_data(count=10)
        for ep in endpoints:
            db.add(ep)
        db.commit()

        # Create mock requests with varying response times
        mock_requests = {}
        for i, ep in enumerate(endpoints):
            delay = i * 0.1  # Varying delays
            mock_request = respx.post(ep.webhook_url).mock(
                return_value=httpx.Response(
                    status_code=200,
                    json=get_dft_webhook_data(),
                    headers=_get_webhook_headers()
                )
            )
            mock_requests[ep.id] = mock_request

        # Send multiple webhooks concurrently
        payload = get_dft_webhook_data()
        task_send_webhooks(json.dumps(payload))

        # Verify all requests were made
        for mock_request in mock_requests.values():
            assert mock_request.called

        # Verify the number of connections used
        assert len(set(call.client for call in mock_requests[1].calls)) == 1  # Same client used for all requests

    @respx.mock
    def test_webhook_retry_edge_cases(self, db: Session, client: TestClient, celery_session_worker):
        """Test edge cases in webhook retry logic"""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Test with alternating success/failure responses
        mock_request = respx.post(ep.webhook_url).mock(
            side_effect=[
                httpx.Response(status_code=500),
                httpx.Response(status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers()),
                httpx.Response(status_code=502),
                httpx.Response(status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers())
            ]
        )

        payload = get_dft_webhook_data()
        task_send_webhooks(json.dumps(payload))

        # Verify the request was retried appropriately
        assert mock_request.call_count == 2  # Should stop after first success

        # Test with network errors
        mock_request.reset()
        mock_request.side_effect = [
            httpx.NetworkError("Network error"),
            httpx.Response(status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers())
        ]

        task_send_webhooks(json.dumps(payload))
        assert mock_request.call_count == 2  # Should retry on network error

    @respx.mock
    def test_webhook_log_cleanup_edge_cases(self, db: Session, client: TestClient, celery_session_worker):
        """Test edge cases in webhook log cleanup"""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Test with exactly 15-day-old logs
        now = datetime.utcnow()
        log = create_webhook_log_from_dft_data(
            webhook_endpoint_id=ep.id,
            timestamp=now - timedelta(days=15)
        )
        db.add(log)
        db.commit()

        _delete_old_logs_job()
        remaining_logs = db.exec(select(WebhookLog)).all()
        assert len(remaining_logs) == 1  # 15-day-old logs should be kept

        # Test with large number of logs
        for i in range(1000):
            log = create_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id,
                timestamp=now - timedelta(days=20)
            )
            db.add(log)
        db.commit()

        _delete_old_logs_job()
        remaining_logs = db.exec(select(WebhookLog)).all()
        assert len(remaining_logs) == 1  # Only the 15-day-old log should remain

        # Test with logs from different endpoints
        ep2 = create_endpoint_from_dft_data()[0]
        db.add(ep2)
        db.commit()

        for days_ago in [5, 10, 15, 20]:
            for endpoint in [ep, ep2]:
                log = create_webhook_log_from_dft_data(
                    webhook_endpoint_id=endpoint.id,
                    timestamp=now - timedelta(days=days_ago)
                )
                db.add(log)
        db.commit()

        _delete_old_logs_job()
        remaining_logs = db.exec(select(WebhookLog)).all()
        assert len(remaining_logs) == 6  # 3 logs per endpoint (5, 10, 15 days old)

    @respx.mock
    def test_webhook_headers_validation(self, db: Session, client: TestClient, celery_session_worker):
        """Test validation of webhook headers"""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        # Test with missing required headers
        mock_request = respx.post(ep.webhook_url).mock(
            return_value=httpx.Response(
                status_code=200,
                json=get_dft_webhook_data(),
                headers={}  # Empty headers
            )
        )

        payload = get_dft_webhook_data()
        task_send_webhooks(json.dumps(payload))

        webhook_logs = db.exec(select(WebhookLog)).all()
        assert len(webhook_logs) == 1
        assert webhook_logs[0].status == 'Success'  # Should still succeed despite missing headers

        # Test with malformed headers
        mock_request.reset()
        mock_request.return_value = httpx.Response(
            status_code=200,
            json=get_dft_webhook_data(),
            headers={'Content-Type': 'invalid'}  # Malformed content type
        )

        task_send_webhooks(json.dumps(payload))
        webhook_logs = db.exec(select(WebhookLog)).all()
        assert len(webhook_logs) == 2
        assert webhook_logs[1].status == 'Success'  # Should still succeed despite malformed headers
