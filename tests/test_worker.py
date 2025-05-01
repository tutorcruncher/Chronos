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

        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
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

        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10
        assert all(mock_request.called for mock_request in mock_requests)

    @respx.mock
    def test_send_correct_branch(self, db: Session, client: TestClient, celery_session_worker):
        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 0

        mock_requests = []
        for tc_id in range(1, 6):
            ep = create_endpoint_from_dft_data(tc_id=tc_id)[0]
            mock_requests.append(
                respx.post(ep.webhook_url).mock(
                    return_value=httpx.Response(
                        status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers()
                    )
                )
            )
            db.add(ep)

            ep = create_endpoint_from_dft_data(tc_id=tc_id + 10, branch_id=199)[0]
            mock_requests.append(
                respx.post(ep.webhook_url).mock(
                    return_value=httpx.Response(
                        status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers()
                    )
                )
            )
            db.add(ep)

            ep = create_endpoint_from_dft_data(tc_id=tc_id + 100, branch_id=299)[0]
            mock_requests.append(
                respx.post(ep.webhook_url).mock(
                    return_value=httpx.Response(
                        status_code=200, json=get_dft_webhook_data(), headers=_get_webhook_headers()
                    )
                )
            )
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

        payload = get_dft_webhook_data()
        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 5
        assert sum(mock_request.call_count for mock_request in mock_requests[:5]) == 5

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

        payload = get_dft_webhook_data(branch_id=199)
        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10
        assert sum(mock_request.call_count for mock_request in mock_requests[5:10]) == 5

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
        mock_request = respx.post('https://test_endpoint_1.com').mock(
            return_value=httpx.Response(status_code=409, json=payload, headers=headers)
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
        payload = get_dft_webhook_data()
        eps = create_endpoint_from_dft_data(webhook_url='https://test-http-errors.com')
        for ep in eps:
            db.add(ep)
        db.commit()

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0
        assert not mock_logger.info.called

        mock_request = respx.post(ep.webhook_url).mock(side_effect=httpx.TimeoutException(message='Timeout error'))
        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert mock_logger.info.call_count == 4
        assert 'Timeout error sending webhook to' in mock_logger.info.call_args_list[1][0][0]
        assert mock_request.call_count == 1
        assert len(webhooks) == 1

        webhook = webhooks[0]
        assert webhook.status == 'Unexpected response'
        assert webhook.status_code == 999
        assert webhook.response_body == '{"Message": "No response from endpoint"}'
        assert webhook.response_headers == '{"Message": "No response from endpoint"}'

        mock_request = respx.post(eps[0].webhook_url).mock(side_effect=httpx.RequestError(message='Connection error'))
        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert mock_logger.info.call_count == 8
        assert 'HTTP error sending webhook to' in mock_logger.info.call_args_list[5][0][0]
        assert mock_request.call_count == 2
        assert len(webhooks) == 2

        mock_request = respx.post(eps[0].webhook_url).mock(side_effect=httpx.HTTPError(message='HTTP error'))
        task_send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert mock_logger.info.call_count == 12
        assert 'HTTP error sending webhook to' in mock_logger.info.call_args_list[9][0][0]
        assert mock_request.call_count == 3
        assert len(webhooks) == 3

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
