import hashlib
import hmac
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
    def test_send_webhook_one(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers()
        mock_request = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))

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
        for ep in eps:
            mock_request = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))
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
        assert mock_request.called

    @respx.mock
    def test_send_correct_branch(self, db: Session, client: TestClient, celery_session_worker):
        endpoints = db.exec(select(WebhookEndpoint)).all()
        assert len(endpoints) == 0

        for tc_id in range(1, 6):
            ep = create_endpoint_from_dft_data(tc_id=tc_id)[0]
            db.add(ep)

            ep = create_endpoint_from_dft_data(tc_id=tc_id + 10, branch_id=199)[0]
            db.add(ep)

            ep = create_endpoint_from_dft_data(tc_id=tc_id + 100, branch_id=299)[0]
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
        headers = _get_webhook_headers()
        mock_request = respx.post(endpoints[0].webhook_url).mock(return_value=get_successful_response(payload, headers))

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 5
        assert len(mock_request.calls) == 5

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
        headers = _get_webhook_headers()
        mock_request.return_value = get_successful_response(payload, headers)

        sending_webhooks = task_send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10
        assert len(mock_request.calls) == 10

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

    @respx.mock
    def test_split_events_multiple(self, db: Session, client: TestClient, celery_session_worker):
        """An endpoint with 3 events produces 3 webhook logs (one per event)."""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        payload['events'] = [
            {'branch': 99, 'event': 'test_event_1', 'data': {'test': 'data1'}},
            {'branch': 99, 'event': 'test_event_2', 'data': {'test': 'data2'}},
            {'branch': 99, 'event': 'test_event_3', 'data': {'test': 'data3'}},
        ]
        headers = _get_webhook_headers()
        mock_request = respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))

        task_send_webhooks(json.dumps(payload))

        assert mock_request.call_count == 3
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 3
        for webhook in webhooks:
            assert webhook.status == 'Success'
            assert webhook.status_code == 200

    @respx.mock
    def test_split_events_single_event_per_request_body(self, db: Session, client: TestClient, celery_session_worker):
        """Each request body contains exactly one event with other payload fields preserved."""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        payload['events'] = [
            {'branch': 99, 'event': 'test_event_1', 'data': {'test': 'data1'}},
            {'branch': 99, 'event': 'test_event_2', 'data': {'test': 'data2'}},
        ]
        headers = _get_webhook_headers()
        respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))

        task_send_webhooks(json.dumps(payload))

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 2

        request_bodies = [json.loads(wh.request_body) for wh in webhooks]
        event_names = sorted([rb['events'][0]['event'] for rb in request_bodies])
        assert event_names == ['test_event_1', 'test_event_2']
        for rb in request_bodies:
            assert len(rb['events']) == 1
            assert rb['request_time'] == 1234567890

    @respx.mock
    def test_split_events_multiple_endpoints(self, db: Session, client: TestClient, celery_session_worker):
        """Multiple endpoints each get a separate request per event."""
        ep1 = create_endpoint_from_dft_data(tc_id=1, webhook_url='https://endpoint-one.com')[0]
        ep2 = create_endpoint_from_dft_data(tc_id=2, webhook_url='https://endpoint-two.com')[0]
        db.add(ep1)
        db.add(ep2)
        db.commit()

        payload = get_dft_webhook_data()
        payload['events'] = [
            {'branch': 99, 'event': 'test_event_1', 'data': {'test': 'data1'}},
            {'branch': 99, 'event': 'test_event_2', 'data': {'test': 'data2'}},
            {'branch': 99, 'event': 'test_event_3', 'data': {'test': 'data3'}},
        ]
        headers = _get_webhook_headers()
        mock_ep1 = respx.post(ep1.webhook_url).mock(return_value=get_successful_response(payload, headers))
        mock_ep2 = respx.post(ep2.webhook_url).mock(return_value=get_successful_response(payload, headers))

        task_send_webhooks(json.dumps(payload))

        assert mock_ep1.call_count == 3
        assert mock_ep2.call_count == 3

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 6

        for ep in [ep1, ep2]:
            ep_logs = db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep.id)).all()
            assert len(ep_logs) == 3
            for log in ep_logs:
                body = json.loads(log.request_body)
                assert len(body['events']) == 1

    @respx.mock
    def test_split_events_signature_per_event(self, db: Session, client: TestClient, celery_session_worker):
        """HMAC signature is computed on each individual split payload, not the original."""
        ep = create_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        payload['events'] = [
            {'branch': 99, 'event': 'test_event_1', 'data': {'test': 'data1'}},
            {'branch': 99, 'event': 'test_event_2', 'data': {'test': 'data2'}},
        ]
        headers = _get_webhook_headers()
        respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, headers))

        task_send_webhooks(json.dumps(payload))

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 2

        for webhook in webhooks:
            request_headers = json.loads(webhook.request_headers)
            actual_sig = request_headers['webhook-signature']
            expected_sig = hmac.new(ep.api_key.encode(), webhook.request_body.encode(), hashlib.sha256).hexdigest()
            assert actual_sig == expected_sig

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
