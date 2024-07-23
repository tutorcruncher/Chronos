import json
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, col, select

from app.main import app
from app.sql_models import Endpoint, WebhookLog
from app.worker import _delete_old_logs_job, send_webhooks
from tests.test_helpers import (
    _get_webhook_headers,
    create_endpoint_from_dft_data,
    create_webhook_log_from_dft_data,
    get_dft_webhook_data,
    get_failed_response,
    get_successful_response,
)

send_webhook_url = app.url_path_for('send_webhook')


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

    @patch('app.worker.session.request')
    def test_send_webhook_one(self, mock_response, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers(payload)
        mock_response.return_value = get_successful_response(payload, headers)

        endpoints = db.exec(select(Endpoint)).all()
        assert len(endpoints) == 1

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        sending_webhooks = send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1

        webhook = webhooks[0]
        assert webhook.status == 'Success'
        assert webhook.status_code == 200

    @patch('app.worker.session.request')
    def test_send_many_endpoints(self, mock_response, db: Session, client: TestClient, celery_session_worker):
        endpoints = db.exec(select(Endpoint)).all()
        assert len(endpoints) == 0

        for tc_id in range(1, 11):
            ep = create_endpoint_from_dft_data(tc_id=tc_id)
            db.add(ep)
        db.commit()

        endpoints = db.exec(select(Endpoint)).all()
        assert len(endpoints) == 10

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers(payload)
        mock_response.return_value = get_successful_response(payload, headers)

        sending_webhooks = send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10

    @patch('app.worker.session.request')
    def test_send_correct_branch(self, mock_response, db: Session, client: TestClient, celery_session_worker):
        endpoints = db.exec(select(Endpoint)).all()
        assert len(endpoints) == 0

        for tc_id in range(1, 6):
            ep = create_endpoint_from_dft_data(tc_id=tc_id)
            db.add(ep)

            ep = create_endpoint_from_dft_data(tc_id=tc_id + 10, branch_id=199)
            db.add(ep)

            ep = create_endpoint_from_dft_data(tc_id=tc_id + 100, branch_id=299)
            db.add(ep)
        db.commit()

        endpoints = db.exec(select(Endpoint)).all()
        assert len(endpoints) == 15

        endpoints_1 = db.exec(select(Endpoint).where(Endpoint.branch_id == 99)).all()
        assert len(endpoints_1) == 5
        endpoints_2 = db.exec(select(Endpoint).where(Endpoint.branch_id == 199)).all()
        assert len(endpoints_2) == 5
        endpoints_3 = db.exec(select(Endpoint).where(Endpoint.branch_id == 299)).all()
        assert len(endpoints_3) == 5

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers(payload)
        mock_response.return_value = get_successful_response(payload, headers)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        sending_webhooks = send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 5

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_1]))
        ).all()
        assert len(webhooks) == 5

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_2]))
        ).all()
        assert len(webhooks) == 0

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_3]))
        ).all()
        assert len(webhooks) == 0

        payload = get_dft_webhook_data(branch_id=199)
        headers = _get_webhook_headers(payload)
        mock_response.return_value = get_successful_response(payload, headers)

        sending_webhooks = send_webhooks.delay(json.dumps(payload))
        sending_webhooks.get(timeout=10)
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 10

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_1]))
        ).all()
        assert len(webhooks) == 5

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_2]))
        ).all()
        assert len(webhooks) == 5

        webhooks = db.exec(
            select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_3]))
        ).all()
        assert len(webhooks) == 0

    @patch('app.worker.session.request')
    def test_send_webhook_fail_to_send_only_one(
        self, mock_response, db: Session, client: TestClient, celery_session_worker
    ):
        ep = create_endpoint_from_dft_data()
        db.add(ep)
        db.commit()

        payload = get_dft_webhook_data()
        headers = _get_webhook_headers(payload)
        mock_response.return_value = get_failed_response(payload, headers)

        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 0

        send_webhooks(json.dumps(payload))
        webhooks = db.exec(select(WebhookLog)).all()
        assert len(webhooks) == 1

        webhook = webhooks[0]
        assert webhook.status == 'Unexpected response'
        assert webhook.status_code == 409

    def test_delete_old_logs(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_endpoint_from_dft_data()
        db.add(ep)
        db.commit()

        for i in range(1, 31):
            whl = create_webhook_log_from_dft_data(
                endpoint_id=ep.id,
                timestamp=datetime.now() - timedelta(days=i),
            )
            db.add(whl)
        db.commit()

        logs = db.exec(select(WebhookLog)).all()
        assert len(logs) == 30

        _delete_old_logs_job()
        logs = db.exec(select(WebhookLog)).all()
        assert len(logs) == 15
