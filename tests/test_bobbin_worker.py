import hashlib
import hmac
import json
from datetime import UTC, datetime
from unittest.mock import patch

import httpx
import pytest
import respx
from fastapi.testclient import TestClient
from sqlalchemy import delete, or_
from sqlmodel import Session, select

from chronos.sql_models import WebhookEndpoint, WebhookLog
from chronos.worker import task_delete_endpoint, task_send_bobbin_webhooks, task_send_webhooks
from tests.test_helpers import (
    _get_bobbin_headers,
    _get_webhook_headers,
    create_bobbin_endpoint_from_dft_data,
    create_bobbin_webhook_log_from_dft_data,
    create_endpoint_from_dft_data,
    get_dft_bobbin_send_data,
    get_dft_webhook_data,
    get_failed_response,
    get_successful_response,
)


def _logs_for(db: Session, endpoint_id: int) -> list[WebhookLog]:
    return db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == endpoint_id)).all()


class TestBobbinWorkers:
    @pytest.fixture
    def db(self, app_db):
        return app_db

    @pytest.fixture(autouse=True)
    def cleanup_bobbin_data(self, app_db):
        """Remove rows committed by the real Celery tasks (app_db has no transactional rollback)."""
        yield
        ep_ids = app_db.exec(
            select(WebhookEndpoint.id).where(or_(WebhookEndpoint.bobbin_id.is_not(None), WebhookEndpoint.tc_id == 5))
        ).all()
        if ep_ids:
            app_db.exec(delete(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_(ep_ids)))
            app_db.exec(delete(WebhookEndpoint).where(WebhookEndpoint.id.in_(ep_ids)))
        app_db.commit()

    @respx.mock
    def test_send_bobbin_webhook_one(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        payload = get_dft_bobbin_send_data()
        mock_request = respx.post(ep.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert mock_request.called
        logs = _logs_for(db, ep_id)
        assert len(logs) == 1
        assert logs[0].status == 'Success'
        assert logs[0].status_code == 200

        request = mock_request.calls[0].request
        assert request.headers['user-agent'] == 'Bobbin'
        expected_sig = hmac.new(ep.api_key.encode(), request.content, hashlib.sha256).hexdigest()
        assert request.headers['webhook-signature'] == expected_sig
        assert json.loads(request.content.decode()) == payload

    @respx.mock
    def test_send_bobbin_webhook_many_endpoints(self, db: Session, client: TestClient, celery_session_worker):
        eps = create_bobbin_endpoint_from_dft_data(count=5)
        payload = get_dft_bobbin_send_data()
        for ep in eps:
            respx.post(ep.webhook_url).mock(return_value=get_successful_response(payload, _get_bobbin_headers()))
            db.add(ep)
        db.commit()
        ep_ids = [ep.id for ep in eps]

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        logs = db.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_(ep_ids))).all()
        assert len(logs) == 5

    @respx.mock
    def test_send_bobbin_only_matching_org(self, db: Session, client: TestClient, celery_session_worker):
        ep_target = create_bobbin_endpoint_from_dft_data(organization_id=99)[0]
        ep_other = create_bobbin_endpoint_from_dft_data(organization_id=88, webhook_url='https://other-org.com')[0]
        db.add(ep_target)
        db.add(ep_other)
        db.commit()
        target_id, other_id = ep_target.id, ep_other.id

        payload = get_dft_bobbin_send_data(organization_id=99)
        mock_target = respx.post(ep_target.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )
        mock_other = respx.post(ep_other.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )

        task_send_bobbin_webhooks(json.dumps(payload), 99)

        assert mock_target.called
        assert not mock_other.called
        assert len(_logs_for(db, target_id)) == 1
        assert len(_logs_for(db, other_id)) == 0

    @respx.mock
    def test_send_bobbin_events_filter(self, db: Session, client: TestClient, celery_session_worker):
        ep_match = create_bobbin_endpoint_from_dft_data(events=['lesson.completed'])[0]
        ep_miss = create_bobbin_endpoint_from_dft_data(
            bobbin_endpoint_id=2, events=['transcript.ready'], webhook_url='https://miss.com'
        )[0]
        db.add(ep_match)
        db.add(ep_miss)
        db.commit()
        match_id, miss_id = ep_match.id, ep_miss.id

        payload = get_dft_bobbin_send_data(event_type='lesson.completed')
        mock_match = respx.post(ep_match.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )
        mock_miss = respx.post(ep_miss.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert mock_match.called
        assert not mock_miss.called
        assert len(_logs_for(db, match_id)) == 1
        assert len(_logs_for(db, miss_id)) == 0

    @respx.mock
    def test_send_bobbin_inactive_not_sent(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data(active=False)[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        payload = get_dft_bobbin_send_data()
        mock_request = respx.post(ep.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert not mock_request.called
        assert not _logs_for(db, ep_id)

    @respx.mock
    def test_send_bobbin_invalid_url_not_sent(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data(webhook_url='htp://bad.com')[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        payload = get_dft_bobbin_send_data()
        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert not _logs_for(db, ep_id)

    @respx.mock
    def test_send_bobbin_failure_logged(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        payload = get_dft_bobbin_send_data()
        respx.post(ep.webhook_url).mock(return_value=get_failed_response(payload, _get_bobbin_headers()))

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        logs = _logs_for(db, ep_id)
        assert len(logs) == 1
        assert logs[0].status == 'Unexpected response'
        assert logs[0].status_code == 409

    @respx.mock
    def test_send_bobbin_no_response_logged(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        payload = get_dft_bobbin_send_data()
        respx.post(ep.webhook_url).mock(side_effect=httpx.TimeoutException(message='Timeout'))

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        logs = _logs_for(db, ep_id)
        assert len(logs) == 1
        assert logs[0].status == 'No response'
        assert logs[0].status_code == 999

    @respx.mock
    def test_bobbin_and_tc2_share_id_no_collision(self, db: Session, client: TestClient, celery_session_worker):
        """A TC2 endpoint and a Bobbin endpoint with the same integer id/tenant never cross."""
        tc2_ep = create_endpoint_from_dft_data(tc_id=5, branch_id=5)[0]
        bobbin_ep = create_bobbin_endpoint_from_dft_data(
            bobbin_endpoint_id=5, organization_id=5, webhook_url='https://bobbin-five.com'
        )[0]
        db.add(tc2_ep)
        db.add(bobbin_ep)
        db.commit()
        tc2_id, bobbin_id = tc2_ep.id, bobbin_ep.id

        bobbin_payload = get_dft_bobbin_send_data(organization_id=5)
        mock_bobbin = respx.post(bobbin_ep.webhook_url).mock(
            return_value=get_successful_response(bobbin_payload, _get_bobbin_headers())
        )
        mock_tc2 = respx.post(tc2_ep.webhook_url).mock(
            return_value=get_successful_response(bobbin_payload, _get_webhook_headers())
        )

        task_send_bobbin_webhooks(json.dumps(bobbin_payload), 5)

        assert mock_bobbin.called
        assert not mock_tc2.called
        assert len(_logs_for(db, bobbin_id)) == 1
        assert len(_logs_for(db, tc2_id)) == 0

        tc2_payload = get_dft_webhook_data(branch_id=5)
        task_send_webhooks(json.dumps(tc2_payload))

        assert mock_tc2.called
        assert len(_logs_for(db, tc2_id)) == 1
        assert len(_logs_for(db, bobbin_id)) == 1

    def test_delete_bobbin_endpoint_task(self, db: Session, client: TestClient, celery_session_worker):
        """The shared delete task cleans a Bobbin endpoint and its logs."""
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        for _ in range(5):
            db.add(
                create_bobbin_webhook_log_from_dft_data(
                    webhook_endpoint_id=ep_id, timestamp=datetime.now(UTC).replace(tzinfo=None)
                )
            )
        db.commit()

        task_delete_endpoint(ep_id)

        db.expire_all()
        assert db.get(WebhookEndpoint, ep_id) is None
        assert not _logs_for(db, ep_id)

    @respx.mock
    def test_send_bobbin_unexpected_error_logged(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        payload = get_dft_bobbin_send_data()
        with patch('chronos.worker._send_single_webhook', side_effect=ValueError('boom')):
            task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert not _logs_for(db, ep_id)
