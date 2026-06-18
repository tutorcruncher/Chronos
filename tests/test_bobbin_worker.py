import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import httpx
import pytest
import respx
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlmodel import Session, select

from chronos.sql_models import BobbinWebhookEndpoint, BobbinWebhookLog, WebhookEndpoint, WebhookLog
from chronos.worker import (
    BOBBIN_DELETE_JOBS_KEY,
    _delete_old_bobbin_logs_job,
    cache,
    delete_old_bobbin_logs_job,
    task_delete_bobbin_endpoint,
    task_send_bobbin_webhooks,
    task_send_webhooks,
)
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


class TestBobbinWorkers:
    @pytest.fixture
    def db(self, app_db):
        return app_db

    @pytest.fixture(autouse=True)
    def cleanup_bobbin_data(self, app_db):
        yield
        tc2_ids = app_db.exec(select(WebhookEndpoint.id).where(WebhookEndpoint.tc_id == 5)).all()
        if tc2_ids:
            app_db.exec(delete(WebhookLog).where(WebhookLog.webhook_endpoint_id.in_(tc2_ids)))
            app_db.exec(delete(WebhookEndpoint).where(WebhookEndpoint.tc_id == 5))
        app_db.exec(delete(BobbinWebhookLog))
        app_db.exec(delete(BobbinWebhookEndpoint))
        app_db.commit()

    @respx.mock
    def test_send_bobbin_webhook_one(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_bobbin_send_data()
        mock_request = respx.post(ep.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert mock_request.called
        logs = db.exec(select(BobbinWebhookLog)).all()
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

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        logs = db.exec(select(BobbinWebhookLog)).all()
        assert len(logs) == 5

    @respx.mock
    def test_send_bobbin_only_matching_org(self, db: Session, client: TestClient, celery_session_worker):
        ep_target = create_bobbin_endpoint_from_dft_data(organization_id=99)[0]
        ep_other = create_bobbin_endpoint_from_dft_data(organization_id=88, webhook_url='https://other-org.com')[0]
        db.add(ep_target)
        db.add(ep_other)
        db.commit()

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
        assert len(db.exec(select(BobbinWebhookLog)).all()) == 1

    @respx.mock
    def test_send_bobbin_events_filter(self, db: Session, client: TestClient, celery_session_worker):
        ep_match = create_bobbin_endpoint_from_dft_data(events=['lesson.completed'])[0]
        ep_miss = create_bobbin_endpoint_from_dft_data(
            bobbin_endpoint_id=2, events=['transcript.ready'], webhook_url='https://miss.com'
        )[0]
        db.add(ep_match)
        db.add(ep_miss)
        db.commit()

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
        assert len(db.exec(select(BobbinWebhookLog)).all()) == 1

    @respx.mock
    def test_send_bobbin_inactive_not_sent(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data(active=False)[0]
        db.add(ep)
        db.commit()

        payload = get_dft_bobbin_send_data()
        mock_request = respx.post(ep.webhook_url).mock(
            return_value=get_successful_response(payload, _get_bobbin_headers())
        )

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert not mock_request.called
        assert not db.exec(select(BobbinWebhookLog)).all()

    @respx.mock
    def test_send_bobbin_invalid_url_not_sent(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data(webhook_url='htp://bad.com')[0]
        db.add(ep)
        db.commit()

        payload = get_dft_bobbin_send_data()
        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert not db.exec(select(BobbinWebhookLog)).all()

    @respx.mock
    def test_send_bobbin_failure_logged(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_bobbin_send_data()
        respx.post(ep.webhook_url).mock(return_value=get_failed_response(payload, _get_bobbin_headers()))

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        logs = db.exec(select(BobbinWebhookLog)).all()
        assert len(logs) == 1
        assert logs[0].status == 'Unexpected response'
        assert logs[0].status_code == 409

    @respx.mock
    def test_send_bobbin_no_response_logged(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_bobbin_send_data()
        respx.post(ep.webhook_url).mock(side_effect=httpx.TimeoutException(message='Timeout'))

        task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        logs = db.exec(select(BobbinWebhookLog)).all()
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
        assert len(db.exec(select(BobbinWebhookLog)).all()) == 1
        assert len(db.exec(select(WebhookLog)).all()) == 0

        tc2_payload = get_dft_webhook_data(branch_id=5)
        task_send_webhooks(json.dumps(tc2_payload))

        assert mock_tc2.called
        assert len(db.exec(select(WebhookLog)).all()) == 1
        assert len(db.exec(select(BobbinWebhookLog)).all()) == 1

    def test_delete_bobbin_endpoint_task(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        for _ in range(5):
            db.add(
                create_bobbin_webhook_log_from_dft_data(
                    bobbin_webhook_endpoint_id=ep_id, timestamp=datetime.now(UTC).replace(tzinfo=None)
                )
            )
        db.commit()

        with patch('chronos.worker.WEBHOOK_LOG_DELETE_BATCH_SIZE', 2):
            task_delete_bobbin_endpoint(ep_id)

        db.expire_all()
        assert db.get(BobbinWebhookEndpoint, ep_id) is None
        assert not db.exec(select(BobbinWebhookLog).where(BobbinWebhookLog.bobbin_webhook_endpoint_id == ep_id)).all()

    def test_delete_bobbin_endpoint_task_idempotent(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        db.add(create_bobbin_webhook_log_from_dft_data(bobbin_webhook_endpoint_id=ep_id))
        db.commit()

        task_delete_bobbin_endpoint(ep_id)
        task_delete_bobbin_endpoint(ep_id)
        task_delete_bobbin_endpoint(999999)

        db.expire_all()
        assert db.get(BobbinWebhookEndpoint, ep_id) is None

    def test_delete_old_bobbin_logs(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        for i in range(0, 30):
            db.add(
                create_bobbin_webhook_log_from_dft_data(
                    bobbin_webhook_endpoint_id=ep.id, timestamp=datetime.utcnow() - timedelta(days=i)
                )
            )
        db.commit()
        assert len(db.exec(select(BobbinWebhookLog)).all()) == 30

        _delete_old_bobbin_logs_job()
        assert len(db.exec(select(BobbinWebhookLog)).all()) == 15

    @respx.mock
    def test_send_bobbin_unexpected_error_logged(self, db: Session, client: TestClient, celery_session_worker):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()

        payload = get_dft_bobbin_send_data()
        with patch('chronos.worker._send_single_webhook', side_effect=ValueError('boom')):
            task_send_bobbin_webhooks(json.dumps(payload), payload['organization_id'])

        assert not db.exec(select(BobbinWebhookLog)).all()

    def test_delete_bobbin_endpoint_handles_removed_mid_cleanup(
        self, db: Session, client: TestClient, celery_session_worker
    ):
        ep = create_bobbin_endpoint_from_dft_data()[0]
        db.add(ep)
        db.commit()
        ep_id = ep.id

        db.add(create_bobbin_webhook_log_from_dft_data(bobbin_webhook_endpoint_id=ep_id))
        db.commit()

        original_get = Session.get
        get_call_count = 0

        def get_side_effect(session, model, ident, *args, **kwargs):
            nonlocal get_call_count
            if model is BobbinWebhookEndpoint and ident == ep_id:
                get_call_count += 1
                if get_call_count == 2:
                    with Session(session.get_bind()) as concurrent_db:
                        endpoint = concurrent_db.get(BobbinWebhookEndpoint, ep_id)
                        if endpoint is not None:
                            concurrent_db.delete(endpoint)
                            concurrent_db.commit()
                    return None
            return original_get(session, model, ident, *args, **kwargs)

        with patch.object(Session, 'get', autospec=True, side_effect=get_side_effect):
            task_delete_bobbin_endpoint(ep_id)

        db.expire_all()
        assert db.get(BobbinWebhookEndpoint, ep_id) is None
        assert not db.exec(select(BobbinWebhookLog).where(BobbinWebhookLog.bobbin_webhook_endpoint_id == ep_id)).all()

    def test_delete_old_bobbin_logs_job_dispatch(self, db: Session, client: TestClient, celery_session_worker):
        cache.delete(BOBBIN_DELETE_JOBS_KEY)
        with patch('chronos.worker._delete_old_bobbin_logs_job.delay') as mock_delay:
            asyncio.run(delete_old_bobbin_logs_job())
            asyncio.run(delete_old_bobbin_logs_job())
        mock_delay.assert_called_once()
        cache.delete(BOBBIN_DELETE_JOBS_KEY)
