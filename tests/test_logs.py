import json
from copy import copy

from fastapi.testclient import TestClient
from sqlmodel import Session

from app.main import app
from tests.test_helpers import _get_headers

DFT_WEBHOOK_DATA_FROM_TC2 = {
    'events': [{'branch': 99, 'event': 'test_event', 'data': {'test': 'data'}}],
    'request_time': 1234567890,
}
send_webhook_url = app.url_path_for('send_webhook')


def test_send_webhook_one(session: Session, client: TestClient):
    # receive webhook event
    payload = copy(DFT_WEBHOOK_DATA_FROM_TC2)
    headers = _get_headers(payload)
    r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'
    # send webhook event
    # assert response


def test_send_many_webhooks(session: Session, client: TestClient):
    # receive webhook event
    # send webhook event
    # assert many response
    pass


def test_send_webhook_fail_to_send_one(session: Session, client: TestClient):
    pass


def test_send_webhook_fail_to_send_one_of_many(session: Session, client: TestClient):
    pass


def test_send_webhook_bad_request(session: Session, client: TestClient):
    pass


def test_get_logs_none(session: Session, client: TestClient):
    pass


def test_get_logs_one(session: Session, client: TestClient):
    pass


def test_get_logs_many(session: Session, client: TestClient):
    pass


def test_delete_old_logs(session: Session, client: TestClient):
    pass
