import json
from copy import copy
from unittest import mock
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlmodel import Session

from app.main import app
from tests.test_helpers import _get_headers, get_dft_webhook_data

send_webhook_url = app.url_path_for('send_webhook')


def test_send_webhooks(session: Session, client: TestClient):
    payload = get_dft_webhook_data()
    headers = _get_headers(payload)

    with patch('app.worker.send_webhooks.delay') as mock_task:
        r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
        assert mock_task.called
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'


def test_send_webhook_bad_request(session: Session, client: TestClient):
    payload = get_dft_webhook_data(request_time='I am a string')
    headers = _get_headers(payload)

    with patch('app.worker.send_webhooks.delay') as mock_task:
        r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
        assert not mock_task.called
    assert r.status_code == 422
    assert r.json()['detail'][0]['msg'] == 'Input should be a valid integer, unable to parse string as an integer'


def test_get_logs_none(session: Session, client: TestClient):
    pass


def test_get_logs_one(session: Session, client: TestClient):
    pass


def test_get_logs_many(session: Session, client: TestClient):
    pass
