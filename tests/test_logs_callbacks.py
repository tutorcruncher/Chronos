import json
from datetime import datetime, timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from chronos.main import app
from chronos.sql_models import WebhookLog
from tests.test_helpers import (
    _get_webhook_headers,
    create_endpoint_from_dft_data,
    create_webhook_log_from_dft_data,
    get_dft_webhook_data,
)

send_webhook_url = app.url_path_for('send_webhook')


def test_send_webhooks(session: Session, client: TestClient):
    payload = get_dft_webhook_data()
    headers = _get_webhook_headers(payload)

    with patch('chronos.worker.send_webhooks.delay') as mock_task:
        r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
        assert mock_task.called
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'


def test_send_webhook_bad_request(session: Session, client: TestClient):
    payload = get_dft_webhook_data(request_time='I am a string')
    headers = _get_webhook_headers(payload)

    with patch('chronos.worker.send_webhooks.delay') as mock_task:
        r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
        assert not mock_task.called
    assert r.status_code == 422
    assert r.json()['detail'][0]['msg'] == 'Input should be a valid integer, unable to parse string as an integer'


def test_get_logs_none(session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()
    get_logs_url = app.url_path_for('get_logs', endpoint_id=ep.tc_id, page=1)

    r = client.get(get_logs_url)
    assert r.status_code == 200
    assert r.json() == {
        'logs': [],
        'count': 0,
    }


def test_get_logs_one(session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    whl = create_webhook_log_from_dft_data(
        endpoint_id=ep.id,
        timestamp=datetime.now(),
    )
    session.add(whl)
    session.commit()

    logs = session.exec(select(WebhookLog)).all()
    assert len(logs) == 1

    get_logs_url = app.url_path_for('get_logs', endpoint_id=ep.tc_id, page=0)

    r = client.get(get_logs_url)
    assert r.status_code == 200
    assert len(r.json()['logs']) == 1
    assert r.json()['count'] == 1


def test_get_logs_many(session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    for i in range(1, 101):
        whl = create_webhook_log_from_dft_data(
            endpoint_id=ep.id,
            timestamp=datetime.now() - timedelta(days=i),
        )
        session.add(whl)
    session.commit()

    logs = session.exec(select(WebhookLog)).all()
    assert len(logs) == 100

    get_logs_url = app.url_path_for('get_logs', endpoint_id=ep.tc_id, page=0)

    r = client.get(get_logs_url)
    assert r.status_code == 200
    assert len(r.json()['logs']) == 50
    assert r.json()['count'] == 100

    get_logs_url = app.url_path_for('get_logs', endpoint_id=ep.tc_id, page=1)

    r = client.get(get_logs_url)
    assert r.status_code == 200
    assert len(r.json()['logs']) == 50
    assert r.json()['count'] == 100

    get_logs_url = app.url_path_for('get_logs', endpoint_id=ep.tc_id, page=2)

    r = client.get(get_logs_url)
    assert r.status_code == 200
    assert len(r.json()['logs']) == 0
    assert r.json()['count'] == 100
