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
    get_dft_get_log_data,
    get_dft_webhook_data,
    send_webhook_url,
)


def test_send_webhooks(session: Session, client: TestClient):
    payload = get_dft_webhook_data()
    headers = _get_webhook_headers()

    with patch('chronos.worker.task_send_webhooks.delay') as mock_task:
        r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
        assert mock_task.called
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'


def test_send_webhook_bad_request(session: Session, client: TestClient):
    payload = get_dft_webhook_data(request_time='I am a string')
    headers = _get_webhook_headers()

    with patch('chronos.worker.task_send_webhooks.delay') as mock_task:
        r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
        assert not mock_task.called
    assert r.status_code == 422
    assert r.json()['detail'][0]['msg'] == 'Input should be a valid integer, unable to parse string as an integer'


def test_get_logs_none(session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    payload = get_dft_get_log_data()
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.post(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json() == {
        'logs': [],
        'count': 0,
        'message': 'No logs found for page: 0',
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

    payload = get_dft_get_log_data()
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.post(
        get_logs_url,
        headers=headers,
    )
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

    payload = get_dft_get_log_data()
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.post(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert len(r.json()['logs']) == 50
    assert r.json()['count'] == 100

    payload['page'] = 1
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.post(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert len(r.json()['logs']) == 50
    assert r.json()['count'] == 100

    payload['page'] = 2
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.post(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert len(r.json()['logs']) == 0
    assert r.json()['count'] == 100
    assert r.json()['message'] == 'No logs found for page: 2'
