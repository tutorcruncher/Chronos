import json
from datetime import datetime, timedelta

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from chronos.main import app
from chronos.sql_models import WebhookLog
from chronos.tasks.dispatcher import dispatch_cycle
from tests.test_helpers import (
    _get_webhook_headers,
    create_endpoint_from_dft_data,
    create_webhook_log_from_dft_data,
    get_dft_con_webhook_data,
    get_dft_get_log_data,
    get_dft_webhook_data,
    send_webhook_url,
    send_webhook_with_extension_url,
)


def test_send_webhooks(session: Session, client: TestClient):
    payload = get_dft_webhook_data()
    headers = _get_webhook_headers()

    r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'

    from chronos.worker import job_queue

    assert job_queue.has_active_jobs()
    dispatched = dispatch_cycle()
    assert dispatched == 1
    assert not job_queue.has_active_jobs()


def test_send_webhooks__request_time(session: Session, client: TestClient):
    payload = get_dft_con_webhook_data(_request_time=1234567890, request_time=None)
    headers = _get_webhook_headers()

    r = client.post(send_webhook_with_extension_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'

    from chronos.worker import job_queue

    assert job_queue.has_active_jobs()
    dispatched = dispatch_cycle()
    assert dispatched == 1
    assert not job_queue.has_active_jobs()


def test_send_webhooks_con_endpoint(session: Session, client: TestClient):
    payload = get_dft_con_webhook_data()
    headers = _get_webhook_headers()

    r = client.post(send_webhook_with_extension_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'

    from chronos.worker import job_queue

    assert job_queue.has_active_jobs()
    dispatched = dispatch_cycle()
    assert dispatched == 1
    assert not job_queue.has_active_jobs()


def test_send_webhooks_con_endpoint_deleted_user(session: Session, client: TestClient):
    set_deleted_kwargs = {
        'id': 1,
        'deleted': True,
        'first_name': None,
        'last_name': None,
        'town': None,
        'country': None,
        'review_rating': None,
        'review_duration': None,
        'location': None,
        'photo': None,
        'extra_attributes': None,
        'skills': None,
        'labels': None,
        'created': None,
        'release_timestamp': 'test',
        'request_time': 1234567890,
    }
    payload = get_dft_con_webhook_data(**set_deleted_kwargs)
    headers = _get_webhook_headers()

    r = client.post(send_webhook_with_extension_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 200
    assert r.json()['message'] == 'Sending webhooks to endpoints has been successfully initiated.'

    from chronos.worker import job_queue

    assert job_queue.has_active_jobs()
    dispatched = dispatch_cycle()
    assert dispatched == 1
    assert not job_queue.has_active_jobs()


def test_send_webhook_bad_request(session: Session, client: TestClient):
    payload = get_dft_webhook_data(request_time='I am a string')
    headers = _get_webhook_headers()

    r = client.post(send_webhook_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 422
    assert r.json()['detail'][0]['msg'] == 'Input should be a valid integer, unable to parse string as an integer'

    from chronos.worker import job_queue

    assert not job_queue.has_active_jobs()


def test_get_logs_none(session: Session, client: TestClient):
    eps = create_endpoint_from_dft_data()
    session.add(eps[0])
    session.commit()

    payload = get_dft_get_log_data()
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])

    r = client.get(
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
    eps = create_endpoint_from_dft_data()
    ep = eps[0]
    session.add(ep)
    session.commit()

    whl = create_webhook_log_from_dft_data(
        webhook_endpoint_id=ep.id,
        timestamp=datetime.now(),
    )
    session.add(whl)
    session.commit()

    logs = session.exec(select(WebhookLog)).all()
    assert len(logs) == 1

    payload = get_dft_get_log_data()
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.get(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert len(r.json()['logs']) == 1
    assert r.json()['count'] == 1


def test_get_logs_many(session: Session, client: TestClient):
    eps = create_endpoint_from_dft_data()
    ep = eps[0]
    session.add(ep)
    session.commit()

    for i in range(1, 101):
        whl = create_webhook_log_from_dft_data(
            webhook_endpoint_id=ep.id,
            timestamp=datetime.now() - timedelta(days=i),
        )
        session.add(whl)
    session.commit()

    logs = session.exec(select(WebhookLog)).all()
    assert len(logs) == 100

    payload = get_dft_get_log_data()
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.get(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert len(r.json()['logs']) == 50
    assert r.json()['count'] == 100

    payload['page'] = 1
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.get(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert len(r.json()['logs']) == 50
    assert r.json()['count'] == 100

    payload['page'] = 2
    headers = _get_webhook_headers()
    get_logs_url = app.url_path_for('get_logs', tc_id=payload['tc_id'], page=payload['page'])
    r = client.get(
        get_logs_url,
        headers=headers,
    )
    assert r.status_code == 200
    assert len(r.json()['logs']) == 0
    assert r.json()['count'] == 100
    assert r.json()['message'] == 'No logs found for page: 2'
