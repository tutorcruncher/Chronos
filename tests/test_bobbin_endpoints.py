import json
from datetime import UTC, datetime
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from chronos.main import app
from chronos.sql_models import WebhookEndpoint
from chronos.utils import settings
from tests.test_helpers import (
    _get_bobbin_headers,
    create_bobbin_endpoint_from_dft_data,
    create_bobbin_webhook_log_from_dft_data,
    get_dft_bobbin_endpoint_data,
    get_dft_bobbin_endpoint_deletion_data,
    get_dft_bobbin_send_data,
    get_dft_endpoint_data_list,
)

create_update_url = app.url_path_for('create_update_bobbin_endpoint')
delete_url = app.url_path_for('delete_bobbin_endpoint')
send_url = app.url_path_for('send_bobbin_webhook')


def test_create_bobbin_endpoint(session: Session, client: TestClient):
    payload = get_dft_bobbin_endpoint_data()
    r = client.post(create_update_url, data=json.dumps(payload), headers=_get_bobbin_headers())
    assert r.status_code == 200
    assert r.json() == {'message': 'WebhookEndpoint bobbin_endpoint_1 (org: 99) created'}
    endpoint = session.exec(select(WebhookEndpoint)).one()
    assert endpoint.provider == 'bobbin'
    assert endpoint.bobbin_id == 1
    assert endpoint.org_id == 99
    assert endpoint.tc_id is None
    assert endpoint.events == []


def test_update_bobbin_endpoint(session: Session, client: TestClient):
    session.add(create_bobbin_endpoint_from_dft_data()[0])
    session.commit()

    payload = get_dft_bobbin_endpoint_data(name='renamed', events=['lesson.completed'])
    r = client.post(create_update_url, data=json.dumps(payload), headers=_get_bobbin_headers())
    assert r.status_code == 200
    assert r.json() == {'message': 'WebhookEndpoint renamed (org: 99) updated'}
    endpoint = session.exec(select(WebhookEndpoint)).one()
    assert endpoint.name == 'renamed'
    assert endpoint.events == ['lesson.completed']


def test_create_bobbin_endpoint_invalid_data(session: Session, client: TestClient):
    payload = get_dft_bobbin_endpoint_data(active=50)
    r = client.post(create_update_url, data=json.dumps(payload), headers=_get_bobbin_headers())
    assert r.status_code == 422
    assert r.json()['detail'][0]['msg'] == 'Input should be a valid boolean, unable to interpret input'


def test_delete_bobbin_endpoint(session: Session, client: TestClient):
    ep = create_bobbin_endpoint_from_dft_data()[0]
    session.add(ep)
    session.commit()
    ep_id = ep.id

    for _ in range(3):
        session.add(
            create_bobbin_webhook_log_from_dft_data(
                webhook_endpoint_id=ep_id, timestamp=datetime.now(UTC).replace(tzinfo=None)
            )
        )
    session.commit()

    payload = get_dft_bobbin_endpoint_deletion_data()
    with patch('chronos.views.bobbin.task_delete_endpoint.delay') as mock_delay:
        r = client.post(delete_url, data=json.dumps(payload), headers=_get_bobbin_headers())

    assert r.status_code == 200
    assert r.json() == {'message': f'WebhookEndpoint {ep.name} (org: 99) deletion initiated'}
    mock_delay.assert_called_once_with(ep_id)

    endpoint = session.exec(select(WebhookEndpoint).where(WebhookEndpoint.id == ep_id)).one()
    assert endpoint.active is False


def test_delete_bobbin_endpoint_doesnt_exist(session: Session, client: TestClient):
    payload = get_dft_bobbin_endpoint_deletion_data()
    r = client.post(delete_url, data=json.dumps(payload), headers=_get_bobbin_headers())
    assert r.status_code == 200
    assert r.json()['message'].startswith('WebhookEndpoint 1 (org: 99) not found')


def test_delete_bobbin_endpoint_invalid_data(session: Session, client: TestClient):
    payload = get_dft_bobbin_endpoint_deletion_data(bobbin_endpoint_id='invalid')
    r = client.post(delete_url, data=json.dumps(payload), headers=_get_bobbin_headers())
    assert r.status_code == 422


def test_send_bobbin_webhook_initiated(session: Session, client: TestClient):
    payload = get_dft_bobbin_send_data()
    with patch('chronos.views.bobbin.task_send_webhooks.delay') as mock_delay:
        r = client.post(send_url, data=json.dumps(payload), headers=_get_bobbin_headers())
    assert r.status_code == 200
    assert r.json() == {'message': 'Sending bobbin webhook to endpoints has been successfully initiated.'}
    mock_delay.assert_called_once_with(json.dumps(payload))


def test_send_bobbin_webhook_invalid_data(session: Session, client: TestClient):
    r = client.post(send_url, data=json.dumps({}), headers=_get_bobbin_headers())
    assert r.status_code == 422


def test_bobbin_route_wrong_key_rejected(session: Session, client: TestClient):
    payload = get_dft_bobbin_endpoint_data()
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer wrong-key'}
    with patch.object(settings, 'bobbin_shared_key', 'bobbin-secret'):
        r = client.post(create_update_url, data=json.dumps(payload), headers=headers)
    assert r.status_code == 403
    assert r.json() == {'detail': 'Authorisation token is invalid'}


def test_tc2_key_does_not_authorise_bobbin_routes(session: Session, client: TestClient):
    payload = get_dft_bobbin_endpoint_data()
    with (
        patch.object(settings, 'bobbin_shared_key', 'bobbin-secret'),
        patch.object(settings, 'tc2_shared_key', 'tc2-secret'),
    ):
        r = client.post(
            create_update_url,
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json', 'Authorization': 'Bearer tc2-secret'},
        )
    assert r.status_code == 403


def test_bobbin_key_does_not_authorise_tc2_routes(session: Session, client: TestClient):
    tc2_create_url = app.url_path_for('create_update_endpoint')
    payload = get_dft_endpoint_data_list()
    with (
        patch.object(settings, 'bobbin_shared_key', 'bobbin-secret'),
        patch.object(settings, 'tc2_shared_key', 'tc2-secret'),
    ):
        r = client.post(
            tc2_create_url,
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json', 'Authorization': 'Bearer bobbin-secret'},
        )
    assert r.status_code == 403


def test_get_bobbin_logs(session: Session, client: TestClient):
    ep = create_bobbin_endpoint_from_dft_data()[0]
    session.add(ep)
    session.commit()
    for _ in range(3):
        session.add(
            create_bobbin_webhook_log_from_dft_data(
                webhook_endpoint_id=ep.id, timestamp=datetime.now(UTC).replace(tzinfo=None)
            )
        )
    session.commit()

    url = app.url_path_for('get_bobbin_logs', organization_id=99, bobbin_endpoint_id=1, page=0)
    r = client.get(url, headers=_get_bobbin_headers())
    assert r.status_code == 200
    data = r.json()
    assert data['count'] == 3
    assert len(data['logs']) == 3
    log = data['logs'][0]
    assert log['url'] == ep.webhook_url
    assert log['status'] == 'Success'
    assert log['status_code'] == 200
    assert log['request_headers'] == {'User-Agent': 'Bobbin', 'Content-Type': 'application/json'}


def test_get_bobbin_logs_wrong_org_returns_empty(session: Session, client: TestClient):
    ep = create_bobbin_endpoint_from_dft_data()[0]
    session.add(ep)
    session.commit()
    session.add(create_bobbin_webhook_log_from_dft_data(webhook_endpoint_id=ep.id))
    session.commit()

    url = app.url_path_for('get_bobbin_logs', organization_id=12345, bobbin_endpoint_id=1, page=0)
    r = client.get(url, headers=_get_bobbin_headers())
    assert r.status_code == 200
    assert r.json()['logs'] == []
    assert r.json()['count'] == 0


def test_get_bobbin_logs_empty_page(session: Session, client: TestClient):
    ep = create_bobbin_endpoint_from_dft_data()[0]
    session.add(ep)
    session.commit()

    url = app.url_path_for('get_bobbin_logs', organization_id=99, bobbin_endpoint_id=1, page=5)
    r = client.get(url, headers=_get_bobbin_headers())
    assert r.status_code == 200
    assert r.json() == {'message': 'No logs found for page: 5', 'logs': [], 'count': 250}
