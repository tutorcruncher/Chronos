import json
from datetime import UTC, datetime
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from chronos.main import app
from chronos.sql_models import WebhookEndpoint, WebhookLog
from tests.test_helpers import (
    _get_webhook_headers,
    create_endpoint_from_dft_data,
    create_webhook_log_from_dft_data,
    get_dft_endpoint_data_list,
    get_dft_endpoint_deletion_data,
)

create_update_url = app.url_path_for('create_update_endpoint')
delete_url = app.url_path_for('delete_endpoint')


def test_create_endpoint(session: Session, client: TestClient):
    payload = get_dft_endpoint_data_list()
    headers = _get_webhook_headers()
    r = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert r.status_code == 200
    response = r.json()
    created, updated = response['created'], response['updated']
    integration = payload['integrations'][0]
    assert {
        'message': f'WebhookEndpoint test_endpoint_{integration["tc_id"]} (TC ID: {integration["tc_id"]}) created'
    } in created
    assert not updated


def test_update_endpoint_correct_data(session: Session, client: TestClient):
    eps = create_endpoint_from_dft_data()
    session.add(eps[0])
    session.commit()

    payload = get_dft_endpoint_data_list(name='diff name')
    headers = _get_webhook_headers()
    r = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert r.status_code == 200
    response = r.json()
    created, updated = response['created'], response['updated']
    integration = payload['integrations'][0]
    assert {'message': f'WebhookEndpoint diff name (TC ID: {integration["tc_id"]}) updated'} in updated
    assert not created


def test_update_endpoint_invalid_data(session: Session, client: TestClient):
    payload = get_dft_endpoint_data_list(active=50)
    headers = _get_webhook_headers()

    r = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert r.status_code == 422
    assert r.json()['detail'][0]['msg'] == 'Input should be a valid boolean, unable to interpret input'


def test_delete_endpoint(session: Session, client: TestClient):
    eps = create_endpoint_from_dft_data()
    ep = eps[0]
    session.add(ep)
    session.commit()
    ep_id = ep.id

    for _ in range(3):
        session.add(create_webhook_log_from_dft_data(webhook_endpoint_id=ep_id, timestamp=datetime.now(UTC).replace(tzinfo=None)))
    session.commit()

    payload = get_dft_endpoint_deletion_data()
    headers = _get_webhook_headers()
    with patch('chronos.views.task_delete_endpoint.delay') as mock_delay:
        r = client.post(
            delete_url,
            data=json.dumps(payload),
            headers=headers,
        )

    assert r.status_code == 200
    assert r.json() == {'message': f'WebhookEndpoint {ep.name} (TC ID: {ep.tc_id}) deletion initiated'}
    mock_delay.assert_called_once_with(ep_id)

    endpoint = session.exec(select(WebhookEndpoint).where(WebhookEndpoint.id == ep_id)).one()
    assert endpoint.active is False

    logs = session.exec(select(WebhookLog).where(WebhookLog.webhook_endpoint_id == ep_id)).all()
    assert len(logs) == 3


def test_delete_endpoint_doesnt_exist(session: Session, client: TestClient):
    tc_id = get_dft_endpoint_data_list()['integrations'][0]['tc_id']
    payload = get_dft_endpoint_deletion_data()
    headers = _get_webhook_headers()
    r = client.post(
        delete_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json() == {
        'message': f'WebhookEndpoint with TC ID: {tc_id} not found: No row was found when one was required'
    }


def test_delete_endpoint_invalid_data(session: Session, client: TestClient):
    payload = get_dft_endpoint_deletion_data(tc_id='invalid')
    headers = _get_webhook_headers()
    r = client.post(
        delete_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert r.status_code == 422
    assert r.json()['detail'][0]['msg'] == 'Input should be a valid integer, unable to parse string as an integer'
