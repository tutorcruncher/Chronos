import json

from fastapi.testclient import TestClient
from sqlmodel import Session

from chronos.main import app
from tests.test_helpers import (
    _get_webhook_headers,
    create_endpoint_from_dft_data,
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

    payload = get_dft_endpoint_deletion_data()
    headers = _get_webhook_headers()
    r = client.post(
        delete_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json() == {'message': f'WebhookEndpoint {ep.name} (TC ID: {ep.tc_id}) deleted'}


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
