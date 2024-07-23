import json

from fastapi.testclient import TestClient
from sqlmodel import Session

from app.main import app
from tests.test_helpers import (
    _get_webhook_headers,
    get_dft_endpoint_data,
    create_endpoint_from_dft_data,
)

create_update_url = app.url_path_for('create_update_endpoint')


def test_create_endpoint(session: Session, client: TestClient):
    payload = get_dft_endpoint_data()
    headers = _get_webhook_headers(payload)
    response = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint test_endpoint (TC ID: {payload["tc_id"]}) created'}


def test_update_endpoint_correct_data(session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    payload = get_dft_endpoint_data(name='diff name')
    headers = _get_webhook_headers(payload)
    response = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint diff name (TC ID: {payload["tc_id"]}) updated'}


def test_update_endpoint_invalid_data(session: Session, client: TestClient):
    payload = get_dft_endpoint_data(active=50)

    headers = _get_webhook_headers(payload)
    response = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert response.status_code == 422
    assert response.json()['detail'][0]['msg'] == 'Input should be a valid boolean, unable to interpret input'


def test_delete_endpoint(session: Session, client: TestClient):
    payload = get_dft_endpoint_data()
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    tc_id = payload['tc_id']
    url = app.url_path_for('delete_endpoint', endpoint_tc_id=tc_id)
    response = client.post(url)
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint {payload["name"]} (TC ID: {tc_id}) deleted'}


def test_delete_endpoint_doesnt_exist(session: Session, client: TestClient):
    tc_id = get_dft_endpoint_data()['tc_id']
    url = app.url_path_for('delete_endpoint', endpoint_tc_id=tc_id)
    response = client.post(url)
    assert response.status_code == 200
    assert response.json() == {
        'message': f'Endpoint with TC ID: {tc_id} not found: No row was found when one was required'
    }


def test_delete_endpoint_invalid_data(session: Session, client: TestClient):
    response = client.post(
        '/delete-endpoint/invalid_string/',
    )
    assert response.status_code == 422
    assert (
        response.json()['detail'][0]['msg'] == 'Input should be a valid integer, unable to parse string as an integer'
    )
