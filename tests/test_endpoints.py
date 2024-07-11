import hashlib
import hmac
import json
from copy import copy

from fastapi.testclient import TestClient

from sqlmodel import Session

from app.pydantic_schema import TCIntegration
from app.sql_models import Endpoint

from app.main import app
from app.utils import settings
from tests.test_helpers import _get_headers

DFT_ENDPOINT_DATA_FROM_TC2 = {
    'tc_id': 1,
    'name': 'test_endpoint',
    'branch_id': 99,
    'webhook_url': 'test.com',
    'api_key': 'test',
    'active': True,
}


create_update_url = app.url_path_for('create_update_endpoint')


def test_create_endpoint(session: Session, client: TestClient):
    payload = copy(DFT_ENDPOINT_DATA_FROM_TC2)
    headers = _get_headers(payload)
    response = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint test_endpoint (TC ID: 1) created'}


def test_update_endpoint(session: Session, client: TestClient):
    payload = copy(DFT_ENDPOINT_DATA_FROM_TC2)
    ep = Endpoint(**TCIntegration(**payload).model_dump())
    session.add(ep)
    session.commit()

    payload['name'] = 'diff name'

    headers = _get_headers(payload)
    response = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint diff name (TC ID: 1) updated'}


def test_update_endpoint_invalid_data(session: Session, client: TestClient):
    payload = copy(DFT_ENDPOINT_DATA_FROM_TC2)
    payload['active'] = 50

    headers = _get_headers(payload)
    response = client.post(
        create_update_url,
        data=json.dumps(payload),
        headers=headers,
    )
    assert response.status_code == 422
    assert response.json()['detail'][0]['msg'] == 'Input should be a valid boolean, unable to interpret input'


def test_delete_endpoint(session: Session, client: TestClient):
    ep = Endpoint(**TCIntegration(**DFT_ENDPOINT_DATA_FROM_TC2).model_dump())
    session.add(ep)
    session.commit()

    tc_id = DFT_ENDPOINT_DATA_FROM_TC2['tc_id']
    url = app.url_path_for('delete_endpoint', endpoint_tc_id=tc_id)
    response = client.post(url)
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint {DFT_ENDPOINT_DATA_FROM_TC2["name"]} (TC ID: {tc_id}) deleted'}


def test_delete_endpoint_doesnt_exist(session: Session, client: TestClient):
    tc_id = DFT_ENDPOINT_DATA_FROM_TC2['tc_id']
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
