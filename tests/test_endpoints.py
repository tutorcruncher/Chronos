import json
from copy import copy

from fastapi.testclient import TestClient

from sqlmodel import Session

from app.pydantic_schema import TCIntegration
from app.sql_models import Endpoint


DFT_ENDPOINT_DATA_FROM_TC2 = {
    'tc_id': 1,
    'name': 'test_endpoint',
    'branch_id': 99,
    'webhook_url': 'test.com',
    'api_key': 'test',
    'active': True,
}


def test_create_endpoint(session: Session, client: TestClient):
    create_data = DFT_ENDPOINT_DATA_FROM_TC2

    response = client.post(
        '/create-update-endpoint/',
        data=json.dumps(create_data),
    )
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint test_endpoint (TC ID: 1) created'}


def test_update_endpoint(session: Session, client: TestClient):
    endpoint_info = copy(DFT_ENDPOINT_DATA_FROM_TC2)
    ep = Endpoint(**TCIntegration(**endpoint_info).model_dump())
    session.add(ep)
    session.commit()

    endpoint_info['name'] = 'diff name'

    response = client.post(
        '/create-update-endpoint/',
        data=json.dumps(endpoint_info),
    )
    assert response.status_code == 200
    assert response.json() == {'message': f'Endpoint diff name (TC ID: 1) updated'}


def test_update_endpoint_invalid_data(session: Session, client: TestClient):
    endpoint_info = copy(DFT_ENDPOINT_DATA_FROM_TC2)
    endpoint_info['active'] = 50

    response = client.post(
        '/create-update-endpoint/',
        data=json.dumps(endpoint_info),
    )
    assert response.status_code == 422
    assert response.json()['detail'][0]['msg'] == 'Input should be a valid boolean, unable to interpret input'


def test_delete_endpoint(session: Session, client: TestClient):
    ep = Endpoint(**TCIntegration(**DFT_ENDPOINT_DATA_FROM_TC2).model_dump())
    session.add(ep)
    session.commit()

    response = client.post(
        f'/delete-endpoint/{DFT_ENDPOINT_DATA_FROM_TC2["tc_id"]}',
    )
    assert response.status_code == 200
    assert response.json() == {
        'message': f'Endpoint {DFT_ENDPOINT_DATA_FROM_TC2["name"]} (TC ID: {DFT_ENDPOINT_DATA_FROM_TC2["tc_id"]}) deleted'
    }


def test_delete_endpoint_doesnt_exist(session: Session, client: TestClient):
    response = client.post(
        f'/delete-endpoint/{DFT_ENDPOINT_DATA_FROM_TC2["tc_id"]}',
    )
    assert response.status_code == 200
    assert response.json() == {
        'message': f'Endpoint with TC ID: {DFT_ENDPOINT_DATA_FROM_TC2["tc_id"]} not found: No row was found when one was required'
    }


def test_delete_endpoint_invalid_data(session: Session, client: TestClient):
    response = client.post(
        f'/delete-endpoint/invalid_string',
    )
    assert response.status_code == 422
    assert (
        response.json()['detail'][0]['msg'] == 'Input should be a valid integer, unable to parse string as an integer'
    )
