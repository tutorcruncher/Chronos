import hashlib
import hmac
import json
from typing import Annotated

import requests
from fastapi import APIRouter, Depends, Header
from sqlalchemy import func
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from chronos.db import get_session
from chronos.pydantic_schema import TCDeleteIntegration, TCGetWebhooks, TCIntegration, TCWebhook
from chronos.sql_models import Endpoint, WebhookLog
from chronos.utils import settings
from chronos.worker import send_webhooks

main_router = APIRouter()
session = requests.Session()


def check_headers(webhook_payload: dict, user_agent: str, webhook_signature: str) -> bool:
    """
    Check the headers of the request to ensure it is from TutorCruncher
    """
    json_payload = json.dumps(webhook_payload).encode()

    m = hmac.new(settings.tc2_shared_key.encode(), json_payload, hashlib.sha256)
    return m.hexdigest() == webhook_signature and user_agent == 'TutorCruncher'


@main_router.post(
    '/send-webhook-callback/', description='Receive webhooks from TC and send them to the relevant endpoints'
)
async def send_webhook(
    webhook: TCWebhook,
    user_agent: Annotated[str | None, Header()] = None,
    webhook_signature: Annotated[str | None, Header()] = None,
) -> dict:
    """
    Receive webhook payloads from TC and send them out to the relevant other endpoints
    :param webhook: TCWebhook object, the payload from TC2 using pydantic validation to create the WebhookLog object
    :param user_agent: Header defined in TC to identify the sender
    :param webhook_signature: Header defined in TC generated by hashing the payload with the shared key
    :return:
    """

    # Check the headers to ensure the request is from TutorCruncher
    webhook_payload = webhook.model_dump()
    assert check_headers(webhook_payload, user_agent, webhook_signature)

    # Start job to send webhooks to endpoints on the workers
    send_webhooks.delay(json.dumps(webhook_payload))
    return {'message': 'Sending webhooks to endpoints has been successfully initiated.'}


@main_router.post(
    '/create-update-callback/', description='Receive webhooks from TC and create or update endpoints in Chronos'
)
async def create_update_endpoint(
    integration: TCIntegration,
    user_agent: Annotated[str | None, Header()] = None,
    webhook_signature: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_session),
) -> dict:
    """
    Receive a payload of data that describes an end point and either create or update that end point in Chronos
    :param integration: TCIntegration object, the payload from TC2 using pydantic validation to create the Endpoint
                            object
    :param user_agent: Header defined in TC to identify the sender
    :param webhook_signature: Header defined in TC generated by hashing the payload with the shared key
    :param db: Session object for the database
    :return:
    """

    # Check the headers to ensure the request is from TutorCruncher
    webhook_payload = integration.model_dump()
    assert check_headers(webhook_payload, user_agent, webhook_signature)

    # Check if the endpoint already exists, if it does then we update it. Otherwise, we create a new one.
    try:
        endpoint_qs = select(Endpoint).where(Endpoint.tc_id == integration.tc_id)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound:
        endpoint = Endpoint(**webhook_payload)
        db.add(endpoint)
        db.commit()
        return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) created'}
    else:
        endpoint.sqlmodel_update(integration)
        db.commit()
        return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) updated'}


@main_router.post('/delete-callback/', description='Receive webhooks from TC and delete endpoints in Chronos')
async def delete_endpoint(
    delete_data: TCDeleteIntegration,
    user_agent: Annotated[str | None, Header()] = None,
    webhook_signature: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_session),
):
    """
    Receive a payload of data that describes an end point and delete that end point in Chronos
    :param delete_data: Contains id in TC of the endpoint (TC: Integration) to be deleted and the branch_id and api_key
    :param user_agent: Header defined in TC to identify the sender
    :param webhook_signature: Header defined in TC generated by hashing the payload with the shared key
    :param db: Session object for the database
    :return:
    """

    # Check the headers to ensure the request is from TutorCruncher
    webhook_payload = delete_data.model_dump()
    assert check_headers(webhook_payload, user_agent, webhook_signature)

    # Check the endpoint exists and delete it
    try:
        endpoint_qs = select(Endpoint).filter_by(**webhook_payload)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'Endpoint with TC ID: {webhook_payload["tc_id"]} not found: {e}'}
    db.delete(endpoint)
    db.commit()
    return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) deleted'}


@main_router.post('/logs-callback/', description='Send logs from Chronos to TC')
async def get_logs(
    webhooks: TCGetWebhooks,
    user_agent: Annotated[str | None, Header()] = None,
    webhook_signature: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_session),
):
    """
    Receive a payload of data that describes an end point and delete that end point in Chronos
    :param webhooks: Contains webhook id in TC, branch_id and the page number
    :param user_agent: Header defined in TC to identify the sender
    :param webhook_signature: Header defined in TC generated by hashing the payload with the shared key
    :param db: Session object for the database
    :return:
    """

    # Check the headers to ensure the request is from TutorCruncher
    webhook_payload = webhooks.model_dump()
    assert check_headers(webhook_payload, user_agent, webhook_signature)

    page = webhook_payload.pop('page')

    # Get the endpoint from the TC ID or return an error
    try:
        endpoint_qs = select(Endpoint).filter_by(**webhook_payload)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'Endpoint with TC ID: {webhook_payload["tc_id"]} not found: {e}'}

    # Get the total count of logs for the relevant endpoint
    count_stmt = select(func.count(WebhookLog.id)).where(WebhookLog.endpoint_id == endpoint.id)
    count = db.exec(count_stmt).one()

    offset = page * 50
    if count <= offset:
        return {'message': f'No logs found for page: {page}', 'logs': [], 'count': count}

    # Get the Logs and related endpoint
    statement = (
        select(WebhookLog)
        .where(WebhookLog.endpoint_id == endpoint.id)
        .order_by(WebhookLog.timestamp.desc())
        .offset(offset)
        .limit(50)
    )
    results = db.exec(statement)
    logs = results.all()
    list_of_webhooks = [
        {
            'request_headers': json.loads(log.request_headers),
            'request_body': json.loads(log.request_body),
            'response_headers': json.loads(log.response_headers),
            'response_body': json.loads(log.response_body),
            'status': log.status,
            'status_code': log.status_code,
            'timestamp': log.timestamp,
            'url': endpoint.webhook_url,
        }
        for log in logs
    ]
    return {'logs': list_of_webhooks, 'count': count}
