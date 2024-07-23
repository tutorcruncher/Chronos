import hashlib
import hmac
import json
from typing import Annotated

import requests
from fastapi import APIRouter, Depends, Header, Request
from sqlalchemy import func
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from app.db import get_session
from app.pydantic_schema import TCIntegration, TCWebhook
from app.sql_models import Endpoint, WebhookLog
from app.utils import settings
from app.worker import send_webhooks

main_router = APIRouter()
session = requests.Session()


def check_headers(webhook_payload: dict, user_agent: str, webhook_signature: str) -> bool:
    json_payload = json.dumps(webhook_payload).encode()

    m = hmac.new(settings.tc2_shared_key.encode(), json_payload, hashlib.sha256)
    return m.hexdigest() == webhook_signature and user_agent == 'TutorCruncher'


@main_router.post('/send-webhook/', description='Receive webhooks from TC and send them to the relevant endpoints')
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

    webhook_payload = webhook.model_dump()
    assert check_headers(webhook_payload, user_agent, webhook_signature)

    send_webhooks.delay(json.dumps(webhook_payload))
    return {'message': 'Sending webhooks to endpoints has been successfully initiated.'}


@main_router.post(
    '/create-update-endpoint/', description='Receive webhooks from TC and create or update endpoints in Chronos'
)
async def create_update_endpoint(
    endpoint_info: TCIntegration,
    user_agent: Annotated[str | None, Header()] = None,
    webhook_signature: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_session),
) -> dict:
    """
    Receive a payload of data that describes an end point and either create or update that end point in Chronos
    :param endpoint_info: TCIntegration object, the payload from TC2 using pydantic validation to create the Endpoint
                            object
    :param user_agent: Header defined in TC to identify the sender
    :param webhook_signature: Header defined in TC generated by hashing the payload with the shared key
    :param db: Session object for the database
    :return:
    """
    webhook_payload = endpoint_info.model_dump()
    assert check_headers(webhook_payload, user_agent, webhook_signature)

    try:
        endpoint_qs = select(Endpoint).where(Endpoint.tc_id == endpoint_info.tc_id)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound:
        endpoint = Endpoint(**webhook_payload)
        db.add(endpoint)
        db.commit()
        return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) created'}
    else:
        endpoint.sqlmodel_update(endpoint_info)
        db.commit()
        return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) updated'}


@main_router.post(
    '/delete-endpoint/{endpoint_tc_id}/', description='Receive webhooks from TC and delete endpoints in Chronos'
)
async def delete_endpoint(endpoint_tc_id: int, db: Session = Depends(get_session)):
    """
    Receive a payload of data that describes an end point and delete that end point in Chronos
    :param endpoint_tc_id: The id in TC of the endpoint (TC: Integration) to be deleted
    :param db: Session object for the database
    :return:
    """
    # Should change this to pass a dict for more security so we can sign it
    # webhook_payload = endpoint_info.model_dump()
    # assert check_headers(webhook_payload, user_agent, webhook_signature)

    try:
        endpoint_qs = select(Endpoint).where(Endpoint.tc_id == endpoint_tc_id)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'Endpoint with TC ID: {endpoint_tc_id} not found: {e}'}
    db.delete(endpoint)
    db.commit()
    return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) deleted'}


@main_router.get('/get-logs/{endpoint_id}/{page}/', description='Send logs from Chronos to TC')
async def get_logs(endpoint_id: int, page: int = 0, db: Session = Depends(get_session)):
    # Use the api key here to authenticate the request
    endpoint = db.exec(select(Endpoint).where(Endpoint.tc_id == endpoint_id)).one()

    count_stmt = select(func.count(WebhookLog.id)).where(WebhookLog.endpoint_id == endpoint.id)
    count = db.exec(count_stmt).one()

    offset = page * 50
    if count < offset:
        return {'logs': [], 'count': count}

    statement = (
        select(WebhookLog, Endpoint)
        .where(WebhookLog.endpoint_id == endpoint.id, WebhookLog.endpoint_id == Endpoint.id)
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
        for log, endpoint in logs
    ]
    return {'logs': list_of_webhooks, 'count': count}


@main_router.get('/test/', description='Send logs from Chronos to TC')
async def test(db: Session = Depends(get_session)):
    return {'message': 'Successfully received webhook from TutorCruncher.'}


@main_router.post('/temp-receive-webhook/', description='testing')
async def ss(request: Request, db: Session = Depends(get_session)):
    return {'message': 'Successfully received webhook from TutorCruncher.'}
