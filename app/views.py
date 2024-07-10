import hashlib
import hmac
import json

import requests
from fastapi import APIRouter, Depends, Request
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select
from app.db import get_session
from app.pydantic_schema import TCIntegration
from app.utils import settings

from app.worker import send_webhooks
from app.sql_models import Endpoint, WebhookLog

main_router = APIRouter()
session = requests.Session()


@main_router.post('/send-webhook/', name='Receive webhooks from TC and send them to the relevant endpoints')
async def send_webhook(request: Request) -> dict:
    """
    Receive webhook payloads from TC and send them out to the relevant other endpoints
    :param webhook_payload: TCWebhook object, the payload from TC2 using pydantic validation to create the WebhookLog object
    :param db: Session object for the database
    :return: A confirmation dict?
    """
    debug(request.body())
    debug(request.headers())
    webhook_payload = await request.body()
    m = hmac.new(settings.tc2_shared_key.encode(), webhook_payload, hashlib.sha256)
    assert m.hexdigest() == request.headers['Webhook-Signature']

    json_payload = json.loads(webhook_payload)
    send_webhooks.delay(json_payload)
    # return {'message': f'{total_success} webhooks sent to branch {branch_id}. {total_failed} failed.'}
    return {'message': 'ree'}


@main_router.post('/create-update-endpoint/', name='Receive webhooks from TC and create or update endpoints in Chronos')
async def create_update_endpoint(endpoint_info: TCIntegration, db: Session = Depends(get_session)):
    """
    Receive a payload of data that describes an end point and either create or update that end point in Chronos
    :param endpoint_info: TCIntegration object, the payload from TC2 using pydantic validation to create the Endpoint object
    :param db: Session object for the database
    :return:
    """
    try:
        endpoint_qs = select(Endpoint).where(Endpoint.tc_id == endpoint_info.tc_id)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound:
        endpoint = Endpoint(**endpoint_info.model_dump())
        db.add(endpoint)
        db.commit()
        return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) created'}
    else:
        endpoint.sqlmodel_update(endpoint_info)
        db.commit()
        return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) updated'}


@main_router.post('/delete-endpoint/{endpoint_tc_id}/', name='Receive webhooks from TC and delete endpoints in Chronos')
async def delete_endpoint(endpoint_tc_id: int, db: Session = Depends(get_session)):
    """
    Receive a payload of data that describes an end point and delete that end point in Chronos
    :param endpoint_tc_id: The id in TC of the endpoint (TC: Integration) to be deleted
    :param db: Session object for the database
    :return:
    """
    try:
        endpoint_qs = select(Endpoint).where(Endpoint.tc_id == endpoint_tc_id)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'Endpoint with TC ID: {endpoint_tc_id} not found: {e}'}
    db.delete(endpoint)
    db.commit()
    return {'message': f'Endpoint {endpoint.name} (TC ID: {endpoint.tc_id}) deleted'}


@main_router.get('/get-logs/{endpoint_id}/{page}/', name='Send logs from Chronos to TC')
async def get_logs(endpoint_id: int, page: int = 1, db: Session = Depends(get_session)):
    # Take integration id as an argument and return logs for that integration

    endpoint = db.exec(select(Endpoint).where(Endpoint.tc_id == endpoint_id)).one()

    offset = (page) * 50
    statement = (
        select(WebhookLog, Endpoint)
        .where(WebhookLog.endpoint_id == endpoint.id, WebhookLog.endpoint_id == Endpoint.id)
        .order_by(WebhookLog.timestamp.desc())
        .offset(offset)
        .limit(50)
    )  # need to work out ordering
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
    return {'logs': list_of_webhooks, 'count': len(list_of_webhooks)}
