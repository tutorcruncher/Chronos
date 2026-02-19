import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import nullslast
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from chronos.db import get_session
from chronos.pydantic_schema import TCDeleteIntegration, TCIntegrations, TCPublicProfileWebhook, TCWebhook
from chronos.sql_models import WebhookEndpoint, WebhookLog
from chronos.utils import settings
from chronos.worker import GLOBAL_BRANCH_ID, dispatch_branch_task, task_send_webhooks

main_router = APIRouter()
security = HTTPBearer()


def check_authorisation(authorisation: HTTPAuthorizationCredentials):
    """
    Checks the authorisation token is correct
    """
    if authorisation.credentials != settings.tc2_shared_key:
        raise HTTPException(status_code=403, detail='Authorisation token is invalid')
    else:
        return True


def _extract_branch_id(webhook_payload: dict) -> int:
    """Extract branch_id from a webhook payload.

    Handles both webhook formats:
    - TCWebhook:               {'events': [{'branch': 123, ...}], ...}
    - TCPublicProfileWebhook:  {'branch_id': 123, ...}
    """
    events = webhook_payload.get('events')
    if events:
        return events[0].get('branch', GLOBAL_BRANCH_ID)
    return webhook_payload.get('branch_id', GLOBAL_BRANCH_ID)


def send_webhooks(
    webhook: TCWebhook | TCPublicProfileWebhook,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    url_extension: str = None,
) -> dict:
    """
    Send webhooks to the relevant endpoints
    """
    assert check_authorisation(authorisation)
    webhook_payload = webhook.model_dump()

    # Start job to send webhooks to endpoints on the workers
    if settings.use_round_robin:
        branch_id = _extract_branch_id(webhook_payload)
        dispatch_branch_task(
            task_send_webhooks,
            routing_branch_id=branch_id,
            payload=json.dumps(webhook_payload),
            url_extension=url_extension,
        )
    else:
        task_send_webhooks.delay(json.dumps(webhook_payload), url_extension)
    return {'message': 'Sending webhooks to endpoints has been successfully initiated.'}


@main_router.post(
    '/send-webhook-callback/{url_extension}',
    description='Receive webhooks from TC and send them to the relevant endpoints',
)
async def send_webhook_with_extension(
    webhook: TCPublicProfileWebhook,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    url_extension: str = None,
) -> dict:
    """
    Receive webhook payloads from TC and send them out to the relevant other endpoints
    :param webhook: TCWebhook object, the payload from TC2 using pydantic validation to create the WebhookLog object
    :param authorisation: auth token to check the request is from TC
    :param url_extension: The extension to the URL to send the webhooks to
    :return:
    """
    return send_webhooks(webhook, authorisation, url_extension)


@main_router.post(
    '/send-webhook-callback',
    description='Receive webhooks from TC and send them to the relevant endpoints',
)
async def send_webhook(
    webhook: TCWebhook,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> dict:
    """
    Receive webhook payloads from TC and send them out to the relevant other endpoints
    :param webhook: TCWebhook object, the payload from TC2 using pydantic validation to create the WebhookLog object
    :param authorisation: auth token to check the request is from TC
    :return:
    """
    return send_webhooks(webhook, authorisation)


@main_router.post(
    '/create-update-callback', description='Receive webhooks from TC and create or update endpoints in Chronos'
)
async def create_update_endpoint(
    integration_list: TCIntegrations,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Session = Depends(get_session),
) -> dict:
    """
    Receive a payload of data that describes an end point and either create or update that end point in Chronos
    :param integration_list: TCIntegration object(s), the payload from TC2 using pydantic validation to create the WebhookEndpoint
                            object
    :param authorisation: auth token to check the request is from TC
    :param db: Session object for the database
    :return:
    """
    assert check_authorisation(authorisation)

    created, updated = [], []

    for integration in integration_list.integrations:
        # Check if the endpoint already exists, if it does then we update it. Otherwise, we create a new one.
        webhook_payload = integration.model_dump()
        try:
            endpoint_qs = select(WebhookEndpoint).where(WebhookEndpoint.tc_id == integration.tc_id)
            endpoint = db.exec(endpoint_qs).one()
        except NoResultFound:
            endpoint = WebhookEndpoint(**webhook_payload)
            db.add(endpoint)
            db.commit()
            created.append({'message': f'WebhookEndpoint {endpoint.name} (TC ID: {endpoint.tc_id}) created'})
        else:
            endpoint.sqlmodel_update(integration)
            db.commit()
            updated.append({'message': f'WebhookEndpoint {endpoint.name} (TC ID: {endpoint.tc_id}) updated'})
    return {'created': created, 'updated': updated}


@main_router.post('/delete-endpoint', description='Receive webhooks from TC and delete endpoints in Chronos')
async def delete_endpoint(
    delete_data: TCDeleteIntegration,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Session = Depends(get_session),
):
    """
    Receive a payload of data that describes an end point and delete that end point in Chronos
    :param delete_data: Contains id in TC of the endpoint (TC: Integration) to be deleted and the branch_id and api_key
    :param authorisation: auth token to check the request is from TC
    :param db: Session object for the database
    :return:
    """

    assert check_authorisation(authorisation)
    webhook_payload = delete_data.model_dump()

    # Check the endpoint exists and delete it
    try:
        endpoint_qs = select(WebhookEndpoint).filter_by(**webhook_payload)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'WebhookEndpoint with TC ID: {webhook_payload["tc_id"]} not found: {e}'}

    # Have to delete existing hooks before deleting the endpoint
    webhooks_qs = select(WebhookLog).filter_by(webhook_endpoint_id=endpoint.id)
    webhooks = db.exec(webhooks_qs).all()
    for webhook in webhooks:
        db.delete(webhook)
    db.commit()

    db.delete(endpoint)
    db.commit()
    return {'message': f'WebhookEndpoint {endpoint.name} (TC ID: {endpoint.tc_id}) deleted'}


@main_router.get('/{tc_id}/logs/{page}', description='Send logs from Chronos to TC')
async def get_logs(
    tc_id: int,
    page: int,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Session = Depends(get_session),
):
    """
    Receive a payload of data that describes an end point and delete that end point in Chronos
    :param tc_id: ID of integration in TC
    :param page: Page for logs
    :param authorisation: auth token to check the request is from TC
    :param db: Session object for the database
    :return:
    """

    assert check_authorisation(authorisation)

    # Get the endpoint from the TC ID or return an error
    try:
        endpoint_qs = select(WebhookEndpoint).filter_by(tc_id=tc_id)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'WebhookEndpoint with TC ID: {tc_id} not found: {e}', 'logs': [], 'count': 0}

    offset = page * 50

    # Get the Logs and related endpoint
    logs = db.exec(
        select(WebhookLog)
        .where(WebhookLog.webhook_endpoint_id == endpoint.id)
        .order_by(nullslast(WebhookLog.timestamp.desc()))
        .offset(offset)
        .limit(100)
    ).all()
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

    # If another page is available this will show in TC2 without extra slow queries
    # Its a false count that just indicates more pages available
    count = offset + len(list_of_webhooks)
    if count <= offset:
        return {'message': f'No logs found for page: {page}', 'logs': [], 'count': count}

    return {'logs': list_of_webhooks[:50], 'count': count}


@main_router.get('/', description='Index page for Chronos')
async def index():
    return {'Live': True}
