import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from chronos.db import get_session
from chronos.pydantic_schema import TCDeleteIntegration, TCIntegrations, TCPublicProfileWebhook, TCWebhook
from chronos.sql_models import WebhookEndpoint
from chronos.utils import settings
from chronos.views.shared import check_authorisation, security, serialize_logs_response
from chronos.worker import GLOBAL_BRANCH_ID, dispatch_branch_task, task_delete_endpoint, task_send_webhooks

main_router = APIRouter()


def check_tc_authorisation(authorisation: HTTPAuthorizationCredentials) -> bool:
    """Checks the authorisation token is correct"""
    return check_authorisation(authorisation, settings.tc2_shared_key)


def _extract_branch_id(webhook_payload: dict) -> int:
    """Extract branch_id from a webhook payload.

    Handles both webhook formats:
    - TCWebhook:               {'events': [{'branch': 123, ...}], ...}
    - TCPublicProfileWebhook:  {'branch_id': 123, ...}

    Raises HTTPException 422 if the branch value is present but not a valid integer.
    """
    events = webhook_payload.get('events')
    if events:
        value = events[0].get('branch')
    else:
        value = webhook_payload.get('branch_id')

    if value is None:
        return GLOBAL_BRANCH_ID

    if isinstance(value, bool):
        # because int type casting is compatible with bool
        raise HTTPException(status_code=422, detail=f'Invalid branch_id: {value!r}')

    try:
        return int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f'Invalid branch_id: {value!r}')


def send_webhooks(
    webhook: TCWebhook | TCPublicProfileWebhook,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    url_extension: str = None,
) -> dict:
    """
    Send webhooks to the relevant endpoints
    """
    assert check_tc_authorisation(authorisation)
    webhook_payload = webhook.model_dump()

    # Start job to send webhooks to endpoints on the workers
    if settings.use_round_robin:
        branch_id = _extract_branch_id(webhook_payload)
        # we wouldn't want to import the job_queue singleton in view code
        # so we use a wrapper around the job_queue.enqueue(...) that lives in
        # the worker file
        dispatch_branch_task(
            task_send_webhooks,
            branch_id=branch_id,
            payload=webhook_payload,
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
    assert check_tc_authorisation(authorisation)

    created, updated = [], []

    for integration in integration_list.integrations:
        # Check if the endpoint already exists, if it does then we update it. Otherwise, we create a new one.
        endpoint_fields = integration.to_endpoint_fields()
        try:
            endpoint_qs = select(WebhookEndpoint).where(WebhookEndpoint.tc_id == integration.tc_id)
            endpoint = db.exec(endpoint_qs).one()
        except NoResultFound:
            endpoint = WebhookEndpoint(**endpoint_fields)
            db.add(endpoint)
            db.commit()
            created.append({'message': f'WebhookEndpoint {endpoint.name} (TC ID: {endpoint.tc_id}) created'})
        else:
            endpoint.sqlmodel_update(endpoint_fields)
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

    assert check_tc_authorisation(authorisation)

    # Check the endpoint exists and delete it (delete_data carries the TC2 wire branch_id -> org_id).
    try:
        endpoint_qs = select(WebhookEndpoint).where(
            WebhookEndpoint.tc_id == delete_data.tc_id,
            WebhookEndpoint.org_id == delete_data.branch_id,
        )
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'WebhookEndpoint with TC ID: {delete_data.tc_id} not found: {e}'}

    # Deactivate immediately so no new sends pick this endpoint while cleanup runs.
    endpoint.active = False
    db.commit()

    task_delete_endpoint.delay(endpoint.id)
    return {'message': f'WebhookEndpoint {endpoint.name} (TC ID: {endpoint.tc_id}) deletion initiated'}


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

    assert check_tc_authorisation(authorisation)

    # Get the endpoint from the TC ID or return an error
    try:
        endpoint_qs = select(WebhookEndpoint).filter_by(tc_id=tc_id)
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {'message': f'WebhookEndpoint with TC ID: {tc_id} not found: {e}', 'logs': [], 'count': 0}

    return serialize_logs_response(db, endpoint, page)


@main_router.get('/', description='Index page for Chronos')
async def index():
    return {'Live': True}
