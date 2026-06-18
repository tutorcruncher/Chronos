import json
from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from chronos.db import get_session
from chronos.pydantic_schema import (
    BobbinDeleteIntegration,
    BobbinIntegration,
    BobbinWebhookSend,
    MessageResponse,
    WebhookLogsResponse,
)
from chronos.sql_models import WebhookEndpoint
from chronos.utils import settings
from chronos.views.shared import check_authorisation, security, serialize_logs_response
from chronos.worker import task_delete_endpoint, task_send_webhooks


def verify_bobbin_authorisation(authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)]) -> None:
    """Dependency: authorise every /bobbin/* request against the Bobbin shared key.

    Separate from the TC2 key: a request bearing this key can only ever reach /bobbin/* routes.
    """
    check_authorisation(authorisation, settings.bobbin_shared_key)


# Auth runs as a router-level dependency, so individual routes don't repeat the check.
bobbin_router = APIRouter(prefix='/bobbin', dependencies=[Depends(verify_bobbin_authorisation)])


def _get_bobbin_endpoint(db: Session, organization_id: int, bobbin_endpoint_id: int) -> WebhookEndpoint:
    """Resolve a Bobbin endpoint, scoped on (org, bobbin id). Raises NoResultFound if absent."""
    endpoint_qs = select(WebhookEndpoint).where(
        WebhookEndpoint.org_id == organization_id,
        WebhookEndpoint.bobbin_id == bobbin_endpoint_id,
    )
    return db.exec(endpoint_qs).one()


@bobbin_router.post(
    '/create-update-endpoint',
    description='Create or update a Bobbin webhook endpoint in Chronos',
    response_model=MessageResponse,
)
async def create_update_bobbin_endpoint(
    integration: BobbinIntegration,
    db: Session = Depends(get_session),
) -> dict:
    """Upsert a single Bobbin endpoint, keyed on (organization_id, bobbin_endpoint_id)."""
    endpoint_fields = integration.to_endpoint_fields()
    try:
        endpoint = _get_bobbin_endpoint(db, integration.organization_id, integration.bobbin_endpoint_id)
    except NoResultFound:
        endpoint = WebhookEndpoint(**endpoint_fields)
        db.add(endpoint)
        db.commit()
        return {'message': f'WebhookEndpoint {endpoint.name} (org: {endpoint.org_id}) created'}
    else:
        endpoint.sqlmodel_update(endpoint_fields)
        db.commit()
        return {'message': f'WebhookEndpoint {endpoint.name} (org: {endpoint.org_id}) updated'}


@bobbin_router.post(
    '/delete-endpoint', description='Delete a Bobbin webhook endpoint in Chronos', response_model=MessageResponse
)
async def delete_bobbin_endpoint(
    delete_data: BobbinDeleteIntegration,
    db: Session = Depends(get_session),
) -> dict:
    """Deactivate the endpoint immediately, then clean up its logs asynchronously."""
    try:
        endpoint = _get_bobbin_endpoint(db, delete_data.organization_id, delete_data.bobbin_endpoint_id)
    except NoResultFound as e:
        return {
            'message': f'WebhookEndpoint {delete_data.bobbin_endpoint_id} (org: {delete_data.organization_id}) not found: {e}'
        }

    endpoint.active = False
    db.commit()

    task_delete_endpoint.delay(endpoint.id)
    return {'message': f'WebhookEndpoint {endpoint.name} (org: {endpoint.org_id}) deletion initiated'}


@bobbin_router.post(
    '/send-webhook',
    description='Receive a Bobbin event and fan it out to the org endpoints',
    response_model=MessageResponse,
)
async def send_bobbin_webhook(webhook: BobbinWebhookSend) -> dict:
    """Queue a Bobbin event for delivery via the shared send task (no round-robin)."""
    task_send_webhooks.delay(json.dumps(webhook.model_dump()))
    return {'message': 'Sending bobbin webhook to endpoints has been successfully initiated.'}


@bobbin_router.get(
    '/{organization_id}/{bobbin_endpoint_id}/logs/{page}',
    description='Send Bobbin webhook logs to bobbin-api',
    response_model=WebhookLogsResponse,
)
async def get_bobbin_logs(
    organization_id: int,
    bobbin_endpoint_id: int,
    page: int,
    db: Session = Depends(get_session),
) -> dict:
    """Return a page of delivery logs for an endpoint, double-scoped on org AND endpoint id."""
    try:
        endpoint = _get_bobbin_endpoint(db, organization_id, bobbin_endpoint_id)
    except NoResultFound as e:
        return {
            'message': f'WebhookEndpoint {bobbin_endpoint_id} (org: {organization_id}) not found: {e}',
            'logs': [],
            'count': 0,
        }

    return serialize_logs_response(db, endpoint, page)
