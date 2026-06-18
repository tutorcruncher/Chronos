import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import nullslast
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from chronos.db import get_session
from chronos.pydantic_schema import BobbinDeleteIntegration, BobbinIntegrations, BobbinWebhookSend
from chronos.sql_models import BobbinWebhookEndpoint, BobbinWebhookLog
from chronos.utils import settings
from chronos.worker import task_delete_bobbin_endpoint, task_send_bobbin_webhooks

bobbin_router = APIRouter(prefix='/bobbin')
security = HTTPBearer()


def check_bobbin_authorisation(authorisation: HTTPAuthorizationCredentials) -> bool:
    """Authorise a request against the Bobbin shared key.

    Separate from the TC2 key: a request bearing this key can only ever reach /bobbin/* routes,
    which only ever touch the Bobbin tables.
    """
    if authorisation.credentials != settings.bobbin_shared_key:
        raise HTTPException(status_code=403, detail='Authorisation token is invalid')
    return True


@bobbin_router.post('/create-update-endpoint', description='Create or update Bobbin webhook endpoints in Chronos')
async def create_update_bobbin_endpoint(
    integration_list: BobbinIntegrations,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Session = Depends(get_session),
) -> dict:
    """Upsert one or more Bobbin endpoints, keyed on (organization_id, bobbin_endpoint_id)."""
    assert check_bobbin_authorisation(authorisation)

    created, updated = [], []
    for integration in integration_list.integrations:
        endpoint_qs = select(BobbinWebhookEndpoint).where(
            BobbinWebhookEndpoint.organization_id == integration.organization_id,
            BobbinWebhookEndpoint.bobbin_endpoint_id == integration.bobbin_endpoint_id,
        )
        try:
            endpoint = db.exec(endpoint_qs).one()
        except NoResultFound:
            endpoint = BobbinWebhookEndpoint(**integration.model_dump())
            db.add(endpoint)
            db.commit()
            created.append(
                {'message': f'BobbinWebhookEndpoint {endpoint.name} (org: {endpoint.organization_id}) created'}
            )
        else:
            endpoint.sqlmodel_update(integration)
            db.commit()
            updated.append(
                {'message': f'BobbinWebhookEndpoint {endpoint.name} (org: {endpoint.organization_id}) updated'}
            )
    return {'created': created, 'updated': updated}


@bobbin_router.post('/delete-endpoint', description='Delete a Bobbin webhook endpoint in Chronos')
async def delete_bobbin_endpoint(
    delete_data: BobbinDeleteIntegration,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Session = Depends(get_session),
) -> dict:
    """Deactivate the endpoint immediately, then clean up its logs asynchronously."""
    assert check_bobbin_authorisation(authorisation)

    endpoint_qs = select(BobbinWebhookEndpoint).where(
        BobbinWebhookEndpoint.organization_id == delete_data.organization_id,
        BobbinWebhookEndpoint.bobbin_endpoint_id == delete_data.bobbin_endpoint_id,
    )
    try:
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {
            'message': f'BobbinWebhookEndpoint {delete_data.bobbin_endpoint_id} (org: {delete_data.organization_id}) not found: {e}'
        }

    endpoint.active = False
    db.commit()

    task_delete_bobbin_endpoint.delay(endpoint.id)
    return {'message': f'BobbinWebhookEndpoint {endpoint.name} (org: {endpoint.organization_id}) deletion initiated'}


@bobbin_router.post('/send-webhook', description='Receive a Bobbin event and fan it out to the org endpoints')
async def send_bobbin_webhook(
    webhook: BobbinWebhookSend,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> dict:
    """Queue a Bobbin event for delivery. No round-robin — a plain per-event Celery task."""
    assert check_bobbin_authorisation(authorisation)
    task_send_bobbin_webhooks.delay(json.dumps(webhook.model_dump()), webhook.organization_id)
    return {'message': 'Sending bobbin webhook to endpoints has been successfully initiated.'}


@bobbin_router.get(
    '/{organization_id}/{bobbin_endpoint_id}/logs/{page}', description='Send Bobbin webhook logs to bobbin-api'
)
async def get_bobbin_logs(
    organization_id: int,
    bobbin_endpoint_id: int,
    page: int,
    authorisation: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Session = Depends(get_session),
):
    """Return a page of delivery logs for an endpoint, double-scoped on org AND endpoint id."""
    assert check_bobbin_authorisation(authorisation)

    endpoint_qs = select(BobbinWebhookEndpoint).where(
        BobbinWebhookEndpoint.organization_id == organization_id,
        BobbinWebhookEndpoint.bobbin_endpoint_id == bobbin_endpoint_id,
    )
    try:
        endpoint = db.exec(endpoint_qs).one()
    except NoResultFound as e:
        return {
            'message': f'BobbinWebhookEndpoint {bobbin_endpoint_id} (org: {organization_id}) not found: {e}',
            'logs': [],
            'count': 0,
        }

    offset = page * 50
    logs = db.exec(
        select(BobbinWebhookLog)
        .where(BobbinWebhookLog.bobbin_webhook_endpoint_id == endpoint.id)
        .order_by(nullslast(BobbinWebhookLog.timestamp.desc()))
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

    count = offset + len(list_of_webhooks)
    if count <= offset:
        return {'message': f'No logs found for page: {page}', 'logs': [], 'count': count}

    return {'logs': list_of_webhooks[:50], 'count': count}
