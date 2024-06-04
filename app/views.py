import logfire
import requests
from fastapi import APIRouter, Depends
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from app.pydantic_models import PydanticEndpoint, PydanticWebhook
from app.utils import app_logger

from app.main import engine
from app.sql_models import Endpoint, WebhookLog

main_router = APIRouter()
session = requests.Session()


def get_db_session():
    with Session(engine) as db_session:
        yield db_session


async def webhook_request(api_key: str, url: str, *, method: str = 'GET', data: dict = None) -> dict:
    headers = {'Authorization': f'token {api_key}', 'Content-Type': 'application/json'}
    logfire.debug('TutorCruncher request to url: {url=}: {data=}', url=url, data=data)
    with logfire.span('{method} {url!r}', url=url, method=method):
        r = session.request(method=method, url=url, json=data, headers=headers)
    app_logger.info('Request method=%s url=%s status_code=%s', method, url, r.status_code, extra={'data': data})
    r.raise_for_status()
    return r.json()


@main_router.post('/send-webhook/', name='Receive webhooks from TC and send them to the relevant endpoints')
async def send_webhook(webhook_payload: PydanticWebhook):
    # Receive webhook payloads from TC and send them out to the relevant other endpoints

    # Get branch_id from payload. This will be wrong for now
    branch_id = webhook_payload.branch_id
    with Session(engine) as db_session:
        endpoints_query = select(Endpoint).where(Endpoint.branch_id == branch_id)
        endpoints = db_session.exec(endpoints_query).all()

    for endpoint in endpoints:
        # Send webhook to endpoint
        pass
    pass


@main_router.post('/create-update-endpoint/', name='Receive webhooks from TC and create or update endpoints in Chronos')
async def create_update_endpoint(endpoint_dict: PydanticEndpoint, db_session: Session = Depends(get_db_session)):
    # Receive a payload of data that describes an end point and either create or update that end point in Chronos
    try:
        endpoint = select(Endpoint).where(Endpoint.tc_id == endpoint_dict['tc_id']).one()
    except NoResultFound:
        endpoint = Endpoint(**endpoint_dict.dict())
        db_session.add(endpoint)
        db_session.commit()
        return {'message': f'Endpoint {endpoint.name}{endpoint.tc_id} created'}
    else:
        endpoint.sqlmodel_update(endpoint_dict)
        db_session.commit()
        return {'message': f'Endpoint {endpoint.name}{endpoint.tc_id} updated'}


@main_router.post('/delete-endpoint/', name='Receive webhooks from TC and delete endpoints in Chronos')
async def delete_endpoint(
    endpoint_id: int, db_session: Session = Depends(get_db_session)
):  # Don't think this will be the best option. May need to store id in TC
    # Take integration id as an argument and delete the endpoint for that integration
    try:
        endpoint = db_session.get(Endpoint, endpoint_id)
    except NoResultFound as e:
        return {'message': f'Endpoint {endpoint_id} not found: {e}'}
    db_session.delete(endpoint)
    db_session.commit()
    return {'message': f'Endpoint {endpoint.name}{endpoint_id} deleted'}


@main_router.post('/get-logs/', name='Send logs from Chronos to TC')
async def get_logs(endpoint_id: int, page: int = 1, db_session: Session = Depends(get_db_session)):
    # Take integration id as an argument and return logs for that integration
    offset = (page - 1) * 50
    statement = (
        select(WebhookLog).order_by(WebhookLog.created_at.desc()).offset(offset).limit(50)
    )  # need to work out ordering
    results = db_session.exec(statement)
    logs = results.all()

    # TODO: Send logs to TC as dicts/JSON dump
    return logs
