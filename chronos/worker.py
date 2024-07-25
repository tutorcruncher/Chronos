import hashlib
import hmac
import json
from datetime import datetime, timedelta

import requests
from celery.app import Celery
from fastapi_utilities import repeat_at
from sqlalchemy import delete
from sqlmodel import Session, col, select

from chronos.db import engine, get_session
from chronos.sql_models import Endpoint, WebhookLog
from chronos.utils import app_logger

session = requests.Session()

celery_app = Celery(__name__, broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')


def webhook_request(url: str, *, method: str = 'POST', webhook_sig: str, data: dict = None):
    """
    Send a request to TutorCruncher
    :param url: The endpoint supplied by clients when creating an integration in TC2
    :param method: We should always be sending POST requests as we are sending data to the endpoints
    :param data: The Webhook data supplied by TC2
    :return: Endpoint response
    """
    from chronos.main import logfire

    headers = {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'webhook-signature': webhook_sig,
    }
    logfire.debug('TutorCruncher request to url: {url=}: {data=}', url=url, data=data)
    with logfire.span('{method} {url!r}', url=url, method=method):
        r = session.request(method=method, url=url, json=data, headers=headers)
    app_logger.info('Request method=%s url=%s status_code=%s', method, url, r.status_code, extra={'data': data})
    # r.raise_for_status()
    return r


@celery_app.task
def send_webhooks(
    payload: str,
):
    from chronos.main import logfire

    loaded_payload = json.loads(payload)
    loaded_payload['_request_time'] = loaded_payload.pop('request_time')
    branch_id = loaded_payload['events'][0]['branch']
    total_success, total_failed = 0, 0

    with logfire.span(f'Send webhooks on branch: {branch_id}'):
        with Session(engine) as db:
            endpoints_query = select(Endpoint).where(Endpoint.branch_id == branch_id)
            endpoints = db.exec(endpoints_query).all()

            for endpoint in endpoints:
                webhook_sig = hmac.new(endpoint.api_key.encode(), json.dumps(loaded_payload).encode(), hashlib.sha256)
                sig_hex = webhook_sig.hexdigest()
                response = webhook_request(endpoint.webhook_url, webhook_sig=sig_hex, data=loaded_payload)
                if response.status_code in {200, 201, 202, 204}:
                    status = 'Success'
                    total_success += 1
                else:
                    status = 'Unexpected response'
                    total_failed += 1
                webhooklog = WebhookLog(
                    endpoint_id=endpoint.id,
                    request_headers=json.dumps(dict(response.request.headers)),
                    request_body=json.dumps((response.request.body.decode())),
                    response_headers=json.dumps(dict(response.headers)),
                    response_body=json.dumps(response.content.decode()),
                    status=status,
                    status_code=response.status_code,
                )
                db.add(webhooklog)
            db.commit()
        app_logger.info(
            '%s Webhooks sent for branch %s. Total Sent: %s. Total failed: %s',
            total_success + total_failed,
            branch_id,
            total_success,
            total_failed,
        )


@repeat_at(cron='0 0 * * *')
async def delete_old_logs_job():
    """
    We run cron job at midnight every day that wipes all WebhookLogs older than 15 days
    """
    _delete_old_logs_job.delay(get_session())


@celery_app.task
def _delete_old_logs_job():
    with Session(engine) as db:
        statement = select(WebhookLog).where(WebhookLog.timestamp > datetime.utcnow() - timedelta(days=15))
        results = db.exec(statement).all()

        delete_statement = delete(WebhookLog).where(col(WebhookLog.id).in_([whl.id for whl in results]))
        db.exec(delete_statement)
        db.commit()
        # Need to check how to get count here
        app_logger.info(f'Deleting {len(results)} logs')
