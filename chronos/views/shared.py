import json
import secrets

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import nullslast
from sqlmodel import Session, select

from chronos.sql_models import WebhookEndpoint, WebhookLog

# Shared bearer scheme for every route (TC2 and Bobbin alike).
security = HTTPBearer()

LOGS_PER_PAGE = 50


def check_authorisation(authorisation: HTTPAuthorizationCredentials, expected_key: str) -> bool:
    """Check the bearer token matches the expected shared key, else raise 403.

    The caller passes the key for its own product so a TC2 token can't reach Bobbin routes
    and vice versa.
    """
    # compare_digest avoids a timing oracle: a plain != short-circuits on the first differing byte.
    if not secrets.compare_digest(authorisation.credentials, expected_key):
        raise HTTPException(status_code=403, detail='Authorisation token is invalid')
    return True


def serialize_logs_response(db: Session, endpoint: WebhookEndpoint, page: int) -> dict:
    """Return a page of delivery logs for an endpoint.

    Shared by the TC2 and Bobbin get-logs routes — both read the same WebhookLog table once they
    have resolved their endpoint. Fetches 100 to cheaply detect a next page but returns 50; the
    count is a "more pages available" hint, not an exact total.
    """
    offset = page * LOGS_PER_PAGE
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

    count = offset + len(list_of_webhooks)
    if count <= offset:
        return {'message': f'No logs found for page: {page}', 'logs': [], 'count': count}

    return {'logs': list_of_webhooks[:LOGS_PER_PAGE], 'count': count}
