import hashlib
import hmac
import json

from requests import Request, Response

from app.sql_models import Endpoint, WebhookLog
from app.utils import settings


def get_dft_endpoint_data(**kwargs) -> dict:
    endpoint_dict = {
        'tc_id': 1,
        'name': 'test_endpoint',
        'branch_id': 99,
        'webhook_url': 'test.com',
        'api_key': 'test',
        'active': True,
    }
    for k, v in kwargs.items():
        endpoint_dict[k] = v
    return endpoint_dict


def get_dft_webhook_data(branch_id: int = None, **kwargs) -> dict:
    branch_id = branch_id or 99
    webhook_dict = {
        'events': [{'branch': branch_id, 'event': 'test_event', 'data': {'test': 'data'}}],
        'request_time': 1234567890,
    }
    for k, v in kwargs.items():
        webhook_dict[k] = v
    return webhook_dict


def get_dft_webhook_log_data(branch_id: int = None, endpoint_id: int = None, **kwargs) -> dict:
    branch_id = branch_id or 99
    webhook_log_dict = {
        'request_headers': json.dumps({'User-Agent': 'TutorCruncher', 'Content-Type': 'application/json'}),
        'request_body': json.dumps(
            {
                'events': [{'branch': branch_id, 'event': 'test_event', 'data': {'test': 'data'}}],
                'request_time': 1234567890,
            }
        ),
        'response_headers': json.dumps({'User-Agent': 'TutorCruncher', 'Content-Type': 'application/json'}),
        'response_body': json.dumps({'status_code': 200, 'message': 'success'}),
        'status': 'Success',
        'status_code': 200,
        'timestamp': 1234567890,
        'endpoint_id': endpoint_id or 1,
    }
    for k, v in kwargs.items():
        webhook_log_dict[k] = v
    return webhook_log_dict


def _get_webhook_headers(data: dict) -> dict:
    json_payload = json.dumps(data).encode()
    webhook_signature = hmac.new(settings.tc2_shared_key.encode(), json_payload, hashlib.sha256)
    return {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'Webhook-Signature': webhook_signature.hexdigest(),
    }


def _get_endpoint_headers() -> dict:
    return {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'Authorization': settings.tc2_shared_key,
    }


def create_endpoint_from_dft_data(**kwargs) -> Endpoint:
    webhook_data = get_dft_endpoint_data(**kwargs)
    return Endpoint(**webhook_data)


def create_webhook_log_from_dft_data(**kwargs) -> WebhookLog:
    webhook_data = get_dft_webhook_log_data(**kwargs)
    return WebhookLog(**webhook_data)


def get_successful_response(payload, headers, **kwargs) -> Response:
    response_dict = {'status_code': 200, 'message': 'success'}
    for k, v in kwargs.items():
        response_dict[k] = v
    request = Request()
    request.headers = headers
    request.body = json.dumps(payload).encode()
    response = Response()
    response.request = request
    response.status_code = 200
    response._content = json.dumps(response_dict).encode()
    return response


def get_failed_response(payload, headers, **kwargs) -> Response:
    response_dict = {'status_code': 409, 'message': 'Bad request'}
    for k, v in kwargs.items():
        response_dict[k] = v
    request = Request()
    request.headers = headers
    request.body = json.dumps(payload).encode()
    response = Response()
    response.request = request
    response.status_code = 409
    response._content = json.dumps(response_dict).encode()
    return response
