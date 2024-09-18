import json

from requests import Request, Response

from chronos.main import app
from chronos.sql_models import Endpoint, WebhookLog
from chronos.utils import settings

send_webhook_with_extension_url = app.url_path_for('send_webhook_with_extension', url_extension='test')
send_webhook_url = app.url_path_for('send_webhook')


def get_dft_endpoint_data_list(count: int = 1, **kwargs) -> dict:
    integrations = []
    for i in range(1, count + 1):
        integration_dict = {
            'tc_id': i,
            'name': f'test_endpoint_{i}',
            'branch_id': 99,
            'active': True,
            'webhook_url': f'https://test_endpoint_{i}.com',
            'api_key': 'test_key',
        }
        for k, v in kwargs.items():
            integration_dict[k] = v
        integrations.append(integration_dict)
    return {'integrations': integrations, 'request_time': 1234567890}


def get_dft_endpoint_deletion_data(**kwargs) -> dict:
    endpoint_dict = {
        'tc_id': 1,
        'branch_id': 99,
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


def get_dft_get_log_data(tc_id: int = None, **kwargs) -> dict:
    webhook_dict = {
        'tc_id': tc_id or 1,
        'page': 0,
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


def _get_webhook_headers() -> dict:
    return {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {settings.tc2_shared_key}',
    }


def _get_endpoint_headers() -> dict:
    return {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {settings.tc2_shared_key}',
    }


def create_endpoint_from_dft_data(count: int = 1, **kwargs) -> list[Endpoint]:
    integration_data = get_dft_endpoint_data_list(count=count, **kwargs)
    if len(integration_data['integrations']) == 1:
        return [Endpoint(**integration_data['integrations'][0])]
    else:
        return [Endpoint(**integration) for integration in integration_data['integrations']]


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
