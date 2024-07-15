from datetime import timezone, timedelta, datetime
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlmodel import Session, select, col

from app.sql_models import WebhookLog, Endpoint
from app.worker import send_webhooks, delete_old_logs_job, _delete_old_logs_job
from tests.test_helpers import (
    _get_headers,
    get_dft_webhook_data,
    create_endpoint_from_dft_data,
    get_successful_response,
    get_failed_response,
    create_webhook_log_from_dft_data,
)


@patch('app.worker.session.request')
def test_send_webhook_one(mock_response, session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    payload = get_dft_webhook_data()
    headers = _get_headers(payload)
    mock_response.return_value = get_successful_response(payload, headers)

    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 0

    send_webhooks(payload, session)
    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 1

    webhook = webhooks[0]
    assert webhook.status == 'Success'
    assert webhook.status_code == 200


@patch('app.worker.session.request')
def test_send_many_endpoints(mock_response, session: Session, client: TestClient):
    endpoints = session.exec(select(Endpoint)).all()
    assert len(endpoints) == 0

    for tc_id in range(1, 11):
        ep = create_endpoint_from_dft_data(tc_id=tc_id)
        session.add(ep)
    session.commit()

    endpoints = session.exec(select(Endpoint)).all()
    assert len(endpoints) == 10

    payload = get_dft_webhook_data()
    headers = _get_headers(payload)
    mock_response.return_value = get_successful_response(payload, headers)

    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 0

    send_webhooks(payload, session)
    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 10


@patch('app.worker.session.request')
def test_send_correct_branch(mock_response, session: Session, client: TestClient):
    endpoints = session.exec(select(Endpoint)).all()
    assert len(endpoints) == 0

    for tc_id in range(1, 6):
        ep = create_endpoint_from_dft_data(tc_id=tc_id)
        session.add(ep)

        ep = create_endpoint_from_dft_data(tc_id=tc_id + 10, branch_id=199)
        session.add(ep)

        ep = create_endpoint_from_dft_data(tc_id=tc_id + 100, branch_id=299)
        session.add(ep)
    session.commit()

    endpoints = session.exec(select(Endpoint)).all()
    assert len(endpoints) == 15

    endpoints_1 = session.exec(select(Endpoint).where(Endpoint.branch_id == 99)).all()
    assert len(endpoints_1) == 5
    endpoints_2 = session.exec(select(Endpoint).where(Endpoint.branch_id == 199)).all()
    assert len(endpoints_2) == 5
    endpoints_3 = session.exec(select(Endpoint).where(Endpoint.branch_id == 299)).all()
    assert len(endpoints_3) == 5

    payload = get_dft_webhook_data()
    headers = _get_headers(payload)
    mock_response.return_value = get_successful_response(payload, headers)

    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 0

    send_webhooks(payload, session)
    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 5

    webhooks = session.exec(
        select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_1]))
    ).all()
    assert len(webhooks) == 5

    webhooks = session.exec(
        select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_2]))
    ).all()
    assert len(webhooks) == 0

    webhooks = session.exec(
        select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_3]))
    ).all()
    assert len(webhooks) == 0

    payload = get_dft_webhook_data(branch_id=199)
    headers = _get_headers(payload)
    mock_response.return_value = get_successful_response(payload, headers)

    send_webhooks(payload, session)
    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 10

    webhooks = session.exec(
        select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_1]))
    ).all()
    assert len(webhooks) == 5

    webhooks = session.exec(
        select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_2]))
    ).all()
    assert len(webhooks) == 5

    webhooks = session.exec(
        select(WebhookLog).where(col(WebhookLog.endpoint_id).in_([ep.id for ep in endpoints_3]))
    ).all()
    assert len(webhooks) == 0


@patch('app.worker.session.request')
def test_send_webhook_fail_to_send_only_one(mock_response, session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    payload = get_dft_webhook_data()
    headers = _get_headers(payload)
    mock_response.return_value = get_failed_response(payload, headers)

    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 0

    send_webhooks(payload, session)
    webhooks = session.exec(select(WebhookLog)).all()
    assert len(webhooks) == 1

    webhook = webhooks[0]
    assert webhook.status == 'Unexpected response'
    assert webhook.status_code == 409


def test_delete_old_logs(session: Session, client: TestClient):
    ep = create_endpoint_from_dft_data()
    session.add(ep)
    session.commit()

    for i in range(1, 31):
        whl = create_webhook_log_from_dft_data(
            endpoint_id=ep.id,
            timestamp=datetime.now() - timedelta(days=i),
        )
        session.add(whl)
    session.commit()

    logs = session.exec(select(WebhookLog)).all()
    assert len(logs) == 30

    # with patch('app.worker._delete_old_logs_job.delay') as mock_task:
    #     debug(1)
    #     await delete_old_logs_job()
    #     debug(2)
    #     assert mock_task.called

    _delete_old_logs_job(session)
    logs = session.exec(select(WebhookLog)).all()
    assert len(logs) == 15
