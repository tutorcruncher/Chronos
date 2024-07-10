from sqlmodel import Session
from fastapi.testclient import TestClient


def test_send_webhook(session: Session, client: TestClient):
    # receive webhook event
    # send webhook event
    # assert response
    pass


def test_send_many_webhooks(session: Session, client: TestClient):
    # receive webhook event
    # send webhook event
    # assert many response
    pass


def test_send_webhook_fail_to_send_one(session: Session, client: TestClient):
    pass


def test_send_webhook_fail_to_send_one_of_many(session: Session, client: TestClient):
    pass


def test_send_webhook_bad_request(session: Session, client: TestClient):
    pass


def test_get_logs_none(session: Session, client: TestClient):
    pass


def test_get_logs_one(session: Session, client: TestClient):
    pass


def test_get_logs_many(session: Session, client: TestClient):
    pass


def test_delete_old_logs(session: Session, client: TestClient):
    pass
