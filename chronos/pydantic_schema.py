from typing import Any

from pydantic import BaseModel


class TCIntegration(BaseModel):
    """
    Pydantic model for the TCIntegration object
    """

    tc_id: int
    name: str
    branch_id: int
    active: bool
    webhook_url: str
    api_key: str


class TCDeleteIntegration(BaseModel):
    """
    Pydantic model for deleting an integration in Chronos
    """

    tc_id: int
    branch_id: int
    api_key: str


class TCWebhook(BaseModel):
    """
    Pydantic model for the TCWebhook. This is the payload from TC2
    """

    events: list[dict[str, Any]]
    request_time: int


class TCGetWebhooks(BaseModel):
    """
    Pydantic model for getting webhooks from Chronos
    """

    tc_id: int
    branch_id: int
    page: int
