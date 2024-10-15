import json
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


class TCIntegrations(BaseModel):
    """
    Pydantic model for the TCIntegrations object
    """

    integrations: list[TCIntegration]
    request_time: int


class TCDeleteIntegration(BaseModel):
    """
    Pydantic model for the TCDeleteIntegration object
    """

    tc_id: int
    branch_id: int


class TCWebhook(BaseModel):
    """
    Pydantic model for the TCWebhook. This is the payload from TC2
    """

    events: list[dict[str, Any]]
    request_time: int


class RequestData(BaseModel):
    """
    Pydantic model for the RequestData object
    """

    request_headers: str
    request_body: str
    response_headers: str = '"{"Message": "No response from endpoint"}"'
    response_body: str = '"{"Message": "No response from endpoint"}"'
    status_code: int = 999
