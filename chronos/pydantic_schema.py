from typing import Any

from pydantic import BaseModel


class TCIntegration(BaseModel):
    tc_id: int
    name: str
    branch_id: int
    active: bool
    webhook_url: str
    api_key: str


class TCDeleteIntegration(BaseModel):
    tc_id: int
    branch_id: int
    api_key: str


class TCWebhook(BaseModel):
    events: list[dict[str, Any]]
    request_time: int


class TCGetWebhooks(BaseModel):
    tc_id: int
    branch_id: int
    page: int
