from typing import Any

from pydantic import BaseModel


class TCIntegration(BaseModel):
    tc_id: int
    name: str
    branch_id: int
    webhook_url: str
    api_key: str
    active: bool


class TCWebhook(BaseModel):
    events: list[dict[str, Any]]
    request_time: int
