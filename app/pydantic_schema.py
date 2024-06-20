from pydantic import BaseModel


class TCIntegration(BaseModel):
    tc_id: int
    name: str
    branch_id: int
    webhook_url: str
    api_key: str
    active: bool


class TCEvent(BaseModel):
    action: str


class TCWebhook(BaseModel):
    events: list[TCEvent]
    _request_time: int
