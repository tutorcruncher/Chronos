from pydantic import BaseModel


class PydanticEndpoint(BaseModel):
    name: str
    branch_id: int
    webhook_url: str
    api_key: str
    active: bool


class PydanticWebhook(BaseModel):
    request_headers: str
    request_body: str
