from typing import Optional

from pydantic import BaseModel


class MessageResponse(BaseModel):
    """A simple `{message: ...}` response (Bobbin create/update, delete, send)."""

    message: str


class WebhookLogsResponse(BaseModel):
    """A page of delivery logs, plus a `message` when there are none for the page."""

    logs: list[dict]
    count: int
    message: Optional[str] = None


class RequestData(BaseModel):
    """
    Pydantic model for the RequestData object
    """

    endpoint_id: int
    request_headers: str
    request_body: str
    response_headers: str = '{"Message": "No response from endpoint"}'
    response_body: str = '{"Message": "No response from endpoint"}'
    status_code: int = 999
    successful_response: str = False
