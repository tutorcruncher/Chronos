from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, Field

from chronos.sql_models import Provider


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

    def to_endpoint_fields(self) -> dict:
        """Map the TC2 wire shape (branch_id) onto the shared WebhookEndpoint columns."""
        return {
            'provider': Provider.TC2,
            'tc_id': self.tc_id,
            'org_id': self.branch_id,
            'name': self.name,
            'active': self.active,
            'webhook_url': self.webhook_url,
            'api_key': self.api_key,
        }


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


class TCPublicProfileWebhook(BaseModel):
    """
    Pydantic model for the TCPublicProfileWebhook. This is the payload from for the public profile endpoint
    """

    id: int
    branch_id: int
    deleted: bool
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    town: Optional[str] = None
    country: Optional[str] = None
    review_rating: Optional[float] = None
    review_duration: Optional[int] = None
    location: Optional[dict] = None
    photo: Optional[str] = None
    extra_attributes: Optional[list[dict]] = None
    skills: Optional[list[dict]] = None
    labels: Optional[list[dict]] = None
    created: Optional[str] = None
    release_timestamp: str
    request_time: Optional[int] = Field(validation_alias=AliasChoices('request_time', '_request_time'))


class BobbinIntegration(BaseModel):
    """A single Bobbin webhook endpoint to create or update in Chronos."""

    bobbin_endpoint_id: int
    organization_id: int
    name: str
    active: bool
    webhook_url: str
    api_key: str

    def to_endpoint_fields(self) -> dict:
        """Map the bobbin-api wire shape onto the shared WebhookEndpoint columns.

        Bobbin's organization_id is stored in org_id and its endpoint id in bobbin_id;
        provider='bobbin' makes this row unambiguously a Bobbin endpoint.
        """
        return {
            'provider': Provider.BOBBIN,
            'bobbin_id': self.bobbin_endpoint_id,
            'org_id': self.organization_id,
            'name': self.name,
            'active': self.active,
            'webhook_url': self.webhook_url,
            'api_key': self.api_key,
        }


class BobbinDeleteIntegration(BaseModel):
    """Identifies a Bobbin endpoint to delete (org-scoped)."""

    bobbin_endpoint_id: int
    organization_id: int


class BobbinWebhookSend(BaseModel):
    """A single Bobbin domain event to fan out to the org's matching endpoints."""

    event_type: str
    organization_id: int
    data: dict
    request_time: int


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
