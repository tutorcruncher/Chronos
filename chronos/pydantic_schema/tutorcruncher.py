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
