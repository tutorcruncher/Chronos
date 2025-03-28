from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, Field


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
