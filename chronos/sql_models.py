import datetime
from enum import StrEnum
from typing import Optional

from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class WebhookStatus(StrEnum):
    SUCCESS = 'Success'
    NO_RESPONSE = 'No response'
    UNEXPECTED_RESPONSE = 'Unexpected response'


class WebhookEndpoint(SQLModel, table=True):
    """
    The model for the webhook endpoint table
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    tc_id: int = Field(unique=True)
    name: str
    branch_id: int
    webhook_url: str
    api_key: str
    active: bool
    timestamp: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow, nullable=False, index=True
    )  # do we care?

    def __repr__(self):
        return f'WebhookEndpoint(id={self.id}, name={self.name}, webhook_url={self.webhook_url})'


class WebhookLog(SQLModel, table=True):
    """
    The model for the webhook log table
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    request_headers: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    request_body: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    response_headers: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    response_body: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    status: str
    status_code: Optional[int]
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow, nullable=False, index=True)

    webhook_endpoint_id: int | None = Field(default=None, foreign_key='webhookendpoint.id', index=True)

    def __repr__(self):
        return f'WebhookLog(id={self.id}, payload={self.request_body}, created_at={self.timestamp})'


class BobbinWebhookEndpoint(SQLModel, table=True):
    """A webhook endpoint owned by a Bobbin organization.

    Kept in a separate table from WebhookEndpoint (TC2) so the two products never share rows,
    ids or constraints. Identity is org-scoped: (organization_id, bobbin_endpoint_id) is unique,
    never the bare endpoint id, so a Bobbin endpoint id can freely coincide with a TC2 tc_id.
    """

    __table_args__ = (UniqueConstraint('organization_id', 'bobbin_endpoint_id', name='uq_bobbin_org_endpoint'),)

    id: Optional[int] = Field(default=None, primary_key=True)
    organization_id: int = Field(index=True)  # bobbin-api Organization.id — its own namespace, NOT a TC2 branch_id
    bobbin_endpoint_id: int  # bobbin-api WebhookEndpoint.id (the mirror join key, analogous to tc_id)
    name: str
    webhook_url: str
    api_key: str  # per-endpoint HMAC secret, mirrored from bobbin-api
    events: list[str] = Field(default_factory=list, sa_type=JSONB)  # event-name filter; [] == all events
    active: bool = True
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow, nullable=False, index=True)

    def __repr__(self):
        return f'BobbinWebhookEndpoint(id={self.id}, org={self.organization_id}, webhook_url={self.webhook_url})'


class BobbinWebhookLog(SQLModel, table=True):
    """Delivery log for a Bobbin webhook. Mirrors WebhookLog but FKs the Bobbin endpoint table."""

    id: Optional[int] = Field(default=None, primary_key=True)
    request_headers: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    request_body: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    response_headers: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    response_body: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    status: str
    status_code: Optional[int]
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow, nullable=False, index=True)

    bobbin_webhook_endpoint_id: int | None = Field(default=None, foreign_key='bobbinwebhookendpoint.id', index=True)

    def __repr__(self):
        return f'BobbinWebhookLog(id={self.id}, payload={self.request_body}, created_at={self.timestamp})'
