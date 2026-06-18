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


class Provider(StrEnum):
    """The source product an endpoint (and its webhooks) belong to."""

    TC2 = 'tc2'
    BOBBIN = 'bobbin'


class WebhookEndpoint(SQLModel, table=True):
    """A webhook endpoint, owned by either a TC2 branch or a Bobbin organization.

    A single table serves both products, discriminated by ``provider``:
    - TC2:    ``provider='tc2'``,    ``tc_id`` set,     ``org_id`` is the TC2 branch.
    - Bobbin: ``provider='bobbin'``, ``bobbin_id`` set, ``org_id`` is the Bobbin organization id.

    ``provider`` defaults to ``tc2`` (the original product); the Bobbin ingest sets it explicitly.
    Senders filter on ``provider`` so a TC2 branch and a Bobbin org that share an ``org_id`` integer
    never cross-deliver. ``(org_id, bobbin_id)`` is unique for Bobbin rows; NULL ``bobbin_id`` lets
    many TC2 rows share an ``org_id``.
    """

    __table_args__ = (UniqueConstraint('org_id', 'bobbin_id', name='uq_org_bobbin'),)

    id: Optional[int] = Field(default=None, primary_key=True)
    provider: str = Field(default=Provider.TC2)  # Provider value: 'tc2' | 'bobbin' (low cardinality, not indexed)
    tc_id: Optional[int] = Field(default=None, unique=True)  # set for TC2 endpoints
    bobbin_id: Optional[int] = Field(default=None)  # bobbin-api WebhookEndpoint.id, set for Bobbin endpoints
    name: str
    org_id: int  # TC2 branch_id OR Bobbin organization_id, depending on provider
    webhook_url: str
    api_key: str
    active: bool
    events: list[str] = Field(default_factory=list, sa_type=JSONB)  # Bobbin event filter ([] == all); empty for TC2
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
