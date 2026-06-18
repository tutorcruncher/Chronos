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
    """A webhook endpoint, owned by either a TC2 branch or a Bobbin organization.

    A single table serves both products. Each row is exactly one of:
    - TC2:    ``tc_id`` set, ``bobbin_id`` NULL, ``branch_id`` is the TC2 branch.
    - Bobbin: ``bobbin_id`` set, ``tc_id`` NULL, ``branch_id`` holds the Bobbin organization id.

    The populated id column is the source discriminator: senders filter on it so a TC2 branch and a
    Bobbin org that share an integer never cross-deliver. ``(branch_id, bobbin_id)`` is unique for
    Bobbin rows; NULL ``bobbin_id`` lets many TC2 rows share a branch.
    """

    __table_args__ = (UniqueConstraint('branch_id', 'bobbin_id', name='uq_branch_bobbin'),)

    id: Optional[int] = Field(default=None, primary_key=True)
    tc_id: Optional[int] = Field(default=None, unique=True)  # set for TC2 endpoints
    bobbin_id: Optional[int] = Field(default=None)  # bobbin-api WebhookEndpoint.id, set for Bobbin endpoints
    name: str
    branch_id: int  # TC2 branch_id OR Bobbin organization_id, depending on source
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
