import datetime
from typing import Optional

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class Endpoint(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tc_id: int = Field(unique=True)
    name: str
    branch_id: int
    webhook_url: str
    api_key: str
    active: bool
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow, nullable=False)  # do we care?

    def __repr__(self):
        return f'Endpoint(id={self.id}, name={self.name}, webhook_url={self.webhook_url})'


class WebhookLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    request_headers: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    request_body: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    response_headers: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    response_body: Optional[dict] = Field(nullable=True, sa_type=JSONB)
    status: str
    status_code: Optional[int]
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow, nullable=False)

    endpoint_id: int | None = Field(default=None, foreign_key='endpoint.id')

    def __repr__(self):
        return f'WebhookLog(id={self.id}, payload={self.payload}, created_at={self.created_at})'
