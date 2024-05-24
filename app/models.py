from typing import Optional

from sqlmodel import Field, Session, SQLModel, create_engine
from app.utils import settings


class Webhook(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    headers: str
    payload: str
    created_at: Optional[str] = Field(default=None)

    def __repr__(self):
        return f'WebhookLog(id={self.id}, payload={self.payload}, created_at={self.created_at})'


class WebhookResponse(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    headers: str
    payload: str
    status_code: int
    created_at: Optional[str] = Field(default=None)

    webhook_id: Optional[int] = Field(default=None, foreign_key='webhook.id')


class Integration(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    branch_id: int
    webhook_url: str
    created_at: Optional[str] = Field(default=None)

    def __repr__(self):
        return f'Integration(id={self.id}, name={self.name}, webhook_url={self.webhook_url})'
