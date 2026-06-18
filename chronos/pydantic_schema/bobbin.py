from pydantic import BaseModel

from chronos.sql_models import Provider


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
