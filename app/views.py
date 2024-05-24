from fastapi import APIRouter, Header

from app.models import Admin, Company

main_router = APIRouter()



@main_router.post('/send-webhook/', name='Receive webhooks from TC and send them to the relevant endpoints')
async def send_webhook() -> Admin.pydantic_schema():
    # Receive webhook payloads from TC and send them out to the relevant other endpoints
    pass
