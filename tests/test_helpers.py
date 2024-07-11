import hashlib
import hmac
import json

from app.utils import settings


def _get_headers(data: dict):
    json_payload = json.dumps(data).encode()
    webhook_signature = hmac.new(settings.tc2_shared_key.encode(), json_payload, hashlib.sha256)
    return {
        'User-Agent': 'TutorCruncher',
        'Content-Type': 'application/json',
        'Webhook-Signature': webhook_signature.hexdigest(),
    }
