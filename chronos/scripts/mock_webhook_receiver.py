"""
HTTP receiver for manual Chronos webhook retry / auto-disable testing.

Query parameters on POST /hook and POST /lab-ext/{segment}:

- status=<int> — HTTP status to return (default 200). Use 503, 429, 400, etc.
- sleep=<float> — seconds to wait before responding (timeout testing vs webhook_http_timeout_seconds).

Stateful mode (bucket + fail_until):

- bucket=<str> — counter key (required with fail_until).
- fail_until=<int> — first N requests return fail_status, then status (default 200).
- fail_status=<int> — status while failing (default 503).

Use /lab-ext as webhook_url base (no query string) with Chronos url_extension so deliveries hit
/lab-ext/<extension> (see webhook_retry_disable_lab.py public-profile scenario).

POST /tc2-disabled-notify — logs JSON body (point tc2_endpoint_disabled_url here during disable tests).

Run: uv run uvicorn chronos.scripts.mock_webhook_receiver:app --host 127.0.0.1 --port 18080
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import FastAPI, Request, Response

logger = logging.getLogger(__name__)

app = FastAPI(title='Chronos webhook lab receiver')

_fail_remaining: dict[str, int] = {}
_bucket_lock = asyncio.Lock()


async def _hook_response(request: Request) -> Response:
    qs = dict(request.query_params)
    sleep_s = float(qs.get('sleep') or 0)
    if sleep_s > 0:
        await asyncio.sleep(sleep_s)

    body = await request.body()

    bucket = qs.get('bucket')
    fail_until_raw = qs.get('fail_until')

    if bucket is not None and fail_until_raw is not None:
        fail_until = int(fail_until_raw)
        fail_status = int(qs.get('fail_status') or 503)
        success_status = int(qs.get('status') or 200)
        async with _bucket_lock:
            remaining = _fail_remaining.get(bucket, fail_until)
            if remaining > 0:
                _fail_remaining[bucket] = remaining - 1
                status = fail_status
            else:
                status = success_status
    else:
        status = int(qs.get('status') or 200)

    payload: dict[str, Any] = {
        'ok': status in {200, 201, 202, 204},
        'returned_status': status,
        'query': qs,
        'body_len': len(body),
        'path': str(request.url.path),
    }
    content = json.dumps(payload)
    return Response(content=content, status_code=status, media_type='application/json')


@app.post('/hook')
async def hook(request: Request) -> Response:
    return await _hook_response(request)


@app.post('/lab-ext/{segment}')
async def hook_with_extension(segment: str, request: Request) -> Response:
    """Chronos appends /{url_extension} to webhook_url; use base .../lab-ext for this path."""
    return await _hook_response(request)


@app.post('/tc2-disabled-notify')
async def tc2_disabled_notify(request: Request) -> dict[str, bool]:
    raw = (await request.body()).decode('utf-8', errors='replace')
    try:
        parsed = json.loads(raw) if raw else {}
        logger.info('tc2_endpoint_disabled_url callback: %s', json.dumps(parsed, indent=2))
    except json.JSONDecodeError:
        logger.info('tc2_endpoint_disabled_url callback (raw): %s', raw)
    return {'received': True}


@app.get('/health')
async def health() -> dict[str, str]:
    return {'status': 'ok'}
