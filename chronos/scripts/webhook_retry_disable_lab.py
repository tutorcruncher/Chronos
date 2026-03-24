"""
Drive Chronos against the mock webhook receiver for manual retry / auto-disable testing.

One invocation runs every lab check in sequence (retry-stateful, 503, 429, timeout,
public-profile, disable-negative, auto-disable batch, TutorCruncher exempt URL).

Prerequisites
-------------
- Postgres + Redis running; Chronos DB migrated (e.g. make reset-db).
- Terminal 1: make run-server-dev
- Terminal 2: make run-worker
- If USE_ROUND_ROBIN=true (default): Terminal 3: make run-dispatcher
  Or set USE_ROUND_ROBIN=false in .env for a two-process lab.

For quick retry iteration, lower backoff before starting the worker, e.g. in .env::

    WEBHOOK_RETRY_BACKOFF_BASE_SECONDS=0.1
    WEBHOOK_RETRY_BACKOFF_MULTIPLIER=2

To see TC2 disable notifications, set in .env (receiver must be running)::

    TC2_ENDPOINT_DISABLED_URL=http://127.0.0.1:18080/tc2-disabled-notify

TutorCruncher exempt scenario: add to /etc/hosts so the lab hostname hits the mock::

    127.0.0.1 chronos-lab-exempt.tutorcruncher.com

Or pass ``--skip-exempt-tc`` to skip that step.

Mock receiver::

    make run-webhook-lab-receiver

Run all checks (uses tc_id_base, tc_id_base+1, … for unique integrations)::

    uv run python -m chronos.scripts.webhook_retry_disable_lab
    uv run python -m chronos.scripts.webhook_retry_disable_lab --tc-id-base 92000

Print setup checklist only::

    uv run python -m chronos.scripts.webhook_retry_disable_lab --runbook
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx

RUNBOOK = """
Chronos webhook lab — process checklist
=======================================

1. Postgres + Redis
2. Mock receiver (port 18080 by default):
     make run-webhook-lab-receiver
3. Chronos API:
     make run-server-dev
4. Celery worker:
     make run-worker
5. If use_round_robin is True (default), dispatcher:
     make run-dispatcher
   Or set USE_ROUND_ROBIN=false for a two-process lab.

Speed up retries (optional, before starting the worker):
  WEBHOOK_RETRY_BACKOFF_BASE_SECONDS=0.1
  WEBHOOK_RETRY_BACKOFF_MULTIPLIER=2

Timeout scenario (timeout-retry):
  Receiver sleep must exceed Chronos client timeout (default WEBHOOK_HTTP_TIMEOUT_SECONDS=8).

Disable callback (optional):
  TC2_ENDPOINT_DISABLED_URL=http://127.0.0.1:18080/tc2-disabled-notify

TutorCruncher auto-disable exemption (disable-exempt-tc with default lab host):
  Add to /etc/hosts: 127.0.0.1 chronos-lab-exempt.tutorcruncher.com
  so deliveries hit the mock while the URL stays a *.tutorcruncher.com host.

public-profile: the mock must expose POST /lab-ext/{segment}. Restart the receiver after
upgrading Chronos or you may see HTTP 404 on that scenario.

Shared key: must match tc2_shared_key / Authorization Bearer (default test-key).
"""


def _shared_key(args: argparse.Namespace) -> str:
    return args.shared_key or os.environ.get('TC2_SHARED_KEY', 'test-key')


def _auth_headers(shared_key: str) -> dict[str, str]:
    return {'Authorization': f'Bearer {shared_key}', 'Content-Type': 'application/json'}


def _hook_url(receiver_base: str, query: dict[str, str | int | float]) -> str:
    base = receiver_base.rstrip('/')
    q = urlencode({k: str(v) for k, v in query.items()})
    return f'{base}/hook?{q}'


def _integrations_body(
    *,
    tc_id: int,
    branch_id: int,
    webhook_url: str,
    name: str,
    api_key: str,
    request_time: int,
) -> dict:
    return {
        'integrations': [
            {
                'tc_id': tc_id,
                'name': name,
                'branch_id': branch_id,
                'active': True,
                'webhook_url': webhook_url,
                'api_key': api_key,
            }
        ],
        'request_time': request_time,
    }


def _webhook_body(*, branch_id: int, num_events: int, request_time: int) -> dict:
    events = [{'branch': branch_id, 'event': 'lab_event', 'data': {'index': i}} for i in range(num_events)]
    return {'events': events, 'request_time': request_time}


def _public_profile_body(*, branch_id: int, request_time: int) -> dict:
    return {
        'id': 1,
        'branch_id': branch_id,
        'deleted': False,
        'first_name': 'lab',
        'last_name': 'lab',
        'town': 'x',
        'country': 'x',
        'review_rating': 4.5,
        'review_duration': 100,
        'location': {'lat': 1.0, 'long': 1.0},
        'photo': 'x',
        'extra_attributes': [{'test': 'test'}],
        'skills': [{'test': 'test'}],
        'labels': [{'test': 'test'}],
        'created': 'test',
        'release_timestamp': 'test',
        'request_time': request_time,
    }


def _register(
    client: httpx.Client,
    chronos_base: str,
    shared_key: str,
    body: dict,
) -> None:
    r = client.post(
        f'{chronos_base.rstrip("/")}/create-update-callback',
        headers=_auth_headers(shared_key),
        json=body,
        timeout=30.0,
    )
    r.raise_for_status()
    print('create-update-callback:', r.json())


def _send_webhook(
    client: httpx.Client,
    chronos_base: str,
    shared_key: str,
    body: dict,
) -> None:
    r = client.post(
        f'{chronos_base.rstrip("/")}/send-webhook-callback',
        headers=_auth_headers(shared_key),
        json=body,
        timeout=30.0,
    )
    r.raise_for_status()
    print('send-webhook-callback:', r.json())


def _send_public_profile(
    client: httpx.Client,
    chronos_base: str,
    shared_key: str,
    body: dict,
    url_extension: str,
) -> None:
    ext = url_extension.strip('/')
    r = client.post(
        f'{chronos_base.rstrip("/")}/send-webhook-callback/{ext}',
        headers=_auth_headers(shared_key),
        json=body,
        timeout=30.0,
    )
    r.raise_for_status()
    print(f'send-webhook-callback/{ext}:', r.json())


def _poll_logs(
    client: httpx.Client,
    chronos_base: str,
    shared_key: str,
    tc_id: int,
    *,
    min_logs: int,
    max_wait_s: float,
    interval_s: float,
) -> dict:
    deadline = time.monotonic() + max_wait_s
    last: dict = {}
    page = 0
    while time.monotonic() < deadline:
        r = client.get(
            f'{chronos_base.rstrip("/")}/{tc_id}/logs/{page}',
            headers=_auth_headers(shared_key),
            timeout=30.0,
        )
        r.raise_for_status()
        last = r.json()
        logs = last.get('logs') or []
        if len(logs) >= min_logs:
            return last
        time.sleep(interval_s)
    return last


def _log_count(data: dict) -> int:
    return len(data.get('logs') or [])


def _print_log_summary(data: dict) -> None:
    logs = data.get('logs') or []
    if not logs:
        print('No logs yet:', data.get('message', data))
        return
    print(f'Log count (this page): {len(logs)}')
    for i, row in enumerate(logs):
        print(f'  [{i}] status={row.get("status")} status_code={row.get("status_code")} ts={row.get("timestamp")}')


def cmd_runbook() -> None:
    print(RUNBOOK.strip())


def cmd_retry_stateful(args: argparse.Namespace) -> None:
    shared_key = _shared_key(args)
    bucket = args.bucket or f'lab-{uuid.uuid4().hex[:12]}'
    if not args.bucket:
        print(f'Using auto bucket={bucket} (mock keeps counters per bucket; reuse --bucket only when debugging).')
    hook = _hook_url(
        args.receiver_base,
        {
            'bucket': bucket,
            'fail_until': args.fail_until,
            'fail_status': args.fail_status,
        },
    )
    body = _integrations_body(
        tc_id=args.tc_id,
        branch_id=args.branch_id,
        webhook_url=hook,
        name=args.name,
        api_key=args.api_key,
        request_time=args.request_time,
    )
    with httpx.Client() as client:
        _register(client, args.chronos_base, shared_key, body)
        _send_webhook(
            client,
            args.chronos_base,
            shared_key,
            _webhook_body(branch_id=args.branch_id, num_events=1, request_time=args.request_time + 1),
        )
        print('Polling logs...')
        data = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=args.min_logs,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
    _print_log_summary(data)


def _cmd_retry_fixed_status(args: argparse.Namespace, status: int) -> None:
    shared_key = _shared_key(args)
    hook = _hook_url(args.receiver_base, {'status': status})
    body = _integrations_body(
        tc_id=args.tc_id,
        branch_id=args.branch_id,
        webhook_url=hook,
        name=args.name,
        api_key=args.api_key,
        request_time=args.request_time,
    )
    print(f'All responses will be HTTP {status}; expect up to 4 attempts (retries depend on retryability).')
    print('Use shortened WEBHOOK_RETRY_BACKOFF_* in .env or wait for full backoff.')
    with httpx.Client() as client:
        _register(client, args.chronos_base, shared_key, body)
        _send_webhook(
            client,
            args.chronos_base,
            shared_key,
            _webhook_body(branch_id=args.branch_id, num_events=1, request_time=args.request_time + 1),
        )
        print('Polling logs...')
        data = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=args.min_logs,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
    _print_log_summary(data)


def cmd_retry_503(args: argparse.Namespace) -> None:
    _cmd_retry_fixed_status(args, 503)


def cmd_retry_429(args: argparse.Namespace) -> None:
    _cmd_retry_fixed_status(args, 429)


def cmd_timeout_retry(args: argparse.Namespace) -> None:
    shared_key = _shared_key(args)
    hook = _hook_url(args.receiver_base, {'sleep': args.sleep_seconds})
    body = _integrations_body(
        tc_id=args.tc_id,
        branch_id=args.branch_id,
        webhook_url=hook,
        name=args.name,
        api_key=args.api_key,
        request_time=args.request_time,
    )
    print(
        f'Receiver waits {args.sleep_seconds}s before responding; Chronos should time out first '
        '(No response / retries). Ensure sleep > WEBHOOK_HTTP_TIMEOUT_SECONDS on the worker.'
    )
    with httpx.Client() as client:
        _register(client, args.chronos_base, shared_key, body)
        _send_webhook(
            client,
            args.chronos_base,
            shared_key,
            _webhook_body(branch_id=args.branch_id, num_events=1, request_time=args.request_time + 1),
        )
        print('Polling logs...')
        data = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=args.min_logs,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
    _print_log_summary(data)


def cmd_disable(args: argparse.Namespace) -> None:
    shared_key = _shared_key(args)
    hook = _hook_url(args.receiver_base, {'status': 400})
    body = _integrations_body(
        tc_id=args.tc_id,
        branch_id=args.branch_id,
        webhook_url=hook,
        name=args.name,
        api_key=args.api_key,
        request_time=args.request_time,
    )
    wh = _webhook_body(branch_id=args.branch_id, num_events=args.event_count, request_time=args.request_time + 1)
    with httpx.Client() as client:
        _register(client, args.chronos_base, shared_key, body)
        _send_webhook(client, args.chronos_base, shared_key, wh)
        print('Polling logs (expect >=10 failures, then endpoint may auto-disable)...')
        data = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=args.min_logs,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
    _print_log_summary(data)
    print('If TC2_ENDPOINT_DISABLED_URL points at the mock /tc2-disabled-notify, check receiver logs.')


def cmd_disable_negative(args: argparse.Namespace) -> None:
    """Below min_attempts / threshold: endpoint should stay active; verify with a follow-up send."""
    shared_key = _shared_key(args)
    hook = _hook_url(args.receiver_base, {'status': 400})
    body = _integrations_body(
        tc_id=args.tc_id,
        branch_id=args.branch_id,
        webhook_url=hook,
        name=args.name,
        api_key=args.api_key,
        request_time=args.request_time,
    )
    first = _webhook_body(branch_id=args.branch_id, num_events=args.event_count, request_time=args.request_time + 1)
    second = _webhook_body(branch_id=args.branch_id, num_events=1, request_time=args.request_time + 2)
    with httpx.Client() as client:
        _register(client, args.chronos_base, shared_key, body)
        _send_webhook(client, args.chronos_base, shared_key, first)
        print(f'Polling until >= {args.event_count} logs (below disable threshold)...')
        data = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=args.event_count,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
        n1 = _log_count(data)
        print(f'After first batch: {n1} log(s) on page')
        _print_log_summary(data)
        print('Sending follow-up single event (endpoint should still be active)...')
        _send_webhook(client, args.chronos_base, shared_key, second)
        data2 = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=n1 + 1,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
    n2 = _log_count(data2)
    print(f'After follow-up: {n2} log(s) on page (expected > {n1} if still active).')
    _print_log_summary(data2)


def cmd_disable_exempt_tc(args: argparse.Namespace) -> None:
    """Many failures on a *.tutorcruncher.com URL: auto-disable must not run."""
    shared_key = _shared_key(args)
    webhook_url = args.exempt_webhook_url
    if not webhook_url:
        host = args.tutorcruncher_lab_host
        port = args.tutorcruncher_lab_port
        q = urlencode({'status': 400})
        webhook_url = f'http://{host}:{port}/hook?{q}'
        print(
            f'Using default exempt URL: {webhook_url}\n'
            f'If deliveries do not hit the mock, add to /etc/hosts: 127.0.0.1 {host}'
        )
    body = _integrations_body(
        tc_id=args.tc_id,
        branch_id=args.branch_id,
        webhook_url=webhook_url,
        name=args.name,
        api_key=args.api_key,
        request_time=args.request_time,
    )
    batch = _webhook_body(branch_id=args.branch_id, num_events=args.event_count, request_time=args.request_time + 1)
    follow = _webhook_body(branch_id=args.branch_id, num_events=1, request_time=args.request_time + 2)
    with httpx.Client() as client:
        _register(client, args.chronos_base, shared_key, body)
        _send_webhook(client, args.chronos_base, shared_key, batch)
        print('Polling logs after failure batch...')
        data = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=args.min_logs,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
        n1 = _log_count(data)
        _print_log_summary(data)
        print('Follow-up send: if endpoint stayed active (TutorCruncher host exempt), expect another log line.')
        _send_webhook(client, args.chronos_base, shared_key, follow)
        data2 = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=n1 + 1,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
    n2 = _log_count(data2)
    print(f'After follow-up: {n2} log(s) (expected > {n1} when mock receives traffic).')
    _print_log_summary(data2)


def cmd_public_profile(args: argparse.Namespace) -> None:
    shared_key = _shared_key(args)
    base = args.receiver_base.rstrip('/')
    webhook_url = f'{base}/lab-ext'
    body = _integrations_body(
        tc_id=args.tc_id,
        branch_id=args.branch_id,
        webhook_url=webhook_url,
        name=args.name,
        api_key=args.api_key,
        request_time=args.request_time,
    )
    profile = _public_profile_body(branch_id=args.branch_id, request_time=args.request_time + 1)
    with httpx.Client() as client:
        _register(client, args.chronos_base, shared_key, body)
        _send_public_profile(client, args.chronos_base, shared_key, profile, args.url_extension)
        print('Polling logs (delivery URL is .../lab-ext/<extension>)...')
        data = _poll_logs(
            client,
            args.chronos_base,
            shared_key,
            args.tc_id,
            min_logs=args.min_logs,
            max_wait_s=args.poll_seconds,
            interval_s=args.poll_interval,
        )
    _print_log_summary(data)


def _build_ns(base: argparse.Namespace, **overrides: Any) -> argparse.Namespace:
    d = vars(base).copy()
    d.update(overrides)
    return argparse.Namespace(**d)


def cmd_run_all(args: argparse.Namespace) -> int:
    """Run every lab scenario in sequence; each uses a distinct tc_id (base + index)."""
    b = args.tc_id_base
    rt = args.request_time

    scenarios: list[tuple[str, Callable[[argparse.Namespace], None], dict]] = [
        (
            'retry-stateful (fail_until then 200)',
            cmd_retry_stateful,
            {'tc_id': b + 0, 'min_logs': 4, 'request_time': rt, 'bucket': args.bucket},
        ),
        (
            'retry-503',
            cmd_retry_503,
            {'tc_id': b + 1, 'min_logs': 4, 'request_time': rt + 10_000},
        ),
        (
            'retry-429',
            cmd_retry_429,
            {'tc_id': b + 2, 'min_logs': 4, 'request_time': rt + 20_000},
        ),
        (
            'timeout-retry (receiver sleep > Chronos HTTP timeout)',
            cmd_timeout_retry,
            {'tc_id': b + 3, 'min_logs': 4, 'request_time': rt + 30_000},
        ),
        (
            'public-profile (url_extension + /lab-ext)',
            cmd_public_profile,
            {'tc_id': b + 4, 'min_logs': 1, 'request_time': rt + 40_000},
        ),
        (
            'disable-negative (below threshold, follow-up still delivers)',
            cmd_disable_negative,
            {
                'tc_id': b + 5,
                'event_count': args.negative_batch_events,
                'poll_seconds': args.poll_seconds,
                'request_time': rt + 50_000,
            },
        ),
        (
            'disable (batch 400s → auto-disable when threshold met)',
            cmd_disable,
            {
                'tc_id': b + 6,
                'min_logs': 10,
                'event_count': args.disable_batch_events,
                'poll_seconds': args.poll_seconds,
                'request_time': rt + 60_000,
            },
        ),
    ]

    if not args.skip_exempt_tc:
        scenarios.append(
            (
                'disable-exempt-tc (*.tutorcruncher.com host not auto-disabled)',
                cmd_disable_exempt_tc,
                {
                    'tc_id': b + 7,
                    'min_logs': 10,
                    'event_count': args.exempt_batch_events,
                    'poll_seconds': args.exempt_poll_seconds,
                    'request_time': rt + 70_000,
                },
            )
        )

    total = len(scenarios)
    for i, (title, fn, kw) in enumerate(scenarios, start=1):
        ns = _build_ns(args, **kw)
        bar = '=' * 72
        print(f'\n{bar}\n[{i}/{total}] {title}\n{bar}')
        try:
            fn(ns)
        except Exception as exc:
            print(f'\nFAILED: {title}\n{exc!r}', file=sys.stderr)
            return 1

    print(f'\n{"=" * 72}\nAll {total} lab check(s) finished without error.\n{"=" * 72}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--runbook',
        action='store_true',
        help='Print setup checklist for web/worker/receiver and exit',
    )
    parser.add_argument(
        '--tc-id-base',
        type=int,
        default=91000,
        help='First TC integration id; scenarios use base, base+1, … (default 91000)',
    )
    parser.add_argument(
        '--skip-exempt-tc',
        action='store_true',
        help='Skip TutorCruncher host exemption step (when /etc/hosts is not set up)',
    )
    parser.add_argument(
        '--chronos-base-url',
        dest='chronos_base',
        default='http://127.0.0.1:5000',
        help='Chronos API base URL',
    )
    parser.add_argument(
        '--receiver-base',
        dest='receiver_base',
        default='http://127.0.0.1:18080',
        help='Mock receiver base URL',
    )
    parser.add_argument(
        '--shared-key',
        default='',
        dest='shared_key',
        help='Bearer token (default: env TC2_SHARED_KEY or test-key)',
    )
    parser.add_argument('--branch-id', type=int, default=99, dest='branch_id')
    parser.add_argument('--name', default='webhook-lab', dest='name')
    parser.add_argument('--api-key', default='lab-secret-key', dest='api_key')
    parser.add_argument('--request-time', type=int, default=1_700_000_000, dest='request_time')
    parser.add_argument('--poll-seconds', type=float, default=120.0, dest='poll_seconds')
    parser.add_argument(
        '--poll-interval', type=float, default=0.5, dest='poll_interval', help='Seconds between log polls'
    )
    parser.add_argument(
        '--bucket',
        default=None,
        help='retry-stateful only: fixed mock bucket key (default: random each run)',
    )
    parser.add_argument('--fail-until', type=int, default=3, dest='fail_until')
    parser.add_argument('--fail-status', type=int, default=503, dest='fail_status')
    parser.add_argument(
        '--sleep-seconds',
        type=float,
        default=15.0,
        dest='sleep_seconds',
        help='timeout-retry: receiver sleep (must exceed worker WEBHOOK_HTTP_TIMEOUT_SECONDS)',
    )
    parser.add_argument(
        '--disable-batch-events',
        type=int,
        default=12,
        dest='disable_batch_events',
        help='Event count for auto-disable batch (>=10)',
    )
    parser.add_argument(
        '--negative-batch-events',
        type=int,
        default=5,
        dest='negative_batch_events',
        help='Event count for disable-negative (stay below min_attempts)',
    )
    parser.add_argument(
        '--exempt-batch-events',
        type=int,
        default=12,
        dest='exempt_batch_events',
        help='Event count for TutorCruncher exempt URL batch',
    )
    parser.add_argument(
        '--exempt-poll-seconds',
        type=float,
        default=300.0,
        dest='exempt_poll_seconds',
        help='Poll timeout for exempt-tc step (often slower without /etc/hosts)',
    )
    parser.add_argument(
        '--exempt-webhook-url',
        default='',
        dest='exempt_webhook_url',
        help='Full *.tutorcruncher.com webhook URL for exempt step (empty: lab default host+port)',
    )
    parser.add_argument(
        '--tutorcruncher-lab-host',
        default='chronos-lab-exempt.tutorcruncher.com',
        dest='tutorcruncher_lab_host',
    )
    parser.add_argument('--tutorcruncher-lab-port', type=int, default=18080, dest='tutorcruncher_lab_port')
    parser.add_argument('--url-extension', default='labprofile', dest='url_extension')

    args = parser.parse_args(argv)

    if args.runbook:
        cmd_runbook()
        return 0

    parsed = urlparse(args.receiver_base)
    if not parsed.scheme or not parsed.netloc:
        print('Invalid --receiver-base URL', file=sys.stderr)
        return 2

    return cmd_run_all(args)


if __name__ == '__main__':
    raise SystemExit(main())
