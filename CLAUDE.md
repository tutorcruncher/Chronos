# Chronos – CLAUDE.md

This file helps AI assistants (and humans) understand how Chronos works and what it does.

## What Chronos Is

**Chronos** is a webhook microservice for **TutorCruncher (TC)**. It offloads all webhook delivery from the main TC application so that:

- TC sends **one request** to Chronos per webhook event; Chronos then fans out to client endpoints, stores logs, and manages retries/cleanup.
- Webhook logs and failed deliveries no longer bloat the main TC database or slow down TC jobs.

In short: TC pushes webhook payloads to Chronos; Chronos stores **endpoints** and **webhook logs**, delivers payloads to client URLs, and exposes APIs for TC to manage endpoints and read logs.

## High-Level Architecture

- **Web service**: FastAPI app (uvicorn). Handles HTTP API: receive webhooks from TC, CRUD endpoints, fetch logs. Auth via Bearer token (`tc2_shared_key`).
- **Worker(s)**: Celery workers. Execute `task_send_webhooks` (and the log-deletion task). Pull tasks from the `celery` queue (Redis).
- **Dispatcher**: A **single** long-running Celery task (`job_dispatcher`) on a dedicated worker. Consumes only the `dispatcher` queue. Runs a round-robin loop that moves jobs from **per-branch Redis queues** into the Celery queue so all branches get fair processing.
- **Redis**: Broker and result backend for Celery; also stores per-branch job queues and dispatcher cursor for round-robin.
- **PostgreSQL**: Stores `WebhookEndpoint` and `WebhookLog` (SQLModel).

Process types (see `Procfile` / `Makefile`):

- `web`: uvicorn serving the FastAPI app.
- `worker`: Celery worker for default queue (webhook sending, delete-old-logs).
- `dispatcher`: Celery worker that runs only `job_dispatcher` with no time limits.

## Core Concepts

- **Branch**: A TC entity (e.g. a company/tenant). Each webhook is associated with a `branch_id`. Endpoints are registered per branch. When `use_round_robin` is on, work is queued and dispatched **per branch** so one busy branch cannot starve others.
- **Endpoint**: A client's webhook URL and metadata, stored as `WebhookEndpoint` (tc_id, name, branch_id, webhook_url, api_key, active, etc.). TC creates/updates/deletes these via Chronos API.
- **Webhook payload**: Either `TCWebhook` (events + request_time) or `TCPublicProfileWebhook` (public profile fields). Branch can come from `events[0].branch` or `branch_id`; missing branch is treated as `GLOBAL_BRANCH_ID` (0).
- **Round-robin**: When `settings.use_round_robin` is True, the API does **not** push directly to Celery. Instead it enqueues to a Redis per-branch list. The dispatcher task runs a loop, cycling over branches with pending jobs and dispatching one (or a batch) per branch into the Celery queue each cycle, with backpressure when the Celery queue is too long.

## API Endpoints

All TC-facing endpoints require `Authorization: Bearer <tc2_shared_key>`.

| Method + Path | Purpose |
|---------------|--------|
| `GET /` | Health/live check. |
| `POST /send-webhook-callback` | Accept `TCWebhook`, enqueue sending to all active endpoints for the payload's branch. |
| `POST /send-webhook-callback/{url_extension}` | Same but with URL path suffix for endpoints (e.g. public profile). Body: `TCPublicProfileWebhook`. |
| `POST /create-update-callback` | Create or update `WebhookEndpoint`(s) from TC (`TCIntegrations`). |
| `POST /delete-endpoint` | Delete an endpoint by TC payload (`TCDeleteIntegration`: tc_id, branch_id). Deletes its logs first. |
| `GET /{tc_id}/logs/{page}` | Paginated webhook logs for the endpoint with that `tc_id`. Returns up to 50 logs per page (internal query fetches 100, returns 50; count used for "more pages" hint). |

Request/response shapes are defined in `chronos/pydantic_schema.py` (e.g. `TCWebhook`, `TCPublicProfileWebhook`, `TCIntegrations`, `TCDeleteIntegration`).

## Data Models (SQLModel / PostgreSQL)

- **`WebhookEndpoint`** (`chronos/sql_models.py`): id, tc_id (unique), name, branch_id, webhook_url, api_key, active, timestamp.
- **`WebhookLog`**: id, request_headers/body, response_headers/body (JSONB), status, status_code, timestamp, webhook_endpoint_id (FK). Written by the worker after each delivery attempt.

DB session: `chronos/db.py` – engine from `pg_dsn` (or `test_pg_dsn` when `settings.testing`). Tables created via `init_db()` (used by `chronos/scripts/create_db_tables.py`, which requires `dev_mode`).

## Flow: From TC Request to Client Delivery

1. TC sends a webhook to `POST /send-webhook-callback` (or with `url_extension`). Views validate body as `TCWebhook` or `TCPublicProfileWebhook` and check Bearer token.
2. **Enqueue**:
   - If **round-robin**: `dispatch_branch_task(task_send_webhooks, branch_id, payload, url_extension)` is called. For event-style payloads, one job per event is enqueued to the branch's Redis list; otherwise one job. The **dispatcher** later pops from these lists and calls `task_send_webhooks.apply_async(...)` into the Celery queue.
   - If **no round-robin**: `task_send_webhooks.delay(payload_json, url_extension)` is called directly.
3. **Worker** runs `task_send_webhooks`: loads payload, resolves `branch_id`, loads all active `WebhookEndpoint` rows for that branch, then runs `_async_post_webhooks` (async HTTP with httpx). For each endpoint it signs the payload (HMAC-SHA256 with endpoint's api_key), POSTs to endpoint's URL (with optional `url_extension` path), and builds `WebhookLog` rows. Batch events are split into one request per event for compatibility. Logs are written to the DB in the same worker run.
4. **Cleanup**: An APScheduler job runs every hour and, with a Redis lock (`DELETE_JOBS_KEY`), enqueues a Celery task `_delete_old_logs_job` that deletes `WebhookLog` rows older than 15 days (in batches of 4999).

## Round-Robin Dispatcher and Job Queue

- **`chronos/tasks/queue.py`**: `JobQueue` uses Redis:
  - Per-branch list: `jobs:branch:{branch_id}`.
  - Set of active branches: `jobs:branches:active`.
  - Cursor: `jobs:dispatcher:cursor` (last branch that was served).
  - `enqueue(task_name, branch_id, **kwargs)` pushes a `JobPayload` (task_name, branch_id, kwargs, enqueued_at, trace_context) and adds branch to the set.
  - `peek(branch_id)`, `ack(branch_id)`, `get_active_branches()`, `get_cursor()` / `set_cursor()`, `get_celery_queue_length()` (length of Redis list `celery`).

- **`chronos/tasks/dispatcher.py`**: `dispatch_cycle(batch_limit)` runs one round: get active branches, rotate by cursor (bisect_right), for each branch peek → validate → `task.apply_async(kwargs)` → ack → set cursor, until batch_limit or no more branches. Poison payloads (e.g. invalid JSON) are acked and skipped. Trace context is restored when dispatching.

- **`chronos/worker.py`**: `job_dispatcher_task` runs in a loop: if no active jobs, sleep idle_delay; if Celery queue length ≥ `dispatcher_max_celery_queue`, sleep cycle_delay; else run `dispatch_cycle()` then sleep cycle_delay. Uses `acks_late=False` so the broker doesn't redeliver this never-ending task.

- **`chronos/tasks/worker_startup.py`**: On `worker_ready`, if the worker consumes the `dispatcher` queue, it starts `job_dispatcher_task.apply_async(countdown=60)`.

Dispatcher must be run with `--soft-time-limit=0 --time-limit=0` so it never times out (see `Procfile` / `Makefile`).

## Configuration (Settings)

`chronos/settings.py` uses pydantic-settings with `.env`. Important options:

- **Dev/Test**: `testing`, `dev_mode`, `log_level`, `on_beta`.
- **Databases**: `pg_dsn`, `test_pg_dsn`, `redis_url`.
- **Auth**: `tc2_shared_key` (Bearer token for TC).
- **Observability**: `logfire_token`, `sentry_dsn`.
- **Round-robin**: `use_round_robin`; `dispatcher_max_celery_queue`, `dispatcher_batch_limit`, `dispatcher_cycle_delay_seconds`, `dispatcher_idle_delay_seconds`.
- **HTTP client**: `webhook_http_timeout_seconds`, `webhook_http_max_connections`.

CORS: in production, origins are beta or secure TutorCruncher; in `dev_mode`, `*`.

## Running the Project

- **Local**: `make install-dev`, set `.env` (e.g. `pg_dsn`, `test_pg_dsn`, `dev_mode=True`), `make reset-db`, `make run-server-dev`, `make run-worker`, and optionally `make run-dispatcher` if using round-robin.
- **Production (e.g. Render)**: Web and worker (and dispatcher) as separate services; build `make install`, start with `make run-server` / `make run-worker` / dispatcher command from Procfile. Environment: `HOST`, `PORT`, `pg_dsn`, `redis_url`, `tc2_shared_key`, `logfire_token`, etc.

## Testing

- **pytest** in `tests/`, with `TESTING=True` so the app uses `test_pg_dsn`.
- **conftest.py**: Session-scoped engine and table create/drop; per-test transaction rollback; `client` fixture overrides `get_session` with test session; `clear_job_queue` clears Redis job queues after each test; Celery pytest plugin for broker/backend.
- Run: `make test` (runs pytest with coverage). CI (`.github/workflows/main.yml`) runs lint and tests on push/PR; deploy on tags via Render deploy hooks.

## Observability

- **Logfire**: If `logfire_token` is set, `chronos/observability.py` configures Logfire and instruments FastAPI, Celery, psycopg, requests, pydantic. Used in both web and worker.
- **Sentry**: If `sentry_dsn` is set, Sentry is initialized in `chronos/main.py` for the web process.
- Worker processes dispose the DB engine on fork (`worker_process_init`) and optionally instrument the worker (Logfire).

## Webhook Retries and Disable-on-Failure

### Retry logic

Retries are non-blocking: on a retryable failure, `task_retry_single_webhook` is enqueued via Celery `apply_async(..., countdown=backoff)` rather than blocking the worker.

**What is retried:**
- No response / timeout — server unreachable or too slow, likely transient.
- `429 Too Many Requests` — server asked us to back off.
- `5xx` — server-side error, likely transient (restart, deploy, overload).

**What is NOT retried:**
- `2xx` — success.
- `3xx` — redirect. We do not follow POST redirects; the customer should update the endpoint URL.
- `4xx` (except 429) — client error (wrong URL, auth failure, resource gone). Retrying the same request won't help. Persistent 4xx failures are caught by the auto-disable threshold instead.

**Backoff schedule** (configurable via settings):
- Attempt 1 → wait 1 s → attempt 2
- Attempt 2 → wait 10 s → attempt 3
- Attempt 3 → wait 100 s → attempt 4
- Attempt 4 → wait 1000 s → attempt 5
- Stop after 4 retries.

Settings: `webhook_retry_backoff_base_seconds` (1.0), `webhook_retry_backoff_multiplier` (10.0), `webhook_retry_max_attempts` (4).

### Disable-on-failure

After each delivery batch, Chronos checks `_check_and_disable_endpoint_if_needed` for every endpoint that had a failure. If **>20%** of attempts in the last 60 minutes are non-`Success` **and** there have been at least 10 attempts in that window, `WebhookEndpoint.active` is set to `False`. On disable, Chronos POSTs to `tc2_endpoint_disabled_url` (if set) so TC2 can surface the event to the customer. Endpoints whose `webhook_url` host is `tutorcruncher.com` or `*.tutorcruncher.com` are skipped (never auto-disabled).

Settings: `webhook_disable_failure_rate_threshold` (0.20), `webhook_disable_min_attempts` (10), `webhook_disable_failure_window_minutes` (60), `tc2_endpoint_disabled_url`.

## File Map (Summary)

| Path | Role |
|------|------|
| `chronos/main.py` | FastAPI app, CORS, Sentry, Logfire, router mount, cron router. |
| `chronos/views.py` | All HTTP endpoints; auth; enqueue or direct Celery for webhooks. |
| `chronos/worker.py` | Celery app, Redis cache, JobQueue, `task_send_webhooks`, `_delete_old_logs_job`, `job_dispatcher_task`, `dispatch_branch_task`, APScheduler lifespan and delete-old-logs trigger. |
| `chronos/tasks/dispatcher.py` | `dispatch_cycle()` – round-robin from Redis branch queues to Celery. |
| `chronos/tasks/queue.py` | `JobQueue` – Redis per-branch lists and Celery queue length. |
| `chronos/tasks/worker_startup.py` | Start dispatcher task when dispatcher worker is ready. |
| `chronos/sql_models.py` | `WebhookEndpoint`, `WebhookLog`. |
| `chronos/pydantic_schema.py` | Request/response models for API and job payloads. |
| `chronos/db.py` | Engine, `init_db`, `get_session`. |
| `chronos/settings.py` | Pydantic Settings. |
| `chronos/observability.py` | Logfire configure and instrument. |
| `chronos/scripts/create_db_tables.py` | Calls `init_db()` when `dev_mode`. |
| `chronos/scripts/mock_webhook_receiver.py` | Local FastAPI receiver for manual lab (`/hook`, `/lab-ext/{segment}`, TC2 notify route). |
| `chronos/scripts/webhook_retry_disable_lab.py` | Manual lab: `python -m chronos.scripts.webhook_retry_disable_lab` runs all retry/disable checks in one go; `--runbook` prints setup only. |

## Updating CLAUDE.md

After adding or materially changing Chronos code (behavior, HTTP API, settings, worker or dispatcher logic, data models, observability, or operational scripts such as under `chronos/scripts/`), **review this file** and update any sections that are now inaccurate or missing. Prefer small, targeted edits to the relevant subsection. Skip updates for trivial changes that do not affect how Chronos works (typos, formatting-only refactors, renames with no semantic change).

Use this document to reason about where to add or change behavior (e.g. new webhook types, new endpoints, or dispatcher tuning).
