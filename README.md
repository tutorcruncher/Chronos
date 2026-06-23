# TC - Chronos  
  
Welcome to TC - Chronos.
This is a microservice that takes on all the webhook work from TC. Before creation of Chronos, about 50% of the database was taken up by webhook logs & failed deliveries are causing some slowness in jobs etc. To battle this we will simply send 1 request from TC2 to Chronos per webhook then let chronos manage the rest.

# Functionality
This means the Chronos needs some certain functionality:
* An object for storing endpoints where our clients wish webhooks to be sent to
* An object for storing information about the webhook logs

* An endpoint for CRUD operations on the Endpoint objects
* An endpoint for retrieving webhook logs to be displayed in TC2
* An endpoint for distributing webhooks to all Endpoints related to a branch
* Delete-endpoint requests are acknowledged quickly and cleaned up asynchronously on the worker

* A job that runs periodically deleting all logs older than 15 days
* Large webhook-log deletes are processed in batches to avoid overwhelming Postgres

# SetUp Local
To set up the Chronos system locally follow these steps:

1. Create a virtual environment
2. Install the requirements by running `make install-dev`
3. Create a `.env` file in the root of the project with the following content:

Set pg_dsn and test_pg_dsn 
You will need to export dev_mode = True to create tables

4. Create the DB in Postgres by calling `make reset-db` (drops/creates the DBs and runs `make migrate`)
5. Start the server using `make run-server-dev`
6. Start the celery worker with `make run-worker`

# Database migrations

The schema is managed with **Alembic**. There is no `alembic.ini` — config lives in `pyproject.toml`
under `[tool.alembic]` (`script_location = "migrations"`), which Alembic 1.18 discovers automatically.
Migrations live in `migrations/versions/` with timestamped filenames. `migrations/env.py` reads the
DSN from `chronos.settings` (`pg_dsn`, or `test_pg_dsn` when `TESTING`), so you don't pass `--url`.

* Apply all migrations: `make migrate` (i.e. `alembic upgrade head`).
* After changing a model in `chronos/sql_models.py`, generate a migration against a local DB:
  `uv run alembic revision --autogenerate -m "describe the change"`, then review the generated file
  before committing (autogenerate is a starting point, not gospel).
* `tests/test_migrations.py` applies every migration to a throwaway DB and fails if the result
  doesn't match the models — so a model change without its migration breaks CI.

> **Adopting Alembic on a pre-existing live DB:** if the `webhookendpoint`/`webhooklog` tables were
> created by the old `create_all` path (no `alembic_version` table yet), baseline the DB once with
> `alembic stamp head` instead of `alembic upgrade head`, so Alembic records the current revision
> without trying to re-create existing tables. Fresh databases just use `make migrate`.

# SetUp a new live system
To set up the Chronos system in render follow these steps:

1. Create a render project with the following services:
    * A Postgres database
    * A Redis database
    * A web service
    * A worker service
2. Setup web service:
    * Set to build from this repo and deploy from master branch
    * Set `make install` as the build command
    * Set `make migrate` as the **pre-deploy command** so migrations run against the live DB before
      the new code starts serving (Render runs the pre-deploy command once, after build, before the
      new instances go live — set it on the web service only so it runs a single time per deploy)
    * Set `make run-server` as the start command
3. Setup worker service:
    * Set to build from this repo and deploy from master branch
    * Set `make install` as the build command
    * Set `make run-worker` as the start command
4. Setup a Postgres instance
5. Setup a Redis instance
6. Return to environment variables on web service and set the following:
    * HOST
    * PORT
    * logfire_token
    * tc2_shared_key
7. Setup shared environment variables from internally created postgres and redis instance
    * pg_dsn
    * redis_url

# Bobbin webhooks

Chronos also serves the Bobbin (bobbin-api) product's outbound webhooks via the `/bobbin/*`
routes (in `chronos/views/bobbin.py`). Bobbin shares TC2's `WebhookEndpoint` / `WebhookLog`
tables rather than having its own. A required `provider` column (`'tutorcruncher'` | `'bobbin'`)
discriminates the two products; the generic `org_id` column holds the TC2 branch **or** the Bobbin
organization id, and `bobbin_id` holds the bobbin-api endpoint id (`tc_id` stays NULL for Bobbin
rows). Senders filter on `provider`, so a TC2 branch and a Bobbin org that share an `org_id` integer
never cross-deliver.

Only the **ingest** differs per product: Bobbin has its own shared key (`bobbin_shared_key`), its own
routes, and enqueues directly (no round-robin). Everything downstream — the single `task_send_webhooks`
delivery task, signing, retries, auto-disable, and log cleanup — is shared. Auto-disable notifications
are per-provider: TC2 posts to `tc2_endpoint_disabled_url`, Bobbin to `bobbin_endpoint_disabled_url`
(unset ⇒ the endpoint is disabled silently).

When deploying to an existing live system:

> **⚠️ Migrate before you deploy.** The new code reads `provider`/`org_id`/`bobbin_id` on every send
> and CRUD route, but Chronos has no Alembic and `create_all` never `ALTER`s an existing table. The
> `branch_id`→`org_id` rename is **not backward-compatible with the currently-running code**: the old
> code references `branch_id`, the new code references `org_id`, so there is no column name that
> satisfies both. Render fires all deploy hooks together on a git tag with no release phase, so if you
> push the tag first the table still has `branch_id` and no `provider`/`bobbin_id` — every webhook
> delivery and endpoint CRUD 500s, and the `org_id` lookup sequential-scans until `uq_org_bobbin`
> exists. Run the DDL against prod **before** the tag, and do the cutover during low traffic (the old
> code breaks the moment `branch_id` is renamed, so the migrate→deploy gap is a brief downtime window).

1. **Manually migrate the live `webhookendpoint` table** against the prod DSN, before pushing the tag
   that triggers the deploy. Existing rows are all TC2, so the one-off `DEFAULT` backfills them
   correctly (the model field itself is required):
   - `ALTER TABLE webhookendpoint ADD COLUMN provider varchar NOT NULL DEFAULT 'tutorcruncher';`
   - `ALTER TABLE webhookendpoint RENAME COLUMN branch_id TO org_id;`
   - `ALTER TABLE webhookendpoint ADD COLUMN bobbin_id integer;`
   - `ALTER TABLE webhookendpoint ALTER COLUMN tc_id DROP NOT NULL;`
   - `ALTER TABLE webhookendpoint ADD CONSTRAINT uq_org_bobbin UNIQUE (org_id, bobbin_id);`

   (`webhooklog` is unchanged — Bobbin deliveries log into it as-is.)
2. Push the tag to deploy the code (the unified model lives in `chronos/sql_models.py`).
3. Set `bobbin_shared_key` (and optionally `bobbin_endpoint_disabled_url`) as environment variables on
   the **web and worker** services.
