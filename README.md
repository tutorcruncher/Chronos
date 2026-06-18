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

4. Create the DB in Postgres by calling `make reset-db`
5. Start the server using `make run-server-dev`
6. Start the celery worker with `make run-worker`

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

1. Deploy the code (the unified model lives in `chronos/sql_models.py`).
2. **Manually migrate the live `webhookendpoint` table** — Chronos has no Alembic and `create_all`
   never `ALTER`s existing tables, so this is a one-off human step against the prod DSN. Existing rows
   are all TC2, so the one-off `DEFAULT` backfills them correctly (the model field itself is required):
   - `ALTER TABLE webhookendpoint ADD COLUMN provider varchar NOT NULL DEFAULT 'tutorcruncher';`
   - `ALTER TABLE webhookendpoint RENAME COLUMN branch_id TO org_id;`
   - `ALTER TABLE webhookendpoint ADD COLUMN bobbin_id integer;`
   - `ALTER TABLE webhookendpoint ALTER COLUMN tc_id DROP NOT NULL;`
   - `ALTER TABLE webhookendpoint ADD CONSTRAINT uq_org_bobbin UNIQUE (org_id, bobbin_id);`

   (`webhooklog` is unchanged — Bobbin deliveries log into it as-is.)
3. Set `bobbin_shared_key` (and optionally `bobbin_endpoint_disabled_url`) as environment variables on
   the **web and worker** services.
