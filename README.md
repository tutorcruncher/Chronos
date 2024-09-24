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

* A job that runs periodically deleting all logs older than 15 days

# SetUp Local
To set up the Chronos system locally follow these steps:

1. Create a virtual environment
2. Install the requirements by running `make install-dev`
3. Create a `.env` file in the root of the project with the following content:
```
PG_DSN = "postgresql://postgres:waffle@localhost:5432/chronos"
TEST_PG_DSN = "postgresql://postgres:waffle@localhost:5432/test_chronos"
DEV_MODE = True
```
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
