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

# SetUp
To set up the Chronos system locally follow these steps:

1. Create the DB in Postgres by calling `make reset-db`
2. Create the tables by calling `python -m app.scripts.create_db_tables
3. Start the server using `python -m uvicorn app.main:app --reload`
  
