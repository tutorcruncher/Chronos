# TC - Hermes  
  
![hermes_v2_favicon (1)](https://github.com/tutorcruncher/hermes/assets/70067036/2019e4cf-056e-4e85-9694-5fa0b76dd2a4)

  
Hermes is our sales system that manages the connection between TutorCruncher (the system), the callbooker on our   
website, and Pipedrive (our current sales CRM).  
  
This system is built using TortoiseORM and FastAPI.
  
## Glossary  
  
Objects are named different things depending on which system you use:  
  
| Hermes   | TutorCruncher | Pipedrive    | Description                                                       |  
|----------|---------------|--------------|-------------------------------------------------------------------|  
| Company  | Cligency      | Organisation | A business that is a potential/current customer of TutorCruncher. |  
| Contact  | SR            | Person       | Someone who works for the Company.                                |  
| Pipeline |               | Pipeline     | The sales pipelines                                               |  
| Stage    |               | Stage        | The stages of the pipelines                                       |  
| Deal     |               | Deal         | A potential sale with a Company.                                  |  
| Meeting  |               | Activity     | A meeting with a Contact.                                         |

  
## Project structure  
  
This project consists of 4 apps:  
- `admin`: Used as an admin interface to deal with configuration  
- `callbooker`: Deals with callbacks and availability from the callbooker on the website  
- `pipedrive`: Deals with callbacks and data sync to Pipedrive  
- `tc2`: Deals with callbacks and data sync to Pipedrive
- `hermes` : Deals with data to do with the entire Hermes system

## Running locally  

Simply install requirements with `make install` and run the app with `uvicorn app.main:app --reload`. You'll be able to see the server running at `https://localhost:8000`.

Since Hermes works with tutorcruncher.com, the TC2 system and Pipedrive, there is quite a lot to set up to get the system working:

### TC2:  - Set this up first
  
Run TC2 locally on port 5000. `python manage.py runserver 5000`
  
#### TC2 Environment Variables
```  
# Hermes
HERMES_URL = 'https://${your ngrok domain}'  
HERMES_API_KEY = ${tc2 api integration private key} # same as tc2_api_key in hermes env vars
```  

#### Create a Meta Admins:  
Create 5 Admins in TC2, for each of the following:
- payg / startup sales
- enterprise sales
- support 1
- support 2
- bdr
  
#### Meta Client Custom Fields:
```  
pipedrive_url: str (Long Textbox)          # url of the Organization in pipedrive
pipedrive_id: int (Number)                 # id of the Organization in pipedrive
pipedrive_deal_stage: str (Long Textbox)   # name of the stage of the deal in pipedrive
pipedrive_pipeline: str (Long Textbox)     # name of the pipeline of the deal in pipedrive i.e Startup 
```  
  
#### Add API Integration to META:  

Navigate to ... > Settings > API Integrations > Create API Integration

- Name: `Hermes`  
- URL: `http://localhost:8000/tc2/callback/`  

this will create a private key, copy this and set it as `tc2_api_key` in hermes env vars and `HERMES_API_KEY` in tc2 env vars.
  

### Hermes:

Install the dependencies with `make install`.   
You may need to create the database with `make reset-db`.  
  
Then run the server with `python -m uvicorn app.main:app --reload`  
  
You'll be able to view the admin interface at http://localhost:8000/.   

To first create an admin, go to `/init` to create an admin user.
Then go to `/login` to log in.
  
#### Hermes Environment Variables
```  
# TC2  
tc2_api_key={tc2 api integration private key}          # can be found in tc2 meta > settings > api
tc2_base_url='http://localhost:5000'                    # url of your tc2 instance
  
# Pipedrive  
pd_api_key={pipedrive api key}                         # can be found in pipedrive > settings > personal preferences > api  
pd_base_url='https://seb-sandbox2.pipedrive.com'        # your pipedrive sandbox url
```

#### Create a Hermes Admins:
Create 5 Admins in Hermes, for each of the following:
- payg / startup sales
- enterprise sales
- support 1
- support 2
- bdr

When creating the admin, ensure that the `email` matches their Google account email address, and that the `tc2_admin_id` matches the id of the admin in TC2. and that the `pd_user_id` matches the id of the user in Pipedrive.

### Ngrok:  
We use ngrok to expose our local hermes server to the internet, so that we can receive webhooks from Pipedrive.
 
run ngrok on port 8000  
  
`ngrok http --${your ngrok domain} 8000`  
  
set the `HERMES_URL` env var to the ngrok url provided.

HINT: if you create a account with ngrok, it will give you a static url that you can use for the webhooks, that will never expire ;)
  
### Pipedrive:

Pipedrive is our current sales CRM. We use it to manage our sales pipelines and deals.

Create a pipedrive sandbox account.  
Navigate to Profile > Tools and apps > Webhooks > Create 6 new webhooks:
- Event action: `*`
- Event object: `deal`, `organization`, `pipeline`, `person`, `product`, `stage`, `user`
- Endpoint URL: `https://${your ngrok domain}/pipedrive/callback/`
- HTTP Auth: `None`  
  
#### Setting the Data Fields:  
Navigate to Company Settings > Data Fields

###### Organization Custom Fields:
```  
website  
paid_invoice_count  
has_booked_call  
has_signed_up  
tc2_status  
tc2_cligency_url
hermes_id
bdr_person_id
utm_campaign
utm_source
estimated_income
currency
```  
###### Person Custom Fields:
```
hermes_id
```
###### Deal Custom Fields:
```
hermes_id
```

#### Pipedrive Users:
Get your Pipedrive Owner ID for your Hermes Admins:  
- Navigate to ... > User Overview > select your user  
- Copy the number at the end of the URL  

### Return to Hermes for the next steps:
#### Config Tab  
  
- Navigate to the Hermes Config tab in the admin interface  
  
Edit Hermes config in the admin interface:  
- Set Price Plan Pipelines to their associated Hermes Pipeline ID i.e ()  
- Set Pipeline `dft_entry_pipeline_state` (warning, dropdown is not filtered by pipeline)  
Create Admin:  
- Create an admin user in the admin interface  
- ensure the admins email address and `tc_admin_id` match the ones in TC2  
  
#### Pipelines Tab
  
Create a pipeline for each of the following:  
- `PAYG`  
- `STARTUP`  
- `ENTERPRISE`  
  
#### Setup Callbooker:  
  
in order for the callbooker to work on tutorcruncher.com, you need to set the following env vars:  
- set `DEV_MODE=True` in `.env`  
- set `G_PRIVATE_KEY` in `.env` to the private key of the google service account  
- set `G_PRIVATE_KEY_ID` in `.env` to the private key id of the google service account  
  
Ensure admin has a matching email address to the one in the sales or support team (i.e fionn@tutorcruncher.com)  
  
##### Edit in tutorcruncher.com
- `sales_reps.yml` and `support_reps.yml`, `hermes_admin_id` to match the sales and support admins ids in hermes.
- set `HERMES_URL` = `http://localhost:8000`


## Testing  

Unittests can be run with `make test`. You will need to install the test dependencies with `make install-dev` first.

### Testing with live data

to test with live data, run `make restore-from-live` to download and restore the live database to your local machine.

also add your `{your ngork url}/pipedrive/callback` to TutorCruncher Pipedrive webhooks,

then you should see all the webhooks and responses in both your local nrork site and any changes you make in pipedrive will be reflected in your local database.

## Migrations

We use `aerich` to create and run migrations.

If you have changed a model then you can create migrations with `aerich migrate`. A new file will be created in the `migrations/models` directory. If you need to run any migrations, use `aerich upgrade`.

If you mess up your migrations, you can reset the database with `make reset-db`. Delete the migrations folder and run `aerich init-db` to recreate the migrations folder and create the initial migration.

More details can be found in the aerich docs.

## Deploying to Heroku  
Create Meta Admins for the sales and support teams in TC2, and heremes, ensuring their tc2_admin_id matches the one in hermes.  
  
