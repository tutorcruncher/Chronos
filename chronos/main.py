import logging.config
import os

import logfire
import sentry_sdk
from fastapi import FastAPI
from logfire import PydanticPlugin
from starlette.middleware.cors import CORSMiddleware

from chronos.logging import config
from chronos.utils import settings as _app_settings
from chronos.views import main_router
from chronos.worker import cronjob

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _app_settings.sentry_dsn:
    sentry_sdk.init(
        dsn=_app_settings.sentry_dsn,
    )


app = FastAPI()

if _app_settings.on_beta:
    allowed_origins = ['https://beta.tutorcruncher.com']
else:
    allowed_origins = ['https://secure.tutorcruncher.com']

if _app_settings.dev_mode:
    allowed_origins = ['*']

app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_methods=['*'], allow_headers=['*'])


if bool(_app_settings.logfire_token):
    logfire.instrument_fastapi(app)
    logfire.instrument_celery()
    logfire.configure(
        send_to_logfire=True,
        token=_app_settings.logfire_token,
        pydantic_plugin=PydanticPlugin(record='all'),
    )
    logfire.instrument_psycopg()
    logfire.instrument_requests()

logging.config.dictConfig(config)

app.include_router(main_router, prefix='')
app.include_router(cronjob, prefix='')

COMMIT = os.getenv('HEROKU_SLUG_COMMIT', '-')[:7]
RELEASE_CREATED_AT = os.getenv('HEROKU_RELEASE_CREATED_AT', '-')
# logfire.info('starting app {commit=} {release_created_at=}', commit=COMMIT, release_created_at=RELEASE_CREATED_AT)
