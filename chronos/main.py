import logging.config
import os

import logfire
import sentry_sdk
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from chronos.logging import config
from chronos.utils import settings as _app_settings
from chronos.views import main_router
from chronos.worker import cronjob, lifespan

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _app_settings.sentry_dsn:
    sentry_sdk.init(
        dsn=_app_settings.sentry_dsn,
    )

app = FastAPI(lifespan=lifespan)

if _app_settings.on_beta:
    allowed_origins = ['https://beta.tutorcruncher.com']
else:
    allowed_origins = ['https://secure.tutorcruncher.com']

if _app_settings.dev_mode:
    allowed_origins = ['*']

app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_methods=['*'], allow_headers=['*'])


if bool(_app_settings.logfire_token):
    logfire.configure(
        service_name='chronos',
        token=_app_settings.logfire_token,
        send_to_logfire=True,
        console=False,
    )
    logfire.instrument_fastapi(app)
    logfire.instrument_celery()
    logfire.instrument_pydantic(record=_app_settings.logfire_log_level)
    logfire.instrument_psycopg()
    logfire.instrument_requests()

logging.config.dictConfig(config)

app.include_router(main_router, prefix='')
app.include_router(cronjob, prefix='')
