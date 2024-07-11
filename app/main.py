import logging.config
import os

import logfire
import sentry_sdk
from fastapi import FastAPI
from logfire import PydanticPlugin
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette.middleware.cors import CORSMiddleware

from app.logging import config
from app.utils import settings as _app_settings
from app.views import main_router

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _app_settings.sentry_dsn:
    sentry_sdk.init(
        dsn=_app_settings.sentry_dsn,
    )


app = FastAPI()

allowed_origins = ['https://secure.tutorcruncher.com']
if _app_settings.dev_mode:
    allowed_origins = ['*']
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_methods=['*'], allow_headers=['*'])
if bool(_app_settings.logfire_token):
    logfire.instrument_fastapi(app)
    logfire.configure(
        send_to_logfire=True,
        token=_app_settings.logfire_token,
        pydantic_plugin=PydanticPlugin(record='all'),
    )

    FastAPIInstrumentor.instrument_app(app)

logging.config.dictConfig(config)

app.include_router(main_router, prefix='')

COMMIT = os.getenv('HEROKU_SLUG_COMMIT', '-')[:7]
RELEASE_CREATED_AT = os.getenv('HEROKU_RELEASE_CREATED_AT', '-')
# logfire.info('starting app {commit=} {release_created_at=}', commit=COMMIT, release_created_at=RELEASE_CREATED_AT)
