import logging.config
import os

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
    from chronos.observability import instrument_web_app

    instrument_web_app(app)

logging.config.dictConfig(config)
# Remove excessive sqlalchemy logging
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.engine.Engine').disabled = True
app.include_router(main_router, prefix='')
app.include_router(cronjob, prefix='')
