import logging
import os

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from chronos.logging import setup_logging
from chronos.observability import init_sentry
from chronos.utils import settings as _app_settings

init_sentry(for_celery_worker=False)

from chronos.views import main_router  # noqa: E402
from chronos.worker import cronjob, lifespan  # noqa: E402

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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

setup_logging()
# Remove excessive sqlalchemy logging
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.engine.Engine').disabled = True
app.include_router(main_router, prefix='')
app.include_router(cronjob, prefix='')
