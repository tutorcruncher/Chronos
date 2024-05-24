import logging.config
import os

import logfire
import sentry_sdk
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi_admin.app import app as admin_app
from logfire import PydanticPlugin
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette.middleware.cors import CORSMiddleware
from tortoise.contrib.fastapi import register_tortoise

from app.admin import resources, views  # noqa: F401
from app.admin.auth import AuthProvider
from app.base_schema import build_custom_field_schema
from app.callbooker.views import cb_router
from app.views import main_router
from app.logging import config
from app.pipedrive.views import pipedrive_router
from app.settings import Settings
from app.tc2.views import tc2_router

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_app_settings = Settings()
if _app_settings.sentry_dsn:
    sentry_sdk.init(
        dsn=_app_settings.sentry_dsn,
    )

app = FastAPI()

allowed_origins = ['*']
# allowed_origins = ['https://secure.tutorcruncher.com']
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


@app.on_event('startup')
async def _startup():
    pass
    # from app.models import Admin
    # from app.utils import get_redis_client
    #
    # await admin_app.configure(
    #     template_folders=[os.path.join(BASE_DIR, 'admin/templates/')],
    #     providers=[AuthProvider(Admin)],
    #     language_switch=False,
    #     redis=await get_redis_client(),
    #     admin_path='',
    #     favicon_url='/assets/favicon.ico',
    # )
    # from app.utils import get_config
    #
    # await get_config()
    # await build_custom_field_schema()