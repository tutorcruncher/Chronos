import logging

import logfire
import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from chronos.utils import settings

_configured = False


def init_sentry(*, for_celery_worker: bool = False):
    if not settings.sentry_dsn:
        return
    integrations = [
        LoggingIntegration(level=logging.INFO, event_level=logging.WARNING),
    ]
    if for_celery_worker:
        integrations.append(CeleryIntegration())
    sentry_sdk.init(dsn=settings.sentry_dsn, integrations=integrations)


def configure_logfire():
    global _configured
    if _configured:
        return
    logfire.configure(
        service_name='chronos',
        token=settings.logfire_token,
        send_to_logfire=True,
        console=False,
        distributed_tracing=True,
    )
    _configured = True


def instrument_worker():
    configure_logfire()
    logfire.instrument_celery()
    logfire.instrument_pydantic(record=settings.logfire_log_level)
    logfire.instrument_psycopg()
    logfire.instrument_requests()


def instrument_web_app(app):
    configure_logfire()
    logfire.instrument_fastapi(app)
    logfire.instrument_celery()
    logfire.instrument_pydantic(record=settings.logfire_log_level)
    logfire.instrument_psycopg()
    logfire.instrument_requests()
