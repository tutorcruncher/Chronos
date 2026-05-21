import logging

import logfire
from logfire.integrations.logging import LogfireLoggingHandler

from chronos.utils import settings

_configured = False


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


def _attach_logfire_handler():
    """Bridge Python stdlib logging to Logfire for the 'chronos' logger tree.

    Sets an explicit level so Celery's --loglevel=error on the root logger
    does not filter out INFO/WARNING records before they reach the handler.
    """
    chronos_logger = logging.getLogger('chronos')
    chronos_logger.setLevel(logging.INFO)
    if not any(isinstance(h, LogfireLoggingHandler) for h in chronos_logger.handlers):
        chronos_logger.addHandler(LogfireLoggingHandler())


def instrument_worker():
    configure_logfire()
    _attach_logfire_handler()
    logfire.instrument_celery()
    logfire.instrument_httpx()
    logfire.instrument_psycopg()


def instrument_web_app(app):
    configure_logfire()
    _attach_logfire_handler()
    logfire.instrument_fastapi(app)
    logfire.instrument_celery()
    logfire.instrument_httpx()
    logfire.instrument_psycopg()
