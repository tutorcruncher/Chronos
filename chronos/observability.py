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

    Sets the logger level to INFO so Celery's --loglevel=error on the root
    logger does not filter records before they reach any handler. To avoid
    flooding stderr with INFO in production (where the StreamHandler from
    dictConfig expects the level set by settings.log_level), the existing
    stderr handlers are given an explicit level matching the configured one.
    """
    chronos_logger = logging.getLogger('chronos')
    for h in chronos_logger.handlers:
        if h.level == logging.NOTSET:
            h.setLevel(settings.log_level.upper())
    chronos_logger.setLevel(logging.INFO)
    # Guard against duplicate handlers: instrument_worker() and instrument_web_app()
    # can both call this, and Celery's prefork pool may fire worker_process_init
    # more than once in edge cases (e.g. pool restart).
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
