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
    logger does not filter records before they reach any handler.

    In the web process, dictConfig has already installed a StreamHandler and
    set propagate=False.  In worker processes dictConfig never runs, so
    propagate defaults to True and there are no handlers — without the guard
    below, every INFO record would propagate to Celery's root StreamHandler
    (which has level=NOTSET) and flood production stderr.
    """
    chronos_logger = logging.getLogger('chronos')

    if not chronos_logger.handlers:
        # Worker process: add a stderr handler at the configured level so
        # ERROR+ operational logs still appear on the console.
        stderr_handler = logging.StreamHandler()
        stderr_handler.setLevel(settings.log_level.upper())
        chronos_logger.addHandler(stderr_handler)
    else:
        # Web process: dictConfig installed a StreamHandler with level=NOTSET.
        # Pin it to the configured level so lowering the logger to INFO below
        # doesn't flood stderr.
        for h in chronos_logger.handlers:
            if h.level == logging.NOTSET and not isinstance(h, LogfireLoggingHandler):
                h.setLevel(settings.log_level.upper())

    chronos_logger.setLevel(logging.INFO)
    chronos_logger.propagate = False

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
