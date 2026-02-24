import logfire

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
