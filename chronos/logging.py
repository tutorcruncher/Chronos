import logging.config

from chronos.utils import settings

logging_level = settings.log_level


def _logging_dict_config():
    handlers = {
        'default': {'formatter': 'default', 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stderr'},
        'access': {'formatter': 'access', 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stdout'},
    }
    chronos_handlers = ['default']
    uvicorn_handlers = ['default']
    if settings.logfire_token:
        handlers['logfire'] = {'class': 'logfire.LogfireLoggingHandler'}
        chronos_handlers.append('logfire')
        uvicorn_handlers.append('logfire')

    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'defaults': 'uvicorn.logging.DefaultFormatter',
                'fmt': '%(levelprefix)s %(message)s',
                'use_colors': None,
            },
            'access': {
                'defaults': 'uvicorn.logging.AccessFormatter',
                'fmt': "%(levelprefix)s %(client_addr)s - '%(request_line)s' %(status_code)s",  # noqa: E501
            },
        },
        'handlers': handlers,
        'loggers': {
            'chronos': {'handlers': chronos_handlers, 'level': logging_level, 'propagate': False},
            'uvicorn': {'handlers': uvicorn_handlers, 'level': logging_level, 'propagate': False},
            'uvicorn.error': {'level': logging_level},
            'uvicorn.access': {'handlers': ['access'], 'level': logging_level, 'propagate': False},
        },
    }


def setup_logging():
    logging.config.dictConfig(_logging_dict_config())
