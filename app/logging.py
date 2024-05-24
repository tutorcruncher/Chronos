from app.utils import settings

logging_level = settings.log_level

config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {'()': 'uvicorn.logging.DefaultFormatter', 'fmt': '%(levelprefix)s %(message)s', 'use_colors': None},
        'access': {
            '()': 'uvicorn.logging.AccessFormatter',
            'fmt': "%(levelprefix)s %(client_addr)s - '%(request_line)s' %(status_code)s",  # noqa: E501
        },
    },
    'handlers': {
        'default': {'formatter': 'default', 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stderr'},
        'access': {'formatter': 'access', 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stdout'},
    },
    'loggers': {
        'hermes': {'handlers': ['default'], 'level': logging_level, 'propagate': False},
        'uvicorn': {'handlers': ['default'], 'level': logging_level, 'propagate': False},
        'uvicorn.error': {'level': logging_level},
        'uvicorn.access': {'handlers': ['access'], 'level': logging_level, 'propagate': False},
    },
}
