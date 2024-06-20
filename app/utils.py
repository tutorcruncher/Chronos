import hashlib
import logging

from app.settings import Settings


settings = Settings()
app_logger = logging.getLogger('base')


def get_bearer(auth: str):
    try:
        return auth.split(' ')[1]
    except (AttributeError, IndexError):
        return
