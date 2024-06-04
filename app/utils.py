import hashlib
import logging

from app.settings import Settings


settings = Settings()
app_logger = logging.getLogger('base')


async def sign_args(*args):
    s = settings.signing_key + ':' + '-'.join(str(a) for a in args if a)
    return hashlib.sha256(s.encode()).hexdigest()


def get_bearer(auth: str):
    try:
        return auth.split(' ')[1]
    except (AttributeError, IndexError):
        return
