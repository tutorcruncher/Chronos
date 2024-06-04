import hashlib
import logging

import aioredis

from app.settings import Settings

settings = Settings()
app_logger = logging.getapp_logger('base')


async def sign_args(*args):
    s = settings.signing_key + ':' + '-'.join(str(a) for a in args if a)
    return hashlib.sha256(s.encode()).hexdigest()


def get_bearer(auth: str):
    try:
        return auth.split(' ')[1]
    except (AttributeError, IndexError):
        return


async def get_redis_client() -> 'aioredis.Redis':
    return aioredis.from_url(str(settings.redis_dsn))
