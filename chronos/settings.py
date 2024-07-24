from pathlib import Path
from typing import Optional

from pydantic import RedisDsn
from pydantic_settings import BaseSettings

THIS_DIR = Path(__file__).parent.resolve()


class Settings(BaseSettings):
    # Dev and Test settings
    testing: bool = False
    dev_mode: bool = True
    log_level: str = 'INFO'
    on_heroku: bool = False

    logfire_token: Optional[str] = None

    # Postgres
    # pg_dsn: PostgresDsn = Field('postgres://postgres@localhost:5432/chronos', validation_alias='DATABASE_URL')
    pg_dsn: str = 'postgresql://postgres:postgres@localhost:5432/chronos'
    test_pg_dsn: str = 'postgresql://postgres:postgres@localhost:5432/test_chronos'

    # # Redis
    # redis_dsn: RedisDsn = Field('redis://localhost:6399', validation_alias='REDISCLOUD_URL')
    redis_url: RedisDsn = 'redis://localhost:6399'

    # Sentry
    sentry_dsn: Optional[str] = None

    dft_timezone: str = 'Europe/London'
    tc2_shared_key: str = 'test-key'
    host: str = '0.0.0.0'
    port: int = 8000
