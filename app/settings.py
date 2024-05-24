from pathlib import Path
from typing import Optional

from pydantic import Field, PostgresDsn, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

THIS_DIR = Path(__file__).parent.resolve()


class Settings(BaseSettings):
    # Dev and Test settings
    testing: bool = False
    dev_mode: bool = False
    log_level: str = 'INFO'

    logfire_token: Optional[str] = None

    # Postgres
    pg_dsn: PostgresDsn = Field('postgres://postgres@localhost:5432/hermes', validation_alias='DATABASE_URL')

    # Redis
    redis_dsn: RedisDsn = Field('redis://localhost:6379', validation_alias='REDISCLOUD_URL')

    # Sentry
    sentry_dsn: Optional[str] = None

    dft_timezone: str = 'Europe/London'
    signing_key: str = 'test-key'
    host: str = '0.0.0.0'
    port: int = 8000
