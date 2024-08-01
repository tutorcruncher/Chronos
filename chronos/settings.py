from pathlib import Path
from typing import Optional

from pydantic import RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

THIS_DIR = Path(__file__).parent.resolve()


class Settings(BaseSettings):
    # Dev and Test settings
    testing: bool = False
    dev_mode: bool = False
    log_level: str = 'INFO'
    on_heroku: bool = False

    logfire_token: Optional[str] = None
    logfire_ignore_no_config: int = 1

    # Postgres
    pg_dsn: str = 'postgresql://postgres:postgres@localhost:5432/chronos'
    test_pg_dsn: str = 'postgresql://postgres:postgres@localhost:5432/test_chronos'

    # Redis
    redis_url: RedisDsn = 'redis://localhost:6399'

    # Sentry
    sentry_dsn: Optional[str] = None

    dft_timezone: str = 'Europe/London'
    tc2_shared_key: str = 'test-key'

    # Do we need this?
    host: str = '0.0.0.0'
    port: int = 8000

    # Read local env file for local variables
    model_config = SettingsConfigDict(env_file='.env', extra='allow')
