from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

THIS_DIR = Path(__file__).parent.resolve()


class Settings(BaseSettings):
    # Dev and Test settings
    testing: bool = False
    dev_mode: bool = False
    log_level: str = 'ERROR'
    on_beta: bool = False

    logfire_token: Optional[str] = None
    logfire_log_level: str = 'all'

    # Postgres
    pg_dsn: str = 'postgresql://postgres:postgres@localhost:5432/chronos'
    test_pg_dsn: str = 'postgresql://postgres:postgres@localhost:5432/test_chronos'

    # Redis
    redis_url: str = 'redis://localhost:6379/0'

    # Sentry
    sentry_dsn: Optional[str] = None

    dft_timezone: str = 'Europe/London'
    tc2_shared_key: str = 'test-key'

    # Read local env file for local variables
    model_config = SettingsConfigDict(env_file='.env', extra='allow')
