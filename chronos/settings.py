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

    # Round-robin dispatcher feature flag
    # So the dispatcher mode can be turned off gradually if we want to
    # switch back to the previous mode of handling tasks
    use_round_robin: bool = True

    # Round-robin dispatcher tuning

    # Backpressure threshold. If the broker queue (`celery`) has 100 or more pending tasks, dispatcher pauses dispatching temporarily.
    dispatcher_max_celery_queue: int = 100
    # Maximum number of jobs dispatched in one round-robin cycle (default cycle runs every 10 ms)
    dispatcher_batch_limit: int = 100
    # Sleep duration between normal dispatcher cycles while there is work.
    dispatcher_cycle_delay_seconds: float = 0.01
    # Sleep duration when no active branches have queued jobs.
    dispatcher_idle_delay_seconds: float = 1.0

    # Webhook HTTP client tuning

    webhook_http_timeout_seconds: float = 8.0
    webhook_http_max_connections: int = 250

    # Read local env file for local variables
    model_config = SettingsConfigDict(env_file='.env', extra='allow')
