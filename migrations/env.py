import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, pool
from sqlmodel import SQLModel

sys.path.insert(0, str(Path(__file__).parent.parent))  # make the repo root importable (no alembic.ini)

import chronos.sql_models  # noqa: E402, F401  (registers the tables on SQLModel.metadata)
from alembic import context  # noqa: E402
from chronos.utils import settings  # noqa: E402

config = context.config

# SQLModel models register their tables on SQLModel.metadata; autogenerate diffs against it.
target_metadata = SQLModel.metadata

logging.getLogger('alembic').setLevel(logging.WARNING)


def _database_url() -> str:
    """The DSN to migrate — the test DSN under TESTING, else the live one."""
    return settings.test_pg_dsn if settings.testing else settings.pg_dsn


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL against a URL, no DBAPI needed)."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (against a live connection)."""
    connectable = create_engine(_database_url(), poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
