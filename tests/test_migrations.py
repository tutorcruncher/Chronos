"""Guard that the Alembic migrations stay in sync with the SQLModel models.

Chronos has no autogenerate-on-startup; the migrations are the source of truth for the live
schema. This test applies every migration to a throwaway database and asserts the resulting
schema matches ``SQLModel.metadata`` exactly, so a model change committed without its migration
fails CI rather than silently diverging from production.
"""

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel

import chronos.sql_models  # noqa: F401  (registers the tables on SQLModel.metadata)
from alembic import command
from chronos.utils import settings


@pytest.fixture
def scratch_db_url():
    """Create an empty throwaway database, yield its URL, and drop it afterwards."""
    base = make_url(settings.test_pg_dsn)
    scratch_name = f'{base.database}_migration_check'
    admin_url = base.set(database='postgres')

    admin_engine = create_engine(admin_url, isolation_level='AUTOCOMMIT', poolclass=NullPool)
    with admin_engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{scratch_name}"'))
        conn.execute(text(f'CREATE DATABASE "{scratch_name}"'))
    admin_engine.dispose()

    try:
        yield base.set(database=scratch_name)
    finally:
        admin_engine = create_engine(admin_url, isolation_level='AUTOCOMMIT', poolclass=NullPool)
        with admin_engine.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{scratch_name}"'))
        admin_engine.dispose()


def test_migrations_match_models(scratch_db_url):
    config = Config('alembic.ini')
    config.set_main_option('sqlalchemy.url', str(scratch_db_url))
    command.upgrade(config, 'head')

    engine = create_engine(scratch_db_url, poolclass=NullPool)
    try:
        with engine.connect() as conn:
            context = MigrationContext.configure(conn)
            diffs = compare_metadata(context, SQLModel.metadata)
    finally:
        engine.dispose()

    assert diffs == [], f'Models and migrations have diverged — run `alembic revision --autogenerate`: {diffs}'
