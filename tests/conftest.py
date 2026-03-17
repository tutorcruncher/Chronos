from urllib.parse import urlparse

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlmodel import Session, SQLModel, create_engine

import chronos.sql_models  # noqa: F401
from chronos.db import get_session
from chronos.main import app
from chronos.utils import settings
from chronos.worker import job_queue

pytest_plugins = ('celery.contrib.pytest',)


def _ensure_test_database_exists(db_url):
    """Create the test database if it doesn't exist."""
    parsed = urlparse(db_url)
    db_name = parsed.path.lstrip('/') or 'test_chronos'
    maintenance_url = db_url.rsplit('/', 1)[0] + '/postgres'
    temp_engine = create_engine(maintenance_url, isolation_level='AUTOCOMMIT', poolclass=NullPool)
    with temp_engine.connect() as conn:
        result = conn.execute(
            text('SELECT 1 FROM pg_database WHERE datname = :dbname'),
            {'dbname': db_name},
        ).fetchone()
        if not result:
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    temp_engine.dispose()


@pytest.fixture(scope='session')
def engine():
    _ensure_test_database_exists(settings.test_pg_dsn)
    return create_engine(settings.test_pg_dsn, echo=True)


@pytest.fixture(scope='session')
def create_tables(engine):
    SQLModel.metadata.create_all(engine)
    yield
    SQLModel.metadata.drop_all(engine)


@pytest.fixture
def session(engine, create_tables):
    connection = engine.connect()
    transaction = connection.begin()
    with Session(bind=connection) as session:
        yield session
    transaction.rollback()
    connection.close()
    session.close()


@pytest.fixture(name='client')
def client_fixture(session: Session):
    def get_session_override():
        return session

    app.dependency_overrides[get_session] = get_session_override

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def clear_job_queue():
    yield
    job_queue.clear_all()


@pytest.fixture(scope='session')
def celery_config():
    return {'broker_url': 'redis://', 'result_backend': 'redis://'}


@pytest.fixture(scope='session')
def celery_enable_logging():
    return True
