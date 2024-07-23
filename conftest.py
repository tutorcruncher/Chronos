import pytest
from sqlalchemy import NullPool
from sqlmodel.pool import StaticPool
from sqlmodel import Session, create_engine, SQLModel
from fastapi.testclient import TestClient
import app.sql_models  # noqa: F401

from app.db import init_db, get_session, get_engine
from app.main import app
from app.utils import settings
from app.worker import celery_app as cel_app


pytest_plugins = ('celery.contrib.pytest',)


@pytest.fixture(scope='session')
def engine():
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


@pytest.fixture(scope='session')
def celery_config():
    return {'broker_url': 'redis://', 'result_backend': 'redis://'}


@pytest.fixture(scope='session')
def celery_enable_logging():
    return True
