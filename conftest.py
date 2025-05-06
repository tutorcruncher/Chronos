import pytest
from sqlmodel import Session, create_engine, SQLModel
from fastapi.testclient import TestClient
import chronos.sql_models  # noqa: F401

from chronos.db import get_session
from chronos.main import app
from chronos.settings import Settings
from chronos.utils import settings

pytest_plugins = ('celery.contrib.pytest',)


@pytest.fixture(scope='session')
def engine():
    return create_engine(settings.test_pg_dsn, echo=True)


@pytest.fixture(scope='session')
def create_tables(engine):
    SQLModel.metadata.create_all(engine)
    yield
    SQLModel.metadata.drop_all(engine)



@pytest.fixture(scope='session')
def celery_includes():
    return ['chronos.worker']

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
    return {
        'broker_url': 'redis://',
        'result_backend': 'redis://',
        'task_always_eager': True,
        'task_eager_propagates': True,
    }


@pytest.fixture(scope='session')
def celery_enable_logging():
    return True
