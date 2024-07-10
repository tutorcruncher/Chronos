import pytest
from sqlalchemy import NullPool
from sqlmodel.pool import StaticPool
from sqlmodel import Session, create_engine
from fastapi.testclient import TestClient
import app.sql_models  # noqa: F401

from app.db import init_db, get_session
from app.main import app
from app.utils import settings


@pytest.fixture(name='session')
def session_fixture():
    engine = create_engine(settings.test_pg_dsn, echo=True, poolclass=NullPool)
    init_db(engine)

    connection = engine.connect()
    transaction = connection.begin()
    with Session(bind=connection) as session:
        yield session
    session.close()
    transaction.rollback()
    connection.close()
    engine.dispose()


@pytest.fixture(name='client')
def client_fixture(session: Session):
    def get_session_override():
        return session

    app.dependency_overrides[get_session] = get_session_override

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
