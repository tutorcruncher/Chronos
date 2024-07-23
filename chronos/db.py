# Need to import the models to be able to create them in the DB
from sqlmodel import Session, SQLModel, create_engine

import chronos.sql_models  # noqa: F401
from chronos.utils import settings


def get_engine():
    dsn_settings = settings.test_pg_dsn if settings.dev_mode else settings.pg_dsn
    return create_engine(dsn_settings, echo=settings.dev_mode)


engine = get_engine()


def init_db(_engine=engine):
    SQLModel.metadata.create_all(_engine)


def get_session():
    if settings.dev_mode:
        init_db(engine)

        connection = engine.connect()
        transaction = connection.begin()
        with Session(bind=connection) as session:
            yield session
        session.close()
        transaction.rollback()
        connection.close()
        engine.dispose()
    else:
        with Session(engine) as db:
            yield db
