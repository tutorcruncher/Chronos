# Need to import the models to be able to create them in the DB
from sqlmodel import Session, SQLModel, create_engine

import app.sql_models  # noqa: F401
from app.utils import settings

engine = create_engine(settings.pg_dsn, echo=settings.dev_mode)


def init_db(_engine=engine):
    SQLModel.metadata.create_all(_engine)


def get_session():
    with Session(engine) as db:
        yield db
