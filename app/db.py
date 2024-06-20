# Need to import the models to be able to create them in the DB
import app.sql_models  # noqa: F401

from sqlmodel import create_engine, SQLModel, Session
from app.utils import settings


engine = create_engine(settings.pg_dsn, echo=settings.dev_mode)


def init_db():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as db:
        yield db
