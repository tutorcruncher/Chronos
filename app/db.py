from sqlmodel import create_engine, SQLModel, Session
from app.utils import settings


if settings.dev_mode:
    engine = create_engine(settings.pg_dsn, echo=True)
else:
    engine = create_engine(settings.pg_dsn)


def init_db():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as db:
        yield db
