from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


def make_engine(database_url: str):
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, future=True, connect_args=connect_args)


def make_session_factory(database_url: str) -> sessionmaker[Session]:
    return sessionmaker(bind=make_engine(database_url), expire_on_commit=False, future=True)


def create_schema(session_factory: sessionmaker[Session]) -> None:
    bind = session_factory.kw["bind"]
    Base.metadata.create_all(bind)


def session_dependency(session_factory: sessionmaker[Session]):
    def dependency() -> Generator[Session]:
        with session_factory() as session:
            yield session

    return dependency
