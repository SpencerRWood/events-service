"""Real PostgreSQL fixtures; unavailable dependencies fail rather than skip."""

from collections.abc import Iterator
from uuid import uuid4

import pytest
from pydantic import SecretStr
from scripts.isolated_postgres import isolated_postgres
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url

from wood_events_service.config import ProducerCredential, Settings
from wood_events_service.migrate import migrate

TEST_TOKEN = "unit-test-producer-credential-0123456789"  # noqa: S105 -- test-only


@pytest.fixture
def broker_engine(engine: Engine, database_url: str) -> Iterator[Engine]:
    """An isolated schema per broker/notification lifecycle test."""
    schema = "lifecycle_" + uuid4().hex
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA {schema}"))
    url = (
        make_url(database_url)
        .update_query_dict({"options": f"-c search_path={schema}"})
        .render_as_string(hide_password=False)
    )
    migrate(url)
    active = create_engine(url, hide_parameters=True)
    try:
        yield active
    finally:
        active.dispose()
        with engine.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with isolated_postgres() as url:
        migrate(url)
        yield url


@pytest.fixture
def engine(database_url: str) -> Iterator[Engine]:
    active = create_engine(database_url, hide_parameters=True)
    yield active
    active.dispose()


@pytest.fixture
def settings(database_url: str) -> Settings:
    return Settings(
        database_url=SecretStr(database_url),
        producer_credentials=[
            ProducerCredential(
                source="test", token=SecretStr(TEST_TOKEN), scopes={"events:write"}
            )
        ],
    )
