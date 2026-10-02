"""Real PostgreSQL fixtures; unavailable dependencies fail rather than skip."""

from collections.abc import Iterator

import pytest
from pydantic import SecretStr
from scripts.isolated_postgres import isolated_postgres
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from wood_events_service.config import ProducerCredential, Settings
from wood_events_service.migrate import migrate

TEST_TOKEN = "unit-test-producer-credential-0123456789"  # noqa: S105 -- test-only


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
