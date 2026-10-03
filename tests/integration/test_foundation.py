"""PostgreSQL invariants, HTTP boundaries and migration lifecycle."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect, text, update
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.orm import Session
from tests.conftest import TEST_TOKEN
from tests.unit.test_contracts_security import event, notification

from events_service.config import Settings
from events_service.main import create_app
from events_service.migrate import migrate
from events_service.retention import cleanup
from events_service.runtime_check import verify
from events_service.security import SecretPolicy
from events_service.storage import (
    Base,
    BrokerDelivery,
    EventRecord,
    IdempotencyConflictError,
    NotificationRecord,
    NotifyDelivery,
    ResponseRecord,
    Store,
    check_schema,
    make_engine,
)

HEADERS = {"Authorization": f"Bearer {TEST_TOKEN}"}


def test_http_auth_validation_and_durable_acceptance(
    engine: Engine, settings: Settings
) -> None:
    with TestClient(create_app("broker", settings, engine=engine)) as client:
        payload = event().model_dump(mode="json")
        assert client.get("/health/live").json()["service"] == "wood-broker"
        assert client.get("/health/ready").status_code == 200
        assert client.post("/v1/events", json=payload).status_code == 401
        assert (
            client.post(
                "/v1/events", json=payload, headers={"Authorization": "Bearer bad"}
            ).status_code
            == 401
        )
        invalid = client.post(
            "/v1/events", json={**payload, "token": TEST_TOKEN}, headers=HEADERS
        )
        assert invalid.status_code == 422
        assert TEST_TOKEN not in invalid.text
        assert (
            client.post(
                "/v1/events", json={**payload, "source": "other"}, headers=HEADERS
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/v1/events",
                json={**payload, "data": {"token": "anything"}},
                headers=HEADERS,
            ).status_code
            == 422
        )
        accepted = client.post("/v1/events", json=payload, headers=HEADERS)
        assert accepted.status_code == 201
        assert not accepted.json()["duplicate"]
        duplicate = client.post("/v1/events", json=payload, headers=HEADERS)
        assert duplicate.json()["record_id"] == accepted.json()["record_id"]
        assert duplicate.json()["duplicate"]
        assert (
            client.post(
                "/v1/events", json={**payload, "severity": "error"}, headers=HEADERS
            ).status_code
            == 409
        )
        assert client.post("/v1/notifications", json={}).status_code == 404
    with Session(engine) as session:
        stored = session.get(EventRecord, accepted.json()["record_id"])
        assert stored is not None
        assert stored.payload == payload
        assert TEST_TOKEN not in str(stored.payload)


def test_notify_runs_independently_and_rejects_broker_scope(
    engine: Engine, settings: Settings
) -> None:
    payload = notification().model_dump(mode="json")
    with TestClient(create_app("notify", settings, engine=engine)) as client:
        assert client.get("/health/ready").json()["service"] == "wood-notify"
        assert (
            client.post("/v1/notifications", json=payload, headers=HEADERS).status_code
            == 403
        )
    credential = settings.producer_credentials[0].model_copy(
        update={"scopes": {"notifications:write"}}
    )
    configured = settings.model_copy(update={"producer_credentials": [credential]})
    with TestClient(create_app("notify", configured, engine=engine)) as client:
        accepted = client.post("/v1/notifications", json=payload, headers=HEADERS)
        assert accepted.status_code == 201
        assert client.post("/v1/events", json={}).status_code == 404
    with Session(engine) as session:
        assert session.get(NotificationRecord, accepted.json()["record_id"]) is not None


def test_concurrent_idempotency_and_conflicting_identities(
    engine: Engine, settings: Settings
) -> None:
    store = Store(engine, SecretPolicy(settings.secret_values()))
    original = event(idempotency_key=uuid4().hex)
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(store.accept, [original] * 8))
    assert sum(not result.duplicate for result in receipts) == 1
    retry = original.model_copy(update={"event_id": uuid4()})
    assert store.accept(retry).record_id == original.event_id
    other = event(idempotency_key=uuid4().hex)
    store.accept(other)
    with pytest.raises(IdempotencyConflictError):
        store.accept(
            original.model_copy(update={"idempotency_key": other.idempotency_key})
        )
    with pytest.raises(IdempotencyConflictError):
        store.accept(retry.model_copy(update={"data": {"changed": True}}))


def test_immutable_parent_append_only_attempts_and_correlated_children(
    engine: Engine, settings: Settings
) -> None:
    original = event()
    Store(engine, SecretPolicy(settings.secret_values())).accept(original)
    attempt = BrokerDelivery(
        id=uuid4(),
        event_id=original.event_id,
        correlation_id=original.correlation_id,
        destination="generic-consumer",
        outcome="delivered",
        attempted_at=datetime.now(UTC),
    )
    with Session(engine) as session, session.begin():
        session.add(attempt)
        attempt_id = attempt.id
    with pytest.raises(DBAPIError), engine.begin() as connection:
        connection.execute(
            update(EventRecord)
            .where(EventRecord.id == original.event_id)
            .values(source="changed")
        )
    with pytest.raises(DBAPIError), engine.begin() as connection:
        connection.execute(
            update(BrokerDelivery)
            .where(BrokerDelivery.id == attempt_id)
            .values(outcome="pending")
        )
    with pytest.raises(DBAPIError), engine.begin() as connection:
        connection.execute(
            text("DELETE FROM broker_delivery_attempts WHERE id = :id"),
            {"id": attempt_id},
        )
    with pytest.raises(DBAPIError), Session(engine) as session, session.begin():
        session.add(
            BrokerDelivery(
                id=uuid4(),
                event_id=original.event_id,
                correlation_id=uuid4(),
                destination="test",
                outcome="delivered",
                attempted_at=datetime.now(UTC),
            )
        )


def test_response_identity_and_notification_attempts(
    engine: Engine, settings: Settings
) -> None:
    request = notification()
    Store(engine, SecretPolicy(settings.secret_values())).accept(request)
    response_id = uuid4()
    provider_id = uuid4().hex
    with Session(engine) as session, session.begin():
        session.add(
            NotifyDelivery(
                id=uuid4(),
                request_id=request.request_id,
                correlation_id=request.correlation_id,
                destination="ntfy",
                outcome="pending",
                attempted_at=datetime.now(UTC),
            )
        )
        session.add(
            ResponseRecord(
                id=response_id,
                request_id=request.request_id,
                correlation_id=request.correlation_id,
                provider="telegram",
                provider_response_id=provider_id,
                payload={"selected_action": "acknowledge"},
                received_at=datetime.now(UTC),
            )
        )
    with pytest.raises(DBAPIError), Session(engine) as session, session.begin():
        session.add(
            ResponseRecord(
                id=uuid4(),
                request_id=request.request_id,
                correlation_id=request.correlation_id,
                provider="telegram",
                provider_response_id=provider_id,
                payload={},
                received_at=datetime.now(UTC),
            )
        )
    with pytest.raises(DBAPIError), Session(engine) as session, session.begin():
        session.add(
            ResponseRecord(
                id=uuid4(),
                request_id=request.request_id,
                correlation_id=uuid4(),
                provider="test",
                provider_response_id=uuid4().hex,
                payload={},
                received_at=datetime.now(UTC),
            )
        )


def test_retention_preserves_live_children_and_uses_explicit_cutoff(
    engine: Engine, settings: Settings
) -> None:
    now = datetime.now(UTC)
    old = now - timedelta(days=100)
    parent_id, correlation_id = uuid4(), uuid4()
    with Session(engine) as session, session.begin():
        session.add(
            EventRecord(
                id=parent_id,
                source="test",
                correlation_id=correlation_id,
                payload={},
                accepted_at=old,
            )
        )
        session.flush()
        session.add(
            BrokerDelivery(
                id=uuid4(),
                event_id=parent_id,
                correlation_id=correlation_id,
                destination="test",
                outcome="delivered",
                attempted_at=now,
            )
        )
    cleanup(engine, settings, as_of=now)
    with Session(engine) as session:
        assert session.get(EventRecord, parent_id) is not None
    cleanup(engine, settings, as_of=now + timedelta(days=100))
    with Session(engine) as session:
        assert session.get(EventRecord, parent_id) is None
    assert all(count == 0 for count in cleanup(engine, settings, as_of=now).values())
    with pytest.raises(ValueError, match="timezone aware"):
        cleanup(engine, settings, as_of=datetime(2026, 1, 1))  # noqa: DTZ001


def test_migrations_round_trip_and_match_models(database_url: str) -> None:
    # Independent schema prevents destructive migration checks touching other tests.
    root = create_engine(database_url)
    schema = "migration_" + uuid4().hex
    with root.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA {schema}"))
    url = (
        make_url(database_url)
        .update_query_dict({"options": f"-c search_path={schema}"})
        .render_as_string(hide_password=False)
    )
    separate = create_engine(url)
    try:
        migrate(url)
        check_schema(separate)
        assert set(inspect(separate).get_table_names(schema=schema)) == {
            "alembic_version",
            "broker_events",
            "broker_delivery_jobs",
            "notify_states",
            "notify_delivery_jobs",
            "notify_suppression_windows",
            "notify_messages",
            "notify_requests",
            "broker_delivery_attempts",
            "notify_delivery_attempts",
            "notify_responses",
        }
        with separate.connect() as connection:
            differences = compare_metadata(
                MigrationContext.configure(connection), Base.metadata
            )
        assert differences == []
        migrate(url)  # repeat-safe upgrade
        migrate(url, "base", downgrade=True)
        assert inspect(separate).get_table_names(schema=schema) == ["alembic_version"]
        migrate(url)
        check_schema(separate)
    finally:
        separate.dispose()
        with root.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        root.dispose()


def test_startup_validates_settings_and_database(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("WES_DATABASE_URL", raising=False)
    monkeypatch.delenv("WES_PRODUCER_CREDENTIALS", raising=False)
    with (
        pytest.raises(RuntimeError, match="Invalid runtime"),
        TestClient(create_app("broker")),
    ):
        pass
    with TestClient(create_app("broker", settings)) as client:
        assert client.get("/health/ready").status_code == 200
    active = make_engine(settings)
    try:
        with patch("events_service.storage.Engine.connect") as connect:
            connect.return_value.__enter__.return_value.scalar.return_value = "old"
            with pytest.raises(SQLAlchemyError, match="migration"):
                check_schema(active)
    finally:
        active.dispose()


def test_both_real_processes_persist_across_restart(database_url: str) -> None:
    verify(database_url)


def test_database_failures_have_safe_http_and_startup_errors(
    engine: Engine, settings: Settings
) -> None:
    with TestClient(create_app("broker", settings, engine=engine)) as client:
        with patch.object(Store, "accept", side_effect=SQLAlchemyError(TEST_TOKEN)):
            failure = client.post(
                "/v1/events", json=event().model_dump(mode="json"), headers=HEADERS
            )
        assert failure.status_code == 503
        assert TEST_TOKEN not in failure.text
        with patch(
            "events_service.main.check_schema",
            side_effect=SQLAlchemyError(TEST_TOKEN),
        ):
            assert client.get("/health/ready").status_code == 503
    with (
        patch(
            "events_service.main.check_schema",
            side_effect=SQLAlchemyError(TEST_TOKEN),
        ),
        pytest.raises(RuntimeError, match="Database unavailable") as startup,
        TestClient(create_app("broker", settings, engine=engine)),
    ):
        pass
    assert TEST_TOKEN not in str(startup.value)
