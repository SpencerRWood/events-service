"""Durable scheduling, retries, crash recovery, replay and source-isolated history."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from tests.integration.test_foundation import HEADERS
from tests.unit.test_contracts_security import event

from events_service.config import Settings
from events_service.contracts import EventEnvelope
from events_service.delivery import BrokerWorker, DeliveryResult
from events_service.history import BrokerHistory
from events_service.main import create_app
from events_service.retention import cleanup
from events_service.routing import Subscription, SubscriptionRouter
from events_service.security import SecretPolicy
from events_service.storage import BrokerJob, EventRecord, Store


@pytest.fixture
def configured(settings: Settings) -> Settings:
    credential = settings.producer_credentials[0].model_copy(
        update={"scopes": {"events:write", "events:read", "events:replay"}}
    )
    return settings.model_copy(
        update={
            "producer_credentials": [credential],
            "subscriptions": [
                Subscription(consumer=name, url=f"http://{name}.test/events")
                for name in ("one", "two")
            ],
            "webhook_max_attempts": 3,
            "webhook_backoff_seconds": 2,
            "webhook_max_backoff_seconds": 3,
            "broker_poll_seconds": 0.01,
        }
    )


class FakeWebhook:
    def __init__(self, results: dict[str, list[DeliveryResult]]) -> None:
        self.results = results
        self.calls: list[tuple[str, EventEnvelope]] = []

    def send(self, _url: str, envelope: EventEnvelope, consumer: str) -> DeliveryResult:
        self.calls.append((consumer, envelope))
        return self.results[consumer].pop(0)


def store(engine: Engine, configured: Settings) -> Store:
    return Store(
        engine,
        SecretPolicy(configured.secret_values()),
        SubscriptionRouter(configured.subscriptions),
    )


def test_atomic_jobs_duplicates_and_invalid_input(
    broker_engine: Engine, configured: Settings
) -> None:
    with TestClient(
        create_app("broker", configured, engine=broker_engine, start_worker=False)
    ) as client:
        payload = event(idempotency_key="retry").model_dump(mode="json")
        assert (
            client.post(
                "/v1/events", json={**payload, "schema_version": "2"}, headers=HEADERS
            ).status_code
            == 422
        )
        with Session(broker_engine) as session:
            assert session.scalar(select(func.count()).select_from(EventRecord)) == 0
            assert session.scalar(select(func.count()).select_from(BrokerJob)) == 0
        accepted = client.post("/v1/events", json=payload, headers=HEADERS).json()
        retried = client.post(
            "/v1/events", json={**payload, "event_id": str(uuid4())}, headers=HEADERS
        ).json()
        assert accepted["record_id"] == retried["record_id"]
        assert retried["duplicate"]
        with Session(broker_engine) as session:
            assert session.scalar(select(func.count()).select_from(BrokerJob)) == 2
        with patch(
            "events_service.routing.SubscriptionRouter.destinations",
            side_effect=SQLAlchemyError("failure"),
        ):
            assert (
                client.post(
                    "/v1/events", json=event().model_dump(mode="json"), headers=HEADERS
                ).status_code
                == 503
            )
        with Session(broker_engine) as session:
            assert session.scalar(select(func.count()).select_from(EventRecord)) == 1


def test_independent_consumers_bounded_retry_and_replay(
    broker_engine: Engine, configured: Settings
) -> None:
    original = event()
    store(broker_engine, configured).accept(original)
    transport = FakeWebhook(
        {
            "one": [DeliveryResult("delivered"), DeliveryResult("delivered")],
            "two": [DeliveryResult("transient-failure", "timeout")] * 3,
        }
    )
    worker = BrokerWorker(broker_engine, configured, transport)
    now = datetime.now(UTC)
    assert worker.deliver_one(as_of=now)
    assert worker.deliver_one(as_of=now)
    assert not worker.deliver_one(as_of=now + timedelta(seconds=1))
    assert worker.deliver_one(as_of=now + timedelta(seconds=2))
    assert not worker.deliver_one(as_of=now + timedelta(seconds=4))
    assert worker.deliver_one(as_of=now + timedelta(seconds=5))
    assert not worker.deliver_one(as_of=now + timedelta(days=1))
    history = BrokerHistory(broker_engine)
    state = history.event(original.event_id, "test")
    assert state is not None
    deliveries = state["deliveries"]
    assert isinstance(deliveries, list)
    assert [item["status"] for item in deliveries] == [
        "delivered",
        "terminal-failure",
    ]
    prior = history.attempts(original.event_id, "test", 100, 0)
    assert len(prior) == 4
    assert worker.replay(original.event_id, "one", "test")
    with pytest.raises(ValueError, match="pending"):
        worker.replay(original.event_id, "one", "test")
    assert worker.deliver_one(as_of=now + timedelta(days=2))
    after = history.attempts(original.event_id, "test", 100, 0)
    assert after[:4] == prior
    assert len(after) == 5
    assert all(sent == original for _, sent in transport.calls)


def test_terminal_missing_consumer_and_restart(
    broker_engine: Engine, configured: Settings
) -> None:
    original = event()
    store(broker_engine, configured).accept(original)
    removed = configured.model_copy(
        update={"subscriptions": configured.subscriptions[:1]}
    )
    transport = FakeWebhook({"one": [DeliveryResult("terminal-failure", "http-400")]})
    # New worker after acceptance observes durable jobs, without an in-memory queue.
    worker = BrokerWorker(broker_engine, removed, transport)
    assert worker.deliver_one()
    assert worker.deliver_one()
    assert not worker.deliver_one()
    with pytest.raises(ValueError, match="unconfigured"):
        worker.replay(original.event_id, "two", "test")
    assert not worker.replay(original.event_id, "one", "other")
    rows = BrokerHistory(broker_engine).attempts(original.event_id, "test", 100, 0)
    assert {row["error_code"] for row in rows} == {"http-400", "consumer-unconfigured"}


def test_history_scopes_correlation_pagination_and_replay(
    broker_engine: Engine, configured: Settings, settings: Settings
) -> None:
    original = event()
    other = event(source="other", correlation_id=original.correlation_id)
    store(broker_engine, configured).accept(other)
    with TestClient(
        create_app("broker", configured, engine=broker_engine, start_worker=False)
    ) as client:
        assert (
            client.post(
                "/v1/events", json=original.model_dump(mode="json"), headers=HEADERS
            ).status_code
            == 201
        )
        route = f"/v1/events/{original.event_id}"
        assert client.get(route).status_code == 401
        assert client.get(route, headers=HEADERS).json()["event"]["event_id"] == str(
            original.event_id
        )
        assert (
            len(
                client.get(
                    f"/v1/events?correlation_id={original.correlation_id}",
                    headers=HEADERS,
                ).json()
            )
            == 1
        )
        assert (
            client.get(f"/v1/events?correlation_id={uuid4()}", headers=HEADERS).json()
            == []
        )
        assert client.get("/v1/events?limit=1&offset=1", headers=HEADERS).json() == []
        assert client.get("/v1/events?limit=101", headers=HEADERS).status_code == 422
        assert (
            client.get(f"/v1/events/{other.event_id}", headers=HEADERS).status_code
            == 404
        )
        assert (
            client.get(
                f"/v1/events/{other.event_id}/attempts", headers=HEADERS
            ).status_code
            == 404
        )
        assert client.get(route + "/attempts", headers=HEADERS).json() == []
        replay = route + "/deliveries/one/replay"
        assert client.post(replay, headers=HEADERS).status_code == 409
        assert (
            client.post(
                route + "/deliveries/missing/replay", headers=HEADERS
            ).status_code
            == 404
        )
        with Session(broker_engine) as session, session.begin():
            job = session.scalar(
                select(BrokerJob).where(
                    BrokerJob.event_id == original.event_id, BrokerJob.consumer == "one"
                )
            )
            assert job is not None
            job.status = "delivered"
        assert client.post(replay, headers=HEADERS).status_code == 202
    with TestClient(
        create_app("broker", settings, engine=broker_engine, start_worker=False)
    ) as client:
        assert client.get(route, headers=HEADERS).status_code == 403
        assert client.post(replay, headers=HEADERS).status_code == 403


def test_concurrent_workers_skip_locked_jobs(
    broker_engine: Engine, configured: Settings
) -> None:
    original = event()
    store(broker_engine, configured).accept(original)
    entered, release = Event(), Event()

    class BlockingWebhook:
        def send(
            self, _url: str, _event: EventEnvelope, _consumer: str
        ) -> DeliveryResult:
            entered.set()
            assert release.wait(timeout=5)
            return DeliveryResult("delivered")

    first = BrokerWorker(broker_engine, configured, BlockingWebhook())
    second = BrokerWorker(
        broker_engine,
        configured,
        FakeWebhook(
            {"one": [DeliveryResult("delivered")], "two": [DeliveryResult("delivered")]}
        ),
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(first.deliver_one)
        try:
            assert entered.wait(timeout=5)
            assert second.deliver_one()
            assert not second.deliver_one()
        finally:
            release.set()
        assert future.result(timeout=5)
    assert (
        len(BrokerHistory(broker_engine).attempts(original.event_id, "test", 100, 0))
        == 2
    )


def test_crash_after_send_retries_same_event_without_rewriting_evidence(
    broker_engine: Engine, configured: Settings
) -> None:
    single = configured.model_copy(
        update={"subscriptions": configured.subscriptions[:1]}
    )
    original = event()
    store(broker_engine, single).accept(original)
    transport = FakeWebhook({"one": [DeliveryResult("delivered")] * 2})
    worker = BrokerWorker(broker_engine, single, transport)

    def crash_after_send(*_args: object) -> DeliveryResult:
        # The downstream side effect happens, then this transaction is lost.
        transport.calls.append(("one", original))
        raise SQLAlchemyError("crash after downstream acceptance")

    with (
        patch.object(transport, "send", side_effect=crash_after_send),
        pytest.raises(SQLAlchemyError),
    ):
        worker.deliver_one()
    assert (
        BrokerHistory(broker_engine).attempts(original.event_id, "test", 100, 0) == []
    )
    assert BrokerWorker(broker_engine, single, transport).deliver_one()
    assert [sent.event_id for _, sent in transport.calls] == [original.event_id] * 2


def test_pending_jobs_pin_retention_and_finished_jobs_expire(
    broker_engine: Engine, configured: Settings
) -> None:
    original = event()
    store(broker_engine, configured).accept(original)
    later = datetime.now(UTC) + timedelta(days=100)
    cleanup(broker_engine, configured, as_of=later)
    assert BrokerHistory(broker_engine).event(original.event_id, "test") is not None
    transport = FakeWebhook(
        {name: [DeliveryResult("delivered")] for name in ("one", "two")}
    )
    worker = BrokerWorker(broker_engine, configured, transport)
    worker.deliver_one()
    worker.deliver_one()
    cleanup(broker_engine, configured, as_of=later)
    assert BrokerHistory(broker_engine).event(original.event_id, "test") is None


def test_worker_recovers_database_failure_and_stops(
    broker_engine: Engine, configured: Settings
) -> None:
    worker = BrokerWorker(broker_engine, configured, FakeWebhook({}))

    async def run() -> None:
        stop = asyncio.Event()
        with patch.object(
            worker, "deliver_one", side_effect=[SQLAlchemyError("safe"), False, False]
        ):
            task = asyncio.create_task(worker.run(stop))
            await asyncio.sleep(0.015)
            stop.set()
            await task

    asyncio.run(run())
