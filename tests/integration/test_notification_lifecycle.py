"""Real PostgreSQL notification policy, retry, suppression and response capture."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from typing import cast
from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from tests.integration.test_foundation import HEADERS
from tests.unit.test_contracts_security import event, notification
from tests.unit.test_notification_providers import WEBHOOK_SECRET

from events_service.config import Settings
from events_service.contracts import NotificationRequest, ResponseAction
from events_service.main import create_app
from events_service.notification_lifecycle import NotificationLifecycle
from events_service.notification_models import (
    NotificationJob,
)
from events_service.notification_policy import NotificationPolicy
from events_service.providers import ProviderResult, TelegramProvider, callback_data
from events_service.retention import cleanup
from events_service.security import SecretPolicy
from events_service.storage import (
    NotificationRecord,
    NotifyDelivery,
    ResponseRecord,
    Store,
)


class FakeProvider:
    def __init__(self, results: list[ProviderResult] | None = None) -> None:
        self.results = results or []
        self.calls: list[NotificationRequest] = []

    def send(self, request: NotificationRequest) -> ProviderResult:
        self.calls.append(request)
        return (
            self.results.pop(0)
            if self.results
            else ProviderResult("delivered", message_id=len(self.calls))
        )


def worker_for(client: TestClient) -> NotificationLifecycle:
    return cast(NotificationLifecycle, cast(FastAPI, client.app).state.notifications)


@pytest.fixture
def notify_settings(settings: Settings) -> Settings:
    credential = settings.producer_credentials[0].model_copy(
        update={
            "scopes": {
                "notifications:write",
                "notifications:read",
                "notifications:retry",
                "notifications:consume",
            }
        }
    )
    return Settings(
        database_url=settings.database_url,
        producer_credentials=[credential],
        telegram_chat_id=-123,
        telegram_webhook_secret=SecretStr(WEBHOOK_SECRET),
        suppression_seconds=0,
        notification_max_attempts=2,
        notification_backoff_seconds=1,
        notification_max_backoff_seconds=1,
        notification_poll_seconds=0.01,
    )


def lifecycle(
    engine: Engine, settings: Settings, provider: FakeProvider
) -> tuple[NotificationLifecycle, Store]:
    worker = NotificationLifecycle(
        engine, settings, {"telegram": provider, "ntfy": provider}
    )
    return worker, Store(
        engine,
        SecretPolicy(settings.secret_values()),
        notification_hook=worker.schedule,
    )


def action_request(**changes: object) -> NotificationRequest:
    return notification(
        channels=["telegram"],
        notification_type="action-required",
        response_actions=[
            ResponseAction(action="approve", label="Approve"),
            ResponseAction(action="cancel", label="Cancel"),
        ],
        response_deadline=datetime.now(UTC) + timedelta(hours=1),
        **changes,
    )


def update_for(
    request: NotificationRequest,
    *,
    identity: str = "query-1",
    message_id: int = 1,
    index: int = 0,
) -> dict[str, object]:
    return {
        "update_id": 1,
        "callback_query": {
            "id": identity,
            "from": {"id": 42, "first_name": "Test"},
            "message": {
                "message_id": message_id,
                "chat": {"id": -123},
                "text": "Action",
            },
            "data": callback_data(request.request_id, index, WEBHOOK_SECRET),
        },
    }


def test_direct_notifications_are_atomic_and_queryable(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    provider = FakeProvider()
    original = notification(channels=["ntfy", "telegram"])
    with TestClient(
        create_app(
            "notify",
            notify_settings,
            engine=broker_engine,
            providers={"ntfy": provider, "telegram": provider},
            start_worker=False,
        )
    ) as client:
        payload = original.model_dump(mode="json")
        assert client.post("/v1/notifications", json=payload).status_code == 401
        assert (
            client.post(
                "/v1/notifications",
                json={**payload, "context": {"password": "bad"}},
                headers=HEADERS,
            ).status_code
            == 422
        )
        assert (
            client.post("/v1/notifications", json=payload, headers=HEADERS).status_code
            == 201
        )
        assert not provider.calls
        assert client.post("/v1/notifications", json=payload, headers=HEADERS).json()[
            "duplicate"
        ]
        with Session(broker_engine) as session:
            assert (
                session.scalar(select(func.count()).select_from(NotificationJob)) == 2
            )
        worker = worker_for(client)
        assert worker.deliver_one()
        assert worker.deliver_one()
        route = f"/v1/notifications/{original.request_id}"
        state = client.get(route, headers=HEADERS).json()
        assert [job["status"] for job in state["channels"]] == [
            "delivered",
            "delivered",
        ]
        attempts = client.get(route + "/attempts", headers=HEADERS).json()
        assert len(attempts) == 2
        assert all(
            row["correlation_id"] == str(original.correlation_id) for row in attempts
        )
        assert client.get(route + "/responses", headers=HEADERS).json() == []
        assert (
            len(
                client.get(
                    f"/v1/notifications?correlation_id={original.correlation_id}",
                    headers=HEADERS,
                ).json()
            )
            == 1
        )
        assert (
            client.get("/v1/notifications?limit=1&offset=1", headers=HEADERS).json()
            == []
        )
        assert (
            client.get("/v1/notifications?limit=101", headers=HEADERS).status_code
            == 422
        )
        assert (
            client.get(f"/v1/notifications/{uuid4()}", headers=HEADERS).status_code
            == 404
        )
        assert client.get(route).status_code == 401


def test_broker_event_routing_dedupe_correlation_and_scope(
    broker_engine: Engine, notify_settings: Settings, settings: Settings
) -> None:
    configured = notify_settings.model_copy(
        update={
            "notification_policies": [
                NotificationPolicy(
                    name="operational",
                    channels=["ntfy"],
                    event_types={"check"},
                    sources={"test"},
                )
            ]
        }
    )
    original = event()
    provider = FakeProvider()
    with TestClient(
        create_app(
            "notify",
            configured,
            engine=broker_engine,
            providers={"ntfy": provider},
            start_worker=False,
        )
    ) as client:
        first = client.post(
            "/v1/broker-events", json=original.model_dump(mode="json"), headers=HEADERS
        )
        assert first.status_code == 201
        duplicate = client.post(
            "/v1/broker-events", json=original.model_dump(mode="json"), headers=HEADERS
        )
        assert duplicate.json()["duplicate"]
        assert first.json()["record_id"] == duplicate.json()["record_id"]
        route = f"/v1/notifications/{first.json()['record_id']}"
        stored = client.get(route, headers=HEADERS).json()["request"]
        assert stored["correlation_id"] == str(original.correlation_id)
        assert stored["causation_id"] == str(original.event_id)
        assert worker_for(client).deliver_one()
        assert len(provider.calls) == 1
        no_match = event(event_type="unmatched")
        ignored = client.post(
            "/v1/broker-events", json=no_match.model_dump(mode="json"), headers=HEADERS
        ).json()
        assert (
            client.get(
                f"/v1/notifications/{ignored['record_id']}", headers=HEADERS
            ).json()["response_state"]
            == "suppressed"
        )
        assert not worker_for(client).deliver_one()
        changed = original.model_copy(update={"data": {"changed": True}})
        assert (
            client.post(
                "/v1/broker-events",
                json=changed.model_dump(mode="json"),
                headers=HEADERS,
            ).status_code
            == 409
        )
        assert (
            client.post(
                "/v1/broker-events",
                json=event(data={"token": "unknown"}).model_dump(mode="json"),
                headers=HEADERS,
            ).status_code
            == 422
        )
    with TestClient(
        create_app("notify", settings, engine=broker_engine, start_worker=False)
    ) as client:
        assert (
            client.post(
                "/v1/broker-events",
                json=original.model_dump(mode="json"),
                headers=HEADERS,
            ).status_code
            == 403
        )
        assert client.get(route, headers=HEADERS).status_code == 403


def test_suppression_is_durable_concurrent_and_source_scoped(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    configured = notify_settings.model_copy(update={"suppression_seconds": 60})
    worker, store = lifecycle(broker_engine, configured, FakeProvider())
    requests = [
        notification(channels=["ntfy"], suppression_key="same-condition")
        for _ in range(8)
    ]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(store.accept, requests))
    with Session(broker_engine) as session:
        jobs = session.scalars(select(NotificationJob)).all()
        assert sum(job.status == "pending" for job in jobs) == 1
        assert sum(job.status == "suppressed" for job in jobs) == 7
        assert session.scalar(select(func.count()).select_from(NotifyDelivery)) == 7
    store.accept(
        notification(
            source="other", channels=["ntfy"], suppression_key="same-condition"
        )
    )
    assert worker.deliver_one()
    assert worker.deliver_one()
    assert not worker.deliver_one()


def test_automatic_equivalent_event_suppression_ignores_event_identity(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    configured = notify_settings.model_copy(
        update={
            "suppression_seconds": 60,
            "notification_policies": [
                NotificationPolicy(name="events", channels=["ntfy"])
            ],
        }
    )
    worker, store = lifecycle(broker_engine, configured, FakeProvider())
    store.accept(worker.router.from_event(event(data={"status": "failed"})))
    store.accept(worker.router.from_event(event(data={"status": "failed"})))
    assert worker.deliver_one()
    assert not worker.deliver_one()


def test_bounded_retry_preserves_original_and_prior_attempts(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    provider = FakeProvider(
        [
            ProviderResult("transient-failure", "timeout"),
            ProviderResult("transient-failure", "http-503"),
            ProviderResult("delivered"),
        ]
    )
    worker, store = lifecycle(broker_engine, notify_settings, provider)
    original = notification(channels=["ntfy"])
    store.accept(original)
    now = datetime.now(UTC)
    assert worker.deliver_one(as_of=now)
    assert not worker.deliver_one(as_of=now)
    assert worker.deliver_one(as_of=now + timedelta(seconds=1))
    assert not worker.deliver_one(as_of=now + timedelta(days=1))
    assert worker.retry(original.request_id, "ntfy", "test")
    with pytest.raises(ValueError, match="Only failed"):
        worker.retry(original.request_id, "ntfy", "test")
    assert worker.deliver_one(as_of=now + timedelta(days=2))
    with Session(broker_engine) as session:
        attempts = session.scalars(
            select(NotifyDelivery).order_by(NotifyDelivery.attempted_at)
        ).all()
        assert [row.outcome for row in attempts] == [
            "transient-failure",
            "transient-failure",
            "delivered",
        ]
        assert all(row.request_id == original.request_id for row in attempts)
    assert all(sent == original for sent in provider.calls)
    assert not worker.retry(original.request_id, "ntfy", "other")


def test_terminal_unconfigured_and_failed_channel_isolation(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    worker, store = lifecycle(
        broker_engine,
        notify_settings,
        FakeProvider(
            [
                ProviderResult("terminal-failure", "http-400"),
                ProviderResult("delivered"),
            ]
        ),
    )
    store.accept(notification(channels=["ntfy", "telegram", "missing"]))
    assert worker.deliver_one()
    assert worker.deliver_one()
    assert worker.deliver_one()
    assert not worker.deliver_one()
    with Session(broker_engine) as session:
        assert sorted(session.scalars(select(NotificationJob.status))) == [
            "delivered",
            "terminal-failure",
            "terminal-failure",
        ]


def test_callback_normalization_duplicates_expiration_and_no_execution(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    provider = FakeProvider()
    original = action_request()
    webhook_headers = {"X-Telegram-Bot-Api-Secret-Token": WEBHOOK_SECRET}
    with TestClient(
        create_app(
            "notify",
            notify_settings,
            engine=broker_engine,
            providers={"telegram": provider},
            start_worker=False,
        )
    ) as client:
        assert (
            client.post(
                "/v1/notifications",
                json=original.model_dump(mode="json"),
                headers=HEADERS,
            ).status_code
            == 201
        )
        assert worker_for(client).deliver_one()
        payload = update_for(original)
        assert client.post("/v1/telegram/callbacks", json=payload).status_code == 401
        first = client.post(
            "/v1/telegram/callbacks", json=payload, headers=webhook_headers
        )
        assert first.status_code == 200
        response = first.json()["response"]
        assert response["request_id"] == str(original.request_id)
        assert response["correlation_id"] == str(original.correlation_id)
        assert response["selected_action"] == "approve"
        duplicate = client.post(
            "/v1/telegram/callbacks", json=payload, headers=webhook_headers
        )
        assert duplicate.json()["duplicate"]
        assert duplicate.json()["response"] == response
        assert client.post(
            "/v1/telegram/callbacks",
            json=update_for(original, identity="query-2", index=1),
            headers=webhook_headers,
        ).json() == {"ignored": True, "reason": "already-answered"}
        assert (
            client.post(
                "/v1/telegram/callbacks",
                json=update_for(original, index=1),
                headers=webhook_headers,
            ).status_code
            == 409
        )
        route = f"/v1/notifications/{original.request_id}"
        assert (
            client.get(route, headers=HEADERS).json()["response_state"] == "responded"
        )
        assert client.get(route + "/responses", headers=HEADERS).json() == [response]
        assert len(provider.calls) == 1
        with Session(broker_engine) as session:
            assert session.scalar(select(func.count()).select_from(ResponseRecord)) == 1
        assert (
            worker_for(client).expire(as_of=datetime.now(UTC) + timedelta(days=1)) == 0
        )


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("chat", 403),
        ("user", 403),
        ("message", 404),
        ("signature", 422),
        ("undeclared", 422),
    ],
)
def test_callback_validation(
    broker_engine: Engine, notify_settings: Settings, kind: str, expected: int
) -> None:
    original = action_request()
    configured = notify_settings.model_copy(update={"telegram_allowed_user_ids": {42}})
    with TestClient(
        create_app(
            "notify",
            configured,
            engine=broker_engine,
            providers={"telegram": FakeProvider()},
            start_worker=False,
        )
    ) as client:
        client.post(
            "/v1/notifications", json=original.model_dump(mode="json"), headers=HEADERS
        )
        worker_for(client).deliver_one()
        payload = update_for(original)
        query = payload["callback_query"]
        assert isinstance(query, dict)
        if kind == "chat":
            query["message"] = {"message_id": 1, "chat": {"id": -456}}
        elif kind == "user":
            query["from"] = {"id": 43}
        elif kind == "message":
            query["message"] = {"message_id": 999, "chat": {"id": -123}}
        elif kind == "signature":
            query["data"] = "invalid"
        else:
            query["data"] = callback_data(original.request_id, 15, WEBHOOK_SECRET)
        assert (
            client.post(
                "/v1/telegram/callbacks",
                json=payload,
                headers={"X-Telegram-Bot-Api-Secret-Token": WEBHOOK_SECRET},
            ).status_code
            == expected
        )


def test_expiration_is_deterministic_and_blocks_late_callbacks(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    original = action_request()
    worker, store = lifecycle(broker_engine, notify_settings, FakeProvider())
    store.accept(original)
    worker.deliver_one()
    later = datetime.now(UTC) + timedelta(days=1)
    assert worker.expire(as_of=later) == 1
    assert worker.expire(as_of=later) == 0
    with pytest.raises(ValueError, match="aware"):
        worker.expire(as_of=datetime(2026, 1, 1))  # noqa: DTZ001
    with pytest.raises(ValueError, match="Only failed"):
        worker.retry(original.request_id, "telegram", "test")
    with TestClient(
        create_app(
            "notify",
            notify_settings,
            engine=broker_engine,
            providers={},
            start_worker=False,
        )
    ) as client:
        assert client.post(
            "/v1/telegram/callbacks",
            json=update_for(original),
            headers={"X-Telegram-Bot-Api-Secret-Token": WEBHOOK_SECRET},
        ).json() == {"ignored": True, "reason": "expired"}
        assert (
            client.get(
                f"/v1/notifications/{original.request_id}", headers=HEADERS
            ).json()["response_state"]
            == "expired"
        )


def test_telegram_acknowledges_terminal_presses_and_retries_without_new_decisions(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    acknowledgements: list[dict[str, object]] = []
    messages: list[httpx.Request] = []

    def telegram(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("sendMessage"):
            messages.append(request)
            return httpx.Response(
                200, json={"ok": True, "result": {"message_id": len(messages)}}
            )
        assert request.url.path.endswith("answerCallbackQuery")
        acknowledgements.append(json.loads(request.content))
        if len(acknowledgements) == 1:
            return httpx.Response(503, json={"ok": False})
        return httpx.Response(200, json={"ok": True, "result": True})

    configured = notify_settings.model_copy(
        update={"telegram_bot_token": SecretStr("test-only-telegram-bot")}
    )
    provider = TelegramProvider(configured, httpx.MockTransport(telegram))
    original = action_request()
    headers = {"X-Telegram-Bot-Api-Secret-Token": WEBHOOK_SECRET}
    with TestClient(
        create_app(
            "notify",
            configured,
            engine=broker_engine,
            providers={"telegram": provider},
            start_worker=False,
        )
    ) as client:
        client.post(
            "/v1/notifications",
            json=original.model_dump(mode="json"),
            headers=HEADERS,
        )
        assert worker_for(client).deliver_one()
        callback = update_for(original)
        failed_ack = client.post(
            "/v1/telegram/callbacks", json=callback, headers=headers
        )
        assert failed_ack.status_code == 503
        replay = client.post("/v1/telegram/callbacks", json=callback, headers=headers)
        assert replay.status_code == 200
        assert replay.json()["duplicate"]
        response = replay.json()["response"]
        repeated = client.post(
            "/v1/telegram/callbacks",
            json=update_for(original, identity="new-press", index=1),
            headers=headers,
        )
        assert repeated.status_code == 200
        assert repeated.json() == {"ignored": True, "reason": "already-answered"}
        assert client.get(
            f"/v1/notifications/{original.request_id}/responses", headers=HEADERS
        ).json() == [response]

        expiring = action_request()
        client.post(
            "/v1/notifications",
            json=expiring.model_dump(mode="json"),
            headers=HEADERS,
        )
        assert worker_for(client).deliver_one()
        assert (
            worker_for(client).expire(as_of=datetime.now(UTC) + timedelta(days=1)) == 1
        )
        expired = client.post(
            "/v1/telegram/callbacks",
            json=update_for(expiring, identity="late-press", message_id=2),
            headers=headers,
        )
        assert expired.status_code == 200
        assert expired.json() == {"ignored": True, "reason": "expired"}
        assert acknowledgements == [
            {"callback_query_id": "query-1", "text": "Response recorded."},
            {"callback_query_id": "query-1", "text": "Response recorded."},
            {
                "callback_query_id": "new-press",
                "text": "This request has already been answered.",
            },
            {"callback_query_id": "late-press", "text": "This request has expired."},
        ]
        assert client.post("/v1/telegram/callbacks", json=callback).status_code == 401
        assert (
            client.post(
                "/v1/telegram/callbacks",
                json=update_for(original, index=1),
                headers=headers,
            ).status_code
            == 409
        )
        invalid = update_for(original)
        query = invalid["callback_query"]
        assert isinstance(query, dict)
        query["data"] = "invalid"
        assert (
            client.post(
                "/v1/telegram/callbacks", json=invalid, headers=headers
            ).status_code
            == 422
        )
        assert len(acknowledgements) == 4
        with Session(broker_engine) as session:
            assert session.scalar(select(func.count()).select_from(ResponseRecord)) == 1


def test_concurrent_callbacks_capture_one_response(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    original = action_request()
    with TestClient(
        create_app(
            "notify",
            notify_settings,
            engine=broker_engine,
            providers={"telegram": FakeProvider()},
            start_worker=False,
        )
    ) as client:
        client.post(
            "/v1/notifications", json=original.model_dump(mode="json"), headers=HEADERS
        )
        worker_for(client).deliver_one()

        def send(_index: int) -> dict[str, object]:
            result = client.post(
                "/v1/telegram/callbacks",
                json=update_for(original),
                headers={"X-Telegram-Bot-Api-Secret-Token": WEBHOOK_SECRET},
            )
            assert result.status_code == 200
            return cast(dict[str, object], result.json())

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(send, range(8)))
        assert sum(not row["duplicate"] for row in results) == 1


def test_retention_preserves_pending_and_awaiting_response(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    worker, store = lifecycle(broker_engine, notify_settings, FakeProvider())
    original = action_request()
    store.accept(original)
    later = datetime.now(UTC) + timedelta(days=100)
    cleanup(broker_engine, notify_settings, as_of=later)
    worker.deliver_one()
    cleanup(broker_engine, notify_settings, as_of=later)
    with Session(broker_engine) as session:
        assert session.get(NotificationRecord, original.request_id) is not None
    worker.expire(as_of=later)
    cleanup(broker_engine, notify_settings, as_of=later + timedelta(days=100))
    with Session(broker_engine) as session:
        assert session.get(NotificationRecord, original.request_id) is None


def test_worker_recovery_and_missing_parent_guard(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    worker, _store = lifecycle(broker_engine, notify_settings, FakeProvider())

    async def run() -> None:
        stop = asyncio.Event()
        with patch.object(
            worker, "deliver_one", side_effect=SQLAlchemyError("redacted")
        ):
            task = asyncio.create_task(worker.run(stop))
            await asyncio.sleep(0.015)
            stop.set()
            await task

    asyncio.run(run())


def test_notification_workers_skip_locked_and_recover_lost_transaction(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    entered, release = Event(), Event()
    original = notification(channels=["ntfy"])

    class BlockingProvider(FakeProvider):
        def send(self, request: NotificationRequest) -> ProviderResult:
            entered.set()
            assert release.wait(timeout=5)
            return super().send(request)

    first, store = lifecycle(broker_engine, notify_settings, BlockingProvider())
    store.accept(original)
    store.accept(notification(channels=["telegram"]))
    second, _store = lifecycle(broker_engine, notify_settings, FakeProvider())
    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(first.deliver_one)
        try:
            assert entered.wait(timeout=5)
            assert second.deliver_one()
            assert not second.deliver_one()
        finally:
            release.set()
        assert future.result(timeout=5)
    with Session(broker_engine) as session:
        assert session.scalar(select(func.count()).select_from(NotifyDelivery)) == 2

    provider = FakeProvider()
    worker, store = lifecycle(broker_engine, notify_settings, provider)
    pending = notification(channels=["ntfy"], title="Crash check")
    store.accept(pending)

    def crash_after_send(_request: NotificationRequest) -> ProviderResult:
        provider.calls.append(pending)
        raise SQLAlchemyError("downstream accepted before transaction loss")

    with (
        patch.object(provider, "send", side_effect=crash_after_send),
        pytest.raises(SQLAlchemyError),
    ):
        worker.deliver_one()
    assert worker.deliver_one()
    assert [request.request_id for request in provider.calls] == [
        pending.request_id
    ] * 2
    with Session(broker_engine) as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(NotifyDelivery)
                .where(NotifyDelivery.request_id == pending.request_id)
            )
            == 1
        )


def test_interactive_policy_requires_telegram_and_source_history_is_private(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    with TestClient(
        create_app(
            "notify",
            notify_settings,
            engine=broker_engine,
            providers={},
            start_worker=False,
        )
    ) as client:
        request = action_request().model_copy(update={"channels": ["ntfy"]})
        assert (
            client.post(
                "/v1/notifications",
                json=request.model_dump(mode="json"),
                headers=HEADERS,
            ).status_code
            == 422
        )
        with Session(broker_engine) as session:
            assert (
                session.scalar(select(func.count()).select_from(NotificationRecord))
                == 0
            )
        _worker, store = lifecycle(broker_engine, notify_settings, FakeProvider())
        other = notification(source="other", channels=["ntfy"])
        store.accept(other)
        with patch.object(worker_for(client), "expire") as expire:
            assert (
                client.get(
                    f"/v1/notifications/{other.request_id}", headers=HEADERS
                ).status_code
                == 404
            )
            expire.assert_not_called()
        assert (
            client.post(
                f"/v1/notifications/{other.request_id}/channels/ntfy/retry",
                headers=HEADERS,
            ).status_code
            == 404
        )
        assert (
            client.post(
                f"/v1/notifications/{uuid4()}/channels/ntfy/retry", headers=HEADERS
            ).status_code
            == 404
        )
        assert (
            client.get(
                f"/v1/notifications?correlation_id={uuid4()}", headers=HEADERS
            ).json()
            == []
        )


def test_expired_acceptance_does_not_send_and_empty_provider_environment_is_ignored(
    broker_engine: Engine, notify_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = FakeProvider()
    worker, store = lifecycle(broker_engine, notify_settings, provider)
    expired = action_request().model_copy(
        update={"response_deadline": datetime.now(UTC) - timedelta(seconds=1)}
    )
    store.accept(expired)
    assert not worker.deliver_one()
    assert not provider.calls
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("NTFY_BASE_URL", "")
    monkeypatch.setenv("SMTP_HOST", "")
    parsed = Settings(
        database_url=notify_settings.database_url,
        producer_credentials=notify_settings.producer_credentials,
    )
    assert parsed.telegram_bot_token is None
    assert parsed.ntfy_base_url is None
    assert parsed.smtp_host is None


def test_expiration_backlog_never_delivers_overdue_requests(
    broker_engine: Engine, notify_settings: Settings
) -> None:
    provider = FakeProvider()
    worker, store = lifecycle(broker_engine, notify_settings, provider)
    deadline = datetime.now(UTC) + timedelta(hours=1)
    # More requests than one bounded expiration sweep can process.
    for index in range(101):
        store.accept(
            notification(
                channels=["ntfy"],
                response_deadline=deadline + timedelta(seconds=index),
            )
        )
    assert worker.deliver_one(as_of=deadline + timedelta(hours=1))
    assert not provider.calls
    with Session(broker_engine) as session:
        assert set(session.scalars(select(NotificationJob.status))) == {"expired"}
