"""Real DB, fake Telegram acceptance of the S01→S04 transport boundary."""

import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import func, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.orm import Session
from tests.integration.test_foundation import HEADERS
from tests.integration.test_notification_lifecycle import worker_for
from tests.unit.test_contracts_security import notification
from tests.unit.test_notification_providers import BOT_TOKEN, WEBHOOK_SECRET

from events_service.config import ProducerCredential, Settings
from events_service.contracts import NotificationRequest
from events_service.main import create_app
from events_service.notification_models import (
    MessageEdit,
    NotificationJob,
    NotificationMessage,
)
from events_service.providers import TelegramProvider, callback_data
from events_service.retention import cleanup
from events_service.storage import NotificationRecord

CALLBACK_HEADERS = {"X-Telegram-Bot-Api-Secret-Token": WEBHOOK_SECRET}
OTHER_TOKEN = "other-test-only-producer-0123456789"  # noqa: S105


def proposal(**changes: object) -> NotificationRequest:
    return notification(
        **{
            "channels": ["telegram"],
            "notification_type": "action-required",
            "response_actions": [
                {"action": "approve", "label": "Approve & Merge"},
                {"action": "reject", "label": "Reject"},
            ],
            "url_buttons": [
                {
                    "label": "View PR",
                    "url": "https://github.com/example/handbook/pull/1",
                }
            ],
            "responder_policy": {"user_ids": [42], "private_chat_id": 42},
            "response_deadline": datetime.now(UTC) + timedelta(hours=72),
            **changes,
        }
    )


def press(request: NotificationRequest, **changes: Any) -> dict[str, Any]:
    return {
        "update_id": 100,
        "callback_query": {
            "id": "query-1",
            "from": {"id": 42},
            "message": {"message_id": 1, "chat": {"id": 42, "type": "private"}},
            "data": callback_data(request.request_id, 0, WEBHOOK_SECRET),
            **changes,
        },
    }


class FakeTelegram:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.messages: dict[int, dict[str, Any]] = {}
        self.failure: tuple[str, str] | None = None

    def handle(self, request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        payload = json.loads(request.content)
        self.calls.append((method, payload))
        failure = None
        if self.failure is not None and self.failure[0] == method:
            _, failure = self.failure
            self.failure = None
        if failure == "rate-limit":
            return httpx.Response(429, json={"ok": False, "error_code": 429})
        if failure == "forbidden":
            return httpx.Response(403, json={"ok": False})
        value: Any
        if method == "sendMessage":
            message_id = len(self.messages) + 1
            value = {
                "message_id": message_id,
                "chat": {"id": payload["chat_id"], "type": "private"},
                "text": payload["text"],
                "reply_markup": payload.get("reply_markup"),
            }
            self.messages[message_id] = value
        elif method == "editMessageText":
            message_id = payload["message_id"]
            value = self.messages[message_id]
            assert payload["chat_id"] == value["chat"]["id"]
            if (
                value["text"] == payload["text"]
                and value.get("reply_markup") == payload["reply_markup"]
            ):
                return httpx.Response(
                    400,
                    json={
                        "ok": False,
                        "description": "Bad Request: message is not modified",
                    },
                )
            value.update(text=payload["text"], reply_markup=payload["reply_markup"])
        else:
            assert method == "answerCallbackQuery"
            value = True
        if failure == "timeout-after":
            raise httpx.ReadTimeout("fixture accepted, response lost")
        if failure == "crash-after":
            raise SQLAlchemyError("fixture crash after external effect")
        if failure == "invalid-response":
            return httpx.Response(200, json={"ok": True, "result": {}})
        return httpx.Response(200, json={"ok": True, "result": value})


@pytest.fixture
def transport(
    broker_engine: Engine, settings: Settings
) -> Iterator[tuple[TestClient, FakeTelegram, Settings]]:
    credential = settings.producer_credentials[0].model_copy(
        update={
            "scopes": {
                "notifications:write",
                "notifications:read",
                "notifications:retry",
            }
        }
    )
    configured = Settings(
        database_url=settings.database_url,
        producer_credentials=[
            credential,
            ProducerCredential(
                source="other",
                token=SecretStr(OTHER_TOKEN),
                scopes={"notifications:write", "notifications:read"},
            ),
        ],
        telegram_bot_token=SecretStr(BOT_TOKEN),
        telegram_webhook_secret=SecretStr(WEBHOOK_SECRET),
        telegram_chat_id=42,
        suppression_seconds=0,
        notification_max_attempts=2,
        notification_backoff_seconds=1,
        notification_max_backoff_seconds=1,
    )
    fake = FakeTelegram()
    provider = TelegramProvider(configured, httpx.MockTransport(fake.handle))
    with TestClient(
        create_app(
            "notify",
            configured,
            engine=broker_engine,
            providers={"telegram": provider},
            start_worker=False,
        )
    ) as client:
        yield client, fake, configured


def deliver(client: TestClient, request: NotificationRequest) -> None:
    result = client.post(
        "/v1/notifications", json=request.model_dump(mode="json"), headers=HEADERS
    )
    assert result.status_code == 201
    assert worker_for(client).deliver_one()


def edit_request(outcome: str = "merged", **changes: Any) -> dict[str, Any]:
    return {
        "edit_id": str(uuid4()),
        "outcome": outcome,
        "message": "Verified terminal outcome.",
        **changes,
    }


def test_three_actions_link_does_not_consume_and_private_owner_provenance(
    transport: Any,
) -> None:
    client, fake, _ = transport
    request = proposal()
    deliver(client, request)
    buttons = fake.calls[0][1]["reply_markup"]["inline_keyboard"]
    assert [b[0]["text"] for b in buttons] == ["Approve & Merge", "Reject", "View PR"]
    assert buttons[2] == [{"text": "View PR", "url": str(request.url_buttons[0].url)}]
    assert "callback_data" not in buttons[2][0]
    # There is no callback identity for the URL. Forging one cannot consume a decision.
    invalid = press(request, data=callback_data(request.request_id, 2, WEBHOOK_SECRET))
    assert (
        client.post(
            "/v1/telegram/callbacks", json=invalid, headers=CALLBACK_HEADERS
        ).status_code
        == 422
    )
    assert (
        client.get(
            f"/v1/notifications/{request.request_id}/responses", headers=HEADERS
        ).json()
        == []
    )
    denied = press(request, **{"from": {"id": 43}})
    assert (
        client.post(
            "/v1/telegram/callbacks", json=denied, headers=CALLBACK_HEADERS
        ).status_code
        == 403
    )
    accepted = client.post(
        "/v1/telegram/callbacks", json=press(request), headers=CALLBACK_HEADERS
    )
    assert accepted.status_code == 200
    response = accepted.json()["response"]
    assert response["selected_action"] == "approve"
    assert response["telegram_provenance"] == {
        "schema_version": "1",
        "webhook_authenticated": True,
        "callback_signature_verified": True,
        "user_id": 42,
        "chat_id": 42,
        "chat_type": "private",
        "message_id": 1,
        "update_id": 100,
        "request_policy_verified": True,
    }
    replay = client.post(
        "/v1/telegram/callbacks", json=press(request), headers=CALLBACK_HEADERS
    ).json()
    assert replay == {"duplicate": True, "response": response}
    repeat = client.post(
        "/v1/telegram/callbacks",
        json=press(request, id="another-press"),
        headers=CALLBACK_HEADERS,
    ).json()
    assert repeat == {"ignored": True, "reason": "already-answered"}


@pytest.mark.parametrize(
    "chat", [{"id": 42}, {"id": 42, "type": "group"}, {"id": 43, "type": "private"}]
)
def test_unverified_chat_cannot_consume_owner_response(
    transport: Any, chat: dict[str, object]
) -> None:
    client, _, _ = transport
    request = proposal()
    deliver(client, request)
    payload = press(request, message={"message_id": 1, "chat": chat})
    assert (
        client.post(
            "/v1/telegram/callbacks", json=payload, headers=CALLBACK_HEADERS
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/v1/telegram/callbacks", json=press(request), headers=CALLBACK_HEADERS
        ).status_code
        == 200
    )


@pytest.mark.parametrize("outcome", ["merged", "rejected", "expired", "blocked"])
def test_source_owned_terminal_edit_is_frozen_and_removes_controls(
    transport: Any, outcome: str
) -> None:
    client, fake, _ = transport
    request = proposal()
    deliver(client, request)
    path = f"/v1/notifications/{request.request_id}/edits"
    edit = edit_request(outcome)
    assert client.post(path, json=edit).status_code == 401
    other = {"Authorization": f"Bearer {OTHER_TOKEN}"}
    assert client.post(path, json=edit, headers=other).status_code == 404
    assert client.get(path, headers=other).status_code == 404
    assert client.post(path, json=edit, headers=HEADERS).status_code == 202
    assert client.post(path, json=edit, headers=HEADERS).status_code == 202
    assert (
        client.post(
            path, json={**edit, "message": "changed"}, headers=HEADERS
        ).status_code
        == 409
    )
    assert client.post(path, json=edit_request(), headers=HEADERS).status_code == 409
    assert (
        client.post(path, json={**edit, "chat_id": 99}, headers=HEADERS).status_code
        == 422
    )
    assert worker_for(client).deliver_one()
    assert (
        fake.messages[1]["text"] == f"{outcome.upper()}\n\nVerified terminal outcome."
    )
    assert fake.messages[1]["reply_markup"] == {"inline_keyboard": []}
    result = client.post(
        "/v1/telegram/callbacks", json=press(request), headers=CALLBACK_HEADERS
    )
    assert result.json() == {"ignored": True, "reason": "terminal-outcome"}
    assert client.get(path, headers=HEADERS).json()[0]["status"] == "delivered"
    assert not worker_for(client).deliver_one()


@pytest.mark.parametrize(
    "failure", ["timeout-after", "crash-after", "invalid-response", "rate-limit"]
)
def test_edit_recovers_after_ambiguous_effect_or_crash(
    transport: Any, failure: str
) -> None:
    client, fake, _ = transport
    request = proposal()
    deliver(client, request)
    path = f"/v1/notifications/{request.request_id}/edits"
    edit = edit_request()
    client.post(path, json=edit, headers=HEADERS)
    fake.failure = ("editMessageText", failure)
    now = datetime.now(UTC)
    if failure == "crash-after":
        with pytest.raises(SQLAlchemyError):
            worker_for(client).deliver_one(as_of=now)
    else:
        assert worker_for(client).deliver_one(as_of=now)
    assert client.get(path, headers=HEADERS).json()[0]["status"] in {
        "uncertain",
        "dispatching",
    }
    # A fresh worker uses the DB record, not process-local delivery memory.
    from events_service.message_edits import MessageEdits  # noqa: PLC0415

    worker = worker_for(client)
    recovered = MessageEdits(worker.engine, worker.settings, worker.providers)
    assert recovered.deliver_one(now + timedelta(minutes=1))
    result = client.get(path, headers=HEADERS).json()[0]
    assert result["status"] == "delivered"
    assert result["attempts"] == 2
    assert len(fake.messages) == 1
    edits = [body for method, body in fake.calls if method == "editMessageText"]
    assert len(edits) == 2
    assert edits[0] == edits[1]


@pytest.mark.parametrize(
    "failure", ["timeout-after", "crash-after", "invalid-response"]
)
def test_scoped_prompt_delivery_never_blindly_resends(
    transport: Any, failure: str
) -> None:
    client, fake, _ = transport
    request = proposal()
    client.post(
        "/v1/notifications", json=request.model_dump(mode="json"), headers=HEADERS
    )
    fake.failure = ("sendMessage", failure)
    if failure == "crash-after":
        with pytest.raises(SQLAlchemyError):
            worker_for(client).deliver_one()
    else:
        assert worker_for(client).deliver_one()
    assert not worker_for(client).deliver_one()
    retry = client.post(
        f"/v1/notifications/{request.request_id}/channels/telegram/retry",
        headers=HEADERS,
    )
    assert retry.status_code == 409
    assert len(fake.messages) == 1


def test_expiration_enqueues_terminal_edit_and_cannot_be_replayed(
    transport: Any,
) -> None:
    client, fake, _ = transport
    request = proposal()
    deliver(client, request)
    later = datetime.now(UTC) + timedelta(days=4)
    worker = worker_for(client)
    assert worker.expire(as_of=later) == 1
    assert worker.expire(as_of=later) == 0
    assert worker.deliver_one(as_of=later)
    assert fake.messages[1]["text"].startswith("EXPIRED\n")
    assert fake.messages[1]["reply_markup"] == {"inline_keyboard": []}
    assert client.post(
        "/v1/telegram/callbacks", json=press(request), headers=CALLBACK_HEADERS
    ).json()["ignored"]


def test_duplicate_and_competing_edits_are_serialized(
    transport: Any, broker_engine: Engine
) -> None:
    client, _, _ = transport
    request = proposal()
    deliver(client, request)
    path, edit = f"/v1/notifications/{request.request_id}/edits", edit_request()
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda _: client.post(path, json=edit, headers=HEADERS).status_code,
                range(8),
            )
        )
    assert results == [202] * 8
    with Session(broker_engine) as session:
        assert session.scalar(select(func.count()).select_from(MessageEdit)) == 1
    assert worker_for(client).deliver_one()


def test_terminal_failure_and_uncertain_edits_survive_retention(
    transport: Any, broker_engine: Engine
) -> None:
    client, fake, configured = transport
    request = proposal()
    deliver(client, request)
    path = f"/v1/notifications/{request.request_id}/edits"
    client.post(path, json=edit_request("blocked"), headers=HEADERS)
    fake.failure = ("editMessageText", "forbidden")
    assert worker_for(client).deliver_one()
    assert client.get(path, headers=HEADERS).json()[0]["status"] == "terminal-failure"
    cleanup(broker_engine, configured, as_of=datetime.now(UTC) + timedelta(days=100))
    with Session(broker_engine) as session:
        assert session.get(NotificationRecord, request.request_id) is not None
        assert (
            session.scalar(select(func.count()).select_from(NotificationMessage)) == 1
        )


def test_existing_stored_payload_retries_without_new_schema_conflict(
    transport: Any, broker_engine: Engine
) -> None:
    client, _, _ = transport
    request = notification(channels=["telegram"])
    deliver(client, request)
    # Simulate a pre-S04 immutable payload with a raw insert, without updating
    # or disabling the append-only trigger on existing requests.
    original = request.model_dump(mode="json")
    original.pop("url_buttons")
    original.pop("responder_policy")
    original["request_id"] = str(uuid4())
    with Session(broker_engine) as session, session.begin():
        session.add(
            NotificationRecord(
                id=UUID(original["request_id"]),
                source=request.source,
                correlation_id=request.correlation_id,
                causation_id=None,
                idempotency_key=None,
                payload=original,
            )
        )
    result = client.post("/v1/notifications", json=original, headers=HEADERS)
    assert result.status_code == 201
    assert result.json()["duplicate"]


@pytest.mark.parametrize(
    "changes",
    [
        {"url_buttons": [{"label": "View", "url": "http://example.test"}]},
        {
            "url_buttons": [
                {"label": "View", "url": "https://user:password@example.test"}
            ]
        },
        {"responder_policy": {"user_ids": [], "private_chat_id": 42}},
        {"responder_policy": {"user_ids": [42], "private_chat_id": -123}},
        {"response_deadline": None},
        {"responder_policy": {"user_ids": [42], "private_chat_id": 43}},
        {"channels": ["telegram", "ntfy"]},
    ],
)
def test_invalid_or_unroutable_scoped_requests_fail_before_delivery(
    transport: Any, changes: dict[str, Any]
) -> None:
    client, fake, _ = transport
    raw = {**proposal().model_dump(mode="json"), **changes}
    assert (
        client.post("/v1/notifications", json=raw, headers=HEADERS).status_code == 422
    )
    assert not fake.calls


def test_edit_requires_confirmed_original_message_and_safe_content(
    transport: Any,
) -> None:
    client, fake, _ = transport
    request = proposal()
    client.post(
        "/v1/notifications", json=request.model_dump(mode="json"), headers=HEADERS
    )
    path = f"/v1/notifications/{request.request_id}/edits"
    assert client.post(path, json=edit_request(), headers=HEADERS).status_code == 422
    assert worker_for(client).deliver_one()
    assert (
        client.post(
            path, json=edit_request(message=BOT_TOKEN), headers=HEADERS
        ).status_code
        == 422
    )
    assert (
        client.post(
            path, json=edit_request(message="😀" * 2048), headers=HEADERS
        ).status_code
        == 422
    )
    assert len(fake.calls) == 1


def test_expiration_during_prompt_delivery_still_removes_controls(
    transport: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake, _ = transport
    request = proposal()
    worker = worker_for(client)
    provider = worker.providers["telegram"]
    original = provider.send
    later = datetime.now(UTC) + timedelta(days=4)

    def send_and_expire(value: NotificationRequest) -> Any:
        result = original(value)
        assert worker.expire(as_of=later) == 1
        return result

    monkeypatch.setattr(provider, "send", send_and_expire)
    deliver(client, request)
    assert worker.deliver_one(as_of=later)
    assert fake.messages[1]["reply_markup"] == {"inline_keyboard": []}
    assert fake.messages[1]["text"].startswith("EXPIRED")


def test_global_responder_allowlist_is_never_widened(transport: Any) -> None:
    client, fake, _ = transport
    worker = worker_for(client)
    worker.settings.telegram_allowed_user_ids = {99}
    raw = proposal().model_dump(mode="json")
    assert (
        client.post("/v1/notifications", json=raw, headers=HEADERS).status_code == 422
    )
    assert not fake.calls


def test_definite_prompt_rate_limit_can_retry_without_duplicates(
    transport: Any,
) -> None:
    client, fake, _ = transport
    request = proposal()
    fake.failure = ("sendMessage", "rate-limit")
    deliver(client, request)
    assert not fake.messages
    assert worker_for(client).deliver_one(
        as_of=datetime.now(UTC) + timedelta(seconds=2)
    )
    assert len(fake.messages) == 1


def test_edit_exhaustion_retains_uncertainty_and_message_binding(
    transport: Any,
    broker_engine: Engine,
) -> None:
    client, fake, configured = transport
    request = proposal()
    deliver(client, request)
    path = f"/v1/notifications/{request.request_id}/edits"
    client.post(path, json=edit_request(), headers=HEADERS)
    now = datetime.now(UTC)
    for attempt in range(2):
        fake.failure = ("editMessageText", "rate-limit")
        assert worker_for(client).deliver_one(
            as_of=now + timedelta(seconds=attempt * 2)
        )
    assert not worker_for(client).deliver_one(as_of=now + timedelta(seconds=10))
    result = client.get(path, headers=HEADERS).json()[0]
    assert result["status"] == "uncertain"
    assert result["attempts"] == 2
    cleanup(broker_engine, configured, as_of=now + timedelta(days=100))
    with Session(broker_engine) as session:
        assert session.get(NotificationRecord, request.request_id) is not None


def test_edit_uses_frozen_message_after_runtime_chat_configuration_changes(
    transport: Any,
) -> None:
    client, fake, _ = transport
    request = proposal()
    deliver(client, request)
    worker_for(client).settings.telegram_chat_id = 999
    client.post(
        f"/v1/notifications/{request.request_id}/edits",
        json=edit_request(),
        headers=HEADERS,
    )
    assert worker_for(client).deliver_one()
    assert fake.calls[-1][1]["chat_id"] == 42


def test_delivered_terminal_edit_can_be_retained_then_cleaned(
    transport: Any,
    broker_engine: Engine,
) -> None:
    client, _, configured = transport
    request = proposal()
    deliver(client, request)
    client.post(
        f"/v1/notifications/{request.request_id}/edits",
        json=edit_request(),
        headers=HEADERS,
    )
    assert worker_for(client).deliver_one()
    later = datetime.now(UTC) + timedelta(days=100)
    cleanup(broker_engine, configured, as_of=later)
    with Session(broker_engine) as session:
        assert session.get(NotificationRecord, request.request_id) is None
        assert session.scalar(select(func.count()).select_from(NotificationJob)) == 0


def test_database_enforces_frozen_edit_payload_and_original_message(
    transport: Any,
    broker_engine: Engine,
) -> None:
    client, _, _ = transport
    request = proposal()
    deliver(client, request)
    payload = edit_request()
    client.post(
        f"/v1/notifications/{request.request_id}/edits", json=payload, headers=HEADERS
    )
    with Session(broker_engine) as session:
        with pytest.raises(DBAPIError, match="immutable"):
            session.execute(
                update(MessageEdit)
                .where(MessageEdit.id == UUID(payload["edit_id"]))
                .values(payload={**payload, "message": "mutated"})
            )
        session.rollback()
