from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from services.broker.app.main import create_app
from shared.config import BrokerSettings


def client(tmp_path: Path, transport: httpx.MockTransport) -> TestClient:
    http_client = httpx.Client(transport=transport)
    app = create_app(
        BrokerSettings(
            database_url=f"sqlite:///{tmp_path / 'broker.db'}",
            max_delivery_attempts=2,
            retry_backoff_seconds=1,
        ),
        dispatcher_client=http_client,
    )
    return TestClient(app)


def event_payload(**overrides):
    payload = {
        "event_id": "evt-1",
        "event_type": "wood.test",
        "source": "tests",
        "severity": "error",
        "occurred_at": datetime(2026, 8, 12, tzinfo=UTC).isoformat(),
        "payload": {"ok": True},
    }
    payload.update(overrides)
    return payload


def test_ingest_persists_event_before_delivery_and_records_success(tmp_path: Path) -> None:
    delivered: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        delivered.append(json.loads(request.content))
        return httpx.Response(204)

    with client(tmp_path, httpx.MockTransport(handler)) as test_client:
        subscription = test_client.post(
            "/subscriptions",
            json={
                "name": "consumer",
                "target_url": "https://consumer.example.test/events",
                "event_type": "wood.test",
                "source": "tests",
                "severity": "error",
            },
        )
        assert subscription.status_code == 201

        response = test_client.post("/events", json=event_payload())

        assert response.status_code == 202
        assert response.json()["event_id"] == "evt-1"
        assert delivered[0]["event_id"] == "evt-1"

        deliveries = test_client.get("/deliveries?event_id=evt-1").json()
        assert deliveries == [
            {
                "id": 1,
                "event_id": "evt-1",
                "subscription_id": 1,
                "target_url": "https://consumer.example.test/events",
                "status": "delivered",
                "attempts": 1,
                "last_status_code": 204,
                "last_error": None,
                "next_attempt_after": None,
            }
        ]


def test_invalid_envelope_is_rejected_without_persistence(tmp_path: Path) -> None:
    with client(tmp_path, httpx.MockTransport(lambda _: httpx.Response(204))) as test_client:
        response = test_client.post("/events", json=event_payload(extra=True))

        assert response.status_code == 422
        assert test_client.get("/events").json() == []


def test_duplicate_event_id_preserves_original_identity(tmp_path: Path) -> None:
    with client(tmp_path, httpx.MockTransport(lambda _: httpx.Response(204))) as test_client:
        assert test_client.post("/events", json=event_payload()).status_code == 202

        duplicate = test_client.post("/events", json=event_payload())

        assert duplicate.status_code == 409
        assert duplicate.json()["detail"]["event"]["event_id"] == "evt-1"
        assert len(test_client.get("/events").json()) == 1


def test_missing_event_id_is_generated_and_remains_queryable(tmp_path: Path) -> None:
    with client(tmp_path, httpx.MockTransport(lambda _: httpx.Response(204))) as test_client:
        payload = event_payload()
        payload.pop("event_id")

        response = test_client.post("/events", json=payload)

        assert response.status_code == 202
        event_id = response.json()["event_id"]
        assert event_id.startswith("evt-")
        assert test_client.get(f"/events/{event_id}").json()["event_id"] == event_id


def test_failed_delivery_can_be_retried_without_resubmitting_event(tmp_path: Path) -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503, text="temporary failure")
        return httpx.Response(200)

    with client(tmp_path, httpx.MockTransport(handler)) as test_client:
        test_client.post(
            "/subscriptions",
            json={"name": "consumer", "target_url": "https://consumer.example.test/events"},
        )
        assert test_client.post("/events", json=event_payload()).status_code == 202

        failed = test_client.get("/deliveries").json()[0]
        assert failed["status"] == "failed"
        assert failed["attempts"] == 1
        assert failed["last_status_code"] == 503

        retried = test_client.post(f"/deliveries/{failed['id']}/retry")
        assert retried.status_code == 200
        assert retried.json()["status"] == "delivered"
        assert retried.json()["attempts"] == 2
        assert len(test_client.get("/events").json()) == 1


def test_delivery_becomes_terminal_after_bounded_attempts(tmp_path: Path) -> None:
    with client(tmp_path, httpx.MockTransport(lambda _: httpx.Response(503, text="down"))) as test_client:
        test_client.post(
            "/subscriptions",
            json={"name": "consumer", "target_url": "https://consumer.example.test/events"},
        )
        assert test_client.post("/events", json=event_payload()).status_code == 202

        first = test_client.get("/deliveries").json()[0]
        assert first["status"] == "failed"

        second = test_client.post(f"/deliveries/{first['id']}/retry")

        assert second.status_code == 200
        assert second.json()["status"] == "terminal"
        assert second.json()["attempts"] == 2
        assert test_client.get("/deliveries").json()[0]["status"] == "terminal"


def test_unmatched_subscription_does_not_create_delivery(tmp_path: Path) -> None:
    with client(tmp_path, httpx.MockTransport(lambda _: httpx.Response(204))) as test_client:
        test_client.post(
            "/subscriptions",
            json={
                "name": "consumer",
                "target_url": "https://consumer.example.test/events",
                "event_type": "other.event",
            },
        )

        assert test_client.post("/events", json=event_payload()).status_code == 202
        assert test_client.get("/deliveries").json() == []
