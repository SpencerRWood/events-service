from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from services.notify.app.adapters import DeliveryResult
from services.notify.app.main import create_app
from services.notify.app.schemas import BrokerEventIn
from shared.config import NotifySettings, NtfySettings


@dataclass
class FakeAdapter:
    results: list[DeliveryResult]
    calls: list[tuple[BrokerEventIn, str | None]]

    def deliver(self, event: BrokerEventIn, target: str | None) -> DeliveryResult:
        self.calls.append((event, target))
        if self.results:
            return self.results.pop(0)
        return DeliveryResult(200)


def client(
    tmp_path: Path,
    adapters: dict[str, FakeAdapter],
    *,
    ntfy_base_url: str = "http://ntfy:80",
) -> TestClient:
    app = create_app(
        NotifySettings(
            database_url=f"sqlite:///{tmp_path / 'notify.db'}",
            max_delivery_attempts=2,
            retry_backoff_seconds=1,
        ),
        NtfySettings(base_url=ntfy_base_url),
        adapters=adapters,
    )
    return TestClient(app)


def event_payload(**overrides):
    payload = {
        "schema_version": "v1",
        "event_id": "evt-1",
        "event_type": "wood.test",
        "source": "broker",
        "severity": "critical",
        "occurred_at": datetime(2026, 8, 12, tzinfo=UTC).isoformat(),
        "subject": "Wake up",
        "payload": {"dedup_key": "alarm-1"},
    }
    payload.update(overrides)
    return payload


def test_consumes_broker_event_and_routes_matching_policy_to_ntfy(tmp_path: Path) -> None:
    ntfy = FakeAdapter(results=[DeliveryResult(204)], calls=[])
    with client(tmp_path, {"ntfy": ntfy}) as test_client:
        policy = test_client.post(
            "/policies",
            json={
                "name": "critical ntfy",
                "channel": "ntfy",
                "event_type": "wood.test",
                "source": "broker",
                "severity": "critical",
                "target": "alerts",
            },
        )
        assert policy.status_code == 201

        response = test_client.post("/broker-events", json=event_payload())

        assert response.status_code == 202
        assert response.json()["matched_policy_count"] == 1
        assert response.json()["deliveries"][0]["status"] == "delivered"
        assert ntfy.calls[0][0].event_id == "evt-1"
        assert ntfy.calls[0][1] == "alerts"


def test_unmatched_policy_does_not_deliver(tmp_path: Path) -> None:
    ntfy = FakeAdapter(results=[DeliveryResult(204)], calls=[])
    with client(tmp_path, {"ntfy": ntfy}) as test_client:
        test_client.post(
            "/policies",
            json={"name": "other source", "channel": "ntfy", "source": "other"},
        )

        response = test_client.post("/broker-events", json=event_payload())

        assert response.status_code == 202
        assert response.json()["matched_policy_count"] == 0
        assert test_client.get("/deliveries").json() == []
        assert ntfy.calls == []


def test_duplicate_broker_event_is_suppressed_by_policy_key(tmp_path: Path) -> None:
    ntfy = FakeAdapter(results=[DeliveryResult(204), DeliveryResult(204)], calls=[])
    with client(tmp_path, {"ntfy": ntfy}) as test_client:
        test_client.post("/policies", json={"name": "all ntfy", "channel": "ntfy"})

        first = test_client.post("/broker-events", json=event_payload())
        second = test_client.post("/broker-events", json=event_payload())

        assert first.status_code == 202
        assert second.status_code == 202
        assert len(ntfy.calls) == 1
        assert len(test_client.get("/deliveries").json()) == 1


def test_dedup_key_suppresses_distinct_events_in_same_alert_storm(tmp_path: Path) -> None:
    ntfy = FakeAdapter(results=[DeliveryResult(204), DeliveryResult(204)], calls=[])
    with client(tmp_path, {"ntfy": ntfy}) as test_client:
        test_client.post("/policies", json={"name": "all ntfy", "channel": "ntfy"})

        first = test_client.post("/broker-events", json=event_payload(event_id="evt-1"))
        second = test_client.post("/broker-events", json=event_payload(event_id="evt-2"))

        assert first.status_code == 202
        assert second.status_code == 202
        assert len(ntfy.calls) == 1
        delivery = test_client.get("/deliveries").json()[0]
        assert delivery["event_id"] == "evt-1"
        assert delivery["dedup_key"] == "alarm-1"


def test_failed_notification_can_be_retried_without_resubmitting_event(tmp_path: Path) -> None:
    ntfy = FakeAdapter(results=[DeliveryResult(503, "down"), DeliveryResult(200)], calls=[])
    with client(tmp_path, {"ntfy": ntfy}) as test_client:
        test_client.post("/policies", json={"name": "all ntfy", "channel": "ntfy"})
        assert test_client.post("/broker-events", json=event_payload()).status_code == 202

        failed = test_client.get("/deliveries").json()[0]
        assert failed["status"] == "failed"
        assert failed["attempts"] == 1

        retried = test_client.post(f"/deliveries/{failed['id']}/retry")

        assert retried.status_code == 200
        assert retried.json()["status"] == "delivered"
        assert retried.json()["attempts"] == 2
        assert len(ntfy.calls) == 2


def test_notification_becomes_terminal_after_bounded_attempts(tmp_path: Path) -> None:
    ntfy = FakeAdapter(results=[DeliveryResult(503, "down"), DeliveryResult(503, "down")], calls=[])
    with client(tmp_path, {"ntfy": ntfy}) as test_client:
        test_client.post("/policies", json={"name": "all ntfy", "channel": "ntfy"})
        test_client.post("/broker-events", json=event_payload())

        failed = test_client.get("/deliveries").json()[0]
        terminal = test_client.post(f"/deliveries/{failed['id']}/retry")

        assert terminal.status_code == 200
        assert terminal.json()["status"] == "terminal"
        assert terminal.json()["attempts"] == 2


def test_provider_credentials_are_not_stored_in_delivery_records(tmp_path: Path) -> None:
    telegram = FakeAdapter(results=[DeliveryResult(200)], calls=[])
    with client(tmp_path, {"telegram": telegram}) as test_client:
        test_client.post(
            "/policies",
            json={"name": "telegram", "channel": "telegram", "target": "chat-1"},
        )

        test_client.post("/broker-events", json=event_payload())

        delivery = test_client.get("/deliveries").json()[0]
        assert "token" not in str(delivery).lower()
        assert "password" not in str(delivery).lower()
        assert delivery["channel"] == "telegram"


def test_smtp_adapter_is_isolated_behind_policy_channel(tmp_path: Path) -> None:
    smtp = FakeAdapter(results=[DeliveryResult(250)], calls=[])
    with client(tmp_path, {"smtp": smtp}) as test_client:
        test_client.post(
            "/policies",
            json={"name": "smtp", "channel": "smtp", "target": "ops@example.test"},
        )

        response = test_client.post("/broker-events", json=event_payload())

        assert response.status_code == 202
        assert response.json()["deliveries"][0]["channel"] == "smtp"
        assert smtp.calls[0][1] == "ops@example.test"


def test_default_ntfy_adapter_uses_configurable_base_url(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    app = create_app(
        NotifySettings(database_url=f"sqlite:///{tmp_path / 'notify.db'}"),
        NtfySettings(base_url="https://ntfy.example.test"),
        ntfy_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    with TestClient(app) as test_client:
        test_client.post("/policies", json={"name": "ntfy", "channel": "ntfy", "target": "alerts"})
        assert test_client.post("/broker-events", json=event_payload()).status_code == 202

    assert str(requests[0].url) == "https://ntfy.example.test/alerts"
