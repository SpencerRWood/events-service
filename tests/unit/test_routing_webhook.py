"""Routing predicates and safe transport classification."""

import httpx
import pytest
from pydantic import ValidationError
from tests.unit.test_contracts_security import event

from wood_events_service.config import Settings
from wood_events_service.delivery import HttpWebhook
from wood_events_service.routing import Subscription, SubscriptionRouter


@pytest.mark.parametrize(
    ("rule", "matches"),
    [
        ({}, True),
        ({"event_types": {"check"}}, True),
        ({"event_types": {"other"}}, False),
        ({"sources": {"test"}}, True),
        ({"sources": {"other"}}, False),
        ({"severities": {"error"}}, False),
        ({"severities": {"info"}}, True),
        ({"data_equals": {"state": "failed"}}, True),
        ({"data_equals": {"missing": None}}, False),
        ({"data_equals": {"state": "ok"}}, False),
        ({"sources": {"other"}, "event_types": {"check"}}, False),
    ],
)
def test_subscription_matching(rule: dict[str, object], matches: bool) -> None:
    subscription = Subscription.model_validate(
        {"consumer": "one", "url": "https://consumer.test/events", **rule}
    )
    original = event(data={"state": "failed"})
    assert subscription.matches(original) is matches
    router = SubscriptionRouter([subscription])
    assert router.destinations(original) == (["one"] if matches else [])


@pytest.mark.parametrize(
    "url",
    [
        "file:///tmp/event",
        "https://",
        "http://user:pass@host",
        "http://host/?token=x",
        "http://host/#x",
    ],
)
def test_webhook_configuration_rejects_credential_urls(url: str) -> None:
    with pytest.raises(ValidationError):
        Subscription(consumer="one", url=url)


def test_duplicate_consumers_rejected(settings: Settings) -> None:
    subscription = Subscription(consumer="one", url="http://localhost/events")
    with pytest.raises(ValidationError, match="unique"):
        Settings(
            database_url=settings.database_url,
            producer_credentials=settings.producer_credentials,
            subscriptions=[subscription, subscription],
        )


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (200, "delivered"),
        (204, "delivered"),
        (301, "terminal-failure"),
        (400, "terminal-failure"),
        (401, "terminal-failure"),
        (408, "transient-failure"),
        (425, "transient-failure"),
        (429, "transient-failure"),
        (500, "transient-failure"),
        (503, "transient-failure"),
    ],
)
def test_webhook_status_identity_timeout_and_no_response_capture(
    status: int, outcome: str
) -> None:
    original = event()

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["Idempotency-Key"] == f"{original.event_id}:consumer"
        assert request.headers["X-Correlation-ID"] == str(original.correlation_id)
        assert request.extensions["timeout"] == dict.fromkeys(
            ["connect", "read", "write", "pool"], 0.5
        )
        return httpx.Response(
            status, headers={"Location": "http://other"}, text="secret"
        )

    result = HttpWebhook(0.5, httpx.MockTransport(handle)).send(
        "http://consumer/events", original, "consumer"
    )
    assert result.outcome == outcome
    assert result.error_code == (None if outcome == "delivered" else f"http-{status}")


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (httpx.ReadTimeout("secret"), "timeout"),
        (httpx.ConnectError("secret"), "transport-error"),
    ],
)
def test_webhook_network_failures_are_safe(
    error: httpx.TransportError, code: str
) -> None:
    def handle(_request: httpx.Request) -> httpx.Response:
        raise error

    result = HttpWebhook(0.5, httpx.MockTransport(handle)).send(
        "http://consumer/events", event(), "consumer"
    )
    assert result.outcome == "transient-failure"
    assert result.error_code == code
