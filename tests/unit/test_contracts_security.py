"""Contract, configuration and credential boundary failures."""

import json
import logging
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import SecretStr, ValidationError
from tests.conftest import TEST_TOKEN

from wood_events_service.config import ProducerCredential, Settings
from wood_events_service.contracts import (
    EventEnvelope,
    NormalizedResponse,
    NotificationRequest,
    ResponseAction,
    Severity,
)
from wood_events_service.security import SecretPolicy, StructuredFormatter, TokenAuth


def event(**changes: object) -> EventEnvelope:
    values: dict[str, object] = {
        "event_type": "check",
        "source": "test",
        "severity": "info",
        "occurred_at": datetime.now(UTC),
        "correlation_id": uuid4(),
        "data": {},
    }
    return EventEnvelope.model_validate({**values, **changes})


def notification(**changes: object) -> NotificationRequest:
    values: dict[str, object] = {
        "source": "test",
        "notification_type": "informational",
        "severity": "info",
        "title": "Check",
        "message": "Structured condition",
        "correlation_id": uuid4(),
        "policy_key": "test",
    }
    return NotificationRequest.model_validate({**values, **changes})


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": "2"},
        {"occurred_at": datetime(2026, 1, 1)},  # noqa: DTZ001
        {"source": ""},
        {"event_type": "a" * 129},
        {"severity": "debug"},
        {"credentials": "untrusted"},
        {"correlation_id": "invalid"},
    ],
)
def test_event_rejects_invalid_contract(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        event(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"policy_key": None},
        {"channels": ["ntfy", "ntfy"]},
        {"notification_type": "action-required"},
        {"response_actions": [{"action": "approve", "label": "A"}] * 2},
    ],
)
def test_notification_policy_validation(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        notification(**changes)


def test_identity_and_generic_response_contracts() -> None:
    accepted = event()
    assert EventEnvelope.model_validate_json(accepted.model_dump_json()) == accepted
    request = notification(
        notification_type="action-required",
        channels=["telegram"],
        response_actions=[ResponseAction(action="retry", label="Retry")],
    )
    response = NormalizedResponse(
        request_id=request.request_id,
        correlation_id=request.correlation_id,
        causation_id=accepted.event_id,
        selected_action="retry",
        received_at=datetime.now(UTC),
        provider="telegram",
        provider_response_id="callback-123",
    )
    assert response.correlation_id == request.correlation_id
    assert response.response_id != request.request_id
    assert request.severity == Severity.INFO


@pytest.mark.parametrize(
    "payload",
    [
        {"context": {"api_key": "unknown"}},
        {"data": [{"password": "unknown"}]},
        {"title": TEST_TOKEN},
        {"message": f"embedded {TEST_TOKEN}"},
    ],
)
def test_secret_policy_rejects_nested_credentials(payload: object) -> None:
    with pytest.raises(ValueError, match="Credential-bearing"):
        SecretPolicy((TEST_TOKEN,)).ensure_safe(payload)


def test_structured_log_and_config_do_not_expose_secrets(settings: Settings) -> None:
    policy = SecretPolicy(settings.secret_values())
    record = logging.LogRecord(
        "wes", logging.ERROR, "", 0, "value %s", (TEST_TOKEN,), None
    )
    correlation_id = uuid4()
    record.__dict__["correlation_id"] = correlation_id
    formatted = StructuredFormatter(policy).format(record)
    assert TEST_TOKEN not in formatted
    assert "[REDACTED]" in formatted
    assert json.loads(formatted)["correlation_id"] == str(correlation_id)
    assert TEST_TOKEN not in repr(settings)
    assert "database_url" not in settings.model_dump()
    assert "producer_credentials" not in settings.model_dump()
    policy.ensure_safe({"safe": [1, True, None, "hello"]})


def test_auth_is_scoped_and_source_bound(settings: Settings) -> None:
    auth = TokenAuth(settings.producer_credentials)
    assert auth.authenticate(TEST_TOKEN, "events:write") == "test"
    with pytest.raises(HTTPException) as missing:
        auth.authenticate("bad", "events:write")
    assert missing.value.status_code == 401
    with pytest.raises(HTTPException) as scope:
        auth.authenticate(TEST_TOKEN, "notifications:write")
    assert scope.value.status_code == 403


@pytest.mark.parametrize(
    "changes",
    [
        {"database_url": "sqlite://"},
        {"event_retention_days": 0},
        {"producer_credentials": []},
        {
            "producer_credentials": [
                {"source": "test", "token": "short", "scopes": ["events:write"]}
            ]
        },
        {
            "producer_credentials": [
                {"source": "test", "token": TEST_TOKEN, "scopes": []}
            ]
        },
    ],
)
def test_invalid_runtime_fails_closed(
    settings: Settings, changes: dict[str, object]
) -> None:
    values = {
        "database_url": settings.database_url,
        "producer_credentials": settings.producer_credentials,
    }
    with pytest.raises(ValidationError):
        Settings.model_validate({**values, **changes})


def test_duplicate_runtime_tokens_fail(settings: Settings) -> None:
    with pytest.raises(ValidationError, match="unique"):
        Settings(
            database_url=settings.database_url,
            producer_credentials=[
                settings.producer_credentials[0],
                settings.producer_credentials[0],
            ],
        )
    assert (
        ProducerCredential(
            source="other", token=SecretStr(TEST_TOKEN), scopes={"notifications:write"}
        ).token.get_secret_value()
        == TEST_TOKEN
    )


def test_secrets_in_json_keys_and_escaped_log_values_are_excluded() -> None:
    value = 'secret-with-quote-"-and-newline-\n-1234567890'
    policy = SecretPolicy((value,))
    with pytest.raises(ValueError, match="Credential-bearing"):
        policy.ensure_safe({value: "ordinary value"})
    record = logging.LogRecord("wes", logging.INFO, "", 0, "%s", (value,), None)
    assert (
        json.loads(StructuredFormatter(policy).format(record))["message"]
        == "[REDACTED]"
    )
