"""Provider protocol payloads, normalized failures and credential boundaries."""

import json
import smtplib
from unittest.mock import MagicMock, patch
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr, ValidationError
from tests.unit.test_contracts_security import event, notification

from events_service.config import Settings
from events_service.contracts import ResponseAction, Severity
from events_service.notification_policy import (
    NotificationPolicy,
    NotificationRouter,
)
from events_service.providers import (
    HttpProvider,
    NtfyProvider,
    SmtpProvider,
    TelegramProvider,
    callback_data,
    classify,
    parse_callback,
    provider_registry,
)
from events_service.security import SecretPolicy

BOT_TOKEN = "test-bot-token-not-a-real-provider-token"  # noqa: S105
WEBHOOK_SECRET = "test-webhook-secret-01234567890123456789"  # noqa: S105


@pytest.fixture
def provider_settings(settings: Settings) -> Settings:
    return Settings(
        database_url=settings.database_url,
        producer_credentials=settings.producer_credentials,
        ntfy_base_url="http://ntfy.test",
        ntfy_topic="test-topic",
        ntfy_auth_token=SecretStr("test-ntfy-auth-token-0123456789"),
        telegram_bot_token=SecretStr(BOT_TOKEN),
        telegram_chat_id=-123,
        telegram_webhook_secret=SecretStr(WEBHOOK_SECRET),
        smtp_host="smtp.test",
        smtp_sender="sender@example.test",
        smtp_recipients=["recipient@example.test"],
        smtp_username=SecretStr("smtp-test-user"),
        smtp_password=SecretStr("smtp-test-password"),
    )


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (204, "delivered"),
        (302, "terminal-failure"),
        (400, "terminal-failure"),
        (408, "transient-failure"),
        (429, "transient-failure"),
        (503, "transient-failure"),
    ],
)
def test_provider_http_classification(status: int, outcome: str) -> None:
    assert classify(status).outcome == outcome


def test_ntfy_and_telegram_payloads_are_adapter_owned(
    provider_settings: Settings,
) -> None:
    original = notification(
        channels=["telegram", "ntfy"],
        title="Unicode ✓",
        message="Condition",
        response_actions=[ResponseAction(action="approve", label="Approve")],
    )
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.extensions["timeout"]["read"] == 5
        if request.url.host == "ntfy.test":
            body = json.loads(request.content)
            assert body == {
                "topic": "test-topic",
                "title": original.title,
                "message": original.message,
                "priority": 3,
            }
            assert (
                request.headers["Authorization"]
                == "Bearer test-ntfy-auth-token-0123456789"
            )
            return httpx.Response(200, json={"id": "message"})
        body = json.loads(request.content)
        if request.url.path.endswith("answerCallbackQuery"):
            assert body == {"callback_query_id": "query-1"}
            return httpx.Response(200, json={"ok": True, "result": True})
        assert body["chat_id"] == -123
        assert body["text"] == "Unicode ✓\n\nCondition"
        signed = body["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
        assert len(signed.encode()) <= 64
        assert parse_callback(signed, WEBHOOK_SECRET) == (original.request_id, 0)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 123}})

    transport = httpx.MockTransport(handle)
    assert (
        NtfyProvider(provider_settings, transport).send(original).outcome == "delivered"
    )
    telegram = TelegramProvider(provider_settings, transport)
    assert telegram.send(original).message_id == 123
    telegram.acknowledge("query-1")
    assert len(seen) == 3
    assert set(provider_registry(provider_settings)) == {"ntfy", "telegram", "smtp"}
    for secret in (
        BOT_TOKEN,
        WEBHOOK_SECRET,
        "smtp-test-password",
        "test-ntfy-auth-token-0123456789",
    ):
        assert secret not in repr(provider_settings)
        assert secret not in str(provider_settings.model_dump())
        with pytest.raises(ValueError, match="Credential-bearing"):
            SecretPolicy(provider_settings.secret_values()).ensure_safe(
                {"message": secret}
            )


@pytest.mark.parametrize(
    "value",
    [
        "bad",
        "0.0.bad",
        callback_data(uuid4(), 0, "different-secret"),
        "not-a-uuid.0.signature",
        "0.999.signature",
    ],
)
def test_invalid_signed_callbacks_fail(value: str) -> None:
    with pytest.raises(ValueError, match="Invalid callback"):
        parse_callback(value, WEBHOOK_SECRET)


@pytest.mark.parametrize(
    ("response", "outcome", "code"),
    [
        (
            httpx.Response(
                200, json={"ok": False, "error_code": 429, "description": BOT_TOKEN}
            ),
            "transient-failure",
            "http-429",
        ),
        (
            httpx.Response(200, json={"ok": False}),
            "terminal-failure",
            "invalid-response",
        ),
        (
            httpx.Response(200, json={"ok": True, "result": {}}),
            "terminal-failure",
            "invalid-response",
        ),
        (
            httpx.Response(200, content=b"not json"),
            "terminal-failure",
            "invalid-response",
        ),
        (httpx.Response(200, json=[]), "terminal-failure", "invalid-response"),
        (
            httpx.Response(200, content=b"x" * 65537),
            "terminal-failure",
            "oversize-response",
        ),
        (httpx.Response(500, text=BOT_TOKEN), "transient-failure", "http-500"),
    ],
)
def test_telegram_failures_never_persist_provider_text(
    provider_settings: Settings, response: httpx.Response, outcome: str, code: str
) -> None:
    transport = httpx.MockTransport(lambda _request: response)
    result = TelegramProvider(provider_settings, transport).send(notification())
    assert result.outcome == outcome
    assert result.error_code == code
    assert BOT_TOKEN not in str(result)


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (httpx.ReadTimeout(BOT_TOKEN), "timeout"),
        (httpx.ConnectError(BOT_TOKEN), "transport-error"),
        (httpx.InvalidURL(BOT_TOKEN), "invalid-response"),
    ],
)
def test_http_adapter_transport_errors(
    provider_settings: Settings, error: Exception, code: str
) -> None:
    def handle(_request: httpx.Request) -> httpx.Response:
        raise error

    result, _body = HttpProvider(provider_settings, httpx.MockTransport(handle)).post(
        "http://provider.test", {}
    )
    assert result.error_code == code


def test_unconfigured_and_oversize_telegram(
    settings: Settings, provider_settings: Settings
) -> None:
    assert (
        NtfyProvider(settings).send(notification()).error_code
        == "provider-unconfigured"
    )
    assert (
        TelegramProvider(settings).send(notification()).error_code
        == "provider-unconfigured"
    )
    assert (
        SmtpProvider(settings).send(notification()).error_code
        == "provider-unconfigured"
    )
    assert (
        TelegramProvider(provider_settings)
        .send(notification(message="x" * 4096))
        .error_code
        == "message-too-long"
    )
    assert (
        TelegramProvider(settings)
        .send(
            notification(
                response_actions=[ResponseAction(action="approve", label="Approve")]
            )
        )
        .error_code
        == "provider-unconfigured"
    )


@pytest.mark.parametrize("tls", ["starttls", "ssl"])
def test_smtp_uses_tls_credentials_and_stable_message_id(
    provider_settings: Settings, tls: str
) -> None:
    configured = provider_settings.model_copy(update={"smtp_tls": tls})
    original = notification()
    connection = MagicMock()
    client = connection.__enter__.return_value
    client.send_message.return_value = {}
    with (
        patch("events_service.providers.smtplib.SMTP", return_value=connection),
        patch("events_service.providers.smtplib.SMTP_SSL", return_value=connection),
    ):
        assert SmtpProvider(configured).send(original).outcome == "delivered"
    assert client.starttls.call_count == (1 if tls == "starttls" else 0)
    client.login.assert_called_once_with("smtp-test-user", "smtp-test-password")
    message = client.send_message.call_args.args[0]
    assert message["Message-ID"] == f"<{original.request_id}@wood-notify>"
    assert message["Subject"] == original.title


@pytest.mark.parametrize(
    ("error", "outcome", "code"),
    [
        (smtplib.SMTPDataError(451, b"secret"), "transient-failure", "smtp-451"),
        (
            smtplib.SMTPAuthenticationError(535, b"secret"),
            "terminal-failure",
            "smtp-535",
        ),
        (
            smtplib.SMTPRecipientsRefused({"test": (450, b"secret")}),
            "transient-failure",
            "recipients-refused",
        ),
        (
            smtplib.SMTPRecipientsRefused({"test": (550, b"secret")}),
            "terminal-failure",
            "recipients-refused",
        ),
        (OSError("secret"), "transient-failure", "smtp-transport-error"),
        (smtplib.SMTPException("secret"), "terminal-failure", "smtp-error"),
    ],
)
def test_smtp_errors_are_normalized(
    provider_settings: Settings, error: Exception, outcome: str, code: str
) -> None:
    with patch("events_service.providers.smtplib.SMTP", side_effect=error):
        result = SmtpProvider(provider_settings).send(notification())
    assert result.outcome == outcome
    assert result.error_code == code


def test_smtp_partial_failure_does_not_retry_successful_recipients(
    provider_settings: Settings,
) -> None:
    connection = MagicMock()
    connection.__enter__.return_value.send_message.return_value = {
        "failed": (550, b"secret")
    }
    with patch("events_service.providers.smtplib.SMTP", return_value=connection):
        assert (
            SmtpProvider(provider_settings).send(notification()).error_code
            == "partial-recipient-failure"
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"ntfy_base_url": "file:///tmp/messages"},
        {"ntfy_base_url": "http://user:pass@host", "ntfy_topic": "test"},
        {"ntfy_base_url": "http://ntfy"},
        {"ntfy_base_url": "http://ntfy", "ntfy_topic": "bad/topic"},
        {"telegram_bot_token": BOT_TOKEN},
        {"telegram_webhook_secret": "short"},
        {"smtp_host": "smtp"},
        {"smtp_username": "user"},
        {"smtp_sender": "bad\naddress"},
    ],
)
def test_invalid_provider_configuration_fails_closed(
    settings: Settings, changes: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate(
            {
                "database_url": settings.database_url,
                "producer_credentials": settings.producer_credentials,
                **changes,
            }
        )


def test_policy_all_dimensions_and_requested_channel_intersection() -> None:
    policy = NotificationPolicy(
        name="failures",
        channels=["telegram", "ntfy"],
        sources={"test"},
        event_types={"check"},
        severities={Severity.ERROR},
        notification_types={"error"},
        policy_keys={"broker-event"},
    )
    router = NotificationRouter([policy])
    request = router.from_event(event(severity="error"))
    assert router.channels(request, "check") == ["ntfy", "telegram"]
    assert router.channels(
        request.model_copy(update={"channels": ["telegram", "smtp"]}), "check"
    ) == ["telegram"]
    for field, value in [
        ("source", "other"),
        ("severity", "info"),
        ("notification_type", "success"),
        ("policy_key", "other"),
    ]:
        assert router.channels(request.model_copy(update={field: value}), "check") == []
    assert router.channels(request, "different") == []
