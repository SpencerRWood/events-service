"""Replaceable ntfy, Telegram Bot API and SMTP delivery adapters."""

import base64
import hmac
import json
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Protocol
from uuid import UUID

import httpx

from events_service.config import Settings
from events_service.contracts import NotificationRequest


@dataclass(frozen=True)
class ProviderResult:
    outcome: str
    error_code: str | None = None
    message_id: int | None = None


class Provider(Protocol):
    def send(self, request: NotificationRequest) -> ProviderResult: ...


def callback_data(request_id: UUID, index: int, secret: str) -> str:
    value = f"{request_id.hex}.{index}"
    digest = hmac.digest(secret.encode(), value.encode(), "sha256")[:16]
    signature = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    return f"{value}.{signature}"


def parse_callback(value: str, secret: str) -> tuple[UUID, int]:
    try:
        request_id, action, _signature = value.split(".")
        identity, index = UUID(hex=request_id), int(action)
        if (
            index < 0
            or index >= 16
            or not hmac.compare_digest(value, callback_data(identity, index, secret))
        ):
            raise ValueError
        return identity, index
    except ValueError, TypeError:
        raise ValueError("Invalid callback identity") from None


def classify(status: int) -> ProviderResult:
    if 200 <= status < 300:
        return ProviderResult("delivered")
    transient = status in {408, 425, 429} or 500 <= status < 600
    return ProviderResult(
        "transient-failure" if transient else "terminal-failure", f"http-{status}"
    )


class HttpProvider:
    def __init__(
        self, settings: Settings, transport: httpx.BaseTransport | None = None
    ) -> None:
        self.settings = settings
        self.transport = transport

    def post(  # noqa: PLR0911 -- bounded response and normalized failure cases
        self,
        url: str,
        payload: dict[str, object],
        headers: dict[str, str] | None = None,
    ) -> tuple[ProviderResult, dict[str, object]]:
        try:
            with (
                httpx.Client(
                    timeout=self.settings.provider_timeout_seconds,
                    follow_redirects=False,
                    trust_env=False,
                    transport=self.transport,
                ) as client,
                client.stream("POST", url, json=payload, headers=headers) as response,
            ):
                result = classify(response.status_code)
                if result.outcome != "delivered":
                    return result, {}
                body = bytearray()
                for chunk in response.iter_bytes():
                    body.extend(chunk)
                    if len(body) > 65536:
                        return ProviderResult(
                            "terminal-failure", "oversize-response"
                        ), {}
                decoded: object = json.loads(body) if body else {}
                if not isinstance(decoded, dict):
                    return ProviderResult("terminal-failure", "invalid-response"), {}
                return result, decoded
        except httpx.TimeoutException:
            return ProviderResult("transient-failure", "timeout"), {}
        except httpx.TransportError:
            return ProviderResult("transient-failure", "transport-error"), {}
        except httpx.InvalidURL, ValueError:
            return ProviderResult("terminal-failure", "invalid-response"), {}


class NtfyProvider(HttpProvider):
    def send(self, request: NotificationRequest) -> ProviderResult:
        if self.settings.ntfy_base_url is None or self.settings.ntfy_topic is None:
            return ProviderResult("terminal-failure", "provider-unconfigured")
        token = self.settings.ntfy_auth_token
        result, _body = self.post(
            self.settings.ntfy_base_url.rstrip("/"),
            {
                "topic": self.settings.ntfy_topic,
                "title": request.title,
                "message": request.message,
                "priority": {
                    "info": 3,
                    "success": 3,
                    "warning": 4,
                    "error": 4,
                    "critical": 5,
                }[request.severity],
            },
            {"Authorization": f"Bearer {token.get_secret_value()}"} if token else None,
        )
        return result


class TelegramProvider(HttpProvider):
    def call(
        self, method: str, payload: dict[str, object]
    ) -> tuple[ProviderResult, dict[str, object]]:
        token = self.settings.telegram_bot_token
        if token is None:
            return ProviderResult("terminal-failure", "provider-unconfigured"), {}
        result, body = self.post(
            f"{self.settings.telegram_api_base_url.rstrip('/')}/bot{token.get_secret_value()}/{method}",
            payload,
        )
        if result.outcome == "delivered" and body.get("ok") is not True:
            code = body.get("error_code")
            result = (
                classify(code)
                if isinstance(code, int) and 400 <= code <= 599
                else ProviderResult("terminal-failure", "invalid-response")
            )
        return result, body

    def send(self, request: NotificationRequest) -> ProviderResult:
        payload: dict[str, object] = {
            "chat_id": self.settings.telegram_chat_id,
            "text": f"{request.title}\n\n{request.message}",
        }
        if len(str(payload["text"])) > 4096:
            return ProviderResult("terminal-failure", "message-too-long")
        if request.response_actions:
            secret = self.settings.telegram_webhook_secret
            if secret is None:
                return ProviderResult("terminal-failure", "provider-unconfigured")
            payload["reply_markup"] = {
                "inline_keyboard": [
                    [
                        {
                            "text": action.label,
                            "callback_data": callback_data(
                                request.request_id, index, secret.get_secret_value()
                            ),
                        }
                    ]
                    for index, action in enumerate(request.response_actions)
                ]
            }
        result, body = self.call("sendMessage", payload)
        if result.outcome != "delivered":
            return result
        message = body.get("result")
        if not isinstance(message, dict) or not isinstance(
            message.get("message_id"), int
        ):
            return ProviderResult("terminal-failure", "invalid-response")
        return ProviderResult("delivered", message_id=message["message_id"])

    def acknowledge(self, callback_id: str) -> None:
        # Capture is already committed. Acknowledgment only clears the UI spinner.
        self.call("answerCallbackQuery", {"callback_query_id": callback_id})


class SmtpProvider:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send(self, request: NotificationRequest) -> ProviderResult:  # noqa: PLR0911 -- SMTP failure classification
        configured = self.settings
        if (
            not configured.smtp_host
            or not configured.smtp_sender
            or not configured.smtp_recipients
        ):
            return ProviderResult("terminal-failure", "provider-unconfigured")
        try:
            message = EmailMessage()
            message["Subject"] = request.title
            message["From"] = configured.smtp_sender
            message["To"] = ", ".join(configured.smtp_recipients)
            message["Message-ID"] = f"<{request.request_id}@wood-notify>"
            message.set_content(request.message)
            context = ssl.create_default_context()
            connection = (
                smtplib.SMTP_SSL(
                    configured.smtp_host,
                    configured.smtp_port,
                    timeout=configured.provider_timeout_seconds,
                    context=context,
                )
                if configured.smtp_tls == "ssl"
                else smtplib.SMTP(
                    configured.smtp_host,
                    configured.smtp_port,
                    timeout=configured.provider_timeout_seconds,
                )
            )
            with connection as client:
                if configured.smtp_tls == "starttls":
                    client.starttls(context=context)
                if configured.smtp_username and configured.smtp_password:
                    client.login(
                        configured.smtp_username.get_secret_value(),
                        configured.smtp_password.get_secret_value(),
                    )
                refused = client.send_message(message)
                if refused:
                    return ProviderResult(
                        "terminal-failure", "partial-recipient-failure"
                    )
            return ProviderResult("delivered")
        except smtplib.SMTPRecipientsRefused as error:
            transient = any(
                400 <= code < 500 for code, _message in error.recipients.values()
            )
            return ProviderResult(
                "transient-failure" if transient else "terminal-failure",
                "recipients-refused",
            )
        except smtplib.SMTPResponseException as error:
            return ProviderResult(
                "transient-failure"
                if 400 <= error.smtp_code < 500
                else "terminal-failure",
                f"smtp-{error.smtp_code}",
            )
        except smtplib.SMTPServerDisconnected:
            return ProviderResult("transient-failure", "smtp-transport-error")
        except smtplib.SMTPException, ValueError:
            return ProviderResult("terminal-failure", "smtp-error")
        except OSError:
            return ProviderResult("transient-failure", "smtp-transport-error")


def provider_registry(settings: Settings) -> dict[str, Provider]:
    return {
        "ntfy": NtfyProvider(settings),
        "telegram": TelegramProvider(settings),
        "smtp": SmtpProvider(settings),
    }
