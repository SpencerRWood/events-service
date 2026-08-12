from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import smtplib
from typing import Protocol

import httpx

from shared.config import NotifySettings, NtfySettings

from .schemas import BrokerEventIn


@dataclass(frozen=True)
class DeliveryResult:
    status_code: int | None
    error: str | None = None


class NotificationAdapter(Protocol):
    def deliver(self, event: BrokerEventIn, target: str | None) -> DeliveryResult:
        pass


class NtfyAdapter:
    def __init__(
        self,
        settings: NtfySettings,
        *,
        timeout_seconds: float,
        client: httpx.Client | None = None,
    ) -> None:
        self._settings = settings
        self._timeout_seconds = timeout_seconds
        self._client = client

    def deliver(self, event: BrokerEventIn, target: str | None) -> DeliveryResult:
        topic = target or event.event_type.replace(".", "-")
        url = f"{str(self._settings.base_url).rstrip('/')}/{topic}"
        headers = {"Title": event.subject or event.event_type, "Priority": _ntfy_priority(event.severity)}
        try:
            if self._client is not None:
                response = self._client.post(url, content=_message(event), headers=headers, timeout=self._timeout_seconds)
            else:
                with httpx.Client(timeout=self._timeout_seconds) as client:
                    response = client.post(url, content=_message(event), headers=headers)
        except httpx.HTTPError as err:
            return DeliveryResult(None, str(err))
        if 200 <= response.status_code < 300:
            return DeliveryResult(response.status_code)
        return DeliveryResult(response.status_code, response.text[:1000])


class TelegramAdapter:
    def __init__(
        self,
        settings: NotifySettings,
        *,
        timeout_seconds: float,
        client: httpx.Client | None = None,
    ) -> None:
        self._settings = settings
        self._timeout_seconds = timeout_seconds
        self._client = client

    def deliver(self, event: BrokerEventIn, target: str | None) -> DeliveryResult:
        if not self._settings.telegram_bot_token:
            return DeliveryResult(None, "Telegram bot token is not configured.")
        chat_id = target or self._settings.telegram_chat_id
        if not chat_id:
            return DeliveryResult(None, "Telegram chat id is not configured.")
        base_url = str(self._settings.telegram_base_url).rstrip("/")
        token = self._settings.telegram_bot_token.get_secret_value()
        url = f"{base_url}/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": _message(event)}
        try:
            if self._client is not None:
                response = self._client.post(url, json=payload, timeout=self._timeout_seconds)
            else:
                with httpx.Client(timeout=self._timeout_seconds) as client:
                    response = client.post(url, json=payload)
        except httpx.HTTPError as err:
            return DeliveryResult(None, str(err))
        if 200 <= response.status_code < 300:
            return DeliveryResult(response.status_code)
        return DeliveryResult(response.status_code, response.text[:1000])


class SmtpAdapter:
    def __init__(self, settings: NotifySettings) -> None:
        self._settings = settings

    def deliver(self, event: BrokerEventIn, target: str | None) -> DeliveryResult:
        if not self._settings.smtp_host:
            return DeliveryResult(None, "SMTP host is not configured.")
        sender = self._settings.smtp_from_address
        recipient = target or self._settings.smtp_to_address
        if not sender or not recipient:
            return DeliveryResult(None, "SMTP sender or recipient is not configured.")
        message = EmailMessage()
        message["From"] = sender
        message["To"] = recipient
        message["Subject"] = event.subject or event.event_type
        message.set_content(_message(event))
        try:
            with smtplib.SMTP(self._settings.smtp_host, self._settings.smtp_port) as smtp:
                if self._settings.smtp_username and self._settings.smtp_password:
                    smtp.starttls()
                    smtp.login(
                        self._settings.smtp_username,
                        self._settings.smtp_password.get_secret_value(),
                    )
                smtp.send_message(message)
        except OSError as err:
            return DeliveryResult(None, str(err))
        return DeliveryResult(250)


def default_adapters(
    settings: NotifySettings,
    ntfy_settings: NtfySettings,
    *,
    ntfy_client: httpx.Client | None = None,
    telegram_client: httpx.Client | None = None,
) -> dict[str, NotificationAdapter]:
    return {
        "ntfy": NtfyAdapter(ntfy_settings, timeout_seconds=settings.delivery_timeout_seconds, client=ntfy_client),
        "telegram": TelegramAdapter(settings, timeout_seconds=settings.delivery_timeout_seconds, client=telegram_client),
        "smtp": SmtpAdapter(settings),
    }


def _message(event: BrokerEventIn) -> str:
    subject = event.subject or event.event_type
    return f"[{event.severity}] {subject}\nsource={event.source}\nevent_id={event.event_id}"


def _ntfy_priority(severity: str) -> str:
    return {
        "debug": "1",
        "info": "3",
        "warning": "4",
        "error": "5",
        "critical": "5",
    }.get(severity, "3")
