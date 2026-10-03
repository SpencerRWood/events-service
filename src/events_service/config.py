"""Runtime-only secrets, separated from operational settings and references."""

import re
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

from events_service.notification_policy import NotificationPolicy
from events_service.routing import Subscription

Scope = Literal[
    "events:write",
    "events:read",
    "events:replay",
    "notifications:write",
    "notifications:read",
    "notifications:retry",
    "notifications:consume",
]


class ProducerCredential(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str = Field(min_length=1)
    token: SecretStr
    scopes: set[Scope]

    @model_validator(mode="after")
    def validate_token(self) -> Self:
        if len(self.token.get_secret_value()) < 32 or not self.scopes:
            raise ValueError(
                "producer requires a token of at least 32 characters and scopes"
            )
        return self


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="WES_", extra="forbid", populate_by_name=True, env_ignore_empty=True
    )
    database_url: SecretStr = Field(repr=False, exclude=True)
    producer_credentials: list[ProducerCredential] = Field(
        min_length=1, repr=False, exclude=True
    )
    secret_references: dict[str, str] = Field(default_factory=dict)
    event_retention_days: int = Field(default=90, ge=1, le=3650)
    notification_retention_days: int = Field(default=90, ge=1, le=3650)
    delivery_retention_days: int = Field(default=90, ge=1, le=3650)
    response_retention_days: int = Field(default=90, ge=1, le=3650)
    database_timeout_seconds: int = Field(default=5, ge=1, le=30)
    subscriptions: list[Subscription] = Field(default_factory=list, max_length=256)
    webhook_timeout_seconds: float = Field(default=5, gt=0, le=30)
    webhook_max_attempts: int = Field(default=5, ge=1, le=20)
    webhook_backoff_seconds: float = Field(default=1, gt=0, le=300)
    webhook_max_backoff_seconds: float = Field(default=60, gt=0, le=3600)
    broker_poll_seconds: float = Field(default=1, gt=0, le=60)
    notification_policies: list[NotificationPolicy] = Field(
        default_factory=list, max_length=256
    )
    suppression_seconds: int = Field(default=60, ge=0, le=86400)
    provider_timeout_seconds: float = Field(default=5, gt=0, le=30)
    notification_max_attempts: int = Field(default=5, ge=1, le=20)
    notification_backoff_seconds: float = Field(default=1, gt=0, le=300)
    notification_max_backoff_seconds: float = Field(default=60, gt=0, le=3600)
    notification_poll_seconds: float = Field(default=1, gt=0, le=60)
    ntfy_base_url: str | None = Field(default=None, validation_alias="NTFY_BASE_URL")
    ntfy_topic: str | None = Field(default=None, validation_alias="NTFY_TOPIC")
    ntfy_auth_token: SecretStr | None = Field(
        default=None, validation_alias="NTFY_AUTH_TOKEN", repr=False, exclude=True
    )
    telegram_bot_token: SecretStr | None = Field(
        default=None, validation_alias="TELEGRAM_BOT_TOKEN", repr=False, exclude=True
    )
    telegram_chat_id: int | None = Field(
        default=None, validation_alias="TELEGRAM_CHAT_ID"
    )
    telegram_webhook_secret: SecretStr | None = Field(
        default=None,
        validation_alias="TELEGRAM_WEBHOOK_SECRET",
        repr=False,
        exclude=True,
    )
    telegram_allowed_user_ids: set[int] = Field(
        default_factory=set, validation_alias="TELEGRAM_ALLOWED_USER_IDS"
    )
    telegram_api_base_url: str = Field(
        default="https://api.telegram.org", validation_alias="TELEGRAM_API_BASE_URL"
    )
    smtp_host: str | None = Field(default=None, validation_alias="SMTP_HOST")
    smtp_port: int = Field(default=587, ge=1, le=65535, validation_alias="SMTP_PORT")
    smtp_username: SecretStr | None = Field(
        default=None, validation_alias="SMTP_USERNAME", repr=False, exclude=True
    )
    smtp_password: SecretStr | None = Field(
        default=None, validation_alias="SMTP_PASSWORD", repr=False, exclude=True
    )
    smtp_sender: str | None = Field(default=None, validation_alias="SMTP_SENDER")
    smtp_recipients: list[str] = Field(
        default_factory=list, validation_alias="SMTP_RECIPIENTS"
    )
    smtp_tls: Literal["starttls", "ssl"] = Field(
        default="starttls", validation_alias="SMTP_TLS"
    )

    @model_validator(mode="after")
    def validate_runtime(self) -> Self:
        url = make_url(self.database_url.get_secret_value())
        if url.drivername != "postgresql+psycopg" or not url.database:
            raise ValueError(
                "WES_DATABASE_URL must use postgresql+psycopg with a database"
            )
        tokens = [item.token.get_secret_value() for item in self.producer_credentials]
        if len(set(tokens)) != len(tokens):
            raise ValueError("producer tokens must be unique")
        consumers = [item.consumer for item in self.subscriptions]
        if len(set(consumers)) != len(consumers):
            raise ValueError("subscription consumer names must be unique")
        self.validate_providers()
        return self

    def validate_providers(self) -> None:
        for value in (self.ntfy_base_url, self.telegram_api_base_url):
            if value is not None:
                url = urlsplit(value)
                if (
                    url.scheme not in {"http", "https"}
                    or not url.hostname
                    or url.username
                    or url.password
                    or url.query
                    or url.fragment
                ):
                    raise ValueError(
                        "provider URL must be HTTP(S) without credentials or query"
                    )
        if bool(self.ntfy_base_url) != bool(self.ntfy_topic):
            raise ValueError("ntfy requires both base URL and topic")
        if self.ntfy_topic and not re.fullmatch(
            r"[A-Za-z0-9_-]{1,128}", self.ntfy_topic
        ):
            raise ValueError("invalid ntfy topic")
        if self.telegram_bot_token is not None and (
            self.telegram_chat_id is None or self.telegram_webhook_secret is None
        ):
            raise ValueError("Telegram requires chat ID and webhook secret")
        if self.telegram_webhook_secret is not None and not re.fullmatch(
            r"[A-Za-z0-9_-]{32,256}", self.telegram_webhook_secret.get_secret_value()
        ):
            raise ValueError(
                "Telegram webhook secret must be 32-256 URL-safe characters"
            )
        if self.smtp_host and (not self.smtp_sender or not self.smtp_recipients):
            raise ValueError("SMTP requires sender and recipients")
        if bool(self.smtp_username) != bool(self.smtp_password):
            raise ValueError("SMTP login requires username and password")
        if any(
            "\r" in value or "\n" in value
            for value in [self.smtp_sender or "", *self.smtp_recipients]
        ):
            raise ValueError("invalid SMTP address")

    def secret_values(self) -> tuple[str, ...]:
        url = make_url(self.database_url.get_secret_value())
        return (
            self.database_url.get_secret_value(),
            *([url.password] if url.password else []),
            *(item.token.get_secret_value() for item in self.producer_credentials),
            *(
                item.auth_token.get_secret_value()
                for item in self.subscriptions
                if item.auth_token is not None
            ),
            *(
                value.get_secret_value()
                for value in (
                    self.ntfy_auth_token,
                    self.telegram_bot_token,
                    self.telegram_webhook_secret,
                    self.smtp_username,
                    self.smtp_password,
                )
                if value is not None
            ),
        )
