from __future__ import annotations

from pydantic import AnyHttpUrl, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class BrokerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BROKER_")

    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)
    database_url: str
    delivery_timeout_seconds: float = Field(default=5.0, gt=0)
    max_delivery_attempts: int = Field(default=3, ge=1)
    retry_backoff_seconds: int = Field(default=30, ge=0)


class NtfySettings(BaseSettings):
    model_config = SettingsConfigDict(populate_by_name=True)

    base_url: AnyHttpUrl = Field(default="http://ntfy:80", validation_alias="NTFY_BASE_URL")


class NotifySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NOTIFY_", env_ignore_empty=True)

    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)
    database_url: str
    delivery_timeout_seconds: float = Field(default=5.0, gt=0)
    max_delivery_attempts: int = Field(default=3, ge=1)
    retry_backoff_seconds: int = Field(default=30, ge=0)
    telegram_base_url: AnyHttpUrl = "https://api.telegram.org"
    telegram_bot_token: SecretStr | None = None
    telegram_chat_id: str | None = None
    smtp_host: str | None = None
    smtp_port: int = Field(default=587, ge=1, le=65535)
    smtp_username: str | None = None
    smtp_password: SecretStr | None = None
    smtp_from_address: str | None = None
    smtp_to_address: str | None = None


class ExternalInfrastructureSettings(BaseSettings):
    database_url: PostgresDsn | None = Field(
        default=None,
        validation_alias="WOOD_EVENTS_DATABASE_URL",
    )
