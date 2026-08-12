from __future__ import annotations

from pydantic import AnyHttpUrl, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class BrokerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BROKER_")

    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)


class NtfySettings(BaseSettings):
    base_url: AnyHttpUrl = Field(default="http://ntfy:80", validation_alias="NTFY_BASE_URL")


class NotifySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NOTIFY_")

    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)


class ExternalInfrastructureSettings(BaseSettings):
    database_url: PostgresDsn | None = Field(
        default=None,
        validation_alias="WOOD_EVENTS_DATABASE_URL",
    )
