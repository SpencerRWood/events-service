"""Runtime-only secrets, separated from operational settings and references."""

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

Scope = Literal["events:write", "notifications:write"]


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
    model_config = SettingsConfigDict(env_prefix="WES_", extra="forbid")
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
        return self

    def secret_values(self) -> tuple[str, ...]:
        url = make_url(self.database_url.get_secret_value())
        return (
            self.database_url.get_secret_value(),
            *([url.password] if url.password else []),
            *(item.token.get_secret_value() for item in self.producer_credentials),
        )
