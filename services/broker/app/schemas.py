from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from shared.models import EventSeverity


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "v1"
    event_id: str | None = Field(default=None, min_length=1)
    event_type: str = Field(min_length=1)
    source: str = Field(min_length=1)
    severity: EventSeverity = "info"
    occurred_at: datetime
    subject: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class EventOut(BaseModel):
    id: int
    event_id: str
    schema_version: str
    event_type: str
    source: str
    severity: str
    occurred_at: datetime
    subject: str | None
    payload: dict[str, Any]


class SubscriptionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    target_url: AnyHttpUrl
    event_type: str | None = Field(default=None, min_length=1)
    source: str | None = Field(default=None, min_length=1)
    severity: EventSeverity | None = None
    active: bool = True


class SubscriptionOut(BaseModel):
    id: int
    name: str
    target_url: str
    event_type: str | None
    source: str | None
    severity: str | None
    active: bool


class DeliveryOut(BaseModel):
    id: int
    event_id: str
    subscription_id: int
    target_url: str
    status: str
    attempts: int
    last_status_code: int | None
    last_error: str | None
    next_attempt_after: datetime | None
