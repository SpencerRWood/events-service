from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from shared.models import EventSeverity


NotificationChannel = Literal["ntfy", "telegram", "smtp"]


class BrokerEventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["v1"] = "v1"
    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    source: str = Field(min_length=1)
    severity: EventSeverity = "info"
    occurred_at: datetime
    subject: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class NotificationPolicyIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    channel: NotificationChannel
    event_type: str | None = Field(default=None, min_length=1)
    source: str | None = Field(default=None, min_length=1)
    severity: EventSeverity | None = None
    target: str | None = Field(default=None, min_length=1)
    dedup_window_seconds: int = Field(default=300, ge=0)
    active: bool = True


class NotificationPolicyOut(BaseModel):
    id: int
    name: str
    channel: str
    event_type: str | None
    source: str | None
    severity: str | None
    target: str | None
    dedup_window_seconds: int
    active: bool


class NotificationDeliveryOut(BaseModel):
    id: int
    event_id: str
    dedup_key: str
    policy_id: int
    channel: str
    status: str
    attempts: int
    target: str | None
    last_status_code: int | None
    last_error: str | None
    next_attempt_after: datetime | None


class BrokerEventConsumeOut(BaseModel):
    event_id: str
    matched_policy_count: int
    deliveries: list[NotificationDeliveryOut]
