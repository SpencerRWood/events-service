from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


EventSeverity = Literal["debug", "info", "warning", "error", "critical"]


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["v1"] = "v1"
    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    source: str = Field(min_length=1)
    severity: EventSeverity = "info"
    occurred_at: datetime
    subject: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
