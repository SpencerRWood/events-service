"""Provider-independent v1 transport contracts."""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID, uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    model_validator,
)

Name = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[\w.-]+$")]
Text = Annotated[str, Field(min_length=1, max_length=4096)]


class Contract(BaseModel):
    """Reject unknown fields rather than accepting provider credentials."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["1"] = "1"


class Severity(StrEnum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class EventEnvelope(Contract):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: Name
    source: Name
    severity: Severity
    occurred_at: AwareDatetime
    correlation_id: UUID
    causation_id: UUID | None = None
    data: dict[str, JsonValue]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    idempotency_key: Name | None = None


class ResponseAction(Contract):
    action: Name
    label: Annotated[str, Field(min_length=1, max_length=128)]


class NotificationRequest(Contract):
    request_id: UUID = Field(default_factory=uuid4)
    source: Name
    notification_type: Literal[
        "informational", "success", "warning", "error", "action-required"
    ]
    severity: Severity
    title: Annotated[str, Field(min_length=1, max_length=256)]
    message: Text
    correlation_id: UUID
    causation_id: UUID | None = None
    channels: list[Name] = Field(default_factory=list, max_length=16)
    policy_key: Name | None = None
    suppression_key: Name | None = None
    idempotency_key: Name | None = None
    context: dict[str, JsonValue] = Field(default_factory=dict)
    response_actions: list[ResponseAction] = Field(default_factory=list, max_length=16)
    response_deadline: AwareDatetime | None = None

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if not self.channels and not self.policy_key:
            raise ValueError("channels or policy_key is required")
        if len(set(self.channels)) != len(self.channels):
            raise ValueError("channels must be unique")
        actions = [item.action for item in self.response_actions]
        if len(set(actions)) != len(actions):
            raise ValueError("response actions must be unique")
        if self.notification_type == "action-required" and not actions:
            raise ValueError("action-required requests require response actions")
        return self


class NormalizedResponse(Contract):
    response_id: UUID = Field(default_factory=uuid4)
    request_id: UUID
    correlation_id: UUID
    causation_id: UUID | None = None
    selected_action: Name
    responder_reference: Name | None = None
    received_at: AwareDatetime
    provider: Name
    provider_response_id: Name
    data: dict[str, JsonValue] = Field(default_factory=dict)


class Receipt(Contract):
    record_id: UUID
    correlation_id: UUID
    accepted_at: datetime
    duplicate: bool
