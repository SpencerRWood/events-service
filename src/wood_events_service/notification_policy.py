"""Provider-independent, declarative notification policy."""

import json
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field

from wood_events_service.contracts import (
    EventEnvelope,
    Name,
    NotificationRequest,
    Severity,
)


class NotificationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: Name
    channels: list[Name] = Field(min_length=1, max_length=16)
    sources: set[Name] = Field(default_factory=set)
    event_types: set[Name] = Field(default_factory=set)
    notification_types: set[str] = Field(default_factory=set)
    severities: set[Severity] = Field(default_factory=set)
    policy_keys: set[Name] = Field(default_factory=set)

    def matches(
        self, request: NotificationRequest, event_type: str | None = None
    ) -> bool:
        return (
            (not self.sources or request.source in self.sources)
            and (not self.event_types or event_type in self.event_types)
            and (
                not self.notification_types
                or request.notification_type in self.notification_types
            )
            and (not self.severities or request.severity in self.severities)
            and (not self.policy_keys or request.policy_key in self.policy_keys)
        )


class NotificationRouter:
    def __init__(self, policies: list[NotificationPolicy]) -> None:
        self.policies = policies

    def channels(
        self, request: NotificationRequest, event_type: str | None = None
    ) -> list[str]:
        eligible = {
            channel
            for policy in self.policies
            if policy.matches(request, event_type)
            for channel in policy.channels
        }
        if request.channels:
            return sorted(
                eligible.intersection(request.channels)
                if self.policies
                else set(request.channels)
            )
        return sorted(eligible)

    def from_event(self, event: EventEnvelope) -> NotificationRequest:
        types = {
            "info": "informational",
            "success": "success",
            "warning": "warning",
            "error": "error",
            "critical": "error",
        }
        return NotificationRequest.model_validate(
            {
                "request_id": uuid5(
                    NAMESPACE_URL, f"wes-notify:{event.source}:{event.event_id}"
                ),
                "source": event.source,
                "notification_type": types[event.severity],
                "severity": event.severity,
                "title": event.event_type,
                "message": json.dumps(event.data, ensure_ascii=False, sort_keys=True),
                "correlation_id": event.correlation_id,
                "causation_id": event.event_id,
                "policy_key": "broker-event",
                "idempotency_key": f"event-{event.event_id}",
                "context": {
                    "event_type": event.event_type,
                    "event": event.model_dump(mode="json"),
                },
            }
        )
