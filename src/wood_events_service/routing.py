"""Declarative subscription matching, separate from transport execution."""

from typing import Annotated, Protocol, Self
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from wood_events_service.contracts import EventEnvelope, Name, Severity


class Subscription(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    consumer: Annotated[
        str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    ]
    url: str
    event_types: set[Name] = Field(default_factory=set)
    sources: set[Name] = Field(default_factory=set)
    severities: set[Severity] = Field(default_factory=set)
    data_equals: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_url(self) -> Self:
        url = urlsplit(self.url)
        parsed = httpx.URL(self.url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
            or (parsed.port is not None and not 1 <= parsed.port <= 65535)
        ):
            raise ValueError(
                "webhooks require an HTTP URL without credentials or query"
            )
        return self

    def matches(self, event: EventEnvelope) -> bool:
        return (
            (not self.event_types or event.event_type in self.event_types)
            and (not self.sources or event.source in self.sources)
            and (not self.severities or event.severity in self.severities)
            and all(
                key in event.data and event.data[key] == value
                for key, value in self.data_equals.items()
            )
        )


class Router(Protocol):
    def destinations(self, event: EventEnvelope) -> list[str]: ...


class SubscriptionRouter:
    def __init__(self, subscriptions: list[Subscription]) -> None:
        self.subscriptions = subscriptions

    def destinations(self, event: EventEnvelope) -> list[str]:
        return [item.consumer for item in self.subscriptions if item.matches(event)]
