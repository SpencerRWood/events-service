"""Database-backed at-least-once delivery with bounded retry and safe evidence."""

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

import httpx
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from wood_events_service.config import Settings
from wood_events_service.contracts import EventEnvelope
from wood_events_service.storage import BrokerDelivery, BrokerJob, EventRecord


@dataclass(frozen=True)
class DeliveryResult:
    outcome: str
    error_code: str | None = None


class Webhook(Protocol):
    def send(self, url: str, event: EventEnvelope, consumer: str) -> DeliveryResult: ...


class HttpWebhook:
    def __init__(
        self,
        timeout: float,
        transport: httpx.BaseTransport | None = None,
        credentials: dict[str, SecretStr] | None = None,
    ):
        self.timeout = timeout
        self.transport = transport
        self.credentials = credentials or {}

    def send(self, url: str, event: EventEnvelope, consumer: str) -> DeliveryResult:
        headers = {
            "Idempotency-Key": f"{event.event_id}:{consumer}",
            "X-Event-ID": str(event.event_id),
            "X-Correlation-ID": str(event.correlation_id),
        }
        if consumer in self.credentials:
            headers["Authorization"] = (
                f"Bearer {self.credentials[consumer].get_secret_value()}"
            )
        try:
            # Never follow redirects or read/store an unbounded consumer response.
            with (
                httpx.Client(
                    timeout=self.timeout,
                    follow_redirects=False,
                    trust_env=False,
                    transport=self.transport,
                ) as client,
                client.stream(
                    "POST",
                    url,
                    json=event.model_dump(mode="json"),
                    headers=headers,
                ) as response,
            ):
                status = response.status_code
        except httpx.TimeoutException:
            return DeliveryResult("transient-failure", "timeout")
        except httpx.TransportError:
            return DeliveryResult("transient-failure", "transport-error")
        except httpx.InvalidURL:
            return DeliveryResult("terminal-failure", "invalid-url")
        if 200 <= status < 300:
            return DeliveryResult("delivered")
        transient = status in {408, 425, 429} or 500 <= status < 600
        return DeliveryResult(
            "transient-failure" if transient else "terminal-failure", f"http-{status}"
        )


class BrokerWorker:
    def __init__(self, engine: Engine, settings: Settings, webhook: Webhook) -> None:
        self.engine = engine
        self.settings = settings
        self.webhook = webhook
        self.urls = {item.consumer: item.url for item in settings.subscriptions}

    def deliver_one(self, *, as_of: datetime | None = None) -> bool:
        now = as_of or datetime.now(UTC)
        with Session(self.engine) as session, session.begin():
            # Lock through the bounded HTTP attempt. Concurrent workers skip the
            # row; a crash rolls back scheduling and safely repeats the same ID.
            job = session.scalar(
                select(BrokerJob)
                .where(BrokerJob.status == "pending", BrokerJob.next_attempt_at <= now)
                .order_by(BrokerJob.next_attempt_at, BrokerJob.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if job is None:
                return False
            record = session.get(EventRecord, job.event_id)
            if record is None:
                raise RuntimeError("Delivery parent missing")
            url = self.urls.get(job.consumer)
            result = (
                self.webhook.send(
                    url, EventEnvelope.model_validate(record.payload), job.consumer
                )
                if url is not None
                else DeliveryResult("terminal-failure", "consumer-unconfigured")
            )
            job.attempts += 1
            job.updated_at = datetime.now(UTC) if as_of is None else now
            session.add(
                BrokerDelivery(
                    id=uuid4(),
                    event_id=record.id,
                    correlation_id=record.correlation_id,
                    destination=job.consumer,
                    outcome=result.outcome,
                    error_code=result.error_code,
                    attempted_at=job.updated_at,
                )
            )
            if result.outcome == "transient-failure":
                job.status = (
                    "pending"
                    if job.attempts < self.settings.webhook_max_attempts
                    else "terminal-failure"
                )
                delay = min(
                    self.settings.webhook_max_backoff_seconds,
                    self.settings.webhook_backoff_seconds * 2 ** (job.attempts - 1),
                )
                job.next_attempt_at = job.updated_at + timedelta(seconds=delay)
            else:
                job.status = result.outcome
            return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                worked = await asyncio.to_thread(self.deliver_one)
            except SQLAlchemyError:
                # Durable work remains pending; never expose driver exception text.
                worked = False
            if not worked:
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=self.settings.broker_poll_seconds
                    )

    def replay(self, event_id: UUID, consumer: str, source: str) -> bool:
        with Session(self.engine) as session, session.begin():
            job = session.scalar(
                select(BrokerJob)
                .join(EventRecord, BrokerJob.event_id == EventRecord.id)
                .where(
                    BrokerJob.event_id == event_id,
                    BrokerJob.consumer == consumer,
                    EventRecord.source == source,
                )
                .with_for_update(of=BrokerJob)
            )
            if job is None:
                return False
            if consumer not in self.urls:
                raise ValueError("Consumer is unconfigured")
            if job.status == "pending":
                raise ValueError("Delivery is already pending")
            job.status = "pending"
            job.attempts = 0
            job.generation += 1
            job.next_attempt_at = datetime.now(UTC)
            job.updated_at = job.next_attempt_at
            return True
