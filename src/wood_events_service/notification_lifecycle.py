"""Atomic notification planning, bounded scheduling and read-only outcome capture."""

import asyncio
import hashlib
import json
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from wood_events_service.config import Settings
from wood_events_service.contracts import NotificationRequest
from wood_events_service.notification_models import (
    NotificationJob,
    NotificationMessage,
    NotificationState,
    SuppressionWindow,
)
from wood_events_service.notification_policy import NotificationRouter
from wood_events_service.providers import Provider, ProviderResult
from wood_events_service.storage import NotificationRecord, NotifyDelivery


class NotificationLifecycle:
    def __init__(
        self, engine: Engine, settings: Settings, providers: dict[str, Provider]
    ) -> None:
        self.engine = engine
        self.settings = settings
        self.providers = providers
        self.router = NotificationRouter(settings.notification_policies)

    def schedule(
        self, session: Session, request: NotificationRequest, accepted_at: datetime
    ) -> None:
        event_type = request.context.get("event_type")
        channels = self.router.channels(
            request, event_type if isinstance(event_type, str) else None
        )
        if request.response_actions and channels and "telegram" not in channels:
            raise ValueError("Interactive notifications require Telegram")
        suppressed = not channels or self.is_suppressed(session, request, accepted_at)
        expired = (
            request.response_deadline is not None
            and request.response_deadline <= accepted_at
        )
        response_state = (
            "suppressed"
            if suppressed
            else (
                "expired"
                if expired
                else ("awaiting-response" if request.response_actions else "none")
            )
        )
        session.add(
            NotificationState(
                id=request.request_id,
                response_state=response_state,
                response_deadline=request.response_deadline,
                updated_at=accepted_at,
            )
        )
        for channel in channels:
            status = (
                "suppressed" if suppressed else ("expired" if expired else "pending")
            )
            session.add(
                NotificationJob(
                    id=uuid4(),
                    request_id=request.request_id,
                    channel=channel,
                    status=status,
                    next_attempt_at=accepted_at,
                    updated_at=accepted_at,
                )
            )
            if status == "suppressed":
                session.add(
                    NotifyDelivery(
                        id=uuid4(),
                        request_id=request.request_id,
                        correlation_id=request.correlation_id,
                        destination=channel,
                        outcome="suppressed",
                        attempted_at=accepted_at,
                        error_code="suppression-window",
                    )
                )

    def is_suppressed(
        self, session: Session, request: NotificationRequest, now: datetime
    ) -> bool:
        if self.settings.suppression_seconds == 0:
            return False
        content: object = request.suppression_key or {
            key: value
            for key, value in request.model_dump(mode="json").items()
            if key
            in {
                "source",
                "title",
                "message",
                "notification_type",
                "severity",
                "channels",
                "policy_key",
                "response_actions",
            }
        }
        key = hashlib.sha256(
            json.dumps([request.source, content], sort_keys=True).encode()
        ).hexdigest()
        session.execute(
            insert(SuppressionWindow)
            .values(key=key, until=now)
            .on_conflict_do_nothing()
        )
        window = session.scalar(
            select(SuppressionWindow)
            .where(SuppressionWindow.key == key)
            .with_for_update()
        )
        if window is None:
            raise RuntimeError("Suppression window missing")
        if window.until > now:
            return True
        window.until = now + timedelta(seconds=self.settings.suppression_seconds)
        return False

    def expire(self, *, as_of: datetime, request_id: UUID | None = None) -> int:
        if as_of.tzinfo is None:
            raise ValueError("Expiration requires an aware timestamp")
        statement = select(NotificationState).where(
            NotificationState.response_state.in_(["awaiting-response", "none"]),
            NotificationState.response_deadline <= as_of,
        )
        if request_id is not None:
            statement = statement.where(NotificationState.id == request_id)
        with Session(self.engine) as session, session.begin():
            rows = session.scalars(
                statement.order_by(NotificationState.response_deadline)
                .limit(100)
                .with_for_update(skip_locked=True)
            ).all()
            for state in rows:
                state.response_state = "expired"
                state.updated_at = as_of
                session.execute(
                    update(NotificationJob)
                    .where(
                        NotificationJob.request_id == state.id,
                        NotificationJob.status.in_(["pending", "transient-failure"]),
                    )
                    .values(status="expired", updated_at=as_of)
                )
            return len(rows)

    def deliver_one(self, *, as_of: datetime | None = None) -> bool:
        now = as_of or datetime.now(UTC)
        self.expire(as_of=now)
        with Session(self.engine) as session, session.begin():
            row = session.execute(
                select(NotificationJob, NotificationState)
                .join(
                    NotificationState,
                    NotificationState.id == NotificationJob.request_id,
                )
                .where(
                    NotificationJob.status.in_(["pending", "transient-failure"]),
                    NotificationJob.next_attempt_at <= now,
                )
                .order_by(NotificationJob.next_attempt_at, NotificationJob.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            ).first()
            if row is None:
                return False
            job, state = row
            parent = session.get(NotificationRecord, job.request_id)
            if parent is None:
                raise RuntimeError("Notification parent missing")
            request = NotificationRequest.model_validate(parent.payload)
            # The bounded expiration sweep may leave more overdue requests.
            # Recheck this locked request before performing any remote delivery.
            if (
                request.response_deadline is not None
                and request.response_deadline <= now
            ):
                state.response_state = "expired"
                state.updated_at = now
                session.execute(
                    update(NotificationJob)
                    .where(
                        NotificationJob.request_id == state.id,
                        NotificationJob.status.in_(["pending", "transient-failure"]),
                    )
                    .values(status="expired", updated_at=now)
                )
                return True
            provider = self.providers.get(job.channel)
            result = (
                provider.send(request)
                if provider is not None
                else ProviderResult("terminal-failure", "provider-unconfigured")
            )
            completed = datetime.now(UTC) if as_of is None else now
            job.attempts += 1
            job.updated_at = completed
            job.status = result.outcome
            if result.outcome == "transient-failure":
                if job.attempts >= self.settings.notification_max_attempts:
                    job.status = "terminal-failure"
                delay = min(
                    self.settings.notification_max_backoff_seconds,
                    self.settings.notification_backoff_seconds
                    * 2 ** (job.attempts - 1),
                )
                job.next_attempt_at = completed + timedelta(seconds=delay)
            session.add(
                NotifyDelivery(
                    id=uuid4(),
                    request_id=parent.id,
                    correlation_id=parent.correlation_id,
                    destination=job.channel,
                    outcome=result.outcome,
                    error_code=result.error_code,
                    attempted_at=completed,
                )
            )
            if (
                job.channel == "telegram"
                and result.message_id is not None
                and self.settings.telegram_chat_id is not None
            ):
                session.add(
                    NotificationMessage(
                        id=uuid4(),
                        request_id=parent.id,
                        correlation_id=parent.correlation_id,
                        chat_id=self.settings.telegram_chat_id,
                        message_id=result.message_id,
                        delivered_at=completed,
                    )
                )
            state.updated_at = completed
            return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                worked = await asyncio.to_thread(self.deliver_one)
            except SQLAlchemyError:
                worked = False
            if not worked:
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=self.settings.notification_poll_seconds
                    )

    def retry(self, request_id: UUID, channel: str, source: str) -> bool:
        now = datetime.now(UTC)
        self.expire(as_of=now, request_id=request_id)
        with Session(self.engine) as session, session.begin():
            row = session.execute(
                select(NotificationJob, NotificationState)
                .join(
                    NotificationState,
                    NotificationState.id == NotificationJob.request_id,
                )
                .join(
                    NotificationRecord,
                    NotificationRecord.id == NotificationJob.request_id,
                )
                .where(
                    NotificationJob.request_id == request_id,
                    NotificationJob.channel == channel,
                    NotificationRecord.source == source,
                )
                .with_for_update(of=(NotificationJob, NotificationState))
            ).first()
            if row is None:
                return False
            job, state = row
            if state.response_state in {
                "expired",
                "responded",
                "suppressed",
            } or job.status not in {"terminal-failure", "transient-failure"}:
                raise ValueError("Only failed, active notifications can be retried")
            job.status = "pending"
            job.attempts = 0
            job.generation += 1
            job.next_attempt_at = now
            job.updated_at = now
            return True
