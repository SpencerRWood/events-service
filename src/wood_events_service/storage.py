"""Durable transport records and concurrency-safe producer idempotency."""

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    String,
    UniqueConstraint,
    create_engine,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from wood_events_service.config import Settings
from wood_events_service.contracts import EventEnvelope, NotificationRequest, Receipt
from wood_events_service.routing import Router
from wood_events_service.security import SecretPolicy


class Base(DeclarativeBase):
    pass


class EnvelopeRecord(Base):
    __abstract__ = True
    id: Mapped[UUID] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(128))
    correlation_id: Mapped[UUID] = mapped_column(index=True)
    causation_id: Mapped[UUID | None]
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), index=True
    )


class EventRecord(EnvelopeRecord):
    __tablename__ = "broker_events"
    __table_args__ = (UniqueConstraint("source", "idempotency_key"),)


class NotificationRecord(EnvelopeRecord):
    __tablename__ = "notify_requests"
    __table_args__ = (UniqueConstraint("source", "idempotency_key"),)


class AttemptRecord:
    id: Mapped[UUID] = mapped_column(primary_key=True)
    correlation_id: Mapped[UUID] = mapped_column(index=True)
    destination: Mapped[str] = mapped_column(String(128))
    outcome: Mapped[str] = mapped_column(String(32))
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    error_code: Mapped[str | None] = mapped_column(String(128))


class BrokerDelivery(AttemptRecord, Base):
    __tablename__ = "broker_delivery_attempts"
    event_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_events.id", ondelete="RESTRICT"), index=True
    )


class BrokerJob(Base):
    """Mutable scheduling state; attempt evidence remains append-only."""

    __tablename__ = "broker_delivery_jobs"
    __table_args__ = (
        UniqueConstraint("event_id", "consumer"),
        CheckConstraint("status IN ('pending', 'delivered', 'terminal-failure')"),
        CheckConstraint("attempts >= 0 AND generation >= 0"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    event_id: Mapped[UUID] = mapped_column(
        ForeignKey("broker_events.id", ondelete="RESTRICT"), index=True
    )
    consumer: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), default="pending")
    attempts: Mapped[int] = mapped_column(default=0)
    generation: Mapped[int] = mapped_column(default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class NotifyDelivery(AttemptRecord, Base):
    __tablename__ = "notify_delivery_attempts"
    request_id: Mapped[UUID] = mapped_column(
        ForeignKey("notify_requests.id", ondelete="RESTRICT"), index=True
    )


class ResponseRecord(Base):
    __tablename__ = "notify_responses"
    __table_args__ = (UniqueConstraint("provider", "provider_response_id"),)
    id: Mapped[UUID] = mapped_column(primary_key=True)
    request_id: Mapped[UUID] = mapped_column(
        ForeignKey("notify_requests.id", ondelete="RESTRICT"), index=True
    )
    correlation_id: Mapped[UUID] = mapped_column(index=True)
    causation_id: Mapped[UUID | None]
    provider: Mapped[str] = mapped_column(String(128))
    provider_response_id: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class IdempotencyConflictError(Exception):
    """An identity was reused with different logical content."""


def make_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={
            "connect_timeout": settings.database_timeout_seconds,
            "options": (
                f"-c statement_timeout={settings.database_timeout_seconds * 1000}"
            ),
        },
    )


def check_schema(engine: Engine) -> None:
    """Readiness requires connectivity and the exact foundation migration."""
    with engine.connect() as connection:
        revision = connection.scalar(text("SELECT version_num FROM alembic_version"))
        if revision != "0003_notify_lifecycle":
            raise SQLAlchemyError("Database migration required")


class Store:
    def __init__(
        self,
        engine: Engine,
        secrets: SecretPolicy,
        router: Router | None = None,
        notification_hook: Callable[[Session, NotificationRequest, datetime], None]
        | None = None,
    ) -> None:
        self.engine = engine
        self.secrets = secrets
        self.router = router
        self.notification_hook = notification_hook

    def accept(self, envelope: EventEnvelope | NotificationRequest) -> Receipt:
        payload = envelope.model_dump(mode="json")
        self.secrets.ensure_safe(payload)
        model: type[EventRecord] | type[NotificationRecord]
        if isinstance(envelope, EventEnvelope):
            model, record_id, identity = EventRecord, envelope.event_id, "event_id"
        else:
            model, record_id, identity = (
                NotificationRecord,
                envelope.request_id,
                "request_id",
            )
        with Session(self.engine) as session, session.begin():
            inserted = session.scalar(
                insert(model)
                .values(
                    id=record_id,
                    source=envelope.source,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.causation_id,
                    idempotency_key=envelope.idempotency_key,
                    payload=payload,
                )
                .on_conflict_do_nothing()
                .returning(model.id)
            )
            if inserted is not None:
                stored = session.get(model, inserted)
            else:
                # Check both identities: a request cannot conflate two existing rows.
                by_id = session.get(model, record_id)
                by_key = (
                    session.scalar(
                        select(model).where(
                            model.source == envelope.source,
                            model.idempotency_key == envelope.idempotency_key,
                        )
                    )
                    if envelope.idempotency_key is not None
                    else None
                )
                if by_id is not None and by_key is not None and by_id.id != by_key.id:
                    raise IdempotencyConflictError
                stored = by_id or by_key
                if stored is None:
                    raise IdempotencyConflictError
                expected = dict(stored.payload)
                proposed = dict(payload)
                if by_id is None:
                    expected.pop(identity)
                    proposed.pop(identity)
                if expected != proposed:
                    raise IdempotencyConflictError
            if stored is None:
                raise IdempotencyConflictError
            if (
                inserted is not None
                and isinstance(envelope, NotificationRequest)
                and self.notification_hook
            ):
                self.notification_hook(session, envelope, stored.accepted_at)
            if inserted is not None and isinstance(envelope, EventEnvelope):
                session.add_all(
                    BrokerJob(
                        id=uuid4(),
                        event_id=stored.id,
                        consumer=consumer,
                        next_attempt_at=stored.accepted_at,
                        updated_at=stored.accepted_at,
                    )
                    for consumer in (
                        self.router.destinations(envelope) if self.router else []
                    )
                )
            return Receipt(
                record_id=stored.id,
                correlation_id=stored.correlation_id,
                accepted_at=stored.accepted_at,
                duplicate=inserted is None,
            )
