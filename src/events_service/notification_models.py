"""Notification scheduling and immutable provider message references."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from events_service.storage import Base


class NotificationState(Base):
    __tablename__ = "notify_states"
    __table_args__ = (
        CheckConstraint(
            "response_state IN ('none', 'awaiting-response', 'responded', "
            "'expired', 'suppressed', 'closed')",
            name="ck_notify_states_response_state",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        ForeignKey("notify_requests.id", ondelete="RESTRICT"), primary_key=True
    )
    response_state: Mapped[str] = mapped_column(String(32))
    response_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class NotificationJob(Base):
    __tablename__ = "notify_delivery_jobs"
    __table_args__ = (
        UniqueConstraint("request_id", "channel"),
        CheckConstraint(
            "status IN ('pending', 'delivered', 'suppressed', 'transient-failure', "
            "'terminal-failure', 'expired', 'dispatching')"
        ),
        CheckConstraint("attempts >= 0 AND generation >= 0"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    request_id: Mapped[UUID] = mapped_column(
        ForeignKey("notify_requests.id", ondelete="RESTRICT"), index=True
    )
    channel: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32))
    attempts: Mapped[int] = mapped_column(default=0)
    generation: Mapped[int] = mapped_column(default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SuppressionWindow(Base):
    __tablename__ = "notify_suppression_windows"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    until: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class NotificationMessage(Base):
    __tablename__ = "notify_messages"
    __table_args__ = (UniqueConstraint("chat_id", "message_id"),)
    id: Mapped[UUID] = mapped_column(primary_key=True)
    request_id: Mapped[UUID] = mapped_column(
        ForeignKey("notify_requests.id", ondelete="RESTRICT"), index=True
    )
    correlation_id: Mapped[UUID] = mapped_column(index=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    message_id: Mapped[int] = mapped_column(BigInteger)
    delivered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class MessageEdit(Base):
    """One immutable terminal edit per original, source-owned notification."""

    __tablename__ = "notify_message_edits"
    __table_args__ = (
        UniqueConstraint("request_id"),
        CheckConstraint("attempts >= 0"),
        CheckConstraint(
            "status IN ('pending','dispatching','uncertain',"
            "'delivered','terminal-failure')"
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    request_id: Mapped[UUID] = mapped_column(
        ForeignKey("notify_requests.id", ondelete="RESTRICT"), index=True
    )
    message_ref: Mapped[UUID] = mapped_column(
        ForeignKey("notify_messages.id", ondelete="RESTRICT")
    )
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(32))
    attempts: Mapped[int] = mapped_column(default=0)
    error_code: Mapped[str | None] = mapped_column(String(128))
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class RecoveryNotificationPart(Base):
    """Frozen Telegram parts; aggregate job advances only after confirmed delivery."""

    __tablename__ = "notify_recovery_parts"
    __table_args__ = (
        UniqueConstraint("request_id", "position"),
        CheckConstraint("position >= 1 AND attempts >= 0"),
        CheckConstraint(
            "status IN ('pending', 'dispatching', 'delivered', "
            "'transient-failure', 'terminal-failure')"
        ),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True)
    request_id: Mapped[UUID] = mapped_column(
        ForeignKey("notify_requests.id", ondelete="RESTRICT"), index=True
    )
    position: Mapped[int]
    payload: Mapped[dict[str, str]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(32))
    attempts: Mapped[int] = mapped_column(default=0)
    error_code: Mapped[str | None] = mapped_column(String(128))
    message_id: Mapped[int | None] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
