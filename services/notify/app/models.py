from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import DateTime, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class NotificationChannel(StrEnum):
    ntfy = "ntfy"
    telegram = "telegram"
    smtp = "smtp"


class NotificationStatus(StrEnum):
    delivered = "delivered"
    failed = "failed"
    suppressed = "suppressed"
    terminal = "terminal"


class NotificationPolicyRecord(Base):
    __tablename__ = "notify_policies"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    channel: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    severity: Mapped[str | None] = mapped_column(String(32), nullable=True)
    target: Mapped[str | None] = mapped_column(String(512), nullable=True)
    dedup_window_seconds: Mapped[int] = mapped_column(Integer, default=300)
    active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class NotificationDeliveryRecord(Base):
    __tablename__ = "notify_deliveries"
    __table_args__ = (
        UniqueConstraint("dedup_key", "policy_id", "channel", name="uq_notify_delivery_dedup_policy_channel"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), index=True)
    dedup_key: Mapped[str] = mapped_column(String(255), index=True)
    policy_id: Mapped[int] = mapped_column(Integer, index=True)
    channel: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32))
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    target: Mapped[str | None] = mapped_column(String(512), nullable=True)
    last_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    broker_event: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
