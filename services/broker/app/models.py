from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class DeliveryStatus(StrEnum):
    pending = "pending"
    delivered = "delivered"
    failed = "failed"
    terminal = "terminal"


class EventRecord(Base):
    __tablename__ = "broker_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    schema_version: Mapped[str] = mapped_column(String(16))
    event_type: Mapped[str] = mapped_column(String(255), index=True)
    source: Mapped[str] = mapped_column(String(255), index=True)
    severity: Mapped[str] = mapped_column(String(32), index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    subject: Mapped[str | None] = mapped_column(String(512), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    deliveries: Mapped[list[DeliveryRecord]] = relationship(
        back_populates="event",
        cascade="all, delete-orphan",
    )


class SubscriptionRecord(Base):
    __tablename__ = "broker_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    target_url: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    severity: Mapped[str | None] = mapped_column(String(32), nullable=True)
    active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    deliveries: Mapped[list[DeliveryRecord]] = relationship(back_populates="subscription")


class DeliveryRecord(Base):
    __tablename__ = "broker_deliveries"
    __table_args__ = (
        UniqueConstraint("event_id", "subscription_id", name="uq_delivery_event_subscription"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("broker_events.id"), index=True)
    subscription_id: Mapped[int] = mapped_column(ForeignKey("broker_subscriptions.id"), index=True)
    target_url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default=DeliveryStatus.pending.value)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    event: Mapped[EventRecord] = relationship(back_populates="deliveries")
    subscription: Mapped[SubscriptionRecord] = relationship(back_populates="deliveries")
