from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from shared.models import EventEnvelope

from .dispatcher import WebhookDispatcher
from .models import DeliveryRecord, DeliveryStatus, EventRecord, SubscriptionRecord, utcnow
from .schemas import DeliveryOut, EventIn, EventOut, SubscriptionIn, SubscriptionOut


def event_payload(event: EventRecord) -> dict[str, object]:
    return {
        "schema_version": event.schema_version,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "source": event.source,
        "severity": event.severity,
        "occurred_at": event.occurred_at.isoformat(),
        "subject": event.subject,
        "payload": event.payload,
    }


def to_event_out(record: EventRecord) -> EventOut:
    return EventOut(
        id=record.id,
        event_id=record.event_id,
        schema_version=record.schema_version,
        event_type=record.event_type,
        source=record.source,
        severity=record.severity,
        occurred_at=record.occurred_at,
        subject=record.subject,
        payload=record.payload or {},
    )


def to_subscription_out(record: SubscriptionRecord) -> SubscriptionOut:
    return SubscriptionOut(
        id=record.id,
        name=record.name,
        target_url=record.target_url,
        event_type=record.event_type,
        source=record.source,
        severity=record.severity,
        active=record.active,
    )


def to_delivery_out(record: DeliveryRecord) -> DeliveryOut:
    return DeliveryOut(
        id=record.id,
        event_id=record.event.event_id,
        subscription_id=record.subscription_id,
        target_url=record.target_url,
        status=record.status,
        attempts=record.attempts,
        last_status_code=record.last_status_code,
        last_error=record.last_error,
        next_attempt_after=record.next_attempt_after,
    )


def matching_subscriptions(session: Session, envelope: EventEnvelope) -> list[SubscriptionRecord]:
    subscriptions = session.scalars(
        select(SubscriptionRecord).where(SubscriptionRecord.active.is_(True))
    ).all()
    return [
        subscription
        for subscription in subscriptions
        if (subscription.event_type is None or subscription.event_type == envelope.event_type)
        and (subscription.source is None or subscription.source == envelope.source)
        and (subscription.severity is None or subscription.severity == envelope.severity)
    ]


def create_subscription(session: Session, payload: SubscriptionIn) -> SubscriptionOut:
    record = SubscriptionRecord(
        name=payload.name,
        target_url=str(payload.target_url),
        event_type=payload.event_type,
        source=payload.source,
        severity=payload.severity,
        active=payload.active,
    )
    session.add(record)
    try:
        session.commit()
    except IntegrityError as err:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Subscription name already exists.",
        ) from err
    session.refresh(record)
    return to_subscription_out(record)


def list_subscriptions(session: Session) -> list[SubscriptionOut]:
    records = session.scalars(select(SubscriptionRecord).order_by(SubscriptionRecord.id)).all()
    return [to_subscription_out(record) for record in records]


def list_events(session: Session) -> list[EventOut]:
    records = session.scalars(select(EventRecord).order_by(EventRecord.id)).all()
    return [to_event_out(record) for record in records]


def get_event(session: Session, event_id: str) -> EventOut:
    record = session.scalar(select(EventRecord).where(EventRecord.event_id == event_id))
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found.")
    return to_event_out(record)


def list_deliveries(session: Session, event_id: str | None = None) -> list[DeliveryOut]:
    statement = select(DeliveryRecord).join(DeliveryRecord.event).order_by(DeliveryRecord.id)
    if event_id is not None:
        statement = statement.where(EventRecord.event_id == event_id)
    records = session.scalars(statement).all()
    return [to_delivery_out(record) for record in records]


def deliver(
    session: Session,
    delivery: DeliveryRecord,
    dispatcher: WebhookDispatcher,
    *,
    max_attempts: int,
    retry_backoff_seconds: int,
) -> DeliveryRecord:
    code, error = dispatcher.post(delivery.target_url, event_payload(delivery.event))
    delivery.attempts += 1
    delivery.last_status_code = code
    delivery.last_error = error
    delivery.updated_at = utcnow()
    if error is None:
        delivery.status = DeliveryStatus.delivered.value
        delivery.next_attempt_after = None
    elif delivery.attempts >= max_attempts:
        delivery.status = DeliveryStatus.terminal.value
        delivery.next_attempt_after = None
    else:
        delivery.status = DeliveryStatus.failed.value
        delivery.next_attempt_after = utcnow() + timedelta(seconds=retry_backoff_seconds)
    session.add(delivery)
    session.commit()
    session.refresh(delivery)
    return delivery


def ingest_event(
    session: Session,
    payload: EventIn,
    dispatcher: WebhookDispatcher,
    *,
    max_attempts: int,
    retry_backoff_seconds: int,
) -> EventOut:
    envelope = EventEnvelope(
        schema_version=payload.schema_version,
        event_id=payload.event_id or f"evt-{uuid4()}",
        event_type=payload.event_type,
        source=payload.source,
        severity=payload.severity,
        occurred_at=payload.occurred_at,
        subject=payload.subject,
        payload=payload.payload,
    )
    occurred_at = envelope.occurred_at
    if occurred_at.tzinfo is not None:
        occurred_at = occurred_at.astimezone(UTC).replace(tzinfo=None)

    record = EventRecord(
        event_id=envelope.event_id,
        schema_version=envelope.schema_version,
        event_type=envelope.event_type,
        source=envelope.source,
        severity=envelope.severity,
        occurred_at=occurred_at,
        subject=envelope.subject,
        payload=envelope.payload,
    )
    session.add(record)
    try:
        session.commit()
    except IntegrityError as err:
        session.rollback()
        existing = session.scalar(select(EventRecord).where(EventRecord.event_id == envelope.event_id))
        if existing is None:
            raise
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Event ID already exists.", "event": to_event_out(existing).model_dump(mode="json")},
        ) from err
    session.refresh(record)

    for subscription in matching_subscriptions(session, envelope):
        delivery_record = DeliveryRecord(
            event_id=record.id,
            subscription_id=subscription.id,
            target_url=subscription.target_url,
            status=DeliveryStatus.pending.value,
        )
        session.add(delivery_record)
        session.commit()
        session.refresh(delivery_record)
        deliver(
            session,
            delivery_record,
            dispatcher,
            max_attempts=max_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        )

    return to_event_out(record)


def retry_delivery(
    session: Session,
    delivery_id: int,
    dispatcher: WebhookDispatcher,
    *,
    max_attempts: int,
    retry_backoff_seconds: int,
) -> DeliveryOut:
    delivery = session.get(DeliveryRecord, delivery_id)
    if delivery is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Delivery not found.")
    if delivery.status == DeliveryStatus.delivered.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Delivered records do not need retry.",
        )
    return to_delivery_out(
        deliver(
            session,
            delivery,
            dispatcher,
            max_attempts=max_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        )
    )
