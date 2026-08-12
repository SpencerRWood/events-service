from __future__ import annotations

from datetime import timedelta

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .adapters import NotificationAdapter
from .models import NotificationDeliveryRecord, NotificationPolicyRecord, NotificationStatus, utcnow
from .schemas import (
    BrokerEventConsumeOut,
    BrokerEventIn,
    NotificationDeliveryOut,
    NotificationPolicyIn,
    NotificationPolicyOut,
)


def to_policy_out(record: NotificationPolicyRecord) -> NotificationPolicyOut:
    return NotificationPolicyOut(
        id=record.id,
        name=record.name,
        channel=record.channel,
        event_type=record.event_type,
        source=record.source,
        severity=record.severity,
        target=record.target,
        dedup_window_seconds=record.dedup_window_seconds,
        active=record.active,
    )


def to_delivery_out(record: NotificationDeliveryRecord) -> NotificationDeliveryOut:
    return NotificationDeliveryOut(
        id=record.id,
        event_id=record.event_id,
        dedup_key=record.dedup_key,
        policy_id=record.policy_id,
        channel=record.channel,
        status=record.status,
        attempts=record.attempts,
        target=record.target,
        last_status_code=record.last_status_code,
        last_error=record.last_error,
        next_attempt_after=record.next_attempt_after,
    )


def create_policy(session: Session, payload: NotificationPolicyIn) -> NotificationPolicyOut:
    record = NotificationPolicyRecord(
        name=payload.name,
        channel=payload.channel,
        event_type=payload.event_type,
        source=payload.source,
        severity=payload.severity,
        target=payload.target,
        dedup_window_seconds=payload.dedup_window_seconds,
        active=payload.active,
    )
    session.add(record)
    try:
        session.commit()
    except IntegrityError as err:
        session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Policy name already exists.") from err
    session.refresh(record)
    return to_policy_out(record)


def list_policies(session: Session) -> list[NotificationPolicyOut]:
    records = session.scalars(select(NotificationPolicyRecord).order_by(NotificationPolicyRecord.id)).all()
    return [to_policy_out(record) for record in records]


def list_deliveries(session: Session, event_id: str | None = None) -> list[NotificationDeliveryOut]:
    statement = select(NotificationDeliveryRecord).order_by(NotificationDeliveryRecord.id)
    if event_id is not None:
        statement = statement.where(NotificationDeliveryRecord.event_id == event_id)
    records = session.scalars(statement).all()
    return [to_delivery_out(record) for record in records]


def matching_policies(session: Session, event: BrokerEventIn) -> list[NotificationPolicyRecord]:
    policies = session.scalars(select(NotificationPolicyRecord).where(NotificationPolicyRecord.active.is_(True))).all()
    return [
        policy
        for policy in policies
        if (policy.event_type is None or policy.event_type == event.event_type)
        and (policy.source is None or policy.source == event.source)
        and (policy.severity is None or policy.severity == event.severity)
    ]


def consume_broker_event(
    session: Session,
    event: BrokerEventIn,
    adapters: dict[str, NotificationAdapter],
    *,
    max_attempts: int,
    retry_backoff_seconds: int,
) -> BrokerEventConsumeOut:
    deliveries: list[NotificationDeliveryOut] = []
    policies = matching_policies(session, event)
    dedup_key = str(event.payload.get("dedup_key") or event.event_id)
    for policy in policies:
        existing = session.scalar(
            select(NotificationDeliveryRecord).where(
                NotificationDeliveryRecord.dedup_key == dedup_key,
                NotificationDeliveryRecord.policy_id == policy.id,
                NotificationDeliveryRecord.channel == policy.channel,
            )
        )
        if existing is not None:
            deliveries.append(to_delivery_out(existing))
            continue

        delivery = NotificationDeliveryRecord(
            event_id=event.event_id,
            dedup_key=dedup_key,
            policy_id=policy.id,
            channel=policy.channel,
            status=NotificationStatus.failed.value,
            target=policy.target,
            broker_event=event.model_dump(mode="json"),
        )
        session.add(delivery)
        session.commit()
        session.refresh(delivery)
        delivery = _deliver(
            session,
            delivery,
            event,
            adapters,
            max_attempts=max_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        deliveries.append(to_delivery_out(delivery))
    return BrokerEventConsumeOut(
        event_id=event.event_id,
        matched_policy_count=len(policies),
        deliveries=deliveries,
    )


def retry_delivery(
    session: Session,
    delivery_id: int,
    adapters: dict[str, NotificationAdapter],
    *,
    max_attempts: int,
    retry_backoff_seconds: int,
) -> NotificationDeliveryOut:
    delivery = session.get(NotificationDeliveryRecord, delivery_id)
    if delivery is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Delivery not found.")
    if delivery.status == NotificationStatus.delivered.value:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Delivered records do not need retry.")
    event = BrokerEventIn.model_validate(delivery.broker_event)
    return to_delivery_out(
        _deliver(
            session,
            delivery,
            event,
            adapters,
            max_attempts=max_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        )
    )


def _deliver(
    session: Session,
    delivery: NotificationDeliveryRecord,
    event: BrokerEventIn,
    adapters: dict[str, NotificationAdapter],
    *,
    max_attempts: int,
    retry_backoff_seconds: int,
) -> NotificationDeliveryRecord:
    adapter = adapters.get(delivery.channel)
    if adapter is None:
        result_code = None
        result_error = f"Notification adapter is not configured for channel {delivery.channel}."
    else:
        result = adapter.deliver(event, delivery.target)
        result_code = result.status_code
        result_error = result.error

    delivery.attempts += 1
    delivery.last_status_code = result_code
    delivery.last_error = result_error
    delivery.updated_at = utcnow()
    if result_error is None:
        delivery.status = NotificationStatus.delivered.value
        delivery.next_attempt_after = None
    elif delivery.attempts >= max_attempts:
        delivery.status = NotificationStatus.terminal.value
        delivery.next_attempt_after = None
    else:
        delivery.status = NotificationStatus.failed.value
        delivery.next_attempt_after = utcnow() + timedelta(seconds=retry_backoff_seconds)
    session.add(delivery)
    session.commit()
    session.refresh(delivery)
    return delivery
