"""Deterministic, dependency-aware cleanup with explicit caller-owned scheduling."""

from datetime import datetime, timedelta

from sqlalchemy import delete, exists, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from wood_events_service.config import Settings
from wood_events_service.notification_models import (
    NotificationJob,
    NotificationMessage,
    NotificationState,
    SuppressionWindow,
)
from wood_events_service.storage import (
    BrokerDelivery,
    BrokerJob,
    EventRecord,
    NotificationRecord,
    NotifyDelivery,
    ResponseRecord,
)


def cleanup(engine: Engine, settings: Settings, *, as_of: datetime) -> dict[str, int]:
    if as_of.tzinfo is None:
        raise ValueError("Retention cutoff must be timezone aware")
    removed: dict[str, int] = {}
    with Session(engine) as session, session.begin():
        session.execute(text("SET LOCAL wes.retention = 'on'"))
        session.execute(
            delete(SuppressionWindow).where(SuppressionWindow.until < as_of)
        )
        removed[NotificationMessage.__tablename__] = len(
            session.scalars(
                delete(NotificationMessage)
                .where(
                    NotificationMessage.delivered_at
                    < as_of - timedelta(days=settings.response_retention_days),
                    ~exists().where(
                        NotificationState.id == NotificationMessage.request_id,
                        NotificationState.response_state == "awaiting-response",
                    ),
                )
                .returning(NotificationMessage.id)
            ).all()
        )
        removed[NotificationJob.__tablename__] = len(
            session.scalars(
                delete(NotificationJob)
                .where(
                    NotificationJob.status.not_in(["pending", "transient-failure"]),
                    NotificationJob.updated_at
                    < as_of - timedelta(days=settings.delivery_retention_days),
                    ~exists().where(
                        NotificationState.id == NotificationJob.request_id,
                        NotificationState.response_state == "awaiting-response",
                    ),
                )
                .returning(NotificationJob.id)
            ).all()
        )
        removed[NotificationState.__tablename__] = len(
            session.scalars(
                delete(NotificationState)
                .where(
                    NotificationState.response_state != "awaiting-response",
                    NotificationState.updated_at
                    < as_of - timedelta(days=settings.notification_retention_days),
                    ~exists().where(NotificationJob.request_id == NotificationState.id),
                    ~exists().where(
                        NotificationMessage.request_id == NotificationState.id
                    ),
                )
                .returning(NotificationState.id)
            ).all()
        )
        # Pending jobs pin their parent; finished scheduling state expires only
        # after the delivery retention window, independently of attempt history.
        removed[BrokerJob.__tablename__] = len(
            session.scalars(
                delete(BrokerJob)
                .where(
                    BrokerJob.status != "pending",
                    BrokerJob.updated_at
                    < as_of - timedelta(days=settings.delivery_retention_days),
                )
                .returning(BrokerJob.id)
            ).all()
        )
        for model, column, days in (
            (
                BrokerDelivery,
                BrokerDelivery.attempted_at,
                settings.delivery_retention_days,
            ),
            (
                NotifyDelivery,
                NotifyDelivery.attempted_at,
                settings.delivery_retention_days,
            ),
            (
                ResponseRecord,
                ResponseRecord.received_at,
                settings.response_retention_days,
            ),
        ):
            deleted = session.scalars(
                delete(model)
                .where(column < as_of - timedelta(days=days))
                .returning(model.id)
            ).all()
            removed[model.__tablename__] = len(deleted)
        for parent_model, days, children in (
            (
                EventRecord,
                settings.event_retention_days,
                (
                    exists().where(BrokerDelivery.event_id == EventRecord.id),
                    exists().where(BrokerJob.event_id == EventRecord.id),
                ),
            ),
            (
                NotificationRecord,
                settings.notification_retention_days,
                (
                    exists().where(NotifyDelivery.request_id == NotificationRecord.id),
                    exists().where(ResponseRecord.request_id == NotificationRecord.id),
                    exists().where(NotificationState.id == NotificationRecord.id),
                    exists().where(NotificationJob.request_id == NotificationRecord.id),
                    exists().where(
                        NotificationMessage.request_id == NotificationRecord.id
                    ),
                ),
            ),
        ):
            deleted = session.scalars(
                delete(parent_model)
                .where(
                    parent_model.accepted_at < as_of - timedelta(days=days),
                    *(~child for child in children),
                )
                .returning(parent_model.id)
            ).all()
            removed[parent_model.__tablename__] = len(deleted)
    return removed
