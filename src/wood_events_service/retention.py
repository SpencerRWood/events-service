"""Deterministic, dependency-aware cleanup with explicit caller-owned scheduling."""

from datetime import datetime, timedelta

from sqlalchemy import delete, exists, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from wood_events_service.config import Settings
from wood_events_service.storage import (
    BrokerDelivery,
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
                (exists().where(BrokerDelivery.event_id == EventRecord.id),),
            ),
            (
                NotificationRecord,
                settings.notification_retention_days,
                (
                    exists().where(NotifyDelivery.request_id == NotificationRecord.id),
                    exists().where(ResponseRecord.request_id == NotificationRecord.id),
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
