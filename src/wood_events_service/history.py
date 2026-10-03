"""Bounded broker queries, always restricted to the authenticated source."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from wood_events_service.storage import BrokerDelivery, BrokerJob, EventRecord


class BrokerHistory:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def events(
        self, source: str, correlation_id: UUID | None, limit: int, offset: int
    ) -> list[dict[str, object]]:
        statement = select(EventRecord).where(EventRecord.source == source)
        if correlation_id is not None:
            statement = statement.where(EventRecord.correlation_id == correlation_id)
        with Session(self.engine) as session:
            return [
                {"event": row.payload, "accepted_at": row.accepted_at}
                for row in session.scalars(
                    statement.order_by(EventRecord.accepted_at, EventRecord.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    def event(self, event_id: UUID, source: str) -> dict[str, object] | None:
        with Session(self.engine) as session:
            record = session.scalar(
                select(EventRecord).where(
                    EventRecord.id == event_id, EventRecord.source == source
                )
            )
            if record is None:
                return None
            jobs = session.scalars(
                select(BrokerJob)
                .where(BrokerJob.event_id == event_id)
                .order_by(BrokerJob.consumer)
            )
            return {
                "event": record.payload,
                "accepted_at": record.accepted_at,
                "deliveries": [
                    {
                        "consumer": job.consumer,
                        "status": job.status,
                        "attempts": job.attempts,
                        "generation": job.generation,
                        "next_attempt_at": job.next_attempt_at,
                    }
                    for job in jobs
                ],
            }

    def attempts(
        self, event_id: UUID, source: str, limit: int, offset: int
    ) -> list[dict[str, object]]:
        with Session(self.engine) as session:
            return [
                {
                    "attempt_id": row.id,
                    "event_id": row.event_id,
                    "correlation_id": row.correlation_id,
                    "consumer": row.destination,
                    "outcome": row.outcome,
                    "error_code": row.error_code,
                    "attempted_at": row.attempted_at,
                }
                for row in session.scalars(
                    select(BrokerDelivery)
                    .join(EventRecord, BrokerDelivery.event_id == EventRecord.id)
                    .where(EventRecord.source == source, EventRecord.id == event_id)
                    .order_by(BrokerDelivery.attempted_at, BrokerDelivery.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]
