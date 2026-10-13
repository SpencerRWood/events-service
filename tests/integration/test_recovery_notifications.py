"""Consolidated recovery scorecards and at-most-once ambiguous Telegram dispatch."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from tests.integration.test_notification_lifecycle import FakeProvider

from events_service.config import Settings
from events_service.contracts import EventEnvelope, NotificationRequest, Severity
from events_service.notification_lifecycle import NotificationLifecycle
from events_service.notification_models import (
    NotificationJob,
    NotificationMessage,
    RecoveryNotificationPart,
)
from events_service.notification_policy import NotificationPolicy, NotificationRouter
from events_service.providers import ProviderResult
from events_service.security import SecretPolicy
from events_service.storage import NotificationRecord, NotifyDelivery, Store


def completed(status: str = "SUCCESS") -> EventEnvelope:
    return EventEnvelope(
        event_type="recovery.run-completed",
        source="recovery-verification",
        severity=Severity.WARNING,
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        data={
            "title": "Backup & Recovery Scorecard",
            "execution_status": status,
            "message": "Dagster: readiness — "
            + status
            + "\nHomelab backup: UNAVAILABLE; recovery UNKNOWN"
            + "\nInfrastructure backup: FAILED; recovery NOT_READY"
            + "\nEvidence: https://dagster.example.test/runs/example",
        },
    )


def runtime(
    engine: Engine, settings: Settings, provider: FakeProvider
) -> tuple[NotificationLifecycle, Store]:
    active = settings.model_copy(
        update={
            "notification_policies": [
                NotificationPolicy(
                    name="recovery-scorecards",
                    channels=["telegram"],
                    sources={"recovery-verification"},
                    event_types={"recovery.run-completed"},
                )
            ],
            "notification_backoff_seconds": 1,
            "notification_max_attempts": 2,
            "telegram_chat_id": -123,
        }
    )
    worker = NotificationLifecycle(engine, active, {"telegram": provider})
    return worker, Store(
        engine, SecretPolicy(active.secret_values()), notification_hook=worker.schedule
    )


@pytest.mark.parametrize("status", ["SUCCESS", "FAILURE", "CANCELED"])
def test_terminal_scorecard_delivered_once_despite_duplicate_event(
    broker_engine: Engine,
    settings: Settings,
    status: str,
) -> None:
    provider = FakeProvider()
    worker, store = runtime(broker_engine, settings, provider)
    request = worker.router.from_event(completed(status))
    assert request.title == "Backup & Recovery Scorecard"
    assert "Homelab backup" in request.message
    assert "Infrastructure backup" in request.message
    assert not store.accept(request).duplicate
    assert store.accept(request).duplicate
    assert worker.deliver_one()
    assert not worker.deliver_one()
    assert len(provider.calls) == 1
    with Session(broker_engine) as session:
        assert session.scalar(select(func.count()).select_from(NotificationRecord)) == 1
        assert (
            session.scalar(select(func.count()).select_from(NotificationMessage)) == 1
        )


def test_uncertain_acceptance_is_not_automatically_or_manually_resent(
    broker_engine: Engine,
    settings: Settings,
) -> None:
    provider = FakeProvider([ProviderResult("transient-failure", "timeout")])
    worker, store = runtime(broker_engine, settings, provider)
    request = worker.router.from_event(completed())
    store.accept(request)
    assert worker.deliver_one()
    assert not worker.deliver_one(as_of=datetime.now(UTC) + timedelta(hours=1))
    with pytest.raises(ValueError, match="Reconcile uncertain"):
        worker.retry(request.request_id, "telegram", request.source)
    assert len(provider.calls) == 1
    with Session(broker_engine) as session:
        row = session.scalar(select(NotifyDelivery))
        assert row is not None
        assert row.error_code == "delivery-uncertain"


def test_crash_after_dispatch_intent_cannot_replay_a_second_send(
    broker_engine: Engine,
    settings: Settings,
) -> None:
    class CrashedProvider(FakeProvider):
        def send(self, request: NotificationRequest) -> ProviderResult:
            self.calls.append(request)
            raise RuntimeError("simulated process death after remote side effect")

    provider = CrashedProvider()
    worker, store = runtime(broker_engine, settings, provider)
    store.accept(worker.router.from_event(completed()))
    with pytest.raises(RuntimeError, match="process death"):
        worker.deliver_one()
    restarted, _ = runtime(broker_engine, settings, provider)
    assert not restarted.deliver_one()
    assert len(provider.calls) == 1
    with Session(broker_engine) as session:
        row = session.scalar(select(NotificationJob))
        assert row is not None
        assert row.status == "dispatching"


def test_known_rate_limit_rejection_can_retry_without_ambiguous_resend(
    broker_engine: Engine,
    settings: Settings,
) -> None:
    provider = FakeProvider(
        [
            ProviderResult("transient-failure", "http-429"),
            ProviderResult("delivered", message_id=42),
        ]
    )
    worker, store = runtime(broker_engine, settings, provider)
    store.accept(worker.router.from_event(completed()))
    now = datetime.now(UTC)
    assert worker.deliver_one(as_of=now)
    assert worker.deliver_one(as_of=now + timedelta(seconds=2))
    assert len(provider.calls) == 2
    with Session(broker_engine) as session:
        assert (
            session.scalar(select(func.count()).select_from(NotificationMessage)) == 1
        )


@pytest.mark.parametrize(
    "data",
    [
        {},
        {
            "title": "Backup & Recovery Scorecard",
            "execution_status": "STARTED",
            "message": "invalid",
        },
    ],
)
def test_malformed_scorecard_is_rejected(data: dict[str, str]) -> None:
    envelope = completed().model_copy(update={"data": data})
    with pytest.raises(ValueError, match="Invalid recovery"):
        NotificationRouter([]).from_event(envelope)


def multipart_event() -> EventEnvelope:
    original = completed()
    return original.model_copy(
        update={
            "data": {
                **original.data,
                "synthetic": True,
                "evidence_link": "https://dagster.example.test/runs/current",
                "report_blocks": [
                    {
                        "heading": "Recovery dependencies",
                        "text": f"Finding {i}: " + "action " * 50,
                    }
                    for i in range(40)
                ],
            }
        }
    )


def test_partial_delivery_restarts_and_retries_only_failed_part(
    broker_engine: Engine,
    settings: Settings,
) -> None:
    provider = FakeProvider(
        [
            ProviderResult("delivered", message_id=41),
            ProviderResult("transient-failure", "http-429"),
        ]
    )
    worker, store = runtime(broker_engine, settings, provider)
    event = multipart_event()
    request = worker.router.from_event(event)
    store.accept(request)
    now = datetime.now(UTC)
    assert worker.deliver_one(as_of=now)
    assert worker.deliver_one(as_of=now)
    restarted, _ = runtime(broker_engine, settings, provider)
    assert store.accept(restarted.router.from_event(event)).duplicate
    assert restarted.deliver_one(as_of=now + timedelta(seconds=2))
    while restarted.deliver_one(as_of=now + timedelta(seconds=3)):
        pass
    assert (
        provider.calls[1].context["recovery_part"]
        == provider.calls[2].context["recovery_part"]
    )
    assert (
        provider.calls[0].context["recovery_part"]
        != provider.calls[1].context["recovery_part"]
    )
    with Session(broker_engine) as session:
        parts = session.scalars(
            select(RecoveryNotificationPart).order_by(RecoveryNotificationPart.position)
        ).all()
        assert len(parts) > 1
        assert all(p.status == "delivered" for p in parts)
        assert [p.attempts for p in parts[:2]] == [1, 2]
        assert session.scalar(select(NotificationJob.status)) == "delivered"
        assert len(provider.calls) == len(parts) + 1


@pytest.mark.parametrize(
    "uncertain",
    [
        ProviderResult("transient-failure", "timeout"),
        ProviderResult("terminal-failure", "invalid-response"),
        ProviderResult("terminal-failure", "oversize-response"),
    ],
)
def test_uncertain_middle_part_remains_visible_and_blocks_later_parts(
    broker_engine: Engine,
    settings: Settings,
    uncertain: ProviderResult,
) -> None:
    provider = FakeProvider(
        [
            ProviderResult("delivered", message_id=41),
            uncertain,
        ]
    )
    worker, store = runtime(broker_engine, settings, provider)
    request = worker.router.from_event(multipart_event())
    store.accept(request)
    assert worker.deliver_one()
    assert worker.deliver_one()
    assert not worker.deliver_one()
    with pytest.raises(ValueError, match="Reconcile uncertain"):
        worker.retry(request.request_id, "telegram", request.source)
    with Session(broker_engine) as session:
        parts = session.scalars(
            select(RecoveryNotificationPart).order_by(RecoveryNotificationPart.position)
        ).all()
        assert parts[0].status == "delivered"
        assert parts[1].error_code == "delivery-uncertain"
        assert all(p.status == "pending" for p in parts[2:])
    assert len(provider.calls) == 2


def test_multipart_crash_and_restart_preserve_confirmed_and_uncertain_parts(
    broker_engine: Engine,
    settings: Settings,
) -> None:
    class CrashedProvider(FakeProvider):
        def send(self, request: NotificationRequest) -> ProviderResult:
            if self.calls:
                self.calls.append(request)
                raise RuntimeError("process death after second remote side effect")
            return super().send(request)

    provider = CrashedProvider()
    worker, store = runtime(broker_engine, settings, provider)
    request = worker.router.from_event(multipart_event())
    store.accept(request)
    assert worker.deliver_one()
    with pytest.raises(RuntimeError, match="process death"):
        worker.deliver_one()
    restarted, replay_store = runtime(broker_engine, settings, provider)
    assert replay_store.accept(request).duplicate
    assert not restarted.deliver_one()
    with pytest.raises(ValueError, match="Reconcile uncertain"):
        restarted.retry(request.request_id, "telegram", request.source)
    with Session(broker_engine) as session:
        parts = session.scalars(
            select(RecoveryNotificationPart).order_by(RecoveryNotificationPart.position)
        ).all()
        assert parts[0].status == "delivered"
        assert parts[1].status == "dispatching"
        assert all(p.status == "pending" for p in parts[2:])
    assert len(provider.calls) == 2
