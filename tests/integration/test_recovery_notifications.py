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
from events_service.notification_models import NotificationJob, NotificationMessage
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
