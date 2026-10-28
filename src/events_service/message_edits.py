"""Durable terminal edits; callers can identify requests, never chat/message targets."""

from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from events_service.config import Settings
from events_service.contracts import NotificationEdit, NotificationRequest
from events_service.notification_models import (
    MessageEdit,
    NotificationMessage,
    NotificationState,
)
from events_service.providers import Provider, ProviderResult, TelegramProvider
from events_service.security import SecretPolicy
from events_service.storage import (
    IdempotencyConflictError,
    NotificationRecord,
    NotifyDelivery,
)


def describe(row: MessageEdit) -> dict[str, object]:
    return {
        "edit": row.payload,
        "status": row.status,
        "attempts": row.attempts,
        "error_code": row.error_code,
        "updated_at": row.updated_at,
    }


class MessageEdits:
    def __init__(
        self, engine: Engine, settings: Settings, providers: dict[str, Provider]
    ) -> None:
        self.engine = engine
        self.settings = settings
        self.providers = providers

    def queue(
        self,
        session: Session,
        parent: NotificationRecord,
        edit: NotificationEdit,
        now: datetime,
    ) -> MessageEdit:
        # Caller holds the request's response-state lock, also used by callbacks.
        SecretPolicy(self.settings.secret_values()).ensure_safe(
            edit.model_dump(mode="json")
        )
        text = f"{edit.outcome.upper()}\n\n{edit.message}"
        if len(text.encode("utf-16-le")) // 2 > 4096:
            raise ValueError("Terminal message exceeds Telegram's limit")
        existing = session.scalar(
            select(MessageEdit).where(MessageEdit.request_id == parent.id)
        )
        by_id = session.get(MessageEdit, edit.edit_id)
        if existing is not None or by_id is not None:
            if (
                existing is None
                or existing.id != edit.edit_id
                or existing.payload != edit.model_dump(mode="json")
            ):
                raise IdempotencyConflictError
            return existing
        messages = session.scalars(
            select(NotificationMessage).where(
                NotificationMessage.request_id == parent.id
            )
        ).all()
        if len(messages) != 1:
            raise ValueError(
                "Exactly one confirmed original Telegram message is required"
            )
        row = MessageEdit(
            id=edit.edit_id,
            request_id=parent.id,
            message_ref=messages[0].id,
            payload=edit.model_dump(mode="json"),
            status="pending",
            attempts=0,
            next_attempt_at=now,
            updated_at=now,
        )
        session.add(row)
        state = session.get(NotificationState, parent.id)
        if state is not None and state.response_state in {"awaiting-response", "none"}:
            state.response_state = "closed"
            state.updated_at = now
        session.flush()
        return row

    def submit(
        self, request_id: UUID, source: str, edit: NotificationEdit
    ) -> dict[str, object] | None:
        with Session(self.engine) as session, session.begin():
            state = session.scalar(
                select(NotificationState)
                .join(NotificationRecord, NotificationRecord.id == NotificationState.id)
                .where(
                    NotificationRecord.id == request_id,
                    NotificationRecord.source == source,
                )
                .with_for_update(of=NotificationState)
            )
            if state is None:
                return None
            parent = session.get(NotificationRecord, request_id)
            if parent is None:
                raise RuntimeError("Notification parent missing")
            return describe(self.queue(session, parent, edit, datetime.now(UTC)))

    def expired(
        self, session: Session, parent: NotificationRecord, now: datetime
    ) -> None:
        request = NotificationRequest.model_validate(parent.payload)
        if request.responder_policy is None:
            return
        message = session.scalar(
            select(NotificationMessage.id)
            .where(NotificationMessage.request_id == parent.id)
            .limit(1)
        )
        existing = session.scalar(
            select(MessageEdit.id).where(MessageEdit.request_id == parent.id)
        )
        if message is not None and existing is None:
            self.queue(
                session,
                parent,
                NotificationEdit(
                    edit_id=uuid5(NAMESPACE_URL, f"wes-expired:{parent.id}"),
                    outcome="expired",
                    message="This request has expired.",
                ),
                now,
            )

    def deliver_one(self, now: datetime) -> bool:
        with Session(self.engine) as session, session.begin():
            row = session.scalar(
                select(MessageEdit)
                .where(
                    MessageEdit.status.in_(["pending", "uncertain", "dispatching"]),
                    MessageEdit.attempts < self.settings.notification_max_attempts,
                    MessageEdit.next_attempt_at <= now,
                )
                .order_by(MessageEdit.next_attempt_at, MessageEdit.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if row is None:
                return False
            message = session.get(NotificationMessage, row.message_ref)
            if message is None:
                raise RuntimeError("Stored Telegram message missing")
            edit = NotificationEdit.model_validate(row.payload)
            identity, attempt = row.id, row.attempts + 1
            chat_id, message_id = message.chat_id, message.message_id
            row.status, row.attempts, row.updated_at = "dispatching", attempt, now
            # A dead worker leaves a durable lease. Replaying this exact terminal
            # text is safe even if the earlier HTTP response was lost.
            row.next_attempt_at = now + timedelta(
                seconds=self.settings.provider_timeout_seconds * 2
            )
        provider = self.providers.get("telegram")
        result = (
            provider.edit(
                chat_id, message_id, f"{edit.outcome.upper()}\n\n{edit.message}"
            )
            if isinstance(provider, TelegramProvider)
            else ProviderResult("terminal-failure", "provider-unconfigured")
        )
        with Session(self.engine) as session, session.begin():
            row = session.get(MessageEdit, identity, with_for_update=True)
            if row is None or row.attempts != attempt or row.status != "dispatching":
                return True
            row.status = result.outcome
            if result.outcome == "transient-failure" or result.error_code in {
                "invalid-response",
                "oversize-response",
            }:
                row.status = "uncertain"
            row.error_code, row.updated_at = result.error_code, now
            row.next_attempt_at = now + timedelta(
                seconds=min(
                    self.settings.notification_max_backoff_seconds,
                    self.settings.notification_backoff_seconds * 2 ** (attempt - 1),
                )
            )
            parent = session.get(NotificationRecord, row.request_id)
            if parent is None:
                raise RuntimeError("Notification parent missing")
            session.add(
                NotifyDelivery(
                    id=uuid4(),
                    request_id=row.request_id,
                    correlation_id=parent.correlation_id,
                    destination="telegram-edit",
                    outcome=result.outcome,
                    error_code=result.error_code,
                    attempted_at=now,
                )
            )
        return True
