"""Authenticated, message-bound Telegram callbacks normalize responses only."""

from datetime import UTC, datetime
from uuid import uuid4

from fastapi import HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from events_service.contracts import Name, NormalizedResponse, NotificationRequest
from events_service.notification_lifecycle import NotificationLifecycle
from events_service.notification_models import (
    NotificationMessage,
    NotificationState,
)
from events_service.providers import parse_callback
from events_service.storage import NotificationRecord, ResponseRecord


class TelegramUser(BaseModel):
    id: int


class TelegramMessage(BaseModel):
    message_id: int = Field(ge=1)
    chat: TelegramUser


class TelegramQuery(BaseModel):
    id: Name
    sender: TelegramUser = Field(validation_alias="from")
    message: TelegramMessage
    data: str = Field(min_length=1, max_length=64)


class TelegramUpdate(BaseModel):
    update_id: int = Field(ge=0)
    callback_query: TelegramQuery


def capture(
    lifecycle: NotificationLifecycle, update: TelegramUpdate
) -> dict[str, object]:
    configured = lifecycle.settings
    query = update.callback_query
    secret = configured.telegram_webhook_secret
    if secret is None:
        raise HTTPException(503, "Telegram is unconfigured")
    if query.message.chat.id != configured.telegram_chat_id or (
        configured.telegram_allowed_user_ids
        and query.sender.id not in configured.telegram_allowed_user_ids
    ):
        raise HTTPException(403, "Callback responder is not allowed")
    try:
        request_id, index = parse_callback(query.data, secret.get_secret_value())
    except ValueError:
        raise HTTPException(422, "Invalid callback identity") from None
    now = datetime.now(UTC)
    lifecycle.expire(as_of=now, request_id=request_id)
    with Session(lifecycle.engine) as session, session.begin():
        state = session.scalar(
            select(NotificationState)
            .where(NotificationState.id == request_id)
            .with_for_update()
        )
        parent = session.get(NotificationRecord, request_id)
        message = session.scalar(
            select(NotificationMessage).where(
                NotificationMessage.request_id == request_id,
                NotificationMessage.chat_id == query.message.chat.id,
                NotificationMessage.message_id == query.message.message_id,
            )
        )
        if state is None or parent is None or message is None:
            raise HTTPException(404, "Notification message not found")
        request = NotificationRequest.model_validate(parent.payload)
        if index >= len(request.response_actions):
            raise HTTPException(422, "Undeclared response action")
        action = request.response_actions[index].action
        previous = session.scalar(
            select(ResponseRecord).where(
                ResponseRecord.provider == "telegram",
                ResponseRecord.provider_response_id == query.id,
            )
        )
        if previous is not None:
            if (
                previous.request_id != request_id
                or previous.payload["selected_action"] != action
                or previous.payload["responder_reference"] != str(query.sender.id)
                or previous.payload["data"] != {"message_id": query.message.message_id}
            ):
                raise HTTPException(409, "Callback identity conflict")
            return {"response": previous.payload, "duplicate": True}
        if state.response_state == "expired" or (
            request.response_deadline is not None and request.response_deadline <= now
        ):
            raise HTTPException(410, "Response deadline expired")
        if state.response_state != "awaiting-response":
            raise HTTPException(409, "Notification is not awaiting response")
        response = NormalizedResponse(
            response_id=uuid4(),
            request_id=request_id,
            correlation_id=request.correlation_id,
            causation_id=request.causation_id,
            selected_action=action,
            responder_reference=str(query.sender.id),
            received_at=now,
            provider="telegram",
            provider_response_id=query.id,
            data={"message_id": query.message.message_id},
        )
        payload = response.model_dump(mode="json")
        session.add(
            ResponseRecord(
                id=response.response_id,
                request_id=request_id,
                correlation_id=response.correlation_id,
                causation_id=response.causation_id,
                provider="telegram",
                provider_response_id=query.id,
                payload=payload,
                received_at=now,
            )
        )
        state.response_state = "responded"
        state.updated_at = now
        return {"response": payload, "duplicate": False}
