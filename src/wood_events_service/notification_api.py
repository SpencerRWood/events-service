"""Scoped direct/broker notification history, retry and Telegram webhook routes."""

import hmac
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from wood_events_service.config import Scope
from wood_events_service.contracts import EventEnvelope, Receipt
from wood_events_service.notification_lifecycle import NotificationLifecycle
from wood_events_service.notification_models import NotificationJob, NotificationState
from wood_events_service.providers import TelegramProvider
from wood_events_service.security import ServiceAuth
from wood_events_service.storage import (
    IdempotencyConflictError,
    NotificationRecord,
    NotifyDelivery,
    ResponseRecord,
    Store,
)
from wood_events_service.telegram_callbacks import TelegramUpdate, capture

Limit = Annotated[int, Query(ge=1, le=100)]
Offset = Annotated[int, Query(ge=0, le=100000)]
bearer = HTTPBearer(auto_error=False)


def register_notification_routes(app: FastAPI) -> None:  # noqa: PLR0915 -- service-scoped endpoints
    def auth_scope(
        scope: Scope, request: Request, credential: HTTPAuthorizationCredentials | None
    ) -> str:
        auth: ServiceAuth = request.app.state.auth
        return auth.authenticate(credential.credentials if credential else "", scope)

    def reader(
        request: Request,
        credential: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        return auth_scope("notifications:read", request, credential)

    def replayer(
        request: Request,
        credential: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        return auth_scope("notifications:retry", request, credential)

    def relay(
        request: Request,
        credential: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        return auth_scope("notifications:consume", request, credential)

    def telegram_auth(
        request: Request,
        value: Annotated[
            str | None, Header(alias="X-Telegram-Bot-Api-Secret-Token")
        ] = None,
    ) -> None:
        lifecycle: NotificationLifecycle = request.app.state.notifications
        secret = lifecycle.settings.telegram_webhook_secret
        if secret is None:
            raise HTTPException(503, "Telegram is unconfigured")
        if not hmac.compare_digest(
            (value or "").encode(), secret.get_secret_value().encode()
        ):
            raise HTTPException(401, "Telegram webhook authentication required")

    @app.post("/v1/broker-events", status_code=201)
    def consume_event(
        request: Request,
        envelope: EventEnvelope,
        _source: Annotated[str, Depends(relay)],
    ) -> Receipt:
        lifecycle: NotificationLifecycle = request.app.state.notifications
        store: Store = request.app.state.store
        try:
            return store.accept(lifecycle.router.from_event(envelope))
        except ValueError:
            raise HTTPException(
                422, "Invalid or credential-bearing event content"
            ) from None
        except IdempotencyConflictError:
            raise HTTPException(409, "Identity reused with different content") from None

    def detail(request: Request, request_id: UUID, source: str) -> dict[str, object]:
        lifecycle: NotificationLifecycle = request.app.state.notifications
        with Session(lifecycle.engine) as session:
            parent = session.scalar(
                select(NotificationRecord).where(
                    NotificationRecord.id == request_id,
                    NotificationRecord.source == source,
                )
            )
            if parent is None:
                raise HTTPException(404, "Notification not found")
            lifecycle.expire(as_of=datetime.now(UTC), request_id=request_id)
            state = session.get(NotificationState, request_id)
            jobs = session.scalars(
                select(NotificationJob)
                .where(NotificationJob.request_id == request_id)
                .order_by(NotificationJob.channel)
            )
            return {
                "request": parent.payload,
                "accepted_at": parent.accepted_at,
                "response_state": state.response_state if state else "none",
                "response_deadline": state.response_deadline if state else None,
                "channels": [
                    {
                        "channel": job.channel,
                        "status": job.status,
                        "attempts": job.attempts,
                        "generation": job.generation,
                        "next_attempt_at": job.next_attempt_at,
                    }
                    for job in jobs
                ],
            }

    @app.get("/v1/notifications")
    def notifications(
        request: Request,
        source: Annotated[str, Depends(reader)],
        correlation_id: UUID | None = None,
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> list[dict[str, object]]:
        lifecycle: NotificationLifecycle = request.app.state.notifications
        statement = select(NotificationRecord.id).where(
            NotificationRecord.source == source
        )
        if correlation_id is not None:
            statement = statement.where(
                NotificationRecord.correlation_id == correlation_id
            )
        with Session(lifecycle.engine) as session:
            identities = session.scalars(
                statement.order_by(
                    NotificationRecord.accepted_at, NotificationRecord.id
                )
                .limit(limit)
                .offset(offset)
            ).all()
        return [detail(request, identity, source) for identity in identities]

    @app.get("/v1/notifications/{request_id}")
    def notification(
        request_id: UUID, request: Request, source: Annotated[str, Depends(reader)]
    ) -> dict[str, object]:
        return detail(request, request_id, source)

    @app.get("/v1/notifications/{request_id}/attempts")
    def attempts(
        request_id: UUID,
        request: Request,
        source: Annotated[str, Depends(reader)],
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> list[dict[str, object]]:
        detail(request, request_id, source)
        lifecycle: NotificationLifecycle = request.app.state.notifications
        with Session(lifecycle.engine) as session:
            return [
                {
                    "attempt_id": row.id,
                    "request_id": row.request_id,
                    "correlation_id": row.correlation_id,
                    "channel": row.destination,
                    "outcome": row.outcome,
                    "error_code": row.error_code,
                    "attempted_at": row.attempted_at,
                }
                for row in session.scalars(
                    select(NotifyDelivery)
                    .where(NotifyDelivery.request_id == request_id)
                    .order_by(NotifyDelivery.attempted_at, NotifyDelivery.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @app.get("/v1/notifications/{request_id}/responses")
    def responses(
        request_id: UUID,
        request: Request,
        source: Annotated[str, Depends(reader)],
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> list[dict[str, object]]:
        detail(request, request_id, source)
        lifecycle: NotificationLifecycle = request.app.state.notifications
        with Session(lifecycle.engine) as session:
            return [
                row.payload
                for row in session.scalars(
                    select(ResponseRecord)
                    .where(ResponseRecord.request_id == request_id)
                    .order_by(ResponseRecord.received_at, ResponseRecord.id)
                    .limit(limit)
                    .offset(offset)
                )
            ]

    @app.post(
        "/v1/notifications/{request_id}/channels/{channel}/retry", status_code=202
    )
    def retry(
        request_id: UUID,
        channel: str,
        request: Request,
        source: Annotated[str, Depends(replayer)],
    ) -> dict[str, str]:
        lifecycle: NotificationLifecycle = request.app.state.notifications
        try:
            found = lifecycle.retry(request_id, channel, source)
        except ValueError as error:
            raise HTTPException(409, str(error)) from None
        if not found:
            raise HTTPException(404, "Notification channel not found")
        return {"request_id": str(request_id), "channel": channel, "status": "pending"}

    @app.post("/v1/telegram/callbacks", dependencies=[Depends(telegram_auth)])
    def callback(update: TelegramUpdate, request: Request) -> dict[str, object]:
        lifecycle: NotificationLifecycle = request.app.state.notifications
        result = capture(lifecycle, update)
        provider = lifecycle.providers.get("telegram")
        if isinstance(provider, TelegramProvider):
            provider.acknowledge(update.callback_query.id)
        return result
