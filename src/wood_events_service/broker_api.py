"""Scoped broker history and replay endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from wood_events_service.config import Scope
from wood_events_service.delivery import BrokerWorker
from wood_events_service.history import BrokerHistory
from wood_events_service.security import ServiceAuth

Limit = Annotated[int, Query(ge=1, le=100)]
Offset = Annotated[int, Query(ge=0, le=100000)]
bearer = HTTPBearer(auto_error=False)


def register_broker_routes(app: FastAPI) -> None:
    def authenticate(scope: Scope, request: Request, token: str) -> str:
        auth: ServiceAuth = request.app.state.auth
        return auth.authenticate(token, scope)

    def reader(
        request: Request,
        credential: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        return authenticate(
            "events:read", request, credential.credentials if credential else ""
        )

    def replayer(
        request: Request,
        credential: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        return authenticate(
            "events:replay", request, credential.credentials if credential else ""
        )

    @app.get("/v1/events")
    def events(
        request: Request,
        source: Annotated[str, Depends(reader)],
        correlation_id: UUID | None = None,
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> list[dict[str, object]]:
        history: BrokerHistory = request.app.state.history
        return history.events(source, correlation_id, limit, offset)

    @app.get("/v1/events/{event_id}")
    def event(
        event_id: UUID, request: Request, source: Annotated[str, Depends(reader)]
    ) -> dict[str, object]:
        history: BrokerHistory = request.app.state.history
        result = history.event(event_id, source)
        if result is None:
            raise HTTPException(404, "Event not found")
        return result

    @app.get("/v1/events/{event_id}/attempts")
    def attempts(
        event_id: UUID,
        request: Request,
        source: Annotated[str, Depends(reader)],
        limit: Limit = 50,
        offset: Offset = 0,
    ) -> list[dict[str, object]]:
        history: BrokerHistory = request.app.state.history
        if history.event(event_id, source) is None:
            raise HTTPException(404, "Event not found")
        return history.attempts(event_id, source, limit, offset)

    @app.post("/v1/events/{event_id}/deliveries/{consumer}/replay", status_code=202)
    def replay(
        event_id: UUID,
        consumer: str,
        request: Request,
        source: Annotated[str, Depends(replayer)],
    ) -> dict[str, str]:
        worker: BrokerWorker = request.app.state.worker
        try:
            found = worker.replay(event_id, consumer, source)
        except ValueError as error:
            raise HTTPException(409, str(error)) from None
        if not found:
            raise HTTPException(404, "Delivery not found")
        return {"event_id": str(event_id), "consumer": consumer, "status": "pending"}
