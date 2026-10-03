"""Thin independently runnable broker and notify HTTP applications."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from events_service.broker_api import register_broker_routes
from events_service.config import Scope, Settings
from events_service.contracts import EventEnvelope, NotificationRequest, Receipt
from events_service.delivery import BrokerWorker, HttpWebhook, Webhook
from events_service.history import BrokerHistory
from events_service.notification_api import register_notification_routes
from events_service.notification_lifecycle import NotificationLifecycle
from events_service.providers import Provider, provider_registry
from events_service.routing import SubscriptionRouter
from events_service.security import (
    SecretPolicy,
    ServiceAuth,
    StructuredFormatter,
    TokenAuth,
)
from events_service.storage import (
    IdempotencyConflictError,
    Store,
    check_schema,
    make_engine,
)

Service = Literal["broker", "notify"]
bearer = HTTPBearer(auto_error=False)


def create_app(  # noqa: PLR0913, PLR0915 -- service-scoped routes and injectable boundaries
    service: Service,
    settings: Settings | None = None,
    *,
    engine: Engine | None = None,
    auth: ServiceAuth | None = None,
    webhook: Webhook | None = None,
    start_worker: bool = True,
    providers: dict[str, Provider] | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            configured = settings or Settings()  # type: ignore[call-arg]
            active_engine = engine or make_engine(configured)
        except Exception:
            raise RuntimeError("Invalid runtime configuration") from None
        policy = SecretPolicy(configured.secret_values())
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredFormatter(policy))
        logger = logging.getLogger(f"wes.{service}")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        app.state.store = Store(
            active_engine,
            policy,
            SubscriptionRouter(configured.subscriptions)
            if service == "broker"
            else None,
        )
        if service == "notify":
            app.state.notifications = NotificationLifecycle(
                active_engine,
                configured,
                providers if providers is not None else provider_registry(configured),
            )
            app.state.store.notification_hook = app.state.notifications.schedule
        app.state.auth = auth or TokenAuth(configured.producer_credentials)
        app.state.logger = logger
        stop = asyncio.Event()
        worker_task: asyncio.Task[None] | None = None
        try:
            check_schema(active_engine)
            if service == "broker":
                app.state.history = BrokerHistory(active_engine)
                app.state.worker = BrokerWorker(
                    active_engine,
                    configured,
                    webhook
                    or HttpWebhook(
                        configured.webhook_timeout_seconds,
                        credentials={
                            item.consumer: item.auth_token
                            for item in configured.subscriptions
                            if item.auth_token is not None
                        },
                    ),
                )
                if start_worker:
                    worker_task = asyncio.create_task(app.state.worker.run(stop))
            elif start_worker:
                worker_task = asyncio.create_task(app.state.notifications.run(stop))
            yield
        except SQLAlchemyError:
            raise RuntimeError("Database unavailable or migration required") from None
        finally:
            stop.set()
            if worker_task is not None:
                await worker_task
            logger.removeHandler(handler)
            handler.close()
            if engine is None:
                active_engine.dispose()

    app = FastAPI(title=f"wood-{service}", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        _request: Request, _error: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": "Invalid v1 contract"})

    @app.exception_handler(SQLAlchemyError)
    async def database_error(
        _request: Request, _error: SQLAlchemyError
    ) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": "Storage unavailable"})

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok", "service": f"wood-{service}"}

    @app.get("/health/ready")
    def ready(request: Request) -> dict[str, str]:
        check_schema(request.app.state.store.engine)
        return {"status": "ready", "service": f"wood-{service}"}

    scope: Scope = "events:write" if service == "broker" else "notifications:write"

    def producer(
        request: Request,
        credential: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        boundary: ServiceAuth = request.app.state.auth
        return boundary.authenticate(
            credential.credentials if credential else "", scope
        )

    def accept(
        request: Request, envelope: EventEnvelope | NotificationRequest, source: str
    ) -> Receipt:
        if envelope.source != source:
            raise HTTPException(403, "Credential source does not match payload")
        store: Store = request.app.state.store
        try:
            receipt = store.accept(envelope)
        except ValueError:
            raise HTTPException(422, "Invalid or credential-bearing content") from None
        except IdempotencyConflictError:
            raise HTTPException(409, "Identity reused with different content") from None
        identifier = "event_id" if service == "broker" else "request_id"
        request.app.state.logger.info(
            "accepted",
            extra={
                identifier: receipt.record_id,
                "correlation_id": receipt.correlation_id,
            },
        )
        return receipt

    if service == "broker":
        register_broker_routes(app)

        @app.post("/v1/events", status_code=201)
        def ingest_event(
            request: Request,
            envelope: EventEnvelope,
            source: Annotated[str, Depends(producer)],
        ) -> Receipt:
            return accept(request, envelope, source)
    else:
        register_notification_routes(app)

        @app.post("/v1/notifications", status_code=201)
        def ingest_notification(
            request: Request,
            envelope: NotificationRequest,
            source: Annotated[str, Depends(producer)],
        ) -> Receipt:
            return accept(request, envelope, source)

    return app
