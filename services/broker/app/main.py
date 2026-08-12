from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, Query
from sqlalchemy.orm import Session

from shared.config import BrokerSettings

from .database import create_schema, make_session_factory, session_dependency
from .dispatcher import WebhookDispatcher
from .schemas import DeliveryOut, EventIn, EventOut, SubscriptionIn, SubscriptionOut
from .service import (
    create_subscription,
    get_event,
    ingest_event,
    list_deliveries,
    list_events,
    list_subscriptions,
    retry_delivery,
)


def create_app(
    settings: BrokerSettings | None = None,
    dispatcher_client: httpx.Client | None = None,
) -> FastAPI:
    resolved = settings or BrokerSettings()
    session_factory = make_session_factory(resolved.database_url)
    dispatcher = WebhookDispatcher(
        timeout_seconds=resolved.delivery_timeout_seconds,
        client=dispatcher_client,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        create_schema(session_factory)
        yield

    app = FastAPI(title="wood-broker", version="0.1.0", lifespan=lifespan)
    get_session = session_dependency(session_factory)

    @app.get("/healthz", tags=["health"])
    def healthz() -> dict[str, str]:
        return {"service": "wood-broker", "status": "ok"}

    @app.get("/readyz", tags=["health"])
    def readyz() -> dict[str, str | int]:
        return {"service": "wood-broker", "status": "ready", "port": resolved.port}

    @app.post("/subscriptions", response_model=SubscriptionOut, status_code=201, tags=["broker"])
    def create_subscription_route(
        payload: SubscriptionIn,
        session: Session = Depends(get_session),
    ) -> SubscriptionOut:
        return create_subscription(session, payload)

    @app.get("/subscriptions", response_model=list[SubscriptionOut], tags=["broker"])
    def list_subscriptions_route(session: Session = Depends(get_session)) -> list[SubscriptionOut]:
        return list_subscriptions(session)

    @app.post("/events", response_model=EventOut, status_code=202, tags=["broker"])
    def ingest_event_route(
        payload: EventIn,
        session: Session = Depends(get_session),
    ) -> EventOut:
        return ingest_event(
            session,
            payload,
            dispatcher,
            max_attempts=resolved.max_delivery_attempts,
            retry_backoff_seconds=resolved.retry_backoff_seconds,
        )

    @app.get("/events", response_model=list[EventOut], tags=["broker"])
    def list_events_route(session: Session = Depends(get_session)) -> list[EventOut]:
        return list_events(session)

    @app.get("/events/{event_id}", response_model=EventOut, tags=["broker"])
    def get_event_route(event_id: str, session: Session = Depends(get_session)) -> EventOut:
        return get_event(session, event_id)

    @app.get("/deliveries", response_model=list[DeliveryOut], tags=["broker"])
    def list_deliveries_route(
        event_id: str | None = Query(default=None),
        session: Session = Depends(get_session),
    ) -> list[DeliveryOut]:
        return list_deliveries(session, event_id=event_id)

    @app.post("/deliveries/{delivery_id}/retry", response_model=DeliveryOut, tags=["broker"])
    def retry_delivery_route(
        delivery_id: int,
        session: Session = Depends(get_session),
    ) -> DeliveryOut:
        return retry_delivery(
            session,
            delivery_id,
            dispatcher,
            max_attempts=resolved.max_delivery_attempts,
            retry_backoff_seconds=resolved.retry_backoff_seconds,
        )

    return app


def create_runtime_app() -> FastAPI:
    return create_app()
