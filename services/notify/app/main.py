from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, Query
from sqlalchemy.orm import Session

from shared.config import NotifySettings, NtfySettings

from .adapters import NotificationAdapter, default_adapters
from .database import create_schema, make_session_factory, session_dependency
from .schemas import (
    BrokerEventConsumeOut,
    BrokerEventIn,
    NotificationDeliveryOut,
    NotificationPolicyIn,
    NotificationPolicyOut,
)
from .service import (
    consume_broker_event,
    create_policy,
    list_deliveries,
    list_policies,
    retry_delivery,
)


def create_app(
    settings: NotifySettings | None = None,
    ntfy_settings: NtfySettings | None = None,
    adapters: dict[str, NotificationAdapter] | None = None,
    ntfy_client: httpx.Client | None = None,
    telegram_client: httpx.Client | None = None,
) -> FastAPI:
    resolved = settings or NotifySettings()
    ntfy = ntfy_settings or NtfySettings()
    session_factory = make_session_factory(resolved.database_url)
    adapter_registry = adapters or default_adapters(
        resolved,
        ntfy,
        ntfy_client=ntfy_client,
        telegram_client=telegram_client,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        create_schema(session_factory)
        yield

    app = FastAPI(title="wood-notify", version="0.1.0", lifespan=lifespan)
    get_session = session_dependency(session_factory)

    @app.get("/healthz", tags=["health"])
    def healthz() -> dict[str, str]:
        return {"service": "wood-notify", "status": "ok"}

    @app.get("/readyz", tags=["health"])
    def readyz() -> dict[str, str | int]:
        return {
            "service": "wood-notify",
            "status": "ready",
            "port": resolved.port,
            "ntfy_base_url": str(ntfy.base_url),
        }

    @app.post("/policies", response_model=NotificationPolicyOut, status_code=201, tags=["notify"])
    def create_policy_route(
        payload: NotificationPolicyIn,
        session: Session = Depends(get_session),
    ) -> NotificationPolicyOut:
        return create_policy(session, payload)

    @app.get("/policies", response_model=list[NotificationPolicyOut], tags=["notify"])
    def list_policies_route(session: Session = Depends(get_session)) -> list[NotificationPolicyOut]:
        return list_policies(session)

    @app.post("/broker-events", response_model=BrokerEventConsumeOut, status_code=202, tags=["notify"])
    def consume_broker_event_route(
        payload: BrokerEventIn,
        session: Session = Depends(get_session),
    ) -> BrokerEventConsumeOut:
        return consume_broker_event(
            session,
            payload,
            adapter_registry,
            max_attempts=resolved.max_delivery_attempts,
            retry_backoff_seconds=resolved.retry_backoff_seconds,
        )

    @app.get("/deliveries", response_model=list[NotificationDeliveryOut], tags=["notify"])
    def list_deliveries_route(
        event_id: str | None = Query(default=None),
        session: Session = Depends(get_session),
    ) -> list[NotificationDeliveryOut]:
        return list_deliveries(session, event_id=event_id)

    @app.post("/deliveries/{delivery_id}/retry", response_model=NotificationDeliveryOut, tags=["notify"])
    def retry_delivery_route(
        delivery_id: int,
        session: Session = Depends(get_session),
    ) -> NotificationDeliveryOut:
        return retry_delivery(
            session,
            delivery_id,
            adapter_registry,
            max_attempts=resolved.max_delivery_attempts,
            retry_backoff_seconds=resolved.retry_backoff_seconds,
        )

    return app


def create_runtime_app() -> FastAPI:
    return create_app()
