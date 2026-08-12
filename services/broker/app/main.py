from __future__ import annotations

from fastapi import FastAPI

from shared.config import BrokerSettings


def create_app(settings: BrokerSettings | None = None) -> FastAPI:
    resolved = settings or BrokerSettings()
    app = FastAPI(title="wood-broker", version="0.1.0")

    @app.get("/healthz", tags=["health"])
    def healthz() -> dict[str, str]:
        return {"service": "wood-broker", "status": "ok"}

    @app.get("/readyz", tags=["health"])
    def readyz() -> dict[str, str | int]:
        return {"service": "wood-broker", "status": "ready", "port": resolved.port}

    return app


app = create_app()
