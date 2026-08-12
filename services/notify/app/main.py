from __future__ import annotations

from fastapi import FastAPI

from shared.config import NotifySettings, NtfySettings


def create_app(
    settings: NotifySettings | None = None,
    ntfy_settings: NtfySettings | None = None,
) -> FastAPI:
    resolved = settings or NotifySettings()
    ntfy = ntfy_settings or NtfySettings()
    app = FastAPI(title="wood-notify", version="0.1.0")

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

    return app


app = create_app()
