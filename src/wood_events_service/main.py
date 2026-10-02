"""Application factory for the FastAPI service."""

from fastapi import FastAPI

from wood_events_service.api.router import api_router


def create_app() -> FastAPI:
    """Create the FastAPI application."""
    app = FastAPI(title="wood-events-service")
    app.include_router(api_router)
    return app


app = create_app()
