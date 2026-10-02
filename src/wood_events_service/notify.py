"""Uvicorn entry point for wood-notify."""

from wood_events_service.main import create_app

app = create_app("notify")
