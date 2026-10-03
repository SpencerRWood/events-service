"""Uvicorn entry point for wood-notify."""

from events_service.main import create_app

app = create_app("notify")
