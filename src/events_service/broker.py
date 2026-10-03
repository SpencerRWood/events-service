"""Uvicorn entry point for wood-broker."""

from events_service.main import create_app

app = create_app("broker")
