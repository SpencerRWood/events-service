"""Uvicorn entry point for wood-broker."""

from wood_events_service.main import create_app

app = create_app("broker")
