"""Health route placeholders."""

from typing import Any

from fastapi import APIRouter

router = APIRouter()


@router.get("")
def read_health() -> dict[str, Any]:
    """Return service health."""
    # TODO: Implement FastAPI health endpoint behavior.
    raise NotImplementedError
