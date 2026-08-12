from __future__ import annotations

from typing import Any

import httpx


class WebhookDispatcher:
    def __init__(self, *, timeout_seconds: float, client: httpx.Client | None = None) -> None:
        self._timeout_seconds = timeout_seconds
        self._client = client

    def post(self, url: str, payload: dict[str, Any]) -> tuple[int | None, str | None]:
        try:
            if self._client is not None:
                response = self._client.post(url, json=payload, timeout=self._timeout_seconds)
            else:
                with httpx.Client(timeout=self._timeout_seconds) as client:
                    response = client.post(url, json=payload)
        except httpx.HTTPError as err:
            return None, str(err)
        if 200 <= response.status_code < 300:
            return response.status_code, None
        return response.status_code, response.text[:1000]
