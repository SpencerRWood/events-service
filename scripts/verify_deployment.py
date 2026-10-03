"""Read-only post-promotion verification. Never migrate or send notifications."""

import json
import os
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx
from sqlalchemy import create_engine

from events_service.config import Settings
from events_service.storage import check_schema
from scripts.telegram_webhook import register


def verify(
    settings: Settings | None = None, *, transport: httpx.BaseTransport | None = None
) -> dict[str, object]:
    settings = settings or Settings()  # type: ignore[call-arg]
    if settings.telegram_bot_token is None:
        raise RuntimeError("The required Telegram provider is disabled")
    engine = create_engine(
        settings.database_url.get_secret_value(),
        hide_parameters=True,
        connect_args={"connect_timeout": settings.database_timeout_seconds},
    )
    try:
        check_schema(engine)
    finally:
        engine.dispose()
    with httpx.Client(timeout=5, trust_env=False, transport=transport) as client:
        for name in ("BROKER", "NOTIFY"):
            base = os.environ[f"WES_VERIFY_{name}_URL"].rstrip("/")
            for path in ("/health/live", "/health/ready"):
                response = client.get(base + path)
                expected = "ready" if path == "/health/ready" else "ok"
                if (
                    response.status_code != 200
                    or response.json().get("status") != expected
                    or response.json().get("service") != f"wood-{name.lower()}"
                ):
                    raise RuntimeError("Deployed service health check failed")
        callback = os.environ["WES_VERIFY_CALLBACK_URL"]
        register(settings, callback)  # Validate the URL, without registration.
        if client.post(callback, json={}).status_code != 401:
            raise RuntimeError("Public callback does not enforce authentication")
        for path in ("/v1/notifications", "/v1/events", "/health/ready"):
            public = urlsplit(callback)
            if client.get(f"https://{public.netloc}{path}").status_code != 404:
                raise RuntimeError("Public ingress exposes an unintended route")
        bot = settings.telegram_bot_token.get_secret_value()
        base = settings.telegram_api_base_url.rstrip("/") + "/bot" + bot
        observed = client.post(base + "/getWebhookInfo")
        if observed.status_code != 200 or not observed.json().get("ok"):
            raise RuntimeError("Telegram webhook state is unavailable")
        info = observed.json()["result"]
        if (
            info.get("url") != callback
            or info.get("allowed_updates") != ["callback_query"]
            or (info.get("last_error_date") and info.get("pending_update_count", 0))
        ):
            raise RuntimeError("Telegram webhook is absent, mismatched or has errors")
    return {
        "observed_at": datetime.now(UTC).isoformat(),
        "state": "passed",
        "checks": [
            "real-database-schema",
            "broker-health",
            "notify-health",
            "https-callback-auth",
            "private-api-boundary",
            "webhook-registration",
        ],
        "limitations": [
            "does not attest image identity or live end-to-end interaction"
        ],
    }


if __name__ == "__main__":
    try:
        print(json.dumps(verify()))
    except Exception:
        raise SystemExit(
            "Deployed runtime verification failed; details withheld"
        ) from None
