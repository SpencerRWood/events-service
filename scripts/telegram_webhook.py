"""Preview or register the reviewed public HTTPS callback without exposing secrets."""

import argparse
import json
from urllib.parse import urlsplit

import httpx

from events_service.config import Settings


def register(
    settings: Settings,
    url: str,
    *,
    apply: bool = False,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, object]:
    endpoint = urlsplit(url)
    if (
        endpoint.scheme != "https"
        or not endpoint.hostname
        or endpoint.username
        or endpoint.password
        or endpoint.query
        or endpoint.fragment
        or endpoint.path != "/v1/telegram/callbacks"
        or endpoint.port not in {None, 443}
    ):
        raise ValueError("A reviewed HTTPS callback URL on port 443 is required")
    bot, secret = settings.telegram_bot_token, settings.telegram_webhook_secret
    if bot is None or secret is None:
        raise ValueError("Telegram configuration is required")
    result: dict[str, object] = {"operation": "setWebhook", "apply": apply}
    if not apply:
        return result
    base = settings.telegram_api_base_url.rstrip("/") + "/bot" + bot.get_secret_value()
    with httpx.Client(timeout=10, trust_env=False, transport=transport) as client:
        response = client.post(
            base + "/setWebhook",
            json={
                "url": url,
                "secret_token": secret.get_secret_value(),
                "allowed_updates": ["callback_query"],
                "drop_pending_updates": False,
            },
        )
        if response.status_code != 200 or not response.json().get("ok"):
            raise RuntimeError("Telegram webhook registration failed")
        observed = client.post(base + "/getWebhookInfo")
        if observed.status_code != 200 or not observed.json().get("ok"):
            raise RuntimeError("Telegram webhook verification failed")
        info = observed.json()["result"]
        if info.get("url") != url or info.get("allowed_updates") != ["callback_query"]:
            raise RuntimeError(
                "Telegram webhook configuration differs from the request"
            )
        result["registered"] = True
        result["pending_updates"] = info.get("pending_update_count", 0)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        result = register(Settings(), args.url, apply=args.apply)  # type: ignore[call-arg]
    except Exception:
        raise SystemExit(
            "Telegram webhook operation failed; details withheld"
        ) from None
    print(json.dumps(result))


if __name__ == "__main__":
    main()
