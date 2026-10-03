"""Real notify process with isolated HTTP provider substitutes for runtime checks."""

import json
import secrets
import threading
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import httpx

from events_service.providers import callback_data


def wait_channels(
    client: httpx.Client, request_id: str, headers: dict[str, str]
) -> dict[str, object]:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        response = client.get(f"/v1/notifications/{request_id}", headers=headers)
        if response.status_code == 404:
            time.sleep(0.05)
            continue
        response.raise_for_status()
        body: dict[str, object] = response.json()
        channels = body["channels"]
        if (
            isinstance(channels, list)
            and channels
            and all(job["status"] == "delivered" for job in channels)
        ):
            return body
        time.sleep(0.05)
    raise RuntimeError("Notification channels did not deliver")


def verify_notifications(environment: dict[str, str], token: str) -> None:
    from events_service.runtime_check import require, running  # noqa: PLC0415

    received: list[tuple[str, dict[str, object]]] = []
    bot_token, webhook_secret = secrets.token_hex(32), secrets.token_hex(32)
    chat_id = -123

    class Consumer(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received.append((self.path, body))
            if self.path.endswith("sendMessage"):
                response = {
                    "ok": True,
                    "result": {"message_id": len(received), "chat": {"id": chat_id}},
                }
                status = 200
            elif self.path.endswith("answerCallbackQuery"):
                response, status = {"ok": True, "result": True}, 200
            else:
                first = sum(path == "/" for path, _ in received) == 1
                response, status = {"id": "local-ntfy"}, (503 if first else 200)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())

        def log_message(self, _format: str, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Consumer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    configured = {
        **environment,
        "NTFY_BASE_URL": f"http://127.0.0.1:{server.server_port}",
        "NTFY_TOPIC": "runtime-check",
        "NTFY_AUTH_TOKEN": "",
        "TELEGRAM_API_BASE_URL": f"http://127.0.0.1:{server.server_port}",
        "TELEGRAM_BOT_TOKEN": bot_token,
        "TELEGRAM_CHAT_ID": str(chat_id),
        "TELEGRAM_WEBHOOK_SECRET": webhook_secret,
        "TELEGRAM_ALLOWED_USER_IDS": "[42]",
        "WES_NOTIFICATION_POLICIES": json.dumps(
            [
                {
                    "name": "broker",
                    "channels": ["ntfy", "telegram"],
                    "event_types": ["runtime.notify"],
                },
                {
                    "name": "direct",
                    "channels": ["telegram"],
                    "policy_keys": ["runtime-interaction"],
                },
            ]
        ),
        "WES_SUPPRESSION_SECONDS": "0",
        "WES_NOTIFICATION_POLL_SECONDS": "0.05",
        "WES_NOTIFICATION_BACKOFF_SECONDS": "0.05",
        "WES_NOTIFICATION_MAX_ATTEMPTS": "3",
    }
    event_id, correlation = str(uuid4()), str(uuid4())
    event = {
        "event_id": event_id,
        "event_type": "runtime.notify",
        "source": "runtime-check",
        "severity": "error",
        "occurred_at": datetime.now(UTC).isoformat(),
        "correlation_id": correlation,
        "data": {"state": "failed"},
    }
    request_id = str(uuid4())
    direct = {
        "request_id": request_id,
        "source": "runtime-check",
        "notification_type": "action-required",
        "severity": "warning",
        "title": "Runtime interaction",
        "message": "Choose a generic response",
        "channels": ["telegram"],
        "policy_key": "runtime-interaction",
        "correlation_id": correlation,
        "response_actions": [{"action": "acknowledge", "label": "Acknowledge"}],
        "response_deadline": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
    }
    headers = {"Authorization": f"Bearer {token}"}
    try:
        with running("notify", configured) as client:
            broker_environment = {
                **configured,
                "WES_SUBSCRIPTIONS": json.dumps(
                    [
                        {
                            "consumer": "wood-notify",
                            "url": str(client.base_url).rstrip("/")
                            + "/v1/broker-events",
                            "event_types": ["runtime.notify"],
                            "auth_token": token,
                        }
                    ]
                ),
                "WES_BROKER_POLL_SECONDS": "0.05",
            }
            with running("broker", broker_environment) as broker:
                accepted = broker.post("/v1/events", json=event, headers=headers)
                require(
                    accepted.status_code == 201, "Broker-to-notify ingestion failed"
                )
                derived_id = str(
                    uuid5(NAMESPACE_URL, f"wes-notify:runtime-check:{event_id}")
                )
                derived = wait_channels(client, derived_id, headers)
            request = derived["request"]
            require(
                isinstance(request, dict) and request["causation_id"] == event_id,
                "Notify causation changed",
            )
            require(
                len(
                    client.get(
                        f"/v1/notifications/{derived_id}/attempts", headers=headers
                    ).json()
                )
                == 3,
                "Notify retry evidence missing",
            )
            require(
                client.post(
                    "/v1/notifications", json=direct, headers=headers
                ).status_code
                == 201,
                "Direct notification failed",
            )
            wait_channels(client, request_id, headers)
            with_keyboard = [
                (index + 1, body)
                for index, (path, body) in enumerate(received)
                if path.endswith("sendMessage") and "reply_markup" in body
            ]
            require(len(with_keyboard) == 1, "Telegram keyboard missing")
            message_id = with_keyboard[0][0]
            callback = {
                "update_id": 1,
                "callback_query": {
                    "id": "runtime-callback",
                    "from": {"id": 42},
                    "message": {"message_id": message_id, "chat": {"id": chat_id}},
                    "data": callback_data(UUID(request_id), 0, webhook_secret),
                },
            }
            webhook_headers = {"X-Telegram-Bot-Api-Secret-Token": webhook_secret}
            require(
                client.post("/v1/telegram/callbacks", json=callback).status_code == 401,
                "Unauthenticated callback accepted",
            )
            captured = client.post(
                "/v1/telegram/callbacks", json=callback, headers=webhook_headers
            )
            require(
                captured.status_code == 200
                and captured.json()["response"]["correlation_id"] == correlation,
                "Response capture failed",
            )
            response = captured.json()["response"]
        with running("notify", configured) as client:
            require(
                client.post("/v1/broker-events", json=event, headers=headers).json()[
                    "duplicate"
                ],
                "Notify restart lost event identity",
            )
            duplicate = client.post(
                "/v1/telegram/callbacks", json=callback, headers=webhook_headers
            )
            require(
                duplicate.status_code == 200
                and duplicate.json()["duplicate"]
                and duplicate.json()["response"] == response,
                "Notify callback replay changed response",
            )
            state = client.get(
                f"/v1/notifications/{request_id}", headers=headers
            ).json()
            require(state["response_state"] == "responded", "Responded state lost")
            require(
                client.get(
                    f"/v1/notifications/{request_id}/responses", headers=headers
                ).json()
                == [response],
                "Response query failed",
            )
            require(
                bot_token not in json.dumps(state)
                and webhook_secret not in json.dumps(state),
                "Provider credential persisted",
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
