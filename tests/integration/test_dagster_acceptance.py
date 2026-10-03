"""Real Dagster job, real service/DB and an isolated Telegram HTTP substitute."""

import json
import os
import secrets
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import UUID, uuid4

import httpx
import pytest
from dagster import EnvVar
from examples.dagster_interaction import NotifyResource, telegram_interaction

from events_service.runtime_check import running


@pytest.mark.parametrize(("action_index", "decision"), [(0, "proceed"), (1, "stop")])
def test_originating_dagster_workflow_decides(
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    action_index: int,
    decision: str,
) -> None:
    token, bot, secret = (secrets.token_hex(32) for _ in range(3))
    chat_id = -(uuid4().int % (2**40))
    correlation, request_id = str(uuid4()), str(uuid4())
    sent = threading.Event()
    callbacks: list[dict[str, object]] = []
    failures: list[str] = []

    class Telegram(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path.endswith("sendMessage"):
                callbacks.append(
                    {
                        "update_id": 10,
                        "callback_query": {
                            "id": "dagster-acceptance-" + request_id,
                            "from": {"id": 42},
                            "message": {"message_id": 99, "chat": {"id": chat_id}},
                            "data": body["reply_markup"]["inline_keyboard"][
                                action_index
                            ][0]["callback_data"],
                        },
                    }
                )
                result: object = {"message_id": 99, "chat": {"id": chat_id}}
                sent.set()
            else:
                result = True
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "result": result}).encode())

        def log_message(self, _format: str, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Telegram)
    provider_thread = threading.Thread(target=server.serve_forever, daemon=True)
    provider_thread.start()
    monkeypatch.setenv("WES_WORKFLOW_TOKEN", token)
    environment = {
        "PATH": os.environ["PATH"],
        "WES_DATABASE_URL": database_url,
        "WES_PRODUCER_CREDENTIALS": json.dumps(
            [
                {
                    "source": "dagster-acceptance",
                    "token": token,
                    "scopes": ["notifications:write", "notifications:read"],
                }
            ]
        ),
        "TELEGRAM_BOT_TOKEN": bot,
        "TELEGRAM_CHAT_ID": str(chat_id),
        "TELEGRAM_WEBHOOK_SECRET": secret,
        "TELEGRAM_API_BASE_URL": f"http://127.0.0.1:{server.server_port}",
        "WES_NOTIFICATION_POLL_SECONDS": "0.05",
        "WES_SUPPRESSION_SECONDS": "0",
    }
    try:
        with running("notify", environment) as notify:
            url = str(notify.base_url)

            def respond() -> None:
                if not sent.wait(timeout=10):
                    failures.append("Telegram did not receive the workflow request")
                    return
                # Delivery commits before callback validation; readiness of this
                # record is checked through the same source-scoped history API.
                with httpx.Client(base_url=url, timeout=3) as client:
                    from events_service.notification_runtime import (  # noqa: PLC0415
                        wait_channels,
                    )

                    wait_channels(
                        client, request_id, {"Authorization": f"Bearer {token}"}
                    )
                    result = client.post(
                        "/v1/telegram/callbacks",
                        json=callbacks[0],
                        headers={"X-Telegram-Bot-Api-Secret-Token": secret},
                    )
                    if result.status_code != 200:
                        failures.append("Telegram callback was not captured")

            callback_thread = threading.Thread(target=respond, daemon=True)
            callback_thread.start()
            result = telegram_interaction.execute_in_process(
                resources={
                    "notify": NotifyResource(
                        base_url=url,
                        source="dagster-acceptance",
                        token=EnvVar("WES_WORKFLOW_TOKEN"),
                        response_timeout_seconds=15,
                    )
                },
                run_config={
                    "ops": {
                        "request_action": {
                            "config": {
                                "request_id": request_id,
                                "correlation_id": correlation,
                                "response_deadline": (
                                    datetime.now(UTC) + timedelta(minutes=1)
                                ).isoformat(),
                            }
                        }
                    }
                },
            )
            callback_thread.join(timeout=15)
            assert not callback_thread.is_alive()
            assert not failures
            assert result.success
            evidence = result.output_for_node("next_workflow_decision")
            assert evidence["decision"] == decision
            assert evidence["correlation_id"] == correlation
            assert evidence["request_id"] == request_id
            assert UUID(evidence["response_id"])
            responses = notify.get(
                f"/v1/notifications/{request_id}/responses",
                headers={"Authorization": f"Bearer {token}"},
            ).json()
            assert evidence["response_id"] == responses[0]["response_id"]
            assert token not in json.dumps(evidence)
    finally:
        server.shutdown()
        server.server_close()
        provider_thread.join(timeout=5)
