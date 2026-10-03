"""Real broker process and local HTTP consumers for the candidate runtime gate."""

import json
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, text


def wait_delivered(
    client: httpx.Client, event_id: str, headers: dict[str, str]
) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        response = client.get(f"/v1/events/{event_id}", headers=headers)
        response.raise_for_status()
        jobs = response.json()["deliveries"]
        if len(jobs) == 2 and all(job["status"] == "delivered" for job in jobs):
            return
        time.sleep(0.05)
    raise RuntimeError("Broker delivery did not complete")


def verify_broker(url: str, environment: dict[str, str], token: str) -> None:
    # Imported here to reuse the existing subprocess lifecycle without a cycle.
    from wood_events_service.runtime_check import require, running  # noqa: PLC0415

    engine = create_engine(url, hide_parameters=True)
    received: list[tuple[str, str, dict[str, object]]] = []
    event_id, correlation = str(uuid4()), str(uuid4())

    class Consumer(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            # A consumer can observe the committed event before its side effect.
            with engine.connect() as connection:
                committed = connection.scalar(
                    text("SELECT id FROM broker_events WHERE id = :id"),
                    {"id": payload["event_id"]},
                )
            received.append((self.path, self.headers["Idempotency-Key"], payload))
            first_retry = (
                self.path == "/retry"
                and sum(path == "/retry" for path, _, _ in received) == 1
            )
            status = 500 if committed is None else (503 if first_retry else 204)
            self.send_response(status)
            self.end_headers()

        def log_message(self, _format: str, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Consumer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    subscriptions = [
        {
            "consumer": name,
            "url": f"http://127.0.0.1:{server.server_port}/{name}",
            "event_types": ["runtime.broker"],
        }
        for name in ("ok", "retry")
    ]
    configured = {
        **environment,
        "WES_SUBSCRIPTIONS": json.dumps(subscriptions),
        "WES_BROKER_POLL_SECONDS": "0.05",
        "WES_WEBHOOK_BACKOFF_SECONDS": "0.05",
        "WES_WEBHOOK_MAX_ATTEMPTS": "3",
        "WES_WEBHOOK_TIMEOUT_SECONDS": "1",
    }
    payload = {
        "event_id": event_id,
        "event_type": "runtime.broker",
        "source": "runtime-check",
        "severity": "info",
        "occurred_at": datetime.now(UTC).isoformat(),
        "correlation_id": correlation,
        "data": {"operational_state": "check"},
    }
    headers = {"Authorization": f"Bearer {token}"}
    try:
        with running("broker", configured) as client:
            require(
                client.post("/v1/events", json=payload, headers=headers).status_code
                == 201,
                "Broker runtime ingestion failed",
            )
            wait_delivered(client, event_id, headers)
            attempts = client.get(
                f"/v1/events/{event_id}/attempts", headers=headers
            ).json()
            require(len(attempts) == 3, "Retry evidence missing")
        with running("broker", configured) as client:
            duplicate = client.post("/v1/events", json=payload, headers=headers)
            require(duplicate.json()["duplicate"], "Broker restart lost identity")
            require(
                client.get(f"/v1/events/{event_id}/attempts", headers=headers).json()
                == attempts,
                "Restart rewrote delivery evidence",
            )
            require(
                client.post(
                    f"/v1/events/{event_id}/deliveries/ok/replay", headers=headers
                ).status_code
                == 202,
                "Runtime replay failed",
            )
            wait_delivered(client, event_id, headers)
            require(
                len(
                    client.get(
                        f"/v1/events/{event_id}/attempts", headers=headers
                    ).json()
                )
                == 4,
                "Replay evidence missing",
            )
            require(
                len(
                    client.get(
                        f"/v1/events?correlation_id={correlation}", headers=headers
                    ).json()
                )
                == 1,
                "Correlation query failed",
            )
        require(len(received) == 4, "Unexpected logical delivery count")
        require(
            all(
                item["event_id"] == event_id and item["correlation_id"] == correlation
                for _, _, item in received
            ),
            "Delivery changed identity",
        )
        require(
            len({key for _, key, _ in received}) == 2,
            "Retry/replay changed consumer idempotency identity",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        engine.dispose()
