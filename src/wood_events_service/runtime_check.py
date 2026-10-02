"""Repository-owned candidate checks: migrations, both processes and durability."""

import json
import os
import secrets
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, text

from wood_events_service.migrate import migrate


@contextmanager
def running(service: str, environment: dict[str, str]) -> Iterator[httpx.Client]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    process = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "uvicorn",
            f"wood_events_service.{service}:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--no-access-log",
        ],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=3) as client:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"{service} process exited during startup")
                try:
                    response = client.get("/health/ready")
                    if response.status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError(f"{service} readiness timed out")
            yield client
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def verify(url: str) -> None:
    """Only use a disposable database: this upgrades and checks foundation state."""
    migrate(url)
    token = secrets.token_hex(32)
    credentials = json.dumps(
        [
            {
                "source": "runtime-check",
                "token": token,
                "scopes": ["events:write", "notifications:write"],
            }
        ]
    )
    environment = {
        **os.environ,
        "WES_DATABASE_URL": url,
        "WES_PRODUCER_CREDENTIALS": credentials,
    }
    correlation = str(uuid4())
    event_id, request_id = str(uuid4()), str(uuid4())
    event: dict[str, object] = {
        "schema_version": "1",
        "event_id": event_id,
        "source": "runtime-check",
        "event_type": "runtime.test",
        "severity": "info",
        "occurred_at": datetime.now(UTC).isoformat(),
        "correlation_id": correlation,
        "data": {"test": True},
        "idempotency_key": f"runtime-{event_id}",
    }
    notification: dict[str, object] = {
        "schema_version": "1",
        "request_id": request_id,
        "source": "runtime-check",
        "notification_type": "informational",
        "severity": "info",
        "title": "Check",
        "message": "Foundation check",
        "correlation_id": correlation,
        "causation_id": event_id,
        "policy_key": "runtime-test",
    }
    headers = {"Authorization": f"Bearer {token}"}
    for service, endpoint, payload in (
        ("broker", "/v1/events", event),
        ("notify", "/v1/notifications", notification),
    ):
        with running(service, environment) as client:
            require(
                client.post(endpoint, json=payload).status_code == 401,
                "Unauthenticated input was accepted",
            )
            accepted = client.post(endpoint, json=payload, headers=headers)
            require(accepted.status_code == 201, f"{service} ingestion failed")
            require(
                accepted.json()["correlation_id"] == correlation, "Correlation changed"
            )
        # A new process must observe committed durable state and the original identity.
        with running(service, environment) as client:
            duplicate = client.post(endpoint, json=payload, headers=headers)
            require(
                duplicate.status_code == 201 and duplicate.json()["duplicate"],
                "Restart lost producer identity",
            )
            require(
                duplicate.json()["record_id"] == accepted.json()["record_id"],
                "Restart changed identity",
            )
    engine = create_engine(url, hide_parameters=True)
    try:
        with engine.connect() as connection:
            for table, record_id in (
                ("broker_events", event_id),
                ("notify_requests", request_id),
            ):
                row = connection.execute(
                    text(
                        f"SELECT correlation_id, payload FROM {table} WHERE id = :id"  # noqa: S608
                    ),
                    {"id": record_id},
                ).one()
                require(
                    str(row.correlation_id) == correlation,
                    "Persisted correlation mismatch",
                )
                require(token not in json.dumps(row.payload), "Credential persisted")
    finally:
        engine.dispose()


def main() -> None:
    try:
        verify(os.environ["RUNTIME_DATABASE_URL"])
    except Exception:
        raise SystemExit("Foundation runtime checks failed") from None
    print(
        json.dumps(
            {
                "state": "passed",
                "checks": [
                    "migrations",
                    "broker-startup",
                    "notify-startup",
                    "authentication",
                    "correlation",
                    "durable-restart",
                    "credential-exclusion",
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
