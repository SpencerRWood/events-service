"""A unique disposable PostgreSQL instance for tests and local verification."""

import secrets
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid4


def command(*args: str) -> str:
    result = subprocess.run(
        args, capture_output=True, text=True, timeout=30, check=False
    )
    if result.returncode:
        raise RuntimeError("Disposable PostgreSQL command failed")
    return result.stdout.strip()


@contextmanager
def isolated_postgres() -> Iterator[str]:
    name = f"wes-test-{uuid4().hex[:12]}"
    password = secrets.token_hex(24)
    try:
        command(
            "docker",
            "run",
            "--detach",
            "--name",
            name,
            "--publish",
            "127.0.0.1::5432",
            "--env",
            "POSTGRES_USER=wes_test",
            "--env",
            f"POSTGRES_PASSWORD={password}",
            "--env",
            "POSTGRES_DB=wes_test",
            "postgres:16-alpine",
        )
        address = command("docker", "port", name, "5432/tcp")
        port = address.rsplit(":", 1)[1]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            probe = subprocess.run(
                [
                    "docker",
                    "exec",
                    name,
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "wes_test",
                    "-d",
                    "wes_test",
                ],
                capture_output=True,
                timeout=5,
                check=False,
            )
            if not probe.returncode:
                yield f"postgresql+psycopg://wes_test:{password}@127.0.0.1:{port}/wes_test"
                return
            time.sleep(0.2)
        raise RuntimeError("Disposable PostgreSQL readiness timed out")
    finally:
        subprocess.run(
            ["docker", "rm", "--force", name],
            capture_output=True,
            timeout=10,
            check=False,
        )
