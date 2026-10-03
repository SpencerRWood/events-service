"""Operational checks exercise real schema state without mutating a live runtime."""

import json
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from scripts.telegram_webhook import register
from scripts.verify_deployment import verify
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url

from events_service.config import Settings
from events_service.migrate import migrate

CALLBACK = "https://dev-events-service.woodhost.cloud/v1/telegram/callbacks"


def telegram_settings(settings: Settings) -> Settings:
    return settings.model_copy(
        update={
            "telegram_bot_token": SecretStr("operations-test-bot"),
            "telegram_chat_id": -123,
            "telegram_webhook_secret": SecretStr("s" * 32),
        }
    )


def test_registration_previews_and_verifies_without_exposing_values(
    settings: Settings,
) -> None:
    configured = telegram_settings(settings)
    calls: list[str] = []

    def provider(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path.rsplit("/", 1)[-1])
        if request.url.path.endswith("setWebhook"):
            body = json.loads(request.content)
            assert body["allowed_updates"] == ["callback_query"]
            assert body["secret_token"] == "s" * 32
            assert not body["drop_pending_updates"]
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(
            200,
            json={
                "ok": True,
                "result": {
                    "url": CALLBACK,
                    "allowed_updates": ["callback_query"],
                    "pending_update_count": 0,
                },
            },
        )

    transport = httpx.MockTransport(provider)
    assert register(configured, CALLBACK, transport=transport)["apply"] is False
    assert not calls
    result = register(configured, CALLBACK, apply=True, transport=transport)
    assert result["registered"]
    assert calls == ["setWebhook", "getWebhookInfo"]
    assert "operations-test-bot" not in json.dumps(result)
    assert "s" * 32 not in json.dumps(result)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/v1/telegram/callbacks",
        "https://user:password@example.com/v1/telegram/callbacks",
        "https://example.com/v1/telegram/callbacks?token=x",
        "https://example.com:8443/v1/telegram/callbacks",
        "https://example.com/v1/events",
    ],
)
def test_registration_rejects_unreviewable_urls(settings: Settings, url: str) -> None:
    with pytest.raises(ValueError, match="HTTPS callback"):
        register(telegram_settings(settings), url)


@pytest.mark.parametrize("failure", [None, "health", "callback", "api", "webhook"])
def test_deployment_verification_fails_closed(
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    failure: str | None,
) -> None:
    monkeypatch.setenv("WES_VERIFY_BROKER_URL", "http://broker")
    monkeypatch.setenv("WES_VERIFY_NOTIFY_URL", "http://notify")
    monkeypatch.setenv("WES_VERIFY_CALLBACK_URL", CALLBACK)

    def runtime(request: httpx.Request) -> httpx.Response:
        if request.url.host in {"broker", "notify"}:
            return httpx.Response(
                503 if failure == "health" else 200,
                json={
                    "status": "ready" if request.url.path.endswith("ready") else "ok",
                    "service": f"wood-{request.url.host}",
                },
            )
        if request.url.path.endswith("getWebhookInfo"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {
                        "url": "" if failure == "webhook" else CALLBACK,
                        "allowed_updates": ["callback_query"],
                    },
                },
            )
        if request.method == "POST":
            return httpx.Response(200 if failure == "callback" else 401)
        return httpx.Response(200 if failure == "api" else 404)

    if failure:
        with pytest.raises(RuntimeError):
            verify(telegram_settings(settings), transport=httpx.MockTransport(runtime))
    else:
        result = verify(
            telegram_settings(settings), transport=httpx.MockTransport(runtime)
        )
        assert result["state"] == "passed"
        assert result["limitations"]


def test_concurrent_migrations_serialize_on_a_fresh_schema(
    engine: Engine,
    database_url: str,
) -> None:
    schema = "migration_" + uuid4().hex
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA {schema}"))
    url = (
        make_url(database_url)
        .update_query_dict(
            {
                "options": f"-c search_path={schema}",
            }
        )
        .render_as_string(hide_password=False)
    )
    active = create_engine(url, hide_parameters=True)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(migrate, [url, url]))
        with active.connect() as connection:
            assert (
                connection.scalar(text("SELECT version_num FROM alembic_version"))
                == "0003_notify_lifecycle"
            )
    finally:
        active.dispose()
        with engine.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
