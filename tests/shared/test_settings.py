import pytest
from pydantic import ValidationError

from shared.config import BrokerSettings, ExternalInfrastructureSettings, NotifySettings, NtfySettings


def test_broker_database_url_is_required(monkeypatch) -> None:
    monkeypatch.delenv("BROKER_DATABASE_URL", raising=False)

    with pytest.raises(ValidationError):
        BrokerSettings()


def test_notify_database_url_is_required(monkeypatch) -> None:
    monkeypatch.delenv("NOTIFY_DATABASE_URL", raising=False)

    with pytest.raises(ValidationError):
        NotifySettings()


def test_ntfy_base_url_is_configurable(monkeypatch) -> None:
    monkeypatch.setenv("NTFY_BASE_URL", "https://ntfy.example.test")

    settings = NtfySettings()

    assert str(settings.base_url) == "https://ntfy.example.test/"


def test_external_database_url_defaults_to_absent() -> None:
    settings = ExternalInfrastructureSettings()

    assert settings.database_url is None
