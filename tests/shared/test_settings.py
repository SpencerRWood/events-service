from shared.config import ExternalInfrastructureSettings, NtfySettings


def test_ntfy_base_url_is_configurable(monkeypatch) -> None:
    monkeypatch.setenv("NTFY_BASE_URL", "https://ntfy.example.test")

    settings = NtfySettings()

    assert str(settings.base_url) == "https://ntfy.example.test/"


def test_external_database_url_defaults_to_absent() -> None:
    settings = ExternalInfrastructureSettings()

    assert settings.database_url is None
