from fastapi.testclient import TestClient

from services.notify.app.main import create_app
from shared.config import NotifySettings, NtfySettings


def test_notify_health_endpoint(tmp_path) -> None:
    client = TestClient(
        create_app(
            NotifySettings(database_url=f"sqlite:///{tmp_path / 'notify.db'}"),
            NtfySettings(base_url="http://ntfy:80"),
        )
    )

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"service": "wood-notify", "status": "ok"}
