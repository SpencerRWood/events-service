from fastapi.testclient import TestClient

from services.notify.app.main import create_app


def test_notify_health_endpoint() -> None:
    client = TestClient(create_app())

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"service": "wood-notify", "status": "ok"}
