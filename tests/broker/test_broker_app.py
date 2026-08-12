from fastapi.testclient import TestClient

from services.broker.app.main import create_app


def test_broker_health_endpoint() -> None:
    client = TestClient(create_app())

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"service": "wood-broker", "status": "ok"}
