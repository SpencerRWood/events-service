from fastapi.testclient import TestClient

from services.broker.app.main import create_app
from shared.config import BrokerSettings


def test_broker_health_endpoint(tmp_path) -> None:
    client = TestClient(
        create_app(BrokerSettings(database_url=f"sqlite:///{tmp_path / 'broker.db'}"))
    )

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"service": "wood-broker", "status": "ok"}
