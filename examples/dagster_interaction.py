"""A bounded Dagster acceptance job; Dagster alone chooses what happens next."""

import time

import httpx
from dagster import Config, ConfigurableResource, job, op


class NotifyResource(ConfigurableResource["NotifyResource"]):
    base_url: str
    source: str
    token: str
    response_timeout_seconds: float = 30

    def client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=5,
            trust_env=False,
        )


class InteractionConfig(Config):
    request_id: str
    correlation_id: str
    response_deadline: str


@op
def request_action(config: InteractionConfig, notify: NotifyResource) -> dict[str, str]:
    identity = {
        "request_id": config.request_id,
        "correlation_id": config.correlation_id,
    }
    with notify.client() as client:
        accepted = client.post(
            "/v1/notifications",
            json={
                **identity,
                "source": notify.source,
                "notification_type": "action-required",
                "severity": "info",
                "title": "Workflow acceptance",
                "message": "Select the next workflow decision",
                "channels": ["telegram"],
                "response_actions": [
                    {"action": "continue", "label": "Continue"},
                    {"action": "cancel", "label": "Cancel"},
                ],
                "response_deadline": config.response_deadline,
            },
        )
    if accepted.status_code != 201:
        raise RuntimeError("Workflow notification was not accepted")
    return identity


@op
def observe_response(
    identity: dict[str, str], notify: NotifyResource
) -> dict[str, str]:
    deadline = time.monotonic() + notify.response_timeout_seconds
    with notify.client() as client:
        while time.monotonic() < deadline:
            result = client.get(f"/v1/notifications/{identity['request_id']}/responses")
            if result.status_code != 200:
                raise RuntimeError("Workflow response query failed")
            responses = result.json()
            if responses:
                response = responses[0]
                if (
                    response["request_id"] != identity["request_id"]
                    or response["correlation_id"] != identity["correlation_id"]
                ):
                    raise RuntimeError("Workflow response identity changed")
                return {
                    **identity,
                    "response_id": response["response_id"],
                    "action": response["selected_action"],
                }
            time.sleep(0.05)
    raise RuntimeError("Workflow response wait expired without a decision")


@op
def next_workflow_decision(response: dict[str, str]) -> dict[str, str]:
    # Domain interpretation belongs here, in the originating workflow.
    decisions = {"continue": "proceed", "cancel": "stop"}
    if response["action"] not in decisions:
        raise RuntimeError("Workflow received an undeclared action")
    return {**response, "decision": decisions[response["action"]]}


@job
def telegram_interaction() -> None:
    next_workflow_decision(observe_response(request_action()))
