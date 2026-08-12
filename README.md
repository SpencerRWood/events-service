# Wood Events Service

This repository contains the V0.1 foundation for:

- `wood-broker`
- `wood-notify`
- bundled ntfy runtime infrastructure

`wood-broker` and `wood-notify` are independently runnable FastAPI services. The
root Compose topology wires both services to a bundled ntfy container for local
and production stack deployment, while keeping ntfy replaceable through
configuration.

Application behavior beyond foundation wiring is intentionally deferred to later
V0.1 stories. This story does not implement event ingestion, persistence,
routing, notification policy, provider delivery, retries, or database
migrations.

## Services

- `wood-broker`: future machine-to-machine event ingestion and distribution.
- `wood-notify`: future machine-to-human notification policy and delivery.

## Local Checks

```bash
python -m pytest
```

## Runtime

The services can be started independently:

```bash
uvicorn services.broker.app.main:app --host 0.0.0.0 --port 8000
uvicorn services.notify.app.main:app --host 0.0.0.0 --port 8000
```

Or together with bundled ntfy:

```bash
docker compose up --build
```
