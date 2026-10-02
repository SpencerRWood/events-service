# Wood Events Service

R1 foundation for internal event and human interaction transport. This repository
contains two independently runnable FastAPI applications and shared typed v1
contracts, PostgreSQL persistence, Alembic migrations and scoped producer auth.
The approved template baseline uses Python 3.14, uv and Hatchling.

## Service boundaries

`wood-broker` accepts `POST /v1/events`; `wood-notify` accepts
`POST /v1/notifications`. Each exposes `/health/live` and `/health/ready`.
Readiness checks PostgreSQL connectivity and the required migration revision.
Both validate configuration and schema at startup; they never create tables
implicitly. A single container image runs either application by its command.

Routing, webhooks, provider delivery, suppression, response capture and queries
belong to subsequent R1 Stories. This foundation defines their generic contracts
and durable tables. It does not execute workflow actions or send notifications.
Dagster and originating services own orchestration. Infrastructure owns production
deployment, external PostgreSQL, secret injection, promotion and rollback.

## Runtime configuration and secret references

Inject `WES_DATABASE_URL` (`postgresql+psycopg://…`) and
`WES_PRODUCER_CREDENTIALS` from the established Infisical runtime context.
There is no filesystem dotenv loading and no default producer credential.
The credential variable is a JSON array with entries shaped as:

```json
[{"source":"producer-name","token":"runtime-injected-token-of-at-least-32-characters","scopes":["events:write"]}]
```

Scopes are `events:write` and `notifications:write`. Every credential is bound to
its declared source; a request cannot impersonate another source. Authentication
uses the replaceable `ServiceAuth` interface and a Bearer header. Tokens must be
unique. Inject only the credentials required by each independently deployed
service. APIs remain private; local Compose binds host ports to loopback.

`WES_SECRET_REFERENCES` is a separate optional JSON map from variable names to
operator-verified secret references. It describes locations; it does not fetch
credentials. Locations must come from the canonical environment's secret mapping;
this repository does not invent Infisical paths. OpenProject credentials use Wood
Tools' existing Infisical context and are not application secrets.

Future provider configuration names are `NTFY_BASE_URL`, `TELEGRAM_BOT_TOKEN`,
`SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME` and `SMTP_PASSWORD`; they are reserved
for the provider Story and have no active adapters here. Provider secret locations
remain operator-owned and must be verified before that integration.

Non-secret settings use the `WES_` prefix: `EVENT_RETENTION_DAYS`,
`NOTIFICATION_RETENTION_DAYS`, `DELIVERY_RETENTION_DAYS`, `RESPONSE_RETENTION_DAYS`
(90 by default, 1–3650 allowed), and `DATABASE_TIMEOUT_SECONDS` (5 by default).
The example environment file declares names without credential values.

Credential-like fields in arbitrary JSON and known injected secrets anywhere in
payloads are rejected before persistence. Errors omit submitted input and database
details. Structured acceptance logs include stable IDs and correlation IDs without
payloads. Secrets are omitted from settings dumps and redacted in diagnostics.
Producers must still exclude unknown third-party secrets from free text; no content
classifier can establish that arbitrary text is safe to store.

## v1 contracts and persistence

See `src/wood_events_service/contracts.py` and each application's `/openapi.json`.
Events require type, source, severity, timezone-aware occurrence time,
correlation ID and structured data. Notifications require title, message,
notification type, correlation ID and requested channels or a policy key.
`action-required` requests require unique generic response actions. The normalized
response contract carries stable response/request IDs, provider response identity,
the selected action, a timezone-aware receipt time and correlation/causation fields.

Send an explicit `event_id` or `request_id` for retries, or supply an
`idempotency_key`. IDs are otherwise generated on acceptance. The key is scoped
to source within each service, and remains reserved until retention removes the
record. A duplicate with the same normalized payload returns the original receipt
(`201`, `duplicate=true`); a conflicting payload or conflated identities returns
`409`. When retrying by key with a newly generated ID, only that generated ID is
ignored in comparison. Accepted data commits before the receipt is returned.

PostgreSQL tables are `broker_events`, `notify_requests`,
`broker_delivery_attempts`, `notify_delivery_attempts` and `notify_responses`.
Immutable parent and response records and append-only attempt evidence are guarded
by database triggers. Child correlation must match its parent. Provider response
identity is unique. Delivery records store normalized outcome/error codes, not
provider configuration or credential-bearing responses.

Run migrations separately before starting either application:

```sh
uv run python -m wood_events_service.migrate
uv run uvicorn wood_events_service.broker:app --host 127.0.0.1 --port 8000 --no-access-log
uv run uvicorn wood_events_service.notify:app --host 127.0.0.1 --port 8001 --no-access-log
```

`retention.cleanup(engine, settings, as_of=<aware datetime>)` provides deterministic
transactional cleanup for an externally scheduled maintenance task. It removes old
children first and preserves parents that still have retained children. It enables
the transaction-local deletion exception to the immutable-record trigger; ordinary
updates/deletes fail. PostgreSQL credentials remain privileged infrastructure
configuration, never producer input. Retention cannot choose a workflow outcome.

## Local development and verification

```sh
uv sync --frozen --group dev
wood repo validate --json
wood repo verify --json
uv build
```

Tests start unique disposable PostgreSQL containers and clean them up. Docker is
required; unavailable dependencies fail clearly rather than silently skipping
persistence tests. Repository verification migrates a disposable database and runs
both real service processes, proving authentication, correlation and durable retry
after restart. It is safe to retry and never uses the production database.

Local Compose includes PostgreSQL, a one-shot migration container, broker on
loopback port 8000 and notify on loopback port 8001. Inject scoped credentials and
optionally a URL-safe `WES_LOCAL_POSTGRES_PASSWORD`, then run
`docker compose up --build`. Its named volume preserves local data. The local-only
default database password is public development configuration. Empty producer
credentials fail service startup; Compose configuration validation needs no secrets.
Production PostgreSQL need not be bundled with the applications.

## Centralized release contract

The consumer directly uses `release-container.yml@v3` and `.github/release.toml`.
Its additive `[runtime]` contract selects `wood_events_service.runtime_check`.
The shared gate supplies temporary PostgreSQL via `RUNTIME_DATABASE_URL` and runs
this module in the exact verified candidate digest. The module proves migrations,
both startup paths, authenticated acceptance, correlation, credential exclusion and
durability across process restarts. Failure or timeout blocks release image tags,
Git tagging and GitHub Release publication. The private candidate tag is built
before it can be tested. There is no copied promotion wiring in this Story.

The shared workflows extension must be reviewed and published to v3 before the
consumer is merged. Local validation demonstrates the new contract but does not
prove that the currently published v3 supports it. CI, release digest and delivered
runtime evidence are verified separately through Wood Tools after reviewed delivery.
