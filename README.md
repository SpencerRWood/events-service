# Events Service

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

The broker routes accepted events through durable webhook jobs. Notify applies
declarative policy to direct requests and broker events, delivers through ntfy,
Telegram and SMTP adapters, suppresses repeated conditions, and captures generic
Telegram responses. Both expose scoped history and retry APIs. Neither service
executes workflow actions.
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

Scopes are `events:write`, `events:read`, `events:replay`, `notifications:write`,
`notifications:read`, `notifications:retry` and `notifications:consume`.
Producer and history credentials are bound to their declared source. The
`notifications:consume` scope is reserved for a trusted broker relay, which carries
the original producer source in its event envelope. Authentication
uses the replaceable `ServiceAuth` interface and a Bearer header. Tokens must be
unique. Inject only the credentials required by each independently deployed
service. APIs remain private; local Compose binds host ports to loopback.

`WES_SECRET_REFERENCES` is a separate optional JSON map from variable names to
operator-verified secret references. It describes locations; it does not fetch
credentials. Locations must come from the canonical environment's secret mapping;
this repository does not invent Infisical paths. OpenProject credentials use Wood
Tools' existing Infisical context and are not application secrets.

The checked-in `.infisical.json` selects project
`7ea10433-2eeb-4c57-95a9-b793dd40c7a4` on
`https://dev-infisical.woodhost.cloud`. The verified development folder is
`/events-service`. Populate values in environment `dev`; the example file
contains names only. Optional empty provider variables are ignored. Launch with:

```sh
infisical run --env=dev --path=/events-service -- uv run python -m events_service.migrate
bash scripts/run_service.sh dev notify
bash scripts/run_service.sh dev broker
```

This local launcher injects secrets; it does not create production topology,
register a Telegram webhook or promote a release. Other environments require their
own operator-verified mapping. Production instances should receive only their own
scoped credentials and provider secrets.

Development persistence uses Infrastructure's `events_service` database and
dedicated `events_service` role at the restricted LAN endpoint
`192.168.1.21:25433`. Infrastructure's Ansible PostgreSQL role owns provisioning
and database isolation. The role can migrate and access its own schema; it
cannot create roles or databases or connect to other databases. The canonical
credential remains `WES_DATABASE_URL` in `dev /events-service`; do not create
separate application password, username, or hostname secrets.

The initial `dev /events-service` credentials give `infrastructure`, `homelab`,
`rag-service`, `openproject-reports`, `wood-reports` and `workflows` exactly
`events:write` and `notifications:write`. The independent `wood-notify` relay has exactly
`notifications:consume`. All seven tokens are unique and at least 48 characters.
The secret `WES_SUBSCRIPTIONS` value contains one broad `wood-notify` subscription
to `http://wood-notify:8000/v1/broker-events`, with the same relay token. Local
Compose provides the `wood-notify` network alias; infrastructure must provide that
internal name when deploying the services. No Events production topology is
defined yet. Keep both JSON values entirely in Infisical, including the subscription
authentication token. Database and provider configuration must come from their
infrastructure or provider owners before enabling the corresponding runtime.

| Name | Value and purpose |
| --- | --- |
| `WES_DATABASE_URL` | Secret PostgreSQL connection URL using `postgresql+psycopg`. |
| `WES_PRODUCER_CREDENTIALS` | Secret JSON array of source, token and scopes; tokens are unique and at least 32 characters. |
| `WES_SUBSCRIPTIONS` | Broker routing JSON; optional `auth_token` is a runtime-only secret for an authenticated consumer. |
| `NTFY_BASE_URL`, `NTFY_TOPIC` | Both required to enable ntfy; HTTP(S) endpoint and topic. |
| `NTFY_AUTH_TOKEN` | Optional secret ntfy Bearer token. |
| `TELEGRAM_BOT_TOKEN` | Secret Bot API token; enables the Telegram adapter. |
| `TELEGRAM_CHAT_ID` | Numeric destination chat ID, including negative group IDs. |
| `TELEGRAM_WEBHOOK_SECRET` | Secret, generated 32–256 character value containing letters, digits, `_` or `-`; authenticates webhooks and signs callback data. |
| `TELEGRAM_ALLOWED_USER_IDS` | Optional JSON array of numeric user IDs; empty permits any responder in the configured chat. |
| `TELEGRAM_API_BASE_URL` | Defaults to `https://api.telegram.org`; override only for an operator-controlled Bot API service or local test substitute. |
| `SMTP_HOST`, `SMTP_PORT` | Server and port; port defaults to 587. |
| `SMTP_USERNAME`, `SMTP_PASSWORD` | Optional paired secret SMTP login values. |
| `SMTP_SENDER`, `SMTP_RECIPIENTS` | Sender address and JSON recipient array, required when SMTP is enabled. |
| `SMTP_TLS` | `starttls` (default) or `ssl`; certificate verification is enabled. |

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

See `src/events_service/contracts.py` and each application's `/openapi.json`.
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
`broker_delivery_jobs`, `broker_delivery_attempts`, `notify_delivery_attempts`
and `notify_responses`.
Immutable parent and response records and append-only attempt evidence are guarded
by database triggers. Child correlation must match its parent. Provider response
identity is unique. Delivery records store normalized outcome/error codes, not
provider configuration or credential-bearing responses.

Run migrations separately before starting either application:

```sh
uv run python -m events_service.migrate
uv run uvicorn events_service.broker:app --host 127.0.0.1 --port 8000 --no-access-log
uv run uvicorn events_service.notify:app --host 127.0.0.1 --port 8001 --no-access-log
```

`retention.cleanup(engine, settings, as_of=<aware datetime>)` provides deterministic
transactional cleanup for an externally scheduled maintenance task. It removes old
children first and preserves parents that still have retained children. It enables
the transaction-local deletion exception to the immutable-record trigger; ordinary
updates/deletes fail. PostgreSQL credentials remain privileged infrastructure
configuration, never producer input. Retention cannot choose a workflow outcome.

## Broker routing, delivery and history

Set `WES_SUBSCRIPTIONS` to an operator-controlled JSON array:

```json
[{"consumer":"operations","url":"http://consumer:8080/events","event_types":["job.failed"],"sources":["scheduler"],"severities":["error","critical"],"data_equals":{"environment":"dev"}}]
```

Each consumer has one unique name and URL. Empty predicates match all events;
nonempty predicates are combined with AND. Values within each type/source/severity
set are alternatives. `data_equals` compares exact top-level structured data
values. Multiple matching subscriptions produce independent jobs. Only accepted
new events are routed; duplicates and later configuration edits do not add jobs
to existing events. An event with no matches is still durably queryable.

The acceptance transaction commits the immutable event and all its jobs together.
The broker's lifespan worker resumes pending jobs after restart. PostgreSQL row
locks with `SKIP LOCKED` prevent concurrent workers from claiming the same job;
each attempt runs inside its scheduling transaction. URLs remain in runtime
configuration, and jobs persist consumer names only. A removed consumer records a
terminal `consumer-unconfigured` failure. Existing pending jobs use the current URL
for their consumer name, so retain stable consumer identities when changing config.

Webhooks POST the original v1 event JSON with `X-Event-ID`, `X-Correlation-ID` and
`Idempotency-Key: <event_id>:<consumer>`. Timeout defaults to 5 seconds per HTTP
phase. Redirects are terminal; response bodies, credentials and exception details
are never retained. URLs must be HTTP(S) and contain no userinfo, query or fragment.
Delivery uses no environment proxy. An optional subscription `auth_token` adds
Bearer authentication, stays out of persisted jobs/settings dumps, and is included
in the credential redaction policy. Configure trusted
internal consumer URLs; payloads cannot choose destinations.

2xx succeeds; network/timeouts, 408, 425, 429 and 5xx retry. Other statuses fail
terminally. `WES_WEBHOOK_MAX_ATTEMPTS` defaults to 5 (1–20). Exponential delays
start at `WES_WEBHOOK_BACKOFF_SECONDS` (1) and cap at
`WES_WEBHOOK_MAX_BACKOFF_SECONDS` (60). The worker polls at
`WES_BROKER_POLL_SECONDS` (1). Exhaustion sets the job to terminal failure while
preserving the final transient attempt classification. Attempts append evidence;
another consumer's success survives any failure.

Delivery is at-least-once: a crash after a consumer accepts but before the database
commit can resend the same event. Consumers must durably deduplicate the stable
idempotency header to avoid duplicate effects. The broker prevents duplicate jobs
on producer retries; it cannot atomically commit a remote consumer's side effect.

Authenticated, source-restricted endpoints:

- `GET /v1/events?correlation_id=<uuid>&limit=50&offset=0` lists committed events.
- `GET /v1/events/<event_id>` returns the original event and per-consumer state.
- `GET /v1/events/<event_id>/attempts?limit=50&offset=0` returns immutable attempts.
- `POST /v1/events/<event_id>/deliveries/<consumer>/replay` queues another bounded
  cycle for an existing finished job, preserving IDs and prior evidence.

Reads require `events:read`; replay requires `events:replay`. Events belonging to
another source return 404. Lists allow 1–100 rows and offset up to 100000. Replay
returns 202, or 409 if already pending or the consumer is unconfigured. A replay
increments job generation and resets its cycle count; prior attempts remain.
Retention preserves events with pending jobs. Finished jobs expire after the
delivery retention window, after which they are no longer replayable.
There is no bulk log ingestion endpoint.

## Notification lifecycle

`POST /v1/notifications` accepts the v1 direct notification contract with
`notifications:write`. `POST /v1/broker-events` accepts an original v1 event using
the trusted `notifications:consume` relay scope. Broker-derived request IDs are
deterministic per source/event ID and retries preserve correlation and causation.
The immutable request and all scheduling state commit before any provider call.

Example `WES_NOTIFICATION_POLICIES`:

```json
[{"name":"failures","sources":["scheduler"],"event_types":["job.failed"],"severities":["error","critical"],"channels":["ntfy","telegram"]},{"name":"interactive","notification_types":["action-required"],"policy_keys":["approval-request"],"channels":["telegram"]}]
```

Policy dimensions are ANDed; values inside each set are alternatives. Requested
channels intersect matching policy channels. With no policies, explicitly requested
channels are used; a policy-key-only request or unmatched event is durably marked
suppressed. Broker messages render event type as title and structured event data as
message. Policy does not interpret domain-specific workflow fields. Interactive
requests with routed channels require Telegram.

Point a broker subscription at notify's `/v1/broker-events` and supply a relay
token that is present in notify's credential array with `notifications:consume`.
The subscriber JSON contains `consumer`, `url`, matching predicates and optional
`auth_token`; store that entire JSON value in Infisical when it contains a token.
Only the event identity and consumer name are persisted by the broker.

`WES_SUPPRESSION_SECONDS` defaults to 60; 0 disables suppression. A supplied
`suppression_key` groups conditions by source. Otherwise equivalent source, type,
severity, title, message, channels, policy key and actions form a fingerprint,
ignoring correlation and generated identities. A database-locked window makes
concurrent duplicates safe. Suppressed requests and attempt evidence remain
queryable; they never trigger delivery.

Each channel has independent pending/delivered/suppressed/transient-failure/
terminal-failure/expired scheduling state.
Workers serialize channels within one request while processing separate requests
concurrently, preserving consistent response/retry state.
Transient HTTP/network and SMTP 4xx failures use bounded exponential retry.
Controls are `WES_PROVIDER_TIMEOUT_SECONDS`
(5), `WES_NOTIFICATION_MAX_ATTEMPTS` (5), `WES_NOTIFICATION_BACKOFF_SECONDS` (1),
`WES_NOTIFICATION_MAX_BACKOFF_SECONDS` (60) and `WES_NOTIFICATION_POLL_SECONDS` (1).
Provider bodies and exception text are never persisted. A partially successful
multi-recipient SMTP send is terminal to avoid automatically resending to successful
recipients; history exposes its normalized failure code.

Notification delivery is at-least-once. Telegram/ntfy/SMTP do not share the broker's
consumer deduplication contract, so a process crash after provider acceptance and
before the scheduling commit can duplicate a message. SMTP Message-ID stays stable;
the service does not claim exactly-once remote delivery.

Telegram uses signed, compact callback data referencing the request and declared
action index. Configure the Bot API webhook URL as notify's
`/v1/telegram/callbacks`, with `secret_token` equal to `TELEGRAM_WEBHOOK_SECRET`,
and `allowed_updates=["callback_query"]`. Infrastructure owns public HTTPS exposure
and webhook registration. The endpoint validates the secret header, signature,
configured chat, optional responder allowlist, persisted message reference and
declared action. First valid response is captured; an exact callback replay returns
the original response. Different subsequent callbacks conflict. Normalized responses
preserve request/correlation/causation identity; capture executes no domain action.
Rotating the webhook/signing secret invalidates existing keyboards.

The originating request supplies its response deadline. The worker and history
queries record expiration deterministically without selecting an outcome. Expired
requests reject new responses; previously accepted callback duplicates remain
idempotent. Awaiting responses and pending delivery jobs pin retained parent/message
records until expiration or response capture.

Source-restricted `notifications:read` endpoints are:

- `GET /v1/notifications?correlation_id=<uuid>&limit=50&offset=0`
- `GET /v1/notifications/<request_id>`
- `GET /v1/notifications/<request_id>/attempts`
- `GET /v1/notifications/<request_id>/responses`

Lists allow 1–100 rows; attempt/response endpoints also support offset pagination.
`POST /v1/notifications/<request_id>/channels/<channel>/retry` requires
`notifications:retry` and starts a new bounded cycle for a failed active channel.
It preserves the request and prior attempts. Suppressed, delivered, expired or
responded requests cannot be resent through this endpoint.

Protocol references: [ntfy JSON publishing](https://docs.ntfy.sh/publish/#publish-as-json)
and [Telegram Bot API](https://core.telegram.org/bots/api).

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
after restart. It also runs a real broker with two local HTTP consumers, transient
retry, delivery history and replay across process restart. It is safe to retry
and never uses the production database. Verification strips inherited application
and provider variables, substitutes local HTTP providers, and proves authenticated
broker-to-notify delivery, transient retry, direct action buttons, normalized
callbacks and callback replay after process restart. It sends no live alerts.

Local Compose includes PostgreSQL, a one-shot migration container, broker on
loopback port 8000 and notify on loopback port 8001. Inject scoped credentials and
optionally a URL-safe `WES_LOCAL_POSTGRES_PASSWORD`, then run
`docker compose up --build`. Its named volume preserves local data. The local-only
default database password is public development configuration. Empty producer
credentials fail service startup; Compose configuration validation needs no secrets.
Production PostgreSQL need not be bundled with the applications.
An optional `notifications` Compose profile bundles ntfy for local use on loopback
port 8080. Set `NTFY_BASE_URL=http://ntfy` and a topic, then use
`docker compose --profile notifications up --build`. Endpoint configuration remains
environment-supplied and the adapter can be replaced independently.

## Centralized release contract

The consumer directly uses `release-container.yml@v3` and `.github/release.toml`.
Its additive `[runtime]` contract selects `events_service.runtime_check`.
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
