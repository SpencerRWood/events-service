# Deployment and acceptance for Story #461

Application source, migrations and the candidate runtime gate live here.
Infrastructure owns the selected environment, image digest, PostgreSQL role,
Infisical resolution, Compose topology, Caddy ingress and deployment rollback.
Homelab retains ownership of edge Caddy and public ports 80/443. The existing
Infrastructure edge-route directory is the handoff between those owners; no
second application production Compose file is needed.

## Reviewed runtime inputs

The verified secret source is project `7ea10433-2eeb-4c57-95a9-b793dd40c7a4`,
environment `dev`, path `/events-service`. Do not copy resolved values into Git,
evidence, logs or Dagster run configuration. Separate protected host files provide
broker database/producer/subscription inputs and notify database/producer/policy/
Telegram inputs. Optional ntfy and SMTP keys are required only when enabled.

The existing `wood-notify` subscription resolves on the infrastructure proxy
network to `/v1/broker-events`. Its relay credential needs only
`notifications:consume`. Add a reviewed `WES_NOTIFICATION_POLICIES` JSON policy
that routes the desired broker event types to `telegram`; a subscription alone
does not route a notification when no event policy matches. For example, a
non-secret policy for acceptance events is:

```json
[{"name":"events-acceptance","event_types":["acceptance.telegram"],"channels":["telegram"]}]
```

The originating workflow needs a unique credential bound to its source with
`notifications:write` and `notifications:read`. Preserve existing producer tokens
and event-only scopes; provision this credential through the secret owner. Live
broker acceptance additionally needs source-bound event history/replay scopes.
Generic consumer subscription credentials also remain in Infisical.

## Release and promotion

The release job uses published `release-container.yml@v3`: it verifies a candidate
digest, runs `events_service.runtime_check` in that image with disposable
PostgreSQL, and then publishes image tags, the Git tag and GitHub Release. The
Telegram-only gate starts real services without any ntfy/SMTP settings. The
optional-adapter gate still tests ntfy retry and multi-provider delivery.

After initial bootstrap, `promote-container-to-dev.yml@v3` consumes that job's
release tag and digest outputs, validates the exact one-file Infrastructure pin
PR, waits for `infrastructure-validation`, and merges it. Infrastructure's existing
release/deploy workflow performs deployment, health verification and configuration
rollback. `INFRASTRUCTURE_PROMOTION_TOKEN` must be scoped to Infrastructure;
`EVENTS_SERVICE_PROMOTION_ENABLED=true` enables this downstream job.

The first Events pin is intentionally empty and the service is disabled. The
shared promoter requires an existing valid digest-qualified pin: bootstrap the
first **actual released artifact** in a reviewed Infrastructure change, enable
`services.events_service`, and only then enable automatic promotion. Do not use a
fake digest or promote an unvalidated local build. Production remains disabled;
its host, environment-specific machine identity and secret mapping need review.

## Post-promotion checks

`wood repo verify --json` owns both the disposable foundation gate and the required
`deployed-runtime` gate. An unavailable deployed runtime is a failed requirement,
even when local validation passes. The second gate reads the real schema, checks
both live/ready endpoints, probes unauthenticated HTTPS callback rejection, verifies
that event/notification/health APIs are not exposed publicly, and checks Telegram's
registered webhook. It never migrates, changes webhooks or sends alerts.

The runtime exposes host-loopback ports 18000 (broker) and 18001 (notify). Use the
deployment host or SSH forwarding from the reviewing machine, then provide:

```sh
export WES_VERIFY_BROKER_URL=http://127.0.0.1:18000
export WES_VERIFY_NOTIFY_URL=http://127.0.0.1:18001
export WES_VERIFY_CALLBACK_URL=https://dev-events-service.woodhost.cloud/v1/telegram/callbacks
infisical run --env=dev --path=/events-service -- wood repo verify --json
```

The callback hostname is a proposed Infrastructure route. DNS, certificate issuance,
public reachability and ingress configuration must be verified before registering:

```sh
infisical run --env=dev --path=/events-service -- uv run python -m scripts.telegram_webhook \
  --url https://dev-events-service.woodhost.cloud/v1/telegram/callbacks
# After review and verified public HTTPS exposure, apply the same command with --apply.
```

The helper preserves pending updates, supplies the secret through the request body,
accepts only callback queries, and verifies the registered URL and update types.
It omits token, chat ID, signing secret and provider response details from output.
Registration alone is not proof that Telegram can deliver callbacks.

## Originating Dagster contract

`examples/dagster_interaction.py` belongs to the originating workflow, outside the
installed service package and image. It uses a Dagster resource with an
`EnvVar("WES_WORKFLOW_TOKEN")` credential reference; never put the resolved token
into run configuration. Bind the source and private notify URL at the caller.
Supply stable request/correlation IDs and a fixed response deadline in persisted
run configuration so submission retries keep the same payload.

The bounded acceptance job submits a direct Telegram action request, observes its
normalized durable response through the source-scoped API, and selects `proceed`
or `stop` in a separate Dagster op. Its output is request/correlation/response IDs,
the generic selected action and the workflow decision. Timeout fails the run and
does not imply an action. Production waits longer than this bounded acceptance
example should use the originating workflow's durable sensor/resumption design.

Repository tests execute the actual Dagster job against a real notify subprocess
and disposable PostgreSQL, using an isolated Telegram HTTP server and authenticated
signed callbacks. Both Continue and Cancel paths preserve durable response identity.
These tests do not claim a live human action, public callback or deployed Dagster run.

## Acceptance evidence required after review

| Criterion | Implementation/check | Delivery evidence still required |
| --- | --- | --- |
| Platform deployment and secrets | Infrastructure role, manifests, protected resolver, private networks | Selected released digest, successful deployment, correct secret mapping |
| Migrations and health | Serialized transactional Alembic lock; live/ready checks | Deployed image identity and post-promotion verification record |
| Candidate before release and promotion | Shared v3 runtime gate and downstream exact pin job | Exact release run/attempt, candidate digest, publication and promotion PR |
| Rollback/recovery | Existing platform configuration rollback; schema is never blindly downgraded | Exercise recovery using the previous compatible released pin and health check |
| Machine consumer and Telegram | Existing broker runtime gate plus optional and Telegram-only notify gates | Live producer → broker → durable generic consumer and live producer → broker → notify → Telegram |
| Dagster action/response/decision | Real Dagster acceptance job and both-action integration tests | Live Telegram action, durable normalized response, originating Dagster run/decision IDs |
| Correlation and failures | Broker/notify lifecycle tests: retry, terminal states, replay, deduplication, expiration and restart | Correlated live IDs and documented recovery observations |
| Operational boundaries | This document and Infrastructure deployment guide | Public route and webhook delivery verification |

Keep one correlation ID across each event/derived notification/delivery/response/
workflow chain. Save IDs and normalized states rather than message content or
credentials. Keep local tests, candidate tests, live acceptance, deployed health
and rollback observations distinct; one cannot substitute for another.
