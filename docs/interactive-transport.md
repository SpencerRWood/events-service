# S04 — source-owned interactive Telegram transport

This extends the existing v1 Events notification API for
[OP-576 / DA-R1-S04](https://projects.woodhost.cloud/work_packages/576).
Events transports decisions and receipts. Documentation Automation owns proposal
identity, eligibility, the absolute 72-hour approval deadline, rejection history,
and GitHub merge authority. No GitHub credential, merge endpoint, new Telegram bot
framework, schedule activation or live notification is introduced here.

## Request contract

The source-bound `notifications:write` credential posts the existing
`POST /v1/notifications` request, with these optional additions:

```json
{
  "response_actions": [
    {"action": "approve", "label": "Approve & Merge"},
    {"action": "reject", "label": "Reject"}
  ],
  "url_buttons": [
    {"label": "View PR", "url": "https://github.com/example/handbook/pull/1"}
  ],
  "responder_policy": {"user_ids": [42], "private_chat_id": 42},
  "response_deadline": "2026-11-01T12:00:00Z"
}
```

This fragment accompanies the existing request/source/correlation IDs, title,
message, notification type, severity and channels. IDs above are fixtures, not
onboarding values. S05 freezes proposal ID, digest and exact head binding in its
own ledger and immutable request context. A new proposal head requires a new
request ID. Changing an existing request's policy or payload is a conflict.

Callback actions retain their signed indexes; URL buttons are separate HTTPS
links, contain no callback data and cannot consume a response. URLs reject user
credentials. Existing consumers can omit both additions. Persisted pre-S04
requests retain their idempotency through typed default normalization.

Requests with a responder policy require actions, a deadline, Telegram as their
only channel, and a private chat matching the configured destination. Requested
users must remain within any configured global allowlist. The per-request policy
never widens that global policy or changes credentials. An authenticated Telegram
webhook must carry the matching stored message ID, valid callback signature,
allowed user ID and `chat.type = private`; the sender's ID must equal the private
chat ID. Missing provenance is rejected before consuming the response. Generic
existing consumers retain their existing global responder behavior.

The existing source-scoped responses endpoint adds `telegram_provenance` to new
responses: authenticated webhook/signature flags, Telegram user/chat/message and
update IDs, reported chat type, and whether a request-specific policy was verified.
Absence of a verified request-specific policy does not establish owner approval.
Application code must validate these fields alongside request/correlation/action,
provider response ID and time. Duplicate callback IDs return the original durable
response; new presses on answered or terminal notifications cannot create another
decision. Unauthorized presses cannot consume the owner's valid response.

## Durable terminal message updates

`POST /v1/notifications/{request_id}/edits` requires `notifications:write` for the
same source that owns the request:

```json
{
  "edit_id": "5b892762-4459-4ceb-aa38-9c49c77d48fe",
  "outcome": "merged",
  "message": "Verified handbook proposal merged."
}
```

Allowed outcomes are `merged`, `rejected`, `expired` and `blocked`. The transport
does not verify the application's business outcome. S05 must verify that outcome
before sending the receipt. Events prefixes the outcome and removes every inline
button, including View PR. These terminal edits cannot reopen approval controls.

The API resolves exactly one confirmed original Telegram message from durable
storage. A caller cannot supply a chat/message target. Missing, ambiguous or
multipart originals are rejected. The immutable edit ID, request and content are
insert-or-reuse; changed content or a second terminal outcome returns 409. A unique
database constraint plus the response-state row lock serializes edit submission
and callback consumption. Terminal submission closes any unanswered response gate
before provider I/O, including when receipt delivery later fails.

`GET /v1/notifications/{request_id}/edits` requires source-scoped
`notifications:read` and reports payload, status, attempts, bounded error code and
update time. States are pending, dispatching, uncertain, delivered and
terminal-failure. The original stored message reference is never derived from
current runtime destination configuration. The worker commits dispatch intent and
a lease before provider I/O. A dead lease or ambiguous HTTP result can retry only
the exact same frozen terminal text and empty keyboard. Telegram's explicit
“message is not modified” response confirms successful idempotent recovery.
Retries are bounded by the existing provider timeout, attempts and backoff settings.
Exhausted or terminal failures retain their message binding for diagnosis; they
do not invent delivery success or authorize another business action.

Per-request approval prompts also commit send intent before I/O. Timeouts, lost
send responses, server errors and interrupted sends remain uncertain and cannot
be blindly retried; they require owner/provider diagnosis. A definite 429 rejection
can use the existing bounded retry. Verified message IDs are stored only after
the response confirms the configured private chat. Expiry automatically queues a
terminal edit when a confirmed message exists, including expiry during an in-flight
send. No original message means no fabricated edit target. Pending/uncertain edits
and interrupted prompt deliveries survive retention; confirmed terminal edits
follow existing retention cutoffs.

## Planning and verification

The user approved Events Service's scope rollover from completed Events R1
(Stories 458–461) to Documentation Automation initiative 571, planning release 29,
epic 572 in the Codex conversation on 2026-10-27. Release 29 was verified open;
the fetched Story base is `52855203dcffa68b289421d8d307f1acd2a870c8` on
`development`, which is also the intended PR target. Repository version calculation
remains owned by semantic-release; planning R1 is not a requested SemVer. The
mapping change accompanies this explicitly authorized Story and is distinct from
release publication, provisioning and deployment.

Migration 0006 adds terminal-edit storage and a closed response state without
rewriting existing request payloads or response evidence. Fake Telegram plus
disposable PostgreSQL tests cover URL/callback separation, responder/private-chat
checks, replay, source isolation, terminal edits, crash and lost-HTTP recovery,
expiry, bounded retries, retention and existing-consumer regression. Run
`wood repo validate --json` and `wood repo verify --json`. Deployed-runtime
verification requires the separately configured environment; local simulator
success does not attest deployed code or live Telegram acceptance. S06 owns live
credentials, verified owner/private-chat onboarding, deployment and activation.
