# wood-broker

`wood-broker` handles machine-to-machine event ingestion and distribution.

The V0.1 broker lifecycle accepts versioned event envelopes, persists accepted
events before delivery, routes them through subscription rules, delivers to
generic HTTP webhook consumers, records delivery history, and supports retrying
failed deliveries without producer resubmission.

Broker persistence is configured with `BROKER_DATABASE_URL`. Deployment should
point that setting at the centralized PostgreSQL database; this repository does
not bundle PostgreSQL in Compose.

Human notification policy and provider-specific notification delivery remain in
`wood-notify`.
