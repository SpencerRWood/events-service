# wood-notify

`wood-notify` handles machine-to-human notification policy and delivery.

The V0.1 notification lifecycle consumes broker events through `/broker-events`,
matches configurable policy by severity, source, event type, and channel, and
delivers through isolated ntfy, Telegram, and SMTP adapters.

Delivery history and retry are persisted in the configured centralized
PostgreSQL database via `NOTIFY_DATABASE_URL`. Provider credentials are read
from environment/deployment configuration and are not stored in notification
delivery records.
