"""Alembic environment with an externally supplied transactional connection."""

from alembic import context

from wood_events_service.storage import Base

context.configure(
    connection=context.config.attributes["connection"], target_metadata=Base.metadata
)
with context.begin_transaction():
    context.run_migrations()
