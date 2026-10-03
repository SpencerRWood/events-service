"""Durable per-consumer broker scheduling, independent of immutable evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0002_broker_lifecycle"
down_revision = "0001_foundation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "broker_delivery_jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "event_id",
            sa.Uuid(),
            sa.ForeignKey("broker_events.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("consumer", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("event_id", "consumer"),
        sa.CheckConstraint("status IN ('pending', 'delivered', 'terminal-failure')"),
        sa.CheckConstraint("attempts >= 0 AND generation >= 0"),
    )
    for column in ("event_id", "next_attempt_at"):
        op.create_index(
            f"ix_broker_delivery_jobs_{column}", "broker_delivery_jobs", [column]
        )


def downgrade() -> None:
    op.drop_table("broker_delivery_jobs")
