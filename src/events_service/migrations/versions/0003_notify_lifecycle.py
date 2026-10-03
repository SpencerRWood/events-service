"""Notification policy state, durable jobs, suppression and message evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0003_notify_lifecycle"
down_revision = "0002_broker_lifecycle"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notify_states",
        sa.Column(
            "id",
            sa.Uuid(),
            sa.ForeignKey("notify_requests.id", ondelete="RESTRICT"),
            primary_key=True,
        ),
        sa.Column("response_state", sa.String(32), nullable=False),
        sa.Column("response_deadline", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "response_state IN ('none', 'awaiting-response', 'responded', "
            "'expired', 'suppressed')"
        ),
    )
    op.create_index(
        "ix_notify_states_response_deadline", "notify_states", ["response_deadline"]
    )
    op.create_table(
        "notify_delivery_jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "request_id",
            sa.Uuid(),
            sa.ForeignKey("notify_requests.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("channel", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("request_id", "channel"),
        sa.CheckConstraint(
            "status IN ('pending', 'delivered', 'suppressed', 'transient-failure', "
            "'terminal-failure', 'expired')"
        ),
        sa.CheckConstraint("attempts >= 0 AND generation >= 0"),
    )
    for column in ("request_id", "next_attempt_at"):
        op.create_index(
            f"ix_notify_delivery_jobs_{column}", "notify_delivery_jobs", [column]
        )
    op.create_table(
        "notify_suppression_windows",
        sa.Column("key", sa.String(64), primary_key=True),
        sa.Column("until", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_notify_suppression_windows_until", "notify_suppression_windows", ["until"]
    )
    op.create_table(
        "notify_messages",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "request_id",
            sa.Uuid(),
            sa.ForeignKey("notify_requests.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("chat_id", "message_id"),
    )
    for column in ("request_id", "correlation_id", "delivered_at"):
        op.create_index(f"ix_notify_messages_{column}", "notify_messages", [column])
    op.execute(
        "CREATE TRIGGER immutable_record BEFORE UPDATE OR DELETE ON notify_messages "
        "FOR EACH ROW EXECUTE FUNCTION wes_immutable_record()"
    )
    op.execute(
        "CREATE TRIGGER correlated_child BEFORE INSERT ON notify_messages "
        "FOR EACH ROW EXECUTE FUNCTION wes_correlated_child()"
    )


def downgrade() -> None:
    for table in (
        "notify_messages",
        "notify_suppression_windows",
        "notify_delivery_jobs",
        "notify_states",
    ):
        op.drop_table(table)
