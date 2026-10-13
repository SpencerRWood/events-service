"""Durable, ordered recovery Telegram parts under the existing logical request."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0005_recovery_parts"
down_revision = "0004_recovery_dispatch"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notify_recovery_parts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "request_id",
            sa.Uuid(),
            sa.ForeignKey("notify_requests.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(128)),
        sa.Column("message_id", sa.BigInteger()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("request_id", "position"),
        sa.CheckConstraint("position >= 1 AND attempts >= 0"),
        sa.CheckConstraint(
            "status IN ('pending', 'dispatching', 'delivered', "
            "'transient-failure', 'terminal-failure')"
        ),
    )
    op.create_index(
        "ix_notify_recovery_parts_request_id", "notify_recovery_parts", ["request_id"]
    )


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT count(*) FROM notify_recovery_parts")):
        raise RuntimeError("Retain recovery delivery evidence before downgrade")
    op.drop_table("notify_recovery_parts")
