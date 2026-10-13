"""Persist recovery Telegram dispatch intent before the remote side effect."""

from alembic import op
from sqlalchemy import text

revision = "0004_recovery_dispatch"
down_revision = "0003_notify_lifecycle"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "notify_delivery_jobs_status_check", "notify_delivery_jobs", type_="check"
    )
    op.create_check_constraint(
        "notify_delivery_jobs_status_check",
        "notify_delivery_jobs",
        "status IN ('pending', 'delivered', 'suppressed', 'transient-failure', "
        "'terminal-failure', 'expired', 'dispatching')",
    )


def downgrade() -> None:
    if op.get_bind().scalar(
        text("SELECT count(*) FROM notify_delivery_jobs WHERE status = 'dispatching'")
    ):
        raise RuntimeError("Reconcile outstanding dispatches before downgrade")
    op.drop_constraint(
        "notify_delivery_jobs_status_check", "notify_delivery_jobs", type_="check"
    )
    op.create_check_constraint(
        "notify_delivery_jobs_status_check",
        "notify_delivery_jobs",
        "status IN ('pending', 'delivered', 'suppressed', 'transient-failure', "
        "'terminal-failure', 'expired')",
    )
