"""Immutable events, notifications, responses and append-only delivery evidence."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision = "0001_foundation"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("broker_events", "notify_requests"):
        op.create_table(
            table,
            sa.Column("id", sa.Uuid(), primary_key=True),
            sa.Column("source", sa.String(128), nullable=False),
            sa.Column("correlation_id", sa.Uuid(), nullable=False),
            sa.Column("causation_id", sa.Uuid()),
            sa.Column("idempotency_key", sa.String(128)),
            sa.Column("payload", pg.JSONB(), nullable=False),
            sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("source", "idempotency_key"),
        )
        op.create_index(f"ix_{table}_correlation_id", table, ["correlation_id"])
        op.create_index(f"ix_{table}_accepted_at", table, ["accepted_at"])
    for table, parent, key in (
        ("broker_delivery_attempts", "broker_events", "event_id"),
        ("notify_delivery_attempts", "notify_requests", "request_id"),
    ):
        op.create_table(
            table,
            sa.Column("id", sa.Uuid(), primary_key=True),
            sa.Column(
                key,
                sa.Uuid(),
                sa.ForeignKey(f"{parent}.id", ondelete="RESTRICT"),
                nullable=False,
            ),
            sa.Column("correlation_id", sa.Uuid(), nullable=False),
            sa.Column("destination", sa.String(128), nullable=False),
            sa.Column("outcome", sa.String(32), nullable=False),
            sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("error_code", sa.String(128)),
            sa.CheckConstraint(
                "outcome IN ('pending', 'delivered', 'transient-failure', "
                "'terminal-failure', 'suppressed')"
            ),
        )
        for column in (key, "correlation_id", "attempted_at"):
            op.create_index(f"ix_{table}_{column}", table, [column])
    op.create_table(
        "notify_responses",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "request_id",
            sa.Uuid(),
            sa.ForeignKey("notify_requests.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("causation_id", sa.Uuid()),
        sa.Column("provider", sa.String(128), nullable=False),
        sa.Column("provider_response_id", sa.String(128), nullable=False),
        sa.Column("payload", pg.JSONB(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("provider", "provider_response_id"),
    )
    for column in ("request_id", "correlation_id", "received_at"):
        op.create_index(f"ix_notify_responses_{column}", "notify_responses", [column])
    op.execute("""
        CREATE FUNCTION wes_immutable_record() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' AND current_setting('wes.retention', true) = 'on' THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'transport records are immutable';
        END $$;
    """)
    for table in (
        "broker_events",
        "notify_requests",
        "broker_delivery_attempts",
        "notify_delivery_attempts",
        "notify_responses",
    ):
        op.execute(
            f"CREATE TRIGGER immutable_record BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION wes_immutable_record()"
        )
    op.execute("""
        CREATE FUNCTION wes_correlated_child() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE expected uuid;
        BEGIN
            IF TG_TABLE_NAME = 'broker_delivery_attempts' THEN
                SELECT correlation_id INTO expected FROM broker_events
                    WHERE id = NEW.event_id;
            ELSE
                SELECT correlation_id INTO expected FROM notify_requests
                    WHERE id = NEW.request_id;
            END IF;
            IF expected IS DISTINCT FROM NEW.correlation_id THEN
                RAISE EXCEPTION 'child correlation must match parent';
            END IF;
            RETURN NEW;
        END $$;
    """)
    for table in (
        "broker_delivery_attempts",
        "notify_delivery_attempts",
        "notify_responses",
    ):
        op.execute(
            f"CREATE TRIGGER correlated_child BEFORE INSERT ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION wes_correlated_child()"
        )


def downgrade() -> None:
    for table in (
        "notify_responses",
        "notify_delivery_attempts",
        "broker_delivery_attempts",
        "notify_requests",
        "broker_events",
    ):
        op.drop_table(table)
    op.execute("DROP FUNCTION wes_correlated_child()")
    op.execute("DROP FUNCTION wes_immutable_record()")
