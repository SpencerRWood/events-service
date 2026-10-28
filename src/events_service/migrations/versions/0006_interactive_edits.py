"""Source-owned, original-message-bound durable terminal edits."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0006_interactive_edits"
down_revision = "0005_recovery_parts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "notify_states_response_state_check", "notify_states", type_="check"
    )
    op.create_check_constraint(
        "ck_notify_states_response_state",
        "notify_states",
        "response_state IN ('none','awaiting-response','responded',"
        "'expired','suppressed','closed')",
    )
    op.create_table(
        "notify_message_edits",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "request_id",
            sa.Uuid(),
            sa.ForeignKey("notify_requests.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "message_ref",
            sa.Uuid(),
            sa.ForeignKey("notify_messages.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(128)),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("request_id"),
        sa.CheckConstraint("attempts >= 0"),
        sa.CheckConstraint(
            "status IN ('pending','dispatching','uncertain',"
            "'delivered','terminal-failure')"
        ),
    )
    op.create_index(
        "ix_notify_message_edits_request_id", "notify_message_edits", ["request_id"]
    )
    op.create_index(
        "ix_notify_message_edits_next_attempt_at",
        "notify_message_edits",
        ["next_attempt_at"],
    )
    op.execute("""
        CREATE FUNCTION wes_bound_message_edit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'UPDATE' AND
                (NEW.id, NEW.request_id, NEW.message_ref, NEW.payload)
                IS DISTINCT FROM
                (OLD.id, OLD.request_id, OLD.message_ref, OLD.payload) THEN
                RAISE EXCEPTION 'terminal edit identity and payload are immutable';
            END IF;
            IF NOT EXISTS (SELECT 1 FROM notify_messages
                WHERE id=NEW.message_ref AND request_id=NEW.request_id) THEN
                RAISE EXCEPTION 'terminal edit must bind its original message';
            END IF;
            RETURN NEW;
        END; $$
    """)
    op.execute("""
        CREATE TRIGGER bound_message_edit BEFORE INSERT OR UPDATE
        ON notify_message_edits FOR EACH ROW
        EXECUTE FUNCTION wes_bound_message_edit()
    """)


def downgrade() -> None:
    if op.get_bind().scalar(sa.text("SELECT count(*) FROM notify_message_edits")):
        raise RuntimeError("Retain message edit evidence before downgrade")
    op.drop_table("notify_message_edits")
    op.execute("DROP FUNCTION wes_bound_message_edit()")
    if op.get_bind().scalar(
        sa.text("SELECT count(*) FROM notify_states WHERE response_state='closed'")
    ):
        raise RuntimeError("Retain terminal response state before downgrade")
    op.drop_constraint(
        "ck_notify_states_response_state", "notify_states", type_="check"
    )
    op.create_check_constraint(
        "notify_states_response_state_check",
        "notify_states",
        "response_state IN ('none','awaiting-response','responded',"
        "'expired','suppressed')",
    )
