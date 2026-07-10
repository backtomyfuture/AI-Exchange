"""Record the business schema that predates Alembic."""

from __future__ import annotations

from alembic import op


revision = "20260710_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS emails_log (
            id TEXT PRIMARY KEY,
            subject TEXT,
            sender TEXT,
            received_at TIMESTAMP,
            status TEXT DEFAULT 'pending',
            classification JSONB,
            draft_content TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            routing_log JSONB,
            active_skills JSONB,
            original_draft TEXT,
            final_draft TEXT,
            draft_diff TEXT,
            approver_user_id TEXT,
            rejection_reason TEXT
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app_kv_store (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        """
        DO $$
        DECLARE
            relation_kind "char";
        BEGIN
            SELECT c.relkind INTO relation_kind
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema()
              AND c.relname = 'processed_emails';

            IF relation_kind IN ('r', 'p') THEN
                DROP TABLE processed_emails;
            ELSIF relation_kind = 'm' THEN
                DROP MATERIALIZED VIEW processed_emails;
            END IF;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE OR REPLACE VIEW processed_emails AS
        SELECT id, processed_at FROM emails_log
        """
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
