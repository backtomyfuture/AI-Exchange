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
            id pg_catalog.text PRIMARY KEY,
            subject pg_catalog.text,
            sender pg_catalog.text,
            received_at pg_catalog.timestamp,
            status pg_catalog.text DEFAULT 'pending',
            classification pg_catalog.jsonb,
            draft_content pg_catalog.text,
            processed_at pg_catalog.timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamp DEFAULT CURRENT_TIMESTAMP,
            routing_log pg_catalog.jsonb,
            active_skills pg_catalog.jsonb,
            original_draft pg_catalog.text,
            final_draft pg_catalog.text,
            draft_diff pg_catalog.text,
            approver_user_id pg_catalog.text,
            rejection_reason pg_catalog.text
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app_kv_store (
            key pg_catalog.text PRIMARY KEY,
            value pg_catalog.text,
            updated_at pg_catalog.timestamp DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        """
        DO $$
        DECLARE
            relation_kind pg_catalog."char";
        BEGIN
            SELECT c.relkind INTO relation_kind
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n
              ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
            WHERE n.nspname OPERATOR(pg_catalog.=)
                  pg_catalog.current_schema()
              AND c.relname OPERATOR(pg_catalog.=) 'processed_emails';

            IF relation_kind IS NOT NULL
               AND relation_kind OPERATOR(pg_catalog.<>)
                   'v'::pg_catalog."char" THEN
                RAISE EXCEPTION
                    'processed_emails exists as relation kind %; back up and migrate it explicitly before retrying because only an ordinary view may be replaced automatically',
                    relation_kind
                    USING ERRCODE = 'wrong_object_type';
            END IF;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE OR REPLACE VIEW processed_emails
        WITH (security_invoker = true) AS
        SELECT id, processed_at FROM emails_log
        """
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
