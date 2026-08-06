"""Persist durable plain-text daily digest executions."""

from __future__ import annotations

from alembic import op


revision = "20260805_0008"
down_revision = "20260728_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # This record has an independent identity from pipeline command receipts:
    # one account/scope/window can have only one immutable delivery bundle.
    op.execute(
        """
        CREATE TABLE public.daily_digest_executions (
            account_id pg_catalog.int8 NOT NULL,
            delivery_scope_hash pg_catalog.bpchar(64) NOT NULL,
            window_start pg_catalog.timestamptz NOT NULL,
            window_end pg_catalog.timestamptz NOT NULL,
            state pg_catalog.text NOT NULL,
            is_backfill pg_catalog.bool NOT NULL DEFAULT false,
            delivery_parts pg_catalog.jsonb NOT NULL,
            attempt_count pg_catalog.int8 NOT NULL DEFAULT 0,
            last_attempt_at pg_catalog.timestamptz,
            last_error_code pg_catalog.text,
            confirmed_at pg_catalog.timestamptz,
            missed_at pg_catalog.timestamptz,
            missed_reported_at pg_catalog.timestamptz,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_daily_digest_executions PRIMARY KEY (
                account_id,
                delivery_scope_hash,
                window_start,
                window_end
            )
        )
        """
    )


def downgrade() -> None:
    raise RuntimeError("daily_digest_execution_migration_is_forward_only")
