"""Align the existing business schema with the P0 processing contract."""

from __future__ import annotations

from alembic import op


revision = "20260710_0002"
down_revision = "20260710_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS error_message TEXT")
    op.execute("ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS content_ref JSONB")
    op.execute(
        "ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS "
        "version BIGINT NOT NULL DEFAULT 0"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_emails_log_status_processed "
        "ON emails_log(status, processed_at DESC)"
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
