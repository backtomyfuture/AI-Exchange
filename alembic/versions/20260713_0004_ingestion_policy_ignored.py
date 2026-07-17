"""Allow verified ignored-folder receipts in the durable inbox."""

from __future__ import annotations

from alembic import op


revision = "20260713_0004"
down_revision = "20260710_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE event_inbox IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $migration$
        BEGIN
            IF EXISTS (SELECT 1 FROM event_inbox LIMIT 1) THEN
                RAISE EXCEPTION 'event_inbox_not_empty_for_0004_migration'
                    USING ERRCODE = 'P0001';
            END IF;
        END
        $migration$
        """
    )
    op.execute(
        """
        ALTER TABLE event_inbox
        DROP CONSTRAINT ck_event_inbox_processing_policy
        """
    )
    op.execute(
        """
        ALTER TABLE event_inbox
        ADD CONSTRAINT ck_event_inbox_processing_policy CHECK (
            processing_policy IN (
                'full', 'archive', 'metadata_only',
                'historical_suppressed', 'ignored'
            )
        )
        """
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
