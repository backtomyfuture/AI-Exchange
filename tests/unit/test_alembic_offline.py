from __future__ import annotations

from io import StringIO
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_0005_downgrade_is_explicitly_forward_only() -> None:
    path = (
        PROJECT_ROOT / "alembic/versions/20260713_0005_sync_reconciliation_control.py"
    )
    spec = spec_from_file_location("task7_forward_only_revision", path)
    assert spec is not None and spec.loader is not None
    revision = module_from_spec(spec)
    spec.loader.exec_module(revision)

    with pytest.raises(RuntimeError, match="Forward-only production migration"):
        revision.downgrade()


def test_offline_upgrade_emits_fail_closed_0004_policy_migration_sql() -> None:
    output = StringIO()
    config = Config(str(PROJECT_ROOT / "alembic.ini"), output_buffer=output)
    config.set_main_option(
        "sqlalchemy.url",
        "postgresql://offline:offline@localhost/offline",
    )

    command.upgrade(config, "head", sql=True)

    rendered = output.getvalue()
    assert "LOCK TABLE event_inbox IN ACCESS EXCLUSIVE MODE" in rendered
    assert "event_inbox_not_empty_for_0004_migration" in rendered
    assert "DROP CONSTRAINT ck_event_inbox_processing_policy" in rendered


def test_offline_upgrade_emits_dormant_0005_sync_control_sql() -> None:
    output = StringIO()
    config = Config(str(PROJECT_ROOT / "alembic.ini"), output_buffer=output)
    config.set_main_option(
        "sqlalchemy.url",
        "postgresql://offline:offline@localhost/offline",
    )

    command.upgrade(config, "head", sql=True)

    rendered = output.getvalue()
    revision_sql = rendered.split("20260713_0004 -> 20260713_0005", 1)[1]
    normalized = " ".join(revision_sql.split())
    assert "20260713_0004 -> 20260713_0005" in rendered
    assert "CREATE TABLE sync_cold_start_plans" in rendered
    assert "boundary_cursor_version pg_catalog.int8" in rendered
    assert "ready_at pg_catalog.timestamptz" in rendered
    assert "blocked_fingerprint pg_catalog.bpchar(64)" in rendered
    assert "blocked_at pg_catalog.timestamptz" in rendered
    assert "boundary_cursor IS NOT NULL" in rendered
    assert (
        "boundary_cursor_version IS NULL AND apply_cursor IS NULL "
        "AND apply_cursor_version IS NULL AND plan_hash IS NULL"
    ) in normalized
    assert "cursor_binding_plan_id pg_catalog.uuid GENERATED ALWAYS AS" in rendered
    assert "CREATE TABLE pipeline_command_receipts" in rendered
    assert "CREATE VIEW cold_start_command_receipts" in rendered
    assert "WITH CASCADED CHECK OPTION" in rendered
    assert "cold_start.preview" in rendered
    assert "cold_start.approve" in rendered
    assert "cold_start.apply_page" in rendered
    assert "cold_start_applying" in rendered
    assert "uq_sync_cursors_cold_start_plan" in rendered
    assert "uq_sync_cursors_cold_start_binding" in rendered
    assert "fk_sync_cursors_cold_start_plan" in rendered
    assert "fk_sync_cold_start_plan_active_cursor" in rendered
    assert (
        revision_sql.count("MATCH SIMPLE ON UPDATE NO ACTION ON DELETE RESTRICT") == 2
    )
    assert revision_sql.count("DEFERRABLE INITIALLY DEFERRED") == 2
    assert "ix_sync_cold_start_plans_state_expiry" in rendered
    assert "result_type = 'sync_cold_start_plan'" in rendered
    assert "IF NOT EXISTS" not in revision_sql
    assert "CREATE OR REPLACE" not in revision_sql
    assert "target_reservation_id" not in rendered
    assert "cold_start.cancel" not in rendered
