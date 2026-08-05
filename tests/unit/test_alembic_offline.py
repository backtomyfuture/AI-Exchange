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


def test_0006_downgrade_is_explicitly_forward_only() -> None:
    path = (
        PROJECT_ROOT / "alembic/versions/20260716_0006_greenfield_runtime_authority.py"
    )
    spec = spec_from_file_location("task10g_forward_only_revision", path)
    assert spec is not None and spec.loader is not None
    revision = module_from_spec(spec)
    spec.loader.exec_module(revision)

    assert revision.revision == "20260716_0006"
    assert revision.down_revision == "20260713_0005"
    with pytest.raises(RuntimeError, match="Forward-only production migration"):
        revision.downgrade()


def test_0007_downgrade_is_explicitly_forward_only() -> None:
    path = PROJECT_ROOT / "alembic/versions/20260728_0007_polling_only_ingress.py"
    spec = spec_from_file_location("polling_only_forward_only_revision", path)
    assert spec is not None and spec.loader is not None
    revision = module_from_spec(spec)
    spec.loader.exec_module(revision)

    assert revision.revision == "20260728_0007"
    assert revision.down_revision == "20260716_0006"
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
    revision_sql = rendered.split("20260713_0004 -> 20260713_0005", 1)[1].split(
        "20260713_0005 -> 20260716_0006",
        1,
    )[0]
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


def test_offline_upgrade_emits_atomic_greenfield_0006_authority_sql() -> None:
    output = StringIO()
    config = Config(str(PROJECT_ROOT / "alembic.ini"), output_buffer=output)
    config.set_main_option(
        "sqlalchemy.url",
        "postgresql://offline:offline@localhost/offline",
    )

    command.upgrade(config, "head", sql=True)

    rendered = output.getvalue()
    revision_sql = rendered.split("20260713_0005 -> 20260716_0006", 1)[1].split(
        "20260716_0006 -> 20260728_0007",
        1,
    )[0]
    normalized = " ".join(revision_sql.split())
    lock_position = revision_sql.index("LOCK TABLE")
    reject_position = revision_sql.index("greenfield_reinitialize_required")
    first_mutation_position = min(
        revision_sql.index("ALTER TABLE event_inbox"),
        revision_sql.index("DROP TABLE pipeline_shadow_comparisons"),
        revision_sql.index("CREATE TABLE pipeline_runtime_capabilities"),
    )

    assert lock_position < reject_position < first_mutation_position
    assert "IN ACCESS EXCLUSIVE MODE" in revision_sql
    for relation in (
        "emails_log",
        "app_kv_store",
        "pipeline_ownership",
        "event_inbox",
        "sync_cursors",
        "emails",
        "audit_events",
        "pipeline_shadow_comparisons",
        "sync_cold_start_plans",
        "pipeline_command_receipts",
    ):
        assert relation in revision_sql[:first_mutation_position]

    assert "DROP FUNCTION guard_pipeline_shadow_comparison()" in revision_sql
    assert "DROP TABLE pipeline_shadow_comparisons" in revision_sql
    assert "execution_epoch pg_catalog.int8 NOT NULL DEFAULT 0" in normalized
    assert "authority_epoch pg_catalog.int8 NOT NULL" in normalized
    assert "capability_hash pg_catalog.bpchar(64) NOT NULL" in normalized
    assert "lease_session_id pg_catalog.uuid" in normalized
    assert "owner_authority_epoch pg_catalog.int8 NOT NULL" in normalized
    assert "owner_capability_hash pg_catalog.bpchar(64) NOT NULL" in normalized
    assert "processing_execution_epoch pg_catalog.int8" in normalized

    for relation in (
        "pipeline_runtime_capabilities",
        "pipeline_initializations",
        "pipeline_folder_scopes",
        "pipeline_runtime_authority",
        "pipeline_runtime_instances",
    ):
        assert f"CREATE TABLE {relation}" in revision_sql

    assert "phase2_ingestion" in revision_sql
    assert "phase3_approval_send" in revision_sql
    assert "phase4_graph_projection" in revision_sql
    assert (
        "95771c6d473119376654d5530f7fe189c77d83e56fe08e91179f48b1040df86f"
    ) in revision_sql
    assert "runtime.initialize" in revision_sql
    assert "runtime.pause" in revision_sql
    assert "runtime.resume_ingress" in revision_sql
    assert "inbox.requeue" in revision_sql
    assert "SET search_path = pg_catalog" in revision_sql
    greenfield_routines = {
        "greenfield_apply_email_event",
        "greenfield_begin_inbox_effect",
        "greenfield_claim_inbox",
        "greenfield_drain_web_instance",
        "greenfield_fail_inbox",
        "greenfield_finish_inbox",
        "greenfield_get_runtime_authority",
        "greenfield_heartbeat_web_instance",
        "greenfield_initialize_runtime",
        "greenfield_insert_webhook_event",
        "greenfield_pause_runtime",
        "greenfield_reap_inbox",
        "greenfield_register_web_instance",
        "greenfield_renew_inbox",
        "greenfield_requeue_inbox",
        "greenfield_resume_ingress",
    }
    for routine in greenfield_routines:
        assert f"CREATE FUNCTION public.{routine}(" in revision_sql
    assert revision_sql.count("SECURITY DEFINER") == len(greenfield_routines)
    assert "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC" in normalized
    assert "CREATE OR REPLACE" not in revision_sql
    assert "legacy_compat" not in revision_sql
    assert "CREATE TABLE pipeline_shadow" not in revision_sql


def test_offline_upgrade_emits_only_the_session_fenced_0007_polling_boundary() -> None:
    output = StringIO()
    config = Config(str(PROJECT_ROOT / "alembic.ini"), output_buffer=output)
    config.set_main_option(
        "sqlalchemy.url",
        "postgresql://offline:offline@localhost/offline",
    )

    command.upgrade(config, "head", sql=True)

    rendered = output.getvalue()
    revision_sql = rendered.split("20260716_0006 -> 20260728_0007", 1)[1]
    normalized = " ".join(revision_sql.split())

    assert "CREATE FUNCTION public.greenfield_commit_sync_page(" in revision_sql
    assert "SECURITY DEFINER" in revision_sql
    assert "SET search_path = pg_catalog" in revision_sql
    assert "REVOKE ALL ON FUNCTION public.greenfield_commit_sync_page" in normalized
    assert "p_events pg_catalog.jsonb" in normalized
    assert "p_activation pg_catalog.bool" in normalized
    assert "greenfield_sync_policy_unavailable" in revision_sql
    assert "greenfield_insert_webhook_event" not in revision_sql
    assert "CREATE TABLE" not in revision_sql
