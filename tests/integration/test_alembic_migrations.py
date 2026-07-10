from __future__ import annotations

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver


BASELINE_COLUMNS = [
    "id",
    "subject",
    "sender",
    "received_at",
    "status",
    "classification",
    "draft_content",
    "processed_at",
    "updated_at",
    "routing_log",
    "active_skills",
    "original_draft",
    "final_draft",
    "draft_diff",
    "approver_user_id",
    "rejection_reason",
]


@pytest.mark.integration
def test_empty_database_upgrades_to_head(alembic_runner, empty_schema):
    alembic_runner.upgrade(empty_schema, "head")

    assert empty_schema.table_exists("emails_log")
    assert empty_schema.column_exists("emails_log", "error_message")
    assert empty_schema.column_exists("emails_log", "content_ref")
    assert empty_schema.column_exists("emails_log", "version")
    assert empty_schema.table_exists("app_kv_store")
    assert empty_schema.table_exists("processed_emails")


@pytest.mark.integration
def test_baseline_creates_exact_legacy_schema(alembic_runner, empty_schema):
    alembic_runner.upgrade(empty_schema, "20260710_0001")

    columns = empty_schema.scalar(
        "SELECT string_agg(column_name::text, ',' ORDER BY ordinal_position) "
        "FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'emails_log'"
    )
    assert columns.split(",") == BASELINE_COLUMNS


@pytest.mark.integration
def test_legacy_schema_upgrades_idempotently(alembic_runner, legacy_schema):
    alembic_runner.upgrade(legacy_schema, "head")
    alembic_runner.upgrade(legacy_schema, "head")

    assert legacy_schema.scalar("SELECT count(*) FROM alembic_version") == 1
    assert legacy_schema.scalar("SELECT count(*) FROM emails_log") == 2
    assert legacy_schema.scalar(
        "SELECT subject FROM emails_log WHERE id = 'legacy-1'"
    ) == "First legacy email"
    assert legacy_schema.column_exists("emails_log", "error_message")
    assert legacy_schema.column_exists("emails_log", "content_ref")
    assert legacy_schema.column_exists("emails_log", "version")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_applies_checkpoint_migrations_with_autocommit(
    postgres_database_factory,
):
    from src.db.bootstrap import bootstrap_database

    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn)
    await bootstrap_database(schema.dsn)

    assert schema.scalar("SELECT count(*) FROM alembic_version") == 1
    assert schema.scalar("SELECT count(*) FROM checkpoint_migrations") == (
        len(AsyncPostgresSaver.MIGRATIONS) - 1
    )
    assert schema.scalar("SELECT max(v) FROM checkpoint_migrations") == (
        len(AsyncPostgresSaver.MIGRATIONS) - 1
    )
    assert schema.table_exists("checkpoints")
    assert schema.table_exists("checkpoint_blobs")
    assert schema.table_exists("checkpoint_writes")
    assert schema.scalar(
        "SELECT count(*) FROM pg_indexes "
        "WHERE schemaname = current_schema() "
        "AND indexname IN ("
        "'checkpoints_thread_id_idx', "
        "'checkpoint_blobs_thread_id_idx', "
        "'checkpoint_writes_thread_id_idx'"
        ")"
    ) == 3
