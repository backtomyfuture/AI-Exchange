from __future__ import annotations

import psycopg
import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from sqlalchemy.exc import DBAPIError


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

CHECKPOINT_INDEXES = (
    (6, "checkpoints_thread_id_idx", "checkpoints"),
    (7, "checkpoint_blobs_thread_id_idx", "checkpoint_blobs"),
    (8, "checkpoint_writes_thread_id_idx", "checkpoint_writes"),
)


def _checkpoint_index_state(schema, index_name: str) -> dict | None:
    with psycopg.connect(schema.dsn) as conn:
        row = conn.execute(
            """
            SELECT
                current_schema(),
                index_namespace.nspname,
                table_namespace.nspname,
                table_relation.relname,
                index_metadata.indisvalid,
                index_metadata.indisready,
                index_metadata.indisunique,
                index_metadata.indisprimary,
                index_metadata.indisexclusion,
                index_metadata.indpred IS NOT NULL,
                index_metadata.indexprs IS NOT NULL,
                access_method.amname,
                index_metadata.indnatts,
                index_metadata.indnkeyatts,
                ARRAY(
                    SELECT attribute.attname::text
                    FROM unnest(index_metadata.indkey::smallint[])
                         WITH ORDINALITY AS key_column(attnum, position)
                    JOIN pg_attribute AS attribute
                      ON attribute.attrelid = table_relation.oid
                     AND attribute.attnum = key_column.attnum
                    WHERE key_column.position <= index_metadata.indnkeyatts
                    ORDER BY key_column.position
                ),
                index_metadata.indoption::smallint[],
                pg_get_indexdef(index_relation.oid)
            FROM pg_class AS index_relation
            JOIN pg_namespace AS index_namespace
              ON index_namespace.oid = index_relation.relnamespace
            JOIN pg_index AS index_metadata
              ON index_metadata.indexrelid = index_relation.oid
            JOIN pg_class AS table_relation
              ON table_relation.oid = index_metadata.indrelid
            JOIN pg_namespace AS table_namespace
              ON table_namespace.oid = table_relation.relnamespace
            JOIN pg_am AS access_method
              ON access_method.oid = index_relation.relam
            WHERE index_namespace.nspname = current_schema()
              AND index_relation.relname = %s
            """,
            (index_name,),
        ).fetchone()

    if row is None:
        return None

    fields = (
        "current_schema",
        "index_schema",
        "table_schema",
        "table_name",
        "is_valid",
        "is_ready",
        "is_unique",
        "is_primary",
        "is_exclusion",
        "has_predicate",
        "has_expressions",
        "access_method",
        "total_columns",
        "key_column_count",
        "key_columns",
        "index_options",
        "definition",
    )
    return dict(zip(fields, row, strict=True))


def _assert_expected_checkpoint_index(schema, index_name: str, table_name: str):
    state = _checkpoint_index_state(schema, index_name)
    assert state is not None
    assert state["index_schema"] == state["current_schema"]
    assert state["table_schema"] == state["current_schema"]
    assert state["table_name"] == table_name
    assert state["is_valid"] is True
    assert state["is_ready"] is True
    assert state["is_unique"] is False
    assert state["is_primary"] is False
    assert state["is_exclusion"] is False
    assert state["has_predicate"] is False
    assert state["has_expressions"] is False
    assert state["access_method"] == "btree"
    assert state["total_columns"] == 1
    assert state["key_column_count"] == 1
    assert state["key_columns"] == ["thread_id"]
    assert state["index_options"] == [0]
    assert state["definition"] == (
        f"CREATE INDEX {index_name} ON {state['table_schema']}.{table_name} "
        "USING btree (thread_id)"
    )


def _assert_expected_checkpoint_indexes(schema):
    for _version, index_name, table_name in CHECKPOINT_INDEXES:
        _assert_expected_checkpoint_index(schema, index_name, table_name)


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
def test_baseline_refuses_populated_processed_emails_table(
    alembic_runner,
    empty_schema,
):
    empty_schema.execute(
        "CREATE TABLE processed_emails (id TEXT PRIMARY KEY, processed_at TIMESTAMP)"
    )
    empty_schema.execute(
        "INSERT INTO processed_emails (id, processed_at) "
        "VALUES ('table-sentinel', TIMESTAMP '2026-07-10 09:00:00')"
    )

    with pytest.raises(
        DBAPIError,
        match="processed_emails.*back up.*migrate.*ordinary view",
    ):
        alembic_runner.upgrade(empty_schema, "20260710_0001")

    assert empty_schema.scalar(
        "SELECT relkind::text FROM pg_class AS c "
        "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = current_schema() AND c.relname = 'processed_emails'"
    ) == "r"
    assert empty_schema.scalar("SELECT count(*) FROM processed_emails") == 1
    assert empty_schema.scalar("SELECT id FROM processed_emails") == "table-sentinel"


@pytest.mark.integration
def test_baseline_refuses_populated_processed_emails_materialized_view(
    alembic_runner,
    empty_schema,
):
    empty_schema.execute(
        "CREATE MATERIALIZED VIEW processed_emails AS "
        "SELECT 'materialized-sentinel'::text AS id, "
        "TIMESTAMP '2026-07-10 09:00:00' AS processed_at"
    )

    with pytest.raises(
        DBAPIError,
        match="processed_emails.*back up.*migrate.*ordinary view",
    ):
        alembic_runner.upgrade(empty_schema, "20260710_0001")

    assert empty_schema.scalar(
        "SELECT relkind::text FROM pg_class AS c "
        "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = current_schema() AND c.relname = 'processed_emails'"
    ) == "m"
    assert empty_schema.scalar("SELECT count(*) FROM processed_emails") == 1
    assert empty_schema.scalar("SELECT id FROM processed_emails") == (
        "materialized-sentinel"
    )


@pytest.mark.integration
def test_baseline_replaces_existing_processed_emails_view_idempotently(
    alembic_runner,
    legacy_schema,
):
    legacy_schema.execute(
        "CREATE VIEW processed_emails AS "
        "SELECT id, processed_at FROM emails_log WHERE false"
    )

    alembic_runner.upgrade(legacy_schema, "20260710_0001")
    alembic_runner.upgrade(legacy_schema, "20260710_0001")

    assert legacy_schema.scalar(
        "SELECT relkind::text FROM pg_class AS c "
        "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = current_schema() AND c.relname = 'processed_emails'"
    ) == "v"
    assert legacy_schema.scalar("SELECT count(*) FROM processed_emails") == 2


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
    _assert_expected_checkpoint_indexes(schema)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_records_existing_correct_checkpoint_index(
    postgres_database_factory,
):
    from src.db.bootstrap import bootstrap_database

    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn)
    schema.execute("DELETE FROM checkpoint_migrations WHERE v = 6")

    await bootstrap_database(schema.dsn)

    assert schema.scalar(
        "SELECT count(*) FROM checkpoint_migrations WHERE v = 6"
    ) == 1
    _assert_expected_checkpoint_indexes(schema)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version_recorded",
    [False, True],
    ids=["version-not-recorded", "version-recorded"],
)
@pytest.mark.parametrize(
    ("wrong_definition", "wrong_table", "wrong_columns", "has_predicate"),
    [
        (
            "CREATE INDEX checkpoints_thread_id_idx "
            "ON checkpoints(checkpoint_id)",
            "checkpoints",
            ["checkpoint_id"],
            False,
        ),
        (
            "CREATE INDEX checkpoints_thread_id_idx "
            "ON checkpoint_blobs(thread_id)",
            "checkpoint_blobs",
            ["thread_id"],
            False,
        ),
        (
            "CREATE INDEX checkpoints_thread_id_idx ON checkpoints(thread_id) "
            "WHERE checkpoint_id IS NOT NULL",
            "checkpoints",
            ["thread_id"],
            True,
        ),
    ],
    ids=["wrong-column", "wrong-table", "predicate"],
)
async def test_bootstrap_rejects_wrong_valid_checkpoint_index(
    postgres_database_factory,
    version_recorded,
    wrong_definition,
    wrong_table,
    wrong_columns,
    has_predicate,
):
    from src.db.bootstrap import bootstrap_database

    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn)
    if not version_recorded:
        schema.execute("DELETE FROM checkpoint_migrations WHERE v = 6")
    schema.execute("DROP INDEX CONCURRENTLY checkpoints_thread_id_idx")
    schema.execute(wrong_definition)

    with pytest.raises(RuntimeError, match="checkpoints_thread_id_idx"):
        await bootstrap_database(schema.dsn)

    assert schema.scalar(
        "SELECT count(*) FROM checkpoint_migrations WHERE v = 6"
    ) == int(version_recorded)
    wrong_index = _checkpoint_index_state(schema, "checkpoints_thread_id_idx")
    assert wrong_index is not None
    assert wrong_index["is_valid"] is True
    assert wrong_index["is_ready"] is True
    assert wrong_index["table_name"] == wrong_table
    assert wrong_index["key_columns"] == wrong_columns
    assert wrong_index["has_predicate"] is has_predicate


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_rebuilds_recorded_invalid_expected_checkpoint_index(
    postgres_database_factory,
):
    from src.db.bootstrap import bootstrap_database

    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn)
    schema.execute(
        "UPDATE pg_index SET indisvalid = false, indisready = false "
        "WHERE indexrelid = 'checkpoints_thread_id_idx'::regclass"
    )
    invalid_index = _checkpoint_index_state(schema, "checkpoints_thread_id_idx")
    assert invalid_index is not None
    assert invalid_index["key_columns"] == ["thread_id"]
    assert invalid_index["is_valid"] is False
    assert invalid_index["is_ready"] is False

    await bootstrap_database(schema.dsn)

    assert schema.scalar(
        "SELECT count(*) FROM checkpoint_migrations WHERE v = 6"
    ) == 1
    _assert_expected_checkpoint_indexes(schema)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_rebuilds_recorded_missing_checkpoint_index(
    postgres_database_factory,
):
    from src.db.bootstrap import bootstrap_database

    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn)
    schema.execute("DROP INDEX CONCURRENTLY checkpoint_blobs_thread_id_idx")

    await bootstrap_database(schema.dsn)

    assert schema.scalar(
        "SELECT count(*) FROM checkpoint_migrations WHERE v = 7"
    ) == 1
    _assert_expected_checkpoint_indexes(schema)
