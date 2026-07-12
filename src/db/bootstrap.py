"""Explicit deployment-time bootstrap for business and checkpoint schemas."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.db.migration_settings import load_migration_settings
from src.db.schema import get_current_database_revision


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_CONFIG_PATH = PROJECT_ROOT / "alembic.ini"


@dataclass(frozen=True)
class _CheckpointIndexSpec:
    name: str
    table_name: str
    drop_sql: str
    key_columns: tuple[str, ...] = ("thread_id",)


@dataclass(frozen=True)
class _CheckpointIndexState:
    current_schema: str
    index_schema: str
    table_schema: str
    table_name: str
    index_kind: str
    is_valid: bool
    is_ready: bool
    is_unique: bool
    is_primary: bool
    is_exclusion: bool
    has_predicate: bool
    has_expressions: bool
    access_method: str
    total_columns: int
    key_column_count: int
    key_columns: tuple[str, ...]
    index_options: tuple[int, ...]
    default_opclasses: tuple[bool, ...]
    index_collations: tuple[int, ...]
    column_collations: tuple[int, ...]
    definition: str


_CHECKPOINT_INDEX_SPECS = {
    6: _CheckpointIndexSpec(
        name="checkpoints_thread_id_idx",
        table_name="checkpoints",
        drop_sql=(
            "DROP INDEX CONCURRENTLY IF EXISTS checkpoints_thread_id_idx"
        ),
    ),
    7: _CheckpointIndexSpec(
        name="checkpoint_blobs_thread_id_idx",
        table_name="checkpoint_blobs",
        drop_sql=(
            "DROP INDEX CONCURRENTLY IF EXISTS checkpoint_blobs_thread_id_idx"
        ),
    ),
    8: _CheckpointIndexSpec(
        name="checkpoint_writes_thread_id_idx",
        table_name="checkpoint_writes",
        drop_sql=(
            "DROP INDEX CONCURRENTLY IF EXISTS checkpoint_writes_thread_id_idx"
        ),
    ),
}

_CHECKPOINT_INDEX_QUERY = """
    SELECT
        current_schema(),
        index_namespace.nspname,
        table_namespace.nspname,
        table_relation.relname,
        index_relation.relkind::text,
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
        ARRAY(
            SELECT operator_class.opcdefault
            FROM unnest(index_metadata.indclass::oid[])
                 WITH ORDINALITY AS indexed_opclass(opclass_oid, position)
            JOIN pg_opclass AS operator_class
              ON operator_class.oid = indexed_opclass.opclass_oid
            WHERE indexed_opclass.position <= index_metadata.indnkeyatts
            ORDER BY indexed_opclass.position
        ),
        index_metadata.indcollation::oid[],
        ARRAY(
            SELECT attribute.attcollation
            FROM unnest(index_metadata.indkey::smallint[])
                 WITH ORDINALITY AS key_column(attnum, position)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = table_relation.oid
             AND attribute.attnum = key_column.attnum
            WHERE key_column.position <= index_metadata.indnkeyatts
            ORDER BY key_column.position
        ),
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
"""


def _upgrade_business_schema(dsn: str) -> None:
    config = Config(str(ALEMBIC_CONFIG_PATH))
    config.set_main_option("sqlalchemy.url", dsn.replace("%", "%%"))
    command.upgrade(config, "head")


async def _read_checkpoint_index(
    conn: psycopg.AsyncConnection,
    spec: _CheckpointIndexSpec,
) -> _CheckpointIndexState | None:
    async with conn.cursor() as cur:
        await cur.execute(_CHECKPOINT_INDEX_QUERY, (spec.name,))
        row = await cur.fetchone()

    if row is None:
        return None

    return _CheckpointIndexState(
        current_schema=row[0],
        index_schema=row[1],
        table_schema=row[2],
        table_name=row[3],
        index_kind=row[4],
        is_valid=row[5],
        is_ready=row[6],
        is_unique=row[7],
        is_primary=row[8],
        is_exclusion=row[9],
        has_predicate=row[10],
        has_expressions=row[11],
        access_method=row[12],
        total_columns=row[13],
        key_column_count=row[14],
        key_columns=tuple(row[15]),
        index_options=tuple(row[16]),
        default_opclasses=tuple(row[17]),
        index_collations=tuple(row[18]),
        column_collations=tuple(row[19]),
        definition=row[20],
    )


def _has_expected_checkpoint_index_definition(
    state: _CheckpointIndexState,
    spec: _CheckpointIndexSpec,
) -> bool:
    return (
        state.index_schema == state.current_schema
        and state.table_schema == state.current_schema
        and state.table_name == spec.table_name
        and state.index_kind == "i"
        and not state.is_unique
        and not state.is_primary
        and not state.is_exclusion
        and not state.has_predicate
        and not state.has_expressions
        and state.access_method == "btree"
        and state.total_columns == len(spec.key_columns)
        and state.key_column_count == len(spec.key_columns)
        and state.key_columns == spec.key_columns
        and state.index_options == (0,) * len(spec.key_columns)
        and state.default_opclasses == (True,) * len(spec.key_columns)
        and state.index_collations == state.column_collations
    )


def _unexpected_index_error(
    state: _CheckpointIndexState,
    spec: _CheckpointIndexSpec,
) -> RuntimeError:
    return RuntimeError(
        f"Checkpoint index {spec.name} has an unexpected definition; "
        f"refusing to replace it automatically: {state.definition}"
    )


async def _ensure_checkpoint_index(
    conn: psycopg.AsyncConnection,
    spec: _CheckpointIndexSpec,
    migration: str,
) -> None:
    state = await _read_checkpoint_index(conn, spec)

    if state is not None:
        if not _has_expected_checkpoint_index_definition(state, spec):
            raise _unexpected_index_error(state, spec)
        if state.is_valid and state.is_ready:
            return

        # The identifiers are fixed above; never interpolate catalog values here.
        async with conn.cursor() as cur:
            await cur.execute(spec.drop_sql)

    async with conn.cursor() as cur:
        await cur.execute(migration)

    rebuilt = await _read_checkpoint_index(conn, spec)
    if rebuilt is None:
        raise RuntimeError(
            f"Checkpoint migration did not create expected index {spec.name}"
        )
    if not _has_expected_checkpoint_index_definition(rebuilt, spec):
        raise _unexpected_index_error(rebuilt, spec)
    if not rebuilt.is_valid or not rebuilt.is_ready:
        raise RuntimeError(
            f"Checkpoint index {spec.name} is not valid and ready after migration"
        )


async def _apply_checkpoint_migrations(dsn: str) -> int:
    migrations = AsyncPostgresSaver.MIGRATIONS
    async with await psycopg.AsyncConnection.connect(
        dsn,
        autocommit=True,
        prepare_threshold=0,
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute(migrations[0])
            await cur.execute("SELECT v FROM checkpoint_migrations")
            applied = {row[0] for row in await cur.fetchall()}

        applied_count = 0
        for version, migration in enumerate(migrations):
            if version == 0:
                continue

            index_spec = _CHECKPOINT_INDEX_SPECS.get(version)
            if index_spec is not None:
                await _ensure_checkpoint_index(conn, index_spec, migration)
                if version in applied:
                    continue
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO checkpoint_migrations (v) VALUES (%s) "
                        "ON CONFLICT (v) DO NOTHING",
                        (version,),
                    )
                applied_count += 1
                continue

            if version in applied:
                continue
            async with conn.cursor() as cur:
                await cur.execute(migration)
                await cur.execute(
                    "INSERT INTO checkpoint_migrations (v) VALUES (%s) "
                    "ON CONFLICT (v) DO NOTHING",
                    (version,),
                )
            applied_count += 1

    return applied_count


async def bootstrap_database(
    dsn: str,
    *,
    expected_migration_role: str,
    expected_runtime_role: str,
    target_schema: str,
) -> dict[str, str | int]:
    """Upgrade both schemas; this is the only supported schema-writing entrypoint."""
    if not expected_migration_role or not expected_runtime_role or not target_schema:
        raise RuntimeError("Database bootstrap identity contract is incomplete")
    if expected_migration_role == expected_runtime_role:
        raise RuntimeError("Database bootstrap identity contract is invalid")
    await asyncio.to_thread(_upgrade_business_schema, dsn)
    business_revision = await get_current_database_revision(dsn)
    if business_revision is None or "," in business_revision:
        raise RuntimeError("Business schema revision is unavailable after bootstrap")
    checkpoint_count = await _apply_checkpoint_migrations(dsn)
    summary: dict[str, str | int] = {
        "alembic": business_revision,
        "checkpoint": checkpoint_count,
    }
    logger.info("Database bootstrap complete: %s", summary)
    return summary


def main() -> None:
    try:
        settings = load_migration_settings()
        asyncio.run(
            bootstrap_database(
                settings.database_url.get_secret_value(),
                expected_migration_role=settings.expected_migration_role,
                expected_runtime_role=settings.expected_runtime_role,
                target_schema=settings.target_schema,
            )
        )
    except Exception:
        raise RuntimeError("database_bootstrap_failed") from None


if __name__ == "__main__":
    main()
