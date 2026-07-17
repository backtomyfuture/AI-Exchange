"""Explicit deployment-time bootstrap for business and checkpoint schemas."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from hashlib import sha256
from importlib.metadata import version as package_version
from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import sql

from src.db.access_contract import (
    AUDITOR_ROUTINE_EXECUTE_BY_REVISION,
    AUDITOR_RELATION_ACCESS_BY_REVISION,
    MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION,
    MAINTENANCE_RELATION_ACCESS_BY_REVISION,
    RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
    RUNTIME_RELATION_ACCESS_BY_REVISION,
    RelationAccess,
    RoutineAccess,
)
from src.db.migration_settings import load_migration_settings
from src.db.roles import require_migration_database_role
from src.db.schema import get_current_database_revision
from src.db.schema_contract import require_database_schema_contract


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_CONFIG_PATH = PROJECT_ROOT / "alembic.ini"
_ROUTINE_NAME = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z", flags=re.ASCII)
_ROUTINE_IDENTITY_ARGUMENTS = re.compile(
    r"(?:[a-z_][a-z0-9_]*[ ]+)?"
    r"(?:(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*(?:\[\])?"
    r"|timestamp with time zone)"
    r"(?:,[ ]+(?:[a-z_][a-z0-9_]*[ ]+)?"
    r"(?:(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*(?:\[\])?"
    r"|timestamp with time zone))*\Z",
    flags=re.ASCII,
)
_CHECKPOINT_PACKAGE_NAME = "langgraph-checkpoint-postgres"
_CHECKPOINT_PACKAGE_VERSION = "3.0.4"
_CHECKPOINT_MIGRATION_COUNT = 10
_CHECKPOINT_MIGRATION_SHA256 = (
    "98d38ed91d4a57a2fb066323f26f2902efcb01f483e09f3ff2be31304a799d35"
)


class CheckpointMigrationCompatibilityError(RuntimeError):
    """Raised before DDL when the pinned third-party migration set drifts."""


def _require_checkpoint_migration_manifest() -> None:
    try:
        migrations = tuple(AsyncPostgresSaver.MIGRATIONS)
        digest = sha256("\0".join(migrations).encode()).hexdigest()
        compatible = (
            package_version(_CHECKPOINT_PACKAGE_NAME) == _CHECKPOINT_PACKAGE_VERSION
            and len(migrations) == _CHECKPOINT_MIGRATION_COUNT
            and digest == _CHECKPOINT_MIGRATION_SHA256
        )
    except Exception:
        compatible = False
    if not compatible:
        raise CheckpointMigrationCompatibilityError(
            "checkpoint_migration_manifest_invalid"
        )


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
        drop_sql=("DROP INDEX CONCURRENTLY IF EXISTS checkpoints_thread_id_idx"),
    ),
    7: _CheckpointIndexSpec(
        name="checkpoint_blobs_thread_id_idx",
        table_name="checkpoint_blobs",
        drop_sql=("DROP INDEX CONCURRENTLY IF EXISTS checkpoint_blobs_thread_id_idx"),
    ),
    8: _CheckpointIndexSpec(
        name="checkpoint_writes_thread_id_idx",
        table_name="checkpoint_writes",
        drop_sql=("DROP INDEX CONCURRENTLY IF EXISTS checkpoint_writes_thread_id_idx"),
    ),
}

_CHECKPOINT_INDEX_QUERY = """
    SELECT
        %s::pg_catalog.text,
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
            FROM pg_catalog.unnest(index_metadata.indkey::pg_catalog.int2[])
                 WITH ORDINALITY AS key_column(attnum, position)
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = table_relation.oid
             AND attribute.attnum = key_column.attnum
            WHERE key_column.position <= index_metadata.indnkeyatts
            ORDER BY key_column.position
        ),
        index_metadata.indoption::pg_catalog.int2[],
        ARRAY(
            SELECT operator_class.opcdefault
            FROM pg_catalog.unnest(index_metadata.indclass::pg_catalog.oid[])
                 WITH ORDINALITY AS indexed_opclass(opclass_oid, position)
            JOIN pg_catalog.pg_opclass AS operator_class
              ON operator_class.oid = indexed_opclass.opclass_oid
            WHERE indexed_opclass.position <= index_metadata.indnkeyatts
            ORDER BY indexed_opclass.position
        ),
        index_metadata.indcollation::pg_catalog.oid[],
        ARRAY(
            SELECT attribute.attcollation
            FROM pg_catalog.unnest(index_metadata.indkey::pg_catalog.int2[])
                 WITH ORDINALITY AS key_column(attnum, position)
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = table_relation.oid
             AND attribute.attnum = key_column.attnum
            WHERE key_column.position <= index_metadata.indnkeyatts
            ORDER BY key_column.position
        ),
        pg_catalog.pg_get_indexdef(index_relation.oid)
    FROM pg_catalog.pg_class AS index_relation
    JOIN pg_catalog.pg_namespace AS index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    JOIN pg_catalog.pg_index AS index_metadata
      ON index_metadata.indexrelid = index_relation.oid
    JOIN pg_catalog.pg_class AS table_relation
      ON table_relation.oid = index_metadata.indrelid
    JOIN pg_catalog.pg_namespace AS table_namespace
      ON table_namespace.oid = table_relation.relnamespace
    JOIN pg_catalog.pg_am AS access_method
      ON access_method.oid = index_relation.relam
    WHERE index_namespace.nspname = %s
      AND index_relation.relname = %s
"""


def _upgrade_business_schema(dsn: str) -> None:
    config = Config(str(ALEMBIC_CONFIG_PATH))
    config.set_main_option("sqlalchemy.url", dsn.replace("%", "%%"))
    command.upgrade(config, "head")


async def _require_empty_event_inbox_for_0004(
    dsn: str,
    *,
    target_schema: str,
) -> None:
    """Fail closed before the policy CHECK migration scans a live Inbox."""

    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL("SELECT EXISTS (SELECT 1 FROM {}.event_inbox LIMIT 1)").format(
                    sql.Identifier(target_schema)
                )
            )
            row = await cursor.fetchone()
    if row is None or not isinstance(row[0], bool):
        raise RuntimeError("event_inbox_preflight_unavailable_for_0004_migration")
    if row[0]:
        raise RuntimeError("event_inbox_not_empty_for_0004_migration")


async def _revoke_relation_access(
    conn: psycopg.AsyncConnection,
    *,
    target_schema: str,
    role: str,
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT relation.relname::pg_catalog.text, "
            "ARRAY("
            "SELECT attribute.attname::pg_catalog.text "
            "FROM pg_catalog.pg_attribute AS attribute "
            "WHERE attribute.attrelid = relation.oid "
            "AND attribute.attnum > 0 AND NOT attribute.attisdropped "
            "ORDER BY attribute.attnum) "
            "FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS relation_schema "
            "ON relation_schema.oid = relation.relnamespace "
            "WHERE relation_schema.nspname = %s "
            "AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')",
            (target_schema,),
        )
        relations = await cursor.fetchall()
        for relation_name, columns in relations:
            relation = sql.Identifier(target_schema, relation_name)
            grantee = sql.Identifier(role)
            await cursor.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON TABLE {} FROM {}").format(
                    relation,
                    grantee,
                )
            )
            if not columns:
                continue
            column_list = sql.SQL(", ").join(map(sql.Identifier, columns))
            for privilege in ("SELECT", "INSERT", "UPDATE", "REFERENCES"):
                await cursor.execute(
                    sql.SQL("REVOKE {} ({}) ON TABLE {} FROM {}").format(
                        sql.SQL(privilege),
                        column_list,
                        relation,
                        grantee,
                    )
                )


async def _grant_relation_access(
    conn: psycopg.AsyncConnection,
    *,
    target_schema: str,
    role: str,
    manifest: dict[str, RelationAccess],
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT relation.relname::pg_catalog.text "
            "FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS relation_schema "
            "ON relation_schema.oid = relation.relnamespace "
            "WHERE relation_schema.nspname = %s "
            "AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')",
            (target_schema,),
        )
        existing_relations = {row[0] for row in await cursor.fetchall()}
        for relation_name, access in manifest.items():
            if relation_name not in existing_relations:
                continue
            relation = sql.Identifier(target_schema, relation_name)
            grantee = sql.Identifier(role)
            if access.table_privileges:
                privileges = sql.SQL(", ").join(map(sql.SQL, access.table_privileges))
                await cursor.execute(
                    sql.SQL("GRANT {} ON TABLE {} TO {}").format(
                        privileges,
                        relation,
                        grantee,
                    )
                )
            for privilege, columns in (
                ("SELECT", access.select_columns),
                ("INSERT", access.insert_columns),
                ("UPDATE", access.update_columns),
            ):
                if not columns:
                    continue
                column_list = sql.SQL(", ").join(map(sql.Identifier, columns))
                await cursor.execute(
                    sql.SQL("GRANT {} ({}) ON TABLE {} TO {}").format(
                        sql.SQL(privilege),
                        column_list,
                        relation,
                        grantee,
                    )
                )
            if access.delete:
                await cursor.execute(
                    sql.SQL("GRANT DELETE ON TABLE {} TO {}").format(
                        relation,
                        grantee,
                    )
                )


def _validate_routine_manifest(manifest: tuple[RoutineAccess, ...]) -> None:
    identities: set[tuple[str, str]] = set()
    for spec in manifest:
        if type(spec) is not RoutineAccess:
            raise RuntimeError("Database routine access contract is invalid")
        identity = (spec.name, spec.identity_arguments)
        if (
            type(spec.name) is not str
            or type(spec.identity_arguments) is not str
            or _ROUTINE_NAME.fullmatch(spec.name) is None
            or _ROUTINE_IDENTITY_ARGUMENTS.fullmatch(spec.identity_arguments) is None
            or identity in identities
        ):
            raise RuntimeError("Database routine access contract is invalid")
        identities.add(identity)


async def _revoke_routine_access(
    conn: psycopg.AsyncConnection,
    *,
    target_schema: str,
    roles: tuple[str, ...],
) -> None:
    schema = sql.Identifier(target_schema)
    async with conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL(
                "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {} FROM PUBLIC"
            ).format(schema)
        )
        for role in roles:
            await cursor.execute(
                sql.SQL(
                    "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {} FROM {}"
                ).format(schema, sql.Identifier(role))
            )


async def _grant_routine_access(
    conn: psycopg.AsyncConnection,
    *,
    target_schema: str,
    role: str,
    manifest: tuple[RoutineAccess, ...],
) -> None:
    _validate_routine_manifest(manifest)
    if not manifest:
        return

    expected = {(spec.name, spec.identity_arguments) for spec in manifest}
    names = sorted({spec.name for spec in manifest})
    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT routine.proname::pg_catalog.text, "
            "pg_catalog.pg_get_function_identity_arguments(routine.oid) "
            "FROM pg_catalog.pg_proc AS routine "
            "JOIN pg_catalog.pg_namespace AS routine_schema "
            "ON routine_schema.oid = routine.pronamespace "
            "WHERE routine_schema.nspname = %s "
            "AND routine.proname = ANY(%s::pg_catalog.text[]) "
            "ORDER BY routine.proname, routine.oid",
            (target_schema, names),
        )
        actual = {(str(row[0]), str(row[1])) for row in await cursor.fetchall()}
        if actual != expected:
            raise RuntimeError("Database routine access contract is unavailable")

        for spec in manifest:
            await cursor.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION {}.{}({}) TO {}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(spec.name),
                    sql.SQL(spec.identity_arguments),
                    sql.Identifier(role),
                )
            )


async def _apply_database_access_contract(
    dsn: str,
    *,
    target_schema: str,
    runtime_role: str,
    maintenance_role: str,
    auditor_role: str,
    business_revision: str | None = None,
) -> None:
    selected_revision = business_revision or await get_current_database_revision(dsn)
    if (
        selected_revision not in RUNTIME_RELATION_ACCESS_BY_REVISION
        or selected_revision not in MAINTENANCE_RELATION_ACCESS_BY_REVISION
        or selected_revision not in AUDITOR_RELATION_ACCESS_BY_REVISION
        or selected_revision not in RUNTIME_ROUTINE_EXECUTE_BY_REVISION
        or selected_revision not in MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION
        or selected_revision not in AUDITOR_ROUTINE_EXECUTE_BY_REVISION
    ):
        raise RuntimeError("Database access contract revision is unavailable")
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        for role, manifest in (
            (runtime_role, RUNTIME_RELATION_ACCESS_BY_REVISION[selected_revision]),
            (
                maintenance_role,
                MAINTENANCE_RELATION_ACCESS_BY_REVISION[selected_revision],
            ),
            (auditor_role, AUDITOR_RELATION_ACCESS_BY_REVISION[selected_revision]),
        ):
            await _revoke_relation_access(
                conn,
                target_schema=target_schema,
                role=role,
            )
            await _grant_relation_access(
                conn,
                target_schema=target_schema,
                role=role,
                manifest=manifest,
            )
        routine_manifests = (
            (runtime_role, RUNTIME_ROUTINE_EXECUTE_BY_REVISION[selected_revision]),
            (
                maintenance_role,
                MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION[selected_revision],
            ),
            (auditor_role, AUDITOR_ROUTINE_EXECUTE_BY_REVISION[selected_revision]),
        )
        await _revoke_routine_access(
            conn,
            target_schema=target_schema,
            roles=tuple(role for role, _manifest in routine_manifests),
        )
        for role, manifest in routine_manifests:
            await _grant_routine_access(
                conn,
                target_schema=target_schema,
                role=role,
                manifest=manifest,
            )
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT pg_catalog.current_database()")
            row = await cursor.fetchone()
            if row is None or not isinstance(row[0], str):
                raise RuntimeError("Database access contract target is unavailable")
            database = sql.Identifier(row[0])
            auditor = sql.Identifier(auditor_role)
            schema = sql.Identifier(target_schema)
            await cursor.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                    database,
                    auditor,
                )
            )
            await cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    database,
                    auditor,
                )
            )
            await cursor.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON SCHEMA {} FROM {}").format(
                    schema,
                    auditor,
                )
            )
            await cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    schema,
                    auditor,
                )
            )
        await conn.commit()


async def _read_checkpoint_index(
    conn: psycopg.AsyncConnection,
    spec: _CheckpointIndexSpec,
    target_schema: str,
) -> _CheckpointIndexState | None:
    async with conn.cursor() as cur:
        await cur.execute("SET search_path TO pg_catalog")
        try:
            await cur.execute(
                _CHECKPOINT_INDEX_QUERY,
                (target_schema, target_schema, spec.name),
            )
            row = await cur.fetchone()
        finally:
            await cur.execute(
                "SELECT pg_catalog.set_config('search_path', %s, false)",
                (target_schema,),
            )

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
    target_schema: str,
) -> None:
    state = await _read_checkpoint_index(conn, spec, target_schema)

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

    rebuilt = await _read_checkpoint_index(conn, spec, target_schema)
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


async def _apply_checkpoint_migrations(dsn: str, target_schema: str) -> int:
    _require_checkpoint_migration_manifest()
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
                await _ensure_checkpoint_index(
                    conn,
                    index_spec,
                    migration,
                    target_schema,
                )
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
    expected_maintenance_role: str,
    expected_auditor_role: str,
    target_schema: str,
) -> dict[str, str | int]:
    """Upgrade both schemas; this is the only supported schema-writing entrypoint."""
    if (
        not expected_migration_role
        or not expected_runtime_role
        or not expected_maintenance_role
        or not expected_auditor_role
        or not target_schema
    ):
        raise RuntimeError("Database bootstrap identity contract is incomplete")
    if (
        len(
            {
                expected_migration_role,
                expected_runtime_role,
                expected_maintenance_role,
                expected_auditor_role,
            }
        )
        != 4
    ):
        raise RuntimeError("Database bootstrap identity contract is invalid")
    await require_migration_database_role(
        dsn,
        expected_migration_role=expected_migration_role,
        expected_runtime_role=expected_runtime_role,
        expected_maintenance_role=expected_maintenance_role,
        expected_auditor_role=expected_auditor_role,
        target_schema=target_schema,
        allow_acl_reconciliation=True,
    )
    _require_checkpoint_migration_manifest()
    preexisting_revision = await get_current_database_revision(dsn)
    known_preexisting_revision = preexisting_revision in {
        "20260710_0002",
        "20260710_0003",
        "20260713_0004",
        "20260713_0005",
    }
    await require_database_schema_contract(
        dsn,
        target_schema=target_schema,
        require_complete=False,
        require_business_complete=known_preexisting_revision,
        expected_revision=(
            preexisting_revision if known_preexisting_revision else None
        ),
    )
    checkpoint_count = 0
    if known_preexisting_revision:
        checkpoint_count = await _apply_checkpoint_migrations(dsn, target_schema)
        await require_database_schema_contract(
            dsn,
            target_schema=target_schema,
            require_complete=True,
            expected_revision=preexisting_revision,
        )
    if preexisting_revision == "20260710_0003":
        await _require_empty_event_inbox_for_0004(
            dsn,
            target_schema=target_schema,
        )
    await asyncio.to_thread(_upgrade_business_schema, dsn)
    business_revision = await get_current_database_revision(dsn)
    if business_revision is None or "," in business_revision:
        raise RuntimeError("Business schema revision is unavailable after bootstrap")
    if not known_preexisting_revision:
        checkpoint_count = await _apply_checkpoint_migrations(dsn, target_schema)
    await _apply_database_access_contract(
        dsn,
        target_schema=target_schema,
        runtime_role=expected_runtime_role,
        maintenance_role=expected_maintenance_role,
        auditor_role=expected_auditor_role,
        business_revision=business_revision,
    )
    await require_database_schema_contract(
        dsn,
        target_schema=target_schema,
        require_complete=True,
        expected_revision=business_revision,
    )
    await require_migration_database_role(
        dsn,
        expected_migration_role=expected_migration_role,
        expected_runtime_role=expected_runtime_role,
        expected_maintenance_role=expected_maintenance_role,
        expected_auditor_role=expected_auditor_role,
        target_schema=target_schema,
    )
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
                expected_maintenance_role=settings.expected_maintenance_role,
                expected_auditor_role=settings.expected_auditor_role,
                target_schema=settings.target_schema,
            )
        )
    except Exception:
        raise RuntimeError("database_bootstrap_failed") from None


if __name__ == "__main__":
    main()
