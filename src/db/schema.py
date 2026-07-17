"""Read-only runtime checks for the deployed business schema revision."""

from __future__ import annotations

import psycopg

from src.db.roles import DatabaseRoleError, require_runtime_database_role
from src.db.schema_contract import require_database_schema_contract


EXPECTED_DATABASE_REVISION = "20260716_0006"
RUNTIME_COMPATIBLE_DATABASE_REVISIONS = frozenset({EXPECTED_DATABASE_REVISION})


class DatabaseRevisionError(RuntimeError):
    """Raised when the runtime database is not at the expected revision."""


async def get_current_database_revision(dsn: str) -> str | None:
    """Read the complete Alembic revision set without changing database state."""
    try:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT version_num FROM alembic_version")
                rows = await cur.fetchall()
    except psycopg.errors.UndefinedTable:
        return None

    revisions = sorted(row[0] for row in rows)
    if not revisions:
        return None
    return ",".join(revisions)


async def require_current_database(dsn: str) -> None:
    """Fail readiness unless the database has exactly the expected revision."""
    await _require_database_revision(dsn, frozenset({EXPECTED_DATABASE_REVISION}))


async def require_runtime_database(
    dsn: str,
    *,
    durable_inbox_enabled: bool,
    ingestion_shadow_enabled: bool,
    sync_reconciliation_enabled: bool,
    role_separation_required: bool = False,
    expected_runtime_role: str = "",
    expected_migration_role: str = "",
    expected_maintenance_role: str = "",
    expected_auditor_role: str = "",
    target_schema: str = "public",
) -> None:
    """Require the one greenfield schema and reject unavailable capabilities."""
    if sync_reconciliation_enabled:
        raise DatabaseRevisionError("sync_reconciliation_capability_unavailable")

    if not role_separation_required:
        raise DatabaseRoleError("database_role_preflight_failed")

    # Retained keyword arguments are a call-shape compatibility seam only. They
    # cannot select a predecessor revision or mint database authority.
    _ = durable_inbox_enabled, ingestion_shadow_enabled
    await require_runtime_database_role(
        dsn,
        expected_runtime_role=expected_runtime_role,
        expected_migration_role=expected_migration_role,
        expected_maintenance_role=expected_maintenance_role,
        expected_auditor_role=expected_auditor_role,
        target_schema=target_schema,
    )
    current_revision = await _require_database_revision(
        dsn,
        RUNTIME_COMPATIBLE_DATABASE_REVISIONS,
    )
    await require_database_schema_contract(
        dsn,
        target_schema=target_schema,
        require_complete=True,
        expected_revision=current_revision,
    )


async def _require_database_revision(
    dsn: str,
    allowed_revisions: frozenset[str],
) -> str:
    """Fail closed unless the database has one exact allowed Alembic head."""
    current_revision = await get_current_database_revision(dsn)
    if current_revision in allowed_revisions:
        return current_revision

    found = current_revision or "unversioned"
    expected = ", ".join(sorted(allowed_revisions))
    raise DatabaseRevisionError(
        "Database schema revision mismatch: "
        f"expected one of [{expected}], found {found}. "
        "Run `python -m src.db.bootstrap` before starting the service."
    )
