"""Read-only runtime checks for the deployed business schema revision."""

from __future__ import annotations

import psycopg


EXPECTED_DATABASE_REVISION = "20260710_0002"


class DatabaseRevisionError(RuntimeError):
    """Raised when the runtime database is not at the expected revision."""


async def get_current_database_revision(dsn: str) -> str | None:
    """Read the complete Alembic revision set without changing database state."""
    try:
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True
        ) as conn:
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
    current_revision = await get_current_database_revision(dsn)
    if current_revision == EXPECTED_DATABASE_REVISION:
        return

    found = current_revision or "unversioned"
    raise DatabaseRevisionError(
        "Database schema revision mismatch: "
        f"expected {EXPECTED_DATABASE_REVISION}, found {found}. "
        "Run `python -m src.db.bootstrap` before starting the service."
    )
