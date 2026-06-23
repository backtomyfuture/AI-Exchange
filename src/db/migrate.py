"""
Database migration runner for AI Email Assistant.

Runs idempotent schema migrations at startup so that:
1. ``migrations/*.sql`` user migrations are applied in lexical order, tracked
   by a ``schema_migrations`` table.
2. ``langgraph-checkpoint-postgres`` migrations are applied via the autocommit
   workaround documented in ``AGENTS.md``: entries 6-8 use
   ``CREATE INDEX CONCURRENTLY`` which cannot run inside the implicit
   transaction block opened by ``AsyncPostgresSaver.setup()``. Running them
   manually with autocommit avoids the failure on a fresh database.

Both phases are safe to re-run on every boot. Once a version is recorded in
its tracking table the corresponding SQL is skipped.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import psycopg

logger = logging.getLogger(__name__)


# Default location of user-authored SQL migrations relative to repo root.
DEFAULT_MIGRATIONS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "migrations",
)


async def run_migrations(
    dsn: str,
    migrations_dir: Optional[str] = None,
    *,
    apply_checkpoint_migrations: bool = True,
) -> dict:
    """
    Apply pending user SQL migrations and LangGraph checkpoint migrations.

    Args:
        dsn: PostgreSQL DSN. Will be used with ``autocommit=True``.
        migrations_dir: Override path to ``migrations/`` directory. Defaults
            to ``<repo>/migrations``. If the directory does not exist, the user
            phase is silently skipped.
        apply_checkpoint_migrations: If True, also apply
            ``AsyncPostgresSaver.MIGRATIONS`` with autocommit so that
            ``CREATE INDEX CONCURRENTLY`` entries succeed on a fresh DB.

    Returns:
        Dict summarising applied counts: ``{"user": int, "checkpoint": int}``.
    """
    migrations_dir = migrations_dir or DEFAULT_MIGRATIONS_DIR
    summary = {"user": 0, "checkpoint": 0}

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await _ensure_schema_migrations_table(conn)
        summary["user"] = await _apply_user_migrations(conn, migrations_dir)
        if apply_checkpoint_migrations:
            summary["checkpoint"] = await _apply_checkpoint_migrations(conn)

    logger.info(
        "Migrations done: %d user SQL files applied, %d checkpoint migrations applied.",
        summary["user"],
        summary["checkpoint"],
    )
    return summary


async def _ensure_schema_migrations_table(conn: psycopg.AsyncConnection) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


async def _list_applied_versions(conn: psycopg.AsyncConnection) -> set:
    async with conn.cursor() as cur:
        await cur.execute("SELECT version FROM schema_migrations")
        rows = await cur.fetchall()
    return {row[0] for row in rows}


def _list_migration_files(migrations_dir: str) -> list:
    if not os.path.isdir(migrations_dir):
        return []
    files = [
        f for f in os.listdir(migrations_dir)
        if f.endswith(".sql") and not f.startswith(".")
    ]
    files.sort()
    return [os.path.join(migrations_dir, f) for f in files]


async def _apply_user_migrations(
    conn: psycopg.AsyncConnection, migrations_dir: str
) -> int:
    """Apply unseen ``migrations/*.sql`` files in lexical order."""
    files = _list_migration_files(migrations_dir)
    if not files:
        logger.info("No user SQL migrations found at %s", migrations_dir)
        return 0

    applied = await _list_applied_versions(conn)
    count = 0

    for path in files:
        version = os.path.basename(path)
        if version in applied:
            logger.debug("Skipping already-applied migration: %s", version)
            continue

        with open(path, "r", encoding="utf-8") as fh:
            sql = fh.read()

        logger.info("Applying user migration: %s", version)
        try:
            async with conn.cursor() as cur:
                # autocommit=True; each statement commits independently.
                # Migration files may include their own BEGIN/COMMIT pairs which
                # will be ignored by psycopg in autocommit mode (handled per
                # statement by the server).
                await cur.execute(sql)
                await cur.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s) "
                    "ON CONFLICT (version) DO NOTHING",
                    (version,),
                )
            count += 1
        except Exception as exc:
            logger.exception("User migration %s failed: %s", version, exc)
            raise

    return count


async def _apply_checkpoint_migrations(conn: psycopg.AsyncConnection) -> int:
    """
    Apply ``AsyncPostgresSaver.MIGRATIONS`` with autocommit.

    Mirrors the workaround documented in ``AGENTS.md``: runs each migration in
    order, recording version ``i`` in ``checkpoint_migrations`` for ``i > 0``
    so subsequent ``checkpointer.setup()`` calls become no-ops.
    """
    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    except ImportError:
        logger.warning(
            "langgraph not installed; skipping checkpoint migrations. "
            "This is fine for unit tests."
        )
        return 0

    migrations = AsyncPostgresSaver.MIGRATIONS

    # First migration creates checkpoint_migrations table; always run it.
    async with conn.cursor() as cur:
        await cur.execute(migrations[0])

    # Discover already-applied versions.
    applied: set = set()
    async with conn.cursor() as cur:
        await cur.execute("SELECT v FROM checkpoint_migrations")
        rows = await cur.fetchall()
        applied = {row[0] for row in rows}

    count = 0
    for i, migration_sql in enumerate(migrations):
        if i == 0 or i in applied:
            continue

        logger.info("Applying checkpoint migration v=%d", i)
        try:
            async with conn.cursor() as cur:
                await cur.execute(migration_sql)
                await cur.execute(
                    "INSERT INTO checkpoint_migrations (v) VALUES (%s) "
                    "ON CONFLICT (v) DO NOTHING",
                    (i,),
                )
            count += 1
        except Exception as exc:
            logger.exception("Checkpoint migration v=%d failed: %s", i, exc)
            raise

    return count
