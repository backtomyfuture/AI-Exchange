"""
Unit tests for ``src.db.migrate``.

We avoid spinning up real PostgreSQL by mocking ``psycopg.AsyncConnection.connect``
and the cursor's ``execute``/``fetchall``. The tests focus on the orchestration
behaviour: ordering, idempotency, autocommit isolation and graceful handling of
missing migrations directories.
"""

from __future__ import annotations

import os
import tempfile
import textwrap
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.db import migrate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeCursor:
    """Minimal async cursor that records executed SQL and returns canned rows."""

    def __init__(self, fetchall_responses: list):
        self.executed: list = []
        self._responses = list(fetchall_responses)
        self._last_sql = ""

    async def execute(self, sql: str, params=None):
        self._last_sql = sql.strip()
        self.executed.append((self._last_sql, params))

    async def fetchall(self):
        if self._responses:
            return self._responses.pop(0)
        return []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeConnection:
    """Async connection stub that hands out FakeCursor instances."""

    def __init__(self, fetchall_responses=None):
        self.cursor_obj = FakeCursor(fetchall_responses or [])

    def cursor(self):
        return self.cursor_obj

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _patched_connect(conn: FakeConnection):
    """Patch psycopg.AsyncConnection.connect to return our FakeConnection.

    psycopg's connect is awaitable and returns an async-context-manager.
    """

    async def _connect(*args, **kwargs):
        # autocommit must be True per migrate.run_migrations contract
        assert kwargs.get("autocommit") is True
        return conn

    return _connect


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_migrations_skips_missing_directory(tmp_path):
    """If migrations dir does not exist we still create schema_migrations + checkpoint."""
    fake_conn = FakeConnection(
        fetchall_responses=[
            [],  # SELECT v FROM checkpoint_migrations -> empty
        ]
    )
    fake_conn.cursor_obj._responses.insert(0, [])  # SELECT version FROM schema_migrations

    with patch(
        "psycopg.AsyncConnection.connect",
        side_effect=_patched_connect(fake_conn),
    ), patch.object(
        migrate, "_apply_checkpoint_migrations", new=AsyncMock(return_value=0)
    ):
        result = await migrate.run_migrations(
            "postgresql://x/y", migrations_dir=str(tmp_path / "does_not_exist"),
        )

    assert result == {"user": 0, "checkpoint": 0}


@pytest.mark.asyncio
async def test_run_migrations_applies_pending_files_in_order(tmp_path):
    """002 should be applied before 003 even if listed in reverse on disk."""
    sql_dir = tmp_path / "migrations"
    sql_dir.mkdir()
    (sql_dir / "003_later.sql").write_text("CREATE TABLE later();")
    (sql_dir / "001_init.sql").write_text("CREATE TABLE init_t();")
    (sql_dir / "002_middle.sql").write_text("CREATE TABLE mid_t();")

    fake_conn = FakeConnection(fetchall_responses=[[]])  # no rows applied yet

    with patch(
        "psycopg.AsyncConnection.connect",
        side_effect=_patched_connect(fake_conn),
    ), patch.object(
        migrate, "_apply_checkpoint_migrations", new=AsyncMock(return_value=0)
    ):
        result = await migrate.run_migrations(
            "postgresql://x/y", migrations_dir=str(sql_dir)
        )

    assert result["user"] == 3
    # Ensure SQL files were executed in lexical order (exclude bootstrap DDL)
    file_executions = [
        sql for sql, _ in fake_conn.cursor_obj.executed
        if sql.startswith("CREATE TABLE") and "schema_migrations" not in sql
    ]
    assert file_executions[0].startswith("CREATE TABLE init_t")
    assert file_executions[1].startswith("CREATE TABLE mid_t")
    assert file_executions[2].startswith("CREATE TABLE later")


@pytest.mark.asyncio
async def test_run_migrations_is_idempotent(tmp_path):
    """If schema_migrations already lists the file, it is not re-applied."""
    sql_dir = tmp_path / "migrations"
    sql_dir.mkdir()
    (sql_dir / "001_init.sql").write_text("CREATE TABLE init_t();")
    (sql_dir / "002_new.sql").write_text("CREATE TABLE new_t();")

    fake_conn = FakeConnection(
        fetchall_responses=[[("001_init.sql",)]]  # 001 already applied
    )

    with patch(
        "psycopg.AsyncConnection.connect",
        side_effect=_patched_connect(fake_conn),
    ), patch.object(
        migrate, "_apply_checkpoint_migrations", new=AsyncMock(return_value=0)
    ):
        result = await migrate.run_migrations(
            "postgresql://x/y", migrations_dir=str(sql_dir)
        )

    assert result["user"] == 1
    # Filter out bootstrap DDL; only one user migration should run.
    file_executions = [
        sql for sql, _ in fake_conn.cursor_obj.executed
        if sql.startswith("CREATE TABLE") and "schema_migrations" not in sql
    ]
    assert file_executions == ["CREATE TABLE new_t();"]


@pytest.mark.asyncio
async def test_apply_checkpoint_migrations_skips_already_applied():
    """Versions present in checkpoint_migrations are not re-executed."""
    fake_migrations = [
        "CREATE TABLE IF NOT EXISTS checkpoint_migrations(v INT PRIMARY KEY);",
        "SELECT 1;",  # v=1
        "SELECT 2;",  # v=2
        "SELECT 3;",  # v=3
    ]

    # The mock for AsyncPostgresSaver.MIGRATIONS
    fake_module = MagicMock()
    fake_module.AsyncPostgresSaver.MIGRATIONS = fake_migrations

    fake_conn = FakeConnection(fetchall_responses=[[(1,)]])  # v=1 already applied

    with patch.dict(
        "sys.modules",
        {"langgraph.checkpoint.postgres.aio": fake_module},
    ):
        count = await migrate._apply_checkpoint_migrations(fake_conn)

    # Migrations 2 and 3 applied, plus the initial bootstrap (v=0). Bootstrap is
    # always run but not counted (it's just a CREATE TABLE IF NOT EXISTS).
    assert count == 2

    sqls = [sql for sql, _ in fake_conn.cursor_obj.executed]
    # v=1 must NOT be in the executed list (already applied), but v=2/v=3 must.
    assert "SELECT 1;" not in sqls
    assert "SELECT 2;" in sqls
    assert "SELECT 3;" in sqls


@pytest.mark.asyncio
async def test_run_migrations_propagates_user_migration_error(tmp_path):
    """A failing user migration must abort and leave the version unrecorded."""
    sql_dir = tmp_path / "migrations"
    sql_dir.mkdir()
    (sql_dir / "001_bad.sql").write_text("INVALID SQL")

    class FailingCursor(FakeCursor):
        async def execute(self, sql, params=None):
            self.executed.append((sql.strip(), params))
            if "INVALID SQL" in sql:
                raise RuntimeError("boom")

    fake_conn = FakeConnection(fetchall_responses=[[]])
    fake_conn.cursor_obj = FailingCursor([])

    with patch(
        "psycopg.AsyncConnection.connect",
        side_effect=_patched_connect(fake_conn),
    ), patch.object(
        migrate, "_apply_checkpoint_migrations", new=AsyncMock(return_value=0)
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await migrate.run_migrations(
                "postgresql://x/y", migrations_dir=str(sql_dir)
            )


@pytest.mark.asyncio
async def test_run_migrations_uses_autocommit():
    """Connect must be called with autocommit=True for CREATE INDEX CONCURRENTLY."""
    seen_kwargs = {}

    async def _connect(*args, **kwargs):
        seen_kwargs.update(kwargs)
        fake = FakeConnection(fetchall_responses=[[], []])
        return fake

    with patch("psycopg.AsyncConnection.connect", side_effect=_connect), \
         patch.object(migrate, "_apply_checkpoint_migrations",
                      new=AsyncMock(return_value=0)):
        await migrate.run_migrations(
            "postgresql://x/y", migrations_dir=os.path.join(tempfile.gettempdir(),
                                                            "nonexistent_migrations_dir"),
        )

    assert seen_kwargs.get("autocommit") is True
