from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import psycopg

from src.db.schema import (
    EXPECTED_DATABASE_REVISION,
    DatabaseRevisionError,
    get_current_database_revision,
    require_current_database,
)


class _RevisionCursor:
    def __init__(self, row):
        self.row = row
        self.statements: list[str] = []

    async def execute(self, statement: str) -> None:
        self.statements.append(statement)

    async def fetchall(self):
        return self.row

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _RevisionConnection:
    def __init__(self, row):
        self.cursor_obj = _RevisionCursor(row)

    def cursor(self):
        return self.cursor_obj

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


@pytest.mark.asyncio
async def test_get_current_database_revision_is_read_only():
    connection = _RevisionConnection([(EXPECTED_DATABASE_REVISION,)])

    async def connect(*_args, **kwargs):
        assert kwargs["autocommit"] is True
        return connection

    with patch("src.db.schema.psycopg.AsyncConnection.connect", side_effect=connect):
        revision = await get_current_database_revision("postgresql://test/test")

    assert revision == EXPECTED_DATABASE_REVISION
    assert connection.cursor_obj.statements == [
        "SELECT version_num FROM alembic_version"
    ]


@pytest.mark.asyncio
async def test_get_current_database_revision_treats_missing_table_as_unversioned():
    connection = _RevisionConnection([])

    async def missing_table(_statement: str) -> None:
        raise psycopg.errors.UndefinedTable("alembic_version does not exist")

    connection.cursor_obj.execute = missing_table

    async def connect(*_args, **_kwargs):
        return connection

    with patch("src.db.schema.psycopg.AsyncConnection.connect", side_effect=connect):
        revision = await get_current_database_revision("postgresql://test/test")

    assert revision is None


@pytest.mark.asyncio
async def test_require_current_database_accepts_expected_revision():
    with patch(
        "src.db.schema.get_current_database_revision",
        new=AsyncMock(return_value=EXPECTED_DATABASE_REVISION),
    ):
        await require_current_database("postgresql://test/test")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "current_revision",
    [None, "20260710_0001", "20260710_0001,20260710_0002"],
)
async def test_require_current_database_rejects_missing_or_stale_revision(
    current_revision,
):
    with patch(
        "src.db.schema.get_current_database_revision",
        new=AsyncMock(return_value=current_revision),
    ), pytest.raises(DatabaseRevisionError, match="python -m src.db.bootstrap"):
        await require_current_database("postgresql://test/test")


def test_database_manager_has_no_runtime_schema_initializer():
    from src.utils.db_async import AsyncDatabaseManager

    assert not hasattr(AsyncDatabaseManager, "_init_db")


@pytest.mark.asyncio
async def test_lifespan_checks_revision_before_context_setup():
    from src import main as main_module

    settings = SimpleNamespace(LOG_LEVEL="INFO", database_url="postgresql://test/test")
    context = MagicMock()
    context.setup_async = AsyncMock()
    revision_error = DatabaseRevisionError("database schema is stale")

    with patch.object(main_module, "get_settings", return_value=settings), patch.object(
        main_module,
        "require_current_database",
        new=AsyncMock(side_effect=revision_error),
    ) as revision_check, patch.object(
        main_module, "get_app_context", return_value=context
    ):
        with pytest.raises(DatabaseRevisionError, match="stale"):
            async with main_module.lifespan(main_module.app):
                pass

    revision_check.assert_awaited_once_with(settings.database_url)
    context.setup_async.assert_not_awaited()
