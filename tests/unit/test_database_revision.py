from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import psycopg

from src.db import schema as database_schema
from src.db.schema import (
    EXPECTED_DATABASE_REVISION,
    DatabaseRevisionError,
    get_current_database_revision,
    require_current_database,
)
from src.db.roles import DatabaseRoleError


_PHASE_2_FLAGS_DISABLED = {
    "durable_inbox_enabled": False,
    "ingestion_shadow_enabled": False,
    "sync_reconciliation_enabled": False,
}

_ROLE_BOUNDARY = {
    "role_separation_required": True,
    "expected_runtime_role": "runtime_user",
    "expected_migration_role": "migration_owner",
    "target_schema": "public",
}


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
    with (
        patch(
            "src.db.schema.get_current_database_revision",
            new=AsyncMock(return_value=current_revision),
        ),
        pytest.raises(DatabaseRevisionError, match="python -m src.db.bootstrap"),
    ):
        await require_current_database("postgresql://test/test")


@pytest.mark.asyncio
@pytest.mark.parametrize("current_revision", ["20260710_0002", "20260710_0003"])
async def test_runtime_gate_accepts_code_first_and_migration_first_when_flags_disabled(
    current_revision,
):
    with patch(
        "src.db.schema.get_current_database_revision",
        new=AsyncMock(return_value=current_revision),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **_PHASE_2_FLAGS_DISABLED,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "enabled_flag",
    [
        "durable_inbox_enabled",
        "ingestion_shadow_enabled",
        "sync_reconciliation_enabled",
    ],
)
async def test_runtime_gate_requires_expand_revision_when_any_phase_2_flag_is_enabled(
    enabled_flag,
):
    flags = {**_PHASE_2_FLAGS_DISABLED, enabled_flag: True}
    schema_contract = AsyncMock()
    with (
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=AsyncMock(),
            create=True,
        ),
        patch(
            "src.db.schema.get_current_database_revision",
            new=AsyncMock(return_value="20260710_0003"),
        ),
        patch.object(
            database_schema,
            "require_database_schema_contract",
            new=schema_contract,
        ),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **flags,
            **_ROLE_BOUNDARY,
        )
    schema_contract.assert_awaited_once_with(
        "postgresql://test/test",
        target_schema="public",
        require_complete=True,
    )

    with (
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=AsyncMock(),
            create=True,
        ),
        patch(
            "src.db.schema.get_current_database_revision",
            new=AsyncMock(return_value="20260710_0002"),
        ),
        pytest.raises(DatabaseRevisionError, match="python -m src.db.bootstrap"),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **flags,
            **_ROLE_BOUNDARY,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "current_revision",
    [None, "20260710_9999", "20260710_0002,20260710_0003"],
    ids=["unversioned", "unknown", "multiple-heads"],
)
async def test_runtime_gate_rejects_unversioned_unknown_and_multiple_heads(
    current_revision,
):
    with (
        patch(
            "src.db.schema.get_current_database_revision",
            new=AsyncMock(return_value=current_revision),
        ),
        pytest.raises(DatabaseRevisionError, match="python -m src.db.bootstrap"),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **_PHASE_2_FLAGS_DISABLED,
        )


@pytest.mark.asyncio
async def test_runtime_gate_checks_role_invariants_before_revision():
    role_gate = AsyncMock()
    revision_reader = AsyncMock(return_value=EXPECTED_DATABASE_REVISION)
    schema_contract = AsyncMock()

    with (
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=role_gate,
            create=True,
        ),
        patch.object(
            database_schema,
            "get_current_database_revision",
            new=revision_reader,
        ),
        patch.object(
            database_schema,
            "require_database_schema_contract",
            new=schema_contract,
        ),
    ):
        await database_schema.require_runtime_database(
            "postgresql://runtime/private",
            **_PHASE_2_FLAGS_DISABLED,
            **_ROLE_BOUNDARY,
        )

    role_gate.assert_awaited_once_with(
        "postgresql://runtime/private",
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        target_schema="public",
    )
    revision_reader.assert_awaited_once_with("postgresql://runtime/private")
    schema_contract.assert_awaited_once_with(
        "postgresql://runtime/private",
        target_schema="public",
        require_complete=True,
    )


@pytest.mark.asyncio
async def test_runtime_role_failure_prevents_revision_read():
    role_gate = AsyncMock(
        side_effect=DatabaseRoleError("database_role_preflight_failed")
    )
    revision_reader = AsyncMock(return_value=EXPECTED_DATABASE_REVISION)

    with (
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=role_gate,
            create=True,
        ),
        patch.object(
            database_schema,
            "get_current_database_revision",
            new=revision_reader,
        ),
        pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await database_schema.require_runtime_database(
            "postgresql://runtime/private",
            **_PHASE_2_FLAGS_DISABLED,
            **_ROLE_BOUNDARY,
        )

    revision_reader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "enabled_flag",
    [
        "durable_inbox_enabled",
        "ingestion_shadow_enabled",
        "sync_reconciliation_enabled",
    ],
)
async def test_phase_2_activation_requires_role_separation_even_on_expand_head(
    enabled_flag: str,
):
    revision_reader = AsyncMock(return_value="20260710_0003")
    flags = {**_PHASE_2_FLAGS_DISABLED, enabled_flag: True}
    with (
        patch.object(
            database_schema,
            "get_current_database_revision",
            new=revision_reader,
        ),
        pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await database_schema.require_runtime_database(
            "postgresql://runtime/private",
            **flags,
            role_separation_required=False,
            expected_runtime_role="runtime_user",
            expected_migration_role="migration_owner",
            target_schema="public",
        )

    revision_reader.assert_not_awaited()


def test_database_manager_has_no_runtime_schema_initializer():
    from src.utils.db_async import AsyncDatabaseManager

    assert not hasattr(AsyncDatabaseManager, "_init_db")


@pytest.mark.asyncio
async def test_lifespan_checks_revision_before_context_setup():
    from src import main as main_module

    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        database_url="postgresql://test/test",
        DURABLE_INBOX_ENABLED=True,
        INGESTION_SHADOW_ENABLED=False,
        SYNC_RECONCILIATION_ENABLED=True,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_SCHEMA="public",
    )
    context = MagicMock()
    context.setup_async = AsyncMock()
    revision_error = DatabaseRevisionError("database schema is stale")
    runtime_revision_check = AsyncMock(side_effect=revision_error)
    legacy_revision_check = AsyncMock(
        side_effect=AssertionError("legacy_revision_gate_was_called")
    )

    with (
        patch.object(main_module, "get_settings", return_value=settings),
        patch.object(
            main_module,
            "require_runtime_database",
            new=runtime_revision_check,
            create=True,
        ) as revision_check,
        patch.object(
            main_module,
            "require_current_database",
            new=legacy_revision_check,
            create=True,
        ),
        patch.object(main_module, "get_app_context", return_value=context),
    ):
        with pytest.raises(DatabaseRevisionError, match="stale"):
            async with main_module.lifespan(main_module.app):
                pass

    revision_check.assert_awaited_once_with(
        settings.database_url,
        durable_inbox_enabled=True,
        ingestion_shadow_enabled=False,
        sync_reconciliation_enabled=True,
        role_separation_required=True,
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        target_schema="public",
    )
    legacy_revision_check.assert_not_awaited()
    context.setup_async.assert_not_awaited()
