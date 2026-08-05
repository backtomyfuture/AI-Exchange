from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.db import schema as database_schema
from src.db.schema import (
    EXPECTED_DATABASE_REVISION,
    DatabaseRevisionError,
    get_current_database_revision,
    require_current_database,
)
from src.db.roles import DatabaseRoleError
from src.db.runtime_boundary import require_runtime_database_boundary


_PHASE_2_FLAGS_DISABLED = {
    "durable_inbox_enabled": False,
    "ingestion_shadow_enabled": False,
    "sync_reconciliation_enabled": False,
}

_ROLE_BOUNDARY = {
    "role_separation_required": True,
    "expected_runtime_role": "runtime_user",
    "expected_migration_role": "migration_owner",
    "expected_maintenance_role": "maintenance_user",
    "expected_auditor_role": "checkpoint_auditor",
    "target_schema": "public",
}


def test_runtime_revision_is_exact_polling_only_0007_head() -> None:
    assert EXPECTED_DATABASE_REVISION == "20260728_0007"
    assert database_schema.RUNTIME_COMPATIBLE_DATABASE_REVISIONS == frozenset(
        {EXPECTED_DATABASE_REVISION}
    )


class _RevisionCursor:
    def __init__(self, row, *, relation: tuple[str | None] | None = None):
        self.row = row
        self.relation = relation
        self.statements: list[str] = []

    async def execute(self, statement: str) -> None:
        self.statements.append(statement)

    async def fetchall(self):
        return self.row

    async def fetchone(self):
        return self.relation

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
    connection.cursor_obj.relation = ("public.alembic_version",)

    async def connect(*_args, **kwargs):
        assert kwargs["autocommit"] is True
        return connection

    with patch("src.db.schema.psycopg.AsyncConnection.connect", side_effect=connect):
        revision = await get_current_database_revision("postgresql://test/test")

    assert revision == EXPECTED_DATABASE_REVISION
    assert connection.cursor_obj.statements == [
        "SELECT pg_catalog.to_regclass('public.alembic_version')",
        "SELECT version_num FROM public.alembic_version",
    ]


@pytest.mark.asyncio
async def test_get_current_database_revision_skips_missing_table_without_querying_it():
    connection = _RevisionConnection([],)
    connection.cursor_obj.relation = (None,)

    async def connect(*_args, **_kwargs):
        return connection

    with patch("src.db.schema.psycopg.AsyncConnection.connect", side_effect=connect):
        revision = await get_current_database_revision("postgresql://test/test")

    assert revision is None
    assert connection.cursor_obj.statements == [
        "SELECT pg_catalog.to_regclass('public.alembic_version')"
    ]


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
async def test_runtime_gate_accepts_only_0007_when_flags_are_disabled() -> None:
    schema_contract = AsyncMock()
    with (
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=AsyncMock(),
        ),
        patch.object(
            database_schema,
            "get_current_database_revision",
            new=AsyncMock(return_value=EXPECTED_DATABASE_REVISION),
        ),
        patch.object(
            database_schema,
            "require_database_schema_contract",
            new=schema_contract,
        ),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **_PHASE_2_FLAGS_DISABLED,
            **_ROLE_BOUNDARY,
        )

    schema_contract.assert_awaited_once_with(
        "postgresql://test/test",
        target_schema="public",
        require_complete=True,
        expected_revision=EXPECTED_DATABASE_REVISION,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "legacy_revision",
    [
        "20260710_0002",
        "20260710_0003",
        "20260713_0004",
        "20260713_0005",
        "20260716_0006",
    ],
)
async def test_runtime_gate_rejects_every_legacy_revision(
    legacy_revision: str,
) -> None:
    with (
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=AsyncMock(),
        ),
        patch.object(
            database_schema,
            "get_current_database_revision",
            new=AsyncMock(return_value=legacy_revision),
        ),
        pytest.raises(DatabaseRevisionError, match=EXPECTED_DATABASE_REVISION),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **_PHASE_2_FLAGS_DISABLED,
            **_ROLE_BOUNDARY,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "enabled_flag",
    [
        "durable_inbox_enabled",
        "ingestion_shadow_enabled",
    ],
)
async def test_runtime_flags_do_not_select_a_different_revision(
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
            new=AsyncMock(return_value=EXPECTED_DATABASE_REVISION),
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
        expected_revision=EXPECTED_DATABASE_REVISION,
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
            new=AsyncMock(return_value="20260713_0005"),
        ),
        pytest.raises(DatabaseRevisionError, match="python -m src.db.bootstrap"),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **flags,
            **_ROLE_BOUNDARY,
        )


@pytest.mark.asyncio
async def test_runtime_gate_rejects_sync_before_capability_verifier_without_db_access():
    flags = {**_PHASE_2_FLAGS_DISABLED, "sync_reconciliation_enabled": True}
    role_gate = AsyncMock()
    revision_gate = AsyncMock(return_value="20260710_0003")
    schema_contract = AsyncMock()

    with (
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=role_gate,
        ),
        patch.object(
            database_schema,
            "get_current_database_revision",
            new=revision_gate,
        ),
        patch.object(
            database_schema,
            "require_database_schema_contract",
            new=schema_contract,
        ),
        pytest.raises(
            DatabaseRevisionError,
            match="sync_reconciliation_capability_unavailable",
        ),
    ):
        await database_schema.require_runtime_database(
            "postgresql://private:secret@database/email_agent",
            **flags,
            **_ROLE_BOUNDARY,
        )

    role_gate.assert_not_awaited()
    revision_gate.assert_not_awaited()
    schema_contract.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_and_readiness_boundaries_fail_closed_for_unverified_sync():
    from src import server as server_module

    settings = SimpleNamespace(
        database_url="postgresql://private:secret@database/email_agent",
        DURABLE_INBOX_ENABLED=False,
        INGESTION_SHADOW_ENABLED=False,
        SYNC_RECONCILIATION_ENABLED=True,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="public",
    )
    connect = AsyncMock(
        side_effect=AssertionError("unverified_sync_must_not_connect_to_database")
    )
    server_module._READINESS_STATES.clear()
    try:
        with patch.object(
            database_schema.psycopg.AsyncConnection,
            "connect",
            new=connect,
        ):
            with pytest.raises(
                DatabaseRevisionError,
                match="sync_reconciliation_capability_unavailable",
            ):
                await require_runtime_database_boundary(settings)
            with pytest.raises(
                DatabaseRevisionError,
                match="sync_reconciliation_capability_unavailable",
            ):
                await server_module._require_cached_runtime_database(settings)
    finally:
        server_module._READINESS_STATES.clear()

    connect.assert_not_awaited()


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
        patch.object(
            database_schema,
            "require_runtime_database_role",
            new=AsyncMock(),
        ),
        patch(
            "src.db.schema.get_current_database_revision",
            new=AsyncMock(return_value=current_revision),
        ),
        pytest.raises(DatabaseRevisionError, match="python -m src.db.bootstrap"),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            **_PHASE_2_FLAGS_DISABLED,
            **_ROLE_BOUNDARY,
        )


@pytest.mark.asyncio
async def test_runtime_gate_requires_role_separation_even_when_flags_are_disabled():
    revision_reader = AsyncMock(return_value=EXPECTED_DATABASE_REVISION)

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
            **_PHASE_2_FLAGS_DISABLED,
            role_separation_required=False,
        )

    revision_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_gate_checks_role_invariants_before_revision():
    call_order: list[str] = []

    async def record_role(*_args, **_kwargs) -> None:
        call_order.append("role")

    async def record_revision(*_args, **_kwargs) -> str:
        call_order.append("revision")
        return EXPECTED_DATABASE_REVISION

    async def record_schema_contract(*_args, **_kwargs) -> None:
        call_order.append("schema_contract")

    role_gate = AsyncMock(side_effect=record_role)
    revision_reader = AsyncMock(side_effect=record_revision)
    schema_contract = AsyncMock(side_effect=record_schema_contract)

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
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )
    revision_reader.assert_awaited_once_with("postgresql://runtime/private")
    schema_contract.assert_awaited_once_with(
        "postgresql://runtime/private",
        target_schema="public",
        require_complete=True,
        expected_revision=EXPECTED_DATABASE_REVISION,
    )
    assert call_order == ["role", "revision", "schema_contract"]


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
            expected_maintenance_role="maintenance_user",
            target_schema="public",
        )

    revision_reader.assert_not_awaited()


def test_database_manager_has_no_runtime_schema_initializer():
    from src.utils.db_async import AsyncDatabaseManager

    assert not hasattr(AsyncDatabaseManager, "_init_db")


@pytest.mark.asyncio
async def test_lifespan_checks_revision_before_context_setup():
    from src import server as server_module

    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        database_url="postgresql://test/test",
        DURABLE_INBOX_ENABLED=True,
        INGESTION_SHADOW_ENABLED=False,
        SYNC_RECONCILIATION_ENABLED=True,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="public",
    )
    context = MagicMock()
    revision_error = DatabaseRevisionError("database schema is stale")
    runtime_revision_check = AsyncMock(side_effect=revision_error)

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            new=runtime_revision_check,
        ) as revision_check,
        patch.object(
            server_module,
            "get_runtime_app_context",
            return_value=context,
        ),
    ):
        with pytest.raises(DatabaseRevisionError, match="stale"):
            async with server_module.application_lifespan(server_module.app):
                pass

    revision_check.assert_awaited_once_with(settings)
    context.create_ingestion_runtime.assert_not_called()
