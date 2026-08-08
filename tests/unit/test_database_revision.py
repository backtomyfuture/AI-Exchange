"""Runtime revision gates for the one greenfield database baseline."""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.db import schema as database_schema
from src.db.runtime_boundary import require_runtime_database_boundary
from src.db.schema import (
    EXPECTED_DATABASE_REVISION,
    DatabaseRevisionError,
    get_current_database_revision,
    require_current_database,
)
from src.db.roles import DatabaseRoleError


ROLE_BOUNDARY = {
    "role_separation_required": True,
    "expected_runtime_role": "runtime_user",
    "expected_migration_role": "migration_owner",
    "expected_maintenance_role": "maintenance_user",
    "expected_auditor_role": "checkpoint_auditor",
    "target_schema": "public",
}


def test_runtime_accepts_only_the_greenfield_baseline_revision() -> None:
    assert EXPECTED_DATABASE_REVISION == "20260808_0001"
    assert database_schema.RUNTIME_COMPATIBLE_DATABASE_REVISIONS == frozenset(
        {EXPECTED_DATABASE_REVISION}
    )


class _RevisionCursor:
    def __init__(self, rows: list[tuple[str]], relation: tuple[str | None]):
        self.rows = rows
        self.relation = relation
        self.statements: list[str] = []

    async def execute(self, statement: str) -> None:
        self.statements.append(statement)

    async def fetchall(self) -> list[tuple[str]]:
        return self.rows

    async def fetchone(self) -> tuple[str | None]:
        return self.relation

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _RevisionConnection:
    def __init__(self, rows: list[tuple[str]], relation: tuple[str | None]):
        self.cursor_obj = _RevisionCursor(rows, relation)

    def cursor(self):
        return self.cursor_obj

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


@pytest.mark.asyncio
async def test_get_current_database_revision_is_read_only() -> None:
    connection = _RevisionConnection(
        [(EXPECTED_DATABASE_REVISION,)], ("public.alembic_version",)
    )

    with patch(
        "src.db.schema.psycopg.AsyncConnection.connect",
        new=AsyncMock(return_value=connection),
    ) as connect:
        revision = await get_current_database_revision("postgresql://test/test")

    assert revision == EXPECTED_DATABASE_REVISION
    assert connect.await_args.kwargs["autocommit"] is True
    assert connection.cursor_obj.statements == [
        "SELECT pg_catalog.to_regclass('public.alembic_version')",
        "SELECT version_num FROM public.alembic_version",
    ]


@pytest.mark.asyncio
async def test_get_current_database_revision_treats_missing_table_as_unversioned() -> None:
    connection = _RevisionConnection([], (None,))

    with patch(
        "src.db.schema.psycopg.AsyncConnection.connect",
        new=AsyncMock(return_value=connection),
    ):
        revision = await get_current_database_revision("postgresql://test/test")

    assert revision is None
    assert connection.cursor_obj.statements == [
        "SELECT pg_catalog.to_regclass('public.alembic_version')"
    ]


@pytest.mark.asyncio
async def test_current_database_gate_accepts_only_the_baseline() -> None:
    with patch(
        "src.db.schema.get_current_database_revision",
        new=AsyncMock(return_value=EXPECTED_DATABASE_REVISION),
    ):
        await require_current_database("postgresql://test/test")


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [None, "obsolete", "a,b"])
async def test_current_database_gate_rejects_every_other_revision(
    revision: str | None,
) -> None:
    with (
        patch(
            "src.db.schema.get_current_database_revision",
            new=AsyncMock(return_value=revision),
        ),
        pytest.raises(DatabaseRevisionError, match=EXPECTED_DATABASE_REVISION),
    ):
        await require_current_database("postgresql://test/test")


@pytest.mark.asyncio
async def test_runtime_gate_proves_role_revision_and_catalog_in_order() -> None:
    role_gate = AsyncMock()
    revision_gate = AsyncMock(return_value=EXPECTED_DATABASE_REVISION)
    catalog_gate = AsyncMock()
    with (
        patch.object(database_schema, "require_runtime_database_role", new=role_gate),
        patch.object(database_schema, "get_current_database_revision", new=revision_gate),
        patch.object(database_schema, "require_database_schema_contract", new=catalog_gate),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            durable_inbox_enabled=True,
            **ROLE_BOUNDARY,
        )

    role_gate.assert_awaited_once_with(
        "postgresql://test/test",
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )
    revision_gate.assert_awaited_once_with("postgresql://test/test")
    catalog_gate.assert_awaited_once_with(
        "postgresql://test/test",
        target_schema="public",
        require_complete=True,
        expected_revision=EXPECTED_DATABASE_REVISION,
    )


@pytest.mark.asyncio
async def test_runtime_gate_fails_before_database_access_without_role_separation() -> None:
    revision_gate = AsyncMock()
    with (
        patch.object(database_schema, "get_current_database_revision", new=revision_gate),
        pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            durable_inbox_enabled=True,
        )

    revision_gate.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_gate_rejects_a_non_baseline_revision_after_role_preflight() -> None:
    catalog_gate = AsyncMock()
    with (
        patch.object(database_schema, "require_runtime_database_role", new=AsyncMock()),
        patch.object(
            database_schema,
            "get_current_database_revision",
            new=AsyncMock(return_value="obsolete"),
        ),
        patch.object(database_schema, "require_database_schema_contract", new=catalog_gate),
        pytest.raises(DatabaseRevisionError, match=EXPECTED_DATABASE_REVISION),
    ):
        await database_schema.require_runtime_database(
            "postgresql://test/test",
            durable_inbox_enabled=False,
            **ROLE_BOUNDARY,
        )

    catalog_gate.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_boundary_forwards_only_current_runtime_settings() -> None:
    settings = SimpleNamespace(
        database_url="postgresql://test/test",
        DURABLE_INBOX_ENABLED=True,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="public",
    )
    gate = AsyncMock()

    await require_runtime_database_boundary(settings, require_database=gate)

    gate.assert_awaited_once_with(
        "postgresql://test/test",
        durable_inbox_enabled=True,
        **ROLE_BOUNDARY,
    )


def test_runtime_gate_has_no_retired_feature_flags() -> None:
    parameters = inspect.signature(database_schema.require_runtime_database).parameters

    assert "ingestion_shadow_enabled" not in parameters
    assert "sync_reconciliation_enabled" not in parameters
