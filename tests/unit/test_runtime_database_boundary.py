from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.db.runtime_boundary import require_runtime_database_boundary


pytestmark = pytest.mark.asyncio


async def test_shared_runtime_boundary_maps_all_security_identity_fields() -> None:
    settings = SimpleNamespace(
        database_url="postgresql://runtime:PRIVATE@db/email",
        DURABLE_INBOX_ENABLED=True,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="runtime_schema",
    )
    gate = AsyncMock()

    await require_runtime_database_boundary(settings, require_database=gate)

    gate.assert_awaited_once_with(
        settings.database_url,
        durable_inbox_enabled=True,
        role_separation_required=True,
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="runtime_schema",
    )


async def test_shared_runtime_boundary_defaults_fail_closed_identity_to_empty() -> None:
    settings = SimpleNamespace(database_url="postgresql://runtime:PRIVATE@db/email")
    gate = AsyncMock()

    await require_runtime_database_boundary(settings, require_database=gate)

    gate.assert_awaited_once_with(
        settings.database_url,
        durable_inbox_enabled=False,
        role_separation_required=False,
        expected_runtime_role="",
        expected_migration_role="",
        expected_maintenance_role="",
        expected_auditor_role="",
        target_schema="public",
    )
