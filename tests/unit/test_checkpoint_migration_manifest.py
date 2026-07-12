"""Fail-closed compatibility checks for third-party checkpoint DDL."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.db import bootstrap


def test_checkpoint_migration_manifest_accepts_pinned_dependency():
    bootstrap._require_checkpoint_migration_manifest()


@pytest.mark.parametrize("mutation", ["version", "append", "rewrite", "reorder"])
def test_checkpoint_migration_manifest_rejects_dependency_drift(mutation):
    migrations = list(bootstrap.AsyncPostgresSaver.MIGRATIONS)
    version = bootstrap._CHECKPOINT_PACKAGE_VERSION
    if mutation == "version":
        version = "999.0.0"
    elif mutation == "append":
        migrations.append("SELECT 2")
    elif mutation == "rewrite":
        migrations[1] = f"{migrations[1]}\nSELECT 2"
    else:
        migrations[1], migrations[2] = migrations[2], migrations[1]

    with (
        patch.object(
            bootstrap,
            "package_version",
            return_value=version,
        ),
        patch.object(
            bootstrap.AsyncPostgresSaver,
            "MIGRATIONS",
            migrations,
        ),
        pytest.raises(
            bootstrap.CheckpointMigrationCompatibilityError,
            match="checkpoint_migration_manifest_invalid",
        ),
    ):
        bootstrap._require_checkpoint_migration_manifest()


@pytest.mark.asyncio
async def test_bootstrap_checks_checkpoint_manifest_before_first_schema_write():
    manifest_error = bootstrap.CheckpointMigrationCompatibilityError(
        "checkpoint_migration_manifest_invalid"
    )
    with (
        patch.object(
            bootstrap,
            "require_migration_database_role",
            new=AsyncMock(),
        ),
        patch.object(
            bootstrap,
            "require_database_schema_contract",
            new=AsyncMock(),
        ),
        patch.object(
            bootstrap,
            "_require_checkpoint_migration_manifest",
            side_effect=manifest_error,
        ),
        patch.object(
            bootstrap,
            "_upgrade_business_schema",
            side_effect=AssertionError("schema write reached after manifest failure"),
        ) as upgrade,
        pytest.raises(
            bootstrap.CheckpointMigrationCompatibilityError,
            match="checkpoint_migration_manifest_invalid",
        ),
    ):
        await bootstrap.bootstrap_database(
            "postgresql://migration/private",
            expected_migration_role="migration_owner",
            expected_runtime_role="runtime_user",
            target_schema="public",
        )

    upgrade.assert_not_called()
