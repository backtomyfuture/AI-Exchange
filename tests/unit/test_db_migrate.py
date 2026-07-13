from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import SecretStr

from src.db import migrate
from src.db.migration_settings import MigrationSettings


MIGRATION_DSN = (
    "postgresql://migration_owner:private@postgres/email_agent"
    "?options=-csearch_path%3Dpublic"
)


def _migration_settings() -> MigrationSettings:
    return MigrationSettings(
        database_url=SecretStr(MIGRATION_DSN),
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )


@pytest.mark.asyncio
async def test_run_migrations_delegates_to_explicit_bootstrap():
    expected = {"alembic": "20260710_0002", "checkpoint": 9}

    with (
        patch.object(
            migrate,
            "bootstrap_database",
            new=AsyncMock(return_value=expected),
        ) as bootstrap,
        patch.object(
            migrate,
            "load_migration_settings",
            return_value=_migration_settings(),
            create=True,
        ) as loader,
    ):
        result = await migrate.run_migrations()

    assert result == expected
    loader.assert_called_once_with()
    bootstrap.assert_awaited_once_with(
        MIGRATION_DSN,
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )


@pytest.mark.asyncio
async def test_compatibility_wrapper_never_reads_filesystem_sql(tmp_path):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "999_should_not_run.sql").write_text(
        "RAISE AN ERROR IF THIS IS READ", encoding="utf-8"
    )

    with (
        patch.object(
            migrate,
            "bootstrap_database",
            new=AsyncMock(return_value={"alembic": "20260710_0002", "checkpoint": 9}),
        ) as bootstrap,
        patch.object(
            migrate,
            "load_migration_settings",
            return_value=_migration_settings(),
            create=True,
        ),
        patch("builtins.open", side_effect=AssertionError("SQL read")),
    ):
        await migrate.run_migrations(
            migrations_dir=str(migration_dir),
            apply_checkpoint_migrations=False,
        )

    bootstrap.assert_awaited_once_with(
        MIGRATION_DSN,
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )


@pytest.mark.asyncio
async def test_compatibility_wrapper_rejects_caller_supplied_dsn():
    with pytest.raises(TypeError):
        await migrate.run_migrations("postgresql://runtime/private")


@pytest.mark.asyncio
async def test_compatibility_wrapper_cuts_off_secret_bearing_failure():
    bootstrap = AsyncMock(side_effect=RuntimeError(f"failed {MIGRATION_DSN}"))
    with (
        patch.object(
            migrate,
            "bootstrap_database",
            new=bootstrap,
        ),
        patch.object(
            migrate,
            "load_migration_settings",
            return_value=_migration_settings(),
        ),
        pytest.raises(RuntimeError) as caught,
    ):
        await migrate.run_migrations()

    assert str(caught.value) == "database_bootstrap_failed"
    assert MIGRATION_DSN not in str(caught.value)
    assert caught.value.__cause__ is None
