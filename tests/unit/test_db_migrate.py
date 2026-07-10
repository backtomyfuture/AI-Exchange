from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.db import migrate


@pytest.mark.asyncio
async def test_run_migrations_delegates_to_explicit_bootstrap():
    expected = {"alembic": "20260710_0002", "checkpoint": 9}

    with patch.object(
        migrate,
        "bootstrap_database",
        new=AsyncMock(return_value=expected),
    ) as bootstrap:
        result = await migrate.run_migrations("postgresql://test/test")

    assert result == expected
    bootstrap.assert_awaited_once_with("postgresql://test/test")


@pytest.mark.asyncio
async def test_compatibility_wrapper_never_reads_filesystem_sql(tmp_path):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    (migration_dir / "999_should_not_run.sql").write_text(
        "RAISE AN ERROR IF THIS IS READ", encoding="utf-8"
    )

    with patch.object(
        migrate,
        "bootstrap_database",
        new=AsyncMock(return_value={"alembic": "20260710_0002", "checkpoint": 9}),
    ) as bootstrap, patch("builtins.open", side_effect=AssertionError("SQL read")):
        await migrate.run_migrations(
            "postgresql://test/test",
            migrations_dir=str(migration_dir),
            apply_checkpoint_migrations=False,
        )

    bootstrap.assert_awaited_once_with("postgresql://test/test")
