from __future__ import annotations

import pytest

from src.db.schema_contract import GREENFIELD_DATABASE_REVISION, require_database_schema_contract


@pytest.mark.integration
@pytest.mark.asyncio
async def test_empty_database_accepts_the_one_supported_polling_baseline(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, GREENFIELD_DATABASE_REVISION)

    await require_database_schema_contract(
        empty_schema.dsn,
        target_schema="public",
        require_complete=False,
        require_business_complete=True,
        expected_revision=GREENFIELD_DATABASE_REVISION,
    )
