from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.ingestion.ownership import PipelineOwnershipRepository
from src.ingestion.repository import InboxRepository


@dataclass(slots=True)
class DurableProcessingRuntime:
    schema: Any
    pool: AsyncConnectionPool
    ownership: PipelineOwnershipRepository
    repository: InboxRepository


@pytest.fixture
def db(empty_schema, alembic_runner):
    """Upgrade one disposable role-separated database to the Alembic head."""

    alembic_runner.upgrade(empty_schema, "head")
    return empty_schema


@pytest_asyncio.fixture
async def durable_processing_runtime(
    postgres_database_factory,
) -> DurableProcessingRuntime:
    """Role-separated runtime for Task-8 aggregate transaction tests."""

    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    pool = AsyncConnectionPool(
        conninfo=schema.runtime_dsn,
        min_size=1,
        max_size=20,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    ownership = PipelineOwnershipRepository(pool)
    await ownership.bootstrap(8, "legacy_compat")
    try:
        yield DurableProcessingRuntime(
            schema=schema,
            pool=pool,
            ownership=ownership,
            repository=InboxRepository(pool),
        )
    finally:
        await pool.close()
