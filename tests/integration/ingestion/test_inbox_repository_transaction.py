from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.domain.errors import StaleFence
from src.ingestion.models import (
    ChangeKind,
    IngressReceipt,
    IngressSource,
    NormalizedIngressEvent,
    ProcessingPolicy,
)
from src.ingestion.ownership import PipelineOwnershipRepository
from src.ingestion.repository import InboxRepository


class _NeverPool:
    def connection(self):
        raise AssertionError("caller-owned insert must not check out a pool connection")


@dataclass(slots=True)
class _Runtime:
    schema: Any
    pool: AsyncConnectionPool
    repository: InboxRepository


@pytest_asyncio.fixture
async def transaction_runtime(postgres_database_factory) -> _Runtime:
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    pool = AsyncConnectionPool(
        conninfo=schema.runtime_dsn,
        min_size=1,
        max_size=4,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    await PipelineOwnershipRepository(pool).bootstrap(8, "durable_v1")
    try:
        yield _Runtime(schema, pool, InboxRepository(_NeverPool()))
    finally:
        await pool.close()


def _event(token: str, *, policy: ProcessingPolicy = ProcessingPolicy.FULL):
    return NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.SYNC,
        raw_event_type="create",
        kind=ChangeKind.CREATE,
        external_email_id=f"message-{token}",
        folder="INBOX",
        source_version=f"cursor-{token}",
        dedupe_key=hashlib.sha256(token.encode()).hexdigest(),
        payload={"token": token},
        source_event_at=datetime.now(UTC),
        processing_policy=policy,
    )


async def _scalar(connection, statement: str, params=()):
    cursor = await connection.execute(statement, params)
    row = await cursor.fetchone()
    return None if row is None else next(iter(row.values()))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_insert_uses_the_callers_top_level_xid(
    transaction_runtime: _Runtime,
) -> None:
    async with transaction_runtime.pool.connection() as connection:
        async with connection.transaction():
            await transaction_runtime.repository._configure_transaction(connection)
            xid_before = await _scalar(
                connection,
                "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text",
            )
            receipt = await transaction_runtime.repository.transaction(
                connection
            ).insert(_event("same-xid"), 1, 1)
            xid_after = await _scalar(
                connection,
                "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text",
            )
            visible_inside = await _scalar(
                connection,
                "SELECT pg_catalog.count(*) FROM event_inbox WHERE id = %s",
                (receipt.inbox_id,),
            )

    assert xid_before == xid_after
    assert visible_inside == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_outer_rollback_removes_inbox_row_and_suppression_audit(
    transaction_runtime: _Runtime,
) -> None:
    receipt = None
    with pytest.raises(RuntimeError, match="inject rollback"):
        async with transaction_runtime.pool.connection() as connection:
            async with connection.transaction():
                await transaction_runtime.repository._configure_transaction(connection)
                receipt = await transaction_runtime.repository.transaction(
                    connection
                ).insert(_event("rollback", policy=ProcessingPolicy.IGNORED), 1, 1)
                raise RuntimeError("inject rollback")

    assert receipt is not None
    async with transaction_runtime.pool.connection() as connection:
        assert (
            await _scalar(
                connection,
                "SELECT pg_catalog.count(*) FROM event_inbox WHERE id = %s",
                (receipt.inbox_id,),
            )
            == 0
        )
        assert (
            await _scalar(
                connection,
                "SELECT pg_catalog.count(*) FROM audit_events "
                "WHERE action = 'ingress.policy_suppressed'",
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_insert_keeps_first_write_wins_dedupe_boundary(
    transaction_runtime: _Runtime,
) -> None:
    original = _event("dedupe")
    drifted = replace(
        original,
        payload={"token": "changed"},
        processing_policy=ProcessingPolicy.IGNORED,
    )
    async with transaction_runtime.pool.connection() as connection:
        async with connection.transaction():
            await transaction_runtime.repository._configure_transaction(connection)
            transaction = transaction_runtime.repository.transaction(connection)
            first = await transaction.insert(original, 1, 1)
            duplicate = await transaction.insert(drifted, 1, 1)
            stored = await connection.execute(
                "SELECT payload, processing_policy FROM event_inbox WHERE id = %s",
                (first.inbox_id,),
            )
            row = await stored.fetchone()

    assert duplicate == IngressReceipt(first.inbox_id, True)
    assert row == {"payload": {"token": "dedupe"}, "processing_policy": "full"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bound_transaction_object_rejects_a_new_top_level_xid(
    transaction_runtime: _Runtime,
) -> None:
    async with transaction_runtime.pool.connection() as connection:
        transaction = transaction_runtime.repository.transaction(connection)
        async with connection.transaction():
            await transaction_runtime.repository._configure_transaction(connection)
            await transaction.insert(_event("first-xid"), 1, 1)
        async with connection.transaction():
            await transaction_runtime.repository._configure_transaction(connection)
            with pytest.raises(StaleFence):
                await transaction.insert(_event("second-xid"), 1, 1)
