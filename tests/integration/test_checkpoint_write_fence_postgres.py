from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import psycopg
import pytest
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.db.checkpoint_saver import (
    FencedAsyncPostgresSaver,
    configure_checkpoint_pool_connection,
)
from src.db.maintenance_fence import (
    CheckpointMaintenanceFenceError,
    RuntimeCheckpointMaintenanceFence,
)
from src.maintenance import checkpoint_repository as checkpoint_repository_module
from src.maintenance.checkpoint_repository import (
    CheckpointRepositoryError,
    PostgresCheckpointRepository,
)


pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _short_exclusive_settle(monkeypatch) -> None:
    monkeypatch.setattr(
        checkpoint_repository_module,
        "DEFAULT_MAINTENANCE_EXCLUSIVE_SETTLE_SECONDS",
        0.001,
    )


@pytest.fixture
async def checkpoint_schema(postgres_database_factory):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    return schema


def _checkpoint(thread_id: str) -> tuple[dict, dict[str, str]]:
    checkpoint = empty_checkpoint()
    values = {
        "email_id": thread_id,
        "content_ref": {
            "account_id": 8,
            "object_id": "00000000-0000-4000-8000-000000000127",
            "key_version": "v1",
            "sha256": "c" * 64,
        },
        "attachment_tokens": [],
        "pdf_token": None,
    }
    versions = {
        channel: f"{checkpoint['id']}:{index}" for index, channel in enumerate(values)
    }
    checkpoint["channel_values"] = values
    checkpoint["channel_versions"] = versions
    checkpoint["updated_channels"] = list(values)
    return checkpoint, versions


async def _write_checkpoint(saver, thread_id: str) -> None:
    checkpoint, versions = _checkpoint(thread_id)
    await saver.aput(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        checkpoint,
        {},
        versions,
    )


async def _seed_cleanup_plan(schema, thread_id: str):
    schema.execute(
        """
        INSERT INTO emails_log (id, status, updated_at)
        VALUES (%s, 'sent', CURRENT_TIMESTAMP - INTERVAL '72 hours')
        """,
        (thread_id,),
    )
    async with await psycopg.AsyncConnection.connect(
        schema.dsn,
        autocommit=True,
        prepare_threshold=0,
    ) as conn:
        await _write_checkpoint(AsyncPostgresSaver(conn), thread_id)

    repository = PostgresCheckpointRepository(schema.maintenance_dsn)
    snapshot = await repository.scan_candidates(
        cutoff=datetime.now(UTC) - timedelta(hours=48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    candidate = next(
        item for item in snapshot.candidates if item.thread_id == thread_id
    )
    plan = SimpleNamespace(
        database_fingerprint=snapshot.database_fingerprint,
        database_timezone=snapshot.database_timezone,
        alembic_revision=snapshot.alembic_revision,
        checkpoint_revision=snapshot.checkpoint_revision,
        cutoff=snapshot.cutoff,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        candidates=snapshot.candidates,
    )
    return repository, plan, candidate


async def _open_runtime_pool(dsn: str) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        conninfo=dsn,
        kwargs={"autocommit": True, "prepare_threshold": 0},
        min_size=1,
        max_size=1,
        configure=configure_checkpoint_pool_connection,
        open=False,
    )
    await pool.open(wait=True, timeout=5)
    return pool


async def _pool_backend_pid(pool: AsyncConnectionPool) -> int:
    async with pool.connection() as conn:
        row = await (await conn.execute("SELECT pg_backend_pid()")).fetchone()
    assert row is not None
    return int(row[0])


async def _fence_backend_pid(fence: RuntimeCheckpointMaintenanceFence) -> int:
    connection = fence._connection
    assert connection is not None
    row = await (await connection.execute("SELECT pg_backend_pid()")).fetchone()
    assert row is not None
    return int(row[0])


async def _terminate_backends(admin_dsn: str, pids: list[int]) -> None:
    async with await psycopg.AsyncConnection.connect(
        admin_dsn,
        autocommit=True,
    ) as conn:
        await conn.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity WHERE pid = ANY(%s)",
            (pids,),
        )
        async with asyncio.timeout(5):
            while True:
                row = await (
                    await conn.execute(
                        "SELECT count(*) FROM pg_stat_activity WHERE pid = ANY(%s)",
                        (pids,),
                    )
                ).fetchone()
                if row == (0,):
                    return
                await asyncio.sleep(0.01)


async def test_pool_session_shared_lock_makes_maintenance_execution_unavailable(
    checkpoint_schema,
) -> None:
    repository, plan, _candidate = await _seed_cleanup_plan(
        checkpoint_schema,
        "pool-lock-blocks-maintenance",
    )
    pool = await _open_runtime_pool(checkpoint_schema.runtime_dsn)
    try:
        with pytest.raises(
            CheckpointRepositoryError,
            match="^cleanup_lock_unavailable$",
        ):
            async with repository.execution_session(plan=plan):
                pass
    finally:
        await pool.close()

    async with repository.execution_session(plan=plan):
        pass


async def test_pool_shared_lock_survives_dedicated_backend_loss_while_monitor_paused(
    checkpoint_schema,
) -> None:
    repository, plan, _candidate = await _seed_cleanup_plan(
        checkpoint_schema,
        "pool-lock-survives-dedicated-loss",
    )
    exits: list[int] = []
    fence = RuntimeCheckpointMaintenanceFence(
        checkpoint_schema.runtime_dsn,
        fail_stop=lambda _reason: None,
        monitor_interval_seconds=3600,
        hard_exit=exits.append,
    )
    await fence.start()
    pool = await _open_runtime_pool(checkpoint_schema.runtime_dsn)
    try:
        await _terminate_backends(
            checkpoint_schema.admin_dsn,
            [await _fence_backend_pid(fence)],
        )

        with pytest.raises(
            CheckpointRepositoryError,
            match="^cleanup_lock_unavailable$",
        ):
            async with repository.execution_session(plan=plan):
                pass
        assert exits == []
    finally:
        await pool.close()
        await fence.close()


async def test_writer_recovery_after_maintenance_fails_before_checkpoint_sql(
    checkpoint_schema,
) -> None:
    thread_id = "recovered-writer-must-stop"
    repository, plan, candidate = await _seed_cleanup_plan(
        checkpoint_schema,
        thread_id,
    )
    fail_stop_called = threading.Event()
    exits: list[int] = []
    fence = RuntimeCheckpointMaintenanceFence(
        checkpoint_schema.runtime_dsn,
        fail_stop=lambda _reason: fail_stop_called.set(),
        monitor_interval_seconds=3600,
        hard_exit=exits.append,
    )
    await fence.start()
    pool = await _open_runtime_pool(checkpoint_schema.runtime_dsn)
    try:
        await _terminate_backends(
            checkpoint_schema.admin_dsn,
            [await _fence_backend_pid(fence), await _pool_backend_pid(pool)],
        )
    finally:
        await pool.close()

    async with repository.execution_session(plan=plan) as session:
        result = await session.delete_candidate(candidate)
    assert result.deleted is True
    assert (
        checkpoint_schema.scalar(
            "SELECT count(*) FROM checkpoints WHERE thread_id = %s",
            (thread_id,),
        )
        == 0
    )

    recovered_pool = await _open_runtime_pool(checkpoint_schema.runtime_dsn)
    try:
        saver = FencedAsyncPostgresSaver(
            recovered_pool,
            write_guard=fence.assert_held,
        )
        with pytest.raises(
            CheckpointMaintenanceFenceError,
            match="^checkpoint_maintenance_fence_failed$",
        ):
            await _write_checkpoint(saver, thread_id)
    finally:
        await recovered_pool.close()
        await fence.close()

    assert exits == [70]
    assert await asyncio.to_thread(fail_stop_called.wait, 0.5)
    assert (
        checkpoint_schema.scalar(
            "SELECT count(*) FROM checkpoints WHERE thread_id = %s",
            (thread_id,),
        )
        == 0
    )


async def test_pool_lock_closes_gap_when_dedicated_session_dies_after_guard(
    checkpoint_schema,
) -> None:
    repository, plan, _candidate = await _seed_cleanup_plan(
        checkpoint_schema,
        "guard-write-gap-plan",
    )
    fence = RuntimeCheckpointMaintenanceFence(
        checkpoint_schema.runtime_dsn,
        fail_stop=lambda _reason: None,
        monitor_interval_seconds=3600,
        hard_exit=lambda _code: None,
    )
    await fence.start()
    pool = await _open_runtime_pool(checkpoint_schema.runtime_dsn)
    guard_passed = asyncio.Event()
    allow_write = asyncio.Event()

    async def pausing_guard() -> None:
        await fence.assert_held()
        guard_passed.set()
        await allow_write.wait()

    writer = asyncio.create_task(
        _write_checkpoint(
            FencedAsyncPostgresSaver(pool, write_guard=pausing_guard),
            "guard-write-gap-new-row",
        )
    )
    try:
        await asyncio.wait_for(guard_passed.wait(), timeout=2)
        await _terminate_backends(
            checkpoint_schema.admin_dsn,
            [await _fence_backend_pid(fence)],
        )

        with pytest.raises(
            CheckpointRepositoryError,
            match="^cleanup_lock_unavailable$",
        ):
            async with repository.execution_session(plan=plan):
                pass

        allow_write.set()
        await asyncio.wait_for(writer, timeout=2)
        assert (
            checkpoint_schema.scalar(
                "SELECT count(*) FROM checkpoints WHERE thread_id = %s",
                ("guard-write-gap-new-row",),
            )
            == 1
        )
    finally:
        allow_write.set()
        if not writer.done():
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
        await pool.close()
        await fence.close()
