from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.db.maintenance_fence import CHECKPOINT_MAINTENANCE_LOCK_KEY


pytestmark = pytest.mark.asyncio


async def test_fenced_saver_rejects_missing_write_guard() -> None:
    from src.db.checkpoint_saver import (
        CheckpointWriteFenceConfigurationError,
        FencedAsyncPostgresSaver,
    )

    with pytest.raises(
        CheckpointWriteFenceConfigurationError,
        match="^checkpoint_write_fence_not_bound$",
    ):
        FencedAsyncPostgresSaver(MagicMock(), write_guard=None)


async def test_mutating_cursor_acquires_pool_connection_before_guarding_write() -> None:
    from src.db.checkpoint_saver import FencedAsyncPostgresSaver

    events: list[str] = []

    @asynccontextmanager
    async def upstream_cursor(_self, *, pipeline: bool = False):
        assert pipeline is True
        events.append("pool_connection_acquired")
        yield MagicMock()

    async def guard() -> None:
        events.append("dedicated_fence_asserted")

    with patch.object(AsyncPostgresSaver, "_cursor", new=upstream_cursor):
        saver = FencedAsyncPostgresSaver(MagicMock(), write_guard=guard)
        async with saver._cursor(pipeline=True):
            events.append("checkpoint_sql")

    assert events == [
        "pool_connection_acquired",
        "dedicated_fence_asserted",
        "checkpoint_sql",
    ]


async def test_read_only_cursor_does_not_probe_dedicated_write_fence() -> None:
    from src.db.checkpoint_saver import FencedAsyncPostgresSaver

    @asynccontextmanager
    async def upstream_cursor(_self, *, pipeline: bool = False):
        assert pipeline is False
        yield MagicMock()

    guard = AsyncMock()
    with patch.object(AsyncPostgresSaver, "_cursor", new=upstream_cursor):
        saver = FencedAsyncPostgresSaver(MagicMock(), write_guard=guard)
        async with saver._cursor():
            pass

    guard.assert_not_awaited()


class _RecordingCursor:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def execute(self, _query, _params=None, **_kwargs):
        self.events.append("checkpoint_sql")
        return self

    async def executemany(self, _query, _params):
        self.events.append("checkpoint_sql")
        return self


async def _run_mutation(saver, mutation: str) -> None:
    if mutation == "aput":
        checkpoint = empty_checkpoint()
        checkpoint["channel_values"] = {}
        checkpoint["channel_versions"] = {}
        checkpoint["updated_channels"] = []
        await saver.aput(
            {"configurable": {"thread_id": "thread", "checkpoint_ns": ""}},
            checkpoint,
            {},
            {},
        )
        return
    if mutation == "aput_writes":
        await saver.aput_writes(
            {
                "configurable": {
                    "thread_id": "thread",
                    "checkpoint_ns": "",
                    "checkpoint_id": "checkpoint",
                }
            },
            [("channel", "value")],
            "task",
        )
        return
    if mutation == "adelete_thread":
        await saver.adelete_thread("thread")
        return
    raise AssertionError(f"unexpected mutation: {mutation}")


@pytest.mark.parametrize("mutation", ["aput", "aput_writes", "adelete_thread"])
async def test_every_runtime_mutation_guards_before_first_checkpoint_sql(
    mutation: str,
) -> None:
    from src.db.checkpoint_saver import FencedAsyncPostgresSaver

    events: list[str] = []
    cursor = _RecordingCursor(events)

    @asynccontextmanager
    async def upstream_cursor(_self, *, pipeline: bool = False):
        assert pipeline is True
        events.append("pool_connection_acquired")
        yield cursor

    async def guard() -> None:
        events.append("dedicated_fence_asserted")

    with patch.object(AsyncPostgresSaver, "_cursor", new=upstream_cursor):
        saver = FencedAsyncPostgresSaver(MagicMock(), write_guard=guard)
        await _run_mutation(saver, mutation)

    assert events[:3] == [
        "pool_connection_acquired",
        "dedicated_fence_asserted",
        "checkpoint_sql",
    ]


@pytest.mark.parametrize("mutation", ["aput", "aput_writes", "adelete_thread"])
async def test_failed_guard_stops_every_mutation_before_checkpoint_sql(
    mutation: str,
) -> None:
    from src.db.checkpoint_saver import FencedAsyncPostgresSaver

    events: list[str] = []
    cursor = _RecordingCursor(events)

    @asynccontextmanager
    async def upstream_cursor(_self, *, pipeline: bool = False):
        assert pipeline is True
        events.append("pool_connection_acquired")
        yield cursor

    async def guard() -> None:
        raise RuntimeError("checkpoint_maintenance_fence_failed")

    with patch.object(AsyncPostgresSaver, "_cursor", new=upstream_cursor):
        saver = FencedAsyncPostgresSaver(MagicMock(), write_guard=guard)
        with pytest.raises(RuntimeError, match="checkpoint_maintenance_fence_failed"):
            await _run_mutation(saver, mutation)

    assert events == ["pool_connection_acquired"]


async def test_runtime_saver_forbids_schema_setup_path() -> None:
    from src.db.checkpoint_saver import (
        CheckpointWriteFenceConfigurationError,
        FencedAsyncPostgresSaver,
    )

    saver = FencedAsyncPostgresSaver(MagicMock(), write_guard=AsyncMock())
    with pytest.raises(
        CheckpointWriteFenceConfigurationError,
        match="^checkpoint_runtime_setup_forbidden$",
    ):
        await saver.setup()


class _PoolConnection:
    def __init__(self, *, error: BaseException | None = None) -> None:
        self.error = error
        self.queries: list[tuple[str, tuple[object, ...] | None]] = []

    async def execute(self, query: str, params=None):
        if self.error is not None:
            raise self.error
        self.queries.append((" ".join(query.split()).lower(), params))
        return MagicMock()


async def test_pool_configure_takes_blocking_session_shared_lock() -> None:
    from src.db.checkpoint_saver import configure_checkpoint_pool_connection

    connection = _PoolConnection()
    await configure_checkpoint_pool_connection(connection)

    assert connection.queries == [
        (
            "select pg_catalog.pg_advisory_lock_shared(%s)",
            (CHECKPOINT_MAINTENANCE_LOCK_KEY,),
        )
    ]


async def test_pool_configure_hides_connection_failures() -> None:
    from src.db.checkpoint_saver import (
        CheckpointWriteFenceConfigurationError,
        configure_checkpoint_pool_connection,
    )

    connection = _PoolConnection(
        error=psycopg.OperationalError("PRIVATE-CONNECTION-DETAIL")
    )
    with pytest.raises(
        CheckpointWriteFenceConfigurationError,
        match="^checkpoint_pool_fence_failed$",
    ) as caught:
        await configure_checkpoint_pool_connection(connection)

    assert caught.value.__cause__ is None
    assert "PRIVATE" not in str(caught.value)
