from __future__ import annotations

import asyncio
import inspect
import threading
from unittest.mock import AsyncMock, patch

import psycopg
import pytest

from src.db.maintenance_fence import (
    CHECKPOINT_MAINTENANCE_LOCK_KEY,
    DEFAULT_MAINTENANCE_EXCLUSIVE_SETTLE_SECONDS,
    DEFAULT_RUNTIME_FENCE_MONITOR_INTERVAL_SECONDS,
    DEFAULT_RUNTIME_FENCE_MONITOR_TIMEOUT_SECONDS,
    CheckpointMaintenanceFenceError,
    RuntimeCheckpointMaintenanceFence,
)
from src.maintenance import checkpoint_repository


pytestmark = pytest.mark.asyncio


async def test_default_exclusive_settle_outlasts_runtime_detection_window() -> None:
    assert DEFAULT_MAINTENANCE_EXCLUSIVE_SETTLE_SECONDS > (
        DEFAULT_RUNTIME_FENCE_MONITOR_INTERVAL_SECONDS
        + DEFAULT_RUNTIME_FENCE_MONITOR_TIMEOUT_SECONDS
    )


async def test_cleanup_repository_imports_the_single_maintenance_lock_key() -> None:
    source = inspect.getsource(checkpoint_repository)

    assert (
        checkpoint_repository.CHECKPOINT_MAINTENANCE_LOCK_KEY
        == CHECKPOINT_MAINTENANCE_LOCK_KEY
    )
    assert "_ADVISORY_LOCK_KEY" not in source
    assert "ai-exchange/checkpoint-cleanup/v1" not in source


@pytest.mark.parametrize(
    "timeout_overrides",
    [
        {"monitor_interval_seconds": 0},
        {"connect_timeout_seconds": 0},
        {"monitor_timeout_seconds": 0},
    ],
)
async def test_fence_rejects_nonpositive_timeouts_before_connecting(
    timeout_overrides: dict[str, int],
) -> None:
    connect = AsyncMock()

    with (
        patch.object(psycopg.AsyncConnection, "connect", new=connect),
        pytest.raises(
            CheckpointMaintenanceFenceError,
            match="checkpoint_maintenance_fence_failed",
        ),
    ):
        RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=AsyncMock(),
            **timeout_overrides,
        )

    connect.assert_not_awaited()


class _Cursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self._row = row

    async def fetchone(self) -> tuple[object, ...]:
        return self._row


class _Connection:
    def __init__(
        self,
        *,
        lock_granted: bool = True,
        lock_still_held: bool = True,
        health_error: BaseException | None = None,
        block_health_check: bool = False,
        block_lock_acquire: bool = False,
    ) -> None:
        self.lock_granted = lock_granted
        self.lock_still_held = lock_still_held
        self.health_error = health_error
        self.block_health_check = block_health_check
        self.block_lock_acquire = block_lock_acquire
        self.health_check_started = asyncio.Event()
        self.events: list[str] = []
        self.closed = False

    async def execute(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
    ) -> _Cursor:
        normalized = " ".join(query.split()).lower()
        if "pg_try_advisory_lock_shared" in normalized:
            self.events.append("lock_shared")
            assert params == (CHECKPOINT_MAINTENANCE_LOCK_KEY,)
            if self.block_lock_acquire:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.events.append("lock_acquire_cancelled")
                    raise
            return _Cursor((self.lock_granted,))
        if "from pg_catalog.pg_locks" in normalized:
            self.events.append("health_check")
            unsigned = CHECKPOINT_MAINTENANCE_LOCK_KEY & ((1 << 64) - 1)
            assert params == (
                (unsigned >> 32) & 0xFFFFFFFF,
                unsigned & 0xFFFFFFFF,
            )
            self.health_check_started.set()
            if self.block_health_check:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.events.append("monitor_stopped")
                    raise
            if self.health_error is not None:
                raise self.health_error
            return _Cursor((self.lock_still_held,))
        if "pg_advisory_unlock_shared" in normalized:
            self.events.append("unlock_shared")
            assert params == (CHECKPOINT_MAINTENANCE_LOCK_KEY,)
            return _Cursor((True,))
        raise AssertionError(f"unexpected query: {query}")

    async def close(self) -> None:
        self.events.append("close")
        self.closed = True


async def test_start_uses_dedicated_autocommit_session_and_shared_lock() -> None:
    connection = _Connection()
    connect = AsyncMock(return_value=connection)
    fail_stop = AsyncMock()

    with patch.object(psycopg.AsyncConnection, "connect", new=connect):
        fence = RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=fail_stop,
            monitor_interval_seconds=60,
        )
        await fence.start()
        await fence.close()

    connect.assert_awaited_once_with(
        "postgresql://runtime:PRIVATE@db/email",
        autocommit=True,
        prepare_threshold=0,
    )
    assert connection.events == ["lock_shared", "unlock_shared", "close"]
    fail_stop.assert_not_awaited()


async def test_assert_held_rechecks_exact_lock_on_dedicated_connection() -> None:
    connection = _Connection()

    with patch.object(
        psycopg.AsyncConnection,
        "connect",
        new=AsyncMock(return_value=connection),
    ):
        fence = RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=AsyncMock(),
            monitor_interval_seconds=60,
        )
        await fence.start()
        await fence.assert_held()
        await fence.close()

    assert connection.events == [
        "lock_shared",
        "health_check",
        "unlock_shared",
        "close",
    ]


@pytest.mark.parametrize(
    "failure_mode",
    [
        "not_started",
        "closed",
        "lock_lost",
        "query_error",
        "query_runtime_error",
        "query_type_error",
        "query_value_error",
        "query_timeout",
    ],
)
async def test_assert_held_hard_exits_and_raises_fixed_error_when_proof_fails(
    failure_mode: str,
) -> None:
    callback_called = threading.Event()
    reasons: list[str] = []
    exits: list[int] = []

    def fail_stop(reason: str) -> None:
        reasons.append(reason)
        callback_called.set()

    fence = RuntimeCheckpointMaintenanceFence(
        "postgresql://runtime:PRIVATE-PASSWORD@db/email",
        fail_stop=fail_stop,
        monitor_interval_seconds=60,
        monitor_timeout_seconds=0.01,
        hard_exit=exits.append,
    )
    connection: _Connection | None = None
    if failure_mode != "not_started":
        connection = _Connection(
            lock_still_held=failure_mode != "lock_lost",
            health_error=(
                psycopg.OperationalError("PRIVATE-SOCKET-DETAIL")
                if failure_mode == "query_error"
                else RuntimeError("PRIVATE-RUNTIME-DETAIL")
                if failure_mode == "query_runtime_error"
                else TypeError("PRIVATE-TYPE-DETAIL")
                if failure_mode == "query_type_error"
                else ValueError("PRIVATE-VALUE-DETAIL")
                if failure_mode == "query_value_error"
                else None
            ),
            block_health_check=failure_mode == "query_timeout",
        )
        connection.closed = failure_mode == "closed"
        fence._connection = connection
        fence._lock_acquired = True

    with pytest.raises(
        CheckpointMaintenanceFenceError,
        match="^checkpoint_maintenance_fence_failed$",
    ) as caught:
        await fence.assert_held()

    assert caught.value.__cause__ is None
    assert exits == [70]
    assert await asyncio.to_thread(callback_called.wait, 0.5)
    assert reasons == ["checkpoint_maintenance_fence_connection_lost"]
    assert "PRIVATE" not in str(caught.value)

    if connection is not None:
        await fence.close()


async def test_start_is_idempotent_and_close_before_start_is_safe() -> None:
    connection = _Connection()
    connect = AsyncMock(return_value=connection)
    fail_stop = AsyncMock()

    idle_fence = RuntimeCheckpointMaintenanceFence(
        "postgresql://runtime:PRIVATE@db/email",
        fail_stop=fail_stop,
    )
    await idle_fence.close()

    with patch.object(psycopg.AsyncConnection, "connect", new=connect):
        fence = RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=fail_stop,
            monitor_interval_seconds=60,
        )
        await fence.start()
        await fence.start()
        await fence.close()

    connect.assert_awaited_once()
    assert connection.events == ["lock_shared", "unlock_shared", "close"]


async def test_start_cuts_off_connection_errors_with_fixed_text() -> None:
    private_detail = "PRIVATE-CONNECTION-DETAIL"
    connect = AsyncMock(side_effect=psycopg.OperationalError(private_detail))

    with (
        patch.object(psycopg.AsyncConnection, "connect", new=connect),
        pytest.raises(CheckpointMaintenanceFenceError) as caught,
    ):
        await RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=AsyncMock(),
        ).start()

    assert caught.value.code == "checkpoint_maintenance_fence_failed"
    assert private_detail not in str(caught.value)
    assert caught.value.__cause__ is None


async def test_start_timeout_bounds_shared_lock_acquisition_and_closes_session() -> (
    None
):
    connection = _Connection(block_lock_acquire=True)

    with (
        patch.object(
            psycopg.AsyncConnection,
            "connect",
            new=AsyncMock(return_value=connection),
        ),
        pytest.raises(
            CheckpointMaintenanceFenceError,
            match="^checkpoint_maintenance_fence_failed$",
        ),
    ):
        async with asyncio.timeout(0.5):
            await RuntimeCheckpointMaintenanceFence(
                "postgresql://runtime:PRIVATE@db/email",
                fail_stop=AsyncMock(),
                connect_timeout_seconds=0.01,
            ).start()

    assert connection.events == [
        "lock_shared",
        "lock_acquire_cancelled",
        "close",
    ]


async def test_start_fails_closed_with_fixed_text_when_shared_lock_is_unavailable() -> (
    None
):
    connection = _Connection(lock_granted=False)
    private_dsn = "postgresql://runtime:PRIVATE-PASSWORD@db/email"

    with patch.object(
        psycopg.AsyncConnection,
        "connect",
        new=AsyncMock(return_value=connection),
    ):
        fence = RuntimeCheckpointMaintenanceFence(
            private_dsn,
            fail_stop=AsyncMock(),
            monitor_interval_seconds=60,
        )
        with pytest.raises(CheckpointMaintenanceFenceError) as caught:
            await fence.start()

    assert caught.value.code == "checkpoint_maintenance_fence_unavailable"
    assert str(caught.value) == "checkpoint_maintenance_fence_unavailable"
    assert caught.value.__cause__ is None
    assert "PRIVATE-PASSWORD" not in str(caught.value)
    assert connection.events == ["lock_shared", "close"]


@pytest.mark.parametrize(
    "health_error",
    [
        psycopg.OperationalError("PRIVATE-SOCKET-DETAIL"),
        RuntimeError("PRIVATE-RUNTIME-DETAIL"),
        TypeError("PRIVATE-TYPE-DETAIL"),
        ValueError("PRIVATE-VALUE-DETAIL"),
    ],
)
async def test_connection_loss_calls_injected_fail_stop_once_with_safe_reason(
    health_error: BaseException,
) -> None:
    connection = _Connection(health_error=health_error)
    fail_stop_called = asyncio.Event()
    reasons: list[str] = []
    exits: list[int] = []

    async def fail_stop(reason: str) -> None:
        reasons.append(reason)
        fail_stop_called.set()

    with patch.object(
        psycopg.AsyncConnection,
        "connect",
        new=AsyncMock(return_value=connection),
    ):
        fence = RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE-PASSWORD@db/email",
            fail_stop=fail_stop,
            monitor_interval_seconds=0.001,
            hard_exit=exits.append,
        )
        await fence.start()
        await asyncio.wait_for(fail_stop_called.wait(), timeout=1)
        await asyncio.sleep(0.01)
        await fence.close()

    assert reasons == ["checkpoint_maintenance_fence_connection_lost"]
    assert exits == [70]
    assert "PRIVATE" not in reasons[0]


async def test_healthy_connection_without_exact_shared_lock_triggers_fail_stop() -> (
    None
):
    connection = _Connection(lock_still_held=False)
    fail_stop_called = asyncio.Event()
    exits: list[int] = []

    async def fail_stop(_reason: str) -> None:
        fail_stop_called.set()

    with patch.object(
        psycopg.AsyncConnection,
        "connect",
        new=AsyncMock(return_value=connection),
    ):
        fence = RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=fail_stop,
            monitor_interval_seconds=0.001,
            hard_exit=exits.append,
        )
        await fence.start()
        await asyncio.wait_for(fail_stop_called.wait(), timeout=1)
        await fence.close()

    assert exits == [70]


async def test_fail_stop_hard_exit_does_not_wait_for_blocking_callback() -> None:
    callback_started = threading.Event()
    release_callback = threading.Event()
    exits: list[int] = []

    def blocking_fail_stop(_reason: str) -> None:
        callback_started.set()
        release_callback.wait()

    fence = RuntimeCheckpointMaintenanceFence(
        "postgresql://runtime:PRIVATE@db/email",
        fail_stop=blocking_fail_stop,
        monitor_interval_seconds=60,
        hard_exit=exits.append,
    )
    trigger = asyncio.create_task(
        asyncio.to_thread(lambda: asyncio.run(fence._trigger_fail_stop()))
    )

    try:
        assert await asyncio.to_thread(callback_started.wait, 0.5)
        await asyncio.sleep(0.01)
        assert exits == [70]
    finally:
        release_callback.set()
        await asyncio.wait_for(trigger, timeout=1)


async def test_close_stops_monitor_before_unlocking_and_closing() -> None:
    connection = _Connection(block_health_check=True)

    with patch.object(
        psycopg.AsyncConnection,
        "connect",
        new=AsyncMock(return_value=connection),
    ):
        fence = RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=AsyncMock(),
            monitor_interval_seconds=0.001,
        )
        await fence.start()
        await asyncio.wait_for(connection.health_check_started.wait(), timeout=1)
        await fence.close()

    assert connection.events == [
        "lock_shared",
        "health_check",
        "monitor_stopped",
        "unlock_shared",
        "close",
    ]


async def test_async_context_manager_starts_and_closes_fence() -> None:
    connection = _Connection()

    with patch.object(
        psycopg.AsyncConnection,
        "connect",
        new=AsyncMock(return_value=connection),
    ):
        fence = RuntimeCheckpointMaintenanceFence(
            "postgresql://runtime:PRIVATE@db/email",
            fail_stop=AsyncMock(),
            monitor_interval_seconds=60,
        )
        async with fence as entered:
            assert entered is fence
            assert connection.events == ["lock_shared"]

    assert connection.events == ["lock_shared", "unlock_shared", "close"]
