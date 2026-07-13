"""Runtime lifecycle fence for mutually exclusive checkpoint maintenance."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import os
import threading
from collections.abc import Awaitable, Callable
from typing import Final

import psycopg


CHECKPOINT_MAINTENANCE_LOCK_KEY: Final = int.from_bytes(
    hashlib.sha256(b"ai-exchange/checkpoint-cleanup/v1").digest()[:8],
    byteorder="big",
    signed=True,
)
_LOCK_KEY_UNSIGNED: Final = CHECKPOINT_MAINTENANCE_LOCK_KEY & ((1 << 64) - 1)
_LOCK_CLASS_ID: Final = (_LOCK_KEY_UNSIGNED >> 32) & 0xFFFFFFFF
_LOCK_OBJECT_ID: Final = _LOCK_KEY_UNSIGNED & 0xFFFFFFFF

_FENCE_FAILED = "checkpoint_maintenance_fence_failed"
_FENCE_UNAVAILABLE = "checkpoint_maintenance_fence_unavailable"
_FENCE_CONNECTION_LOST = "checkpoint_maintenance_fence_connection_lost"
_SAFE_ERROR_CODES: Final = frozenset({_FENCE_FAILED, _FENCE_UNAVAILABLE})

DEFAULT_RUNTIME_FENCE_MONITOR_INTERVAL_SECONDS: Final = 5.0
DEFAULT_RUNTIME_FENCE_CONNECT_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_RUNTIME_FENCE_MONITOR_TIMEOUT_SECONDS: Final = 10.0
# Defense-in-depth only: this fixed, non-configurable delay outlasts the default
# monitor interval plus its bounded probe.  Mutual exclusion is proved by the
# dedicated and per-pool-connection session locks, not by elapsed wall-clock time.
DEFAULT_MAINTENANCE_EXCLUSIVE_SETTLE_SECONDS: Final = 20.0

FailStopCallback = Callable[[str], Awaitable[None] | None]
HardExitCallback = Callable[[int], object]


class CheckpointMaintenanceFenceError(RuntimeError):
    """A fixed-text lifecycle error that never exposes connection details."""

    def __init__(self, code: str) -> None:
        safe_code = code if code in _SAFE_ERROR_CODES else _FENCE_FAILED
        self.code = safe_code
        super().__init__(safe_code)


class RuntimeCheckpointMaintenanceFence:
    """Hold a shared session lock for the complete runtime lifecycle."""

    def __init__(
        self,
        dsn: str,
        *,
        fail_stop: FailStopCallback,
        monitor_interval_seconds: float = (
            DEFAULT_RUNTIME_FENCE_MONITOR_INTERVAL_SECONDS
        ),
        connect_timeout_seconds: float = DEFAULT_RUNTIME_FENCE_CONNECT_TIMEOUT_SECONDS,
        monitor_timeout_seconds: float = DEFAULT_RUNTIME_FENCE_MONITOR_TIMEOUT_SECONDS,
        hard_exit: HardExitCallback = os._exit,
    ) -> None:
        if (
            monitor_interval_seconds <= 0
            or connect_timeout_seconds <= 0
            or monitor_timeout_seconds <= 0
        ):
            raise CheckpointMaintenanceFenceError(_FENCE_FAILED)
        self._dsn = dsn
        self._fail_stop = fail_stop
        self._monitor_interval_seconds = monitor_interval_seconds
        self._connect_timeout_seconds = connect_timeout_seconds
        self._monitor_timeout_seconds = monitor_timeout_seconds
        self._hard_exit = hard_exit
        self._connection: psycopg.AsyncConnection | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._lock_acquired = False
        self._fail_stop_triggered = False
        self._probe_lock = asyncio.Lock()

    async def __aenter__(self) -> RuntimeCheckpointMaintenanceFence:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None:
        await self.close()

    async def start(self) -> None:
        """Open an independent connection and acquire the runtime shared lock."""

        if self._connection is not None:
            return

        connection: psycopg.AsyncConnection | None = None
        lock_acquired = False
        try:
            async with asyncio.timeout(self._connect_timeout_seconds):
                connection = await psycopg.AsyncConnection.connect(
                    self._dsn,
                    autocommit=True,
                    prepare_threshold=0,
                )
                lock_row = await (
                    await connection.execute(
                        "SELECT pg_try_advisory_lock_shared(%s)",
                        (CHECKPOINT_MAINTENANCE_LOCK_KEY,),
                    )
                ).fetchone()
                lock_acquired = bool(lock_row and lock_row[0])
            if not lock_acquired:
                raise CheckpointMaintenanceFenceError(_FENCE_UNAVAILABLE)

            self._connection = connection
            self._lock_acquired = True
            self._monitor_task = asyncio.create_task(
                self._monitor_connection(),
                name="checkpoint-maintenance-fence-monitor",
            )
        except CheckpointMaintenanceFenceError:
            raise
        except (
            TimeoutError,
            psycopg.Error,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ):
            raise CheckpointMaintenanceFenceError(_FENCE_FAILED) from None
        finally:
            if connection is not None and not lock_acquired:
                try:
                    await connection.close()
                except (psycopg.Error, OSError):
                    pass

    async def close(self) -> None:
        """Stop monitoring before releasing the shared lock and connection."""

        monitor_task = self._monitor_task
        self._monitor_task = None
        if monitor_task is not None:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        connection = self._connection
        self._connection = None
        lock_acquired = self._lock_acquired
        self._lock_acquired = False
        if connection is None:
            return

        if lock_acquired:
            try:
                await connection.execute(
                    "SELECT pg_advisory_unlock_shared(%s)",
                    (CHECKPOINT_MAINTENANCE_LOCK_KEY,),
                )
            except (psycopg.Error, OSError):
                pass
        try:
            await connection.close()
        except (psycopg.Error, OSError):
            pass

    async def assert_held(self) -> None:
        """Prove that this exact dedicated session still holds the shared lock."""

        connection = self._connection
        if connection is None or connection.closed:
            await self._trigger_fail_stop()
            raise CheckpointMaintenanceFenceError(_FENCE_FAILED)

        try:
            lock_held = await self._probe_exact_shared_lock(connection)
        except (
            TimeoutError,
            psycopg.Error,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ):
            await self._trigger_fail_stop()
            raise CheckpointMaintenanceFenceError(_FENCE_FAILED) from None

        if not lock_held:
            await self._trigger_fail_stop()
            raise CheckpointMaintenanceFenceError(_FENCE_FAILED)

    async def _probe_exact_shared_lock(
        self,
        connection: psycopg.AsyncConnection,
    ) -> bool:
        async with asyncio.timeout(self._monitor_timeout_seconds):
            async with self._probe_lock:
                lock_row = await (
                    await connection.execute(
                        """
                        SELECT pg_catalog.count(*) = 1
                        FROM pg_catalog.pg_locks
                        WHERE locktype = 'advisory'
                          AND pid = pg_catalog.pg_backend_pid()
                          AND classid = %s
                          AND objid = %s
                          AND objsubid = 1
                          AND mode = 'ShareLock'
                          AND granted
                        """,
                        (_LOCK_CLASS_ID, _LOCK_OBJECT_ID),
                    )
                ).fetchone()
        return bool(lock_row and lock_row[0] is True)

    async def _monitor_connection(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._monitor_interval_seconds)
                connection = self._connection
                if connection is None or connection.closed:
                    await self._trigger_fail_stop()
                    return
                if not await self._probe_exact_shared_lock(connection):
                    await self._trigger_fail_stop()
                    return
            except asyncio.CancelledError:
                raise
            except (
                TimeoutError,
                psycopg.Error,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ):
                await self._trigger_fail_stop()
                return

    async def _trigger_fail_stop(self) -> None:
        if self._fail_stop_triggered:
            return
        self._fail_stop_triggered = True

        def invoke_callback_best_effort() -> None:
            try:
                result = self._fail_stop(_FENCE_CONNECTION_LOST)
                if inspect.isawaitable(result):

                    async def await_result() -> None:
                        await result

                    asyncio.run(await_result())
            except BaseException:
                pass

        try:
            threading.Thread(
                target=invoke_callback_best_effort,
                name="checkpoint-maintenance-fail-stop",
                daemon=True,
            ).start()
        finally:
            self._hard_exit(70)
