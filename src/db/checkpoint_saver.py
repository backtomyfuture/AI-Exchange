"""Runtime LangGraph saver guarded by the checkpoint maintenance fence."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Final

import psycopg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.db.maintenance_fence import CHECKPOINT_MAINTENANCE_LOCK_KEY


_WRITE_FENCE_NOT_BOUND: Final = "checkpoint_write_fence_not_bound"
_RUNTIME_SETUP_FORBIDDEN: Final = "checkpoint_runtime_setup_forbidden"
_POOL_FENCE_FAILED: Final = "checkpoint_pool_fence_failed"
_WRITE_FENCE_BINDING_CLOSED: Final = "checkpoint_write_fence_binding_closed"
_SAFE_CONFIGURATION_ERRORS: Final = frozenset(
    {
        _WRITE_FENCE_NOT_BOUND,
        _RUNTIME_SETUP_FORBIDDEN,
        _POOL_FENCE_FAILED,
        _WRITE_FENCE_BINDING_CLOSED,
    }
)

CheckpointWriteGuard = Callable[[], Awaitable[None]]


class CheckpointWriteFenceConfigurationError(RuntimeError):
    """Fixed-text error for a saver that cannot prove its write boundary."""

    def __init__(self, code: str) -> None:
        safe_code = (
            code if code in _SAFE_CONFIGURATION_ERRORS else _WRITE_FENCE_NOT_BOUND
        )
        self.code = safe_code
        super().__init__(safe_code)


async def configure_checkpoint_pool_connection(conn: Any) -> None:
    """Hold the maintenance key in shared mode for this pool session's lifetime."""

    try:
        await conn.execute(
            "SELECT pg_catalog.pg_advisory_lock_shared(%s)",
            (CHECKPOINT_MAINTENANCE_LOCK_KEY,),
        )
    except (psycopg.Error, OSError, RuntimeError, TypeError, ValueError):
        raise CheckpointWriteFenceConfigurationError(_POOL_FENCE_FAILED) from None


class FencedAsyncPostgresSaver(AsyncPostgresSaver):
    """Async saver requiring a dedicated-fence proof before every mutation."""

    def __init__(
        self,
        conn: Any,
        pipe: Any = None,
        serde: Any = None,
        *,
        write_guard: CheckpointWriteGuard | None,
    ) -> None:
        if write_guard is None or not callable(write_guard):
            raise CheckpointWriteFenceConfigurationError(_WRITE_FENCE_NOT_BOUND)
        super().__init__(conn=conn, pipe=pipe, serde=serde)
        self._write_guard = write_guard

    async def setup(self) -> None:
        """Runtime credentials may never run third-party checkpoint DDL."""

        raise CheckpointWriteFenceConfigurationError(_RUNTIME_SETUP_FORBIDDEN)

    @asynccontextmanager
    async def _cursor(self, *, pipeline: bool = False) -> AsyncIterator[Any]:
        # The upstream context acquires the already-configured pool connection
        # first.  Its session-level shared lock then closes the race between
        # this dedicated proof and the caller's first checkpoint statement.
        async with super()._cursor(pipeline=pipeline) as cursor:
            if pipeline:
                await self._write_guard()
            yield cursor
