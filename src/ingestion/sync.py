"""Dormant ordinary Exchange Sync reconciliation coordinator.

This module intentionally contains no scheduler, startup, or runtime wiring.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Generic, Protocol, TypeVar
from uuid import uuid4

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from src.domain.errors import DatabaseOperationError
from src.domain.errors import (
    StaleFence,
    SyncAuthorizationError,
    SyncContractError,
    SyncCursorInvalidError,
    SyncTransientError,
)
from src.ingestion.models import (
    MAX_SYNC_CHANGES_PER_BATCH,
    POSTGRES_BIGINT_MAX,
    ChangeKind,
    IngressSource,
    SyncBatch,
    SyncChange,
)
from src.ingestion.normalization import normalize_sync_change
from src.ingestion.ownership import ownership_advisory_lock_key
from src.ingestion.policy import (
    FolderScope,
    PolicySnapshotUnavailableError,
    ProcessingPolicyResolver,
    require_canonical_folder_key,
)


_SAFE_CODE_PATTERN: Final = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}\Z")
_SYNC_LOCK_TIMEOUT: Final = "5000ms"
_SYNC_STATEMENT_TIMEOUT: Final = "15000ms"
_SYNC_IDLE_TRANSACTION_TIMEOUT: Final = "15000ms"
_COLD_START_REQUIRED_CODE: Final = "sync.cold_start_required"
_LOCAL_CONTRACT_CODE: Final = "sync.local_contract_invalid"
_CURSOR_STALLED_CODE: Final = "sync.cursor_stalled"
_SYNC_AUTHORIZATION_CODE: Final = "exchange.sync.authorization_failed"
_SYNC_CONTRACT_CODE: Final = "exchange.sync.contract_invalid"
_SYNC_CURSOR_INVALID_CODE: Final = "exchange.sync.cursor_invalid"
_SYNC_TRANSIENT_CODE: Final = "exchange.sync.transient_failure"
_ORDINARY_SYNC_CHANGE_KINDS: Final = frozenset(
    {ChangeKind.CREATE, ChangeKind.UPDATE, ChangeKind.DELETE}
)


class SyncRunStatus(StrEnum):
    BUSY_SKIP = "busy_skip"
    COLD_START_PENDING = "cold_start_pending"
    RESET_REQUIRED = "reset_required"
    BLOCKED_CONTRACT = "blocked_contract"
    COLD_START_APPLYING = "cold_start_applying"
    RETRY_DEFERRED = "retry_deferred"
    RETRY_SCHEDULED = "retry_scheduled"
    CAUGHT_UP = "caught_up"
    BUDGET_EXHAUSTED = "budget_exhausted"


_ResultT = TypeVar("_ResultT")


class FolderPermitLease:
    """Concrete one-shot wrapper around one synchronous local release callback."""

    __slots__ = ("_callback", "_released")

    def __init__(self, release_callback: Callable[[], None]) -> None:
        callable_impl = getattr(release_callback, "__call__", None)
        if (
            not callable(release_callback)
            or inspect.iscoroutinefunction(release_callback)
            or inspect.iscoroutinefunction(callable_impl)
        ):
            raise ValueError("release callback must be synchronous")
        self._callback = release_callback
        self._released = False

    def release(self) -> None:
        if self._released:
            raise RuntimeError("permit lease was already released")
        self._released = True
        result = self._callback()
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()
            raise RuntimeError("permit release callback returned an awaitable")
        if result is not None:
            raise RuntimeError("permit release callback must return None")


class FolderPermitProvider(Protocol):
    """Future schedulers supply a nonblocking per-folder permit provider."""

    async def try_acquire(
        self,
        account_id: int,
        canonical_folder: str,
    ) -> FolderPermitLease | None: ...


class ReadyPolicySnapshotProvider(Protocol):
    async def get_ready_snapshot(self, account_id: int) -> object: ...


class SyncPageClient(Protocol):
    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        sync_state: str,
        limit: int,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class _OwnershipSnapshot:
    pipeline_name: str
    generation: int
    fencing_token: int


@dataclass(frozen=True, slots=True)
class _CursorSnapshot:
    cursor: str
    status: str
    version: int
    transient_failures: int
    retry_deferred: bool


@dataclass(frozen=True, slots=True)
class _PreflightSnapshot:
    ownership: _OwnershipSnapshot
    cursor: _CursorSnapshot | None
    immediate_result: SyncRunResult | None


@dataclass(frozen=True, slots=True)
class _SyncSessionOutcome(Generic[_ResultT]):
    acquired: bool
    value: _ResultT | None

    def __post_init__(self) -> None:
        if type(self.acquired) is not bool:
            raise ValueError("acquired must be an exact boolean")
        if not self.acquired and self.value is not None:
            raise ValueError("a busy session cannot contain a value")


@dataclass(frozen=True, slots=True)
class _ChildOutcome(Generic[_ResultT]):
    value: _ResultT | None
    error: BaseException | None


@dataclass(frozen=True, slots=True)
class _ConnectionReturnOutcome:
    returned: bool
    ownership_unknown: bool
    process_error: BaseException | None
    error: BaseException | None

    def __post_init__(self) -> None:
        if type(self.returned) is not bool or type(self.ownership_unknown) is not bool:
            raise ValueError("connection return state must use exact booleans")
        if self.returned == self.ownership_unknown:
            raise ValueError("connection return ownership state is invalid")
        if self.returned and self.error is not None:
            raise ValueError("a confirmed connection return cannot contain an error")
        if self.ownership_unknown and self.process_error is None and self.error is None:
            raise ValueError("unknown connection ownership requires a failure")


class _SyncSessionLease:
    """Retained backend plus a one-way unknown-outcome taint marker."""

    __slots__ = ("_connection", "_tainted")

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._tainted = False

    @property
    def connection(self) -> Any:
        return self._connection

    @property
    def tainted(self) -> bool:
        return self._tainted

    def taint(self) -> None:
        self._tainted = True


def _require_nonnegative_bigint(name: str, value: object) -> int:
    if type(value) is not int or not 0 <= value <= POSTGRES_BIGINT_MAX:
        raise ValueError(f"{name} must be a nonnegative PostgreSQL BIGINT")
    return value


def _require_positive_bigint(name: str, value: object) -> int:
    if type(value) is not int or not 1 <= value <= POSTGRES_BIGINT_MAX:
        raise ValueError(f"{name} must be a positive PostgreSQL BIGINT")
    return value


def _require_canonical_folder(value: object) -> str:
    try:
        return require_canonical_folder_key(value)
    except ValueError:
        raise ValueError("canonical_folder must already be normalized") from None


@dataclass(frozen=True, slots=True)
class SyncRunResult:
    status: SyncRunStatus
    pages_committed: int
    changes_observed: int
    safe_code: str | None

    def __post_init__(self) -> None:
        if type(self.status) is not SyncRunStatus:
            raise ValueError("status must be an exact SyncRunStatus")
        _require_nonnegative_bigint("pages_committed", self.pages_committed)
        _require_nonnegative_bigint("changes_observed", self.changes_observed)
        if self.safe_code is not None and (
            type(self.safe_code) is not str
            or _SAFE_CODE_PATTERN.fullmatch(self.safe_code) is None
        ):
            raise ValueError("safe_code must be a bounded safe code or None")


def sync_advisory_lock_keys(
    account_id: int,
    canonical_folder: str,
) -> tuple[int, int]:
    """Return the frozen, cross-process PostgreSQL session-lock identity."""

    account_id = _require_positive_bigint("account_id", account_id)
    canonical_folder = _require_canonical_folder(canonical_folder)
    digest = hashlib.sha256(
        str(account_id).encode("ascii") + b"\x00" + canonical_folder.encode("utf-8")
    ).digest()
    return (
        int.from_bytes(digest[:4], byteorder="big", signed=True),
        int.from_bytes(digest[4:8], byteorder="big", signed=True),
    )


def _deterministic_retry_delay(
    *,
    account_id: int,
    canonical_folder: str,
    expected_version: int,
    failure_count: int,
    retry_after_seconds: int | None,
) -> int:
    """Return the frozen deterministic full-jitter delay in whole seconds."""

    account_id = _require_positive_bigint("account_id", account_id)
    canonical_folder = _require_canonical_folder(canonical_folder)
    expected_version = _require_nonnegative_bigint(
        "expected_version",
        expected_version,
    )
    failure_count = _require_positive_bigint("failure_count", failure_count)
    if retry_after_seconds is not None and (
        type(retry_after_seconds) is not int or not 0 <= retry_after_seconds <= 3600
    ):
        raise ValueError("retry_after_seconds must be between 0 and 3600")
    jitter_ceiling = min(300, 2 ** min(failure_count - 1, 9))
    digest = hashlib.sha256(
        b"sync-transient-backoff-v1\x00"
        + str(account_id).encode("ascii")
        + b"\x00"
        + canonical_folder.encode("utf-8")
        + b"\x00"
        + str(expected_version).encode("ascii")
        + b"\x00"
        + str(failure_count).encode("ascii")
    ).digest()
    local_delay = 1 + (
        int.from_bytes(digest[:8], byteorder="big", signed=False) % jitter_ceiling
    )
    return min(3600, max(local_delay, retry_after_seconds or 0))


def _database_failure(operation: str, message: str) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation=operation,
        retryable=True,
        message=message,
    )


def _database_invariant(operation: str, message: str) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation=operation,
        retryable=False,
        message=message,
    )


def _row_values(row: object, columns: tuple[str, ...]) -> tuple[object, ...]:
    try:
        if isinstance(row, Mapping):
            return tuple(row[column] for column in columns)
        if isinstance(row, (tuple, list)) and len(row) >= len(columns):
            return tuple(row[index] for index in range(len(columns)))
    except (KeyError, IndexError, TypeError):
        pass
    raise _database_invariant(
        "sync_database_row",
        "sync database row is invalid",
    )


def _connection_is_open_idle(connection: object) -> bool:
    info = getattr(connection, "info", None)
    return (
        getattr(connection, "closed", None) is False
        and getattr(info, "transaction_status", None) is TransactionStatus.IDLE
    )


def _connection_is_open_intrans(connection: object) -> bool:
    info = getattr(connection, "info", None)
    return (
        getattr(connection, "closed", None) is False
        and getattr(info, "transaction_status", None) is TransactionStatus.INTRANS
    )


async def _caller_owned_transaction(
    session: _SyncSessionLease,
    operation: Callable[[Any], Awaitable[_ResultT]],
) -> _ResultT:
    """Run one short XID and taint only unknown begin/commit/rollback outcomes."""

    transaction = session.connection.transaction()
    try:
        await transaction.__aenter__()
    except BaseException:
        session.taint()
        raise
    try:
        connection_is_open_intrans = _connection_is_open_intrans(session.connection)
    except BaseException:
        session.taint()
        raise
    if not connection_is_open_intrans:
        session.taint()
        raise _database_failure(
            "sync_session_tainted",
            "sync transaction did not enter on an open connection",
        )
    try:
        value = await operation(session.connection)
    except BaseException as primary:
        try:
            await transaction.__aexit__(type(primary), primary, primary.__traceback__)
        except BaseException as rollback_error:
            session.taint()
            if not isinstance(rollback_error, Exception):
                raise rollback_error
        try:
            connection_is_open_idle = _connection_is_open_idle(session.connection)
        except BaseException as health_error:
            session.taint()
            if not isinstance(health_error, Exception):
                raise health_error
        else:
            if not connection_is_open_idle:
                session.taint()
        raise primary
    try:
        await transaction.__aexit__(None, None, None)
    except BaseException:
        session.taint()
        raise
    try:
        connection_is_open_idle = _connection_is_open_idle(session.connection)
    except BaseException:
        session.taint()
        raise
    if not connection_is_open_idle:
        session.taint()
        raise _database_failure(
            "sync_session_tainted",
            "sync transaction did not return an idle connection",
        )
    return value


async def _configure_sync_xid(connection: Any, account_id: int) -> None:
    """Configure one shared READ COMMITTED reconciliation transaction."""

    await connection.execute("SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
    await connection.execute(
        "SELECT "
        "pg_catalog.set_config('lock_timeout', %s, true), "
        "pg_catalog.set_config('statement_timeout', %s, true), "
        "pg_catalog.set_config("
        "'idle_in_transaction_session_timeout', %s, true)",
        (
            _SYNC_LOCK_TIMEOUT,
            _SYNC_STATEMENT_TIMEOUT,
            _SYNC_IDLE_TRANSACTION_TIMEOUT,
        ),
    )
    await connection.execute(
        "SELECT pg_catalog.pg_advisory_xact_lock_shared(%s)",
        (ownership_advisory_lock_key(account_id),),
    )


async def _read_current_ownership(
    connection: Any,
    account_id: int,
    expected: _OwnershipSnapshot | None = None,
    *,
    for_key_share: bool = True,
) -> _OwnershipSnapshot:
    """Read or exact-match the current-ingress ownership fence."""

    if type(for_key_share) is not bool:
        raise ValueError("for_key_share must be an exact boolean")
    lock_clause = " FOR KEY SHARE" if for_key_share else ""

    if expected is None:
        cursor = await connection.execute(
            "SELECT pipeline_name, generation, fencing_token "
            "FROM public.pipeline_ownership "
            "WHERE account_id = %s AND state = 'current_ingress'" + lock_clause,
            (account_id,),
        )
    else:
        cursor = await connection.execute(
            "SELECT pipeline_name, generation, fencing_token "
            "FROM public.pipeline_ownership "
            "WHERE account_id = %s AND pipeline_name = %s "
            "AND generation = %s AND fencing_token = %s "
            "AND state = 'current_ingress'" + lock_clause,
            (
                account_id,
                expected.pipeline_name,
                expected.generation,
                expected.fencing_token,
            ),
        )
    row = await cursor.fetchone()
    if row is None:
        raise StaleFence()
    pipeline_name, generation, fencing_token = _row_values(
        row,
        ("pipeline_name", "generation", "fencing_token"),
    )
    if (
        type(pipeline_name) is not str
        or not pipeline_name
        or type(generation) is not int
        or not 1 <= generation <= POSTGRES_BIGINT_MAX
        or type(fencing_token) is not int
        or not 1 <= fencing_token <= POSTGRES_BIGINT_MAX
    ):
        raise _database_invariant(
            "sync_pipeline_ownership",
            "sync pipeline ownership row is invalid",
        )
    snapshot = _OwnershipSnapshot(
        pipeline_name=pipeline_name,
        generation=generation,
        fencing_token=fencing_token,
    )
    if expected is not None and snapshot != expected:
        raise StaleFence()
    return snapshot


def _single_boolean(row: object, column: str) -> bool:
    try:
        if isinstance(row, Mapping):
            value = row[column]
        elif isinstance(row, (tuple, list)) and len(row) == 1:
            value = row[0]
        else:
            raise ValueError
    except (KeyError, IndexError, TypeError, ValueError):
        raise _database_failure(
            "sync_session_acquire",
            "sync session lock result is invalid",
        ) from None
    if type(value) is not bool:
        raise _database_failure(
            "sync_session_acquire",
            "sync session lock result is invalid",
        )
    return value


def _validated_sync_batch(
    raw_page: object,
    configured_page_limit: int,
) -> SyncBatch | None:
    """Return a reconstructed exact DTO or reject before any Inbox DML."""

    if type(raw_page) is not SyncBatch:
        return None
    try:
        if (
            type(raw_page.contract_version) is not str
            or raw_page.contract_version != "exchange_sync_contract_v2"
            or type(raw_page.cursor) is not str
            or type(raw_page.changes) is not tuple
            or len(raw_page.changes) > configured_page_limit
            or type(raw_page.includes_last) is not bool
        ):
            return None
        clean_changes: list[SyncChange] = []
        for change in raw_page.changes:
            if (
                type(change) is not SyncChange
                or type(change.kind) is not ChangeKind
                or change.kind not in _ORDINARY_SYNC_CHANGE_KINDS
                or type(change.external_email_id) is not str
                or (
                    change.source_version is not None
                    and type(change.source_version) is not str
                )
                or (
                    change.item is not None
                    and type(change.item) is not MappingProxyType
                )
            ):
                return None
            clean_changes.append(
                SyncChange(
                    kind=change.kind,
                    external_email_id=change.external_email_id,
                    item=change.item,
                    source_version=change.source_version,
                )
            )
        return SyncBatch(
            contract_version=raw_page.contract_version,
            cursor=raw_page.cursor,
            changes=tuple(clean_changes),
            includes_last=raw_page.includes_last,
        )
    except Exception:
        return None


def _trusted_retry_hint(error: SyncTransientError) -> int | None:
    """Read only a frozen exact-base instance dictionary without descriptors."""

    if type(error) is not SyncTransientError:
        return None
    fields = object.__getattribute__(error, "__dict__")
    if type(fields) is not dict:
        return None
    raw_hint = dict.get(fields, "retry_after_seconds")
    if raw_hint is None:
        return None
    if type(raw_hint) is int and 0 <= raw_hint <= 3600:
        return raw_hint
    return None


class _SyncSessionRunner:
    """Reusable permit/pool/session-lock safety protocol for Sync operations."""

    def __init__(self, *, pool: Any, permit: Any, cleanup_timeout: float) -> None:
        if (
            type(cleanup_timeout) not in (int, float)
            or not math.isfinite(float(cleanup_timeout))
            or not 0 < float(cleanup_timeout) <= 30
        ):
            raise ValueError("cleanup_timeout must be finite and between 0 and 30")
        self._pool = pool
        self._permit = permit
        self._cleanup_timeout = float(cleanup_timeout)

    def _validate_pool_contract(self, connection: object) -> None:
        kwargs = getattr(self._pool, "kwargs", None)
        if (
            not isinstance(kwargs, Mapping)
            or kwargs.get("autocommit") is not True
            or getattr(self._pool, "close_returns", None) is not False
            or getattr(connection, "autocommit", None) is not True
            or not self._connection_is_open_idle(connection)
        ):
            raise _database_failure(
                "sync_pool_contract",
                "sync pool connection contract is invalid",
            )

    @staticmethod
    def _connection_is_open_idle(connection: object) -> bool:
        return _connection_is_open_idle(connection)

    @staticmethod
    def _is_process_control(error: BaseException | None) -> bool:
        return error is not None and not isinstance(error, Exception)

    @staticmethod
    def _consume_task(task: asyncio.Task[Any]) -> None:
        try:
            task.exception()
        except BaseException:
            pass

    async def _hard_bounded_reap(
        self,
        task: asyncio.Task[Any],
    ) -> BaseException | None:
        if task.done():
            self._consume_task(task)
            return None
        task.cancel()
        try:
            async with asyncio.timeout(self._cleanup_timeout):
                await asyncio.shield(task)
        except BaseException as error:
            if task.done():
                self._consume_task(task)
            else:
                task.add_done_callback(self._consume_task)
            if self._is_process_control(error):
                return error
        return None

    @staticmethod
    async def _capture_child(
        awaitable: Awaitable[_ResultT],
    ) -> _ChildOutcome[_ResultT]:
        try:
            return _ChildOutcome(value=await awaitable, error=None)
        except BaseException as error:
            return _ChildOutcome(value=None, error=error)

    async def _run_bounded_child(
        self,
        awaitable: Awaitable[_ResultT],
    ) -> tuple[_ResultT | None, BaseException | None]:
        task = asyncio.create_task(self._capture_child(awaitable))
        try:
            async with asyncio.timeout(self._cleanup_timeout):
                outcome = await asyncio.shield(task)
            return outcome.value, outcome.error
        except BaseException as error:
            reap_process_error = await self._hard_bounded_reap(task)
            if self._is_process_control(error):
                return None, error
            if reap_process_error is not None:
                return None, reap_process_error
            return None, error

    async def _confirmed_close(
        self,
        connection: Any,
    ) -> tuple[bool, BaseException | None]:
        _value, close_error = await self._run_bounded_child(connection.close())
        if getattr(connection, "closed", None) is True:
            return True, close_error

        finish_error: BaseException | None = None
        pgconn = getattr(connection, "pgconn", None)
        finish = getattr(pgconn, "finish", None)
        if callable(finish):
            _value, finish_error = await self._run_bounded_child(
                asyncio.to_thread(finish)
            )
        fallback_error = close_error if close_error is not None else finish_error
        combined_error = next(
            (
                error
                for error in (close_error, finish_error)
                if self._is_process_control(error)
            ),
            fallback_error,
        )
        if finish_error is not None:
            return False, combined_error
        if getattr(connection, "closed", None) is True:
            return True, close_error
        if combined_error is None:
            combined_error = _database_failure(
                "sync_session_cleanup",
                "sync connection physical close was not confirmed",
            )
        return False, combined_error

    async def _return_connection(
        self,
        connection: Any,
        *,
        deadline: float | None = None,
    ) -> _ConnectionReturnOutcome:
        try:
            awaitable = self._pool.putconn(connection)
        except BaseException as error:
            return _ConnectionReturnOutcome(
                returned=False,
                ownership_unknown=True,
                process_error=error if self._is_process_control(error) else None,
                error=error if not self._is_process_control(error) else None,
            )
        if not inspect.isawaitable(awaitable):
            return _ConnectionReturnOutcome(
                returned=False,
                ownership_unknown=True,
                process_error=None,
                error=TypeError("pool putconn must return an awaitable"),
            )

        loop = asyncio.get_running_loop()
        exact_deadline = (
            loop.time() + self._cleanup_timeout if deadline is None else deadline
        )
        child = self._capture_child(awaitable)
        try:
            task = asyncio.create_task(child)
        except BaseException as error:
            child.close()
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            return _ConnectionReturnOutcome(
                returned=False,
                ownership_unknown=True,
                process_error=error if self._is_process_control(error) else None,
                error=error if not self._is_process_control(error) else None,
            )
        interruptions: list[BaseException] = []
        while not task.done():
            remaining = exact_deadline - loop.time()
            if remaining <= 0:
                break
            try:
                done, _pending = await asyncio.wait({task}, timeout=remaining)
                if task in done or task.done():
                    break
            except BaseException as error:
                interruptions.append(error)

        if task.done():
            child_error = task.result().error
            returned = child_error is None
        else:
            task.add_done_callback(self._consume_task)
            child_error = TimeoutError()
            returned = False

        errors = (*interruptions, child_error)
        process_error = next(
            (error for error in errors if self._is_process_control(error)),
            None,
        )
        ordinary_error = next(
            (
                error
                for error in errors
                if error is not None and not self._is_process_control(error)
            ),
            None,
        )
        return _ConnectionReturnOutcome(
            returned=returned,
            ownership_unknown=not returned,
            process_error=process_error,
            error=ordinary_error,
        )

    @staticmethod
    def _release_permit(lease: FolderPermitLease) -> BaseException | None:
        try:
            lease.release()
        except BaseException as error:
            return error
        return None

    async def _close_return_release(
        self,
        connection: Any,
        lease: FolderPermitLease,
    ) -> tuple[BaseException | None, BaseException | None, bool]:
        confirmed, close_error = await self._confirmed_close(connection)
        return_outcome = (
            await self._return_connection(connection) if confirmed else None
        )
        permit_error = self._release_permit(lease)
        errors = tuple(
            error
            for error in (
                close_error,
                None if return_outcome is None else return_outcome.process_error,
                None if return_outcome is None else return_outcome.error,
                permit_error,
            )
            if error is not None
        )
        process_error = next(
            (error for error in errors if self._is_process_control(error)),
            None,
        )
        ordinary_error = next(
            (error for error in errors if not self._is_process_control(error)),
            None,
        )
        return process_error, ordinary_error, confirmed

    async def _unlock(self, connection: Any, keys: tuple[int, int]) -> bool:
        cursor = await connection.execute(
            "SELECT pg_catalog.pg_advisory_unlock(%s, %s) AS released",
            keys,
        )
        row = await cursor.fetchone()
        try:
            if isinstance(row, Mapping):
                value = row["released"]
            elif isinstance(row, (tuple, list)) and len(row) == 1:
                value = row[0]
            else:
                return False
        except (KeyError, IndexError, TypeError):
            return False
        return type(value) is bool and value is True

    async def _bounded_unlock(
        self,
        connection: Any,
        keys: tuple[int, int],
    ) -> tuple[bool, BaseException | None]:
        result, error = await self._run_bounded_child(self._unlock(connection, keys))
        return type(result) is bool and result is True, error

    async def run(
        self,
        account_id: int,
        canonical_folder: str,
        operation: Callable[[_SyncSessionLease], Awaitable[_ResultT]],
    ) -> _SyncSessionOutcome[_ResultT]:
        keys = sync_advisory_lock_keys(account_id, canonical_folder)
        if not callable(operation):
            raise ValueError("operation must be callable")

        lease = await self._permit.try_acquire(
            account_id,
            canonical_folder,
        )
        if lease is None:
            return _SyncSessionOutcome(acquired=False, value=None)
        if type(lease) is not FolderPermitLease:
            raise _database_failure(
                "sync_permit_contract",
                "sync folder permit result is invalid",
            )

        connection: Any | None = None
        try:
            connection = await self._pool.getconn()
        except BaseException as primary:
            permit_error = self._release_permit(lease)
            if self._is_process_control(primary):
                raise primary
            if self._is_process_control(permit_error):
                raise permit_error
            raise _database_failure(
                "sync_pool_checkout",
                "sync pool checkout failed",
            ) from None

        try:
            self._validate_pool_contract(connection)
        except BaseException as primary:
            process_error, cleanup_error, _confirmed = await self._close_return_release(
                connection, lease
            )
            if self._is_process_control(primary):
                raise primary
            if process_error is not None:
                raise process_error
            if cleanup_error is not None:
                raise _database_failure(
                    "sync_session_cleanup",
                    "sync session cleanup failed",
                )
            if isinstance(primary, DatabaseOperationError):
                raise primary
            raise _database_failure(
                "sync_pool_contract",
                "sync pool connection contract is invalid",
            ) from None

        try:
            cursor = await connection.execute(
                "SELECT pg_catalog.pg_try_advisory_lock(%s, %s) AS acquired",
                keys,
            )
            row = await cursor.fetchone()
            acquired = _single_boolean(row, "acquired")
        except BaseException as primary:
            process_error, cleanup_error, _confirmed = await self._close_return_release(
                connection, lease
            )
            if self._is_process_control(primary):
                raise primary
            if process_error is not None:
                raise process_error
            if cleanup_error is not None:
                raise _database_failure(
                    "sync_session_cleanup",
                    "sync session cleanup failed",
                )
            if isinstance(primary, DatabaseOperationError):
                raise primary
            raise _database_failure(
                "sync_session_acquire",
                "sync session lock acquisition failed",
            ) from None

        if not acquired:
            busy_health_error: BaseException | None = None
            try:
                busy_connection_is_open_idle = self._connection_is_open_idle(connection)
            except BaseException as error:
                busy_connection_is_open_idle = False
                busy_health_error = error
            if not busy_connection_is_open_idle:
                (
                    process_error,
                    _cleanup_error,
                    _confirmed,
                ) = await self._close_return_release(connection, lease)
                if self._is_process_control(busy_health_error):
                    raise busy_health_error
                if process_error is not None:
                    raise process_error
                raise _database_failure(
                    "sync_session_cleanup",
                    "sync session cleanup failed",
                )
            return_outcome = await self._return_connection(connection)
            permit_error = self._release_permit(lease)
            process_error = next(
                (
                    error
                    for error in (return_outcome.process_error, permit_error)
                    if self._is_process_control(error)
                ),
                None,
            )
            if process_error is not None:
                raise process_error
            if (
                not return_outcome.returned
                or return_outcome.error is not None
                or permit_error is not None
            ):
                raise _database_failure(
                    "sync_session_cleanup",
                    "sync session cleanup failed",
                )
            return _SyncSessionOutcome(acquired=False, value=None)

        acquired_health_error: BaseException | None = None
        try:
            acquired_connection_is_open_idle = self._connection_is_open_idle(connection)
        except BaseException as error:
            acquired_connection_is_open_idle = False
            acquired_health_error = error
        if not acquired_connection_is_open_idle:
            (
                process_error,
                _cleanup_error,
                _confirmed,
            ) = await self._close_return_release(connection, lease)
            if self._is_process_control(acquired_health_error):
                raise acquired_health_error
            if process_error is not None:
                raise process_error
            raise _database_failure(
                "sync_session_cleanup",
                "sync session cleanup failed",
            )

        value: _ResultT | None = None
        primary_error: BaseException | None = None
        session = _SyncSessionLease(connection)
        try:
            value = await operation(session)
        except BaseException as error:
            primary_error = error

        try:
            connection_is_open_idle = self._connection_is_open_idle(connection)
        except BaseException as health_error:
            session.taint()
            if primary_error is None or (
                self._is_process_control(health_error)
                and not self._is_process_control(primary_error)
            ):
                primary_error = health_error
        else:
            if not connection_is_open_idle:
                session.taint()

        if session.tainted:
            (
                process_error,
                _cleanup_error,
                _confirmed,
            ) = await self._close_return_release(connection, lease)
            if self._is_process_control(primary_error):
                raise primary_error
            if process_error is not None:
                raise process_error
            if primary_error is not None:
                raise primary_error
            raise _database_failure(
                "sync_session_tainted",
                "sync session operation outcome is unknown",
            )

        unlocked, unlock_error = await self._bounded_unlock(connection, keys)
        post_unlock_health_error: BaseException | None = None
        post_unlock_is_open_idle = False
        if unlocked:
            try:
                post_unlock_is_open_idle = self._connection_is_open_idle(connection)
            except BaseException as error:
                post_unlock_health_error = error
        cleanup_failed = not unlocked or not post_unlock_is_open_idle
        close_error: BaseException | None = None
        close_confirmed = True
        if cleanup_failed:
            close_confirmed, close_error = await self._confirmed_close(connection)
        return_outcome = (
            await self._return_connection(connection) if close_confirmed else None
        )
        permit_error = self._release_permit(lease)

        if self._is_process_control(primary_error):
            raise primary_error
        process_error = next(
            (
                error
                for error in (
                    unlock_error,
                    post_unlock_health_error,
                    close_error,
                    None if return_outcome is None else return_outcome.process_error,
                    permit_error,
                )
                if self._is_process_control(error)
            ),
            None,
        )
        if process_error is not None:
            raise process_error
        if (
            cleanup_failed
            or close_error is not None
            or return_outcome is None
            or not return_outcome.returned
            or return_outcome.error is not None
            or permit_error is not None
        ):
            raise _database_failure(
                "sync_session_cleanup",
                "sync session cleanup failed",
            )
        if primary_error is not None:
            raise primary_error
        return _SyncSessionOutcome(acquired=True, value=value)


def _require_positive_pages(name: str, value: object) -> int:
    if type(value) is not int or not 1 <= value <= POSTGRES_BIGINT_MAX:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _require_positive_seconds(name: str, value: object) -> float:
    if (
        type(value) not in (int, float)
        or not math.isfinite(float(value))
        or float(value) <= 0
    ):
        raise ValueError(f"{name} must be positive and finite")
    return float(value)


class SyncCoordinator:
    """Dormant ordinary Sync page coordinator; no runtime wiring lives here."""

    def __init__(
        self,
        *,
        page_client: SyncPageClient,
        snapshot_provider: ReadyPolicySnapshotProvider,
        policy_resolver: ProcessingPolicyResolver,
        folder_permit: FolderPermitProvider,
        sync_pool: Any,
        inbox_repository: Any,
        page_limit: int,
        default_max_pages: int,
        default_max_run_seconds: float,
        cleanup_timeout: float,
    ) -> None:
        if (
            type(page_limit) is not int
            or not 1 <= page_limit <= MAX_SYNC_CHANGES_PER_BATCH
        ):
            raise ValueError(
                f"page_limit must be between 1 and {MAX_SYNC_CHANGES_PER_BATCH}"
            )
        self._page_limit = page_limit
        self._default_max_pages = _require_positive_pages(
            "default_max_pages",
            default_max_pages,
        )
        self._default_max_run_seconds = _require_positive_seconds(
            "default_max_run_seconds",
            default_max_run_seconds,
        )
        try:
            self._session_runner = _SyncSessionRunner(
                pool=sync_pool,
                permit=folder_permit,
                cleanup_timeout=cleanup_timeout,
            )
        except ValueError:
            raise ValueError(
                "cleanup_timeout must be finite and between 0 and 30"
            ) from None
        if type(policy_resolver) is not ProcessingPolicyResolver:
            raise ValueError(
                "policy_resolver must be an exact ProcessingPolicyResolver"
            )
        self._page_client = page_client
        self._snapshot_provider = snapshot_provider
        self._policy_resolver = policy_resolver
        self._inbox_repository = inbox_repository

    async def _ready_scope(
        self,
        account_id: int,
        canonical_folder: str,
    ) -> tuple[FolderScope, object]:
        try:
            snapshot = await self._snapshot_provider.get_ready_snapshot(account_id)
            scopes = self._policy_resolver.configured_scopes(snapshot)
        except PolicySnapshotUnavailableError:
            raise
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            raise PolicySnapshotUnavailableError() from None
        scope = next(
            (
                candidate
                for candidate in scopes
                if candidate.canonical_key == canonical_folder
            ),
            None,
        )
        if scope is None:
            raise PolicySnapshotUnavailableError()
        return scope, snapshot

    @staticmethod
    def _immediate_result(status: str) -> SyncRunResult | None:
        dispositions = {
            "cold_start_pending": (
                SyncRunStatus.COLD_START_PENDING,
                _COLD_START_REQUIRED_CODE,
            ),
            "reset_required": (
                SyncRunStatus.RESET_REQUIRED,
                "sync.cursor_reset_required",
            ),
            "blocked_contract": (
                SyncRunStatus.BLOCKED_CONTRACT,
                "sync.blocked_contract",
            ),
            "cold_start_applying": (
                SyncRunStatus.COLD_START_APPLYING,
                "sync.cold_start_applying",
            ),
        }
        disposition = dispositions.get(status)
        if disposition is None:
            return None
        return SyncRunResult(
            status=disposition[0],
            pages_committed=0,
            changes_observed=0,
            safe_code=disposition[1],
        )

    async def _preflight(
        self,
        session: _SyncSessionLease,
        account_id: int,
        folder: str,
    ) -> _PreflightSnapshot:
        async def operation(connection: Any) -> _PreflightSnapshot:
            await _configure_sync_xid(connection, account_id)
            ownership = await _read_current_ownership(connection, account_id)
            cursor = await connection.execute(
                "SELECT cursor, status, version, transient_failures, "
                "(retry_after_at IS NOT NULL "
                "AND retry_after_at > pg_catalog.clock_timestamp()) "
                "AS retry_deferred FROM public.sync_cursors "
                "WHERE account_id = %s AND folder_key = %s FOR UPDATE",
                (account_id, folder),
            )
            row = await cursor.fetchone()
            if row is None:
                inserted = await connection.execute(
                    "INSERT INTO public.sync_cursors ("
                    "account_id, folder_key, cursor, status, "
                    "blocked_reason_code"
                    ") VALUES (%s, %s, NULL, 'cold_start_pending', %s) "
                    "ON CONFLICT (account_id, folder_key) DO NOTHING "
                    "RETURNING cursor, status, version, transient_failures, "
                    "false AS retry_deferred",
                    (account_id, folder, _COLD_START_REQUIRED_CODE),
                )
                row = await inserted.fetchone()
                if row is None:
                    raced = await connection.execute(
                        "SELECT cursor, status, version, transient_failures, "
                        "(retry_after_at IS NOT NULL AND retry_after_at > "
                        "pg_catalog.clock_timestamp()) AS retry_deferred "
                        "FROM public.sync_cursors "
                        "WHERE account_id = %s AND folder_key = %s FOR UPDATE",
                        (account_id, folder),
                    )
                    row = await raced.fetchone()
            if row is None:
                raise _database_invariant(
                    "sync_cursor_preflight",
                    "sync cursor preflight row is missing",
                )
            raw_cursor, status, version, failures, retry_deferred = _row_values(
                row,
                (
                    "cursor",
                    "status",
                    "version",
                    "transient_failures",
                    "retry_deferred",
                ),
            )
            if (
                type(status) is not str
                or type(version) is not int
                or not 0 <= version <= POSTGRES_BIGINT_MAX
                or type(failures) is not int
                or not 0 <= failures <= POSTGRES_BIGINT_MAX
                or type(retry_deferred) is not bool
            ):
                raise _database_invariant(
                    "sync_cursor_preflight",
                    "sync cursor preflight row is invalid",
                )
            immediate = self._immediate_result(status)
            if immediate is not None:
                return _PreflightSnapshot(ownership, None, immediate)
            if status != "active" or (
                type(raw_cursor) is not str
                or not raw_cursor
                or raw_cursor != raw_cursor.strip()
                or len(raw_cursor) > 8192
            ):
                raise _database_invariant(
                    "sync_cursor_preflight",
                    "active sync cursor row is invalid",
                )
            active = _CursorSnapshot(
                cursor=raw_cursor,
                status=status,
                version=version,
                transient_failures=failures,
                retry_deferred=retry_deferred,
            )
            if retry_deferred:
                return _PreflightSnapshot(
                    ownership,
                    None,
                    SyncRunResult(
                        status=SyncRunStatus.RETRY_DEFERRED,
                        pages_committed=0,
                        changes_observed=0,
                        safe_code="sync.retry_deferred",
                    ),
                )
            return _PreflightSnapshot(ownership, active, None)

        try:
            return await _caller_owned_transaction(session, operation)
        except (StaleFence, DatabaseOperationError):
            raise
        except psycopg.Error:
            raise _database_failure(
                "sync_cursor_preflight",
                "sync cursor preflight failed",
            ) from None

    async def _lock_expected_cursor(
        self,
        connection: Any,
        account_id: int,
        folder: str,
        expected: _CursorSnapshot,
    ) -> None:
        cursor = await connection.execute(
            "SELECT cursor, status, version, transient_failures "
            "FROM public.sync_cursors "
            "WHERE account_id = %s AND folder_key = %s FOR UPDATE",
            (account_id, folder),
        )
        row = await cursor.fetchone()
        if row is None:
            raise StaleFence()
        values = _row_values(
            row,
            ("cursor", "status", "version", "transient_failures"),
        )
        if values != (
            expected.cursor,
            "active",
            expected.version,
            expected.transient_failures,
        ):
            raise StaleFence()

    @staticmethod
    def _contract_fingerprint(
        account_id: int,
        folder: str,
        reason_code: str,
    ) -> str:
        return hashlib.sha256(
            b"sync-contract-fingerprint-v1\x00"
            + str(account_id).encode("ascii")
            + b"\x00"
            + folder.encode("utf-8")
            + b"\x00"
            + reason_code.encode("ascii")
        ).hexdigest()

    @staticmethod
    def _audit_event_key(
        account_id: int,
        folder: str,
        version: int,
        reason_code: str,
    ) -> str:
        return hashlib.sha256(
            b"sync-error-audit-v1\x00"
            + str(account_id).encode("ascii")
            + b"\x00"
            + folder.encode("utf-8")
            + b"\x00"
            + str(version).encode("ascii")
            + b"\x00"
            + reason_code.encode("ascii")
        ).hexdigest()

    async def _append_error_audit(
        self,
        connection: Any,
        *,
        account_id: int,
        folder: str,
        expected_version: int,
        reason_code: str,
        result: str,
    ) -> None:
        object_fingerprint = hashlib.sha256(
            b"sync-cursor-object-v1\x00"
            + str(account_id).encode("ascii")
            + b"\x00"
            + folder.encode("utf-8")
        ).hexdigest()
        await connection.execute(
            "INSERT INTO public.audit_events ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata"
            ") VALUES (%s, %s, %s, NULL, 'sync_cursor', %s, "
            "'sync.error', %s, 'sync_coordinator', %s, %s)",
            (
                str(uuid4()),
                self._audit_event_key(
                    account_id,
                    folder,
                    expected_version,
                    reason_code,
                ),
                account_id,
                object_fingerprint,
                result,
                reason_code,
                Jsonb({"safe_code": reason_code}),
            ),
        )

    async def _commit_error(
        self,
        session: _SyncSessionLease,
        *,
        account_id: int,
        folder: str,
        ownership: _OwnershipSnapshot,
        expected: _CursorSnapshot,
        reason_code: str,
        target: SyncRunStatus,
        pages_committed: int,
        changes_observed: int,
        retry_after_seconds: int | None = None,
    ) -> SyncRunResult:
        if target not in {
            SyncRunStatus.RESET_REQUIRED,
            SyncRunStatus.BLOCKED_CONTRACT,
            SyncRunStatus.RETRY_SCHEDULED,
        }:
            raise ValueError("unsupported sync error target")

        async def operation(connection: Any) -> None:
            await _configure_sync_xid(connection, account_id)
            await self._lock_expected_cursor(
                connection,
                account_id,
                folder,
                expected,
            )
            await _read_current_ownership(connection, account_id, ownership)
            if target is SyncRunStatus.RESET_REQUIRED:
                updated = await connection.execute(
                    "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
                    "UPDATE public.sync_cursors AS cursor SET "
                    "status = 'reset_required', blocked_reason_code = %s, "
                    "contract_fingerprint = NULL, blocked_at = NULL, "
                    "transient_failures = 0, retry_after_at = NULL, "
                    "version = cursor.version + 1, last_attempt_at = stamp.at, "
                    "updated_at = stamp.at FROM stamp "
                    "WHERE account_id = %s AND folder_key = %s "
                    "AND status = 'active' AND cursor.cursor = %s "
                    "AND version = %s RETURNING version",
                    (
                        reason_code,
                        account_id,
                        folder,
                        expected.cursor,
                        expected.version,
                    ),
                )
            elif target is SyncRunStatus.BLOCKED_CONTRACT:
                updated = await connection.execute(
                    "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
                    "UPDATE public.sync_cursors AS cursor SET "
                    "status = 'blocked_contract', blocked_reason_code = %s, "
                    "contract_fingerprint = %s, blocked_at = stamp.at, "
                    "transient_failures = 0, retry_after_at = NULL, "
                    "version = cursor.version + 1, last_attempt_at = stamp.at, "
                    "updated_at = stamp.at FROM stamp "
                    "WHERE account_id = %s AND folder_key = %s "
                    "AND status = 'active' AND cursor.cursor = %s "
                    "AND version = %s RETURNING version",
                    (
                        reason_code,
                        self._contract_fingerprint(
                            account_id,
                            folder,
                            reason_code,
                        ),
                        account_id,
                        folder,
                        expected.cursor,
                        expected.version,
                    ),
                )
            else:
                failure_count = expected.transient_failures + 1
                delay = _deterministic_retry_delay(
                    account_id=account_id,
                    canonical_folder=folder,
                    expected_version=expected.version,
                    failure_count=failure_count,
                    retry_after_seconds=retry_after_seconds,
                )
                updated = await connection.execute(
                    "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
                    "UPDATE public.sync_cursors AS cursor SET "
                    "status = 'active', blocked_reason_code = NULL, "
                    "contract_fingerprint = NULL, blocked_at = NULL, "
                    "transient_failures = %s, "
                    "retry_after_at = stamp.at + "
                    "pg_catalog.make_interval(secs => %s), "
                    "version = cursor.version + 1, last_attempt_at = stamp.at, "
                    "updated_at = stamp.at FROM stamp "
                    "WHERE account_id = %s AND folder_key = %s "
                    "AND status = 'active' AND cursor.cursor = %s "
                    "AND version = %s RETURNING version",
                    (
                        failure_count,
                        delay,
                        account_id,
                        folder,
                        expected.cursor,
                        expected.version,
                    ),
                )
            row = await updated.fetchone()
            if row is None:
                raise StaleFence()
            version = _row_values(row, ("version",))[0]
            if version != expected.version + 1:
                raise _database_invariant(
                    "sync_error_transition",
                    "sync error transition version is invalid",
                )
            await self._append_error_audit(
                connection,
                account_id=account_id,
                folder=folder,
                expected_version=expected.version,
                reason_code=reason_code,
                result=target.value,
            )

        try:
            await _caller_owned_transaction(session, operation)
        except (StaleFence, DatabaseOperationError):
            raise
        except psycopg.Error:
            raise _database_failure(
                "sync_error_transition",
                "sync error transition failed",
            ) from None
        return SyncRunResult(
            status=target,
            pages_committed=pages_committed,
            changes_observed=changes_observed,
            safe_code=reason_code,
        )

    async def _commit_page(
        self,
        session: _SyncSessionLease,
        *,
        account_id: int,
        folder: str,
        ownership: _OwnershipSnapshot,
        expected: _CursorSnapshot,
        next_cursor: str,
        events: tuple[Any, ...],
    ) -> _CursorSnapshot:
        async def operation(connection: Any) -> _CursorSnapshot:
            await _configure_sync_xid(connection, account_id)
            await self._lock_expected_cursor(
                connection,
                account_id,
                folder,
                expected,
            )
            await _read_current_ownership(connection, account_id, ownership)
            transaction = self._inbox_repository.transaction(connection)
            for event in events:
                await transaction.insert(
                    event,
                    ownership.generation,
                    ownership.fencing_token,
                )
            updated = await connection.execute(
                "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
                "UPDATE public.sync_cursors AS cursor SET cursor = %s, "
                "status = 'active', blocked_reason_code = NULL, "
                "contract_fingerprint = NULL, blocked_at = NULL, "
                "transient_failures = 0, retry_after_at = NULL, "
                "version = cursor.version + 1, last_success_at = stamp.at, "
                "last_attempt_at = stamp.at, updated_at = stamp.at FROM stamp "
                "WHERE account_id = %s AND folder_key = %s "
                "AND status = 'active' AND cursor.cursor = %s "
                "AND version = %s RETURNING version",
                (
                    next_cursor,
                    account_id,
                    folder,
                    expected.cursor,
                    expected.version,
                ),
            )
            row = await updated.fetchone()
            if row is None:
                raise StaleFence()
            version = _row_values(row, ("version",))[0]
            if version != expected.version + 1:
                raise _database_invariant(
                    "sync_page_commit",
                    "sync page version is invalid",
                )
            return _CursorSnapshot(
                cursor=next_cursor,
                status="active",
                version=version,
                transient_failures=0,
                retry_deferred=False,
            )

        try:
            return await _caller_owned_transaction(session, operation)
        except (StaleFence, DatabaseOperationError):
            raise
        except psycopg.Error:
            raise _database_failure(
                "sync_page_commit",
                "sync page commit failed",
            ) from None

    async def _run_locked(
        self,
        session: _SyncSessionLease,
        account_id: int,
        scope: FolderScope,
        snapshot: object,
        max_pages: int,
        deadline: float,
    ) -> SyncRunResult:
        preflight = await self._preflight(
            session,
            account_id,
            scope.canonical_key,
        )
        if preflight.immediate_result is not None:
            return preflight.immediate_result
        expected = preflight.cursor
        if expected is None:
            raise _database_invariant(
                "sync_cursor_preflight",
                "active sync cursor snapshot is missing",
            )

        pages_committed = 0
        changes_observed = 0
        loop = asyncio.get_running_loop()
        while pages_committed < max_pages:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return SyncRunResult(
                    SyncRunStatus.BUDGET_EXHAUSTED,
                    pages_committed,
                    changes_observed,
                    "sync.budget_exhausted",
                )
            try:
                async with asyncio.timeout(remaining):
                    raw_page = await self._page_client.sync_emails(
                        account_id,
                        scope.sync_folder,
                        expected.cursor,
                        self._page_limit,
                    )
            except TimeoutError:
                return SyncRunResult(
                    SyncRunStatus.BUDGET_EXHAUSTED,
                    pages_committed,
                    changes_observed,
                    "sync.budget_exhausted",
                )
            except SyncCursorInvalidError:
                return await self._commit_error(
                    session,
                    account_id=account_id,
                    folder=scope.canonical_key,
                    ownership=preflight.ownership,
                    expected=expected,
                    reason_code=_SYNC_CURSOR_INVALID_CODE,
                    target=SyncRunStatus.RESET_REQUIRED,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncTransientError as error:
                retry_after_seconds = _trusted_retry_hint(error)
                return await self._commit_error(
                    session,
                    account_id=account_id,
                    folder=scope.canonical_key,
                    ownership=preflight.ownership,
                    expected=expected,
                    reason_code=_SYNC_TRANSIENT_CODE,
                    target=SyncRunStatus.RETRY_SCHEDULED,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                    retry_after_seconds=retry_after_seconds,
                )
            except SyncAuthorizationError:
                return await self._commit_error(
                    session,
                    account_id=account_id,
                    folder=scope.canonical_key,
                    ownership=preflight.ownership,
                    expected=expected,
                    reason_code=_SYNC_AUTHORIZATION_CODE,
                    target=SyncRunStatus.BLOCKED_CONTRACT,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncContractError:
                return await self._commit_error(
                    session,
                    account_id=account_id,
                    folder=scope.canonical_key,
                    ownership=preflight.ownership,
                    expected=expected,
                    reason_code=_SYNC_CONTRACT_CODE,
                    target=SyncRunStatus.BLOCKED_CONTRACT,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

            page = _validated_sync_batch(raw_page, self._page_limit)
            if page is None:
                return await self._commit_error(
                    session,
                    account_id=account_id,
                    folder=scope.canonical_key,
                    ownership=preflight.ownership,
                    expected=expected,
                    reason_code=_LOCAL_CONTRACT_CODE,
                    target=SyncRunStatus.BLOCKED_CONTRACT,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            if not page.includes_last and page.cursor == expected.cursor:
                return await self._commit_error(
                    session,
                    account_id=account_id,
                    folder=scope.canonical_key,
                    ownership=preflight.ownership,
                    expected=expected,
                    reason_code=_CURSOR_STALLED_CODE,
                    target=SyncRunStatus.BLOCKED_CONTRACT,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            try:
                events = tuple(
                    normalize_sync_change(
                        account_id,
                        scope.canonical_key,
                        page.cursor,
                        change,
                        processing_policy=self._policy_resolver.resolve(
                            IngressSource.SYNC,
                            change.kind.value,
                            change.kind,
                            scope.sync_folder,
                            snapshot,
                        ),
                    )
                    for change in page.changes
                )
            except Exception:
                return await self._commit_error(
                    session,
                    account_id=account_id,
                    folder=scope.canonical_key,
                    ownership=preflight.ownership,
                    expected=expected,
                    reason_code=_LOCAL_CONTRACT_CODE,
                    target=SyncRunStatus.BLOCKED_CONTRACT,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            expected = await self._commit_page(
                session,
                account_id=account_id,
                folder=scope.canonical_key,
                ownership=preflight.ownership,
                expected=expected,
                next_cursor=page.cursor,
                events=events,
            )
            pages_committed += 1
            changes_observed += len(page.changes)
            if page.includes_last:
                return SyncRunResult(
                    SyncRunStatus.CAUGHT_UP,
                    pages_committed,
                    changes_observed,
                    None,
                )
            if loop.time() >= deadline:
                return SyncRunResult(
                    SyncRunStatus.BUDGET_EXHAUSTED,
                    pages_committed,
                    changes_observed,
                    "sync.budget_exhausted",
                )
        return SyncRunResult(
            SyncRunStatus.BUDGET_EXHAUSTED,
            pages_committed,
            changes_observed,
            "sync.budget_exhausted",
        )

    async def run_folder(
        self,
        account_id: int,
        canonical_folder: str,
        *,
        max_pages: int | None = None,
        max_run_seconds: float | None = None,
    ) -> SyncRunResult:
        account_id = _require_positive_bigint("account_id", account_id)
        canonical_folder = _require_canonical_folder(canonical_folder)
        effective_max_pages = (
            self._default_max_pages
            if max_pages is None
            else _require_positive_pages("max_pages", max_pages)
        )
        effective_max_run_seconds = (
            self._default_max_run_seconds
            if max_run_seconds is None
            else _require_positive_seconds("max_run_seconds", max_run_seconds)
        )
        deadline = asyncio.get_running_loop().time() + effective_max_run_seconds
        scope, snapshot = await self._ready_scope(account_id, canonical_folder)

        async def operation(session: _SyncSessionLease) -> SyncRunResult:
            return await self._run_locked(
                session,
                account_id,
                scope,
                snapshot,
                effective_max_pages,
                deadline,
            )

        outcome = await self._session_runner.run(
            account_id,
            canonical_folder,
            operation,
        )
        if not outcome.acquired:
            return SyncRunResult(
                status=SyncRunStatus.BUSY_SKIP,
                pages_committed=0,
                changes_observed=0,
                safe_code="sync.busy",
            )
        if type(outcome.value) is not SyncRunResult:
            raise _database_failure(
                "sync_coordinator_result",
                "sync coordinator result is invalid",
            )
        return outcome.value


__all__ = [
    "FolderPermitLease",
    "FolderPermitProvider",
    "ReadyPolicySnapshotProvider",
    "SyncCoordinator",
    "SyncPageClient",
    "SyncRunResult",
    "SyncRunStatus",
    "sync_advisory_lock_keys",
]
