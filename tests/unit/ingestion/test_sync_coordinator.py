from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from dataclasses import FrozenInstanceError, fields
from typing import Any

import pytest
from psycopg.pq import TransactionStatus

import src.ingestion.sync as sync_module
from src.domain.errors import (
    DatabaseOperationError,
    SyncAuthorizationError,
    SyncContractError,
    SyncCursorInvalidError,
    SyncTransientError,
)
from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    ProcessingPolicy,
    SyncBatch,
    SyncChange,
)
from src.ingestion.policy import (
    FolderScope,
    PolicySnapshot,
    PolicySnapshotUnavailableError,
    ProcessingPolicyResolver,
)
from src.ingestion.sync import (
    FolderPermitLease,
    SyncCoordinator,
    SyncRunResult,
    SyncRunStatus,
    _SyncSessionLease,
    _SyncSessionRunner,
    _caller_owned_transaction,
    _deterministic_retry_delay,
    _trusted_retry_hint,
    sync_advisory_lock_keys,
)


class _HostileInt(int):
    def __int__(self) -> int:
        raise AssertionError("int subclass behavior must not execute")

    def __index__(self) -> int:
        raise AssertionError("int subclass behavior must not execute")

    def __eq__(self, _other: object) -> bool:
        raise AssertionError("int subclass behavior must not execute")

    def __lt__(self, _other: object) -> bool:
        raise AssertionError("int subclass behavior must not execute")


class _HostileStr(str):
    __hash__ = str.__hash__

    def strip(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("str subclass behavior must not execute")

    def encode(self, *_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("str subclass behavior must not execute")

    def __eq__(self, _other: object) -> bool:
        raise AssertionError("str subclass behavior must not execute")


def test_sync_run_status_is_the_exact_frozen_vocabulary() -> None:
    assert {status.value for status in SyncRunStatus} == {
        "busy_skip",
        "cold_start_pending",
        "reset_required",
        "blocked_contract",
        "cold_start_applying",
        "retry_deferred",
        "retry_scheduled",
        "caught_up",
        "budget_exhausted",
    }


def test_sync_run_result_is_immutable_bounded_and_never_exposes_a_cursor() -> None:
    result = SyncRunResult(
        status=SyncRunStatus.CAUGHT_UP,
        pages_committed=2,
        changes_observed=3,
        safe_code=None,
    )

    assert tuple(field.name for field in fields(result)) == (
        "status",
        "pages_committed",
        "changes_observed",
        "safe_code",
    )
    assert not hasattr(result, "cursor")
    assert not hasattr(result, "__dict__")
    assert SyncRunResult.__slots__ == (
        "status",
        "pages_committed",
        "changes_observed",
        "safe_code",
    )
    with pytest.raises(FrozenInstanceError):
        result.pages_committed = 4  # type: ignore[misc]


def test_sync_run_result_accepts_exact_bigint_and_safe_code_boundaries() -> None:
    from src.ingestion.models import POSTGRES_BIGINT_MAX

    assert (
        SyncRunResult(
            SyncRunStatus.CAUGHT_UP,
            0,
            POSTGRES_BIGINT_MAX,
            "a",
        ).safe_code
        == "a"
    )
    assert (
        len(
            SyncRunResult(
                SyncRunStatus.BUDGET_EXHAUSTED,
                POSTGRES_BIGINT_MAX,
                0,
                "a" * 64,
            ).safe_code
            or ""
        )
        == 64
    )


@pytest.mark.parametrize(
    "values",
    [
        (SyncRunStatus.CAUGHT_UP, 2**63, 0, None),
        (SyncRunStatus.CAUGHT_UP, 0, 2**63, None),
        (SyncRunStatus.CAUGHT_UP, 0, 0, "a" * 65),
        (SyncRunStatus.CAUGHT_UP, 0, 0, _HostileStr("safe.code")),
        (SyncRunStatus.CAUGHT_UP, _HostileInt(1), 0, None),
    ],
    ids=[
        "pages-overflow",
        "changes-overflow",
        "safe-code-too-long",
        "hostile-safe-code",
        "hostile-pages",
    ],
)
def test_sync_run_result_rejects_values_past_exact_public_boundaries(
    values: tuple[object, object, object, object],
) -> None:
    with pytest.raises(ValueError):
        SyncRunResult(*values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("values", "message"),
    [
        (
            {
                "status": SyncRunStatus.CAUGHT_UP,
                "pages_committed": True,
                "changes_observed": 0,
                "safe_code": None,
            },
            "pages_committed",
        ),
        (
            {
                "status": SyncRunStatus.CAUGHT_UP,
                "pages_committed": 0,
                "changes_observed": -1,
                "safe_code": None,
            },
            "changes_observed",
        ),
        (
            {
                "status": SyncRunStatus.CAUGHT_UP,
                "pages_committed": 0,
                "changes_observed": 0,
                "safe_code": " raw ",
            },
            "safe_code",
        ),
        (
            {
                "status": "not-a-status",
                "pages_committed": 0,
                "changes_observed": 0,
                "safe_code": None,
            },
            "status",
        ),
    ],
)
def test_sync_run_result_rejects_impure_public_values(
    values: dict[str, object],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        SyncRunResult(**values)  # type: ignore[arg-type]


def test_sync_advisory_lock_keys_match_the_frozen_vector() -> None:
    assert sync_advisory_lock_keys(8, "INBOX") == (258_951_024, -2_028_611_493)
    assert sync_advisory_lock_keys(8, "INBOX") == sync_advisory_lock_keys(
        8,
        "INBOX",
    )


@pytest.mark.parametrize(
    ("account_id", "folder"),
    [
        (True, "INBOX"),
        (0, "INBOX"),
        (8, " inbox "),
        (8, "inbox"),
        (8, "SentItems"),
        (8, ""),
        (8, "bad\x00folder"),
    ],
)
def test_sync_lock_identity_rejects_invalid_or_noncanonical_values(
    account_id: object,
    folder: object,
) -> None:
    with pytest.raises(ValueError):
        sync_advisory_lock_keys(account_id, folder)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("account_id", "folder"),
    [
        (_HostileInt(8), "INBOX"),
        (8, _HostileStr("INBOX")),
        (8, "x" * 513),
        (8, "bad\ud800folder"),
        (8, "bad\x85folder"),
    ],
    ids=[
        "hostile-account",
        "hostile-folder",
        "folder-too-long",
        "surrogate",
        "c1-control",
    ],
)
def test_sync_lock_identity_rejects_hostile_and_unicode_boundary_inputs(
    account_id: object,
    folder: object,
) -> None:
    with pytest.raises(ValueError):
        sync_advisory_lock_keys(account_id, folder)  # type: ignore[arg-type]


def test_sync_lock_identity_accepts_512_and_preserves_unicode_codepoints() -> None:
    composed = sync_advisory_lock_keys(8, "Caf\u00e9")
    decomposed = sync_advisory_lock_keys(8, "Cafe\u0301")

    assert sync_advisory_lock_keys(8, "x" * 512)
    assert composed != decomposed


def test_transient_backoff_is_deterministic_bounded_and_domain_separated() -> None:
    first = _deterministic_retry_delay(
        account_id=8,
        canonical_folder="INBOX",
        expected_version=7,
        failure_count=1,
        retry_after_seconds=None,
    )
    assert first == _deterministic_retry_delay(
        account_id=8,
        canonical_folder="INBOX",
        expected_version=7,
        failure_count=1,
        retry_after_seconds=None,
    )
    assert 1 <= first <= 1

    capped = _deterministic_retry_delay(
        account_id=8,
        canonical_folder="INBOX",
        expected_version=7,
        failure_count=10_000,
        retry_after_seconds=None,
    )
    assert 1 <= capped <= 300
    assert (
        _deterministic_retry_delay(
            account_id=8,
            canonical_folder="INBOX",
            expected_version=7,
            failure_count=10_000,
            retry_after_seconds=900,
        )
        == 900
    )
    assert (
        _deterministic_retry_delay(
            account_id=8,
            canonical_folder="INBOX",
            expected_version=7,
            failure_count=10_000,
            retry_after_seconds=3600,
        )
        == 3600
    )


@pytest.mark.parametrize("retry_after_seconds", [0, 3600])
def test_exact_transient_error_accepts_retry_hint_boundaries(
    retry_after_seconds: int,
) -> None:
    error = SyncTransientError(retry_after_seconds=retry_after_seconds)

    assert _trusted_retry_hint(error) == retry_after_seconds


def test_transient_backoff_has_a_frozen_domain_separated_vector() -> None:
    vectors = {
        (8, "INBOX", 7, 8): 110,
        (9, "INBOX", 7, 8): 53,
        (8, "SENT", 7, 8): 92,
        (8, "INBOX", 8, 8): 23,
        (8, "INBOX", 7, 9): 171,
    }

    assert {
        identity: _deterministic_retry_delay(
            account_id=identity[0],
            canonical_folder=identity[1],
            expected_version=identity[2],
            failure_count=identity[3],
            retry_after_seconds=None,
        )
        for identity in vectors
    } == vectors


def test_backoff_freezes_the_256_to_300_ceiling_and_remote_lower_bound() -> None:
    assert (
        _deterministic_retry_delay(
            account_id=8,
            canonical_folder="INBOX",
            expected_version=7,
            failure_count=9,
            retry_after_seconds=None,
        )
        == 171
    )
    assert (
        _deterministic_retry_delay(
            account_id=8,
            canonical_folder="INBOX",
            expected_version=7,
            failure_count=10,
            retry_after_seconds=None,
        )
        == 31
    )
    assert (
        _deterministic_retry_delay(
            account_id=8,
            canonical_folder="INBOX",
            expected_version=7,
            failure_count=8,
            retry_after_seconds=10,
        )
        == 110
    )


@pytest.mark.parametrize(
    ("account_id", "folder", "expected_version", "failure_count"),
    [
        (_HostileInt(8), "INBOX", 0, 1),
        (8, _HostileStr("INBOX"), 0, 1),
        (8, "INBOX", _HostileInt(0), 1),
        (8, "INBOX", 0, _HostileInt(1)),
    ],
    ids=[
        "hostile-account",
        "hostile-folder",
        "hostile-version",
        "hostile-failure-count",
    ],
)
def test_backoff_rejects_hostile_values_before_subclass_behavior(
    account_id: object,
    folder: object,
    expected_version: object,
    failure_count: object,
) -> None:
    with pytest.raises(ValueError):
        _deterministic_retry_delay(
            account_id=account_id,  # type: ignore[arg-type]
            canonical_folder=folder,  # type: ignore[arg-type]
            expected_version=expected_version,  # type: ignore[arg-type]
            failure_count=failure_count,  # type: ignore[arg-type]
            retry_after_seconds=None,
        )


@pytest.mark.parametrize(
    ("expected_version", "failure_count", "retry_after_seconds"),
    [
        (True, 1, None),
        (-1, 1, None),
        (0, True, None),
        (0, 0, None),
        (0, 1, True),
        (0, 1, -1),
        (0, 1, 3601),
    ],
)
def test_transient_backoff_rejects_invalid_inputs(
    expected_version: object,
    failure_count: object,
    retry_after_seconds: object,
) -> None:
    with pytest.raises(ValueError):
        _deterministic_retry_delay(
            account_id=8,
            canonical_folder="INBOX",
            expected_version=expected_version,  # type: ignore[arg-type]
            failure_count=failure_count,  # type: ignore[arg-type]
            retry_after_seconds=retry_after_seconds,  # type: ignore[arg-type]
        )


class _Cursor:
    def __init__(
        self,
        row: object,
        on_fetchone: Callable[[], None] | None = None,
    ) -> None:
        self._row = row
        self._on_fetchone = on_fetchone

    async def fetchone(self) -> object:
        if self._on_fetchone is not None:
            self._on_fetchone()
        return self._row


class _LockState:
    def __init__(self) -> None:
        self.owned = False


class _Connection:
    def __init__(
        self,
        events: list[str],
        state: _LockState,
        *,
        acquire_result: object = True,
        acquire_error: BaseException | None = None,
        unlock_result: object = True,
        unlock_error: BaseException | None = None,
        unlock_waits: bool = False,
        closed: object = False,
        transaction_status: object = TransactionStatus.IDLE,
        acquire_status_after_fetch: object | None = None,
        unlock_status_after_fetch: object | None = None,
    ) -> None:
        self.events = events
        self.state = state
        self.acquire_result = acquire_result
        self.acquire_error = acquire_error
        self.unlock_result = unlock_result
        self.unlock_error = unlock_error
        self.unlock_waits = unlock_waits
        self.closed = closed
        self.autocommit: object = True
        self.acquire_status_after_fetch = acquire_status_after_fetch
        self.unlock_status_after_fetch = unlock_status_after_fetch
        self.info = type(
            "_ConnectionInfo",
            (),
            {"transaction_status": transaction_status},
        )()

    async def execute(self, statement: str, _params: object = None) -> _Cursor:
        if "pg_try_advisory_lock" in statement:
            self.events.append("db.try_lock")
            if self.acquire_result is True or self.acquire_error is not None:
                self.state.owned = True
            if self.acquire_error is not None:
                raise self.acquire_error
            return _Cursor(
                (self.acquire_result,),
                lambda: self._set_transaction_status(self.acquire_status_after_fetch),
            )
        if "pg_advisory_unlock" in statement:
            self.events.append("db.unlock")
            if self.unlock_waits:
                await asyncio.Event().wait()
            if self.unlock_result is True or self.unlock_error is not None:
                self.state.owned = False
            if self.unlock_error is not None:
                raise self.unlock_error
            return _Cursor(
                (self.unlock_result,),
                lambda: self._set_transaction_status(self.unlock_status_after_fetch),
            )
        raise AssertionError(f"unexpected statement: {statement}")

    def _set_transaction_status(self, status: object | None) -> None:
        if status is not None:
            self.info.transaction_status = status

    async def close(self) -> None:
        self.events.append("connection.close")
        self.closed = True
        self.state.owned = False


class _Pool:
    def __init__(
        self,
        connection: _Connection,
        events: list[str],
        *,
        autocommit: object = True,
        close_returns: object = False,
        put_error: BaseException | None = None,
    ) -> None:
        self.connection = connection
        self.events = events
        self.kwargs = {"autocommit": autocommit}
        self.close_returns = close_returns
        self.put_error = put_error
        self.returned: list[_Connection] = []

    async def getconn(self) -> _Connection:
        self.events.append("pool.getconn")
        return self.connection

    async def putconn(self, connection: _Connection) -> None:
        self.events.append("pool.putconn")
        self.returned.append(connection)
        if self.put_error is not None:
            raise self.put_error


class _Permit:
    def __init__(self, events: list[str], *, acquired: object = True) -> None:
        self.events = events
        self.acquired = acquired
        self.release_calls = 0
        self.lease = FolderPermitLease(self._release)

    def _release(self) -> None:
        self.events.append("permit.release")
        self.release_calls += 1

    async def try_acquire(self, account_id: int, folder: str) -> object:
        assert (account_id, folder) == (8, "INBOX")
        self.events.append("permit.acquire")
        if self.acquired is True:
            return self.lease
        if self.acquired is False:
            return None
        return self.acquired


def _runner(
    *,
    acquire_result: object = True,
    acquire_error: BaseException | None = None,
    unlock_result: object = True,
    unlock_error: BaseException | None = None,
    unlock_waits: bool = False,
    permit_result: object = True,
    autocommit: object = True,
    close_returns: object = False,
    put_error: BaseException | None = None,
    cleanup_timeout: float = 0.05,
    closed: object = False,
    transaction_status: object = TransactionStatus.IDLE,
    acquire_status_after_fetch: object | None = None,
    unlock_status_after_fetch: object | None = None,
) -> tuple[_SyncSessionRunner, _Connection, _Pool, _Permit, _LockState, list[str]]:
    events: list[str] = []
    state = _LockState()
    connection = _Connection(
        events,
        state,
        acquire_result=acquire_result,
        acquire_error=acquire_error,
        unlock_result=unlock_result,
        unlock_error=unlock_error,
        unlock_waits=unlock_waits,
        closed=closed,
        transaction_status=transaction_status,
        acquire_status_after_fetch=acquire_status_after_fetch,
        unlock_status_after_fetch=unlock_status_after_fetch,
    )
    connection.autocommit = autocommit
    pool = _Pool(
        connection,
        events,
        autocommit=autocommit,
        close_returns=close_returns,
        put_error=put_error,
    )
    permit = _Permit(events, acquired=permit_result)
    return (
        _SyncSessionRunner(
            pool=pool,
            permit=permit,
            cleanup_timeout=cleanup_timeout,
        ),
        connection,
        pool,
        permit,
        state,
        events,
    )


@pytest.mark.asyncio
async def test_session_runner_owns_the_frozen_resource_order() -> None:
    runner, connection, pool, permit, state, events = _runner()

    async def operation(supplied: object) -> str:
        assert supplied.connection is connection  # type: ignore[attr-defined]
        events.append("body")
        return "done"

    outcome = await runner.run(8, "INBOX", operation)

    assert outcome.acquired is True
    assert outcome.value == "done"
    assert state.owned is False
    assert connection.closed is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events == [
        "permit.acquire",
        "pool.getconn",
        "db.try_lock",
        "body",
        "db.unlock",
        "pool.putconn",
        "permit.release",
    ]


@pytest.mark.asyncio
async def test_session_runner_busy_skip_never_calls_the_body_or_unlock() -> None:
    runner, connection, pool, permit, state, events = _runner(
        acquire_result=False,
    )

    async def forbidden(_connection: object) -> None:
        raise AssertionError("busy session must not run the operation")

    outcome = await runner.run(8, "INBOX", forbidden)

    assert outcome.acquired is False
    assert outcome.value is None
    assert state.owned is False
    assert connection.closed is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events == [
        "permit.acquire",
        "pool.getconn",
        "db.try_lock",
        "pool.putconn",
        "permit.release",
    ]


@pytest.mark.asyncio
async def test_busy_lock_result_with_nonidle_backend_is_evicted_not_returned() -> None:
    runner, connection, pool, permit, state, events = _runner(
        acquire_result=False,
        acquire_status_after_fetch=TransactionStatus.INTRANS,
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("busy lock must not run the operation")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value.operation == "sync_session_cleanup"
    assert "db.unlock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert pool.returned[0].closed is True
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_acquired_lock_with_nonidle_backend_is_evicted_before_body() -> None:
    runner, connection, pool, permit, state, events = _runner(
        acquire_result=True,
        acquire_status_after_fetch=TransactionStatus.INTRANS,
    )

    async def forbidden(_session: object) -> None:
        events.append("BODY_UNDER_NONIDLE")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value.operation == "sync_session_cleanup"
    assert "BODY_UNDER_NONIDLE" not in events
    assert "db.unlock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert pool.returned[0].closed is True
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_session_runner_permit_skip_never_checks_out_a_connection() -> None:
    runner, connection, pool, permit, state, events = _runner(
        permit_result=False,
    )

    async def forbidden(_connection: object) -> None:
        raise AssertionError("permit skip must not run the operation")

    outcome = await runner.run(8, "INBOX", forbidden)

    assert outcome.acquired is False
    assert outcome.value is None
    assert state.owned is False
    assert connection.closed is False
    assert pool.returned == []
    assert permit.release_calls == 0
    assert events == ["permit.acquire"]


@pytest.mark.asyncio
async def test_acquire_ack_loss_closes_tainted_backend_and_reraises_cancellation() -> (
    None
):
    runner, connection, pool, permit, state, events = _runner(
        acquire_error=asyncio.CancelledError(),
    )

    async def forbidden(_connection: object) -> None:
        raise AssertionError("unknown lock ownership must not run the operation")

    with pytest.raises(asyncio.CancelledError):
        await runner.run(8, "INBOX", forbidden)

    assert state.owned is False
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events == [
        "permit.acquire",
        "pool.getconn",
        "db.try_lock",
        "connection.close",
        "pool.putconn",
        "permit.release",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_result", [1, None, "true"])
async def test_acquire_requires_an_exact_builtin_boolean(
    invalid_result: object,
) -> None:
    runner, connection, pool, permit, state, events = _runner(
        acquire_result=invalid_result,
    )

    async def forbidden(_connection: object) -> None:
        raise AssertionError("invalid lock result must not run the operation")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value.operation == "sync_session_acquire"
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("autocommit", "close_returns"),
    [(False, False), (1, False), (True, True), (True, 0)],
)
async def test_pool_contract_is_validated_before_the_session_lock(
    autocommit: object,
    close_returns: object,
) -> None:
    runner, connection, pool, permit, state, events = _runner(
        autocommit=autocommit,
        close_returns=close_returns,
    )

    async def forbidden(_connection: object) -> None:
        raise AssertionError("invalid pool must not run the operation")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value.operation == "sync_pool_contract"
    assert "db.try_lock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("closed", "transaction_status"),
    [
        (True, TransactionStatus.IDLE),
        (0, TransactionStatus.IDLE),
        (False, TransactionStatus.INTRANS),
        (False, TransactionStatus.INERROR),
        (False, int(TransactionStatus.IDLE)),
    ],
    ids=[
        "already-closed",
        "nonexact-open-flag",
        "open-transaction",
        "failed-transaction",
        "nonexact-idle-status",
    ],
)
async def test_checkout_requires_exact_open_idle_connection_before_lock(
    closed: object,
    transaction_status: object,
) -> None:
    runner, connection, pool, permit, state, events = _runner(
        closed=closed,
        transaction_status=transaction_status,
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("an unhealthy checkout must not acquire the session lock")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value.operation == "sync_pool_contract"
    assert "db.try_lock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_operation_return_with_open_transaction_taints_and_evicts_backend() -> (
    None
):
    runner, connection, pool, permit, state, events = _runner()

    async def operation(_session: object) -> str:
        events.append("body.open-xid")
        connection.info.transaction_status = TransactionStatus.INTRANS
        return "unsafe"

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_tainted"
    assert "db.unlock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_operation_error_with_failed_transaction_taints_and_preserves_primary() -> (
    None
):
    runner, connection, pool, permit, state, events = _runner()

    async def operation(_session: object) -> None:
        events.append("body.failed-xid")
        connection.info.transaction_status = TransactionStatus.INERROR
        raise RuntimeError("operation failed with an open transaction")

    with pytest.raises(RuntimeError, match="open transaction"):
        await runner.run(8, "INBOX", operation)

    assert "db.unlock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


class _RollbackProcessControlTransaction:
    def __init__(
        self,
        connection: _Connection,
        process_error: BaseException,
    ) -> None:
        self._connection = connection
        self._process_error = process_error

    async def __aenter__(self) -> None:
        self._connection.info.transaction_status = TransactionStatus.INTRANS
        return None

    async def __aexit__(
        self,
        _error_type: object,
        _error: object,
        _traceback: object,
    ) -> None:
        raise self._process_error


class _RollbackProcessControlConnection(_Connection):
    def __init__(
        self,
        events: list[str],
        state: _LockState,
        process_error: BaseException,
    ) -> None:
        super().__init__(events, state)
        self._process_error = process_error

    def transaction(self) -> _RollbackProcessControlTransaction:
        return _RollbackProcessControlTransaction(self, self._process_error)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "process_error",
    [asyncio.CancelledError(), SystemExit(23), KeyboardInterrupt()],
)
async def test_rollback_process_control_wins_over_ordinary_body_error(
    process_error: BaseException,
) -> None:
    events: list[str] = []
    state = _LockState()
    connection = _RollbackProcessControlConnection(events, state, process_error)
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(pool=pool, permit=permit, cleanup_timeout=0.01)

    async def operation(session: object) -> None:
        async def failed_body(_connection: object) -> None:
            raise RuntimeError("ordinary transaction body failure")

        await _caller_owned_transaction(session, failed_body)  # type: ignore[arg-type]

    with pytest.raises(type(process_error)):
        await runner.run(8, "INBOX", operation)

    assert "db.unlock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


class _ExitStateTransaction:
    def __init__(
        self,
        connection: _Connection,
        *,
        exit_status: TransactionStatus,
        close_on_exit: bool,
    ) -> None:
        self._connection = connection
        self._exit_status = exit_status
        self._close_on_exit = close_on_exit

    async def __aenter__(self) -> None:
        self._connection.info.transaction_status = TransactionStatus.INTRANS
        return None

    async def __aexit__(
        self,
        _error_type: object,
        _error: object,
        _traceback: object,
    ) -> None:
        self._connection.info.transaction_status = self._exit_status
        if self._close_on_exit:
            self._connection.closed = True


class _ExitStateConnection(_Connection):
    def __init__(
        self,
        events: list[str],
        state: _LockState,
        *,
        exit_status: TransactionStatus,
        close_on_exit: bool = False,
    ) -> None:
        super().__init__(events, state)
        self._exit_status = exit_status
        self._close_on_exit = close_on_exit

    def transaction(self) -> _ExitStateTransaction:
        return _ExitStateTransaction(
            self,
            exit_status=self._exit_status,
            close_on_exit=self._close_on_exit,
        )


class _EnterStateTransaction:
    def __init__(
        self,
        connection: _Connection,
        *,
        enter_status: TransactionStatus,
        close_on_enter: bool,
    ) -> None:
        self._connection = connection
        self._enter_status = enter_status
        self._close_on_enter = close_on_enter

    async def __aenter__(self) -> None:
        self._connection.info.transaction_status = self._enter_status
        if self._close_on_enter:
            self._connection.closed = True

    async def __aexit__(
        self,
        _error_type: object,
        _error: object,
        _traceback: object,
    ) -> None:
        self._connection.info.transaction_status = TransactionStatus.IDLE


class _EnterStateConnection(_Connection):
    def __init__(
        self,
        events: list[str],
        state: _LockState,
        *,
        enter_status: TransactionStatus,
        close_on_enter: bool = False,
    ) -> None:
        super().__init__(events, state)
        self._enter_status = enter_status
        self._close_on_enter = close_on_enter

    def transaction(self) -> _EnterStateTransaction:
        return _EnterStateTransaction(
            self,
            enter_status=self._enter_status,
            close_on_enter=self._close_on_enter,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("enter_status", "close_on_enter"),
    [
        (TransactionStatus.IDLE, False),
        (TransactionStatus.INERROR, False),
        (TransactionStatus.ACTIVE, False),
        (TransactionStatus.INTRANS, True),
    ],
    ids=["idle", "inerror", "active", "closed"],
)
async def test_transaction_enter_requires_exact_open_intrans_before_body(
    enter_status: TransactionStatus,
    close_on_enter: bool,
) -> None:
    events: list[str] = []
    state = _LockState()
    connection = _EnterStateConnection(
        events,
        state,
        enter_status=enter_status,
        close_on_enter=close_on_enter,
    )
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(pool=pool, permit=permit, cleanup_timeout=0.01)

    async def operation(session: object) -> None:
        async def forbidden_body(_connection: object) -> None:
            events.append("xid.body.forbidden")

        await _caller_owned_transaction(  # type: ignore[arg-type]
            session,
            forbidden_body,
        )
        events.append("http.forbidden")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_tainted"
    assert "xid.body.forbidden" not in events
    assert "http.forbidden" not in events
    assert "db.unlock" not in events
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_transaction_commit_must_be_open_idle_before_next_http_boundary() -> None:
    events: list[str] = []
    state = _LockState()
    connection = _ExitStateConnection(
        events,
        state,
        exit_status=TransactionStatus.INTRANS,
    )
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(pool=pool, permit=permit, cleanup_timeout=0.01)

    async def operation(session: object) -> None:
        async def transaction_body(_connection: object) -> None:
            return None

        await _caller_owned_transaction(  # type: ignore[arg-type]
            session,
            transaction_body,
        )
        events.append("http.forbidden")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_tainted"
    assert "http.forbidden" not in events
    assert "db.unlock" not in events
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exit_status", "close_on_exit"),
    [
        (TransactionStatus.INERROR, False),
        (TransactionStatus.IDLE, True),
    ],
    ids=["rollback-inerror", "rollback-closed"],
)
async def test_transaction_rollback_taints_immediately_before_error_can_be_caught(
    exit_status: TransactionStatus,
    close_on_exit: bool,
) -> None:
    events: list[str] = []
    state = _LockState()
    connection = _ExitStateConnection(
        events,
        state,
        exit_status=exit_status,
        close_on_exit=close_on_exit,
    )
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(pool=pool, permit=permit, cleanup_timeout=0.01)

    async def operation(session: object) -> None:
        async def failed_body(_connection: object) -> None:
            raise RuntimeError("transaction body failed")

        try:
            await _caller_owned_transaction(  # type: ignore[arg-type]
                session,
                failed_body,
            )
        except RuntimeError:
            if not session.tainted:  # type: ignore[attr-defined]
                events.append("http.forbidden")
            raise

    with pytest.raises(RuntimeError, match="transaction body failed"):
        await runner.run(8, "INBOX", operation)

    assert "http.forbidden" not in events
    assert "db.unlock" not in events
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("unlock_result", "unlock_error", "unlock_waits"),
    [
        (False, None, False),
        (True, RuntimeError("unlock ACK lost"), False),
        (True, None, True),
    ],
)
async def test_unlock_failure_or_second_cancellation_evicts_the_backend(
    unlock_result: object,
    unlock_error: BaseException | None,
    unlock_waits: bool,
) -> None:
    runner, connection, pool, permit, state, events = _runner(
        unlock_result=unlock_result,
        unlock_error=unlock_error,
        unlock_waits=unlock_waits,
        cleanup_timeout=0.01,
    )

    async def operation(_connection: object) -> str:
        events.append("body")
        return "committed"

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_cleanup"
    assert state.owned is False
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_successful_unlock_with_nonidle_backend_is_evicted_not_reused() -> None:
    runner, connection, pool, permit, state, events = _runner(
        unlock_result=True,
        unlock_status_after_fetch=TransactionStatus.INERROR,
    )

    async def operation(_session: object) -> str:
        return "committed"

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_cleanup"
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert pool.returned[0].closed is True
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_original_process_control_exception_wins_over_cleanup_failure() -> None:
    runner, connection, pool, permit, state, events = _runner(
        unlock_result=False,
    )

    async def cancelled(_connection: object) -> Any:
        events.append("body")
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await runner.run(8, "INBOX", cancelled)

    assert state.owned is False
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_putconn_failure_still_releases_the_permit_last() -> None:
    runner, connection, pool, permit, state, events = _runner(
        put_error=RuntimeError("pool accounting failed"),
    )

    async def operation(_connection: object) -> str:
        events.append("body")
        return "committed"

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_cleanup"
    assert state.owned is False
    assert connection.closed is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_timed_out_cleanup_reaps_its_child_task_in_asyncio_debug_mode() -> None:
    loop = asyncio.get_running_loop()
    previous_debug = loop.get_debug()
    loop.set_debug(True)
    try:
        runner, connection, pool, permit, state, events = _runner(
            unlock_waits=True,
            cleanup_timeout=0.01,
        )

        async def operation(_connection: object) -> str:
            return "committed"

        with pytest.raises(DatabaseOperationError):
            await runner.run(8, "INBOX", operation)
        await asyncio.sleep(0)
        current = asyncio.current_task()
        assert [task for task in asyncio.all_tasks() if task is not current] == []
        assert connection.closed is True
        assert pool.returned == [connection]
        assert permit.release_calls == 1
        assert state.owned is False
        assert events[-1] == "permit.release"
    finally:
        loop.set_debug(previous_debug)


class _UnknownCommitOutcome(RuntimeError):
    pass


@pytest.mark.asyncio
async def test_operation_taint_with_primary_skips_unlock_and_preserves_primary() -> (
    None
):
    runner, connection, pool, permit, state, events = _runner()

    async def operation(session: object) -> None:
        assert session.connection is connection  # type: ignore[attr-defined]
        session.taint()  # type: ignore[attr-defined]
        session.taint()  # one-way and idempotent
        events.append("body.tainted")
        raise _UnknownCommitOutcome("commit acknowledgement lost")

    with pytest.raises(_UnknownCommitOutcome, match="acknowledgement"):
        await runner.run(8, "INBOX", operation)

    assert "db.unlock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert pool.returned[0].closed is True
    assert permit.release_calls == 1
    assert events == [
        "permit.acquire",
        "pool.getconn",
        "db.try_lock",
        "body.tainted",
        "connection.close",
        "pool.putconn",
        "permit.release",
    ]


@pytest.mark.asyncio
async def test_operation_taint_without_primary_is_a_fixed_cleanup_invariant() -> None:
    runner, connection, pool, permit, state, events = _runner()

    async def operation(session: object) -> str:
        assert session.connection is connection  # type: ignore[attr-defined]
        session.taint()  # type: ignore[attr-defined]
        events.append("body.tainted")
        return "unknown"

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_tainted"
    assert "db.unlock" not in events
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert pool.returned[0].closed is True
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


def test_folder_permit_lease_is_concrete_synchronous_and_one_shot() -> None:
    calls: list[str] = []
    lease = FolderPermitLease(lambda: calls.append("released"))

    assert lease.release() is None

    assert calls == ["released"]
    with pytest.raises(RuntimeError, match="already released"):
        lease.release()


def test_folder_permit_lease_rejects_async_release_callback() -> None:
    async def async_release() -> None:
        return None

    with pytest.raises(ValueError, match="synchronous"):
        FolderPermitLease(async_release)


def test_folder_permit_lease_rejects_async_callable_object() -> None:
    class AsyncCallable:
        async def __call__(self) -> None:
            return None

    with pytest.raises(ValueError, match="synchronous"):
        FolderPermitLease(AsyncCallable())


def test_folder_permit_lease_closes_coroutine_return_without_warning(
    recwarn: pytest.WarningsRecorder,
) -> None:
    async def returned_coroutine() -> None:
        return None

    lease = FolderPermitLease(lambda: returned_coroutine())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="awaitable"):
        lease.release()

    assert [
        warning for warning in recwarn if "never awaited" in str(warning.message)
    ] == []


def test_folder_permit_lease_rejects_arbitrary_awaitable_result() -> None:
    class AwaitableResult:
        def __await__(self):
            if False:
                yield None
            return None

    lease = FolderPermitLease(lambda: AwaitableResult())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="awaitable"):
        lease.release()


@pytest.mark.asyncio
async def test_runner_rejects_structural_or_async_lease_before_pool_checkout() -> None:
    class StructuralAsyncLease:
        async def release(self) -> None:
            return None

    runner, connection, pool, permit, state, events = _runner(
        permit_result=StructuralAsyncLease(),
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("invalid lease must not run")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value.operation == "sync_permit_contract"
    assert pool.returned == []
    assert connection.closed is False
    assert state.owned is False
    assert permit.release_calls == 0
    assert events == ["permit.acquire"]


@pytest.mark.asyncio
async def test_cleanup_cancelled_during_unlock_is_reraised_after_safe_cleanup() -> None:
    runner, connection, pool, permit, state, events = _runner(
        unlock_error=asyncio.CancelledError(),
    )

    async def operation(_session: object) -> str:
        return "committed"

    with pytest.raises(asyncio.CancelledError):
        await runner.run(8, "INBOX", operation)

    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
@pytest.mark.parametrize("process_error", [SystemExit(17), KeyboardInterrupt()])
async def test_child_process_control_is_captured_until_unlock_cleanup_finishes(
    process_error: BaseException,
) -> None:
    runner, connection, pool, permit, state, events = _runner(
        unlock_error=process_error,
    )

    async def operation(_session: object) -> str:
        return "committed"

    with pytest.raises(type(process_error)):
        await runner.run(8, "INBOX", operation)

    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


class _ProcessControlCloseConnection(_Connection):
    def __init__(
        self,
        events: list[str],
        state: _LockState,
        process_error: BaseException,
    ) -> None:
        super().__init__(events, state, unlock_result=False)
        self._process_error = process_error

    async def close(self) -> None:
        self.events.append("connection.close")
        raise self._process_error


@pytest.mark.asyncio
@pytest.mark.parametrize("process_error", [SystemExit(19), KeyboardInterrupt()])
async def test_child_process_control_is_captured_until_close_cleanup_finishes(
    process_error: BaseException,
) -> None:
    events: list[str] = []
    state = _LockState()
    connection = _ProcessControlCloseConnection(events, state, process_error)
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.01,
    )

    async def operation(_session: object) -> str:
        return "committed"

    with pytest.raises(type(process_error)):
        await runner.run(8, "INBOX", operation)

    assert connection.closed is False
    assert state.owned is True
    assert pool.returned == []
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_confirmed_close_never_tests_process_control_truthiness() -> None:
    class _HostileKeyboardInterrupt(KeyboardInterrupt):
        def __init__(self) -> None:
            super().__init__()
            self.bool_calls = 0

        def __bool__(self) -> bool:
            self.bool_calls += 1
            raise AssertionError("process-control truthiness must not be inspected")

    events: list[str] = []
    state = _LockState()
    primary = _HostileKeyboardInterrupt()
    connection = _ProcessControlCloseConnection(events, state, primary)
    pool = _Pool(connection, events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=_Permit(events),
        cleanup_timeout=0.01,
    )

    confirmed, error = await runner._confirmed_close(connection)

    assert confirmed is False
    assert error is primary
    assert primary.bool_calls == 0


@pytest.mark.asyncio
async def test_cleanup_cancelled_during_busy_putconn_releases_permit_and_reraises() -> (
    None
):
    runner, connection, pool, permit, state, events = _runner(
        acquire_result=False,
        put_error=asyncio.CancelledError(),
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("busy lock must not run")

    with pytest.raises(asyncio.CancelledError):
        await runner.run(8, "INBOX", forbidden)

    assert connection.closed is False
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


class _StubbornReturnPool(_Pool):
    def __init__(
        self,
        connection: _Connection,
        events: list[str],
        *,
        run_seconds: float,
    ) -> None:
        super().__init__(connection, events)
        self.run_seconds = run_seconds
        self.attempts = 0
        self.cancellations = 0

    async def putconn(self, connection: _Connection) -> None:
        self.events.append("pool.putconn.start")
        self.attempts += 1
        deadline = asyncio.get_running_loop().time() + self.run_seconds
        while (remaining := deadline - asyncio.get_running_loop().time()) > 0:
            try:
                await asyncio.sleep(remaining)
            except asyncio.CancelledError:
                self.cancellations += 1
        self.returned.append(connection)
        self.events.append("pool.putconn.done")


@pytest.mark.asyncio
async def test_busy_return_deadline_quarantines_inflight_child_without_close() -> None:
    events: list[str] = []
    state = _LockState()
    connection = _Connection(events, state, acquire_result=False)
    pool = _StubbornReturnPool(connection, events, run_seconds=0.06)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.01,
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("busy lock must not run")

    started = asyncio.get_running_loop().time()
    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", forbidden)
    elapsed = asyncio.get_running_loop().time() - started

    assert caught.value.operation == "sync_session_cleanup"
    assert elapsed < 0.05
    assert connection.closed is False
    assert pool.attempts == 1
    assert pool.returned == []
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"

    await asyncio.sleep(0.07)
    assert pool.cancellations == 0
    assert pool.attempts == 1
    assert pool.returned == [connection]
    assert connection.closed is False
    assert "connection.close" not in events


class _ControlledReturnPool(_Pool):
    def __init__(self, connection: _Connection, events: list[str]) -> None:
        super().__init__(connection, events)
        self.entered = asyncio.Event()
        self.allow_return = asyncio.Event()
        self.cancellations: list[asyncio.CancelledError] = []
        self.attempts = 0

    async def putconn(self, connection: _Connection) -> None:
        self.events.append("pool.putconn.start")
        self.attempts += 1
        self.entered.set()
        try:
            await self.allow_return.wait()
        except asyncio.CancelledError as error:
            self.cancellations.append(error)
            raise
        self.returned.append(connection)
        self.events.append("pool.putconn.done")


@pytest.mark.asyncio
async def test_busy_return_second_cancel_preserves_first_after_safe_handoff() -> None:
    events: list[str] = []
    state = _LockState()
    connection = _Connection(events, state, acquire_result=False)
    pool = _ControlledReturnPool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.1,
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("busy lock must not run")

    task = asyncio.create_task(runner.run(8, "INBOX", forbidden))
    await asyncio.wait_for(pool.entered.wait(), timeout=1.0)

    assert task.cancel("first return cancellation") is True
    await asyncio.sleep(0)
    assert task.done() is False
    assert task.cancel("second return cancellation") is True
    await asyncio.sleep(0)
    assert task.done() is False
    pool.allow_return.set()

    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert caught.value.args == ("first return cancellation",)
    assert pool.cancellations == []
    assert pool.attempts == 1
    assert pool.returned == [connection]
    assert connection.closed is False
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


class _SynchronousReturnPool(_Pool):
    def __init__(
        self,
        connection: _Connection,
        events: list[str],
        outcome: object,
    ) -> None:
        super().__init__(connection, events)
        self.outcome = outcome
        self.attempts = 0

    def putconn(self, connection: _Connection) -> object:  # type: ignore[override]
        self.events.append("pool.putconn")
        self.attempts += 1
        self.returned.append(connection)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "return_outcome",
    [RuntimeError("sync return failed"), KeyboardInterrupt(), None],
)
async def test_busy_synchronous_return_boundary_is_typed_and_releases_permit_last(
    return_outcome: object,
) -> None:
    events: list[str] = []
    state = _LockState()
    connection = _Connection(events, state, acquire_result=False)
    pool = _SynchronousReturnPool(connection, events, return_outcome)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.05,
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("busy lock must not run")

    expected = (
        type(return_outcome)
        if isinstance(return_outcome, BaseException)
        and not isinstance(return_outcome, Exception)
        else DatabaseOperationError
    )
    with pytest.raises(expected) as caught:
        await runner.run(8, "INBOX", forbidden)

    if expected is KeyboardInterrupt:
        assert caught.value is return_outcome
    else:
        assert caught.value.operation == "sync_session_cleanup"
    assert connection.closed is False
    assert pool.attempts == 1
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_ordinary_operation_error_does_not_mask_cleanup_failure() -> None:
    runner, connection, pool, permit, state, events = _runner(
        unlock_result=False,
    )

    async def failed(_session: object) -> None:
        raise RuntimeError("ordinary operation failed")

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", failed)

    assert caught.value.operation == "sync_session_cleanup"
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


class _StubbornUnlockConnection(_Connection):
    async def execute(self, statement: str, params: object = None) -> _Cursor:
        if "pg_advisory_unlock" not in statement:
            return await super().execute(statement, params)
        self.events.append("db.unlock")
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            await asyncio.sleep(0.08)
        self.state.owned = False
        return _Cursor((True,))


@pytest.mark.asyncio
async def test_cancel_ignoring_child_cannot_extend_second_reap_deadline() -> None:
    events: list[str] = []
    state = _LockState()
    connection = _StubbornUnlockConnection(events, state)
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.01,
    )

    async def operation(_session: object) -> str:
        return "committed"

    started = asyncio.get_running_loop().time()
    with pytest.raises(DatabaseOperationError):
        await runner.run(8, "INBOX", operation)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.06
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"
    await asyncio.sleep(0.1)


class _SecondCancelUnlockConnection(_Connection):
    def __init__(self, events: list[str], state: _LockState) -> None:
        super().__init__(events, state)
        self.reap_started = asyncio.Event()

    async def execute(self, statement: str, params: object = None) -> _Cursor:
        if "pg_advisory_unlock" not in statement:
            return await super().execute(statement, params)
        self.events.append("db.unlock")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.reap_started.set()
            await asyncio.sleep(0.08)
        self.state.owned = False
        return _Cursor((True,))


@pytest.mark.asyncio
async def test_second_cancellation_during_hard_reap_is_not_lost() -> None:
    events: list[str] = []
    state = _LockState()
    connection = _SecondCancelUnlockConnection(events, state)
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.01,
    )

    async def operation(_session: object) -> str:
        return "committed"

    run_task = asyncio.create_task(runner.run(8, "INBOX", operation))
    await asyncio.wait_for(connection.reap_started.wait(), timeout=0.1)
    run_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"
    await asyncio.sleep(0.1)


class _FailingPgConn:
    def finish(self) -> None:
        raise RuntimeError("low-level finish failed")


class _UncloseableConnection(_Connection):
    def __init__(self, events: list[str], state: _LockState) -> None:
        super().__init__(events, state, unlock_result=False)
        self.pgconn = _FailingPgConn()

    async def close(self) -> None:
        self.events.append("connection.close")
        raise RuntimeError("connection close failed")


@pytest.mark.asyncio
async def test_unconfirmed_physical_close_is_quarantined_outside_healthy_pool() -> None:
    events: list[str] = []
    state = _LockState()
    connection = _UncloseableConnection(events, state)
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.01,
    )

    async def operation(_session: object) -> str:
        return "committed"

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_cleanup"
    assert connection.closed is False
    assert state.owned is True
    assert pool.returned == []
    assert "pool.putconn" not in events
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_taint_primary_survives_unconfirmed_close_while_backend_is_quarantined() -> (
    None
):
    events: list[str] = []
    state = _LockState()
    connection = _UncloseableConnection(events, state)
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.01,
    )

    async def operation(session: object) -> None:
        session.taint()  # type: ignore[attr-defined]
        raise _UnknownCommitOutcome("commit acknowledgement lost")

    with pytest.raises(_UnknownCommitOutcome, match="acknowledgement"):
        await runner.run(8, "INBOX", operation)

    assert "db.unlock" not in events
    assert connection.closed is False
    assert state.owned is True
    assert pool.returned == []
    assert "pool.putconn" not in events
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


class _SlowFinishingPgConn:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    def finish(self) -> None:
        time.sleep(0.08)
        self._connection.closed = True
        self._connection.state.owned = False


class _SlowLowLevelCloseConnection(_Connection):
    def __init__(self, events: list[str], state: _LockState) -> None:
        super().__init__(events, state, unlock_result=False)
        self.pgconn = _SlowFinishingPgConn(self)

    async def close(self) -> None:
        self.events.append("connection.close")
        raise RuntimeError("connection close failed")


@pytest.mark.asyncio
async def test_blocking_low_level_finish_is_bounded_and_never_returned_early() -> None:
    events: list[str] = []
    state = _LockState()
    connection = _SlowLowLevelCloseConnection(events, state)
    pool = _Pool(connection, events)
    permit = _Permit(events)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=permit,
        cleanup_timeout=0.01,
    )

    async def operation(_session: object) -> str:
        return "committed"

    started = asyncio.get_running_loop().time()
    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)
    elapsed = asyncio.get_running_loop().time() - started

    assert caught.value.operation == "sync_session_cleanup"
    assert elapsed < 0.06
    assert pool.returned == []
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"
    await asyncio.sleep(0.1)
    assert connection.closed is True
    assert pool.returned == []


def _policy_matrix(
    create_policy: ProcessingPolicy = ProcessingPolicy.FULL,
) -> dict[tuple[IngressSource, str, ChangeKind], ProcessingPolicy]:
    return {
        (
            IngressSource.WEBHOOK,
            "NewMailEvent",
            ChangeKind.CREATE,
        ): create_policy,
        (
            IngressSource.WEBHOOK,
            "CreatedEvent",
            ChangeKind.CREATE,
        ): ProcessingPolicy.IGNORED,
        (
            IngressSource.WEBHOOK,
            "ModifiedEvent",
            ChangeKind.UPDATE,
        ): ProcessingPolicy.METADATA_ONLY,
        (
            IngressSource.WEBHOOK,
            "DeletedEvent",
            ChangeKind.DELETE,
        ): ProcessingPolicy.METADATA_ONLY,
        (IngressSource.SYNC, "create", ChangeKind.CREATE): create_policy,
        (
            IngressSource.SYNC,
            "update",
            ChangeKind.UPDATE,
        ): ProcessingPolicy.METADATA_ONLY,
        (
            IngressSource.SYNC,
            "delete",
            ChangeKind.DELETE,
        ): ProcessingPolicy.METADATA_ONLY,
    }


class _SharedHelperRegressionTransaction:
    def __init__(self, connection: _SharedHelperRegressionConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> None:
        self._connection.events.append("xid.enter")
        self._connection.info.transaction_status = TransactionStatus.INTRANS

    async def __aexit__(
        self,
        _exc_type: object,
        _exc: object,
        _traceback: object,
    ) -> None:
        self._connection.events.append("xid.exit")
        self._connection.info.transaction_status = TransactionStatus.IDLE


class _SharedHelperRegressionConnection:
    def __init__(self) -> None:
        self.closed = False
        self.info = type(
            "_SharedHelperRegressionInfo",
            (),
            {"transaction_status": TransactionStatus.IDLE},
        )()
        self.events: list[str] = []
        self.statements: list[tuple[str, object]] = []

    def transaction(self) -> _SharedHelperRegressionTransaction:
        return _SharedHelperRegressionTransaction(self)

    async def execute(self, statement: str, params: object = None) -> _Cursor:
        self.statements.append((statement, params))
        if statement.startswith("SET LOCAL TRANSACTION"):
            self.events.append("xid.read_committed")
            return _Cursor(None)
        if "set_config('lock_timeout'" in statement:
            self.events.append("xid.timeouts")
            return _Cursor(None)
        if "pg_advisory_xact_lock_shared" in statement:
            self.events.append("ownership.shared_lock")
            assert params is not None
            return _Cursor(None)
        if "FROM public.pipeline_ownership" in statement:
            self.events.append("ownership.current_ingress")
            return _Cursor(
                {
                    "pipeline_name": "pipeline-v2",
                    "generation": 3,
                    "fencing_token": 9,
                }
            )
        if "FROM public.sync_cursors" in statement and "FOR UPDATE" in statement:
            self.events.append("cursor.for_update")
            return _Cursor(
                {
                    "cursor": None,
                    "status": "cold_start_pending",
                    "version": 0,
                    "transient_failures": 0,
                    "retry_deferred": False,
                }
            )
        raise AssertionError(f"unexpected ordinary Sync SQL: {statement}")


@pytest.mark.asyncio
async def test_ordinary_preflight_freezes_shared_xid_and_ownership_sql_order() -> None:
    connection = _SharedHelperRegressionConnection()
    coordinator = object.__new__(SyncCoordinator)

    result = await coordinator._preflight(  # type: ignore[attr-defined]
        _SyncSessionLease(connection),
        8,
        "INBOX",
    )

    assert result.immediate_result == SyncRunResult(
        SyncRunStatus.COLD_START_PENDING,
        0,
        0,
        "sync.cold_start_required",
    )
    assert result.ownership.pipeline_name == "pipeline-v2"
    assert result.ownership.generation == 3
    assert result.ownership.fencing_token == 9
    assert connection.events == [
        "xid.enter",
        "xid.read_committed",
        "xid.timeouts",
        "ownership.shared_lock",
        "ownership.current_ingress",
        "cursor.for_update",
        "xid.exit",
    ]


@pytest.mark.asyncio
async def test_shared_ownership_reader_keeps_ordinary_lock_and_allows_plain_select() -> (
    None
):
    read_ownership = getattr(sync_module, "_read_current_ownership")
    ordinary = _SharedHelperRegressionConnection()
    maintenance = _SharedHelperRegressionConnection()

    ordinary_result = await read_ownership(ordinary, 8)
    maintenance_result = await read_ownership(
        maintenance,
        8,
        for_key_share=False,
    )

    assert ordinary_result == maintenance_result
    ordinary_sql = [
        statement
        for statement, _params in ordinary.statements
        if "FROM public.pipeline_ownership" in statement
    ]
    maintenance_sql = [
        statement
        for statement, _params in maintenance.statements
        if "FROM public.pipeline_ownership" in statement
    ]
    assert len(ordinary_sql) == len(maintenance_sql) == 1
    assert ordinary_sql[0].endswith("FOR KEY SHARE")
    assert "FOR KEY SHARE" not in maintenance_sql[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [None, 0, 1, "false", object()])
async def test_shared_ownership_reader_rejects_non_exact_lock_mode_before_query(
    invalid: object,
) -> None:
    read_ownership = getattr(sync_module, "_read_current_ownership")
    connection = _SharedHelperRegressionConnection()

    with pytest.raises(ValueError, match="for_key_share"):
        await read_ownership(
            connection,
            8,
            for_key_share=invalid,
        )

    assert connection.statements == []
    assert connection.events == []


@pytest.mark.asyncio
async def test_ordinary_preflight_delegates_to_the_shared_cold_start_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure = getattr(sync_module, "_configure_sync_xid")
    read_ownership = getattr(sync_module, "_read_current_ownership")
    assert tuple(inspect.signature(configure).parameters) == (
        "connection",
        "account_id",
    )
    assert tuple(inspect.signature(read_ownership).parameters) == (
        "connection",
        "account_id",
        "expected",
        "for_key_share",
    )
    lock_mode = inspect.signature(read_ownership).parameters["for_key_share"]
    assert lock_mode.kind is inspect.Parameter.KEYWORD_ONLY
    assert lock_mode.default is True

    connection = _SharedHelperRegressionConnection()

    async def recording_configure(candidate: object, account_id: int) -> None:
        connection.events.append("shared.configure")
        await configure(candidate, account_id)

    async def recording_read(
        candidate: object,
        account_id: int,
        expected: object = None,
        *,
        for_key_share: bool = True,
    ) -> object:
        connection.events.append("shared.read_ownership")
        return await read_ownership(
            candidate,
            account_id,
            expected,
            for_key_share=for_key_share,
        )

    monkeypatch.setattr(sync_module, "_configure_sync_xid", recording_configure)
    monkeypatch.setattr(
        sync_module,
        "_read_current_ownership",
        recording_read,
    )
    coordinator = object.__new__(SyncCoordinator)

    await coordinator._preflight(  # type: ignore[attr-defined]
        _SyncSessionLease(connection),
        8,
        "INBOX",
    )

    assert connection.events == [
        "xid.enter",
        "shared.configure",
        "xid.read_committed",
        "xid.timeouts",
        "ownership.shared_lock",
        "shared.read_ownership",
        "ownership.current_ingress",
        "cursor.for_update",
        "xid.exit",
    ]


def _inbox_scope(*, webhook_id: str = "inbox-id") -> FolderScope:
    return FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=(webhook_id,),
        sync_folder="Inbox",
        event_policy_matrix=_policy_matrix(),
    )


class _SnapshotProvider:
    def __init__(self, snapshot: object) -> None:
        self.snapshot = snapshot
        self.calls: list[int] = []

    async def get_ready_snapshot(self, account_id: int) -> object:
        self.calls.append(account_id)
        if isinstance(self.snapshot, BaseException):
            raise self.snapshot
        return self.snapshot


class _ForbiddenClient:
    async def sync_emails(self, *_args: object) -> object:
        raise AssertionError("scope rejection must precede Exchange")


class _ForbiddenPool:
    kwargs = {"autocommit": True}
    close_returns = False

    async def getconn(self) -> object:
        raise AssertionError("scope rejection must precede SQL")


class _ForbiddenInbox:
    def transaction(self, _connection: object) -> object:
        raise AssertionError("scope rejection must precede Inbox DML")


def _coordinator_for_scope_test(
    snapshot: object,
) -> tuple[SyncCoordinator, _SnapshotProvider, _Permit]:
    provider = _SnapshotProvider(snapshot)
    events: list[str] = []
    permit = _Permit(events)
    coordinator = SyncCoordinator(
        page_client=_ForbiddenClient(),
        snapshot_provider=provider,
        policy_resolver=ProcessingPolicyResolver(),
        folder_permit=permit,
        sync_pool=_ForbiddenPool(),
        inbox_repository=_ForbiddenInbox(),
        page_limit=100,
        default_max_pages=2,
        default_max_run_seconds=30.0,
        cleanup_timeout=1.0,
    )
    return coordinator, provider, permit


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot",
    [
        None,
        PolicySnapshot.failed(),
        PolicySnapshot(scopes=(), refreshed=False),
        PolicySnapshot(scopes=(_inbox_scope(), _inbox_scope(webhook_id="other"))),
    ],
)
async def test_unready_policy_snapshot_fails_before_permit_or_sql(
    snapshot: object,
) -> None:
    coordinator, provider, permit = _coordinator_for_scope_test(snapshot)

    with pytest.raises(PolicySnapshotUnavailableError):
        await coordinator.run_folder(8, "INBOX")

    assert provider.calls == [8]
    assert permit.events == []


@pytest.mark.asyncio
async def test_absent_configured_scope_fails_before_permit_or_sql() -> None:
    coordinator, provider, permit = _coordinator_for_scope_test(
        PolicySnapshot(scopes=(_inbox_scope(),)),
    )

    with pytest.raises(PolicySnapshotUnavailableError):
        await coordinator.run_folder(8, "ARCHIVE")

    assert provider.calls == [8]
    assert permit.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["inbox", "Inbox", "SentItems"])
async def test_run_folder_rejects_noncanonical_alias_before_any_resource(
    alias: str,
) -> None:
    coordinator, provider, permit = _coordinator_for_scope_test(
        PolicySnapshot(scopes=(_inbox_scope(),)),
    )

    with pytest.raises(ValueError, match="canonical"):
        await coordinator.run_folder(8, alias)

    assert provider.calls == []
    assert permit.events == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("page_limit", True),
        ("page_limit", 0),
        ("page_limit", 501),
        ("default_max_pages", True),
        ("default_max_pages", 0),
        ("default_max_run_seconds", True),
        ("default_max_run_seconds", 0.0),
        ("default_max_run_seconds", float("inf")),
        ("cleanup_timeout", 0.0),
        ("cleanup_timeout", 31.0),
    ],
)
def test_coordinator_rejects_invalid_operational_budget_inputs(
    field: str,
    value: object,
) -> None:
    values: dict[str, object] = {
        "page_client": _ForbiddenClient(),
        "snapshot_provider": _SnapshotProvider(PolicySnapshot(scopes=())),
        "policy_resolver": ProcessingPolicyResolver(),
        "folder_permit": _Permit([]),
        "sync_pool": _ForbiddenPool(),
        "inbox_repository": _ForbiddenInbox(),
        "page_limit": 100,
        "default_max_pages": 2,
        "default_max_run_seconds": 30.0,
        "cleanup_timeout": 1.0,
    }
    values[field] = value

    with pytest.raises(ValueError, match=field):
        SyncCoordinator(**values)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_pages", "max_run_seconds", "message"),
    [
        (True, None, "max_pages"),
        (0, None, "max_pages"),
        (None, True, "max_run_seconds"),
        (None, 0.0, "max_run_seconds"),
        (None, float("nan"), "max_run_seconds"),
    ],
)
async def test_run_folder_rejects_invalid_budget_overrides_before_snapshot(
    max_pages: object,
    max_run_seconds: object,
    message: str,
) -> None:
    coordinator, provider, permit = _coordinator_for_scope_test(
        PolicySnapshot(scopes=(_inbox_scope(),)),
    )

    with pytest.raises(ValueError, match=message):
        await coordinator.run_folder(
            8,
            "INBOX",
            max_pages=max_pages,  # type: ignore[arg-type]
            max_run_seconds=max_run_seconds,  # type: ignore[arg-type]
        )

    assert provider.calls == []
    assert permit.events == []


def test_internal_session_outcomes_reject_ambiguous_resource_states() -> None:
    session_outcome = getattr(sync_module, "_SyncSessionOutcome")
    connection_outcome = getattr(sync_module, "_ConnectionReturnOutcome")

    with pytest.raises(ValueError, match="exact boolean"):
        session_outcome(acquired=1, value=None)
    with pytest.raises(ValueError, match="busy session"):
        session_outcome(acquired=False, value="impossible")

    invalid_connection_states = (
        {
            "returned": 1,
            "ownership_unknown": False,
            "process_error": None,
            "error": None,
        },
        {
            "returned": True,
            "ownership_unknown": True,
            "process_error": None,
            "error": None,
        },
        {
            "returned": True,
            "ownership_unknown": False,
            "process_error": None,
            "error": RuntimeError("contradictory return"),
        },
        {
            "returned": False,
            "ownership_unknown": True,
            "process_error": None,
            "error": None,
        },
    )
    for values in invalid_connection_states:
        with pytest.raises(ValueError):
            connection_outcome(**values)


def test_row_and_lock_decoders_reject_hostile_shapes_without_guessing() -> None:
    row_values = getattr(sync_module, "_row_values")
    single_boolean = getattr(sync_module, "_single_boolean")

    assert row_values(("cursor", 7, "ignored"), ("cursor", "version")) == (
        "cursor",
        7,
    )
    assert row_values(["cursor", 7], ("cursor", "version")) == ("cursor", 7)
    for row in ({"cursor": "only"}, ("short",), object()):
        with pytest.raises(DatabaseOperationError) as caught:
            row_values(row, ("cursor", "version"))
        assert caught.value.operation == "sync_database_row"
        assert caught.value.retryable is False

    for row in ((True, False), object(), {}):
        with pytest.raises(DatabaseOperationError) as caught:
            single_boolean(row, "acquired")
        assert caught.value.operation == "sync_session_acquire"


def test_folder_permit_release_rejects_non_none_synchronous_result() -> None:
    marker = object()
    lease = FolderPermitLease(lambda: marker)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="must return None"):
        lease.release()

    with pytest.raises(RuntimeError, match="already released"):
        lease.release()


class _ScriptedTransaction:
    def __init__(
        self,
        *,
        enter_error: BaseException | None = None,
        exit_error: BaseException | None = None,
    ) -> None:
        self.enter_error = enter_error
        self.exit_error = exit_error
        self.exit_arguments: tuple[object, object, object] | None = None

    async def __aenter__(self) -> None:
        if self.enter_error is not None:
            raise self.enter_error

    async def __aexit__(
        self,
        error_type: object,
        error: object,
        traceback: object,
    ) -> None:
        self.exit_arguments = (error_type, error, traceback)
        if self.exit_error is not None:
            raise self.exit_error


class _ScriptedTransactionConnection:
    def __init__(self, transaction: _ScriptedTransaction) -> None:
        self._transaction = transaction

    def transaction(self) -> _ScriptedTransaction:
        return self._transaction


@pytest.mark.asyncio
async def test_caller_owned_transaction_taints_unknown_begin_outcome() -> None:
    primary = RuntimeError("begin acknowledgement lost")
    transaction = _ScriptedTransaction(enter_error=primary)
    session = _SyncSessionLease(_ScriptedTransactionConnection(transaction))

    async def forbidden(_connection: object) -> None:
        raise AssertionError("body must not run after unknown begin")

    with pytest.raises(RuntimeError) as caught:
        await _caller_owned_transaction(session, forbidden)

    assert caught.value is primary
    assert session.tainted is True


@pytest.mark.asyncio
async def test_caller_owned_transaction_taints_enter_health_probe_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = RuntimeError("transaction health unavailable")
    transaction = _ScriptedTransaction()
    session = _SyncSessionLease(_ScriptedTransactionConnection(transaction))

    def failed_health(_connection: object) -> bool:
        raise primary

    monkeypatch.setattr(sync_module, "_connection_is_open_intrans", failed_health)

    async def forbidden(_connection: object) -> None:
        raise AssertionError("body must not run without confirmed INTRANS")

    with pytest.raises(RuntimeError) as caught:
        await _caller_owned_transaction(session, forbidden)

    assert caught.value is primary
    assert session.tainted is True


@pytest.mark.asyncio
async def test_caller_owned_transaction_preserves_body_after_ordinary_rollback_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = RuntimeError("body failed")
    rollback_error = RuntimeError("rollback acknowledgement lost")
    transaction = _ScriptedTransaction(exit_error=rollback_error)
    session = _SyncSessionLease(_ScriptedTransactionConnection(transaction))
    monkeypatch.setattr(sync_module, "_connection_is_open_intrans", lambda _: True)
    monkeypatch.setattr(sync_module, "_connection_is_open_idle", lambda _: True)

    async def failed_body(_connection: object) -> None:
        raise primary

    with pytest.raises(RuntimeError) as caught:
        await _caller_owned_transaction(session, failed_body)

    assert caught.value is primary
    assert transaction.exit_arguments is not None
    assert transaction.exit_arguments[1] is primary
    assert session.tainted is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "health_error",
    [RuntimeError("rollback health failed"), asyncio.CancelledError("cancelled")],
    ids=["ordinary-health-error", "process-control-health-error"],
)
async def test_caller_owned_transaction_classifies_rollback_health_failure(
    monkeypatch: pytest.MonkeyPatch,
    health_error: BaseException,
) -> None:
    primary = RuntimeError("body failed")
    transaction = _ScriptedTransaction()
    session = _SyncSessionLease(_ScriptedTransactionConnection(transaction))
    monkeypatch.setattr(sync_module, "_connection_is_open_intrans", lambda _: True)

    def failed_health(_connection: object) -> bool:
        raise health_error

    monkeypatch.setattr(sync_module, "_connection_is_open_idle", failed_health)

    async def failed_body(_connection: object) -> None:
        raise primary

    expected = (
        type(health_error) if not isinstance(health_error, Exception) else RuntimeError
    )
    with pytest.raises(expected) as caught:
        await _caller_owned_transaction(session, failed_body)

    if isinstance(health_error, Exception):
        assert caught.value is primary
    else:
        assert caught.value is health_error
    assert session.tainted is True


@pytest.mark.asyncio
async def test_caller_owned_transaction_taints_unknown_commit_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = RuntimeError("commit acknowledgement lost")
    transaction = _ScriptedTransaction(exit_error=primary)
    session = _SyncSessionLease(_ScriptedTransactionConnection(transaction))
    monkeypatch.setattr(sync_module, "_connection_is_open_intrans", lambda _: True)

    async def completed(_connection: object) -> str:
        return "value"

    with pytest.raises(RuntimeError) as caught:
        await _caller_owned_transaction(session, completed)

    assert caught.value is primary
    assert session.tainted is True


@pytest.mark.asyncio
async def test_caller_owned_transaction_taints_post_commit_health_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = RuntimeError("post-commit health unavailable")
    transaction = _ScriptedTransaction()
    session = _SyncSessionLease(_ScriptedTransactionConnection(transaction))
    monkeypatch.setattr(sync_module, "_connection_is_open_intrans", lambda _: True)

    def failed_health(_connection: object) -> bool:
        raise primary

    monkeypatch.setattr(sync_module, "_connection_is_open_idle", failed_health)

    async def completed(_connection: object) -> str:
        return "value"

    with pytest.raises(RuntimeError) as caught:
        await _caller_owned_transaction(session, completed)

    assert caught.value is primary
    assert session.tainted is True


class _CheckoutFailurePool:
    kwargs = {"autocommit": True}
    close_returns = False

    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.get_calls = 0
        self.put_calls = 0

    async def getconn(self) -> object:
        self.get_calls += 1
        raise self.error

    async def putconn(self, _connection: object) -> None:
        self.put_calls += 1
        raise AssertionError("checkout failure has no connection to return")


class _SuppliedLeasePermit:
    def __init__(self, lease: FolderPermitLease) -> None:
        self.lease = lease
        self.acquire_calls = 0

    async def try_acquire(self, account_id: int, folder: str) -> FolderPermitLease:
        assert (account_id, folder) == (8, "INBOX")
        self.acquire_calls += 1
        return self.lease


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("checkout_error", "release_error", "expected_type"),
    [
        (KeyboardInterrupt(), None, KeyboardInterrupt),
        (RuntimeError("checkout"), KeyboardInterrupt(), KeyboardInterrupt),
        (RuntimeError("checkout"), RuntimeError("release"), DatabaseOperationError),
    ],
    ids=[
        "checkout-process-control",
        "release-process-control",
        "ordinary-checkout-and-release",
    ],
)
async def test_checkout_failure_releases_permit_with_exact_error_precedence(
    checkout_error: BaseException,
    release_error: BaseException | None,
    expected_type: type[BaseException],
) -> None:
    releases: list[str] = []

    def release() -> None:
        releases.append("release")
        if release_error is not None:
            raise release_error

    pool = _CheckoutFailurePool(checkout_error)
    runner = _SyncSessionRunner(
        pool=pool,
        permit=_SuppliedLeasePermit(FolderPermitLease(release)),
        cleanup_timeout=0.01,
    )

    async def forbidden(_session: object) -> None:
        raise AssertionError("body must not run after checkout failure")

    with pytest.raises(expected_type) as caught:
        await runner.run(8, "INBOX", forbidden)

    if expected_type is KeyboardInterrupt:
        expected_error = (
            checkout_error
            if isinstance(checkout_error, KeyboardInterrupt)
            else release_error
        )
        assert caught.value is expected_error
    else:
        assert caught.value.operation == "sync_pool_checkout"
    assert releases == ["release"]
    assert pool.get_calls == 1
    assert pool.put_calls == 0


def _runner_for_connection(
    connection: _Connection,
    events: list[str],
) -> tuple[_SyncSessionRunner, _Pool, _Permit]:
    pool = _Pool(connection, events)
    permit = _Permit(events)
    return (
        _SyncSessionRunner(pool=pool, permit=permit, cleanup_timeout=0.01),
        pool,
        permit,
    )


@pytest.mark.asyncio
async def test_pool_contract_process_control_primary_wins_after_clean_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, connection, pool, permit, _state, _events = _runner()
    primary = KeyboardInterrupt()

    def failed_contract(_connection: object) -> None:
        raise primary

    monkeypatch.setattr(runner, "_validate_pool_contract", failed_contract)

    with pytest.raises(KeyboardInterrupt) as caught:
        await runner.run(8, "INBOX", lambda _: asyncio.sleep(0))

    assert caught.value is primary
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1


@pytest.mark.asyncio
async def test_pool_contract_cleanup_process_control_wins_over_ordinary_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    state = _LockState()
    cleanup_error = KeyboardInterrupt()
    connection = _ProcessControlCloseConnection(events, state, cleanup_error)
    runner, pool, permit = _runner_for_connection(connection, events)

    def failed_contract(_connection: object) -> None:
        raise RuntimeError("contract probe failed")

    monkeypatch.setattr(runner, "_validate_pool_contract", failed_contract)

    with pytest.raises(KeyboardInterrupt) as caught:
        await runner.run(8, "INBOX", lambda _: asyncio.sleep(0))

    assert caught.value is cleanup_error
    assert pool.returned == []
    assert permit.release_calls == 1


@pytest.mark.asyncio
async def test_pool_contract_ordinary_cleanup_failure_has_fixed_public_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    state = _LockState()
    connection = _UncloseableConnection(events, state)
    runner, pool, permit = _runner_for_connection(connection, events)

    def failed_contract(_connection: object) -> None:
        raise RuntimeError("contract probe failed")

    monkeypatch.setattr(runner, "_validate_pool_contract", failed_contract)

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", lambda _: asyncio.sleep(0))

    assert caught.value.operation == "sync_session_cleanup"
    assert pool.returned == []
    assert permit.release_calls == 1


@pytest.mark.asyncio
async def test_pool_contract_unknown_ordinary_failure_is_normalized_after_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, connection, pool, permit, _state, _events = _runner()
    primary = RuntimeError("unexpected pool descriptor failure")

    def failed_contract(_connection: object) -> None:
        raise primary

    monkeypatch.setattr(runner, "_validate_pool_contract", failed_contract)

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", lambda _: asyncio.sleep(0))

    assert caught.value.operation == "sync_pool_contract"
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cleanup_kind",
    ["clean", "ordinary", "process-control"],
)
async def test_lock_acquire_failure_applies_cleanup_error_precedence(
    cleanup_kind: str,
) -> None:
    events: list[str] = []
    state = _LockState()
    primary = RuntimeError("lock acquisition failed")
    cleanup_process_error = KeyboardInterrupt()
    if cleanup_kind == "ordinary":
        connection: _Connection = _UncloseableConnection(events, state)
    elif cleanup_kind == "process-control":
        connection = _ProcessControlCloseConnection(
            events,
            state,
            cleanup_process_error,
        )
    else:
        connection = _Connection(events, state)
    connection.acquire_error = primary
    runner, pool, permit = _runner_for_connection(connection, events)

    async def forbidden(_session: object) -> None:
        raise AssertionError("body must not run after failed lock acquisition")

    expected = (
        KeyboardInterrupt
        if cleanup_kind == "process-control"
        else DatabaseOperationError
    )
    with pytest.raises(expected) as caught:
        await runner.run(8, "INBOX", forbidden)

    if cleanup_kind == "process-control":
        assert caught.value is cleanup_process_error
        assert pool.returned == []
    else:
        assert caught.value.operation == (
            "sync_session_cleanup"
            if cleanup_kind == "ordinary"
            else "sync_session_acquire"
        )
        assert pool.returned == ([connection] if cleanup_kind == "clean" else [])
    assert permit.release_calls == 1


class _HealthSequence:
    def __init__(self, *values: object) -> None:
        self.values = list(values)
        self.calls = 0

    def __call__(self, _connection: object) -> bool:
        self.calls += 1
        if not self.values:
            raise AssertionError("unexpected connection health probe")
        value = self.values.pop(0)
        if isinstance(value, BaseException):
            raise value
        assert type(value) is bool
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize("acquired", [False, True], ids=["busy", "acquired"])
async def test_lock_result_health_process_control_is_preserved_after_eviction(
    monkeypatch: pytest.MonkeyPatch,
    acquired: bool,
) -> None:
    runner, connection, pool, permit, _state, _events = _runner(
        acquire_result=acquired,
    )
    health_error = KeyboardInterrupt()
    probes = _HealthSequence(True, health_error)
    monkeypatch.setattr(runner, "_connection_is_open_idle", probes)

    async def forbidden(_session: object) -> None:
        raise AssertionError("body must not run without confirmed idle health")

    with pytest.raises(KeyboardInterrupt) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value is health_error
    assert probes.calls == 2
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("acquired", [False, True], ids=["busy", "acquired"])
async def test_lock_result_cleanup_process_control_wins_when_health_is_nonidle(
    monkeypatch: pytest.MonkeyPatch,
    acquired: bool,
) -> None:
    events: list[str] = []
    state = _LockState()
    cleanup_error = KeyboardInterrupt()
    connection = _ProcessControlCloseConnection(events, state, cleanup_error)
    connection.acquire_result = acquired
    runner, pool, permit = _runner_for_connection(connection, events)
    probes = _HealthSequence(True, False)
    monkeypatch.setattr(runner, "_connection_is_open_idle", probes)

    async def forbidden(_session: object) -> None:
        raise AssertionError("body must not run on a nonidle lock result")

    with pytest.raises(KeyboardInterrupt) as caught:
        await runner.run(8, "INBOX", forbidden)

    assert caught.value is cleanup_error
    assert probes.calls == 2
    assert pool.returned == []
    assert permit.release_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("primary_kind", "health_kind", "expected_kind"),
    [
        ("none", "ordinary", "health"),
        ("ordinary", "process-control", "health"),
        ("process-control", "ordinary", "primary"),
    ],
)
async def test_post_operation_health_error_uses_exact_process_control_precedence(
    monkeypatch: pytest.MonkeyPatch,
    primary_kind: str,
    health_kind: str,
    expected_kind: str,
) -> None:
    runner, connection, pool, permit, _state, _events = _runner()
    primary: BaseException | None = None
    if primary_kind == "ordinary":
        primary = RuntimeError("operation failed")
    elif primary_kind == "process-control":
        primary = KeyboardInterrupt()
    health_error: BaseException = (
        KeyboardInterrupt()
        if health_kind == "process-control"
        else RuntimeError("health failed")
    )
    probes = _HealthSequence(True, True, health_error)
    monkeypatch.setattr(runner, "_connection_is_open_idle", probes)

    async def operation(_session: object) -> str:
        if primary is not None:
            raise primary
        return "completed"

    expected_error = health_error if expected_kind == "health" else primary
    assert expected_error is not None
    with pytest.raises(type(expected_error)) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value is expected_error
    assert probes.calls == 3
    assert connection.closed is True
    assert pool.returned == [connection]
    assert permit.release_calls == 1


@pytest.mark.asyncio
async def test_tainted_session_cleanup_process_control_wins_without_primary() -> None:
    events: list[str] = []
    state = _LockState()
    cleanup_error = KeyboardInterrupt()
    connection = _ProcessControlCloseConnection(events, state, cleanup_error)
    runner, pool, permit = _runner_for_connection(connection, events)

    async def operation(session: _SyncSessionLease) -> str:
        session.taint()
        return "unknown"

    with pytest.raises(KeyboardInterrupt) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value is cleanup_error
    assert pool.returned == []
    assert permit.release_calls == 1


@pytest.mark.asyncio
async def test_ordinary_body_error_is_preserved_after_fully_successful_cleanup() -> (
    None
):
    runner, connection, pool, permit, state, events = _runner()
    primary = RuntimeError("body failed")

    async def operation(_session: object) -> None:
        raise primary

    with pytest.raises(RuntimeError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value is primary
    assert connection.closed is False
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1
    assert events[-1] == "permit.release"


@pytest.mark.asyncio
async def test_post_unlock_health_probe_failure_forces_physical_eviction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, connection, pool, permit, state, _events = _runner()
    health_error = RuntimeError("post-unlock health failed")
    probes = _HealthSequence(True, True, True, health_error)
    monkeypatch.setattr(runner, "_connection_is_open_idle", probes)

    async def operation(_session: object) -> str:
        return "completed"

    with pytest.raises(DatabaseOperationError) as caught:
        await runner.run(8, "INBOX", operation)

    assert caught.value.operation == "sync_session_cleanup"
    assert probes.calls == 4
    assert connection.closed is True
    assert state.owned is False
    assert pool.returned == [connection]
    assert permit.release_calls == 1


@pytest.mark.asyncio
async def test_hard_reaper_handles_already_done_and_promptly_cancelled_children() -> (
    None
):
    runner, _connection, _pool, _permit, _state, _events = _runner()

    async def completed() -> str:
        return "done"

    done_task = asyncio.create_task(completed())
    assert await done_task == "done"
    assert await runner._hard_bounded_reap(done_task) is None

    pending_task = asyncio.create_task(asyncio.Event().wait())
    await asyncio.sleep(0)
    cancellation = await runner._hard_bounded_reap(pending_task)
    assert isinstance(cancellation, asyncio.CancelledError)
    assert pending_task.done() is True


class _FinishClosesConnection:
    def __init__(self, connection: _CloseNoopConnection) -> None:
        self._connection = connection
        self.calls = 0

    def finish(self) -> None:
        self.calls += 1
        self._connection.closed = True


class _CloseNoopConnection:
    def __init__(self, *, finish_closes: bool) -> None:
        self.closed = False
        self.close_calls = 0
        if finish_closes:
            self.pgconn = _FinishClosesConnection(self)

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
@pytest.mark.parametrize("finish_closes", [True, False])
async def test_confirmed_close_requires_observable_physical_closure(
    finish_closes: bool,
) -> None:
    runner, _connection, _pool, _permit, _state, _events = _runner()
    connection = _CloseNoopConnection(finish_closes=finish_closes)

    confirmed, error = await runner._confirmed_close(connection)

    assert confirmed is finish_closes
    assert connection.close_calls == 1
    if finish_closes:
        assert error is None
        assert connection.closed is True
        assert connection.pgconn.calls == 1
    else:
        assert isinstance(error, DatabaseOperationError)
        assert error.operation == "sync_session_cleanup"
        assert connection.closed is False


class _ArbitraryPutAwaitable:
    def __await__(self):
        if False:
            yield None
        return None


class _ArbitraryAwaitablePool(_Pool):
    def putconn(self, _connection: _Connection) -> _ArbitraryPutAwaitable:  # type: ignore[override]
        self.events.append("pool.putconn")
        return _ArbitraryPutAwaitable()


@pytest.mark.asyncio
@pytest.mark.parametrize("arbitrary_awaitable", [False, True])
async def test_return_connection_create_task_failure_closes_owned_coroutines(
    monkeypatch: pytest.MonkeyPatch,
    recwarn: pytest.WarningsRecorder,
    arbitrary_awaitable: bool,
) -> None:
    events: list[str] = []
    state = _LockState()
    connection = _Connection(events, state)
    pool: object = (
        _ArbitraryAwaitablePool(connection, events)
        if arbitrary_awaitable
        else _Pool(connection, events)
    )
    runner = _SyncSessionRunner(
        pool=pool,
        permit=_Permit(events),
        cleanup_timeout=0.01,
    )
    create_error = RuntimeError("task creation unavailable")

    def failed_create(_awaitable: object) -> object:
        raise create_error

    monkeypatch.setattr(sync_module.asyncio, "create_task", failed_create)

    outcome = await runner._return_connection(connection)

    assert outcome.returned is False
    assert outcome.ownership_unknown is True
    assert outcome.process_error is None
    assert outcome.error is create_error
    assert [
        warning for warning in recwarn if "never awaited" in str(warning.message)
    ] == []


@pytest.mark.asyncio
async def test_return_connection_past_deadline_quarantines_child_immediately() -> None:
    runner, connection, pool, _permit, _state, _events = _runner()
    deadline = asyncio.get_running_loop().time() - 1.0

    outcome = await runner._return_connection(connection, deadline=deadline)

    assert outcome.returned is False
    assert outcome.ownership_unknown is True
    assert isinstance(outcome.error, TimeoutError)
    await asyncio.sleep(0)
    assert pool.returned == [connection]


@pytest.mark.asyncio
async def test_unlock_and_operation_boundaries_reject_hostile_results() -> None:
    runner, _connection, _pool, _permit, _state, _events = _runner()

    class HostileUnlockConnection:
        async def execute(self, _statement: str, _params: object) -> _Cursor:
            return _Cursor((True, False))

    assert await runner._unlock(HostileUnlockConnection(), (1, 2)) is False

    with pytest.raises(ValueError, match="operation"):
        await runner.run(8, "INBOX", object())  # type: ignore[arg-type]


class _SingleRowConnection:
    def __init__(self, row: object) -> None:
        self.row = row
        self.statements: list[tuple[str, object]] = []

    async def execute(self, statement: str, params: object = None) -> _Cursor:
        self.statements.append((statement, params))
        return _Cursor(self.row)


@pytest.mark.asyncio
async def test_ownership_reader_rejects_invalid_row_and_exact_fence_mismatch() -> None:
    read_ownership = getattr(sync_module, "_read_current_ownership")
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")

    invalid = _SingleRowConnection(("", 1, 1))
    with pytest.raises(DatabaseOperationError) as caught:
        await read_ownership(invalid, 8)
    assert caught.value.operation == "sync_pipeline_ownership"
    assert caught.value.retryable is False

    expected = ownership_snapshot("pipeline-v2", 3, 9)
    changed = _SingleRowConnection(("pipeline-v2", 3, 10))
    with pytest.raises(sync_module.StaleFence):
        await read_ownership(changed, 8, expected)


def test_coordinator_requires_exact_policy_resolver_before_storing_dependencies() -> (
    None
):
    values: dict[str, object] = {
        "page_client": _ForbiddenClient(),
        "snapshot_provider": _SnapshotProvider(PolicySnapshot(scopes=())),
        "policy_resolver": object(),
        "folder_permit": _Permit([]),
        "sync_pool": _ForbiddenPool(),
        "inbox_repository": _ForbiddenInbox(),
        "page_limit": 100,
        "default_max_pages": 2,
        "default_max_run_seconds": 30.0,
        "cleanup_timeout": 1.0,
    }

    with pytest.raises(ValueError, match="policy_resolver"):
        SyncCoordinator(**values)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_error",
    [RuntimeError("snapshot failed"), KeyboardInterrupt()],
    ids=["ordinary-normalized", "process-control-preserved"],
)
async def test_ready_scope_classifies_unexpected_snapshot_provider_failures(
    provider_error: BaseException,
) -> None:
    coordinator = object.__new__(SyncCoordinator)
    coordinator._snapshot_provider = _SnapshotProvider(provider_error)  # type: ignore[attr-defined]
    coordinator._policy_resolver = ProcessingPolicyResolver()  # type: ignore[attr-defined]

    expected = (
        PolicySnapshotUnavailableError
        if isinstance(provider_error, Exception)
        else type(provider_error)
    )
    with pytest.raises(expected) as caught:
        await coordinator._ready_scope(8, "INBOX")  # type: ignore[attr-defined]

    if not isinstance(provider_error, Exception):
        assert caught.value is provider_error


class _RowsConnection:
    def __init__(self, rows: list[object]) -> None:
        self.rows = list(rows)
        self.statements: list[tuple[str, object]] = []

    async def execute(self, statement: str, params: object = None) -> _Cursor:
        self.statements.append((statement, params))
        if not self.rows:
            raise AssertionError(f"unexpected SQL after scripted rows: {statement}")
        return _Cursor(self.rows.pop(0))


async def _direct_caller_owned_transaction(
    session: _SyncSessionLease,
    operation: Callable[[object], object],
) -> object:
    return await operation(session.connection)  # type: ignore[misc]


async def _noop_configure(_connection: object, _account_id: int) -> None:
    return None


async def _fixed_ownership(*_args: object, **_kwargs: object) -> object:
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")
    return ownership_snapshot("pipeline-v2", 3, 9)


def _patch_direct_sync_transaction_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sync_module,
        "_caller_owned_transaction",
        _direct_caller_owned_transaction,
    )
    monkeypatch.setattr(sync_module, "_configure_sync_xid", _noop_configure)
    monkeypatch.setattr(sync_module, "_read_current_ownership", _fixed_ownership)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "expected_statement_count"),
    [
        ([None, None, None], 3),
        (
            [
                {
                    "cursor": "cursor-1",
                    "status": "active",
                    "version": "not-an-integer",
                    "transient_failures": 0,
                    "retry_deferred": False,
                }
            ],
            1,
        ),
        (
            [
                {
                    "cursor": " cursor-1 ",
                    "status": "active",
                    "version": 1,
                    "transient_failures": 0,
                    "retry_deferred": False,
                }
            ],
            1,
        ),
    ],
    ids=["insert-race-still-missing", "invalid-types", "invalid-active-cursor"],
)
async def test_preflight_rejects_missing_or_invalid_durable_cursor_rows(
    monkeypatch: pytest.MonkeyPatch,
    rows: list[object],
    expected_statement_count: int,
) -> None:
    _patch_direct_sync_transaction_dependencies(monkeypatch)
    connection = _RowsConnection(rows)
    coordinator = object.__new__(SyncCoordinator)

    with pytest.raises(DatabaseOperationError) as caught:
        await coordinator._preflight(  # type: ignore[attr-defined]
            _SyncSessionLease(connection),
            8,
            "INBOX",
        )

    assert caught.value.operation == "sync_cursor_preflight"
    assert caught.value.retryable is False
    assert len(connection.statements) == expected_statement_count


@pytest.mark.asyncio
async def test_expected_cursor_lock_requires_an_existing_row() -> None:
    coordinator = object.__new__(SyncCoordinator)
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")
    expected = cursor_snapshot("cursor-1", "active", 7, 0, False)
    connection = _SingleRowConnection(None)

    with pytest.raises(sync_module.StaleFence):
        await coordinator._lock_expected_cursor(  # type: ignore[attr-defined]
            connection,
            8,
            "INBOX",
            expected,
        )


@pytest.mark.asyncio
async def test_error_transition_rejects_unsupported_target_before_any_sql() -> None:
    coordinator = object.__new__(SyncCoordinator)
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")

    with pytest.raises(ValueError, match="unsupported"):
        await coordinator._commit_error(  # type: ignore[attr-defined]
            _SyncSessionLease(object()),
            account_id=8,
            folder="INBOX",
            ownership=ownership_snapshot("pipeline-v2", 3, 9),
            expected=cursor_snapshot("cursor-1", "active", 7, 0, False),
            reason_code="sync.invalid",
            target=SyncRunStatus.CAUGHT_UP,
            pages_committed=0,
            changes_observed=0,
        )


async def _noop_lock_expected(*_args: object, **_kwargs: object) -> None:
    return None


async def _noop_audit(*_args: object, **_kwargs: object) -> None:
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_row", [None, (9,)], ids=["missing", "wrong-version"])
async def test_error_transition_requires_exact_single_version_increment(
    monkeypatch: pytest.MonkeyPatch,
    returned_row: object,
) -> None:
    _patch_direct_sync_transaction_dependencies(monkeypatch)
    coordinator = object.__new__(SyncCoordinator)
    monkeypatch.setattr(coordinator, "_lock_expected_cursor", _noop_lock_expected)
    monkeypatch.setattr(coordinator, "_append_error_audit", _noop_audit)
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")
    connection = _SingleRowConnection(returned_row)

    with pytest.raises((sync_module.StaleFence, DatabaseOperationError)) as caught:
        await coordinator._commit_error(  # type: ignore[attr-defined]
            _SyncSessionLease(connection),
            account_id=8,
            folder="INBOX",
            ownership=ownership_snapshot("pipeline-v2", 3, 9),
            expected=cursor_snapshot("cursor-1", "active", 7, 0, False),
            reason_code="exchange.sync.cursor_invalid",
            target=SyncRunStatus.RESET_REQUIRED,
            pages_committed=0,
            changes_observed=0,
        )

    if returned_row is not None:
        assert caught.value.operation == "sync_error_transition"


class _NoopInboxTransaction:
    async def insert(self, *_args: object) -> None:
        raise AssertionError("no events were supplied")


class _NoopInboxRepository:
    def transaction(self, _connection: object) -> _NoopInboxTransaction:
        return _NoopInboxTransaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_row", [None, (9,)], ids=["missing", "wrong-version"])
async def test_page_commit_requires_exact_single_version_increment(
    monkeypatch: pytest.MonkeyPatch,
    returned_row: object,
) -> None:
    _patch_direct_sync_transaction_dependencies(monkeypatch)
    coordinator = object.__new__(SyncCoordinator)
    coordinator._inbox_repository = _NoopInboxRepository()  # type: ignore[attr-defined]
    monkeypatch.setattr(coordinator, "_lock_expected_cursor", _noop_lock_expected)
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")
    connection = _SingleRowConnection(returned_row)

    with pytest.raises((sync_module.StaleFence, DatabaseOperationError)) as caught:
        await coordinator._commit_page(  # type: ignore[attr-defined]
            _SyncSessionLease(connection),
            account_id=8,
            folder="INBOX",
            ownership=ownership_snapshot("pipeline-v2", 3, 9),
            expected=cursor_snapshot("cursor-1", "active", 7, 0, False),
            next_cursor="cursor-2",
            events=(),
        )

    if returned_row is not None:
        assert caught.value.operation == "sync_page_commit"


@pytest.mark.asyncio
async def test_run_locked_rejects_active_preflight_without_cursor_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = object.__new__(SyncCoordinator)
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")
    preflight_snapshot = getattr(sync_module, "_PreflightSnapshot")

    async def inconsistent_preflight(*_args: object) -> object:
        return preflight_snapshot(
            ownership_snapshot("pipeline-v2", 3, 9),
            None,
            None,
        )

    monkeypatch.setattr(coordinator, "_preflight", inconsistent_preflight)

    with pytest.raises(DatabaseOperationError) as caught:
        await coordinator._run_locked(  # type: ignore[attr-defined]
            _SyncSessionLease(object()),
            8,
            _inbox_scope(),
            object(),
            1,
            asyncio.get_running_loop().time() + 1,
        )

    assert caught.value.operation == "sync_cursor_preflight"
    assert caught.value.retryable is False


class _InvalidCoordinatorResultRunner:
    async def run(self, *_args: object) -> object:
        session_outcome = getattr(sync_module, "_SyncSessionOutcome")
        return session_outcome(acquired=True, value=object())


@pytest.mark.asyncio
async def test_run_folder_rejects_non_result_from_acquired_session() -> None:
    coordinator, provider, permit = _coordinator_for_scope_test(
        PolicySnapshot(scopes=(_inbox_scope(),)),
    )
    coordinator._session_runner = _InvalidCoordinatorResultRunner()  # type: ignore[assignment]

    with pytest.raises(DatabaseOperationError) as caught:
        await coordinator.run_folder(8, "INBOX")

    assert caught.value.operation == "sync_coordinator_result"
    assert provider.calls == [8]
    assert permit.events == []


class _ScriptedSyncPageClient:
    def __init__(self, *responses: object) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[int, str, str, int]] = []

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        page_limit: int,
    ) -> object:
        self.calls.append((account_id, folder, cursor, page_limit))
        if not self._responses:
            raise AssertionError("unexpected extra sync page request")
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class _RejectingPolicyResolver:
    def resolve(self, *_args: object) -> ProcessingPolicy:
        raise ValueError("policy contract rejected")


def _ordinary_sync_batch(
    cursor: str,
    *,
    includes_last: bool,
    email_id: str | None = None,
) -> SyncBatch:
    changes: tuple[SyncChange, ...] = ()
    if email_id is not None:
        changes = (
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id=email_id,
                item={"id": email_id, "subject": "Coverage contract"},
                source_version="v1",
            ),
        )
    return SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor=cursor,
        changes=changes,
        includes_last=includes_last,
    )


def _run_locked_harness(
    monkeypatch: pytest.MonkeyPatch,
    page_client: object,
    *,
    immediate_result: SyncRunResult | None = None,
    policy_resolver: object | None = None,
) -> tuple[
    SyncCoordinator,
    object,
    object,
    list[dict[str, object]],
    list[dict[str, object]],
]:
    coordinator = object.__new__(SyncCoordinator)
    coordinator._page_client = page_client  # type: ignore[attr-defined]
    coordinator._page_limit = 100  # type: ignore[attr-defined]
    coordinator._policy_resolver = (  # type: ignore[attr-defined]
        policy_resolver or ProcessingPolicyResolver()
    )
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")(
        "pipeline-v2",
        3,
        9,
    )
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")(
        "cursor-1",
        "active",
        7,
        0,
        False,
    )
    preflight_snapshot = getattr(sync_module, "_PreflightSnapshot")(
        ownership_snapshot,
        None if immediate_result is not None else cursor_snapshot,
        immediate_result,
    )
    committed_pages: list[dict[str, object]] = []
    committed_errors: list[dict[str, object]] = []

    async def preflight(*_args: object) -> object:
        return preflight_snapshot

    async def commit_page(*_args: object, **kwargs: object) -> object:
        committed_pages.append(kwargs)
        expected = kwargs["expected"]
        return getattr(sync_module, "_CursorSnapshot")(
            kwargs["next_cursor"],
            "active",
            expected.version + 1,  # type: ignore[union-attr]
            0,
            False,
        )

    async def commit_error(*_args: object, **kwargs: object) -> SyncRunResult:
        committed_errors.append(kwargs)
        return SyncRunResult(
            status=kwargs["target"],  # type: ignore[arg-type]
            pages_committed=kwargs["pages_committed"],  # type: ignore[arg-type]
            changes_observed=kwargs["changes_observed"],  # type: ignore[arg-type]
            safe_code=kwargs["reason_code"],  # type: ignore[arg-type]
        )

    monkeypatch.setattr(coordinator, "_preflight", preflight)
    monkeypatch.setattr(coordinator, "_commit_page", commit_page)
    monkeypatch.setattr(coordinator, "_commit_error", commit_error)
    return (
        coordinator,
        ownership_snapshot,
        cursor_snapshot,
        committed_pages,
        committed_errors,
    )


@pytest.mark.asyncio
async def test_run_locked_commits_each_valid_page_and_reports_caught_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ScriptedSyncPageClient(
        _ordinary_sync_batch(
            "cursor-2",
            includes_last=False,
            email_id="email-1",
        ),
        _ordinary_sync_batch(
            "cursor-3",
            includes_last=True,
            email_id="email-2",
        ),
    )
    coordinator, ownership, _cursor, pages, errors = _run_locked_harness(
        monkeypatch,
        client,
    )
    scope = _inbox_scope()
    snapshot = PolicySnapshot(scopes=(scope,))

    result = await coordinator._run_locked(  # type: ignore[attr-defined]
        _SyncSessionLease(object()),
        8,
        scope,
        snapshot,
        3,
        asyncio.get_running_loop().time() + 5,
    )

    assert result == SyncRunResult(SyncRunStatus.CAUGHT_UP, 2, 2, None)
    assert client.calls == [
        (8, "Inbox", "cursor-1", 100),
        (8, "Inbox", "cursor-2", 100),
    ]
    assert [page["next_cursor"] for page in pages] == ["cursor-2", "cursor-3"]
    assert all(page["ownership"] == ownership for page in pages)
    assert [event.external_email_id for page in pages for event in page["events"]] == [
        "email-1",
        "email-2",
    ]
    assert not errors


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("upstream_error", "expected_status", "expected_code", "retry_after"),
    [
        (
            SyncCursorInvalidError(),
            SyncRunStatus.RESET_REQUIRED,
            "exchange.sync.cursor_invalid",
            None,
        ),
        (
            SyncTransientError(retry_after_seconds=37),
            SyncRunStatus.RETRY_SCHEDULED,
            "exchange.sync.transient_failure",
            37,
        ),
        (
            SyncAuthorizationError(),
            SyncRunStatus.BLOCKED_CONTRACT,
            "exchange.sync.authorization_failed",
            None,
        ),
        (
            SyncContractError(),
            SyncRunStatus.BLOCKED_CONTRACT,
            "exchange.sync.contract_invalid",
            None,
        ),
    ],
    ids=["cursor-invalid", "transient", "authorization", "contract"],
)
async def test_run_locked_maps_typed_exchange_failures_to_durable_transitions(
    monkeypatch: pytest.MonkeyPatch,
    upstream_error: BaseException,
    expected_status: SyncRunStatus,
    expected_code: str,
    retry_after: int | None,
) -> None:
    client = _ScriptedSyncPageClient(upstream_error)
    coordinator, _ownership, _cursor, pages, errors = _run_locked_harness(
        monkeypatch,
        client,
    )
    scope = _inbox_scope()

    result = await coordinator._run_locked(  # type: ignore[attr-defined]
        _SyncSessionLease(object()),
        8,
        scope,
        PolicySnapshot(scopes=(scope,)),
        1,
        asyncio.get_running_loop().time() + 5,
    )

    assert result.status is expected_status
    assert result.safe_code == expected_code
    assert not pages
    assert len(errors) == 1
    assert errors[0]["target"] is expected_status
    assert errors[0]["reason_code"] == expected_code
    assert errors[0].get("retry_after_seconds") == retry_after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("page", "policy_resolver", "expected_code"),
    [
        (object(), None, "sync.local_contract_invalid"),
        (
            _ordinary_sync_batch("cursor-1", includes_last=False),
            None,
            "sync.cursor_stalled",
        ),
        (
            _ordinary_sync_batch(
                "cursor-2",
                includes_last=True,
                email_id="email-1",
            ),
            _RejectingPolicyResolver(),
            "sync.local_contract_invalid",
        ),
    ],
    ids=["invalid-page", "stalled-cursor", "normalization-failure"],
)
async def test_run_locked_blocks_untrusted_or_nonprogressing_pages(
    monkeypatch: pytest.MonkeyPatch,
    page: object,
    policy_resolver: object | None,
    expected_code: str,
) -> None:
    coordinator, _ownership, _cursor, pages, errors = _run_locked_harness(
        monkeypatch,
        _ScriptedSyncPageClient(page),
        policy_resolver=policy_resolver,
    )
    scope = _inbox_scope()

    result = await coordinator._run_locked(  # type: ignore[attr-defined]
        _SyncSessionLease(object()),
        8,
        scope,
        PolicySnapshot(scopes=(scope,)),
        1,
        asyncio.get_running_loop().time() + 5,
    )

    assert result.status is SyncRunStatus.BLOCKED_CONTRACT
    assert result.safe_code == expected_code
    assert not pages
    assert len(errors) == 1
    assert errors[0]["reason_code"] == expected_code


@pytest.mark.asyncio
async def test_run_locked_returns_preflight_result_without_calling_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    immediate = SyncRunResult(
        SyncRunStatus.RETRY_DEFERRED,
        0,
        0,
        "sync.retry_deferred",
    )
    client = _ScriptedSyncPageClient()
    coordinator, _ownership, _cursor, pages, errors = _run_locked_harness(
        monkeypatch,
        client,
        immediate_result=immediate,
    )

    result = await coordinator._run_locked(  # type: ignore[attr-defined]
        _SyncSessionLease(object()),
        8,
        _inbox_scope(),
        object(),
        1,
        asyncio.get_running_loop().time() + 5,
    )

    assert result is immediate
    assert not client.calls
    assert not pages
    assert not errors


@pytest.mark.asyncio
async def test_run_locked_reports_expired_budget_before_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ScriptedSyncPageClient()
    coordinator, _ownership, _cursor, pages, errors = _run_locked_harness(
        monkeypatch,
        client,
    )

    result = await coordinator._run_locked(  # type: ignore[attr-defined]
        _SyncSessionLease(object()),
        8,
        _inbox_scope(),
        object(),
        1,
        asyncio.get_running_loop().time(),
    )

    assert result == SyncRunResult(
        SyncRunStatus.BUDGET_EXHAUSTED,
        0,
        0,
        "sync.budget_exhausted",
    )
    assert not client.calls
    assert not pages
    assert not errors


@pytest.mark.asyncio
async def test_run_locked_reports_budget_exhaustion_at_the_page_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ScriptedSyncPageClient(
        _ordinary_sync_batch("cursor-2", includes_last=False)
    )
    coordinator, _ownership, _cursor, pages, errors = _run_locked_harness(
        monkeypatch,
        client,
    )

    result = await coordinator._run_locked(  # type: ignore[attr-defined]
        _SyncSessionLease(object()),
        8,
        _inbox_scope(),
        object(),
        1,
        asyncio.get_running_loop().time() + 5,
    )

    assert result == SyncRunResult(
        SyncRunStatus.BUDGET_EXHAUSTED,
        1,
        0,
        "sync.budget_exhausted",
    )
    assert len(client.calls) == 1
    assert len(pages) == 1
    assert not errors


class _ImmediateTimeout:
    async def __aenter__(self) -> None:
        raise TimeoutError

    async def __aexit__(self, *_args: object) -> None:
        return None


@pytest.mark.asyncio
async def test_run_locked_converts_page_timeout_to_budget_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sync_module.asyncio,
        "timeout",
        lambda _seconds: _ImmediateTimeout(),
    )
    client = _ScriptedSyncPageClient()
    coordinator, _ownership, _cursor, pages, errors = _run_locked_harness(
        monkeypatch,
        client,
    )

    result = await coordinator._run_locked(  # type: ignore[attr-defined]
        _SyncSessionLease(object()),
        8,
        _inbox_scope(),
        object(),
        1,
        asyncio.get_running_loop().time() + 5,
    )

    assert result == SyncRunResult(
        SyncRunStatus.BUDGET_EXHAUSTED,
        0,
        0,
        "sync.budget_exhausted",
    )
    assert not client.calls
    assert not pages
    assert not errors


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_deferred", [False, True], ids=["active", "deferred"])
async def test_preflight_returns_exact_active_or_deferred_cursor_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    retry_deferred: bool,
) -> None:
    _patch_direct_sync_transaction_dependencies(monkeypatch)
    connection = _RowsConnection(
        [
            {
                "cursor": "cursor-1",
                "status": "active",
                "version": 7,
                "transient_failures": 2,
                "retry_deferred": retry_deferred,
            }
        ]
    )
    coordinator = object.__new__(SyncCoordinator)

    result = await coordinator._preflight(  # type: ignore[attr-defined]
        _SyncSessionLease(connection),
        8,
        "INBOX",
    )

    if retry_deferred:
        assert result.cursor is None
        assert result.immediate_result == SyncRunResult(
            SyncRunStatus.RETRY_DEFERRED,
            0,
            0,
            "sync.retry_deferred",
        )
    else:
        assert result.immediate_result is None
        assert result.cursor.cursor == "cursor-1"
        assert result.cursor.version == 7
        assert result.cursor.transient_failures == 2


@pytest.mark.asyncio
async def test_expected_cursor_lock_accepts_only_the_exact_active_snapshot() -> None:
    coordinator = object.__new__(SyncCoordinator)
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")
    expected = cursor_snapshot("cursor-1", "active", 7, 2, False)

    await coordinator._lock_expected_cursor(  # type: ignore[attr-defined]
        _SingleRowConnection(("cursor-1", "active", 7, 2)),
        8,
        "INBOX",
        expected,
    )

    with pytest.raises(sync_module.StaleFence):
        await coordinator._lock_expected_cursor(  # type: ignore[attr-defined]
            _SingleRowConnection(("cursor-1", "active", 8, 2)),
            8,
            "INBOX",
            expected,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target",
    [SyncRunStatus.BLOCKED_CONTRACT, SyncRunStatus.RETRY_SCHEDULED],
    ids=["blocked-contract", "retry-scheduled"],
)
async def test_error_transition_commits_each_supported_nonreset_state_and_audit(
    monkeypatch: pytest.MonkeyPatch,
    target: SyncRunStatus,
) -> None:
    _patch_direct_sync_transaction_dependencies(monkeypatch)
    coordinator = object.__new__(SyncCoordinator)
    monkeypatch.setattr(coordinator, "_lock_expected_cursor", _noop_lock_expected)
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")
    connection = _RowsConnection([{"version": 8}, None])
    reason_code = (
        "exchange.sync.contract_invalid"
        if target is SyncRunStatus.BLOCKED_CONTRACT
        else "exchange.sync.transient_failure"
    )

    result = await coordinator._commit_error(  # type: ignore[attr-defined]
        _SyncSessionLease(connection),
        account_id=8,
        folder="INBOX",
        ownership=ownership_snapshot("pipeline-v2", 3, 9),
        expected=cursor_snapshot("cursor-1", "active", 7, 2, False),
        reason_code=reason_code,
        target=target,
        pages_committed=1,
        changes_observed=2,
        retry_after_seconds=37,
    )

    assert result == SyncRunResult(target, 1, 2, reason_code)
    assert len(connection.statements) == 2
    update_statement, update_params = connection.statements[0]
    audit_statement, audit_params = connection.statements[1]
    if target is SyncRunStatus.BLOCKED_CONTRACT:
        assert "status = 'blocked_contract'" in update_statement
        assert update_params[1] == SyncCoordinator._contract_fingerprint(
            8,
            "INBOX",
            reason_code,
        )
    else:
        assert "retry_after_at" in update_statement
        assert update_params[0] == 3
    assert "INSERT INTO public.audit_events" in audit_statement
    assert audit_params[5] == reason_code


class _RecordingInboxTransaction:
    def __init__(self) -> None:
        self.inserted: list[tuple[object, int, int]] = []

    async def insert(
        self,
        event: object,
        generation: int,
        fencing_token: int,
    ) -> None:
        self.inserted.append((event, generation, fencing_token))


class _RecordingInboxRepository:
    def __init__(self) -> None:
        self.transaction_boundary = _RecordingInboxTransaction()

    def transaction(self, _connection: object) -> _RecordingInboxTransaction:
        return self.transaction_boundary


@pytest.mark.asyncio
async def test_page_commit_inserts_events_before_advancing_the_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_direct_sync_transaction_dependencies(monkeypatch)
    coordinator = object.__new__(SyncCoordinator)
    inbox = _RecordingInboxRepository()
    coordinator._inbox_repository = inbox  # type: ignore[attr-defined]
    monkeypatch.setattr(coordinator, "_lock_expected_cursor", _noop_lock_expected)
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")
    expected = cursor_snapshot("cursor-1", "active", 7, 2, False)
    connection = _RowsConnection([{"version": 8}])
    event = object()

    result = await coordinator._commit_page(  # type: ignore[attr-defined]
        _SyncSessionLease(connection),
        account_id=8,
        folder="INBOX",
        ownership=ownership_snapshot("pipeline-v2", 3, 9),
        expected=expected,
        next_cursor="cursor-2",
        events=(event,),
    )

    assert result.cursor == "cursor-2"
    assert result.version == 8
    assert result.transient_failures == 0
    assert inbox.transaction_boundary.inserted == [(event, 3, 9)]


class _CoordinatorOutcomeRunner:
    def __init__(self, *, acquired: bool) -> None:
        self.acquired = acquired

    async def run(
        self,
        _account_id: int,
        _folder: str,
        operation: Callable[[object], object],
    ) -> object:
        outcome = getattr(sync_module, "_SyncSessionOutcome")
        if not self.acquired:
            return outcome(acquired=False, value=None)
        value = await operation(_SyncSessionLease(object()))  # type: ignore[misc]
        return outcome(acquired=True, value=value)


@pytest.mark.asyncio
@pytest.mark.parametrize("acquired", [False, True], ids=["busy", "acquired"])
async def test_run_folder_returns_busy_or_the_locked_sync_result(
    monkeypatch: pytest.MonkeyPatch,
    acquired: bool,
) -> None:
    scope = _inbox_scope()
    coordinator, provider, _permit = _coordinator_for_scope_test(
        PolicySnapshot(scopes=(scope,)),
    )
    coordinator._session_runner = _CoordinatorOutcomeRunner(  # type: ignore[assignment]
        acquired=acquired
    )
    locked_result = SyncRunResult(SyncRunStatus.CAUGHT_UP, 1, 2, None)

    async def run_locked(*_args: object) -> SyncRunResult:
        return locked_result

    monkeypatch.setattr(coordinator, "_run_locked", run_locked)

    result = await coordinator.run_folder(8, "INBOX")

    assert result == (
        locked_result
        if acquired
        else SyncRunResult(SyncRunStatus.BUSY_SKIP, 0, 0, "sync.busy")
    )
    assert provider.calls == [8]


@pytest.mark.asyncio
async def test_shared_ownership_reader_rejects_a_missing_current_generation() -> None:
    read_ownership = getattr(sync_module, "_read_current_ownership")

    with pytest.raises(sync_module.StaleFence):
        await read_ownership(_SingleRowConnection(None), 8)


def test_retry_hint_reads_only_exact_trusted_error_state() -> None:
    assert _trusted_retry_hint(object()) is None  # type: ignore[arg-type]
    assert _trusted_retry_hint(SyncTransientError()) is None
    corrupted = SyncTransientError(retry_after_seconds=1)
    vars(corrupted)["retry_after_seconds"] = 3601
    assert _trusted_retry_hint(corrupted) is None


def test_sync_batch_revalidation_rejects_mutated_exact_dtos() -> None:
    validate = getattr(sync_module, "_validated_sync_batch")

    invalid_contract = _ordinary_sync_batch("cursor-2", includes_last=True)
    object.__setattr__(invalid_contract, "contract_version", "wrong-contract")
    assert validate(invalid_contract, 100) is None

    invalid_member = _ordinary_sync_batch("cursor-2", includes_last=True)
    object.__setattr__(invalid_member, "changes", (object(),))
    assert validate(invalid_member, 100) is None

    invalid_reconstruction = _ordinary_sync_batch(
        "cursor-2",
        includes_last=True,
        email_id="email-1",
    )
    change = invalid_reconstruction.changes[0]
    object.__setattr__(change, "source_version", "\ud800")
    assert validate(invalid_reconstruction, 100) is None


@pytest.mark.asyncio
async def test_unlock_accepts_mapping_success_and_rejects_missing_column() -> None:
    runner = object.__new__(_SyncSessionRunner)

    assert await runner._unlock(  # type: ignore[attr-defined]
        _SingleRowConnection({"released": True}),
        (1, 2),
    )
    assert not await runner._unlock(  # type: ignore[attr-defined]
        _SingleRowConnection({"unexpected": True}),
        (1, 2),
    )


async def _raise_sync_operational_error(*_args: object, **_kwargs: object) -> object:
    raise sync_module.psycopg.OperationalError("database-secret-must-not-escape")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    ["preflight", "commit-error", "commit-page"],
)
async def test_sync_transactions_normalize_psycopg_failures(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    monkeypatch.setattr(
        sync_module,
        "_caller_owned_transaction",
        _raise_sync_operational_error,
    )
    coordinator = object.__new__(SyncCoordinator)
    ownership_snapshot = getattr(sync_module, "_OwnershipSnapshot")(
        "pipeline-v2",
        3,
        9,
    )
    cursor_snapshot = getattr(sync_module, "_CursorSnapshot")(
        "cursor-1",
        "active",
        7,
        0,
        False,
    )

    with pytest.raises(DatabaseOperationError) as caught:
        if operation == "preflight":
            await coordinator._preflight(  # type: ignore[attr-defined]
                _SyncSessionLease(object()),
                8,
                "INBOX",
            )
        elif operation == "commit-error":
            await coordinator._commit_error(  # type: ignore[attr-defined]
                _SyncSessionLease(object()),
                account_id=8,
                folder="INBOX",
                ownership=ownership_snapshot,
                expected=cursor_snapshot,
                reason_code="exchange.sync.cursor_invalid",
                target=SyncRunStatus.RESET_REQUIRED,
                pages_committed=0,
                changes_observed=0,
            )
        else:
            await coordinator._commit_page(  # type: ignore[attr-defined]
                _SyncSessionLease(object()),
                account_id=8,
                folder="INBOX",
                ownership=ownership_snapshot,
                expected=cursor_snapshot,
                next_cursor="cursor-2",
                events=(),
            )

    assert caught.value.operation == {
        "preflight": "sync_cursor_preflight",
        "commit-error": "sync_error_transition",
        "commit-page": "sync_page_commit",
    }[operation]
    assert "database-secret" not in str(caught.value)
