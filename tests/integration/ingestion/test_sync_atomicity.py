from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg.conninfo import make_conninfo
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.domain.errors import (
    DatabaseOperationError,
    StaleFence,
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
from src.ingestion.ownership import PipelineOwnershipRepository
from src.ingestion.policy import FolderScope, PolicySnapshot, ProcessingPolicyResolver
from src.ingestion.repository import InboxRepository
from src.ingestion.sync import (
    FolderPermitLease,
    SyncCoordinator,
    SyncRunStatus,
    sync_advisory_lock_keys,
)
import src.ingestion.sync as sync_module


class _NeverPool:
    def connection(self) -> object:
        raise AssertionError("Sync Inbox writes must use the caller-owned connection")


class _PermitProvider:
    def __init__(self) -> None:
        self.active = False
        self.release_count = 0
        self.acquire_count = 0

    async def try_acquire(
        self,
        _account_id: int,
        _canonical_folder: str,
    ) -> FolderPermitLease | None:
        self.acquire_count += 1
        if self.active:
            return None
        self.active = True
        return FolderPermitLease(self._release)

    def _release(self) -> None:
        self.active = False
        self.release_count += 1


class _TrackingPermitProvider(_PermitProvider):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    def _release(self) -> None:
        self._events.append("permit.release")
        super()._release()


class _SnapshotProvider:
    def __init__(self, snapshot: PolicySnapshot) -> None:
        self.snapshot = snapshot

    async def get_ready_snapshot(self, _account_id: int) -> PolicySnapshot:
        return self.snapshot


class _PageClient:
    def __init__(self) -> None:
        self.outcomes: list[object] = []
        self.calls: list[tuple[int, str, str, int]] = []
        self.on_call: Callable[[int], Awaitable[None]] | None = None

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> object:
        self.calls.append((account_id, folder, cursor, limit))
        if self.on_call is not None:
            await self.on_call(len(self.calls))
        if not self.outcomes:
            raise AssertionError("unexpected Exchange request")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _ExitFaultInfo:
    def __init__(self, connection: _ExitFaultConnection) -> None:
        self._connection = connection

    @property
    def transaction_status(self) -> TransactionStatus:
        override = self._connection.transaction_status_override
        if override is not None:
            return override
        return self._connection.raw.info.transaction_status


class _ExitFaultTransaction:
    def __init__(self, connection: _ExitFaultConnection, transaction: Any) -> None:
        self._connection = connection
        self._transaction = transaction

    async def __aenter__(self) -> object:
        return await self._transaction.__aenter__()

    async def __aexit__(
        self,
        error_type: object,
        error: object,
        traceback: object,
    ) -> object:
        result = await self._transaction.__aexit__(error_type, error, traceback)
        self._connection.transaction_exit_count += 1
        if (
            self._connection.transaction_exit_count
            == self._connection.fault_exit_number
        ):
            self._connection.transaction_status_override = self._connection.fault_status
        return result


class _ExitFaultConnection:
    def __init__(
        self,
        raw: Any,
        *,
        fault_exit_number: int,
        fault_status: TransactionStatus,
    ) -> None:
        self.raw = raw
        self.fault_exit_number = fault_exit_number
        self.fault_status = fault_status
        self.transaction_exit_count = 0
        self.transaction_status_override: TransactionStatus | None = None
        self.info = _ExitFaultInfo(self)

    @property
    def autocommit(self) -> object:
        return self.raw.autocommit

    @property
    def closed(self) -> object:
        return self.raw.closed

    @property
    def pgconn(self) -> object:
        return self.raw.pgconn

    async def execute(self, *args: object, **kwargs: object) -> object:
        return await self.raw.execute(*args, **kwargs)

    def transaction(self) -> _ExitFaultTransaction:
        return _ExitFaultTransaction(self, self.raw.transaction())

    async def close(self) -> None:
        await self.raw.close()


class _ExitFaultPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        fault_exit_number: int,
        fault_status: TransactionStatus,
    ) -> None:
        self._pool = pool
        self._fault_exit_number = fault_exit_number
        self._fault_status = fault_status
        self.kwargs = pool.kwargs
        self.close_returns = pool.close_returns
        self.checked_out: list[_ExitFaultConnection] = []
        self.returned: list[_ExitFaultConnection] = []

    async def getconn(self) -> _ExitFaultConnection:
        connection = _ExitFaultConnection(
            await self._pool.getconn(),
            fault_exit_number=self._fault_exit_number,
            fault_status=self._fault_status,
        )
        self.checked_out.append(connection)
        return connection

    async def putconn(self, connection: _ExitFaultConnection) -> None:
        self.returned.append(connection)
        await self._pool.putconn(connection.raw)


class _ExecuteThenFailConnection:
    def __init__(self, raw: Any, marker: str, error: BaseException) -> None:
        self.raw = raw
        self._marker = marker
        self._error = error

    @property
    def autocommit(self) -> object:
        return self.raw.autocommit

    @property
    def closed(self) -> object:
        return self.raw.closed

    @property
    def info(self) -> object:
        return self.raw.info

    @property
    def pgconn(self) -> object:
        return self.raw.pgconn

    async def execute(
        self, statement: object, *args: object, **kwargs: object
    ) -> object:
        result = await self.raw.execute(statement, *args, **kwargs)
        if isinstance(statement, str) and self._marker in statement:
            raise self._error
        return result

    def transaction(self) -> object:
        return self.raw.transaction()

    async def close(self) -> None:
        await self.raw.close()


class _ExecuteThenFailPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        marker: str,
        error: BaseException,
    ) -> None:
        self._pool = pool
        self._marker = marker
        self._error = error
        self.kwargs = pool.kwargs
        self.close_returns = pool.close_returns

    async def getconn(self) -> _ExecuteThenFailConnection:
        return _ExecuteThenFailConnection(
            await self._pool.getconn(),
            self._marker,
            self._error,
        )

    async def putconn(self, connection: _ExecuteThenFailConnection) -> None:
        await self._pool.putconn(connection.raw)


class _DelayExecuteConnection:
    def __init__(self, raw: Any, marker: str, delay_seconds: float) -> None:
        self.raw = raw
        self._marker = marker
        self._delay_seconds = delay_seconds

    @property
    def autocommit(self) -> object:
        return self.raw.autocommit

    @property
    def closed(self) -> object:
        return self.raw.closed

    @property
    def info(self) -> object:
        return self.raw.info

    @property
    def pgconn(self) -> object:
        return self.raw.pgconn

    async def execute(
        self, statement: object, *args: object, **kwargs: object
    ) -> object:
        if isinstance(statement, str) and self._marker in statement:
            await asyncio.sleep(self._delay_seconds)
        return await self.raw.execute(statement, *args, **kwargs)

    def transaction(self) -> object:
        return self.raw.transaction()

    async def close(self) -> None:
        await self.raw.close()


class _DelayExecutePool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        marker: str,
        delay_seconds: float,
    ) -> None:
        self._pool = pool
        self._marker = marker
        self._delay_seconds = delay_seconds
        self.kwargs = pool.kwargs
        self.close_returns = pool.close_returns

    async def getconn(self) -> _DelayExecuteConnection:
        return _DelayExecuteConnection(
            await self._pool.getconn(),
            self._marker,
            self._delay_seconds,
        )

    async def putconn(self, connection: _DelayExecuteConnection) -> None:
        await self._pool.putconn(connection.raw)


class _AckLossConnection:
    def __init__(
        self,
        raw: Any,
        *,
        marker: str,
        error: BaseException,
        events: list[str],
    ) -> None:
        self.raw = raw
        self._marker = marker
        self._error = error
        self._events = events

    @property
    def autocommit(self) -> object:
        return self.raw.autocommit

    @property
    def closed(self) -> object:
        return self.raw.closed

    @property
    def info(self) -> object:
        return self.raw.info

    @property
    def pgconn(self) -> object:
        return self.raw.pgconn

    async def execute(
        self, statement: object, *args: object, **kwargs: object
    ) -> object:
        result = await self.raw.execute(statement, *args, **kwargs)
        if isinstance(statement, str) and self._marker in statement:
            self._events.append("sql.executed_then_lost")
            raise self._error
        return result

    def transaction(self) -> object:
        return self.raw.transaction()

    async def close(self) -> None:
        self._events.append("connection.close")
        await self.raw.close()


class _AckLossPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        marker: str,
        error: BaseException,
        events: list[str],
    ) -> None:
        self._pool = pool
        self._marker = marker
        self._error = error
        self._events = events
        self.kwargs = pool.kwargs
        self.close_returns = pool.close_returns
        self.backend_pid: int | None = None
        self.returned_closed: bool | None = None

    async def getconn(self) -> _AckLossConnection:
        raw = await self._pool.getconn()
        self.backend_pid = raw.info.backend_pid
        return _AckLossConnection(
            raw,
            marker=self._marker,
            error=self._error,
            events=self._events,
        )

    async def putconn(self, connection: _AckLossConnection) -> None:
        self._events.append("pool.putconn")
        self.returned_closed = connection.closed is True
        await self._pool.putconn(connection.raw)


class _CommitAcknowledgementLost(RuntimeError):
    pass


class _CommitAckLossTransaction:
    def __init__(
        self,
        connection: _CommitAckLossConnection,
        transaction: Any,
    ) -> None:
        self._connection = connection
        self._transaction = transaction

    async def __aenter__(self) -> object:
        return await self._transaction.__aenter__()

    async def __aexit__(
        self,
        error_type: object,
        error: object,
        traceback: object,
    ) -> object:
        result = await self._transaction.__aexit__(error_type, error, traceback)
        self._connection.transaction_exit_count += 1
        if (
            error_type is None
            and self._connection.transaction_exit_count
            == self._connection.fault_exit_number
        ):
            self._connection.events.append("commit.executed_then_ack_lost")
            raise _CommitAcknowledgementLost("commit acknowledgement lost")
        return result


class _CommitAckLossConnection:
    def __init__(
        self,
        raw: Any,
        *,
        fault_exit_number: int,
        events: list[str],
    ) -> None:
        self.raw = raw
        self.fault_exit_number = fault_exit_number
        self.events = events
        self.transaction_exit_count = 0

    @property
    def autocommit(self) -> object:
        return self.raw.autocommit

    @property
    def closed(self) -> object:
        return self.raw.closed

    @property
    def info(self) -> object:
        return self.raw.info

    @property
    def pgconn(self) -> object:
        return self.raw.pgconn

    async def execute(
        self, statement: object, *args: object, **kwargs: object
    ) -> object:
        if isinstance(statement, str) and "pg_advisory_unlock" in statement:
            self.events.append("db.unlock")
        return await self.raw.execute(statement, *args, **kwargs)

    def transaction(self) -> _CommitAckLossTransaction:
        return _CommitAckLossTransaction(self, self.raw.transaction())

    async def close(self) -> None:
        self.events.append("connection.close")
        await self.raw.close()


class _CommitAckLossPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        fault_exit_number: int,
        events: list[str],
    ) -> None:
        self._pool = pool
        self._fault_exit_number = fault_exit_number
        self._events = events
        self.kwargs = pool.kwargs
        self.close_returns = pool.close_returns
        self.backend_pid: int | None = None
        self.returned_closed: bool | None = None

    async def getconn(self) -> _CommitAckLossConnection:
        raw = await self._pool.getconn()
        self.backend_pid = raw.info.backend_pid
        return _CommitAckLossConnection(
            raw,
            fault_exit_number=self._fault_exit_number,
            events=self._events,
        )

    async def putconn(self, connection: _CommitAckLossConnection) -> None:
        self._events.append("pool.putconn")
        self.returned_closed = connection.closed is True
        await self._pool.putconn(connection.raw)


def _matrix() -> dict[tuple[IngressSource, str, ChangeKind], ProcessingPolicy]:
    return {
        (
            IngressSource.WEBHOOK,
            "NewMailEvent",
            ChangeKind.CREATE,
        ): ProcessingPolicy.FULL,
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
        (IngressSource.SYNC, "create", ChangeKind.CREATE): ProcessingPolicy.FULL,
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


def _snapshot() -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("opaque-inbox-id",),
                sync_folder="Inbox",
                event_policy_matrix=_matrix(),
            ),
        ),
    )


def _multi_folder_snapshot() -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            _snapshot().scopes[0],
            FolderScope.configured(
                canonical_key="PROJECTS",
                webhook_ids=("opaque-projects-id",),
                sync_folder="Projects",
                event_policy_matrix=_matrix(),
            ),
        ),
    )


def _batch(
    cursor: str,
    *,
    includes_last: bool,
    changes: tuple[SyncChange, ...] = (),
) -> SyncBatch:
    return SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor=cursor,
        changes=changes,
        includes_last=includes_last,
    )


def _create_change(
    external_id: str,
    *,
    source_version: str = "version-1",
) -> SyncChange:
    return SyncChange(
        kind=ChangeKind.CREATE,
        external_email_id=external_id,
        item={"id": external_id, "subject": "safe subject"},
        source_version=source_version,
    )


@dataclass(slots=True)
class _Runtime:
    schema: Any
    pool: AsyncConnectionPool
    coordinator: SyncCoordinator
    client: _PageClient
    permit: _PermitProvider
    application_name: str


@pytest_asyncio.fixture
async def sync_runtime(postgres_database_factory) -> _Runtime:
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    application_name = f"sync-test-{schema.database_name[-12:]}"
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            schema.runtime_dsn,
            application_name=application_name,
        ),
        min_size=1,
        max_size=3,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    await PipelineOwnershipRepository(pool).bootstrap(8, "durable_v1")
    client = _PageClient()
    permit = _PermitProvider()
    coordinator = SyncCoordinator(
        page_client=client,
        snapshot_provider=_SnapshotProvider(_snapshot()),
        policy_resolver=ProcessingPolicyResolver(),
        folder_permit=permit,
        sync_pool=pool,
        inbox_repository=InboxRepository(_NeverPool()),
        page_limit=100,
        default_max_pages=4,
        default_max_run_seconds=30.0,
        cleanup_timeout=1.0,
    )
    try:
        yield _Runtime(
            schema,
            pool,
            coordinator,
            client,
            permit,
            application_name,
        )
    finally:
        await pool.close()


def _seed_active(runtime: _Runtime, cursor: str = "cursor-1") -> None:
    runtime.schema.runtime_execute(
        "INSERT INTO sync_cursors ("
        "account_id, folder_key, cursor, status, last_success_at"
        ") VALUES (8, 'INBOX', %s, 'active', CURRENT_TIMESTAMP)",
        (cursor,),
    )


def _seed_active_folder(
    runtime: _Runtime,
    folder: str,
    cursor: str,
) -> None:
    runtime.schema.runtime_execute(
        "INSERT INTO sync_cursors ("
        "account_id, folder_key, cursor, status, last_success_at"
        ") VALUES (8, %s, %s, 'active', CURRENT_TIMESTAMP)",
        (folder, cursor),
    )


def _seed_no_http_cursor(runtime: _Runtime, case: str) -> None:
    if case == "cold_start_pending":
        runtime.schema.maintenance_execute(
            "INSERT INTO sync_cursors ("
            "account_id, folder_key, cursor, status, blocked_reason_code"
            ") VALUES (8, 'INBOX', NULL, 'cold_start_pending', "
            "'sync.cold_start_required')"
        )
        return
    if case == "reset_required":
        runtime.schema.maintenance_execute(
            "INSERT INTO sync_cursors ("
            "account_id, folder_key, cursor, status, blocked_reason_code, "
            "last_attempt_at) VALUES (8, 'INBOX', 'cursor-1', "
            "'reset_required', 'sync.cursor_reset_required', "
            "CURRENT_TIMESTAMP)"
        )
        return
    if case == "blocked_contract":
        runtime.schema.maintenance_execute(
            "INSERT INTO sync_cursors ("
            "account_id, folder_key, cursor, status, blocked_reason_code, "
            "contract_fingerprint, blocked_at) VALUES (8, 'INBOX', "
            "'cursor-1', 'blocked_contract', 'sync.local_contract_invalid', "
            "%s, CURRENT_TIMESTAMP)",
            ("a" * 64,),
        )
        return
    if case == "retry_deferred":
        runtime.schema.maintenance_execute(
            "INSERT INTO sync_cursors ("
            "account_id, folder_key, cursor, status, last_success_at, "
            "last_attempt_at, transient_failures, retry_after_at) VALUES ("
            "8, 'INBOX', 'cursor-1', 'active', CURRENT_TIMESTAMP, "
            "CURRENT_TIMESTAMP, 1, CURRENT_TIMESTAMP + interval '1 hour')"
        )
        return
    if case != "cold_start_applying":
        raise AssertionError("unknown no-HTTP cursor case")

    plan_id = str(uuid4())
    with psycopg.connect(runtime.schema.maintenance_dsn) as connection:
        with connection.transaction():
            connection.execute(
                "INSERT INTO sync_cold_start_plans ("
                "plan_id, account_id, folder_key, expected_cursor_status, "
                "expected_cursor_version, pipeline_name, generation, "
                "fencing_token, state, preview_cursor, preview_cursor_version, "
                "boundary_cursor, boundary_cursor_version, apply_cursor, "
                "apply_cursor_version, rolling_hash, page_count, item_count, "
                "contract_fingerprint, folder_scope_config_hash, plan_hash, "
                "actor, reason, expires_at, ready_at, approved_at"
                ") VALUES (%s, 8, 'INBOX', 'cold_start_pending', 0, "
                "'durable_v1', 1, 1, 'approved', 'apply-1', 1, 'apply-1', 1, "
                "'apply-1', 0, %s, 1, 1, %s, %s, %s, "
                "'sync-integration', 'approved fixture', "
                "CURRENT_TIMESTAMP + interval '1 hour', CURRENT_TIMESTAMP, "
                "CURRENT_TIMESTAMP)",
                (plan_id, "b" * 64, "c" * 64, "d" * 64, "e" * 64),
            )
            connection.execute(
                "INSERT INTO sync_cursors ("
                "account_id, folder_key, cursor, status, last_success_at, "
                "last_attempt_at, cold_start_plan_id, cold_start_plan_state"
                ") VALUES (8, 'INBOX', 'apply-1', 'cold_start_applying', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, 'approved')",
                (plan_id,),
            )


def _cursor_row(runtime: _Runtime) -> dict[str, Any] | None:
    with psycopg.connect(
        runtime.schema.runtime_dsn, row_factory=dict_row
    ) as connection:
        return connection.execute(
            "SELECT cursor, status, blocked_reason_code, contract_fingerprint, "
            "version, last_success_at, last_attempt_at, transient_failures, "
            "retry_after_at FROM sync_cursors "
            "WHERE account_id = 8 AND folder_key = 'INBOX'"
        ).fetchone()


def _cursor_row_for_folder(runtime: _Runtime, folder: str) -> dict[str, Any] | None:
    with psycopg.connect(
        runtime.schema.runtime_dsn,
        row_factory=dict_row,
    ) as connection:
        return connection.execute(
            "SELECT cursor, status, version, transient_failures, retry_after_at "
            "FROM sync_cursors WHERE account_id = 8 AND folder_key = %s",
            (folder,),
        ).fetchone()


def _count(runtime: _Runtime, relation: str) -> int:
    if relation not in {"event_inbox", "audit_events"}:
        raise AssertionError("unapproved test relation")
    return int(runtime.schema.scalar(f"SELECT pg_catalog.count(*) FROM {relation}"))


def _sync_error_count(runtime: _Runtime) -> int:
    return int(
        runtime.schema.scalar(
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE object_type = 'sync_cursor' AND action = 'sync.error'"
        )
    )


def _sync_error_text(runtime: _Runtime) -> str:
    return str(
        runtime.schema.scalar(
            "SELECT COALESCE(pg_catalog.string_agg("
            "pg_catalog.concat_ws('|', action, result, actor, reason, "
            "safe_metadata::pg_catalog.text), '|'), '') "
            "FROM audit_events WHERE object_type = 'sync_cursor' "
            "AND action = 'sync.error'"
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_cursor_creates_cold_start_pending_without_exchange(
    sync_runtime: _Runtime,
) -> None:
    first = await sync_runtime.coordinator.run_folder(8, "INBOX")
    second = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert first.status is SyncRunStatus.COLD_START_PENDING
    assert second.status is SyncRunStatus.COLD_START_PENDING
    assert first.pages_committed == second.pages_committed == 0
    assert sync_runtime.client.calls == []
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] is None
    assert row["status"] == "cold_start_pending"
    assert row["version"] == 0
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 0
    assert sync_runtime.permit.release_count == 2


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("account_id", "ownership_case"),
    [(9, "missing"), (8, "quiescing")],
    ids=["missing", "quiescing"],
)
async def test_missing_or_noncurrent_ownership_fails_before_cursor_or_exchange(
    sync_runtime: _Runtime,
    account_id: int,
    ownership_case: str,
) -> None:
    if ownership_case == "quiescing":
        sync_runtime.schema.execute(
            "UPDATE pipeline_ownership SET state = 'quiescing' "
            "WHERE account_id = 8 AND state = 'current_ingress'"
        )
    elif ownership_case != "missing":
        raise AssertionError("unknown ownership case")

    with pytest.raises(StaleFence):
        await sync_runtime.coordinator.run_folder(account_id, "INBOX")

    assert sync_runtime.client.calls == []
    assert (
        sync_runtime.schema.scalar(
            "SELECT pg_catalog.count(*) FROM sync_cursors WHERE account_id = %s",
            (account_id,),
        )
        == 0
    )
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 0
    assert sync_runtime.permit.release_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected_status", "expected_safe_code"),
    [
        (
            "cold_start_pending",
            SyncRunStatus.COLD_START_PENDING,
            "sync.cold_start_required",
        ),
        (
            "reset_required",
            SyncRunStatus.RESET_REQUIRED,
            "sync.cursor_reset_required",
        ),
        (
            "blocked_contract",
            SyncRunStatus.BLOCKED_CONTRACT,
            "sync.blocked_contract",
        ),
        (
            "cold_start_applying",
            SyncRunStatus.COLD_START_APPLYING,
            "sync.cold_start_applying",
        ),
        (
            "retry_deferred",
            SyncRunStatus.RETRY_DEFERRED,
            "sync.retry_deferred",
        ),
    ],
)
async def test_existing_terminal_or_deferred_cursor_state_never_calls_exchange(
    sync_runtime: _Runtime,
    case: str,
    expected_status: SyncRunStatus,
    expected_safe_code: str,
) -> None:
    _seed_no_http_cursor(sync_runtime, case)

    result = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is expected_status
    assert result.safe_code == expected_safe_code
    assert result.pages_committed == 0
    assert result.changes_observed == 0
    assert sync_runtime.client.calls == []
    assert _sync_error_count(sync_runtime) == 0
    assert sync_runtime.permit.release_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_http_runs_outside_a_transaction_and_pages_commit_atomically(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch("cursor-2", includes_last=False),
        _batch(
            "cursor-3",
            includes_last=True,
            changes=(_create_change("message-1"),),
        ),
    ]

    async def assert_no_xid(_call_number: int) -> None:
        async with await psycopg.AsyncConnection.connect(
            sync_runtime.schema.dsn,
            autocommit=True,
        ) as probe:
            cursor = await probe.execute(
                "SELECT pg_catalog.bool_and(xact_start IS NULL) "
                "FROM pg_catalog.pg_stat_activity "
                "WHERE datname = pg_catalog.current_database() "
                "AND application_name = %s",
                (sync_runtime.application_name,),
            )
            row = await cursor.fetchone()
        assert row == (True,)

    sync_runtime.client.on_call = assert_no_xid
    result = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is SyncRunStatus.CAUGHT_UP
    assert result.pages_committed == 2
    assert result.changes_observed == 1
    assert sync_runtime.client.calls == [
        (8, "Inbox", "cursor-1", 100),
        (8, "Inbox", "cursor-2", 100),
    ]
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-3"
    assert row["status"] == "active"
    assert row["version"] == 2
    assert row["transient_failures"] == 0
    assert row["retry_after_at"] is None
    assert _count(sync_runtime, "event_inbox") == 1
    assert sync_runtime.permit.release_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_page_sync_preserves_opaque_cursor_event_policy_and_dedupe_identity(
    sync_runtime: _Runtime,
) -> None:
    seed_cursor = "opaque:seed/segment?token=%2F"
    first_cursor = "opaque:A/segment?token=%2F"
    terminal_cursor = "opaque:B/segment?token=%2F"
    repeated_create = _create_change("message-create")
    update = SyncChange(
        kind=ChangeKind.UPDATE,
        external_email_id="message-update",
        item={"id": "message-update", "subject": "updated subject"},
        source_version="update-version-1",
    )
    delete = SyncChange(
        kind=ChangeKind.DELETE,
        external_email_id="message-delete",
        item=None,
        source_version="delete-version-1",
    )
    _seed_active(sync_runtime, seed_cursor)
    sync_runtime.client.outcomes = [
        _batch(
            first_cursor,
            includes_last=False,
            changes=(repeated_create, update, delete),
        ),
        _batch(
            terminal_cursor,
            includes_last=True,
            changes=(repeated_create,),
        ),
    ]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is SyncRunStatus.CAUGHT_UP
    assert result.pages_committed == 2
    assert result.changes_observed == 4
    assert [call[2] for call in sync_runtime.client.calls] == [
        seed_cursor,
        first_cursor,
    ]
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == terminal_cursor
    assert row["version"] == 2
    with psycopg.connect(
        sync_runtime.schema.maintenance_dsn,
        row_factory=dict_row,
    ) as connection:
        inbox_rows = connection.execute(
            "SELECT source, raw_event_type, change_kind, processing_policy, "
            "payload ->> 'cursor' AS cursor, external_email_id, dedupe_key "
            "FROM event_inbox ORDER BY cursor, change_kind, external_email_id"
        ).fetchall()

    assert [
        (
            inbox_row["cursor"],
            inbox_row["source"],
            inbox_row["raw_event_type"],
            inbox_row["change_kind"],
            inbox_row["processing_policy"],
        )
        for inbox_row in inbox_rows
    ] == [
        (first_cursor, "sync", "create", "create", "full"),
        (first_cursor, "sync", "delete", "delete", "metadata_only"),
        (first_cursor, "sync", "update", "update", "metadata_only"),
        (terminal_cursor, "sync", "create", "create", "full"),
    ]
    create_rows = [
        inbox_row
        for inbox_row in inbox_rows
        if inbox_row["external_email_id"] == "message-create"
    ]
    assert len(create_rows) == 2
    assert create_rows[0]["dedupe_key"] != create_rows[1]["dedupe_key"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_same_folder_uses_postgres_session_lock_across_independent_permits(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    first_client = _PageClient()
    second_client = _PageClient()
    first_client.outcomes = [_batch("cursor-2", includes_last=True)]
    second_client.outcomes = [_batch("cursor-never", includes_last=True)]
    entered_http = asyncio.Event()
    release_http = asyncio.Event()

    async def block_first_http(_call_number: int) -> None:
        entered_http.set()
        await release_http.wait()

    first_client.on_call = block_first_http
    first_permit = _PermitProvider()
    second_permit = _PermitProvider()
    first = _coordinator_with_components(
        sync_runtime,
        client=first_client,
        permit=first_permit,
        snapshot=_snapshot(),
    )
    second = _coordinator_with_components(
        sync_runtime,
        client=second_client,
        permit=second_permit,
        snapshot=_snapshot(),
    )

    first_task = asyncio.create_task(first.run_folder(8, "INBOX"))
    await asyncio.wait_for(entered_http.wait(), timeout=2.0)
    try:
        busy = await second.run_folder(8, "INBOX")
    finally:
        release_http.set()
    completed = await first_task

    assert busy.status is SyncRunStatus.BUSY_SKIP
    assert second_client.calls == []
    assert second_permit.acquire_count == 1
    assert second_permit.release_count == 1
    assert completed.status is SyncRunStatus.CAUGHT_UP
    assert len(first_client.calls) == 1
    assert first_permit.release_count == 1
    assert await _probe_sync_lock(sync_runtime, "INBOX") is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_different_folder_session_locks_allow_concurrent_http(
    sync_runtime: _Runtime,
) -> None:
    _seed_active_folder(sync_runtime, "INBOX", "inbox-cursor-1")
    _seed_active_folder(sync_runtime, "PROJECTS", "projects-cursor-1")
    inbox_client = _PageClient()
    projects_client = _PageClient()
    inbox_client.outcomes = [_batch("inbox-cursor-2", includes_last=True)]
    projects_client.outcomes = [_batch("projects-cursor-2", includes_last=True)]
    inbox_entered = asyncio.Event()
    projects_entered = asyncio.Event()
    release_http = asyncio.Event()

    async def block_inbox(_call_number: int) -> None:
        inbox_entered.set()
        await release_http.wait()

    async def block_projects(_call_number: int) -> None:
        projects_entered.set()
        await release_http.wait()

    inbox_client.on_call = block_inbox
    projects_client.on_call = block_projects
    inbox = _coordinator_with_components(
        sync_runtime,
        client=inbox_client,
        permit=_PermitProvider(),
        snapshot=_multi_folder_snapshot(),
    )
    projects = _coordinator_with_components(
        sync_runtime,
        client=projects_client,
        permit=_PermitProvider(),
        snapshot=_multi_folder_snapshot(),
    )

    inbox_task = asyncio.create_task(inbox.run_folder(8, "INBOX"))
    projects_task = asyncio.create_task(projects.run_folder(8, "PROJECTS"))
    try:
        await asyncio.wait_for(
            asyncio.gather(inbox_entered.wait(), projects_entered.wait()),
            timeout=2.0,
        )
    finally:
        release_http.set()
    inbox_result, projects_result = await asyncio.gather(
        inbox_task,
        projects_task,
    )

    assert inbox_result.status is SyncRunStatus.CAUGHT_UP
    assert projects_result.status is SyncRunStatus.CAUGHT_UP
    assert len(inbox_client.calls) == len(projects_client.calls) == 1
    assert _cursor_row_for_folder(sync_runtime, "INBOX")["cursor"] == (  # type: ignore[index]
        "inbox-cursor-2"
    )
    assert _cursor_row_for_folder(sync_runtime, "PROJECTS")["cursor"] == (  # type: ignore[index]
        "projects-cursor-2"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_external_cancellation_releases_real_session_lock_and_permit(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    cancelled = asyncio.Event()
    entered_http = asyncio.Event()
    sync_runtime.client.outcomes = [_batch("cursor-never", includes_last=True)]

    async def block_http(_call_number: int) -> None:
        entered_http.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    sync_runtime.client.on_call = block_http
    task = asyncio.create_task(sync_runtime.coordinator.run_folder(8, "INBOX"))
    await asyncio.wait_for(entered_http.wait(), timeout=2.0)
    held_pids = _sync_lock_holder_pids(sync_runtime, "INBOX")
    assert len(held_pids) == 1
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert cancelled.is_set()
    assert sync_runtime.permit.active is False
    assert sync_runtime.permit.release_count == 1
    assert await _probe_sync_lock(sync_runtime, "INBOX") is True
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_busy_return_cancel_finishes_real_pool_handoff_without_ghost_capacity(
    sync_runtime: _Runtime,
) -> None:
    pool_application_name = f"g10-sync-return-{sync_runtime.schema.database_name[-8:]}"
    raw_pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            sync_runtime.schema.runtime_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await raw_pool.open()

    class ReturnPool:
        kwargs = raw_pool.kwargs
        close_returns = raw_pool.close_returns

        def __init__(self) -> None:
            self.connection: Any | None = None
            self.checkout_entered = asyncio.Event()
            self.allow_checkout = asyncio.Event()
            self.return_entered = asyncio.Event()
            self.inner_cancellations: list[asyncio.CancelledError] = []
            self.return_completions: list[int] = []

        async def getconn(self) -> Any:
            self.connection = await raw_pool.getconn()
            self.checkout_entered.set()
            await self.allow_checkout.wait()
            return self.connection

        async def putconn(self, connection: Any) -> None:
            assert connection is self.connection
            self.return_entered.set()
            try:
                await raw_pool.putconn(connection)
            except asyncio.CancelledError as error:
                self.inner_cancellations.append(error)
                raise
            self.return_completions.append(connection.info.backend_pid)

    guarded_pool = ReturnPool()
    permit = _PermitProvider()
    runner = sync_module._SyncSessionRunner(
        pool=guarded_pool,
        permit=permit,
        cleanup_timeout=1.0,
    )
    lock_keys = sync_advisory_lock_keys(8, "INBOX")
    lock_owner = await psycopg.AsyncConnection.connect(
        sync_runtime.schema.runtime_dsn,
        autocommit=True,
        row_factory=dict_row,
    )
    await lock_owner.execute(
        "SELECT pg_catalog.pg_advisory_lock(%s, %s)",
        lock_keys,
    )
    release_pool_lock = asyncio.Event()
    pool_lock_held = asyncio.Event()
    borrowed_pids: list[int] = []
    task: asyncio.Task[Any] | None = None
    borrower: asyncio.Task[int] | None = None
    lock_holder: asyncio.Task[None] | None = None

    async def forbidden(_session: object) -> None:
        raise AssertionError("busy runner must not execute its operation")

    async def competing_borrower() -> int:
        connection = await raw_pool.getconn(timeout=3.0)
        backend_pid = connection.info.backend_pid
        assert type(backend_pid) is int
        borrowed_pids.append(backend_pid)
        try:
            cursor = await connection.execute("SELECT 1 AS healthy")
            assert await cursor.fetchone() == {"healthy": 1}
        finally:
            await raw_pool.putconn(connection)
        return backend_pid

    async def hold_pool_lock() -> None:
        async with raw_pool._lock:
            pool_lock_held.set()
            await release_pool_lock.wait()

    try:
        task = asyncio.create_task(runner.run(8, "INBOX", forbidden))
        await asyncio.wait_for(guarded_pool.checkout_entered.wait(), timeout=2.0)
        assert guarded_pool.connection is not None
        old_pid = guarded_pool.connection.info.backend_pid
        assert type(old_pid) is int

        borrower = asyncio.create_task(competing_borrower())
        for _ in range(100):
            if raw_pool.get_stats()["requests_waiting"] == 1:
                break
            await asyncio.sleep(0)
        assert raw_pool.get_stats()["requests_waiting"] == 1

        lock_holder = asyncio.create_task(hold_pool_lock())
        await asyncio.wait_for(pool_lock_held.wait(), timeout=2.0)
        guarded_pool.allow_checkout.set()
        await asyncio.wait_for(guarded_pool.return_entered.wait(), timeout=2.0)
        for _ in range(100):
            if guarded_pool.connection._pool is None:
                break
            await asyncio.sleep(0)
        assert guarded_pool.connection._pool is None
        assert guarded_pool.connection.closed is False
        assert guarded_pool.connection.info.transaction_status is TransactionStatus.IDLE
        assert borrowed_pids == []
        assert permit.acquire_count == 1
        assert permit.release_count == 0
        assert permit.active is True

        assert task.cancel("g10 busy return cancellation") is True
        for _ in range(100):
            await asyncio.sleep(0)
            if guarded_pool.inner_cancellations or task.done():
                break
        assert guarded_pool.inner_cancellations == []
        assert task.done() is False

        release_pool_lock.set()
        assert lock_holder is not None
        await asyncio.wait_for(lock_holder, timeout=2.0)
        with pytest.raises(asyncio.CancelledError) as caught:
            await asyncio.wait_for(task, timeout=5.0)
        assert caught.value.args == ("g10 busy return cancellation",)

        assert borrower is not None
        assert await asyncio.wait_for(borrower, timeout=5.0) == old_pid
        assert borrowed_pids == [old_pid]
        assert guarded_pool.return_completions == [old_pid]
        assert guarded_pool.connection.closed is False
        assert permit.acquire_count == permit.release_count == 1
        assert permit.active is False
        stats = raw_pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"] == 1
    finally:
        guarded_pool.allow_checkout.set()
        release_pool_lock.set()
        if lock_holder is not None and not lock_holder.done():
            try:
                await lock_holder
            except BaseException:
                pass
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        if borrower is not None and not borrower.done():
            borrower.cancel()
            try:
                await borrower
            except BaseException:
                pass
        try:
            await lock_owner.execute(
                "SELECT pg_catalog.pg_advisory_unlock(%s, %s)",
                lock_keys,
            )
        finally:
            await lock_owner.close()
        await raw_pool.close()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stage", "error_kind"),
    [
        ("acquire", "cancelled"),
        ("acquire", "ack-loss"),
        ("unlock", "cancelled"),
        ("unlock", "ack-loss"),
    ],
)
async def test_real_lock_ack_loss_evicts_backend_pid_and_never_reuses_it(
    sync_runtime: _Runtime,
    stage: str,
    error_kind: str,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [_batch("cursor-2", includes_last=True)]
    events: list[str] = []
    error: BaseException
    if error_kind == "cancelled":
        error = asyncio.CancelledError()
    else:
        error = RuntimeError("session lock acknowledgement lost")
    marker = "pg_try_advisory_lock" if stage == "acquire" else "pg_advisory_unlock"
    pool = _AckLossPool(
        sync_runtime.pool,
        marker=marker,
        error=error,
        events=events,
    )
    permit = _TrackingPermitProvider(events)
    coordinator = _coordinator_with_components(
        sync_runtime,
        client=sync_runtime.client,
        permit=permit,
        snapshot=_snapshot(),
        pool=pool,
    )

    if error_kind == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await coordinator.run_folder(8, "INBOX")
    else:
        with pytest.raises(DatabaseOperationError) as caught:
            await coordinator.run_folder(8, "INBOX")
        expected_operation = (
            "sync_session_acquire" if stage == "acquire" else "sync_session_cleanup"
        )
        assert caught.value.operation == expected_operation

    assert pool.backend_pid is not None
    old_pid = pool.backend_pid
    assert pool.returned_closed is True
    assert events[-1] == "permit.release"
    assert events.index("connection.close") < events.index("pool.putconn")
    assert events.index("pool.putconn") < events.index("permit.release")
    assert permit.release_count == 1
    assert len(sync_runtime.client.calls) == (0 if stage == "acquire" else 1)
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == ("cursor-1" if stage == "acquire" else "cursor-2")
    assert row["version"] == (0 if stage == "acquire" else 1)
    assert _count(sync_runtime, "event_inbox") == 0
    await _wait_until_pid_disappears(sync_runtime, old_pid)
    assert await _probe_sync_lock(sync_runtime, "INBOX") is True

    replacement = await sync_runtime.pool.getconn()
    try:
        assert replacement.info.backend_pid != old_pid
    finally:
        await sync_runtime.pool.putconn(replacement)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_commit_ack_loss_taints_backend_but_preserves_committed_page(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    first_client = _PageClient()
    first_client.outcomes = [
        _batch(
            "cursor-2",
            includes_last=False,
            changes=(_create_change("message-commit-ack"),),
        )
    ]
    events: list[str] = []
    pool = _CommitAckLossPool(
        sync_runtime.pool,
        fault_exit_number=2,
        events=events,
    )
    permit = _TrackingPermitProvider(events)
    coordinator = _coordinator_with_components(
        sync_runtime,
        client=first_client,
        permit=permit,
        snapshot=_snapshot(),
        pool=pool,
    )

    with pytest.raises(_CommitAcknowledgementLost, match="acknowledgement"):
        await coordinator.run_folder(8, "INBOX")

    assert pool.backend_pid is not None
    old_pid = pool.backend_pid
    assert "db.unlock" not in events
    assert pool.returned_closed is True
    assert events[-1] == "permit.release"
    assert events.index("connection.close") < events.index("pool.putconn")
    assert events.index("pool.putconn") < events.index("permit.release")
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-2"
    assert row["version"] == 1
    assert _count(sync_runtime, "event_inbox") == 1
    await _wait_until_pid_disappears(sync_runtime, old_pid)
    assert await _probe_sync_lock(sync_runtime, "INBOX") is True

    second_client = _PageClient()
    second_client.outcomes = [_batch("cursor-3", includes_last=True)]
    second = _coordinator_with_components(
        sync_runtime,
        client=second_client,
        permit=_PermitProvider(),
        snapshot=_snapshot(),
    )
    resumed = await second.run_folder(8, "INBOX")

    assert resumed.status is SyncRunStatus.CAUGHT_UP
    assert [first_client.calls[0][2], second_client.calls[0][2]] == [
        "cursor-1",
        "cursor-2",
    ]
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-3"
    assert row["version"] == 2
    assert _count(sync_runtime, "event_inbox") == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_page_budget_commits_each_empty_nonterminal_page(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch("cursor-2", includes_last=False),
        _batch("cursor-3", includes_last=False),
    ]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX", max_pages=2)

    assert result.status is SyncRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == 2
    assert result.changes_observed == 0
    assert [call[2] for call in sync_runtime.client.calls] == [
        "cursor-1",
        "cursor-2",
    ]
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-3"
    assert row["version"] == 2
    assert _count(sync_runtime, "event_inbox") == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_terminal_page_wins_at_the_exact_page_budget(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch("cursor-2", includes_last=False),
        _batch("cursor-3", includes_last=True),
    ]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX", max_pages=2)

    assert result.status is SyncRunStatus.CAUGHT_UP
    assert result.pages_committed == 2
    assert _cursor_row(sync_runtime)["cursor"] == "cursor-3"  # type: ignore[index]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_terminal_unchanged_cursor_is_valid_and_first_write_wins(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    repeated = _batch(
        "cursor-1",
        includes_last=True,
        changes=(_create_change("message-repeat"),),
    )
    sync_runtime.client.outcomes = [repeated, repeated]

    first = await sync_runtime.coordinator.run_folder(8, "INBOX")
    second = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert first.status is second.status is SyncRunStatus.CAUGHT_UP
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 2
    assert _count(sync_runtime, "event_inbox") == 1


class _InjectedFailure(RuntimeError):
    pass


class _FaultingInboxTransaction:
    def __init__(self, transaction: Any) -> None:
        self._transaction = transaction

    async def insert(self, *args: object) -> object:
        await self._transaction.insert(*args)
        raise _InjectedFailure("injected after Inbox insert")


class _FaultingInboxRepository:
    def __init__(self, repository: InboxRepository) -> None:
        self._repository = repository

    def transaction(self, connection: object) -> _FaultingInboxTransaction:
        return _FaultingInboxTransaction(self._repository.transaction(connection))


class _ForbiddenInboxDml:
    def transaction(self, _connection: object) -> object:
        raise AssertionError("local contract rejection must precede Inbox DML")


def _coordinator_with_inbox(runtime: _Runtime, inbox: object) -> SyncCoordinator:
    return SyncCoordinator(
        page_client=runtime.client,
        snapshot_provider=_SnapshotProvider(_snapshot()),
        policy_resolver=ProcessingPolicyResolver(),
        folder_permit=runtime.permit,
        sync_pool=runtime.pool,
        inbox_repository=inbox,
        page_limit=100,
        default_max_pages=4,
        default_max_run_seconds=30.0,
        cleanup_timeout=1.0,
    )


def _coordinator_with_pool(runtime: _Runtime, pool: object) -> SyncCoordinator:
    return SyncCoordinator(
        page_client=runtime.client,
        snapshot_provider=_SnapshotProvider(_snapshot()),
        policy_resolver=ProcessingPolicyResolver(),
        folder_permit=runtime.permit,
        sync_pool=pool,
        inbox_repository=InboxRepository(_NeverPool()),
        page_limit=100,
        default_max_pages=4,
        default_max_run_seconds=30.0,
        cleanup_timeout=1.0,
    )


def _coordinator_with_limit_and_inbox(
    runtime: _Runtime,
    *,
    page_limit: int,
    inbox: object,
) -> SyncCoordinator:
    return SyncCoordinator(
        page_client=runtime.client,
        snapshot_provider=_SnapshotProvider(_snapshot()),
        policy_resolver=ProcessingPolicyResolver(),
        folder_permit=runtime.permit,
        sync_pool=runtime.pool,
        inbox_repository=inbox,
        page_limit=page_limit,
        default_max_pages=4,
        default_max_run_seconds=30.0,
        cleanup_timeout=1.0,
    )


def _coordinator_with_components(
    runtime: _Runtime,
    *,
    client: _PageClient,
    permit: _PermitProvider,
    snapshot: PolicySnapshot,
    pool: object | None = None,
) -> SyncCoordinator:
    return SyncCoordinator(
        page_client=client,
        snapshot_provider=_SnapshotProvider(snapshot),
        policy_resolver=ProcessingPolicyResolver(),
        folder_permit=permit,
        sync_pool=runtime.pool if pool is None else pool,
        inbox_repository=InboxRepository(_NeverPool()),
        page_limit=100,
        default_max_pages=4,
        default_max_run_seconds=30.0,
        cleanup_timeout=1.0,
    )


async def _probe_sync_lock(runtime: _Runtime, folder: str) -> bool:
    keys = sync_advisory_lock_keys(8, folder)
    async with await psycopg.AsyncConnection.connect(
        runtime.schema.runtime_dsn,
        autocommit=True,
    ) as connection:
        acquired_cursor = await connection.execute(
            "SELECT pg_catalog.pg_try_advisory_lock(%s, %s)",
            keys,
        )
        acquired_row = await acquired_cursor.fetchone()
        acquired = acquired_row == (True,)
        if acquired:
            unlocked_cursor = await connection.execute(
                "SELECT pg_catalog.pg_advisory_unlock(%s, %s)",
                keys,
            )
            assert await unlocked_cursor.fetchone() == (True,)
        return acquired


def _application_pids(runtime: _Runtime) -> tuple[int, ...]:
    with psycopg.connect(runtime.schema.dsn) as connection:
        return tuple(
            row[0]
            for row in connection.execute(
                "SELECT pid FROM pg_catalog.pg_stat_activity "
                "WHERE datname = pg_catalog.current_database() "
                "AND application_name = %s ORDER BY pid",
                (runtime.application_name,),
            ).fetchall()
        )


def _sync_lock_holder_pids(runtime: _Runtime, folder: str) -> tuple[int, ...]:
    first_key, second_key = sync_advisory_lock_keys(8, folder)
    unsigned_first = first_key & 0xFFFF_FFFF
    unsigned_second = second_key & 0xFFFF_FFFF
    with psycopg.connect(runtime.schema.dsn) as connection:
        return tuple(
            row[0]
            for row in connection.execute(
                "SELECT pid FROM pg_catalog.pg_locks "
                "WHERE locktype = 'advisory' AND granted "
                "AND classid::pg_catalog.int8 = %s "
                "AND objid::pg_catalog.int8 = %s AND objsubid = 2 "
                "ORDER BY pid",
                (unsigned_first, unsigned_second),
            ).fetchall()
        )


async def _wait_until_pid_disappears(runtime: _Runtime, pid: int) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    while pid in _application_pids(runtime):
        if loop.time() >= deadline:
            raise AssertionError("evicted backend PID remained in pg_stat_activity")
        await asyncio.sleep(0.01)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preflight_exit_must_be_idle_before_first_exchange_request(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [_batch("cursor-2", includes_last=True)]
    pool = _ExitFaultPool(
        sync_runtime.pool,
        fault_exit_number=1,
        fault_status=TransactionStatus.INTRANS,
    )
    coordinator = _coordinator_with_pool(sync_runtime, pool)

    with pytest.raises(DatabaseOperationError) as caught:
        await coordinator.run_folder(8, "INBOX")

    assert caught.value.operation == "sync_session_tainted"
    assert sync_runtime.client.calls == []
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 0
    assert _count(sync_runtime, "event_inbox") == 0
    assert pool.returned == pool.checked_out
    assert pool.returned[0].closed is True
    assert sync_runtime.permit.release_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_page_exit_must_be_idle_before_the_next_exchange_request(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch("cursor-2", includes_last=False),
        _batch("cursor-3", includes_last=True),
    ]
    pool = _ExitFaultPool(
        sync_runtime.pool,
        fault_exit_number=2,
        fault_status=TransactionStatus.INERROR,
    )
    coordinator = _coordinator_with_pool(sync_runtime, pool)

    with pytest.raises(DatabaseOperationError) as caught:
        await coordinator.run_folder(8, "INBOX")

    assert caught.value.operation == "sync_session_tainted"
    assert [call[2] for call in sync_runtime.client.calls] == ["cursor-1"]
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-2"
    assert row["version"] == 1
    assert _count(sync_runtime, "event_inbox") == 0
    assert pool.returned == pool.checked_out
    assert pool.returned[0].closed is True
    assert sync_runtime.permit.release_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_configured_page_limit_plus_one_blocks_before_inbox_dml(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    changes = tuple(_create_change(f"message-limit-{index}") for index in range(101))
    sync_runtime.client.outcomes = [
        _batch("cursor-2", includes_last=True, changes=changes),
    ]
    coordinator = _coordinator_with_limit_and_inbox(
        sync_runtime,
        page_limit=100,
        inbox=_ForbiddenInboxDml(),
    )

    first = await coordinator.run_folder(8, "INBOX")
    replay = await coordinator.run_folder(8, "INBOX")

    assert first.status is replay.status is SyncRunStatus.BLOCKED_CONTRACT
    assert first.safe_code == "sync.local_contract_invalid"
    assert replay.safe_code == "sync.blocked_contract"
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["status"] == "blocked_contract"
    assert row["version"] == 1
    assert len(sync_runtime.client.calls) == 1
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 1


class _SyncBatchSubclass(SyncBatch):
    pass


def _corrupted_batch(case: str) -> object:
    change = _create_change("message-hostile")
    batch: SyncBatch = _batch(
        "cursor-2",
        includes_last=True,
        changes=(change,),
    )
    if case == "subclass":
        return _SyncBatchSubclass(
            contract_version="exchange_sync_contract_v2",
            cursor="cursor-2",
            changes=(change,),
            includes_last=True,
        )
    if case == "contract-version":
        object.__setattr__(batch, "contract_version", "hostile.contract.sentinel")
    elif case == "cursor":
        object.__setattr__(batch, "cursor", " hostile.cursor.sentinel ")
    elif case == "changes-container":
        object.__setattr__(batch, "changes", [change])
    elif case == "includes-last":
        object.__setattr__(batch, "includes_last", 1)
    elif case == "change-kind":
        object.__setattr__(change, "kind", "create")
    elif case == "read-kind":
        object.__setattr__(change, "kind", ChangeKind.READ)
    elif case == "change-item":
        object.__setattr__(
            change,
            "item",
            {"id": "message-hostile", "subject": "hostile.item.sentinel"},
        )
    else:
        raise AssertionError("unknown corruption case")
    return batch


class _ExplodingErrorAttribute:
    def __get__(self, _instance: object, _owner: object) -> object:
        raise AssertionError("hostile error attribute was read")


class _HostileAuthorizationError(SyncAuthorizationError):
    safe_code = _ExplodingErrorAttribute()


class _HostileCursorError(SyncCursorInvalidError):
    safe_code = _ExplodingErrorAttribute()


class _HostileContractError(SyncContractError):
    safe_code = _ExplodingErrorAttribute()


class _HostileTransientError(SyncTransientError):
    safe_code = _ExplodingErrorAttribute()

    def __getattribute__(self, name: str) -> object:
        if name in {"safe_code", "retry_after_seconds", "__dict__"}:
            raise AssertionError("hostile transient attribute was read")
        return super().__getattribute__(name)


class _HostileHintInt(int):
    def __int__(self) -> int:
        raise AssertionError("hostile retry hint was coerced")


class _HostileHintDict(dict[str, object]):
    def get(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("hostile retry dictionary method was called")


def _hostile_sync_error(family: str) -> BaseException:
    if family == "authorization-subclass":
        return _HostileAuthorizationError()
    if family == "cursor-subclass":
        return _HostileCursorError()
    if family == "contract-subclass":
        return _HostileContractError()
    if family == "transient-subclass":
        error = _HostileTransientError.__new__(_HostileTransientError)
        RuntimeError.__init__(error, "hostile transient sentinel")
        object.__getattribute__(error, "__dict__")["retry_after_seconds"] = 3600
        return error
    if family == "contract-instance-safe-code":
        error = SyncContractError()
        object.__setattr__(error, "safe_code", "hostile.instance.sentinel")
        return error
    raise AssertionError("unknown hostile Sync error family")


def _mutated_transient_error(case: str) -> SyncTransientError:
    error = SyncTransientError()
    fields = object.__getattribute__(error, "__dict__")
    if case == "dict-subclass":
        object.__setattr__(
            error,
            "__dict__",
            _HostileHintDict(retry_after_seconds=9),
        )
    elif case == "missing":
        fields.pop("retry_after_seconds")
    elif case == "bool":
        fields["retry_after_seconds"] = True
    elif case == "negative":
        fields["retry_after_seconds"] = -1
    elif case == "overflow":
        fields["retry_after_seconds"] = 3601
    elif case == "int-subclass":
        fields["retry_after_seconds"] = _HostileHintInt(9)
    else:
        raise AssertionError("unknown transient mutation")
    return error


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "subclass",
        "contract-version",
        "cursor",
        "changes-container",
        "includes-last",
        "change-kind",
        "read-kind",
        "change-item",
    ],
)
async def test_hostile_or_corrupted_batch_blocks_with_one_safe_audit_before_dml(
    sync_runtime: _Runtime,
    case: str,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [_corrupted_batch(case)]
    coordinator = _coordinator_with_limit_and_inbox(
        sync_runtime,
        page_limit=100,
        inbox=_ForbiddenInboxDml(),
    )

    first = await coordinator.run_folder(8, "INBOX")
    replay = await coordinator.run_folder(8, "INBOX")

    assert first.status is replay.status is SyncRunStatus.BLOCKED_CONTRACT
    assert first.safe_code == "sync.local_contract_invalid"
    assert replay.safe_code == "sync.blocked_contract"
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 1
    assert len(sync_runtime.client.calls) == 1
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 1
    assert "hostile" not in _sync_error_text(sync_runtime)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ordinary_normalization_error_becomes_fixed_local_contract_block(
    sync_runtime: _Runtime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch(
            "cursor-2",
            includes_last=True,
            changes=(_create_change("message-normalization"),),
        )
    ]
    coordinator = _coordinator_with_limit_and_inbox(
        sync_runtime,
        page_limit=100,
        inbox=_ForbiddenInboxDml(),
    )

    def fail_normalization(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("hostile.normalization.sentinel")

    monkeypatch.setattr(sync_module, "normalize_sync_change", fail_normalization)
    first = await coordinator.run_folder(8, "INBOX")
    replay = await coordinator.run_folder(8, "INBOX")

    assert first.status is replay.status is SyncRunStatus.BLOCKED_CONTRACT
    assert first.safe_code == "sync.local_contract_invalid"
    assert replay.safe_code == "sync.blocked_contract"
    assert len(sync_runtime.client.calls) == 1
    assert _cursor_row(sync_runtime)["cursor"] == "cursor-1"  # type: ignore[index]
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 1
    assert "hostile" not in _sync_error_text(sync_runtime)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_normalization_process_control_propagates_without_contract_block(
    sync_runtime: _Runtime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch(
            "cursor-2",
            includes_last=True,
            changes=(_create_change("message-cancelled"),),
        )
    ]

    def cancel_normalization(*_args: object, **_kwargs: object) -> object:
        raise asyncio.CancelledError()

    monkeypatch.setattr(sync_module, "normalize_sync_change", cancel_normalization)
    with pytest.raises(asyncio.CancelledError):
        await sync_runtime.coordinator.run_folder(8, "INBOX")

    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["status"] == "active"
    assert row["version"] == 0
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 0
    assert sync_runtime.permit.active is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fault_after_inbox_insert_rolls_back_event_and_cursor_together(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch(
            "cursor-2",
            includes_last=True,
            changes=(_create_change("message-rollback"),),
        ),
    ]
    coordinator = _coordinator_with_inbox(
        sync_runtime,
        _FaultingInboxRepository(InboxRepository(_NeverPool())),
    )

    with pytest.raises(_InjectedFailure):
        await coordinator.run_folder(8, "INBOX")

    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 0
    assert _count(sync_runtime, "event_inbox") == 0
    assert sync_runtime.permit.active is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ownership_drift_after_http_raises_stale_fence_and_rolls_back_page(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch(
            "cursor-2",
            includes_last=True,
            changes=(_create_change("message-stale"),),
        ),
    ]

    async def drift_ownership(_call_number: int) -> None:
        sync_runtime.schema.runtime_execute(
            "UPDATE pipeline_ownership SET state = 'quiescing', "
            "reason = 'test drift', updated_at = CURRENT_TIMESTAMP "
            "WHERE account_id = 8 AND generation = 1"
        )

    sync_runtime.client.on_call = drift_ownership
    with pytest.raises(StaleFence):
        await sync_runtime.coordinator.run_folder(8, "INBOX")

    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 0
    assert _count(sync_runtime, "event_inbox") == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cursor_drift_after_http_raises_stale_fence_without_local_block(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch(
            "cursor-2",
            includes_last=True,
            changes=(_create_change("message-cursor-drift"),),
        ),
    ]

    async def drift_cursor(_call_number: int) -> None:
        sync_runtime.schema.runtime_execute(
            "UPDATE sync_cursors SET cursor = 'cursor-external', "
            "version = version + 1, last_attempt_at = CURRENT_TIMESTAMP, "
            "updated_at = CURRENT_TIMESTAMP "
            "WHERE account_id = 8 AND folder_key = 'INBOX'"
        )

    sync_runtime.client.on_call = drift_cursor
    with pytest.raises(StaleFence):
        await sync_runtime.coordinator.run_folder(8, "INBOX")

    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-external"
    assert row["status"] == "active"
    assert row["version"] == 1
    assert len(sync_runtime.client.calls) == 1
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_error_audit_failure_rolls_back_cursor_transition_in_the_same_xid(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [SyncContractError()]
    pool = _ExecuteThenFailPool(
        sync_runtime.pool,
        marker="INSERT INTO public.audit_events",
        error=_InjectedFailure("injected after audit insert"),
    )
    coordinator = _coordinator_with_pool(sync_runtime, pool)

    with pytest.raises(_InjectedFailure, match="after audit insert"):
        await coordinator.run_folder(8, "INBOX")

    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["status"] == "active"
    assert row["version"] == 0
    assert row["last_attempt_at"] is None
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 0
    assert sync_runtime.permit.active is False


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (SyncCursorInvalidError(), SyncRunStatus.RESET_REQUIRED),
        (SyncAuthorizationError(), SyncRunStatus.BLOCKED_CONTRACT),
        (SyncContractError(), SyncRunStatus.BLOCKED_CONTRACT),
        (
            SyncTransientError(retry_after_seconds=90),
            SyncRunStatus.RETRY_SCHEDULED,
        ),
    ],
)
async def test_fixed_errors_preserve_cursor_and_commit_one_safe_audit(
    sync_runtime: _Runtime,
    error: BaseException,
    expected_status: SyncRunStatus,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [error]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is expected_status
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 1
    assert row["last_attempt_at"] is not None
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 1
    audit_text = str(
        sync_runtime.schema.scalar(
            "SELECT pg_catalog.concat_ws('|', action, result, actor, reason, "
            "safe_metadata::pg_catalog.text) FROM audit_events "
            "WHERE object_type = 'sync_cursor' AND action = 'sync.error'"
        )
    )
    assert "cursor-1" not in audit_text
    assert repr(error) not in audit_text
    if isinstance(error, SyncTransientError):
        assert row["status"] == "active"
        assert row["transient_failures"] == 1
        delay = (row["retry_after_at"] - row["last_attempt_at"]).total_seconds()
        assert delay == 90
        replay = await sync_runtime.coordinator.run_folder(8, "INBOX")
        assert replay.status is SyncRunStatus.RETRY_DEFERRED
        assert len(sync_runtime.client.calls) == 1
        assert _sync_error_count(sync_runtime) == 1
    elif isinstance(error, SyncCursorInvalidError):
        assert row["status"] == "reset_required"
        assert row["contract_fingerprint"] is None
        replay = await sync_runtime.coordinator.run_folder(8, "INBOX")
        assert replay.status is SyncRunStatus.RESET_REQUIRED
        assert len(sync_runtime.client.calls) == 1
        assert _sync_error_count(sync_runtime) == 1
    else:
        assert row["status"] == "blocked_contract"
        assert row["contract_fingerprint"] is not None
        replay = await sync_runtime.coordinator.run_folder(8, "INBOX")
        assert replay.status is SyncRunStatus.BLOCKED_CONTRACT
        assert len(sync_runtime.client.calls) == 1
        assert _sync_error_count(sync_runtime) == 1


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "error",
        "expected_status",
        "expected_reason",
        "expected_cursor_status",
        "expected_replay_status",
    ),
    [
        (
            SyncCursorInvalidError(),
            SyncRunStatus.RESET_REQUIRED,
            "exchange.sync.cursor_invalid",
            "reset_required",
            SyncRunStatus.RESET_REQUIRED,
        ),
        (
            SyncAuthorizationError(),
            SyncRunStatus.BLOCKED_CONTRACT,
            "exchange.sync.authorization_failed",
            "blocked_contract",
            SyncRunStatus.BLOCKED_CONTRACT,
        ),
        (
            SyncContractError(),
            SyncRunStatus.BLOCKED_CONTRACT,
            "exchange.sync.contract_invalid",
            "blocked_contract",
            SyncRunStatus.BLOCKED_CONTRACT,
        ),
        (
            SyncTransientError(retry_after_seconds=7),
            SyncRunStatus.RETRY_SCHEDULED,
            "exchange.sync.transient_failure",
            "active",
            SyncRunStatus.RETRY_DEFERRED,
        ),
    ],
)
async def test_second_page_fixed_error_preserves_first_page_and_result_counts(
    sync_runtime: _Runtime,
    error: BaseException,
    expected_status: SyncRunStatus,
    expected_reason: str,
    expected_cursor_status: str,
    expected_replay_status: SyncRunStatus,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch(
            "cursor-2",
            includes_last=False,
            changes=(_create_change("message-page-one"),),
        ),
        error,
    ]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is expected_status
    assert result.safe_code == expected_reason
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert [call[2] for call in sync_runtime.client.calls] == [
        "cursor-1",
        "cursor-2",
    ]
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-2"
    assert row["status"] == expected_cursor_status
    assert row["version"] == 2
    assert row["blocked_reason_code"] == (
        None if expected_status is SyncRunStatus.RETRY_SCHEDULED else expected_reason
    )
    if expected_status is SyncRunStatus.RETRY_SCHEDULED:
        assert row["transient_failures"] == 1
        assert row["retry_after_at"] is not None
    else:
        assert row["transient_failures"] == 0
        assert row["retry_after_at"] is None
    assert _count(sync_runtime, "event_inbox") == 1
    assert _sync_error_count(sync_runtime) == 1
    audit_text = _sync_error_text(sync_runtime)
    assert expected_reason in audit_text
    assert "cursor-1" not in audit_text
    assert "cursor-2" not in audit_text
    assert repr(error) not in audit_text

    replay = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert replay.status is expected_replay_status
    assert len(sync_runtime.client.calls) == 2
    assert _count(sync_runtime, "event_inbox") == 1
    assert _sync_error_count(sync_runtime) == 1


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("family", "expected_status", "expected_reason"),
    [
        (
            "authorization-subclass",
            SyncRunStatus.BLOCKED_CONTRACT,
            "exchange.sync.authorization_failed",
        ),
        (
            "cursor-subclass",
            SyncRunStatus.RESET_REQUIRED,
            "exchange.sync.cursor_invalid",
        ),
        (
            "contract-subclass",
            SyncRunStatus.BLOCKED_CONTRACT,
            "exchange.sync.contract_invalid",
        ),
        (
            "transient-subclass",
            SyncRunStatus.RETRY_SCHEDULED,
            "exchange.sync.transient_failure",
        ),
        (
            "contract-instance-safe-code",
            SyncRunStatus.BLOCKED_CONTRACT,
            "exchange.sync.contract_invalid",
        ),
    ],
)
async def test_hostile_sync_error_attributes_never_override_fixed_local_reason(
    sync_runtime: _Runtime,
    family: str,
    expected_status: SyncRunStatus,
    expected_reason: str,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [_hostile_sync_error(family)]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is expected_status
    assert result.safe_code == expected_reason
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 1
    assert row["blocked_reason_code"] == (
        None if expected_status is SyncRunStatus.RETRY_SCHEDULED else expected_reason
    )
    assert _sync_error_count(sync_runtime) == 1
    audit_text = _sync_error_text(sync_runtime)
    assert expected_reason in audit_text
    assert "hostile" not in audit_text
    if expected_status is SyncRunStatus.RETRY_SCHEDULED:
        assert row["transient_failures"] == 1
        assert (row["retry_after_at"] - row["last_attempt_at"]).total_seconds() == 1


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["dict-subclass", "missing", "bool", "negative", "overflow", "int-subclass"],
)
async def test_invalid_exact_transient_hint_is_ignored_with_local_backoff(
    sync_runtime: _Runtime,
    case: str,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [_mutated_transient_error(case)]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is SyncRunStatus.RETRY_SCHEDULED
    assert result.safe_code == "exchange.sync.transient_failure"
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["status"] == "active"
    assert row["version"] == 1
    assert row["transient_failures"] == 1
    assert (row["retry_after_at"] - row["last_attempt_at"]).total_seconds() == 1
    assert _sync_error_count(sync_runtime) == 1
    assert "hostile" not in _sync_error_text(sync_runtime)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transient_backoff_accumulates_across_expiry_and_success_resets_state(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        SyncTransientError(),
        SyncTransientError(retry_after_seconds=7),
        SyncTransientError(),
        _batch("cursor-2", includes_last=True),
    ]

    expected_delays = (1, 7, 3)
    for failure_count, expected_delay in enumerate(expected_delays, start=1):
        scheduled = await sync_runtime.coordinator.run_folder(8, "INBOX")

        assert scheduled.status is SyncRunStatus.RETRY_SCHEDULED
        row = _cursor_row(sync_runtime)
        assert row is not None
        assert row["cursor"] == "cursor-1"
        assert row["status"] == "active"
        assert row["version"] == failure_count
        assert row["transient_failures"] == failure_count
        assert (
            row["retry_after_at"] - row["last_attempt_at"]
        ).total_seconds() == expected_delay
        deferred = await sync_runtime.coordinator.run_folder(8, "INBOX")
        assert deferred.status is SyncRunStatus.RETRY_DEFERRED
        assert len(sync_runtime.client.calls) == failure_count
        assert _sync_error_count(sync_runtime) == failure_count
        sync_runtime.schema.runtime_execute(
            "UPDATE sync_cursors SET "
            "retry_after_at = pg_catalog.clock_timestamp() - interval '1 second', "
            "updated_at = pg_catalog.clock_timestamp() "
            "WHERE account_id = 8 AND folder_key = 'INBOX'"
        )

    succeeded = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert succeeded.status is SyncRunStatus.CAUGHT_UP
    assert succeeded.pages_committed == 1
    assert succeeded.changes_observed == 0
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-2"
    assert row["version"] == 4
    assert row["transient_failures"] == 0
    assert row["retry_after_at"] is None
    assert len(sync_runtime.client.calls) == 4
    assert _sync_error_count(sync_runtime) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_nonterminal_unchanged_cursor_blocks_before_inbox_dml(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch(
            "cursor-1",
            includes_last=False,
            changes=(_create_change("message-stalled"),),
        ),
    ]

    result = await sync_runtime.coordinator.run_folder(8, "INBOX")
    replay = await sync_runtime.coordinator.run_folder(8, "INBOX")

    assert result.status is SyncRunStatus.BLOCKED_CONTRACT
    assert replay.status is SyncRunStatus.BLOCKED_CONTRACT
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["status"] == "blocked_contract"
    assert row["version"] == 1
    assert row["blocked_reason_code"] == "sync.cursor_stalled"
    assert len(sync_runtime.client.calls) == 1
    assert _count(sync_runtime, "event_inbox") == 0
    assert _sync_error_count(sync_runtime) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_time_budget_cancels_only_inflight_exchange_request(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)
    cancelled = asyncio.Event()
    sync_runtime.client.outcomes = [_batch("cursor-2", includes_last=True)]

    async def block_until_cancelled(_call_number: int) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    sync_runtime.client.on_call = block_until_cancelled
    result = await sync_runtime.coordinator.run_folder(
        8,
        "INBOX",
        max_run_seconds=0.02,
    )

    assert result.status is SyncRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == 0
    assert cancelled.is_set()
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 0
    assert sync_runtime.permit.active is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_deadline_exhausted_before_first_request_never_calls_exchange(
    sync_runtime: _Runtime,
) -> None:
    _seed_active(sync_runtime)

    result = await sync_runtime.coordinator.run_folder(
        8,
        "INBOX",
        max_run_seconds=1e-9,
    )

    assert result.status is SyncRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == 0
    assert result.changes_observed == 0
    assert sync_runtime.client.calls == []
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-1"
    assert row["version"] == 0
    assert _sync_error_count(sync_runtime) == 0


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("includes_last", "expected_status"),
    [
        (False, SyncRunStatus.BUDGET_EXHAUSTED),
        (True, SyncRunStatus.CAUGHT_UP),
    ],
    ids=["nonterminal-stops-after-commit", "terminal-wins-after-commit"],
)
async def test_page_commit_is_not_cancelled_when_deadline_expires(
    sync_runtime: _Runtime,
    includes_last: bool,
    expected_status: SyncRunStatus,
) -> None:
    _seed_active(sync_runtime)
    sync_runtime.client.outcomes = [
        _batch("cursor-2", includes_last=includes_last),
        _batch("cursor-3", includes_last=True),
    ]
    pool = _DelayExecutePool(
        sync_runtime.pool,
        marker="UPDATE public.sync_cursors AS cursor SET cursor = %s",
        delay_seconds=0.05,
    )
    coordinator = _coordinator_with_pool(sync_runtime, pool)

    result = await coordinator.run_folder(
        8,
        "INBOX",
        max_run_seconds=0.02,
    )

    assert result.status is expected_status
    assert result.pages_committed == 1
    assert result.changes_observed == 0
    assert len(sync_runtime.client.calls) == 1
    row = _cursor_row(sync_runtime)
    assert row is not None
    assert row["cursor"] == "cursor-2"
    assert row["version"] == 1
    assert _sync_error_count(sync_runtime) == 0
