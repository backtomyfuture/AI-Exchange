from __future__ import annotations

import asyncio
import inspect
import math
import time
from copy import deepcopy
from dataclasses import FrozenInstanceError, dataclass, fields, replace
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from types import MappingProxyType
from typing import get_type_hints
from uuid import UUID

import pytest
from psycopg.errors import UniqueViolation
from psycopg.pq import TransactionStatus

import src.ingestion.cold_start as cold_start_module
from src.domain.errors import (
    DatabaseOperationError,
    SyncAuthorizationError,
    SyncContractError,
    SyncCursorInvalidError,
    SyncTransientError,
)
from src.ingestion.cold_start import (
    ColdStartOriginPort,
    ColdStartPlanNotFoundError,
    ColdStartPlanState,
    ColdStartPlanView,
    ColdStartRunResult,
    ColdStartRunStatus,
    ColdStartSample,
    ColdStartService,
    ColdStartStateConflictError,
    _apply_page_payload_digest,
    _apply_page_result_digest,
    _approve_payload_digest,
    _approve_result_digest,
    _audit_event_digest,
    _audit_object_digest,
    _batch_digest,
    _blocked_digest,
    _cursor_digest,
    _fetch_ordinary_page,
    _fetch_origin_page,
    _plan_digest,
    _plan_view_from_row,
    _preview_payload_digest,
    _preview_result_digest,
    _preview_rolling_digest,
    _rebuild_sync_batch,
    _sample_external_id_digest,
)
from src.ingestion.command_receipts import (
    CommandReceipt,
    IdempotencyConflict,
    _hash_idempotency_key,
)
from src.ingestion.models import (
    MAX_SYNC_CHANGES_PER_BATCH,
    POSTGRES_BIGINT_MAX,
    ChangeKind,
    IngressSource,
    ProcessingPolicy,
    SyncBatch,
    SyncChange,
    SyncCursorStatus,
)
from src.ingestion.policy import (
    FolderScope,
    PolicySnapshot,
    PolicySnapshotUnavailableError,
    ProcessingPolicyResolver,
)
from src.ingestion.sync import _SyncSessionLease, _deterministic_retry_delay


class _OriginClient:
    def __init__(self, batch: SyncBatch) -> None:
        self.batch = batch
        self.calls: list[tuple[int, str, str | None, int]] = []

    async def fetch_cold_start_page(
        self,
        account_id: int,
        sync_folder: str,
        cursor: str | None,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, sync_folder, cursor, limit))
        return self.batch


class _OrdinaryClient:
    def __init__(self, batch: SyncBatch) -> None:
        self.batch = batch
        self.calls: list[tuple[int, str, str, int]] = []

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, cursor, limit))
        return self.batch


def _empty_batch() -> SyncBatch:
    return SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor="opaque+Boundary/%3D",
        changes=(),
        includes_last=True,
    )


def _vector_batch(**overrides: object) -> SyncBatch:
    values: dict[str, object] = {
        "contract_version": "exchange_sync_contract_v2",
        "cursor": "opaque+Boundary/%3D",
        "changes": (
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="邮件-1",
                source_version="v1",
                item={"subject": "你好", "nested": [1, True, None]},
            ),
            SyncChange(
                kind=ChangeKind.DELETE,
                external_email_id="deleted-2",
                source_version=None,
                item=None,
            ),
        ),
        "includes_last": True,
    }
    values.update(overrides)
    return SyncBatch(**values)  # type: ignore[arg-type]


def _batch_with_changes(count: int) -> SyncBatch:
    return SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor="opaque+Boundary/%3D",
        changes=tuple(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id=f"message-{index}",
                source_version=f"version-{index}",
                item={"nested": {"values": [index, True, None]}},
            )
            for index in range(count)
        ),
        includes_last=True,
    )


_PLAN_ID = UUID("12345678-1234-5678-1234-567812345678")
_CREATED_AT = datetime(2026, 7, 15, 1, 2, 3, 456789, tzinfo=UTC)
_READY_AT = _CREATED_AT + timedelta(minutes=1)
_APPROVED_AT = _READY_AT + timedelta(minutes=1)
_COMPLETED_AT = _APPROVED_AT + timedelta(minutes=1)
_EXPIRES_AT = _CREATED_AT + timedelta(days=1)


class _NoneOffsetTimezone(tzinfo):
    def utcoffset(self, _value: datetime | None) -> None:
        return None

    def dst(self, _value: datetime | None) -> None:
        return None

    def tzname(self, _value: datetime | None) -> str:
        return "invalid-none-offset"


class _RaisingTimezone(tzinfo):
    def utcoffset(self, _value: datetime | None) -> timedelta:
        raise RuntimeError("invalid timezone offset")

    def dst(self, _value: datetime | None) -> None:
        return None

    def tzname(self, _value: datetime | None) -> str:
        return "invalid-raising-offset"


_INVALID_DATABASE_TIMESTAMPS = (
    datetime(2026, 7, 15, 1, 2, 3, tzinfo=_NoneOffsetTimezone()),
    datetime(2026, 7, 15, 1, 2, 3, tzinfo=_RaisingTimezone()),
)


def _plan_view(**overrides: object) -> ColdStartPlanView:
    values: dict[str, object] = {
        "plan_id": _PLAN_ID,
        "account_id": 8,
        "canonical_folder": "INBOX",
        "state": ColdStartPlanState.PREVIEWING,
        "boundary_cursor": None,
        "page_count": 0,
        "item_count": 0,
        "redacted_samples": (),
        "contract_fingerprint": "a" * 64,
        "folder_scope_config_hash": "b" * 64,
        "plan_hash": None,
        "blocked_reason_code": None,
        "blocked_fingerprint": None,
        "expires_at": _EXPIRES_AT,
        "ready_at": None,
        "approved_at": None,
        "completed_at": None,
        "blocked_at": None,
        "created_at": _CREATED_AT,
        "updated_at": _CREATED_AT,
    }
    values.update(overrides)
    return ColdStartPlanView(**values)  # type: ignore[arg-type]


def _ready_plan(**overrides: object) -> ColdStartPlanView:
    values: dict[str, object] = {
        "state": ColdStartPlanState.READY,
        "boundary_cursor": "opaque+Boundary/%3D",
        "page_count": 1,
        "item_count": 1,
        "redacted_samples": (ColdStartSample(ChangeKind.CREATE, "c" * 64),),
        "plan_hash": "d" * 64,
        "ready_at": _READY_AT,
        "updated_at": _READY_AT,
    }
    values.update(overrides)
    return _plan_view(**values)


def _approved_plan(**overrides: object) -> ColdStartPlanView:
    values: dict[str, object] = {
        "state": ColdStartPlanState.APPROVED,
        "approved_at": _APPROVED_AT,
        "updated_at": _APPROVED_AT,
    }
    values.update(overrides)
    return _ready_plan(**values)


def _completed_plan(**overrides: object) -> ColdStartPlanView:
    values: dict[str, object] = {
        "state": ColdStartPlanState.COMPLETED,
        "approved_at": _APPROVED_AT,
        "completed_at": _COMPLETED_AT,
        "updated_at": _COMPLETED_AT,
    }
    values.update(overrides)
    return _ready_plan(**values)


def _blocked_plan(**overrides: object) -> ColdStartPlanView:
    values: dict[str, object] = {
        "state": ColdStartPlanState.BLOCKED,
        "blocked_reason_code": "exchange.sync.contract_invalid",
        "blocked_fingerprint": "f" * 64,
        "blocked_at": _READY_AT,
        "updated_at": _READY_AT,
    }
    values.update(overrides)
    return _plan_view(**values)


def _service_kwargs(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "cold_start_origin": object(),
        "ordinary_page_client": object(),
        "snapshot_provider": object(),
        "policy_resolver": object(),
        "folder_permit": object(),
        "maintenance_pool": object(),
        "inbox_repository": object(),
        "receipt_repository": object(),
        "page_limit": 100,
        "preview_max_pages": 10,
        "preview_max_run_seconds": 20.0,
        "apply_max_pages": 5,
        "apply_max_run_seconds": 10,
        "plan_ttl_seconds": 86_400,
        "locator_timeout": 3.0,
        "cleanup_timeout": 5,
        "contract_fingerprint": "e" * 64,
    }
    values.update(overrides)
    return values


def _plan_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "plan_id": _PLAN_ID,
        "account_id": 8,
        "folder_key": "INBOX",
        "state": "ready",
        "boundary_cursor": "opaque+Boundary/%3D",
        "page_count": 1,
        "item_count": 1,
        "redacted_samples": [
            {"kind": "create", "external_email_id_hash": "c" * 64},
        ],
        "contract_fingerprint": "a" * 64,
        "folder_scope_config_hash": "b" * 64,
        "plan_hash": "d" * 64,
        "blocked_reason_code": None,
        "blocked_fingerprint": None,
        "expires_at": _EXPIRES_AT,
        "ready_at": _READY_AT,
        "approved_at": None,
        "completed_at": None,
        "blocked_at": None,
        "created_at": _CREATED_AT,
        "updated_at": _READY_AT,
    }
    values.update(overrides)
    return values


class _LocatorCursor:
    def __init__(
        self,
        row: object,
        events: list[str],
        *,
        after_fetch_status: object | None = None,
        connection: _LocatorConnection | None = None,
        fetch_error: BaseException | None = None,
        fetch_delay: float = 0.0,
    ) -> None:
        self._row = row
        self._events = events
        self._after_fetch_status = after_fetch_status
        self._connection = connection
        self._fetch_error = fetch_error
        self._fetch_delay = fetch_delay

    async def fetchone(self) -> object:
        self._events.append("cursor.fetchone")
        if self._fetch_delay:
            await asyncio.sleep(self._fetch_delay)
        if self._fetch_error is not None:
            raise self._fetch_error
        if self._after_fetch_status is not None and self._connection is not None:
            self._connection.info.transaction_status = self._after_fetch_status
        return self._row


class _LocatorConnection:
    def __init__(
        self,
        row: object,
        events: list[str],
        *,
        autocommit: object = True,
        closed: object = False,
        transaction_status: object = TransactionStatus.IDLE,
        after_fetch_status: object | None = None,
        execute_error: BaseException | None = None,
        execute_delay: float = 0.0,
        fetch_error: BaseException | None = None,
        fetch_delay: float = 0.0,
        close_error: BaseException | None = None,
        close_delay: float = 0.0,
        close_confirms: bool = True,
    ) -> None:
        self._row = row
        self.events = events
        self.autocommit = autocommit
        self.closed = closed
        self.info = type(
            "_LocatorConnectionInfo",
            (),
            {"transaction_status": transaction_status},
        )()
        self.after_fetch_status = after_fetch_status
        self.execute_error = execute_error
        self.execute_delay = execute_delay
        self.fetch_error = fetch_error
        self.fetch_delay = fetch_delay
        self.close_error = close_error
        self.close_delay = close_delay
        self.close_confirms = close_confirms
        self.statements: list[tuple[str, object]] = []

    async def execute(self, statement: str, params: object = None) -> _LocatorCursor:
        self.events.append("db.execute")
        self.statements.append((statement, params))
        if self.execute_delay:
            await asyncio.sleep(self.execute_delay)
        if self.execute_error is not None:
            raise self.execute_error
        return _LocatorCursor(
            self._row,
            self.events,
            after_fetch_status=self.after_fetch_status,
            connection=self,
            fetch_error=self.fetch_error,
            fetch_delay=self.fetch_delay,
        )

    async def close(self) -> None:
        self.events.append("connection.close")
        if self.close_delay:
            await asyncio.sleep(self.close_delay)
        if self.close_confirms:
            self.closed = True
        if self.close_error is not None:
            raise self.close_error


class _LocatorPool:
    def __init__(
        self,
        connection: _LocatorConnection,
        events: list[str],
        *,
        kwargs: object = None,
        close_returns: object = False,
        get_error: BaseException | None = None,
        get_delay: float = 0.0,
        put_outcomes: list[object] | None = None,
    ) -> None:
        self.connection = connection
        self.events = events
        self.kwargs = (
            MappingProxyType({"autocommit": True}) if kwargs is None else kwargs
        )
        self.close_returns = close_returns
        self.getconn_calls = 0
        self.returned: list[_LocatorConnection] = []
        self.get_error = get_error
        self.get_delay = get_delay
        self.put_outcomes = list(put_outcomes or [])

    async def getconn(self) -> _LocatorConnection:
        self.events.append("pool.getconn")
        self.getconn_calls += 1
        if self.get_delay:
            await asyncio.sleep(self.get_delay)
        if self.get_error is not None:
            raise self.get_error
        return self.connection

    async def putconn(self, connection: _LocatorConnection) -> None:
        self.events.append("pool.putconn")
        self.returned.append(connection)
        if self.put_outcomes:
            outcome = self.put_outcomes.pop(0)
            if type(outcome) is float:
                await asyncio.sleep(outcome)
            elif isinstance(outcome, BaseException):
                raise outcome


class _ConfirmingLocatorPgConn:
    def __init__(
        self,
        connection: _LocatorConnection,
        events: list[str],
        *,
        error: BaseException | None = None,
    ) -> None:
        self._connection = connection
        self._events = events
        self._error = error

    def finish(self) -> None:
        self._events.append("pgconn.finish")
        if self._error is not None:
            raise self._error
        self._connection.closed = True


def _locator_service(
    *,
    row: object = None,
    pool_kwargs: object = None,
    close_returns: object = False,
    connection_autocommit: object = True,
    connection_closed: object = False,
    transaction_status: object = TransactionStatus.IDLE,
    after_fetch_status: object | None = None,
    execute_error: BaseException | None = None,
    execute_delay: float = 0.0,
    fetch_error: BaseException | None = None,
    fetch_delay: float = 0.0,
    close_error: BaseException | None = None,
    close_delay: float = 0.0,
    close_confirms: bool = True,
    get_error: BaseException | None = None,
    get_delay: float = 0.0,
    put_outcomes: list[object] | None = None,
    locator_timeout: float = 0.05,
    cleanup_timeout: float = 0.05,
) -> tuple[ColdStartService, _LocatorConnection, _LocatorPool, list[str]]:
    events: list[str] = []
    connection = _LocatorConnection(
        row,
        events,
        autocommit=connection_autocommit,
        closed=connection_closed,
        transaction_status=transaction_status,
        after_fetch_status=after_fetch_status,
        execute_error=execute_error,
        execute_delay=execute_delay,
        fetch_error=fetch_error,
        fetch_delay=fetch_delay,
        close_error=close_error,
        close_delay=close_delay,
        close_confirms=close_confirms,
    )
    pool = _LocatorPool(
        connection,
        events,
        kwargs=pool_kwargs,
        close_returns=close_returns,
        get_error=get_error,
        get_delay=get_delay,
        put_outcomes=put_outcomes,
    )
    service = ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            maintenance_pool=pool,
            locator_timeout=locator_timeout,
            cleanup_timeout=cleanup_timeout,
        )
    )
    return service, connection, pool, events


def test_cold_start_public_enums_and_dto_fields_are_exact() -> None:
    assert tuple(state.value for state in ColdStartPlanState) == (
        "previewing",
        "ready",
        "approved",
        "completed",
        "blocked",
    )
    assert tuple(status.value for status in ColdStartRunStatus) == (
        "busy_skip",
        "budget_exhausted",
        "previewing",
        "ready",
        "approved",
        "completed",
        "blocked",
        "retry_deferred",
        "retry_scheduled",
    )
    assert tuple(field.name for field in fields(ColdStartSample)) == (
        "kind",
        "external_email_id_hash",
    )
    assert tuple(field.name for field in fields(ColdStartPlanView)) == (
        "plan_id",
        "account_id",
        "canonical_folder",
        "state",
        "boundary_cursor",
        "page_count",
        "item_count",
        "redacted_samples",
        "contract_fingerprint",
        "folder_scope_config_hash",
        "plan_hash",
        "blocked_reason_code",
        "blocked_fingerprint",
        "expires_at",
        "ready_at",
        "approved_at",
        "completed_at",
        "blocked_at",
        "created_at",
        "updated_at",
    )
    assert tuple(field.name for field in fields(ColdStartRunResult)) == (
        "status",
        "plan",
        "pages_committed",
        "changes_observed",
        "safe_code",
    )


def test_cold_start_service_constructor_is_the_frozen_keyword_only_shape() -> None:
    parameters = inspect.signature(ColdStartService.__init__).parameters

    assert tuple(parameters) == (
        "self",
        "cold_start_origin",
        "ordinary_page_client",
        "snapshot_provider",
        "policy_resolver",
        "folder_permit",
        "maintenance_pool",
        "inbox_repository",
        "receipt_repository",
        "page_limit",
        "preview_max_pages",
        "preview_max_run_seconds",
        "apply_max_pages",
        "apply_max_run_seconds",
        "plan_ttl_seconds",
        "locator_timeout",
        "cleanup_timeout",
        "contract_fingerprint",
    )
    assert parameters["self"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        and parameter.default is inspect.Parameter.empty
        for name, parameter in parameters.items()
        if name != "self"
    )


def test_cursor_digest_has_the_frozen_literal_vector() -> None:
    assert _cursor_digest("opaque+Boundary/%3D") == (
        "f7c3ccd6ae2b96145232451b5713d363167e8f156aba14dc6eb5f07f7b9f3780"
    )


def test_identity_and_batch_digests_have_frozen_literal_vectors() -> None:
    assert _sample_external_id_digest(8, "邮件-1") == (
        "a5813857d95c35bb526b13b86fb1a35cd9248dd32a99919f991d13ddc13fab72"
    )
    assert _batch_digest(_vector_batch()) == (
        "11ec7d57ddaae48266a8bad99500bfa2d4a6531e6660cbd0934ca5237dce4001"
    )


def test_command_payload_digests_have_frozen_literal_vectors() -> None:
    assert _preview_payload_digest(
        account_id=8,
        canonical_folder="INBOX",
        actor="operator",
        reason="review-history",
    ) == ("078f27f197fc1370911c1e9bdee43cd662a7ee45d7ea59095d3253e769a1e2c2")
    assert _approve_payload_digest(
        plan_id=_PLAN_ID,
        actor="operator",
        reason="approve-history",
    ) == ("4ccc5c726829472f2c6c631f1c9991f3023d2af41f821d8f719250c9e6421a14")
    assert _apply_page_payload_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=_PLAN_ID,
        plan_version=4,
        cursor_status=SyncCursorStatus.COLD_START_PENDING,
        cursor_version=0,
        request_cursor_hash="f" * 64,
    ) == ("0babb142d038b06a7991811fad390356449d6e59335d490bfe2e124ca5a35529")


@pytest.mark.parametrize(
    ("cursor_status", "expected"),
    [
        (
            SyncCursorStatus.COLD_START_PENDING,
            "0babb142d038b06a7991811fad390356449d6e59335d490bfe2e124ca5a35529",
        ),
        (
            SyncCursorStatus.RESET_REQUIRED,
            "9d659a3f8c0c45389837c097d68881466c612494af11a7bc998aa56004256dc0",
        ),
        (
            SyncCursorStatus.COLD_START_APPLYING,
            "b8f1c61811cb54f58b42411aaf617229d9bb62966847ee887e6ff80ba691750a",
        ),
    ],
)
def test_apply_payload_has_three_frozen_cursor_status_vectors(
    cursor_status: SyncCursorStatus,
    expected: str,
) -> None:
    assert (
        _apply_page_payload_digest(
            account_id=8,
            canonical_folder="INBOX",
            plan_id=_PLAN_ID,
            plan_version=4,
            cursor_status=cursor_status,
            cursor_version=0,
            request_cursor_hash="f" * 64,
        )
        == expected
    )


@pytest.mark.parametrize(
    "cursor_status",
    [
        SyncCursorStatus.ACTIVE,
        SyncCursorStatus.BLOCKED_CONTRACT,
        "cold_start_applying",
        type("CursorStatusText", (str,), {})("cold_start_applying"),
    ],
)
def test_apply_payload_rejects_non_apply_or_non_enum_cursor_status(
    cursor_status: object,
) -> None:
    with pytest.raises(ValueError):
        _apply_page_payload_digest(
            account_id=8,
            canonical_folder="INBOX",
            plan_id=_PLAN_ID,
            plan_version=4,
            cursor_status=cursor_status,  # type: ignore[arg-type]
            cursor_version=0,
            request_cursor_hash="f" * 64,
        )


def test_command_result_digests_have_frozen_literal_vectors() -> None:
    assert _preview_result_digest(
        plan_id=_PLAN_ID,
        account_id=8,
        canonical_folder="INBOX",
        expected_cursor_status=SyncCursorStatus.RESET_REQUIRED,
        expected_cursor_version=7,
        expected_cursor_hash="1" * 64,
        pipeline_name="durable",
        generation=3,
        fencing_token=9,
        contract_fingerprint="a" * 64,
        folder_scope_config_hash="b" * 64,
        created_at=_CREATED_AT,
        expires_at=_EXPIRES_AT,
    ) == ("1a6da9534c56bd508eb17987c914b252104a24e43d82008d96b48ceb7e70551a")
    assert _approve_result_digest(
        plan_id=_PLAN_ID,
        plan_hash="d" * 64,
        pipeline_name="durable",
        generation=3,
        fencing_token=9,
        folder_scope_config_hash="b" * 64,
        approved_at=datetime(2026, 7, 15, 2, 3, 4, 5, tzinfo=UTC),
    ) == ("7f9d6cef16f4da31a518b057b43f50dcc3571c2f15b712903af6bc55a538ecae")
    assert _apply_page_result_digest("e" * 64) == (
        "caefd3600cbd3333bcf9f7d88b76567de486b1cf9ded869530604ab6bc3ce2bd"
    )


def test_plan_rolling_and_audit_digests_have_frozen_literal_vectors() -> None:
    samples = (
        ColdStartSample(ChangeKind.CREATE, "6" * 64),
        ColdStartSample(ChangeKind.DELETE, "7" * 64),
    )

    assert _preview_rolling_digest(None, "e" * 64) == (
        "ab34ba06c0fe25dcc56329f9412d267647f7175db63d5d799b5f6cc739695f46"
    )
    assert _preview_rolling_digest("c" * 64, "e" * 64) == (
        "32a87e1dc3c15defce9df604077b7645ec5290f85becee18141c140a7e3b1797"
    )
    assert _plan_digest(
        plan_id=_PLAN_ID,
        account_id=8,
        canonical_folder="INBOX",
        expected_cursor_status=SyncCursorStatus.RESET_REQUIRED,
        expected_cursor_version=7,
        expected_cursor_hash="1" * 64,
        pipeline_name="durable",
        generation=3,
        fencing_token=9,
        boundary_cursor_hash="2" * 64,
        boundary_cursor_version=2,
        rolling_hash="c" * 64,
        page_count=2,
        item_count=2,
        redacted_samples=samples,
        contract_fingerprint="a" * 64,
        folder_scope_config_hash="b" * 64,
        actor="operator",
        reason="review-history",
        created_at=_CREATED_AT,
        expires_at=_EXPIRES_AT,
    ) == ("09ec0b5d75ae7608d9be02cbb0c0175cad070711634cb864ae5367cbade1659d")
    assert _blocked_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=_PLAN_ID,
        safe_code="exchange.sync.contract_invalid",
    ) == ("78ba32f676da931aac921a3faa8aff30030bc1d30bfcabc1fcea55e827046e92")
    assert _audit_object_digest(_PLAN_ID) == (
        "2c197084460e7a0ed970b80e68760e3366c2376e42123f99c098355262ba1561"
    )
    assert _audit_event_digest(
        action="cold_start.approve",
        plan_id=_PLAN_ID,
        plan_version=4,
    ) == ("0dbfac1545bd8b5442dafab4e0caaba4945fdb2f9c22ff75b2216f28b39ae84f")


@pytest.mark.parametrize(
    "cursor",
    [
        "",
        " cursor",
        "cursor ",
        "cursor\x00value",
        "\ud800",
        "x" * 8193,
        type("CursorText", (str,), {})("cursor"),
    ],
)
def test_cursor_digest_rejects_hostile_tokens(cursor: object) -> None:
    with pytest.raises(ValueError):
        _cursor_digest(cursor)  # type: ignore[arg-type]


def test_batch_digest_rejects_hostile_frozen_json_scalar_subclasses() -> None:
    hostile_change = SyncChange(
        kind=ChangeKind.CREATE,
        external_email_id="message-1",
        source_version=None,
        item={"priority": type("IntegerValue", (int,), {})(1)},
    )

    with pytest.raises(ValueError):
        _batch_digest(
            SyncBatch(
                contract_version="exchange_sync_contract_v2",
                cursor="cursor-1",
                changes=(hostile_change,),
                includes_last=True,
            )
        )


@pytest.mark.parametrize(
    "operation",
    [
        lambda: _sample_external_id_digest(True, "message-1"),
        lambda: _sample_external_id_digest(8, "message\x00id"),
        lambda: _batch_digest(object()),
        lambda: _preview_payload_digest(
            account_id=8,
            canonical_folder="Inbox",
            actor="operator",
            reason="review-history",
        ),
        lambda: _preview_payload_digest(
            account_id=8,
            canonical_folder="INBOX",
            actor=" operator",
            reason="review-history",
        ),
        lambda: _approve_payload_digest(
            plan_id=str(_PLAN_ID),
            actor="operator",
            reason="approve-history",
        ),
        lambda: _preview_result_digest(
            plan_id=_PLAN_ID,
            account_id=8,
            canonical_folder="INBOX",
            expected_cursor_status=SyncCursorStatus.COLD_START_PENDING,
            expected_cursor_version=0,
            expected_cursor_hash="1" * 64,
            pipeline_name="durable",
            generation=3,
            fencing_token=9,
            contract_fingerprint="a" * 64,
            folder_scope_config_hash="b" * 64,
            created_at=datetime(2026, 7, 15, 1, 2, 3, tzinfo=timezone.utc),
            expires_at=_EXPIRES_AT,
        ),
        lambda: _apply_page_payload_digest(
            account_id=8,
            canonical_folder="INBOX",
            plan_id=_PLAN_ID,
            plan_version=True,
            cursor_status=SyncCursorStatus.COLD_START_PENDING,
            cursor_version=0,
            request_cursor_hash="f" * 64,
        ),
        lambda: _preview_rolling_digest("C" * 64, "e" * 64),
        lambda: _plan_digest(
            plan_id=_PLAN_ID,
            account_id=8,
            canonical_folder="INBOX",
            expected_cursor_status=SyncCursorStatus.RESET_REQUIRED,
            expected_cursor_version=7,
            expected_cursor_hash="1" * 64,
            pipeline_name="durable",
            generation=3,
            fencing_token=9,
            boundary_cursor_hash="2" * 64,
            boundary_cursor_version=2,
            rolling_hash="c" * 64,
            page_count=2,
            item_count=2,
            redacted_samples=[],
            contract_fingerprint="a" * 64,
            folder_scope_config_hash="b" * 64,
            actor="operator",
            reason="review-history",
            created_at=_CREATED_AT,
            expires_at=_EXPIRES_AT,
        ),
        lambda: _blocked_digest(
            account_id=8,
            canonical_folder="INBOX",
            plan_id=_PLAN_ID,
            safe_code="arbitrary.safe_code",
        ),
        lambda: _audit_event_digest(
            action="cold_start.approve",
            plan_id=_PLAN_ID,
            plan_version=-1,
        ),
    ],
)
def test_digest_projections_reject_hostile_or_inconsistent_values(
    operation: object,
) -> None:
    with pytest.raises(ValueError):
        operation()  # type: ignore[operator]


def test_plan_digest_revalidates_sample_before_sealing() -> None:
    sample = ColdStartSample(ChangeKind.CREATE, "6" * 64)
    object.__setattr__(sample, "external_email_id_hash", "raw-exchange-message-id")

    with pytest.raises(ValueError):
        _plan_digest(
            plan_id=_PLAN_ID,
            account_id=8,
            canonical_folder="INBOX",
            expected_cursor_status=SyncCursorStatus.RESET_REQUIRED,
            expected_cursor_version=7,
            expected_cursor_hash="1" * 64,
            pipeline_name="durable",
            generation=3,
            fencing_token=9,
            boundary_cursor_hash="2" * 64,
            boundary_cursor_version=1,
            rolling_hash="c" * 64,
            page_count=1,
            item_count=1,
            redacted_samples=(sample,),
            contract_fingerprint="a" * 64,
            folder_scope_config_hash="b" * 64,
            actor="operator",
            reason="review-history",
            created_at=_CREATED_AT,
            expires_at=_EXPIRES_AT,
        )


@pytest.mark.asyncio
async def test_origin_allows_none_but_ordinary_rejects_it_before_client_await() -> None:
    batch = _empty_batch()
    origin = _OriginClient(batch)
    ordinary = _OrdinaryClient(batch)

    returned = await _fetch_origin_page(origin, 8, "Inbox", None, 100)

    assert returned == batch
    assert returned is not batch
    assert origin.calls == [(8, "Inbox", None, 100)]
    with pytest.raises(ValueError, match="ordinary sync cursor"):
        await _fetch_ordinary_page(
            ordinary,
            8,
            "Inbox",
            None,  # type: ignore[arg-type]
            100,
        )
    assert ordinary.calls == []


def test_cold_start_public_types_are_frozen_and_slotted() -> None:
    for dto in (ColdStartSample, ColdStartPlanView, ColdStartRunResult):
        assert "__dict__" not in dto.__slots__
        assert inspect.isclass(dto)


def test_origin_and_ordinary_helpers_keep_clients_structural() -> None:
    assert inspect.iscoroutinefunction(_fetch_origin_page)
    assert inspect.iscoroutinefunction(_fetch_ordinary_page)


def test_cold_start_origin_port_has_the_frozen_async_signature() -> None:
    parameters = inspect.signature(ColdStartOriginPort.fetch_cold_start_page).parameters
    hints = get_type_hints(ColdStartOriginPort.fetch_cold_start_page)

    assert tuple(parameters) == (
        "self",
        "account_id",
        "sync_folder",
        "cursor",
        "limit",
    )
    assert hints == {
        "account_id": int,
        "sync_folder": str,
        "cursor": str | None,
        "limit": int,
        "return": SyncBatch,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor",
    [
        "",
        " leading",
        "trailing ",
        "control\x00cursor",
        "control\x7fcursor",
        "\ud800",
        "x" * 8193,
        type("CursorText", (str,), {})("opaque-cursor"),
    ],
)
async def test_origin_rejects_hostile_nonnull_cursor_before_await(
    cursor: object,
) -> None:
    origin = _OriginClient(_empty_batch())

    with pytest.raises(ValueError):
        await _fetch_origin_page(origin, 8, "Inbox", cursor, 100)  # type: ignore[arg-type]

    assert origin.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor",
    [
        None,
        "",
        " leading",
        "trailing ",
        "control\x00cursor",
        "control\x7fcursor",
        "\ud800",
        "x" * 8193,
        type("CursorText", (str,), {})("opaque-cursor"),
    ],
)
async def test_ordinary_rejects_hostile_cursor_before_await(cursor: object) -> None:
    ordinary = _OrdinaryClient(_empty_batch())

    with pytest.raises(ValueError):
        await _fetch_ordinary_page(ordinary, 8, "Inbox", cursor, 100)  # type: ignore[arg-type]

    assert ordinary.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("account_id", "sync_folder", "limit"),
    [
        (True, "Inbox", 100),
        (0, "Inbox", 100),
        (8, type("FolderText", (str,), {})("Inbox"), 100),
        (8, " Inbox", 100),
        (8, "Inbox", True),
        (8, "Inbox", 0),
        (8, "Inbox", 501),
    ],
)
async def test_page_helpers_reject_hostile_request_fields_before_await(
    account_id: object,
    sync_folder: object,
    limit: object,
) -> None:
    origin = _OriginClient(_empty_batch())
    ordinary = _OrdinaryClient(_empty_batch())

    with pytest.raises(ValueError):
        await _fetch_origin_page(  # type: ignore[arg-type]
            origin,
            account_id,
            sync_folder,
            None,
            limit,
        )
    with pytest.raises(ValueError):
        await _fetch_ordinary_page(  # type: ignore[arg-type]
            ordinary,
            account_id,
            sync_folder,
            "opaque-cursor",
            limit,
        )

    assert origin.calls == []
    assert ordinary.calls == []


@pytest.mark.asyncio
async def test_page_helpers_reject_non_batch_results_after_await() -> None:
    class _BadOrigin:
        async def fetch_cold_start_page(self, *_args: object) -> object:
            return object()

    class _BadOrdinary:
        async def sync_emails(self, *_args: object) -> object:
            return object()

    with pytest.raises(ValueError, match="SyncBatch"):
        await _fetch_origin_page(_BadOrigin(), 8, "Inbox", None, 100)
    with pytest.raises(ValueError, match="SyncBatch"):
        await _fetch_ordinary_page(
            _BadOrdinary(),
            8,
            "Inbox",
            "opaque-cursor",
            100,
        )


@pytest.mark.asyncio
async def test_ordinary_page_is_also_strictly_reconstructed() -> None:
    batch = _empty_batch()
    ordinary = _OrdinaryClient(batch)

    returned = await _fetch_ordinary_page(
        ordinary,
        8,
        "Inbox",
        "opaque-cursor",
        100,
    )

    assert returned == batch
    assert returned is not batch


@pytest.mark.asyncio
async def test_page_helpers_reject_response_count_above_requested_limit() -> None:
    batch = _batch_with_changes(2)
    origin = _OriginClient(batch)
    ordinary = _OrdinaryClient(batch)

    with pytest.raises(ValueError, match="limit"):
        await _fetch_origin_page(origin, 8, "Inbox", None, 1)
    with pytest.raises(ValueError, match="limit"):
        await _fetch_ordinary_page(ordinary, 8, "Inbox", "cursor-1", 1)

    assert origin.calls == [(8, "Inbox", None, 1)]
    assert ordinary.calls == [(8, "Inbox", "cursor-1", 1)]


def test_strict_batch_reconstruction_deeply_detaches_each_change() -> None:
    batch = SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor="cursor-1",
        changes=(
            SyncChange(
                ChangeKind.CREATE,
                "create-1",
                {"nested": {"values": [1, True, None]}},
                "v1",
            ),
            SyncChange(
                ChangeKind.UPDATE,
                "update-1",
                {"nested": {"values": [2]}},
                "v2",
            ),
            SyncChange(ChangeKind.DELETE, "delete-1", None, "v3"),
        ),
        includes_last=True,
    )

    rebuilt = _rebuild_sync_batch(batch, 3)

    assert rebuilt == batch
    assert rebuilt is not batch
    assert all(
        rebuilt_change is not original_change
        for rebuilt_change, original_change in zip(
            rebuilt.changes,
            batch.changes,
            strict=True,
        )
    )
    assert rebuilt.changes[0].item is not batch.changes[0].item
    assert rebuilt.changes[0].item["nested"] is not batch.changes[0].item["nested"]  # type: ignore[index]
    object.__setattr__(batch.changes[0], "external_email_id", "raw-mutated-id")
    object.__setattr__(batch.changes[0], "item", {"raw_id": "must-not-leak"})
    assert rebuilt.changes[0].external_email_id == "create-1"
    assert "raw_id" not in rebuilt.changes[0].item  # type: ignore[operator]


def _mutated_batch(case: str) -> SyncBatch:
    batch = _batch_with_changes(1)
    change = batch.changes[0]
    if case == "read":
        object.__setattr__(change, "kind", ChangeKind.READ)
        object.__setattr__(change, "item", None)
    elif case == "kind-string":
        object.__setattr__(change, "kind", "create")
    elif case == "id-subclass":
        object.__setattr__(
            change,
            "external_email_id",
            type("IdentifierText", (str,), {})("message-0"),
        )
    elif case == "version-subclass":
        object.__setattr__(
            change,
            "source_version",
            type("VersionText", (str,), {})("version-0"),
        )
    elif case == "mutable-item":
        object.__setattr__(change, "item", {"raw_id": "must-not-be-accepted"})
    elif case == "scalar-subclass":
        object.__setattr__(
            change,
            "item",
            MappingProxyType(
                {"value": type("IntegerValue", (int,), {})(1)},
            ),
        )
    elif case == "create-without-item":
        object.__setattr__(change, "item", None)
    elif case == "delete-with-item":
        object.__setattr__(change, "kind", ChangeKind.DELETE)
    elif case == "duplicates":
        object.__setattr__(batch, "changes", (change, change))
    else:  # pragma: no cover - the parametrization is frozen below.
        raise AssertionError(case)
    return batch


@pytest.mark.parametrize(
    "case",
    [
        "read",
        "kind-string",
        "id-subclass",
        "version-subclass",
        "mutable-item",
        "scalar-subclass",
        "create-without-item",
        "delete-with-item",
        "duplicates",
    ],
)
def test_strict_batch_reconstruction_rejects_hostile_change_shapes(case: str) -> None:
    with pytest.raises(ValueError):
        _rebuild_sync_batch(_mutated_batch(case), MAX_SYNC_CHANGES_PER_BATCH)


def test_strict_batch_reconstruction_rejects_global_over_limit_and_deep_input() -> None:
    over_limit = _batch_with_changes(MAX_SYNC_CHANGES_PER_BATCH)
    extra = SyncChange(ChangeKind.DELETE, "extra", None, None)
    object.__setattr__(over_limit, "changes", (*over_limit.changes, extra))

    deeply_nested: object = 1
    for _ in range(1_100):
        deeply_nested = (deeply_nested,)
    deep_batch = _batch_with_changes(1)
    object.__setattr__(
        deep_batch.changes[0],
        "item",
        MappingProxyType({"nested": deeply_nested}),
    )

    with pytest.raises(ValueError):
        _rebuild_sync_batch(over_limit, MAX_SYNC_CHANGES_PER_BATCH)
    with pytest.raises(ValueError):
        _rebuild_sync_batch(deep_batch, MAX_SYNC_CHANGES_PER_BATCH)


def test_strict_batch_reconstruction_normalizes_exceptions_but_not_control_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch = _batch_with_changes(1)

    def _ordinary_failure(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("hostile callback detail")

    monkeypatch.setattr(
        cold_start_module,
        "_materialize_frozen_json",
        _ordinary_failure,
    )
    with pytest.raises(ValueError, match="invalid SyncBatch"):
        _rebuild_sync_batch(batch, 1)

    def _control_flow(*_args: object, **_kwargs: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(
        cold_start_module,
        "_materialize_frozen_json",
        _control_flow,
    )
    with pytest.raises(KeyboardInterrupt):
        _rebuild_sync_batch(batch, 1)


def test_batch_digest_reuses_strict_global_reconstruction_boundary() -> None:
    with pytest.raises(ValueError):
        _batch_digest(_mutated_batch("read"))

    over_limit = _batch_with_changes(MAX_SYNC_CHANGES_PER_BATCH)
    object.__setattr__(
        over_limit,
        "changes",
        (
            *over_limit.changes,
            SyncChange(ChangeKind.DELETE, "extra", None, None),
        ),
    )
    with pytest.raises(ValueError):
        _batch_digest(over_limit)


def test_cold_start_fixed_errors_are_zero_argument_and_privacy_safe() -> None:
    expected = {
        ColdStartPlanNotFoundError: "cold-start plan not found",
        ColdStartStateConflictError: "cold-start plan state conflict",
    }

    for error_type, message in expected.items():
        assert tuple(inspect.signature(error_type).parameters) == ()
        error = error_type()
        assert str(error) == message
        assert error.args == (message,)


def test_cold_start_sample_accepts_only_redacted_change_kinds() -> None:
    sample = ColdStartSample(ChangeKind.CREATE, "a" * 64)

    assert sample.kind is ChangeKind.CREATE
    assert sample.external_email_id_hash == "a" * 64
    with pytest.raises(FrozenInstanceError):
        sample.external_email_id_hash = "b" * 64  # type: ignore[misc]


@pytest.mark.parametrize(
    ("kind", "external_email_id_hash"),
    [
        (ChangeKind.READ, "a" * 64),
        ("create", "a" * 64),
        (ChangeKind.CREATE, "A" * 64),
        (ChangeKind.CREATE, "a" * 63),
        (ChangeKind.CREATE, type("HashText", (str,), {})("a" * 64)),
    ],
)
def test_cold_start_sample_rejects_hostile_values(
    kind: object,
    external_email_id_hash: object,
) -> None:
    with pytest.raises(ValueError):
        ColdStartSample(kind, external_email_id_hash)  # type: ignore[arg-type]


def test_plan_view_reconstructs_and_detaches_nested_samples() -> None:
    sample = ColdStartSample(ChangeKind.CREATE, "c" * 64)

    plan = _plan_view(
        page_count=1,
        item_count=1,
        redacted_samples=(sample,),
    )

    assert plan.redacted_samples[0] is not sample
    object.__setattr__(sample, "external_email_id_hash", "raw-exchange-message-id")
    assert plan.redacted_samples[0].external_email_id_hash == "c" * 64


@pytest.mark.parametrize("field", ["kind", "external_email_id_hash"])
def test_plan_view_revalidates_hostile_mutated_samples(field: str) -> None:
    sample = ColdStartSample(ChangeKind.CREATE, "c" * 64)
    object.__setattr__(
        sample,
        field,
        ChangeKind.READ if field == "kind" else "raw-exchange-message-id",
    )

    with pytest.raises(ValueError):
        _plan_view(
            page_count=1,
            item_count=1,
            redacted_samples=(sample,),
        )


def test_run_result_reconstructs_and_detaches_nested_plan() -> None:
    plan = _ready_plan()

    result = ColdStartRunResult(ColdStartRunStatus.READY, plan, 1, 1, None)

    assert result.plan is not plan
    assert result.plan is not None
    assert result.plan.redacted_samples[0] is not plan.redacted_samples[0]
    object.__setattr__(plan, "state", ColdStartPlanState.PREVIEWING)
    object.__setattr__(
        plan.redacted_samples[0],
        "external_email_id_hash",
        "raw-exchange-message-id",
    )
    assert result.plan.state is ColdStartPlanState.READY
    assert result.plan.redacted_samples[0].external_email_id_hash == "c" * 64


def test_run_result_revalidates_hostile_mutated_plan() -> None:
    plan = _ready_plan()
    object.__setattr__(
        plan.redacted_samples[0],
        "external_email_id_hash",
        "raw-exchange-message-id",
    )

    with pytest.raises(ValueError):
        ColdStartRunResult(ColdStartRunStatus.READY, plan, 1, 1, None)


def test_cold_start_plan_view_accepts_each_valid_state_shape() -> None:
    previewing = _plan_view()
    ready = _ready_plan()
    approved = _approved_plan()
    completed = _completed_plan()
    blocked = _blocked_plan()

    assert tuple(
        plan.state for plan in (previewing, ready, approved, completed, blocked)
    ) == tuple(ColdStartPlanState)


@pytest.mark.parametrize(
    "reason_code",
    [
        "exchange.sync.authorization_failed",
        "exchange.sync.cursor_invalid",
        "exchange.sync.contract_invalid",
        "sync.local_contract_invalid",
        "sync.cursor_stalled",
        "cold_start.expired",
        "cold_start.config_drift",
        "cold_start.fence_drift",
        "cold_start.cursor_drift",
        "cold_start.version_drift",
        "cold_start.plan_hash_drift",
    ],
)
def test_cold_start_blocked_plan_accepts_only_frozen_reason_codes(
    reason_code: str,
) -> None:
    assert (
        _blocked_plan(blocked_reason_code=reason_code).blocked_reason_code
        == reason_code
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("plan_id", str(_PLAN_ID)),
        ("account_id", True),
        ("account_id", 0),
        ("account_id", 2**63),
        ("canonical_folder", "Inbox"),
        ("canonical_folder", type("FolderText", (str,), {})("INBOX")),
        ("state", "previewing"),
        ("boundary_cursor", " boundary"),
        ("boundary_cursor", "boundary\x00cursor"),
        ("boundary_cursor", "x" * 8193),
        ("page_count", True),
        ("page_count", -1),
        ("item_count", 2**63),
        ("redacted_samples", []),
        ("redacted_samples", (object(),)),
        (
            "redacted_samples",
            tuple(ColdStartSample(ChangeKind.CREATE, "1" * 64) for _ in range(21)),
        ),
        ("contract_fingerprint", "A" * 64),
        ("folder_scope_config_hash", "b" * 63),
    ],
)
def test_cold_start_plan_view_rejects_hostile_scalar_and_container_values(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        _plan_view(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("created_at", datetime(2026, 7, 15, 1, 2, 3)),
        (
            "created_at",
            datetime(2026, 7, 15, 1, 2, 3, tzinfo=timezone(timedelta(hours=8))),
        ),
        (
            "expires_at",
            type("HostileDatetime", (datetime,), {})(2026, 7, 16, tzinfo=UTC),
        ),
        ("updated_at", "2026-07-15T01:02:03.456789Z"),
    ],
)
def test_cold_start_plan_view_requires_exact_builtin_utc_timestamps(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        _plan_view(**{field: value})


@pytest.mark.parametrize(
    "overrides",
    [
        {"expires_at": _CREATED_AT},
        {"updated_at": _CREATED_AT - timedelta(microseconds=1)},
        {"state": ColdStartPlanState.READY},
        {"boundary_cursor": "unexpected"},
        {"plan_hash": "d" * 64},
        {
            "state": ColdStartPlanState.BLOCKED,
            "blocked_reason_code": "exchange.sync.contract_invalid",
            "blocked_fingerprint": "f" * 64,
        },
        {
            "state": ColdStartPlanState.BLOCKED,
            "blocked_reason_code": "Unsafe Message With Spaces",
            "blocked_fingerprint": "f" * 64,
            "blocked_at": _READY_AT,
        },
        {
            "state": ColdStartPlanState.BLOCKED,
            "blocked_reason_code": "arbitrary.but.safe_looking",
            "blocked_fingerprint": "f" * 64,
            "blocked_at": _READY_AT,
        },
    ],
)
def test_cold_start_plan_view_rejects_invalid_state_or_time_matrix(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _plan_view(**overrides)


def test_cold_start_plan_view_requires_the_complete_bounded_sample_prefix() -> None:
    with pytest.raises(ValueError):
        _plan_view(
            item_count=2,
            redacted_samples=(ColdStartSample(ChangeKind.CREATE, "1" * 64),),
        )

    samples = tuple(
        ColdStartSample(ChangeKind.CREATE, f"{index:064x}") for index in range(20)
    )
    assert (
        len(
            _plan_view(
                page_count=1,
                item_count=25,
                redacted_samples=samples,
            ).redacted_samples
        )
        == 20
    )


def test_zero_page_plan_cannot_claim_observed_items() -> None:
    with pytest.raises(ValueError):
        _plan_view(
            page_count=0,
            item_count=1,
            redacted_samples=(ColdStartSample(ChangeKind.CREATE, "c" * 64),),
        )


@pytest.mark.parametrize(
    "plan_factory",
    [
        lambda: _ready_plan(page_count=0, item_count=0, redacted_samples=()),
        lambda: _approved_plan(page_count=0, item_count=0, redacted_samples=()),
        lambda: _completed_plan(page_count=0, item_count=0, redacted_samples=()),
        lambda: _ready_plan(
            state=ColdStartPlanState.BLOCKED,
            page_count=0,
            item_count=0,
            redacted_samples=(),
            blocked_reason_code="exchange.sync.contract_invalid",
            blocked_fingerprint="f" * 64,
            blocked_at=_READY_AT,
        ),
    ],
)
def test_boundary_ready_plan_requires_at_least_one_page(plan_factory: object) -> None:
    with pytest.raises(ValueError):
        plan_factory()  # type: ignore[operator]


@pytest.mark.parametrize(
    "plan",
    [
        lambda: _ready_plan(
            state=ColdStartPlanState.BLOCKED,
            blocked_reason_code="exchange.sync.contract_invalid",
            blocked_fingerprint="f" * 64,
            blocked_at=_CREATED_AT,
        ),
        lambda: _ready_plan(
            state=ColdStartPlanState.BLOCKED,
            approved_at=_APPROVED_AT,
            blocked_reason_code="exchange.sync.contract_invalid",
            blocked_fingerprint="f" * 64,
            blocked_at=_READY_AT,
            updated_at=_APPROVED_AT,
        ),
    ],
)
def test_cold_start_blocked_transition_cannot_precede_ready_or_approval(
    plan: object,
) -> None:
    with pytest.raises(ValueError):
        plan()  # type: ignore[operator]


@pytest.mark.parametrize(
    "plan",
    [
        lambda: _ready_plan(ready_at=_EXPIRES_AT, updated_at=_EXPIRES_AT),
        lambda: _approved_plan(
            approved_at=_EXPIRES_AT,
            updated_at=_EXPIRES_AT,
        ),
        lambda: _completed_plan(
            completed_at=_EXPIRES_AT,
            updated_at=_EXPIRES_AT,
        ),
    ],
)
def test_cold_start_success_transitions_must_precede_expiry(plan: object) -> None:
    with pytest.raises(ValueError):
        plan()  # type: ignore[operator]


def test_plan_row_decoder_strictly_reconstructs_and_detaches_the_view() -> None:
    row = _plan_row()

    plan = _plan_view_from_row(row)

    assert plan == _ready_plan()
    assert type(plan.redacted_samples) is tuple
    row["redacted_samples"][0]["external_email_id_hash"] = "f" * 64  # type: ignore[index]
    assert plan.redacted_samples[0].external_email_id_hash == "c" * 64


def _without_plan_row_key(name: str) -> dict[str, object]:
    row = _plan_row()
    del row[name]
    return row


@pytest.mark.parametrize(
    "row_factory",
    [
        lambda: object(),
        lambda: type("RowMapping", (dict,), {})(_plan_row()),
        lambda: _plan_row(unexpected="value"),
        lambda: _without_plan_row_key("plan_hash"),
        lambda: _plan_row(state=ColdStartPlanState.READY),
        lambda: _plan_row(state="cancelled"),
        lambda: _plan_row(redacted_samples=()),
        lambda: _plan_row(redacted_samples=[{"kind": "create"}]),
        lambda: _plan_row(
            redacted_samples=[
                {
                    "kind": "create",
                    "external_email_id_hash": "c" * 64,
                    "raw_id": "must-not-survive",
                }
            ]
        ),
        lambda: _plan_row(
            redacted_samples=[
                {
                    "kind": ChangeKind.CREATE,
                    "external_email_id_hash": "c" * 64,
                }
            ]
        ),
        lambda: _plan_row(
            redacted_samples=[
                {
                    "kind": "create",
                    "external_email_id_hash": type(
                        "HashText",
                        (str,),
                        {},
                    )("c" * 64),
                }
            ]
        ),
        lambda: _plan_row(page_count=True),
        lambda: _plan_row(plan_id=str(_PLAN_ID)),
        lambda: _plan_row(created_at=datetime(2026, 7, 15, 1, 2, 3)),
        lambda: _plan_row(
            created_at=type("HostileDatetime", (datetime,), {})(
                2026,
                7,
                15,
                1,
                2,
                3,
                tzinfo=UTC,
            )
        ),
        *(
            lambda value=value: _plan_row(created_at=value)
            for value in _INVALID_DATABASE_TIMESTAMPS
        ),
    ],
)
def test_plan_row_decoder_rejects_hostile_database_shapes(
    row_factory: object,
) -> None:
    with pytest.raises(ValueError):
        _plan_view_from_row(row_factory())  # type: ignore[operator]


def test_plan_row_decoder_normalizes_aware_database_timestamps_to_utc() -> None:
    offset = timezone(timedelta(hours=8))
    row = _plan_row(
        expires_at=_EXPIRES_AT.astimezone(offset),
        ready_at=_READY_AT.astimezone(offset),
        created_at=_CREATED_AT.astimezone(offset),
        updated_at=_READY_AT.astimezone(offset),
    )

    plan = _plan_view_from_row(row)

    assert plan.expires_at == _EXPIRES_AT
    assert plan.created_at == _CREATED_AT
    assert plan.ready_at == _READY_AT
    assert plan.updated_at == _READY_AT
    assert plan.expires_at.tzinfo is UTC
    assert plan.created_at.tzinfo is UTC
    assert plan.ready_at is not None and plan.ready_at.tzinfo is UTC
    assert plan.updated_at.tzinfo is UTC


def test_apply_cursor_row_decoder_normalizes_aware_database_timestamps_to_utc() -> None:
    offset = timezone(timedelta(hours=8))
    stamp = _CREATED_AT.astimezone(offset)
    row = {
        "cursor": "opaque+Cursor/%3D",
        "status": "active",
        "version": 3,
        "blocked_reason_code": None,
        "contract_fingerprint": "a" * 64,
        "blocked_at": stamp,
        "transient_failures": 1,
        "retry_after_at": stamp,
        "cold_start_plan_id": None,
        "cold_start_plan_state": None,
        "last_attempt_at": stamp,
        "last_success_at": stamp,
        "updated_at": stamp,
    }

    record = cold_start_module._apply_cursor_record_from_row(row)

    assert record.blocked_at == _CREATED_AT
    assert record.retry_after_at == _CREATED_AT
    assert record.last_attempt_at == _CREATED_AT
    assert record.last_success_at == _CREATED_AT
    assert record.updated_at == _CREATED_AT
    assert all(
        timestamp is not None and timestamp.tzinfo is UTC
        for timestamp in (
            record.blocked_at,
            record.retry_after_at,
            record.last_attempt_at,
            record.last_success_at,
            record.updated_at,
        )
    )


@pytest.mark.parametrize("value", _INVALID_DATABASE_TIMESTAMPS)
def test_apply_cursor_row_decoder_rejects_invalid_exact_datetime_timezones(
    value: datetime,
) -> None:
    row = {
        "cursor": "opaque+Cursor/%3D",
        "status": "active",
        "version": 3,
        "blocked_reason_code": None,
        "contract_fingerprint": "a" * 64,
        "blocked_at": None,
        "transient_failures": 0,
        "retry_after_at": None,
        "cold_start_plan_id": None,
        "cold_start_plan_state": None,
        "last_attempt_at": None,
        "last_success_at": None,
        "updated_at": value,
    }

    with pytest.raises(ValueError):
        cold_start_module._apply_cursor_record_from_row(row)


@pytest.mark.asyncio
async def test_database_clock_normalizes_aware_timestamp_to_utc() -> None:
    offset_stamp = _CREATED_AT.astimezone(timezone(timedelta(hours=8)))

    class Connection:
        async def execute(self, statement: str) -> _C1AcceptanceCursor:
            assert statement == "SELECT pg_catalog.clock_timestamp() AS database_now"
            return _C1AcceptanceCursor({"database_now": offset_stamp})

    database_now = await cold_start_module._read_database_now(Connection())

    assert database_now == _CREATED_AT
    assert database_now.tzinfo is UTC


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    (
        datetime(2026, 7, 15, 1, 2, 3),
        type("HostileDatetime", (datetime,), {})(2026, 7, 15, tzinfo=UTC),
        *_INVALID_DATABASE_TIMESTAMPS,
    ),
)
async def test_database_clock_rejects_naive_and_datetime_subclass_values(
    value: object,
) -> None:
    class Connection:
        async def execute(self, _statement: str) -> _C1AcceptanceCursor:
            return _C1AcceptanceCursor({"database_now": value})

    with pytest.raises(DatabaseOperationError) as raised:
        await cold_start_module._read_database_now(Connection())

    assert raised.value.operation == "cold_start_database_clock"


def test_cold_start_run_result_accepts_the_literal_status_contract() -> None:
    previewing = _plan_view()
    ready = _ready_plan()
    approved = _approved_plan()
    completed = _completed_plan()
    blocked = _blocked_plan()

    results = (
        ColdStartRunResult(
            ColdStartRunStatus.BUSY_SKIP,
            None,
            0,
            0,
            "cold_start.busy",
        ),
        ColdStartRunResult(
            ColdStartRunStatus.BUDGET_EXHAUSTED,
            previewing,
            1,
            1,
            "cold_start.budget_exhausted",
        ),
        ColdStartRunResult(
            ColdStartRunStatus.PREVIEWING,
            previewing,
            1,
            1,
            None,
        ),
        ColdStartRunResult(ColdStartRunStatus.READY, ready, 1, 1, None),
        ColdStartRunResult(
            ColdStartRunStatus.APPROVED,
            approved,
            0,
            0,
            None,
        ),
        ColdStartRunResult(
            ColdStartRunStatus.COMPLETED,
            completed,
            1,
            1,
            None,
        ),
        ColdStartRunResult(
            ColdStartRunStatus.BLOCKED,
            blocked,
            0,
            0,
            "exchange.sync.contract_invalid",
        ),
        ColdStartRunResult(
            ColdStartRunStatus.RETRY_DEFERRED,
            approved,
            0,
            0,
            "cold_start.retry_deferred",
        ),
        ColdStartRunResult(
            ColdStartRunStatus.RETRY_SCHEDULED,
            approved,
            0,
            0,
            "exchange.sync.transient_failure",
        ),
    )

    assert tuple(result.status for result in results) == (
        ColdStartRunStatus.BUSY_SKIP,
        ColdStartRunStatus.BUDGET_EXHAUSTED,
        ColdStartRunStatus.PREVIEWING,
        ColdStartRunStatus.READY,
        ColdStartRunStatus.APPROVED,
        ColdStartRunStatus.COMPLETED,
        ColdStartRunStatus.BLOCKED,
        ColdStartRunStatus.RETRY_DEFERRED,
        ColdStartRunStatus.RETRY_SCHEDULED,
    )


@pytest.mark.parametrize(
    "values",
    [
        ("ready", _ready_plan(), 0, 0, None),
        (ColdStartRunStatus.BUSY_SKIP, _ready_plan(), 0, 0, "cold_start.busy"),
        (ColdStartRunStatus.BUSY_SKIP, None, 1, 0, "cold_start.busy"),
        (ColdStartRunStatus.BUSY_SKIP, None, 0, 0, None),
        (ColdStartRunStatus.READY, None, 0, 0, None),
        (ColdStartRunStatus.READY, _ready_plan(), True, 0, None),
        (ColdStartRunStatus.READY, _ready_plan(), 0, -1, None),
        (
            ColdStartRunStatus.BUDGET_EXHAUSTED,
            _ready_plan(),
            0,
            0,
            "wrong.code",
        ),
        (ColdStartRunStatus.READY, _ready_plan(), 0, 0, "unexpected.code"),
        (ColdStartRunStatus.PREVIEWING, _ready_plan(), 0, 0, None),
        (ColdStartRunStatus.READY, _plan_view(), 0, 0, None),
        (ColdStartRunStatus.APPROVED, _ready_plan(), 0, 0, None),
        (ColdStartRunStatus.COMPLETED, _approved_plan(), 0, 0, None),
        (
            ColdStartRunStatus.BLOCKED,
            _ready_plan(),
            0,
            0,
            "exchange.sync.contract_invalid",
        ),
        (
            ColdStartRunStatus.BUDGET_EXHAUSTED,
            _ready_plan(),
            0,
            0,
            "cold_start.budget_exhausted",
        ),
        (
            ColdStartRunStatus.RETRY_DEFERRED,
            _ready_plan(),
            0,
            0,
            "cold_start.retry_deferred",
        ),
        (
            ColdStartRunStatus.RETRY_DEFERRED,
            _approved_plan(),
            1,
            0,
            "cold_start.retry_deferred",
        ),
        (
            ColdStartRunStatus.RETRY_DEFERRED,
            _approved_plan(),
            0,
            1,
            "cold_start.retry_deferred",
        ),
        (
            ColdStartRunStatus.RETRY_SCHEDULED,
            _plan_view(),
            0,
            0,
            "exchange.sync.transient_failure",
        ),
    ],
)
def test_cold_start_run_result_rejects_invalid_status_coupling(
    values: tuple[object, object, object, object, object],
) -> None:
    with pytest.raises(ValueError):
        ColdStartRunResult(*values)  # type: ignore[arg-type]


def test_cold_start_service_accepts_the_frozen_valid_limits() -> None:
    service = ColdStartService(**_service_kwargs())  # type: ignore[arg-type]

    assert service._page_limit == 100
    assert service._contract_fingerprint == "e" * 64


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("page_limit", True),
        ("page_limit", 0),
        ("page_limit", 501),
        ("preview_max_pages", 0),
        ("preview_max_pages", 2**63),
        ("apply_max_pages", 1.0),
        ("preview_max_run_seconds", True),
        ("preview_max_run_seconds", 0),
        ("preview_max_run_seconds", math.inf),
        ("apply_max_run_seconds", math.nan),
        ("plan_ttl_seconds", 0),
        ("plan_ttl_seconds", 604_801),
        ("plan_ttl_seconds", 1.0),
        ("locator_timeout", 0),
        ("locator_timeout", 30.0001),
        ("cleanup_timeout", math.nan),
        ("cleanup_timeout", 31),
        ("contract_fingerprint", "E" * 64),
        ("contract_fingerprint", type("HashText", (str,), {})("e" * 64)),
    ],
)
def test_cold_start_service_rejects_hostile_or_out_of_range_limits(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        ColdStartService(**_service_kwargs(**{field: value}))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field",
    [
        "preview_max_run_seconds",
        "apply_max_run_seconds",
        "locator_timeout",
        "cleanup_timeout",
    ],
)
def test_cold_start_service_rejects_huge_exact_int_duration_as_value_error(
    field: str,
) -> None:
    with pytest.raises(ValueError):
        ColdStartService(  # type: ignore[arg-type]
            **_service_kwargs(**{field: 10**10_000})
        )


def _locator_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "plan_id": _PLAN_ID,
        "account_id": 8,
        "folder_key": "INBOX",
    }
    row.update(overrides)
    return row


def test_locator_is_private_async_and_has_the_frozen_identity_shape() -> None:
    method = ColdStartService._locate_plan_identity

    assert inspect.iscoroutinefunction(method)
    assert tuple(inspect.signature(method).parameters) == ("self", "plan_id")
    assert "_LocatedPlanIdentity" not in cold_start_module.__all__


@pytest.mark.asyncio
@pytest.mark.parametrize("plan_id", [str(_PLAN_ID), object()])
async def test_locator_rejects_nonexact_uuid_before_checkout(plan_id: object) -> None:
    service, _connection, pool, events = _locator_service(row=_locator_row())

    with pytest.raises(ValueError):
        await service._locate_plan_identity(plan_id)  # type: ignore[arg-type]

    assert pool.getconn_calls == 0
    assert events == []


@pytest.mark.asyncio
async def test_locator_happy_path_is_single_checkout_readonly_and_detached() -> None:
    row = _locator_row()
    service, connection, pool, events = _locator_service(row=row)

    located = await service._locate_plan_identity(_PLAN_ID)

    assert tuple(field.name for field in fields(type(located))) == (
        "plan_id",
        "account_id",
        "canonical_folder",
    )
    assert "__dict__" not in type(located).__slots__
    assert located.plan_id == _PLAN_ID
    assert located.account_id == 8
    assert located.canonical_folder == "INBOX"
    with pytest.raises(FrozenInstanceError):
        located.account_id = 9  # type: ignore[misc]
    row["account_id"] = 9
    row["folder_key"] = "SENT"
    assert located.account_id == 8
    assert located.canonical_folder == "INBOX"

    assert pool.getconn_calls == 1
    assert pool.returned == [connection]
    assert connection.closed is False
    assert events == [
        "pool.getconn",
        "db.execute",
        "cursor.fetchone",
        "pool.putconn",
    ]
    assert connection.statements == [
        (
            "SELECT plan_id, account_id, folder_key "
            "FROM public.sync_cold_start_plans WHERE plan_id = %s",
            (_PLAN_ID,),
        )
    ]
    statement = connection.statements[0][0].upper()
    assert statement.startswith("SELECT ")
    assert all(
        forbidden not in statement
        for forbidden in (
            "BEGIN",
            " SET ",
            "FOR UPDATE",
            "INSERT",
            "UPDATE ",
            "DELETE",
            "LOCK",
        )
    )


@pytest.mark.asyncio
async def test_locator_missing_is_raised_only_after_successful_return() -> None:
    service, connection, pool, events = _locator_service(row=None)

    with pytest.raises(ColdStartPlanNotFoundError):
        await service._locate_plan_identity(_PLAN_ID)

    assert pool.returned == [connection]
    assert events[-1] == "pool.putconn"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        type("LocatorRow", (dict,), {})(_locator_row()),
        {"plan_id": _PLAN_ID, "account_id": 8},
        {**_locator_row(), "raw_secret": "must-not-leak"},
        (_PLAN_ID, 8, "INBOX"),
        _locator_row(plan_id=UUID("87654321-4321-8765-4321-876543218765")),
        _locator_row(plan_id=str(_PLAN_ID)),
        _locator_row(account_id=True),
        _locator_row(account_id=0),
        _locator_row(
            account_id=type("AccountId", (int,), {})(8),
        ),
        _locator_row(account_id=POSTGRES_BIGINT_MAX + 1),
        _locator_row(folder_key="Inbox"),
        _locator_row(folder_key="INBOX\x00RAW"),
        _locator_row(
            folder_key=type("FolderText", (str,), {})("INBOX"),
        ),
    ],
)
async def test_locator_hostile_or_mismatched_row_fails_after_return(
    row: object,
) -> None:
    service, connection, pool, events = _locator_service(row=row)

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_row"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start locator row is invalid"
    assert "raw_secret" not in str(caught.value)
    assert pool.returned == [connection]
    assert events[-1] == "pool.putconn"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"pool_kwargs": []},
        {"pool_kwargs": {}},
        {"pool_kwargs": {"autocommit": 1}},
        {"close_returns": True},
    ],
)
async def test_locator_invalid_pool_contract_fails_before_checkout(
    overrides: dict[str, object],
) -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        **overrides,  # type: ignore[arg-type]
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_pool_contract"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start locator pool contract is invalid"
    assert pool.getconn_calls == 0
    assert connection.closed is False
    assert pool.returned == []
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"connection_autocommit": 1},
        {"connection_closed": 0},
        {"transaction_status": TransactionStatus.INTRANS},
        {"transaction_status": int(TransactionStatus.IDLE)},
    ],
)
async def test_locator_invalid_preselect_connection_is_retired(
    overrides: dict[str, object],
) -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        **overrides,  # type: ignore[arg-type]
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_pool_contract"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start locator pool contract is invalid"
    assert "db.execute" not in events
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_postread_nonidle_connection_is_closed_and_retired() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        after_fetch_status=TransactionStatus.INTRANS,
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_cleanup"
    assert caught.value.retryable is True
    assert str(caught.value) == "cold-start locator cleanup failed"
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_checkout_ordinary_failure_is_fixed_and_has_no_cleanup() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        get_error=RuntimeError("driver endpoint detail"),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_checkout"
    assert caught.value.retryable is True
    assert str(caught.value) == "cold-start locator checkout failed"
    assert connection.closed is False
    assert pool.returned == []
    assert events == ["pool.getconn"]


@pytest.mark.asyncio
async def test_locator_checkout_process_control_is_preserved_without_cleanup() -> None:
    primary = KeyboardInterrupt()
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        get_error=primary,
    )

    with pytest.raises(KeyboardInterrupt) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value is primary
    assert connection.closed is False
    assert pool.returned == []
    assert events == ["pool.getconn"]


@pytest.mark.asyncio
@pytest.mark.parametrize("primary", [RuntimeError("read failed"), KeyboardInterrupt()])
async def test_locator_read_primary_is_preserved_after_successful_return(
    primary: BaseException,
) -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        fetch_error=primary,
    )

    with pytest.raises(type(primary)) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value is primary
    assert connection.closed is False
    assert pool.returned == [connection]
    assert events[-1] == "pool.putconn"


@pytest.mark.asyncio
async def test_locator_checkout_timeout_is_bounded_without_cleanup() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        get_delay=0.2,
        locator_timeout=0.02,
        cleanup_timeout=0.02,
    )
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_checkout"
    assert elapsed < 0.15
    assert connection.closed is False
    assert pool.returned == []
    assert events == ["pool.getconn"]


@pytest.mark.asyncio
async def test_locator_checkout_that_swallows_cancel_cannot_cross_deadline() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        locator_timeout=0.01,
        cleanup_timeout=0.02,
    )
    cancellation_seen = asyncio.Event()

    async def cancellation_hostile_getconn() -> _LocatorConnection:
        events.append("pool.getconn")
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancellation_seen.set()
            await asyncio.sleep(0.08)
        return connection

    pool.getconn = cancellation_hostile_getconn  # type: ignore[method-assign]
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_checkout"
    assert elapsed < 0.06
    assert cancellation_seen.is_set()
    assert "db.execute" not in events

    for _ in range(200):
        if connection.closed is True and pool.returned == [connection]:
            break
        await asyncio.sleep(0.001)
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]
    assert "db.execute" not in events


@pytest.mark.asyncio
async def test_locator_read_timeout_retires_the_checked_out_handle() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        fetch_delay=0.2,
        locator_timeout=0.02,
        cleanup_timeout=0.02,
    )
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_cleanup"
    assert elapsed < 0.15
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_timeout_is_one_total_primary_deadline() -> None:
    service, connection, pool, _events = _locator_service(
        row=_locator_row(),
        get_delay=0.02,
        execute_delay=0.02,
        fetch_delay=0.02,
        put_outcomes=[0.02, None],
        locator_timeout=0.05,
        cleanup_timeout=0.03,
    )
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_cleanup"
    assert elapsed < 0.12
    assert connection.closed is True
    assert pool.returned[-1].closed is True


@pytest.mark.asyncio
async def test_locator_first_return_failure_closes_and_final_returns_handle() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        put_outcomes=[RuntimeError("return acknowledgement lost"), None],
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_cleanup"
    assert connection.closed is True
    assert pool.returned == [connection, connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]
    assert events.count("pool.putconn") == 2


@pytest.mark.asyncio
async def test_locator_normal_return_timeout_closes_and_final_returns_handle() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        put_outcomes=[0.2, None],
        locator_timeout=0.02,
        cleanup_timeout=0.02,
    )
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_cleanup"
    assert elapsed < 0.15
    assert connection.closed is True
    assert pool.returned == [connection, connection]
    assert events.count("pool.putconn") == 2
    assert events[-1] == "pool.putconn"
    assert "connection.close" in events
    await asyncio.sleep(0.22)
    assert events.count("pool.putconn") == 2


@pytest.mark.asyncio
async def test_locator_unconfirmed_physical_close_is_never_repooled() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        after_fetch_status=TransactionStatus.INTRANS,
        close_error=RuntimeError("close failed"),
        close_confirms=False,
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_cleanup"
    assert connection.closed is False
    assert pool.returned == []
    assert "pool.putconn" not in events


@pytest.mark.asyncio
async def test_locator_low_level_finish_confirmation_retires_closed_handle() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        after_fetch_status=TransactionStatus.INTRANS,
        close_error=RuntimeError("close failed"),
        close_confirms=False,
    )
    connection.pgconn = _ConfirmingLocatorPgConn(connection, events)

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_cleanup"
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-3:] == ["connection.close", "pgconn.finish", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_final_retirement_failure_never_attempts_a_third_return() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        after_fetch_status=TransactionStatus.INTRANS,
        put_outcomes=[RuntimeError("final accounting failed")],
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value.operation == "cold_start_locator_cleanup"
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events.count("pool.putconn") == 1


@pytest.mark.asyncio
async def test_locator_return_process_control_retires_before_reraise() -> None:
    primary = KeyboardInterrupt()
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        put_outcomes=[primary, None],
    )

    with pytest.raises(KeyboardInterrupt) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value is primary
    assert connection.closed is True
    assert pool.returned == [connection, connection]
    assert events.count("pool.putconn") == 2
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_falsey_return_process_control_preserves_exact_identity() -> None:
    class _FalseyKeyboardInterrupt(KeyboardInterrupt):
        def __bool__(self) -> bool:
            return False

    primary = _FalseyKeyboardInterrupt()
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        put_outcomes=[primary],
    )

    with pytest.raises(_FalseyKeyboardInterrupt) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value is primary
    assert connection.closed is True
    assert pool.returned == [connection, connection]
    assert events.count("pool.putconn") == 2
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_return_process_control_never_calls_hostile_bool() -> None:
    class _HostileSystemExit(SystemExit):
        def __init__(self) -> None:
            super().__init__()
            self.bool_calls = 0

        def __bool__(self) -> bool:
            self.bool_calls += 1
            raise AssertionError("process-control truthiness must not be inspected")

    primary = _HostileSystemExit()
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        put_outcomes=[primary],
    )

    with pytest.raises(_HostileSystemExit) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value is primary
    assert primary.bool_calls == 0
    assert connection.closed is True
    assert pool.returned == [connection, connection]
    assert events.count("pool.putconn") == 2
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_original_read_process_control_wins_ambiguous_return() -> None:
    primary = KeyboardInterrupt()
    cleanup_process = SystemExit()
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        fetch_error=primary,
        put_outcomes=[RuntimeError("return failed"), None],
        close_error=cleanup_process,
    )

    with pytest.raises(KeyboardInterrupt) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value is primary
    assert connection.closed is True
    assert pool.returned == [connection, connection]
    assert events.count("pool.putconn") == 2
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_original_read_process_control_wins_return_process_control() -> (
    None
):
    primary = KeyboardInterrupt()
    cleanup_process = SystemExit()
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        fetch_error=primary,
        put_outcomes=[cleanup_process],
    )

    with pytest.raises(KeyboardInterrupt) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    assert caught.value is primary
    assert connection.closed is True
    assert pool.returned == [connection, connection]
    assert events.count("pool.putconn") == 2
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_cleanup_uses_one_independent_total_deadline() -> None:
    service, connection, pool, _events = _locator_service(
        row=_locator_row(),
        after_fetch_status=TransactionStatus.INTRANS,
        close_delay=0.03,
        put_outcomes=[0.2],
        locator_timeout=0.02,
        cleanup_timeout=0.05,
    )
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_cleanup"
    assert elapsed < 0.15
    assert connection.closed is True
    assert pool.returned == [connection]


@pytest.mark.asyncio
async def test_locator_external_cancel_during_checkout_has_no_cleanup() -> None:
    service, connection, pool, events = _locator_service(row=_locator_row())
    checkout_started = asyncio.Event()
    cancelled: list[asyncio.CancelledError] = []

    async def blocking_getconn() -> _LocatorConnection:
        events.append("pool.getconn")
        pool.getconn_calls += 1
        checkout_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as error:
            cancelled.append(error)
            raise

    pool.getconn = blocking_getconn  # type: ignore[method-assign]
    task = asyncio.create_task(service._locate_plan_identity(_PLAN_ID))
    await checkout_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert cancelled == [caught.value]
    assert connection.closed is False
    assert pool.returned == []
    assert events == ["pool.getconn"]


@pytest.mark.asyncio
async def test_locator_external_cancel_during_fetch_is_cleaned_before_reraise() -> None:
    service, connection, pool, events = _locator_service(row=_locator_row())
    fetch_started = asyncio.Event()
    cancelled: list[asyncio.CancelledError] = []

    class _BlockingCursor:
        async def fetchone(self) -> object:
            events.append("cursor.fetchone")
            fetch_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError as error:
                cancelled.append(error)
                raise

    async def execute(statement: str, params: object = None) -> _BlockingCursor:
        events.append("db.execute")
        connection.statements.append((statement, params))
        return _BlockingCursor()

    connection.execute = execute  # type: ignore[method-assign]
    task = asyncio.create_task(service._locate_plan_identity(_PLAN_ID))
    await fetch_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert cancelled == [caught.value]
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_external_cancel_during_stuck_return_retires_handle() -> None:
    service, connection, pool, events = _locator_service(row=_locator_row())
    first_return_started = asyncio.Event()
    cancelled: list[asyncio.CancelledError] = []
    propagated: list[asyncio.CancelledError] = []
    return_attempts: list[_LocatorConnection] = []
    allow_return = asyncio.Event()

    async def blocking_first_putconn(candidate: _LocatorConnection) -> None:
        events.append("pool.putconn")
        return_attempts.append(candidate)
        if len(return_attempts) == 1:
            first_return_started.set()
            try:
                await allow_return.wait()
            except asyncio.CancelledError as error:
                cancelled.append(error)
                raise
        pool.returned.append(candidate)

    pool.putconn = blocking_first_putconn  # type: ignore[method-assign]

    async def observed_locator() -> object:
        try:
            return await service._locate_plan_identity(_PLAN_ID)
        except asyncio.CancelledError as error:
            propagated.append(error)
            raise

    task = asyncio.create_task(observed_locator())
    await first_return_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert propagated == [caught.value]
    assert cancelled == []
    assert connection.closed is True
    assert return_attempts == [connection, connection]
    assert pool.returned == [connection]
    assert events.count("pool.putconn") == 2
    assert "connection.close" in events

    allow_return.set()
    for _ in range(100):
        if len(pool.returned) == 2:
            break
        await asyncio.sleep(0)
    assert pool.returned == [connection, connection]
    assert return_attempts == [connection, connection]


@pytest.mark.asyncio
async def test_locator_second_cancel_cannot_interrupt_shielded_close() -> None:
    service, connection, pool, events = _locator_service(row=_locator_row())
    fetch_started = asyncio.Event()
    close_started = asyncio.Event()
    allow_close = asyncio.Event()
    first_cancellation: list[asyncio.CancelledError] = []

    class _BlockingCursor:
        async def fetchone(self) -> object:
            events.append("cursor.fetchone")
            fetch_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError as error:
                first_cancellation.append(error)
                raise

    async def execute(statement: str, params: object = None) -> _BlockingCursor:
        events.append("db.execute")
        connection.statements.append((statement, params))
        return _BlockingCursor()

    async def blocking_close() -> None:
        events.append("connection.close")
        close_started.set()
        await allow_close.wait()
        connection.closed = True

    connection.execute = execute  # type: ignore[method-assign]
    connection.close = blocking_close  # type: ignore[method-assign]
    task = asyncio.create_task(service._locate_plan_identity(_PLAN_ID))
    await fetch_started.wait()
    task.cancel()
    await close_started.wait()

    task.cancel()
    await asyncio.sleep(0)
    allow_close.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert first_cancellation == [caught.value]
    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]


@pytest.mark.asyncio
async def test_locator_final_return_second_cancel_is_bounded_without_third_return() -> (
    None
):
    primary = asyncio.CancelledError("primary locator cancellation")
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        fetch_error=primary,
        cleanup_timeout=0.02,
    )
    final_return_started = asyncio.Event()
    return_attempts: list[_LocatorConnection] = []
    cancellation_count = 0

    async def stubborn_final_putconn(candidate: _LocatorConnection) -> None:
        nonlocal cancellation_count
        events.append("pool.putconn")
        return_attempts.append(candidate)
        final_return_started.set()
        deadline = asyncio.get_running_loop().time() + 0.2
        while (remaining := deadline - asyncio.get_running_loop().time()) > 0:
            try:
                await asyncio.sleep(remaining)
            except asyncio.CancelledError:
                cancellation_count += 1

    pool.putconn = stubborn_final_putconn  # type: ignore[method-assign]
    task = asyncio.create_task(service._locate_plan_identity(_PLAN_ID))
    await final_return_started.wait()
    started = asyncio.get_running_loop().time()

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value is primary
    assert elapsed < 0.15
    assert connection.closed is True
    assert return_attempts == [connection]
    assert events.count("pool.putconn") == 1
    await asyncio.sleep(0.22)
    assert cancellation_count == 0
    assert events.count("pool.putconn") == 1


@pytest.mark.asyncio
async def test_locator_close_timeout_cannot_resume_retirement_after_deadline() -> None:
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        after_fetch_status=TransactionStatus.INTRANS,
        cleanup_timeout=0.02,
    )
    cancellation_count = 0

    async def stubborn_close() -> None:
        nonlocal cancellation_count
        events.append("connection.close")
        deadline = asyncio.get_running_loop().time() + 0.08
        while (remaining := deadline - asyncio.get_running_loop().time()) > 0:
            try:
                await asyncio.sleep(remaining)
            except asyncio.CancelledError:
                cancellation_count += 1
        connection.closed = True

    connection.close = stubborn_close  # type: ignore[method-assign]
    connection.pgconn = _ConfirmingLocatorPgConn(connection, events)
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_cleanup"
    assert elapsed < 0.15
    assert connection.closed is False
    assert cancellation_count == 1
    assert "pgconn.finish" not in events
    assert "pool.putconn" not in events

    await asyncio.sleep(0.1)
    assert connection.closed is True
    assert "pgconn.finish" not in events
    assert "pool.putconn" not in events
    assert pool.returned == []


@pytest.mark.asyncio
async def test_locator_finish_timeout_cannot_start_final_return_after_deadline() -> (
    None
):
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
        after_fetch_status=TransactionStatus.INTRANS,
        close_confirms=False,
        cleanup_timeout=0.02,
    )

    class _StubbornFinish:
        def finish(self) -> None:
            events.append("pgconn.finish")
            time.sleep(0.08)
            connection.closed = True

    connection.pgconn = _StubbornFinish()
    started = asyncio.get_running_loop().time()

    with pytest.raises(DatabaseOperationError) as caught:
        await service._locate_plan_identity(_PLAN_ID)

    elapsed = asyncio.get_running_loop().time() - started
    assert caught.value.operation == "cold_start_locator_cleanup"
    assert elapsed < 0.15
    assert connection.closed is False
    assert events[-2:] == ["connection.close", "pgconn.finish"]
    assert "pool.putconn" not in events

    await asyncio.sleep(0.1)
    assert connection.closed is True
    assert "pool.putconn" not in events
    assert pool.returned == []


@pytest.mark.asyncio
async def test_locator_external_cancel_during_cleanup_wins_ordinary_primary() -> None:
    primary = RuntimeError("ordinary read failed")
    service, connection, pool, events = _locator_service(
        row=_locator_row(),
    )
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    async def failing_execute(
        statement: str,
        params: object = None,
    ) -> object:
        events.append("db.execute")
        connection.statements.append((statement, params))
        connection.info.transaction_status = TransactionStatus.INTRANS
        raise primary

    async def blocking_close() -> None:
        events.append("connection.close")
        close_started.set()
        await allow_close.wait()
        connection.closed = True

    connection.execute = failing_execute  # type: ignore[method-assign]
    connection.close = blocking_close  # type: ignore[method-assign]
    task = asyncio.create_task(service._locate_plan_identity(_PLAN_ID))
    await close_started.wait()

    task.cancel()
    await asyncio.sleep(0)
    allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert connection.closed is True
    assert pool.returned == [connection]
    assert events[-2:] == ["connection.close", "pool.putconn"]


def _c1_policy_matrix() -> dict[
    tuple[IngressSource, str, ChangeKind], ProcessingPolicy
]:
    return {
        (IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE): (
            ProcessingPolicy.FULL
        ),
        (IngressSource.WEBHOOK, "CreatedEvent", ChangeKind.CREATE): (
            ProcessingPolicy.IGNORED
        ),
        (IngressSource.WEBHOOK, "ModifiedEvent", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.WEBHOOK, "DeletedEvent", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "create", ChangeKind.CREATE): ProcessingPolicy.FULL,
        (IngressSource.SYNC, "update", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "delete", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
    }


def _c1_snapshot() -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("inbox-id",),
                sync_folder="Inbox",
                event_policy_matrix=_c1_policy_matrix(),
            ),
        )
    )


class _C1SnapshotProvider:
    def __init__(self, snapshot: object, events: list[str]) -> None:
        self._snapshot = snapshot
        self._events = events

    async def get_ready_snapshot(self, account_id: int) -> object:
        assert account_id == 8
        self._events.append("snapshot")
        return self._snapshot


class _C1Permit:
    def __init__(self, events: list[str], *, busy: bool) -> None:
        self._events = events
        self._busy = busy

    async def try_acquire(self, account_id: int, folder: str) -> None:
        assert (account_id, folder) == (8, "INBOX")
        self._events.append("permit")
        if not self._busy:
            raise AssertionError("this C1 boundary fake only models permit miss")
        return None


class _C1ForbiddenPool:
    kwargs = MappingProxyType({"autocommit": True})
    close_returns = False

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def getconn(self) -> object:
        self._events.append("pool")
        raise AssertionError("pool checkout is forbidden in this C1 test")


class _C1ForbiddenOrigin:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def fetch_cold_start_page(self, *_args: object) -> SyncBatch:
        self._events.append("origin")
        raise AssertionError("Origin is forbidden in this C1 test")


def _c1_boundary_service(
    *,
    snapshot: object,
    permit_busy: bool,
    events: list[str],
) -> ColdStartService:
    return ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            cold_start_origin=_C1ForbiddenOrigin(events),
            snapshot_provider=_C1SnapshotProvider(snapshot, events),
            policy_resolver=ProcessingPolicyResolver(),
            folder_permit=_C1Permit(events, busy=permit_busy),
            maintenance_pool=_C1ForbiddenPool(events),
        )
    )


def test_preview_has_the_exact_frozen_public_signature() -> None:
    method = ColdStartService.preview
    parameters = tuple(inspect.signature(method).parameters.values())

    assert inspect.iscoroutinefunction(method)
    assert tuple(parameter.name for parameter in parameters) == (
        "self",
        "account_id",
        "folder",
        "actor",
        "reason",
        "idempotency_key",
    )
    assert tuple(parameter.kind for parameter in parameters) == (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.KEYWORD_ONLY,
    )


def test_resume_has_the_exact_frozen_public_signature() -> None:
    method = ColdStartService.resume
    parameters = tuple(inspect.signature(method).parameters.values())

    assert inspect.iscoroutinefunction(method)
    assert tuple(parameter.name for parameter in parameters) == (
        "self",
        "plan_id",
    )
    assert tuple(parameter.kind for parameter in parameters) == (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )


def test_approve_has_the_exact_frozen_public_signature() -> None:
    method = ColdStartService.approve
    parameters = tuple(inspect.signature(method).parameters.values())

    assert inspect.iscoroutinefunction(method)
    assert tuple(parameter.name for parameter in parameters) == (
        "self",
        "plan_id",
        "actor",
        "reason",
        "idempotency_key",
    )
    assert tuple(parameter.kind for parameter in parameters) == (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.KEYWORD_ONLY,
    )


def test_apply_has_the_exact_frozen_public_signature() -> None:
    method = ColdStartService.apply
    parameters = tuple(inspect.signature(method).parameters.values())

    assert inspect.iscoroutinefunction(method)
    assert tuple(parameter.name for parameter in parameters) == (
        "self",
        "plan_id",
    )
    assert tuple(parameter.kind for parameter in parameters) == (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("account_id", True),
        ("account_id", 0),
        ("account_id", type("AccountId", (int,), {})(8)),
        ("account_id", POSTGRES_BIGINT_MAX + 1),
        ("folder", "Inbox"),
        ("folder", type("FolderText", (str,), {})("INBOX")),
        ("actor", " operator"),
        ("actor", "a" * 129),
        ("actor", type("ActorText", (str,), {})("operator")),
        ("reason", "review\x80history"),
        ("reason", "r" * 513),
        ("idempotency_key", ""),
        ("idempotency_key", "k" * 4097),
        ("idempotency_key", "界" * 1366),
        ("idempotency_key", "\ud800"),
        ("idempotency_key", type("KeyText", (str,), {})("key")),
    ],
)
async def test_preview_rejects_public_input_before_any_resource(
    field: str,
    invalid: object,
) -> None:
    events: list[str] = []
    service = _c1_boundary_service(
        snapshot=_c1_snapshot(),
        permit_busy=True,
        events=events,
    )
    values: dict[str, object] = {
        "account_id": 8,
        "folder": "INBOX",
        "actor": "operator",
        "reason": "review history",
        "idempotency_key": "preview-key",
    }
    values[field] = invalid

    with pytest.raises(ValueError):
        await service.preview(
            values["account_id"],  # type: ignore[arg-type]
            values["folder"],  # type: ignore[arg-type]
            actor=values["actor"],  # type: ignore[arg-type]
            reason=values["reason"],  # type: ignore[arg-type]
            idempotency_key=values["idempotency_key"],  # type: ignore[arg-type]
        )

    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "idempotency_key",
    [" \x00 ", "控制\x1f键", "k" * 4096],
)
async def test_preview_key_does_not_inherit_operator_text_rules(
    idempotency_key: str,
) -> None:
    events: list[str] = []
    service = _c1_boundary_service(
        snapshot=PolicySnapshot.failed(),
        permit_busy=True,
        events=events,
    )

    with pytest.raises(PolicySnapshotUnavailableError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key=idempotency_key,
        )

    assert events == ["snapshot"]


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot", [None, PolicySnapshot.failed()])
async def test_preview_policy_unavailable_precedes_permit_and_pool(
    snapshot: object,
) -> None:
    events: list[str] = []
    service = _c1_boundary_service(
        snapshot=snapshot,
        permit_busy=True,
        events=events,
    )

    with pytest.raises(PolicySnapshotUnavailableError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert events == ["snapshot"]


@pytest.mark.asyncio
async def test_preview_permit_miss_is_busy_without_pool_or_origin() -> None:
    events: list[str] = []
    service = _c1_boundary_service(
        snapshot=_c1_snapshot(),
        permit_busy=True,
        events=events,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result == ColdStartRunResult(
        ColdStartRunStatus.BUSY_SKIP,
        None,
        0,
        0,
        "cold_start.busy",
    )
    assert events == ["snapshot", "permit"]


class _C1AcceptanceCursor:
    def __init__(self, row: object) -> None:
        self._row = row

    async def fetchone(self) -> object:
        return self._row


class _C1AcceptanceTransaction:
    def __init__(self, connection: _C1AcceptanceConnection) -> None:
        self._connection = connection
        self._plans_snapshot: dict[UUID, dict[str, object]] = {}
        self._receipts_snapshot: dict[tuple[int, str, str], CommandReceipt] | None = (
            None
        )
        self._cursor_snapshot: tuple[object, ...] = ()
        self._apply_cursor_snapshot: tuple[object, ...] | None = None
        self._apply_transitions_snapshot: list[dict[str, object]] | None = None
        self._inbox_snapshot: list[tuple[object, int, int]] | None = None
        self._audits_snapshot: list[dict[str, object]] = []

    async def __aenter__(self) -> None:
        self._plans_snapshot = deepcopy(self._connection.plans)
        repository = getattr(self._connection, "receipt_repository", None)
        if repository is not None:
            self._receipts_snapshot = deepcopy(repository.receipts)
        self._cursor_snapshot = (
            self._connection.cursor_status,
            self._connection.cursor_value,
            self._connection.cursor_version,
            self._connection.cursor_blocked_reason,
            self._connection.cursor_contract_fingerprint,
            self._connection.cursor_blocked_at,
        )
        if isinstance(self._connection, _C5ApplyConnection):
            self._apply_cursor_snapshot = (
                self._connection.cursor_transient_failures,
                self._connection.cursor_retry_after_at,
                self._connection.cursor_cold_start_plan_id,
                self._connection.cursor_cold_start_plan_state,
                self._connection.cursor_last_attempt_at,
                self._connection.cursor_last_success_at,
                self._connection.cursor_updated_at,
            )
            self._apply_transitions_snapshot = deepcopy(
                self._connection.apply_transitions
            )
        inbox_repository = getattr(self._connection, "inbox_repository", None)
        if inbox_repository is not None:
            # NormalizedIngressEvent is frozen and owns immutable MappingProxy
            # payloads, so copying the transactional list is both complete and
            # intentionally avoids attempting to pickle MappingProxyType.
            self._inbox_snapshot = list(inbox_repository.inserted)
        self._audits_snapshot = deepcopy(self._connection.audits)
        self._connection.events.append("xid.enter")
        self._connection.info.transaction_status = TransactionStatus.INTRANS

    def _restore_snapshot(self) -> None:
        self._connection.plans = deepcopy(self._plans_snapshot)
        repository = getattr(self._connection, "receipt_repository", None)
        if repository is not None and self._receipts_snapshot is not None:
            repository.receipts = deepcopy(self._receipts_snapshot)
        (
            self._connection.cursor_status,
            self._connection.cursor_value,
            self._connection.cursor_version,
            self._connection.cursor_blocked_reason,
            self._connection.cursor_contract_fingerprint,
            self._connection.cursor_blocked_at,
        ) = self._cursor_snapshot
        if self._apply_cursor_snapshot is not None:
            (
                self._connection.cursor_transient_failures,
                self._connection.cursor_retry_after_at,
                self._connection.cursor_cold_start_plan_id,
                self._connection.cursor_cold_start_plan_state,
                self._connection.cursor_last_attempt_at,
                self._connection.cursor_last_success_at,
                self._connection.cursor_updated_at,
            ) = self._apply_cursor_snapshot
        if self._apply_transitions_snapshot is not None:
            self._connection.apply_transitions = deepcopy(
                self._apply_transitions_snapshot
            )
        inbox_repository = getattr(self._connection, "inbox_repository", None)
        if inbox_repository is not None and self._inbox_snapshot is not None:
            inbox_repository.inserted = list(self._inbox_snapshot)
        self._connection.audits = deepcopy(self._audits_snapshot)

    async def __aexit__(
        self,
        exc_type: object,
        _exc: object,
        _traceback: object,
    ) -> None:
        if exc_type is not None:
            self._restore_snapshot()
        self._connection.events.append(
            "xid.rollback" if exc_type is not None else "xid.commit"
        )
        self._connection.info.transaction_status = TransactionStatus.IDLE
        if exc_type is None:
            self._connection.successful_commit_count += 1
            outcome = self._connection.unknown_commit_outcomes.pop(
                self._connection.successful_commit_count,
                None,
            )
            if outcome is not None:
                if outcome in {"pre", "mixed"}:
                    self._restore_snapshot()
                if outcome == "mixed":
                    next(iter(self._connection.plans.values()))["version"] = 1
                self._connection.events.append(f"xid.commit_unknown.{outcome}")
                error = getattr(
                    self._connection,
                    "unknown_commit_error",
                    RuntimeError("commit acknowledgement lost"),
                )
                self._connection.last_unknown_commit_error = error  # type: ignore[attr-defined]
                raise error


class _C1AcceptanceConnection:
    def __init__(
        self,
        *,
        cursor_status: str,
        cursor_value: str | None,
        cursor_version: int,
        folder_scope_config_hash: str,
    ) -> None:
        self.closed = False
        self.autocommit = True
        self.info = type(
            "_C1AcceptanceInfo",
            (),
            {"transaction_status": TransactionStatus.IDLE},
        )()
        self.cursor_status = cursor_status
        self.cursor_value = cursor_value
        self.cursor_version = cursor_version
        self.cursor_blocked_reason: str | None = None
        self.cursor_contract_fingerprint: str | None = None
        self.cursor_blocked_at: datetime | None = None
        self.ownership_pipeline_name = "pipeline-v2"
        self.ownership_generation = 3
        self.ownership_fencing_token = 9
        self.folder_scope_config_hash = folder_scope_config_hash
        self.cursor_missing = False
        self.plan_insert_error: BaseException | None = None
        self.insert_row_mutation: dict[str, object] = {}
        self.database_now = _CREATED_AT + timedelta(seconds=1)
        self.clock_read_count = 0
        self.expire_on_clock_read: int | None = None
        self.expire_on_preview_update = False
        self.expire_on_approve_update = False
        self.successful_commit_count = 0
        self.unknown_commit_outcomes: dict[int, str] = {}
        self.audit_insert_error: BaseException | None = None
        self.events: list[str] = []
        self.statements: list[tuple[str, object]] = []
        self.plans: dict[UUID, dict[str, object]] = {}
        self.audits: list[dict[str, object]] = []

    def transaction(self) -> _C1AcceptanceTransaction:
        return _C1AcceptanceTransaction(self)

    async def execute(
        self,
        statement: str,
        params: object = None,
    ) -> _C1AcceptanceCursor:
        assert self.info.transaction_status is TransactionStatus.INTRANS
        assert type(statement) is str
        self.statements.append((statement, params))
        if statement.startswith("SET LOCAL TRANSACTION"):
            self.events.append("xid.read_committed")
            return _C1AcceptanceCursor(None)
        if "set_config('lock_timeout'" in statement:
            self.events.append("xid.timeouts")
            return _C1AcceptanceCursor(None)
        if "pg_advisory_xact_lock_shared" in statement:
            self.events.append("ownership.shared_lock")
            return _C1AcceptanceCursor(None)
        if "FROM public.pipeline_ownership" in statement:
            self.events.append("ownership.current_ingress")
            return _C1AcceptanceCursor(
                {
                    "pipeline_name": self.ownership_pipeline_name,
                    "generation": self.ownership_generation,
                    "fencing_token": self.ownership_fencing_token,
                }
            )
        if "FROM public.sync_cursors" in statement and "FOR UPDATE" in statement:
            self.events.append("cursor.lock")
            if self.cursor_missing:
                return _C1AcceptanceCursor(None)
            return _C1AcceptanceCursor(
                {
                    "cursor": self.cursor_value,
                    "status": self.cursor_status,
                    "version": self.cursor_version,
                }
            )
        if statement == "SELECT pg_catalog.clock_timestamp() AS database_now":
            self.events.append("clock.read")
            self.clock_read_count += 1
            if self.clock_read_count == self.expire_on_clock_read:
                row = next(iter(self.plans.values()))
                self.database_now = row["expires_at"]  # type: ignore[assignment]
            return _C1AcceptanceCursor({"database_now": self.database_now})
        if statement.startswith(
            "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
            "INSERT INTO public.sync_cold_start_plans"
        ):
            self.events.append("plan.insert")
            if self.plan_insert_error is not None:
                raise self.plan_insert_error
            insert_columns = statement.split(") SELECT ", 1)[0]
            assert insert_columns.count("created_at") == 1
            assert insert_columns.count("updated_at") == 1
            assert statement.count("pg_catalog.clock_timestamp()") == 1
            assert "CURRENT_TIMESTAMP" not in statement
            assert "stamp.at + (%s * INTERVAL '1 second')" in statement
            assert statement.count("stamp.at") == 3
            assert "FROM stamp RETURNING" in statement
            assert type(params) is tuple and len(params) == 14
            (
                plan_id,
                account_id,
                folder_key,
                expected_cursor_status,
                expected_cursor,
                expected_cursor_version,
                pipeline_name,
                generation,
                fencing_token,
                contract_fingerprint,
                folder_scope_config_hash,
                actor,
                reason,
                plan_ttl_seconds,
            ) = params
            assert type(plan_id) is UUID
            assert plan_id not in self.plans
            assert plan_ttl_seconds == 86_400
            row: dict[str, object] = {
                "plan_id": plan_id,
                "account_id": account_id,
                "folder_key": folder_key,
                "expected_cursor_status": expected_cursor_status,
                "expected_cursor": expected_cursor,
                "expected_cursor_version": expected_cursor_version,
                "pipeline_name": pipeline_name,
                "generation": generation,
                "fencing_token": fencing_token,
                "state": "previewing",
                "version": 0,
                "preview_cursor": None,
                "preview_cursor_version": 0,
                "boundary_cursor": None,
                "boundary_cursor_version": None,
                "apply_cursor": None,
                "apply_cursor_version": None,
                "rolling_hash": None,
                "page_count": 0,
                "item_count": 0,
                "redacted_samples": [],
                "contract_fingerprint": contract_fingerprint,
                "folder_scope_config_hash": folder_scope_config_hash,
                "plan_hash": None,
                "actor": actor,
                "reason": reason,
                "blocked_reason_code": None,
                "blocked_fingerprint": None,
                "expires_at": _EXPIRES_AT,
                "ready_at": None,
                "approved_at": None,
                "completed_at": None,
                "blocked_at": None,
                "created_at": _CREATED_AT,
                "updated_at": _CREATED_AT,
            }
            row.update(self.insert_row_mutation)
            self.plans[plan_id] = row
            return _C1AcceptanceCursor(dict(row))
        if statement.startswith(
            "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
            "UPDATE public.sync_cold_start_plans AS plan SET state = 'approved'"
        ):
            self.events.append("plan.approve")
            assert type(params) is dict
            row = self.plans[params["plan_id"]]
            assert row["state"] == "ready"
            assert row["version"] == params["expected_version"]
            if self.expire_on_approve_update:
                self.database_now = row["expires_at"]  # type: ignore[assignment]
                return _C1AcceptanceCursor(None)
            if self.database_now >= row["expires_at"]:
                return _C1AcceptanceCursor(None)
            row.update(
                {
                    "state": "approved",
                    "version": row["version"] + 1,
                    "approved_at": self.database_now,
                    "updated_at": self.database_now,
                }
            )
            return _C1AcceptanceCursor(dict(row))
        if statement.startswith(
            "UPDATE public.sync_cold_start_plans AS plan SET state = 'blocked'"
        ):
            self.events.append("plan.block")
            assert type(params) is dict
            row = self.plans[params["plan_id"]]
            assert row["state"] == params["expected_state"]
            assert row["version"] == params["expected_version"]
            row.update(
                {
                    "state": "blocked",
                    "version": row["version"] + 1,
                    "blocked_reason_code": params["safe_code"],
                    "blocked_fingerprint": params["blocked_fingerprint"],
                    "blocked_at": params["blocked_at"],
                    "updated_at": params["blocked_at"],
                }
            )
            return _C1AcceptanceCursor(dict(row))
        if statement.startswith(
            "UPDATE public.sync_cursors AS cursor SET status = 'blocked_contract'"
        ):
            self.events.append("cursor.block")
            assert type(params) is dict
            assert self.cursor_status == params["expected_status"]
            assert self.cursor_value == params["expected_cursor"]
            assert self.cursor_version == params["expected_version"]
            self.cursor_status = "blocked_contract"
            self.cursor_version += 1
            self.cursor_blocked_reason = params["safe_code"]
            self.cursor_contract_fingerprint = params["blocked_fingerprint"]
            self.cursor_blocked_at = params["blocked_at"]
            return _C1AcceptanceCursor({"version": self.cursor_version})
        if statement.startswith("INSERT INTO public.audit_events"):
            approval = "'cold_start.approve'" in statement
            self.events.append("audit.approve" if approval else "audit.block")
            if self.audit_insert_error is not None:
                raise self.audit_insert_error
            assert type(params) is tuple and len(params) == 8
            self.audits.append(
                {
                    "id": params[0],
                    "event_key": params[1],
                    "account_id": params[2],
                    "object_fingerprint": params[3],
                    "result": params[4],
                    "actor": params[5],
                    "reason": params[6],
                    "safe_metadata": params[7].obj,
                    "created_at": self.database_now,
                }
            )
            return _C1AcceptanceCursor(None)
        if statement.startswith(
            "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
            "UPDATE public.sync_cold_start_plans AS plan SET"
        ):
            self.events.append("plan.preview_page")
            assert type(params) is dict
            plan_id = params["plan_id"]
            row = self.plans[plan_id]
            assert row["account_id"] == params["account_id"]
            assert row["folder_key"] == params["folder_key"]
            assert row["state"] == "previewing"
            assert row["version"] == params["expected_version"]
            assert row["preview_cursor"] == params["expected_preview_cursor"]
            assert (
                row["preview_cursor_version"]
                == params["expected_preview_cursor_version"]
            )
            assert row["rolling_hash"] == params["expected_rolling_hash"]
            assert row["page_count"] == params["expected_page_count"]
            assert row["item_count"] == params["expected_item_count"]
            if self.expire_on_preview_update:
                self.database_now = row["expires_at"]  # type: ignore[assignment]
                return _C1AcceptanceCursor(None)
            if self.database_now >= row["expires_at"]:
                return _C1AcceptanceCursor(None)
            samples = params["redacted_samples"]
            row.update(
                {
                    "state": params["target_state"],
                    "version": row["version"] + 1,
                    "preview_cursor": params["next_cursor"],
                    "preview_cursor_version": row["preview_cursor_version"] + 1,
                    "boundary_cursor": params["boundary_cursor"],
                    "boundary_cursor_version": params["boundary_cursor_version"],
                    "rolling_hash": params["rolling_hash"],
                    "page_count": row["page_count"] + 1,
                    "item_count": params["item_count"],
                    "redacted_samples": list(samples.obj),
                    "plan_hash": params["plan_hash"],
                    "ready_at": (
                        self.database_now if params["target_state"] == "ready" else None
                    ),
                    "updated_at": self.database_now,
                }
            )
            return _C1AcceptanceCursor(dict(row))
        if (
            "FROM public.sync_cold_start_plans" in statement
            and "WHERE plan_id = %s" in statement
        ):
            self.events.append("plan.read")
            assert type(params) is tuple and len(params) == 1
            return _C1AcceptanceCursor(self.plans.get(params[0]))
        if (
            "FROM public.sync_cold_start_plans" in statement
            and "state IN ('previewing', 'ready', 'approved')" in statement
        ):
            self.events.append("plan.open")
            row = next(iter(self.plans.values()), None)
            return _C1AcceptanceCursor(
                None if row is None else {"plan_id": row["plan_id"]}
            )
        raise AssertionError(f"unexpected C1 acceptance SQL: {statement}")


class _C1ReceiptTransaction:
    def __init__(self, repository: _C1ReceiptRepository) -> None:
        self._repository = repository

    def _require_xid(self) -> None:
        assert (
            self._repository.connection.info.transaction_status
            is TransactionStatus.INTRANS
        )

    async def lookup(
        self,
        *,
        account_id: int,
        command_name: str,
        idempotency_key: str,
        canonical_payload_hash: str,
    ) -> CommandReceipt | None:
        self._require_xid()
        self._repository.connection.events.append("receipt.lookup")
        if self._repository.lookup_error is not None:
            raise self._repository.lookup_error
        existing = self._repository.receipts.get(
            (account_id, command_name, idempotency_key)
        )
        if (
            existing is not None
            and existing.canonical_payload_hash != canonical_payload_hash
        ):
            raise IdempotencyConflict()
        return existing

    async def insert(
        self,
        *,
        account_id: int,
        command_name: str,
        idempotency_key: str,
        canonical_payload_hash: str,
        outcome: str,
        result_type: str,
        result_id: str,
        result_hash: str,
        authority_epoch: int,
    ) -> CommandReceipt:
        self._require_xid()
        if command_name == "cold_start.preview":
            expected_predecessor = "plan.insert"
        elif command_name == "cold_start.approve":
            expected_predecessor = "audit.approve"
        else:
            expected_predecessor = "plan.apply_page"
        assert self._repository.connection.events[-1] == expected_predecessor
        self._repository.connection.events.append("receipt.insert")
        if self._repository.insert_error is not None:
            raise self._repository.insert_error
        fault_hook = getattr(self._repository.connection, "_raise_body_fault", None)
        if fault_hook is not None:
            fault_hook("success.receipt_conflict")
        identity = (account_id, command_name, idempotency_key)
        receipt = CommandReceipt(
            id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            account_id=account_id,
            command_name=command_name,
            idempotency_key_hash=_hash_idempotency_key(*identity),
            canonical_payload_hash=canonical_payload_hash,
            outcome=outcome,
            result_type=result_type,
            result_id=result_id,
            result_hash=result_hash,
            authority_epoch=authority_epoch,
            created_at=_CREATED_AT,
        )
        if self._repository.insert_mutation:
            receipt = replace(receipt, **self._repository.insert_mutation)
        self._repository.receipts[identity] = receipt
        if fault_hook is not None:
            fault_hook("success.receipt_after_write")
        if (
            getattr(self._repository.connection, "body_fault_point", None)
            == "success.receipt_hostile_projection"
        ):
            return _C9HostileReceipt()  # type: ignore[return-value]
        return receipt


class _C1ReceiptRepository:
    def __init__(self, connection: _C1AcceptanceConnection) -> None:
        self.connection = connection
        self.receipts: dict[tuple[int, str, str], CommandReceipt] = {}
        self.insert_error: BaseException | None = None
        self.insert_mutation: dict[str, object] = {}
        self.lookup_error: BaseException | None = None
        connection.receipt_repository = self

    def transaction(self, connection: object) -> _C1ReceiptTransaction:
        assert connection is self.connection
        return _C1ReceiptTransaction(self)


@dataclass(frozen=True, slots=True)
class _C1RunnerOutcome:
    acquired: bool
    value: object


class _C1RetainedRunner:
    def __init__(self, connection: _C1AcceptanceConnection) -> None:
        self.session = _SyncSessionLease(connection)

    async def run(
        self,
        account_id: int,
        canonical_folder: str,
        operation: object,
    ) -> _C1RunnerOutcome:
        assert (account_id, canonical_folder) == (8, "INBOX")
        value = await operation(self.session)  # type: ignore[operator]
        return _C1RunnerOutcome(acquired=True, value=value)


def _c1_acceptance_service(
    *,
    cursor_status: str,
    cursor_value: str | None,
    cursor_version: int,
) -> tuple[
    ColdStartService,
    _C1AcceptanceConnection,
    _C1ReceiptRepository,
    list[str],
]:
    events: list[str] = []
    snapshot = _c1_snapshot()
    scope = snapshot.scopes[0]
    connection = _C1AcceptanceConnection(
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
        folder_scope_config_hash=scope.config_hash,
    )
    receipts = _C1ReceiptRepository(connection)
    service = ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            snapshot_provider=_C1SnapshotProvider(snapshot, events),
            policy_resolver=ProcessingPolicyResolver(),
            receipt_repository=receipts,
        )
    )
    service._session_runner = _C1RetainedRunner(connection)  # type: ignore[assignment]

    async def stop_before_c2(*_args: object, **_kwargs: object) -> ColdStartRunResult:
        connection.events.append("origin.probe")
        raise asyncio.CancelledError("stop before Batch 4C2")

    service._resume_preview_locked = stop_before_c2  # type: ignore[attr-defined]
    return service, connection, receipts, events


async def _c1_seed_accepted_preview(service: ColdStartService) -> None:
    with pytest.raises(asyncio.CancelledError, match="Batch 4C2"):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )


class _C2Origin:
    def __init__(
        self,
        outcomes: list[object],
        events: list[str],
    ) -> None:
        self.outcomes = outcomes
        self.events = events
        self.calls: list[tuple[int, str, str | None, int]] = []

    async def fetch_cold_start_page(
        self,
        account_id: int,
        sync_folder: str,
        cursor: str | None,
        limit: int,
    ) -> object:
        self.events.append("origin.fetch")
        self.calls.append((account_id, sync_folder, cursor, limit))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, asyncio.Event):
            await outcome.wait()
            raise AssertionError("blocked Origin unexpectedly resumed")
        if callable(outcome):
            outcome = outcome()
            if inspect.isawaitable(outcome):
                outcome = await outcome
        return outcome


class _C2SnapshotProvider:
    def __init__(self, snapshots: list[object], events: list[str]) -> None:
        self.snapshots = snapshots
        self.events = events

    async def get_ready_snapshot(self, account_id: int) -> object:
        assert account_id == 8
        self.events.append("snapshot")
        return self.snapshots.pop(0)


def _c2_preview_service(
    outcomes: list[object],
    *,
    preview_max_pages: int,
    preview_max_run_seconds: float = 20.0,
    snapshots: list[object] | None = None,
) -> tuple[
    ColdStartService,
    _C1AcceptanceConnection,
    _C1ReceiptRepository,
    _C2Origin,
]:
    snapshot = _c1_snapshot()
    scope = snapshot.scopes[0]
    connection = _C1AcceptanceConnection(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
        folder_scope_config_hash=scope.config_hash,
    )
    origin = _C2Origin(outcomes, connection.events)
    receipts = _C1ReceiptRepository(connection)
    snapshot_provider: object
    if snapshots is None:
        snapshot_provider = _C1SnapshotProvider(snapshot, connection.events)
    else:
        snapshot_provider = _C2SnapshotProvider(snapshots, connection.events)
    service = ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            cold_start_origin=origin,
            snapshot_provider=snapshot_provider,
            policy_resolver=ProcessingPolicyResolver(),
            receipt_repository=receipts,
            preview_max_pages=preview_max_pages,
            preview_max_run_seconds=preview_max_run_seconds,
        )
    )
    service._session_runner = _C1RetainedRunner(connection)  # type: ignore[assignment]
    return service, connection, receipts, origin


@pytest.mark.asyncio
async def test_preview_uses_plain_ownership_select_under_shared_advisory_xid() -> None:
    terminal = _vector_batch(
        cursor="opaque+Boundary/%3D",
        changes=(),
        includes_last=True,
    )
    service, connection, _receipts, _origin = _c2_preview_service(
        [terminal],
        preview_max_pages=2,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-plain-ownership",
    )

    assert result.status is ColdStartRunStatus.READY
    ownership_sql = [
        statement
        for statement, _params in connection.statements
        if "FROM public.pipeline_ownership" in statement
    ]
    assert len(ownership_sql) >= 3
    assert all("FOR KEY SHARE" not in statement for statement in ownership_sql)


class _C3Runner:
    def __init__(
        self,
        first: _C1AcceptanceConnection,
        second: _C1AcceptanceConnection,
    ) -> None:
        self.connections = [first, second]
        self.sessions: list[_SyncSessionLease] = []
        self.calls = 0
        self.shift_post_copy_timestamps = False
        self.before_recovery: object | None = None

    @staticmethod
    def _copy_durable_state(
        source: _C1AcceptanceConnection,
        target: _C1AcceptanceConnection,
    ) -> None:
        target.plans = {plan_id: dict(row) for plan_id, row in source.plans.items()}
        target.cursor_status = source.cursor_status
        target.cursor_value = source.cursor_value
        target.cursor_version = source.cursor_version
        target.cursor_blocked_reason = source.cursor_blocked_reason
        target.cursor_contract_fingerprint = source.cursor_contract_fingerprint
        target.cursor_blocked_at = source.cursor_blocked_at
        target.ownership_pipeline_name = source.ownership_pipeline_name
        target.ownership_generation = source.ownership_generation
        target.ownership_fencing_token = source.ownership_fencing_token
        target.database_now = source.database_now
        target.audits = [dict(audit) for audit in source.audits]

    async def run(
        self,
        account_id: int,
        canonical_folder: str,
        operation: object,
    ) -> _C1RunnerOutcome:
        assert (account_id, canonical_folder) == (8, "INBOX")
        assert self.calls < 2
        if self.calls == 1:
            self._copy_durable_state(self.connections[0], self.connections[1])
            if self.shift_post_copy_timestamps:
                row = next(iter(self.connections[1].plans.values()))
                ready_at = row["ready_at"]
                updated_at = row["updated_at"]
                assert type(ready_at) is datetime
                assert type(updated_at) is datetime
                shifted_at = updated_at + timedelta(microseconds=1)
                row["ready_at"] = shifted_at
                row["updated_at"] = shifted_at
            if callable(self.before_recovery):
                self.before_recovery()
        connection = self.connections[self.calls]
        self.calls += 1
        session = _SyncSessionLease(connection)
        self.sessions.append(session)
        value = await operation(session)  # type: ignore[operator]
        return _C1RunnerOutcome(acquired=True, value=value)


def _c3_preview_service(
    *,
    first_outcome: str,
    second_unknown: bool = False,
    batch: SyncBatch | None = None,
) -> tuple[
    ColdStartService,
    _C3Runner,
    _C1ReceiptRepository,
    _C2Origin,
]:
    if batch is None:
        batch = _vector_batch(
            cursor="opaque+Boundary/%3D",
            changes=(
                SyncChange(
                    kind=ChangeKind.CREATE,
                    external_email_id="message-1",
                    source_version="v1",
                    item={"subject": "one"},
                ),
            ),
            includes_last=True,
        )
    service, first, receipts, origin = _c2_preview_service(
        [batch],
        preview_max_pages=2,
    )
    scope = _c1_snapshot().scopes[0]
    second = _C1AcceptanceConnection(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
        folder_scope_config_hash=scope.config_hash,
    )
    first.unknown_commit_outcomes[3] = first_outcome
    if second_unknown:
        second.unknown_commit_outcomes[1] = "post"
    runner = _C3Runner(first, second)
    service._session_runner = runner  # type: ignore[assignment]
    return service, runner, receipts, origin


@pytest.mark.asyncio
@pytest.mark.parametrize("first_outcome", ["post", "pre"])
async def test_preview_unknown_commit_recovers_exact_post_or_pre_without_second_http(
    first_outcome: str,
) -> None:
    service, runner, receipts, origin = _c3_preview_service(
        first_outcome=first_outcome,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.READY
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert result.plan is not None and result.plan.page_count == 1
    assert runner.calls == 2
    assert runner.sessions[0].tainted is True
    assert runner.sessions[1].tainted is False
    assert origin.calls == [(8, "Inbox", None, 100)]
    final = next(iter(runner.connections[1].plans.values()))
    assert final["state"] == "ready"
    assert final["version"] == 1
    assert final["page_count"] == 1
    assert runner.connections[1].events.count("plan.preview_page") == (
        0 if first_outcome == "post" else 1
    )
    assert runner.connections[1].events.count("xid.read_committed") == 1
    assert len(receipts.receipts) == 1


@pytest.mark.asyncio
async def test_preview_exact_post_ack_recovery_ignores_later_ownership_fence() -> None:
    service, runner, receipts, origin = _c3_preview_service(
        first_outcome="post",
    )
    second = runner.connections[1]

    def advance_fence() -> None:
        second.ownership_fencing_token += 1

    runner.before_recovery = advance_fence

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.READY
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert runner.calls == 2
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert len(receipts.receipts) == 1
    assert second.events.count("plan.preview_page") == 0


@pytest.mark.asyncio
async def test_preview_unknown_commit_rejects_legal_but_nonidentical_post_projection() -> (
    None
):
    service, runner, _receipts, origin = _c3_preview_service(
        first_outcome="post",
    )
    runner.shift_post_copy_timestamps = True

    with pytest.raises(DatabaseOperationError) as caught:
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert caught.value.operation == "cold_start_preview_recovery"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start preview recovery state is invalid"
    assert runner.calls == 2
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert runner.connections[1].events.count("plan.preview_page") == 0
    assert runner.connections[1].events.count("xid.read_committed") == 1


@pytest.mark.asyncio
async def test_preview_unknown_commit_mixed_state_is_fixed_invariant_without_http() -> (
    None
):
    service, runner, _receipts, origin = _c3_preview_service(
        first_outcome="mixed",
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert caught.value.operation == "cold_start_preview_recovery"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start preview recovery state is invalid"
    assert runner.calls == 2
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert runner.connections[1].events.count("plan.preview_page") == 0
    assert runner.connections[1].audits == []


@pytest.mark.asyncio
async def test_preview_second_unknown_commit_propagates_without_third_attempt_or_http() -> (
    None
):
    service, runner, _receipts, origin = _c3_preview_service(
        first_outcome="pre",
        second_unknown=True,
    )

    with pytest.raises(RuntimeError, match="commit acknowledgement lost"):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert runner.calls == 2
    assert runner.sessions[0].tainted is True
    assert runner.sessions[1].tainted is True
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert runner.connections[1].events.count("plan.preview_page") == 1


@pytest.mark.asyncio
async def test_preview_unknown_nonterminal_commit_returns_progress_without_more_http() -> (
    None
):
    nonterminal = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=(),
        includes_last=False,
    )
    service, runner, _receipts, origin = _c3_preview_service(
        first_outcome="post",
        batch=nonterminal,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.PREVIEWING
    assert result.pages_committed == 1
    assert result.changes_observed == 0
    assert result.safe_code is None
    assert result.plan is not None
    assert result.plan.page_count == 1
    assert result.plan.boundary_cursor is None
    assert runner.calls == 2
    assert origin.calls == [(8, "Inbox", None, 100)]


@pytest.mark.asyncio
async def test_preview_first_nonterminal_page_starts_from_none_and_commits_progress() -> (
    None
):
    batch = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="message-1",
                source_version="v1",
                item={"subject": "one"},
            ),
        ),
        includes_last=False,
    )
    service, connection, receipts, origin = _c2_preview_service(
        [batch],
        preview_max_pages=1,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert result.safe_code == "cold_start.budget_exhausted"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.PREVIEWING
    assert result.plan.boundary_cursor is None
    assert result.plan.page_count == 1
    assert result.plan.item_count == 1
    assert result.plan.redacted_samples == (
        ColdStartSample(
            ChangeKind.CREATE,
            _sample_external_id_digest(8, "message-1"),
        ),
    )
    assert origin.calls == [(8, "Inbox", None, 100)]
    plan = next(iter(connection.plans.values()))
    assert plan["preview_cursor"] == "opaque+Page1/%3D"
    assert plan["preview_cursor_version"] == 1
    assert plan["rolling_hash"] == _preview_rolling_digest(
        None,
        _batch_digest(batch),
    )
    assert plan["boundary_cursor"] is None
    assert plan["plan_hash"] is None
    assert (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
    ) == (
        "cold_start_pending",
        None,
        2,
    )
    assert len(receipts.receipts) == 1
    assert connection.events.count("plan.preview_page") == 1


@pytest.mark.asyncio
async def test_preview_empty_terminal_page_seals_a_ready_boundary() -> None:
    batch = SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor="opaque+Boundary/%3D",
        changes=(),
        includes_last=True,
    )
    service, connection, _receipts, origin = _c2_preview_service(
        [batch],
        preview_max_pages=3,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.READY
    assert result.pages_committed == 1
    assert result.changes_observed == 0
    assert result.safe_code is None
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.READY
    assert result.plan.boundary_cursor == "opaque+Boundary/%3D"
    assert result.plan.page_count == 1
    assert result.plan.item_count == 0
    assert result.plan.redacted_samples == ()
    assert result.plan.ready_at == connection.database_now
    rolling_hash = _preview_rolling_digest(None, _batch_digest(batch))
    assert result.plan.plan_hash == _plan_digest(
        plan_id=result.plan.plan_id,
        account_id=8,
        canonical_folder="INBOX",
        expected_cursor_status=SyncCursorStatus.COLD_START_PENDING,
        expected_cursor_version=2,
        expected_cursor_hash=None,
        pipeline_name="pipeline-v2",
        generation=3,
        fencing_token=9,
        boundary_cursor_hash=_cursor_digest("opaque+Boundary/%3D"),
        boundary_cursor_version=1,
        rolling_hash=rolling_hash,
        page_count=1,
        item_count=0,
        redacted_samples=(),
        contract_fingerprint="e" * 64,
        folder_scope_config_hash=result.plan.folder_scope_config_hash,
        actor="operator",
        reason="review history",
        created_at=_CREATED_AT,
        expires_at=_EXPIRES_AT,
    )
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert connection.events.count("plan.preview_page") == 1


@pytest.mark.asyncio
async def test_preview_multipage_chain_caps_samples_and_accepts_terminal_same_cursor() -> (
    None
):
    def changes(start: int, count: int) -> tuple[SyncChange, ...]:
        return tuple(
            SyncChange(
                kind=(ChangeKind.DELETE if index % 3 == 0 else ChangeKind.CREATE),
                external_email_id=f"message-{index}",
                source_version=None if index % 3 == 0 else f"v{index}",
                item=None if index % 3 == 0 else {"index": index},
            )
            for index in range(start, start + count)
        )

    first = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=changes(0, 12),
        includes_last=False,
    )
    terminal = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=changes(12, 12),
        includes_last=True,
    )
    service, connection, _receipts, origin = _c2_preview_service(
        [first, terminal],
        preview_max_pages=3,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.READY
    assert result.pages_committed == 2
    assert result.changes_observed == 24
    assert result.plan is not None
    assert result.plan.page_count == 2
    assert result.plan.item_count == 24
    assert result.plan.boundary_cursor == "opaque+Page1/%3D"
    expected_samples = tuple(
        ColdStartSample(
            change.kind,
            _sample_external_id_digest(8, change.external_email_id),
        )
        for change in (*first.changes, *terminal.changes)[:20]
    )
    assert result.plan.redacted_samples == expected_samples
    first_rolling = _preview_rolling_digest(None, _batch_digest(first))
    final_rolling = _preview_rolling_digest(
        first_rolling,
        _batch_digest(terminal),
    )
    plan = next(iter(connection.plans.values()))
    assert plan["rolling_hash"] == final_rolling
    assert plan["preview_cursor_version"] == 2
    assert plan["boundary_cursor_version"] == 2
    assert origin.calls == [
        (8, "Inbox", None, 100),
        (8, "Inbox", "opaque+Page1/%3D", 100),
    ]
    assert connection.events.count("plan.preview_page") == 2
    assert (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
    ) == (
        "cold_start_pending",
        None,
        2,
    )


@pytest.mark.asyncio
async def test_preview_transient_propagates_identical_error_without_mutation() -> None:
    transient = SyncTransientError(retry_after_seconds=7)
    service, connection, receipts, origin = _c2_preview_service(
        [transient],
        preview_max_pages=3,
    )

    with pytest.raises(SyncTransientError) as caught:
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert caught.value is transient
    plan = next(iter(connection.plans.values()))
    assert plan["state"] == "previewing"
    assert plan["version"] == 0
    assert plan["page_count"] == 0
    assert len(receipts.receipts) == 1
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert "plan.preview_page" not in connection.events


@pytest.mark.asyncio
async def test_preview_http_timeout_returns_budget_without_cancelling_database_xid() -> (
    None
):
    never = asyncio.Event()
    service, connection, receipts, origin = _c2_preview_service(
        [never],
        preview_max_pages=3,
        preview_max_run_seconds=0.01,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == 0
    assert result.changes_observed == 0
    assert result.safe_code == "cold_start.budget_exhausted"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.PREVIEWING
    assert len(receipts.receipts) == 1
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert connection.info.transaction_status is TransactionStatus.IDLE
    assert connection.events[-1] == "origin.fetch"
    assert "plan.preview_page" not in connection.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "safe_code"),
    [
        (
            SyncAuthorizationError(),
            "exchange.sync.authorization_failed",
        ),
        (SyncCursorInvalidError(), "exchange.sync.cursor_invalid"),
        (SyncContractError(), "exchange.sync.contract_invalid"),
        (object(), "sync.local_contract_invalid"),
    ],
)
async def test_preview_fatal_origin_or_local_contract_blocks_plan_and_cursor(
    outcome: object,
    safe_code: str,
) -> None:
    service, connection, receipts, origin = _c2_preview_service(
        [outcome],
        preview_max_pages=3,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.pages_committed == 0
    assert result.changes_observed == 0
    assert result.safe_code == safe_code
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    assert result.plan.page_count == 0
    assert result.plan.boundary_cursor is None
    expected_fingerprint = _blocked_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=result.plan.plan_id,
        safe_code=safe_code,
    )
    assert result.plan.blocked_fingerprint == expected_fingerprint
    assert result.plan.blocked_reason_code == safe_code
    assert connection.cursor_status == "blocked_contract"
    assert connection.cursor_value is None
    assert connection.cursor_version == 3
    assert connection.cursor_blocked_reason == safe_code
    assert connection.cursor_contract_fingerprint == expected_fingerprint
    assert connection.cursor_blocked_at == result.plan.blocked_at
    assert len(receipts.receipts) == 1
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert len(connection.audits) == 1
    assert connection.audits[0]["reason"] == safe_code
    assert connection.audits[0]["safe_metadata"] == {
        "plan_id": str(result.plan.plan_id),
        "safe_code": safe_code,
    }
    block_audit_statement, block_audit_params = next(
        (statement, params)
        for statement, params in connection.statements
        if statement.startswith("INSERT INTO public.audit_events")
        and "'cold_start.block'" in statement
    )
    assert "created_at" not in block_audit_statement.split(") VALUES ", 1)[0]
    assert type(block_audit_params) is tuple and len(block_audit_params) == 8
    assert (
        connection.events.index("plan.block")
        < connection.events.index("cursor.block")
        < connection.events.index("audit.block")
    )


@pytest.mark.asyncio
async def test_preview_nonterminal_same_cursor_blocks_and_preserves_prior_progress() -> (
    None
):
    first = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="message-1",
                source_version="v1",
                item={"subject": "one"},
            ),
        ),
        includes_last=False,
    )
    stalled = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=(),
        includes_last=False,
    )
    service, connection, _receipts, origin = _c2_preview_service(
        [first, stalled],
        preview_max_pages=3,
    )

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert result.safe_code == "sync.cursor_stalled"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    assert result.plan.page_count == 1
    assert result.plan.item_count == 1
    assert result.plan.boundary_cursor is None
    assert result.plan.plan_hash is None
    assert result.plan.redacted_samples == (
        ColdStartSample(
            ChangeKind.CREATE,
            _sample_external_id_digest(8, "message-1"),
        ),
    )
    assert origin.calls == [
        (8, "Inbox", None, 100),
        (8, "Inbox", "opaque+Page1/%3D", 100),
    ]
    plan = next(iter(connection.plans.values()))
    assert plan["version"] == 2
    assert plan["preview_cursor"] == "opaque+Page1/%3D"
    assert plan["rolling_hash"] == _preview_rolling_digest(
        None,
        _batch_digest(first),
    )
    assert connection.events.count("plan.preview_page") == 1
    assert connection.events.count("plan.block") == 1


@pytest.mark.asyncio
async def test_preview_expiry_blocks_before_origin_request() -> None:
    terminal = _empty_batch()
    service, connection, _receipts, origin = _c2_preview_service(
        [terminal],
        preview_max_pages=3,
    )
    connection.database_now = _EXPIRES_AT

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == "cold_start.expired"
    assert result.pages_committed == result.changes_observed == 0
    assert result.plan is not None
    assert result.plan.blocked_at == _EXPIRES_AT
    assert origin.calls == []
    assert connection.cursor_status == "blocked_contract"
    assert connection.events.count("plan.block") == 1
    assert "plan.preview_page" not in connection.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drift", "safe_code", "expected_plan_version", "expected_cursor_version"),
    [
        ("config", "cold_start.config_drift", 1, 3),
        ("fence", "cold_start.fence_drift", 1, 3),
        ("cursor", "cold_start.cursor_drift", 1, 4),
        ("version", "cold_start.version_drift", 2, 3),
    ],
)
async def test_preview_post_http_drift_blocks_without_committing_stale_page(
    drift: str,
    safe_code: str,
    expected_plan_version: int,
    expected_cursor_version: int,
) -> None:
    terminal = _empty_batch()
    snapshots: list[object] | None = None
    if drift == "config":
        changed_scope = FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("changed-inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=_c1_policy_matrix(),
        )
        snapshots = [_c1_snapshot(), PolicySnapshot(scopes=(changed_scope,))]
    service, connection, _receipts, origin = _c2_preview_service(
        [terminal],
        preview_max_pages=3,
        snapshots=snapshots,
    )

    if drift != "config":

        def mutate_after_http() -> SyncBatch:
            if drift == "fence":
                connection.ownership_fencing_token += 1
            elif drift == "cursor":
                connection.cursor_version += 1
            else:
                next(iter(connection.plans.values()))["version"] = 1
            return terminal

        origin.outcomes[0] = mutate_after_http

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == safe_code
    assert result.pages_committed == result.changes_observed == 0
    assert result.plan is not None
    assert result.plan.page_count == 0
    assert result.plan.boundary_cursor is None
    assert result.plan.plan_hash is None
    plan = next(iter(connection.plans.values()))
    assert plan["version"] == expected_plan_version
    assert connection.cursor_version == expected_cursor_version
    assert connection.cursor_status == "blocked_contract"
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert "plan.preview_page" not in connection.events
    assert connection.events.count("plan.block") == 1
    assert connection.events.count("audit.block") == 1


def _install_c2_locator(
    service: ColdStartService,
    connection: _C1AcceptanceConnection,
) -> None:
    async def locate(plan_id: UUID) -> object:
        connection.events.append("locator")
        row = connection.plans.get(plan_id)
        if row is None:
            raise ColdStartPlanNotFoundError()
        return cold_start_module._LocatedPlanIdentity(  # noqa: SLF001
            plan_id=plan_id,
            account_id=8,
            canonical_folder="INBOX",
        )

    service._locate_plan_identity = locate  # type: ignore[method-assign]


@pytest.mark.asyncio
async def test_resume_continues_from_durable_preview_cursor_with_fresh_counts() -> None:
    first = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="message-1",
                source_version="v1",
                item={"subject": "one"},
            ),
        ),
        includes_last=False,
    )
    terminal = _vector_batch(
        cursor="opaque+Boundary/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.DELETE,
                external_email_id="message-2",
                source_version=None,
                item=None,
            ),
        ),
        includes_last=True,
    )
    service, connection, _receipts, origin = _c2_preview_service(
        [first, terminal],
        preview_max_pages=1,
    )

    initial = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )
    assert initial.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert initial.plan is not None
    _install_c2_locator(service, connection)
    event_count = len(connection.events)

    resumed = await service.resume(initial.plan.plan_id)

    assert resumed.status is ColdStartRunStatus.READY
    assert resumed.pages_committed == 1
    assert resumed.changes_observed == 1
    assert resumed.plan is not None
    assert resumed.plan.page_count == 2
    assert resumed.plan.item_count == 2
    assert resumed.plan.boundary_cursor == "opaque+Boundary/%3D"
    assert origin.calls == [
        (8, "Inbox", None, 100),
        (8, "Inbox", "opaque+Page1/%3D", 100),
    ]
    resume_events = connection.events[event_count:]
    assert resume_events[0] == "locator"
    assert resume_events.index("locator") < resume_events.index("snapshot")


@pytest.mark.asyncio
async def test_resume_rejects_invalid_plan_id_before_locator_or_policy() -> None:
    service, connection, _receipts, _origin = _c2_preview_service(
        [],
        preview_max_pages=1,
    )
    _install_c2_locator(service, connection)

    with pytest.raises(ValueError):
        await service.resume("12345678-1234-5678-1234-567812345678")  # type: ignore[arg-type]

    assert connection.events == []


@pytest.mark.asyncio
async def test_preview_replay_after_progress_returns_current_view_without_origin() -> (
    None
):
    first = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=(),
        includes_last=False,
    )
    unused = _empty_batch()
    service, connection, _receipts, origin = _c2_preview_service(
        [first, unused],
        preview_max_pages=1,
    )
    initial = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )
    assert initial.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    connection.cursor_missing = True

    replay = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert replay.status is ColdStartRunStatus.PREVIEWING
    assert replay.pages_committed == replay.changes_observed == 0
    assert replay.plan is not None
    assert replay.plan.page_count == 1
    assert replay.plan.boundary_cursor is None
    assert origin.calls == [(8, "Inbox", None, 100)]


@pytest.mark.asyncio
async def test_preview_post_http_expiry_blocks_response_before_page_commit() -> None:
    terminal = _empty_batch()
    service, connection, _receipts, origin = _c2_preview_service(
        [terminal],
        preview_max_pages=2,
    )

    def expire_after_http() -> SyncBatch:
        connection.database_now = _EXPIRES_AT
        return terminal

    origin.outcomes[0] = expire_after_http

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == "cold_start.expired"
    assert result.pages_committed == result.changes_observed == 0
    assert result.plan is not None and result.plan.page_count == 0
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert "plan.preview_page" not in connection.events


@pytest.mark.asyncio
async def test_preview_expiry_at_update_cas_blocks_in_same_post_http_xid() -> None:
    terminal = _empty_batch()
    service, connection, _receipts, origin = _c2_preview_service(
        [terminal],
        preview_max_pages=2,
    )
    connection.expire_on_preview_update = True

    result = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == "cold_start.expired"
    assert result.pages_committed == result.changes_observed == 0
    assert result.plan is not None and result.plan.page_count == 0
    assert connection.cursor_status == "blocked_contract"
    assert connection.events.count("plan.preview_page") == 1
    assert connection.events.count("plan.block") == 1
    assert connection.events.count("audit.block") == 1
    assert origin.calls == [(8, "Inbox", None, 100)]


@pytest.mark.asyncio
async def test_preview_post_http_policy_unavailable_preserves_open_plan() -> None:
    terminal = _empty_batch()
    service, connection, receipts, origin = _c2_preview_service(
        [terminal],
        preview_max_pages=2,
        snapshots=[_c1_snapshot(), PolicySnapshot.failed()],
    )

    with pytest.raises(PolicySnapshotUnavailableError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    plan = next(iter(connection.plans.values()))
    assert plan["state"] == "previewing"
    assert plan["version"] == 0
    assert plan["page_count"] == 0
    assert connection.cursor_status == "cold_start_pending"
    assert connection.cursor_version == 2
    assert len(receipts.receipts) == 1
    assert connection.audits == []
    assert origin.calls == [(8, "Inbox", None, 100)]


@pytest.mark.asyncio
async def test_preview_origin_cancellation_is_not_converted_or_persisted() -> None:
    cancellation = asyncio.CancelledError("operator cancelled preview")
    service, connection, receipts, origin = _c2_preview_service(
        [cancellation],
        preview_max_pages=2,
    )

    with pytest.raises(asyncio.CancelledError) as caught:
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert caught.value is cancellation
    plan = next(iter(connection.plans.values()))
    assert plan["state"] == "previewing"
    assert plan["version"] == 0
    assert len(receipts.receipts) == 1
    assert connection.audits == []
    assert origin.calls == [(8, "Inbox", None, 100)]
    assert connection.info.transaction_status is TransactionStatus.IDLE


@pytest.mark.asyncio
async def test_resume_ready_plan_is_state_conflict_without_origin() -> None:
    terminal = _empty_batch()
    service, connection, _receipts, origin = _c2_preview_service(
        [terminal],
        preview_max_pages=2,
    )
    ready = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )
    assert ready.plan is not None
    _install_c2_locator(service, connection)

    with pytest.raises(ColdStartStateConflictError):
        await service.resume(ready.plan.plan_id)

    assert origin.calls == [(8, "Inbox", None, 100)]


async def _c4_ready_service() -> tuple[
    ColdStartService,
    _C1AcceptanceConnection,
    _C1ReceiptRepository,
    _C2Origin,
    ColdStartPlanView,
]:
    terminal = _vector_batch(
        cursor="opaque+Boundary/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="message-1",
                source_version="v1",
                item={"subject": "one"},
            ),
        ),
        includes_last=True,
    )
    service, connection, receipts, origin = _c2_preview_service(
        [terminal],
        preview_max_pages=2,
    )
    ready = await service.preview(
        8,
        "INBOX",
        actor="preview-operator",
        reason="review historical mail",
        idempotency_key="preview-key",
    )
    assert ready.status is ColdStartRunStatus.READY
    assert ready.plan is not None
    _install_c2_locator(service, connection)
    return service, connection, receipts, origin, ready.plan


@pytest.mark.asyncio
async def test_approve_ready_plan_commits_audit_receipt_and_replays_without_live_cursor() -> (
    None
):
    service, connection, receipts, origin, ready = await _c4_ready_service()

    approved = await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )

    assert approved.status is ColdStartRunStatus.APPROVED
    assert approved.pages_committed == approved.changes_observed == 0
    assert approved.safe_code is None
    assert approved.plan is not None
    assert approved.plan.state is ColdStartPlanState.APPROVED
    assert approved.plan.approved_at == connection.database_now
    assert approved.plan.boundary_cursor == ready.boundary_cursor
    assert approved.plan.plan_hash == ready.plan_hash
    approval_receipt = receipts.receipts[(8, "cold_start.approve", "approve-key")]
    assert approval_receipt.authority_epoch == 9
    assert approval_receipt.result_id == str(ready.plan_id)
    assert approval_receipt.result_hash == _approve_result_digest(
        plan_id=ready.plan_id,
        plan_hash=ready.plan_hash,  # type: ignore[arg-type]
        pipeline_name="pipeline-v2",
        generation=3,
        fencing_token=9,
        folder_scope_config_hash=ready.folder_scope_config_hash,
        approved_at=connection.database_now,
    )
    assert len(connection.audits) == 1
    assert connection.audits[0]["actor"] == "approver"
    assert connection.audits[0]["reason"] == "approved historical suppression"
    assert connection.audits[0]["safe_metadata"] == {
        "plan_id": str(ready.plan_id),
        "plan_hash": ready.plan_hash,
        "page_count": 1,
        "item_count": 1,
        "redacted_samples": [
            {
                "kind": "create",
                "external_email_id_hash": ready.redacted_samples[
                    0
                ].external_email_id_hash,
            }
        ],
    }
    approval_audit_statement, approval_audit_params = next(
        (statement, params)
        for statement, params in connection.statements
        if statement.startswith("INSERT INTO public.audit_events")
        and "'cold_start.approve'" in statement
    )
    assert "created_at" not in approval_audit_statement.split(") VALUES ", 1)[0]
    assert type(approval_audit_params) is tuple and len(approval_audit_params) == 8
    origin_calls = list(origin.calls)
    approval_audits = len(connection.audits)
    approval_inserts = connection.events.count("receipt.insert")
    connection.cursor_missing = True

    replay = await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )

    assert replay == approved
    assert origin.calls == origin_calls
    assert len(connection.audits) == approval_audits
    assert connection.events.count("receipt.insert") == approval_inserts


@pytest.mark.asyncio
async def test_approve_new_key_after_approval_is_state_conflict_without_new_audit() -> (
    None
):
    service, connection, receipts, _origin, ready = await _c4_ready_service()
    await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )

    with pytest.raises(ColdStartStateConflictError):
        await service.approve(
            ready.plan_id,
            actor="approver",
            reason="approved historical suppression",
            idempotency_key="different-key",
        )

    assert len(connection.audits) == 1
    assert len(receipts.receipts) == 2


@pytest.mark.asyncio
async def test_approve_previewing_plan_is_state_conflict_without_http_or_audit() -> (
    None
):
    nonterminal = _vector_batch(
        cursor="opaque+Page1/%3D",
        changes=(),
        includes_last=False,
    )
    service, connection, _receipts, origin = _c2_preview_service(
        [nonterminal],
        preview_max_pages=1,
    )
    previewing = await service.preview(
        8,
        "INBOX",
        actor="preview-operator",
        reason="review historical mail",
        idempotency_key="preview-key",
    )
    assert previewing.plan is not None
    _install_c2_locator(service, connection)

    with pytest.raises(ColdStartStateConflictError):
        await service.approve(
            previewing.plan.plan_id,
            actor="approver",
            reason="approved historical suppression",
            idempotency_key="approve-key",
        )

    assert origin.calls == [(8, "Inbox", None, 100)]
    assert connection.audits == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drift", "safe_code", "expected_cursor_version"),
    [
        ("expired", "cold_start.expired", 3),
        ("config", "cold_start.config_drift", 3),
        ("fence", "cold_start.fence_drift", 3),
        ("cursor", "cold_start.cursor_drift", 4),
        ("version", "cold_start.version_drift", 3),
        ("plan_hash", "cold_start.plan_hash_drift", 3),
    ],
)
async def test_approve_drift_blocks_both_rows_without_success_receipt(
    drift: str,
    safe_code: str,
    expected_cursor_version: int,
) -> None:
    service, connection, receipts, origin, ready = await _c4_ready_service()
    if drift == "expired":
        connection.database_now = _EXPIRES_AT
    elif drift == "config":
        changed_scope = FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("changed-inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=_c1_policy_matrix(),
        )
        service._snapshot_provider = _C1SnapshotProvider(  # type: ignore[assignment]
            PolicySnapshot(scopes=(changed_scope,)),
            connection.events,
        )
    elif drift == "fence":
        connection.ownership_fencing_token = 10
    elif drift == "cursor":
        connection.cursor_version = 3
    elif drift == "version":
        next(iter(connection.plans.values()))["version"] = 41
    else:
        next(iter(connection.plans.values()))["plan_hash"] = "f" * 64

    result = await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == safe_code
    assert result.pages_committed == result.changes_observed == 0
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    assert result.plan.boundary_cursor == ready.boundary_cursor
    assert result.plan.page_count == ready.page_count
    assert result.plan.approved_at is None
    assert connection.cursor_status == "blocked_contract"
    assert connection.cursor_version == expected_cursor_version
    assert len(receipts.receipts) == 1
    assert (8, "cold_start.approve", "approve-key") not in receipts.receipts
    assert len(connection.audits) == 1
    assert connection.audits[0]["reason"] == safe_code
    assert origin.calls == [(8, "Inbox", None, 100)]


@pytest.mark.asyncio
async def test_approve_expiry_at_update_cas_blocks_in_same_xid() -> None:
    service, connection, receipts, _origin, ready = await _c4_ready_service()
    connection.expire_on_approve_update = True

    result = await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == "cold_start.expired"
    assert result.plan is not None and result.plan.approved_at is None
    assert connection.events.count("plan.approve") == 1
    assert connection.events.count("plan.block") == 1
    assert connection.events.count("audit.block") == 1
    assert len(receipts.receipts) == 1


@pytest.mark.asyncio
async def test_approve_key_payload_mismatch_preserves_idempotency_conflict() -> None:
    service, connection, receipts, _origin, ready = await _c4_ready_service()
    await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )
    connection.cursor_missing = True

    with pytest.raises(IdempotencyConflict):
        await service.approve(
            ready.plan_id,
            actor="approver",
            reason="different reason",
            idempotency_key="approve-key",
        )

    assert len(receipts.receipts) == 2
    assert len(connection.audits) == 1


@pytest.mark.asyncio
async def test_approve_replay_rejects_tampered_sealed_plan_projection() -> None:
    service, connection, _receipts, _origin, ready = await _c4_ready_service()
    await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )
    row = next(iter(connection.plans.values()))
    row["preview_cursor"] = "opaque+Tampered/%3D"
    row["boundary_cursor"] = "opaque+Tampered/%3D"

    with pytest.raises(ColdStartStateConflictError):
        await service.approve(
            ready.plan_id,
            actor="approver",
            reason="approved historical suppression",
            idempotency_key="approve-key",
        )

    assert len(connection.audits) == 1


@pytest.mark.asyncio
async def test_approve_rejects_all_input_before_locator() -> None:
    service, connection, _receipts, _origin = _c2_preview_service(
        [],
        preview_max_pages=1,
    )
    _install_c2_locator(service, connection)

    with pytest.raises(ValueError):
        await service.approve(
            _PLAN_ID,
            actor=" approver",
            reason="approved historical suppression",
            idempotency_key="approve-key",
        )

    assert connection.events == []


@pytest.mark.asyncio
async def test_approve_replay_rejects_hostile_receipt_before_comparison() -> None:
    class _HostileInt(int):
        def __eq__(self, _other: object) -> bool:
            raise AssertionError("hostile approval receipt comparison executed")

    service, connection, receipts, _origin, ready = await _c4_ready_service()
    await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )
    identity = (8, "cold_start.approve", "approve-key")
    receipts.receipts[identity] = replace(
        receipts.receipts[identity],
        authority_epoch=_HostileInt(9),
    )

    with pytest.raises(ColdStartStateConflictError):
        await service.approve(
            ready.plan_id,
            actor="approver",
            reason="approved historical suppression",
            idempotency_key="approve-key",
        )

    assert len(connection.audits) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["audit", "receipt_conflict", "receipt_tamper"])
async def test_approve_fault_rolls_back_plan_audit_and_receipt(fault: str) -> None:
    service, connection, receipts, _origin, ready = await _c4_ready_service()
    if fault == "audit":
        connection.audit_insert_error = RuntimeError("audit unavailable")
    elif fault == "receipt_conflict":
        receipts.insert_error = IdempotencyConflict()
    else:
        receipts.insert_mutation["authority_epoch"] = 10

    expected_error = {
        "audit": RuntimeError,
        "receipt_conflict": IdempotencyConflict,
        "receipt_tamper": ColdStartStateConflictError,
    }[fault]
    with pytest.raises(expected_error):
        await service.approve(
            ready.plan_id,
            actor="approver",
            reason="approved historical suppression",
            idempotency_key="approve-key",
        )

    plan = next(iter(connection.plans.values()))
    assert plan["state"] == "ready"
    assert plan["version"] == 1
    assert plan["approved_at"] is None
    assert connection.audits == []
    assert set(receipts.receipts) == {(8, "cold_start.preview", "preview-key")}


@pytest.mark.asyncio
async def test_approve_commit_ack_loss_replays_one_receipt_and_audit() -> None:
    service, connection, receipts, _origin, ready = await _c4_ready_service()
    connection.unknown_commit_outcomes[4] = "post"

    with pytest.raises(RuntimeError, match="commit acknowledgement lost"):
        await service.approve(
            ready.plan_id,
            actor="approver",
            reason="approved historical suppression",
            idempotency_key="approve-key",
        )

    assert next(iter(connection.plans.values()))["state"] == "approved"
    assert len(connection.audits) == 1
    assert len(receipts.receipts) == 2
    service._session_runner = _C1RetainedRunner(connection)  # type: ignore[assignment]
    connection.cursor_missing = True

    replay = await service.approve(
        ready.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )

    assert replay.status is ColdStartRunStatus.APPROVED
    assert len(connection.audits) == 1
    assert len(receipts.receipts) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_status", "cursor_value", "cursor_version"),
    [
        ("cold_start_pending", None, 2),
        ("reset_required", "opaque+Old/%3D", 7),
    ],
)
async def test_preview_acceptance_and_same_key_replay_are_exactly_once(
    cursor_status: str,
    cursor_value: str | None,
    cursor_version: int,
) -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
    )

    await _c1_seed_accepted_preview(service)

    assert len(connection.plans) == 1
    plan_id, plan = next(iter(connection.plans.items()))
    assert plan["expected_cursor_status"] == cursor_status
    assert plan["expected_cursor"] == cursor_value
    assert plan["expected_cursor_version"] == cursor_version
    assert plan["created_at"] == plan["updated_at"] == _CREATED_AT
    assert plan["expires_at"] == _EXPIRES_AT
    assert connection.events.index("cursor.lock") < connection.events.index(
        "plan.insert"
    )
    assert connection.events.index("plan.insert") < connection.events.index(
        "receipt.insert"
    )
    insert_statement = next(
        statement
        for statement, _params in connection.statements
        if statement.startswith(
            "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
            "INSERT INTO public.sync_cold_start_plans"
        )
    )
    insert_columns = insert_statement.split(") SELECT ", 1)[0]
    assert insert_columns.count("created_at") == 1
    assert insert_columns.count("updated_at") == 1
    assert insert_statement.count("pg_catalog.clock_timestamp()") == 1
    assert "CURRENT_TIMESTAMP" not in insert_statement
    assert "stamp.at + (%s * INTERVAL '1 second')" in insert_statement
    assert insert_statement.count("stamp.at") == 3
    assert "FROM stamp RETURNING" in insert_statement

    receipt = receipts.receipts[(8, "cold_start.preview", "preview-key")]
    expected_cursor_hash = (
        None if cursor_value is None else _cursor_digest(cursor_value)
    )
    assert receipt.result_id == str(plan_id)
    assert receipt.authority_epoch == 9
    assert receipt.result_hash == _preview_result_digest(
        plan_id=plan_id,
        account_id=8,
        canonical_folder="INBOX",
        expected_cursor_status=SyncCursorStatus(cursor_status),
        expected_cursor_version=cursor_version,
        expected_cursor_hash=expected_cursor_hash,
        pipeline_name="pipeline-v2",
        generation=3,
        fencing_token=9,
        contract_fingerprint="e" * 64,
        folder_scope_config_hash=plan["folder_scope_config_hash"],  # type: ignore[arg-type]
        created_at=_CREATED_AT,
        expires_at=_EXPIRES_AT,
    )
    origin_calls = connection.events.count("origin.probe")
    ownership_reads = connection.events.count("ownership.current_ingress")
    cursor_locks = connection.events.count("cursor.lock")
    connection.cursor_missing = True

    replay = await service.preview(
        8,
        "INBOX",
        actor="operator",
        reason="review history",
        idempotency_key="preview-key",
    )

    assert replay.status is ColdStartRunStatus.PREVIEWING
    assert replay.plan is not None and replay.plan.plan_id == plan_id
    assert replay.pages_committed == replay.changes_observed == 0
    assert replay.safe_code is None
    assert connection.events.count("origin.probe") == origin_calls
    assert connection.events.count("ownership.current_ingress") == ownership_reads
    assert connection.events.count("cursor.lock") == cursor_locks
    assert connection.events.count("plan.insert") == 1
    assert connection.events.count("receipt.insert") == 1


@pytest.mark.asyncio
async def test_preview_payload_mismatch_preserves_idempotency_conflict() -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    await _c1_seed_accepted_preview(service)

    with pytest.raises(IdempotencyConflict):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="different review",
            idempotency_key="preview-key",
        )

    assert len(connection.plans) == 1
    assert len(receipts.receipts) == 1
    assert connection.events.count("origin.probe") == 1


@pytest.mark.asyncio
async def test_preview_different_key_conflicts_with_existing_open_plan() -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="reset_required",
        cursor_value="opaque+Old/%3D",
        cursor_version=7,
    )
    await _c1_seed_accepted_preview(service)

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="different-key",
        )

    assert len(connection.plans) == 1
    assert len(receipts.receipts) == 1
    assert connection.events.count("origin.probe") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_status", "cursor_value"),
    [
        ("active", "opaque+Current/%3D"),
        ("blocked_contract", None),
        ("cold_start_applying", "opaque+Applying/%3D"),
        ("cold_start_pending", "opaque+Unexpected/%3D"),
        ("reset_required", None),
    ],
)
async def test_preview_rejects_every_ineligible_cursor_without_origin_or_writes(
    cursor_status: str,
    cursor_value: str | None,
) -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=2,
    )

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.plans == {}
    assert receipts.receipts == {}
    assert "origin.probe" not in connection.events
    assert connection.events[-1] == "xid.rollback"


@pytest.mark.asyncio
async def test_preview_missing_cursor_row_is_state_conflict_before_receipt() -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    connection.cursor_missing = True

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.plans == {}
    assert receipts.receipts == {}
    assert connection.events.count("receipt.lookup") == 1
    assert "origin.probe" not in connection.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_status", "cursor_version"),
    [("unknown", 2), ("cold_start_pending", True)],
)
async def test_preview_malformed_cursor_row_is_fixed_database_error(
    cursor_status: str,
    cursor_version: object,
) -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status=cursor_status,
        cursor_value=None,
        cursor_version=cursor_version,  # type: ignore[arg-type]
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert caught.value.operation == "cold_start_cursor_row"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start cursor row is invalid"
    assert connection.plans == {}
    assert receipts.receipts == {}
    assert "origin.probe" not in connection.events


@pytest.mark.asyncio
async def test_preview_hostile_open_plan_row_is_fixed_database_error() -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    await _c1_seed_accepted_preview(service)
    next(iter(connection.plans.values()))["plan_id"] = "not-a-uuid"

    with pytest.raises(DatabaseOperationError) as caught:
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="different-key",
        )

    assert caught.value.operation == "cold_start_plan_row"
    assert caught.value.retryable is False
    assert connection.events.count("origin.probe") == 1
    assert len(receipts.receipts) == 1


@pytest.mark.asyncio
async def test_preview_unique_plan_race_maps_to_public_state_conflict() -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    connection.plan_insert_error = UniqueViolation("do not expose database detail")

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.plans == {}
    assert receipts.receipts == {}
    assert "origin.probe" not in connection.events


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [3, True])
async def test_preview_rejects_tampered_inserted_plan_row_before_origin(
    value: object,
) -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    connection.insert_row_mutation["expected_cursor_version"] = value

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.plans == {}
    assert receipts.receipts == {}
    assert "origin.probe" not in connection.events


@pytest.mark.asyncio
async def test_preview_rejects_tampered_inserted_receipt_before_origin() -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    receipts.insert_mutation["authority_epoch"] = 10

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.plans == {}
    assert receipts.receipts == {}
    assert "origin.probe" not in connection.events


@pytest.mark.asyncio
async def test_preview_receipt_insert_race_preserves_idempotency_conflict() -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    receipts.insert_error = IdempotencyConflict()

    with pytest.raises(IdempotencyConflict):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.plans == {}
    assert receipts.receipts == {}
    assert "origin.probe" not in connection.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "not-a-uuid"),
        ("account_id", 9),
        ("command_name", "cold_start.approve"),
        ("idempotency_key_hash", "0" * 64),
        ("canonical_payload_hash", "1" * 64),
        ("outcome", "failed"),
        ("result_type", "other"),
        ("result_id", "87654321-4321-8765-4321-876543218765"),
        ("result_hash", "2" * 64),
        ("authority_epoch", 10),
        (
            "created_at",
            datetime(
                2026,
                7,
                15,
                9,
                2,
                3,
                456789,
                tzinfo=timezone(timedelta(hours=8)),
            ),
        ),
    ],
)
async def test_preview_replay_rejects_every_tampered_receipt_field(
    field: str,
    value: object,
) -> None:
    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    await _c1_seed_accepted_preview(service)
    identity = (8, "cold_start.preview", "preview-key")
    receipts.receipts[identity] = replace(
        receipts.receipts[identity],
        **{field: value},
    )

    expected_error = (
        IdempotencyConflict
        if field == "canonical_payload_hash"
        else ColdStartStateConflictError
    )
    with pytest.raises(expected_error):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.events.count("origin.probe") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["account_id", "authority_epoch"])
async def test_preview_replay_rejects_hostile_receipt_before_comparison(
    field: str,
) -> None:
    class _HostileInt(int):
        def __eq__(self, _other: object) -> bool:
            raise AssertionError("hostile receipt comparison executed")

    service, connection, receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    await _c1_seed_accepted_preview(service)
    identity = (8, "cold_start.preview", "preview-key")
    receipts.receipts[identity] = replace(
        receipts.receipts[identity],
        **{field: _HostileInt(8 if field == "account_id" else 9)},
    )

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.events.count("origin.probe") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("expected_cursor_version", 3),
        ("pipeline_name", "pipeline-v3"),
        ("generation", 4),
        ("fencing_token", 10),
        ("contract_fingerprint", "a" * 64),
        ("folder_scope_config_hash", "b" * 64),
        ("actor", "different-operator"),
        ("reason", "different reason"),
        ("expires_at", _EXPIRES_AT + timedelta(seconds=1)),
    ],
)
async def test_preview_replay_rejects_tampered_accepted_plan_projection(
    field: str,
    value: object,
) -> None:
    service, connection, _receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    await _c1_seed_accepted_preview(service)
    plan = next(iter(connection.plans.values()))
    plan[field] = value

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.events.count("origin.probe") == 1


@pytest.mark.asyncio
async def test_preview_replay_maps_malformed_plan_row_to_public_state_conflict() -> (
    None
):
    service, connection, _receipts, _events = _c1_acceptance_service(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
    )
    await _c1_seed_accepted_preview(service)
    plan = next(iter(connection.plans.values()))
    plan["expected_cursor_version"] = True

    with pytest.raises(ColdStartStateConflictError):
        await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )

    assert connection.events.count("origin.probe") == 1


class _C5ApplyConnection(_C1AcceptanceConnection):
    def __init__(
        self,
        *,
        cursor_status: str,
        cursor_value: str | None,
        cursor_version: int,
        folder_scope_config_hash: str,
    ) -> None:
        super().__init__(
            cursor_status=cursor_status,
            cursor_value=cursor_value,
            cursor_version=cursor_version,
            folder_scope_config_hash=folder_scope_config_hash,
        )
        self.cursor_transient_failures = 0
        self.cursor_retry_after_at: datetime | None = None
        self.cursor_cold_start_plan_id: UUID | None = None
        self.cursor_cold_start_plan_state: str | None = None
        self.cursor_last_attempt_at: datetime | None = None
        self.cursor_last_success_at: datetime | None = None
        self.cursor_updated_at = _CREATED_AT
        self.apply_transitions: list[dict[str, object]] = []
        self.cursor_apply_delay = 0.0

    def _raise_body_fault(self, point: str) -> None:
        if getattr(self, "body_fault_point", None) != point:
            return
        error = getattr(self, "body_fault_error", None)
        assert isinstance(error, BaseException)
        raise error

    async def execute(
        self,
        statement: str,
        params: object = None,
    ) -> _C1AcceptanceCursor:
        if statement.startswith(
            "SELECT cursor, status, version, blocked_reason_code, "
        ):
            assert self.info.transaction_status is TransactionStatus.INTRANS
            self.statements.append((statement, params))
            self.events.append("cursor.apply_lock")
            if self.cursor_missing:
                return _C1AcceptanceCursor(None)
            return _C1AcceptanceCursor(
                {
                    "cursor": self.cursor_value,
                    "status": self.cursor_status,
                    "version": self.cursor_version,
                    "blocked_reason_code": self.cursor_blocked_reason,
                    "contract_fingerprint": self.cursor_contract_fingerprint,
                    "blocked_at": self.cursor_blocked_at,
                    "transient_failures": self.cursor_transient_failures,
                    "retry_after_at": self.cursor_retry_after_at,
                    "cold_start_plan_id": self.cursor_cold_start_plan_id,
                    "cold_start_plan_state": self.cursor_cold_start_plan_state,
                    "last_attempt_at": self.cursor_last_attempt_at,
                    "last_success_at": self.cursor_last_success_at,
                    "updated_at": self.cursor_updated_at,
                }
            )
        if statement.startswith(
            "UPDATE public.sync_cursors AS cursor SET "
            "transient_failures = %(failure_count)s"
        ):
            assert self.info.transaction_status is TransactionStatus.INTRANS
            self.statements.append((statement, params))
            self.events.append("cursor.apply_retry")
            assert type(params) is dict
            assert self.cursor_status == params["expected_status"]
            assert self.cursor_value == params["expected_cursor"]
            assert self.cursor_version == params["expected_version"]
            assert self.cursor_blocked_reason == params["expected_blocked_reason_code"]
            assert (
                self.cursor_contract_fingerprint
                == params["expected_contract_fingerprint"]
            )
            assert self.cursor_blocked_at == params["expected_blocked_at"]
            assert self.cursor_transient_failures == params["expected_failures"]
            assert self.cursor_retry_after_at == params["expected_retry_after_at"]
            assert self.cursor_cold_start_plan_id == params["expected_plan_id"]
            assert self.cursor_cold_start_plan_state == params["expected_plan_state"]
            assert self.cursor_last_attempt_at == params["expected_last_attempt_at"]
            assert self.cursor_last_success_at == params["expected_last_success_at"]
            assert self.cursor_updated_at == params["expected_updated_at"]
            stamp = params["database_stamp"]
            self.cursor_transient_failures = params["failure_count"]  # type: ignore[assignment]
            self.cursor_retry_after_at = stamp + timedelta(  # type: ignore[operator]
                seconds=params["retry_delay_seconds"]  # type: ignore[arg-type]
            )
            self.cursor_last_attempt_at = stamp  # type: ignore[assignment]
            self.cursor_updated_at = stamp  # type: ignore[assignment]
            self._raise_body_fault("retry.cursor_after_mutation")
            return _C1AcceptanceCursor(
                {
                    "cursor": self.cursor_value,
                    "status": self.cursor_status,
                    "version": self.cursor_version,
                    "blocked_reason_code": self.cursor_blocked_reason,
                    "contract_fingerprint": self.cursor_contract_fingerprint,
                    "blocked_at": self.cursor_blocked_at,
                    "transient_failures": self.cursor_transient_failures,
                    "retry_after_at": self.cursor_retry_after_at,
                    "cold_start_plan_id": self.cursor_cold_start_plan_id,
                    "cold_start_plan_state": self.cursor_cold_start_plan_state,
                    "last_attempt_at": self.cursor_last_attempt_at,
                    "last_success_at": self.cursor_last_success_at,
                    "updated_at": self.cursor_updated_at,
                }
            )
        if (
            statement.startswith(
                "UPDATE public.sync_cursors AS cursor SET status = 'blocked_contract'"
            )
            and type(params) is dict
            and "expected_transient_failures" in params
        ):
            assert self.info.transaction_status is TransactionStatus.INTRANS
            self.statements.append((statement, params))
            self.events.append("cursor.block")
            assert self.cursor_status == params["expected_status"]
            assert self.cursor_value == params["expected_cursor"]
            assert self.cursor_version == params["expected_version"]
            assert self.cursor_blocked_reason == params["expected_blocked_reason_code"]
            assert (
                self.cursor_contract_fingerprint
                == params["expected_contract_fingerprint"]
            )
            assert self.cursor_blocked_at == params["expected_blocked_at"]
            assert (
                self.cursor_transient_failures == params["expected_transient_failures"]
            )
            assert self.cursor_retry_after_at == params["expected_retry_after_at"]
            assert self.cursor_cold_start_plan_id == params["expected_plan_id"]
            assert self.cursor_cold_start_plan_state == params["expected_plan_state"]
            assert self.cursor_last_attempt_at == params["expected_last_attempt_at"]
            assert self.cursor_last_success_at == params["expected_last_success_at"]
            assert self.cursor_updated_at == params["expected_updated_at"]
            stamp = params["blocked_at"]
            self.cursor_status = "blocked_contract"
            self.cursor_version += 1
            self.cursor_blocked_reason = params["safe_code"]
            self.cursor_contract_fingerprint = params["blocked_fingerprint"]
            self.cursor_blocked_at = stamp  # type: ignore[assignment]
            self.cursor_transient_failures = 0
            self.cursor_retry_after_at = None
            self.cursor_cold_start_plan_id = None
            self.cursor_cold_start_plan_state = None
            self.cursor_last_attempt_at = stamp  # type: ignore[assignment]
            self.cursor_updated_at = stamp  # type: ignore[assignment]
            self._raise_body_fault("block.cursor_after_mutation")
            return _C1AcceptanceCursor(
                {
                    "cursor": self.cursor_value,
                    "status": self.cursor_status,
                    "version": self.cursor_version,
                    "blocked_reason_code": self.cursor_blocked_reason,
                    "contract_fingerprint": self.cursor_contract_fingerprint,
                    "blocked_at": self.cursor_blocked_at,
                    "transient_failures": self.cursor_transient_failures,
                    "retry_after_at": self.cursor_retry_after_at,
                    "cold_start_plan_id": self.cursor_cold_start_plan_id,
                    "cold_start_plan_state": self.cursor_cold_start_plan_state,
                    "last_attempt_at": self.cursor_last_attempt_at,
                    "last_success_at": self.cursor_last_success_at,
                    "updated_at": self.cursor_updated_at,
                }
            )
        if statement.startswith(
            "UPDATE public.sync_cursors AS cursor SET cursor = %(next_cursor)s"
        ):
            if self.cursor_apply_delay:
                await asyncio.sleep(self.cursor_apply_delay)
            assert self.info.transaction_status is TransactionStatus.INTRANS
            self.statements.append((statement, params))
            self.events.append("cursor.apply_page")
            assert type(params) is dict
            assert self.cursor_status == params["expected_status"]
            assert self.cursor_value == params["expected_cursor"]
            assert self.cursor_version == params["expected_version"]
            assert self.cursor_blocked_reason == params["expected_blocked_reason_code"]
            assert (
                self.cursor_contract_fingerprint
                == params["expected_contract_fingerprint"]
            )
            assert self.cursor_blocked_at == params["expected_blocked_at"]
            assert (
                self.cursor_transient_failures == params["expected_transient_failures"]
            )
            assert self.cursor_retry_after_at == params["expected_retry_after_at"]
            assert self.cursor_cold_start_plan_id == params["expected_plan_id"]
            assert self.cursor_cold_start_plan_state == params["expected_plan_state"]
            stamp = params["database_stamp"]
            self.cursor_value = params["next_cursor"]  # type: ignore[assignment]
            self.cursor_status = params["target_status"]  # type: ignore[assignment]
            self.cursor_version += 1
            self.cursor_blocked_reason = None
            self.cursor_contract_fingerprint = None
            self.cursor_blocked_at = None
            self.cursor_transient_failures = 0
            self.cursor_retry_after_at = None
            self.cursor_cold_start_plan_id = params["target_plan_id"]  # type: ignore[assignment]
            self.cursor_cold_start_plan_state = params["target_plan_state"]  # type: ignore[assignment]
            self.cursor_last_attempt_at = stamp  # type: ignore[assignment]
            self.cursor_last_success_at = stamp  # type: ignore[assignment]
            self.cursor_updated_at = stamp  # type: ignore[assignment]
            self._raise_body_fault("success.cursor_after_mutation")
            return _C1AcceptanceCursor(
                {
                    "cursor": self.cursor_value,
                    "status": self.cursor_status,
                    "version": self.cursor_version,
                    "blocked_reason_code": self.cursor_blocked_reason,
                    "contract_fingerprint": self.cursor_contract_fingerprint,
                    "blocked_at": self.cursor_blocked_at,
                    "transient_failures": self.cursor_transient_failures,
                    "retry_after_at": self.cursor_retry_after_at,
                    "cold_start_plan_id": self.cursor_cold_start_plan_id,
                    "cold_start_plan_state": self.cursor_cold_start_plan_state,
                    "last_attempt_at": self.cursor_last_attempt_at,
                    "last_success_at": self.cursor_last_success_at,
                    "updated_at": self.cursor_updated_at,
                }
            )
        if statement.startswith(
            "UPDATE public.sync_cold_start_plans AS plan SET state = %(target_state)s"
        ):
            assert self.info.transaction_status is TransactionStatus.INTRANS
            self.statements.append((statement, params))
            self.events.append("plan.apply_page")
            assert type(params) is dict
            row = self.plans[params["plan_id"]]
            assert row["state"] == "approved"
            assert row["version"] == params["expected_version"]
            assert row["apply_cursor"] == params["expected_apply_cursor"]
            assert (
                row["apply_cursor_version"] == params["expected_apply_cursor_version"]
            )
            row.update(
                {
                    "state": params["target_state"],
                    "version": row["version"] + 1,
                    "apply_cursor": params["next_cursor"],
                    "apply_cursor_version": params["next_cursor_version"],
                    "completed_at": (
                        params["database_stamp"]
                        if params["target_state"] == "completed"
                        else None
                    ),
                    "updated_at": params["database_stamp"],
                }
            )
            self.apply_transitions.append(
                {
                    "cursor_status": self.cursor_status,
                    "cursor_version": self.cursor_version,
                    "cursor_plan_id": self.cursor_cold_start_plan_id,
                    "plan_state": row["state"],
                    "plan_version": row["version"],
                    "apply_cursor": row["apply_cursor"],
                    "apply_cursor_version": row["apply_cursor_version"],
                    "cursor_stamp": self.cursor_updated_at,
                    "plan_stamp": row["updated_at"],
                }
            )
            if getattr(self, "body_fault_point", None) == "success.plan_bad_projection":
                return _C1AcceptanceCursor({"invalid": "plan projection"})
            self._raise_body_fault("success.plan_after_mutation")
            return _C1AcceptanceCursor(dict(row))
        result = await super().execute(statement, params)
        if statement.startswith(
            "UPDATE public.sync_cold_start_plans AS plan SET state = 'blocked'"
        ):
            self._raise_body_fault("block.plan_after_mutation")
        if statement.startswith("INSERT INTO public.audit_events"):
            self._raise_body_fault("block.audit_after_append")
        return result


class _C5OrdinaryClient:
    def __init__(self, outcomes: list[object], events: list[str]) -> None:
        self.outcomes = outcomes
        self.events = events
        self.calls: list[tuple[int, str, str, int]] = []

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> object:
        assert cursor is not None
        self.events.append("ordinary.fetch")
        self.calls.append((account_id, folder, cursor, limit))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, asyncio.Event):
            await outcome.wait()
            raise AssertionError("blocked ordinary request unexpectedly resumed")
        if callable(outcome):
            outcome = outcome()
            if inspect.isawaitable(outcome):
                outcome = await outcome
        return outcome


class _C5InboxTransaction:
    def __init__(self, repository: _C5InboxRepository) -> None:
        self._repository = repository

    async def insert(
        self,
        event: object,
        generation: int,
        fencing_token: int,
    ) -> None:
        assert (
            self._repository.connection.info.transaction_status
            is TransactionStatus.INTRANS
        )
        self._repository.connection.events.append("inbox.insert")
        self._repository.inserted.append((event, generation, fencing_token))
        self._repository.connection._raise_body_fault(  # type: ignore[attr-defined]
            "success.inbox_after_append"
        )


class _C5InboxRepository:
    def __init__(self, connection: _C5ApplyConnection) -> None:
        self.connection = connection
        self.inserted: list[tuple[object, int, int]] = []
        self.ownership_lock_modes: list[bool] = []
        connection.inbox_repository = self

    def transaction(
        self,
        connection: object,
        *,
        for_key_share: bool = True,
    ) -> _C5InboxTransaction:
        assert connection is self.connection
        assert type(for_key_share) is bool
        assert self.connection.info.transaction_status is TransactionStatus.INTRANS
        self.ownership_lock_modes.append(for_key_share)
        self.connection.events.append(
            "inbox.bind.locked" if for_key_share else "inbox.bind.plain"
        )
        return _C5InboxTransaction(self)


class _C5BusyRunner:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def run(
        self,
        account_id: int,
        canonical_folder: str,
        _operation: object,
    ) -> _C1RunnerOutcome:
        assert (account_id, canonical_folder) == (8, "INBOX")
        self.events.append("permit")
        return _C1RunnerOutcome(acquired=False, value=None)


async def _c5_approved_service(
    outcomes: list[object],
    *,
    apply_max_pages: int = 5,
    apply_max_run_seconds: float = 10.0,
    cursor_status: str = "cold_start_pending",
    cursor_value: str | None = None,
    cursor_version: int = 2,
) -> tuple[
    ColdStartService,
    _C5ApplyConnection,
    _C1ReceiptRepository,
    _C5OrdinaryClient,
    _C5InboxRepository,
    ColdStartPlanView,
]:
    snapshot = _c1_snapshot()
    scope = snapshot.scopes[0]
    connection = _C5ApplyConnection(
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
        folder_scope_config_hash=scope.config_hash,
    )
    if cursor_status == "cold_start_pending":
        connection.cursor_blocked_reason = "cold_start.pending"
    elif cursor_status == "reset_required":
        connection.cursor_blocked_reason = "exchange.sync.cursor_invalid"
        connection.cursor_last_attempt_at = _CREATED_AT
    preview_terminal = _vector_batch(
        cursor="opaque+Boundary/%3D",
        changes=(),
        includes_last=True,
    )
    origin = _C2Origin([preview_terminal], connection.events)
    ordinary = _C5OrdinaryClient(outcomes, connection.events)
    receipts = _C1ReceiptRepository(connection)
    inbox = _C5InboxRepository(connection)
    service = ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            cold_start_origin=origin,
            ordinary_page_client=ordinary,
            snapshot_provider=_C1SnapshotProvider(snapshot, connection.events),
            policy_resolver=ProcessingPolicyResolver(),
            inbox_repository=inbox,
            receipt_repository=receipts,
            preview_max_pages=2,
            apply_max_pages=apply_max_pages,
            apply_max_run_seconds=apply_max_run_seconds,
        )
    )
    service._session_runner = _C1RetainedRunner(connection)  # type: ignore[assignment]
    ready = await service.preview(
        8,
        "INBOX",
        actor="preview-operator",
        reason="review historical mail",
        idempotency_key="preview-key",
    )
    assert ready.plan is not None
    _install_c2_locator(service, connection)
    approved = await service.approve(
        ready.plan.plan_id,
        actor="approver",
        reason="approved historical suppression",
        idempotency_key="approve-key",
    )
    assert approved.plan is not None
    connection.database_now += timedelta(microseconds=1)
    connection.events.clear()
    return service, connection, receipts, ordinary, inbox, approved.plan


@pytest.mark.asyncio
async def test_c5_transaction_rollback_restores_every_durable_apply_field() -> None:
    (
        service,
        connection,
        receipts,
        _ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([])
    del service
    expected_plan = dict(connection.plans[approved.plan_id])
    expected_cursor = (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
        connection.cursor_blocked_reason,
        connection.cursor_contract_fingerprint,
        connection.cursor_blocked_at,
        connection.cursor_transient_failures,
        connection.cursor_retry_after_at,
        connection.cursor_cold_start_plan_id,
        connection.cursor_cold_start_plan_state,
        connection.cursor_last_attempt_at,
        connection.cursor_last_success_at,
        connection.cursor_updated_at,
    )
    expected_receipts = dict(receipts.receipts)
    expected_inbox = list(inbox.inserted)
    expected_audits = [dict(audit) for audit in connection.audits]
    expected_apply_transitions = [
        dict(transition) for transition in connection.apply_transitions
    ]

    with pytest.raises(RuntimeError, match="rollback fixture"):
        async with connection.transaction():
            connection.plans[approved.plan_id]["version"] = 999
            connection.cursor_status = "cold_start_applying"
            connection.cursor_value = "opaque+Mutated/%3D"
            connection.cursor_version = 999
            connection.cursor_blocked_reason = "mutated"
            connection.cursor_contract_fingerprint = "f" * 64
            connection.cursor_blocked_at = connection.database_now
            connection.cursor_transient_failures = 9
            connection.cursor_retry_after_at = connection.database_now
            connection.cursor_cold_start_plan_id = approved.plan_id
            connection.cursor_cold_start_plan_state = "approved"
            connection.cursor_last_attempt_at = connection.database_now
            connection.cursor_last_success_at = connection.database_now
            connection.cursor_updated_at = connection.database_now
            connection.apply_transitions.append({"mutated": True})
            receipts.receipts[(8, "cold_start.apply_page", "mutated")] = next(
                iter(receipts.receipts.values())
            )
            inbox.inserted.append((object(), 99, 99))
            connection.audits.append({"mutated": True})
            raise RuntimeError("rollback fixture")

    assert connection.plans[approved.plan_id] == expected_plan
    assert (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
        connection.cursor_blocked_reason,
        connection.cursor_contract_fingerprint,
        connection.cursor_blocked_at,
        connection.cursor_transient_failures,
        connection.cursor_retry_after_at,
        connection.cursor_cold_start_plan_id,
        connection.cursor_cold_start_plan_state,
        connection.cursor_last_attempt_at,
        connection.cursor_last_success_at,
        connection.cursor_updated_at,
    ) == expected_cursor
    assert receipts.receipts == expected_receipts
    assert inbox.inserted == expected_inbox
    assert connection.audits == expected_audits
    assert connection.apply_transitions == expected_apply_transitions


class _C6HostileTransient(SyncTransientError):
    descriptor_reads = 0

    def __init__(self) -> None:
        RuntimeError.__init__(self, "hostile transient")

    def __getattribute__(self, name: str) -> object:
        if name in {
            "safe_code",
            "retry_after",
            "retry_after_seconds",
            "__dict__",
        }:
            type(self).descriptor_reads += 1
            raise AssertionError("hostile transient descriptor was read")
        return super().__getattribute__(name)

    def __str__(self) -> str:
        type(self).descriptor_reads += 1
        raise AssertionError("hostile transient text was read")

    def __repr__(self) -> str:
        type(self).descriptor_reads += 1
        raise AssertionError("hostile transient representation was read")


class _C7HostileFatalMixin:
    descriptor_reads = 0

    def __init__(self) -> None:
        RuntimeError.__init__(self, "hostile fatal")

    def __getattribute__(self, name: str) -> object:
        if name in {
            "safe_code",
            "safe_summary",
            "kind",
            "retryable",
            "__dict__",
        }:
            type(self).descriptor_reads += 1
            raise AssertionError("hostile fatal descriptor was read")
        return super().__getattribute__(name)

    def __str__(self) -> str:
        type(self).descriptor_reads += 1
        raise AssertionError("hostile fatal text was read")

    def __repr__(self) -> str:
        type(self).descriptor_reads += 1
        raise AssertionError("hostile fatal representation was read")


class _C7HostileAuthorization(_C7HostileFatalMixin, SyncAuthorizationError):
    pass


class _C7HostileCursorInvalid(_C7HostileFatalMixin, SyncCursorInvalidError):
    pass


class _C7HostileContract(_C7HostileFatalMixin, SyncContractError):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_status", "cursor_value", "cursor_version"),
    [
        ("cold_start_pending", None, 2),
        ("reset_required", "opaque+Old/%3D", 7),
    ],
)
@pytest.mark.parametrize(
    "error",
    [SyncTransientError(retry_after_seconds=17), _C6HostileTransient()],
    ids=["exact-base", "hostile-subclass"],
)
async def test_apply_prebinding_transient_propagates_same_object_without_mutation(
    cursor_status: str,
    cursor_value: str | None,
    cursor_version: int,
    error: SyncTransientError,
) -> None:
    _C6HostileTransient.descriptor_reads = 0
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service(
        [error],
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
    )
    expected_plan = dict(connection.plans[approved.plan_id])
    expected_cursor = (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
        connection.cursor_transient_failures,
        connection.cursor_retry_after_at,
        connection.cursor_cold_start_plan_id,
        connection.cursor_cold_start_plan_state,
        connection.cursor_last_attempt_at,
        connection.cursor_last_success_at,
        connection.cursor_updated_at,
    )
    expected_receipts = dict(receipts.receipts)
    expected_audits = [dict(audit) for audit in connection.audits]

    with pytest.raises(SyncTransientError) as caught:
        await service.apply(approved.plan_id)

    assert caught.value is error
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert connection.plans[approved.plan_id] == expected_plan
    assert (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
        connection.cursor_transient_failures,
        connection.cursor_retry_after_at,
        connection.cursor_cold_start_plan_id,
        connection.cursor_cold_start_plan_state,
        connection.cursor_last_attempt_at,
        connection.cursor_last_success_at,
        connection.cursor_updated_at,
    ) == expected_cursor
    assert receipts.receipts == expected_receipts
    assert inbox.inserted == []
    assert connection.audits == expected_audits
    assert _C6HostileTransient.descriptor_reads == 0


async def _c6_applying_service(
    outcomes: list[object],
    *,
    apply_max_pages: int = 1,
) -> tuple[
    ColdStartService,
    _C5ApplyConnection,
    _C1ReceiptRepository,
    _C5OrdinaryClient,
    _C5InboxRepository,
    ColdStartPlanView,
]:
    first = _vector_batch(
        cursor="opaque+Apply1/%3D",
        changes=(),
        includes_last=False,
    )
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([first], apply_max_pages=1)
    bound = await service.apply(approved.plan_id)
    assert bound.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert bound.plan is not None
    assert connection.cursor_status == "cold_start_applying"
    assert connection.cursor_value == first.cursor
    assert connection.cursor_version == 3
    assert connection.cursor_cold_start_plan_id == approved.plan_id
    ordinary.outcomes.extend(outcomes)
    ordinary.calls.clear()
    connection.events.clear()
    connection.apply_transitions.clear()
    connection.database_now += timedelta(microseconds=1)
    service._apply_max_pages = apply_max_pages
    return service, connection, receipts, ordinary, inbox, bound.plan


@pytest.mark.asyncio
async def test_apply_applying_transient_schedules_defers_at_db_time_and_clears() -> (
    None
):
    transient = SyncTransientError(retry_after_seconds=17)
    terminal = _vector_batch(
        cursor="opaque+Active/%3D",
        changes=(),
        includes_last=True,
    )
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([transient, terminal])
    expected_plan = dict(connection.plans[applying.plan_id])
    expected_receipts = dict(receipts.receipts)
    expected_audits = [dict(audit) for audit in connection.audits]
    previous_last_success_at = connection.cursor_last_success_at
    retry_stamp = connection.database_now
    expected_delay = _deterministic_retry_delay(
        account_id=8,
        canonical_folder="INBOX",
        expected_version=3,
        failure_count=1,
        retry_after_seconds=17,
    )

    scheduled = await service.apply(applying.plan_id)

    assert scheduled.status is ColdStartRunStatus.RETRY_SCHEDULED
    assert scheduled.plan == applying
    assert scheduled.pages_committed == scheduled.changes_observed == 0
    assert scheduled.safe_code == "exchange.sync.transient_failure"
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert connection.plans[applying.plan_id] == expected_plan
    assert connection.cursor_status == "cold_start_applying"
    assert connection.cursor_value == "opaque+Apply1/%3D"
    assert connection.cursor_version == 3
    assert connection.cursor_cold_start_plan_id == applying.plan_id
    assert connection.cursor_cold_start_plan_state == "approved"
    assert connection.cursor_transient_failures == 1
    assert connection.cursor_retry_after_at == retry_stamp + timedelta(
        seconds=expected_delay
    )
    assert connection.cursor_last_attempt_at == retry_stamp
    assert connection.cursor_updated_at == retry_stamp
    assert connection.cursor_last_success_at == previous_last_success_at
    assert receipts.receipts == expected_receipts
    assert inbox.inserted == []
    assert connection.audits == expected_audits

    deferred_state = (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
        connection.cursor_transient_failures,
        connection.cursor_retry_after_at,
        connection.cursor_last_attempt_at,
        connection.cursor_last_success_at,
        connection.cursor_updated_at,
    )
    deferred = await service.apply(applying.plan_id)

    assert deferred.status is ColdStartRunStatus.RETRY_DEFERRED
    assert deferred.plan == applying
    assert deferred.pages_committed == deferred.changes_observed == 0
    assert deferred.safe_code == "cold_start.retry_deferred"
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
        connection.cursor_transient_failures,
        connection.cursor_retry_after_at,
        connection.cursor_last_attempt_at,
        connection.cursor_last_success_at,
        connection.cursor_updated_at,
    ) == deferred_state

    retry_boundary = connection.cursor_retry_after_at
    assert retry_boundary is not None
    connection.database_now = retry_boundary
    completed = await service.apply(applying.plan_id)

    assert completed.status is ColdStartRunStatus.COMPLETED
    assert completed.pages_committed == 1
    assert completed.changes_observed == 0
    assert ordinary.calls == [
        (8, "Inbox", "opaque+Apply1/%3D", 100),
        (8, "Inbox", "opaque+Apply1/%3D", 100),
    ]
    assert connection.cursor_status == "active"
    assert connection.cursor_value == terminal.cursor
    assert connection.cursor_version == 4
    assert connection.cursor_transient_failures == 0
    assert connection.cursor_retry_after_at is None
    assert connection.cursor_last_attempt_at == retry_boundary
    assert connection.cursor_last_success_at == retry_boundary
    assert connection.cursor_updated_at == retry_boundary
    assert connection.cursor_cold_start_plan_id is None
    assert connection.cursor_cold_start_plan_state is None
    assert len(receipts.receipts) == len(expected_receipts) + 1


@pytest.mark.asyncio
async def test_apply_applying_transient_preserves_progress_from_same_invocation() -> (
    None
):
    committed = _vector_batch(
        cursor="opaque+Apply2/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="post-boundary-2",
                source_version="v2",
                item={"subject": "two"},
            ),
        ),
        includes_last=False,
    )
    transient = SyncTransientError()
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service(
        [committed, transient],
        apply_max_pages=2,
    )
    initial_receipt_count = len(receipts.receipts)
    retry_stamp = connection.database_now
    expected_delay = _deterministic_retry_delay(
        account_id=8,
        canonical_folder="INBOX",
        expected_version=4,
        failure_count=1,
        retry_after_seconds=None,
    )

    result = await service.apply(applying.plan_id)

    assert result.status is ColdStartRunStatus.RETRY_SCHEDULED
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert result.safe_code == "exchange.sync.transient_failure"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.APPROVED
    assert ordinary.calls == [
        (8, "Inbox", "opaque+Apply1/%3D", 100),
        (8, "Inbox", "opaque+Apply2/%3D", 100),
    ]
    assert connection.cursor_status == "cold_start_applying"
    assert connection.cursor_value == committed.cursor
    assert connection.cursor_version == 4
    assert connection.cursor_transient_failures == 1
    assert connection.cursor_retry_after_at == retry_stamp + timedelta(
        seconds=expected_delay
    )
    assert connection.cursor_cold_start_plan_id == applying.plan_id
    assert connection.cursor_cold_start_plan_state == "approved"
    assert len(inbox.inserted) == 1
    assert len(receipts.receipts) == initial_receipt_count + 1
    plan_row = connection.plans[applying.plan_id]
    assert plan_row["apply_cursor"] == committed.cursor
    assert plan_row["apply_cursor_version"] == 4


@pytest.mark.asyncio
async def test_apply_applying_hostile_transient_ignores_hint_without_descriptor_read() -> (
    None
):
    transient = _C6HostileTransient()
    _C6HostileTransient.descriptor_reads = 0
    (
        service,
        connection,
        _receipts,
        ordinary,
        _inbox,
        applying,
    ) = await _c6_applying_service([transient])
    retry_stamp = connection.database_now
    connection.cursor_version = 7
    connection.plans[applying.plan_id]["apply_cursor_version"] = 7
    connection.plans[applying.plan_id]["version"] = 7
    connection.cursor_transient_failures = 7
    connection.cursor_retry_after_at = retry_stamp
    expected_delay = _deterministic_retry_delay(
        account_id=8,
        canonical_folder="INBOX",
        expected_version=7,
        failure_count=8,
        retry_after_seconds=None,
    )
    assert expected_delay == 110

    result = await service.apply(applying.plan_id)

    assert result.status is ColdStartRunStatus.RETRY_SCHEDULED
    assert connection.cursor_retry_after_at == retry_stamp + timedelta(
        seconds=expected_delay
    )
    assert connection.cursor_transient_failures == 8
    assert connection.cursor_version == 7
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert _C6HostileTransient.descriptor_reads == 0


def _c7_apply_fatal_outcome(
    case: str,
    monkeypatch: pytest.MonkeyPatch,
) -> object:
    if case == "authorization":
        return SyncAuthorizationError()
    if case == "cursor-invalid":
        return SyncCursorInvalidError()
    if case == "remote-contract":
        return SyncContractError()
    if case == "hostile-authorization":
        _C7HostileAuthorization.descriptor_reads = 0
        return _C7HostileAuthorization()
    if case == "hostile-cursor-invalid":
        _C7HostileCursorInvalid.descriptor_reads = 0
        return _C7HostileCursorInvalid()
    if case == "hostile-remote-contract":
        _C7HostileContract.descriptor_reads = 0
        return _C7HostileContract()
    if case == "hostile-batch":
        return _mutated_batch("id-subclass")
    if case == "malformed-v2":
        malformed = _vector_batch(changes=())
        object.__setattr__(malformed, "contract_version", "legacy-contract")
        return malformed
    if case == "oversized-v2":
        oversized = _batch_with_changes(100)
        object.__setattr__(
            oversized,
            "changes",
            (
                *oversized.changes,
                SyncChange(ChangeKind.DELETE, "extra", None, None),
            ),
        )
        return oversized
    if case == "normalization":

        def fail_normalization(*_args: object, **_kwargs: object) -> object:
            raise ValueError("hostile normalization detail")

        monkeypatch.setattr(
            cold_start_module,
            "normalize_sync_change",
            fail_normalization,
        )
        return _vector_batch(
            cursor="opaque+Normalize/%3D",
            changes=(
                SyncChange(
                    ChangeKind.CREATE,
                    "post-boundary-normalize",
                    {"subject": "normalize"},
                    "v2",
                ),
            ),
            includes_last=True,
        )
    if case == "stalled":
        return _vector_batch(
            cursor="opaque+Boundary/%3D",
            changes=(),
            includes_last=False,
        )
    raise AssertionError(case)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_status", "cursor_value", "cursor_version"),
    [
        ("cold_start_pending", None, 2),
        ("reset_required", "opaque+Old/%3D", 7),
    ],
    ids=["pending", "reset"],
)
@pytest.mark.parametrize(
    ("fatal_case", "safe_code"),
    [
        ("authorization", "exchange.sync.authorization_failed"),
        ("cursor-invalid", "exchange.sync.cursor_invalid"),
        ("remote-contract", "exchange.sync.contract_invalid"),
        ("hostile-authorization", "exchange.sync.authorization_failed"),
        ("hostile-cursor-invalid", "exchange.sync.cursor_invalid"),
        ("hostile-remote-contract", "exchange.sync.contract_invalid"),
        ("hostile-batch", "sync.local_contract_invalid"),
        ("malformed-v2", "sync.local_contract_invalid"),
        ("oversized-v2", "sync.local_contract_invalid"),
        ("normalization", "sync.local_contract_invalid"),
        ("stalled", "sync.cursor_stalled"),
    ],
)
async def test_apply_prebinding_fatal_blocks_plan_and_full_cursor_atomically(
    cursor_status: str,
    cursor_value: str | None,
    cursor_version: int,
    fatal_case: str,
    safe_code: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome = _c7_apply_fatal_outcome(fatal_case, monkeypatch)
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service(
        [outcome],
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
    )
    plan_before = dict(connection.plans[approved.plan_id])
    receipts_before = dict(receipts.receipts)
    audits_before = [dict(audit) for audit in connection.audits]
    last_success_before = connection.cursor_last_success_at
    blocked_at = connection.database_now

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.pages_committed == 0
    assert result.changes_observed == 0
    assert result.safe_code == safe_code
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    assert result.plan.blocked_reason_code == safe_code
    expected_fingerprint = _blocked_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=approved.plan_id,
        safe_code=safe_code,
    )
    assert result.plan.blocked_fingerprint == expected_fingerprint
    plan_after = connection.plans[approved.plan_id]
    assert plan_after["version"] == plan_before["version"] + 1
    assert plan_after["apply_cursor"] is None
    assert plan_after["apply_cursor_version"] is None
    assert plan_after["blocked_at"] == blocked_at
    assert plan_after["updated_at"] == blocked_at
    assert connection.cursor_status == "blocked_contract"
    assert connection.cursor_value == cursor_value
    assert connection.cursor_version == cursor_version + 1
    assert connection.cursor_blocked_reason == safe_code
    assert connection.cursor_contract_fingerprint == expected_fingerprint
    assert connection.cursor_blocked_at == blocked_at
    assert connection.cursor_transient_failures == 0
    assert connection.cursor_retry_after_at is None
    assert connection.cursor_cold_start_plan_id is None
    assert connection.cursor_cold_start_plan_state is None
    assert connection.cursor_last_attempt_at == blocked_at
    assert connection.cursor_last_success_at == last_success_before
    assert connection.cursor_updated_at == blocked_at
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert receipts.receipts == receipts_before
    assert inbox.inserted == []
    assert connection.apply_transitions == []
    assert connection.audits[:-1] == audits_before
    assert len(connection.audits) == len(audits_before) + 1
    assert connection.audits[-1]["reason"] == safe_code
    assert connection.audits[-1]["safe_metadata"] == {
        "plan_id": str(approved.plan_id),
        "safe_code": safe_code,
    }
    hostile_types = {
        "hostile-authorization": _C7HostileAuthorization,
        "hostile-cursor-invalid": _C7HostileCursorInvalid,
        "hostile-remote-contract": _C7HostileContract,
    }
    if fatal_case in hostile_types:
        assert hostile_types[fatal_case].descriptor_reads == 0
    plan_index = connection.events.index("plan.block")
    cursor_index = connection.events.index("cursor.block")
    audit_index = connection.events.index("audit.block")
    receipt_lookups = [
        index
        for index, event in enumerate(connection.events)
        if event == "receipt.lookup"
    ]
    assert len(receipt_lookups) == 2
    assert connection.events.index("ordinary.fetch") < receipt_lookups[-1] < plan_index
    transaction_enter = max(
        index
        for index, event in enumerate(connection.events[:plan_index])
        if event == "xid.enter"
    )
    transaction_commit = next(
        index
        for index, event in enumerate(
            connection.events[audit_index + 1 :], audit_index + 1
        )
        if event == "xid.commit"
    )
    assert (
        transaction_enter < plan_index < cursor_index < audit_index < transaction_commit
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_failures", [0, 7], ids=["clear", "expired-retry"])
async def test_apply_applying_fatal_preserves_progress_and_clears_optional_retry(
    retry_failures: int,
) -> None:
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([SyncContractError()])
    if retry_failures:
        connection.cursor_transient_failures = retry_failures
        connection.cursor_retry_after_at = connection.database_now
    plan_before = dict(connection.plans[applying.plan_id])
    cursor_version_before = connection.cursor_version
    cursor_value_before = connection.cursor_value
    last_success_before = connection.cursor_last_success_at
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]
    blocked_at = connection.database_now

    result = await service.apply(applying.plan_id)

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.pages_committed == 0
    assert result.changes_observed == 0
    assert result.safe_code == "exchange.sync.contract_invalid"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    plan_after = connection.plans[applying.plan_id]
    assert plan_after["version"] == plan_before["version"] + 1
    assert plan_after["apply_cursor"] == plan_before["apply_cursor"]
    assert plan_after["apply_cursor_version"] == plan_before["apply_cursor_version"]
    assert plan_after["blocked_at"] == blocked_at
    assert connection.cursor_status == "blocked_contract"
    assert connection.cursor_value == cursor_value_before
    assert connection.cursor_version == cursor_version_before + 1
    assert connection.cursor_transient_failures == 0
    assert connection.cursor_retry_after_at is None
    assert connection.cursor_cold_start_plan_id is None
    assert connection.cursor_cold_start_plan_state is None
    assert connection.cursor_last_attempt_at == blocked_at
    assert connection.cursor_last_success_at == last_success_before
    assert connection.cursor_updated_at == blocked_at
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert receipts.receipts == receipts_before
    assert inbox.inserted == inbox_before
    assert connection.audits[:-1] == audits_before
    assert len(connection.audits) == len(audits_before) + 1
    assert connection.audits[-1]["reason"] == "exchange.sync.contract_invalid"


@pytest.mark.asyncio
async def test_apply_second_page_fatal_preserves_first_page_progress_and_counts() -> (
    None
):
    committed = _vector_batch(
        cursor="opaque+Apply2/%3D",
        changes=(
            SyncChange(
                ChangeKind.CREATE,
                "post-boundary-fatal",
                {"subject": "committed before fatal"},
                "v2",
            ),
        ),
        includes_last=False,
    )
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service(
        [committed, SyncAuthorizationError()],
        apply_max_pages=2,
    )
    plan_version_before = connection.plans[applying.plan_id]["version"]
    cursor_version_before = connection.cursor_version
    receipt_count_before = len(receipts.receipts)
    inbox_count_before = len(inbox.inserted)
    audit_count_before = len(connection.audits)
    blocked_at = connection.database_now

    result = await service.apply(applying.plan_id)

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert result.safe_code == "exchange.sync.authorization_failed"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    plan_after = connection.plans[applying.plan_id]
    assert plan_after["version"] == plan_version_before + 2
    assert plan_after["apply_cursor"] == committed.cursor
    assert plan_after["apply_cursor_version"] == cursor_version_before + 1
    assert plan_after["blocked_at"] == blocked_at
    assert connection.cursor_status == "blocked_contract"
    assert connection.cursor_value == committed.cursor
    assert connection.cursor_version == cursor_version_before + 2
    assert connection.cursor_transient_failures == 0
    assert connection.cursor_retry_after_at is None
    assert connection.cursor_cold_start_plan_id is None
    assert connection.cursor_cold_start_plan_state is None
    assert ordinary.calls == [
        (8, "Inbox", "opaque+Apply1/%3D", 100),
        (8, "Inbox", committed.cursor, 100),
    ]
    assert len(inbox.inserted) == inbox_count_before + 1
    assert len(receipts.receipts) == receipt_count_before + 1
    assert len(connection.audits) == audit_count_before + 1
    assert connection.audits[-1]["reason"] == "exchange.sync.authorization_failed"
    assert len(connection.apply_transitions) == 1
    assert connection.apply_transitions[0]["apply_cursor"] == committed.cursor


def _c8_mutate_apply_drift(
    service: ColdStartService,
    connection: _C5ApplyConnection,
    plan_id: UUID,
    drifts: tuple[str, ...],
    *,
    schedule_retry: bool,
) -> tuple[dict[str, object], dict[str, object]]:
    plan = connection.plans[plan_id]
    for drift in drifts:
        if drift == "expiry":
            connection.database_now = plan["expires_at"]  # type: ignore[assignment]
        elif drift == "config":
            changed_scope = FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("changed-inbox-id",),
                sync_folder="Inbox",
                event_policy_matrix=_c1_policy_matrix(),
            )
            service._snapshot_provider = _C1SnapshotProvider(  # type: ignore[assignment]
                PolicySnapshot(scopes=(changed_scope,)),
                connection.events,
            )
        elif drift == "fence":
            connection.ownership_fencing_token += 1
        elif drift == "cursor":
            connection.cursor_version += 1
        elif drift == "version":
            plan["version"] = plan["version"] + 10  # type: ignore[operator]
        elif drift == "plan_hash":
            plan["plan_hash"] = "f" * 64
        else:
            raise AssertionError(drift)
    if schedule_retry:
        connection.cursor_transient_failures = 4
        connection.cursor_retry_after_at = connection.database_now + timedelta(
            minutes=5
        )
    cursor = {
        "value": connection.cursor_value,
        "version": connection.cursor_version,
        "last_success_at": connection.cursor_last_success_at,
    }
    return dict(plan), cursor


def _c8_full_cursor_snapshot(connection: _C5ApplyConnection) -> tuple[object, ...]:
    return (
        connection.cursor_status,
        connection.cursor_value,
        connection.cursor_version,
        connection.cursor_blocked_reason,
        connection.cursor_contract_fingerprint,
        connection.cursor_blocked_at,
        connection.cursor_transient_failures,
        connection.cursor_retry_after_at,
        connection.cursor_cold_start_plan_id,
        connection.cursor_cold_start_plan_state,
        connection.cursor_last_attempt_at,
        connection.cursor_last_success_at,
        connection.cursor_updated_at,
    )


def _c8_assert_apply_drift_block(
    *,
    result: ColdStartRunResult,
    connection: _C5ApplyConnection,
    receipts: _C1ReceiptRepository,
    inbox: _C5InboxRepository,
    plan_id: UUID,
    safe_code: str,
    plan_before: dict[str, object],
    cursor_before: dict[str, object],
    receipts_before: dict[tuple[int, str, str], CommandReceipt],
    inbox_before: list[tuple[object, int, int]],
    audits_before: list[dict[str, object]],
) -> None:
    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.pages_committed == result.changes_observed == 0
    assert result.safe_code == safe_code
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    plan_after = connection.plans[plan_id]
    assert plan_after["version"] == plan_before["version"] + 1  # type: ignore[operator]
    for field in (
        "boundary_cursor",
        "boundary_cursor_version",
        "apply_cursor",
        "apply_cursor_version",
        "page_count",
        "item_count",
        "redacted_samples",
        "plan_hash",
    ):
        assert plan_after[field] == plan_before[field]
    expected_fingerprint = _blocked_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=plan_id,
        safe_code=safe_code,
    )
    assert plan_after["blocked_reason_code"] == safe_code
    assert plan_after["blocked_fingerprint"] == expected_fingerprint
    assert plan_after["blocked_at"] == connection.database_now
    assert plan_after["updated_at"] == connection.database_now
    assert connection.cursor_status == "blocked_contract"
    assert connection.cursor_value == cursor_before["value"]
    assert connection.cursor_version == cursor_before["version"] + 1  # type: ignore[operator]
    assert connection.cursor_blocked_reason == safe_code
    assert connection.cursor_contract_fingerprint == expected_fingerprint
    assert connection.cursor_blocked_at == connection.database_now
    assert connection.cursor_transient_failures == 0
    assert connection.cursor_retry_after_at is None
    assert connection.cursor_cold_start_plan_id is None
    assert connection.cursor_cold_start_plan_state is None
    assert connection.cursor_last_attempt_at == connection.database_now
    assert connection.cursor_last_success_at == cursor_before["last_success_at"]
    assert connection.cursor_updated_at == connection.database_now
    assert receipts.receipts == receipts_before
    assert inbox.inserted == inbox_before
    assert connection.apply_transitions == []
    assert connection.audits[:-1] == audits_before
    assert len(connection.audits) == len(audits_before) + 1
    assert connection.audits[-1]["reason"] == safe_code
    assert connection.audits[-1]["safe_metadata"] == {
        "plan_id": str(plan_id),
        "safe_code": safe_code,
    }


_C8_DRIFT_PRIORITY_CASES = [
    (
        ("expiry", "config", "fence", "cursor", "version", "plan_hash"),
        "cold_start.expired",
    ),
    (
        ("config", "fence", "cursor", "version", "plan_hash"),
        "cold_start.config_drift",
    ),
    (
        ("fence", "cursor", "version", "plan_hash"),
        "cold_start.fence_drift",
    ),
    (("cursor", "version", "plan_hash"), "cold_start.cursor_drift"),
    (("version", "plan_hash"), "cold_start.version_drift"),
    (("plan_hash",), "cold_start.plan_hash_drift"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drifts", "safe_code"),
    [
        (("expiry",), "cold_start.expired"),
        (("config",), "cold_start.config_drift"),
        (("fence",), "cold_start.fence_drift"),
        (("cursor",), "cold_start.cursor_drift"),
        (("version",), "cold_start.version_drift"),
        (("plan_hash",), "cold_start.plan_hash_drift"),
    ],
    ids=["expiry", "config", "fence", "cursor", "version", "plan-hash"],
)
async def test_apply_transient_post_http_drift_blocks_in_retry_xid(
    drifts: tuple[str, ...],
    safe_code: str,
) -> None:
    captured: dict[str, object] = {}

    def drift_then_fail_transiently() -> SyncBatch:
        plan_before, cursor_before = _c8_mutate_apply_drift(
            service,
            connection,
            applying.plan_id,
            drifts,
            schedule_retry=False,
        )
        captured["plan"] = plan_before
        captured["cursor"] = cursor_before
        raise SyncTransientError(retry_after_seconds=17)

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([drift_then_fail_transiently])
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    result = await service.apply(applying.plan_id)

    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert "cursor.apply_retry" not in connection.events
    _c8_assert_apply_drift_block(
        result=result,
        connection=connection,
        receipts=receipts,
        inbox=inbox,
        plan_id=applying.plan_id,
        safe_code=safe_code,
        plan_before=captured["plan"],  # type: ignore[arg-type]
        cursor_before=captured["cursor"],  # type: ignore[arg-type]
        receipts_before=receipts_before,
        inbox_before=inbox_before,
        audits_before=audits_before,
    )
    mutation_events = [
        event
        for event in connection.events
        if event in {"plan.block", "cursor.block", "audit.block", "cursor.apply_retry"}
    ]
    assert mutation_events == ["plan.block", "cursor.block", "audit.block"]


@pytest.mark.asyncio
async def test_apply_transient_completed_receipt_without_result_evidence_fails_closed() -> (
    None
):
    payload_hash = ""

    def complete_durably_then_report_transient() -> SyncBatch:
        connection.database_now += timedelta(microseconds=1)
        stamp = connection.database_now
        plan = connection.plans[applying.plan_id]
        next_cursor = "opaque+ConcurrentTerminal/%3D"
        next_cursor_version = connection.cursor_version + 1
        plan.update(
            {
                "state": "completed",
                "version": plan["version"] + 1,  # type: ignore[operator]
                "apply_cursor": next_cursor,
                "apply_cursor_version": next_cursor_version,
                "completed_at": stamp,
                "updated_at": stamp,
            }
        )
        connection.cursor_status = "active"
        connection.cursor_value = next_cursor
        connection.cursor_version = next_cursor_version
        connection.cursor_cold_start_plan_id = None
        connection.cursor_cold_start_plan_state = None
        connection.cursor_last_attempt_at = stamp
        connection.cursor_last_success_at = stamp
        connection.cursor_updated_at = stamp
        receipts.receipts[(8, "cold_start.apply_page", payload_hash)] = CommandReceipt(
            id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
            account_id=8,
            command_name="cold_start.apply_page",
            idempotency_key_hash=_hash_idempotency_key(
                8,
                "cold_start.apply_page",
                payload_hash,
            ),
            canonical_payload_hash=payload_hash,
            outcome="succeeded",
            result_type="sync_cold_start_plan",
            result_id=str(applying.plan_id),
            result_hash="a" * 64,
            authority_epoch=9,
            created_at=stamp,
        )
        raise SyncTransientError(retry_after_seconds=17)

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([complete_durably_then_report_transient])
    plan_before = dict(connection.plans[applying.plan_id])
    payload_hash = _apply_page_payload_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=applying.plan_id,
        plan_version=plan_before["version"],  # type: ignore[arg-type]
        cursor_status=SyncCursorStatus.COLD_START_APPLYING,
        cursor_version=connection.cursor_version,
        request_cursor_hash=_cursor_digest("opaque+Apply1/%3D"),
    )
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(applying.plan_id)

    assert caught.value.operation == "cold_start_apply_retry"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start apply state is invalid"
    completed = connection.plans[applying.plan_id]
    assert completed["state"] == "completed"
    assert completed["version"] == plan_before["version"] + 1  # type: ignore[operator]
    assert completed["apply_cursor"] == "opaque+ConcurrentTerminal/%3D"
    assert connection.cursor_status == "active"
    assert connection.cursor_value == completed["apply_cursor"]
    assert connection.cursor_version == completed["apply_cursor_version"]
    assert (8, "cold_start.apply_page", payload_hash) in receipts.receipts
    assert inbox.inserted == inbox_before
    assert connection.audits == audits_before
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert not any(
        event in {"plan.block", "cursor.block", "audit.block", "cursor.apply_retry"}
        for event in connection.events
    )

    durable_before_replay = (
        dict(connection.plans[applying.plan_id]),
        _c8_full_cursor_snapshot(connection),
        dict(receipts.receipts),
        list(inbox.inserted),
        [dict(audit) for audit in connection.audits],
    )
    replay = await service.apply(applying.plan_id)

    assert replay.status is ColdStartRunStatus.COMPLETED
    assert replay.pages_committed == replay.changes_observed == 0
    assert replay.safe_code is None
    assert replay.plan is not None
    assert replay.plan.state is ColdStartPlanState.COMPLETED
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert (
        dict(connection.plans[applying.plan_id]),
        _c8_full_cursor_snapshot(connection),
        dict(receipts.receipts),
        list(inbox.inserted),
        [dict(audit) for audit in connection.audits],
    ) == durable_before_replay


@pytest.mark.asyncio
async def test_apply_transient_rebound_authority_is_conflict_without_block() -> None:
    rebound_plan_id = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
    captured: dict[str, object] = {}

    def rebind_authority_then_report_transient() -> SyncBatch:
        connection.cursor_cold_start_plan_id = rebound_plan_id
        captured["plan"] = dict(connection.plans[applying.plan_id])
        captured["cursor"] = _c8_full_cursor_snapshot(connection)
        raise SyncTransientError(retry_after_seconds=17)

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([rebind_authority_then_report_transient])
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    with pytest.raises(
        ColdStartStateConflictError,
        match="^cold-start plan state conflict$",
    ):
        await service.apply(applying.plan_id)

    assert connection.plans[applying.plan_id] == captured["plan"]
    assert _c8_full_cursor_snapshot(connection) == captured["cursor"]
    assert connection.cursor_cold_start_plan_id == rebound_plan_id
    assert receipts.receipts == receipts_before
    assert inbox.inserted == inbox_before
    assert connection.audits == audits_before
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert not any(
        event in {"plan.block", "cursor.block", "audit.block", "cursor.apply_retry"}
        for event in connection.events
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drifts", "safe_code"),
    _C8_DRIFT_PRIORITY_CASES,
    ids=["expiry", "config", "fence", "cursor", "version", "plan-hash"],
)
async def test_apply_preflight_drift_priority_blocks_before_http(
    drifts: tuple[str, ...],
    safe_code: str,
) -> None:
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([])
    plan_before, cursor_before = _c8_mutate_apply_drift(
        service,
        connection,
        applying.plan_id,
        drifts,
        schedule_retry=True,
    )
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    result = await service.apply(applying.plan_id)

    assert ordinary.calls == []
    _c8_assert_apply_drift_block(
        result=result,
        connection=connection,
        receipts=receipts,
        inbox=inbox,
        plan_id=applying.plan_id,
        safe_code=safe_code,
        plan_before=plan_before,
        cursor_before=cursor_before,
        receipts_before=receipts_before,
        inbox_before=inbox_before,
        audits_before=audits_before,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "illegal_pair",
    [
        "missing-binding",
        "wrong-plan",
        "wrong-state",
        "retry-singleton",
        "timestamp-projection",
    ],
)
async def test_apply_schema_illegal_cursor_pair_is_fixed_conflict_without_block(
    illegal_pair: str,
) -> None:
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([])
    if illegal_pair == "missing-binding":
        connection.cursor_cold_start_plan_id = None
        connection.cursor_cold_start_plan_state = None
    elif illegal_pair == "wrong-plan":
        connection.cursor_cold_start_plan_id = UUID(
            "99999999-9999-4999-8999-999999999999"
        )
    elif illegal_pair == "wrong-state":
        connection.cursor_cold_start_plan_state = "ready"
    elif illegal_pair == "retry-singleton":
        connection.cursor_transient_failures = 1
        connection.cursor_retry_after_at = None
    else:
        connection.cursor_updated_at += timedelta(microseconds=1)
    plan_before = dict(connection.plans[applying.plan_id])
    cursor_before = _c8_full_cursor_snapshot(connection)
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    with pytest.raises(
        ColdStartStateConflictError,
        match="^cold-start plan state conflict$",
    ):
        await service.apply(applying.plan_id)

    assert ordinary.calls == []
    assert connection.plans[applying.plan_id] == plan_before
    assert _c8_full_cursor_snapshot(connection) == cursor_before
    assert receipts.receipts == receipts_before
    assert inbox.inserted == inbox_before
    assert connection.audits == audits_before
    assert "plan.block" not in connection.events
    assert "cursor.block" not in connection.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drifts", "safe_code"),
    _C8_DRIFT_PRIORITY_CASES,
    ids=["expiry", "config", "fence", "cursor", "version", "plan-hash"],
)
async def test_apply_post_http_drift_priority_discards_stale_page(
    drifts: tuple[str, ...],
    safe_code: str,
) -> None:
    stale = _vector_batch(
        cursor="opaque+Stale/%3D",
        changes=(
            SyncChange(
                ChangeKind.CREATE,
                "stale-post-http",
                {"subject": "must not commit"},
                "v2",
            ),
        ),
        includes_last=True,
    )
    captured: dict[str, object] = {}

    def mutate_after_http() -> SyncBatch:
        plan_before, cursor_before = _c8_mutate_apply_drift(
            service,
            connection,
            applying.plan_id,
            drifts,
            schedule_retry=False,
        )
        captured["plan"] = plan_before
        captured["cursor"] = cursor_before
        return stale

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([mutate_after_http])
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    result = await service.apply(applying.plan_id)

    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert type(captured["plan"]) is dict
    assert type(captured["cursor"]) is dict
    _c8_assert_apply_drift_block(
        result=result,
        connection=connection,
        receipts=receipts,
        inbox=inbox,
        plan_id=applying.plan_id,
        safe_code=safe_code,
        plan_before=captured["plan"],  # type: ignore[arg-type]
        cursor_before=captured["cursor"],  # type: ignore[arg-type]
        receipts_before=receipts_before,
        inbox_before=inbox_before,
        audits_before=audits_before,
    )


@pytest.mark.asyncio
async def test_apply_post_http_snapshot_unavailable_propagates_without_block() -> None:
    stale = _vector_batch(
        cursor="opaque+Stale/%3D",
        changes=(),
        includes_last=True,
    )

    def lose_snapshot_after_http() -> SyncBatch:
        service._snapshot_provider = _C1SnapshotProvider(  # type: ignore[assignment]
            PolicySnapshot.failed(),
            connection.events,
        )
        return stale

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([lose_snapshot_after_http])
    plan_before = dict(connection.plans[applying.plan_id])
    cursor_before = _c8_full_cursor_snapshot(connection)
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    with pytest.raises(PolicySnapshotUnavailableError):
        await service.apply(applying.plan_id)

    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert connection.plans[applying.plan_id] == plan_before
    assert _c8_full_cursor_snapshot(connection) == cursor_before
    assert receipts.receipts == receipts_before
    assert inbox.inserted == inbox_before
    assert connection.audits == audits_before
    assert "plan.block" not in connection.events
    assert "cursor.block" not in connection.events


@pytest.mark.asyncio
async def test_apply_preflight_snapshot_unavailable_never_enters_apply_xid() -> None:
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([])
    service._snapshot_provider = _C1SnapshotProvider(  # type: ignore[assignment]
        PolicySnapshot.failed(),
        connection.events,
    )
    plan_before = dict(connection.plans[applying.plan_id])
    cursor_before = _c8_full_cursor_snapshot(connection)
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    with pytest.raises(PolicySnapshotUnavailableError):
        await service.apply(applying.plan_id)

    assert ordinary.calls == []
    assert connection.plans[applying.plan_id] == plan_before
    assert _c8_full_cursor_snapshot(connection) == cursor_before
    assert receipts.receipts == receipts_before
    assert inbox.inserted == inbox_before
    assert connection.audits == audits_before
    assert "xid.enter" not in connection.events


@pytest.mark.asyncio
async def test_apply_process_control_exception_propagates_without_block() -> None:
    cancellation = asyncio.CancelledError("stop apply")
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([cancellation])
    plan_before = dict(connection.plans[applying.plan_id])
    cursor_before = _c8_full_cursor_snapshot(connection)
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    with pytest.raises(asyncio.CancelledError) as caught:
        await service.apply(applying.plan_id)

    assert caught.value is cancellation
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert connection.plans[applying.plan_id] == plan_before
    assert _c8_full_cursor_snapshot(connection) == cursor_before
    assert receipts.receipts == receipts_before
    assert inbox.inserted == inbox_before
    assert connection.audits == audits_before
    assert "plan.block" not in connection.events
    assert "cursor.block" not in connection.events


@pytest.mark.asyncio
async def test_apply_post_http_existing_prepared_receipt_is_invariant_not_drift() -> (
    None
):
    terminal = _vector_batch(
        cursor="opaque+Receipt/%3D",
        changes=(),
        includes_last=True,
    )
    payload_hash = ""

    def inject_receipt_after_http() -> SyncBatch:
        receipts.receipts[(8, "cold_start.apply_page", payload_hash)] = CommandReceipt(
            id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
            account_id=8,
            command_name="cold_start.apply_page",
            idempotency_key_hash=_hash_idempotency_key(
                8,
                "cold_start.apply_page",
                payload_hash,
            ),
            canonical_payload_hash=payload_hash,
            outcome="succeeded",
            result_type="sync_cold_start_plan",
            result_id=str(applying.plan_id),
            result_hash="a" * 64,
            authority_epoch=9,
            created_at=connection.database_now,
        )
        return terminal

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([inject_receipt_after_http])
    plan = connection.plans[applying.plan_id]
    payload_hash = _apply_page_payload_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=applying.plan_id,
        plan_version=plan["version"],  # type: ignore[arg-type]
        cursor_status=SyncCursorStatus.COLD_START_APPLYING,
        cursor_version=connection.cursor_version,
        request_cursor_hash=_cursor_digest("opaque+Apply1/%3D"),
    )
    plan_before = dict(plan)
    cursor_before = _c8_full_cursor_snapshot(connection)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(applying.plan_id)

    assert caught.value.operation == "cold_start_apply_page"
    assert caught.value.retryable is False
    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    assert connection.plans[applying.plan_id] == plan_before
    assert _c8_full_cursor_snapshot(connection) == cursor_before
    assert inbox.inserted == inbox_before
    assert connection.audits == audits_before
    assert (8, "cold_start.apply_page", payload_hash) in receipts.receipts
    assert "plan.block" not in connection.events
    assert "cursor.block" not in connection.events


@pytest.mark.asyncio
async def test_apply_post_http_legal_external_progress_is_preserved_when_blocked() -> (
    None
):
    stale = _vector_batch(
        cursor="opaque+Stale/%3D",
        changes=(),
        includes_last=True,
    )
    captured: dict[str, object] = {}

    def advance_legal_progress_after_http() -> SyncBatch:
        connection.database_now += timedelta(microseconds=1)
        stamp = connection.database_now
        plan = connection.plans[applying.plan_id]
        plan.update(
            {
                "version": plan["version"] + 1,  # type: ignore[operator]
                "apply_cursor": "opaque+External/%3D",
                "apply_cursor_version": connection.cursor_version + 1,
                "updated_at": stamp,
            }
        )
        connection.cursor_value = "opaque+External/%3D"
        connection.cursor_version += 1
        connection.cursor_last_attempt_at = stamp
        connection.cursor_last_success_at = stamp
        connection.cursor_updated_at = stamp
        captured["plan"] = dict(plan)
        captured["cursor"] = {
            "value": connection.cursor_value,
            "version": connection.cursor_version,
            "last_success_at": connection.cursor_last_success_at,
        }
        return stale

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([advance_legal_progress_after_http])
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    result = await service.apply(applying.plan_id)

    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    _c8_assert_apply_drift_block(
        result=result,
        connection=connection,
        receipts=receipts,
        inbox=inbox,
        plan_id=applying.plan_id,
        safe_code="cold_start.cursor_drift",
        plan_before=captured["plan"],  # type: ignore[arg-type]
        cursor_before=captured["cursor"],  # type: ignore[arg-type]
        receipts_before=receipts_before,
        inbox_before=inbox_before,
        audits_before=audits_before,
    )
    assert connection.cursor_value == "opaque+External/%3D"
    assert connection.plans[applying.plan_id]["apply_cursor"] == ("opaque+External/%3D")


@pytest.mark.asyncio
async def test_apply_second_page_post_http_drift_counts_only_first_page() -> None:
    first = _vector_batch(
        cursor="opaque+Apply2/%3D",
        changes=(
            SyncChange(
                ChangeKind.CREATE,
                "committed-before-drift",
                {"subject": "committed"},
                "v2",
            ),
        ),
        includes_last=False,
    )
    stale = _vector_batch(
        cursor="opaque+Stale/%3D",
        changes=(
            SyncChange(
                ChangeKind.CREATE,
                "stale-after-drift",
                {"subject": "discarded"},
                "v2",
            ),
        ),
        includes_last=True,
    )

    def fence_after_second_http() -> SyncBatch:
        connection.ownership_fencing_token += 1
        return stale

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service(
        [first, fence_after_second_http],
        apply_max_pages=2,
    )
    receipt_count_before = len(receipts.receipts)
    inbox_count_before = len(inbox.inserted)
    audit_count_before = len(connection.audits)

    result = await service.apply(applying.plan_id)

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == "cold_start.fence_drift"
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert ordinary.calls == [
        (8, "Inbox", "opaque+Apply1/%3D", 100),
        (8, "Inbox", "opaque+Apply2/%3D", 100),
    ]
    assert len(receipts.receipts) == receipt_count_before + 1
    assert len(inbox.inserted) == inbox_count_before + 1
    assert inbox.inserted[-1][0].external_email_id == "committed-before-drift"  # type: ignore[attr-defined]
    assert len(connection.audits) == audit_count_before + 1
    assert connection.audits[-1]["reason"] == "cold_start.fence_drift"
    plan = connection.plans[applying.plan_id]
    assert plan["apply_cursor"] == "opaque+Apply2/%3D"
    assert plan["state"] == "blocked"
    assert len(connection.apply_transitions) == 1


@pytest.mark.asyncio
async def test_apply_fatal_revalidation_prefers_post_http_drift_code() -> None:
    captured: dict[str, object] = {}

    def fence_then_fail() -> SyncBatch:
        connection.ownership_fencing_token += 1
        plan_before, cursor_before = _c8_mutate_apply_drift(
            service,
            connection,
            applying.plan_id,
            (),
            schedule_retry=False,
        )
        captured["plan"] = plan_before
        captured["cursor"] = cursor_before
        raise SyncAuthorizationError()

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([fence_then_fail])
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    result = await service.apply(applying.plan_id)

    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    _c8_assert_apply_drift_block(
        result=result,
        connection=connection,
        receipts=receipts,
        inbox=inbox,
        plan_id=applying.plan_id,
        safe_code="cold_start.fence_drift",
        plan_before=captured["plan"],  # type: ignore[arg-type]
        cursor_before=captured["cursor"],  # type: ignore[arg-type]
        receipts_before=receipts_before,
        inbox_before=inbox_before,
        audits_before=audits_before,
    )


@pytest.mark.asyncio
async def test_apply_fatal_revalidation_classifies_retry_pair_as_cursor_drift() -> None:
    captured: dict[str, object] = {}

    def schedule_retry_then_fail() -> SyncBatch:
        connection.cursor_transient_failures = 1
        connection.cursor_retry_after_at = connection.database_now + timedelta(
            seconds=10
        )
        plan_before, cursor_before = _c8_mutate_apply_drift(
            service,
            connection,
            applying.plan_id,
            (),
            schedule_retry=False,
        )
        captured["plan"] = plan_before
        captured["cursor"] = cursor_before
        raise SyncAuthorizationError()

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([schedule_retry_then_fail])
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    result = await service.apply(applying.plan_id)

    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    _c8_assert_apply_drift_block(
        result=result,
        connection=connection,
        receipts=receipts,
        inbox=inbox,
        plan_id=applying.plan_id,
        safe_code="cold_start.cursor_drift",
        plan_before=captured["plan"],  # type: ignore[arg-type]
        cursor_before=captured["cursor"],  # type: ignore[arg-type]
        receipts_before=receipts_before,
        inbox_before=inbox_before,
        audits_before=audits_before,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "safe_code"),
    [
        ("contract", "cold_start.config_drift"),
        ("cursor-status-binding", "cold_start.cursor_drift"),
        ("cursor-value", "cold_start.cursor_drift"),
        ("cursor-retry", "cold_start.cursor_drift"),
        ("cursor-timestamps", "cold_start.cursor_drift"),
        ("plan-version", "cold_start.version_drift"),
    ],
)
async def test_apply_post_http_legal_field_drift_is_classified_exactly(
    field: str,
    safe_code: str,
) -> None:
    terminal = _vector_batch(
        cursor="opaque+Stale/%3D",
        changes=(),
        includes_last=True,
    )
    captured: dict[str, object] = {}

    def mutate_legal_field_after_http() -> SyncBatch:
        plan = connection.plans[applying.plan_id]
        if field == "contract":
            service._contract_fingerprint = "d" * 64
        elif field == "cursor-status-binding":
            connection.cursor_status = "active"
            connection.cursor_cold_start_plan_id = None
            connection.cursor_cold_start_plan_state = None
        elif field == "cursor-value":
            connection.cursor_value = "opaque+ExternalValue/%3D"
        elif field == "cursor-retry":
            connection.cursor_transient_failures = 1
            connection.cursor_retry_after_at = connection.database_now + timedelta(
                minutes=5
            )
        elif field == "cursor-timestamps":
            stamp = connection.cursor_last_attempt_at + timedelta(  # type: ignore[operator]
                microseconds=1
            )
            connection.cursor_last_attempt_at = stamp
            connection.cursor_updated_at = stamp
        else:
            plan["version"] = plan["version"] + 1  # type: ignore[operator]
        captured["plan"] = dict(plan)
        captured["cursor"] = {
            "value": connection.cursor_value,
            "version": connection.cursor_version,
            "last_success_at": connection.cursor_last_success_at,
        }
        return terminal

    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([mutate_legal_field_after_http])
    receipts_before = dict(receipts.receipts)
    inbox_before = list(inbox.inserted)
    audits_before = [dict(audit) for audit in connection.audits]

    result = await service.apply(applying.plan_id)

    assert ordinary.calls == [(8, "Inbox", "opaque+Apply1/%3D", 100)]
    _c8_assert_apply_drift_block(
        result=result,
        connection=connection,
        receipts=receipts,
        inbox=inbox,
        plan_id=applying.plan_id,
        safe_code=safe_code,
        plan_before=captured["plan"],  # type: ignore[arg-type]
        cursor_before=captured["cursor"],  # type: ignore[arg-type]
        receipts_before=receipts_before,
        inbox_before=inbox_before,
        audits_before=audits_before,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_status", "cursor_value", "cursor_version"),
    [
        ("cold_start_pending", None, 2),
        ("reset_required", "opaque+Old/%3D", 7),
    ],
)
async def test_apply_realistic_prebinding_state_matrix_is_not_cursor_drift(
    cursor_status: str,
    cursor_value: str | None,
    cursor_version: int,
) -> None:
    terminal = _vector_batch(
        cursor="opaque+Active/%3D",
        changes=(),
        includes_last=True,
    )
    (
        service,
        connection,
        _receipts,
        ordinary,
        _inbox,
        approved,
    ) = await _c5_approved_service(
        [terminal],
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
    )
    assert connection.cursor_blocked_reason is not None
    if cursor_status == "reset_required":
        assert connection.cursor_last_attempt_at is not None

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.COMPLETED
    assert result.safe_code is None
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "cursor_status",
        "cursor_value",
        "cursor_version",
        "returned_cursor",
        "change_count",
    ),
    [
        (
            "cold_start_pending",
            None,
            2,
            "opaque+Boundary/%3D",
            0,
        ),
        (
            "cold_start_pending",
            None,
            2,
            "opaque+Active/%3D",
            1,
        ),
        (
            "reset_required",
            "opaque+Old/%3D",
            7,
            "opaque+ResetActive/%3D",
            1,
        ),
    ],
)
async def test_apply_first_direct_terminal_commits_exactly_once_from_boundary(
    cursor_status: str,
    cursor_value: str | None,
    cursor_version: int,
    returned_cursor: str,
    change_count: int,
) -> None:
    changes = (
        (
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="post-boundary-1",
                source_version="v2",
                item={"subject": "new mail"},
            ),
        )
        if change_count
        else ()
    )
    batch = _vector_batch(
        cursor=returned_cursor,
        changes=changes,
        includes_last=True,
    )
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service(
        [batch],
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
    )
    pre_version = next(iter(connection.plans.values()))["version"]

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.COMPLETED
    assert result.pages_committed == 1
    assert result.changes_observed == change_count
    assert result.safe_code is None
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.COMPLETED
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert connection.cursor_status == "active"
    assert connection.cursor_value == batch.cursor
    assert connection.cursor_version == cursor_version + 1
    assert connection.cursor_cold_start_plan_id is None
    assert connection.cursor_cold_start_plan_state is None
    plan = next(iter(connection.plans.values()))
    assert plan["version"] == pre_version + 1
    assert plan["apply_cursor"] == batch.cursor
    assert plan["apply_cursor_version"] == connection.cursor_version
    assert plan["completed_at"] == connection.database_now
    assert plan["updated_at"] == connection.cursor_updated_at
    assert connection.cursor_last_attempt_at == connection.database_now
    assert connection.cursor_last_success_at == connection.database_now
    assert len(inbox.inserted) == change_count
    if inbox.inserted:
        event, generation, fencing_token = inbox.inserted[0]
        assert (generation, fencing_token) == (3, 9)
        assert event.source is IngressSource.SYNC  # type: ignore[attr-defined]
        assert event.processing_policy is ProcessingPolicy.FULL  # type: ignore[attr-defined]
        assert event.payload["cursor"] == batch.cursor  # type: ignore[attr-defined]
        assert event.external_email_id == "post-boundary-1"  # type: ignore[attr-defined]
    payload_hash = _apply_page_payload_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=approved.plan_id,
        plan_version=pre_version,  # type: ignore[arg-type]
        cursor_status=SyncCursorStatus(cursor_status),
        cursor_version=cursor_version,
        request_cursor_hash=_cursor_digest("opaque+Boundary/%3D"),
    )
    receipt = receipts.receipts[(8, "cold_start.apply_page", payload_hash)]
    assert receipt.canonical_payload_hash == payload_hash
    assert receipt.result_id == str(approved.plan_id)
    assert receipt.result_hash == _apply_page_result_digest(_batch_digest(batch))
    assert receipt.authority_epoch == 9
    assert connection.events.index("receipt.lookup") < connection.events.index(
        "ordinary.fetch"
    )
    if change_count:
        assert connection.events.index("inbox.insert") < connection.events.index(
            "cursor.apply_page"
        )
    else:
        assert "inbox.insert" not in connection.events
    assert (
        connection.events.index("cursor.apply_page")
        < connection.events.index("plan.apply_page")
        < connection.events.index("receipt.insert")
    )

    replay = await service.apply(approved.plan_id)

    assert replay.status is ColdStartRunStatus.COMPLETED
    assert replay.pages_committed == replay.changes_observed == 0
    assert replay.plan == result.plan
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert (
        len(
            [
                identity
                for identity in receipts.receipts
                if identity[1] == "cold_start.apply_page"
            ]
        )
        == 1
    )


@pytest.mark.asyncio
async def test_apply_nonterminal_binds_then_later_terminal_completes() -> None:
    first = _vector_batch(
        cursor="opaque+Apply1/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="post-boundary-1",
                source_version="v2",
                item={"subject": "one"},
            ),
        ),
        includes_last=False,
    )
    later = _vector_batch(
        cursor="opaque+Apply2/%3D",
        changes=(),
        includes_last=False,
    )
    terminal = _vector_batch(
        cursor="opaque+Apply3/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.UPDATE,
                external_email_id="post-boundary-1",
                source_version="v3",
                item={"id": "post-boundary-1", "is_read": True},
            ),
        ),
        includes_last=True,
    )
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([first, later, terminal])

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.COMPLETED
    assert result.pages_committed == 3
    assert result.changes_observed == 2
    assert ordinary.calls == [
        (8, "Inbox", "opaque+Boundary/%3D", 100),
        (8, "Inbox", "opaque+Apply1/%3D", 100),
        (8, "Inbox", "opaque+Apply2/%3D", 100),
    ]
    assert connection.apply_transitions == [
        {
            "cursor_status": "cold_start_applying",
            "cursor_version": 3,
            "cursor_plan_id": approved.plan_id,
            "plan_state": "approved",
            "plan_version": 3,
            "apply_cursor": first.cursor,
            "apply_cursor_version": 3,
            "cursor_stamp": connection.database_now,
            "plan_stamp": connection.database_now,
        },
        {
            "cursor_status": "cold_start_applying",
            "cursor_version": 4,
            "cursor_plan_id": approved.plan_id,
            "plan_state": "approved",
            "plan_version": 4,
            "apply_cursor": later.cursor,
            "apply_cursor_version": 4,
            "cursor_stamp": connection.database_now,
            "plan_stamp": connection.database_now,
        },
        {
            "cursor_status": "active",
            "cursor_version": 5,
            "cursor_plan_id": None,
            "plan_state": "completed",
            "plan_version": 5,
            "apply_cursor": terminal.cursor,
            "apply_cursor_version": 5,
            "cursor_stamp": connection.database_now,
            "plan_stamp": connection.database_now,
        },
    ]
    assert [
        event.processing_policy for event, _generation, _fence in inbox.inserted
    ] == [ProcessingPolicy.FULL, ProcessingPolicy.METADATA_ONLY]
    assert inbox.ownership_lock_modes == [False, False, False]
    bind_indices = [
        index
        for index, event in enumerate(connection.events)
        if event == "inbox.bind.plain"
    ]
    assert len(bind_indices) == 3
    for bind_index in bind_indices:
        xid_start = max(
            index
            for index, event in enumerate(connection.events[:bind_index])
            if event == "xid.enter"
        )
        assert "ownership.shared_lock" in connection.events[xid_start:bind_index]
        assert "xid.commit" not in connection.events[xid_start:bind_index]
    assert (
        len(
            [
                identity
                for identity in receipts.receipts
                if identity[1] == "cold_start.apply_page"
            ]
        )
        == 3
    )


@pytest.mark.asyncio
async def test_apply_page_budget_preserves_first_nonterminal_binding() -> None:
    first = _vector_batch(
        cursor="opaque+Apply1/%3D",
        changes=(),
        includes_last=False,
    )
    (
        service,
        connection,
        _receipts,
        ordinary,
        _inbox,
        approved,
    ) = await _c5_approved_service([first], apply_max_pages=1)

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == 1
    assert result.changes_observed == 0
    assert result.safe_code == "cold_start.budget_exhausted"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.APPROVED
    assert connection.cursor_status == "cold_start_applying"
    assert connection.cursor_cold_start_plan_id == approved.plan_id
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
async def test_apply_time_budget_cancels_only_http_and_preserves_approved_state() -> (
    None
):
    blocked = asyncio.Event()
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service(
        [blocked],
        # Leave enough budget for the preflight even under branch-coverage
        # instrumentation so this test deterministically reaches, and times out,
        # the HTTP await it is intended to exercise.
        apply_max_run_seconds=0.1,
    )

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == result.changes_observed == 0
    assert result.safe_code == "cold_start.budget_exhausted"
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.APPROVED
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert connection.cursor_status == "cold_start_pending"
    assert connection.apply_transitions == []
    assert inbox.inserted == []
    assert not any(
        identity[1] == "cold_start.apply_page" for identity in receipts.receipts
    )


@pytest.mark.asyncio
async def test_apply_terminal_page_xid_finishes_after_deadline() -> None:
    terminal = _vector_batch(
        cursor="opaque+Active/%3D",
        changes=(),
        includes_last=True,
    )
    (
        service,
        connection,
        _receipts,
        ordinary,
        _inbox,
        approved,
    ) = await _c5_approved_service(
        [terminal],
        apply_max_run_seconds=0.01,
    )
    connection.cursor_apply_delay = 0.02

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.COMPLETED
    assert result.pages_committed == 1
    assert result.changes_observed == 0
    assert result.safe_code is None
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert connection.cursor_status == "active"
    assert next(iter(connection.plans.values()))["state"] == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize("illegal_state", ["previewing", "ready", "blocked"])
async def test_apply_nonapproved_open_state_is_fixed_conflict_without_http(
    illegal_state: str,
) -> None:
    (
        service,
        connection,
        _receipts,
        ordinary,
        _inbox,
        approved,
    ) = await _c5_approved_service([])
    audits_before = [dict(audit) for audit in connection.audits]
    plan = next(iter(connection.plans.values()))
    plan["state"] = illegal_state
    if illegal_state == "ready":
        plan["approved_at"] = None
    elif illegal_state == "blocked":
        plan["blocked_reason_code"] = "cold_start.version_drift"
        plan["blocked_fingerprint"] = "a" * 64
        plan["blocked_at"] = connection.database_now

    with pytest.raises(
        ColdStartStateConflictError,
        match="^cold-start plan state conflict$",
    ):
        await service.apply(approved.plan_id)

    assert ordinary.calls == []
    assert connection.apply_transitions == []
    assert connection.audits == audits_before


@pytest.mark.asyncio
async def test_apply_existing_prestate_receipt_is_invariant_without_http() -> None:
    (
        service,
        connection,
        receipts,
        ordinary,
        _inbox,
        approved,
    ) = await _c5_approved_service([])
    plan = next(iter(connection.plans.values()))
    payload_hash = _apply_page_payload_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=approved.plan_id,
        plan_version=plan["version"],  # type: ignore[arg-type]
        cursor_status=SyncCursorStatus.COLD_START_PENDING,
        cursor_version=2,
        request_cursor_hash=_cursor_digest("opaque+Boundary/%3D"),
    )
    receipts.receipts[(8, "cold_start.apply_page", payload_hash)] = CommandReceipt(
        id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        account_id=8,
        command_name="cold_start.apply_page",
        idempotency_key_hash=_hash_idempotency_key(
            8,
            "cold_start.apply_page",
            payload_hash,
        ),
        canonical_payload_hash=payload_hash,
        outcome="succeeded",
        result_type="sync_cold_start_plan",
        result_id=str(approved.plan_id),
        result_hash="a" * 64,
        authority_epoch=9,
        created_at=connection.database_now,
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(approved.plan_id)

    assert caught.value.operation == "cold_start_apply_preflight"
    assert caught.value.retryable is False
    assert ordinary.calls == []
    assert connection.apply_transitions == []


@pytest.mark.asyncio
async def test_apply_validates_input_before_locator_and_busy_after_policy() -> None:
    (
        service,
        connection,
        _receipts,
        ordinary,
        _inbox,
        approved,
    ) = await _c5_approved_service([])

    with pytest.raises(ValueError):
        await service.apply(True)  # type: ignore[arg-type]

    assert connection.events == []
    service._session_runner = _C5BusyRunner(connection.events)  # type: ignore[assignment]

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.BUSY_SKIP
    assert result.plan is None
    assert result.pages_committed == result.changes_observed == 0
    assert result.safe_code == "cold_start.busy"
    assert connection.events == ["locator", "snapshot", "permit"]
    assert ordinary.calls == []


@dataclass(frozen=True, slots=True)
class _C9DurableApplySnapshot:
    plans: dict[UUID, dict[str, object]]
    cursor: tuple[object, ...]
    receipts: dict[tuple[int, str, str], CommandReceipt]
    inbox: list[tuple[object, int, int]]
    audits: list[dict[str, object]]
    apply_transitions: list[dict[str, object]]
    successful_commit_count: int
    commit_events: int
    rollback_events: int


def _c9_durable_apply_snapshot(
    connection: _C5ApplyConnection,
    receipts: _C1ReceiptRepository,
    inbox: _C5InboxRepository,
) -> _C9DurableApplySnapshot:
    return _C9DurableApplySnapshot(
        plans=deepcopy(connection.plans),
        cursor=_c8_full_cursor_snapshot(connection),
        receipts=deepcopy(receipts.receipts),
        inbox=deepcopy(inbox.inserted),
        audits=deepcopy(connection.audits),
        apply_transitions=deepcopy(connection.apply_transitions),
        successful_commit_count=connection.successful_commit_count,
        commit_events=connection.events.count("xid.commit"),
        rollback_events=connection.events.count("xid.rollback"),
    )


def _c9_install_body_fault(
    connection: _C5ApplyConnection,
    point: str,
) -> BaseException | None:
    if point == "success.receipt_conflict":
        error: BaseException | None = IdempotencyConflict()
    elif point == "success.receipt_hostile_projection":
        error = None
    else:
        error = RuntimeError(f"F5 body fault at {point}")
    connection.body_fault_point = point  # type: ignore[attr-defined]
    connection.body_fault_error = error  # type: ignore[attr-defined]
    return error


def _c9_assert_body_fault_rolled_back(
    *,
    service: ColdStartService,
    connection: _C5ApplyConnection,
    receipts: _C1ReceiptRepository,
    ordinary: _C5OrdinaryClient,
    inbox: _C5InboxRepository,
    before: _C9DurableApplySnapshot,
    request_cursor: str,
) -> None:
    assert connection.plans == before.plans
    assert _c8_full_cursor_snapshot(connection) == before.cursor
    assert receipts.receipts == before.receipts
    assert inbox.inserted == before.inbox
    assert connection.audits == before.audits
    assert connection.apply_transitions == before.apply_transitions
    assert connection.info.transaction_status is TransactionStatus.IDLE
    assert connection.successful_commit_count == before.successful_commit_count + 1
    assert connection.events.count("xid.commit") == before.commit_events + 1
    assert connection.events.count("xid.rollback") == before.rollback_events + 1
    assert connection.events[-1] == "xid.rollback"
    assert not any(
        event.startswith("xid.commit_unknown") for event in connection.events
    )
    assert type(service._session_runner) is _C1RetainedRunner
    assert service._session_runner.session.tainted is False  # type: ignore[attr-defined]
    assert ordinary.calls == [(8, "Inbox", request_cursor, 100)]
    assert ordinary.outcomes == []


class _C9HostileReceipt:
    descriptor_reads = 0

    def __getattribute__(self, name: str) -> object:
        if name not in {"descriptor_reads", "__class__"}:
            type(self).descriptor_reads += 1
            raise AssertionError("hostile receipt descriptor was read")
        return super().__getattribute__(name)


_C9_APPLY_DML_EVENTS = frozenset(
    {
        "inbox.insert",
        "cursor.apply_page",
        "plan.apply_page",
        "receipt.insert",
        "plan.block",
        "cursor.block",
        "audit.block",
        "cursor.apply_retry",
    }
)


def _c9_apply_dml_events(connection: _C5ApplyConnection) -> list[str]:
    """Project the review-derived F5 DML order and downstream absence contract."""

    return [event for event in connection.events if event in _C9_APPLY_DML_EVENTS]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_point",
    [
        "success.inbox_after_append",
        "success.cursor_after_mutation",
        "success.plan_after_mutation",
        "success.plan_bad_projection",
        "success.receipt_after_write",
        "success.receipt_conflict",
        "success.receipt_hostile_projection",
    ],
    ids=[
        "inbox",
        "cursor-13-fields",
        "plan-transition",
        "plan-projection",
        "receipt-after-write",
        "receipt-conflict",
        "hostile-receipt",
    ],
)
async def test_apply_success_page_body_fault_rolls_back_atomically(
    fault_point: str,
) -> None:
    terminal = _vector_batch(
        cursor="opaque+F5Terminal/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="f5-success-body-fault",
                item={"subject": "must roll back"},
                source_version="v2",
            ),
        ),
        includes_last=True,
    )
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([terminal])
    before = _c9_durable_apply_snapshot(connection, receipts, inbox)
    _C9HostileReceipt.descriptor_reads = 0
    error = _c9_install_body_fault(connection, fault_point)

    with pytest.raises(RuntimeError) as caught:
        await service.apply(approved.plan_id)

    if fault_point == "success.plan_bad_projection":
        assert type(caught.value) is ColdStartStateConflictError
    elif fault_point == "success.receipt_hostile_projection":
        assert type(caught.value) is DatabaseOperationError
        assert caught.value.operation == "cold_start_apply_receipt"
        assert caught.value.retryable is False
        assert _C9HostileReceipt.descriptor_reads == 0
    else:
        assert error is not None
        assert caught.value is error
    if fault_point == "success.inbox_after_append":
        expected_dml_events = ["inbox.insert"]
    elif fault_point == "success.cursor_after_mutation":
        expected_dml_events = ["inbox.insert", "cursor.apply_page"]
    elif fault_point in {
        "success.plan_after_mutation",
        "success.plan_bad_projection",
    }:
        expected_dml_events = [
            "inbox.insert",
            "cursor.apply_page",
            "plan.apply_page",
        ]
    else:
        expected_dml_events = [
            "inbox.insert",
            "cursor.apply_page",
            "plan.apply_page",
            "receipt.insert",
        ]
    assert _c9_apply_dml_events(connection) == expected_dml_events
    _c9_assert_body_fault_rolled_back(
        service=service,
        connection=connection,
        receipts=receipts,
        ordinary=ordinary,
        inbox=inbox,
        before=before,
        request_cursor="opaque+Boundary/%3D",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_point",
    [
        "block.plan_after_mutation",
        "block.cursor_after_mutation",
        "block.audit_after_append",
    ],
    ids=["plan", "cursor-13-fields", "audit"],
)
async def test_apply_fatal_block_body_fault_rolls_back_atomically(
    fault_point: str,
) -> None:
    fatal = SyncAuthorizationError()
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([fatal])
    before = _c9_durable_apply_snapshot(connection, receipts, inbox)
    error = _c9_install_body_fault(connection, fault_point)
    assert error is not None

    with pytest.raises(RuntimeError) as caught:
        await service.apply(approved.plan_id)

    assert caught.value is error
    if fault_point == "block.plan_after_mutation":
        expected_dml_events = ["plan.block"]
    elif fault_point == "block.cursor_after_mutation":
        expected_dml_events = ["plan.block", "cursor.block"]
    else:
        expected_dml_events = ["plan.block", "cursor.block", "audit.block"]
    assert _c9_apply_dml_events(connection) == expected_dml_events
    _c9_assert_body_fault_rolled_back(
        service=service,
        connection=connection,
        receipts=receipts,
        ordinary=ordinary,
        inbox=inbox,
        before=before,
        request_cursor="opaque+Boundary/%3D",
    )


@pytest.mark.asyncio
async def test_apply_retry_update_body_fault_rolls_back_atomically() -> None:
    transient = SyncTransientError(retry_after_seconds=17)
    (
        service,
        connection,
        receipts,
        ordinary,
        inbox,
        applying,
    ) = await _c6_applying_service([transient])
    before = _c9_durable_apply_snapshot(connection, receipts, inbox)
    error = _c9_install_body_fault(connection, "retry.cursor_after_mutation")
    assert error is not None

    with pytest.raises(RuntimeError) as caught:
        await service.apply(applying.plan_id)

    assert caught.value is error
    assert _c9_apply_dml_events(connection) == ["cursor.apply_retry"]
    _c9_assert_body_fault_rolled_back(
        service=service,
        connection=connection,
        receipts=receipts,
        ordinary=ordinary,
        inbox=inbox,
        before=before,
        request_cursor="opaque+Apply1/%3D",
    )


class _C10ApplyRecoveryRunner:
    """Two retained sessions sharing one durable fake database."""

    def __init__(
        self,
        first: _C5ApplyConnection,
        second: _C5ApplyConnection,
        receipts: _C1ReceiptRepository,
        inbox: _C5InboxRepository,
    ) -> None:
        self.connections = (first, second)
        self.receipts = receipts
        self.inbox = inbox
        self.calls = 0
        self.sessions: list[_SyncSessionLease] = []
        self.before_recovery: object | None = None
        self.recovery_busy = False
        self.second_unknown = False

    @staticmethod
    def _copy_durable_state(
        source: _C5ApplyConnection,
        target: _C5ApplyConnection,
    ) -> None:
        target.plans = deepcopy(source.plans)
        target.cursor_status = source.cursor_status
        target.cursor_value = source.cursor_value
        target.cursor_version = source.cursor_version
        target.cursor_blocked_reason = source.cursor_blocked_reason
        target.cursor_contract_fingerprint = source.cursor_contract_fingerprint
        target.cursor_blocked_at = source.cursor_blocked_at
        target.cursor_transient_failures = source.cursor_transient_failures
        target.cursor_retry_after_at = source.cursor_retry_after_at
        target.cursor_cold_start_plan_id = source.cursor_cold_start_plan_id
        target.cursor_cold_start_plan_state = source.cursor_cold_start_plan_state
        target.cursor_last_attempt_at = source.cursor_last_attempt_at
        target.cursor_last_success_at = source.cursor_last_success_at
        target.cursor_updated_at = source.cursor_updated_at
        target.ownership_pipeline_name = source.ownership_pipeline_name
        target.ownership_generation = source.ownership_generation
        target.ownership_fencing_token = source.ownership_fencing_token
        target.database_now = source.database_now
        target.audits = deepcopy(source.audits)
        target.apply_transitions = deepcopy(source.apply_transitions)

    async def run(
        self,
        account_id: int,
        canonical_folder: str,
        operation: object,
    ) -> _C1RunnerOutcome:
        assert (account_id, canonical_folder) == (8, "INBOX")
        assert self.calls < 2
        if self.calls == 1:
            first, second = self.connections
            self._copy_durable_state(first, second)
            self.receipts.connection = second
            second.receipt_repository = self.receipts  # type: ignore[attr-defined]
            self.inbox.connection = second
            second.inbox_repository = self.inbox  # type: ignore[attr-defined]
            if callable(self.before_recovery):
                self.before_recovery()
            if self.second_unknown:
                second.unknown_commit_outcomes[second.successful_commit_count + 1] = (
                    "post"
                )
        connection = self.connections[self.calls]
        self.calls += 1
        if self.calls == 2 and self.recovery_busy:
            return _C1RunnerOutcome(acquired=False, value=None)
        session = _SyncSessionLease(connection)
        self.sessions.append(session)
        value = await operation(session)  # type: ignore[operator]
        return _C1RunnerOutcome(acquired=True, value=value)


async def _c10_apply_ack_loss_service(
    *,
    outcome: str,
    terminal: bool,
) -> tuple[
    ColdStartService,
    _C10ApplyRecoveryRunner,
    _C1ReceiptRepository,
    _C5OrdinaryClient,
    _C5InboxRepository,
    ColdStartPlanView,
    SyncBatch,
]:
    batch = _vector_batch(
        cursor=("opaque+C10Terminal/%3D" if terminal else "opaque+C10Nonterminal/%3D"),
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="c10-ack-loss",
                source_version="v2",
                item={"subject": "retained apply page"},
            ),
        ),
        includes_last=terminal,
    )
    (
        service,
        first,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([batch], apply_max_pages=5)
    scope = _c1_snapshot().scopes[0]
    second = _C5ApplyConnection(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
        folder_scope_config_hash=scope.config_hash,
    )
    first.unknown_commit_outcomes[first.successful_commit_count + 2] = outcome
    runner = _C10ApplyRecoveryRunner(first, second, receipts, inbox)
    service._session_runner = runner  # type: ignore[assignment]
    return service, runner, receipts, ordinary, inbox, approved, batch


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", [True, False], ids=["terminal", "nonterminal"])
@pytest.mark.parametrize("outcome", ["post", "pre"], ids=["exact-post", "exact-pre"])
async def test_apply_commit_ack_loss_recovers_exact_post_or_replays_exact_pre(
    terminal: bool,
    outcome: str,
) -> None:
    (
        service,
        runner,
        receipts,
        ordinary,
        inbox,
        approved,
        batch,
    ) = await _c10_apply_ack_loss_service(outcome=outcome, terminal=terminal)

    result = await service.apply(approved.plan_id)

    assert result.status is (
        ColdStartRunStatus.COMPLETED if terminal else ColdStartRunStatus.APPROVED
    )
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert result.safe_code is None
    assert result.plan is not None
    assert result.plan.state is (
        ColdStartPlanState.COMPLETED if terminal else ColdStartPlanState.APPROVED
    )
    recovered_row = runner.connections[1].plans[approved.plan_id]
    assert recovered_row["apply_cursor"] == batch.cursor
    assert runner.calls == 2
    assert runner.sessions[0].tainted is True
    assert runner.sessions[1].tainted is False
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert len(inbox.inserted) == 1
    assert (
        len(
            [
                identity
                for identity in receipts.receipts
                if identity[1] == "cold_start.apply_page"
            ]
        )
        == 1
    )
    recovery_events = runner.connections[1].events
    assert recovery_events.count("cursor.apply_page") == (0 if outcome == "post" else 1)
    assert recovery_events.count("plan.apply_page") == (0 if outcome == "post" else 1)
    assert recovery_events.count("receipt.insert") == (0 if outcome == "post" else 1)


@pytest.mark.asyncio
async def test_apply_exact_post_ack_recovery_ignores_later_ownership_fence() -> None:
    (
        service,
        runner,
        receipts,
        ordinary,
        _inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="post", terminal=True)
    second = runner.connections[1]

    def advance_fence() -> None:
        second.ownership_fencing_token += 1

    runner.before_recovery = advance_fence

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.COMPLETED
    assert result.pages_committed == 1
    assert result.changes_observed == 1
    assert runner.calls == 2
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert len(_c10_apply_receipt_identities(receipts)) == 1
    assert second.events.count("cursor.apply_page") == 0
    assert second.events.count("plan.apply_page") == 0


def _c10_apply_receipt_identities(
    receipts: _C1ReceiptRepository,
) -> list[tuple[int, str, str]]:
    return [
        identity
        for identity in receipts.receipts
        if identity[1] == "cold_start.apply_page"
    ]


def _c10_install_valid_prestate_receipt(
    *,
    connection: _C5ApplyConnection,
    receipts: _C1ReceiptRepository,
    approved: ColdStartPlanView,
    batch: SyncBatch,
) -> None:
    row = connection.plans[approved.plan_id]
    request_cursor = row["boundary_cursor"]
    assert type(request_cursor) is str
    payload_hash = _apply_page_payload_digest(
        account_id=8,
        canonical_folder="INBOX",
        plan_id=approved.plan_id,
        plan_version=row["version"],  # type: ignore[arg-type]
        cursor_status=SyncCursorStatus(connection.cursor_status),
        cursor_version=connection.cursor_version,
        request_cursor_hash=_cursor_digest(request_cursor),
    )
    identity = (8, "cold_start.apply_page", payload_hash)
    receipts.receipts[identity] = CommandReceipt(
        id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        account_id=8,
        command_name="cold_start.apply_page",
        idempotency_key_hash=_hash_idempotency_key(*identity),
        canonical_payload_hash=payload_hash,
        outcome="succeeded",
        result_type="sync_cold_start_plan",
        result_id=str(approved.plan_id),
        result_hash=_apply_page_result_digest(_batch_digest(batch)),
        authority_epoch=9,
        created_at=_CREATED_AT,
    )


def _c10_assert_apply_recovery_invariant(error: DatabaseOperationError) -> None:
    assert error.operation == "cold_start_apply_recovery"
    assert error.retryable is False
    assert str(error) == "cold-start apply recovery state is invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "variant",
    [
        "timestamp-shifted-post",
        "post-without-receipt",
        "pre-with-receipt",
        "mixed-plan-cursor",
        "advanced-terminal",
    ],
)
async def test_apply_commit_ack_recovery_rejects_nonexact_durable_state(
    variant: str,
) -> None:
    outcome = "pre" if variant == "pre-with-receipt" else "post"
    terminal = variant != "advanced-terminal"
    (
        service,
        runner,
        receipts,
        ordinary,
        _inbox,
        approved,
        batch,
    ) = await _c10_apply_ack_loss_service(outcome=outcome, terminal=terminal)
    first, second = runner.connections
    pre_plan = deepcopy(first.plans[approved.plan_id])
    pre_cursor = _c8_full_cursor_snapshot(first)

    def mutate() -> None:
        if variant == "timestamp-shifted-post":
            shifted = second.cursor_updated_at + timedelta(microseconds=1)
            second.cursor_last_attempt_at = shifted
            second.cursor_last_success_at = shifted
            second.cursor_updated_at = shifted
            row = second.plans[approved.plan_id]
            row["completed_at"] = shifted
            row["updated_at"] = shifted
        elif variant == "post-without-receipt":
            identity = _c10_apply_receipt_identities(receipts)
            assert len(identity) == 1
            del receipts.receipts[identity[0]]
        elif variant == "pre-with-receipt":
            _c10_install_valid_prestate_receipt(
                connection=second,
                receipts=receipts,
                approved=approved,
                batch=batch,
            )
        elif variant == "mixed-plan-cursor":
            (
                second.cursor_status,
                second.cursor_value,
                second.cursor_version,
                second.cursor_blocked_reason,
                second.cursor_contract_fingerprint,
                second.cursor_blocked_at,
                second.cursor_transient_failures,
                second.cursor_retry_after_at,
                second.cursor_cold_start_plan_id,
                second.cursor_cold_start_plan_state,
                second.cursor_last_attempt_at,
                second.cursor_last_success_at,
                second.cursor_updated_at,
            ) = pre_cursor
        else:
            stamp = second.cursor_updated_at + timedelta(microseconds=1)
            later_cursor = "opaque+C10LaterTerminal/%3D"
            second.cursor_status = "active"
            second.cursor_value = later_cursor
            second.cursor_version += 1
            second.cursor_cold_start_plan_id = None
            second.cursor_cold_start_plan_state = None
            second.cursor_last_attempt_at = stamp
            second.cursor_last_success_at = stamp
            second.cursor_updated_at = stamp
            row = second.plans[approved.plan_id]
            row.update(
                {
                    "state": "completed",
                    "version": row["version"] + 1,
                    "apply_cursor": later_cursor,
                    "apply_cursor_version": second.cursor_version,
                    "completed_at": stamp,
                    "updated_at": stamp,
                }
            )

    runner.before_recovery = mutate

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(approved.plan_id)

    _c10_assert_apply_recovery_invariant(caught.value)
    assert runner.calls == 2
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    if variant == "mixed-plan-cursor":
        assert second.plans[approved.plan_id] != pre_plan


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("result_hash", "f" * 64),
        ("authority_epoch", 10),
        ("result_id", "00000000-0000-4000-8000-000000000000"),
        ("id", UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")),
        ("created_at", _CREATED_AT + timedelta(microseconds=1)),
    ],
    ids=["result-hash", "authority", "identity", "receipt-id", "created-at"],
)
async def test_apply_commit_ack_recovery_rejects_tampered_receipt(
    field: str,
    value: object,
) -> None:
    (
        service,
        runner,
        receipts,
        ordinary,
        _inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="post", terminal=True)

    def mutate() -> None:
        identities = _c10_apply_receipt_identities(receipts)
        assert len(identities) == 1
        identity = identities[0]
        receipts.receipts[identity] = replace(
            receipts.receipts[identity],
            **{field: value},
        )

    runner.before_recovery = mutate

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(approved.plan_id)

    _c10_assert_apply_recovery_invariant(caught.value)
    assert runner.calls == 2
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
async def test_apply_commit_ack_recovery_maps_idempotency_conflict_to_invariant() -> (
    None
):
    (
        service,
        runner,
        receipts,
        ordinary,
        _inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="post", terminal=True)

    def mutate() -> None:
        identities = _c10_apply_receipt_identities(receipts)
        assert len(identities) == 1
        identity = identities[0]
        receipts.receipts[identity] = replace(
            receipts.receipts[identity],
            canonical_payload_hash="f" * 64,
        )

    runner.before_recovery = mutate

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(approved.plan_id)

    _c10_assert_apply_recovery_invariant(caught.value)
    assert runner.calls == 2
    assert runner.connections[1].events[-1] == "xid.rollback"
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
async def test_apply_commit_ack_recovery_maps_persisted_invalid_receipt_to_invariant() -> (
    None
):
    (
        service,
        runner,
        receipts,
        ordinary,
        _inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="post", terminal=True)
    persisted_invalid = RuntimeError("command_receipt_persisted_invalid")

    def mutate() -> None:
        receipts.lookup_error = persisted_invalid

    runner.before_recovery = mutate

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(approved.plan_id)

    _c10_assert_apply_recovery_invariant(caught.value)
    assert runner.calls == 2
    assert runner.connections[1].events[-1] == "xid.rollback"
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lookup_error",
    [
        RuntimeError("command_receipt_transaction_invalid"),
        UniqueViolation("receipt lookup database failure"),
        asyncio.CancelledError("receipt lookup cancelled"),
    ],
    ids=["unrelated-runtime", "database-driver", "process-control"],
)
async def test_apply_commit_ack_recovery_preserves_non_evidence_lookup_error(
    lookup_error: BaseException,
) -> None:
    (
        service,
        runner,
        receipts,
        ordinary,
        _inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="post", terminal=True)

    def mutate() -> None:
        receipts.lookup_error = lookup_error

    runner.before_recovery = mutate

    with pytest.raises(BaseException) as caught:
        await service.apply(approved.plan_id)

    assert caught.value is lookup_error
    assert runner.calls == 2
    assert runner.connections[1].events[-1] == "xid.rollback"
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
async def test_apply_commit_ack_recovery_second_unknown_has_no_third_runner() -> None:
    (
        service,
        runner,
        _receipts,
        ordinary,
        _inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="pre", terminal=True)
    second_error = RuntimeError("second recovery commit acknowledgement lost")
    runner.connections[1].unknown_commit_error = second_error  # type: ignore[attr-defined]
    runner.second_unknown = True

    with pytest.raises(RuntimeError) as caught:
        await service.apply(approved.plan_id)

    assert caught.value is second_error
    assert runner.calls == 2
    assert len(runner.sessions) == 2
    assert runner.sessions[0].tainted is True
    assert runner.sessions[1].tainted is True
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
async def test_apply_commit_ack_recovery_busy_rethrows_first_error_object() -> None:
    (
        service,
        runner,
        _receipts,
        ordinary,
        _inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="post", terminal=True)
    first_error = RuntimeError("first apply commit acknowledgement lost")
    runner.connections[0].unknown_commit_error = first_error  # type: ignore[attr-defined]
    runner.recovery_busy = True

    with pytest.raises(RuntimeError) as caught:
        await service.apply(approved.plan_id)

    assert caught.value is first_error
    assert runner.calls == 2
    assert len(runner.sessions) == 1
    assert runner.sessions[0].tainted is True
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
async def test_apply_second_page_ack_loss_counts_each_page_once_without_more_http() -> (
    None
):
    first_batch = _vector_batch(
        cursor="opaque+C10First/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="c10-first",
                source_version="v2",
                item={"subject": "first"},
            ),
        ),
        includes_last=False,
    )
    terminal_batch = _vector_batch(
        cursor="opaque+C10Second/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.UPDATE,
                external_email_id="c10-first",
                source_version="v3",
                item={"id": "c10-first", "is_read": True},
            ),
        ),
        includes_last=True,
    )
    (
        service,
        first,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([first_batch, terminal_batch], apply_max_pages=5)
    scope = _c1_snapshot().scopes[0]
    second = _C5ApplyConnection(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
        folder_scope_config_hash=scope.config_hash,
    )
    first.unknown_commit_outcomes[first.successful_commit_count + 4] = "post"
    runner = _C10ApplyRecoveryRunner(first, second, receipts, inbox)
    service._session_runner = runner  # type: ignore[assignment]

    result = await service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.COMPLETED
    assert result.pages_committed == 2
    assert result.changes_observed == 2
    assert runner.calls == 2
    assert ordinary.calls == [
        (8, "Inbox", "opaque+Boundary/%3D", 100),
        (8, "Inbox", first_batch.cursor, 100),
    ]
    assert len(inbox.inserted) == 2
    assert len(_c10_apply_receipt_identities(receipts)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [RuntimeError("F6 body sentinel"), asyncio.CancelledError("F6 cancel sentinel")],
    ids=["ordinary-error", "process-control"],
)
async def test_apply_body_failure_never_enters_ack_recovery(
    error: BaseException,
) -> None:
    terminal = _vector_batch(
        cursor="opaque+C10Body/%3D",
        changes=(),
        includes_last=True,
    )
    (
        service,
        first,
        receipts,
        ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service([terminal])
    scope = _c1_snapshot().scopes[0]
    second = _C5ApplyConnection(
        cursor_status="cold_start_pending",
        cursor_value=None,
        cursor_version=2,
        folder_scope_config_hash=scope.config_hash,
    )
    runner = _C10ApplyRecoveryRunner(first, second, receipts, inbox)
    service._session_runner = runner  # type: ignore[assignment]
    first.body_fault_point = "success.receipt_after_write"  # type: ignore[attr-defined]
    first.body_fault_error = error  # type: ignore[attr-defined]

    with pytest.raises(BaseException) as caught:
        await service.apply(approved.plan_id)

    assert caught.value is error
    assert runner.calls == 1
    assert len(runner.sessions) == 1
    assert runner.sessions[0].tainted is False
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@pytest.mark.asyncio
async def test_apply_prestate_ack_replay_expiry_race_rolls_back_block_mutation() -> (
    None
):
    (
        service,
        runner,
        receipts,
        ordinary,
        inbox,
        approved,
        _batch,
    ) = await _c10_apply_ack_loss_service(outcome="pre", terminal=True)
    pre = _c9_durable_apply_snapshot(runner.connections[0], receipts, inbox)

    def expire_during_replay() -> None:
        runner.connections[1].expire_on_clock_read = 2

    runner.before_recovery = expire_during_replay

    with pytest.raises(DatabaseOperationError) as caught:
        await service.apply(approved.plan_id)

    _c10_assert_apply_recovery_invariant(caught.value)
    recovered_connection = runner.connections[1]
    assert recovered_connection.plans == pre.plans
    assert _c8_full_cursor_snapshot(recovered_connection) == pre.cursor
    assert receipts.receipts == pre.receipts
    assert inbox.inserted == pre.inbox
    assert recovered_connection.audits == pre.audits
    assert recovered_connection.apply_transitions == pre.apply_transitions
    assert recovered_connection.events[-1] == "xid.rollback"
    assert ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]


@dataclass(frozen=True, slots=True)
class _C11CursorProcessImage:
    status: str
    value: str | None
    version: int
    blocked_reason: str | None
    contract_fingerprint: str | None
    blocked_at: datetime | None
    transient_failures: int
    retry_after_at: datetime | None
    cold_start_plan_id: UUID | None
    cold_start_plan_state: str | None
    last_attempt_at: datetime | None
    last_success_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class _C11DurableProcessImage:
    plans: dict[UUID, dict[str, object]]
    cursor: _C11CursorProcessImage
    ownership_pipeline_name: str
    ownership_generation: int
    ownership_fencing_token: int
    database_now: datetime
    receipts: dict[tuple[int, str, str], CommandReceipt]
    inbox: list[tuple[object, int, int]]
    audits: list[dict[str, object]]
    folder_scope_config_hash: str
    scope_canonical_key: str
    scope_webhook_ids: tuple[str, ...]
    scope_sync_folder: str
    scope_policy_matrix: tuple[
        tuple[IngressSource, str, ChangeKind, ProcessingPolicy], ...
    ]


class _C11ReceiptTransaction(_C1ReceiptTransaction):
    def __init__(self, repository: _C11ReceiptRepository) -> None:
        super().__init__(repository)
        self._c11_repository = repository

    async def lookup(
        self,
        *,
        account_id: int,
        command_name: str,
        idempotency_key: str,
        canonical_payload_hash: str,
    ) -> CommandReceipt | None:
        self._c11_repository.lookup_keys.append(
            (
                account_id,
                command_name,
                idempotency_key,
                canonical_payload_hash,
            )
        )
        return await super().lookup(
            account_id=account_id,
            command_name=command_name,
            idempotency_key=idempotency_key,
            canonical_payload_hash=canonical_payload_hash,
        )


class _C11ReceiptRepository(_C1ReceiptRepository):
    def __init__(self, connection: _C5ApplyConnection) -> None:
        super().__init__(connection)
        self.lookup_keys: list[tuple[int, str, str, str]] = []

    def transaction(self, connection: object) -> _C11ReceiptTransaction:
        assert connection is self.connection
        return _C11ReceiptTransaction(self)


class _C11SingleInvocationRunner:
    def __init__(self, connection: _C5ApplyConnection) -> None:
        self.connection = connection
        self.calls = 0
        self.sessions: list[_SyncSessionLease] = []

    async def run(
        self,
        account_id: int,
        canonical_folder: str,
        operation: object,
    ) -> _C1RunnerOutcome:
        assert (account_id, canonical_folder) == (8, "INBOX")
        self.calls += 1
        if self.calls != 1:
            raise AssertionError("fresh process entered retained F6 recovery")
        session = _SyncSessionLease(self.connection)
        self.sessions.append(session)
        value = await operation(session)  # type: ignore[operator]
        return _C1RunnerOutcome(acquired=True, value=value)


@dataclass(frozen=True, slots=True)
class _C11FreshProcess:
    service: ColdStartService
    connection: _C5ApplyConnection
    runner: _C11SingleInvocationRunner
    ordinary: _C5OrdinaryClient
    snapshot_provider: _C1SnapshotProvider
    policy_resolver: ProcessingPolicyResolver
    receipts: _C11ReceiptRepository
    inbox: _C5InboxRepository
    scope: FolderScope
    locator: object


def _c11_capture_durable_process_image(
    *,
    connection: _C5ApplyConnection,
    receipts: _C1ReceiptRepository,
    inbox: _C5InboxRepository,
    scope: FolderScope,
) -> _C11DurableProcessImage:
    return _C11DurableProcessImage(
        plans=deepcopy(connection.plans),
        cursor=_C11CursorProcessImage(
            status=connection.cursor_status,
            value=connection.cursor_value,
            version=connection.cursor_version,
            blocked_reason=connection.cursor_blocked_reason,
            contract_fingerprint=connection.cursor_contract_fingerprint,
            blocked_at=connection.cursor_blocked_at,
            transient_failures=connection.cursor_transient_failures,
            retry_after_at=connection.cursor_retry_after_at,
            cold_start_plan_id=connection.cursor_cold_start_plan_id,
            cold_start_plan_state=connection.cursor_cold_start_plan_state,
            last_attempt_at=connection.cursor_last_attempt_at,
            last_success_at=connection.cursor_last_success_at,
            updated_at=connection.cursor_updated_at,
        ),
        ownership_pipeline_name=connection.ownership_pipeline_name,
        ownership_generation=connection.ownership_generation,
        ownership_fencing_token=connection.ownership_fencing_token,
        database_now=connection.database_now,
        receipts=deepcopy(receipts.receipts),
        # Normalized ingress events are frozen and their payloads are immutable.
        inbox=list(inbox.inserted),
        audits=deepcopy(connection.audits),
        folder_scope_config_hash=connection.folder_scope_config_hash,
        scope_canonical_key=scope.canonical_key,
        scope_webhook_ids=tuple(sorted(scope.webhook_ids)),
        scope_sync_folder=scope.sync_folder,
        scope_policy_matrix=tuple(
            (source, raw_event_type, kind, policy)
            for (
                source,
                raw_event_type,
                kind,
            ), policy in scope.event_policy_matrix.items()
        ),
    )


def _c11_scope_from_process_image(image: _C11DurableProcessImage) -> FolderScope:
    return FolderScope.configured(
        canonical_key=image.scope_canonical_key,
        webhook_ids=image.scope_webhook_ids,
        sync_folder=image.scope_sync_folder,
        event_policy_matrix={
            (source, raw_event_type, kind): policy
            for source, raw_event_type, kind, policy in image.scope_policy_matrix
        },
    )


def _c11_rebuild_fresh_process(
    image: _C11DurableProcessImage,
    outcomes: list[object],
    *,
    apply_max_pages: int,
    scope: FolderScope | None = None,
    database_now: datetime | None = None,
    ownership_fencing_token: int | None = None,
) -> _C11FreshProcess:
    current_scope = _c11_scope_from_process_image(image) if scope is None else scope
    cursor = image.cursor
    connection = _C5ApplyConnection(
        cursor_status=cursor.status,
        cursor_value=cursor.value,
        cursor_version=cursor.version,
        folder_scope_config_hash=image.folder_scope_config_hash,
    )
    connection.cursor_blocked_reason = cursor.blocked_reason
    connection.cursor_contract_fingerprint = cursor.contract_fingerprint
    connection.cursor_blocked_at = cursor.blocked_at
    connection.cursor_transient_failures = cursor.transient_failures
    connection.cursor_retry_after_at = cursor.retry_after_at
    connection.cursor_cold_start_plan_id = cursor.cold_start_plan_id
    connection.cursor_cold_start_plan_state = cursor.cold_start_plan_state
    connection.cursor_last_attempt_at = cursor.last_attempt_at
    connection.cursor_last_success_at = cursor.last_success_at
    connection.cursor_updated_at = cursor.updated_at
    connection.ownership_pipeline_name = image.ownership_pipeline_name
    connection.ownership_generation = image.ownership_generation
    connection.ownership_fencing_token = (
        image.ownership_fencing_token
        if ownership_fencing_token is None
        else ownership_fencing_token
    )
    connection.database_now = (
        image.database_now if database_now is None else database_now
    )
    connection.plans = deepcopy(image.plans)
    connection.audits = deepcopy(image.audits)

    ordinary = _C5OrdinaryClient(list(outcomes), connection.events)
    snapshot_provider = _C1SnapshotProvider(
        PolicySnapshot(scopes=(current_scope,)),
        connection.events,
    )
    policy_resolver = ProcessingPolicyResolver()
    receipts = _C11ReceiptRepository(connection)
    receipts.receipts = deepcopy(image.receipts)
    inbox = _C5InboxRepository(connection)
    inbox.inserted = list(image.inbox)
    service = ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            cold_start_origin=_C1ForbiddenOrigin(connection.events),
            ordinary_page_client=ordinary,
            snapshot_provider=snapshot_provider,
            policy_resolver=policy_resolver,
            receipt_repository=receipts,
            inbox_repository=inbox,
            apply_max_pages=apply_max_pages,
        )
    )
    runner = _C11SingleInvocationRunner(connection)
    service._session_runner = runner  # type: ignore[assignment]
    _install_c2_locator(service, connection)
    locator = service._locate_plan_identity  # type: ignore[attr-defined]

    # Fresh process-local facts are deliberately not accepted from the image.
    assert connection.events == []
    assert connection.successful_commit_count == 0
    assert connection.unknown_commit_outcomes == {}
    assert connection.apply_transitions == []
    assert runner.calls == 0 and runner.sessions == []
    return _C11FreshProcess(
        service=service,
        connection=connection,
        runner=runner,
        ordinary=ordinary,
        snapshot_provider=snapshot_provider,
        policy_resolver=policy_resolver,
        receipts=receipts,
        inbox=inbox,
        scope=current_scope,
        locator=locator,
    )


async def _c11_approved_process_image(
    *,
    cursor_status: str = "cold_start_pending",
    cursor_value: str | None = None,
    cursor_version: int = 2,
) -> tuple[
    _C11DurableProcessImage,
    ColdStartPlanView,
]:
    (
        _service,
        connection,
        receipts,
        _ordinary,
        inbox,
        approved,
    ) = await _c5_approved_service(
        [],
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
    )
    scope = _c1_snapshot().scopes[0]
    return (
        _c11_capture_durable_process_image(
            connection=connection,
            receipts=receipts,
            inbox=inbox,
            scope=scope,
        ),
        approved,
    )


@pytest.mark.asyncio
async def test_apply_fresh_process_resumes_durable_progress_and_completed_replay() -> (
    None
):
    first = _vector_batch(
        cursor="opaque+C11First/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="c11-first",
                source_version="v1",
                item={"subject": "first durable process page"},
            ),
        ),
        includes_last=False,
    )
    terminal = _vector_batch(
        cursor="opaque+C11Terminal/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.UPDATE,
                external_email_id="c11-first",
                source_version="v2",
                item={"id": "c11-first", "is_read": True},
            ),
        ),
        includes_last=True,
    )
    seed, approved = await _c11_approved_process_image()
    process_a = _c11_rebuild_fresh_process(seed, [first], apply_max_pages=1)

    result_a = await process_a.service.apply(approved.plan_id)

    assert result_a.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert result_a.pages_committed == result_a.changes_observed == 1
    assert process_a.ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert process_a.runner.calls == 1 and len(process_a.runner.sessions) == 1
    assert process_a.connection.cursor_value == first.cursor
    assert process_a.connection.cursor_status == "cold_start_applying"
    first_receipts = _c10_apply_receipt_identities(process_a.receipts)
    assert len(first_receipts) == 1
    image_after_a = _c11_capture_durable_process_image(
        connection=process_a.connection,
        receipts=process_a.receipts,
        inbox=process_a.inbox,
        scope=process_a.scope,
    )
    process_b = _c11_rebuild_fresh_process(
        image_after_a,
        [terminal],
        apply_max_pages=5,
    )

    result_b = await process_b.service.apply(approved.plan_id)

    assert result_b.status is ColdStartRunStatus.COMPLETED
    assert result_b.pages_committed == result_b.changes_observed == 1
    assert process_b.ordinary.calls == [(8, "Inbox", first.cursor, 100)]
    assert process_b.runner.calls == 1 and len(process_b.runner.sessions) == 1
    second_receipts = _c10_apply_receipt_identities(process_b.receipts)
    assert len(second_receipts) == 2
    assert first_receipts[0] in second_receipts
    new_receipts = set(second_receipts) - set(first_receipts)
    assert len(new_receipts) == 1
    second_receipt = new_receipts.pop()
    second_lookup_key = (
        second_receipt[0],
        second_receipt[1],
        second_receipt[2],
        second_receipt[2],
    )
    assert process_b.receipts.lookup_keys == [second_lookup_key, second_lookup_key]
    assert all(
        lookup[2] != first_receipts[0][2] for lookup in process_b.receipts.lookup_keys
    )
    assert len(process_b.inbox.inserted) == 2
    assert process_b.service is not process_a.service
    assert process_b.connection is not process_a.connection
    assert process_b.runner is not process_a.runner
    assert process_b.ordinary is not process_a.ordinary
    assert process_b.snapshot_provider is not process_a.snapshot_provider
    assert process_b.policy_resolver is not process_a.policy_resolver
    assert process_b.receipts is not process_a.receipts
    assert process_b.inbox is not process_a.inbox
    assert process_b.locator is not process_a.locator

    image_after_b = _c11_capture_durable_process_image(
        connection=process_b.connection,
        receipts=process_b.receipts,
        inbox=process_b.inbox,
        scope=process_b.scope,
    )
    changed_scope = FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=("changed-c11-inbox-id",),
        sync_folder="Inbox",
        event_policy_matrix=_c1_policy_matrix(),
    )
    completed_row = image_after_b.plans[approved.plan_id]
    process_c = _c11_rebuild_fresh_process(
        image_after_b,
        [],
        apply_max_pages=5,
        scope=changed_scope,
        database_now=completed_row["expires_at"],  # type: ignore[arg-type]
        ownership_fencing_token=image_after_b.ownership_fencing_token + 1,
    )
    plans_before_c = deepcopy(process_c.connection.plans)
    cursor_before_c = _c8_full_cursor_snapshot(process_c.connection)
    receipts_before_c = deepcopy(process_c.receipts.receipts)
    inbox_before_c = list(process_c.inbox.inserted)
    audits_before_c = deepcopy(process_c.connection.audits)

    result_c = await process_c.service.apply(approved.plan_id)

    assert result_c.status is ColdStartRunStatus.COMPLETED
    assert result_c.pages_committed == result_c.changes_observed == 0
    assert process_c.ordinary.calls == []
    assert process_c.receipts.lookup_keys == []
    assert process_c.runner.calls == 1 and len(process_c.runner.sessions) == 1
    assert process_c.connection.plans == plans_before_c
    assert _c8_full_cursor_snapshot(process_c.connection) == cursor_before_c
    assert process_c.receipts.receipts == receipts_before_c
    assert process_c.inbox.inserted == inbox_before_c
    assert process_c.connection.audits == audits_before_c
    assert not any(
        event
        in {
            "cursor.apply_page",
            "plan.apply_page",
            "inbox.insert",
            "receipt.insert",
            "plan.block",
            "cursor.block",
            "audit.block",
        }
        for event in process_c.connection.events
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor_status", "cursor_value", "cursor_version"),
    [
        ("cold_start_pending", None, 2),
        ("reset_required", "opaque+C11OldReset/%3D", 7),
    ],
    ids=["pending", "reset"],
)
async def test_apply_fresh_prebinding_process_uses_sealed_boundary(
    cursor_status: str,
    cursor_value: str | None,
    cursor_version: int,
) -> None:
    image, approved = await _c11_approved_process_image(
        cursor_status=cursor_status,
        cursor_value=cursor_value,
        cursor_version=cursor_version,
    )
    terminal = _vector_batch(
        cursor="opaque+C11PrebindingTerminal/%3D",
        changes=(),
        includes_last=True,
    )
    process = _c11_rebuild_fresh_process(image, [terminal], apply_max_pages=5)

    result = await process.service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.COMPLETED
    assert result.pages_committed == 1
    assert result.changes_observed == 0
    assert process.ordinary.calls == [(8, "Inbox", "opaque+Boundary/%3D", 100)]
    assert all(call[2] is not None for call in process.ordinary.calls)
    if cursor_value is not None:
        assert all(call[2] != cursor_value for call in process.ordinary.calls)
    assert process.runner.calls == 1
    assert len(process.runner.sessions) == 1
    assert process.runner.sessions[0].tainted is False
    apply_receipts = _c10_apply_receipt_identities(process.receipts)
    assert len(apply_receipts) == 1
    receipt = apply_receipts[0]
    lookup_key = (receipt[0], receipt[1], receipt[2], receipt[2])
    assert process.receipts.lookup_keys == [lookup_key, lookup_key]
    assert not any(
        event.startswith("xid.commit_unknown") for event in process.connection.events
    )


async def _c11_applying_process_image() -> tuple[
    _C11DurableProcessImage,
    ColdStartPlanView,
    SyncBatch,
]:
    approved_image, approved = await _c11_approved_process_image()
    first = _vector_batch(
        cursor="opaque+C11DurableProgress/%3D",
        changes=(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id="c11-durable-progress",
                source_version="v1",
                item={"subject": "durable before fresh-process drift"},
            ),
        ),
        includes_last=False,
    )
    process = _c11_rebuild_fresh_process(
        approved_image,
        [first],
        apply_max_pages=1,
    )
    result = await process.service.apply(approved.plan_id)
    assert result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert result.pages_committed == result.changes_observed == 1
    assert process.runner.calls == 1
    return (
        _c11_capture_durable_process_image(
            connection=process.connection,
            receipts=process.receipts,
            inbox=process.inbox,
            scope=process.scope,
        ),
        approved,
        first,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drift", "safe_code"),
    [
        ("expiry", "cold_start.expired"),
        ("config", "cold_start.config_drift"),
        ("fence", "cold_start.fence_drift"),
    ],
)
async def test_apply_fresh_applying_process_blocks_drift_before_http(
    drift: str,
    safe_code: str,
) -> None:
    image, approved, first = await _c11_applying_process_image()
    changed_scope: FolderScope | None = None
    database_now: datetime | None = None
    fencing_token: int | None = None
    if drift == "expiry":
        database_now = image.plans[approved.plan_id]["expires_at"]  # type: ignore[assignment]
    elif drift == "config":
        changed_scope = FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("changed-c11-drift-id",),
            sync_folder="Inbox",
            event_policy_matrix=_c1_policy_matrix(),
        )
    else:
        assert drift == "fence"
        fencing_token = image.ownership_fencing_token + 1
    process = _c11_rebuild_fresh_process(
        image,
        [],
        apply_max_pages=5,
        scope=changed_scope,
        database_now=database_now,
        ownership_fencing_token=fencing_token,
    )
    plan_before = deepcopy(process.connection.plans[approved.plan_id])
    cursor_before = _c8_full_cursor_snapshot(process.connection)
    receipts_before = deepcopy(process.receipts.receipts)
    inbox_before = list(process.inbox.inserted)
    audits_before = deepcopy(process.connection.audits)
    assert plan_before["apply_cursor"] == first.cursor
    assert len(_c10_apply_receipt_identities(process.receipts)) == 1
    assert len(inbox_before) == 1

    result = await process.service.apply(approved.plan_id)

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == safe_code
    assert result.pages_committed == result.changes_observed == 0
    assert process.ordinary.calls == []
    assert process.receipts.lookup_keys == []
    assert process.runner.calls == 1 and len(process.runner.sessions) == 1
    assert process.runner.sessions[0].tainted is False
    plan_after = process.connection.plans[approved.plan_id]
    assert plan_after["state"] == "blocked"
    assert plan_after["version"] == plan_before["version"] + 1  # type: ignore[operator]
    assert plan_after["apply_cursor"] == plan_before["apply_cursor"]
    assert plan_after["apply_cursor_version"] == plan_before["apply_cursor_version"]
    assert process.connection.cursor_status == "blocked_contract"
    assert process.connection.cursor_value == cursor_before[1] == first.cursor
    assert process.connection.cursor_version == cursor_before[2] + 1  # type: ignore[operator]
    assert process.receipts.receipts == receipts_before
    assert process.inbox.inserted == inbox_before
    assert process.connection.audits[:-1] == audits_before
    assert len(process.connection.audits) == len(audits_before) + 1
    assert process.connection.audits[-1]["reason"] == safe_code
    mutation_events = [
        event
        for event in process.connection.events
        if event
        in {
            "cursor.apply_page",
            "plan.apply_page",
            "inbox.insert",
            "receipt.insert",
            "plan.block",
            "cursor.block",
            "audit.block",
        }
    ]
    assert mutation_events == ["plan.block", "cursor.block", "audit.block"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "illegal_pair",
    ["plan-apply-cursor", "cursor-plan-binding"],
)
async def test_apply_fresh_process_rejects_illegal_persisted_pair_without_mutation(
    illegal_pair: str,
) -> None:
    image, approved, _first = await _c11_applying_process_image()
    if illegal_pair == "plan-apply-cursor":
        malformed_plans = deepcopy(image.plans)
        assert malformed_plans[approved.plan_id]["apply_cursor"] is not None
        malformed_plans[approved.plan_id]["apply_cursor_version"] = None
        image = replace(image, plans=malformed_plans)
    else:
        assert illegal_pair == "cursor-plan-binding"
        assert image.cursor.cold_start_plan_id == approved.plan_id
        image = replace(
            image,
            cursor=replace(image.cursor, cold_start_plan_state=None),
        )
    process = _c11_rebuild_fresh_process(image, [], apply_max_pages=5)
    plans_before = deepcopy(process.connection.plans)
    cursor_before = _c8_full_cursor_snapshot(process.connection)
    receipts_before = deepcopy(process.receipts.receipts)
    inbox_before = list(process.inbox.inserted)
    audits_before = deepcopy(process.connection.audits)

    with pytest.raises(
        ColdStartStateConflictError,
        match="^cold-start plan state conflict$",
    ):
        await process.service.apply(approved.plan_id)

    assert process.ordinary.calls == []
    assert process.receipts.lookup_keys == []
    assert process.runner.calls == 1 and len(process.runner.sessions) == 1
    assert process.runner.sessions[0].tainted is False
    assert process.connection.plans == plans_before
    assert _c8_full_cursor_snapshot(process.connection) == cursor_before
    assert process.receipts.receipts == receipts_before
    assert process.inbox.inserted == inbox_before
    assert process.connection.audits == audits_before
    assert not any(
        event in {"plan.block", "cursor.block", "audit.block"}
        for event in process.connection.events
    )


@pytest.mark.asyncio
async def test_apply_fresh_process_keeps_structural_cursor_row_error_as_invariant() -> (
    None
):
    image, approved, _first = await _c11_applying_process_image()
    process = _c11_rebuild_fresh_process(image, [], apply_max_pages=5)
    original_execute = process.connection.execute

    async def execute_with_missing_key(
        statement: str,
        params: object = None,
    ) -> _C1AcceptanceCursor:
        cursor = await original_execute(statement, params)
        if statement.startswith(
            "SELECT cursor, status, version, blocked_reason_code, "
        ):
            row = await cursor.fetchone()
            assert type(row) is dict
            malformed = dict(row)
            del malformed["updated_at"]
            return _C1AcceptanceCursor(malformed)
        return cursor

    process.connection.execute = execute_with_missing_key  # type: ignore[method-assign]

    with pytest.raises(DatabaseOperationError) as caught:
        await process.service.apply(approved.plan_id)

    assert caught.value.operation == "cold_start_apply_cursor_row"
    assert caught.value.retryable is False
    assert str(caught.value) == "cold-start apply state is invalid"
    assert process.ordinary.calls == []
    assert process.runner.calls == 1


# The cases below exercise cold-start's defensive seams directly.  The ordinary
# workflow tests above prove the happy paths; these tests prove that hostile
# database projections and impossible recovery/result shapes fail closed without
# relying on a fake to throw at an unrelated, earlier boundary.


def _coverage_internal_plan_row(**overrides: object) -> dict[str, object]:
    values = _plan_row()
    values.update(
        {
            "expected_cursor_status": "cold_start_pending",
            "expected_cursor": None,
            "expected_cursor_version": 0,
            "pipeline_name": "pipeline-v2",
            "generation": 3,
            "fencing_token": 9,
            "version": 1,
            "preview_cursor": "opaque+Boundary/%3D",
            "preview_cursor_version": 1,
            "boundary_cursor_version": 1,
            "apply_cursor": None,
            "apply_cursor_version": None,
            "rolling_hash": "e" * 64,
            "actor": "operator",
            "reason": "review history",
        }
    )
    values.update(overrides)
    return values


def _coverage_plan_record(**overrides: object) -> object:
    return cold_start_module._cold_start_plan_from_row(  # type: ignore[attr-defined]
        _coverage_internal_plan_row(**overrides)
    )


def _coverage_sealed_approved_plan(**overrides: object) -> object:
    values = _coverage_internal_plan_row(
        state="approved",
        version=2,
        approved_at=_APPROVED_AT,
        updated_at=_APPROVED_AT,
    )
    values.update(overrides)
    record = cold_start_module._cold_start_plan_from_row(values)  # type: ignore[attr-defined]
    values["plan_hash"] = cold_start_module._sealed_existing_plan_digest(record)  # type: ignore[attr-defined]
    return cold_start_module._cold_start_plan_from_row(values)  # type: ignore[attr-defined]


def _coverage_apply_cursor_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "cursor": "opaque+Cursor/%3D",
        "status": "active",
        "version": 3,
        "blocked_reason_code": None,
        "contract_fingerprint": None,
        "blocked_at": None,
        "transient_failures": 0,
        "retry_after_at": None,
        "cold_start_plan_id": None,
        "cold_start_plan_state": None,
        "last_attempt_at": _CREATED_AT,
        "last_success_at": _CREATED_AT,
        "updated_at": _CREATED_AT,
    }
    values.update(overrides)
    return values


def _coverage_apply_cursor(**overrides: object) -> object:
    return cold_start_module._apply_cursor_record_from_row(
        _coverage_apply_cursor_row(**overrides)
    )


def _coverage_receipt(**overrides: object) -> CommandReceipt:
    values: dict[str, object] = {
        "id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "account_id": 8,
        "command_name": "cold_start.preview",
        "idempotency_key_hash": "a" * 64,
        "canonical_payload_hash": "b" * 64,
        "outcome": "succeeded",
        "result_type": "sync_cold_start_plan",
        "result_id": str(_PLAN_ID),
        "result_hash": "c" * 64,
        "authority_epoch": 9,
        "created_at": _CREATED_AT,
    }
    values.update(overrides)
    return CommandReceipt(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        (
            lambda: _plan_view(
                page_count=1,
                item_count=1,
                redacted_samples=(object(),),
            ),
            "redacted_samples must contain exact ColdStartSample values",
        ),
        (
            lambda: _approved_plan(
                approved_at=_CREATED_AT,
                updated_at=_READY_AT,
            ),
            "approved_at must not precede ready_at",
        ),
        (
            lambda: _completed_plan(
                completed_at=_READY_AT,
                updated_at=_APPROVED_AT,
            ),
            "completed_at must not precede approved_at",
        ),
        (
            lambda: ColdStartRunResult(
                ColdStartRunStatus.READY,
                object(),  # type: ignore[arg-type]
                0,
                0,
                None,
            ),
            "plan must be an exact ColdStartPlanView",
        ),
        (
            lambda: cold_start_module._LocatedPlanIdentity(  # type: ignore[attr-defined]
                _PLAN_ID,
                8,
                type("FolderText", (str,), {})("INBOX"),
            ),
            "canonical_folder must be an exact string",
        ),
        (
            lambda: cold_start_module._require_exact_text(  # type: ignore[attr-defined]
                "actor",
                "\ud800",
                max_length=128,
            ),
            "actor must contain valid Unicode scalar text",
        ),
    ],
)
def test_defensive_dtos_reject_nested_types_and_invalid_chronology(
    operation: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        operation()  # type: ignore[operator]


def test_plain_json_validator_rejects_nonfinite_numbers_and_container_cycles() -> None:
    assert (
        cold_start_module._validate_plain_json(1.25, active_ids=set()) is None  # type: ignore[attr-defined]
    )
    with pytest.raises(ValueError, match="non-finite"):
        cold_start_module._validate_plain_json(float("nan"), active_ids=set())  # type: ignore[attr-defined]

    dictionary_cycle: dict[str, object] = {}
    dictionary_cycle["self"] = dictionary_cycle
    with pytest.raises(ValueError, match="container cycle"):
        cold_start_module._validate_plain_json(  # type: ignore[attr-defined]
            dictionary_cycle,
            active_ids=set(),
        )

    list_cycle: list[object] = []
    list_cycle.append(list_cycle)
    with pytest.raises(ValueError, match="container cycle"):
        cold_start_module._validate_plain_json(list_cycle, active_ids=set())  # type: ignore[attr-defined]


def test_json_string_and_canonical_digest_reject_hostile_outer_shapes() -> None:
    with pytest.raises(ValueError, match="must be an exact string"):
        cold_start_module._require_json_string("value", 1)  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="Unicode scalar text"):
        cold_start_module._require_json_string("value", "\ud800")  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="NUL-free ASCII"):
        cold_start_module._canonical_digest("domain\x00suffix", {"v": 1})  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="exact object"):
        cold_start_module._canonical_digest("domain", [])  # type: ignore[attr-defined]


def test_frozen_json_materializer_rejects_nonfinite_numbers_and_indirect_cycles() -> (
    None
):
    assert (
        cold_start_module._materialize_frozen_json(1.25, active_ids=set())  # type: ignore[attr-defined]
        == 1.25
    )
    with pytest.raises(ValueError, match="non-finite"):
        cold_start_module._materialize_frozen_json(float("inf"), active_ids=set())  # type: ignore[attr-defined]

    mapping_backing: dict[str, object] = {}
    cyclic_mapping = MappingProxyType(mapping_backing)
    mapping_backing["self"] = cyclic_mapping
    with pytest.raises(ValueError, match="container cycle"):
        cold_start_module._materialize_frozen_json(  # type: ignore[attr-defined]
            cyclic_mapping,
            active_ids=set(),
        )

    tuple_backing: dict[str, object] = {}
    tuple_proxy = MappingProxyType(tuple_backing)
    cyclic_tuple: tuple[object, ...] = (tuple_proxy,)
    tuple_backing["self"] = cyclic_tuple
    with pytest.raises(ValueError, match="container cycle"):
        cold_start_module._materialize_frozen_json(  # type: ignore[attr-defined]
            cyclic_tuple,
            active_ids=set(),
        )


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (object(), "unexpected shape"),
        (
            _coverage_apply_cursor_row(status=SyncCursorStatus.ACTIVE),
            "status must be an exact string",
        ),
        (_coverage_apply_cursor_row(status="unknown"), "status is unknown"),
        (
            _coverage_apply_cursor_row(
                cold_start_plan_state=ColdStartPlanState.APPROVED
            ),
            "plan state must be an exact string",
        ),
        (
            _coverage_apply_cursor_row(cold_start_plan_state="unknown"),
            "plan state is unknown",
        ),
    ],
)
def test_apply_cursor_decoder_rejects_hostile_status_and_plan_state_shapes(
    row: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        cold_start_module._apply_cursor_record_from_row(row)


def test_applied_cursor_decoder_maps_shape_and_timestamp_failures_to_invariant() -> (
    None
):
    with pytest.raises(DatabaseOperationError) as shape:
        cold_start_module._applied_cursor_from_row(object())  # type: ignore[attr-defined]
    assert shape.value.operation == "cold_start_apply_cursor_row"
    assert shape.value.retryable is False

    with pytest.raises(DatabaseOperationError) as timestamp:
        cold_start_module._applied_cursor_from_row(  # type: ignore[attr-defined]
            _coverage_apply_cursor_row(last_attempt_at=None)
        )
    assert timestamp.value.operation == "cold_start_apply_cursor_row"
    assert timestamp.value.retryable is False


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda _row: object(), "unexpected shape"),
        (
            lambda row: {
                **row,
                "expected_cursor_status": SyncCursorStatus.RESET_REQUIRED,
            },
            "expected cursor status must be an exact string",
        ),
        (
            lambda row: {**row, "expected_cursor_status": "unknown"},
            "expected cursor status is unknown",
        ),
        (
            lambda row: {**row, "preview_cursor_version": 2},
            "preview cursor version does not match page count",
        ),
        (
            lambda row: {**row, "preview_cursor": None},
            "nonzero preview progress is incomplete",
        ),
        (
            lambda row: {**row, "boundary_cursor_version": 2},
            "boundary cursor does not match preview progress",
        ),
        (
            lambda row: {**row, "apply_cursor": "cursor-without-version"},
            "apply cursor binding is incomplete",
        ),
    ],
)
def test_internal_plan_decoder_rejects_hostile_progress_shapes(
    mutate: object,
    message: str,
) -> None:
    row = mutate(_coverage_internal_plan_row())  # type: ignore[operator]
    with pytest.raises(ValueError, match=message):
        cold_start_module._cold_start_plan_from_row(row)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"preview_cursor": "unexpected", "rolling_hash": "e" * 64},
            "zero-page preview progress is inconsistent",
        ),
        (
            {"boundary_cursor_version": 0},
            "boundary cursor version must be absent",
        ),
    ],
)
def test_internal_zero_page_plan_rejects_hidden_progress(
    overrides: dict[str, object],
    message: str,
) -> None:
    row = _coverage_internal_plan_row(
        **{
            "state": "previewing",
            "version": 0,
            "boundary_cursor": None,
            "page_count": 0,
            "item_count": 0,
            "redacted_samples": [],
            "plan_hash": None,
            "ready_at": None,
            "updated_at": _CREATED_AT,
            "preview_cursor": None,
            "preview_cursor_version": 0,
            "boundary_cursor_version": None,
            "rolling_hash": None,
            **overrides,
        }
    )

    with pytest.raises(ValueError, match=message):
        cold_start_module._cold_start_plan_from_row(row)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_database_row_readers_reject_missing_and_hostile_rows() -> None:
    class Connection:
        def __init__(self, rows: list[object]) -> None:
            self.rows = rows

        async def execute(
            self,
            _statement: str,
            _params: object = None,
        ) -> _C1AcceptanceCursor:
            return _C1AcceptanceCursor(self.rows.pop(0))

    with pytest.raises(ColdStartStateConflictError):
        await cold_start_module._read_cold_start_cursor(Connection([None]), 8, "INBOX")  # type: ignore[attr-defined]

    with pytest.raises(DatabaseOperationError) as malformed:
        await cold_start_module._read_cold_start_cursor(  # type: ignore[attr-defined]
            Connection(
                [
                    {
                        "cursor": None,
                        "status": SyncCursorStatus.RESET_REQUIRED,
                        "version": 0,
                    }
                ]
            ),
            8,
            "INBOX",
        )
    assert malformed.value.operation == "cold_start_cursor_row"
    assert malformed.value.retryable is False

    with pytest.raises(ColdStartStateConflictError):
        await cold_start_module._read_apply_cursor(Connection([None]), 8, "INBOX")  # type: ignore[attr-defined]

    with pytest.raises(DatabaseOperationError) as clock:
        await cold_start_module._read_database_now(
            Connection([{"database_now": _CREATED_AT, "extra": 1}])
        )
    assert clock.value.operation == "cold_start_database_clock"
    assert clock.value.retryable is False


@pytest.mark.parametrize(
    "row",
    [
        object(),
        {"plan_id": _PLAN_ID, "extra": "hidden"},
    ],
)
def test_open_plan_row_rejects_nonexact_projection(row: object) -> None:
    with pytest.raises(DatabaseOperationError) as caught:
        cold_start_module._require_open_plan_row(row)  # type: ignore[attr-defined]
    assert caught.value.operation == "cold_start_plan_row"
    assert caught.value.retryable is False


@pytest.mark.parametrize(
    "receipt",
    [
        object(),
        _coverage_receipt(result_id="not-a-uuid"),
        _coverage_receipt(result_id="AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
    ],
)
def test_receipt_plan_identity_rejects_hostile_and_noncanonical_text(
    receipt: object,
) -> None:
    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._receipt_plan_id(receipt)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "operation",
    [
        lambda: cold_start_module._preview_budget_result(  # type: ignore[attr-defined]
            object(), pages_committed=0, changes_observed=0
        ),
        lambda: cold_start_module._apply_budget_result(  # type: ignore[attr-defined]
            _ready_plan(), pages_committed=0, changes_observed=0
        ),
        lambda: cold_start_module._apply_retry_deferred_result(_ready_plan()),  # type: ignore[attr-defined]
        lambda: cold_start_module._preview_blocked_result(  # type: ignore[attr-defined]
            _ready_plan(), pages_committed=0, changes_observed=0
        ),
    ],
)
def test_result_factories_reject_wrong_plan_type_or_state(operation: object) -> None:
    with pytest.raises(ColdStartStateConflictError):
        operation()  # type: ignore[operator]


def test_preview_sample_accumulator_rejects_hostile_existing_prefixes() -> None:
    with pytest.raises(ValueError, match="exact tuple"):
        cold_start_module._append_preview_samples(  # type: ignore[attr-defined]
            [], account_id=8, batch=_empty_batch()
        )
    with pytest.raises(ValueError, match="existing samples are invalid"):
        cold_start_module._append_preview_samples(  # type: ignore[attr-defined]
            (object(),), account_id=8, batch=_empty_batch()
        )


def test_recovery_shape_guards_fail_closed_before_comparing_fields() -> None:
    expected = _coverage_plan_record()
    scope = _c1_snapshot().scopes[0]

    assert (
        cold_start_module._preview_recovery_environment_matches(  # type: ignore[attr-defined]
            object(),
            expected=expected,
            ownership=expected.ownership,  # type: ignore[attr-defined]
            cursor_binding=(None, SyncCursorStatus.COLD_START_PENDING, 0),
            scope=scope,
            contract_fingerprint="e" * 64,
        )
        is False
    )
    assert (
        cold_start_module._apply_recovery_environment_matches(  # type: ignore[attr-defined]
            object(),
            expected=expected,
            ownership=expected.ownership,  # type: ignore[attr-defined]
            scope=scope,
            contract_fingerprint="e" * 64,
        )
        is False
    )


def test_apply_and_block_validation_helpers_reject_nonexact_projections() -> None:
    plan = _coverage_sealed_approved_plan()

    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._validate_apply_cursor_tuple(  # type: ignore[attr-defined]
            object(), plan_id=_PLAN_ID
        )
    with pytest.raises(DatabaseOperationError) as receipt:
        cold_start_module._validate_apply_receipt_identity(  # type: ignore[attr-defined]
            object(), plan=plan, payload_hash="a" * 64
        )
    assert receipt.value.operation == "cold_start_apply_receipt"

    invalid_validator_calls = (
        lambda: cold_start_module._validate_scheduled_retry_cursor(  # type: ignore[attr-defined]
            object(),
            previous=object(),
            failure_count=1,
            retry_delay_seconds=1,
            database_stamp=_CREATED_AT,
        ),
        lambda: cold_start_module._validate_applied_cursor(  # type: ignore[attr-defined]
            object(),
            previous=object(),
            next_cursor="cursor",
            terminal=True,
            plan_id=_PLAN_ID,
            database_stamp=_CREATED_AT,
        ),
        lambda: cold_start_module._validate_applied_plan(  # type: ignore[attr-defined]
            object(),
            previous=object(),
            applied_cursor=object(),
            batch=_empty_batch(),
            database_stamp=_CREATED_AT,
        ),
    )
    for operation in invalid_validator_calls:
        with pytest.raises(DatabaseOperationError) as caught:
            operation()
        assert caught.value.retryable is False

    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._approval_audit_metadata(object())  # type: ignore[attr-defined]
    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._validate_blocked_preview_plan(  # type: ignore[attr-defined]
            plan,
            previous=plan,
            safe_code="cold_start.cursor_drift",
            blocked_fingerprint="f" * 64,
            blocked_at=_APPROVED_AT,
        )
    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._validate_blocked_apply_cursor(  # type: ignore[attr-defined]
            object(),
            previous=object(),
            safe_code="cold_start.cursor_drift",
            blocked_fingerprint="f" * 64,
            blocked_at=_APPROVED_AT,
        )


def test_sealed_plan_and_expected_cursor_guards_reject_incomplete_bindings() -> None:
    incomplete = replace(_coverage_plan_record(), boundary_cursor_version=None)
    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._sealed_existing_plan_digest(incomplete)  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="not cold-start eligible"):
        cold_start_module._require_expected_cursor_binding(  # type: ignore[attr-defined]
            SyncCursorStatus.ACTIVE, None
        )
    with pytest.raises(ValueError, match="must be absent"):
        cold_start_module._require_expected_cursor_binding(  # type: ignore[attr-defined]
            SyncCursorStatus.COLD_START_PENDING, "a" * 64
        )
    with pytest.raises(ValueError, match="canonical_folder must be an exact string"):
        cold_start_module._require_digest_identity(  # type: ignore[attr-defined]
            8, type("FolderText", (str,), {})("INBOX")
        )


def test_digest_lifecycle_and_plan_projection_guards_reject_inconsistency() -> None:
    preview_kwargs: dict[str, object] = {
        "plan_id": _PLAN_ID,
        "account_id": 8,
        "canonical_folder": "INBOX",
        "expected_cursor_status": SyncCursorStatus.COLD_START_PENDING,
        "expected_cursor_version": 0,
        "expected_cursor_hash": None,
        "pipeline_name": "durable",
        "generation": 3,
        "fencing_token": 9,
        "contract_fingerprint": "a" * 64,
        "folder_scope_config_hash": "b" * 64,
        "created_at": _CREATED_AT,
        "expires_at": _CREATED_AT,
    }
    with pytest.raises(ValueError, match="expires_at must follow created_at"):
        _preview_result_digest(**preview_kwargs)  # type: ignore[arg-type]

    plan_kwargs: dict[str, object] = {
        "plan_id": _PLAN_ID,
        "account_id": 8,
        "canonical_folder": "INBOX",
        "expected_cursor_status": SyncCursorStatus.COLD_START_PENDING,
        "expected_cursor_version": 0,
        "expected_cursor_hash": None,
        "pipeline_name": "durable",
        "generation": 3,
        "fencing_token": 9,
        "boundary_cursor_hash": "a" * 64,
        "boundary_cursor_version": 1,
        "rolling_hash": "b" * 64,
        "page_count": 1,
        "item_count": 1,
        "redacted_samples": (ColdStartSample(ChangeKind.CREATE, "c" * 64),),
        "contract_fingerprint": "d" * 64,
        "folder_scope_config_hash": "e" * 64,
        "actor": "operator",
        "reason": "review history",
        "created_at": _CREATED_AT,
        "expires_at": _EXPIRES_AT,
    }
    with pytest.raises(ValueError, match="must equal page_count"):
        _plan_digest(**{**plan_kwargs, "boundary_cursor_version": 2})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="contain exact samples"):
        _plan_digest(**{**plan_kwargs, "redacted_samples": (object(),)})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="expires_at must follow created_at"):
        _plan_digest(**{**plan_kwargs, "expires_at": _CREATED_AT})  # type: ignore[arg-type]


def test_sync_batch_rebuilder_rejects_non_change_member_exactly() -> None:
    batch = _empty_batch()
    object.__setattr__(batch, "changes", (object(),))

    with pytest.raises(ValueError, match="change shape"):
        _rebuild_sync_batch(batch, 1)


async def _invoke_coverage_public_method(
    service: ColdStartService,
    endpoint: str,
) -> ColdStartRunResult:
    if endpoint == "preview":
        return await service.preview(
            8,
            "INBOX",
            actor="operator",
            reason="review history",
            idempotency_key="preview-key",
        )
    if endpoint == "resume":
        return await service.resume(_PLAN_ID)
    if endpoint == "approve":
        return await service.approve(
            _PLAN_ID,
            actor="operator",
            reason="review history",
            idempotency_key="approve-key",
        )
    assert endpoint == "apply"
    return await service.apply(_PLAN_ID)


class _CoverageRunner:
    def __init__(self, value: object) -> None:
        self.value = value
        self.calls = 0

    async def run(self, *_args: object, **_kwargs: object) -> _C1RunnerOutcome:
        self.calls += 1
        return _C1RunnerOutcome(acquired=True, value=self.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["preview", "resume", "approve", "apply"])
async def test_public_methods_reject_hostile_success_result_projection(
    endpoint: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ColdStartService(**_service_kwargs())  # type: ignore[arg-type]
    runner = _CoverageRunner(object())
    scope = _c1_snapshot().scopes[0]
    identity = cold_start_module._LocatedPlanIdentity(  # type: ignore[attr-defined]
        _PLAN_ID,
        8,
        "INBOX",
    )

    async def locate(_plan_id: UUID) -> object:
        return identity

    async def ready_scope(_account_id: int, _folder: str) -> tuple[object, object]:
        return scope, object()

    monkeypatch.setattr(service, "_locate_plan_identity", locate)
    monkeypatch.setattr(service, "_ready_scope", ready_scope)
    service._session_runner = runner  # type: ignore[assignment]

    with pytest.raises(DatabaseOperationError) as caught:
        await _invoke_coverage_public_method(service, endpoint)

    assert caught.value.operation == "cold_start_result"
    assert caught.value.retryable is False
    assert runner.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["resume", "approve", "apply"])
async def test_plan_id_public_methods_reject_hostile_locator_projection(
    endpoint: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ColdStartService(**_service_kwargs())  # type: ignore[arg-type]
    runner = _CoverageRunner(object())

    async def locate(_plan_id: UUID) -> object:
        return object()

    monkeypatch.setattr(service, "_locate_plan_identity", locate)
    service._session_runner = runner  # type: ignore[assignment]

    with pytest.raises(DatabaseOperationError) as caught:
        await _invoke_coverage_public_method(service, endpoint)

    assert caught.value.operation == "cold_start_locator_row"
    assert caught.value.retryable is False
    assert runner.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["preview", "resume", "approve", "apply"])
async def test_public_methods_reject_hostile_policy_scope_projection(
    endpoint: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ColdStartService(**_service_kwargs())  # type: ignore[arg-type]
    runner = _CoverageRunner(object())
    identity = cold_start_module._LocatedPlanIdentity(  # type: ignore[attr-defined]
        _PLAN_ID,
        8,
        "INBOX",
    )

    async def locate(_plan_id: UUID) -> object:
        return identity

    async def ready_scope(_account_id: int, _folder: str) -> tuple[object, object]:
        return object(), object()

    monkeypatch.setattr(service, "_locate_plan_identity", locate)
    monkeypatch.setattr(service, "_ready_scope", ready_scope)
    service._session_runner = runner  # type: ignore[assignment]

    with pytest.raises(PolicySnapshotUnavailableError):
        await _invoke_coverage_public_method(service, endpoint)

    assert runner.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_type"),
    [
        (RuntimeError("snapshot backend detail"), PolicySnapshotUnavailableError),
        (KeyboardInterrupt("shutdown"), KeyboardInterrupt),
    ],
)
async def test_ready_scope_normalizes_only_ordinary_snapshot_failures(
    failure: BaseException,
    expected_type: type[BaseException],
) -> None:
    class Provider:
        async def get_ready_snapshot(self, _account_id: int) -> object:
            raise failure

    service = ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            snapshot_provider=Provider(),
            policy_resolver=ProcessingPolicyResolver(),
        )
    )

    with pytest.raises(expected_type) as caught:
        await service._ready_scope(8, "INBOX")

    if expected_type is KeyboardInterrupt:
        assert caught.value is failure


@pytest.mark.asyncio
async def test_ready_scope_rejects_missing_configured_folder() -> None:
    class Provider:
        async def get_ready_snapshot(self, _account_id: int) -> PolicySnapshot:
            return _c1_snapshot()

    service = ColdStartService(  # type: ignore[arg-type]
        **_service_kwargs(
            snapshot_provider=Provider(),
            policy_resolver=ProcessingPolicyResolver(),
        )
    )

    with pytest.raises(PolicySnapshotUnavailableError):
        await service._ready_scope(8, "MISSING")


@pytest.mark.asyncio
async def test_cold_start_cursor_reader_rejects_hostile_projection_shape() -> None:
    class Connection:
        async def execute(
            self,
            _statement: str,
            _params: object = None,
        ) -> _C1AcceptanceCursor:
            return _C1AcceptanceCursor(
                {
                    "cursor": None,
                    "status": "cold_start_pending",
                    "version": 0,
                    "extra": 1,
                }
            )

    with pytest.raises(DatabaseOperationError) as caught:
        await cold_start_module._read_cold_start_cursor(  # type: ignore[attr-defined]
            Connection(), 8, "INBOX"
        )

    assert caught.value.operation == "cold_start_cursor_row"
    assert caught.value.retryable is False


def test_apply_plan_and_cursor_guards_cover_pending_and_invalid_terminal_shapes() -> (
    None
):
    approved = _coverage_sealed_approved_plan()
    pending = _coverage_apply_cursor(
        cursor=None,
        status="cold_start_pending",
        blocked_reason_code="cold_start.required",
        last_attempt_at=None,
        last_success_at=None,
    )

    assert (
        cold_start_module._validate_apply_cursor_tuple(  # type: ignore[attr-defined]
            pending, plan_id=_PLAN_ID
        )
        is None
    )
    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._validate_completed_apply_plan(approved)  # type: ignore[attr-defined]


def test_apply_drift_and_prestate_reject_wrong_boundary_types() -> None:
    approved = _coverage_sealed_approved_plan()
    pending = _coverage_apply_cursor(
        cursor=None,
        status="cold_start_pending",
        blocked_reason_code="cold_start.required",
        last_attempt_at=None,
        last_success_at=None,
    )
    scope = _c1_snapshot().scopes[0]

    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._apply_drift_code(  # type: ignore[attr-defined]
            approved,
            identity=object(),
            cursor=pending,
            ownership=approved.ownership,
            scope=scope,
            contract_fingerprint="a" * 64,
            database_now=_APPROVED_AT,
        )
    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._validate_apply_prestate(  # type: ignore[attr-defined]
            approved,
            cursor=pending,
            ownership=approved.ownership,
            scope=object(),
            contract_fingerprint="a" * 64,
            database_now=_APPROVED_AT,
        )


def test_commit_and_approval_projection_validators_reject_nontransitions() -> None:
    ready = _coverage_plan_record()
    approved = _coverage_sealed_approved_plan()

    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._validate_committed_preview_page(  # type: ignore[attr-defined]
            ready,
            previous=ready,
            batch=_empty_batch(),
            rolling_hash="e" * 64,
            samples=ready.view.redacted_samples,
            plan_hash=ready.view.plan_hash,
        )
    with pytest.raises(ColdStartStateConflictError):
        cold_start_module._validate_approved_plan(  # type: ignore[attr-defined]
            approved, previous=approved
        )


def test_plain_json_rejects_non_json_scalar_after_container_checks() -> None:
    with pytest.raises(ValueError, match="exact built-in JSON values"):
        cold_start_module._validate_plain_json(object(), active_ids=set())  # type: ignore[attr-defined]


def test_batch_digest_revalidates_outer_change_and_item_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as scoped:
        hostile_outer = _empty_batch()
        object.__setattr__(hostile_outer, "contract_version", "v1")
        scoped.setattr(
            cold_start_module, "_rebuild_sync_batch", lambda value, _limit: value
        )
        with pytest.raises(ValueError, match="hostile outer shape"):
            _batch_digest(hostile_outer)

    with monkeypatch.context() as scoped:
        hostile_change = _empty_batch()
        object.__setattr__(hostile_change, "changes", (object(),))
        scoped.setattr(
            cold_start_module, "_rebuild_sync_batch", lambda value, _limit: value
        )
        with pytest.raises(ValueError, match="exact SyncChange"):
            _batch_digest(hostile_change)

    with monkeypatch.context() as scoped:
        batch = _batch_with_changes(1)
        scoped.setattr(
            cold_start_module, "_rebuild_sync_batch", lambda value, _limit: value
        )
        scoped.setattr(
            cold_start_module, "_materialize_frozen_json", lambda *_args, **_kwargs: []
        )
        with pytest.raises(ValueError, match="materialize to an exact object"):
            _batch_digest(batch)


def test_strict_batch_rebuilder_rejects_nonobject_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cold_start_module,
        "_materialize_frozen_json",
        lambda *_args, **_kwargs: [],
    )

    with pytest.raises(ValueError, match="materialize to an exact object"):
        _rebuild_sync_batch(_batch_with_changes(1), 1)
