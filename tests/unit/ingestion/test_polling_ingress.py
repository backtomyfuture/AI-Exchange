from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import pytest

from src.domain.errors import SyncContractError, SyncTransientError
from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    ProcessingPolicy,
    SyncBatch,
    SyncChange,
)
from src.ingestion.polling import (
    PollingCursorCheckpoint,
    PollingIngress,
    PollingIngressOutcome,
    PollingPageCommitResult,
    PollingRuntime,
    PostgresPollingCursorStore,
)
from src.ingestion.policy import FolderScope, PolicySnapshot


def _scope() -> FolderScope:
    return FolderScope.configured(
        canonical_key="INBOX",
        sync_folder="INBOX",
        event_policy_matrix={
            (IngressSource.SYNC, "create", ChangeKind.CREATE): ProcessingPolicy.FULL,
            (IngressSource.SYNC, "update", ChangeKind.UPDATE): ProcessingPolicy.METADATA_ONLY,
            (IngressSource.SYNC, "delete", ChangeKind.DELETE): ProcessingPolicy.METADATA_ONLY,
        },
    )


@dataclass
class _PageClient:
    calls: list[tuple[int, str, str | None, int]]

    async def sync_polling(
        self,
        account_id: int,
        folder: str,
        sync_state: str | None,
        limit: int,
        *,
        discard_items: bool = False,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, sync_state, limit))
        return SyncBatch(
            contract_version="exchange_sync_contract_v2",
            cursor="activation-cursor",
            changes=(
                SyncChange(
                    kind=ChangeKind.CREATE,
                    external_email_id="historic-message",
                    item={},
                ),
            ),
            includes_last=True,
        )


class _CursorStore:
    def __init__(self) -> None:
        self.checkpoint = PollingCursorCheckpoint(cursor=None, version=0)
        self.activation_commits: list[tuple[PollingCursorCheckpoint, str]] = []
        self.delta_commits: list[object] = []

    async def load(
        self,
        account_id: int,
        folder: str,
    ) -> PollingCursorCheckpoint:
        assert (account_id, folder) == (8, "INBOX")
        return self.checkpoint

    async def commit_activation_boundary(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
    ) -> None:
        self.activation_commits.append((checkpoint, next_cursor))
        self.checkpoint = PollingCursorCheckpoint(cursor=next_cursor, version=1)

    async def commit_delta(self, *_args: object, **_kwargs: object) -> None:
        self.delta_commits.append((_args, _kwargs))
        checkpoint, next_cursor, _events = _args
        assert checkpoint == self.checkpoint
        assert isinstance(next_cursor, str)
        self.checkpoint = PollingCursorCheckpoint(
            cursor=next_cursor,
            version=checkpoint.version + 1,
        )


class _Result:
    def __init__(self, row: object | None = None) -> None:
        self._row = row

    async def fetchone(self) -> object | None:
        return self._row


class _PollingConnection:
    def __init__(self) -> None:
        self.row: dict[str, object] | None = None

    async def execute(self, statement: str, params: object = None) -> _Result:
        if "FROM public.sync_cursors" in statement:
            return _Result(self.row)
        raise AssertionError(f"unexpected cursor-store SQL: {statement}")


class _ConnectionContext:
    def __init__(self, connection: _PollingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _PollingConnection:
        return self._connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _PollingPool:
    def __init__(self) -> None:
        self.connection_value = _PollingConnection()

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self.connection_value)


class _PageCommitter:
    def __init__(self) -> None:
        self.commits: list[tuple[object, str, tuple[object, ...], bool]] = []
        self.connection: _PollingConnection | None = None

    async def commit_page(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
        events: object,
        *,
        activation: bool,
    ) -> PollingPageCommitResult:
        exact_events = tuple(events)
        self.commits.append((checkpoint, next_cursor, exact_events, activation))
        if self.connection is not None:
            self.connection.row = {
                "cursor": next_cursor,
                "status": "active",
                "version": checkpoint.version + 1,
            }
        return PollingPageCommitResult(
            cursor=next_cursor,
            version=checkpoint.version + 1,
            inserted_count=0 if activation else len(exact_events),
            duplicate_count=0,
        )


class _Ingress:
    def __init__(self) -> None:
        self.calls = 0
        self.completed = asyncio.Event()

    async def sync_once(self) -> PollingIngressOutcome:
        self.calls += 1
        self.completed.set()
        return PollingIngressOutcome.BASELINED


class _BlockingIngress:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.completed = asyncio.Event()

    async def sync_once(self) -> PollingIngressOutcome:
        self.started.set()
        await self.release.wait()
        self.completed.set()
        return PollingIngressOutcome.BASELINED


class _TransientThenCommitIngress:
    def __init__(self) -> None:
        self.calls = 0
        self.recovered = asyncio.Event()

    async def sync_once(self) -> PollingIngressOutcome:
        self.calls += 1
        if self.calls == 1:
            raise SyncTransientError(retry_after_seconds=0)
        self.recovered.set()
        return PollingIngressOutcome.COMMITTED


class _ContractThenCommitIngress:
    """One non-retryable gateway result followed by a healthy delta."""

    def __init__(self) -> None:
        self.calls = 0
        self.recovered = asyncio.Event()

    async def sync_once(self) -> PollingIngressOutcome:
        self.calls += 1
        if self.calls == 1:
            raise SyncContractError()
        self.recovered.set()
        return PollingIngressOutcome.COMMITTED


class _ReadyThenUnexpectedFailureIngress:
    """Reach ready, fail once with private detail, then wait for a retry."""

    def __init__(self) -> None:
        self.calls = 0
        self.failure_started = asyncio.Event()
        self.release_failure = asyncio.Event()
        self.retry_started = asyncio.Event()
        self.release_retry = asyncio.Event()

    async def sync_once(self) -> PollingIngressOutcome:
        self.calls += 1
        if self.calls == 1:
            return PollingIngressOutcome.COMMITTED
        if self.calls == 2:
            self.failure_started.set()
            await self.release_failure.wait()
            raise RuntimeError("PRIVATE-POLLING-DETAIL")
        self.retry_started.set()
        await self.release_retry.wait()
        return PollingIngressOutcome.COMMITTED


@dataclass
class _PagedClient:
    pages: list[SyncBatch]
    calls: list[tuple[int, str, str | None, int, bool]]

    async def sync_polling(
        self,
        account_id: int,
        folder: str,
        sync_state: str | None,
        limit: int,
        *,
        discard_items: bool = False,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, sync_state, limit, discard_items))
        return self.pages.pop(0)


def _complete_delta(
    cursor: str,
    change_count: int,
) -> SyncBatch:
    return SyncBatch(
        contract_version="exchange_sync_contract_v2",
        cursor=cursor,
        changes=tuple(
            SyncChange(
                kind=ChangeKind.CREATE,
                external_email_id=f"message-{index}",
                item={},
            )
            for index in range(change_count)
        ),
        includes_last=True,
    )


@pytest.mark.asyncio
async def test_first_poll_establishes_cursor_and_discards_historical_items() -> None:
    """Fresh data must not enqueue mail that predates polling activation."""

    scope = _scope()
    pages = _PageClient(calls=[])
    store = _CursorStore()
    ingress = PollingIngress(
        account_id=8,
        scope=scope,
        snapshot=PolicySnapshot(scopes=(scope,)),
        page_client=pages,
        cursor_store=store,
    )

    outcome = await ingress.sync_once()

    assert outcome is PollingIngressOutcome.BASELINED
    assert pages.calls == [(8, "INBOX", None, 500)]
    assert store.activation_commits == [
        (PollingCursorCheckpoint(cursor=None, version=0), "activation-cursor")
    ]
    assert store.delta_commits == []


@pytest.mark.asyncio
async def test_activation_treats_a_limit_sized_gateway_delta_as_complete() -> None:
    """The Gateway exhausts EWS paging before returning its final cursor."""

    scope = _scope()
    pages = _PagedClient(
        pages=[
            _complete_delta("activation-cursor", 500),
        ],
        calls=[],
    )
    store = _CursorStore()
    ingress = PollingIngress(
        account_id=8,
        scope=scope,
        snapshot=PolicySnapshot(scopes=(scope,)),
        page_client=pages,
        cursor_store=store,
    )

    outcome = await ingress.sync_once()

    assert outcome is PollingIngressOutcome.BASELINED
    assert pages.calls == [
        (8, "INBOX", None, 500, True),
    ]
    assert store.activation_commits == [
        (PollingCursorCheckpoint(cursor=None, version=0), "activation-cursor")
    ]
    assert store.delta_commits == []


def test_polling_ingress_rejects_non_gateway_inbox_aliases() -> None:
    """Gateway polling must use its documented uppercase INBOX identity."""

    scope = FolderScope.configured(
        canonical_key="INBOX",
        sync_folder="Inbox",
        event_policy_matrix=_scope().event_policy_matrix,
    )

    with pytest.raises(ValueError, match="Gateway INBOX"):
        PollingIngress(
            account_id=8,
            scope=scope,
            snapshot=PolicySnapshot(scopes=(scope,)),
            page_client=_PageClient(calls=[]),
            cursor_store=_CursorStore(),
        )


@pytest.mark.asyncio
async def test_subsequent_poll_normalizes_new_mail_into_the_existing_inbox_commit() -> None:
    """Only post-activation changes are supplied to the Durable Inbox store."""

    scope = _scope()
    pages = _PageClient(calls=[])
    store = _CursorStore()
    store.checkpoint = PollingCursorCheckpoint(cursor="previous-cursor", version=4)
    ingress = PollingIngress(
        account_id=8,
        scope=scope,
        snapshot=PolicySnapshot(scopes=(scope,)),
        page_client=pages,
        cursor_store=store,
    )

    outcome = await ingress.sync_once()

    assert outcome is PollingIngressOutcome.COMMITTED
    assert pages.calls == [(8, "INBOX", "previous-cursor", 500)]
    assert store.activation_commits == []
    assert len(store.delta_commits) == 1
    checkpoint, cursor, events = store.delta_commits[0][0]
    assert checkpoint == PollingCursorCheckpoint(cursor="previous-cursor", version=4)
    assert cursor == "activation-cursor"
    assert len(events) == 1
    assert events[0].source is IngressSource.SYNC
    assert events[0].external_email_id == "historic-message"
    assert events[0].folder == "INBOX"


@pytest.mark.asyncio
async def test_active_polling_commits_a_complete_gateway_delta_once() -> None:
    """A limit-sized complete delta does not cause a speculative second fetch."""

    scope = _scope()
    pages = _PagedClient(
        pages=[
            _complete_delta("cursor-1", 500),
        ],
        calls=[],
    )
    store = _CursorStore()
    store.checkpoint = PollingCursorCheckpoint(cursor="cursor-0", version=4)
    ingress = PollingIngress(
        account_id=8,
        scope=scope,
        snapshot=PolicySnapshot(scopes=(scope,)),
        page_client=pages,
        cursor_store=store,
    )

    outcome = await ingress.sync_once()

    assert outcome is PollingIngressOutcome.COMMITTED
    assert pages.calls == [
        (8, "INBOX", "cursor-0", 500, False),
    ]
    assert [commit[0][1] for commit in store.delta_commits] == ["cursor-1"]
    assert len(store.delta_commits[0][0][2]) == 500


@pytest.mark.asyncio
async def test_polling_ingress_rejects_an_incomplete_gateway_delta() -> None:
    """The adapter must not infer pagination from a response item count."""

    scope = _scope()
    pages = _PagedClient(
        pages=[
            SyncBatch(
                contract_version="exchange_sync_contract_v2",
                cursor="untrusted-partial-state",
                changes=(),
                includes_last=False,
            )
        ],
        calls=[],
    )
    store = _CursorStore()
    ingress = PollingIngress(
        account_id=8,
        scope=scope,
        snapshot=PolicySnapshot(scopes=(scope,)),
        page_client=pages,
        cursor_store=store,
    )

    with pytest.raises(RuntimeError, match="polling_sync_response_incomplete"):
        await ingress.sync_once()

    assert store.activation_commits == []


@pytest.mark.asyncio
async def test_postgres_store_commits_a_fresh_activation_boundary_without_inbox_work() -> None:
    """The cursor becomes active atomically, while historical mail remains absent."""

    pool = _PollingPool()
    committer = _PageCommitter()
    committer.connection = pool.connection_value
    store = PostgresPollingCursorStore(
        pool,
        committer,
        account_id=8,
        folder="INBOX",
    )

    checkpoint = await store.load(8, "INBOX")
    await store.commit_activation_boundary(checkpoint, "opaque-activation-cursor")

    assert checkpoint == PollingCursorCheckpoint(cursor=None, version=0)
    assert pool.connection_value.row == {
        "cursor": "opaque-activation-cursor",
        "status": "active",
        "version": 1,
    }
    assert await store.load(8, "INBOX") == PollingCursorCheckpoint(
        cursor="opaque-activation-cursor",
        version=1,
    )
    assert committer.commits == [
        (
            PollingCursorCheckpoint(cursor=None, version=0),
            "opaque-activation-cursor",
            (),
            True,
        )
    ]


@pytest.mark.asyncio
async def test_postgres_store_writes_existing_inbox_before_advancing_active_cursor() -> None:
    """A successful delta binds its Inbox event and next cursor together."""

    pool = _PollingPool()
    pool.connection_value.row = {
        "cursor": "previous-cursor",
        "status": "active",
        "version": 7,
    }
    committer = _PageCommitter()
    committer.connection = pool.connection_value
    store = PostgresPollingCursorStore(
        pool,
        committer,
        account_id=8,
        folder="INBOX",
    )
    # Build the event through the same public polling seam so its source and
    # folder identity are the ones the repository must accept.
    scope = _scope()
    pages = _PageClient(calls=[])
    capture = _CursorStore()
    capture.checkpoint = PollingCursorCheckpoint(cursor="previous-cursor", version=7)
    ingress = PollingIngress(
        account_id=8,
        scope=scope,
        snapshot=PolicySnapshot(scopes=(scope,)),
        page_client=pages,
        cursor_store=capture,
    )
    await ingress.sync_once()
    checkpoint, next_cursor, events = capture.delta_commits[0][0]

    await store.commit_delta(checkpoint, next_cursor, events)

    assert committer.commits == [(checkpoint, next_cursor, tuple(events), False)]
    assert pool.connection_value.row == {
        "cursor": "activation-cursor",
        "status": "active",
        "version": 8,
    }


@pytest.mark.asyncio
async def test_polling_runtime_establishes_the_activation_boundary_before_ready() -> None:
    """Application readiness is published only after the first polling cycle."""

    ingress = _Ingress()
    runtime = PollingRuntime((ingress,), interval_seconds=60)

    await runtime.start()

    assert runtime.live is True
    assert runtime.ready is False
    await asyncio.wait_for(ingress.completed.wait(), timeout=1)
    assert ingress.calls == 1
    assert runtime.ready is True

    await runtime.stop()

    assert runtime.ready is False


@pytest.mark.asyncio
async def test_polling_runtime_does_not_block_service_start_on_historical_baseline() -> None:
    """A long initial history scan keeps readiness false without blocking startup."""

    ingress = _BlockingIngress()
    runtime = PollingRuntime((ingress,), interval_seconds=60)

    await asyncio.wait_for(runtime.start(), timeout=0.1)

    assert runtime.live is True
    await asyncio.wait_for(ingress.started.wait(), timeout=1)
    assert runtime.ready is False

    ingress.release.set()
    await asyncio.wait_for(ingress.completed.wait(), timeout=1)
    assert runtime.ready is True

    await runtime.stop()


@pytest.mark.asyncio
async def test_polling_runtime_retries_a_transient_activation_failure_without_dying() -> None:
    """A short Gateway outage leaves readiness false, but preserves liveness."""

    ingress = _TransientThenCommitIngress()
    runtime = PollingRuntime((ingress,), interval_seconds=0.01)

    await runtime.start()

    assert runtime.live is True
    assert runtime.ready is False
    await asyncio.wait_for(ingress.recovered.wait(), timeout=1)
    assert ingress.calls == 2
    assert runtime.live is True
    assert runtime.ready is True

    await runtime.stop()


@pytest.mark.asyncio
async def test_polling_runtime_records_and_recovers_from_a_nontransient_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A rejected Gateway response must not silently kill the polling scheduler."""

    ingress = _ContractThenCommitIngress()
    runtime = PollingRuntime((ingress,), interval_seconds=0.01)
    caplog.set_level(logging.ERROR, logger="src.ingestion.polling")

    await runtime.start()

    await asyncio.wait_for(ingress.recovered.wait(), timeout=1)
    assert ingress.calls == 2
    assert runtime.live is True
    assert runtime.ready is True
    assert "error_type=SyncContractError" in caplog.text
    assert "safe_code=exchange.sync.contract_invalid" in caplog.text

    await runtime.stop()


@pytest.mark.asyncio
async def test_polling_runtime_degrades_readiness_without_leaking_unknown_failure_detail(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unexpected poll failure is retried safely without a restart signal."""

    ingress = _ReadyThenUnexpectedFailureIngress()
    runtime = PollingRuntime((ingress,), interval_seconds=0.01)
    caplog.set_level(logging.ERROR, logger="src.ingestion.polling")

    await runtime.start()
    try:
        await asyncio.wait_for(ingress.failure_started.wait(), timeout=1)
        assert runtime.live is True
        assert runtime.ready is True

        ingress.release_failure.set()
        await asyncio.wait_for(ingress.retry_started.wait(), timeout=1)

        assert ingress.calls == 3
        assert runtime.live is True
        assert runtime.ready is False
        assert "error_type=RuntimeError" in caplog.text
        assert "safe_code=polling.unexpected_failure" in caplog.text
        assert "PRIVATE-POLLING-DETAIL" not in caplog.text
    finally:
        ingress.release_retry.set()
        await runtime.stop()
