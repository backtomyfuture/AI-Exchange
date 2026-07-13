from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.domain.email_state import PipelineGenerationState
from src.domain.errors import (
    DatabaseOperationError,
    ErrorKind,
    ManualReviewRequired,
    StaleFence,
)
from src.ingestion.models import (
    ChangeKind,
    InboxDispositionStatus,
    InboxStatus,
    IngressReceipt,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.ownership import (
    PipelineOwnershipRepository,
    PipelineRetirementBlocked,
    ownership_advisory_lock_key,
)
from src.ingestion.repository import InboxRepository


@dataclass(slots=True)
class _InboxRuntime:
    schema: Any
    pool: AsyncConnectionPool
    ownership: PipelineOwnershipRepository
    repository: InboxRepository


class _TypedFailure(RuntimeError):
    def __init__(self, kind: ErrorKind) -> None:
        self.kind = kind
        super().__init__("private exception content must never be persisted")


class _ExplodingKindFailure(RuntimeError):
    @property
    def kind(self):
        raise RuntimeError("private kind getter content")


class _AllowRetirement:
    async def assert_ready(self, _connection, _generation) -> None:
        return None


async def _configure_repeatable_read(connection) -> None:
    await connection.execute(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    )


class _ProbedConnection:
    def __init__(
        self,
        connection,
        *,
        match,
        before: asyncio.Event | None = None,
        after: asyncio.Event | None = None,
        release: asyncio.Event | None = None,
    ) -> None:
        self._connection = connection
        self._match = match
        self._before = before
        self._after = after
        self._release = release
        self._matched = False

    def __getattr__(self, name: str):
        return getattr(self._connection, name)

    def transaction(self, *args, **kwargs):
        return self._connection.transaction(*args, **kwargs)

    async def execute(self, statement, params=None):
        try:
            rendered = statement.as_string(self._connection)
        except AttributeError:
            rendered = str(statement)
        matched = not self._matched and self._match(rendered)
        if matched:
            self._matched = True
            if self._before is not None:
                self._before.set()
        cursor = await self._connection.execute(statement, params)
        if matched:
            if self._after is not None:
                self._after.set()
            if self._release is not None:
                await self._release.wait()
        return cursor


class _ProbedConnectionContext:
    def __init__(self, context, **probe) -> None:
        self._context = context
        self._probe = probe

    async def __aenter__(self):
        connection = await self._context.__aenter__()
        return _ProbedConnection(connection, **self._probe)

    async def __aexit__(self, *args):
        return await self._context.__aexit__(*args)


class _ProbedPool:
    def __init__(self, pool, **probe) -> None:
        self._pool = pool
        self._probe = probe

    def connection(self):
        return _ProbedConnectionContext(
            self._pool.connection(),
            **self._probe,
        )


async def _mark_generation_draining(runtime: _InboxRuntime) -> None:
    quiesced = await runtime.ownership.quiesce(
        8,
        1,
        1,
        "test",
        "prepare deterministic retirement race",
    )
    assert quiesced.state is PipelineGenerationState.QUIESCING
    async with runtime.pool.connection() as connection:
        async with connection.transaction():
            transaction = runtime.ownership.transaction(connection)
            locked = await transaction._lock_quiesced(8, 1, 1)
            draining = await transaction._mark_draining(
                locked,
                actor="test",
                reason="run deterministic retirement race",
            )
    assert draining.state is PipelineGenerationState.DRAINING


@pytest_asyncio.fixture
async def inbox_runtime(postgres_database_factory) -> _InboxRuntime:
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    pool = AsyncConnectionPool(
        conninfo=schema.runtime_dsn,
        min_size=1,
        max_size=20,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    ownership = PipelineOwnershipRepository(pool)
    await ownership.bootstrap(8, "durable_v1")
    try:
        yield _InboxRuntime(
            schema=schema,
            pool=pool,
            ownership=ownership,
            repository=InboxRepository(pool),
        )
    finally:
        await pool.close()


def _event(
    ordinal: object,
    *,
    account_id: int = 8,
    policy: ProcessingPolicy = ProcessingPolicy.FULL,
    dedupe_key: str | None = None,
    payload: dict[str, object] | None = None,
) -> NormalizedIngressEvent:
    token = str(ordinal)
    return NormalizedIngressEvent(
        account_id=account_id,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id=f"exchange-message-{token}",
        folder="INBOX",
        source_version=f"version-{token}",
        dedupe_key=dedupe_key
        or hashlib.sha256(f"{account_id}:{token}".encode()).hexdigest(),
        payload=payload or {"routing": {"ordinal": token}},
        source_event_at=datetime.now(UTC),
        processing_policy=policy,
    )


async def _fetchone(
    pool: AsyncConnectionPool,
    statement: str,
    params: tuple[object, ...] = (),
) -> dict[str, Any] | None:
    async with pool.connection() as connection:
        cursor = await connection.execute(statement, params)
        return await cursor.fetchone()


async def _scalar(
    pool: AsyncConnectionPool,
    statement: str,
    params: tuple[object, ...] = (),
) -> object:
    row = await _fetchone(pool, statement, params)
    if row is None:
        return None
    return next(iter(row.values()))


async def _execute(
    pool: AsyncConnectionPool,
    statement: str,
    params: tuple[object, ...] = (),
) -> None:
    async with pool.connection() as connection:
        await connection.execute(statement, params)


def _audit_event_key(inbox_id: str, action: str, attempts: int) -> str:
    raw = f"event_inbox\x00{inbox_id}\x00{action}\x00{attempts}".encode()
    return hashlib.sha256(raw).hexdigest()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_production_dict_row_pool_supports_the_full_lease_lifecycle(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    event = _event(
        "nested",
        payload={
            "routing": {
                "aliases": ["INBOX", {"kind": "primary"}],
                "enabled": True,
            }
        },
    )

    first = await repo.insert(event, generation=1, fencing_token=1)
    duplicate = await repo.insert(event, generation=1, fencing_token=1)

    assert duplicate == IngressReceipt(inbox_id=first.inbox_id, duplicate=True)
    stored = await _fetchone(
        inbox_runtime.pool,
        "SELECT payload, status, processing_started_at FROM event_inbox WHERE id = %s",
        (first.inbox_id,),
    )
    assert stored == {
        "payload": event.payload_for_storage(),
        "status": InboxStatus.PENDING.value,
        "processing_started_at": None,
    }

    lease = (
        await repo.claim_batch("worker-a", {"durable_v1"}, limit=10, lease_seconds=60)
    )[0]
    assert lease.pipeline_name == "durable_v1"
    assert lease.event.payload_for_storage() == event.payload_for_storage()
    assert lease.attempts == 0

    renewed = await repo.renew(lease, lease_seconds=60)
    assert renewed is not None
    assert renewed.lease_until > lease.lease_until
    assert await repo.begin_effect(lease) is False
    assert await repo.begin_effect(renewed) is True
    first_marker = await _scalar(
        inbox_runtime.pool,
        "SELECT effect_started_at FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert await repo.begin_effect(renewed) is True
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT effect_started_at FROM event_inbox WHERE id = %s",
            (lease.id,),
        )
        == first_marker
    )
    assert await repo.complete(renewed) is True
    assert await repo.complete(renewed) is False
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT status FROM event_inbox WHERE id = %s",
            (lease.id,),
        )
        == InboxStatus.COMPLETED.value
    )
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'ingress.completed' AND object_fingerprint = %s",
            (hashlib.sha256(lease.id.encode()).hexdigest(),),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy",
    [ProcessingPolicy.IGNORED, ProcessingPolicy.HISTORICAL_SUPPRESSED],
)
async def test_suppressed_insert_is_terminal_and_audited_exactly_once(
    inbox_runtime: _InboxRuntime,
    policy: ProcessingPolicy,
) -> None:
    repo = inbox_runtime.repository
    event = _event(f"suppressed-{policy.value}", policy=policy)

    receipts = await asyncio.gather(*(repo.insert(event, 1, 1) for _ in range(16)))
    first = next(receipt for receipt in receipts if not receipt.duplicate)

    assert sum(not receipt.duplicate for receipt in receipts) == 1
    assert {receipt.inbox_id for receipt in receipts} == {first.inbox_id}
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT status FROM event_inbox WHERE id = %s",
            (first.inbox_id,),
        )
        == InboxStatus.COMPLETED.value
    )
    assert await repo.claim_batch("worker", {"durable_v1"}, 10, 60) == []
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'ingress.policy_suppressed'",
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_payload_and_policy_drift_is_first_write_wins(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    full = _event("full-first", payload={"first": True})
    ignored_duplicate = replace(
        full,
        payload={"second": True},
        processing_policy=ProcessingPolicy.IGNORED,
    )
    receipt = await repo.insert(full, 1, 1)

    duplicate = await repo.insert(ignored_duplicate, 1, 1)

    assert duplicate == IngressReceipt(receipt.inbox_id, True)
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT payload, processing_policy, status FROM event_inbox WHERE id = %s",
        (receipt.inbox_id,),
    ) == {
        "payload": {"first": True},
        "processing_policy": ProcessingPolicy.FULL.value,
        "status": InboxStatus.PENDING.value,
    }
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'ingress.policy_suppressed'",
        )
        == 0
    )

    ignored = _event(
        "ignored-first",
        policy=ProcessingPolicy.IGNORED,
        payload={"first": "ignored"},
    )
    full_duplicate = replace(
        ignored,
        payload={"second": "full"},
        processing_policy=ProcessingPolicy.FULL,
    )
    suppressed = await repo.insert(ignored, 1, 1)
    assert await repo.insert(full_duplicate, 1, 1) == IngressReceipt(
        suppressed.inbox_id, True
    )
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT payload, processing_policy, status FROM event_inbox WHERE id = %s",
        (suppressed.inbox_id,),
    ) == {
        "payload": {"first": "ignored"},
        "processing_policy": ProcessingPolicy.IGNORED.value,
        "status": InboxStatus.COMPLETED.value,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_global_dedupe_collision_across_accounts_fails_closed(
    inbox_runtime: _InboxRuntime,
) -> None:
    await inbox_runtime.ownership.bootstrap(9, "durable_v1")
    key = "d" * 64
    first = _event("tenant-a", dedupe_key=key)
    other_tenant = _event("tenant-a", account_id=9, dedupe_key=key)
    await inbox_runtime.repository.insert(first, 1, 1)

    with pytest.raises(DatabaseOperationError) as caught:
        await inbox_runtime.repository.insert(other_tenant, 1, 1)

    assert caught.value.operation == "insert_event_inbox"
    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox dedupe identity conflict"
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT pg_catalog.count(*) FROM event_inbox WHERE dedupe_key = %s",
            (key,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_same_account_dedupe_cannot_forge_immutable_source_identity(
    inbox_runtime: _InboxRuntime,
) -> None:
    key = "e" * 64
    original = _event("identity-original", dedupe_key=key)
    forged = replace(original, external_email_id="different-message")
    await inbox_runtime.repository.insert(original, 1, 1)

    with pytest.raises(DatabaseOperationError) as caught:
        await inbox_runtime.repository.insert(forged, 1, 1)

    assert caught.value.operation == "insert_event_inbox"
    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox dedupe identity conflict"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_skip_locked_claims_are_disjoint_and_complete_the_batch(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    for ordinal in range(20):
        await repo.insert(_event(f"claim-{ordinal:02d}"), 1, 1)

    first, second = await asyncio.gather(
        repo.claim_batch("worker-a", {"durable_v1"}, 10, 60),
        repo.claim_batch("worker-b", {"durable_v1"}, 10, 60),
    )

    first_ids = {item.id for item in first}
    second_ids = {item.id for item in second}
    assert first_ids.isdisjoint(second_ids)
    assert len(first_ids | second_ids) == 20


@pytest.mark.integration
@pytest.mark.asyncio
async def test_quiesce_is_a_real_insert_and_claim_barrier(
    inbox_runtime: _InboxRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = inbox_runtime.repository
    existing = await repo.insert(_event("before-barrier"), 1, 1)
    waiting_event = _event("loses-to-barrier")
    key = ownership_advisory_lock_key(8)
    lock_attempts = 0
    both_waiting = asyncio.Event()
    original_lock = repo._acquire_account_lock

    async def observed_lock(connection, account_id: int) -> None:
        nonlocal lock_attempts
        lock_attempts += 1
        if lock_attempts == 2:
            both_waiting.set()
        await original_lock(connection, account_id)

    monkeypatch.setattr(repo, "_acquire_account_lock", observed_lock)

    async with inbox_runtime.pool.connection() as connection:
        async with connection.transaction():
            await connection.execute(
                "SELECT pg_catalog.pg_advisory_xact_lock(%s)", (key,)
            )
            insert_task = asyncio.create_task(repo.insert(waiting_event, 1, 1))
            claim_task = asyncio.create_task(
                repo.claim_batch("worker", {"durable_v1"}, 10, 60)
            )
            await asyncio.wait_for(both_waiting.wait(), timeout=5)
            await connection.execute(
                "UPDATE pipeline_ownership "
                "SET state = 'quiescing', reason = 'test barrier', "
                "updated_at = pg_catalog.clock_timestamp() "
                "WHERE account_id = 8 AND generation = 1"
            )

    with pytest.raises(StaleFence):
        await asyncio.wait_for(insert_task, timeout=5)
    assert await asyncio.wait_for(claim_task, timeout=5) == []
    assert await repo.insert(_event("before-barrier"), 1, 1) == IngressReceipt(
        existing.inbox_id, True
    )
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT pg_catalog.count(*) FROM event_inbox",
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_claim_first_commits_before_quiesce_can_return(
    inbox_runtime: _InboxRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("claim-first-barrier"), 1, 1)
    claim_has_lock = asyncio.Event()
    release_claim = asyncio.Event()
    quiesce_attempted = asyncio.Event()
    original_claim_lock = repo._acquire_account_lock
    original_control_lock = inbox_runtime.ownership._acquire_account_lock

    async def hold_claim_lock(connection, account_id: int) -> None:
        await original_claim_lock(connection, account_id)
        claim_has_lock.set()
        await release_claim.wait()

    async def observe_control_lock(connection, account_id: int) -> None:
        quiesce_attempted.set()
        await original_control_lock(connection, account_id)

    monkeypatch.setattr(repo, "_acquire_account_lock", hold_claim_lock)
    monkeypatch.setattr(
        inbox_runtime.ownership,
        "_acquire_account_lock",
        observe_control_lock,
    )
    claim_task = asyncio.create_task(repo.claim_batch("worker", {"durable_v1"}, 1, 60))
    await asyncio.wait_for(claim_has_lock.wait(), timeout=5)
    quiesce_task = asyncio.create_task(
        inbox_runtime.ownership.quiesce(
            8,
            1,
            1,
            "test",
            "claim-first barrier",
        )
    )
    await asyncio.wait_for(quiesce_attempted.wait(), timeout=5)
    assert quiesce_task.done() is False

    release_claim.set()
    leases = await asyncio.wait_for(claim_task, timeout=5)
    quiesced = await asyncio.wait_for(quiesce_task, timeout=5)

    assert len(leases) == 1
    assert quiesced.state is PipelineGenerationState.QUIESCING
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT status FROM event_inbox WHERE id = %s",
            (leases[0].id,),
        )
        == InboxStatus.LEASED.value
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_repeatable_read_session_cannot_claim_across_quiesce_barrier(
    inbox_runtime: _InboxRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await inbox_runtime.repository.insert(_event("repeatable-barrier"), 1, 1)
    repeatable_pool = AsyncConnectionPool(
        conninfo=inbox_runtime.schema.runtime_dsn,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
        configure=_configure_repeatable_read,
    )
    await repeatable_pool.open()
    repository = InboxRepository(repeatable_pool)
    lock_attempted = asyncio.Event()
    original_lock = repository._acquire_account_lock

    async def observed_lock(connection, account_id: int) -> None:
        lock_attempted.set()
        await original_lock(connection, account_id)

    monkeypatch.setattr(repository, "_acquire_account_lock", observed_lock)
    key = ownership_advisory_lock_key(8)
    try:
        assert (
            await _scalar(repeatable_pool, "SHOW default_transaction_isolation")
            == "repeatable read"
        )
        async with inbox_runtime.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_catalog.pg_advisory_xact_lock(%s)", (key,)
                )
                claim_task = asyncio.create_task(
                    repository.claim_batch("worker", {"durable_v1"}, 1, 60)
                )
                await asyncio.wait_for(lock_attempted.wait(), timeout=5)
                await connection.execute(
                    "UPDATE pipeline_ownership SET state = 'quiescing', "
                    "reason = 'repeatable read barrier', "
                    "updated_at = pg_catalog.clock_timestamp() "
                    "WHERE account_id = 8 AND generation = 1"
                )

        assert await asyncio.wait_for(claim_task, timeout=5) == []
    finally:
        await repeatable_pool.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_claim_uses_one_statement_timestamp_for_all_lease_markers(
    inbox_runtime: _InboxRuntime,
) -> None:
    await inbox_runtime.repository.insert(_event("claim-timestamp"), 1, 1)

    lease = (
        await inbox_runtime.repository.claim_batch(
            "worker", {"durable_v1"}, 1, lease_seconds=60
        )
    )[0]

    row = await _fetchone(
        inbox_runtime.pool,
        "SELECT lease_until, processing_started_at, updated_at "
        "FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert row is not None
    statement_time = row["lease_until"] - timedelta(seconds=60)
    assert row["processing_started_at"] == statement_time
    assert row["updated_at"] == statement_time


@pytest.mark.integration
@pytest.mark.asyncio
async def test_renewal_and_reclaim_close_same_worker_aba(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("aba"), 1, 1)
    old = (await repo.claim_batch("same-worker", {"durable_v1"}, 1, 60))[0]

    renewed = await repo.renew(old, 60)
    assert renewed is not None
    assert await repo.begin_effect(old) is False
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
        "- pg_catalog.make_interval(secs => 1) WHERE id = %s",
        (old.id,),
    )
    assert await repo.recover_expired_leases(10) == 1
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET available_at = pg_catalog.clock_timestamp() "
        "- pg_catalog.make_interval(secs => 1) WHERE id = %s",
        (old.id,),
    )
    replacement = (await repo.claim_batch("same-worker", {"durable_v1"}, 1, 60))[0]

    assert replacement.attempts == old.attempts + 1
    assert replacement.lease_until != old.lease_until
    assert await repo.complete(old) is False
    assert await repo.begin_effect(old) is False
    assert await repo.renew(old, 60) is None
    with pytest.raises(StaleFence):
        await repo.fail(old, _TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY))
    assert await repo.complete(replacement) is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_renewal_never_shortens_a_long_existing_deadline(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("long-renewal"), 1, 1)
    original = (
        await repo.claim_batch("worker", {"durable_v1"}, 1, lease_seconds=3600)
    )[0]

    renewed = await repo.renew(original, lease_seconds=1)

    assert renewed is not None
    assert renewed.lease_until > original.lease_until
    assert (renewed.lease_until - original.lease_until).total_seconds() <= 0.001
    assert await repo.begin_effect(original) is False
    assert await repo.begin_effect(renewed) is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_renew_after_effect_rotates_token_and_preserves_effect_authority(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("renew-after-effect"), 1, 1)
    original = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
    assert await repo.begin_effect(original) is True

    renewed = await repo.renew(original, 60)

    assert renewed is not None
    assert renewed.lease_until > original.lease_until
    assert await repo.begin_effect(original) is False
    assert await repo.begin_effect(renewed) is True
    assert await repo.complete(original) is False
    assert await repo.complete(renewed) is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_claim_clears_safe_error_and_preserves_first_processing_marker(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("retry-reclaim"), 1, 1)
    first = (await repo.claim_batch("worker-a", {"durable_v1"}, 1, 60))[0]
    first_marker = await _scalar(
        inbox_runtime.pool,
        "SELECT processing_started_at FROM event_inbox WHERE id = %s",
        (first.id,),
    )
    await repo.fail(first, _TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY))
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET available_at = pg_catalog.clock_timestamp() "
        "- pg_catalog.make_interval(secs => 1) WHERE id = %s",
        (first.id,),
    )

    replacement = (await repo.claim_batch("worker-b", {"durable_v1"}, 1, 60))[0]

    assert replacement.attempts == 1
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT processing_started_at, safe_error_code, safe_error_summary "
        "FROM event_inbox WHERE id = %s",
        (first.id,),
    ) == {
        "processing_started_at": first_marker,
        "safe_error_code": None,
        "safe_error_summary": None,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_claim_allows_draining_but_defensively_excludes_suppressed_policy(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    executable = await repo.insert(_event("draining-executable"), 1, 1)
    anomalous = _event("anomalous-ignored", policy=ProcessingPolicy.IGNORED)
    await _execute(
        inbox_runtime.pool,
        "INSERT INTO event_inbox ("
        "id, account_id, external_email_id, folder_key, source, raw_event_type, "
        "change_kind, dedupe_key, source_version, source_event_at, payload, "
        "processing_policy, pipeline_name, generation, fencing_token, status, "
        "available_at"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "'durable_v1', 1, 1, 'pending', pg_catalog.clock_timestamp())",
        (
            str(uuid4()),
            anomalous.account_id,
            anomalous.external_email_id,
            anomalous.folder,
            anomalous.source.value,
            anomalous.raw_event_type,
            anomalous.kind.value,
            anomalous.dedupe_key,
            anomalous.source_version,
            anomalous.source_event_at,
            Jsonb(anomalous.payload_for_storage()),
            anomalous.processing_policy.value,
        ),
    )
    await inbox_runtime.ownership.quiesce(8, 1, 1, "test", "drain work")
    async with inbox_runtime.pool.connection() as connection:
        async with connection.transaction():
            transaction = inbox_runtime.ownership.transaction(connection)
            locked = await transaction._lock_quiesced(8, 1, 1)
            await transaction._mark_draining(
                locked,
                actor="test",
                reason="drain existing work",
            )

    leases = await repo.claim_batch("worker", {"durable_v1"}, 10, 60)

    assert [lease.id for lease in leases] == [executable.inbox_id]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_already_leased_work_can_finish_while_ownership_is_quiescing(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("quiescing-completion"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
    await inbox_runtime.ownership.quiesce(8, 1, 1, "test", "drain lease")

    assert await repo.begin_effect(lease) is True
    assert await repo.complete(lease) is True


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["reaper", "retirement"])
async def test_reaper_and_retirement_use_account_lock_first_linearization(
    inbox_runtime: _InboxRuntime,
    monkeypatch: pytest.MonkeyPatch,
    winner: str,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event(f"retirement-race-{winner}"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
    await _mark_generation_draining(inbox_runtime)
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
        "- INTERVAL '1 second' WHERE id = %s",
        (lease.id,),
    )
    guarded = PipelineOwnershipRepository(
        inbox_runtime.pool,
        retirement_guard=_AllowRetirement(),
    )
    reaper_has_lock = asyncio.Event()
    release_reaper = asyncio.Event()
    reaper_attempted = asyncio.Event()
    retirement_has_lock = asyncio.Event()
    release_retirement = asyncio.Event()
    retirement_attempted = asyncio.Event()
    original_reaper_lock = repo._acquire_account_lock
    original_retirement_lock = guarded._acquire_account_lock

    async def reaper_lock(connection, account_id: int) -> None:
        reaper_attempted.set()
        await original_reaper_lock(connection, account_id)
        reaper_has_lock.set()
        if winner == "reaper":
            await release_reaper.wait()

    async def retirement_lock(connection, account_id: int) -> None:
        retirement_attempted.set()
        await original_retirement_lock(connection, account_id)
        retirement_has_lock.set()
        if winner == "retirement":
            await release_retirement.wait()

    monkeypatch.setattr(repo, "_acquire_account_lock", reaper_lock)
    monkeypatch.setattr(guarded, "_acquire_account_lock", retirement_lock)

    if winner == "reaper":
        reaper_task = asyncio.create_task(repo.recover_expired_leases(1))
        await asyncio.wait_for(reaper_has_lock.wait(), timeout=5)
        retirement_task = asyncio.create_task(
            guarded.retire(8, 1, 1, "test", "race with reaper")
        )
        await asyncio.wait_for(retirement_attempted.wait(), timeout=5)
        assert retirement_has_lock.is_set() is False
        release_reaper.set()
    else:
        retirement_task = asyncio.create_task(
            guarded.retire(8, 1, 1, "test", "race with reaper")
        )
        await asyncio.wait_for(retirement_has_lock.wait(), timeout=5)
        reaper_task = asyncio.create_task(repo.recover_expired_leases(1))
        await asyncio.wait_for(reaper_attempted.wait(), timeout=5)
        assert reaper_has_lock.is_set() is False
        release_retirement.set()

    with pytest.raises(PipelineRetirementBlocked, match="unresolved work"):
        await asyncio.wait_for(retirement_task, timeout=5)
    assert await asyncio.wait_for(reaper_task, timeout=5) == 1
    assert (await inbox_runtime.ownership.get(8, 1)).state is (
        PipelineGenerationState.DRAINING
    )
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts, lease_owner, safe_error_code "
        "FROM event_inbox WHERE id = %s",
        (lease.id,),
    ) == {
        "status": InboxStatus.RETRY_WAIT.value,
        "attempts": 1,
        "lease_owner": None,
        "safe_error_code": "inbox.lease_expired",
    }


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        pytest.param(RuntimeError("private dead detail"), id="dead-letter"),
        pytest.param(
            _TypedFailure(ErrorKind.SEND_UNKNOWN),
            id="manual-review",
        ),
    ],
)
async def test_concurrent_terminal_failure_writes_one_disposition_and_one_audit(
    inbox_runtime: _InboxRuntime,
    error: BaseException,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event(f"terminal-race-{type(error).__name__}"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]

    results = await asyncio.gather(
        repo.fail(lease, error),
        repo.fail(lease, error),
        return_exceptions=True,
    )

    assert sum(not isinstance(result, BaseException) for result in results) == 1
    assert sum(isinstance(result, StaleFence) for result in results) == 1
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT attempts FROM event_inbox WHERE id = %s",
            (lease.id,),
        )
        == 1
    )
    assert (
        await _scalar(
            inbox_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE object_fingerprint = %s AND object_type = 'event_inbox'",
            (hashlib.sha256(lease.id.encode()).hexdigest(),),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_attempts_zero_through_six_and_bigint_are_bounded(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    expected: dict[int, InboxDispositionStatus] = {}
    for old_attempts in (*range(7), POSTGRES_BIGINT_MAX):
        receipt = await repo.insert(_event(f"attempts-{old_attempts}"), 1, 1)
        await _execute(
            inbox_runtime.pool,
            "UPDATE event_inbox SET attempts = %s WHERE id = %s",
            (old_attempts, receipt.inbox_id),
        )
        lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
        disposition = await repo.fail(
            lease,
            _TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY),
        )
        expected_status = (
            InboxDispositionStatus.RETRY_WAIT
            if old_attempts < 5
            else InboxDispositionStatus.DEAD_LETTER
        )
        expected[old_attempts] = disposition.status
        assert disposition.status is expected_status
        assert disposition.attempts == min(old_attempts + 1, POSTGRES_BIGINT_MAX)
        if expected_status is InboxDispositionStatus.RETRY_WAIT:
            assert disposition.available_at is not None
            delay = (disposition.available_at - datetime.now(UTC)).total_seconds()
            assert 0 < delay <= 900
        else:
            assert disposition.available_at is None

    assert expected == {
        0: InboxDispositionStatus.RETRY_WAIT,
        1: InboxDispositionStatus.RETRY_WAIT,
        2: InboxDispositionStatus.RETRY_WAIT,
        3: InboxDispositionStatus.RETRY_WAIT,
        4: InboxDispositionStatus.RETRY_WAIT,
        5: InboxDispositionStatus.DEAD_LETTER,
        6: InboxDispositionStatus.DEAD_LETTER,
        POSTGRES_BIGINT_MAX: InboxDispositionStatus.DEAD_LETTER,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failure_classification_is_closed_and_never_persists_exception_text(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    cases: list[tuple[BaseException, InboxDispositionStatus, str]] = [
        (
            _TypedFailure(ErrorKind.RATE_LIMITED),
            InboxDispositionStatus.RETRY_WAIT,
            "inbox.rate_limited",
        ),
        (
            _TypedFailure(ErrorKind.AUTHENTICATION),
            InboxDispositionStatus.DEAD_LETTER,
            "inbox.authentication_failure",
        ),
        (
            _TypedFailure(ErrorKind.VALIDATION),
            InboxDispositionStatus.DEAD_LETTER,
            "inbox.validation_failure",
        ),
        (
            _TypedFailure(ErrorKind.PERMANENT_DEPENDENCY),
            InboxDispositionStatus.DEAD_LETTER,
            "inbox.permanent_dependency_failure",
        ),
        (
            _TypedFailure(ErrorKind.POLICY_REJECTED),
            InboxDispositionStatus.DEAD_LETTER,
            "inbox.policy_rejected",
        ),
        (
            _TypedFailure(ErrorKind.INTERNAL_INVARIANT),
            InboxDispositionStatus.DEAD_LETTER,
            "inbox.internal_invariant",
        ),
        (
            _TypedFailure(ErrorKind.SEND_UNKNOWN),
            InboxDispositionStatus.MANUAL_REVIEW,
            "inbox.send_unknown",
        ),
        (
            ManualReviewRequired(
                reason="private-reason",
                safe_summary="private summary must not be trusted",
            ),
            InboxDispositionStatus.MANUAL_REVIEW,
            "inbox.manual_review_required",
        ),
        (
            DatabaseOperationError(
                operation="private-operation",
                retryable=True,
                message="private database detail",
            ),
            InboxDispositionStatus.RETRY_WAIT,
            "inbox.database_transient",
        ),
        (
            DatabaseOperationError(
                operation="private-operation",
                retryable=False,
                message="private database detail",
            ),
            InboxDispositionStatus.DEAD_LETTER,
            "inbox.database_failure",
        ),
        (
            RuntimeError("private unknown detail"),
            InboxDispositionStatus.DEAD_LETTER,
            "inbox.internal_invariant",
        ),
    ]

    for index, (error, expected_status, expected_code) in enumerate(cases):
        await repo.insert(_event(f"classification-{index}"), 1, 1)
        lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
        disposition = await repo.fail(lease, error)
        assert disposition.status is expected_status
        assert disposition.safe_error_code == expected_code
        row = await _fetchone(
            inbox_runtime.pool,
            "SELECT safe_error_code, safe_error_summary FROM event_inbox WHERE id = %s",
            (lease.id,),
        )
        assert row is not None
        assert row["safe_error_code"] == expected_code
        assert "private" not in (row["safe_error_summary"] or "").lower()

    async with inbox_runtime.pool.connection() as connection:
        cursor = await connection.execute(
            "SELECT email_id, object_type, safe_metadata FROM audit_events "
            "WHERE object_type = 'event_inbox'"
        )
        audit_rows = await cursor.fetchall()
    assert audit_rows
    for audit_row in audit_rows:
        assert audit_row["email_id"] is None
        assert audit_row["object_type"] == "event_inbox"
        rendered = repr(audit_row["safe_metadata"]).lower()
        assert "private" not in rendered
        assert "exchange-message" not in rendered


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kind_getter_failure_dead_letters_without_leak_or_stuck_lease(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("exploding-kind"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]

    disposition = await repo.fail(
        lease,
        _ExplodingKindFailure("private outer content"),
    )

    assert disposition.status is InboxDispositionStatus.DEAD_LETTER
    assert disposition.safe_error_code == "inbox.internal_invariant"
    row = await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts, lease_owner, lease_until, "
        "safe_error_code, safe_error_summary FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert row == {
        "status": InboxStatus.DEAD_LETTER.value,
        "attempts": 1,
        "lease_owner": None,
        "lease_until": None,
        "safe_error_code": "inbox.internal_invariant",
        "safe_error_summary": "Inbox processing invariant failed",
    }
    assert "private" not in repr(row).lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_after_effect_every_unproven_failure_requires_manual_review(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("effect-failure"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
    assert await repo.begin_effect(lease) is True

    disposition = await repo.fail(
        lease,
        _TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY),
    )

    assert disposition.status is InboxDispositionStatus.MANUAL_REVIEW
    assert disposition.available_at is None
    assert disposition.safe_error_code == "inbox.effect_outcome_unknown"
    row = await _fetchone(
        inbox_runtime.pool,
        "SELECT status, effect_started_at, attempts, lease_owner, lease_until "
        "FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert row is not None
    assert row["status"] == InboxStatus.MANUAL_REVIEW.value
    assert row["effect_started_at"] is not None
    assert row["attempts"] == 1
    assert row["lease_owner"] is None
    assert row["lease_until"] is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expired_leases_recover_before_effect_and_quarantine_after_effect(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("expired-before"), 1, 1)
    before = (await repo.claim_batch("worker-a", {"durable_v1"}, 1, 60))[0]
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
        "- pg_catalog.make_interval(secs => 1) WHERE id = %s",
        (before.id,),
    )
    assert await repo.recover_expired_leases(1) == 1
    before_row = await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts, lease_owner, safe_error_code FROM event_inbox "
        "WHERE id = %s",
        (before.id,),
    )
    assert before_row == {
        "status": InboxStatus.RETRY_WAIT.value,
        "attempts": 1,
        "lease_owner": None,
        "safe_error_code": "inbox.lease_expired",
    }

    await repo.insert(_event("expired-after"), 1, 1)
    after = (await repo.claim_batch("worker-b", {"durable_v1"}, 1, 60))[0]
    assert await repo.begin_effect(after) is True
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
        "- pg_catalog.make_interval(secs => 1) WHERE id = %s",
        (after.id,),
    )
    assert await repo.recover_expired_leases(1) == 1
    after_row = await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts, effect_started_at, safe_error_code "
        "FROM event_inbox WHERE id = %s",
        (after.id,),
    )
    assert after_row is not None
    assert after_row["status"] == InboxStatus.MANUAL_REVIEW.value
    assert after_row["attempts"] == 1
    assert after_row["effect_started_at"] is not None
    assert after_row["safe_error_code"] == "inbox.effect_outcome_unknown"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_effect_first_and_expiry_reaper_linearize_on_the_database_row(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("effect-first-race"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 2))[0]
    effect_updated = asyncio.Event()
    release_effect = asyncio.Event()
    reaper_select_started = asyncio.Event()
    effect_repo = InboxRepository(
        _ProbedPool(
            inbox_runtime.pool,
            match=lambda statement: "effect_started_at = COALESCE" in statement,
            after=effect_updated,
            release=release_effect,
        )
    )
    reaper_repo = InboxRepository(
        _ProbedPool(
            inbox_runtime.pool,
            match=lambda statement: "FOR UPDATE OF e SKIP LOCKED LIMIT" in statement,
            before=reaper_select_started,
        )
    )

    effect_task = asyncio.create_task(effect_repo.begin_effect(lease))
    await asyncio.wait_for(effect_updated.wait(), timeout=5)
    deadline = asyncio.get_running_loop().time() + 5
    while not await _scalar(
        inbox_runtime.pool,
        "SELECT pg_catalog.clock_timestamp() >= lease_until "
        "FROM event_inbox WHERE id = %s",
        (lease.id,),
    ):
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("lease did not reach its database expiry boundary")
        await asyncio.sleep(0.01)

    reaper_task = asyncio.create_task(reaper_repo.recover_expired_leases(1))
    await asyncio.wait_for(reaper_select_started.wait(), timeout=5)
    release_effect.set()

    assert await asyncio.wait_for(effect_task, timeout=5) is True
    assert await asyncio.wait_for(reaper_task, timeout=5) == 0
    intermediate = await _fetchone(
        inbox_runtime.pool,
        "SELECT status, effect_started_at, attempts FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert intermediate is not None
    assert intermediate["status"] == InboxStatus.LEASED.value
    assert intermediate["effect_started_at"] is not None
    assert intermediate["attempts"] == 0
    assert await repo.recover_expired_leases(1) == 1
    row = await _fetchone(
        inbox_runtime.pool,
        "SELECT status, effect_started_at, attempts, safe_error_code "
        "FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert row is not None
    assert row["status"] == InboxStatus.MANUAL_REVIEW.value
    assert row["effect_started_at"] is not None
    assert row["attempts"] == 1
    assert row["safe_error_code"] == "inbox.effect_outcome_unknown"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reaper_first_invalidates_concurrent_begin_effect_token(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("reaper-first-race"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
        "- INTERVAL '1 second' WHERE id = %s",
        (lease.id,),
    )
    reaper_updated = asyncio.Event()
    release_reaper = asyncio.Event()
    effect_update_started = asyncio.Event()
    reaper_repo = InboxRepository(
        _ProbedPool(
            inbox_runtime.pool,
            match=lambda statement: (
                statement.lstrip().startswith("UPDATE")
                and "effect_started_at IS NOT DISTINCT FROM" in statement
            ),
            after=reaper_updated,
            release=release_reaper,
        )
    )
    effect_repo = InboxRepository(
        _ProbedPool(
            inbox_runtime.pool,
            match=lambda statement: "effect_started_at = COALESCE" in statement,
            before=effect_update_started,
        )
    )

    reaper_task = asyncio.create_task(reaper_repo.recover_expired_leases(1))
    await asyncio.wait_for(reaper_updated.wait(), timeout=5)
    effect_task = asyncio.create_task(effect_repo.begin_effect(lease))
    await asyncio.wait_for(effect_update_started.wait(), timeout=5)
    release_reaper.set()

    assert await asyncio.wait_for(reaper_task, timeout=5) == 1
    assert await asyncio.wait_for(effect_task, timeout=5) is False
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, effect_started_at, attempts, safe_error_code "
        "FROM event_inbox WHERE id = %s",
        (lease.id,),
    ) == {
        "status": InboxStatus.RETRY_WAIT.value,
        "effect_started_at": None,
        "attempts": 1,
        "safe_error_code": "inbox.lease_expired",
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expiry_recovery_saturates_postgres_bigint_without_overflow(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    receipt = await repo.insert(_event("expired-bigint"), 1, 1)
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET attempts = %s WHERE id = %s",
        (POSTGRES_BIGINT_MAX, receipt.inbox_id),
    )
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
        "- pg_catalog.make_interval(secs => 1) WHERE id = %s",
        (lease.id,),
    )

    assert await repo.recover_expired_leases(1) == 1
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts, safe_error_code FROM event_inbox WHERE id = %s",
        (lease.id,),
    ) == {
        "status": InboxStatus.DEAD_LETTER.value,
        "attempts": POSTGRES_BIGINT_MAX,
        "safe_error_code": "inbox.lease_expired",
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cancelled_failure_is_re_raised_without_disposition_write(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("cancelled"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]

    with pytest.raises(asyncio.CancelledError):
        await repo.fail(lease, asyncio.CancelledError())

    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts, lease_owner, safe_error_code FROM event_inbox "
        "WHERE id = %s",
        (lease.id,),
    ) == {
        "status": InboxStatus.LEASED.value,
        "attempts": 0,
        "lease_owner": "worker",
        "safe_error_code": None,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_audit_conflict_rolls_back_terminal_transition(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("audit-conflict"), 1, 1)
    lease = (await repo.claim_batch("worker", {"durable_v1"}, 1, 60))[0]
    event_key = _audit_event_key(lease.id, "ingress.completed", lease.attempts)
    await _execute(
        inbox_runtime.pool,
        "INSERT INTO audit_events ("
        "id, event_key, account_id, email_id, object_type, object_fingerprint, "
        "action, result, actor, reason, safe_metadata"
        ") VALUES (%s, %s, %s, NULL, 'event_inbox', %s, "
        "'ingress.tampered', 'tampered', 'attacker', 'tampered', %s)",
        (
            str(uuid4()),
            event_key,
            lease.account_id,
            hashlib.sha256(lease.id.encode()).hexdigest(),
            Jsonb({"tampered": True}),
        ),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await repo.complete(lease)

    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox audit invariant failed"
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, lease_owner, lease_until FROM event_inbox WHERE id = %s",
        (lease.id,),
    ) == {
        "status": InboxStatus.LEASED.value,
        "lease_owner": "worker",
        "lease_until": lease.lease_until,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poisoned_reaper_audit_isolated_and_limit_one_still_makes_progress(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    for name in ("poison", "normal-a", "normal-b"):
        await repo.insert(_event(f"reaper-{name}"), 1, 1)
    leases = await repo.claim_batch("worker", {"durable_v1"}, 3, 60)
    by_email = {lease.event.external_email_id: lease for lease in leases}
    poison = by_email["exchange-message-reaper-poison"]
    normals = [
        by_email["exchange-message-reaper-normal-a"],
        by_email["exchange-message-reaper-normal-b"],
    ]
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET attempts = 5, "
        "lease_until = pg_catalog.clock_timestamp() - INTERVAL '30 seconds' "
        "WHERE id = %s",
        (poison.id,),
    )
    for offset, normal in enumerate(normals, start=20):
        await _execute(
            inbox_runtime.pool,
            "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
            "- pg_catalog.make_interval(secs => %s) WHERE id = %s",
            (offset, normal.id),
        )

    await _execute(
        inbox_runtime.pool,
        "INSERT INTO audit_events ("
        "id, event_key, account_id, email_id, object_type, object_fingerprint, "
        "action, result, actor, reason, safe_metadata"
        ") VALUES (%s, %s, %s, NULL, 'event_inbox', %s, "
        "'ingress.tampered', 'tampered', 'attacker', 'tampered', %s)",
        (
            str(uuid4()),
            _audit_event_key(poison.id, "ingress.dead_letter", 6),
            poison.account_id,
            hashlib.sha256(poison.id.encode()).hexdigest(),
            Jsonb({"secret-conflict-content": True}),
        ),
    )

    for _ in normals:
        with pytest.raises(DatabaseOperationError) as caught:
            await repo.recover_expired_leases(limit=1)
        assert caught.value.operation == "event_inbox_invariant"
        assert caught.value.retryable is False
        assert str(caught.value) == "event inbox audit invariant failed"
        assert "secret-conflict-content" not in str(caught.value)

    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts, lease_owner FROM event_inbox WHERE id = %s",
        (poison.id,),
    ) == {
        "status": InboxStatus.LEASED.value,
        "attempts": 5,
        "lease_owner": "worker",
    }
    for normal in normals:
        assert await _fetchone(
            inbox_runtime.pool,
            "SELECT status, attempts, lease_owner, safe_error_code "
            "FROM event_inbox WHERE id = %s",
            (normal.id,),
        ) == {
            "status": InboxStatus.RETRY_WAIT.value,
            "attempts": 1,
            "lease_owner": None,
            "safe_error_code": "inbox.lease_expired",
        }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reaper_only_isolates_compare_mismatch_not_other_audit_errors(
    inbox_runtime: _InboxRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = inbox_runtime.repository
    await repo.insert(_event("reaper-generic-audit-error"), 1, 1)
    await repo.insert(_event("reaper-generic-audit-normal"), 1, 1)
    leases = await repo.claim_batch("worker", {"durable_v1"}, 2, 60)
    terminal, normal = leases
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET attempts = 5, "
        "lease_until = pg_catalog.clock_timestamp() - INTERVAL '30 seconds' "
        "WHERE id = %s",
        (terminal.id,),
    )
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET lease_until = pg_catalog.clock_timestamp() "
        "- INTERVAL '20 seconds' WHERE id = %s",
        (normal.id,),
    )

    async def fail_audit(*_args, **_kwargs) -> None:
        raise DatabaseOperationError(
            operation="injected_audit_failure",
            retryable=False,
            message="fixed injected audit failure",
        )

    monkeypatch.setattr(repo, "_append_audit", fail_audit)
    with pytest.raises(DatabaseOperationError) as caught:
        await repo.recover_expired_leases(2)

    assert caught.value.operation == "injected_audit_failure"
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts FROM event_inbox WHERE id = %s",
        (terminal.id,),
    ) == {"status": InboxStatus.LEASED.value, "attempts": 5}
    assert await _fetchone(
        inbox_runtime.pool,
        "SELECT status, attempts FROM event_inbox WHERE id = %s",
        (normal.id,),
    ) == {"status": InboxStatus.LEASED.value, "attempts": 0}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stats_include_every_operator_backlog_state(
    inbox_runtime: _InboxRuntime,
) -> None:
    repo = inbox_runtime.repository
    empty = await repo.stats()
    assert empty.pending == 0
    assert empty.retry_wait == 0
    assert empty.leased == 0
    assert empty.dead_letter == 0
    assert empty.manual_review == 0
    assert empty.oldest_pending_seconds == 0

    await repo.insert(_event("stats-leased"), 1, 1)
    leased = (await repo.claim_batch("worker-a", {"durable_v1"}, 1, 60))[0]

    await repo.insert(_event("stats-retry"), 1, 1)
    retry_lease = (await repo.claim_batch("worker-b", {"durable_v1"}, 1, 60))[0]
    await repo.fail(retry_lease, _TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY))

    dead_receipt = await repo.insert(_event("stats-dead"), 1, 1)
    await _execute(
        inbox_runtime.pool,
        "UPDATE event_inbox SET attempts = 5 WHERE id = %s",
        (dead_receipt.inbox_id,),
    )
    dead_lease = (await repo.claim_batch("worker-c", {"durable_v1"}, 1, 60))[0]
    await repo.fail(dead_lease, _TypedFailure(ErrorKind.TRANSIENT_DEPENDENCY))

    await repo.insert(_event("stats-manual"), 1, 1)
    manual_lease = (await repo.claim_batch("worker-d", {"durable_v1"}, 1, 60))[0]
    await repo.fail(manual_lease, _TypedFailure(ErrorKind.SEND_UNKNOWN))

    pending = await repo.insert(_event("stats-pending"), 1, 1)
    assert pending.duplicate is False

    stats = await repo.stats()

    assert stats.pending == 1
    assert stats.retry_wait == 1
    assert stats.leased == 1
    assert stats.dead_letter == 1
    assert stats.manual_review == 1
    assert stats.oldest_pending_seconds >= 0
    assert leased.id != pending.inbox_id
