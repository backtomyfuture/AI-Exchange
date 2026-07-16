from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.domain.email_state import PipelineGenerationState
from src.domain.errors import DatabaseOperationError, ManualReviewRequired, StaleFence
from src.ingestion.email_events import EmailEventDisposition, EmailStatus
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.ownership import PipelineOwnershipRepository
from src.ingestion.repository import EmailEventTransaction, InboxRepository


@dataclass(slots=True)
class _EmailRuntime:
    schema: Any
    pool: AsyncConnectionPool
    ownership: PipelineOwnershipRepository
    repository: InboxRepository


class _AllowRetirement:
    async def assert_ready(self, _connection, _generation) -> None:
        return None


class _TupleCursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self._row = row

    async def fetchone(self) -> tuple[object, ...]:
        return self._row


class _TupleConnection:
    def __init__(self, row: tuple[object, ...]) -> None:
        self._row = row

    async def execute(self, _query, _params) -> _TupleCursor:
        return _TupleCursor(self._row)


@dataclass(slots=True)
class _WorkerLifecycleSpy:
    complete_calls: int = 0
    fail_calls: int = 0
    renew_calls: int = 0
    begin_effect_calls: int = 0

    async def complete(self) -> None:
        self.complete_calls += 1

    async def fail(self) -> None:
        self.fail_calls += 1

    async def renew(self) -> None:
        self.renew_calls += 1

    async def begin_effect(self) -> None:
        self.begin_effect_calls += 1


class SimulatedCommitAckLoss(RuntimeError):
    pass


class _CommitAckLossTransaction:
    def __init__(self, transaction) -> None:
        self._transaction = transaction

    async def __aenter__(self):
        return await self._transaction.__aenter__()

    async def __aexit__(self, exc_type, exc, traceback):
        result = await self._transaction.__aexit__(exc_type, exc, traceback)
        if exc_type is None:
            raise SimulatedCommitAckLoss("transaction committed but ACK was lost")
        return result


class _CommitAckLossConnection:
    def __init__(self, connection) -> None:
        self._connection = connection

    @property
    def info(self):
        return self._connection.info

    async def execute(self, query, params=None):
        return await self._connection.execute(query, params)

    def transaction(self):
        return _CommitAckLossTransaction(self._connection.transaction())


class _CommitAckLossPool:
    def __init__(self, pool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def connection(self):
        async with self._pool.connection() as connection:
            yield _CommitAckLossConnection(connection)


async def _worker_handoff(application, lifecycle: _WorkerLifecycleSpy) -> None:
    if application.should_process:
        await lifecycle.begin_effect()
    elif application.may_complete_without_processing:
        await lifecycle.complete()


@pytest_asyncio.fixture
async def email_runtime(postgres_database_factory) -> _EmailRuntime:
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
        yield _EmailRuntime(
            schema=schema,
            pool=pool,
            ownership=ownership,
            repository=InboxRepository(pool),
        )
    finally:
        await pool.close()


def _event(
    token: str,
    *,
    source: IngressSource = IngressSource.WEBHOOK,
    kind: ChangeKind = ChangeKind.CREATE,
    external_email_id: str | None = None,
    folder: str = "INBOX",
    payload: dict[str, object] | None = None,
    account_id: int = 8,
) -> NormalizedIngressEvent:
    return NormalizedIngressEvent(
        account_id=account_id,
        source=source,
        raw_event_type=f"Synthetic{kind.value.title()}Event",
        kind=kind,
        external_email_id=external_email_id or f"synthetic-message-{token}",
        folder=folder,
        source_version=f"version-{token}",
        dedupe_key=hashlib.sha256(f"{account_id}:{token}".encode()).hexdigest(),
        payload=payload or {},
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=datetime.now(UTC),
    )


async def _insert_and_claim(
    runtime: _EmailRuntime,
    event: NormalizedIngressEvent,
    *,
    worker: str = "email-worker",
    generation: int = 1,
    fencing_token: int = 1,
):
    receipt = await runtime.repository.insert(event, generation, fencing_token)
    leases = await runtime.repository.claim_batch(
        worker,
        {"durable_v1"},
        limit=10,
        lease_seconds=60,
    )
    return next(lease for lease in leases if lease.id == receipt.inbox_id)


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
    return next(iter(row.values())) if row is not None else None


async def _execute(
    pool: AsyncConnectionPool,
    statement: str,
    params: tuple[object, ...] = (),
) -> None:
    async with pool.connection() as connection:
        await connection.execute(statement, params)


async def _wait_for_advisory_waiter(pool: AsyncConnectionPool) -> None:
    for _ in range(100):
        waiting = await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_locks "
            "WHERE locktype = 'advisory' AND granted = false",
        )
        if isinstance(waiting, int) and waiting > 0:
            return
        await asyncio.sleep(0.01)
    pytest.fail("control-plane advisory lock did not enter the wait queue")


async def _promote_generation_two(runtime: _EmailRuntime) -> None:
    quiesced = await runtime.ownership.quiesce(
        8,
        1,
        1,
        "test",
        "prepare cross-generation email event",
    )
    assert quiesced.state is PipelineGenerationState.QUIESCING
    async with runtime.pool.connection() as connection:
        async with connection.transaction():
            transaction = runtime.ownership.transaction(connection)
            locked = await transaction._lock_quiesced(8, 1, 1)
            draining = await transaction._mark_draining(
                locked,
                actor="test",
                reason="drain old email owner",
            )
            current = await transaction._insert_current(
                account_id=8,
                pipeline_name="durable_v1",
                generation=2,
                fencing_token=2,
                actor="test",
                reason="promote new email ingress owner",
            )
    assert draining.state is PipelineGenerationState.DRAINING
    assert current.state is PipelineGenerationState.CURRENT_INGRESS


async def _retire_generation_one(runtime: _EmailRuntime) -> None:
    retirement = PipelineOwnershipRepository(
        runtime.pool,
        retirement_guard=_AllowRetirement(),
    )
    retired = await retirement.retire(
        8,
        1,
        1,
        "test",
        "retire terminal email owner",
    )
    assert retired.state is PipelineGenerationState.RETIRED


@pytest.mark.asyncio
async def test_exact_lease_lock_supports_a_composite_tuple_row() -> None:
    event = _event("tuple-exact-lease")
    received_at = datetime.now(UTC)
    lease_until = received_at + timedelta(seconds=60)
    repository = InboxRepository(pool=None)
    lease = InboxLease(
        id=str(uuid4()),
        account_id=event.account_id,
        pipeline_name="durable_v1",
        generation=1,
        fencing_token=1,
        lease_owner="tuple-worker",
        attempts=1,
        event=event,
        received_at=received_at,
        lease_until=lease_until,
    )
    tuple_row = (
        lease.id,
        lease.account_id,
        event.external_email_id,
        event.folder,
        event.source.value,
        event.raw_event_type,
        event.kind.value,
        event.dedupe_key,
        event.source_version,
        event.source_event_at,
        event.payload,
        event.processing_policy.value,
        lease.pipeline_name,
        lease.generation,
        lease.fencing_token,
        lease.lease_owner,
        lease.lease_until,
        lease.attempts,
        lease.received_at,
        "leased",
        True,
    )
    transaction = repository.transaction(_TupleConnection(tuple_row))

    await transaction._lock_exact_lease(lease)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_first_create_persists_processing_and_exact_owner_atomically(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event("first-create", payload={"is_read": True})
    lease = await _insert_and_claim(email_runtime, event)

    application = await email_runtime.repository.apply_email_event(lease)

    assert application.disposition is EmailEventDisposition.CREATOR_ELECTED
    assert application.persisted_status is EmailStatus.PROCESSING
    assert application.should_process is True
    assert application.version == 1
    assert application.may_complete_without_processing is False
    row = await _fetchone(
        email_runtime.pool,
        "SELECT account_id, external_email_id, source_folder_key, status, version, "
        "owner_generation, owner_fencing_token, processing_inbox_id, "
        "create_seen_at, processing_started_at, source_deleted_at, "
        "is_read, is_read_refresh_required FROM emails WHERE id = %s",
        (application.email_id,),
    )
    assert row is not None
    assert row["account_id"] == lease.account_id
    assert row["external_email_id"] == event.external_email_id
    assert row["source_folder_key"] == event.folder
    assert row["status"] == EmailStatus.PROCESSING.value
    assert row["version"] == 1
    assert row["owner_generation"] == lease.generation
    assert row["owner_fencing_token"] == lease.fencing_token
    assert str(row["processing_inbox_id"]) == lease.id
    assert row["create_seen_at"] is not None
    assert row["processing_started_at"] is not None
    assert row["source_deleted_at"] is None
    assert row["is_read"] is True
    assert row["is_read_refresh_required"] is False
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE email_id = %s AND action = 'email.processing_attempt'",
            (application.email_id,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_webhook_and_sync_create_with_distinct_dedupe_elect_once(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-cross-source-message"
    webhook = _event(
        "webhook-create",
        external_email_id=external_email_id,
        payload={"is_read": True},
    )
    sync = _event(
        "sync-create",
        source=IngressSource.SYNC,
        external_email_id=external_email_id,
        payload={"item": {"is_read": False}},
    )
    webhook_receipt, sync_receipt = await asyncio.gather(
        email_runtime.repository.insert(webhook, 1, 1),
        email_runtime.repository.insert(sync, 1, 1),
    )
    leases = await email_runtime.repository.claim_batch(
        "concurrent-worker",
        {"durable_v1"},
        limit=10,
        lease_seconds=60,
    )
    by_id = {lease.id: lease for lease in leases}

    first, second = await asyncio.gather(
        email_runtime.repository.apply_email_event(by_id[webhook_receipt.inbox_id]),
        email_runtime.repository.apply_email_event(by_id[sync_receipt.inbox_id]),
    )

    assert sum(result.should_process for result in (first, second)) == 1
    assert {first.disposition, second.disposition} == {
        EmailEventDisposition.CREATOR_ELECTED,
        EmailEventDisposition.AGGREGATE_NOOP,
    }
    loser = next(result for result in (first, second) if not result.should_process)
    assert loser.may_complete_without_processing is True
    assert first.email_id == second.email_id
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails "
            "WHERE account_id = 8 AND external_email_id = %s",
            (external_email_id,),
        )
        == 1
    )
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'email.processing_attempt'",
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pristine_active_attempt_concurrently_elects_once(
    email_runtime: _EmailRuntime,
) -> None:
    lease = await _insert_and_claim(email_runtime, _event("pristine-race"))

    first, second = await asyncio.gather(
        email_runtime.repository.apply_email_event(lease),
        email_runtime.repository.apply_email_event(lease),
    )

    assert sum(result.should_process for result in (first, second)) == 1
    assert {first.disposition, second.disposition} == {
        EmailEventDisposition.CREATOR_ELECTED,
        EmailEventDisposition.PROCESSING_ALREADY_ELECTED,
    }
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'email.processing_attempt'",
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker_column",
    ["external_effects_started_at", "source_deleted_at"],
)
async def test_exact_processing_owner_replay_recognizes_receipt_before_markers(
    email_runtime: _EmailRuntime,
    marker_column: str,
) -> None:
    lease = await _insert_and_claim(
        email_runtime,
        _event(f"exact-owner-marker-{marker_column}"),
    )
    elected = await email_runtime.repository.apply_email_event(lease)
    assert elected.should_process is True
    await _execute(
        email_runtime.pool,
        f"UPDATE emails SET {marker_column} = pg_catalog.clock_timestamp() "
        "WHERE id = %s AND processing_inbox_id = %s",
        (elected.email_id, lease.id),
    )

    replay = await email_runtime.repository.apply_email_event(lease)

    assert replay.disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED
    assert replay.persisted_status is EmailStatus.PROCESSING
    assert replay.should_process is False
    assert replay.may_complete_without_processing is False
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE email_id = %s AND action = 'email.processing_attempt'",
            (elected.email_id,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_attempt_worker_handoff_never_mutates_lease_lifecycle(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-worker-handoff"
    creator_lease = await _insert_and_claim(
        email_runtime,
        _event("worker-owner", external_email_id=external_email_id),
    )
    creator = await email_runtime.repository.apply_email_event(creator_lease)
    duplicate = await email_runtime.repository.apply_email_event(creator_lease)
    loser_lease = await _insert_and_claim(
        email_runtime,
        _event("worker-loser", external_email_id=external_email_id),
        worker="worker-loser",
    )
    loser = await email_runtime.repository.apply_email_event(loser_lease)
    creator_spy = _WorkerLifecycleSpy()
    duplicate_spy = _WorkerLifecycleSpy()
    loser_spy = _WorkerLifecycleSpy()

    await _worker_handoff(creator, creator_spy)
    await _worker_handoff(duplicate, duplicate_spy)
    await _worker_handoff(loser, loser_spy)

    assert creator_spy == _WorkerLifecycleSpy(begin_effect_calls=1)
    assert duplicate_spy == _WorkerLifecycleSpy()
    assert loser_spy == _WorkerLifecycleSpy(complete_calls=1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_commit_ack_loss_after_clean_commit_replays_exact_receipt(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event("lost-commit-ack")
    lease = await _insert_and_claim(email_runtime, event)
    fault_repository = InboxRepository(_CommitAckLossPool(email_runtime.pool))

    with pytest.raises(SimulatedCommitAckLoss):
        await fault_repository.apply_email_event(lease)

    receipt = await _fetchone(
        email_runtime.pool,
        "SELECT a.id, a.created_at, a.email_id FROM audit_events AS a "
        "JOIN emails AS e ON e.id = a.email_id "
        "WHERE e.account_id = %s AND e.external_email_id = %s "
        "AND a.action = 'email.processing_attempt'",
        (event.account_id, event.external_email_id),
    )
    assert receipt is not None

    replay = await email_runtime.repository.apply_email_event(lease)
    persisted = await _fetchone(
        email_runtime.pool,
        "SELECT id, created_at, email_id FROM audit_events "
        "WHERE email_id = %s AND action = 'email.processing_attempt'",
        (receipt["email_id"],),
    )

    assert replay.disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED
    assert replay.should_process is False
    assert replay.may_complete_without_processing is False
    assert persisted == receipt
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE email_id = %s AND action = 'email.processing_attempt'",
            (receipt["email_id"],),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_forged_payload_lease_is_rejected_and_new_shell_rolls_back(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event("forged-payload", payload={"is_read": False})
    lease = await _insert_and_claim(email_runtime, event)
    forged_event = replace(event, payload={"is_read": True, "forged": True})
    forged_lease = replace(lease, event=forged_event)

    with pytest.raises(StaleFence):
        await email_runtime.repository.apply_email_event(forged_lease)

    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails "
            "WHERE account_id = %s AND external_email_id = %s",
            (lease.account_id, event.external_email_id),
        )
        == 0
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("persisted_is_read", "forged_is_read"),
    [(1, True), (True, 1)],
)
async def test_forged_payload_rejects_json_boolean_number_type_confusion(
    email_runtime: _EmailRuntime,
    persisted_is_read: object,
    forged_is_read: object,
) -> None:
    event = _event(
        f"forged-json-type-{persisted_is_read}-{forged_is_read}",
        payload={"is_read": persisted_is_read},
    )
    lease = await _insert_and_claim(email_runtime, event)
    forged_lease = replace(
        lease,
        event=replace(event, payload={"is_read": forged_is_read}),
    )

    with pytest.raises(StaleFence):
        await email_runtime.repository.apply_email_event(forged_lease)

    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails "
            "WHERE account_id = %s AND external_email_id = %s",
            (event.account_id, event.external_email_id),
        )
        == 0
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_metadata_shell_then_delayed_create_preserves_conflicting_projection(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-delayed-create"
    update = _event(
        "metadata-shell",
        kind=ChangeKind.UPDATE,
        external_email_id=external_email_id,
        folder="Archive",
        payload={"is_read": True},
    )
    update_lease = await _insert_and_claim(email_runtime, update)
    shell = await email_runtime.repository.apply_email_event(update_lease)
    assert shell.disposition is EmailEventDisposition.METADATA_SHELL_CREATED
    assert shell.version == 0
    assert shell.may_complete_without_processing is True
    assert await email_runtime.repository.complete(update_lease) is True

    create = _event(
        "delayed-create",
        external_email_id=external_email_id,
        folder="INBOX",
        payload={"is_read": False},
    )
    create_lease = await _insert_and_claim(email_runtime, create)
    application = await email_runtime.repository.apply_email_event(create_lease)

    assert application.disposition is EmailEventDisposition.CREATOR_ELECTED
    row = await _fetchone(
        email_runtime.pool,
        "SELECT source_folder_key, is_read, is_read_refresh_required, version "
        "FROM emails WHERE id = %s",
        (application.email_id,),
    )
    assert row == {
        "source_folder_key": "Archive",
        "is_read": True,
        "is_read_refresh_required": True,
        "version": 1,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_before_create_persists_tombstone_and_never_reopens(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-delete-first"
    delete = _event(
        "delete-first",
        kind=ChangeKind.DELETE,
        external_email_id=external_email_id,
    )
    delete_lease = await _insert_and_claim(email_runtime, delete)
    tombstone = await email_runtime.repository.apply_email_event(delete_lease)

    assert tombstone.disposition is EmailEventDisposition.TOMBSTONE_CREATED
    assert tombstone.persisted_status is EmailStatus.CANCELLED
    assert tombstone.should_cancel is True
    assert tombstone.cancel_pending_side_effects is False
    assert tombstone.version == 1
    assert await email_runtime.repository.complete(delete_lease) is True
    deleted_at = await _scalar(
        email_runtime.pool,
        "SELECT source_deleted_at FROM emails WHERE id = %s",
        (tombstone.email_id,),
    )
    assert deleted_at is not None

    create = _event("late-create", external_email_id=external_email_id)
    create_lease = await _insert_and_claim(email_runtime, create)
    late = await email_runtime.repository.apply_email_event(create_lease)
    row = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, create_seen_at, source_deleted_at "
        "FROM emails WHERE id = %s",
        (tombstone.email_id,),
    )

    assert late.disposition is EmailEventDisposition.AGGREGATE_NOOP
    assert late.should_process is False
    assert late.may_complete_without_processing is True
    assert row == {
        "status": EmailStatus.CANCELLED.value,
        "version": 1,
        "create_seen_at": None,
        "source_deleted_at": deleted_at,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_create_delete_finishes_with_one_immutable_tombstone(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-create-delete-race"
    create = _event("race-create", external_email_id=external_email_id)
    delete = _event(
        "race-delete",
        kind=ChangeKind.DELETE,
        external_email_id=external_email_id,
    )
    create_receipt, delete_receipt = await asyncio.gather(
        email_runtime.repository.insert(create, 1, 1),
        email_runtime.repository.insert(delete, 1, 1),
    )
    leases = await email_runtime.repository.claim_batch(
        "create-delete-race",
        {"durable_v1"},
        limit=10,
        lease_seconds=60,
    )
    by_id = {lease.id: lease for lease in leases}

    create_result, delete_result = await asyncio.gather(
        email_runtime.repository.apply_email_event(by_id[create_receipt.inbox_id]),
        email_runtime.repository.apply_email_event(by_id[delete_receipt.inbox_id]),
    )
    before = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, create_seen_at, source_deleted_at, "
        "processing_inbox_id FROM emails "
        "WHERE account_id = 8 AND external_email_id = %s",
        (external_email_id,),
    )
    assert before is not None
    repeated = await email_runtime.repository.apply_email_event(
        by_id[delete_receipt.inbox_id]
    )
    after = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, create_seen_at, source_deleted_at, "
        "processing_inbox_id FROM emails "
        "WHERE account_id = 8 AND external_email_id = %s",
        (external_email_id,),
    )

    assert delete_result.should_cancel is True
    assert create_result.should_process in {True, False}
    assert before["status"] == EmailStatus.CANCELLED.value
    assert before["version"] in {1, 2}
    assert before["source_deleted_at"] is not None
    assert before["processing_inbox_id"] is None
    assert repeated.disposition is EmailEventDisposition.AGGREGATE_NOOP
    assert after == before


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_deletes_after_create_cancel_once_and_keep_deletion_fact(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-create-then-delete"
    create_lease = await _insert_and_claim(
        email_runtime,
        _event("create-before-delete", external_email_id=external_email_id),
    )
    created = await email_runtime.repository.apply_email_event(create_lease)

    deletes = [
        _event(
            f"delete-after-{index}",
            kind=ChangeKind.DELETE,
            external_email_id=external_email_id,
        )
        for index in range(2)
    ]
    receipts = await asyncio.gather(
        *(email_runtime.repository.insert(event, 1, 1) for event in deletes)
    )
    leases = await email_runtime.repository.claim_batch(
        "delete-workers",
        {"durable_v1"},
        limit=10,
        lease_seconds=60,
    )
    by_id = {lease.id: lease for lease in leases}
    results = await asyncio.gather(
        *(
            email_runtime.repository.apply_email_event(by_id[receipt.inbox_id])
            for receipt in receipts
        )
    )
    row = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, source_deleted_at, processing_inbox_id "
        "FROM emails WHERE id = %s",
        (created.email_id,),
    )

    assert sum(result.should_cancel for result in results) == 1
    assert {result.disposition for result in results} == {
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventDisposition.AGGREGATE_NOOP,
    }
    assert row is not None
    assert row["status"] == EmailStatus.CANCELLED.value
    assert row["version"] == 2
    assert row["source_deleted_at"] is not None
    assert row["processing_inbox_id"] is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_exact_create_replay_after_distinct_delete_cancellation_fails_closed(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-create-replay-after-delete"
    create_lease = await _insert_and_claim(
        email_runtime,
        _event("create-before-distinct-delete", external_email_id=external_email_id),
    )
    created = await email_runtime.repository.apply_email_event(create_lease)
    assert created.should_process is True
    delete_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "distinct-delete-before-create-replay",
            kind=ChangeKind.DELETE,
            external_email_id=external_email_id,
        ),
        worker="distinct-delete-worker",
    )
    assert delete_lease.id != create_lease.id

    deleted = await email_runtime.repository.apply_email_event(delete_lease)

    assert deleted.should_cancel is True
    assert deleted.persisted_status is EmailStatus.CANCELLED
    assert await _fetchone(
        email_runtime.pool,
        "SELECT status, processing_inbox_id FROM emails WHERE id = %s",
        (created.email_id,),
    ) == {
        "status": EmailStatus.CANCELLED.value,
        "processing_inbox_id": None,
    }
    inbox_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM event_inbox WHERE id = %s",
        (create_lease.id,),
    )
    email_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM emails WHERE id = %s",
        (created.email_id,),
    )
    receipt_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM audit_events WHERE email_id = %s "
        "AND action = 'email.processing_attempt'",
        (created.email_id,),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await email_runtime.repository.apply_email_event(create_lease)

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM event_inbox WHERE id = %s",
            (create_lease.id,),
        )
        == inbox_before
    )
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM emails WHERE id = %s",
            (created.email_id,),
        )
        == email_before
    )
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM audit_events WHERE email_id = %s "
            "AND action = 'email.processing_attempt'",
            (created.email_id,),
        )
        == receipt_before
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_same_active_and_renewed_attempt_authorizes_processing_once(
    email_runtime: _EmailRuntime,
) -> None:
    lease = await _insert_and_claim(email_runtime, _event("same-attempt"))

    creator = await email_runtime.repository.apply_email_event(lease)
    sequential = await email_runtime.repository.apply_email_event(lease)
    concurrent = await asyncio.gather(
        email_runtime.repository.apply_email_event(lease),
        email_runtime.repository.apply_email_event(lease),
    )
    renewed = await email_runtime.repository.renew(lease, lease_seconds=120)
    assert renewed is not None
    renewed_result = await email_runtime.repository.apply_email_event(renewed)

    assert creator.disposition is EmailEventDisposition.CREATOR_ELECTED
    for duplicate in (sequential, *concurrent, renewed_result):
        assert duplicate.disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED
        assert duplicate.should_process is False
        assert duplicate.may_complete_without_processing is False
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'email.processing_attempt'",
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reaper_incremented_attempt_resumes_once(
    email_runtime: _EmailRuntime,
) -> None:
    lease = await _insert_and_claim(email_runtime, _event("reclaimed-attempt"))
    first = await email_runtime.repository.apply_email_event(lease)
    assert first.version == 1
    await _execute(
        email_runtime.pool,
        "UPDATE event_inbox SET lease_until = received_at + INTERVAL '1 microsecond' "
        "WHERE id = %s",
        (lease.id,),
    )
    assert await email_runtime.repository.recover_expired_leases(10) == 1
    await _execute(
        email_runtime.pool,
        "UPDATE event_inbox SET available_at = pg_catalog.clock_timestamp() "
        "WHERE id = %s",
        (lease.id,),
    )
    reclaimed = (
        await email_runtime.repository.claim_batch(
            "reclaimed-worker",
            {"durable_v1"},
            limit=10,
            lease_seconds=60,
        )
    )[0]
    assert reclaimed.id == lease.id
    assert reclaimed.attempts == 1

    first, second = await asyncio.gather(
        email_runtime.repository.apply_email_event(reclaimed),
        email_runtime.repository.apply_email_event(reclaimed),
    )
    repeated = await asyncio.gather(
        email_runtime.repository.apply_email_event(reclaimed),
        email_runtime.repository.apply_email_event(reclaimed),
    )

    assert sum(result.should_process for result in (first, second)) == 1
    resumed = next(result for result in (first, second) if result.should_process)
    duplicate = next(result for result in (first, second) if not result.should_process)
    assert resumed.disposition is EmailEventDisposition.PROCESSING_RESUMED
    assert resumed.version == 3
    for result in (duplicate, *repeated):
        assert result.disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED
        assert result.may_complete_without_processing is False
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'email.processing_attempt'",
        )
        == 2
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_wait_reclaimed_attempt_returns_to_processing_and_clears_error(
    email_runtime: _EmailRuntime,
) -> None:
    lease = await _insert_and_claim(email_runtime, _event("retry-wait-resume"))
    created = await email_runtime.repository.apply_email_event(lease)
    failed = await email_runtime.repository.finish_email_processing_failure(
        lease,
        created.email_id,
        created.version,
        DatabaseOperationError(
            operation="synthetic.retry_wait",
            retryable=True,
            message="Synthetic retry wait",
        ),
    )
    assert failed.email_status is EmailStatus.RETRY_WAIT
    await _execute(
        email_runtime.pool,
        "UPDATE event_inbox SET available_at = pg_catalog.clock_timestamp() "
        "WHERE id = %s",
        (lease.id,),
    )
    reclaimed = (
        await email_runtime.repository.claim_batch(
            "retry-wait-worker",
            {"durable_v1"},
            limit=10,
            lease_seconds=60,
        )
    )[0]

    resumed = await email_runtime.repository.apply_email_event(reclaimed)
    row = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, processing_inbox_id, safe_error_code, "
        "safe_error_summary FROM emails WHERE id = %s",
        (created.email_id,),
    )

    assert resumed.disposition is EmailEventDisposition.PROCESSING_RESUMED
    assert resumed.should_process is True
    assert row == {
        "status": EmailStatus.PROCESSING.value,
        "version": 3,
        "processing_inbox_id": UUID(lease.id),
        "safe_error_code": None,
        "safe_error_summary": None,
    }
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE email_id = %s AND action = 'email.processing_attempt'",
            (created.email_id,),
        )
        == 2
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expired_exact_lease_and_reaper_race_roll_back_new_shell(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event("expired-lease")
    lease = await _insert_and_claim(email_runtime, event)
    expired_until = lease.received_at + timedelta(microseconds=1)
    await _execute(
        email_runtime.pool,
        "UPDATE event_inbox SET lease_until = %s WHERE id = %s",
        (expired_until, lease.id),
    )
    expired = replace(lease, lease_until=expired_until)

    applied, recovered = await asyncio.gather(
        email_runtime.repository.apply_email_event(expired),
        email_runtime.repository.recover_expired_leases(10),
        return_exceptions=True,
    )

    assert isinstance(applied, StaleFence)
    # The reaper deliberately uses SKIP LOCKED. If the stale applier locks the
    # Inbox row first, this pass may make no progress; the applier then rolls
    # back and the next bounded pass must reclaim the expired lease.
    assert recovered in {0, 1}
    if recovered == 0:
        recovered = await email_runtime.repository.recover_expired_leases(10)
    assert recovered == 1
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails "
            "WHERE account_id = %s AND external_email_id = %s",
            (event.account_id, event.external_email_id),
        )
        == 0
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_same_external_id_isolated_across_accounts(
    email_runtime: _EmailRuntime,
) -> None:
    await email_runtime.ownership.bootstrap(9, "durable_v1")
    external_email_id = "synthetic-shared-external-id"
    events = [
        _event(
            f"account-{account_id}",
            account_id=account_id,
            external_email_id=external_email_id,
        )
        for account_id in (8, 9)
    ]
    receipts = await asyncio.gather(
        *(email_runtime.repository.insert(event, 1, 1) for event in events)
    )
    leases = await email_runtime.repository.claim_batch(
        "multi-account-worker",
        {"durable_v1"},
        limit=10,
        lease_seconds=60,
    )
    by_id = {lease.id: lease for lease in leases}

    applications = await asyncio.gather(
        *(
            email_runtime.repository.apply_email_event(by_id[receipt.inbox_id])
            for receipt in receipts
        )
    )

    assert all(application.should_process for application in applications)
    assert len({application.email_id for application in applications}) == 2
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails WHERE external_email_id = %s",
            (external_email_id,),
        )
        == 2
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_metadata_projection_is_sticky_and_never_advances_business_version(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-projection-ordering"
    create_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "projection-create",
            external_email_id=external_email_id,
            payload={"is_read": False},
        ),
    )
    created = await email_runtime.repository.apply_email_event(create_lease)
    assert created.version == 1

    known_true = await _insert_and_claim(
        email_runtime,
        _event(
            "projection-known-true",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
            folder="Archive",
            payload={"is_read": True},
        ),
    )
    first_projection = await email_runtime.repository.apply_email_event(known_true)
    assert first_projection.disposition is EmailEventDisposition.AGGREGATE_UPDATED
    assert first_projection.version == 1
    assert await email_runtime.repository.complete(known_true) is True
    row = await _fetchone(
        email_runtime.pool,
        "SELECT source_folder_key, is_read, is_read_refresh_required, version "
        "FROM emails WHERE id = %s",
        (created.email_id,),
    )
    assert row == {
        "source_folder_key": "Archive",
        "is_read": True,
        "is_read_refresh_required": False,
        "version": 1,
    }

    ambiguous_false = await _insert_and_claim(
        email_runtime,
        _event(
            "projection-ambiguous-false",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
            folder="Archive",
            payload={"is_read": False},
        ),
    )
    second_projection = await email_runtime.repository.apply_email_event(
        ambiguous_false
    )
    assert second_projection.version == 1
    assert await email_runtime.repository.complete(ambiguous_false) is True

    known_read = await _insert_and_claim(
        email_runtime,
        _event(
            "projection-read",
            kind=ChangeKind.READ,
            external_email_id=external_email_id,
            folder="Archive",
            payload={"is_read": False},
        ),
    )
    read_projection = await email_runtime.repository.apply_email_event(known_read)
    assert read_projection.version == 1
    row = await _fetchone(
        email_runtime.pool,
        "SELECT source_folder_key, is_read, is_read_refresh_required, version "
        "FROM emails WHERE id = %s",
        (created.email_id,),
    )
    assert row == {
        "source_folder_key": "Archive",
        "is_read": True,
        "is_read_refresh_required": True,
        "version": 1,
    }

    known_update = await _insert_and_claim(
        email_runtime,
        _event(
            "projection-known-update",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
            folder="Archive",
            payload={"is_read": True},
        ),
    )
    update_projection = await email_runtime.repository.apply_email_event(known_update)
    assert update_projection.disposition is EmailEventDisposition.AGGREGATE_NOOP
    assert await _fetchone(
        email_runtime.pool,
        "SELECT is_read, is_read_refresh_required, version FROM emails WHERE id = %s",
        (created.email_id,),
    ) == {
        "is_read": True,
        "is_read_refresh_required": True,
        "version": 1,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_waiting_approval_projection_does_not_advance_business_version(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-waiting-approval-projection"
    create_lease = await _insert_and_claim(
        email_runtime,
        _event("waiting-approval-create", external_email_id=external_email_id),
    )
    created = await email_runtime.repository.apply_email_event(create_lease)
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET status = 'waiting_approval', version = 2, "
        "processing_inbox_id = NULL WHERE id = %s",
        (created.email_id,),
    )
    update_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "waiting-approval-update",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
            folder="ApprovalFolder",
            payload={"is_read": True},
        ),
    )

    application = await email_runtime.repository.apply_email_event(update_lease)
    row = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, source_folder_key, is_read FROM emails WHERE id = %s",
        (created.email_id,),
    )

    assert application.disposition is EmailEventDisposition.AGGREGATE_UPDATED
    assert application.version == 2
    assert row == {
        "status": EmailStatus.WAITING_APPROVAL.value,
        "version": 2,
        "source_folder_key": "ApprovalFolder",
        "is_read": True,
    }


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("shell_kind", [ChangeKind.UPDATE, ChangeKind.READ])
async def test_delayed_create_agreement_preserves_sticky_refresh_requirement(
    email_runtime: _EmailRuntime,
    shell_kind: ChangeKind,
) -> None:
    external_email_id = f"synthetic-sticky-create-{shell_kind.value}"
    shell_lease = await _insert_and_claim(
        email_runtime,
        _event(
            f"sticky-shell-{shell_kind.value}",
            kind=shell_kind,
            external_email_id=external_email_id,
            folder="StableFolder",
            payload={"is_read": True},
        ),
    )
    shell = await email_runtime.repository.apply_email_event(shell_lease)
    assert shell.disposition is EmailEventDisposition.METADATA_SHELL_CREATED
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET is_read_refresh_required = true WHERE id = %s",
        (shell.email_id,),
    )
    assert await email_runtime.repository.complete(shell_lease) is True
    create_lease = await _insert_and_claim(
        email_runtime,
        _event(
            f"sticky-create-{shell_kind.value}",
            external_email_id=external_email_id,
            folder="StableFolder",
            payload={"is_read": True},
        ),
    )

    created = await email_runtime.repository.apply_email_event(create_lease)
    row = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, source_folder_key, is_read, "
        "is_read_refresh_required FROM emails WHERE id = %s",
        (shell.email_id,),
    )

    assert created.disposition is EmailEventDisposition.CREATOR_ELECTED
    assert row == {
        "status": EmailStatus.PROCESSING.value,
        "version": 1,
        "source_folder_key": "StableFolder",
        "is_read": True,
        "is_read_refresh_required": True,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unknown_read_shell_and_exact_metadata_noop_preserve_timestamp(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event(
        "unknown-read-shell",
        kind=ChangeKind.UPDATE,
        payload={"is_read": "not-a-boolean"},
    )
    lease = await _insert_and_claim(email_runtime, event)
    shell = await email_runtime.repository.apply_email_event(lease)
    before = await _fetchone(
        email_runtime.pool,
        "SELECT is_read, is_read_refresh_required, version, updated_at "
        "FROM emails WHERE id = %s",
        (shell.email_id,),
    )
    assert before is not None
    assert before["is_read"] is None
    assert before["is_read_refresh_required"] is True
    assert before["version"] == 0
    assert await email_runtime.repository.complete(lease) is True

    duplicate = await _insert_and_claim(
        email_runtime,
        _event(
            "unknown-read-shell-repeat",
            kind=ChangeKind.UPDATE,
            external_email_id=event.external_email_id,
            payload={"is_read": "still-not-a-boolean"},
        ),
    )
    noop = await email_runtime.repository.apply_email_event(duplicate)
    after = await _fetchone(
        email_runtime.pool,
        "SELECT is_read, is_read_refresh_required, version, updated_at "
        "FROM emails WHERE id = %s",
        (shell.email_id,),
    )
    assert noop.disposition is EmailEventDisposition.AGGREGATE_NOOP
    assert after == before


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_after_external_effect_start_only_records_source_fact(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-effect-started-delete"
    create_lease = await _insert_and_claim(
        email_runtime,
        _event("effect-started-create", external_email_id=external_email_id),
    )
    created = await email_runtime.repository.apply_email_event(create_lease)
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET external_effects_started_at = "
        "pg_catalog.clock_timestamp() WHERE id = %s",
        (created.email_id,),
    )
    delete_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "effect-started-delete",
            kind=ChangeKind.DELETE,
            external_email_id=external_email_id,
        ),
    )

    deleted = await email_runtime.repository.apply_email_event(delete_lease)
    row = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, processing_inbox_id, source_deleted_at "
        "FROM emails WHERE id = %s",
        (created.email_id,),
    )

    assert deleted.should_cancel is False
    assert deleted.cancel_pending_side_effects is False
    assert deleted.persisted_status is EmailStatus.PROCESSING
    assert row is not None
    assert row["status"] == EmailStatus.PROCESSING.value
    assert row["version"] == 2
    assert str(row["processing_inbox_id"]) == create_lease.id
    assert row["source_deleted_at"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_processing_election_succeeds_at_bigint_max_minus_two(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-version-budget-success"
    shell_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "version-shell-success",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
        ),
    )
    shell = await email_runtime.repository.apply_email_event(shell_lease)
    assert await email_runtime.repository.complete(shell_lease) is True
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET version = %s WHERE id = %s",
        (POSTGRES_BIGINT_MAX - 2, shell.email_id),
    )
    create_lease = await _insert_and_claim(
        email_runtime,
        _event("version-create-success", external_email_id=external_email_id),
    )

    application = await email_runtime.repository.apply_email_event(create_lease)
    replay = await email_runtime.repository.apply_email_event(create_lease)

    assert application.should_process is True
    assert application.version == POSTGRES_BIGINT_MAX - 1
    assert replay.disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED
    assert replay.version == POSTGRES_BIGINT_MAX - 1
    assert replay.should_process is False
    assert replay.may_complete_without_processing is False


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("version", [POSTGRES_BIGINT_MAX - 1, POSTGRES_BIGINT_MAX])
async def test_processing_election_rejects_exhausted_version_without_receipt(
    email_runtime: _EmailRuntime,
    version: int,
) -> None:
    external_email_id = f"synthetic-version-budget-{version}"
    shell_lease = await _insert_and_claim(
        email_runtime,
        _event(
            f"version-shell-{version}",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
        ),
    )
    shell = await email_runtime.repository.apply_email_event(shell_lease)
    assert await email_runtime.repository.complete(shell_lease) is True
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET version = %s WHERE id = %s",
        (version, shell.email_id),
    )
    create_lease = await _insert_and_claim(
        email_runtime,
        _event(f"version-create-{version}", external_email_id=external_email_id),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await email_runtime.repository.apply_email_event(create_lease)

    assert caught.value.operation == "email.processing_version_exhausted"
    assert caught.value.retryable is False
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'email.processing_attempt'",
        )
        == 0
    )
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT version FROM emails WHERE id = %s",
            (shell.email_id,),
        )
        == version
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ordinary_authority_change_rejects_bigint_max(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-ordinary-version-max"
    shell_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "ordinary-version-shell",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
        ),
    )
    shell = await email_runtime.repository.apply_email_event(shell_lease)
    assert await email_runtime.repository.complete(shell_lease) is True
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET version = %s WHERE id = %s",
        (POSTGRES_BIGINT_MAX, shell.email_id),
    )
    delete_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "ordinary-version-delete",
            kind=ChangeKind.DELETE,
            external_email_id=external_email_id,
        ),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await email_runtime.repository.apply_email_event(delete_lease)

    assert caught.value.operation == "email.version_exhausted"
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT source_deleted_at FROM emails WHERE id = %s",
            (shell.email_id,),
        )
        is None
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("status", [EmailStatus.MANUAL_REVIEW, EmailStatus.DEAD_LETTER])
async def test_manual_review_and_dead_letter_never_recover_from_ordinary_create(
    email_runtime: _EmailRuntime,
    status: EmailStatus,
) -> None:
    lease = await _insert_and_claim(
        email_runtime,
        _event(f"nonrecoverable-{status.value}"),
    )
    created = await email_runtime.repository.apply_email_event(lease)
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET status = %s, safe_error_code = 'email.synthetic', "
        "safe_error_summary = 'Synthetic bounded failure', version = version + 1 "
        "WHERE id = %s",
        (status.value, created.email_id),
    )
    assert await email_runtime.repository.complete(lease) is True
    distinct_lease = await _insert_and_claim(
        email_runtime,
        _event(
            f"nonrecoverable-distinct-{status.value}",
            external_email_id=lease.event.external_email_id,
        ),
        worker=f"nonrecoverable-distinct-{status.value}",
    )
    assert distinct_lease.id != lease.id
    email_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM emails WHERE id = %s",
        (created.email_id,),
    )
    receipt_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM audit_events WHERE email_id = %s "
        "AND action = 'email.processing_attempt'",
        (created.email_id,),
    )

    replay = await email_runtime.repository.apply_email_event(distinct_lease)

    assert replay.should_process is False
    assert replay.persisted_status is status
    assert replay.disposition is EmailEventDisposition.AGGREGATE_NOOP
    assert replay.may_complete_without_processing is True
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM emails WHERE id = %s",
            (created.email_id,),
        )
        == email_before
    )
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM audit_events WHERE email_id = %s "
            "AND action = 'email.processing_attempt'",
            (created.email_id,),
        )
        == receipt_before
    )
    assert await email_runtime.repository.complete(distinct_lease) is True


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("status", [EmailStatus.MANUAL_REVIEW, EmailStatus.DEAD_LETTER])
async def test_exact_authorized_attempt_with_nonprocessing_status_fails_closed(
    email_runtime: _EmailRuntime,
    status: EmailStatus,
) -> None:
    lease = await _insert_and_claim(
        email_runtime,
        _event(f"authorized-status-conflict-{status.value}"),
    )
    created = await email_runtime.repository.apply_email_event(lease)
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET status = %s, safe_error_code = 'email.synthetic', "
        "safe_error_summary = 'Synthetic bounded failure', version = version + 1 "
        "WHERE id = %s",
        (status.value, created.email_id),
    )
    inbox_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    email_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM emails WHERE id = %s",
        (created.email_id,),
    )
    receipt_before = await _fetchone(
        email_runtime.pool,
        "SELECT * FROM audit_events WHERE email_id = %s "
        "AND action = 'email.processing_attempt'",
        (created.email_id,),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await email_runtime.repository.apply_email_event(lease)

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM event_inbox WHERE id = %s",
            (lease.id,),
        )
        == inbox_before
    )
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM emails WHERE id = %s",
            (created.email_id,),
        )
        == email_before
    )
    assert (
        await _fetchone(
            email_runtime.pool,
            "SELECT * FROM audit_events WHERE email_id = %s "
            "AND action = 'email.processing_attempt'",
            (created.email_id,),
        )
        == receipt_before
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_bound_form_reuses_xid_then_rejects_new_transaction(
    email_runtime: _EmailRuntime,
) -> None:
    lease = await _insert_and_claim(email_runtime, _event("transaction-xid"))
    async with email_runtime.pool.connection() as connection:
        transaction = email_runtime.repository.transaction(connection)
        with pytest.raises(RuntimeError, match="transaction is required"):
            await transaction.apply_email_event(lease)
        async with connection.transaction():
            first = await transaction.apply_email_event(lease)
            repeated = await transaction.apply_email_event(lease)
        assert first.should_process is True
        assert repeated.disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED

        async with connection.transaction():
            with pytest.raises(StaleFence):
                await transaction.apply_email_event(lease)


class _RollbackOuterTransaction(RuntimeError):
    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_bound_form_leaves_no_change_after_outer_rollback(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event("transaction-rollback")
    lease = await _insert_and_claim(email_runtime, event)

    with pytest.raises(_RollbackOuterTransaction):
        async with email_runtime.pool.connection() as connection:
            async with connection.transaction():
                transaction = email_runtime.repository.transaction(connection)
                application = await transaction.apply_email_event(lease)
                assert application.should_process is True
                raise _RollbackOuterTransaction

    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails "
            "WHERE account_id = %s AND external_email_id = %s",
            (event.account_id, event.external_email_id),
        )
        == 0
    )
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'email.processing_attempt'",
        )
        == 0
    )


async def _configure_repeatable_read(connection) -> None:
    await connection.execute(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_bound_form_rejects_non_read_committed(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event("transaction-isolation")
    lease = await _insert_and_claim(email_runtime, event)
    repeatable_pool = AsyncConnectionPool(
        conninfo=email_runtime.schema.runtime_dsn,
        min_size=1,
        max_size=1,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
        configure=_configure_repeatable_read,
    )
    await repeatable_pool.open()
    try:
        async with repeatable_pool.connection() as connection:
            async with connection.transaction():
                transaction = email_runtime.repository.transaction(connection)
                with pytest.raises(RuntimeError, match="requires READ COMMITTED"):
                    await transaction.apply_email_event(lease)
    finally:
        await repeatable_pool.close()

    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails "
            "WHERE account_id = %s AND external_email_id = %s",
            (event.account_id, event.external_email_id),
        )
        == 0
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_new_emails_do_not_deadlock_upgrading_ownership_lock(
    email_runtime: _EmailRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [_event(f"ownership-upgrade-{index}") for index in range(2)]
    receipts = await asyncio.gather(
        *(email_runtime.repository.insert(event, 1, 1) for event in events)
    )
    leases = await email_runtime.repository.claim_batch(
        "ownership-upgrade-workers",
        {"durable_v1"},
        limit=10,
        lease_seconds=60,
    )
    by_id = {lease.id: lease for lease in leases}
    both_inserted = asyncio.Event()
    release = asyncio.Event()
    inserted_count = 0
    original_insert = EmailEventTransaction._insert_neutral_shell

    async def pause_after_insert(transaction, lease):
        nonlocal inserted_count
        result = await original_insert(transaction, lease)
        inserted_count += 1
        if inserted_count == 2:
            both_inserted.set()
        await release.wait()
        return result

    monkeypatch.setattr(
        EmailEventTransaction,
        "_insert_neutral_shell",
        pause_after_insert,
    )
    tasks = [
        asyncio.create_task(
            email_runtime.repository.apply_email_event(by_id[receipt.inbox_id])
        )
        for receipt in receipts
    ]
    await asyncio.wait_for(both_inserted.wait(), timeout=2)
    release.set()
    results = await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True),
        timeout=5,
    )

    assert all(not isinstance(result, BaseException) for result in results), results
    assert all(result.should_process is True for result in results)
    assert all(result.persisted_status is EmailStatus.PROCESSING for result in results)
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM emails WHERE status = 'processing'",
        )
        == 2
    )
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE action = 'email.processing_attempt'",
        )
        == 2
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_shared_barrier_serializes_concurrent_quiesce(
    email_runtime: _EmailRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = await _insert_and_claim(email_runtime, _event("quiesce-barrier"))
    entered = asyncio.Event()
    release = asyncio.Event()
    original_insert = EmailEventTransaction._insert_neutral_shell

    async def paused_insert(transaction, locked_lease):
        entered.set()
        await release.wait()
        return await original_insert(transaction, locked_lease)

    monkeypatch.setattr(
        EmailEventTransaction,
        "_insert_neutral_shell",
        paused_insert,
    )
    apply_task = asyncio.create_task(email_runtime.repository.apply_email_event(lease))
    await asyncio.wait_for(entered.wait(), timeout=1)
    quiesce_task = asyncio.create_task(
        email_runtime.ownership.quiesce(
            8,
            1,
            1,
            "test",
            "serialize with email apply",
        )
    )
    try:
        await _wait_for_advisory_waiter(email_runtime.pool)
        assert quiesce_task.done() is False
        assert (
            await _scalar(
                email_runtime.pool,
                "SELECT state FROM pipeline_ownership "
                "WHERE account_id = 8 AND generation = 1",
            )
            == PipelineGenerationState.CURRENT_INGRESS.value
        )
    finally:
        release.set()

    application, quiesced = await asyncio.gather(apply_task, quiesce_task)
    assert application.should_process is True
    assert quiesced.state is PipelineGenerationState.QUIESCING


@pytest.mark.integration
@pytest.mark.asyncio
async def test_email_shared_barrier_serializes_concurrent_retire(
    email_runtime: _EmailRuntime,
) -> None:
    await _promote_generation_two(email_runtime)
    retirement = PipelineOwnershipRepository(
        email_runtime.pool,
        retirement_guard=_AllowRetirement(),
    )
    async with email_runtime.pool.connection() as connection:
        async with connection.transaction():
            await email_runtime.repository._acquire_account_lock(connection, 8)
            retire_task = asyncio.create_task(
                retirement.retire(
                    8,
                    1,
                    1,
                    "test",
                    "serialize with email data plane",
                )
            )
            await _wait_for_advisory_waiter(email_runtime.pool)
            assert retire_task.done() is False
            assert (
                await _scalar(
                    email_runtime.pool,
                    "SELECT state FROM pipeline_ownership "
                    "WHERE account_id = 8 AND generation = 1",
                )
                == PipelineGenerationState.DRAINING.value
            )

    retired = await retire_task
    assert retired.state is PipelineGenerationState.RETIRED


@pytest.mark.integration
@pytest.mark.asyncio
async def test_every_persisted_lease_field_is_exactly_revalidated(
    email_runtime: _EmailRuntime,
) -> None:
    event = _event("forged-every-field", payload={"is_read": False})
    lease = await _insert_and_claim(email_runtime, event)
    await email_runtime.ownership.bootstrap(9, "durable_v1")
    await _promote_generation_two(email_runtime)
    one_microsecond = timedelta(microseconds=1)
    forged_account_event = replace(event, account_id=9)
    variants = {
        "id": replace(lease, id=str(uuid4())),
        "account_id": replace(
            lease,
            account_id=9,
            event=forged_account_event,
        ),
        "pipeline_name": replace(lease, pipeline_name="forged_pipeline"),
        "generation": replace(
            lease,
            generation=2,
        ),
        "fencing_token": replace(lease, fencing_token=2),
        "lease_owner": replace(lease, lease_owner="forged-worker"),
        "attempts": replace(lease, attempts=lease.attempts + 1),
        "received_at": replace(
            lease,
            received_at=lease.received_at + one_microsecond,
        ),
        "lease_until": replace(
            lease,
            lease_until=lease.lease_until + timedelta(seconds=1),
        ),
        "source": replace(
            lease,
            event=replace(event, source=IngressSource.SYNC),
        ),
        "raw_event_type": replace(
            lease,
            event=replace(event, raw_event_type="ForgedSyntheticEvent"),
        ),
        "change_kind": replace(
            lease,
            event=replace(event, kind=ChangeKind.UPDATE),
        ),
        "external_email_id": replace(
            lease,
            event=replace(event, external_email_id="forged-external-id"),
        ),
        "folder": replace(
            lease,
            event=replace(event, folder="ForgedFolder"),
        ),
        "source_version": replace(
            lease,
            event=replace(event, source_version="forged-version"),
        ),
        "dedupe_key": replace(
            lease,
            event=replace(event, dedupe_key="f" * 64),
        ),
        "payload": replace(
            lease,
            event=replace(event, payload={"is_read": True, "forged": True}),
        ),
        "processing_policy": replace(
            lease,
            event=replace(event, processing_policy=ProcessingPolicy.ARCHIVE),
        ),
        "source_event_at": replace(
            lease,
            event=replace(
                event,
                source_event_at=event.source_event_at + timedelta(seconds=1),
            ),
        ),
    }

    for field, forged in variants.items():
        try:
            await email_runtime.repository.apply_email_event(forged)
        except StaleFence:
            pass
        except Exception as error:
            pytest.fail(f"{field} raised {type(error).__name__}: {error}")
        else:
            pytest.fail(f"{field} was accepted")
        assert (
            await _scalar(
                email_runtime.pool,
                "SELECT pg_catalog.count(*) FROM emails",
            )
            == 0
        ), field


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    [
        "account_id",
        "email_id",
        "object_type",
        "object_fingerprint",
        "action",
        "result",
        "actor",
        "reason",
        "execution_epoch",
        "attempts",
        "generation",
        "fencing_token",
        "safe_metadata_extra",
    ],
)
async def test_processing_receipt_collision_drift_rolls_back_email_transition(
    email_runtime: _EmailRuntime,
    drift: str,
) -> None:
    external_email_id = "synthetic-receipt-collision"
    shell_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "receipt-collision-shell",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
        ),
    )
    shell = await email_runtime.repository.apply_email_event(shell_lease)
    assert await email_runtime.repository.complete(shell_lease) is True
    create_lease = await _insert_and_claim(
        email_runtime,
        _event("receipt-collision-create", external_email_id=external_email_id),
    )
    event_key = hashlib.sha256(
        b"email-processing-attempt-v1\x00"
        + create_lease.id.encode("ascii")
        + b"\x000\x00"
        + str(create_lease.attempts).encode("ascii")
    ).hexdigest()
    fingerprint = hashlib.sha256(
        b"email-processing-attempt-object-v1\x00"
        + shell.email_id.encode("ascii")
        + b"\x00"
        + create_lease.id.encode("ascii")
    ).hexdigest()
    values: dict[str, object] = {
        "account_id": create_lease.account_id,
        "email_id": shell.email_id,
        "object_type": "email_processing_attempt",
        "object_fingerprint": fingerprint,
        "action": "email.processing_attempt",
        "result": "authorized",
        "actor": "inbox_repository",
        "reason": "email.processing_attempt_authorized",
        "safe_metadata": {
            "execution_epoch": 0,
            "attempts": create_lease.attempts,
            "generation": create_lease.generation,
            "fencing_token": create_lease.fencing_token,
        },
    }
    if drift == "account_id":
        values["account_id"] = 9
        values["email_id"] = None
    elif drift == "email_id":
        values["email_id"] = None
    elif drift == "object_type":
        values["object_type"] = "email_processing_attempt_drift"
    elif drift == "object_fingerprint":
        values["object_fingerprint"] = "f" * 64
    elif drift == "action":
        values["action"] = "email.processing_attempt_drift"
    elif drift == "result":
        values["result"] = "drifted"
    elif drift == "actor":
        values["actor"] = "drifted_actor"
    elif drift == "reason":
        values["reason"] = "email.processing_attempt_drift"
    else:
        metadata = dict(values["safe_metadata"])
        if drift == "safe_metadata_extra":
            metadata["extra"] = True
        else:
            metadata[drift] = 999
        values["safe_metadata"] = metadata
    await _execute(
        email_runtime.pool,
        "INSERT INTO audit_events ("
        "id, event_key, account_id, email_id, object_type, object_fingerprint, "
        "action, result, actor, reason, safe_metadata"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            str(uuid4()),
            event_key,
            values["account_id"],
            values["email_id"],
            values["object_type"],
            values["object_fingerprint"],
            values["action"],
            values["result"],
            values["actor"],
            values["reason"],
            Jsonb(values["safe_metadata"]),
        ),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await email_runtime.repository.apply_email_event(create_lease)

    assert caught.value.operation == "event_inbox_invariant"
    assert await _fetchone(
        email_runtime.pool,
        "SELECT status, version, create_seen_at, processing_inbox_id "
        "FROM emails WHERE id = %s",
        (shell.email_id,),
    ) == {
        "status": EmailStatus.INGESTED.value,
        "version": 0,
        "create_seen_at": None,
        "processing_inbox_id": None,
    }, drift


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata_field", "forged_value"),
    [("execution_epoch", False), ("attempts", True)],
)
async def test_processing_receipt_rejects_json_boolean_number_type_confusion(
    email_runtime: _EmailRuntime,
    metadata_field: str,
    forged_value: bool,
) -> None:
    external_email_id = f"synthetic-receipt-json-type-{metadata_field}"
    shell_lease = await _insert_and_claim(
        email_runtime,
        _event(
            f"receipt-json-shell-{metadata_field}",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
        ),
    )
    shell = await email_runtime.repository.apply_email_event(shell_lease)
    assert await email_runtime.repository.complete(shell_lease) is True
    create_lease = await _insert_and_claim(
        email_runtime,
        _event(
            f"receipt-json-create-{metadata_field}",
            external_email_id=external_email_id,
        ),
    )
    await _execute(
        email_runtime.pool,
        "UPDATE event_inbox SET lease_until = received_at + INTERVAL '1 microsecond' "
        "WHERE id = %s",
        (create_lease.id,),
    )
    # A committed neutral email relation makes the unlinked reaper skip rather
    # than reverse the global email -> ownership -> inbox lock order. This test
    # is about JSON receipt type exactness, so project the reclaimed attempt
    # explicitly after proving the safe skip.
    assert await email_runtime.repository.recover_expired_leases(10) == 0
    await _execute(
        email_runtime.pool,
        "UPDATE event_inbox SET status = 'retry_wait', lease_owner = NULL, "
        "lease_until = NULL, attempts = attempts + 1, "
        "available_at = pg_catalog.clock_timestamp(), "
        "safe_error_code = 'inbox.lease_expired', "
        "safe_error_summary = 'Inbox worker lease expired' WHERE id = %s",
        (create_lease.id,),
    )
    reclaimed = next(
        lease
        for lease in await email_runtime.repository.claim_batch(
            "receipt-json-reclaimed-worker",
            {"durable_v1"},
            limit=10,
            lease_seconds=60,
        )
        if lease.id == create_lease.id
    )
    assert reclaimed.attempts == 1
    await _execute(
        email_runtime.pool,
        "UPDATE emails SET status = 'processing', version = 1, "
        "processing_inbox_id = %s, "
        "create_seen_at = pg_catalog.clock_timestamp(), "
        "processing_started_at = pg_catalog.clock_timestamp() WHERE id = %s",
        (reclaimed.id, shell.email_id),
    )
    event_key = hashlib.sha256(
        b"email-processing-attempt-v1\x00"
        + reclaimed.id.encode("ascii")
        + b"\x000\x00"
        + str(reclaimed.attempts).encode("ascii")
    ).hexdigest()
    fingerprint = hashlib.sha256(
        b"email-processing-attempt-object-v1\x00"
        + shell.email_id.encode("ascii")
        + b"\x00"
        + reclaimed.id.encode("ascii")
    ).hexdigest()
    safe_metadata: dict[str, object] = {
        "execution_epoch": 0,
        "attempts": reclaimed.attempts,
        "generation": reclaimed.generation,
        "fencing_token": reclaimed.fencing_token,
    }
    safe_metadata[metadata_field] = forged_value
    await _execute(
        email_runtime.pool,
        "INSERT INTO audit_events ("
        "id, event_key, account_id, email_id, object_type, object_fingerprint, "
        "action, result, actor, reason, safe_metadata"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            str(uuid4()),
            event_key,
            reclaimed.account_id,
            shell.email_id,
            "email_processing_attempt",
            fingerprint,
            "email.processing_attempt",
            "authorized",
            "inbox_repository",
            "email.processing_attempt_authorized",
            Jsonb(safe_metadata),
        ),
    )

    with pytest.raises(DatabaseOperationError) as caught:
        await email_runtime.repository.apply_email_event(reclaimed)

    assert caught.value.operation == "event_inbox_invariant"
    assert (
        await _scalar(
            email_runtime.pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE event_key = %s AND action = 'email.processing_attempt'",
            (event_key,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("owner_state", ["draining", "retired"])
@pytest.mark.parametrize("kind", list(ChangeKind))
async def test_cross_generation_event_matrix(
    email_runtime: _EmailRuntime,
    owner_state: str,
    kind: ChangeKind,
) -> None:
    external_email_id = f"synthetic-cross-{owner_state}-{kind.value}"
    if owner_state == "draining" and kind is ChangeKind.CREATE:
        first_event = _event(
            f"cross-shell-{owner_state}-{kind.value}",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
            folder="OwnerFolder",
            payload={"is_read": False},
        )
        first_lease = await _insert_and_claim(email_runtime, first_event)
        initial = await email_runtime.repository.apply_email_event(first_lease)
        assert await email_runtime.repository.complete(first_lease) is True
    elif owner_state == "retired":
        first_event = _event(
            f"cross-terminal-{owner_state}-{kind.value}",
            kind=ChangeKind.UPDATE,
            external_email_id=external_email_id,
            folder="OwnerFolder",
            payload={"is_read": False},
        )
        first_lease = await _insert_and_claim(email_runtime, first_event)
        initial = await email_runtime.repository.apply_email_event(first_lease)
        assert await email_runtime.repository.complete(first_lease) is True
        await _execute(
            email_runtime.pool,
            "UPDATE emails SET status = 'sent', version = 1 WHERE id = %s",
            (initial.email_id,),
        )
    else:
        first_event = _event(
            f"cross-created-{owner_state}-{kind.value}",
            external_email_id=external_email_id,
            folder="OwnerFolder",
            payload={"is_read": False},
        )
        first_lease = await _insert_and_claim(email_runtime, first_event)
        initial = await email_runtime.repository.apply_email_event(first_lease)

    await _promote_generation_two(email_runtime)
    if owner_state == "retired":
        await _retire_generation_one(email_runtime)

    incoming_event = _event(
        f"cross-incoming-{owner_state}-{kind.value}",
        kind=kind,
        external_email_id=external_email_id,
        folder="IncomingFolder",
        payload={"is_read": True},
    )
    incoming_lease = await _insert_and_claim(
        email_runtime,
        incoming_event,
        worker=f"worker-{owner_state}-{kind.value}",
        generation=2,
        fencing_token=2,
    )

    if owner_state == "draining" and kind is ChangeKind.CREATE:
        with pytest.raises(ManualReviewRequired) as caught:
            await email_runtime.repository.apply_email_event(incoming_lease)
        assert caught.value.reason == "email.sticky_owner_mismatch"
        row = await _fetchone(
            email_runtime.pool,
            "SELECT status, version, owner_generation, owner_fencing_token "
            "FROM emails WHERE id = %s",
            (initial.email_id,),
        )
        assert row == {
            "status": EmailStatus.INGESTED.value,
            "version": 0,
            "owner_generation": 1,
            "owner_fencing_token": 1,
        }
        return

    application = await email_runtime.repository.apply_email_event(incoming_lease)
    row = await _fetchone(
        email_runtime.pool,
        "SELECT status, version, owner_generation, owner_fencing_token, "
        "source_folder_key, is_read, source_deleted_at "
        "FROM emails WHERE id = %s",
        (initial.email_id,),
    )
    assert row is not None
    assert row["owner_generation"] == 1
    assert row["owner_fencing_token"] == 1
    if kind is ChangeKind.CREATE:
        assert owner_state == "retired"
        assert application.disposition is EmailEventDisposition.AGGREGATE_NOOP
        assert row["status"] == EmailStatus.SENT.value
        assert row["version"] == 1
        assert row["source_folder_key"] == "OwnerFolder"
    elif kind in {ChangeKind.UPDATE, ChangeKind.READ}:
        assert application.disposition is EmailEventDisposition.AGGREGATE_UPDATED
        assert row["status"] == (
            EmailStatus.PROCESSING.value
            if owner_state == "draining"
            else EmailStatus.SENT.value
        )
        assert row["version"] == 1
        assert row["source_folder_key"] == "IncomingFolder"
        assert row["is_read"] is True
    else:
        assert row["source_deleted_at"] is not None
        assert row["version"] == 2
        if owner_state == "draining":
            assert application.should_cancel is True
            assert row["status"] == EmailStatus.CANCELLED.value
        else:
            assert application.should_cancel is False
            assert row["status"] == EmailStatus.SENT.value


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retired_unresolved_owner_rejects_cross_generation_delete(
    email_runtime: _EmailRuntime,
) -> None:
    external_email_id = "synthetic-retired-unresolved-delete"
    owner_lease = await _insert_and_claim(
        email_runtime,
        _event("retired-unresolved-create", external_email_id=external_email_id),
    )
    owner = await email_runtime.repository.apply_email_event(owner_lease)
    await _promote_generation_two(email_runtime)
    await _execute(
        email_runtime.pool,
        "UPDATE pipeline_ownership SET state = 'retired', "
        "reason = 'synthetic unresolved retirement' "
        "WHERE account_id = 8 AND generation = 1 AND state = 'draining'",
    )
    incoming_lease = await _insert_and_claim(
        email_runtime,
        _event(
            "retired-unresolved-delete",
            kind=ChangeKind.DELETE,
            external_email_id=external_email_id,
        ),
        worker="retired-unresolved-worker",
        generation=2,
        fencing_token=2,
    )

    with pytest.raises(ManualReviewRequired) as caught:
        await email_runtime.repository.apply_email_event(incoming_lease)

    assert caught.value.reason == "email.retired_owner_unresolved"
    assert await _fetchone(
        email_runtime.pool,
        "SELECT status, version, source_deleted_at FROM emails WHERE id = %s",
        (owner.email_id,),
    ) == {
        "status": EmailStatus.PROCESSING.value,
        "version": 1,
        "source_deleted_at": None,
    }
