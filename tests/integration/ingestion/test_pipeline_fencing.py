from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.domain.errors import StaleFence
from src.domain.email_state import PipelineGenerationState
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    IngressSource,
    NormalizedIngressEvent,
    ProcessingPolicy,
)
from src.ingestion.ownership import (
    PipelineOwnershipRepository,
    PipelineRetirementBlocked,
)


class _AllowRetirement:
    async def assert_ready(self, _connection, _generation) -> None:
        return None


@pytest_asyncio.fixture
async def ownership_runtime(postgres_database_factory):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    pool = AsyncConnectionPool(
        conninfo=schema.runtime_dsn,
        min_size=1,
        max_size=20,
        open=False,
    )
    await pool.open()
    try:
        yield schema, pool, PipelineOwnershipRepository(pool)
    finally:
        await pool.close()


def _lease(*, generation: int, fencing_token: int) -> InboxLease:
    now = datetime.now(UTC)
    event = NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id="message-1",
        folder="INBOX",
        source_version="version-1",
        dedupe_key="a" * 64,
        payload={"id": "message-1"},
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=now,
    )
    return InboxLease(
        id=str(uuid4()),
        account_id=8,
        pipeline_name="legacy_compat" if generation == 1 else "durable_v1",
        generation=generation,
        fencing_token=fencing_token,
        lease_owner="worker-1",
        attempts=1,
        event=event,
        received_at=now,
        lease_until=now + timedelta(minutes=5),
    )


async def _scalar(pool, statement: str, params: tuple[object, ...] = ()):
    async with pool.connection() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(statement, params)
            row = await cursor.fetchone()
    return row[0] if row else None


async def _handoff_to_generation_two(ownership, pool):
    old = await ownership.get(8, 1)
    if old is None:
        old = await ownership.bootstrap(8, "legacy_compat")
    if old.state is PipelineGenerationState.CURRENT_INGRESS:
        old = await ownership.quiesce(8, 1, 1, "test", "prepare handoff")
    assert old.state is PipelineGenerationState.QUIESCING
    async with pool.connection() as connection:
        async with connection.transaction():
            transaction = ownership.transaction(connection)
            locked = await transaction._lock_quiesced(8, 1, 1)
            await transaction._mark_draining(
                locked,
                actor="test",
                reason="handoff old generation",
            )
            new = await transaction._insert_current(
                account_id=8,
                pipeline_name="durable_v1",
                generation=2,
                fencing_token=2,
                actor="test",
                reason="handoff new generation",
            )
    return old, new


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_creates_generation_one_with_runtime_role(
    ownership_runtime,
) -> None:
    _schema, _pool, ownership = ownership_runtime

    generation = await ownership.bootstrap(8, "legacy_compat")

    assert generation.account_id == 8
    assert generation.generation == 1
    assert generation.fencing_token == 1
    assert generation.pipeline_name == "legacy_compat"
    assert generation.state is PipelineGenerationState.CURRENT_INGRESS
    assert await ownership.get(8, 1) == generation
    assert await ownership.current_ingress(8) == generation
    assert await ownership.next_generation(8) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ownership_supports_the_production_dict_row_pool(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    pool = AsyncConnectionPool(
        conninfo=schema.runtime_dsn,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    try:
        ownership = PipelineOwnershipRepository(pool)
        first = await ownership.bootstrap(8, "legacy_compat")
        assert await ownership.get(8, 1) == first
        assert await ownership.current_ingress(8) == first
        assert await ownership.next_generation(8) == 2
        await ownership.quiesce(8, 1, 1, "test", "prepare handoff")
        async with pool.connection() as connection:
            async with connection.transaction():
                transaction = ownership.transaction(connection)
                locked = await transaction._lock_quiesced(8, 1, 1)
                await transaction._mark_draining(
                    locked,
                    actor="test",
                    reason="handoff old generation",
                )
                second = await transaction._insert_current(
                    account_id=8,
                    pipeline_name="durable_v1",
                    generation=2,
                    fencing_token=2,
                    actor="test",
                    reason="handoff new generation",
                )
        assert await ownership.current_ingress(8) == second
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_bootstrap_is_idempotent(ownership_runtime) -> None:
    _schema, pool, ownership = ownership_runtime

    generations = await asyncio.gather(
        *(ownership.bootstrap(8, "legacy_compat") for _ in range(16))
    )

    assert all(generation == generations[0] for generation in generations)
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM pipeline_ownership WHERE account_id = %s",
            (8,),
        )
        == 1
    )
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE account_id = %s AND action = 'pipeline.bootstrap'",
            (8,),
        )
        == 1
    )
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM pipeline_ownership "
            "WHERE account_id = %s AND state = 'current_ingress'",
            (8,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_rejects_pipeline_conflict(ownership_runtime) -> None:
    _schema, _pool, ownership = ownership_runtime
    original = await ownership.bootstrap(8, "legacy_compat")

    with pytest.raises(StaleFence):
        await ownership.bootstrap(8, "durable_v1")

    assert await ownership.current_ingress(8) == original


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_after_quiesce_never_creates_new_current(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    old = await ownership.bootstrap(8, "legacy_compat")

    quiesced = await ownership.quiesce(
        account_id=8,
        expected_generation=old.generation,
        expected_fencing_token=old.fencing_token,
        actor="test",
        reason="prepare cutover",
    )

    assert quiesced.state is PipelineGenerationState.QUIESCING
    assert await ownership.current_ingress(8) is None
    assert await ownership.next_generation(8) == 2
    with pytest.raises(StaleFence):
        await ownership.bootstrap(8, "legacy_compat")
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM pipeline_ownership WHERE account_id = %s",
            (8,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_quiesce_same_command_is_idempotent(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    await ownership.bootstrap(8, "legacy_compat")

    results = await asyncio.gather(
        *(ownership.quiesce(8, 1, 1, "operator", "prepare cutover") for _ in range(16))
    )

    assert all(result.state is PipelineGenerationState.QUIESCING for result in results)
    assert await ownership.current_ingress(8) is None
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE account_id = %s AND action = 'pipeline.quiesce'",
            (8,),
        )
        == 1
    )

    replay = await ownership.quiesce(8, 1, 1, "other", "different replay")
    assert replay.state is PipelineGenerationState.QUIESCING
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE account_id = %s AND action = 'pipeline.quiesce'",
            (8,),
        )
        == 1
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("generation", "fencing_token"),
    [(2, 1), (1, 2)],
)
async def test_quiesce_rejects_stale_identity_without_mutation(
    ownership_runtime,
    generation: int,
    fencing_token: int,
) -> None:
    _schema, _pool, ownership = ownership_runtime
    current = await ownership.bootstrap(8, "legacy_compat")

    with pytest.raises(StaleFence):
        await ownership.quiesce(
            8,
            generation,
            fencing_token,
            "test",
            "stale request",
        )

    assert await ownership.current_ingress(8) == current


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_local_handoff_rolls_back_as_one_unit(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    await ownership.bootstrap(8, "legacy_compat")
    await ownership.quiesce(8, 1, 1, "test", "prepare handoff")
    audit_count_before = await _scalar(
        pool,
        "SELECT pg_catalog.count(*) FROM audit_events WHERE account_id = %s",
        (8,),
    )

    with pytest.raises(RuntimeError, match="inject rollback"):
        async with pool.connection() as connection:
            async with connection.transaction():
                transaction = ownership.transaction(connection)
                locked = await transaction._lock_quiesced(8, 1, 1)
                await transaction._mark_draining(
                    locked,
                    actor="test",
                    reason="handoff old generation",
                )
                await transaction._insert_current(
                    account_id=8,
                    pipeline_name="durable_v1",
                    generation=2,
                    fencing_token=2,
                    actor="test",
                    reason="handoff new generation",
                )
                raise RuntimeError("inject rollback")

    assert (await ownership.get(8, 1)).state is PipelineGenerationState.QUIESCING
    assert await ownership.get(8, 2) is None
    assert await ownership.current_ingress(8) is None
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM audit_events WHERE account_id = %s",
            (8,),
        )
        == audit_count_before
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_savepoint_rollback_cannot_leave_a_phantom_draining_authority(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    await ownership.bootstrap(8, "legacy_compat")
    await ownership.quiesce(8, 1, 1, "test", "prepare handoff")

    async with pool.connection() as connection:
        async with connection.transaction():
            transaction = ownership.transaction(connection)
            locked = await transaction._lock_quiesced(8, 1, 1)
            with pytest.raises(RuntimeError, match="rollback savepoint"):
                async with connection.transaction():
                    await transaction._mark_draining(
                        locked,
                        actor="test",
                        reason="handoff old generation",
                    )
                    raise RuntimeError("rollback savepoint")

            with pytest.raises(StaleFence):
                await transaction._insert_current(
                    account_id=8,
                    pipeline_name="durable_v1",
                    generation=2,
                    fencing_token=2,
                    actor="test",
                    reason="must revalidate draining state",
                )

    assert (await ownership.get(8, 1)).state is PipelineGenerationState.QUIESCING
    assert await ownership.get(8, 2) is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_handoff_object_cannot_split_steps_across_transactions(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    await ownership.bootstrap(8, "legacy_compat")
    await ownership.quiesce(8, 1, 1, "test", "prepare handoff")

    async with pool.connection() as connection:
        transaction = ownership.transaction(connection)
        async with connection.transaction():
            locked = await transaction._lock_quiesced(8, 1, 1)

        async with connection.transaction():
            with pytest.raises(StaleFence):
                await transaction._mark_draining(
                    locked,
                    actor="test",
                    reason="must remain in one transaction",
                )

    assert (await ownership.get(8, 1)).state is PipelineGenerationState.QUIESCING
    assert await ownership.get(8, 2) is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_local_handoff_requires_caller_transaction(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    await ownership.bootstrap(8, "legacy_compat")
    await ownership.quiesce(8, 1, 1, "test", "prepare handoff")

    async with pool.connection() as connection:
        transaction = ownership.transaction(connection)
        with pytest.raises(RuntimeError, match="transaction is required"):
            await transaction._lock_quiesced(8, 1, 1)

    assert (await ownership.get(8, 1)).state is PipelineGenerationState.QUIESCING


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_local_handoff_rejects_out_of_order_calls(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    await ownership.bootstrap(8, "legacy_compat")

    async with pool.connection() as connection:
        async with connection.transaction():
            transaction = ownership.transaction(connection)
            with pytest.raises(StaleFence):
                await transaction._lock_quiesced(8, 1, 1)

    await ownership.quiesce(8, 1, 1, "test", "prepare handoff")
    async with pool.connection() as connection:
        async with connection.transaction():
            transaction = ownership.transaction(connection)
            locked = await transaction._lock_quiesced(8, 1, 1)
            with pytest.raises(StaleFence):
                await transaction._lock_quiesced(8, 1, 1)
            with pytest.raises(StaleFence):
                await transaction._insert_current(
                    account_id=8,
                    pipeline_name="durable_v1",
                    generation=2,
                    fencing_token=2,
                    actor="test",
                    reason="insert before draining",
                )
            await transaction._mark_draining(
                locked,
                actor="test",
                reason="handoff old generation",
            )
            with pytest.raises(StaleFence):
                await transaction._mark_draining(
                    locked,
                    actor="test",
                    reason="repeat handoff",
                )
            with pytest.raises(StaleFence):
                await transaction._insert_current(
                    account_id=8,
                    pipeline_name="durable_v1",
                    generation=3,
                    fencing_token=2,
                    actor="test",
                    reason="skip generation",
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_local_handoff_success_is_atomic(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime

    old, new = await _handoff_to_generation_two(ownership, pool)

    assert (await ownership.get(8, old.generation)).state is (
        PipelineGenerationState.DRAINING
    )
    assert new.state is PipelineGenerationState.CURRENT_INGRESS
    assert await ownership.current_ingress(8) == new
    assert new.generation == old.generation + 1
    assert new.fencing_token == old.fencing_token + 1
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM audit_events WHERE account_id = %s "
            "AND action IN ('pipeline.mark_draining', 'pipeline.insert_current')",
            (8,),
        )
        == 2
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fence_state_matrix_and_default_retirement_guard(
    ownership_runtime,
) -> None:
    _schema, pool, ownership = ownership_runtime
    current = await ownership.bootstrap(8, "legacy_compat")
    assert await ownership.assert_fence(8, 1, 1) == current
    assert await ownership.can_execute(_lease(generation=1, fencing_token=1)) is True

    quiesced = await ownership.quiesce(8, 1, 1, "test", "prepare handoff")
    assert await ownership.assert_fence(8, 1, 1) == quiesced
    assert await ownership.can_execute(_lease(generation=1, fencing_token=1)) is True

    old, _new = await _handoff_to_generation_two(ownership, pool)
    draining = await ownership.get(8, old.generation)
    assert draining.state is PipelineGenerationState.DRAINING
    assert await ownership.assert_fence(8, 1, 1) == draining
    assert await ownership.can_execute(_lease(generation=1, fencing_token=1)) is True

    with pytest.raises(PipelineRetirementBlocked, match="retirement evidence"):
        await ownership.retire(8, 1, 1, "test", "retire old generation")
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE account_id = %s AND action = 'pipeline.retire'",
            (8,),
        )
        == 0
    )

    guarded = PipelineOwnershipRepository(
        pool,
        retirement_guard=_AllowRetirement(),
    )
    retired = await guarded.retire(8, 1, 1, "test", "retire old generation")
    assert retired.state is PipelineGenerationState.RETIRED
    assert (
        await _scalar(
            pool,
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE account_id = %s AND action = 'pipeline.retire'",
            (8,),
        )
        == 1
    )
    with pytest.raises(StaleFence):
        await ownership.assert_fence(8, 1, 1)
    assert await ownership.can_execute(_lease(generation=1, fencing_token=1)) is False
    with pytest.raises(StaleFence):
        await ownership.assert_fence(8, 1, 2)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_control_state_transitions_are_fenced(
    ownership_runtime,
) -> None:
    _schema, _pool, ownership = ownership_runtime
    await ownership.bootstrap(8, "legacy_compat")

    with pytest.raises(StaleFence):
        await ownership.retire(8, 1, 1, "test", "cannot retire current")

    await ownership.quiesce(8, 1, 1, "test", "prepare handoff")
    with pytest.raises(StaleFence):
        await ownership.quiesce(8, 1, 2, "test", "stale replay")


def _seed_inbox(schema, *, status: str) -> None:
    now = datetime.now(UTC)
    lease_owner = "worker-1" if status == "leased" else None
    lease_until = now + timedelta(minutes=5) if status == "leased" else None
    safe_error_code = (
        "test.blocked"
        if status in {"retry_wait", "dead_letter", "manual_review"}
        else None
    )
    schema.execute(
        "INSERT INTO event_inbox ("
        "id, account_id, external_email_id, folder_key, source, "
        "raw_event_type, change_kind, dedupe_key, payload, "
        "processing_policy, pipeline_name, generation, fencing_token, "
        "status, lease_owner, lease_until, safe_error_code) "
        "VALUES (%s, 8, %s, 'INBOX', 'webhook', 'NewMailEvent', "
        "'create', %s, '{}'::pg_catalog.jsonb, 'full', "
        "'legacy_compat', 1, 1, %s, %s, %s, %s)",
        (
            str(uuid4()),
            f"message-{status}",
            uuid4().hex.ljust(64, "0"),
            status,
            lease_owner,
            lease_until,
            safe_error_code,
        ),
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    ["pending", "retry_wait", "leased", "manual_review", "dead_letter"],
)
async def test_retire_blocks_unresolved_or_unaccounted_inbox_state(
    ownership_runtime,
    status: str,
) -> None:
    schema, pool, ownership = ownership_runtime
    await _handoff_to_generation_two(ownership, pool)
    _seed_inbox(schema, status=status)
    guarded = PipelineOwnershipRepository(
        pool,
        retirement_guard=_AllowRetirement(),
    )

    with pytest.raises(PipelineRetirementBlocked, match="unresolved work"):
        await guarded.retire(8, 1, 1, "test", "retire old generation")

    assert (await ownership.get(8, 1)).state is PipelineGenerationState.DRAINING


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["send_unknown", "dead_letter"])
async def test_retire_blocks_nonterminal_or_unaccounted_email_state(
    ownership_runtime,
    status: str,
) -> None:
    schema, pool, ownership = ownership_runtime
    await _handoff_to_generation_two(ownership, pool)
    email_id = f"message-{status}"
    processing_inbox_id = None
    safe_error_code = None
    if status == "dead_letter":
        processing_inbox_id = str(uuid4())
        schema.execute(
            "INSERT INTO event_inbox ("
            "id, account_id, external_email_id, folder_key, source, "
            "raw_event_type, change_kind, dedupe_key, payload, "
            "processing_policy, pipeline_name, generation, fencing_token, status"
            ") VALUES (%s, 8, %s, 'INBOX', 'webhook', 'NewMailEvent', "
            "'create', %s, '{}'::pg_catalog.jsonb, 'full', "
            "'legacy_compat', 1, 1, 'completed')",
            (
                processing_inbox_id,
                email_id,
                uuid4().hex.ljust(64, "0"),
            ),
        )
        safe_error_code = "test.dead_letter"
    schema.execute(
        "INSERT INTO emails ("
        "id, account_id, external_email_id, source_folder_key, status, "
        "owner_generation, owner_fencing_token, processing_inbox_id, "
        "safe_error_code, is_read"
        ") VALUES (%s, 8, %s, 'INBOX', %s, 1, 1, %s, %s, false)",
        (
            str(uuid4()),
            email_id,
            status,
            processing_inbox_id,
            safe_error_code,
        ),
    )
    guarded = PipelineOwnershipRepository(
        pool,
        retirement_guard=_AllowRetirement(),
    )

    with pytest.raises(PipelineRetirementBlocked, match="unresolved work"):
        await guarded.retire(8, 1, 1, "test", "retire old generation")

    assert (await ownership.get(8, 1)).state is PipelineGenerationState.DRAINING


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["send_failed", "delivery_failed"])
async def test_outcome_known_terminal_email_failure_does_not_block_retirement(
    ownership_runtime,
    status: str,
) -> None:
    schema, pool, ownership = ownership_runtime
    await _handoff_to_generation_two(ownership, pool)
    schema.execute(
        "INSERT INTO emails ("
        "id, account_id, external_email_id, source_folder_key, status, "
        "owner_generation, owner_fencing_token, is_read"
        ") VALUES (%s, 8, %s, 'INBOX', %s, 1, 1, false)",
        (str(uuid4()), f"message-{status}", status),
    )
    guarded = PipelineOwnershipRepository(
        pool,
        retirement_guard=_AllowRetirement(),
    )

    retired = await guarded.retire(8, 1, 1, "test", "retire old generation")

    assert retired.state is PipelineGenerationState.RETIRED
