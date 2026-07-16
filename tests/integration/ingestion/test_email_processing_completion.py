from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg.types.json import Jsonb

from src.domain.errors import DatabaseOperationError
from src.ingestion.email_events import EmailStatus
from src.ingestion.models import (
    ChangeKind,
    InboxStatus,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.processing import (
    ProcessingCompletion,
    ProcessingCompletionRejected,
    ProcessingReceiptConflict,
)


def _event(token: str) -> NormalizedIngressEvent:
    return NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id=f"task8-{token}",
        folder="INBOX",
        source_version=f"version-{token}",
        dedupe_key=hashlib.sha256(f"task8:{token}".encode()).hexdigest(),
        payload={"id": f"task8-{token}"},
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=datetime.now(UTC),
    )


async def _elect(runtime: Any, token: str):
    receipt = await runtime.repository.insert(_event(token), 1, 1)
    leases = await runtime.repository.claim_batch(
        "task8-worker",
        {"legacy_compat"},
        limit=10,
        lease_seconds=60,
    )
    lease = next(item for item in leases if item.id == receipt.inbox_id)
    application = await runtime.repository.apply_email_event(lease)
    assert application.should_process is True
    assert application.persisted_status is EmailStatus.PROCESSING
    return lease, application


async def _fetchone(runtime: Any, statement: str, params=()):
    async with runtime.pool.connection() as connection:
        cursor = await connection.execute(statement, params)
        return await cursor.fetchone()


async def _execute(runtime: Any, statement: str, params=()) -> None:
    async with runtime.pool.connection() as connection:
        await connection.execute(statement, params)


def _failure_result_event_key(inbox_id: str, attempts: int) -> str:
    return hashlib.sha256(
        b"email-processing-result-v1\x00"
        + inbox_id.encode("ascii")
        + b"\x000\x00"
        + str(attempts).encode("ascii")
        + b"\x00failure"
    ).hexdigest()


@pytest.mark.asyncio
async def test_effect_start_and_finish_are_atomic_and_exactly_replayable(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "finish")

    assert await runtime.repository.begin_processing_effect(
        lease,
        application.email_id,
        application.version,
    )
    marked = await _fetchone(
        runtime,
        "SELECT e.external_effects_started_at, i.effect_started_at "
        "FROM emails AS e JOIN event_inbox AS i "
        "ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert marked is not None
    assert marked["external_effects_started_at"] is not None
    assert marked["effect_started_at"] is not None

    completion = ProcessingCompletion.no_action()
    first = await runtime.repository.finish_email_processing(
        lease,
        application.email_id,
        application.version,
        completion,
    )
    replay = await runtime.repository.finish_email_processing(
        lease,
        application.email_id,
        application.version,
        completion,
    )

    assert first.email_status is EmailStatus.NO_ACTION
    assert first.inbox_status is InboxStatus.COMPLETED
    assert first.replayed is False
    assert replay.email_status is EmailStatus.NO_ACTION
    assert replay.inbox_status is InboxStatus.COMPLETED
    assert replay.replayed is True
    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "i.status AS inbox_status, i.lease_owner, i.lease_until "
        "FROM emails AS e JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (lease.id, application.email_id),
    )
    assert persisted == {
        "email_status": "no_action",
        "version": application.version + 1,
        "processing_inbox_id": None,
        "inbox_status": "completed",
        "lease_owner": None,
        "lease_until": None,
    }
    audit = await _fetchone(
        runtime,
        "SELECT pg_catalog.count(*) AS count, "
        "pg_catalog.min(pg_catalog.length(safe_metadata->>'lease_token_hash')) "
        "AS token_hash_length FROM audit_events "
        "WHERE email_id = %s AND action = 'email.processing_completed'",
        (application.email_id,),
    )
    assert audit == {"count": 1, "token_hash_length": 64}


@pytest.mark.asyncio
async def test_finish_rejects_changed_replay_and_stale_expected_version(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "finish-conflict")

    with pytest.raises(ProcessingCompletionRejected):
        await runtime.repository.finish_email_processing(
            lease,
            application.email_id,
            application.version - 1,
            ProcessingCompletion.no_action(),
        )
    current = await _fetchone(
        runtime,
        "SELECT status, version FROM emails WHERE id = %s",
        (application.email_id,),
    )
    assert current == {"status": "processing", "version": application.version}

    await runtime.repository.finish_email_processing(
        lease,
        application.email_id,
        application.version,
        ProcessingCompletion.no_action(),
    )
    with pytest.raises(ProcessingReceiptConflict):
        await runtime.repository.finish_email_processing(
            lease,
            application.email_id,
            application.version,
            ProcessingCompletion.notified_readonly(),
        )


@pytest.mark.asyncio
async def test_failure_updates_email_inbox_attempt_and_receipt_together(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "failure")
    failure = DatabaseOperationError(
        operation="dependency.call",
        retryable=True,
        message="unsafe details must not be persisted",
    )

    result = await runtime.repository.finish_email_processing_failure(
        lease,
        application.email_id,
        application.version,
        failure,
    )
    replay = await runtime.repository.finish_email_processing_failure(
        lease,
        application.email_id,
        application.version,
        failure,
    )

    assert result.email_status is EmailStatus.RETRY_WAIT
    assert result.inbox_status is InboxStatus.RETRY_WAIT
    assert result.replayed is False
    assert replay.email_status is EmailStatus.RETRY_WAIT
    assert replay.inbox_status is InboxStatus.RETRY_WAIT
    assert replay.replayed is True
    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "e.safe_error_code AS email_error, i.status AS inbox_status, "
        "i.attempts, i.safe_error_code AS inbox_error, i.available_at "
        "FROM emails AS e JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (lease.id, application.email_id),
    )
    assert persisted["email_status"] == "retry_wait"
    assert persisted["inbox_status"] == "retry_wait"
    assert persisted["version"] == application.version + 1
    assert persisted["processing_inbox_id"] == UUID(lease.id)
    assert persisted["attempts"] == lease.attempts + 1
    assert persisted["email_error"] == "inbox.database_transient"
    assert persisted["inbox_error"] == "inbox.database_transient"
    assert persisted["available_at"] is not None
    audit = await _fetchone(
        runtime,
        "SELECT action, result, reason, safe_metadata FROM audit_events "
        "WHERE email_id = %s AND action = 'email.processing_failed'",
        (application.email_id,),
    )
    assert audit is not None
    assert audit["result"] == "retry_wait"
    assert audit["reason"] == "inbox.database_transient"
    assert "unsafe details" not in str(audit["safe_metadata"])


@pytest.mark.asyncio
async def test_failure_replay_rejects_different_classification_for_same_key(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "failure-classification-conflict")
    await runtime.repository.finish_email_processing_failure(
        lease,
        application.email_id,
        application.version,
        DatabaseOperationError(
            operation="dependency.call",
            retryable=True,
            message="transient details",
        ),
    )

    with pytest.raises(ProcessingReceiptConflict):
        await runtime.repository.finish_email_processing_failure(
            lease,
            application.email_id,
            application.version,
            DatabaseOperationError(
                operation="dependency.call",
                retryable=False,
                message="permanent details",
            ),
        )

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.safe_error_code AS "
        "email_error, i.status AS inbox_status, i.attempts, "
        "i.safe_error_code AS inbox_error FROM emails AS e "
        "JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (lease.id, application.email_id),
    )
    assert persisted == {
        "email_status": "retry_wait",
        "version": application.version + 1,
        "email_error": "inbox.database_transient",
        "inbox_status": "retry_wait",
        "attempts": lease.attempts + 1,
        "inbox_error": "inbox.database_transient",
    }


@pytest.mark.asyncio
async def test_preseeded_failure_receipt_conflict_preserves_both_aggregates(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "failure-preseeded-conflict")
    event_key = _failure_result_event_key(lease.id, lease.attempts)
    await _execute(
        runtime,
        "INSERT INTO audit_events ("
        "id, event_key, account_id, email_id, object_type, object_fingerprint, "
        "action, result, actor, reason, safe_metadata"
        ") VALUES (%s, %s, %s, %s, 'email_processing_result', %s, "
        "'email.processing_failed', 'dead_letter', 'inbox_repository', "
        "'inbox.database_failure', %s)",
        (
            str(uuid4()),
            event_key,
            lease.account_id,
            application.email_id,
            "f" * 64,
            Jsonb({"tampered": True}),
        ),
    )

    with pytest.raises(ProcessingReceiptConflict):
        await runtime.repository.finish_email_processing_failure(
            lease,
            application.email_id,
            application.version,
            DatabaseOperationError(
                operation="dependency.call",
                retryable=True,
                message="must not escape",
            ),
        )

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "e.safe_error_code AS email_error, i.status AS inbox_status, "
        "i.attempts, i.lease_owner, i.lease_until, "
        "i.safe_error_code AS inbox_error FROM emails AS e "
        "JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (lease.id, application.email_id),
    )
    assert persisted == {
        "email_status": "processing",
        "version": application.version,
        "processing_inbox_id": UUID(lease.id),
        "email_error": None,
        "inbox_status": "leased",
        "attempts": lease.attempts,
        "lease_owner": lease.lease_owner,
        "lease_until": lease.lease_until,
        "inbox_error": None,
    }
    assert await _fetchone(
        runtime,
        "SELECT pg_catalog.count(*) AS count FROM audit_events WHERE event_key = %s",
        (event_key,),
    ) == {"count": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control_flow",
    [asyncio.CancelledError(), KeyboardInterrupt(), SystemExit()],
)
async def test_process_control_exceptions_leave_lease_for_reaper(
    durable_processing_runtime,
    control_flow: BaseException,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(
        runtime,
        f"control-{type(control_flow).__name__}",
    )

    with pytest.raises(type(control_flow)) as caught:
        await runtime.repository.finish_email_processing_failure(
            lease,
            application.email_id,
            application.version,
            control_flow,
        )
    assert caught.value is control_flow
    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, i.status AS inbox_status, "
        "i.attempts FROM emails AS e JOIN event_inbox AS i "
        "ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert persisted == {
        "email_status": "processing",
        "version": application.version,
        "inbox_status": "leased",
        "attempts": lease.attempts,
    }
    audit = await _fetchone(
        runtime,
        "SELECT pg_catalog.count(*) AS count FROM audit_events "
        "WHERE email_id = %s AND action = 'email.processing_failed'",
        (application.email_id,),
    )
    assert audit == {"count": 0}


@pytest.mark.asyncio
async def test_waiting_approval_version_exhaustion_dead_letters_both_aggregates(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "version-exhaustion")
    async with runtime.pool.connection() as connection:
        await connection.execute(
            "UPDATE emails SET version = %s WHERE id = %s",
            (POSTGRES_BIGINT_MAX - 1, application.email_id),
        )

    result = await runtime.repository.finish_email_processing(
        lease,
        application.email_id,
        POSTGRES_BIGINT_MAX - 1,
        ProcessingCompletion.waiting_approval(),
    )

    assert result.email_status is EmailStatus.DEAD_LETTER
    assert result.inbox_status is InboxStatus.DEAD_LETTER
    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "e.safe_error_code AS email_error, i.status AS inbox_status, "
        "i.safe_error_code AS inbox_error FROM emails AS e JOIN event_inbox AS i "
        "ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert persisted == {
        "email_status": "dead_letter",
        "version": POSTGRES_BIGINT_MAX,
        "processing_inbox_id": UUID(lease.id),
        "email_error": "email.version_exhausted_before_nonterminal_completion",
        "inbox_status": "dead_letter",
        "inbox_error": "email.version_exhausted_before_nonterminal_completion",
    }


@pytest.mark.asyncio
async def test_post_effect_waiting_approval_version_exhaustion_is_manual_review(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "post-effect-version-exhaustion")
    assert await runtime.repository.begin_processing_effect(
        lease,
        application.email_id,
        application.version,
    )
    async with runtime.pool.connection() as connection:
        await connection.execute(
            "UPDATE emails SET version = %s WHERE id = %s",
            (POSTGRES_BIGINT_MAX - 1, application.email_id),
        )

    result = await runtime.repository.finish_email_processing(
        lease,
        application.email_id,
        POSTGRES_BIGINT_MAX - 1,
        ProcessingCompletion.waiting_approval(),
    )

    assert result.email_status is EmailStatus.MANUAL_REVIEW
    assert result.inbox_status is InboxStatus.MANUAL_REVIEW
    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.safe_error_code AS "
        "email_error, i.status AS inbox_status, i.safe_error_code AS inbox_error, "
        "e.external_effects_started_at, i.effect_started_at FROM emails AS e "
        "JOIN event_inbox AS i ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert persisted["email_status"] == "manual_review"
    assert persisted["inbox_status"] == "manual_review"
    assert persisted["version"] == POSTGRES_BIGINT_MAX
    assert persisted["email_error"] == "inbox.effect_outcome_unknown"
    assert persisted["inbox_error"] == "inbox.effect_outcome_unknown"
    assert persisted["external_effects_started_at"] is not None
    assert persisted["effect_started_at"] is not None


@pytest.mark.asyncio
async def test_effect_start_is_idempotent_but_fails_closed_on_stale_version(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "effect-cas")

    assert not await runtime.repository.begin_processing_effect(
        lease,
        application.email_id,
        application.version - 1,
    )
    assert await runtime.repository.begin_processing_effect(
        lease,
        application.email_id,
        application.version,
    )
    first = await _fetchone(
        runtime,
        "SELECT e.external_effects_started_at, i.effect_started_at, "
        "e.updated_at AS email_updated_at, i.updated_at AS inbox_updated_at "
        "FROM emails AS e JOIN event_inbox AS i "
        "ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert await runtime.repository.begin_processing_effect(
        lease,
        application.email_id,
        application.version,
    )
    second = await _fetchone(
        runtime,
        "SELECT e.external_effects_started_at, i.effect_started_at, "
        "e.updated_at AS email_updated_at, i.updated_at AS inbox_updated_at "
        "FROM emails AS e JOIN event_inbox AS i "
        "ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert second == first


@pytest.mark.asyncio
async def test_effect_start_rejects_legacy_one_sided_marker(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "effect-one-sided")
    assert await runtime.repository.begin_effect(lease)

    assert not await runtime.repository.begin_processing_effect(
        lease,
        application.email_id,
        application.version,
    )
    persisted = await _fetchone(
        runtime,
        "SELECT e.external_effects_started_at, i.effect_started_at "
        "FROM emails AS e JOIN event_inbox AS i "
        "ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert persisted["external_effects_started_at"] is None
    assert persisted["effect_started_at"] is not None
