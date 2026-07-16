from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
import psycopg

from src.domain.errors import DatabaseOperationError
from src.ingestion.email_events import EmailStatus
from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)


def _event(token: str) -> NormalizedIngressEvent:
    return NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id=f"task8-linked-retry-{token}",
        folder="INBOX",
        source_version=f"version-{token}",
        dedupe_key=hashlib.sha256(f"task8-linked-retry:{token}".encode()).hexdigest(),
        payload={"id": f"task8-linked-retry-{token}"},
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=datetime.now(UTC),
    )


async def _execute(runtime: Any, statement: str, params=()) -> None:
    async with runtime.pool.connection() as connection:
        await connection.execute(statement, params)


async def _fetchone(runtime: Any, statement: str, params=()):
    async with runtime.pool.connection() as connection:
        cursor = await connection.execute(statement, params)
        return await cursor.fetchone()


async def _execute_as_migration(runtime: Any, statement: str, params=()) -> None:
    connection = await psycopg.AsyncConnection.connect(
        runtime.schema.dsn,
        autocommit=True,
    )
    try:
        await connection.execute(statement, params)
    finally:
        await connection.close()


async def _claim(runtime: Any, token: str, *, worker: str):
    receipt = await runtime.repository.insert(_event(token), 1, 1)
    leases = await runtime.repository.claim_batch(
        worker,
        {"legacy_compat"},
        limit=10,
        lease_seconds=60,
    )
    return next(lease for lease in leases if lease.id == receipt.inbox_id)


async def _elect(runtime: Any, token: str):
    lease = await _claim(runtime, token, worker="task8-first-worker")
    application = await runtime.repository.apply_email_event(lease)
    assert application.should_process is True
    assert application.persisted_status is EmailStatus.PROCESSING
    return lease, application


async def _reclaim_without_apply(runtime: Any, inbox_id: str):
    await _execute(
        runtime,
        "UPDATE event_inbox SET available_at = "
        "pg_catalog.clock_timestamp() - INTERVAL '1 second' WHERE id = %s",
        (inbox_id,),
    )
    leases = await runtime.repository.claim_batch(
        "task8-replacement-worker",
        {"legacy_compat"},
        limit=10,
        lease_seconds=60,
    )
    return next(lease for lease in leases if lease.id == inbox_id)


async def _expire(runtime: Any, inbox_id: str) -> None:
    await _execute(
        runtime,
        "UPDATE event_inbox SET lease_until = received_at + "
        "INTERVAL '1 microsecond' WHERE id = %s",
        (inbox_id,),
    )


@pytest.mark.asyncio
async def test_reaper_recovers_reclaimed_retry_before_apply_as_lease_expiry(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    first_lease, application = await _elect(runtime, "retry")
    first_result = await runtime.repository.finish_email_processing_failure(
        first_lease,
        application.email_id,
        application.version,
        DatabaseOperationError(
            operation="dependency.call",
            retryable=True,
            message="safe test failure",
        ),
    )
    assert first_result.email_status is EmailStatus.RETRY_WAIT

    reclaimed = await _reclaim_without_apply(runtime, first_lease.id)
    assert reclaimed.attempts == first_lease.attempts + 1
    assert await _fetchone(
        runtime,
        "SELECT pg_catalog.count(*) AS count FROM audit_events "
        "WHERE email_id = %s AND action = 'email.processing_attempt' "
        "AND safe_metadata->>'attempts' = %s",
        (application.email_id, str(reclaimed.attempts)),
    ) == {"count": 0}
    await _expire(runtime, reclaimed.id)

    assert await runtime.repository.recover_expired_leases(10) == 1

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "e.safe_error_code AS email_error, i.status AS inbox_status, "
        "i.attempts, i.safe_error_code AS inbox_error "
        "FROM emails AS e JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (reclaimed.id, application.email_id),
    )
    assert persisted == {
        "email_status": "retry_wait",
        "version": application.version + 2,
        "processing_inbox_id": UUID(reclaimed.id),
        "email_error": "inbox.lease_expired",
        "inbox_status": "retry_wait",
        "attempts": reclaimed.attempts + 1,
        "inbox_error": "inbox.lease_expired",
    }
    receipt = await _fetchone(
        runtime,
        "SELECT result, reason, safe_metadata->>'attempts' AS attempts "
        "FROM audit_events WHERE email_id = %s "
        "AND action = 'email.processing_failed' "
        "AND safe_metadata->>'attempts' = %s",
        (application.email_id, str(reclaimed.attempts)),
    )
    assert receipt == {
        "result": "retry_wait",
        "reason": "inbox.lease_expired",
        "attempts": str(reclaimed.attempts),
    }


@pytest.mark.asyncio
async def test_reaper_dead_letters_reclaimed_retry_when_version_budget_is_spent(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    first_lease, application = await _elect(runtime, "version-budget")
    await runtime.repository.finish_email_processing_failure(
        first_lease,
        application.email_id,
        application.version,
        DatabaseOperationError(
            operation="dependency.call",
            retryable=True,
            message="safe test failure",
        ),
    )
    reclaimed = await _reclaim_without_apply(runtime, first_lease.id)
    await _execute(
        runtime,
        "UPDATE emails SET version = %s WHERE id = %s",
        (POSTGRES_BIGINT_MAX - 2, application.email_id),
    )
    await _expire(runtime, reclaimed.id)

    assert await runtime.repository.recover_expired_leases(10) == 1

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.safe_error_code AS "
        "email_error, i.status AS inbox_status, i.attempts, "
        "i.safe_error_code AS inbox_error FROM emails AS e "
        "JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (reclaimed.id, application.email_id),
    )
    assert persisted == {
        "email_status": "dead_letter",
        "version": POSTGRES_BIGINT_MAX - 1,
        "email_error": "inbox.lease_expired",
        "inbox_status": "dead_letter",
        "attempts": reclaimed.attempts + 1,
        "inbox_error": "inbox.lease_expired",
    }


@pytest.mark.asyncio
async def test_reaper_fails_closed_for_processing_without_attempt_receipt(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease = await _claim(runtime, "missing-receipt", worker="task8-corrupt-worker")
    email_id = str(uuid4())
    await _execute_as_migration(
        runtime,
        "INSERT INTO emails ("
        "id, account_id, external_email_id, source_folder_key, status, version, "
        "owner_generation, owner_fencing_token, processing_inbox_id, "
        "create_seen_at, processing_started_at, is_read, "
        "is_read_refresh_required) VALUES ("
        "%s, %s, %s, %s, 'processing', 0, %s, %s, %s, "
        "pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp(), false, false)",
        (
            email_id,
            lease.account_id,
            lease.event.external_email_id,
            lease.event.folder,
            lease.generation,
            lease.fencing_token,
            lease.id,
        ),
    )
    await _expire(runtime, lease.id)

    assert await runtime.repository.recover_expired_leases(10) == 1

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.safe_error_code AS "
        "email_error, i.status AS inbox_status, i.attempts, "
        "i.safe_error_code AS inbox_error FROM emails AS e "
        "JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (lease.id, email_id),
    )
    assert persisted == {
        "email_status": "manual_review",
        "version": 1,
        "email_error": "inbox.stale_ownership",
        "inbox_status": "manual_review",
        "attempts": lease.attempts + 1,
        "inbox_error": "inbox.stale_ownership",
    }
