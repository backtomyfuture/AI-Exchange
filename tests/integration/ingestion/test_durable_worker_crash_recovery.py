from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from src.ingestion.email_events import EmailStatus
from src.domain.errors import StaleFence
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.processing import (
    ProcessingCompletion,
    ProcessingCompletionRejected,
    ProcessingFinishResult,
)
from src.ingestion.repository import InboxRepository


class _ProbedConnection:
    """Pause one real SQL statement while its transaction keeps acquired locks."""

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
        if matched and self._before is not None:
            self._before.set()
        cursor = await self._connection.execute(statement, params)
        if matched:
            self._matched = True
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
        return _ProbedConnectionContext(self._pool.connection(), **self._probe)


def _is_apply_processing_update(statement: str) -> bool:
    return (
        statement.lstrip().startswith("UPDATE")
        and "SET status = 'processing'" in statement
        and "processing_inbox_id = COALESCE" in statement
    )


def _is_reaper_candidate_scan(statement: str) -> bool:
    return (
        statement.lstrip().startswith("SELECT")
        and "WHERE e.status = 'leased'" in statement
        and "ORDER BY e.lease_until, e.id LIMIT" in statement
    )


def _is_reaper_inbox_update(statement: str) -> bool:
    return (
        statement.lstrip().startswith("UPDATE")
        and "attempts = %s, available_at" in statement
        and "lease_until <= pg_catalog.clock_timestamp()" in statement
    )


def _is_apply_inbox_lock(statement: str) -> bool:
    return (
        statement.lstrip().startswith("SELECT")
        and "e.status AS inbox_status" in statement
        and "AS lease_active" in statement
        and "FOR UPDATE" in statement
    )


def _is_processing_email_lock(statement: str) -> bool:
    return (
        statement.lstrip().startswith("SELECT")
        and "WHERE e.account_id = %s AND e.id = %s" in statement
        and statement.rstrip().endswith("FOR UPDATE")
    )


def _is_renew_update(statement: str) -> bool:
    return (
        statement.lstrip().startswith("UPDATE")
        and "SET lease_until = GREATEST(" in statement
        and "AND e.status = 'leased'" in statement
    )


def _is_effect_inbox_update(statement: str) -> bool:
    return (
        statement.lstrip().startswith("UPDATE")
        and "SET effect_started_at = COALESCE(" in statement
        and "AND status = 'leased'" in statement
    )


def _is_finish_inbox_update(statement: str) -> bool:
    return (
        statement.lstrip().startswith("UPDATE")
        and "SET status = %s, lease_owner = NULL" in statement
        and "AND status = 'leased' RETURNING id" in statement
        and "attempts = %s, available_at" not in statement
    )


def _event(token: str) -> NormalizedIngressEvent:
    return NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id=f"task8-reaper-{token}",
        folder="INBOX",
        source_version=f"version-{token}",
        dedupe_key=hashlib.sha256(f"task8-reaper:{token}".encode()).hexdigest(),
        payload={"id": f"task8-reaper-{token}"},
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=datetime.now(UTC),
    )


async def _claim(runtime: Any, token: str):
    receipt = await runtime.repository.insert(_event(token), 1, 1)
    leases = await runtime.repository.claim_batch(
        "task8-crashed-worker",
        {"legacy_compat"},
        limit=10,
        lease_seconds=60,
    )
    return next(item for item in leases if item.id == receipt.inbox_id)


async def _elect(runtime: Any, token: str):
    lease = await _claim(runtime, token)
    application = await runtime.repository.apply_email_event(lease)
    assert application.should_process is True
    assert application.persisted_status is EmailStatus.PROCESSING
    return lease, application


async def _execute(runtime: Any, statement: str, params=()) -> None:
    async with runtime.pool.connection() as connection:
        await connection.execute(statement, params)


async def _fetchone(runtime: Any, statement: str, params=()):
    async with runtime.pool.connection() as connection:
        cursor = await connection.execute(statement, params)
        return await cursor.fetchone()


async def _expire(runtime: Any, inbox_id: str) -> None:
    await _execute(
        runtime,
        "UPDATE event_inbox SET lease_until = received_at + "
        "INTERVAL '1 microsecond' WHERE id = %s",
        (inbox_id,),
    )


async def _expire_token(runtime: Any, lease: InboxLease) -> InboxLease:
    row = await _fetchone(
        runtime,
        "UPDATE event_inbox SET lease_until = received_at + "
        "INTERVAL '1 microsecond' WHERE id = %s RETURNING lease_until",
        (lease.id,),
    )
    assert row is not None
    return replace(lease, lease_until=row["lease_until"])


async def _shorten_token(runtime: Any, lease: InboxLease) -> InboxLease:
    row = await _fetchone(
        runtime,
        "UPDATE event_inbox SET lease_until = "
        "pg_catalog.clock_timestamp() + INTERVAL '250 milliseconds' "
        "WHERE id = %s RETURNING lease_until",
        (lease.id,),
    )
    assert row is not None
    return replace(lease, lease_until=row["lease_until"])


async def _wait_for_database_expiry(runtime: Any, lease: InboxLease) -> None:
    deadline = asyncio.get_running_loop().time() + 5
    while True:
        row = await _fetchone(
            runtime,
            "SELECT pg_catalog.clock_timestamp() >= lease_until AS expired "
            "FROM event_inbox WHERE id = %s",
            (lease.id,),
        )
        assert row is not None
        if row["expired"] is True:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("lease did not reach its database expiry boundary")
        await asyncio.sleep(0.01)


def _repository_with_probe(runtime: Any, **probe) -> InboxRepository:
    return InboxRepository(_ProbedPool(runtime.pool, **probe))


@pytest.mark.asyncio
async def test_linked_pre_effect_expiry_retries_both_aggregates(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "linked-pre-effect")
    await _expire(runtime, lease.id)

    assert await runtime.repository.recover_expired_leases(10) == 1

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "e.safe_error_code AS email_error, i.status AS inbox_status, "
        "i.attempts, i.safe_error_code AS inbox_error "
        "FROM emails AS e JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (lease.id, application.email_id),
    )
    assert persisted == {
        "email_status": "retry_wait",
        "version": application.version + 1,
        "processing_inbox_id": UUID(lease.id),
        "email_error": "inbox.lease_expired",
        "inbox_status": "retry_wait",
        "attempts": lease.attempts + 1,
        "inbox_error": "inbox.lease_expired",
    }


@pytest.mark.asyncio
async def test_linked_post_effect_expiry_moves_both_to_manual_review(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "linked-post-effect")
    assert await runtime.repository.begin_processing_effect(
        lease,
        application.email_id,
        application.version,
    )
    await _expire(runtime, lease.id)

    assert await runtime.repository.recover_expired_leases(10) == 1

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "e.safe_error_code AS email_error, i.status AS inbox_status, "
        "i.attempts, i.safe_error_code AS inbox_error, "
        "e.external_effects_started_at, i.effect_started_at "
        "FROM emails AS e JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
        (lease.id, application.email_id),
    )
    assert persisted["email_status"] == "manual_review"
    assert persisted["inbox_status"] == "manual_review"
    assert persisted["version"] == application.version + 1
    assert persisted["processing_inbox_id"] == UUID(lease.id)
    assert persisted["attempts"] == lease.attempts + 1
    assert persisted["email_error"] == "inbox.effect_outcome_unknown"
    assert persisted["inbox_error"] == "inbox.effect_outcome_unknown"
    assert persisted["external_effects_started_at"] is not None
    assert persisted["effect_started_at"] is not None


@pytest.mark.asyncio
async def test_unlinked_expiry_retains_inbox_only_recovery(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease = await _claim(runtime, "unlinked")
    await _expire(runtime, lease.id)

    assert await runtime.repository.recover_expired_leases(10) == 1

    inbox = await _fetchone(
        runtime,
        "SELECT status, attempts, safe_error_code FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert inbox == {
        "status": "retry_wait",
        "attempts": lease.attempts + 1,
        "safe_error_code": "inbox.lease_expired",
    }
    email_count = await _fetchone(
        runtime,
        "SELECT pg_catalog.count(*) AS count FROM emails "
        "WHERE account_id = %s AND external_email_id = %s",
        (lease.account_id, lease.event.external_email_id),
    )
    assert email_count == {"count": 0}


@pytest.mark.asyncio
async def test_unlinked_reaper_skips_when_any_email_relation_appeared(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease = await _claim(runtime, "relation-appeared")
    await _expire(runtime, lease.id)
    await _execute(
        runtime,
        "INSERT INTO emails (id, account_id, external_email_id, "
        "source_folder_key, status, owner_generation, owner_fencing_token, "
        "is_read, is_read_refresh_required) VALUES "
        "(%s, %s, %s, %s, 'ingested', %s, %s, NULL, true)",
        (
            str(uuid4()),
            lease.account_id,
            lease.event.external_email_id,
            lease.event.folder,
            lease.generation,
            lease.fencing_token,
        ),
    )

    assert await runtime.repository.recover_expired_leases(10) == 0
    inbox = await _fetchone(
        runtime,
        "SELECT status, lease_owner, attempts FROM event_inbox WHERE id = %s",
        (lease.id,),
    )
    assert inbox == {
        "status": "leased",
        "lease_owner": lease.lease_owner,
        "attempts": lease.attempts,
    }


@pytest.mark.asyncio
async def test_linked_reaper_dead_letters_when_retry_version_budget_is_spent(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    lease, application = await _elect(runtime, "version-budget")
    await _execute(
        runtime,
        "UPDATE emails SET version = %s WHERE id = %s",
        (POSTGRES_BIGINT_MAX - 2, application.email_id),
    )
    await _expire(runtime, lease.id)

    assert await runtime.repository.recover_expired_leases(10) == 1

    persisted = await _fetchone(
        runtime,
        "SELECT e.status AS email_status, e.version, e.processing_inbox_id, "
        "i.status AS inbox_status FROM emails AS e JOIN event_inbox AS i "
        "ON i.id = e.processing_inbox_id WHERE e.id = %s",
        (application.email_id,),
    )
    assert persisted == {
        "email_status": "dead_letter",
        "version": POSTGRES_BIGINT_MAX - 1,
        "processing_inbox_id": UUID(lease.id),
        "inbox_status": "dead_letter",
    }


@pytest.mark.asyncio
async def test_apply_vs_reaper_repeats_without_deadlock_or_split_state(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    for iteration in range(20):
        lease = await _claim(runtime, f"apply-reap-{iteration}")
        if iteration % 2 == 0:
            # Apply locks and elects while the token is still live. The reaper
            # then snapshots the expired candidate before Apply commits; its
            # next READ COMMITTED statement must see the newly linked email.
            lease = await _shorten_token(runtime, lease)
            apply_elected = asyncio.Event()
            release_apply = asyncio.Event()
            reaper_scanned = asyncio.Event()
            release_reaper = asyncio.Event()
            apply_repository = _repository_with_probe(
                runtime,
                match=_is_apply_processing_update,
                after=apply_elected,
                release=release_apply,
            )
            reaper_repository = _repository_with_probe(
                runtime,
                match=_is_reaper_candidate_scan,
                after=reaper_scanned,
                release=release_reaper,
            )

            apply_task = asyncio.create_task(apply_repository.apply_email_event(lease))
            await asyncio.wait_for(apply_elected.wait(), timeout=5)
            await _wait_for_database_expiry(runtime, lease)
            reaper_task = asyncio.create_task(
                reaper_repository.recover_expired_leases(1)
            )
            await asyncio.wait_for(reaper_scanned.wait(), timeout=5)
            assert not apply_task.done()
            assert not reaper_task.done()

            release_apply.set()
            application = await asyncio.wait_for(apply_task, timeout=5)
            assert application.should_process is True
            linked = await _fetchone(
                runtime,
                "SELECT status, processing_inbox_id FROM emails WHERE id = %s",
                (application.email_id,),
            )
            assert linked == {
                "status": "processing",
                "processing_inbox_id": UUID(lease.id),
            }
            release_reaper.set()
            assert await asyncio.wait_for(reaper_task, timeout=5) == 1

            persisted = await _fetchone(
                runtime,
                "SELECT i.status AS inbox_status, i.attempts, "
                "e.status AS email_status, e.processing_inbox_id "
                "FROM event_inbox AS i JOIN emails AS e "
                "ON e.account_id = i.account_id "
                "AND e.external_email_id = i.external_email_id "
                "WHERE i.id = %s",
                (lease.id,),
            )
            assert persisted == {
                "inbox_status": "retry_wait",
                "attempts": 1,
                "email_status": "retry_wait",
                "processing_inbox_id": UUID(lease.id),
            }
        else:
            # The inverse ordering holds the expired Inbox update open while
            # Apply reaches its exact Inbox lock. Apply must roll its tentative
            # email shell back after the reaper commits.
            lease = await _expire_token(runtime, lease)
            reaper_updated = asyncio.Event()
            release_reaper = asyncio.Event()
            apply_reached_inbox = asyncio.Event()
            reaper_repository = _repository_with_probe(
                runtime,
                match=_is_reaper_inbox_update,
                after=reaper_updated,
                release=release_reaper,
            )
            apply_repository = _repository_with_probe(
                runtime,
                match=_is_apply_inbox_lock,
                before=apply_reached_inbox,
            )

            reaper_task = asyncio.create_task(
                reaper_repository.recover_expired_leases(1)
            )
            await asyncio.wait_for(reaper_updated.wait(), timeout=5)
            apply_task = asyncio.create_task(apply_repository.apply_email_event(lease))
            await asyncio.wait_for(apply_reached_inbox.wait(), timeout=5)
            assert not reaper_task.done()
            assert not apply_task.done()

            release_reaper.set()
            assert await asyncio.wait_for(reaper_task, timeout=5) == 1
            with pytest.raises(StaleFence):
                await asyncio.wait_for(apply_task, timeout=5)
            persisted = await _fetchone(
                runtime,
                "SELECT i.status, i.attempts, pg_catalog.count(e.id) AS emails "
                "FROM event_inbox AS i LEFT JOIN emails AS e "
                "ON e.account_id = i.account_id "
                "AND e.external_email_id = i.external_email_id "
                "WHERE i.id = %s GROUP BY i.status, i.attempts",
                (lease.id,),
            )
            assert persisted == {
                "status": "retry_wait",
                "attempts": 1,
                "emails": 0,
            }


@pytest.mark.asyncio
async def test_renew_vs_effect_repeats_without_deadlock_or_one_sided_marker(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    for iteration in range(20):
        lease, application = await _elect(runtime, f"renew-effect-{iteration}")
        if iteration % 2 == 0:
            # Effect owns both aggregate rows; Renew has reached and is blocked
            # on the exact Inbox UPDATE until the dual marker commits.
            effect_inbox_updated = asyncio.Event()
            release_effect = asyncio.Event()
            renew_reached_update = asyncio.Event()
            effect_repository = _repository_with_probe(
                runtime,
                match=_is_effect_inbox_update,
                after=effect_inbox_updated,
                release=release_effect,
            )
            renew_repository = _repository_with_probe(
                runtime,
                match=_is_renew_update,
                before=renew_reached_update,
            )

            effect_task = asyncio.create_task(
                effect_repository.begin_processing_effect(
                    lease,
                    application.email_id,
                    application.version,
                )
            )
            await asyncio.wait_for(effect_inbox_updated.wait(), timeout=5)
            renew_task = asyncio.create_task(renew_repository.renew(lease, 60))
            await asyncio.wait_for(renew_reached_update.wait(), timeout=5)
            assert not effect_task.done()
            assert not renew_task.done()

            release_effect.set()
            effect_result = await asyncio.wait_for(effect_task, timeout=5)
            renew_result = await asyncio.wait_for(renew_task, timeout=5)
            assert effect_result is True
        else:
            # Renew holds the Inbox row while Effect holds the Email row. Once
            # Renew commits the rotated token, Effect's old token must fail
            # before either aggregate marker is written.
            effect_email_locked = asyncio.Event()
            release_effect = asyncio.Event()
            renew_updated = asyncio.Event()
            release_renew = asyncio.Event()
            effect_repository = _repository_with_probe(
                runtime,
                match=_is_processing_email_lock,
                after=effect_email_locked,
                release=release_effect,
            )
            renew_repository = _repository_with_probe(
                runtime,
                match=_is_renew_update,
                after=renew_updated,
                release=release_renew,
            )

            effect_task = asyncio.create_task(
                effect_repository.begin_processing_effect(
                    lease,
                    application.email_id,
                    application.version,
                )
            )
            await asyncio.wait_for(effect_email_locked.wait(), timeout=5)
            renew_task = asyncio.create_task(renew_repository.renew(lease, 60))
            await asyncio.wait_for(renew_updated.wait(), timeout=5)
            assert not effect_task.done()
            assert not renew_task.done()

            release_renew.set()
            renew_result = await asyncio.wait_for(renew_task, timeout=5)
            release_effect.set()
            effect_result = await asyncio.wait_for(effect_task, timeout=5)
            assert effect_result is False

        assert type(renew_result) is InboxLease
        markers = await _fetchone(
            runtime,
            "SELECT e.external_effects_started_at IS NOT NULL AS email_marked, "
            "i.effect_started_at IS NOT NULL AS inbox_marked "
            "FROM emails AS e JOIN event_inbox AS i "
            "ON i.id = e.processing_inbox_id WHERE e.id = %s",
            (application.email_id,),
        )
        assert markers["email_marked"] is markers["inbox_marked"]
        assert markers["email_marked"] is (iteration % 2 == 0)
        if not effect_result:
            assert await runtime.repository.begin_processing_effect(
                renew_result,
                application.email_id,
                application.version,
            )
        result = await runtime.repository.finish_email_processing(
            renew_result,
            application.email_id,
            application.version,
            ProcessingCompletion.no_action(),
        )
        assert type(result) is ProcessingFinishResult
        assert result.email_status is EmailStatus.NO_ACTION


@pytest.mark.asyncio
async def test_renew_vs_finish_repeats_with_one_legal_winner_and_no_deadlock(
    durable_processing_runtime,
) -> None:
    runtime = durable_processing_runtime
    for iteration in range(20):
        lease, application = await _elect(runtime, f"renew-finish-{iteration}")
        completion = ProcessingCompletion.no_action()
        if iteration % 2 == 0:
            # Finish owns Email and Inbox after both CAS updates; Renew reaches
            # the real Inbox UPDATE and waits for the terminal commit.
            finish_inbox_updated = asyncio.Event()
            release_finish = asyncio.Event()
            renew_reached_update = asyncio.Event()
            finish_repository = _repository_with_probe(
                runtime,
                match=_is_finish_inbox_update,
                after=finish_inbox_updated,
                release=release_finish,
            )
            renew_repository = _repository_with_probe(
                runtime,
                match=_is_renew_update,
                before=renew_reached_update,
            )

            finish_task = asyncio.create_task(
                finish_repository.finish_email_processing(
                    lease,
                    application.email_id,
                    application.version,
                    completion,
                )
            )
            await asyncio.wait_for(finish_inbox_updated.wait(), timeout=5)
            renew_task = asyncio.create_task(renew_repository.renew(lease, 60))
            await asyncio.wait_for(renew_reached_update.wait(), timeout=5)
            assert not finish_task.done()
            assert not renew_task.done()

            release_finish.set()
            finish_result = await asyncio.wait_for(finish_task, timeout=5)
            renew_result = await asyncio.wait_for(renew_task, timeout=5)
            assert type(finish_result) is ProcessingFinishResult
            assert renew_result is None
            assert finish_result.email_status is EmailStatus.NO_ACTION
        else:
            # Renew owns Inbox while Finish owns Email. Finish resumes only
            # after token rotation commits, so its stale finalization rejects.
            finish_email_locked = asyncio.Event()
            release_finish = asyncio.Event()
            renew_updated = asyncio.Event()
            release_renew = asyncio.Event()
            finish_repository = _repository_with_probe(
                runtime,
                match=_is_processing_email_lock,
                after=finish_email_locked,
                release=release_finish,
            )
            renew_repository = _repository_with_probe(
                runtime,
                match=_is_renew_update,
                after=renew_updated,
                release=release_renew,
            )

            finish_task = asyncio.create_task(
                finish_repository.finish_email_processing(
                    lease,
                    application.email_id,
                    application.version,
                    completion,
                )
            )
            await asyncio.wait_for(finish_email_locked.wait(), timeout=5)
            renew_task = asyncio.create_task(renew_repository.renew(lease, 60))
            await asyncio.wait_for(renew_updated.wait(), timeout=5)
            assert not finish_task.done()
            assert not renew_task.done()

            release_renew.set()
            renew_result = await asyncio.wait_for(renew_task, timeout=5)
            release_finish.set()
            with pytest.raises(ProcessingCompletionRejected):
                await asyncio.wait_for(finish_task, timeout=5)
            assert type(renew_result) is InboxLease
            finish_result = await runtime.repository.finish_email_processing(
                renew_result,
                application.email_id,
                application.version,
                completion,
            )
            assert type(finish_result) is ProcessingFinishResult
        persisted = await _fetchone(
            runtime,
            "SELECT e.status AS email_status, e.processing_inbox_id, "
            "i.status AS inbox_status, i.lease_owner FROM emails AS e "
            "JOIN event_inbox AS i ON i.id = %s WHERE e.id = %s",
            (lease.id, application.email_id),
        )
        assert persisted == {
            "email_status": "no_action",
            "processing_inbox_id": None,
            "inbox_status": "completed",
            "lease_owner": None,
        }
