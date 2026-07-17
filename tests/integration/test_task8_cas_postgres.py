from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from psycopg_pool import AsyncConnectionPool

from src.safety.approval_claim import (
    claim_approval,
    claim_rejection,
    claim_send,
)
from src.utils.db_async import AsyncDatabaseManager


@pytest.fixture
def database_manager(
    migrated_postgres_pool: AsyncConnectionPool,
) -> AsyncDatabaseManager:
    """Bind production database methods to the isolated migrated test database."""
    manager = AsyncDatabaseManager(
        SimpleNamespace(database_url=migrated_postgres_pool.conninfo)
    )
    manager._pool = migrated_postgres_pool
    return manager


async def _insert_email(
    pool: AsyncConnectionPool,
    email_id: str,
    *,
    status: str,
    draft_content: str | None = None,
    error_message: str | None = None,
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO emails_log (id, status, draft_content, error_message)
                VALUES (%s, %s, %s, %s)
                """,
                (email_id, status, draft_content, error_message),
            )


async def _read_email(
    pool: AsyncConnectionPool,
    email_id: str,
) -> tuple[str, str | None, str | None]:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT status, draft_content, error_message
                FROM emails_log
                WHERE id = %s
                """,
                (email_id,),
            )
            row = await cur.fetchone()
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_concurrent_approval_claims_have_one_postgres_winner(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    email_id = "task8-concurrent-approval"
    await _insert_email(
        migrated_postgres_pool,
        email_id,
        status="waiting_approval",
    )

    outcomes = await asyncio.gather(
        *(
            claim_approval(email_id, f"approver-{index}", database_manager)
            for index in range(16)
        )
    )

    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 15
    assert (await _read_email(migrated_postgres_pool, email_id))[0] == "approved"


@pytest.mark.asyncio
async def test_approval_and_rejection_race_has_one_postgres_winner(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    email_id = "task8-approval-rejection-race"
    await _insert_email(
        migrated_postgres_pool,
        email_id,
        status="waiting_approval",
    )

    approval_won, rejection_won = await asyncio.gather(
        claim_approval(email_id, "approver", database_manager),
        claim_rejection(email_id, "rejector", database_manager),
    )

    assert sorted((approval_won, rejection_won)) == [False, True]
    persisted_status = (await _read_email(migrated_postgres_pool, email_id))[0]
    assert persisted_status == ("approved" if approval_won else "rejected")


@pytest.mark.asyncio
async def test_concurrent_send_claims_have_one_postgres_winner(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    email_id = "task8-concurrent-send"
    await _insert_email(migrated_postgres_pool, email_id, status="approved")

    outcomes = await asyncio.gather(
        *(claim_send(email_id, database_manager) for _ in range(16))
    )

    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 15
    assert (await _read_email(migrated_postgres_pool, email_id))[0] == "sending"


@pytest.mark.asyncio
async def test_save_draft_if_status_is_gated_by_waiting_approval(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    editable_id = "task8-editable-draft"
    approved_id = "task8-approved-draft"
    await _insert_email(
        migrated_postgres_pool,
        editable_id,
        status="waiting_approval",
        draft_content="original editable draft",
    )
    await _insert_email(
        migrated_postgres_pool,
        approved_id,
        status="waiting_approval",
        draft_content="approved immutable draft",
    )
    assert await claim_approval(approved_id, "approver", database_manager) is True

    editable_result, approved_result = await asyncio.gather(
        database_manager.save_draft_if_status(editable_id, "updated draft"),
        database_manager.save_draft_if_status(approved_id, "forbidden update"),
    )

    assert editable_result is True
    assert approved_result is False
    assert (await _read_email(migrated_postgres_pool, editable_id))[:2] == (
        "waiting_approval",
        "updated draft",
    )
    assert (await _read_email(migrated_postgres_pool, approved_id))[:2] == (
        "approved",
        "approved immutable draft",
    )


@pytest.mark.asyncio
async def test_startup_recovery_maps_incomplete_states_and_preserves_others(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    manual_recovery_cases = {
        "approved": "approval_handoff_incomplete",
        "saving_draft": "draft_save_outcome_unknown",
        "recovering": "self_healing_interrupted",
    }
    send_recovery_cases = {"sending": "send_outcome_unknown"}
    protected_statuses = {
        "waiting_approval",
        "rejected",
        "sent",
        "manual_review",
        "draft_saved",
        "send_unknown",
    }
    for status in (
        *manual_recovery_cases,
        *send_recovery_cases,
        *protected_statuses,
    ):
        await _insert_email(
            migrated_postgres_pool,
            f"task8-recovery-{status}",
            status=status,
            error_message=f"original-{status}",
        )

    affected = await database_manager.recover_incomplete_approval_states()

    assert affected == len(manual_recovery_cases) + len(send_recovery_cases)
    for initial_status, error_code in manual_recovery_cases.items():
        status, _, error_message = await _read_email(
            migrated_postgres_pool,
            f"task8-recovery-{initial_status}",
        )
        assert status == "manual_review"
        assert error_message == error_code
    for initial_status, error_code in send_recovery_cases.items():
        status, _, error_message = await _read_email(
            migrated_postgres_pool,
            f"task8-recovery-{initial_status}",
        )
        assert status == "send_unknown"
        assert error_message == error_code
    for protected_status in protected_statuses:
        status, _, error_message = await _read_email(
            migrated_postgres_pool,
            f"task8-recovery-{protected_status}",
        )
        assert status == protected_status
        assert error_message == f"original-{protected_status}"

    assert await database_manager.recover_incomplete_approval_states() == 0


@pytest.mark.asyncio
async def test_self_healing_claim_never_steals_manual_review_and_claims_error(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    manual_id = "task8-healing-manual"
    error_id = "task8-healing-error"
    await _insert_email(migrated_postgres_pool, manual_id, status="manual_review")
    await _insert_email(migrated_postgres_pool, error_id, status="error")

    manual_claimed, *error_claim_outcomes = await asyncio.gather(
        database_manager.claim_self_healing(
            manual_id,
            immediate=frozenset({"error", "delivery_failed"}),
            stale=frozenset({"pending", "ingested", "analyzed"}),
            stale_after_seconds=1800,
        ),
        database_manager.claim_self_healing(
            error_id,
            immediate=frozenset({"error", "delivery_failed"}),
            stale=frozenset({"pending", "ingested", "analyzed"}),
            stale_after_seconds=1800,
        ),
        database_manager.claim_self_healing(
            error_id,
            immediate=frozenset({"error", "delivery_failed"}),
            stale=frozenset({"pending", "ingested", "analyzed"}),
            stale_after_seconds=1800,
        ),
    )

    assert manual_claimed is False
    assert error_claim_outcomes.count(True) == 1
    assert error_claim_outcomes.count(False) == 1
    assert (await _read_email(migrated_postgres_pool, manual_id))[0] == "manual_review"
    assert (await _read_email(migrated_postgres_pool, error_id))[0] == "recovering"


@pytest.mark.asyncio
async def test_self_healing_never_reclaims_running_or_stale_recovering_rows(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    fresh_id = "task8-healing-fresh-recovering"
    stale_id = "task8-healing-stale-recovering"
    await _insert_email(migrated_postgres_pool, fresh_id, status="recovering")
    await _insert_email(migrated_postgres_pool, stale_id, status="recovering")
    async with migrated_postgres_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE emails_log
                SET updated_at = CURRENT_TIMESTAMP - INTERVAL '2 hours'
                WHERE id = %s
                """,
                (stale_id,),
            )

    fresh_claimed, *stale_claim_outcomes = await asyncio.gather(
        database_manager.claim_self_healing(
            fresh_id,
            immediate=frozenset({"error"}),
            stale=frozenset({"pending", "ingested", "analyzed"}),
            stale_after_seconds=1800,
        ),
        database_manager.claim_self_healing(
            stale_id,
            immediate=frozenset({"error"}),
            stale=frozenset({"pending", "ingested", "analyzed"}),
            stale_after_seconds=1800,
        ),
        database_manager.claim_self_healing(
            stale_id,
            immediate=frozenset({"error"}),
            stale=frozenset({"pending", "ingested", "analyzed"}),
            stale_after_seconds=1800,
        ),
    )

    assert fresh_claimed is False
    assert stale_claim_outcomes == [False, False]
    assert (await _read_email(migrated_postgres_pool, fresh_id))[0] == "recovering"
    assert (await _read_email(migrated_postgres_pool, stale_id))[0] == "recovering"


@pytest.mark.asyncio
async def test_manual_review_cas_cannot_overwrite_newer_terminal_state(
    migrated_postgres_pool: AsyncConnectionPool,
    database_manager: AsyncDatabaseManager,
) -> None:
    analyzed_id = "task8-manual-analyzed"
    error_id = "task8-manual-error"
    sent_id = "task8-manual-sent"
    await _insert_email(migrated_postgres_pool, analyzed_id, status="analyzed")
    await _insert_email(migrated_postgres_pool, error_id, status="error")
    await _insert_email(migrated_postgres_pool, sent_id, status="sent")
    expected = frozenset(
        {
            "pending",
            "recovering",
            "ingested",
            "analyzed",
            "drafted",
            "error",
            "delivery_failed",
        }
    )

    analyzed_won, error_won, sent_won = await asyncio.gather(
        database_manager.compare_and_set_manual_review(
            analyzed_id,
            expected=expected,
            error_code="reviewer_model_failed",
        ),
        database_manager.compare_and_set_manual_review(
            error_id,
            expected=expected,
            error_code="reviewer_model_failed",
        ),
        database_manager.compare_and_set_manual_review(
            sent_id,
            expected=expected,
            error_code="reviewer_model_failed",
        ),
    )

    assert analyzed_won is True
    assert error_won is True
    assert sent_won is False
    assert (await _read_email(migrated_postgres_pool, analyzed_id))[0] == (
        "manual_review"
    )
    assert (await _read_email(migrated_postgres_pool, error_id))[0] == (
        "manual_review"
    )
    assert (await _read_email(migrated_postgres_pool, sent_id))[0] == "sent"
