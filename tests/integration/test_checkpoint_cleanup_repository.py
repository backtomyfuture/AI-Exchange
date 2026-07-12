from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import psycopg
import pytest
import ormsgpack
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.db.bootstrap import bootstrap_database
from src.maintenance.checkpoint_repository import (
    CheckpointRepositoryError,
    PostgresCheckpointRepository,
)


pytestmark = pytest.mark.asyncio
_DEFAULT_UPDATED_AT = object()


@pytest.fixture
async def checkpoint_schema(postgres_database_factory):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    return schema


def _old(hours: int = 72) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours)


async def _insert_email(
    dsn: str,
    thread_id: str,
    *,
    status: str | None = "sent",
    updated_at: datetime | None | object = _DEFAULT_UPDATED_AT,
) -> None:
    if updated_at is _DEFAULT_UPDATED_AT:
        updated_at = _old()
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            """
            INSERT INTO emails_log (id, status, updated_at)
            VALUES (
                %s,
                %s,
                CASE WHEN %s::timestamptz IS NULL THEN NULL
                     ELSE %s::timestamptz AT TIME ZONE current_setting('TimeZone')
                END
            )
            """,
            (thread_id, status, updated_at, updated_at),
        )


def _slim_state(
    thread_id: str,
    *,
    attachment_tokens: object = None,
    pdf_token: object = None,
) -> dict[str, object]:
    if attachment_tokens is None:
        attachment_tokens = []
    return {
        "email_id": thread_id,
        "content_ref": {
            "account_id": 8,
            "object_id": "00000000-0000-4000-8000-000000000127",
            "key_version": "v1",
            "sha256": "c" * 64,
        },
        "attachment_tokens": attachment_tokens,
        "pdf_token": pdf_token,
    }


async def _put_checkpoint(
    dsn: str,
    thread_id: str,
    *,
    state: dict[str, object] | None = None,
    checkpoint_ns: str = "",
    write_count: int = 0,
) -> None:
    checkpoint = empty_checkpoint()
    values = state if state is not None else _slim_state(thread_id)
    versions = {
        channel: f"{checkpoint['id']}:{index}"
        for index, channel in enumerate(values)
    }
    checkpoint["channel_values"] = values
    checkpoint["channel_versions"] = versions
    checkpoint["updated_channels"] = list(values)
    config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
        }
    }

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        saver = AsyncPostgresSaver(conn)
        next_config = await saver.aput(config, checkpoint, {}, versions)
        for index in range(write_count):
            await saver.aput_writes(
                next_config,
                [(f"write-{index}", {"index": index})],
                task_id=f"task-{index}",
            )


async def _valid_thread(
    dsn: str,
    thread_id: str,
    *,
    status: str = "sent",
    updated_at: datetime | None = None,
    checkpoints: int = 1,
    writes_per_checkpoint: int = 0,
) -> None:
    await _insert_email(
        dsn,
        thread_id,
        status=status,
        updated_at=updated_at or _old(),
    )
    for _ in range(checkpoints):
        await _put_checkpoint(
            dsn,
            thread_id,
            write_count=writes_per_checkpoint,
        )


async def _scan(
    dsn: str,
    *,
    cutoff: datetime | None = None,
    limit: int = 100,
    max_physical_rows: int = 500,
    max_estimated_logical_bytes: int = 64 * 1024 * 1024,
):
    return await PostgresCheckpointRepository(dsn).scan_candidates(
        cutoff=cutoff or _old(48),
        limit=limit,
        max_physical_rows=max_physical_rows,
        max_estimated_logical_bytes=max_estimated_logical_bytes,
    )


def _excluded(snapshot) -> dict[str, int]:
    return {bucket.reason: bucket.count for bucket in snapshot.excluded_buckets}


def _execution_plan(snapshot, *, expires_at: datetime | None = None):
    return SimpleNamespace(
        database_fingerprint=snapshot.database_fingerprint,
        database_timezone=snapshot.database_timezone,
        alembic_revision=snapshot.alembic_revision,
        checkpoint_revision=snapshot.checkpoint_revision,
        cutoff=snapshot.cutoff,
        expires_at=expires_at or (datetime.now(UTC) + timedelta(minutes=5)),
        candidates=snapshot.candidates,
    )


async def test_scan_selects_only_strictly_old_terminal_rows(checkpoint_schema):
    cutoff = _old(48)
    await _valid_thread(
        checkpoint_schema.dsn,
        "old-sent",
        updated_at=cutoff - timedelta(seconds=1),
    )
    await _insert_email(
        checkpoint_schema.dsn,
        "equal-cutoff",
        status="rejected",
        updated_at=cutoff,
    )
    await _insert_email(
        checkpoint_schema.dsn,
        "new-terminal",
        status="draft_saved",
        updated_at=cutoff + timedelta(seconds=1),
    )
    await _insert_email(
        checkpoint_schema.dsn,
        "waiting",
        status="waiting_approval",
        updated_at=cutoff - timedelta(days=1),
    )
    await _insert_email(
        checkpoint_schema.dsn,
        "unknown",
        status="future_status",
        updated_at=cutoff - timedelta(days=1),
    )
    await _insert_email(
        checkpoint_schema.dsn,
        "null-status",
        status=None,
        updated_at=cutoff - timedelta(days=1),
    )
    await _insert_email(
        checkpoint_schema.dsn,
        "null-updated",
        status="sent",
        updated_at=None,
    )

    snapshot = await _scan(checkpoint_schema.dsn, cutoff=cutoff)

    assert [candidate.thread_id for candidate in snapshot.candidates] == ["old-sent"]
    assert snapshot.candidates[0].updated_at.tzinfo is not None
    assert snapshot.alembic_revision == "20260710_0002"
    assert snapshot.checkpoint_revision == len(AsyncPostgresSaver.MIGRATIONS) - 1
    assert len(snapshot.database_fingerprint) == 64
    assert snapshot.database_timezone
    assert _excluded(snapshot)["status_not_terminal"] == 3
    assert _excluded(snapshot)["invalid_updated_at"] == 1
    assert _excluded(snapshot)["too_recent"] == 2


@pytest.mark.parametrize(
    "revision",
    ["20260710_0002", "20260710_0003"],
    ids=["code-first", "migration-first"],
)
async def test_scan_accepts_compatible_business_revision_and_reports_actual_value(
    checkpoint_schema,
    revision,
):
    checkpoint_schema.execute(
        "UPDATE alembic_version SET version_num = %s",
        (revision,),
    )
    await _valid_thread(checkpoint_schema.dsn, f"compatible-{revision}")

    snapshot = await _scan(checkpoint_schema.dsn)

    assert snapshot.alembic_revision == revision


@pytest.mark.parametrize(
    "revisions",
    [
        ("20260710_0001",),
        ("20260710_0004",),
        ("20260710_9999",),
        ("20260710_0002", "20260710_0003"),
    ],
    ids=["incompatible", "future-unaudited", "unknown", "multiple-heads"],
)
async def test_scan_rejects_incompatible_unknown_and_multiple_business_revisions(
    checkpoint_schema,
    revisions,
):
    checkpoint_schema.execute("DELETE FROM alembic_version")
    for revision in revisions:
        checkpoint_schema.execute(
            "INSERT INTO alembic_version (version_num) VALUES (%s)",
            (revision,),
        )

    with pytest.raises(CheckpointRepositoryError) as error:
        await _scan(checkpoint_schema.dsn)

    assert error.value.code == "cleanup_schema_revision_mismatch"
    assert str(error.value) == "cleanup_schema_revision_mismatch"


async def test_scan_accepts_langgraph_standard_zero_migration_marker(
    checkpoint_schema,
):
    checkpoint_schema.execute(
        "INSERT INTO checkpoint_migrations (v) VALUES (0)"
    )
    await _valid_thread(checkpoint_schema.dsn, "standard-setup-marker")

    snapshot = await _scan(checkpoint_schema.dsn)

    assert [candidate.thread_id for candidate in snapshot.candidates] == [
        "standard-setup-marker"
    ]
    assert snapshot.checkpoint_revision == len(AsyncPostgresSaver.MIGRATIONS) - 1


async def test_scan_rejects_checkpoint_migration_history_with_missing_version(
    checkpoint_schema,
):
    missing_version = max(1, (len(AsyncPostgresSaver.MIGRATIONS) - 1) // 2)
    checkpoint_schema.execute(
        "DELETE FROM checkpoint_migrations WHERE v = %s",
        (missing_version,),
    )

    with pytest.raises(CheckpointRepositoryError) as error:
        await _scan(checkpoint_schema.dsn)

    assert error.value.code == "cleanup_checkpoint_migrations_mismatch"
    assert str(error.value) == "cleanup_checkpoint_migrations_mismatch"


async def test_scan_rejects_checkpoint_migration_history_with_unknown_version(
    checkpoint_schema,
):
    unknown_version = len(AsyncPostgresSaver.MIGRATIONS)
    checkpoint_schema.execute(
        "INSERT INTO checkpoint_migrations (v) VALUES (%s)",
        (unknown_version,),
    )

    with pytest.raises(CheckpointRepositoryError) as error:
        await _scan(checkpoint_schema.dsn)

    assert error.value.code == "cleanup_checkpoint_migrations_mismatch"
    assert str(error.value) == "cleanup_checkpoint_migrations_mismatch"


async def test_scan_counts_each_physical_table_without_cartesian_multiplication(
    checkpoint_schema,
):
    await _valid_thread(
        checkpoint_schema.dsn,
        "multi-row",
        checkpoints=2,
        writes_per_checkpoint=1,
    )

    snapshot = await _scan(checkpoint_schema.dsn)
    candidate = snapshot.candidates[0]

    assert candidate.checkpoint_rows == 2
    assert candidate.checkpoint_blob_rows == 4
    assert candidate.checkpoint_write_rows == 2
    assert candidate.total_rows == 8
    assert candidate.checkpoint_bytes > 0
    assert candidate.checkpoint_blob_bytes > 0
    assert candidate.checkpoint_write_bytes > 0


async def test_scan_fails_closed_for_namespace_orphan_shape_and_cleanup_handles(
    checkpoint_schema,
):
    await _insert_email(checkpoint_schema.dsn, "non-default")
    await _put_checkpoint(
        checkpoint_schema.dsn,
        "non-default",
        checkpoint_ns="child",
    )

    await _insert_email(checkpoint_schema.dsn, "orphan")
    checkpoint_schema.execute(
        """
        INSERT INTO checkpoint_blobs
            (thread_id, checkpoint_ns, channel, version, type, blob)
        VALUES ('orphan', '', 'attachment_tokens', 'v1', 'msgpack', '\\x90')
        """
    )

    await _insert_email(checkpoint_schema.dsn, "legacy")
    await _put_checkpoint(
        checkpoint_schema.dsn,
        "legacy",
        state={"email_id": "legacy", "attachment_tokens": [], "pdf_token": None},
    )

    await _insert_email(checkpoint_schema.dsn, "remote-handles")
    await _put_checkpoint(
        checkpoint_schema.dsn,
        "remote-handles",
        state=_slim_state("remote-handles", attachment_tokens=["remote-token"]),
    )

    snapshot = await _scan(checkpoint_schema.dsn)

    assert snapshot.candidates == ()
    assert _excluded(snapshot)["non_default_namespace"] == 1
    assert _excluded(snapshot)["missing_checkpoint"] == 1
    assert _excluded(snapshot)["slim_state_unproven"] == 1
    assert _excluded(snapshot)["cleanup_handles_present"] == 1


@pytest.mark.parametrize("bad_type", ["pickle", "bytes", "bytearray", "mystery"])
async def test_scan_rejects_unsafe_cleanup_handle_serializers(
    checkpoint_schema,
    bad_type,
):
    thread_id = f"unsafe-{bad_type}"
    await _valid_thread(checkpoint_schema.dsn, thread_id)
    checkpoint_schema.execute(
        "UPDATE checkpoint_blobs SET type = %s "
        "WHERE thread_id = %s AND channel = 'attachment_tokens'",
        (bad_type, thread_id),
    )

    snapshot = await _scan(checkpoint_schema.dsn)

    assert snapshot.candidates == ()
    assert _excluded(snapshot)["slim_state_unproven"] == 1


async def test_scan_classifies_nonempty_inline_pdf_handle_as_present(
    checkpoint_schema,
):
    thread_id = "remote-pdf-handle"
    await _insert_email(checkpoint_schema.dsn, thread_id)
    await _put_checkpoint(
        checkpoint_schema.dsn,
        thread_id,
        state=_slim_state(thread_id, pdf_token="remote-pdf-token"),
    )

    snapshot = await _scan(checkpoint_schema.dsn)

    assert snapshot.candidates == ()
    assert _excluded(snapshot)["cleanup_handles_present"] == 1
    assert _excluded(snapshot)["slim_state_unproven"] == 0


async def test_scan_rejects_malformed_oversized_and_missing_current_blobs(
    checkpoint_schema,
):
    for thread_id in ("malformed", "oversized", "missing-content"):
        await _valid_thread(checkpoint_schema.dsn, thread_id)
    checkpoint_schema.execute(
        "UPDATE checkpoint_blobs SET blob = '\\xc1' "
        "WHERE thread_id = 'malformed' AND channel = 'attachment_tokens'"
    )
    checkpoint_schema.execute(
        "UPDATE checkpoint_blobs SET blob = repeat('00', 20000)::bytea "
        "WHERE thread_id = 'oversized' AND channel = 'attachment_tokens'"
    )
    checkpoint_schema.execute(
        "DELETE FROM checkpoint_blobs "
        "WHERE thread_id = 'missing-content' AND channel = 'content_ref'"
    )

    snapshot = await _scan(checkpoint_schema.dsn)

    assert snapshot.candidates == ()
    assert _excluded(snapshot)["slim_state_unproven"] == 3


async def test_scan_requires_inline_email_id_and_a_real_current_content_blob(
    checkpoint_schema,
):
    for thread_id in ("blob-email", "null-content", "oversize-content"):
        await _valid_thread(checkpoint_schema.dsn, thread_id)

    email_version = checkpoint_schema.scalar(
        "SELECT checkpoint -> 'channel_versions' ->> 'email_id' "
        "FROM checkpoints WHERE thread_id = 'blob-email'"
    )
    checkpoint_schema.execute(
        "UPDATE checkpoints SET checkpoint = jsonb_set("
        "checkpoint, '{channel_values}', "
        "(checkpoint -> 'channel_values') - 'email_id'::text"
        ") WHERE thread_id = 'blob-email'"
    )
    checkpoint_schema.execute(
        """
        INSERT INTO checkpoint_blobs
            (thread_id, checkpoint_ns, channel, version, type, blob)
        VALUES ('blob-email', '', 'email_id', %s, 'msgpack', %s)
        """,
        (email_version, ormsgpack.packb("blob-email")),
    )
    checkpoint_schema.execute(
        "UPDATE checkpoint_blobs SET blob = NULL "
        "WHERE thread_id = 'null-content' AND channel = 'content_ref'"
    )
    checkpoint_schema.execute(
        "UPDATE checkpoint_blobs SET blob = repeat('00', 20000)::bytea "
        "WHERE thread_id = 'oversize-content' AND channel = 'content_ref'"
    )

    snapshot = await _scan(checkpoint_schema.dsn)

    assert snapshot.candidates == ()
    assert _excluded(snapshot)["slim_state_unproven"] == 3


async def test_scan_estimated_bytes_include_out_of_line_write_payload(
    checkpoint_schema,
):
    await _valid_thread(
        checkpoint_schema.dsn,
        "logical-bytes",
        writes_per_checkpoint=1,
    )
    checkpoint_schema.execute(
        "UPDATE checkpoint_writes "
        "SET blob = decode(repeat('ab', 50000), 'hex') "
        "WHERE thread_id = 'logical-bytes'"
    )

    snapshot = await _scan(checkpoint_schema.dsn)

    assert snapshot.candidates[0].checkpoint_write_bytes >= 50_000


async def test_scan_enforces_per_thread_and_aggregate_physical_budgets(
    checkpoint_schema,
):
    await _valid_thread(checkpoint_schema.dsn, "first")
    await _valid_thread(checkpoint_schema.dsn, "second")

    unbounded = await _scan(checkpoint_schema.dsn)
    one_thread_rows = unbounded.candidates[0].total_rows

    aggregate_limited = await _scan(
        checkpoint_schema.dsn,
        max_physical_rows=one_thread_rows,
    )
    thread_limited = await _scan(
        checkpoint_schema.dsn,
        max_physical_rows=one_thread_rows - 1,
    )

    assert len(aggregate_limited.candidates) == 1
    assert _excluded(aggregate_limited)["plan_budget_exceeded"] == 1
    assert thread_limited.candidates == ()
    assert _excluded(thread_limited)["thread_budget_exceeded"] == 2


async def test_revalidate_returns_false_after_inventory_drift(checkpoint_schema):
    await _valid_thread(checkpoint_schema.dsn, "drift")
    repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    snapshot = await repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    candidate = snapshot.candidates[0]
    checkpoint_schema.execute(
        "UPDATE emails_log SET status = 'waiting_approval' WHERE id = 'drift'"
    )

    assert await repository.revalidate_candidate(candidate, plan=snapshot) is False


async def test_delete_candidate_removes_real_checkpoint_rows_only(checkpoint_schema):
    await _valid_thread(
        checkpoint_schema.dsn,
        "delete-me",
        checkpoints=2,
        writes_per_checkpoint=1,
    )
    await _valid_thread(
        checkpoint_schema.dsn,
        "keep-me",
        writes_per_checkpoint=1,
    )
    repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    snapshot = await repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    candidate = next(item for item in snapshot.candidates if item.thread_id == "delete-me")

    async with repository.execution_session(
        plan=_execution_plan(snapshot)
    ) as session:
        result = await session.delete_candidate(candidate)

    assert result.disposition == "deleted"
    assert result.deleted is True
    assert result.stale is False
    assert result.checkpoint_rows == candidate.checkpoint_rows
    assert result.checkpoint_blob_rows == candidate.checkpoint_blob_rows
    assert result.checkpoint_write_rows == candidate.checkpoint_write_rows
    assert result.estimated_logical_bytes == candidate.estimated_logical_bytes
    assert checkpoint_schema.scalar(
        "SELECT count(*) FROM emails_log WHERE id = 'delete-me'"
    ) == 1
    for table_name in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
        assert checkpoint_schema.scalar(
            f"SELECT count(*) FROM {table_name} WHERE thread_id = 'delete-me'"
        ) == 0
        assert checkpoint_schema.scalar(
            f"SELECT count(*) FROM {table_name} WHERE thread_id = 'keep-me'"
        ) > 0


async def test_delete_candidate_returns_stale_without_deleting(checkpoint_schema):
    await _valid_thread(checkpoint_schema.dsn, "stale")
    repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    snapshot = await repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    candidate = snapshot.candidates[0]
    checkpoint_schema.execute(
        "UPDATE checkpoint_blobs SET type = 'mystery' WHERE thread_id = 'stale'"
    )

    async with repository.execution_session(
        plan=_execution_plan(snapshot)
    ) as session:
        result = await session.delete_candidate(candidate)

    assert result.disposition == "stale"
    assert result.deleted is False
    assert result.stale is True
    assert result.checkpoint_rows == 0
    assert result.checkpoint_blob_rows == 0
    assert result.checkpoint_write_rows == 0
    assert result.estimated_logical_bytes == 0
    assert checkpoint_schema.scalar(
        "SELECT count(*) FROM checkpoints WHERE thread_id = 'stale'"
    ) == 1


async def test_delete_error_rolls_back_first_table_deletion(checkpoint_schema):
    await _valid_thread(checkpoint_schema.dsn, "rollback")
    repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    snapshot = await repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    candidate = snapshot.candidates[0]
    checkpoint_schema.execute(
        """
        CREATE FUNCTION fail_checkpoint_blob_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$
        """
    )
    checkpoint_schema.execute(
        """
        CREATE TRIGGER checkpoint_blob_delete_failure
        BEFORE DELETE ON checkpoint_blobs
        FOR EACH STATEMENT EXECUTE FUNCTION fail_checkpoint_blob_delete()
        """
    )

    with pytest.raises(CheckpointRepositoryError) as error:
        async with repository.execution_session(
            plan=_execution_plan(snapshot)
        ) as session:
            await session.delete_candidate(candidate)

    assert error.value.code == "cleanup_delete_failed"
    assert str(error.value) == "cleanup_delete_failed"
    assert checkpoint_schema.scalar(
        "SELECT count(*) FROM checkpoints WHERE thread_id = 'rollback'"
    ) == candidate.checkpoint_rows
    assert checkpoint_schema.scalar(
        "SELECT count(*) FROM checkpoint_blobs WHERE thread_id = 'rollback'"
    ) == candidate.checkpoint_blob_rows


async def test_execution_session_fails_closed_when_advisory_lock_is_held(
    checkpoint_schema,
):
    await _valid_thread(checkpoint_schema.dsn, "locked")
    first_repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    second_repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    snapshot = await first_repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )

    plan = _execution_plan(snapshot)
    async with first_repository.execution_session(plan=plan):
        with pytest.raises(CheckpointRepositoryError) as error:
            async with second_repository.execution_session(plan=plan):
                pass

    assert error.value.code == "cleanup_lock_unavailable"


async def test_scan_rejects_naive_or_too_recent_cutoff_without_connecting():
    repository = PostgresCheckpointRepository("postgresql://unused")

    with pytest.raises(CheckpointRepositoryError) as naive:
        await repository.scan_candidates(
            cutoff=datetime.now(),
            limit=1,
            max_physical_rows=1,
            max_estimated_logical_bytes=1,
        )
    with pytest.raises(CheckpointRepositoryError) as recent:
        await repository.scan_candidates(
            cutoff=datetime.now(UTC) - timedelta(hours=23),
            limit=1,
            max_physical_rows=1,
            max_estimated_logical_bytes=1,
        )

    assert naive.value.code == "cleanup_invalid_cutoff"
    assert recent.value.code == "cleanup_cutoff_too_recent"


async def test_execution_rejects_plan_metadata_from_another_database(
    checkpoint_schema,
):
    await _valid_thread(checkpoint_schema.dsn, "metadata")
    repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    snapshot = await repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    foreign_plan = SimpleNamespace(
        database_fingerprint="0" * 64,
        database_timezone=snapshot.database_timezone,
        alembic_revision=snapshot.alembic_revision,
        checkpoint_revision=snapshot.checkpoint_revision,
        cutoff=snapshot.cutoff,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )

    with pytest.raises(CheckpointRepositoryError) as error:
        async with repository.execution_session(plan=foreign_plan):
            pass

    assert error.value.code == "cleanup_plan_database_mismatch"


async def test_delete_rechecks_plan_expiry_inside_database_transaction(
    checkpoint_schema,
):
    await _valid_thread(checkpoint_schema.dsn, "expired-in-database")
    repository = PostgresCheckpointRepository(checkpoint_schema.dsn)
    snapshot = await repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    candidate = snapshot.candidates[0]
    expiring_plan = _execution_plan(
        snapshot,
        expires_at=datetime.now(UTC) + timedelta(milliseconds=100),
    )

    async with repository.execution_session(plan=expiring_plan) as session:
        await asyncio.sleep(0.2)
        with pytest.raises(CheckpointRepositoryError) as error:
            await session.delete_candidate(candidate)

    assert error.value.code == "cleanup_plan_expired"
    assert checkpoint_schema.scalar(
        "SELECT count(*) FROM checkpoints WHERE thread_id = 'expired-in-database'"
    ) == 1


async def test_scan_has_bounded_database_lock_wait(checkpoint_schema):
    repository = PostgresCheckpointRepository(
        checkpoint_schema.dsn,
        lock_timeout_ms=50,
        statement_timeout_ms=100,
    )
    blocker = await psycopg.AsyncConnection.connect(checkpoint_schema.dsn)
    await blocker.execute("LOCK TABLE emails_log IN ACCESS EXCLUSIVE MODE")
    try:
        with pytest.raises(CheckpointRepositoryError) as error:
            await asyncio.wait_for(
                repository.scan_candidates(
                    cutoff=_old(48),
                    limit=10,
                    max_physical_rows=100,
                    max_estimated_logical_bytes=1024 * 1024,
                ),
                timeout=1,
            )
    finally:
        await blocker.rollback()
        await blocker.close()

    assert error.value.code == "cleanup_scan_failed"


async def test_revalidation_has_bounded_database_lock_wait(checkpoint_schema):
    await _valid_thread(checkpoint_schema.dsn, "bounded-revalidation")
    repository = PostgresCheckpointRepository(
        checkpoint_schema.dsn,
        lock_timeout_ms=50,
        statement_timeout_ms=100,
    )
    snapshot = await repository.scan_candidates(
        cutoff=_old(48),
        limit=10,
        max_physical_rows=100,
        max_estimated_logical_bytes=1024 * 1024,
    )
    blocker = await psycopg.AsyncConnection.connect(checkpoint_schema.dsn)
    await blocker.execute("LOCK TABLE emails_log IN ACCESS EXCLUSIVE MODE")
    try:
        with pytest.raises(CheckpointRepositoryError) as error:
            await asyncio.wait_for(
                repository.revalidate_candidate(
                    snapshot.candidates[0],
                    plan=snapshot,
                ),
                timeout=1,
            )
    finally:
        await blocker.rollback()
        await blocker.close()

    assert error.value.code == "cleanup_revalidation_failed"
