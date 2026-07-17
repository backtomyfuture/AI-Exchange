from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from src.maintenance.cleanup_models import (
    CLEANUP_PLAN_SCHEMA_VERSION,
    CLEANUP_POLICY_VERSION,
    EXCLUSION_REASONS,
    MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES,
    MAX_CLEANUP_PHYSICAL_ROWS,
    MAX_CLEANUP_THREADS,
    CheckpointCleanupPlan,
    CheckpointCleanupReport,
    CleanupCandidate,
    ExclusionBucket,
    empty_exclusion_buckets,
)


NOW = datetime(2026, 7, 12, 4, 0, tzinfo=UTC)


def _candidate(**overrides: object) -> CleanupCandidate:
    values: dict[str, object] = {
        "thread_id": "exchange-item-secret-001",
        "thread_fingerprint": "1" * 64,
        "status": "sent",
        "updated_at": NOW - timedelta(days=30),
        "checkpoint_rows": 2,
        "checkpoint_bytes": 100,
        "checkpoint_blob_rows": 3,
        "checkpoint_blob_bytes": 200,
        "checkpoint_write_rows": 4,
        "checkpoint_write_bytes": 300,
        "inventory_sha256": "2" * 64,
        "slim_state_proven": True,
        "cleanup_handles_empty": True,
    }
    values.update(overrides)
    return CleanupCandidate(**values)  # type: ignore[arg-type]


def _plan(**overrides: object) -> CheckpointCleanupPlan:
    values: dict[str, object] = {
        "schema_version": CLEANUP_PLAN_SCHEMA_VERSION,
        "policy_version": CLEANUP_POLICY_VERSION,
        "created_at": NOW,
        "expires_at": NOW + timedelta(minutes=30),
        "cutoff": NOW - timedelta(days=14),
        "database_fingerprint": "3" * 64,
        "database_timezone": "Asia/Shanghai",
        "alembic_revision": "0009_task9_security",
        "checkpoint_revision": 8,
        "limit": 100,
        "max_physical_rows": 500,
        "max_estimated_logical_bytes": 64 * 1024 * 1024,
        "candidates": (_candidate(),),
        "excluded_buckets": empty_exclusion_buckets(),
    }
    values.update(overrides)
    return CheckpointCleanupPlan(**values)  # type: ignore[arg-type]


def test_cleanup_candidate_is_deeply_immutable_and_hides_raw_thread_id() -> None:
    candidate = _candidate()

    assert candidate.total_rows == 9
    assert candidate.estimated_logical_bytes == 600
    assert "exchange-item-secret-001" not in repr(candidate)
    assert "exchange-item-secret-001" not in str(candidate.public_summary())
    assert candidate.public_summary() == {
        "thread_fingerprint": "1" * 64,
        "status": "sent",
        "updated_at": "2026-06-12T04:00:00Z",
        "physical_rows": 9,
        "estimated_logical_bytes": 600,
        "inventory_sha256": "2" * 64,
    }

    with pytest.raises(FrozenInstanceError):
        candidate.status = "rejected"  # type: ignore[misc]
    assert not hasattr(candidate, "__dict__")


def test_cleanup_candidate_normalizes_aware_time_to_utc() -> None:
    china_time = datetime(2026, 6, 12, 12, 0, tzinfo=timezone(timedelta(hours=8)))

    candidate = _candidate(updated_at=china_time)

    assert candidate.updated_at == datetime(2026, 6, 12, 4, 0, tzinfo=UTC)
    assert candidate.updated_at.tzinfo is UTC


def test_public_candidate_hashes_cannot_be_the_raw_thread_identifier() -> None:
    raw_digest = "1" * 64

    with pytest.raises(ValueError, match="raw thread"):
        _candidate(thread_id=raw_digest, thread_fingerprint=raw_digest)
    with pytest.raises(ValueError, match="raw thread"):
        _candidate(thread_id="2" * 64, inventory_sha256="2" * 64)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("updated_at", datetime(2026, 6, 12, 4, 0)),
        ("thread_fingerprint", "not-a-hash"),
        ("inventory_sha256", "A" * 64),
        ("status", "failed"),
        ("checkpoint_rows", 0),
        ("checkpoint_rows", -1),
        ("checkpoint_bytes", True),
        ("slim_state_proven", False),
        ("cleanup_handles_empty", False),
    ],
)
def test_cleanup_candidate_rejects_unsafe_or_ineligible_values(
    field: str, value: object
) -> None:
    with pytest.raises(ValueError):
        _candidate(**{field: value})


def test_plan_has_content_addressed_id_and_only_tuple_collections() -> None:
    first = _plan()
    second = _plan()

    assert first.plan_id == second.plan_id
    assert len(first.plan_id) == 64
    assert isinstance(first.candidates, tuple)
    assert isinstance(first.excluded_buckets, tuple)
    assert tuple(bucket.reason for bucket in first.excluded_buckets) == EXCLUSION_REASONS
    assert all(bucket.count == 0 for bucket in first.excluded_buckets)
    assert not hasattr(first, "__dict__")

    changed = _plan(cutoff=NOW - timedelta(days=15))
    assert changed.plan_id != first.plan_id


def test_plan_public_summary_contains_no_raw_ids_paths_or_dsn() -> None:
    plan = _plan()

    summary = plan.public_summary()
    rendered = str(summary)

    assert summary["plan_id"] == plan.plan_id
    assert summary["candidate_count"] == 1
    assert summary["scanned_count"] == 1
    assert "exchange-item-secret-001" not in rendered
    assert "/Users/" not in rendered
    assert "postgresql://" not in rendered
    assert "candidates" not in summary


def test_plan_derives_scanned_count_from_fixed_exclusions() -> None:
    buckets = list(empty_exclusion_buckets())
    buckets[0] = ExclusionBucket(reason=buckets[0].reason, count=2)

    plan = _plan(excluded_buckets=tuple(buckets))

    assert plan.scanned_count == 3
    assert plan.public_summary()["scanned_count"] == 3


@pytest.mark.parametrize(
    "database_timezone",
    ["+05:30", "<+05:45>-05:45", "America/Argentina/Buenos_Aires"],
)
def test_plan_accepts_bounded_postgres_timezone_formats(
    database_timezone: str,
) -> None:
    plan = _plan(database_timezone=database_timezone)

    assert plan.database_timezone == database_timezone


def test_plan_rejects_control_characters_in_database_timezone() -> None:
    with pytest.raises(ValueError, match="control"):
        _plan(database_timezone="UTC\nprivate")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("database_timezone", "postgresql://user:secret@db/private"),
        ("database_timezone", "/Users/private/timezone"),
        ("alembic_revision", "../../private/revision"),
        ("alembic_revision", "postgresql://secret"),
    ],
)
def test_plan_rejects_public_metadata_that_could_disclose_dsn_or_paths(
    field: str, value: str
) -> None:
    with pytest.raises(ValueError, match=field):
        _plan(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("created_at", datetime(2026, 7, 12, 4, 0)),
        ("expires_at", NOW),
        ("expires_at", NOW + timedelta(hours=1, microseconds=1)),
        ("cutoff", NOW - timedelta(hours=23)),
        ("schema_version", 99),
        ("policy_version", 99),
        ("limit", 0),
        ("max_physical_rows", True),
        ("max_estimated_logical_bytes", -1),
        ("candidates", [_candidate()]),
        ("excluded_buckets", list(empty_exclusion_buckets())),
    ],
)
def test_plan_rejects_invalid_policy_or_mutable_collections(
    field: str, value: object
) -> None:
    with pytest.raises(ValueError):
        _plan(**{field: value})


def test_plan_rejects_non_fixed_exclusion_buckets_and_exceeded_budgets() -> None:
    wrong_buckets = tuple(
        ExclusionBucket(reason=reason, count=0) for reason in reversed(EXCLUSION_REASONS)
    )
    with pytest.raises(ValueError, match="excluded"):
        _plan(excluded_buckets=wrong_buckets)

    with pytest.raises(ValueError, match="physical"):
        _plan(max_physical_rows=8)

    with pytest.raises(ValueError, match="logical"):
        _plan(max_estimated_logical_bytes=599)

    with pytest.raises(ValueError, match="limit"):
        _plan(limit=0)


def test_plan_rejects_duplicate_or_non_old_candidates() -> None:
    first = _candidate()
    duplicate_raw_id = _candidate(
        thread_fingerprint="8" * 64,
        inventory_sha256="9" * 64,
    )
    with pytest.raises(ValueError, match="duplicate thread"):
        _plan(candidates=(first, duplicate_raw_id))

    with pytest.raises(ValueError, match="older than cutoff"):
        _plan(candidates=(_candidate(updated_at=NOW - timedelta(days=14)),))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("limit", MAX_CLEANUP_THREADS + 1),
        ("max_physical_rows", MAX_CLEANUP_PHYSICAL_ROWS + 1),
        (
            "max_estimated_logical_bytes",
            MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES + 1,
        ),
    ],
)
def test_plan_budgets_can_only_tighten_hard_policy_caps(
    field: str, value: int
) -> None:
    with pytest.raises(ValueError, match="hard policy cap"):
        _plan(**{field: value})


def test_report_is_immutable_and_never_claims_vacuum() -> None:
    plan = _plan()
    report = CheckpointCleanupReport(
        plan_id=plan.plan_id,
        plan_artifact_sha256="4" * 64,
        dry_run=True,
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=2),
        candidate_count=1,
        processed_count=1,
        skipped_count=0,
        deleted_thread_count=0,
        checkpoint_rows=2,
        checkpoint_blob_rows=3,
        checkpoint_write_rows=4,
        estimated_logical_bytes=600,
        error_code=None,
    )

    assert report.vacuum_performed is False
    assert report.public_summary()["plan_id"] == plan.plan_id
    assert not hasattr(report, "__dict__")
    with pytest.raises(FrozenInstanceError):
        report.dry_run = False  # type: ignore[misc]

    with pytest.raises(ValueError, match="vacuum"):
        CheckpointCleanupReport(
            plan_id=plan.plan_id,
            plan_artifact_sha256="4" * 64,
            dry_run=False,
            started_at=NOW,
            completed_at=NOW,
            candidate_count=0,
            processed_count=0,
            skipped_count=0,
            deleted_thread_count=0,
            checkpoint_rows=0,
            checkpoint_blob_rows=0,
            checkpoint_write_rows=0,
            estimated_logical_bytes=0,
            error_code=None,
            vacuum_performed=True,
        )
