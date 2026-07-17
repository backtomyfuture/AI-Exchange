from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.maintenance.checkpoint_cleanup import (
    TERMINAL_CHECKPOINT_STATUSES,
    CleanupAuthorizationError,
    CleanupPlanError,
    CheckpointCleaner,
    select_cleanup_candidates,
)
from src.maintenance.cleanup_backup import BackupReceiptError
from src.maintenance.cleanup_models import (
    CleanupCandidate,
    CheckpointCleanupPlan,
    empty_exclusion_buckets,
)


NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
CUTOFF = NOW - timedelta(hours=24)


def _candidate(
    thread_id: str = "mail-1",
    *,
    status: str = "sent",
    updated_at: datetime = NOW - timedelta(hours=25),
    slim_state_proven: bool = True,
    cleanup_handles_empty: bool = True,
) -> CleanupCandidate:
    return CleanupCandidate(
        thread_id=thread_id,
        thread_fingerprint="a" * 64,
        status=status,
        updated_at=updated_at,
        checkpoint_rows=1,
        checkpoint_bytes=101,
        checkpoint_blob_rows=2,
        checkpoint_blob_bytes=202,
        checkpoint_write_rows=3,
        checkpoint_write_bytes=303,
        inventory_sha256="b" * 64,
        slim_state_proven=slim_state_proven,
        cleanup_handles_empty=cleanup_handles_empty,
    )


@dataclass(frozen=True, slots=True)
class _Scan:
    database_fingerprint: str
    database_timezone: str
    alembic_revision: str
    checkpoint_revision: int
    candidates: tuple[CleanupCandidate, ...]
    excluded_buckets: tuple
    scanned_count: int


class _Artifacts:
    def __init__(self) -> None:
        self.plans: dict[str, CheckpointCleanupPlan] = {}
        self.claimed: list[str] = []
        self.reports: list[object] = []

    def save_plan(self, plan: CheckpointCleanupPlan) -> str:
        self.plans[plan.plan_id] = plan
        return "c" * 64

    def load_plan(self, plan_id: str) -> CheckpointCleanupPlan:
        return self.plans[plan_id]

    def plan_artifact_sha256(self, plan_id: str) -> str:
        assert plan_id in self.plans
        return "c" * 64

    def claim_plan(self, plan_id: str, *, claimed_at: datetime) -> str:
        self.claimed.append(plan_id)
        return "d" * 64

    def save_report(self, report) -> str:
        self.reports.append(report)
        return "e" * 64


class _Session:
    def __init__(self, repository: "_Repository") -> None:
        self.repository = repository

    async def delete_candidate(self, candidate: CleanupCandidate):
        self.repository.delete_calls.append(candidate.thread_id)
        outcome = self.repository.delete_outcomes.get(candidate.thread_id)
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome is not None:
            return outcome
        return SimpleNamespace(
            deleted=True,
            stale=False,
            checkpoint_rows=candidate.checkpoint_rows,
            checkpoint_blob_rows=candidate.checkpoint_blob_rows,
            checkpoint_write_rows=candidate.checkpoint_write_rows,
            estimated_logical_bytes=candidate.estimated_logical_bytes,
        )


class _Repository:
    def __init__(self, candidates: tuple[CleanupCandidate, ...]) -> None:
        self.scan = _Scan(
            database_fingerprint="f" * 64,
            database_timezone="UTC",
            alembic_revision="20260710_0002",
            checkpoint_revision=9,
            candidates=candidates,
            excluded_buckets=empty_exclusion_buckets(),
            scanned_count=len(candidates),
        )
        self.scan_calls: list[dict[str, object]] = []
        self.revalidation: dict[str, bool] = {
            candidate.thread_id: True for candidate in candidates
        }
        self.revalidate_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.delete_outcomes: dict[str, object] = {}
        self.execution_entries = 0

    async def scan_candidates(self, **kwargs):
        self.scan_calls.append(kwargs)
        return self.scan

    async def revalidate_candidate(
        self,
        candidate: CleanupCandidate,
        *,
        plan: CheckpointCleanupPlan,
    ) -> bool:
        assert plan.plan_id
        self.revalidate_calls.append(candidate.thread_id)
        return self.revalidation[candidate.thread_id]

    @asynccontextmanager
    async def execution_session(self, *, plan: CheckpointCleanupPlan):
        assert plan.plan_id
        self.execution_entries += 1
        yield _Session(self)


class _Verifier:
    def __init__(self, *, backup_id: str = "backup-1") -> None:
        self.backup_id = backup_id
        self.completed_at = NOW
        self.calls: list[dict[str, object]] = []
        self.error: BackupReceiptError | None = None

    def verify(self, receipt, **kwargs):
        self.calls.append({"receipt": receipt, **kwargs})
        if self.error is not None:
            raise self.error
        return SimpleNamespace(
            backup_id=self.backup_id,
            completed_at=self.completed_at,
        )


def _cleaner(
    candidates: tuple[CleanupCandidate, ...] = (_candidate(),),
    *,
    verifier: _Verifier | None = None,
):
    repository = _Repository(candidates)
    artifacts = _Artifacts()
    cleaner = CheckpointCleaner(
        repository=repository,
        artifact_store=artifacts,
        backup_verifier=verifier,
        now=lambda: NOW,
    )
    return cleaner, repository, artifacts


def test_select_candidates_uses_narrow_terminal_allowlist_and_strict_cutoff() -> None:
    candidates = (
        _candidate("sent", status="sent"),
        _candidate("rejected", status="rejected"),
        _candidate("draft", status="draft_saved"),
        _candidate("boundary", updated_at=CUTOFF),
    )

    selected = select_cleanup_candidates(candidates, older_than=CUTOFF)

    assert TERMINAL_CHECKPOINT_STATUSES == {
        "sent",
        "rejected",
        "draft_saved",
    }
    assert tuple(item.thread_id for item in selected) == (
        "sent",
        "rejected",
        "draft",
    )


def test_select_candidates_rejects_naive_cutoff() -> None:
    with pytest.raises(CleanupPlanError) as caught:
        select_cleanup_candidates(
            (_candidate(),),
            older_than=CUTOFF.replace(tzinfo=None),
        )

    assert caught.value.code == "cutoff_timezone_required"


@pytest.mark.asyncio
async def test_plan_requires_at_least_24_hours_and_builds_immutable_plan() -> None:
    cleaner, repository, artifacts = _cleaner()

    with pytest.raises(CleanupPlanError) as caught:
        await cleaner.plan(older_than=NOW - timedelta(hours=23, minutes=59), limit=1)
    assert caught.value.code == "retention_period_too_short"

    plan = await cleaner.plan(older_than=CUTOFF, limit=1)

    assert plan.candidates == (_candidate(),)
    assert plan.expires_at == NOW + timedelta(hours=1)
    assert plan.limit == 1
    assert repository.scan_calls == [
        {
            "cutoff": CUTOFF,
            "limit": 1,
            "max_physical_rows": 500,
            "max_estimated_logical_bytes": 64 * 1024 * 1024,
        }
    ]
    assert artifacts.plans[plan.plan_id] is plan
    with pytest.raises(FrozenInstanceError):
        plan.limit = 2  # type: ignore[misc]


@pytest.mark.asyncio
async def test_dry_run_revalidates_but_never_enters_delete_session() -> None:
    candidates = (_candidate("current"), _candidate("stale"))
    cleaner, repository, artifacts = _cleaner(candidates)
    plan = await cleaner.plan(older_than=CUTOFF, limit=2)
    repository.revalidation["stale"] = False

    report = await cleaner.run(
        plan.plan_id,
        dry_run=True,
        backup_id=None,
        limit=2,
    )

    assert repository.revalidate_calls == ["current", "stale"]
    assert repository.execution_entries == 0
    assert repository.delete_calls == []
    assert report.dry_run is True
    assert report.processed_count == 1
    assert report.skipped_count == 1
    assert report.deleted_thread_count == 0
    assert report.error_code is None
    assert artifacts.claimed == []
    assert artifacts.reports == [report]


@pytest.mark.asyncio
async def test_run_rejects_expired_plan_and_limit_drift_before_repository_access() -> None:
    cleaner, repository, _ = _cleaner()
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)
    repository.revalidate_calls.clear()

    with pytest.raises(CleanupPlanError) as caught:
        await cleaner.run(plan.plan_id, dry_run=True, backup_id=None, limit=2)
    assert caught.value.code == "plan_limit_mismatch"
    assert repository.revalidate_calls == []

    cleaner._now = lambda: plan.expires_at  # type: ignore[attr-defined]
    with pytest.raises(CleanupPlanError) as caught:
        await cleaner.run(plan.plan_id, dry_run=True, backup_id=None, limit=1)
    assert caught.value.code == "plan_expired"
    assert repository.revalidate_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        (
            {
                "backup_id": None,
                "backup_receipt": b"receipt",
                "service_quiesced": True,
            },
            "backup_id_required",
        ),
        (
            {
                "backup_id": "backup-1",
                "backup_receipt": None,
                "service_quiesced": True,
            },
            "backup_receipt_required",
        ),
        (
            {
                "backup_id": "backup-1",
                "backup_receipt": b"receipt",
                "service_quiesced": False,
            },
            "service_not_quiesced",
        ),
    ],
)
async def test_execute_fails_closed_before_claim_when_gate_is_missing(
    kwargs: dict[str, object],
    code: str,
) -> None:
    cleaner, repository, artifacts = _cleaner(verifier=_Verifier())
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)

    with pytest.raises(CleanupAuthorizationError) as caught:
        await cleaner.run(plan.plan_id, dry_run=False, limit=1, **kwargs)

    assert caught.value.code == code
    assert artifacts.claimed == []
    assert repository.execution_entries == 0


@pytest.mark.asyncio
async def test_execute_requires_configured_verifier_and_exact_receipt_backup_id() -> None:
    cleaner, repository, artifacts = _cleaner(verifier=None)
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)

    with pytest.raises(CleanupAuthorizationError) as caught:
        await cleaner.run(
            plan.plan_id,
            dry_run=False,
            backup_id="backup-1",
            backup_receipt=b"receipt",
            service_quiesced=True,
            limit=1,
        )
    assert caught.value.code == "backup_verifier_unavailable"

    verifier = _Verifier(backup_id="different-backup")
    cleaner._backup_verifier = verifier  # type: ignore[attr-defined]
    with pytest.raises(CleanupAuthorizationError) as caught:
        await cleaner.run(
            plan.plan_id,
            dry_run=False,
            backup_id="backup-1",
            backup_receipt=b"receipt",
            service_quiesced=True,
            limit=1,
        )
    assert caught.value.code == "backup_id_mismatch"
    assert artifacts.claimed == []
    assert repository.execution_entries == 0


@pytest.mark.asyncio
async def test_execute_rejects_backup_receipt_completed_in_the_future() -> None:
    verifier = _Verifier()
    verifier.completed_at = NOW + timedelta(seconds=1)
    cleaner, repository, artifacts = _cleaner(verifier=verifier)
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)

    with pytest.raises(CleanupAuthorizationError) as caught:
        await cleaner.run(
            plan.plan_id,
            dry_run=False,
            backup_id="backup-1",
            backup_receipt=b"receipt",
            service_quiesced=True,
            limit=1,
        )

    assert caught.value.code == "backup_receipt_invalid"
    assert artifacts.claimed == []
    assert repository.execution_entries == 0


@pytest.mark.asyncio
async def test_execute_rechecks_plan_expiry_immediately_before_claim() -> None:
    verifier = _Verifier()
    cleaner, repository, artifacts = _cleaner(verifier=verifier)
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)
    moments = iter(
        [
            plan.expires_at - timedelta(microseconds=1),
            plan.expires_at,
        ]
    )
    cleaner._now = lambda: next(moments)  # type: ignore[attr-defined]

    with pytest.raises(CleanupPlanError) as caught:
        await cleaner.run(
            plan.plan_id,
            dry_run=False,
            backup_id="backup-1",
            backup_receipt=b"receipt",
            service_quiesced=True,
            limit=1,
        )

    assert caught.value.code == "plan_expired"
    assert artifacts.claimed == []
    assert repository.execution_entries == 0


@pytest.mark.asyncio
async def test_execute_verifies_claims_then_deletes_and_reports_aggregate_counts() -> None:
    candidates = (_candidate("one"), _candidate("two"))
    verifier = _Verifier()
    cleaner, repository, artifacts = _cleaner(candidates, verifier=verifier)
    plan = await cleaner.plan(older_than=CUTOFF, limit=2)
    repository.delete_outcomes["two"] = SimpleNamespace(
        deleted=False,
        stale=True,
        checkpoint_rows=0,
        checkpoint_blob_rows=0,
        checkpoint_write_rows=0,
        estimated_logical_bytes=0,
    )

    report = await cleaner.run(
        plan.plan_id,
        dry_run=False,
        backup_id="backup-1",
        backup_receipt=b"receipt",
        service_quiesced=True,
        limit=2,
    )

    assert verifier.calls == [
        {
            "receipt": b"receipt",
            "expected_plan_id": plan.plan_id,
            "expected_database_fingerprint": plan.database_fingerprint,
            "expected_alembic_revision": plan.alembic_revision,
            "expected_checkpoint_revision": plan.checkpoint_revision,
            "plan_created_at": plan.created_at,
        }
    ]
    assert artifacts.claimed == [plan.plan_id]
    assert repository.execution_entries == 1
    assert repository.delete_calls == ["one", "two"]
    assert report.dry_run is False
    assert report.processed_count == 1
    assert report.deleted_thread_count == 1
    assert report.skipped_count == 1
    assert report.checkpoint_rows == 1
    assert report.checkpoint_blob_rows == 2
    assert report.checkpoint_write_rows == 3
    assert report.estimated_logical_bytes == 606
    assert report.vacuum_performed is False
    assert artifacts.reports == [report]


@pytest.mark.asyncio
async def test_backup_verification_error_is_converted_to_fixed_authorization_code() -> None:
    verifier = _Verifier()
    verifier.error = BackupReceiptError("backup_receipt_signature_invalid")
    cleaner, repository, artifacts = _cleaner(verifier=verifier)
    plan = await cleaner.plan(older_than=CUTOFF, limit=1)

    with pytest.raises(CleanupAuthorizationError) as caught:
        await cleaner.run(
            plan.plan_id,
            dry_run=False,
            backup_id="backup-1",
            backup_receipt=b"receipt",
            service_quiesced=True,
            limit=1,
        )

    assert caught.value.code == "backup_receipt_invalid"
    assert artifacts.claimed == []
    assert repository.execution_entries == 0
    assert "signature" not in str(caught.value)
