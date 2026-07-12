"""Dry-run-first orchestration for guarded LangGraph checkpoint cleanup."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Callable, Protocol, runtime_checkable

from src.maintenance.cleanup_backup import (
    BackupReceiptError,
    BackupReceiptVerifier,
)
from src.maintenance.cleanup_models import (
    CLEANUP_PLAN_SCHEMA_VERSION,
    CLEANUP_POLICY_VERSION,
    MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES,
    MAX_CLEANUP_PHYSICAL_ROWS,
    MAX_CLEANUP_THREADS,
    TERMINAL_CHECKPOINT_STATUSES,
    CleanupCandidate,
    CheckpointCleanupPlan,
    CheckpointCleanupReport,
)


MIN_RETENTION_AGE = timedelta(hours=24)
DEFAULT_PLAN_TTL = timedelta(hours=1)
DEFAULT_MAX_THREADS = MAX_CLEANUP_THREADS
DEFAULT_MAX_PHYSICAL_ROWS = MAX_CLEANUP_PHYSICAL_ROWS
DEFAULT_MAX_ESTIMATED_LOGICAL_BYTES = MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES

_PLAN_ERROR_CODES = frozenset(
    {
        "cutoff_timezone_required",
        "clock_timezone_required",
        "retention_period_too_short",
        "plan_limit_invalid",
        "plan_limit_mismatch",
        "plan_expired",
        "plan_from_future",
        "plan_candidate_invalid",
    }
)
_AUTHORIZATION_ERROR_CODES = frozenset(
    {
        "backup_id_required",
        "backup_receipt_required",
        "service_not_quiesced",
        "backup_verifier_unavailable",
        "backup_receipt_invalid",
        "backup_id_mismatch",
    }
)


class CleanupPlanError(ValueError):
    """A fixed-code validation failure for plan or dry-run requests."""

    def __init__(self, code: str) -> None:
        safe_code = code if code in _PLAN_ERROR_CODES else "plan_candidate_invalid"
        self.code = safe_code
        super().__init__(safe_code)


class CleanupAuthorizationError(ValueError):
    """A fixed-code failure at the destructive execution boundary."""

    def __init__(self, code: str) -> None:
        safe_code = (
            code
            if code in _AUTHORIZATION_ERROR_CODES
            else "backup_receipt_invalid"
        )
        self.code = safe_code
        super().__init__(safe_code)


@runtime_checkable
class _ScanSnapshot(Protocol):
    database_fingerprint: str
    database_timezone: str
    alembic_revision: str
    checkpoint_revision: int
    candidates: tuple[CleanupCandidate, ...]
    excluded_buckets: tuple


class _DeleteResult(Protocol):
    deleted: bool
    stale: bool
    checkpoint_rows: int
    checkpoint_blob_rows: int
    checkpoint_write_rows: int
    estimated_logical_bytes: int


class _ExecutionSession(Protocol):
    async def delete_candidate(
        self,
        candidate: CleanupCandidate,
    ) -> _DeleteResult: ...


class CheckpointCleanupRepository(Protocol):
    async def scan_candidates(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_physical_rows: int,
        max_estimated_logical_bytes: int,
    ) -> _ScanSnapshot: ...

    async def revalidate_candidate(
        self,
        candidate: CleanupCandidate,
        *,
        plan: CheckpointCleanupPlan,
    ) -> bool: ...

    def execution_session(self, *, plan: CheckpointCleanupPlan): ...


class PlanArtifactStoreProtocol(Protocol):
    def save_plan(self, plan: CheckpointCleanupPlan) -> str: ...

    def load_plan(self, plan_id: str) -> CheckpointCleanupPlan: ...

    def plan_artifact_sha256(self, plan_id: str) -> str: ...

    def claim_plan(self, plan_id: str, *, claimed_at: datetime) -> str: ...

    def save_report(self, report: CheckpointCleanupReport) -> str: ...


def _aware_utc(value: datetime, *, code: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise CleanupPlanError(code)
    try:
        offset = value.utcoffset()
    except Exception:
        raise CleanupPlanError(code) from None
    if offset is None:
        raise CleanupPlanError(code)
    return value.astimezone(UTC)


def _validate_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= DEFAULT_MAX_THREADS:
        raise CleanupPlanError("plan_limit_invalid")
    return limit


def select_cleanup_candidates(
    candidates: tuple[CleanupCandidate, ...],
    *,
    older_than: datetime,
) -> tuple[CleanupCandidate, ...]:
    """Apply the Phase 1 allowlist again as a defense-in-depth pure filter."""
    cutoff = _aware_utc(older_than, code="cutoff_timezone_required")
    return tuple(
        candidate
        for candidate in candidates
        if candidate.status in TERMINAL_CHECKPOINT_STATUSES
        and candidate.updated_at < cutoff
        and candidate.slim_state_proven
        and candidate.cleanup_handles_empty
    )


class CheckpointCleaner:
    """Create immutable plans and execute only after every explicit gate passes."""

    __slots__ = (
        "_artifact_store",
        "_backup_verifier",
        "_max_estimated_logical_bytes",
        "_max_physical_rows",
        "_now",
        "_plan_ttl",
        "_repository",
    )

    def __init__(
        self,
        *,
        repository: CheckpointCleanupRepository,
        artifact_store: PlanArtifactStoreProtocol,
        backup_verifier: BackupReceiptVerifier | None = None,
        now: Callable[[], datetime] | None = None,
        plan_ttl: timedelta = DEFAULT_PLAN_TTL,
        max_physical_rows: int = DEFAULT_MAX_PHYSICAL_ROWS,
        max_estimated_logical_bytes: int = (
            DEFAULT_MAX_ESTIMATED_LOGICAL_BYTES
        ),
    ) -> None:
        if plan_ttl <= timedelta(0):
            raise CleanupPlanError("plan_candidate_invalid")
        if (
            type(max_physical_rows) is not int
            or not 1 <= max_physical_rows <= MAX_CLEANUP_PHYSICAL_ROWS
        ):
            raise CleanupPlanError("plan_candidate_invalid")
        if (
            type(max_estimated_logical_bytes) is not int
            or not 1
            <= max_estimated_logical_bytes
            <= MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES
        ):
            raise CleanupPlanError("plan_candidate_invalid")
        self._repository = repository
        self._artifact_store = artifact_store
        self._backup_verifier = backup_verifier
        self._now = now or (lambda: datetime.now(UTC))
        self._plan_ttl = plan_ttl
        self._max_physical_rows = max_physical_rows
        self._max_estimated_logical_bytes = max_estimated_logical_bytes

    def _clock(self) -> datetime:
        return _aware_utc(self._now(), code="clock_timezone_required")

    async def plan(
        self,
        *,
        older_than: datetime,
        limit: int,
    ) -> CheckpointCleanupPlan:
        cutoff = _aware_utc(
            older_than,
            code="cutoff_timezone_required",
        )
        validated_limit = _validate_limit(limit)
        created_at = self._clock()
        if created_at - cutoff < MIN_RETENTION_AGE:
            raise CleanupPlanError("retention_period_too_short")

        scan = await self._repository.scan_candidates(
            cutoff=cutoff,
            limit=validated_limit,
            max_physical_rows=self._max_physical_rows,
            max_estimated_logical_bytes=self._max_estimated_logical_bytes,
        )
        candidates = select_cleanup_candidates(
            tuple(scan.candidates),
            older_than=cutoff,
        )
        if len(candidates) > validated_limit:
            raise CleanupPlanError("plan_candidate_invalid")

        plan = CheckpointCleanupPlan(
            schema_version=CLEANUP_PLAN_SCHEMA_VERSION,
            policy_version=CLEANUP_POLICY_VERSION,
            created_at=created_at,
            expires_at=created_at + self._plan_ttl,
            cutoff=cutoff,
            database_fingerprint=scan.database_fingerprint,
            database_timezone=scan.database_timezone,
            alembic_revision=scan.alembic_revision,
            checkpoint_revision=scan.checkpoint_revision,
            limit=validated_limit,
            max_physical_rows=self._max_physical_rows,
            max_estimated_logical_bytes=self._max_estimated_logical_bytes,
            candidates=candidates,
            excluded_buckets=tuple(scan.excluded_buckets),
        )
        self._artifact_store.save_plan(plan)
        return plan

    def _load_current_plan(
        self,
        plan_id: str,
        *,
        limit: int,
    ) -> tuple[CheckpointCleanupPlan, datetime]:
        validated_limit = _validate_limit(limit)
        plan = self._artifact_store.load_plan(plan_id)
        if plan.plan_id != plan_id:
            raise CleanupPlanError("plan_candidate_invalid")
        if validated_limit != plan.limit:
            raise CleanupPlanError("plan_limit_mismatch")
        now = self._clock()
        if now < plan.created_at:
            raise CleanupPlanError("plan_from_future")
        if now >= plan.expires_at:
            raise CleanupPlanError("plan_expired")
        return plan, now

    def _plan_artifact_hash(self, plan: CheckpointCleanupPlan) -> str:
        return self._artifact_store.plan_artifact_sha256(plan.plan_id)

    def _report(
        self,
        *,
        plan: CheckpointCleanupPlan,
        dry_run: bool,
        started_at: datetime,
        processed_count: int,
        skipped_count: int,
        deleted_thread_count: int,
        checkpoint_rows: int,
        checkpoint_blob_rows: int,
        checkpoint_write_rows: int,
        estimated_logical_bytes: int,
        error_code: str | None,
    ) -> CheckpointCleanupReport:
        report = CheckpointCleanupReport(
            plan_id=plan.plan_id,
            plan_artifact_sha256=self._plan_artifact_hash(plan),
            dry_run=dry_run,
            started_at=started_at,
            completed_at=self._clock(),
            candidate_count=len(plan.candidates),
            processed_count=processed_count,
            skipped_count=skipped_count,
            deleted_thread_count=deleted_thread_count,
            checkpoint_rows=checkpoint_rows,
            checkpoint_blob_rows=checkpoint_blob_rows,
            checkpoint_write_rows=checkpoint_write_rows,
            estimated_logical_bytes=estimated_logical_bytes,
            error_code=error_code,
            vacuum_performed=False,
        )
        self._artifact_store.save_report(report)
        return report

    async def run(
        self,
        plan_id: str,
        *,
        dry_run: bool,
        backup_id: str | None,
        limit: int,
        backup_receipt: str | bytes | None = None,
        service_quiesced: bool = False,
    ) -> CheckpointCleanupReport:
        plan, started_at = self._load_current_plan(plan_id, limit=limit)
        if dry_run:
            return await self._run_dry(plan, started_at=started_at)

        verified_backup_id = self._authorize_execution(
            plan,
            backup_id=backup_id,
            backup_receipt=backup_receipt,
            service_quiesced=service_quiesced,
            execution_started_at=started_at,
        )
        if verified_backup_id != backup_id:
            raise CleanupAuthorizationError("backup_id_mismatch")

        # Claim only after every non-database authorization gate has passed.
        # A later failure deliberately consumes the plan and forces replanning.
        claim_time = self._clock()
        if claim_time >= plan.expires_at:
            raise CleanupPlanError("plan_expired")
        self._artifact_store.claim_plan(plan.plan_id, claimed_at=claim_time)
        return await self._run_execute(plan, started_at=claim_time)

    async def _run_dry(
        self,
        plan: CheckpointCleanupPlan,
        *,
        started_at: datetime,
    ) -> CheckpointCleanupReport:
        processed = 0
        skipped = 0
        checkpoint_rows = 0
        checkpoint_blob_rows = 0
        checkpoint_write_rows = 0
        estimated_bytes = 0
        error_code: str | None = None

        for candidate in plan.candidates:
            try:
                current = await self._repository.revalidate_candidate(
                    candidate,
                    plan=plan,
                )
            except Exception as exc:
                error_code = _safe_repository_error(exc)
                break
            if not current:
                skipped += 1
                continue
            processed += 1
            checkpoint_rows += candidate.checkpoint_rows
            checkpoint_blob_rows += candidate.checkpoint_blob_rows
            checkpoint_write_rows += candidate.checkpoint_write_rows
            estimated_bytes += candidate.estimated_logical_bytes

        return self._report(
            plan=plan,
            dry_run=True,
            started_at=started_at,
            processed_count=processed,
            skipped_count=skipped,
            deleted_thread_count=0,
            checkpoint_rows=checkpoint_rows,
            checkpoint_blob_rows=checkpoint_blob_rows,
            checkpoint_write_rows=checkpoint_write_rows,
            estimated_logical_bytes=estimated_bytes,
            error_code=error_code,
        )

    def _authorize_execution(
        self,
        plan: CheckpointCleanupPlan,
        *,
        backup_id: str | None,
        backup_receipt: str | bytes | None,
        service_quiesced: bool,
        execution_started_at: datetime,
    ) -> str:
        if not isinstance(backup_id, str) or not backup_id.strip():
            raise CleanupAuthorizationError("backup_id_required")
        if backup_receipt is None:
            raise CleanupAuthorizationError("backup_receipt_required")
        if service_quiesced is not True:
            raise CleanupAuthorizationError("service_not_quiesced")
        if self._backup_verifier is None:
            raise CleanupAuthorizationError("backup_verifier_unavailable")
        try:
            verified = self._backup_verifier.verify(
                backup_receipt,
                expected_plan_id=plan.plan_id,
                expected_database_fingerprint=plan.database_fingerprint,
                expected_alembic_revision=plan.alembic_revision,
                expected_checkpoint_revision=plan.checkpoint_revision,
                plan_created_at=plan.created_at,
            )
        except BackupReceiptError:
            raise CleanupAuthorizationError("backup_receipt_invalid") from None
        completed_at = getattr(verified, "completed_at", None)
        if not isinstance(completed_at, datetime) or completed_at.tzinfo is None:
            raise CleanupAuthorizationError("backup_receipt_invalid")
        try:
            completed_at = completed_at.astimezone(UTC)
        except (OverflowError, ValueError):
            raise CleanupAuthorizationError("backup_receipt_invalid") from None
        if completed_at > execution_started_at:
            raise CleanupAuthorizationError("backup_receipt_invalid")
        return verified.backup_id

    async def _run_execute(
        self,
        plan: CheckpointCleanupPlan,
        *,
        started_at: datetime,
    ) -> CheckpointCleanupReport:
        processed = 0
        skipped = 0
        deleted = 0
        checkpoint_rows = 0
        checkpoint_blob_rows = 0
        checkpoint_write_rows = 0
        estimated_bytes = 0
        error_code: str | None = None

        try:
            async with self._repository.execution_session(plan=plan) as session:
                for candidate in plan.candidates:
                    try:
                        result = await session.delete_candidate(candidate)
                    except Exception as exc:
                        error_code = _safe_repository_error(exc)
                        break
                    if result.stale or not result.deleted:
                        skipped += 1
                        continue
                    processed += 1
                    deleted += 1
                    checkpoint_rows += result.checkpoint_rows
                    checkpoint_blob_rows += result.checkpoint_blob_rows
                    checkpoint_write_rows += result.checkpoint_write_rows
                    estimated_bytes += result.estimated_logical_bytes
        except Exception as exc:
            error_code = _safe_repository_error(exc)

        return self._report(
            plan=plan,
            dry_run=False,
            started_at=started_at,
            processed_count=processed,
            skipped_count=skipped,
            deleted_thread_count=deleted,
            checkpoint_rows=checkpoint_rows,
            checkpoint_blob_rows=checkpoint_blob_rows,
            checkpoint_write_rows=checkpoint_write_rows,
            estimated_logical_bytes=estimated_bytes,
            error_code=error_code,
        )


def _safe_repository_error(exc: BaseException) -> str:
    code = getattr(exc, "code", None)
    if isinstance(code, str) and (
        code.startswith("checkpoint_cleanup_")
        or code.startswith("cleanup_")
    ):
        return code
    return "checkpoint_cleanup_repository_error"
