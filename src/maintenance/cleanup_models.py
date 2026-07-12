"""Immutable domain models for guarded checkpoint cleanup.

The models in this module intentionally contain no database or filesystem I/O.
They centralize the invariants that must survive process boundaries before a
cleanup plan can be trusted by an executor.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final


CLEANUP_PLAN_SCHEMA_VERSION: Final = 1
CLEANUP_POLICY_VERSION: Final = 1
MAX_PLAN_LIFETIME: Final = timedelta(hours=1)
MINIMUM_CLEANUP_AGE: Final = timedelta(hours=24)
MAX_CLEANUP_THREADS: Final = 100
MAX_CLEANUP_PHYSICAL_ROWS: Final = 500
MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES: Final = 64 * 1024 * 1024

TERMINAL_CHECKPOINT_STATUSES: Final = frozenset(
    {"sent", "rejected", "draft_saved"}
)

# This order is part of the serialized schema.  New reasons require a schema
# version bump instead of silently changing the meaning of existing artifacts.
EXCLUSION_REASONS: Final = (
    "status_not_terminal",
    "invalid_updated_at",
    "too_recent",
    "missing_checkpoint",
    "non_default_namespace",
    "slim_state_unproven",
    "cleanup_handles_present",
    "thread_budget_exceeded",
    "plan_budget_exceeded",
    "inventory_unavailable",
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_ERROR_CODE_PATTERN = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}\Z")
_PUBLIC_METADATA_PATTERN = re.compile(
    r"[A-Za-z0-9_+-][A-Za-z0-9_+./-]{0,127}\Z"
)
_DATABASE_TIMEZONE_PATTERN = re.compile(
    r"[A-Za-z0-9_+./:<>\-]{1,128}\Z"
)


def _require_text(name: str, value: object, *, max_length: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > max_length:
        raise ValueError(f"{name} must be a non-empty bounded string")
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise ValueError(f"{name} contains forbidden control characters")
    return value


def _require_sha256(name: str, value: object) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _require_public_metadata(name: str, value: object) -> str:
    text = _require_text(name, value, max_length=128)
    if _PUBLIC_METADATA_PATTERN.fullmatch(text) is None or ".." in text:
        raise ValueError(f"{name} must be a safe public metadata token")
    return text


def _require_database_timezone(value: object) -> str:
    """Accept every bounded PostgreSQL timezone spelling, including offsets."""
    text = _require_text("database_timezone", value, max_length=128)
    if (
        _DATABASE_TIMEZONE_PATTERN.fullmatch(text) is None
        or text.startswith("/")
        or "://" in text
        or ".." in text
        or "@" in text
    ):
        raise ValueError("database_timezone must be a safe PostgreSQL timezone")
    return text


def _require_int(name: str, value: object, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _as_utc(name: str, value: object) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include timezone information")
    return value.astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class CleanupCandidate:
    """A bounded, fully-proven thread included in a cleanup plan."""

    thread_id: str = field(repr=False)
    thread_fingerprint: str
    status: str
    updated_at: datetime
    checkpoint_rows: int
    checkpoint_bytes: int
    checkpoint_blob_rows: int
    checkpoint_blob_bytes: int
    checkpoint_write_rows: int
    checkpoint_write_bytes: int
    inventory_sha256: str
    slim_state_proven: bool
    cleanup_handles_empty: bool

    def __post_init__(self) -> None:
        _require_text("thread_id", self.thread_id, max_length=2048)
        _require_sha256("thread_fingerprint", self.thread_fingerprint)
        if self.status not in TERMINAL_CHECKPOINT_STATUSES:
            raise ValueError("status is not eligible for checkpoint cleanup")
        object.__setattr__(
            self, "updated_at", _as_utc("updated_at", self.updated_at)
        )
        for name in (
            "checkpoint_rows",
            "checkpoint_bytes",
            "checkpoint_blob_rows",
            "checkpoint_blob_bytes",
            "checkpoint_write_rows",
            "checkpoint_write_bytes",
        ):
            _require_int(name, getattr(self, name))
        if self.checkpoint_rows < 1:
            raise ValueError("checkpoint_rows must prove a real checkpoint")
        _require_sha256("inventory_sha256", self.inventory_sha256)
        normalized_thread_id = self.thread_id.lower()
        if self.thread_fingerprint == normalized_thread_id:
            raise ValueError("thread_fingerprint cannot expose the raw thread id")
        if self.inventory_sha256 == normalized_thread_id:
            raise ValueError("inventory_sha256 cannot expose the raw thread id")
        if self.slim_state_proven is not True:
            raise ValueError("slim state must be proven before cleanup")
        if self.cleanup_handles_empty is not True:
            raise ValueError("cleanup handles must be proven empty before cleanup")

    @property
    def total_rows(self) -> int:
        return (
            self.checkpoint_rows
            + self.checkpoint_blob_rows
            + self.checkpoint_write_rows
        )

    @property
    def estimated_logical_bytes(self) -> int:
        return (
            self.checkpoint_bytes
            + self.checkpoint_blob_bytes
            + self.checkpoint_write_bytes
        )

    def public_summary(self) -> dict[str, object]:
        """Return an intentionally path-, DSN-, and raw-ID-free summary."""

        return {
            "thread_fingerprint": self.thread_fingerprint,
            "status": self.status,
            "updated_at": _utc_iso(self.updated_at),
            "physical_rows": self.total_rows,
            "estimated_logical_bytes": self.estimated_logical_bytes,
            "inventory_sha256": self.inventory_sha256,
        }


@dataclass(frozen=True, slots=True)
class ExclusionBucket:
    reason: str
    count: int

    def __post_init__(self) -> None:
        if self.reason not in EXCLUSION_REASONS:
            raise ValueError("unknown excluded bucket reason")
        _require_int("excluded bucket count", self.count)


def empty_exclusion_buckets() -> tuple[ExclusionBucket, ...]:
    return tuple(ExclusionBucket(reason=reason, count=0) for reason in EXCLUSION_REASONS)


def _candidate_payload(candidate: CleanupCandidate) -> dict[str, object]:
    return {
        "thread_id": candidate.thread_id,
        "thread_fingerprint": candidate.thread_fingerprint,
        "status": candidate.status,
        "updated_at": _utc_iso(candidate.updated_at),
        "checkpoint_rows": candidate.checkpoint_rows,
        "checkpoint_bytes": candidate.checkpoint_bytes,
        "checkpoint_blob_rows": candidate.checkpoint_blob_rows,
        "checkpoint_blob_bytes": candidate.checkpoint_blob_bytes,
        "checkpoint_write_rows": candidate.checkpoint_write_rows,
        "checkpoint_write_bytes": candidate.checkpoint_write_bytes,
        "inventory_sha256": candidate.inventory_sha256,
        "slim_state_proven": candidate.slim_state_proven,
        "cleanup_handles_empty": candidate.cleanup_handles_empty,
    }


def _exclusion_payload(bucket: ExclusionBucket) -> dict[str, object]:
    return {"reason": bucket.reason, "count": bucket.count}


@dataclass(frozen=True, slots=True)
class CheckpointCleanupPlan:
    """An immutable, content-addressed plan with strict physical budgets."""

    schema_version: int
    policy_version: int
    created_at: datetime
    expires_at: datetime
    cutoff: datetime
    database_fingerprint: str
    database_timezone: str
    alembic_revision: str
    checkpoint_revision: int
    limit: int
    max_physical_rows: int
    max_estimated_logical_bytes: int
    candidates: tuple[CleanupCandidate, ...]
    excluded_buckets: tuple[ExclusionBucket, ...]
    plan_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != CLEANUP_PLAN_SCHEMA_VERSION:
            raise ValueError("unsupported cleanup plan schema_version")
        if self.policy_version != CLEANUP_POLICY_VERSION:
            raise ValueError("unsupported cleanup policy_version")

        created_at = _as_utc("created_at", self.created_at)
        expires_at = _as_utc("expires_at", self.expires_at)
        cutoff = _as_utc("cutoff", self.cutoff)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "cutoff", cutoff)

        lifetime = expires_at - created_at
        if lifetime <= timedelta(0) or lifetime > MAX_PLAN_LIFETIME:
            raise ValueError("plan expiry must be within the one-hour validity window")
        if created_at - cutoff < MINIMUM_CLEANUP_AGE:
            raise ValueError("cleanup cutoff must be at least 24 hours old")

        _require_sha256("database_fingerprint", self.database_fingerprint)
        _require_database_timezone(self.database_timezone)
        _require_public_metadata("alembic_revision", self.alembic_revision)
        _require_int("checkpoint_revision", self.checkpoint_revision, minimum=1)
        _require_int("limit", self.limit, minimum=1)
        _require_int("max_physical_rows", self.max_physical_rows, minimum=1)
        _require_int(
            "max_estimated_logical_bytes",
            self.max_estimated_logical_bytes,
            minimum=1,
        )
        if self.limit > MAX_CLEANUP_THREADS:
            raise ValueError("limit exceeds the hard policy cap")
        if self.max_physical_rows > MAX_CLEANUP_PHYSICAL_ROWS:
            raise ValueError("max_physical_rows exceeds the hard policy cap")
        if (
            self.max_estimated_logical_bytes
            > MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES
        ):
            raise ValueError(
                "max_estimated_logical_bytes exceeds the hard policy cap"
            )

        if not isinstance(self.candidates, tuple):
            raise ValueError("candidates must be an immutable tuple")
        if not all(isinstance(item, CleanupCandidate) for item in self.candidates):
            raise ValueError("candidates contains an invalid item")
        if not isinstance(self.excluded_buckets, tuple):
            raise ValueError("excluded_buckets must be an immutable tuple")
        if not all(
            isinstance(item, ExclusionBucket) for item in self.excluded_buckets
        ):
            raise ValueError("excluded_buckets contains an invalid item")

        expected_reasons = tuple(EXCLUSION_REASONS)
        actual_reasons = tuple(item.reason for item in self.excluded_buckets)
        if actual_reasons != expected_reasons:
            raise ValueError("excluded_buckets must contain the fixed schema order")

        normalized_candidates = tuple(
            sorted(
                self.candidates,
                key=lambda item: (item.updated_at, item.thread_fingerprint),
            )
        )
        object.__setattr__(self, "candidates", normalized_candidates)
        thread_ids = tuple(item.thread_id for item in normalized_candidates)
        if len(set(thread_ids)) != len(thread_ids):
            raise ValueError("plan contains a duplicate thread identifier")
        if any(item.updated_at >= cutoff for item in normalized_candidates):
            raise ValueError("every candidate must be strictly older than cutoff")
        if len(normalized_candidates) > self.limit:
            raise ValueError("candidate count exceeds plan limit")
        total_rows = sum(item.total_rows for item in normalized_candidates)
        if total_rows > self.max_physical_rows:
            raise ValueError("candidate physical row count exceeds plan budget")
        total_bytes = sum(
            item.estimated_logical_bytes for item in normalized_candidates
        )
        if total_bytes > self.max_estimated_logical_bytes:
            raise ValueError("candidate estimated logical bytes exceed plan budget")

        digest = hashlib.sha256(
            _canonical_json_bytes(_plan_identity_payload(self))
        ).hexdigest()
        object.__setattr__(self, "plan_id", digest)

    @property
    def total_rows(self) -> int:
        return sum(candidate.total_rows for candidate in self.candidates)

    @property
    def estimated_logical_bytes(self) -> int:
        return sum(
            candidate.estimated_logical_bytes for candidate in self.candidates
        )

    @property
    def scanned_count(self) -> int:
        return len(self.candidates) + sum(
            bucket.count for bucket in self.excluded_buckets
        )

    def public_summary(self) -> dict[str, object]:
        return {
            "plan_id": self.plan_id,
            "schema_version": self.schema_version,
            "policy_version": self.policy_version,
            "created_at": _utc_iso(self.created_at),
            "expires_at": _utc_iso(self.expires_at),
            "cutoff": _utc_iso(self.cutoff),
            "database_fingerprint": self.database_fingerprint,
            "database_timezone": self.database_timezone,
            "alembic_revision": self.alembic_revision,
            "checkpoint_revision": self.checkpoint_revision,
            "candidate_count": len(self.candidates),
            "scanned_count": self.scanned_count,
            "physical_rows": self.total_rows,
            "estimated_logical_bytes": self.estimated_logical_bytes,
            "excluded_counts": {
                bucket.reason: bucket.count for bucket in self.excluded_buckets
            },
        }


def _plan_identity_payload(plan: CheckpointCleanupPlan) -> dict[str, object]:
    """Return the canonical plan body; deliberately excludes ``plan_id``."""

    return {
        "schema_version": plan.schema_version,
        "policy_version": plan.policy_version,
        "created_at": _utc_iso(plan.created_at),
        "expires_at": _utc_iso(plan.expires_at),
        "cutoff": _utc_iso(plan.cutoff),
        "database_fingerprint": plan.database_fingerprint,
        "database_timezone": plan.database_timezone,
        "alembic_revision": plan.alembic_revision,
        "checkpoint_revision": plan.checkpoint_revision,
        "limit": plan.limit,
        "max_physical_rows": plan.max_physical_rows,
        "max_estimated_logical_bytes": plan.max_estimated_logical_bytes,
        "candidates": [_candidate_payload(item) for item in plan.candidates],
        "excluded_buckets": [
            _exclusion_payload(item) for item in plan.excluded_buckets
        ],
    }


@dataclass(frozen=True, slots=True)
class CheckpointCleanupReport:
    """A private execution report.  Byte counts are logical estimates only."""

    plan_id: str
    plan_artifact_sha256: str
    dry_run: bool
    started_at: datetime
    completed_at: datetime
    candidate_count: int
    processed_count: int
    skipped_count: int
    deleted_thread_count: int
    checkpoint_rows: int
    checkpoint_blob_rows: int
    checkpoint_write_rows: int
    estimated_logical_bytes: int
    error_code: str | None
    vacuum_performed: bool = False

    def __post_init__(self) -> None:
        _require_sha256("plan_id", self.plan_id)
        _require_sha256("plan_artifact_sha256", self.plan_artifact_sha256)
        if not isinstance(self.dry_run, bool):
            raise ValueError("dry_run must be a boolean")
        started_at = _as_utc("started_at", self.started_at)
        completed_at = _as_utc("completed_at", self.completed_at)
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "completed_at", completed_at)
        if completed_at < started_at:
            raise ValueError("completed_at cannot precede started_at")

        for name in (
            "candidate_count",
            "processed_count",
            "skipped_count",
            "deleted_thread_count",
            "checkpoint_rows",
            "checkpoint_blob_rows",
            "checkpoint_write_rows",
            "estimated_logical_bytes",
        ):
            _require_int(name, getattr(self, name))
        if self.processed_count + self.skipped_count > self.candidate_count:
            raise ValueError("processed and skipped counts exceed candidate_count")
        if self.error_code is None:
            if self.processed_count + self.skipped_count != self.candidate_count:
                raise ValueError("successful report must account for every candidate")
        elif (
            not isinstance(self.error_code, str)
            or _ERROR_CODE_PATTERN.fullmatch(self.error_code) is None
        ):
            raise ValueError("error_code must be a bounded machine code")
        if self.deleted_thread_count > self.processed_count:
            raise ValueError("deleted_thread_count exceeds processed_count")
        if self.dry_run and self.deleted_thread_count != 0:
            raise ValueError("dry-run report cannot claim deleted threads")
        if self.vacuum_performed is not False:
            raise ValueError("cleanup report must never claim vacuum was performed")

    def public_summary(self) -> dict[str, object]:
        return {
            "plan_id": self.plan_id,
            "plan_artifact_sha256": self.plan_artifact_sha256,
            "dry_run": self.dry_run,
            "started_at": _utc_iso(self.started_at),
            "completed_at": _utc_iso(self.completed_at),
            "candidate_count": self.candidate_count,
            "processed_count": self.processed_count,
            "skipped_count": self.skipped_count,
            "deleted_thread_count": self.deleted_thread_count,
            "checkpoint_rows": self.checkpoint_rows,
            "checkpoint_blob_rows": self.checkpoint_blob_rows,
            "checkpoint_write_rows": self.checkpoint_write_rows,
            "estimated_logical_bytes": self.estimated_logical_bytes,
            "error_code": self.error_code,
            "vacuum_performed": False,
        }


__all__ = [
    "CLEANUP_PLAN_SCHEMA_VERSION",
    "CLEANUP_POLICY_VERSION",
    "EXCLUSION_REASONS",
    "MAX_CLEANUP_ESTIMATED_LOGICAL_BYTES",
    "MAX_CLEANUP_PHYSICAL_ROWS",
    "MAX_CLEANUP_THREADS",
    "TERMINAL_CHECKPOINT_STATUSES",
    "CheckpointCleanupPlan",
    "CheckpointCleanupReport",
    "CleanupCandidate",
    "ExclusionBucket",
    "empty_exclusion_buckets",
]
