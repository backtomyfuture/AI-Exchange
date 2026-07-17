"""Private, insert-only artifact storage for checkpoint cleanup plans.

The store is deliberately local and synchronous. It provides a small security
boundary around cross-process plan hand-off: canonical JSON, content-addressed
plans, owner-only permissions, no symlink following, and one-shot claims.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, Iterator, Mapping

from src.maintenance.cleanup_models import (
    CheckpointCleanupPlan,
    CheckpointCleanupReport,
    CleanupCandidate,
    ExclusionBucket,
    _canonical_json_bytes,
    _plan_identity_payload,
)


DEFAULT_MAX_ARTIFACT_BYTES: Final = 1024 * 1024
_PLAN_KIND: Final = "checkpoint_cleanup_plan"
_CLAIM_KIND: Final = "checkpoint_cleanup_claim"
_REPORT_KIND: Final = "checkpoint_cleanup_report"
_SHA256_LENGTH: Final = 64
_NOFOLLOW: Final = getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY: Final = getattr(os, "O_DIRECTORY", 0)


class ArtifactStoreError(RuntimeError):
    """Base error for private cleanup artifact operations."""


class ArtifactSecurityError(ArtifactStoreError):
    """The filesystem object does not meet ownership or mode requirements."""


class ArtifactValidationError(ArtifactStoreError):
    """The artifact is malformed, non-canonical, oversized, or unbound."""


class ArtifactAlreadyExistsError(ArtifactStoreError):
    """An insert-only artifact or execution claim already exists."""


class ArtifactNotFoundError(ArtifactStoreError):
    """The requested artifact does not exist."""


class _DuplicateJsonKeyError(ValueError):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKeyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _decode_canonical_json(raw: bytes) -> dict[str, Any]:
    try:
        decoded = raw.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except _DuplicateJsonKeyError:
        raise ArtifactValidationError("artifact contains a duplicate key") from None
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise ArtifactValidationError("artifact is not valid strict JSON") from None
    if not isinstance(value, dict):
        raise ArtifactValidationError("artifact root must be a JSON object")
    if raw != _canonical_json_bytes(value):
        raise ArtifactValidationError("artifact JSON is not canonical")
    return value


def _require_exact_fields(
    value: object, expected: frozenset[str], *, scope: str
) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ArtifactValidationError(f"{scope} must be a JSON object")
    actual = frozenset(value)
    unknown = actual - expected
    if unknown:
        raise ArtifactValidationError(f"{scope} contains unknown fields")
    missing = expected - actual
    if missing:
        raise ArtifactValidationError(f"{scope} is missing required fields")
    return value


def _parse_utc(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ArtifactValidationError(f"{field_name} must use aware UTC ISO format")
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError:
        raise ArtifactValidationError(
            f"{field_name} is not a valid UTC time"
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ArtifactValidationError(
            f"{field_name} must include timezone information"
        )
    return parsed.astimezone(UTC)


def _normalize_utc(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ArtifactValidationError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ArtifactValidationError(
            f"{field_name} must include timezone information"
        )
    return value.astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _plan_mapping(plan: CheckpointCleanupPlan) -> dict[str, object]:
    result = _plan_identity_payload(plan)
    result["plan_id"] = plan.plan_id
    return result


def _plan_artifact_mapping(plan: CheckpointCleanupPlan) -> dict[str, object]:
    return {"artifact_type": _PLAN_KIND, "plan": _plan_mapping(plan)}


def plan_artifact_sha256(plan: CheckpointCleanupPlan) -> str:
    """Hash the exact canonical bytes used by PlanArtifactStore."""

    return hashlib.sha256(
        _canonical_json_bytes(_plan_artifact_mapping(plan))
    ).hexdigest()


_CANDIDATE_FIELDS = frozenset(
    {
        "thread_id",
        "thread_fingerprint",
        "status",
        "updated_at",
        "checkpoint_rows",
        "checkpoint_bytes",
        "checkpoint_blob_rows",
        "checkpoint_blob_bytes",
        "checkpoint_write_rows",
        "checkpoint_write_bytes",
        "inventory_sha256",
        "slim_state_proven",
        "cleanup_handles_empty",
    }
)
_EXCLUSION_FIELDS = frozenset({"reason", "count"})
_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "policy_version",
        "plan_id",
        "created_at",
        "expires_at",
        "cutoff",
        "database_fingerprint",
        "database_timezone",
        "alembic_revision",
        "checkpoint_revision",
        "limit",
        "max_physical_rows",
        "max_estimated_logical_bytes",
        "candidates",
        "excluded_buckets",
    }
)


def _candidate_from_mapping(value: object) -> CleanupCandidate:
    data = _require_exact_fields(value, _CANDIDATE_FIELDS, scope="candidate")
    try:
        return CleanupCandidate(
            thread_id=data["thread_id"],
            thread_fingerprint=data["thread_fingerprint"],
            status=data["status"],
            updated_at=_parse_utc(data["updated_at"], field_name="updated_at"),
            checkpoint_rows=data["checkpoint_rows"],
            checkpoint_bytes=data["checkpoint_bytes"],
            checkpoint_blob_rows=data["checkpoint_blob_rows"],
            checkpoint_blob_bytes=data["checkpoint_blob_bytes"],
            checkpoint_write_rows=data["checkpoint_write_rows"],
            checkpoint_write_bytes=data["checkpoint_write_bytes"],
            inventory_sha256=data["inventory_sha256"],
            slim_state_proven=data["slim_state_proven"],
            cleanup_handles_empty=data["cleanup_handles_empty"],
        )
    except (TypeError, ValueError):
        raise ArtifactValidationError("candidate failed domain validation") from None


def _exclusion_from_mapping(value: object) -> ExclusionBucket:
    data = _require_exact_fields(value, _EXCLUSION_FIELDS, scope="excluded bucket")
    try:
        return ExclusionBucket(reason=data["reason"], count=data["count"])
    except (TypeError, ValueError):
        raise ArtifactValidationError(
            "excluded bucket failed validation"
        ) from None


def _plan_from_artifact(value: Mapping[str, Any]) -> CheckpointCleanupPlan:
    root = _require_exact_fields(
        value,
        frozenset({"artifact_type", "plan"}),
        scope="plan artifact",
    )
    if root["artifact_type"] != _PLAN_KIND:
        raise ArtifactValidationError("unexpected artifact_type")
    data = _require_exact_fields(root["plan"], _PLAN_FIELDS, scope="plan")
    candidates_value = data["candidates"]
    excluded_value = data["excluded_buckets"]
    if not isinstance(candidates_value, list):
        raise ArtifactValidationError("plan candidates must be a JSON array")
    if not isinstance(excluded_value, list):
        raise ArtifactValidationError("plan excluded_buckets must be a JSON array")
    try:
        plan = CheckpointCleanupPlan(
            schema_version=data["schema_version"],
            policy_version=data["policy_version"],
            created_at=_parse_utc(data["created_at"], field_name="created_at"),
            expires_at=_parse_utc(data["expires_at"], field_name="expires_at"),
            cutoff=_parse_utc(data["cutoff"], field_name="cutoff"),
            database_fingerprint=data["database_fingerprint"],
            database_timezone=data["database_timezone"],
            alembic_revision=data["alembic_revision"],
            checkpoint_revision=data["checkpoint_revision"],
            limit=data["limit"],
            max_physical_rows=data["max_physical_rows"],
            max_estimated_logical_bytes=data["max_estimated_logical_bytes"],
            candidates=tuple(
                _candidate_from_mapping(item) for item in candidates_value
            ),
            excluded_buckets=tuple(
                _exclusion_from_mapping(item) for item in excluded_value
            ),
        )
    except ArtifactValidationError:
        raise
    except (TypeError, ValueError):
        raise ArtifactValidationError("plan failed domain validation") from None
    stored_plan_id = data["plan_id"]
    if not isinstance(stored_plan_id, str) or stored_plan_id != plan.plan_id:
        raise ArtifactValidationError("plan content hash mismatch")
    return plan


def _report_mapping(report: CheckpointCleanupReport) -> dict[str, object]:
    return {
        "artifact_type": _REPORT_KIND,
        "report": {
            "plan_id": report.plan_id,
            "plan_artifact_sha256": report.plan_artifact_sha256,
            "dry_run": report.dry_run,
            "started_at": _utc_iso(report.started_at),
            "completed_at": _utc_iso(report.completed_at),
            "candidate_count": report.candidate_count,
            "processed_count": report.processed_count,
            "skipped_count": report.skipped_count,
            "deleted_thread_count": report.deleted_thread_count,
            "checkpoint_rows": report.checkpoint_rows,
            "checkpoint_blob_rows": report.checkpoint_blob_rows,
            "checkpoint_write_rows": report.checkpoint_write_rows,
            "estimated_logical_bytes": report.estimated_logical_bytes,
            "error_code": report.error_code,
            "vacuum_performed": False,
        },
    }


class PlanArtifactStore:
    """Owner-only canonical JSON store with one-shot execution claims."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        max_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
    ) -> None:
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or max_bytes < 1
        ):
            raise ValueError("max_bytes must be a positive integer")
        self._root = Path(root)
        self._max_bytes = max_bytes
        self._ensure_private_root()

    def _ensure_private_root(self) -> None:
        try:
            root_stat = os.lstat(self._root)
        except FileNotFoundError:
            self._root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            try:
                os.mkdir(self._root, mode=0o700)
            except FileExistsError:
                pass
            else:
                os.chmod(self._root, 0o700, follow_symlinks=False)
            root_stat = os.lstat(self._root)
        self._validate_directory_stat(root_stat)

    @staticmethod
    def _validate_directory_stat(root_stat: os.stat_result) -> None:
        if stat.S_ISLNK(root_stat.st_mode):
            raise ArtifactSecurityError("artifact root cannot be a symlink")
        if not stat.S_ISDIR(root_stat.st_mode):
            raise ArtifactSecurityError("artifact root must be a directory")
        if root_stat.st_uid != os.getuid():
            raise ArtifactSecurityError("artifact root has an unsafe owner")
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise ArtifactSecurityError("artifact root mode must be 0700")

    @contextmanager
    def _root_fd(self) -> Iterator[int]:
        try:
            descriptor = os.open(
                self._root, os.O_RDONLY | _DIRECTORY | _NOFOLLOW
            )
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise ArtifactSecurityError(
                    "artifact root is a symlink or not a directory"
                ) from None
            raise ArtifactStoreError("unable to open artifact root") from None
        try:
            self._validate_directory_stat(os.fstat(descriptor))
            yield descriptor
        finally:
            os.close(descriptor)

    @staticmethod
    def _validate_identifier(plan_id: object) -> str:
        if (
            not isinstance(plan_id, str)
            or len(plan_id) != _SHA256_LENGTH
            or any(character not in "0123456789abcdef" for character in plan_id)
        ):
            raise ArtifactValidationError("invalid plan identifier")
        return plan_id

    @staticmethod
    def _name(plan_id: str, kind: str) -> str:
        return f"{plan_id}.{kind}.json"

    @staticmethod
    def _validate_file_stat(file_stat: os.stat_result) -> None:
        if stat.S_ISLNK(file_stat.st_mode):
            raise ArtifactSecurityError("artifact file cannot be a symlink")
        if not stat.S_ISREG(file_stat.st_mode):
            raise ArtifactSecurityError("artifact must be a regular file")
        if file_stat.st_uid != os.getuid():
            raise ArtifactSecurityError("artifact file has an unsafe owner")
        if stat.S_IMODE(file_stat.st_mode) != 0o600:
            raise ArtifactSecurityError("artifact file mode must be 0600")

    def _write_insert_only(self, name: str, raw: bytes, *, label: str) -> None:
        if len(raw) > self._max_bytes:
            raise ArtifactValidationError("artifact is too large")
        with self._root_fd() as root_fd:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW
            try:
                descriptor = os.open(name, flags, 0o600, dir_fd=root_fd)
            except FileExistsError:
                try:
                    existing = os.stat(
                        name, dir_fd=root_fd, follow_symlinks=False
                    )
                except OSError:
                    existing = None
                if existing is not None and stat.S_ISLNK(existing.st_mode):
                    raise ArtifactSecurityError(
                        f"{label} path is an unsafe symlink"
                    ) from None
                suffix = (
                    " has already been claimed"
                    if label == "plan claim"
                    else " already exists"
                )
                raise ArtifactAlreadyExistsError(f"{label}{suffix}") from None
            except OSError as exc:
                if exc.errno == errno.ELOOP:
                    raise ArtifactSecurityError(
                        f"{label} path is an unsafe symlink"
                    ) from None
                raise ArtifactStoreError(f"unable to create {label}") from None
            try:
                os.fchmod(descriptor, 0o600)
                self._validate_file_stat(os.fstat(descriptor))
                view = memoryview(raw)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise ArtifactStoreError(
                            f"short write while storing {label}"
                        )
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.fsync(root_fd)

    def _read(self, name: str, *, label: str) -> bytes:
        with self._root_fd() as root_fd:
            try:
                descriptor = os.open(
                    name,
                    os.O_RDONLY | _NOFOLLOW,
                    dir_fd=root_fd,
                )
            except FileNotFoundError:
                raise ArtifactNotFoundError(f"{label} was not found") from None
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.EMLINK}:
                    raise ArtifactSecurityError(
                        f"{label} path is an unsafe symlink"
                    ) from None
                raise ArtifactStoreError(f"unable to open {label}") from None
            try:
                file_stat = os.fstat(descriptor)
                self._validate_file_stat(file_stat)
                if file_stat.st_size > self._max_bytes:
                    raise ArtifactValidationError("artifact is too large")
                chunks: list[bytes] = []
                remaining = self._max_bytes + 1
                while remaining > 0:
                    chunk = os.read(descriptor, min(65536, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                raw = b"".join(chunks)
                if len(raw) > self._max_bytes:
                    raise ArtifactValidationError("artifact is too large")
                return raw
            finally:
                os.close(descriptor)

    def save_plan(self, plan: CheckpointCleanupPlan) -> str:
        if not isinstance(plan, CheckpointCleanupPlan):
            raise ArtifactValidationError(
                "plan must be a CheckpointCleanupPlan"
            )
        raw = _canonical_json_bytes(_plan_artifact_mapping(plan))
        self._write_insert_only(
            self._name(plan.plan_id, "plan"),
            raw,
            label="plan artifact",
        )
        return plan.plan_id

    def load_plan(self, plan_id: str) -> CheckpointCleanupPlan:
        validated_id = self._validate_identifier(plan_id)
        raw = self._read(
            self._name(validated_id, "plan"), label="plan artifact"
        )
        plan = _plan_from_artifact(_decode_canonical_json(raw))
        if plan.plan_id != validated_id:
            raise ArtifactValidationError(
                "plan content hash does not match identifier"
            )
        return plan

    def plan_artifact_sha256(self, plan_id: str) -> str:
        """Return a verified hash for a canonical plan loaded from this store."""

        return plan_artifact_sha256(self.load_plan(plan_id))

    def claim_plan(self, plan_id: str, *, claimed_at: datetime) -> str:
        plan = self.load_plan(plan_id)
        normalized_claimed_at = _normalize_utc(
            claimed_at, field_name="claimed_at"
        )
        if not (plan.created_at <= normalized_claimed_at < plan.expires_at):
            raise ArtifactValidationError(
                "claim is outside the plan validity window"
            )
        payload = {
            "artifact_type": _CLAIM_KIND,
            "claim": {
                "plan_id": plan.plan_id,
                "plan_artifact_sha256": plan_artifact_sha256(plan),
                "claimed_at": _utc_iso(normalized_claimed_at),
            },
        }
        raw = _canonical_json_bytes(payload)
        self._write_insert_only(
            self._name(plan.plan_id, "claim"),
            raw,
            label="plan claim",
        )
        return hashlib.sha256(raw).hexdigest()

    def _load_claim(self, plan: CheckpointCleanupPlan) -> Mapping[str, Any]:
        try:
            raw = self._read(
                self._name(plan.plan_id, "claim"),
                label="plan claim",
            )
        except ArtifactNotFoundError:
            raise ArtifactValidationError(
                "plan has no execution claim"
            ) from None
        root = _require_exact_fields(
            _decode_canonical_json(raw),
            frozenset({"artifact_type", "claim"}),
            scope="claim artifact",
        )
        if root["artifact_type"] != _CLAIM_KIND:
            raise ArtifactValidationError("unexpected claim artifact_type")
        claim = _require_exact_fields(
            root["claim"],
            frozenset({"plan_id", "plan_artifact_sha256", "claimed_at"}),
            scope="claim",
        )
        if claim["plan_id"] != plan.plan_id:
            raise ArtifactValidationError("claim plan identifier mismatch")
        if claim["plan_artifact_sha256"] != plan_artifact_sha256(plan):
            raise ArtifactValidationError("claim plan artifact hash mismatch")
        _parse_utc(claim["claimed_at"], field_name="claimed_at")
        return claim

    def save_report(self, report: CheckpointCleanupReport) -> str:
        if not isinstance(report, CheckpointCleanupReport):
            raise ArtifactValidationError(
                "report must be a CheckpointCleanupReport"
            )
        plan = self.load_plan(report.plan_id)
        expected_hash = plan_artifact_sha256(plan)
        if report.plan_artifact_sha256 != expected_hash:
            raise ArtifactValidationError(
                "report plan artifact hash mismatch"
            )
        if not (plan.created_at <= report.started_at < plan.expires_at):
            raise ArtifactValidationError(
                "report start is outside the plan validity window"
            )
        if not report.dry_run:
            claim = self._load_claim(plan)
            claimed_at = _parse_utc(claim["claimed_at"], field_name="claimed_at")
            if report.started_at < claimed_at:
                raise ArtifactValidationError(
                    "report started before the plan claim"
                )
        if report.candidate_count != len(plan.candidates):
            raise ArtifactValidationError(
                "report candidate count does not match plan"
            )
        if report.checkpoint_rows > sum(
            item.checkpoint_rows for item in plan.candidates
        ):
            raise ArtifactValidationError(
                "report checkpoint row count exceeds plan"
            )
        if report.checkpoint_blob_rows > sum(
            item.checkpoint_blob_rows for item in plan.candidates
        ):
            raise ArtifactValidationError(
                "report blob row count exceeds plan"
            )
        if report.checkpoint_write_rows > sum(
            item.checkpoint_write_rows for item in plan.candidates
        ):
            raise ArtifactValidationError(
                "report write row count exceeds plan"
            )
        if report.estimated_logical_bytes > plan.estimated_logical_bytes:
            raise ArtifactValidationError(
                "report logical byte count exceeds plan"
            )
        raw = _canonical_json_bytes(_report_mapping(report))
        report_kind = "dry-run-report" if report.dry_run else "execute-report"
        self._write_insert_only(
            self._name(plan.plan_id, report_kind),
            raw,
            label="cleanup report",
        )
        return hashlib.sha256(raw).hexdigest()


__all__ = [
    "ArtifactAlreadyExistsError",
    "ArtifactNotFoundError",
    "ArtifactSecurityError",
    "ArtifactStoreError",
    "ArtifactValidationError",
    "PlanArtifactStore",
    "plan_artifact_sha256",
]
