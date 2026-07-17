"""Dormant cold-start preview, approval, and apply state machine contracts.

The module owns maintenance-role orchestration only; runtime activation and a
production cold-start origin adapter remain intentionally out of scope.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Protocol
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from src.domain.errors import (
    DatabaseOperationError,
    SyncAuthorizationError,
    SyncContractError,
    SyncCursorInvalidError,
    SyncTransientError,
)
from src.ingestion.command_receipts import (
    CommandReceipt,
    IdempotencyConflict,
    _hash_idempotency_key,
)
from src.ingestion.folder_identity import require_canonical_folder_identity
from src.ingestion.models import (
    MAX_SYNC_CHANGES_PER_BATCH,
    POSTGRES_BIGINT_MAX,
    ChangeKind,
    IngressSource,
    SyncBatch,
    SyncChange,
    SyncCursorStatus,
)
from src.ingestion.normalization import normalize_sync_change
from src.ingestion.policy import FolderScope, PolicySnapshotUnavailableError
from src.ingestion.sync import (
    _ConnectionReturnOutcome,
    _OwnershipSnapshot,
    _SyncSessionLease,
    _SyncSessionRunner,
    _caller_owned_transaction,
    _configure_sync_xid,
    _deterministic_retry_delay,
    _read_current_ownership,
    _trusted_retry_hint,
)


_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_CODE_PATTERN: Final = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}\Z")
_MAX_CURSOR_LENGTH: Final = 8192
_MAX_REDACTED_SAMPLES: Final = 20
_MAX_PLAN_TTL_SECONDS: Final = 7 * 24 * 60 * 60
_MAX_RESOURCE_TIMEOUT_SECONDS: Final = 30
_PREVIEW_COMMAND: Final = "cold_start.preview"
_APPROVE_COMMAND: Final = "cold_start.approve"
_APPLY_PAGE_COMMAND: Final = "cold_start.apply_page"
_PLAN_RESULT_TYPE: Final = "sync_cold_start_plan"
_LOCATOR_RETURNED: Final = object()
_LOCATE_PLAN_SQL: Final = (
    "SELECT plan_id, account_id, folder_key "
    "FROM public.sync_cold_start_plans WHERE plan_id = %s"
)
_PLAN_ROW_KEYS: Final = frozenset(
    {
        "plan_id",
        "account_id",
        "folder_key",
        "expected_cursor_status",
        "expected_cursor",
        "expected_cursor_version",
        "pipeline_name",
        "generation",
        "fencing_token",
        "state",
        "version",
        "preview_cursor",
        "preview_cursor_version",
        "boundary_cursor",
        "boundary_cursor_version",
        "apply_cursor",
        "apply_cursor_version",
        "rolling_hash",
        "page_count",
        "item_count",
        "redacted_samples",
        "contract_fingerprint",
        "folder_scope_config_hash",
        "plan_hash",
        "actor",
        "reason",
        "blocked_reason_code",
        "blocked_fingerprint",
        "expires_at",
        "ready_at",
        "approved_at",
        "completed_at",
        "blocked_at",
        "created_at",
        "updated_at",
    }
)
_PLAN_SELECT_COLUMNS: Final = ", ".join(sorted(_PLAN_ROW_KEYS))
_BLOCKED_REASON_CODES: Final = frozenset(
    {
        "exchange.sync.authorization_failed",
        "exchange.sync.cursor_invalid",
        "exchange.sync.contract_invalid",
        "sync.local_contract_invalid",
        "sync.cursor_stalled",
        "cold_start.expired",
        "cold_start.config_drift",
        "cold_start.fence_drift",
        "cold_start.cursor_drift",
        "cold_start.version_drift",
        "cold_start.plan_hash_drift",
    }
)


class ColdStartPlanNotFoundError(RuntimeError):
    """Fixed-shape privacy-safe absence signal."""

    def __init__(self) -> None:
        super().__init__("cold-start plan not found")


class ColdStartStateConflictError(RuntimeError):
    """Fixed-shape privacy-safe compare-and-swap conflict signal."""

    def __init__(self) -> None:
        super().__init__("cold-start plan state conflict")


class ColdStartOriginPort(Protocol):
    """Explicit provider capability that alone may start from a null cursor."""

    async def fetch_cold_start_page(
        self,
        account_id: int,
        sync_folder: str,
        cursor: str | None,
        limit: int,
    ) -> SyncBatch: ...


def _require_exact_int(
    name: str,
    value: object,
    *,
    minimum: int,
    maximum: int = POSTGRES_BIGINT_MAX,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an exact integer in the supported range")
    return value


def _require_positive_duration(
    name: str,
    value: object,
    *,
    maximum: float | None = None,
) -> int | float:
    if type(value) not in (int, float):
        raise ValueError(f"{name} must be a positive finite built-in number")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        raise ValueError(f"{name} must be a positive finite built-in number") from None
    if not finite or value <= 0:
        raise ValueError(f"{name} must be a positive finite built-in number")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} exceeds the supported maximum")
    return value


def _require_sha256(name: str, value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be an exact lowercase SHA-256 digest")
    return value


def _require_safe_code(name: str, value: object) -> str:
    if type(value) is not str or _SAFE_CODE_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a privacy-safe code")
    return value


def _require_cursor(name: str, value: object) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > _MAX_CURSOR_LENGTH
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F
            for character in value
        )
    ):
        raise ValueError(f"{name} must be an exact non-empty bounded cursor")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain valid Unicode scalar text") from None
    return value


def _require_exact_text(name: str, value: object, *, max_length: int) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > max_length
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F
            for character in value
        )
    ):
        raise ValueError(f"{name} must be exact bounded text")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain valid Unicode scalar text") from None
    return value


def _require_idempotency_key(value: object) -> str:
    if type(value) is not str:
        raise ValueError("idempotency_key must be an exact string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("idempotency_key must contain valid UTF-8 text") from None
    if not 1 <= len(encoded) <= 4096:
        raise ValueError("idempotency_key must contain between 1 and 4096 bytes")
    return value


def _require_optional_cursor(name: str, value: object) -> str | None:
    if value is None:
        return None
    return _require_cursor(name, value)


def _require_utc_datetime(name: str, value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is not UTC:
        raise ValueError(f"{name} must be an exact built-in UTC datetime")
    return value


def _require_optional_utc_datetime(
    name: str,
    value: object,
) -> datetime | None:
    if value is None:
        return None
    return _require_utc_datetime(name, value)


def _normalize_database_datetime(name: str, value: object) -> datetime:
    if type(value) is not datetime:
        raise ValueError(f"{name} must be an exact built-in aware datetime")
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        normalized = value.astimezone(UTC)
    except Exception:
        raise ValueError(f"{name} must be a valid aware datetime") from None
    if type(normalized) is not datetime or normalized.tzinfo is not UTC:
        raise ValueError(f"{name} could not be normalized to exact UTC")
    return normalized


def _normalize_optional_database_datetime(
    name: str,
    value: object,
) -> datetime | None:
    if value is None:
        return None
    return _normalize_database_datetime(name, value)


@dataclass(frozen=True, slots=True)
class _LocatedPlanIdentity:
    plan_id: UUID
    account_id: int
    canonical_folder: str

    def __post_init__(self) -> None:
        _require_uuid("plan_id", self.plan_id)
        _require_exact_int("account_id", self.account_id, minimum=1)
        if type(self.canonical_folder) is not str:
            raise ValueError("canonical_folder must be an exact string")
        require_canonical_folder_identity(self.canonical_folder)


@dataclass(frozen=True, slots=True)
class _LocatorPrimaryOutcome:
    connection: Any
    row: object
    primary_error: BaseException | None
    return_error: BaseException | None
    failure: str | None
    returned: bool


@dataclass(frozen=True, slots=True)
class _ColdStartPlanRecord:
    view: ColdStartPlanView
    expected_cursor_status: SyncCursorStatus
    expected_cursor: str | None
    expected_cursor_version: int
    ownership: _OwnershipSnapshot
    version: int
    preview_cursor: str | None
    preview_cursor_version: int
    boundary_cursor_version: int | None
    apply_cursor: str | None
    apply_cursor_version: int | None
    rolling_hash: str | None
    actor: str
    reason: str


@dataclass(frozen=True, slots=True)
class _PreviewAcceptance:
    plan: _ColdStartPlanRecord
    replayed: bool


@dataclass(frozen=True, slots=True)
class _ApplyCursorRecord:
    cursor: str | None
    status: SyncCursorStatus
    version: int
    blocked_reason_code: str | None
    contract_fingerprint: str | None
    blocked_at: datetime | None
    transient_failures: int
    retry_after_at: datetime | None
    cold_start_plan_id: UUID | None
    cold_start_plan_state: ColdStartPlanState | None
    last_attempt_at: datetime | None
    last_success_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class _AppliedCursorRecord:
    cursor: _ApplyCursorRecord
    last_attempt_at: datetime
    last_success_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class _ApplyPreflight:
    plan: _ColdStartPlanRecord
    cursor: _ApplyCursorRecord | None
    request_cursor: str | None
    payload_hash: str | None
    immediate_status: ColdStartRunStatus | None


@dataclass(frozen=True, slots=True)
class _ApplyPageCommit:
    plan: _ColdStartPlanRecord
    cursor: _ApplyCursorRecord
    receipt: CommandReceipt


@dataclass(frozen=True, slots=True)
class _ApplyCommitEvidence:
    prepared: _ApplyPreflight
    batch: SyncBatch
    events: tuple[Any, ...]
    committed: _ApplyPageCommit
    pages_committed: int
    changes_observed: int


class _ApplyCommitOutcomeUnknown(RuntimeError):
    """Internal retained apply page used only after a tainted commit outcome."""

    __slots__ = ("primary", "evidence")

    def __init__(
        self,
        *,
        primary: Exception,
        evidence: _ApplyCommitEvidence,
    ) -> None:
        super().__init__("cold-start apply commit outcome is unknown")
        self.primary = primary
        self.evidence = evidence


class _PreviewCommitOutcomeUnknown(RuntimeError):
    """Internal retained page used only after a tainted commit outcome."""

    __slots__ = (
        "primary",
        "expected",
        "expected_post",
        "batch",
        "pages_committed",
        "changes_observed",
    )

    def __init__(
        self,
        *,
        primary: Exception,
        expected: _ColdStartPlanRecord,
        expected_post: _ColdStartPlanRecord,
        batch: SyncBatch,
        pages_committed: int,
        changes_observed: int,
    ) -> None:
        super().__init__("cold-start preview commit outcome is unknown")
        self.primary = primary
        self.expected = expected
        self.expected_post = expected_post
        self.batch = batch
        self.pages_committed = pages_committed
        self.changes_observed = changes_observed


class ColdStartPlanState(StrEnum):
    PREVIEWING = "previewing"
    READY = "ready"
    APPROVED = "approved"
    COMPLETED = "completed"
    BLOCKED = "blocked"


class ColdStartRunStatus(StrEnum):
    BUSY_SKIP = "busy_skip"
    BUDGET_EXHAUSTED = "budget_exhausted"
    PREVIEWING = "previewing"
    READY = "ready"
    APPROVED = "approved"
    COMPLETED = "completed"
    BLOCKED = "blocked"
    RETRY_DEFERRED = "retry_deferred"
    RETRY_SCHEDULED = "retry_scheduled"


@dataclass(frozen=True, slots=True)
class ColdStartSample:
    kind: ChangeKind
    external_email_id_hash: str

    def __post_init__(self) -> None:
        if type(self.kind) is not ChangeKind or self.kind not in {
            ChangeKind.CREATE,
            ChangeKind.UPDATE,
            ChangeKind.DELETE,
        }:
            raise ValueError("kind must be an exact redacted ChangeKind")
        _require_sha256("external_email_id_hash", self.external_email_id_hash)


@dataclass(frozen=True, slots=True)
class ColdStartPlanView:
    plan_id: UUID
    account_id: int
    canonical_folder: str
    state: ColdStartPlanState
    boundary_cursor: str | None
    page_count: int
    item_count: int
    redacted_samples: tuple[ColdStartSample, ...]
    contract_fingerprint: str
    folder_scope_config_hash: str
    plan_hash: str | None
    blocked_reason_code: str | None
    blocked_fingerprint: str | None
    expires_at: datetime
    ready_at: datetime | None
    approved_at: datetime | None
    completed_at: datetime | None
    blocked_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if type(self.plan_id) is not UUID:
            raise ValueError("plan_id must be an exact UUID")
        _require_exact_int("account_id", self.account_id, minimum=1)
        if type(self.canonical_folder) is not str:
            raise ValueError("canonical_folder must be an exact string")
        require_canonical_folder_identity(self.canonical_folder)
        if type(self.state) is not ColdStartPlanState:
            raise ValueError("state must be an exact ColdStartPlanState")
        _require_optional_cursor("boundary_cursor", self.boundary_cursor)
        _require_exact_int("page_count", self.page_count, minimum=0)
        _require_exact_int("item_count", self.item_count, minimum=0)
        if (
            type(self.redacted_samples) is not tuple
            or len(self.redacted_samples) > _MAX_REDACTED_SAMPLES
            or len(self.redacted_samples) != min(self.item_count, _MAX_REDACTED_SAMPLES)
        ):
            raise ValueError("redacted_samples must be an exact bounded sample tuple")
        samples: list[ColdStartSample] = []
        for sample in self.redacted_samples:
            if type(sample) is not ColdStartSample:
                raise ValueError(
                    "redacted_samples must contain exact ColdStartSample values"
                )
            samples.append(ColdStartSample(sample.kind, sample.external_email_id_hash))
        object.__setattr__(self, "redacted_samples", tuple(samples))
        if self.page_count == 0 and self.item_count != 0:
            raise ValueError("a zero-page plan cannot contain observed items")
        _require_sha256("contract_fingerprint", self.contract_fingerprint)
        _require_sha256(
            "folder_scope_config_hash",
            self.folder_scope_config_hash,
        )
        if self.plan_hash is not None:
            _require_sha256("plan_hash", self.plan_hash)
        if self.blocked_reason_code is not None:
            _require_safe_code("blocked_reason_code", self.blocked_reason_code)
            if self.blocked_reason_code not in _BLOCKED_REASON_CODES:
                raise ValueError("blocked_reason_code is not a frozen reason code")
        if self.blocked_fingerprint is not None:
            _require_sha256("blocked_fingerprint", self.blocked_fingerprint)

        expires_at = _require_utc_datetime("expires_at", self.expires_at)
        ready_at = _require_optional_utc_datetime("ready_at", self.ready_at)
        approved_at = _require_optional_utc_datetime(
            "approved_at",
            self.approved_at,
        )
        completed_at = _require_optional_utc_datetime(
            "completed_at",
            self.completed_at,
        )
        blocked_at = _require_optional_utc_datetime("blocked_at", self.blocked_at)
        created_at = _require_utc_datetime("created_at", self.created_at)
        updated_at = _require_utc_datetime("updated_at", self.updated_at)
        if expires_at <= created_at or updated_at < created_at:
            raise ValueError("plan timestamps are not chronologically valid")
        for timestamp in (ready_at, approved_at, completed_at, blocked_at):
            if timestamp is not None and not created_at <= timestamp <= updated_at:
                raise ValueError("plan transition timestamp is outside its lifecycle")
        if ready_at is not None and approved_at is not None and approved_at < ready_at:
            raise ValueError("approved_at must not precede ready_at")
        if (
            approved_at is not None
            and completed_at is not None
            and completed_at < approved_at
        ):
            raise ValueError("completed_at must not precede approved_at")
        if any(
            timestamp is not None and timestamp >= expires_at
            for timestamp in (ready_at, approved_at, completed_at)
        ):
            raise ValueError("successful transitions must precede expiry")

        boundary_ready = (
            self.boundary_cursor is not None
            and self.plan_hash is not None
            and ready_at is not None
            and self.page_count >= 1
        )
        boundary_absent = (
            self.boundary_cursor is None and self.plan_hash is None and ready_at is None
        )
        blocked_absent = (
            self.blocked_reason_code is None
            and self.blocked_fingerprint is None
            and blocked_at is None
        )
        blocked_present = (
            self.blocked_reason_code is not None
            and self.blocked_fingerprint is not None
            and blocked_at is not None
        )

        if self.state is ColdStartPlanState.PREVIEWING:
            valid = (
                boundary_absent
                and blocked_absent
                and approved_at is None
                and completed_at is None
            )
        elif self.state is ColdStartPlanState.READY:
            valid = (
                boundary_ready
                and blocked_absent
                and approved_at is None
                and completed_at is None
            )
        elif self.state is ColdStartPlanState.APPROVED:
            valid = (
                boundary_ready
                and blocked_absent
                and approved_at is not None
                and completed_at is None
            )
        elif self.state is ColdStartPlanState.COMPLETED:
            valid = (
                boundary_ready
                and blocked_absent
                and approved_at is not None
                and completed_at is not None
            )
        else:
            valid = (
                blocked_present
                and completed_at is None
                and (
                    (boundary_absent and approved_at is None)
                    or (
                        boundary_ready
                        and blocked_at >= ready_at
                        and (
                            approved_at is None
                            or (approved_at >= ready_at and blocked_at >= approved_at)
                        )
                    )
                )
            )
        if not valid:
            raise ValueError("cold-start plan state fields are inconsistent")


def _rebuild_plan_view(value: ColdStartPlanView) -> ColdStartPlanView:
    return ColdStartPlanView(
        plan_id=value.plan_id,
        account_id=value.account_id,
        canonical_folder=value.canonical_folder,
        state=value.state,
        boundary_cursor=value.boundary_cursor,
        page_count=value.page_count,
        item_count=value.item_count,
        redacted_samples=value.redacted_samples,
        contract_fingerprint=value.contract_fingerprint,
        folder_scope_config_hash=value.folder_scope_config_hash,
        plan_hash=value.plan_hash,
        blocked_reason_code=value.blocked_reason_code,
        blocked_fingerprint=value.blocked_fingerprint,
        expires_at=value.expires_at,
        ready_at=value.ready_at,
        approved_at=value.approved_at,
        completed_at=value.completed_at,
        blocked_at=value.blocked_at,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


@dataclass(frozen=True, slots=True)
class ColdStartRunResult:
    status: ColdStartRunStatus
    plan: ColdStartPlanView | None
    pages_committed: int
    changes_observed: int
    safe_code: str | None

    def __post_init__(self) -> None:
        if type(self.status) is not ColdStartRunStatus:
            raise ValueError("status must be an exact ColdStartRunStatus")
        if self.plan is not None:
            if type(self.plan) is not ColdStartPlanView:
                raise ValueError("plan must be an exact ColdStartPlanView")
            object.__setattr__(self, "plan", _rebuild_plan_view(self.plan))
        _require_exact_int("pages_committed", self.pages_committed, minimum=0)
        _require_exact_int("changes_observed", self.changes_observed, minimum=0)
        if self.safe_code is not None:
            _require_safe_code("safe_code", self.safe_code)

        expected_plan_states = {
            ColdStartRunStatus.PREVIEWING: ColdStartPlanState.PREVIEWING,
            ColdStartRunStatus.READY: ColdStartPlanState.READY,
            ColdStartRunStatus.APPROVED: ColdStartPlanState.APPROVED,
            ColdStartRunStatus.COMPLETED: ColdStartPlanState.COMPLETED,
            ColdStartRunStatus.BLOCKED: ColdStartPlanState.BLOCKED,
        }
        if self.status is ColdStartRunStatus.BUSY_SKIP:
            valid = (
                self.plan is None
                and self.pages_committed == 0
                and self.changes_observed == 0
                and self.safe_code == "cold_start.busy"
            )
        elif self.plan is None:
            valid = False
        elif self.status in expected_plan_states:
            valid = self.plan.state is expected_plan_states[self.status] and (
                self.safe_code == self.plan.blocked_reason_code
                if self.status is ColdStartRunStatus.BLOCKED
                else self.safe_code is None
            )
        elif self.status is ColdStartRunStatus.BUDGET_EXHAUSTED:
            valid = (
                self.plan.state
                in {ColdStartPlanState.PREVIEWING, ColdStartPlanState.APPROVED}
                and self.safe_code == "cold_start.budget_exhausted"
            )
        elif self.status is ColdStartRunStatus.RETRY_DEFERRED:
            valid = (
                self.plan.state is ColdStartPlanState.APPROVED
                and self.pages_committed == 0
                and self.changes_observed == 0
                and self.safe_code == "cold_start.retry_deferred"
            )
        else:
            valid = (
                self.status is ColdStartRunStatus.RETRY_SCHEDULED
                and self.plan.state is ColdStartPlanState.APPROVED
                and self.safe_code == "exchange.sync.transient_failure"
            )
        if not valid:
            raise ValueError("cold-start run result fields are inconsistent")


class ColdStartService:
    """Batch-4 boundary with a private locator; state operations land later."""

    def __init__(
        self,
        *,
        cold_start_origin: Any,
        ordinary_page_client: Any,
        snapshot_provider: Any,
        policy_resolver: Any,
        folder_permit: Any,
        maintenance_pool: Any,
        inbox_repository: Any,
        receipt_repository: Any,
        page_limit: int,
        preview_max_pages: int,
        preview_max_run_seconds: float,
        apply_max_pages: int,
        apply_max_run_seconds: float,
        plan_ttl_seconds: int,
        locator_timeout: float,
        cleanup_timeout: float,
        contract_fingerprint: str,
    ) -> None:
        _require_exact_int(
            "page_limit",
            page_limit,
            minimum=1,
            maximum=MAX_SYNC_CHANGES_PER_BATCH,
        )
        _require_exact_int("preview_max_pages", preview_max_pages, minimum=1)
        _require_positive_duration(
            "preview_max_run_seconds",
            preview_max_run_seconds,
        )
        _require_exact_int("apply_max_pages", apply_max_pages, minimum=1)
        _require_positive_duration(
            "apply_max_run_seconds",
            apply_max_run_seconds,
        )
        _require_exact_int(
            "plan_ttl_seconds",
            plan_ttl_seconds,
            minimum=1,
            maximum=_MAX_PLAN_TTL_SECONDS,
        )
        _require_positive_duration(
            "locator_timeout",
            locator_timeout,
            maximum=_MAX_RESOURCE_TIMEOUT_SECONDS,
        )
        _require_positive_duration(
            "cleanup_timeout",
            cleanup_timeout,
            maximum=_MAX_RESOURCE_TIMEOUT_SECONDS,
        )
        _require_sha256("contract_fingerprint", contract_fingerprint)
        self._cold_start_origin = cold_start_origin
        self._ordinary_page_client = ordinary_page_client
        self._snapshot_provider = snapshot_provider
        self._policy_resolver = policy_resolver
        self._folder_permit = folder_permit
        self._maintenance_pool = maintenance_pool
        self._inbox_repository = inbox_repository
        self._receipt_repository = receipt_repository
        self._page_limit = page_limit
        self._preview_max_pages = preview_max_pages
        self._preview_max_run_seconds = preview_max_run_seconds
        self._apply_max_pages = apply_max_pages
        self._apply_max_run_seconds = apply_max_run_seconds
        self._plan_ttl_seconds = plan_ttl_seconds
        self._locator_timeout = locator_timeout
        self._cleanup_timeout = cleanup_timeout
        self._contract_fingerprint = contract_fingerprint
        self._session_runner = _SyncSessionRunner(
            pool=maintenance_pool,
            permit=folder_permit,
            cleanup_timeout=cleanup_timeout,
        )

    async def _ready_scope(
        self,
        account_id: int,
        canonical_folder: str,
    ) -> tuple[Any, object]:
        try:
            snapshot = await self._snapshot_provider.get_ready_snapshot(account_id)
            scopes = self._policy_resolver.configured_scopes(snapshot)
        except PolicySnapshotUnavailableError:
            raise
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            raise PolicySnapshotUnavailableError() from None
        scope = next(
            (
                candidate
                for candidate in scopes
                if candidate.canonical_key == canonical_folder
            ),
            None,
        )
        if scope is None:
            raise PolicySnapshotUnavailableError()
        return scope, snapshot

    async def preview(
        self,
        account_id: int,
        folder: str,
        *,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> ColdStartRunResult:
        exact_account_id = _require_exact_int("account_id", account_id, minimum=1)
        canonical_folder = require_canonical_folder_identity(folder)
        exact_actor = _require_exact_text("actor", actor, max_length=128)
        exact_reason = _require_exact_text("reason", reason, max_length=512)
        exact_idempotency_key = _require_idempotency_key(idempotency_key)
        scope, snapshot = await self._ready_scope(
            exact_account_id,
            canonical_folder,
        )
        if type(scope) is not FolderScope:
            raise PolicySnapshotUnavailableError()

        async def operation(session: _SyncSessionLease) -> ColdStartRunResult:
            async def accept(connection: Any) -> _PreviewAcceptance:
                return await self._accept_preview(
                    connection,
                    account_id=exact_account_id,
                    canonical_folder=canonical_folder,
                    scope=scope,
                    actor=exact_actor,
                    reason=exact_reason,
                    idempotency_key=exact_idempotency_key,
                )

            acceptance = await _caller_owned_transaction(session, accept)
            if acceptance.replayed:
                return _cold_start_result_from_plan(acceptance.plan.view)
            return await self._resume_preview_locked(
                session,
                scope,
                snapshot,
                acceptance.plan,
            )

        try:
            outcome = await self._session_runner.run(
                exact_account_id,
                canonical_folder,
                operation,
            )
        except _PreviewCommitOutcomeUnknown as unknown:
            return await self._recover_preview_commit(unknown)
        if not outcome.acquired:
            return ColdStartRunResult(
                status=ColdStartRunStatus.BUSY_SKIP,
                plan=None,
                pages_committed=0,
                changes_observed=0,
                safe_code="cold_start.busy",
            )
        if type(outcome.value) is not ColdStartRunResult:
            raise _locator_database_error(
                "cold_start_result",
                retryable=False,
                message="cold-start result is invalid",
            )
        return outcome.value

    async def resume(self, plan_id: UUID) -> ColdStartRunResult:
        exact_plan_id = _require_uuid("plan_id", plan_id)
        identity = await self._locate_plan_identity(exact_plan_id)
        if type(identity) is not _LocatedPlanIdentity:
            raise _locator_database_error(
                "cold_start_locator_row",
                retryable=False,
                message="cold-start locator row is invalid",
            )
        scope, snapshot = await self._ready_scope(
            identity.account_id,
            identity.canonical_folder,
        )
        if type(scope) is not FolderScope:
            raise PolicySnapshotUnavailableError()

        async def operation(session: _SyncSessionLease) -> ColdStartRunResult:
            async def load(connection: Any) -> _ColdStartPlanRecord:
                return await self._load_resume_preview(
                    connection,
                    identity=identity,
                    scope=scope,
                )

            plan = await _caller_owned_transaction(session, load)
            if plan.view.state is ColdStartPlanState.BLOCKED:
                return _preview_blocked_result(
                    plan.view,
                    pages_committed=0,
                    changes_observed=0,
                )
            return await self._resume_preview_locked(
                session,
                scope,
                snapshot,
                plan,
            )

        try:
            outcome = await self._session_runner.run(
                identity.account_id,
                identity.canonical_folder,
                operation,
            )
        except _PreviewCommitOutcomeUnknown as unknown:
            return await self._recover_preview_commit(unknown)
        if not outcome.acquired:
            return ColdStartRunResult(
                status=ColdStartRunStatus.BUSY_SKIP,
                plan=None,
                pages_committed=0,
                changes_observed=0,
                safe_code="cold_start.busy",
            )
        if type(outcome.value) is not ColdStartRunResult:
            raise _locator_database_error(
                "cold_start_result",
                retryable=False,
                message="cold-start result is invalid",
            )
        return outcome.value

    async def approve(
        self,
        plan_id: UUID,
        *,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> ColdStartRunResult:
        exact_plan_id = _require_uuid("plan_id", plan_id)
        exact_actor = _require_exact_text("actor", actor, max_length=128)
        exact_reason = _require_exact_text("reason", reason, max_length=512)
        exact_idempotency_key = _require_idempotency_key(idempotency_key)
        identity = await self._locate_plan_identity(exact_plan_id)
        if type(identity) is not _LocatedPlanIdentity:
            raise _locator_database_error(
                "cold_start_locator_row",
                retryable=False,
                message="cold-start locator row is invalid",
            )
        scope, _snapshot = await self._ready_scope(
            identity.account_id,
            identity.canonical_folder,
        )
        if type(scope) is not FolderScope:
            raise PolicySnapshotUnavailableError()

        async def operation(session: _SyncSessionLease) -> ColdStartRunResult:
            async def approve_plan(connection: Any) -> _ColdStartPlanRecord:
                return await self._approve_plan(
                    connection,
                    identity=identity,
                    scope=scope,
                    actor=exact_actor,
                    reason=exact_reason,
                    idempotency_key=exact_idempotency_key,
                )

            plan = await _caller_owned_transaction(session, approve_plan)
            return _cold_start_result_from_plan(plan.view)

        outcome = await self._session_runner.run(
            identity.account_id,
            identity.canonical_folder,
            operation,
        )
        if not outcome.acquired:
            return ColdStartRunResult(
                status=ColdStartRunStatus.BUSY_SKIP,
                plan=None,
                pages_committed=0,
                changes_observed=0,
                safe_code="cold_start.busy",
            )
        if type(outcome.value) is not ColdStartRunResult:
            raise _locator_database_error(
                "cold_start_result",
                retryable=False,
                message="cold-start result is invalid",
            )
        return outcome.value

    async def apply(self, plan_id: UUID) -> ColdStartRunResult:
        exact_plan_id = _require_uuid("plan_id", plan_id)
        identity = await self._locate_plan_identity(exact_plan_id)
        if type(identity) is not _LocatedPlanIdentity:
            raise _locator_database_error(
                "cold_start_locator_row",
                retryable=False,
                message="cold-start locator row is invalid",
            )
        scope, snapshot = await self._ready_scope(
            identity.account_id,
            identity.canonical_folder,
        )
        if type(scope) is not FolderScope:
            raise PolicySnapshotUnavailableError()

        async def operation(session: _SyncSessionLease) -> ColdStartRunResult:
            return await self._run_apply_locked(
                session,
                identity=identity,
                scope=scope,
                _snapshot=snapshot,
            )

        try:
            outcome = await self._session_runner.run(
                identity.account_id,
                identity.canonical_folder,
                operation,
            )
        except _ApplyCommitOutcomeUnknown as unknown:
            return await self._recover_apply_commit(unknown)
        if not outcome.acquired:
            return ColdStartRunResult(
                status=ColdStartRunStatus.BUSY_SKIP,
                plan=None,
                pages_committed=0,
                changes_observed=0,
                safe_code="cold_start.busy",
            )
        if type(outcome.value) is not ColdStartRunResult:
            raise _locator_database_error(
                "cold_start_result",
                retryable=False,
                message="cold-start result is invalid",
            )
        return outcome.value

    async def _run_apply_locked(
        self,
        session: _SyncSessionLease,
        *,
        identity: _LocatedPlanIdentity,
        scope: FolderScope,
        _snapshot: object,
    ) -> ColdStartRunResult:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._apply_max_run_seconds
        pages_committed = 0
        changes_observed = 0
        current_scope = scope
        current_plan: _ColdStartPlanRecord | None = None

        while pages_committed < self._apply_max_pages:
            if pages_committed:
                current_scope, _ = await self._ready_scope(
                    identity.account_id,
                    identity.canonical_folder,
                )
                if type(current_scope) is not FolderScope:
                    raise PolicySnapshotUnavailableError()

            async def preflight(connection: Any) -> _ApplyPreflight:
                return await self._preflight_apply_page(
                    connection,
                    identity=identity,
                    scope=current_scope,
                )

            prepared = await _caller_owned_transaction(session, preflight)
            current_plan = prepared.plan
            if prepared.immediate_status is not None:
                if prepared.immediate_status is ColdStartRunStatus.COMPLETED:
                    return _cold_start_result_from_plan(current_plan.view)
                if prepared.immediate_status is ColdStartRunStatus.BLOCKED:
                    blocked_code = current_plan.view.blocked_reason_code
                    if blocked_code is None:
                        raise _apply_invariant("cold_start_apply_preflight")
                    return ColdStartRunResult(
                        status=ColdStartRunStatus.BLOCKED,
                        plan=current_plan.view,
                        pages_committed=pages_committed,
                        changes_observed=changes_observed,
                        safe_code=blocked_code,
                    )
                if prepared.immediate_status is ColdStartRunStatus.RETRY_DEFERRED:
                    return _apply_retry_deferred_result(current_plan.view)
                raise _apply_invariant("cold_start_apply_preflight")
            if prepared.request_cursor is None:
                raise _apply_invariant("cold_start_apply_preflight")

            remaining = deadline - loop.time()
            if remaining <= 0:
                return _apply_budget_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            try:
                async with asyncio.timeout(remaining):
                    batch = await _fetch_ordinary_page(
                        self._ordinary_page_client,
                        identity.account_id,
                        current_scope.sync_folder,
                        prepared.request_cursor,
                        self._page_limit,
                    )
            except TimeoutError:
                return _apply_budget_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncTransientError as error:
                if (
                    prepared.cursor is None
                    or prepared.cursor.status
                    is not SyncCursorStatus.COLD_START_APPLYING
                ):
                    raise
                retry_after_seconds = _trusted_retry_hint(error)
                retry_scope, _ = await self._ready_scope(
                    identity.account_id,
                    identity.canonical_folder,
                )
                if type(retry_scope) is not FolderScope:
                    raise PolicySnapshotUnavailableError()

                async def schedule_retry(connection: Any) -> _ColdStartPlanRecord:
                    return await self._schedule_apply_retry(
                        connection,
                        scope=retry_scope,
                        prepared=prepared,
                        retry_after_seconds=retry_after_seconds,
                    )

                current_plan = await _caller_owned_transaction(
                    session,
                    schedule_retry,
                )
                if current_plan.view.state is ColdStartPlanState.BLOCKED:
                    blocked_code = current_plan.view.blocked_reason_code
                    if blocked_code is None:
                        raise _apply_invariant("cold_start_apply_retry")
                    return ColdStartRunResult(
                        status=ColdStartRunStatus.BLOCKED,
                        plan=current_plan.view,
                        pages_committed=pages_committed,
                        changes_observed=changes_observed,
                        safe_code=blocked_code,
                    )
                if current_plan.view.state is not ColdStartPlanState.APPROVED:
                    raise _apply_invariant("cold_start_apply_retry")
                return ColdStartRunResult(
                    status=ColdStartRunStatus.RETRY_SCHEDULED,
                    plan=current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                    safe_code="exchange.sync.transient_failure",
                )
            except SyncAuthorizationError:
                return await self._block_apply_locked(
                    session,
                    prepared=prepared,
                    safe_code="exchange.sync.authorization_failed",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncCursorInvalidError:
                return await self._block_apply_locked(
                    session,
                    prepared=prepared,
                    safe_code="exchange.sync.cursor_invalid",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncContractError:
                return await self._block_apply_locked(
                    session,
                    prepared=prepared,
                    safe_code="exchange.sync.contract_invalid",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except ValueError:
                return await self._block_apply_locked(
                    session,
                    prepared=prepared,
                    safe_code="sync.local_contract_invalid",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

            if not batch.includes_last and batch.cursor == prepared.request_cursor:
                return await self._block_apply_locked(
                    session,
                    prepared=prepared,
                    safe_code="sync.cursor_stalled",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

            post_scope, post_snapshot = await self._ready_scope(
                identity.account_id,
                identity.canonical_folder,
            )
            if type(post_scope) is not FolderScope:
                raise PolicySnapshotUnavailableError()
            policies = tuple(
                self._policy_resolver.resolve(
                    IngressSource.SYNC,
                    change.kind.value,
                    change.kind,
                    post_scope.sync_folder,
                    post_snapshot,
                )
                for change in batch.changes
            )
            try:
                events = tuple(
                    normalize_sync_change(
                        identity.account_id,
                        identity.canonical_folder,
                        batch.cursor,
                        change,
                        processing_policy=processing_policy,
                    )
                    for change, processing_policy in zip(
                        batch.changes,
                        policies,
                        strict=True,
                    )
                )
            except ValueError:
                return await self._block_apply_locked(
                    session,
                    prepared=prepared,
                    safe_code="sync.local_contract_invalid",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

            commit_projection: list[_ApplyPageCommit] = []

            async def commit(
                connection: Any,
            ) -> _ApplyPageCommit | _ColdStartPlanRecord:
                committed = await self._commit_apply_page(
                    connection,
                    scope=post_scope,
                    prepared=prepared,
                    batch=batch,
                    events=events,
                )
                if type(committed) is _ApplyPageCommit:
                    commit_projection.append(committed)
                elif not (
                    type(committed) is _ColdStartPlanRecord
                    and committed.view.state is ColdStartPlanState.BLOCKED
                ):
                    raise _apply_invariant("cold_start_apply_page")
                return committed

            try:
                committed_page = await _caller_owned_transaction(session, commit)
            except Exception as error:
                if session.tainted and len(commit_projection) == 1:
                    raise _ApplyCommitOutcomeUnknown(
                        primary=error,
                        evidence=_ApplyCommitEvidence(
                            prepared=prepared,
                            batch=batch,
                            events=events,
                            committed=commit_projection[0],
                            pages_committed=pages_committed,
                            changes_observed=changes_observed,
                        ),
                    ) from error
                raise
            current_plan = (
                committed_page.plan
                if type(committed_page) is _ApplyPageCommit
                else committed_page
            )
            if current_plan.view.state is ColdStartPlanState.BLOCKED:
                blocked_code = current_plan.view.blocked_reason_code
                if blocked_code is None:
                    raise _apply_invariant("cold_start_apply_page")
                return ColdStartRunResult(
                    status=ColdStartRunStatus.BLOCKED,
                    plan=current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                    safe_code=blocked_code,
                )
            pages_committed += 1
            changes_observed += len(batch.changes)
            if current_plan.view.state is ColdStartPlanState.COMPLETED:
                return ColdStartRunResult(
                    status=ColdStartRunStatus.COMPLETED,
                    plan=current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                    safe_code=None,
                )
            if current_plan.view.state is not ColdStartPlanState.APPROVED:
                raise _apply_invariant("cold_start_apply_page")
            current_scope = post_scope
            if loop.time() >= deadline:
                return _apply_budget_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

        if current_plan is None:
            raise _apply_invariant("cold_start_apply_result")
        return _apply_budget_result(
            current_plan.view,
            pages_committed=pages_committed,
            changes_observed=changes_observed,
        )

    async def _preflight_apply_page(
        self,
        connection: Any,
        *,
        identity: _LocatedPlanIdentity,
        scope: FolderScope,
    ) -> _ApplyPreflight:
        await _configure_sync_xid(connection, identity.account_id)
        try:
            current = await _read_cold_start_plan(connection, identity.plan_id)
        except ValueError:
            raise ColdStartStateConflictError() from None
        if current is None:
            raise ColdStartPlanNotFoundError()
        if not _apply_plan_identity_matches(current, identity):
            raise ColdStartStateConflictError()
        if current.view.state is ColdStartPlanState.COMPLETED:
            _validate_completed_apply_plan(current)
            return _ApplyPreflight(
                plan=current,
                cursor=None,
                request_cursor=None,
                payload_hash=None,
                immediate_status=ColdStartRunStatus.COMPLETED,
            )
        if current.view.state is not ColdStartPlanState.APPROVED:
            raise ColdStartStateConflictError()

        ownership = await _read_current_ownership(
            connection,
            identity.account_id,
            for_key_share=False,
        )
        cursor = await _read_apply_cursor(
            connection,
            identity.account_id,
            identity.canonical_folder,
        )
        database_now = await _read_database_now(connection)
        drift_code = _apply_drift_code(
            current,
            identity=identity,
            cursor=cursor,
            ownership=ownership,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_now,
        )
        if drift_code is not None:
            blocked = await self._write_cold_start_block(
                connection,
                current=current,
                cursor_binding=cursor,
                database_now=database_now,
                safe_code=drift_code,
            )
            return _ApplyPreflight(
                plan=blocked,
                cursor=None,
                request_cursor=None,
                payload_hash=None,
                immediate_status=ColdStartRunStatus.BLOCKED,
            )
        request_cursor = _validate_apply_prestate(
            current,
            cursor=cursor,
            ownership=ownership,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_now,
        )
        if request_cursor is None:
            return _ApplyPreflight(
                plan=current,
                cursor=cursor,
                request_cursor=None,
                payload_hash=None,
                immediate_status=ColdStartRunStatus.RETRY_DEFERRED,
            )
        payload_hash = _apply_page_payload_digest(
            account_id=identity.account_id,
            canonical_folder=identity.canonical_folder,
            plan_id=identity.plan_id,
            plan_version=current.version,
            cursor_status=cursor.status,
            cursor_version=cursor.version,
            request_cursor_hash=_cursor_digest(request_cursor),
        )
        receipt_transaction = self._receipt_repository.transaction(connection)
        receipt = await receipt_transaction.lookup(
            account_id=identity.account_id,
            command_name=_APPLY_PAGE_COMMAND,
            idempotency_key=payload_hash,
            canonical_payload_hash=payload_hash,
        )
        if receipt is not None:
            _validate_apply_receipt_identity(
                receipt,
                plan=current,
                payload_hash=payload_hash,
            )
            raise _apply_invariant("cold_start_apply_preflight")
        return _ApplyPreflight(
            plan=current,
            cursor=cursor,
            request_cursor=request_cursor,
            payload_hash=payload_hash,
            immediate_status=None,
        )

    async def _block_apply_locked(
        self,
        session: _SyncSessionLease,
        *,
        prepared: _ApplyPreflight,
        safe_code: str,
        pages_committed: int,
        changes_observed: int,
    ) -> ColdStartRunResult:
        expected = prepared.plan
        scope, _ = await self._ready_scope(
            expected.view.account_id,
            expected.view.canonical_folder,
        )
        if type(scope) is not FolderScope:
            raise PolicySnapshotUnavailableError()

        async def operation(connection: Any) -> _ColdStartPlanRecord:
            return await self._block_apply(
                connection,
                scope=scope,
                prepared=prepared,
                safe_code=safe_code,
            )

        blocked = await _caller_owned_transaction(session, operation)
        blocked_code = blocked.view.blocked_reason_code
        if blocked_code is None:
            raise ColdStartStateConflictError()
        return ColdStartRunResult(
            status=ColdStartRunStatus.BLOCKED,
            plan=blocked.view,
            pages_committed=pages_committed,
            changes_observed=changes_observed,
            safe_code=blocked_code,
        )

    async def _block_apply(
        self,
        connection: Any,
        *,
        scope: FolderScope,
        prepared: _ApplyPreflight,
        safe_code: str,
    ) -> _ColdStartPlanRecord:
        expected_plan = prepared.plan
        expected_cursor = prepared.cursor
        payload_hash = prepared.payload_hash
        if (
            expected_cursor is None
            or prepared.request_cursor is None
            or payload_hash is None
            or prepared.immediate_status is not None
        ):
            raise _apply_invariant("cold_start_apply_block")

        await _configure_sync_xid(connection, expected_plan.view.account_id)
        ownership = await _read_current_ownership(
            connection,
            expected_plan.view.account_id,
            for_key_share=False,
        )
        current_cursor = await _read_apply_cursor(
            connection,
            expected_plan.view.account_id,
            expected_plan.view.canonical_folder,
        )
        try:
            current_plan = await _read_cold_start_plan(
                connection,
                expected_plan.view.plan_id,
            )
        except ValueError:
            raise ColdStartStateConflictError() from None
        if current_plan is None:
            raise ColdStartPlanNotFoundError()
        database_stamp = await _read_database_now(connection)
        identity = _LocatedPlanIdentity(
            plan_id=expected_plan.view.plan_id,
            account_id=expected_plan.view.account_id,
            canonical_folder=expected_plan.view.canonical_folder,
        )
        if (
            not _apply_plan_identity_matches(current_plan, identity)
            or current_plan.view.state is not ColdStartPlanState.APPROVED
        ):
            raise ColdStartStateConflictError()

        receipt_transaction = self._receipt_repository.transaction(connection)
        receipt = await receipt_transaction.lookup(
            account_id=expected_plan.view.account_id,
            command_name=_APPLY_PAGE_COMMAND,
            idempotency_key=payload_hash,
            canonical_payload_hash=payload_hash,
        )
        if receipt is not None:
            _validate_apply_receipt_identity(
                receipt,
                plan=expected_plan,
                payload_hash=payload_hash,
            )
            raise _apply_invariant("cold_start_apply_block")

        drift_code = _apply_drift_code(
            current_plan,
            identity=identity,
            cursor=current_cursor,
            ownership=ownership,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_stamp,
            expected=expected_plan,
            expected_cursor=expected_cursor,
        )
        if drift_code is None:
            if current_plan != expected_plan or current_cursor != expected_cursor:
                raise ColdStartStateConflictError()
            request_cursor = _validate_apply_prestate(
                current_plan,
                cursor=current_cursor,
                ownership=ownership,
                scope=scope,
                contract_fingerprint=self._contract_fingerprint,
                database_now=database_stamp,
            )
            if request_cursor != prepared.request_cursor:
                raise ColdStartStateConflictError()

        return await self._write_cold_start_block(
            connection,
            current=current_plan,
            cursor_binding=current_cursor,
            database_now=database_stamp,
            safe_code=drift_code or safe_code,
        )

    async def _schedule_apply_retry(
        self,
        connection: Any,
        *,
        scope: FolderScope,
        prepared: _ApplyPreflight,
        retry_after_seconds: int | None,
    ) -> _ColdStartPlanRecord:
        expected_plan = prepared.plan
        expected_cursor = prepared.cursor
        if (
            expected_cursor is None
            or expected_cursor.status is not SyncCursorStatus.COLD_START_APPLYING
            or prepared.request_cursor is None
            or prepared.payload_hash is None
            or prepared.immediate_status is not None
        ):
            raise _apply_invariant("cold_start_apply_retry")

        await _configure_sync_xid(connection, expected_plan.view.account_id)
        ownership = await _read_current_ownership(
            connection,
            expected_plan.view.account_id,
            for_key_share=False,
        )
        current_cursor = await _read_apply_cursor(
            connection,
            expected_plan.view.account_id,
            expected_plan.view.canonical_folder,
        )
        try:
            current_plan = await _read_cold_start_plan(
                connection,
                expected_plan.view.plan_id,
            )
        except ValueError:
            raise ColdStartStateConflictError() from None
        if current_plan is None:
            raise ColdStartPlanNotFoundError()
        database_stamp = await _read_database_now(connection)
        identity = _LocatedPlanIdentity(
            plan_id=expected_plan.view.plan_id,
            account_id=expected_plan.view.account_id,
            canonical_folder=expected_plan.view.canonical_folder,
        )
        receipt_transaction = self._receipt_repository.transaction(connection)
        receipt = await receipt_transaction.lookup(
            account_id=expected_plan.view.account_id,
            command_name=_APPLY_PAGE_COMMAND,
            idempotency_key=prepared.payload_hash,
            canonical_payload_hash=prepared.payload_hash,
        )
        if receipt is not None:
            _validate_apply_receipt_identity(
                receipt,
                plan=expected_plan,
                payload_hash=prepared.payload_hash,
            )
            raise _apply_invariant("cold_start_apply_retry")

        drift_code = _apply_drift_code(
            current_plan,
            identity=identity,
            cursor=current_cursor,
            ownership=ownership,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_stamp,
            expected=expected_plan,
            expected_cursor=expected_cursor,
        )
        if drift_code is not None:
            return await self._write_cold_start_block(
                connection,
                current=current_plan,
                cursor_binding=current_cursor,
                database_now=database_stamp,
                safe_code=drift_code,
            )
        if current_plan != expected_plan or current_cursor != expected_cursor:
            raise ColdStartStateConflictError()
        request_cursor = _validate_apply_prestate(
            current_plan,
            cursor=current_cursor,
            ownership=ownership,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_stamp,
        )
        if request_cursor != prepared.request_cursor:
            raise ColdStartStateConflictError()

        failure_count = current_cursor.transient_failures + 1
        retry_delay_seconds = _deterministic_retry_delay(
            account_id=current_plan.view.account_id,
            canonical_folder=current_plan.view.canonical_folder,
            expected_version=current_cursor.version,
            failure_count=failure_count,
            retry_after_seconds=retry_after_seconds,
        )
        updated = await connection.execute(
            "UPDATE public.sync_cursors AS cursor SET "
            "transient_failures = %(failure_count)s, "
            "retry_after_at = %(database_stamp)s + "
            "pg_catalog.make_interval(secs => %(retry_delay_seconds)s), "
            "last_attempt_at = %(database_stamp)s, "
            "updated_at = %(database_stamp)s "
            "WHERE cursor.account_id = %(account_id)s "
            "AND cursor.folder_key = %(folder_key)s "
            "AND cursor.status = %(expected_status)s "
            "AND cursor.cursor IS NOT DISTINCT FROM %(expected_cursor)s "
            "AND cursor.version = %(expected_version)s "
            "AND cursor.blocked_reason_code IS NOT DISTINCT FROM "
            "%(expected_blocked_reason_code)s "
            "AND cursor.contract_fingerprint IS NOT DISTINCT FROM "
            "%(expected_contract_fingerprint)s "
            "AND cursor.blocked_at IS NOT DISTINCT FROM %(expected_blocked_at)s "
            "AND cursor.transient_failures = %(expected_failures)s "
            "AND cursor.retry_after_at IS NOT DISTINCT FROM "
            "%(expected_retry_after_at)s "
            "AND cursor.cold_start_plan_id = %(expected_plan_id)s "
            "AND cursor.cold_start_plan_state = %(expected_plan_state)s "
            "AND cursor.last_attempt_at IS NOT DISTINCT FROM "
            "%(expected_last_attempt_at)s "
            "AND cursor.last_success_at IS NOT DISTINCT FROM "
            "%(expected_last_success_at)s "
            "AND cursor.updated_at = %(expected_updated_at)s RETURNING "
            "cursor, status, version, blocked_reason_code, "
            "contract_fingerprint, blocked_at, transient_failures, retry_after_at, "
            "cold_start_plan_id, cold_start_plan_state, last_attempt_at, "
            "last_success_at, updated_at",
            {
                "failure_count": failure_count,
                "retry_delay_seconds": retry_delay_seconds,
                "database_stamp": database_stamp,
                "account_id": current_plan.view.account_id,
                "folder_key": current_plan.view.canonical_folder,
                "expected_status": current_cursor.status.value,
                "expected_cursor": current_cursor.cursor,
                "expected_version": current_cursor.version,
                "expected_blocked_reason_code": current_cursor.blocked_reason_code,
                "expected_contract_fingerprint": current_cursor.contract_fingerprint,
                "expected_blocked_at": current_cursor.blocked_at,
                "expected_failures": current_cursor.transient_failures,
                "expected_retry_after_at": current_cursor.retry_after_at,
                "expected_plan_id": current_cursor.cold_start_plan_id,
                "expected_plan_state": (
                    None
                    if current_cursor.cold_start_plan_state is None
                    else current_cursor.cold_start_plan_state.value
                ),
                "expected_last_attempt_at": current_cursor.last_attempt_at,
                "expected_last_success_at": current_cursor.last_success_at,
                "expected_updated_at": current_cursor.updated_at,
            },
        )
        row = await updated.fetchone()
        if row is None:
            raise ColdStartStateConflictError()
        applied_cursor = _applied_cursor_from_row(row)
        _validate_scheduled_retry_cursor(
            applied_cursor,
            previous=current_cursor,
            failure_count=failure_count,
            retry_delay_seconds=retry_delay_seconds,
            database_stamp=database_stamp,
        )
        return current_plan

    async def _commit_apply_page(
        self,
        connection: Any,
        *,
        scope: FolderScope,
        prepared: _ApplyPreflight,
        batch: SyncBatch,
        events: tuple[Any, ...],
    ) -> _ApplyPageCommit | _ColdStartPlanRecord:
        expected_plan = prepared.plan
        expected_cursor = prepared.cursor
        payload_hash = prepared.payload_hash
        if (
            expected_cursor is None
            or prepared.request_cursor is None
            or payload_hash is None
            or prepared.immediate_status is not None
        ):
            raise _apply_invariant("cold_start_apply_page")
        await _configure_sync_xid(connection, expected_plan.view.account_id)
        ownership = await _read_current_ownership(
            connection,
            expected_plan.view.account_id,
            for_key_share=False,
        )
        current_cursor = await _read_apply_cursor(
            connection,
            expected_plan.view.account_id,
            expected_plan.view.canonical_folder,
        )
        try:
            current_plan = await _read_cold_start_plan(
                connection,
                expected_plan.view.plan_id,
            )
        except ValueError:
            raise ColdStartStateConflictError() from None
        if current_plan is None:
            raise ColdStartPlanNotFoundError()
        database_stamp = await _read_database_now(connection)
        identity = _LocatedPlanIdentity(
            plan_id=expected_plan.view.plan_id,
            account_id=expected_plan.view.account_id,
            canonical_folder=expected_plan.view.canonical_folder,
        )
        if (
            not _apply_plan_identity_matches(current_plan, identity)
            or current_plan.view.state is not ColdStartPlanState.APPROVED
        ):
            raise ColdStartStateConflictError()

        receipt_transaction = self._receipt_repository.transaction(connection)
        receipt = await receipt_transaction.lookup(
            account_id=expected_plan.view.account_id,
            command_name=_APPLY_PAGE_COMMAND,
            idempotency_key=payload_hash,
            canonical_payload_hash=payload_hash,
        )
        if receipt is not None:
            _validate_apply_receipt_identity(
                receipt,
                plan=expected_plan,
                payload_hash=payload_hash,
            )
            raise _apply_invariant("cold_start_apply_page")

        drift_code = _apply_drift_code(
            current_plan,
            identity=identity,
            cursor=current_cursor,
            ownership=ownership,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_stamp,
            expected=expected_plan,
            expected_cursor=expected_cursor,
        )
        if drift_code is not None:
            return await self._write_cold_start_block(
                connection,
                current=current_plan,
                cursor_binding=current_cursor,
                database_now=database_stamp,
                safe_code=drift_code,
            )
        if current_plan != expected_plan or current_cursor != expected_cursor:
            raise ColdStartStateConflictError()
        request_cursor = _validate_apply_prestate(
            current_plan,
            cursor=current_cursor,
            ownership=ownership,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_stamp,
        )
        if request_cursor != prepared.request_cursor:
            raise ColdStartStateConflictError()

        inbox = self._inbox_repository.transaction(
            connection,
            for_key_share=False,
        )
        for event in events:
            await inbox.insert(
                event,
                current_plan.ownership.generation,
                current_plan.ownership.fencing_token,
            )

        terminal = batch.includes_last
        target_cursor_status = (
            SyncCursorStatus.ACTIVE
            if terminal
            else SyncCursorStatus.COLD_START_APPLYING
        )
        target_plan_state = (
            ColdStartPlanState.COMPLETED if terminal else ColdStartPlanState.APPROVED
        )
        updated_cursor = await connection.execute(
            "UPDATE public.sync_cursors AS cursor SET cursor = %(next_cursor)s, "
            "status = %(target_status)s, blocked_reason_code = NULL, "
            "contract_fingerprint = NULL, blocked_at = NULL, "
            "transient_failures = 0, retry_after_at = NULL, "
            "cold_start_plan_id = %(target_plan_id)s, "
            "cold_start_plan_state = %(target_plan_state)s, "
            "version = cursor.version + 1, "
            "last_attempt_at = %(database_stamp)s, "
            "last_success_at = %(database_stamp)s, "
            "updated_at = %(database_stamp)s "
            "WHERE cursor.account_id = %(account_id)s "
            "AND cursor.folder_key = %(folder_key)s "
            "AND cursor.status = %(expected_status)s "
            "AND cursor.cursor IS NOT DISTINCT FROM %(expected_cursor)s "
            "AND cursor.version = %(expected_version)s "
            "AND cursor.blocked_reason_code IS NOT DISTINCT FROM "
            "%(expected_blocked_reason_code)s "
            "AND cursor.contract_fingerprint IS NOT DISTINCT FROM "
            "%(expected_contract_fingerprint)s "
            "AND cursor.blocked_at IS NOT DISTINCT FROM %(expected_blocked_at)s "
            "AND cursor.transient_failures = %(expected_transient_failures)s "
            "AND cursor.retry_after_at IS NOT DISTINCT FROM %(expected_retry_after_at)s "
            "AND cursor.cold_start_plan_id IS NOT DISTINCT FROM "
            "%(expected_plan_id)s "
            "AND cursor.cold_start_plan_state IS NOT DISTINCT FROM "
            "%(expected_plan_state)s RETURNING cursor, status, version, "
            "blocked_reason_code, contract_fingerprint, blocked_at, "
            "transient_failures, retry_after_at, cold_start_plan_id, "
            "cold_start_plan_state, last_attempt_at, last_success_at, updated_at",
            {
                "next_cursor": batch.cursor,
                "target_status": target_cursor_status.value,
                "target_plan_id": None if terminal else current_plan.view.plan_id,
                "target_plan_state": None if terminal else "approved",
                "database_stamp": database_stamp,
                "account_id": current_plan.view.account_id,
                "folder_key": current_plan.view.canonical_folder,
                "expected_status": current_cursor.status.value,
                "expected_cursor": current_cursor.cursor,
                "expected_version": current_cursor.version,
                "expected_blocked_reason_code": current_cursor.blocked_reason_code,
                "expected_contract_fingerprint": current_cursor.contract_fingerprint,
                "expected_blocked_at": current_cursor.blocked_at,
                "expected_transient_failures": current_cursor.transient_failures,
                "expected_retry_after_at": current_cursor.retry_after_at,
                "expected_plan_id": current_cursor.cold_start_plan_id,
                "expected_plan_state": (
                    None
                    if current_cursor.cold_start_plan_state is None
                    else current_cursor.cold_start_plan_state.value
                ),
            },
        )
        applied_cursor = _applied_cursor_from_row(await updated_cursor.fetchone())
        _validate_applied_cursor(
            applied_cursor,
            previous=current_cursor,
            next_cursor=batch.cursor,
            terminal=terminal,
            plan_id=current_plan.view.plan_id,
            database_stamp=database_stamp,
        )

        updated_plan = await connection.execute(
            "UPDATE public.sync_cold_start_plans AS plan SET "
            "state = %(target_state)s, version = plan.version + 1, "
            "apply_cursor = %(next_cursor)s, "
            "apply_cursor_version = %(next_cursor_version)s, "
            "completed_at = CASE WHEN %(terminal)s "
            "THEN %(database_stamp)s ELSE NULL END, "
            "updated_at = %(database_stamp)s "
            "WHERE plan.plan_id = %(plan_id)s "
            "AND plan.account_id = %(account_id)s "
            "AND plan.folder_key = %(folder_key)s "
            "AND plan.state = 'approved' "
            "AND plan.version = %(expected_version)s "
            "AND plan.boundary_cursor = %(expected_boundary_cursor)s "
            "AND plan.boundary_cursor_version = "
            "%(expected_boundary_cursor_version)s "
            "AND plan.apply_cursor IS NOT DISTINCT FROM "
            "%(expected_apply_cursor)s "
            "AND plan.apply_cursor_version IS NOT DISTINCT FROM "
            "%(expected_apply_cursor_version)s "
            "AND plan.plan_hash = %(expected_plan_hash)s "
            "AND plan.contract_fingerprint = %(expected_contract_fingerprint)s "
            "AND plan.folder_scope_config_hash = %(expected_config_hash)s "
            "AND %(database_stamp)s < plan.expires_at RETURNING "
            f"{_PLAN_SELECT_COLUMNS}",
            {
                "target_state": target_plan_state.value,
                "next_cursor": batch.cursor,
                "next_cursor_version": applied_cursor.cursor.version,
                "terminal": terminal,
                "database_stamp": database_stamp,
                "plan_id": current_plan.view.plan_id,
                "account_id": current_plan.view.account_id,
                "folder_key": current_plan.view.canonical_folder,
                "expected_version": current_plan.version,
                "expected_boundary_cursor": current_plan.view.boundary_cursor,
                "expected_boundary_cursor_version": (
                    current_plan.boundary_cursor_version
                ),
                "expected_apply_cursor": current_plan.apply_cursor,
                "expected_apply_cursor_version": current_plan.apply_cursor_version,
                "expected_plan_hash": current_plan.view.plan_hash,
                "expected_contract_fingerprint": current_plan.view.contract_fingerprint,
                "expected_config_hash": current_plan.view.folder_scope_config_hash,
            },
        )
        plan_row = await updated_plan.fetchone()
        if plan_row is None:
            raise ColdStartStateConflictError()
        try:
            applied_plan = _cold_start_plan_from_row(plan_row)
        except ValueError:
            raise ColdStartStateConflictError() from None
        _validate_applied_plan(
            applied_plan,
            previous=current_plan,
            applied_cursor=applied_cursor,
            batch=batch,
            database_stamp=database_stamp,
        )

        result_hash = _apply_page_result_digest(_batch_digest(batch))
        receipt = await receipt_transaction.insert(
            account_id=current_plan.view.account_id,
            command_name=_APPLY_PAGE_COMMAND,
            idempotency_key=payload_hash,
            canonical_payload_hash=payload_hash,
            outcome="succeeded",
            result_type=_PLAN_RESULT_TYPE,
            result_id=str(current_plan.view.plan_id),
            result_hash=result_hash,
            authority_epoch=current_plan.ownership.fencing_token,
        )
        _validate_apply_page_receipt(
            receipt,
            plan=current_plan,
            payload_hash=payload_hash,
            result_hash=result_hash,
        )
        return _ApplyPageCommit(
            plan=applied_plan,
            cursor=applied_cursor.cursor,
            receipt=receipt,
        )

    async def _recover_apply_commit(
        self,
        unknown: _ApplyCommitOutcomeUnknown,
    ) -> ColdStartRunResult:
        evidence = unknown.evidence
        prepared = evidence.prepared
        expected_plan = prepared.plan
        expected_cursor = prepared.cursor
        payload_hash = prepared.payload_hash
        if (
            type(evidence) is not _ApplyCommitEvidence
            or type(prepared) is not _ApplyPreflight
            or type(evidence.committed) is not _ApplyPageCommit
            or expected_cursor is None
            or prepared.request_cursor is None
            or payload_hash is None
            or prepared.immediate_status is not None
        ):
            raise _apply_recovery_invariant()
        scope: FolderScope | None
        scope_error: PolicySnapshotUnavailableError | None = None
        try:
            candidate_scope, _snapshot = await self._ready_scope(
                expected_plan.view.account_id,
                expected_plan.view.canonical_folder,
            )
        except PolicySnapshotUnavailableError as error:
            scope = None
            scope_error = error
        else:
            if type(candidate_scope) is not FolderScope:
                raise PolicySnapshotUnavailableError()
            scope = candidate_scope

        async def operation(session: _SyncSessionLease) -> _ApplyPageCommit:
            async def recover(connection: Any) -> _ApplyPageCommit:
                await _configure_sync_xid(connection, expected_plan.view.account_id)
                ownership = await _read_current_ownership(
                    connection,
                    expected_plan.view.account_id,
                    for_key_share=False,
                )
                current_cursor = await _read_apply_cursor(
                    connection,
                    expected_plan.view.account_id,
                    expected_plan.view.canonical_folder,
                )
                try:
                    current_plan = await _read_cold_start_plan(
                        connection,
                        expected_plan.view.plan_id,
                    )
                except ValueError:
                    raise _apply_recovery_invariant() from None
                if current_plan is None:
                    raise _apply_recovery_invariant()
                database_now = await _read_database_now(connection)
                receipt_transaction = self._receipt_repository.transaction(connection)
                try:
                    receipt = await receipt_transaction.lookup(
                        account_id=expected_plan.view.account_id,
                        command_name=_APPLY_PAGE_COMMAND,
                        idempotency_key=payload_hash,
                        canonical_payload_hash=payload_hash,
                    )
                except IdempotencyConflict:
                    raise _apply_recovery_invariant() from None
                except RuntimeError as error:
                    if type(error) is RuntimeError and error.args == (
                        "command_receipt_persisted_invalid",
                    ):
                        raise _apply_recovery_invariant() from None
                    raise
                if receipt is not None:
                    try:
                        _validate_apply_page_receipt(
                            receipt,
                            plan=expected_plan,
                            payload_hash=payload_hash,
                            result_hash=_apply_page_result_digest(
                                _batch_digest(evidence.batch)
                            ),
                        )
                    except DatabaseOperationError:
                        raise _apply_recovery_invariant() from None

                if (
                    current_plan == evidence.committed.plan
                    and current_cursor == evidence.committed.cursor
                    and receipt == evidence.committed.receipt
                ):
                    return evidence.committed
                if scope is None:
                    if scope_error is None:
                        raise _apply_recovery_invariant()
                    raise scope_error
                if not _apply_recovery_environment_matches(
                    current_plan,
                    expected=expected_plan,
                    ownership=ownership,
                    scope=scope,
                    contract_fingerprint=self._contract_fingerprint,
                ):
                    raise _apply_recovery_invariant()
                if (
                    current_plan == expected_plan
                    and current_cursor == expected_cursor
                    and receipt is None
                    and database_now < current_plan.view.expires_at
                ):
                    replayed = await self._commit_apply_page(
                        connection,
                        scope=scope,
                        prepared=prepared,
                        batch=evidence.batch,
                        events=evidence.events,
                    )
                    if type(replayed) is not _ApplyPageCommit:
                        raise _apply_recovery_invariant()
                    return replayed
                raise _apply_recovery_invariant()

            return await _caller_owned_transaction(session, recover)

        outcome = await self._session_runner.run(
            expected_plan.view.account_id,
            expected_plan.view.canonical_folder,
            operation,
        )
        if not outcome.acquired:
            raise unknown.primary
        recovered = outcome.value
        if type(recovered) is not _ApplyPageCommit:
            raise _apply_recovery_invariant()
        recovered_plan = recovered.plan
        if recovered_plan.view.state is ColdStartPlanState.COMPLETED:
            status = ColdStartRunStatus.COMPLETED
        elif recovered_plan.view.state is ColdStartPlanState.APPROVED:
            status = ColdStartRunStatus.APPROVED
        else:
            raise _apply_recovery_invariant()
        return ColdStartRunResult(
            status=status,
            plan=recovered_plan.view,
            pages_committed=evidence.pages_committed + 1,
            changes_observed=(evidence.changes_observed + len(evidence.batch.changes)),
            safe_code=None,
        )

    async def _approve_plan(
        self,
        connection: Any,
        *,
        identity: _LocatedPlanIdentity,
        scope: FolderScope,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> _ColdStartPlanRecord:
        await _configure_sync_xid(connection, identity.account_id)
        payload_hash = _approve_payload_digest(
            plan_id=identity.plan_id,
            actor=actor,
            reason=reason,
        )
        receipt_transaction = self._receipt_repository.transaction(connection)
        receipt = await receipt_transaction.lookup(
            account_id=identity.account_id,
            command_name=_APPROVE_COMMAND,
            idempotency_key=idempotency_key,
            canonical_payload_hash=payload_hash,
        )
        if receipt is not None:
            receipt_plan_id = _receipt_plan_id(receipt)
            if receipt_plan_id != identity.plan_id:
                raise ColdStartStateConflictError()
            try:
                replayed = await _read_cold_start_plan(connection, identity.plan_id)
            except ValueError:
                raise ColdStartStateConflictError() from None
            if replayed is None:
                raise ColdStartStateConflictError()
            if (
                replayed.view.plan_id != identity.plan_id
                or replayed.view.account_id != identity.account_id
                or replayed.view.canonical_folder != identity.canonical_folder
                or replayed.view.plan_hash != _sealed_existing_plan_digest(replayed)
            ):
                raise ColdStartStateConflictError()
            _validate_approval_receipt(
                receipt,
                plan=replayed,
                account_id=identity.account_id,
                idempotency_key=idempotency_key,
                canonical_payload_hash=payload_hash,
            )
            return replayed

        ownership = await _read_current_ownership(
            connection,
            identity.account_id,
            for_key_share=False,
        )
        cursor_binding = await _read_cold_start_cursor(
            connection,
            identity.account_id,
            identity.canonical_folder,
        )
        try:
            current = await _read_cold_start_plan(connection, identity.plan_id)
        except ValueError:
            raise ColdStartStateConflictError() from None
        if current is None:
            raise ColdStartPlanNotFoundError()
        database_now = await _read_database_now(connection)
        drift_code = _approval_drift_code(
            current,
            identity=identity,
            ownership=ownership,
            cursor_binding=cursor_binding,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_now,
        )
        if drift_code is not None:
            return await self._write_cold_start_block(
                connection,
                current=current,
                cursor_binding=cursor_binding,
                database_now=database_now,
                safe_code=drift_code,
            )

        approved_cursor = await connection.execute(
            "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
            "UPDATE public.sync_cold_start_plans AS plan SET state = 'approved', "
            "version = plan.version + 1, approved_at = stamp.at, "
            "updated_at = stamp.at FROM stamp "
            "WHERE plan.plan_id = %(plan_id)s "
            "AND plan.account_id = %(account_id)s "
            "AND plan.folder_key = %(folder_key)s AND plan.state = 'ready' "
            "AND plan.version = %(expected_version)s "
            "AND plan.plan_hash = %(expected_plan_hash)s "
            "AND plan.boundary_cursor = %(expected_boundary_cursor)s "
            "AND plan.boundary_cursor_version = "
            "%(expected_boundary_cursor_version)s "
            "AND stamp.at < plan.expires_at RETURNING "
            f"{_PLAN_SELECT_COLUMNS}",
            {
                "plan_id": current.view.plan_id,
                "account_id": current.view.account_id,
                "folder_key": current.view.canonical_folder,
                "expected_version": current.version,
                "expected_plan_hash": current.view.plan_hash,
                "expected_boundary_cursor": current.view.boundary_cursor,
                "expected_boundary_cursor_version": current.boundary_cursor_version,
            },
        )
        approved_row = await approved_cursor.fetchone()
        if approved_row is None:
            latest_database_now = await _read_database_now(connection)
            if latest_database_now >= current.view.expires_at:
                return await self._write_cold_start_block(
                    connection,
                    current=current,
                    cursor_binding=cursor_binding,
                    database_now=latest_database_now,
                    safe_code="cold_start.expired",
                )
            raise ColdStartStateConflictError()
        try:
            approved = _cold_start_plan_from_row(approved_row)
        except ValueError:
            raise ColdStartStateConflictError() from None
        _validate_approved_plan(approved, previous=current)
        approved_at = approved.view.approved_at
        if approved_at is None or approved.view.plan_hash is None:
            raise ColdStartStateConflictError()
        await connection.execute(
            "INSERT INTO public.audit_events ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, "
            "safe_metadata"
            ") VALUES (%s, %s, %s, NULL, 'sync_cold_start_plan', %s, "
            "'cold_start.approve', %s, %s, %s, %s)",
            (
                uuid4(),
                _audit_event_digest(
                    action="cold_start.approve",
                    plan_id=approved.view.plan_id,
                    plan_version=current.version,
                ),
                approved.view.account_id,
                _audit_object_digest(approved.view.plan_id),
                "approved",
                actor,
                reason,
                Jsonb(_approval_audit_metadata(approved.view)),
            ),
        )
        result_hash = _approve_result_digest(
            plan_id=approved.view.plan_id,
            plan_hash=approved.view.plan_hash,
            pipeline_name=approved.ownership.pipeline_name,
            generation=approved.ownership.generation,
            fencing_token=approved.ownership.fencing_token,
            folder_scope_config_hash=approved.view.folder_scope_config_hash,
            approved_at=approved_at,
        )
        receipt = await receipt_transaction.insert(
            account_id=approved.view.account_id,
            command_name=_APPROVE_COMMAND,
            idempotency_key=idempotency_key,
            canonical_payload_hash=payload_hash,
            outcome="succeeded",
            result_type=_PLAN_RESULT_TYPE,
            result_id=str(approved.view.plan_id),
            result_hash=result_hash,
            authority_epoch=approved.ownership.fencing_token,
        )
        _validate_approval_receipt(
            receipt,
            plan=approved,
            account_id=approved.view.account_id,
            idempotency_key=idempotency_key,
            canonical_payload_hash=payload_hash,
        )
        return approved

    async def _load_resume_preview(
        self,
        connection: Any,
        *,
        identity: _LocatedPlanIdentity,
        scope: FolderScope,
    ) -> _ColdStartPlanRecord:
        (
            current,
            ownership,
            cursor_binding,
            database_now,
        ) = await self._read_preview_page_context(
            connection,
            account_id=identity.account_id,
            canonical_folder=identity.canonical_folder,
            plan_id=identity.plan_id,
        )
        if (
            current.view.plan_id != identity.plan_id
            or current.view.account_id != identity.account_id
            or current.view.canonical_folder != identity.canonical_folder
            or current.view.state is not ColdStartPlanState.PREVIEWING
        ):
            raise ColdStartStateConflictError()
        drift_code = _preview_page_drift_code(
            current,
            expected=current,
            ownership=ownership,
            cursor_binding=cursor_binding,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_now,
        )
        if drift_code is not None:
            return await self._write_cold_start_block(
                connection,
                current=current,
                cursor_binding=cursor_binding,
                database_now=database_now,
                safe_code=drift_code,
            )
        return current

    async def _recover_preview_commit(
        self,
        unknown: _PreviewCommitOutcomeUnknown,
    ) -> ColdStartRunResult:
        expected = unknown.expected
        scope: FolderScope | None
        scope_error: PolicySnapshotUnavailableError | None = None
        try:
            candidate_scope, _snapshot = await self._ready_scope(
                expected.view.account_id,
                expected.view.canonical_folder,
            )
        except PolicySnapshotUnavailableError as error:
            scope = None
            scope_error = error
        else:
            if type(candidate_scope) is not FolderScope:
                raise PolicySnapshotUnavailableError()
            scope = candidate_scope

        async def operation(session: _SyncSessionLease) -> _ColdStartPlanRecord:
            async def recover(connection: Any) -> _ColdStartPlanRecord:
                (
                    current,
                    ownership,
                    cursor_binding,
                    database_now,
                ) = await self._read_preview_page_context(
                    connection,
                    account_id=expected.view.account_id,
                    canonical_folder=expected.view.canonical_folder,
                    plan_id=expected.view.plan_id,
                )
                if current == unknown.expected_post:
                    return current
                if scope is None:
                    if scope_error is None:
                        raise _preview_recovery_invariant()
                    raise scope_error
                if not _preview_recovery_environment_matches(
                    current,
                    expected=expected,
                    ownership=ownership,
                    cursor_binding=cursor_binding,
                    scope=scope,
                    contract_fingerprint=self._contract_fingerprint,
                ):
                    raise _preview_recovery_invariant()
                if current != expected or database_now >= current.view.expires_at:
                    raise _preview_recovery_invariant()
                return await self._write_preview_page_from_context(
                    connection,
                    current=current,
                    cursor_binding=cursor_binding,
                    batch=unknown.batch,
                )

            return await _caller_owned_transaction(session, recover)

        outcome = await self._session_runner.run(
            expected.view.account_id,
            expected.view.canonical_folder,
            operation,
        )
        if not outcome.acquired:
            raise unknown.primary
        recovered = outcome.value
        if type(recovered) is not _ColdStartPlanRecord:
            raise _preview_recovery_invariant()
        pages_committed = unknown.pages_committed + 1
        changes_observed = unknown.changes_observed + len(unknown.batch.changes)
        if recovered.view.state is ColdStartPlanState.READY:
            status = ColdStartRunStatus.READY
        elif recovered.view.state is ColdStartPlanState.PREVIEWING:
            status = ColdStartRunStatus.PREVIEWING
        else:
            raise _preview_recovery_invariant()
        return ColdStartRunResult(
            status=status,
            plan=recovered.view,
            pages_committed=pages_committed,
            changes_observed=changes_observed,
            safe_code=None,
        )

    async def _accept_preview(
        self,
        connection: Any,
        *,
        account_id: int,
        canonical_folder: str,
        scope: FolderScope,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> _PreviewAcceptance:
        await _configure_sync_xid(connection, account_id)
        payload_hash = _preview_payload_digest(
            account_id=account_id,
            canonical_folder=canonical_folder,
            actor=actor,
            reason=reason,
        )
        receipt_transaction = self._receipt_repository.transaction(connection)
        receipt = await receipt_transaction.lookup(
            account_id=account_id,
            command_name=_PREVIEW_COMMAND,
            idempotency_key=idempotency_key,
            canonical_payload_hash=payload_hash,
        )
        if receipt is not None:
            plan_id = _receipt_plan_id(receipt)
            try:
                plan = await _read_cold_start_plan(connection, plan_id)
            except ValueError:
                raise ColdStartStateConflictError() from None
            if plan is None:
                raise ColdStartStateConflictError()
            _validate_preview_receipt(
                receipt,
                plan=plan,
                account_id=account_id,
                idempotency_key=idempotency_key,
                canonical_payload_hash=payload_hash,
                actor=actor,
                reason=reason,
            )
            return _PreviewAcceptance(plan=plan, replayed=True)

        ownership = await _read_current_ownership(
            connection,
            account_id,
            for_key_share=False,
        )
        cursor, cursor_status, cursor_version = await _read_cold_start_cursor(
            connection,
            account_id,
            canonical_folder,
        )
        expected_status, expected_cursor = _eligible_preview_cursor(
            cursor_status,
            cursor,
        )
        open_plan = await connection.execute(
            "SELECT plan_id FROM public.sync_cold_start_plans "
            "WHERE account_id = %s AND folder_key = %s "
            "AND state IN ('previewing', 'ready', 'approved') FOR UPDATE",
            (account_id, canonical_folder),
        )
        open_row = await open_plan.fetchone()
        if open_row is not None:
            _require_open_plan_row(open_row)
            raise ColdStartStateConflictError()

        plan_id = uuid4()
        try:
            inserted = await connection.execute(
                "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
                "INSERT INTO public.sync_cold_start_plans ("
                "plan_id, account_id, folder_key, expected_cursor_status, "
                "expected_cursor, expected_cursor_version, pipeline_name, "
                "generation, fencing_token, state, version, preview_cursor, "
                "preview_cursor_version, boundary_cursor, "
                "boundary_cursor_version, apply_cursor, apply_cursor_version, "
                "rolling_hash, page_count, item_count, redacted_samples, "
                "contract_fingerprint, folder_scope_config_hash, plan_hash, "
                "actor, reason, blocked_reason_code, blocked_fingerprint, "
                "expires_at, ready_at, approved_at, completed_at, blocked_at, "
                "created_at, updated_at"
                ") SELECT "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "'previewing', 0, NULL, 0, NULL, NULL, NULL, NULL, NULL, "
                "0, 0, '[]'::pg_catalog.jsonb, %s, %s, NULL, %s, %s, "
                "NULL, NULL, "
                "stamp.at + (%s * INTERVAL '1 second'), "
                "NULL, NULL, NULL, NULL, stamp.at, stamp.at FROM stamp RETURNING "
                f"{_PLAN_SELECT_COLUMNS}",
                (
                    plan_id,
                    account_id,
                    canonical_folder,
                    expected_status.value,
                    expected_cursor,
                    cursor_version,
                    ownership.pipeline_name,
                    ownership.generation,
                    ownership.fencing_token,
                    self._contract_fingerprint,
                    scope.config_hash,
                    actor,
                    reason,
                    self._plan_ttl_seconds,
                ),
            )
        except UniqueViolation:
            raise ColdStartStateConflictError() from None
        inserted_row = await inserted.fetchone()
        try:
            plan = _cold_start_plan_from_row(inserted_row)
        except ValueError:
            raise ColdStartStateConflictError() from None
        _validate_accepted_preview_plan(
            plan,
            plan_id=plan_id,
            account_id=account_id,
            canonical_folder=canonical_folder,
            expected_cursor_status=expected_status,
            expected_cursor=expected_cursor,
            expected_cursor_version=cursor_version,
            ownership=ownership,
            contract_fingerprint=self._contract_fingerprint,
            folder_scope_config_hash=scope.config_hash,
            actor=actor,
            reason=reason,
            plan_ttl_seconds=self._plan_ttl_seconds,
        )
        result_hash = _preview_receipt_result_hash(plan)
        receipt = await receipt_transaction.insert(
            account_id=account_id,
            command_name=_PREVIEW_COMMAND,
            idempotency_key=idempotency_key,
            canonical_payload_hash=payload_hash,
            outcome="succeeded",
            result_type=_PLAN_RESULT_TYPE,
            result_id=str(plan.view.plan_id),
            result_hash=result_hash,
            authority_epoch=plan.ownership.fencing_token,
        )
        _validate_preview_receipt(
            receipt,
            plan=plan,
            account_id=account_id,
            idempotency_key=idempotency_key,
            canonical_payload_hash=payload_hash,
            actor=actor,
            reason=reason,
        )
        return _PreviewAcceptance(plan=plan, replayed=False)

    async def _resume_preview_locked(
        self,
        session: _SyncSessionLease,
        scope: FolderScope,
        _snapshot: object,
        plan: _ColdStartPlanRecord,
    ) -> ColdStartRunResult:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._preview_max_run_seconds
        pages_committed = 0
        changes_observed = 0
        current_scope = scope
        current_plan = plan

        while pages_committed < self._preview_max_pages:
            if deadline - loop.time() <= 0:
                return _preview_budget_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

            async def preflight(connection: Any) -> _ColdStartPlanRecord:
                return await self._preflight_preview_page(
                    connection,
                    scope=current_scope,
                    expected=current_plan,
                )

            current_plan = await _caller_owned_transaction(session, preflight)
            if current_plan.view.state is ColdStartPlanState.BLOCKED:
                return _preview_blocked_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            remaining = deadline - loop.time()
            if remaining <= 0:
                return _preview_budget_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            request_cursor = (
                None
                if current_plan.view.page_count == 0
                else current_plan.preview_cursor
            )
            try:
                async with asyncio.timeout(remaining):
                    batch = await _fetch_origin_page(
                        self._cold_start_origin,
                        current_plan.view.account_id,
                        current_scope.sync_folder,
                        request_cursor,
                        self._page_limit,
                    )
            except TimeoutError:
                return _preview_budget_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncTransientError:
                raise
            except SyncAuthorizationError:
                return await self._block_preview_locked(
                    session,
                    expected=current_plan,
                    safe_code="exchange.sync.authorization_failed",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncCursorInvalidError:
                return await self._block_preview_locked(
                    session,
                    expected=current_plan,
                    safe_code="exchange.sync.cursor_invalid",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except SyncContractError:
                return await self._block_preview_locked(
                    session,
                    expected=current_plan,
                    safe_code="exchange.sync.contract_invalid",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            except ValueError:
                return await self._block_preview_locked(
                    session,
                    expected=current_plan,
                    safe_code="sync.local_contract_invalid",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

            if not batch.includes_last and batch.cursor == request_cursor:
                return await self._block_preview_locked(
                    session,
                    expected=current_plan,
                    safe_code="sync.cursor_stalled",
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )

            post_scope, _post_snapshot = await self._ready_scope(
                current_plan.view.account_id,
                current_plan.view.canonical_folder,
            )
            if type(post_scope) is not FolderScope:
                raise PolicySnapshotUnavailableError()

            commit_projection: list[_ColdStartPlanRecord] = []

            async def commit(connection: Any) -> _ColdStartPlanRecord:
                committed = await self._commit_preview_page(
                    connection,
                    scope=post_scope,
                    expected=current_plan,
                    batch=batch,
                )
                commit_projection.append(committed)
                return committed

            try:
                current_plan = await _caller_owned_transaction(session, commit)
            except Exception as error:
                if (
                    session.tainted
                    and len(commit_projection) == 1
                    and commit_projection[0].view.state
                    in {ColdStartPlanState.PREVIEWING, ColdStartPlanState.READY}
                ):
                    raise _PreviewCommitOutcomeUnknown(
                        primary=error,
                        expected=current_plan,
                        expected_post=commit_projection[0],
                        batch=batch,
                        pages_committed=pages_committed,
                        changes_observed=changes_observed,
                    ) from error
                raise
            if current_plan.view.state is ColdStartPlanState.BLOCKED:
                return _preview_blocked_result(
                    current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                )
            pages_committed += 1
            changes_observed += len(batch.changes)
            if current_plan.view.state is ColdStartPlanState.READY:
                return ColdStartRunResult(
                    status=ColdStartRunStatus.READY,
                    plan=current_plan.view,
                    pages_committed=pages_committed,
                    changes_observed=changes_observed,
                    safe_code=None,
                )
            current_scope = post_scope

        return _preview_budget_result(
            current_plan.view,
            pages_committed=pages_committed,
            changes_observed=changes_observed,
        )

    async def _block_preview_locked(
        self,
        session: _SyncSessionLease,
        *,
        expected: _ColdStartPlanRecord,
        safe_code: str,
        pages_committed: int,
        changes_observed: int,
    ) -> ColdStartRunResult:
        scope, _snapshot = await self._ready_scope(
            expected.view.account_id,
            expected.view.canonical_folder,
        )
        if type(scope) is not FolderScope:
            raise PolicySnapshotUnavailableError()

        async def operation(connection: Any) -> _ColdStartPlanRecord:
            return await self._block_preview(
                connection,
                scope=scope,
                expected=expected,
                safe_code=safe_code,
            )

        blocked = await _caller_owned_transaction(session, operation)
        blocked_code = blocked.view.blocked_reason_code
        if blocked_code is None:
            raise ColdStartStateConflictError()
        return ColdStartRunResult(
            status=ColdStartRunStatus.BLOCKED,
            plan=blocked.view,
            pages_committed=pages_committed,
            changes_observed=changes_observed,
            safe_code=blocked_code,
        )

    async def _block_preview(
        self,
        connection: Any,
        *,
        scope: FolderScope,
        expected: _ColdStartPlanRecord,
        safe_code: str,
    ) -> _ColdStartPlanRecord:
        (
            current,
            ownership,
            cursor_binding,
            database_now,
        ) = await self._read_preview_page_context(
            connection,
            account_id=expected.view.account_id,
            canonical_folder=expected.view.canonical_folder,
            plan_id=expected.view.plan_id,
        )
        drift_code = _preview_page_drift_code(
            current,
            expected=expected,
            ownership=ownership,
            cursor_binding=cursor_binding,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_now,
        )
        return await self._write_cold_start_block(
            connection,
            current=current,
            cursor_binding=cursor_binding,
            database_now=database_now,
            safe_code=drift_code or safe_code,
        )

    async def _write_cold_start_block(
        self,
        connection: Any,
        *,
        current: _ColdStartPlanRecord,
        cursor_binding: tuple[str | None, SyncCursorStatus, int] | _ApplyCursorRecord,
        database_now: datetime,
        safe_code: str,
    ) -> _ColdStartPlanRecord:
        blocked_fingerprint = _blocked_digest(
            account_id=current.view.account_id,
            canonical_folder=current.view.canonical_folder,
            plan_id=current.view.plan_id,
            safe_code=safe_code,
        )
        blocked_plan = await connection.execute(
            "UPDATE public.sync_cold_start_plans AS plan SET state = 'blocked', "
            "version = plan.version + 1, blocked_reason_code = %(safe_code)s, "
            "blocked_fingerprint = %(blocked_fingerprint)s, "
            "blocked_at = %(blocked_at)s, updated_at = %(blocked_at)s "
            "WHERE plan.plan_id = %(plan_id)s "
            "AND plan.account_id = %(account_id)s "
            "AND plan.folder_key = %(folder_key)s "
            "AND plan.state = %(expected_state)s "
            "AND plan.version = %(expected_version)s "
            "AND plan.preview_cursor IS NOT DISTINCT FROM "
            "%(expected_preview_cursor)s "
            "AND plan.preview_cursor_version = "
            "%(expected_preview_cursor_version)s "
            "AND plan.rolling_hash IS NOT DISTINCT FROM "
            "%(expected_rolling_hash)s "
            "AND plan.page_count = %(expected_page_count)s "
            "AND plan.item_count = %(expected_item_count)s "
            "AND plan.boundary_cursor IS NOT DISTINCT FROM "
            "%(expected_boundary_cursor)s "
            "AND plan.boundary_cursor_version IS NOT DISTINCT FROM "
            "%(expected_boundary_cursor_version)s "
            "AND plan.apply_cursor IS NOT DISTINCT FROM "
            "%(expected_apply_cursor)s "
            "AND plan.apply_cursor_version IS NOT DISTINCT FROM "
            "%(expected_apply_cursor_version)s "
            "AND plan.plan_hash IS NOT DISTINCT FROM %(expected_plan_hash)s "
            "RETURNING "
            f"{_PLAN_SELECT_COLUMNS}",
            {
                "safe_code": safe_code,
                "blocked_fingerprint": blocked_fingerprint,
                "blocked_at": database_now,
                "plan_id": current.view.plan_id,
                "account_id": current.view.account_id,
                "folder_key": current.view.canonical_folder,
                "expected_state": current.view.state.value,
                "expected_version": current.version,
                "expected_preview_cursor": current.preview_cursor,
                "expected_preview_cursor_version": current.preview_cursor_version,
                "expected_rolling_hash": current.rolling_hash,
                "expected_page_count": current.view.page_count,
                "expected_item_count": current.view.item_count,
                "expected_boundary_cursor": current.view.boundary_cursor,
                "expected_boundary_cursor_version": current.boundary_cursor_version,
                "expected_apply_cursor": current.apply_cursor,
                "expected_apply_cursor_version": current.apply_cursor_version,
                "expected_plan_hash": current.view.plan_hash,
            },
        )
        plan_row = await blocked_plan.fetchone()
        if plan_row is None:
            raise ColdStartStateConflictError()
        try:
            blocked = _cold_start_plan_from_row(plan_row)
        except ValueError:
            raise ColdStartStateConflictError() from None
        if type(cursor_binding) is _ApplyCursorRecord:
            blocked_cursor = await connection.execute(
                "UPDATE public.sync_cursors AS cursor SET "
                "status = 'blocked_contract', "
                "blocked_reason_code = %(safe_code)s, "
                "contract_fingerprint = %(blocked_fingerprint)s, "
                "blocked_at = %(blocked_at)s, transient_failures = 0, "
                "retry_after_at = NULL, cold_start_plan_id = NULL, "
                "cold_start_plan_state = NULL, version = cursor.version + 1, "
                "last_attempt_at = %(blocked_at)s, updated_at = %(blocked_at)s "
                "WHERE cursor.account_id = %(account_id)s "
                "AND cursor.folder_key = %(folder_key)s "
                "AND cursor.status = %(expected_status)s "
                "AND cursor.cursor IS NOT DISTINCT FROM %(expected_cursor)s "
                "AND cursor.version = %(expected_version)s "
                "AND cursor.blocked_reason_code IS NOT DISTINCT FROM "
                "%(expected_blocked_reason_code)s "
                "AND cursor.contract_fingerprint IS NOT DISTINCT FROM "
                "%(expected_contract_fingerprint)s "
                "AND cursor.blocked_at IS NOT DISTINCT FROM "
                "%(expected_blocked_at)s "
                "AND cursor.transient_failures = "
                "%(expected_transient_failures)s "
                "AND cursor.retry_after_at IS NOT DISTINCT FROM "
                "%(expected_retry_after_at)s "
                "AND cursor.cold_start_plan_id IS NOT DISTINCT FROM "
                "%(expected_plan_id)s "
                "AND cursor.cold_start_plan_state IS NOT DISTINCT FROM "
                "%(expected_plan_state)s "
                "AND cursor.last_attempt_at IS NOT DISTINCT FROM "
                "%(expected_last_attempt_at)s "
                "AND cursor.last_success_at IS NOT DISTINCT FROM "
                "%(expected_last_success_at)s "
                "AND cursor.updated_at = %(expected_updated_at)s RETURNING "
                "cursor, status, version, blocked_reason_code, "
                "contract_fingerprint, blocked_at, transient_failures, "
                "retry_after_at, cold_start_plan_id, cold_start_plan_state, "
                "last_attempt_at, last_success_at, updated_at",
                {
                    "safe_code": safe_code,
                    "blocked_fingerprint": blocked_fingerprint,
                    "blocked_at": database_now,
                    "account_id": current.view.account_id,
                    "folder_key": current.view.canonical_folder,
                    "expected_status": cursor_binding.status.value,
                    "expected_cursor": cursor_binding.cursor,
                    "expected_version": cursor_binding.version,
                    "expected_blocked_reason_code": (
                        cursor_binding.blocked_reason_code
                    ),
                    "expected_contract_fingerprint": (
                        cursor_binding.contract_fingerprint
                    ),
                    "expected_blocked_at": cursor_binding.blocked_at,
                    "expected_transient_failures": (cursor_binding.transient_failures),
                    "expected_retry_after_at": cursor_binding.retry_after_at,
                    "expected_plan_id": cursor_binding.cold_start_plan_id,
                    "expected_plan_state": (
                        None
                        if cursor_binding.cold_start_plan_state is None
                        else cursor_binding.cold_start_plan_state.value
                    ),
                    "expected_last_attempt_at": cursor_binding.last_attempt_at,
                    "expected_last_success_at": cursor_binding.last_success_at,
                    "expected_updated_at": cursor_binding.updated_at,
                },
            )
            cursor_row = await blocked_cursor.fetchone()
            try:
                blocked_apply_cursor = _apply_cursor_record_from_row(cursor_row)
            except ValueError:
                raise ColdStartStateConflictError() from None
            _validate_blocked_apply_cursor(
                blocked_apply_cursor,
                previous=cursor_binding,
                safe_code=safe_code,
                blocked_fingerprint=blocked_fingerprint,
                blocked_at=database_now,
            )
        else:
            cursor, cursor_status, cursor_version = cursor_binding
            blocked_cursor = await connection.execute(
                "UPDATE public.sync_cursors AS cursor SET "
                "status = 'blocked_contract', "
                "blocked_reason_code = %(safe_code)s, "
                "contract_fingerprint = %(blocked_fingerprint)s, "
                "blocked_at = %(blocked_at)s, transient_failures = 0, "
                "retry_after_at = NULL, cold_start_plan_id = NULL, "
                "cold_start_plan_state = NULL, version = cursor.version + 1, "
                "last_attempt_at = %(blocked_at)s, updated_at = %(blocked_at)s "
                "WHERE cursor.account_id = %(account_id)s "
                "AND cursor.folder_key = %(folder_key)s "
                "AND cursor.status = %(expected_status)s "
                "AND cursor.cursor IS NOT DISTINCT FROM %(expected_cursor)s "
                "AND cursor.version = %(expected_version)s RETURNING version",
                {
                    "safe_code": safe_code,
                    "blocked_fingerprint": blocked_fingerprint,
                    "blocked_at": database_now,
                    "account_id": current.view.account_id,
                    "folder_key": current.view.canonical_folder,
                    "expected_status": cursor_status.value,
                    "expected_cursor": cursor,
                    "expected_version": cursor_version,
                },
            )
            cursor_row = await blocked_cursor.fetchone()
            if (
                type(cursor_row) is not dict
                or any(type(key) is not str for key in cursor_row)
                or set(cursor_row) != {"version"}
                or type(cursor_row["version"]) is not int
                or cursor_row["version"] != cursor_version + 1
            ):
                raise ColdStartStateConflictError()
        await connection.execute(
            "INSERT INTO public.audit_events ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, "
            "safe_metadata"
            ") VALUES (%s, %s, %s, NULL, 'sync_cold_start_plan', %s, "
            "'cold_start.block', %s, %s, %s, %s)",
            (
                uuid4(),
                _audit_event_digest(
                    action="cold_start.block",
                    plan_id=current.view.plan_id,
                    plan_version=current.version,
                ),
                current.view.account_id,
                _audit_object_digest(current.view.plan_id),
                "blocked",
                "cold_start_service",
                safe_code,
                Jsonb(
                    {
                        "plan_id": str(current.view.plan_id),
                        "safe_code": safe_code,
                    }
                ),
            ),
        )
        _validate_blocked_preview_plan(
            blocked,
            previous=current,
            safe_code=safe_code,
            blocked_fingerprint=blocked_fingerprint,
            blocked_at=database_now,
        )
        return blocked

    async def _preflight_preview_page(
        self,
        connection: Any,
        *,
        scope: FolderScope,
        expected: _ColdStartPlanRecord,
    ) -> _ColdStartPlanRecord:
        (
            current,
            ownership,
            cursor_binding,
            database_now,
        ) = await self._read_preview_page_context(
            connection,
            account_id=expected.view.account_id,
            canonical_folder=expected.view.canonical_folder,
            plan_id=expected.view.plan_id,
        )
        drift_code = _preview_page_drift_code(
            current,
            expected=expected,
            ownership=ownership,
            cursor_binding=cursor_binding,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_now,
        )
        if drift_code is not None:
            return await self._write_cold_start_block(
                connection,
                current=current,
                cursor_binding=cursor_binding,
                database_now=database_now,
                safe_code=drift_code,
            )
        return current

    async def _commit_preview_page(
        self,
        connection: Any,
        *,
        scope: FolderScope,
        expected: _ColdStartPlanRecord,
        batch: SyncBatch,
    ) -> _ColdStartPlanRecord:
        (
            current,
            ownership,
            cursor_binding,
            database_now,
        ) = await self._read_preview_page_context(
            connection,
            account_id=expected.view.account_id,
            canonical_folder=expected.view.canonical_folder,
            plan_id=expected.view.plan_id,
        )
        drift_code = _preview_page_drift_code(
            current,
            expected=expected,
            ownership=ownership,
            cursor_binding=cursor_binding,
            scope=scope,
            contract_fingerprint=self._contract_fingerprint,
            database_now=database_now,
        )
        if drift_code is not None:
            return await self._write_cold_start_block(
                connection,
                current=current,
                cursor_binding=cursor_binding,
                database_now=database_now,
                safe_code=drift_code,
            )
        return await self._write_preview_page_from_context(
            connection,
            current=current,
            cursor_binding=cursor_binding,
            batch=batch,
        )

    async def _write_preview_page_from_context(
        self,
        connection: Any,
        *,
        current: _ColdStartPlanRecord,
        cursor_binding: tuple[str | None, SyncCursorStatus, int],
        batch: SyncBatch,
    ) -> _ColdStartPlanRecord:
        page_count = current.view.page_count + 1
        item_count = current.view.item_count + len(batch.changes)
        samples = _append_preview_samples(
            current.view.redacted_samples,
            account_id=current.view.account_id,
            batch=batch,
        )
        rolling_hash = _preview_rolling_digest(
            current.rolling_hash,
            _batch_digest(batch),
        )
        terminal = batch.includes_last
        plan_hash = (
            _sealed_preview_plan_digest(
                current,
                boundary_cursor=batch.cursor,
                boundary_cursor_version=page_count,
                rolling_hash=rolling_hash,
                page_count=page_count,
                item_count=item_count,
                redacted_samples=samples,
            )
            if terminal
            else None
        )
        updated = await connection.execute(
            "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
            "UPDATE public.sync_cold_start_plans AS plan SET "
            "state = %(target_state)s, version = plan.version + 1, "
            "preview_cursor = %(next_cursor)s, "
            "preview_cursor_version = plan.preview_cursor_version + 1, "
            "boundary_cursor = %(boundary_cursor)s, "
            "boundary_cursor_version = %(boundary_cursor_version)s, "
            "rolling_hash = %(rolling_hash)s, "
            "page_count = plan.page_count + 1, item_count = %(item_count)s, "
            "redacted_samples = %(redacted_samples)s, "
            "plan_hash = %(plan_hash)s, "
            "ready_at = CASE WHEN %(terminal)s THEN stamp.at ELSE NULL END, "
            "updated_at = stamp.at FROM stamp "
            "WHERE plan.plan_id = %(plan_id)s "
            "AND plan.account_id = %(account_id)s "
            "AND plan.folder_key = %(folder_key)s "
            "AND plan.state = 'previewing' "
            "AND plan.version = %(expected_version)s "
            "AND plan.preview_cursor IS NOT DISTINCT FROM "
            "%(expected_preview_cursor)s "
            "AND plan.preview_cursor_version = "
            "%(expected_preview_cursor_version)s "
            "AND plan.rolling_hash IS NOT DISTINCT FROM "
            "%(expected_rolling_hash)s "
            "AND plan.page_count = %(expected_page_count)s "
            "AND plan.item_count = %(expected_item_count)s "
            "AND stamp.at < plan.expires_at RETURNING "
            f"{_PLAN_SELECT_COLUMNS}",
            {
                "target_state": (
                    ColdStartPlanState.READY.value
                    if terminal
                    else ColdStartPlanState.PREVIEWING.value
                ),
                "next_cursor": batch.cursor,
                "boundary_cursor": batch.cursor if terminal else None,
                "boundary_cursor_version": page_count if terminal else None,
                "rolling_hash": rolling_hash,
                "item_count": item_count,
                "redacted_samples": Jsonb(
                    [
                        {
                            "kind": sample.kind.value,
                            "external_email_id_hash": sample.external_email_id_hash,
                        }
                        for sample in samples
                    ]
                ),
                "plan_hash": plan_hash,
                "terminal": terminal,
                "plan_id": current.view.plan_id,
                "account_id": current.view.account_id,
                "folder_key": current.view.canonical_folder,
                "expected_version": current.version,
                "expected_preview_cursor": current.preview_cursor,
                "expected_preview_cursor_version": current.preview_cursor_version,
                "expected_rolling_hash": current.rolling_hash,
                "expected_page_count": current.view.page_count,
                "expected_item_count": current.view.item_count,
            },
        )
        row = await updated.fetchone()
        if row is None:
            latest_database_now = await _read_database_now(connection)
            if latest_database_now >= current.view.expires_at:
                return await self._write_cold_start_block(
                    connection,
                    current=current,
                    cursor_binding=cursor_binding,
                    database_now=latest_database_now,
                    safe_code="cold_start.expired",
                )
            raise ColdStartStateConflictError()
        try:
            committed = _cold_start_plan_from_row(row)
        except ValueError:
            raise ColdStartStateConflictError() from None
        _validate_committed_preview_page(
            committed,
            previous=current,
            batch=batch,
            rolling_hash=rolling_hash,
            samples=samples,
            plan_hash=plan_hash,
        )
        return committed

    async def _read_preview_page_context(
        self,
        connection: Any,
        *,
        account_id: int,
        canonical_folder: str,
        plan_id: UUID,
    ) -> tuple[
        _ColdStartPlanRecord,
        _OwnershipSnapshot,
        tuple[str | None, SyncCursorStatus, int],
        datetime,
    ]:
        await _configure_sync_xid(connection, account_id)
        ownership = await _read_current_ownership(
            connection,
            account_id,
            for_key_share=False,
        )
        cursor_binding = await _read_cold_start_cursor(
            connection,
            account_id,
            canonical_folder,
        )
        try:
            plan = await _read_cold_start_plan(connection, plan_id)
        except ValueError:
            raise ColdStartStateConflictError() from None
        if plan is None:
            raise ColdStartPlanNotFoundError()
        database_now = await _read_database_now(connection)
        return plan, ownership, cursor_binding, database_now

    @staticmethod
    def _locator_pool_contract_is_valid(pool: object) -> bool:
        try:
            kwargs = getattr(pool, "kwargs", None)
            return (
                isinstance(kwargs, Mapping)
                and kwargs.get("autocommit") is True
                and getattr(pool, "close_returns", None) is False
            )
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            return False

    @staticmethod
    def _locator_connection_is_open_idle(connection: object) -> bool:
        try:
            info = getattr(connection, "info", None)
            return (
                getattr(connection, "autocommit", None) is True
                and getattr(connection, "closed", None) is False
                and getattr(info, "transaction_status", None) is TransactionStatus.IDLE
            )
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            return False

    @staticmethod
    def _locator_process_control(error: BaseException | None) -> bool:
        return error is not None and not isinstance(error, Exception)

    @staticmethod
    async def _capture_locator_cleanup_call(call: Any) -> BaseException | None:
        try:
            await call()
        except BaseException as error:
            return error
        return None

    @staticmethod
    def _consume_locator_cleanup_task(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except BaseException:
            pass

    async def _run_bounded_locator_cleanup_call(
        self,
        call: Any,
        *,
        deadline: float,
    ) -> tuple[BaseException | None, bool]:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return TimeoutError(), True
        task = asyncio.create_task(self._capture_locator_cleanup_call(call))
        done, _pending = await asyncio.wait({task}, timeout=remaining)
        if task in done or task.done():
            return task.result(), False
        task.cancel()
        task.add_done_callback(self._consume_locator_cleanup_task)
        return TimeoutError(), True

    async def _run_locator_retirement(
        self,
        connection: Any,
        *,
        deadline: float,
    ) -> tuple[BaseException | None, BaseException | None, bool]:
        errors: list[BaseException] = []
        confirmed = False

        error, expired = await self._run_bounded_locator_cleanup_call(
            connection.close,
            deadline=deadline,
        )
        if error is not None:
            errors.append(error)
        if not expired:
            try:
                confirmed = getattr(connection, "closed", None) is True
            except BaseException as health_error:
                errors.append(health_error)

        if not expired and not confirmed:
            try:
                pgconn = getattr(connection, "pgconn", None)
                finish = getattr(pgconn, "finish", None)
            except BaseException as finish_error:
                errors.append(finish_error)
                finish = None
            if callable(finish):
                error, expired = await self._run_bounded_locator_cleanup_call(
                    lambda: asyncio.to_thread(finish),
                    deadline=deadline,
                )
                if error is not None:
                    errors.append(error)
                if not expired:
                    try:
                        confirmed = getattr(connection, "closed", None) is True
                    except BaseException as health_error:
                        errors.append(health_error)

        if not expired and confirmed:
            return_outcome = await self._session_runner._return_connection(
                connection,
                deadline=deadline,
            )
            if return_outcome.process_error is not None:
                errors.append(return_outcome.process_error)
            if return_outcome.error is not None:
                errors.append(return_outcome.error)
            if (
                return_outcome.ownership_unknown
                and return_outcome.process_error is None
                and return_outcome.error is None
            ):
                errors.append(
                    _locator_database_error(
                        "cold_start_locator_cleanup",
                        retryable=True,
                        message="cold-start locator cleanup failed",
                    )
                )

        process_error = next(
            (error for error in errors if self._locator_process_control(error)),
            None,
        )
        ordinary_error = next(
            (error for error in errors if not self._locator_process_control(error)),
            None,
        )
        return process_error, ordinary_error, confirmed

    async def _capture_locator_retirement(
        self,
        connection: Any,
        *,
        deadline: float,
    ) -> tuple[BaseException | None, BaseException | None, bool]:
        try:
            return await self._run_locator_retirement(
                connection,
                deadline=deadline,
            )
        except BaseException as error:
            if self._locator_process_control(error):
                return error, None, False
            return None, error, False

    async def _retire_locator_connection(
        self,
        connection: Any,
    ) -> tuple[BaseException | None, BaseException | None, bool]:
        deadline = asyncio.get_running_loop().time() + self._cleanup_timeout
        task = asyncio.create_task(
            self._capture_locator_retirement(
                connection,
                deadline=deadline,
            )
        )
        interruptions: list[BaseException] = []
        while not task.done():
            try:
                await asyncio.shield(task)
            except BaseException as error:
                interruptions.append(error)
        cleanup_process, cleanup_error, confirmed = task.result()
        process_error = next(
            (error for error in interruptions if self._locator_process_control(error)),
            cleanup_process,
        )
        ordinary_error = next(
            (
                error
                for error in interruptions
                if not self._locator_process_control(error)
            ),
            cleanup_error,
        )
        return process_error, ordinary_error, confirmed

    async def _raise_after_locator_retirement(
        self,
        connection: Any,
        *,
        original_errors: tuple[BaseException | None, ...] = (),
        pool_contract: bool = False,
    ) -> None:
        (
            cleanup_process,
            cleanup_error,
            confirmed,
        ) = await self._retire_locator_connection(connection)
        original_process = next(
            (
                error
                for error in original_errors
                if self._locator_process_control(error)
            ),
            None,
        )
        if original_process is not None:
            raise original_process
        if cleanup_process is not None:
            raise cleanup_process
        if pool_contract and cleanup_error is None and confirmed:
            raise _locator_database_error(
                "cold_start_locator_pool_contract",
                retryable=False,
                message="cold-start locator pool contract is invalid",
            )
        raise _locator_database_error(
            "cold_start_locator_cleanup",
            retryable=True,
            message="cold-start locator cleanup failed",
        )

    async def _return_locator_connection(
        self,
        connection: Any,
        connection_holder: list[Any | None],
    ) -> _ConnectionReturnOutcome:
        return_outcome = await self._session_runner._return_connection(connection)
        if return_outcome.returned:
            connection_holder[0] = _LOCATOR_RETURNED
        return return_outcome

    @staticmethod
    def _raise_if_locator_cancel_requested() -> None:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError()

    @staticmethod
    def _consume_locator_primary_task(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except BaseException:
            pass

    def _schedule_late_locator_retirement(
        self,
        task: asyncio.Task[Any],
        connection_holder: list[Any | None],
    ) -> None:
        def retire_after_primary(primary_task: asyncio.Task[Any]) -> None:
            self._consume_locator_primary_task(primary_task)
            connection = connection_holder[0]
            if connection is None or connection is _LOCATOR_RETURNED:
                return
            connection_holder[0] = _LOCATOR_RETURNED
            cleanup_task = asyncio.create_task(
                self._retire_locator_connection(connection)
            )
            cleanup_task.add_done_callback(self._consume_locator_cleanup_task)

        task.add_done_callback(retire_after_primary)

    async def _capture_locator_primary(
        self,
        plan_id: UUID,
        connection_holder: list[Any | None],
    ) -> tuple[_LocatorPrimaryOutcome | None, BaseException | None]:
        try:
            return await self._run_locator_primary(plan_id, connection_holder), None
        except BaseException as error:
            return None, error

    async def _run_locator_primary_bounded(
        self,
        plan_id: UUID,
        connection_holder: list[Any | None],
    ) -> _LocatorPrimaryOutcome:
        task = asyncio.create_task(
            self._capture_locator_primary(plan_id, connection_holder)
        )
        try:
            done, _pending = await asyncio.wait(
                {task},
                timeout=self._locator_timeout,
            )
        except BaseException as primary:
            task.cancel()
            try:
                done_after_cancel, _pending = await asyncio.wait(
                    {task},
                    timeout=self._cleanup_timeout,
                )
            except BaseException:
                done_after_cancel = set()
            if task in done_after_cancel or task.done():
                outcome, child_error = task.result()
                if child_error is not None:
                    raise child_error
                if type(outcome) is _LocatorPrimaryOutcome:
                    raise primary
            if connection_holder[0] is None:
                self._schedule_late_locator_retirement(task, connection_holder)
            else:
                task.add_done_callback(self._consume_locator_primary_task)
            raise primary
        if task in done or task.done():
            outcome, error = task.result()
            if error is not None:
                raise error
            if type(outcome) is not _LocatorPrimaryOutcome:
                raise _locator_database_error(
                    "cold_start_locator_cleanup",
                    retryable=True,
                    message="cold-start locator cleanup failed",
                )
            return outcome

        task.cancel()
        await asyncio.sleep(0)
        if connection_holder[0] is None:
            self._schedule_late_locator_retirement(task, connection_holder)
        else:
            task.add_done_callback(self._consume_locator_primary_task)
        raise TimeoutError()

    async def _run_locator_primary(
        self,
        plan_id: UUID,
        connection_holder: list[Any | None],
    ) -> _LocatorPrimaryOutcome:
        connection = await self._maintenance_pool.getconn()
        connection_holder[0] = connection
        self._raise_if_locator_cancel_requested()
        if not self._locator_connection_is_open_idle(connection):
            return _LocatorPrimaryOutcome(
                connection=connection,
                row=None,
                primary_error=None,
                return_error=None,
                failure="pool_contract",
                returned=False,
            )

        try:
            cursor = await connection.execute(_LOCATE_PLAN_SQL, (plan_id,))
            self._raise_if_locator_cancel_requested()
            row = await cursor.fetchone()
            self._raise_if_locator_cancel_requested()
        except asyncio.CancelledError:
            raise
        except BaseException as primary:
            try:
                healthy = self._locator_connection_is_open_idle(connection)
            except BaseException as health_error:
                return _LocatorPrimaryOutcome(
                    connection=connection,
                    row=None,
                    primary_error=primary,
                    return_error=health_error,
                    failure="cleanup",
                    returned=False,
                )
            if not healthy:
                return _LocatorPrimaryOutcome(
                    connection=connection,
                    row=None,
                    primary_error=primary,
                    return_error=None,
                    failure="cleanup",
                    returned=False,
                )
            return_outcome = await self._return_locator_connection(
                connection,
                connection_holder,
            )
            self._raise_if_locator_cancel_requested()
            return_error = (
                return_outcome.process_error
                if return_outcome.process_error is not None
                else return_outcome.error
            )
            if not return_outcome.returned:
                return _LocatorPrimaryOutcome(
                    connection=connection,
                    row=None,
                    primary_error=primary,
                    return_error=return_error,
                    failure="return_unknown",
                    returned=False,
                )
            return _LocatorPrimaryOutcome(
                connection=connection,
                row=None,
                primary_error=primary,
                return_error=return_error,
                failure=None,
                returned=True,
            )

        if not self._locator_connection_is_open_idle(connection):
            return _LocatorPrimaryOutcome(
                connection=connection,
                row=row,
                primary_error=None,
                return_error=None,
                failure="cleanup",
                returned=False,
            )
        return_outcome = await self._return_locator_connection(
            connection,
            connection_holder,
        )
        self._raise_if_locator_cancel_requested()
        return_error = (
            return_outcome.process_error
            if return_outcome.process_error is not None
            else return_outcome.error
        )
        if not return_outcome.returned:
            return _LocatorPrimaryOutcome(
                connection=connection,
                row=row,
                primary_error=None,
                return_error=return_error,
                failure="return_unknown",
                returned=False,
            )
        return _LocatorPrimaryOutcome(
            connection=connection,
            row=row,
            primary_error=None,
            return_error=return_error,
            failure=None,
            returned=True,
        )

    async def _locate_plan_identity(
        self,
        plan_id: UUID,
    ) -> _LocatedPlanIdentity:
        exact_plan_id = _require_uuid("plan_id", plan_id)
        if not self._locator_pool_contract_is_valid(self._maintenance_pool):
            raise _locator_database_error(
                "cold_start_locator_pool_contract",
                retryable=False,
                message="cold-start locator pool contract is invalid",
            )

        connection_holder: list[Any | None] = [None]
        try:
            outcome = await self._run_locator_primary_bounded(
                exact_plan_id,
                connection_holder,
            )
            process_error = next(
                (
                    error
                    for error in (outcome.primary_error, outcome.return_error)
                    if self._locator_process_control(error)
                ),
                None,
            )
            if process_error is not None:
                raise process_error
        except BaseException as primary:
            connection = connection_holder[0]
            if connection is _LOCATOR_RETURNED:
                if self._locator_process_control(primary):
                    raise primary
                raise _locator_database_error(
                    "cold_start_locator_cleanup",
                    retryable=True,
                    message="cold-start locator cleanup failed",
                ) from None
            if connection is None:
                if self._locator_process_control(primary):
                    raise primary
                raise _locator_database_error(
                    "cold_start_locator_checkout",
                    retryable=True,
                    message="cold-start locator checkout failed",
                ) from None
            await self._raise_after_locator_retirement(
                connection,
                original_errors=(primary,),
            )

        if not outcome.returned:
            await self._raise_after_locator_retirement(
                outcome.connection,
                original_errors=(outcome.primary_error, outcome.return_error),
                pool_contract=outcome.failure == "pool_contract",
            )
        if outcome.primary_error is not None:
            raise outcome.primary_error
        if outcome.return_error is not None:
            raise _locator_database_error(
                "cold_start_locator_cleanup",
                retryable=True,
                message="cold-start locator cleanup failed",
            )
        row = outcome.row
        if row is None:
            raise ColdStartPlanNotFoundError()
        try:
            return _located_plan_identity_from_row(exact_plan_id, row)
        except BaseException as error:
            if self._locator_process_control(error):
                raise
            raise _locator_database_error(
                "cold_start_locator_row",
                retryable=False,
                message="cold-start locator row is invalid",
            ) from None


def _locator_database_error(
    operation: str,
    *,
    retryable: bool,
    message: str,
) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation=operation,
        retryable=retryable,
        message=message,
    )


def _located_plan_identity_from_row(
    expected_plan_id: UUID,
    row: object,
) -> _LocatedPlanIdentity:
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != {"plan_id", "account_id", "folder_key"}
    ):
        raise ValueError("locator row has an unexpected shape")
    row_plan_id = _require_uuid("row.plan_id", row["plan_id"])
    if row_plan_id != expected_plan_id:
        raise ValueError("locator row plan identity does not match")
    account_id = _require_exact_int("row.account_id", row["account_id"], minimum=1)
    folder_key = row["folder_key"]
    if type(folder_key) is not str:
        raise ValueError("locator row folder must be an exact string")
    canonical_folder = require_canonical_folder_identity(folder_key)
    return _LocatedPlanIdentity(
        plan_id=row_plan_id,
        account_id=account_id,
        canonical_folder=canonical_folder,
    )


async def _read_cold_start_cursor(
    connection: Any,
    account_id: int,
    canonical_folder: str,
) -> tuple[str | None, SyncCursorStatus, int]:
    cursor = await connection.execute(
        "SELECT cursor, status, version FROM public.sync_cursors "
        "WHERE account_id = %s AND folder_key = %s FOR UPDATE",
        (account_id, canonical_folder),
    )
    row = await cursor.fetchone()
    if row is None:
        raise ColdStartStateConflictError()
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != {"cursor", "status", "version"}
    ):
        raise _locator_database_error(
            "cold_start_cursor_row",
            retryable=False,
            message="cold-start cursor row is invalid",
        )
    try:
        value = _require_optional_cursor("cursor", row["cursor"])
        raw_status = row["status"]
        if type(raw_status) is not str:
            raise ValueError("cursor status must be an exact string")
        status = SyncCursorStatus(raw_status)
        version = _require_exact_int(
            "cursor.version",
            row["version"],
            minimum=0,
        )
    except ValueError:
        raise _locator_database_error(
            "cold_start_cursor_row",
            retryable=False,
            message="cold-start cursor row is invalid",
        ) from None
    return value, status, version


async def _read_apply_cursor(
    connection: Any,
    account_id: int,
    canonical_folder: str,
) -> _ApplyCursorRecord:
    cursor = await connection.execute(
        "SELECT cursor, status, version, blocked_reason_code, "
        "contract_fingerprint, blocked_at, transient_failures, retry_after_at, "
        "cold_start_plan_id, cold_start_plan_state, last_attempt_at, "
        "last_success_at, updated_at FROM public.sync_cursors "
        "WHERE account_id = %s AND folder_key = %s FOR UPDATE",
        (account_id, canonical_folder),
    )
    row = await cursor.fetchone()
    if row is None:
        raise ColdStartStateConflictError()
    try:
        return _apply_cursor_record_from_row(row)
    except ValueError:
        raise _apply_invariant("cold_start_apply_cursor_row") from None


def _apply_cursor_record_from_row(row: object) -> _ApplyCursorRecord:
    keys = {
        "cursor",
        "status",
        "version",
        "blocked_reason_code",
        "contract_fingerprint",
        "blocked_at",
        "transient_failures",
        "retry_after_at",
        "cold_start_plan_id",
        "cold_start_plan_state",
        "last_attempt_at",
        "last_success_at",
        "updated_at",
    }
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != keys
    ):
        raise ValueError("apply cursor row has an unexpected shape")
    raw_status = row["status"]
    if type(raw_status) is not str:
        raise ValueError("apply cursor status must be an exact string")
    try:
        status = SyncCursorStatus(raw_status)
    except ValueError:
        raise ValueError("apply cursor status is unknown") from None
    raw_plan_id = row["cold_start_plan_id"]
    plan_id = (
        None
        if raw_plan_id is None
        else _require_uuid("cursor.cold_start_plan_id", raw_plan_id)
    )
    raw_plan_state = row["cold_start_plan_state"]
    if raw_plan_state is None:
        plan_state = None
    else:
        if type(raw_plan_state) is not str:
            raise ValueError("apply cursor plan state must be an exact string")
        try:
            plan_state = ColdStartPlanState(raw_plan_state)
        except ValueError:
            raise ValueError("apply cursor plan state is unknown") from None
    record = _ApplyCursorRecord(
        cursor=_require_optional_cursor("cursor", row["cursor"]),
        status=status,
        version=_require_exact_int("cursor.version", row["version"], minimum=0),
        blocked_reason_code=(
            None
            if row["blocked_reason_code"] is None
            else _require_safe_code(
                "cursor.blocked_reason_code",
                row["blocked_reason_code"],
            )
        ),
        contract_fingerprint=(
            None
            if row["contract_fingerprint"] is None
            else _require_sha256(
                "cursor.contract_fingerprint",
                row["contract_fingerprint"],
            )
        ),
        blocked_at=_normalize_optional_database_datetime(
            "cursor.blocked_at",
            row["blocked_at"],
        ),
        transient_failures=_require_exact_int(
            "cursor.transient_failures",
            row["transient_failures"],
            minimum=0,
        ),
        retry_after_at=_normalize_optional_database_datetime(
            "cursor.retry_after_at",
            row["retry_after_at"],
        ),
        cold_start_plan_id=plan_id,
        cold_start_plan_state=plan_state,
        last_attempt_at=_normalize_optional_database_datetime(
            "cursor.last_attempt_at",
            row["last_attempt_at"],
        ),
        last_success_at=_normalize_optional_database_datetime(
            "cursor.last_success_at",
            row["last_success_at"],
        ),
        updated_at=_normalize_database_datetime(
            "cursor.updated_at",
            row["updated_at"],
        ),
    )
    return record


def _applied_cursor_from_row(row: object) -> _AppliedCursorRecord:
    keys = {
        "cursor",
        "status",
        "version",
        "blocked_reason_code",
        "contract_fingerprint",
        "blocked_at",
        "transient_failures",
        "retry_after_at",
        "cold_start_plan_id",
        "cold_start_plan_state",
        "last_attempt_at",
        "last_success_at",
        "updated_at",
    }
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != keys
    ):
        raise _apply_invariant("cold_start_apply_cursor_row")
    try:
        cursor = _apply_cursor_record_from_row(row)
        last_attempt_at = _require_utc_datetime(
            "cursor.last_attempt_at",
            cursor.last_attempt_at,
        )
        last_success_at = _require_utc_datetime(
            "cursor.last_success_at",
            cursor.last_success_at,
        )
    except ValueError:
        raise _apply_invariant("cold_start_apply_cursor_row") from None
    return _AppliedCursorRecord(
        cursor=cursor,
        last_attempt_at=last_attempt_at,
        last_success_at=last_success_at,
        updated_at=cursor.updated_at,
    )


def _eligible_preview_cursor(
    status: SyncCursorStatus,
    cursor: str | None,
) -> tuple[SyncCursorStatus, str | None]:
    if status is SyncCursorStatus.COLD_START_PENDING and cursor is None:
        return status, None
    if status is SyncCursorStatus.RESET_REQUIRED and cursor is not None:
        return status, _require_cursor("expected_cursor", cursor)
    raise ColdStartStateConflictError()


async def _read_cold_start_plan(
    connection: Any,
    plan_id: UUID,
) -> _ColdStartPlanRecord | None:
    cursor = await connection.execute(
        f"SELECT {_PLAN_SELECT_COLUMNS} "
        "FROM public.sync_cold_start_plans WHERE plan_id = %s FOR UPDATE",
        (plan_id,),
    )
    row = await cursor.fetchone()
    return None if row is None else _cold_start_plan_from_row(row)


def _cold_start_plan_from_row(row: object) -> _ColdStartPlanRecord:
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != _PLAN_ROW_KEYS
    ):
        raise ValueError("cold-start internal plan row has an unexpected shape")
    public_row = {key: row[key] for key in _PLAN_VIEW_ROW_KEYS}
    view = _plan_view_from_row(public_row)

    raw_expected_status = row["expected_cursor_status"]
    if type(raw_expected_status) is not str:
        raise ValueError("expected cursor status must be an exact string")
    try:
        expected_status = SyncCursorStatus(raw_expected_status)
    except ValueError:
        raise ValueError("expected cursor status is unknown") from None
    expected_cursor = _require_optional_cursor(
        "expected_cursor",
        row["expected_cursor"],
    )
    _eligible_preview_cursor(expected_status, expected_cursor)
    expected_cursor_version = _require_exact_int(
        "expected_cursor_version",
        row["expected_cursor_version"],
        minimum=0,
    )

    pipeline_name = _require_exact_text(
        "pipeline_name",
        row["pipeline_name"],
        max_length=64,
    )
    ownership = _OwnershipSnapshot(
        pipeline_name=pipeline_name,
        generation=_require_exact_int("generation", row["generation"], minimum=1),
        fencing_token=_require_exact_int(
            "fencing_token",
            row["fencing_token"],
            minimum=1,
        ),
    )
    version = _require_exact_int("plan.version", row["version"], minimum=0)
    preview_cursor = _require_optional_cursor(
        "preview_cursor",
        row["preview_cursor"],
    )
    preview_cursor_version = _require_exact_int(
        "preview_cursor_version",
        row["preview_cursor_version"],
        minimum=0,
    )
    boundary_cursor_version = _optional_nonnegative_int(
        "boundary_cursor_version",
        row["boundary_cursor_version"],
    )
    apply_cursor = _require_optional_cursor("apply_cursor", row["apply_cursor"])
    apply_cursor_version = _optional_nonnegative_int(
        "apply_cursor_version",
        row["apply_cursor_version"],
    )
    rolling_hash = row["rolling_hash"]
    if rolling_hash is not None:
        rolling_hash = _require_sha256("rolling_hash", rolling_hash)
    actor = _require_exact_text("actor", row["actor"], max_length=128)
    reason = _require_exact_text("reason", row["reason"], max_length=512)

    if preview_cursor_version != view.page_count:
        raise ValueError("preview cursor version does not match page count")
    if view.page_count == 0:
        if preview_cursor is not None or rolling_hash is not None:
            raise ValueError("zero-page preview progress is inconsistent")
    elif preview_cursor is None or rolling_hash is None:
        raise ValueError("nonzero preview progress is incomplete")
    if view.boundary_cursor is None:
        if boundary_cursor_version is not None:
            raise ValueError("boundary cursor version must be absent")
    elif (
        boundary_cursor_version != preview_cursor_version
        or view.boundary_cursor != preview_cursor
    ):
        raise ValueError("boundary cursor does not match preview progress")
    if (apply_cursor is None) != (apply_cursor_version is None):
        raise ValueError("apply cursor binding is incomplete")

    return _ColdStartPlanRecord(
        view=view,
        expected_cursor_status=expected_status,
        expected_cursor=expected_cursor,
        expected_cursor_version=expected_cursor_version,
        ownership=ownership,
        version=version,
        preview_cursor=preview_cursor,
        preview_cursor_version=preview_cursor_version,
        boundary_cursor_version=boundary_cursor_version,
        apply_cursor=apply_cursor,
        apply_cursor_version=apply_cursor_version,
        rolling_hash=rolling_hash,
        actor=actor,
        reason=reason,
    )


def _optional_nonnegative_int(name: str, value: object) -> int | None:
    if value is None:
        return None
    return _require_exact_int(name, value, minimum=0)


def _require_open_plan_row(row: object) -> UUID:
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != {"plan_id"}
    ):
        raise _locator_database_error(
            "cold_start_plan_row",
            retryable=False,
            message="cold-start plan row is invalid",
        )
    try:
        return _require_uuid("open_plan.plan_id", row["plan_id"])
    except ValueError:
        raise _locator_database_error(
            "cold_start_plan_row",
            retryable=False,
            message="cold-start plan row is invalid",
        ) from None


def _receipt_plan_id(receipt: object) -> UUID:
    if type(receipt) is not CommandReceipt or type(receipt.result_id) is not str:
        raise ColdStartStateConflictError()
    try:
        plan_id = UUID(receipt.result_id)
    except (AttributeError, ValueError):
        raise ColdStartStateConflictError() from None
    if str(plan_id) != receipt.result_id:
        raise ColdStartStateConflictError()
    return plan_id


def _preview_receipt_result_hash(plan: _ColdStartPlanRecord) -> str:
    expected_cursor_hash = (
        None if plan.expected_cursor is None else _cursor_digest(plan.expected_cursor)
    )
    return _preview_result_digest(
        plan_id=plan.view.plan_id,
        account_id=plan.view.account_id,
        canonical_folder=plan.view.canonical_folder,
        expected_cursor_status=plan.expected_cursor_status,
        expected_cursor_version=plan.expected_cursor_version,
        expected_cursor_hash=expected_cursor_hash,
        pipeline_name=plan.ownership.pipeline_name,
        generation=plan.ownership.generation,
        fencing_token=plan.ownership.fencing_token,
        contract_fingerprint=plan.view.contract_fingerprint,
        folder_scope_config_hash=plan.view.folder_scope_config_hash,
        created_at=plan.view.created_at,
        expires_at=plan.view.expires_at,
    )


def _validate_preview_receipt(
    receipt: object,
    *,
    plan: _ColdStartPlanRecord,
    account_id: int,
    idempotency_key: str,
    canonical_payload_hash: str,
    actor: str,
    reason: str,
) -> None:
    try:
        valid = (
            type(receipt) is CommandReceipt
            and type(receipt.id) is UUID
            and type(receipt.account_id) is int
            and type(receipt.command_name) is str
            and type(receipt.idempotency_key_hash) is str
            and type(receipt.canonical_payload_hash) is str
            and type(receipt.outcome) is str
            and type(receipt.result_type) is str
            and type(receipt.result_id) is str
            and type(receipt.result_hash) is str
            and type(receipt.authority_epoch) is int
            and type(receipt.created_at) is datetime
            and receipt.account_id == account_id
            and receipt.command_name == _PREVIEW_COMMAND
            and receipt.idempotency_key_hash
            == _hash_idempotency_key(
                account_id,
                _PREVIEW_COMMAND,
                idempotency_key,
            )
            and receipt.canonical_payload_hash == canonical_payload_hash
            and receipt.outcome == "succeeded"
            and receipt.result_type == _PLAN_RESULT_TYPE
            and receipt.result_id == str(plan.view.plan_id)
            and receipt.result_hash == _preview_receipt_result_hash(plan)
            and receipt.authority_epoch == plan.ownership.fencing_token
            and plan.actor == actor
            and plan.reason == reason
            and _require_utc_datetime("receipt.created_at", receipt.created_at)
            is receipt.created_at
        )
    except (AttributeError, TypeError, ValueError):
        valid = False
    if not valid:
        raise ColdStartStateConflictError()


def _validate_accepted_preview_plan(
    plan: _ColdStartPlanRecord,
    *,
    plan_id: UUID,
    account_id: int,
    canonical_folder: str,
    expected_cursor_status: SyncCursorStatus,
    expected_cursor: str | None,
    expected_cursor_version: int,
    ownership: _OwnershipSnapshot,
    contract_fingerprint: str,
    folder_scope_config_hash: str,
    actor: str,
    reason: str,
    plan_ttl_seconds: int,
) -> None:
    view = plan.view
    valid = (
        view.plan_id == plan_id
        and view.account_id == account_id
        and view.canonical_folder == canonical_folder
        and view.state is ColdStartPlanState.PREVIEWING
        and plan.expected_cursor_status is expected_cursor_status
        and plan.expected_cursor == expected_cursor
        and plan.expected_cursor_version == expected_cursor_version
        and plan.ownership == ownership
        and plan.version == 0
        and plan.preview_cursor is None
        and plan.preview_cursor_version == 0
        and plan.boundary_cursor_version is None
        and plan.apply_cursor is None
        and plan.apply_cursor_version is None
        and plan.rolling_hash is None
        and view.page_count == 0
        and view.item_count == 0
        and view.redacted_samples == ()
        and view.contract_fingerprint == contract_fingerprint
        and view.folder_scope_config_hash == folder_scope_config_hash
        and plan.actor == actor
        and plan.reason == reason
        and view.created_at == view.updated_at
        and view.expires_at == view.created_at + timedelta(seconds=plan_ttl_seconds)
    )
    if not valid:
        raise ColdStartStateConflictError()


def _apply_plan_identity_matches(
    plan: _ColdStartPlanRecord,
    identity: _LocatedPlanIdentity,
) -> bool:
    return (
        type(plan) is _ColdStartPlanRecord
        and type(identity) is _LocatedPlanIdentity
        and plan.view.plan_id == identity.plan_id
        and plan.view.account_id == identity.account_id
        and plan.view.canonical_folder == identity.canonical_folder
    )


def _validate_completed_apply_plan(plan: _ColdStartPlanRecord) -> None:
    try:
        sealed_hash = _sealed_existing_plan_digest(plan)
    except (ColdStartStateConflictError, ValueError):
        raise ColdStartStateConflictError() from None
    if (
        plan.view.state is not ColdStartPlanState.COMPLETED
        or plan.view.plan_hash != sealed_hash
        or plan.apply_cursor is None
        or plan.apply_cursor_version is None
        or plan.view.completed_at is None
        or plan.view.approved_at is None
        or plan.view.completed_at < plan.view.approved_at
        or plan.view.blocked_reason_code is not None
        or plan.view.blocked_fingerprint is not None
        or plan.view.blocked_at is not None
    ):
        raise ColdStartStateConflictError()


def _validate_apply_cursor_tuple(
    cursor: _ApplyCursorRecord,
    *,
    plan_id: UUID,
) -> None:
    if type(cursor) is not _ApplyCursorRecord or type(plan_id) is not UUID:
        raise ColdStartStateConflictError()
    retry_clear = cursor.transient_failures == 0 and cursor.retry_after_at is None
    retry_scheduled = (
        cursor.transient_failures > 0 and cursor.retry_after_at is not None
    )
    if not retry_clear and not retry_scheduled:
        raise ColdStartStateConflictError()

    unbound = cursor.cold_start_plan_id is None and cursor.cold_start_plan_state is None
    if cursor.status is SyncCursorStatus.COLD_START_APPLYING:
        valid = (
            cursor.cursor is not None
            and cursor.blocked_reason_code is None
            and cursor.contract_fingerprint is None
            and cursor.blocked_at is None
            and cursor.cold_start_plan_id == plan_id
            and cursor.cold_start_plan_state is ColdStartPlanState.APPROVED
            and cursor.last_attempt_at is not None
            and cursor.last_success_at is not None
            and cursor.last_success_at <= cursor.last_attempt_at
            and cursor.updated_at == cursor.last_attempt_at
        )
    elif cursor.status is SyncCursorStatus.ACTIVE:
        valid = (
            unbound
            and cursor.cursor is not None
            and cursor.last_success_at is not None
            and cursor.blocked_reason_code is None
            and cursor.contract_fingerprint is None
            and cursor.blocked_at is None
            and (
                cursor.last_attempt_at is None
                or cursor.last_attempt_at >= cursor.last_success_at
            )
            and cursor.updated_at >= cursor.last_success_at
            and (
                cursor.last_attempt_at is None
                or cursor.updated_at >= cursor.last_attempt_at
            )
        )
    elif cursor.status is SyncCursorStatus.RESET_REQUIRED:
        valid = (
            unbound
            and cursor.cursor is not None
            and cursor.blocked_reason_code is not None
            and cursor.contract_fingerprint is None
            and cursor.blocked_at is None
            and retry_clear
            and cursor.last_attempt_at is not None
            and cursor.updated_at >= cursor.last_attempt_at
        )
    elif cursor.status is SyncCursorStatus.COLD_START_PENDING:
        valid = (
            unbound
            and cursor.cursor is None
            and cursor.blocked_reason_code is not None
            and cursor.contract_fingerprint is None
            and cursor.blocked_at is None
            and retry_clear
        )
    else:
        valid = (
            unbound
            and cursor.status is SyncCursorStatus.BLOCKED_CONTRACT
            and cursor.blocked_reason_code is not None
            and cursor.contract_fingerprint is not None
            and cursor.blocked_at is not None
            and retry_clear
        )
    if not valid:
        raise ColdStartStateConflictError()


def _apply_drift_code(
    current: _ColdStartPlanRecord,
    *,
    identity: _LocatedPlanIdentity,
    cursor: _ApplyCursorRecord,
    ownership: _OwnershipSnapshot,
    scope: FolderScope,
    contract_fingerprint: str,
    database_now: datetime,
    expected: _ColdStartPlanRecord | None = None,
    expected_cursor: _ApplyCursorRecord | None = None,
) -> str | None:
    if (
        type(current) is not _ColdStartPlanRecord
        or type(identity) is not _LocatedPlanIdentity
        or type(cursor) is not _ApplyCursorRecord
        or type(ownership) is not _OwnershipSnapshot
        or type(scope) is not FolderScope
        or type(contract_fingerprint) is not str
        or type(database_now) is not datetime
        or (expected is not None and type(expected) is not _ColdStartPlanRecord)
        or (
            expected_cursor is not None
            and type(expected_cursor) is not _ApplyCursorRecord
        )
        or (expected is None) is not (expected_cursor is None)
        or not _apply_plan_identity_matches(current, identity)
        or current.view.state is not ColdStartPlanState.APPROVED
    ):
        raise ColdStartStateConflictError()
    _validate_apply_cursor_tuple(cursor, plan_id=current.view.plan_id)
    if database_now >= current.view.expires_at:
        return "cold_start.expired"
    if (
        current.view.contract_fingerprint != contract_fingerprint
        or current.view.folder_scope_config_hash != scope.config_hash
        or current.view.canonical_folder != scope.canonical_key
        or (
            expected is not None
            and (
                current.view.contract_fingerprint != expected.view.contract_fingerprint
                or current.view.folder_scope_config_hash
                != expected.view.folder_scope_config_hash
                or current.view.canonical_folder != expected.view.canonical_folder
            )
        )
    ):
        return "cold_start.config_drift"
    if current.ownership != ownership or (
        expected is not None and current.ownership != expected.ownership
    ):
        return "cold_start.fence_drift"

    retry_clear = cursor.transient_failures == 0 and cursor.retry_after_at is None
    progress_pair_valid = (current.apply_cursor is None) is (
        current.apply_cursor_version is None
    )
    if current.apply_cursor is None:
        if cursor.status is SyncCursorStatus.COLD_START_APPLYING:
            raise ColdStartStateConflictError()
        cursor_matches_progress = (
            retry_clear
            and cursor.status is current.expected_cursor_status
            and cursor.cursor == current.expected_cursor
            and cursor.version == current.expected_cursor_version
        )
    elif current.apply_cursor_version is None:
        cursor_matches_progress = True
    else:
        cursor_matches_progress = (
            cursor.status is SyncCursorStatus.COLD_START_APPLYING
            and cursor.cursor == current.apply_cursor
            and cursor.version == current.apply_cursor_version
        )
    if not cursor_matches_progress:
        return "cold_start.cursor_drift"

    if expected is not None and expected_cursor is not None:
        cursor_identity = (
            cursor.cursor,
            cursor.status,
            cursor.version,
            cursor.blocked_reason_code,
            cursor.contract_fingerprint,
            cursor.blocked_at,
            cursor.transient_failures,
            cursor.retry_after_at,
            cursor.cold_start_plan_id,
            cursor.cold_start_plan_state,
            cursor.last_attempt_at,
            cursor.last_success_at,
            cursor.updated_at,
        )
        expected_cursor_identity = (
            expected_cursor.cursor,
            expected_cursor.status,
            expected_cursor.version,
            expected_cursor.blocked_reason_code,
            expected_cursor.contract_fingerprint,
            expected_cursor.blocked_at,
            expected_cursor.transient_failures,
            expected_cursor.retry_after_at,
            expected_cursor.cold_start_plan_id,
            expected_cursor.cold_start_plan_state,
            expected_cursor.last_attempt_at,
            expected_cursor.last_success_at,
            expected_cursor.updated_at,
        )
        if cursor_identity != expected_cursor_identity:
            return "cold_start.cursor_drift"

    progress_delta = 0
    if current.apply_cursor_version is not None:
        progress_delta = current.apply_cursor_version - current.expected_cursor_version
    progress_valid = (
        progress_pair_valid
        and current.preview_cursor_version == current.view.page_count
        and progress_delta >= 0
        and (current.apply_cursor is None or progress_delta >= 1)
        and current.version == current.view.page_count + 1 + progress_delta
    )
    if not progress_valid:
        return "cold_start.version_drift"

    if expected is not None and expected_cursor is not None:
        if (
            current.version != expected.version
            or current.apply_cursor != expected.apply_cursor
            or current.apply_cursor_version != expected.apply_cursor_version
        ):
            return "cold_start.version_drift"

    try:
        sealed_hash = _sealed_existing_plan_digest(current)
    except (ColdStartStateConflictError, ValueError):
        raise ColdStartStateConflictError() from None
    if current.view.plan_hash != sealed_hash:
        return "cold_start.plan_hash_drift"
    return None


def _validate_apply_prestate(
    plan: _ColdStartPlanRecord,
    *,
    cursor: _ApplyCursorRecord,
    ownership: _OwnershipSnapshot,
    scope: FolderScope,
    contract_fingerprint: str,
    database_now: datetime,
) -> str | None:
    try:
        sealed_hash = _sealed_existing_plan_digest(plan)
    except (ColdStartStateConflictError, ValueError):
        raise ColdStartStateConflictError() from None
    boundary_cursor = plan.view.boundary_cursor
    if (
        type(plan) is not _ColdStartPlanRecord
        or type(cursor) is not _ApplyCursorRecord
        or type(ownership) is not _OwnershipSnapshot
        or type(scope) is not FolderScope
        or type(contract_fingerprint) is not str
        or type(database_now) is not datetime
        or plan.view.state is not ColdStartPlanState.APPROVED
        or plan.view.plan_hash is None
        or plan.view.plan_hash != sealed_hash
        or boundary_cursor is None
        or plan.boundary_cursor_version is None
        or plan.view.approved_at is None
        or plan.view.completed_at is not None
        or plan.view.blocked_reason_code is not None
        or plan.view.blocked_fingerprint is not None
        or plan.view.blocked_at is not None
        or database_now >= plan.view.expires_at
        or ownership != plan.ownership
        or scope.canonical_key != plan.view.canonical_folder
        or scope.config_hash != plan.view.folder_scope_config_hash
        or contract_fingerprint != plan.view.contract_fingerprint
    ):
        raise ColdStartStateConflictError()

    retry_clear = cursor.transient_failures == 0 and cursor.retry_after_at is None
    retry_scheduled = (
        cursor.transient_failures > 0 and cursor.retry_after_at is not None
    )
    if not retry_clear and not retry_scheduled:
        raise ColdStartStateConflictError()

    if plan.apply_cursor is None:
        valid = (
            plan.apply_cursor_version is None
            and retry_clear
            and cursor.status is plan.expected_cursor_status
            and cursor.cursor == plan.expected_cursor
            and cursor.version == plan.expected_cursor_version
            and cursor.cold_start_plan_id is None
            and cursor.cold_start_plan_state is None
        )
        request_cursor = boundary_cursor
    else:
        valid = (
            plan.apply_cursor_version is not None
            and cursor.status is SyncCursorStatus.COLD_START_APPLYING
            and cursor.cursor == plan.apply_cursor
            and cursor.version == plan.apply_cursor_version
            and cursor.cold_start_plan_id == plan.view.plan_id
            and cursor.cold_start_plan_state is ColdStartPlanState.APPROVED
            and cursor.blocked_reason_code is None
            and cursor.contract_fingerprint is None
            and cursor.blocked_at is None
            and cursor.last_attempt_at is not None
            and cursor.last_success_at is not None
            and cursor.last_success_at <= cursor.last_attempt_at
            and cursor.updated_at == cursor.last_attempt_at
        )
        request_cursor = plan.apply_cursor
    if not valid:
        raise ColdStartStateConflictError()
    if retry_scheduled:
        if plan.apply_cursor is None or cursor.retry_after_at is None:
            raise ColdStartStateConflictError()
        if database_now < cursor.retry_after_at:
            return None
    return _require_cursor("apply request cursor", request_cursor)


def _validate_apply_receipt_identity(
    receipt: object,
    *,
    plan: _ColdStartPlanRecord,
    payload_hash: str,
) -> None:
    valid = False
    if type(receipt) is CommandReceipt:
        try:
            valid = (
                type(receipt.id) is UUID
                and type(receipt.account_id) is int
                and type(receipt.command_name) is str
                and type(receipt.idempotency_key_hash) is str
                and type(receipt.canonical_payload_hash) is str
                and type(receipt.outcome) is str
                and type(receipt.result_type) is str
                and type(receipt.result_id) is str
                and type(receipt.result_hash) is str
                and type(receipt.authority_epoch) is int
                and type(receipt.created_at) is datetime
                and receipt.created_at.tzinfo is UTC
                and receipt.account_id == plan.view.account_id
                and receipt.command_name == _APPLY_PAGE_COMMAND
                and receipt.idempotency_key_hash
                == _hash_idempotency_key(
                    plan.view.account_id,
                    _APPLY_PAGE_COMMAND,
                    payload_hash,
                )
                and receipt.canonical_payload_hash == payload_hash
                and receipt.outcome == "succeeded"
                and receipt.result_type == _PLAN_RESULT_TYPE
                and receipt.result_id == str(plan.view.plan_id)
                and _SHA256_PATTERN.fullmatch(receipt.result_hash) is not None
                and receipt.authority_epoch == plan.ownership.fencing_token
            )
        except (TypeError, ValueError, UnicodeError):
            valid = False
    if not valid:
        raise _apply_invariant("cold_start_apply_receipt")


def _validate_apply_page_receipt(
    receipt: object,
    *,
    plan: _ColdStartPlanRecord,
    payload_hash: str,
    result_hash: str,
) -> None:
    _validate_apply_receipt_identity(
        receipt,
        plan=plan,
        payload_hash=payload_hash,
    )
    if type(receipt) is not CommandReceipt or receipt.result_hash != result_hash:
        raise _apply_invariant("cold_start_apply_receipt")


def _validate_scheduled_retry_cursor(
    applied: _AppliedCursorRecord,
    *,
    previous: _ApplyCursorRecord,
    failure_count: int,
    retry_delay_seconds: int,
    database_stamp: datetime,
) -> None:
    valid = (
        type(applied) is _AppliedCursorRecord
        and type(previous) is _ApplyCursorRecord
        and type(failure_count) is int
        and failure_count == previous.transient_failures + 1
        and type(retry_delay_seconds) is int
        and retry_delay_seconds >= 1
        and type(database_stamp) is datetime
        and previous.status is SyncCursorStatus.COLD_START_APPLYING
        and previous.last_success_at is not None
        and applied.cursor.cursor == previous.cursor
        and applied.cursor.status is previous.status
        and applied.cursor.version == previous.version
        and applied.cursor.blocked_reason_code == previous.blocked_reason_code
        and applied.cursor.contract_fingerprint == previous.contract_fingerprint
        and applied.cursor.blocked_at == previous.blocked_at
        and applied.cursor.transient_failures == failure_count
        and applied.cursor.retry_after_at
        == database_stamp + timedelta(seconds=retry_delay_seconds)
        and applied.cursor.cold_start_plan_id == previous.cold_start_plan_id
        and applied.cursor.cold_start_plan_state is previous.cold_start_plan_state
        and applied.last_attempt_at == database_stamp
        and applied.last_success_at == previous.last_success_at
        and applied.updated_at == database_stamp
    )
    if not valid:
        raise _apply_invariant("cold_start_apply_retry")


def _validate_applied_cursor(
    applied: _AppliedCursorRecord,
    *,
    previous: _ApplyCursorRecord,
    next_cursor: str,
    terminal: bool,
    plan_id: UUID,
    database_stamp: datetime,
) -> None:
    expected_status = (
        SyncCursorStatus.ACTIVE if terminal else SyncCursorStatus.COLD_START_APPLYING
    )
    valid = (
        type(applied) is _AppliedCursorRecord
        and type(previous) is _ApplyCursorRecord
        and type(next_cursor) is str
        and type(terminal) is bool
        and type(plan_id) is UUID
        and type(database_stamp) is datetime
        and applied.cursor.cursor == next_cursor
        and applied.cursor.status is expected_status
        and applied.cursor.version == previous.version + 1
        and applied.cursor.blocked_reason_code is None
        and applied.cursor.contract_fingerprint is None
        and applied.cursor.blocked_at is None
        and applied.cursor.transient_failures == 0
        and applied.cursor.retry_after_at is None
        and applied.last_attempt_at == database_stamp
        and applied.last_success_at == database_stamp
        and applied.updated_at == database_stamp
    )
    if terminal:
        valid = valid and (
            applied.cursor.cold_start_plan_id is None
            and applied.cursor.cold_start_plan_state is None
        )
    else:
        valid = valid and (
            applied.cursor.cold_start_plan_id == plan_id
            and applied.cursor.cold_start_plan_state is ColdStartPlanState.APPROVED
        )
    if not valid:
        raise _apply_invariant("cold_start_apply_cursor")


def _validate_applied_plan(
    applied: _ColdStartPlanRecord,
    *,
    previous: _ColdStartPlanRecord,
    applied_cursor: _AppliedCursorRecord,
    batch: SyncBatch,
    database_stamp: datetime,
) -> None:
    terminal = batch.includes_last
    expected_state = (
        ColdStartPlanState.COMPLETED if terminal else ColdStartPlanState.APPROVED
    )
    valid = (
        type(applied) is _ColdStartPlanRecord
        and type(previous) is _ColdStartPlanRecord
        and type(applied_cursor) is _AppliedCursorRecord
        and type(batch) is SyncBatch
        and type(database_stamp) is datetime
        and previous.view.state is ColdStartPlanState.APPROVED
        and applied.view.state is expected_state
        and applied.version == previous.version + 1
        and applied.apply_cursor == batch.cursor
        and applied.apply_cursor_version == applied_cursor.cursor.version
        and applied.expected_cursor_status is previous.expected_cursor_status
        and applied.expected_cursor == previous.expected_cursor
        and applied.expected_cursor_version == previous.expected_cursor_version
        and applied.ownership == previous.ownership
        and applied.preview_cursor == previous.preview_cursor
        and applied.preview_cursor_version == previous.preview_cursor_version
        and applied.boundary_cursor_version == previous.boundary_cursor_version
        and applied.rolling_hash == previous.rolling_hash
        and applied.actor == previous.actor
        and applied.reason == previous.reason
        and applied.view.plan_id == previous.view.plan_id
        and applied.view.account_id == previous.view.account_id
        and applied.view.canonical_folder == previous.view.canonical_folder
        and applied.view.boundary_cursor == previous.view.boundary_cursor
        and applied.view.page_count == previous.view.page_count
        and applied.view.item_count == previous.view.item_count
        and applied.view.redacted_samples == previous.view.redacted_samples
        and applied.view.contract_fingerprint == previous.view.contract_fingerprint
        and applied.view.folder_scope_config_hash
        == previous.view.folder_scope_config_hash
        and applied.view.plan_hash == previous.view.plan_hash
        and applied.view.expires_at == previous.view.expires_at
        and applied.view.ready_at == previous.view.ready_at
        and applied.view.approved_at == previous.view.approved_at
        and applied.view.created_at == previous.view.created_at
        and applied.view.updated_at == database_stamp
        and applied.view.blocked_reason_code is None
        and applied.view.blocked_fingerprint is None
        and applied.view.blocked_at is None
    )
    if terminal:
        valid = valid and applied.view.completed_at == database_stamp
    else:
        valid = valid and applied.view.completed_at is None
    if not valid:
        raise _apply_invariant("cold_start_apply_plan")


def _apply_invariant(operation: str) -> DatabaseOperationError:
    return _locator_database_error(
        operation,
        retryable=False,
        message="cold-start apply state is invalid",
    )


def _cold_start_result_from_plan(plan: ColdStartPlanView) -> ColdStartRunResult:
    statuses = {
        ColdStartPlanState.PREVIEWING: ColdStartRunStatus.PREVIEWING,
        ColdStartPlanState.READY: ColdStartRunStatus.READY,
        ColdStartPlanState.APPROVED: ColdStartRunStatus.APPROVED,
        ColdStartPlanState.COMPLETED: ColdStartRunStatus.COMPLETED,
        ColdStartPlanState.BLOCKED: ColdStartRunStatus.BLOCKED,
    }
    return ColdStartRunResult(
        status=statuses[plan.state],
        plan=plan,
        pages_committed=0,
        changes_observed=0,
        safe_code=(
            plan.blocked_reason_code
            if plan.state is ColdStartPlanState.BLOCKED
            else None
        ),
    )


def _preview_budget_result(
    plan: ColdStartPlanView,
    *,
    pages_committed: int,
    changes_observed: int,
) -> ColdStartRunResult:
    if type(plan) is not ColdStartPlanView:
        raise ColdStartStateConflictError()
    return ColdStartRunResult(
        status=ColdStartRunStatus.BUDGET_EXHAUSTED,
        plan=plan,
        pages_committed=pages_committed,
        changes_observed=changes_observed,
        safe_code="cold_start.budget_exhausted",
    )


def _apply_budget_result(
    plan: ColdStartPlanView,
    *,
    pages_committed: int,
    changes_observed: int,
) -> ColdStartRunResult:
    if (
        type(plan) is not ColdStartPlanView
        or plan.state is not ColdStartPlanState.APPROVED
    ):
        raise ColdStartStateConflictError()
    return ColdStartRunResult(
        status=ColdStartRunStatus.BUDGET_EXHAUSTED,
        plan=plan,
        pages_committed=pages_committed,
        changes_observed=changes_observed,
        safe_code="cold_start.budget_exhausted",
    )


def _apply_retry_deferred_result(plan: ColdStartPlanView) -> ColdStartRunResult:
    if (
        type(plan) is not ColdStartPlanView
        or plan.state is not ColdStartPlanState.APPROVED
    ):
        raise ColdStartStateConflictError()
    return ColdStartRunResult(
        status=ColdStartRunStatus.RETRY_DEFERRED,
        plan=plan,
        pages_committed=0,
        changes_observed=0,
        safe_code="cold_start.retry_deferred",
    )


def _preview_blocked_result(
    plan: ColdStartPlanView,
    *,
    pages_committed: int,
    changes_observed: int,
) -> ColdStartRunResult:
    if (
        type(plan) is not ColdStartPlanView
        or plan.state is not ColdStartPlanState.BLOCKED
        or plan.blocked_reason_code is None
    ):
        raise ColdStartStateConflictError()
    return ColdStartRunResult(
        status=ColdStartRunStatus.BLOCKED,
        plan=plan,
        pages_committed=pages_committed,
        changes_observed=changes_observed,
        safe_code=plan.blocked_reason_code,
    )


async def _read_database_now(connection: Any) -> datetime:
    cursor = await connection.execute(
        "SELECT pg_catalog.clock_timestamp() AS database_now"
    )
    row = await cursor.fetchone()
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != {"database_now"}
    ):
        raise _locator_database_error(
            "cold_start_database_clock",
            retryable=False,
            message="cold-start database clock is invalid",
        )
    try:
        return _normalize_database_datetime("database_now", row["database_now"])
    except ValueError:
        raise _locator_database_error(
            "cold_start_database_clock",
            retryable=False,
            message="cold-start database clock is invalid",
        ) from None


def _validate_preview_page_context(
    current: _ColdStartPlanRecord,
    *,
    expected: _ColdStartPlanRecord,
    ownership: _OwnershipSnapshot,
    cursor_binding: tuple[str | None, SyncCursorStatus, int],
    scope: FolderScope,
    contract_fingerprint: str,
    database_now: datetime,
) -> None:
    drift_code = _preview_page_drift_code(
        current,
        expected=expected,
        ownership=ownership,
        cursor_binding=cursor_binding,
        scope=scope,
        contract_fingerprint=contract_fingerprint,
        database_now=database_now,
    )
    if drift_code is not None:
        raise ColdStartStateConflictError()


def _preview_page_drift_code(
    current: _ColdStartPlanRecord,
    *,
    expected: _ColdStartPlanRecord,
    ownership: _OwnershipSnapshot,
    cursor_binding: tuple[str | None, SyncCursorStatus, int],
    scope: FolderScope,
    contract_fingerprint: str,
    database_now: datetime,
) -> str | None:
    if (
        type(current) is not _ColdStartPlanRecord
        or type(expected) is not _ColdStartPlanRecord
        or type(ownership) is not _OwnershipSnapshot
        or type(cursor_binding) is not tuple
        or len(cursor_binding) != 3
        or type(scope) is not FolderScope
    ):
        raise ColdStartStateConflictError()
    cursor, cursor_status, cursor_version = cursor_binding
    if (
        current.view.state is not ColdStartPlanState.PREVIEWING
        or current.view.plan_id != expected.view.plan_id
        or current.view.account_id != expected.view.account_id
        or current.view.canonical_folder != expected.view.canonical_folder
    ):
        raise ColdStartStateConflictError()
    if database_now >= current.view.expires_at:
        return "cold_start.expired"
    if (
        current.view.contract_fingerprint != contract_fingerprint
        or current.view.folder_scope_config_hash != scope.config_hash
        or current.view.canonical_folder != scope.canonical_key
    ):
        return "cold_start.config_drift"
    if current.ownership != ownership:
        return "cold_start.fence_drift"
    if (
        current.expected_cursor != cursor
        or current.expected_cursor_status is not cursor_status
        or current.expected_cursor_version != cursor_version
    ):
        return "cold_start.cursor_drift"
    if current != expected:
        return "cold_start.version_drift"
    return None


def _append_preview_samples(
    existing: tuple[ColdStartSample, ...],
    *,
    account_id: int,
    batch: SyncBatch,
) -> tuple[ColdStartSample, ...]:
    if type(existing) is not tuple:
        raise ValueError("existing samples must be an exact tuple")
    samples = [
        ColdStartSample(sample.kind, sample.external_email_id_hash)
        for sample in existing
        if type(sample) is ColdStartSample
    ]
    if len(samples) != len(existing) or len(samples) > _MAX_REDACTED_SAMPLES:
        raise ValueError("existing samples are invalid")
    for change in batch.changes:
        if len(samples) >= _MAX_REDACTED_SAMPLES:
            break
        samples.append(
            ColdStartSample(
                change.kind,
                _sample_external_id_digest(
                    account_id,
                    change.external_email_id,
                ),
            )
        )
    return tuple(samples)


def _sealed_preview_plan_digest(
    plan: _ColdStartPlanRecord,
    *,
    boundary_cursor: str,
    boundary_cursor_version: int,
    rolling_hash: str,
    page_count: int,
    item_count: int,
    redacted_samples: tuple[ColdStartSample, ...],
) -> str:
    return _plan_digest(
        plan_id=plan.view.plan_id,
        account_id=plan.view.account_id,
        canonical_folder=plan.view.canonical_folder,
        expected_cursor_status=plan.expected_cursor_status,
        expected_cursor_version=plan.expected_cursor_version,
        expected_cursor_hash=(
            None
            if plan.expected_cursor is None
            else _cursor_digest(plan.expected_cursor)
        ),
        pipeline_name=plan.ownership.pipeline_name,
        generation=plan.ownership.generation,
        fencing_token=plan.ownership.fencing_token,
        boundary_cursor_hash=_cursor_digest(boundary_cursor),
        boundary_cursor_version=boundary_cursor_version,
        rolling_hash=rolling_hash,
        page_count=page_count,
        item_count=item_count,
        redacted_samples=redacted_samples,
        contract_fingerprint=plan.view.contract_fingerprint,
        folder_scope_config_hash=plan.view.folder_scope_config_hash,
        actor=plan.actor,
        reason=plan.reason,
        created_at=plan.view.created_at,
        expires_at=plan.view.expires_at,
    )


def _validate_committed_preview_page(
    committed: _ColdStartPlanRecord,
    *,
    previous: _ColdStartPlanRecord,
    batch: SyncBatch,
    rolling_hash: str,
    samples: tuple[ColdStartSample, ...],
    plan_hash: str | None,
) -> None:
    terminal = batch.includes_last
    expected_page_count = previous.view.page_count + 1
    expected_item_count = previous.view.item_count + len(batch.changes)
    valid = (
        committed.view.plan_id == previous.view.plan_id
        and committed.view.account_id == previous.view.account_id
        and committed.view.canonical_folder == previous.view.canonical_folder
        and committed.expected_cursor_status is previous.expected_cursor_status
        and committed.expected_cursor == previous.expected_cursor
        and committed.expected_cursor_version == previous.expected_cursor_version
        and committed.ownership == previous.ownership
        and committed.version == previous.version + 1
        and committed.preview_cursor == batch.cursor
        and committed.preview_cursor_version == previous.preview_cursor_version + 1
        and committed.preview_cursor_version == expected_page_count
        and committed.rolling_hash == rolling_hash
        and committed.view.page_count == expected_page_count
        and committed.view.item_count == expected_item_count
        and committed.view.redacted_samples == samples
        and committed.view.contract_fingerprint == previous.view.contract_fingerprint
        and committed.view.folder_scope_config_hash
        == previous.view.folder_scope_config_hash
        and committed.actor == previous.actor
        and committed.reason == previous.reason
        and committed.view.expires_at == previous.view.expires_at
        and committed.view.created_at == previous.view.created_at
        and previous.view.updated_at <= committed.view.updated_at
        and committed.view.updated_at < committed.view.expires_at
        and committed.apply_cursor is None
        and committed.apply_cursor_version is None
        and committed.view.approved_at is None
        and committed.view.completed_at is None
        and committed.view.blocked_reason_code is None
        and committed.view.blocked_fingerprint is None
        and committed.view.blocked_at is None
    )
    if terminal:
        valid = valid and (
            committed.view.state is ColdStartPlanState.READY
            and committed.view.boundary_cursor == batch.cursor
            and committed.boundary_cursor_version == expected_page_count
            and committed.view.plan_hash == plan_hash
            and committed.view.ready_at == committed.view.updated_at
        )
    else:
        valid = valid and (
            committed.view.state is ColdStartPlanState.PREVIEWING
            and committed.view.boundary_cursor is None
            and committed.boundary_cursor_version is None
            and committed.view.plan_hash is None
            and committed.view.ready_at is None
        )
    if not valid:
        raise ColdStartStateConflictError()


def _preview_recovery_environment_matches(
    current: _ColdStartPlanRecord,
    *,
    expected: _ColdStartPlanRecord,
    ownership: _OwnershipSnapshot,
    cursor_binding: tuple[str | None, SyncCursorStatus, int],
    scope: FolderScope,
    contract_fingerprint: str,
) -> bool:
    if (
        type(current) is not _ColdStartPlanRecord
        or type(expected) is not _ColdStartPlanRecord
        or type(ownership) is not _OwnershipSnapshot
        or type(cursor_binding) is not tuple
        or len(cursor_binding) != 3
        or type(scope) is not FolderScope
    ):
        return False
    cursor, cursor_status, cursor_version = cursor_binding
    return (
        current.view.plan_id == expected.view.plan_id
        and current.view.account_id == expected.view.account_id
        and current.view.canonical_folder == expected.view.canonical_folder
        and ownership == expected.ownership
        and cursor == expected.expected_cursor
        and cursor_status is expected.expected_cursor_status
        and cursor_version == expected.expected_cursor_version
        and scope.canonical_key == expected.view.canonical_folder
        and scope.config_hash == expected.view.folder_scope_config_hash
        and contract_fingerprint == expected.view.contract_fingerprint
    )


def _preview_recovery_invariant() -> DatabaseOperationError:
    return _locator_database_error(
        "cold_start_preview_recovery",
        retryable=False,
        message="cold-start preview recovery state is invalid",
    )


def _apply_recovery_environment_matches(
    current: _ColdStartPlanRecord,
    *,
    expected: _ColdStartPlanRecord,
    ownership: _OwnershipSnapshot,
    scope: FolderScope,
    contract_fingerprint: str,
) -> bool:
    return (
        type(current) is _ColdStartPlanRecord
        and type(expected) is _ColdStartPlanRecord
        and type(ownership) is _OwnershipSnapshot
        and type(scope) is FolderScope
        and current.view.plan_id == expected.view.plan_id
        and current.view.account_id == expected.view.account_id
        and current.view.canonical_folder == expected.view.canonical_folder
        and ownership == expected.ownership
        and scope.canonical_key == expected.view.canonical_folder
        and scope.config_hash == expected.view.folder_scope_config_hash
        and contract_fingerprint == expected.view.contract_fingerprint
    )


def _apply_recovery_invariant() -> DatabaseOperationError:
    return _locator_database_error(
        "cold_start_apply_recovery",
        retryable=False,
        message="cold-start apply recovery state is invalid",
    )


def _sealed_existing_plan_digest(plan: _ColdStartPlanRecord) -> str:
    boundary_cursor = plan.view.boundary_cursor
    boundary_version = plan.boundary_cursor_version
    rolling_hash = plan.rolling_hash
    if boundary_cursor is None or boundary_version is None or rolling_hash is None:
        raise ColdStartStateConflictError()
    return _sealed_preview_plan_digest(
        plan,
        boundary_cursor=boundary_cursor,
        boundary_cursor_version=boundary_version,
        rolling_hash=rolling_hash,
        page_count=plan.view.page_count,
        item_count=plan.view.item_count,
        redacted_samples=plan.view.redacted_samples,
    )


def _approval_drift_code(
    current: _ColdStartPlanRecord,
    *,
    identity: _LocatedPlanIdentity,
    ownership: _OwnershipSnapshot,
    cursor_binding: tuple[str | None, SyncCursorStatus, int],
    scope: FolderScope,
    contract_fingerprint: str,
    database_now: datetime,
) -> str | None:
    if (
        type(current) is not _ColdStartPlanRecord
        or type(identity) is not _LocatedPlanIdentity
        or type(ownership) is not _OwnershipSnapshot
        or type(cursor_binding) is not tuple
        or len(cursor_binding) != 3
        or type(scope) is not FolderScope
        or current.view.plan_id != identity.plan_id
        or current.view.account_id != identity.account_id
        or current.view.canonical_folder != identity.canonical_folder
        or current.view.state is not ColdStartPlanState.READY
    ):
        raise ColdStartStateConflictError()
    cursor, cursor_status, cursor_version = cursor_binding
    if database_now >= current.view.expires_at:
        return "cold_start.expired"
    if (
        current.view.contract_fingerprint != contract_fingerprint
        or current.view.folder_scope_config_hash != scope.config_hash
        or current.view.canonical_folder != scope.canonical_key
    ):
        return "cold_start.config_drift"
    if current.ownership != ownership:
        return "cold_start.fence_drift"
    if (
        current.expected_cursor != cursor
        or current.expected_cursor_status is not cursor_status
        or current.expected_cursor_version != cursor_version
    ):
        return "cold_start.cursor_drift"
    if current.version != current.view.page_count:
        return "cold_start.version_drift"
    if current.view.plan_hash != _sealed_existing_plan_digest(current):
        return "cold_start.plan_hash_drift"
    return None


def _validate_approved_plan(
    approved: _ColdStartPlanRecord,
    *,
    previous: _ColdStartPlanRecord,
) -> None:
    approved_at = approved.view.approved_at
    valid = (
        previous.view.state is ColdStartPlanState.READY
        and approved.view.state is ColdStartPlanState.APPROVED
        and approved.view.plan_id == previous.view.plan_id
        and approved.view.account_id == previous.view.account_id
        and approved.view.canonical_folder == previous.view.canonical_folder
        and approved.expected_cursor_status is previous.expected_cursor_status
        and approved.expected_cursor == previous.expected_cursor
        and approved.expected_cursor_version == previous.expected_cursor_version
        and approved.ownership == previous.ownership
        and approved.version == previous.version + 1
        and approved.preview_cursor == previous.preview_cursor
        and approved.preview_cursor_version == previous.preview_cursor_version
        and approved.boundary_cursor_version == previous.boundary_cursor_version
        and approved.apply_cursor is None
        and approved.apply_cursor_version is None
        and approved.rolling_hash == previous.rolling_hash
        and approved.view.boundary_cursor == previous.view.boundary_cursor
        and approved.view.page_count == previous.view.page_count
        and approved.view.item_count == previous.view.item_count
        and approved.view.redacted_samples == previous.view.redacted_samples
        and approved.view.contract_fingerprint == previous.view.contract_fingerprint
        and approved.view.folder_scope_config_hash
        == previous.view.folder_scope_config_hash
        and approved.view.plan_hash == previous.view.plan_hash
        and approved.actor == previous.actor
        and approved.reason == previous.reason
        and approved.view.ready_at == previous.view.ready_at
        and approved_at is not None
        and approved.view.updated_at == approved_at
        and approved_at >= previous.view.updated_at
        and approved_at < approved.view.expires_at
        and approved.view.completed_at is None
        and approved.view.blocked_reason_code is None
        and approved.view.blocked_fingerprint is None
        and approved.view.blocked_at is None
        and approved.view.created_at == previous.view.created_at
        and approved.view.expires_at == previous.view.expires_at
    )
    if not valid:
        raise ColdStartStateConflictError()


def _approval_audit_metadata(plan: ColdStartPlanView) -> dict[str, object]:
    if (
        type(plan) is not ColdStartPlanView
        or plan.plan_hash is None
        or plan.state is not ColdStartPlanState.APPROVED
    ):
        raise ColdStartStateConflictError()
    return {
        "plan_id": str(plan.plan_id),
        "plan_hash": plan.plan_hash,
        "page_count": plan.page_count,
        "item_count": plan.item_count,
        "redacted_samples": [
            {
                "kind": sample.kind.value,
                "external_email_id_hash": sample.external_email_id_hash,
            }
            for sample in plan.redacted_samples
        ],
    }


def _validate_approval_receipt(
    receipt: object,
    *,
    plan: _ColdStartPlanRecord,
    account_id: int,
    idempotency_key: str,
    canonical_payload_hash: str,
) -> None:
    approved_at = plan.view.approved_at
    plan_hash = plan.view.plan_hash
    try:
        valid = (
            type(receipt) is CommandReceipt
            and type(receipt.id) is UUID
            and type(receipt.account_id) is int
            and type(receipt.command_name) is str
            and type(receipt.idempotency_key_hash) is str
            and type(receipt.canonical_payload_hash) is str
            and type(receipt.outcome) is str
            and type(receipt.result_type) is str
            and type(receipt.result_id) is str
            and type(receipt.result_hash) is str
            and type(receipt.authority_epoch) is int
            and type(receipt.created_at) is datetime
            and approved_at is not None
            and plan_hash is not None
            and plan.view.state
            in {
                ColdStartPlanState.APPROVED,
                ColdStartPlanState.COMPLETED,
                ColdStartPlanState.BLOCKED,
            }
            and receipt.account_id == account_id
            and receipt.command_name == _APPROVE_COMMAND
            and receipt.idempotency_key_hash
            == _hash_idempotency_key(
                account_id,
                _APPROVE_COMMAND,
                idempotency_key,
            )
            and receipt.canonical_payload_hash == canonical_payload_hash
            and receipt.outcome == "succeeded"
            and receipt.result_type == _PLAN_RESULT_TYPE
            and receipt.result_id == str(plan.view.plan_id)
            and receipt.result_hash
            == _approve_result_digest(
                plan_id=plan.view.plan_id,
                plan_hash=plan_hash,
                pipeline_name=plan.ownership.pipeline_name,
                generation=plan.ownership.generation,
                fencing_token=plan.ownership.fencing_token,
                folder_scope_config_hash=plan.view.folder_scope_config_hash,
                approved_at=approved_at,
            )
            and receipt.authority_epoch == plan.ownership.fencing_token
            and _require_utc_datetime("receipt.created_at", receipt.created_at)
            is receipt.created_at
        )
    except (AttributeError, TypeError, ValueError):
        valid = False
    if not valid:
        raise ColdStartStateConflictError()


def _validate_blocked_preview_plan(
    blocked: _ColdStartPlanRecord,
    *,
    previous: _ColdStartPlanRecord,
    safe_code: str,
    blocked_fingerprint: str,
    blocked_at: datetime,
) -> None:
    valid = (
        blocked.view.state is ColdStartPlanState.BLOCKED
        and blocked.view.plan_id == previous.view.plan_id
        and blocked.view.account_id == previous.view.account_id
        and blocked.view.canonical_folder == previous.view.canonical_folder
        and blocked.expected_cursor_status is previous.expected_cursor_status
        and blocked.expected_cursor == previous.expected_cursor
        and blocked.expected_cursor_version == previous.expected_cursor_version
        and blocked.ownership == previous.ownership
        and blocked.version == previous.version + 1
        and blocked.preview_cursor == previous.preview_cursor
        and blocked.preview_cursor_version == previous.preview_cursor_version
        and blocked.boundary_cursor_version == previous.boundary_cursor_version
        and blocked.apply_cursor == previous.apply_cursor
        and blocked.apply_cursor_version == previous.apply_cursor_version
        and blocked.rolling_hash == previous.rolling_hash
        and blocked.view.boundary_cursor == previous.view.boundary_cursor
        and blocked.view.page_count == previous.view.page_count
        and blocked.view.item_count == previous.view.item_count
        and blocked.view.redacted_samples == previous.view.redacted_samples
        and blocked.view.contract_fingerprint == previous.view.contract_fingerprint
        and blocked.view.folder_scope_config_hash
        == previous.view.folder_scope_config_hash
        and blocked.view.plan_hash == previous.view.plan_hash
        and blocked.actor == previous.actor
        and blocked.reason == previous.reason
        and blocked.view.blocked_reason_code == safe_code
        and blocked.view.blocked_fingerprint == blocked_fingerprint
        and blocked.view.blocked_at == blocked_at
        and blocked.view.updated_at == blocked_at
        and blocked.view.created_at == previous.view.created_at
        and blocked.view.expires_at == previous.view.expires_at
        and blocked.view.ready_at == previous.view.ready_at
        and blocked.view.approved_at == previous.view.approved_at
        and blocked.view.completed_at is None
    )
    if not valid:
        raise ColdStartStateConflictError()


def _validate_blocked_apply_cursor(
    blocked: _ApplyCursorRecord,
    *,
    previous: _ApplyCursorRecord,
    safe_code: str,
    blocked_fingerprint: str,
    blocked_at: datetime,
) -> None:
    valid = (
        type(blocked) is _ApplyCursorRecord
        and type(previous) is _ApplyCursorRecord
        and type(safe_code) is str
        and type(blocked_fingerprint) is str
        and type(blocked_at) is datetime
        and blocked.cursor == previous.cursor
        and blocked.status is SyncCursorStatus.BLOCKED_CONTRACT
        and blocked.version == previous.version + 1
        and blocked.blocked_reason_code == safe_code
        and blocked.contract_fingerprint == blocked_fingerprint
        and blocked.blocked_at == blocked_at
        and blocked.transient_failures == 0
        and blocked.retry_after_at is None
        and blocked.cold_start_plan_id is None
        and blocked.cold_start_plan_state is None
        and blocked.last_attempt_at == blocked_at
        and blocked.last_success_at == previous.last_success_at
        and blocked.updated_at == blocked_at
    )
    if not valid:
        raise ColdStartStateConflictError()


def _require_uuid(name: str, value: object) -> UUID:
    if type(value) is not UUID:
        raise ValueError(f"{name} must be an exact UUID")
    return value


def _require_json_string(name: str, value: object) -> str:
    if type(value) is not str:
        raise ValueError(f"{name} must be an exact string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain Unicode scalar text") from None
    return value


def _validate_plain_json(value: object, *, active_ids: set[int]) -> None:
    if value is None or type(value) in (bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("canonical JSON cannot contain non-finite numbers")
        return
    if type(value) is str:
        _require_json_string("canonical JSON string", value)
        return
    if type(value) is dict:
        identity = id(value)
        if identity in active_ids:
            raise ValueError("canonical JSON cannot contain a container cycle")
        active_ids.add(identity)
        try:
            for key, item in value.items():
                _require_json_string("canonical JSON object key", key)
                _validate_plain_json(item, active_ids=active_ids)
        finally:
            active_ids.remove(identity)
        return
    if type(value) is list:
        identity = id(value)
        if identity in active_ids:
            raise ValueError("canonical JSON cannot contain a container cycle")
        active_ids.add(identity)
        try:
            for item in value:
                _validate_plain_json(item, active_ids=active_ids)
        finally:
            active_ids.remove(identity)
        return
    raise ValueError("canonical JSON requires exact built-in JSON values")


def _canonical_digest(domain: str, projection: dict[str, object]) -> str:
    if type(domain) is not str or not domain.isascii() or "\x00" in domain:
        raise ValueError("digest domain must be exact NUL-free ASCII")
    if type(projection) is not dict:
        raise ValueError("digest projection must be an exact object")
    _validate_plain_json(projection, active_ids=set())
    try:
        encoded = json.dumps(
            projection,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, UnicodeEncodeError, ValueError):
        raise ValueError("digest projection is not canonical JSON") from None
    return hashlib.sha256(domain.encode("ascii") + b"\x00" + encoded).hexdigest()


def _materialize_frozen_json(
    value: object,
    *,
    active_ids: set[int],
) -> object:
    if value is None or type(value) in (bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("frozen JSON cannot contain non-finite numbers")
        return value
    if type(value) is str:
        return _require_json_string("frozen JSON string", value)
    if type(value) is MappingProxyType:
        identity = id(value)
        if identity in active_ids:
            raise ValueError("frozen JSON cannot contain a container cycle")
        active_ids.add(identity)
        try:
            materialized: dict[str, object] = {}
            for key, item in value.items():
                exact_key = _require_json_string("frozen JSON object key", key)
                materialized[exact_key] = _materialize_frozen_json(
                    item,
                    active_ids=active_ids,
                )
            return materialized
        finally:
            active_ids.remove(identity)
    if type(value) is tuple:
        identity = id(value)
        if identity in active_ids:
            raise ValueError("frozen JSON cannot contain a container cycle")
        active_ids.add(identity)
        try:
            return [
                _materialize_frozen_json(item, active_ids=active_ids) for item in value
            ]
        finally:
            active_ids.remove(identity)
    raise ValueError("frozen JSON requires exact immutable JSON values")


def _format_digest_timestamp(name: str, value: object) -> str:
    timestamp = _require_utc_datetime(name, value)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _require_expected_cursor_binding(
    status: object,
    cursor_hash: object,
) -> tuple[SyncCursorStatus, str | None]:
    if type(status) is not SyncCursorStatus or status not in {
        SyncCursorStatus.COLD_START_PENDING,
        SyncCursorStatus.RESET_REQUIRED,
    }:
        raise ValueError("expected cursor status is not cold-start eligible")
    if status is SyncCursorStatus.COLD_START_PENDING:
        if cursor_hash is not None:
            raise ValueError("cold-start pending cursor hash must be absent")
        return status, None
    return status, _require_sha256("expected_cursor_hash", cursor_hash)


def _require_digest_identity(
    account_id: object,
    canonical_folder: object,
) -> tuple[int, str]:
    account = _require_exact_int("account_id", account_id, minimum=1)
    if type(canonical_folder) is not str:
        raise ValueError("canonical_folder must be an exact string")
    return account, require_canonical_folder_identity(canonical_folder)


def _cursor_digest(cursor: str) -> str:
    exact_cursor = _require_cursor("cursor", cursor)
    return _canonical_digest(
        "cold-start.cursor.v1",
        {"v": 1, "cursor": exact_cursor},
    )


def _sample_external_id_digest(account_id: int, external_email_id: str) -> str:
    account = _require_exact_int("account_id", account_id, minimum=1)
    identifier = _require_exact_text(
        "external_email_id",
        external_email_id,
        max_length=1024,
    )
    return _canonical_digest(
        "cold-start.sample-external-id.v1",
        {"v": 1, "account_id": account, "external_email_id": identifier},
    )


def _batch_digest(batch: SyncBatch) -> str:
    batch = _rebuild_sync_batch(batch, MAX_SYNC_CHANGES_PER_BATCH)
    _require_cursor("batch.cursor", batch.cursor)
    if (
        type(batch.contract_version) is not str
        or batch.contract_version != "exchange_sync_contract_v2"
        or type(batch.includes_last) is not bool
        or type(batch.changes) is not tuple
    ):
        raise ValueError("batch has a hostile outer shape")

    changes: list[object] = []
    for change in batch.changes:
        if type(change) is not SyncChange or type(change.kind) is not ChangeKind:
            raise ValueError("batch changes must be exact SyncChange values")
        external_email_id = _require_exact_text(
            "change.external_email_id",
            change.external_email_id,
            max_length=1024,
        )
        source_version = change.source_version
        if source_version is not None:
            source_version = _require_exact_text(
                "change.source_version",
                source_version,
                max_length=512,
            )
        item = change.item
        if item is not None:
            item = _materialize_frozen_json(item, active_ids=set())
            if type(item) is not dict:
                raise ValueError("change.item must materialize to an exact object")
        changes.append(
            {
                "kind": change.kind.value,
                "external_email_id": external_email_id,
                "source_version": source_version,
                "item": item,
            }
        )
    return _canonical_digest(
        "cold-start.batch.v1",
        {
            "v": 1,
            "contract_version": batch.contract_version,
            "cursor": batch.cursor,
            "includes_last": batch.includes_last,
            "changes": changes,
        },
    )


def _preview_payload_digest(
    *,
    account_id: int,
    canonical_folder: str,
    actor: str,
    reason: str,
) -> str:
    account, folder = _require_digest_identity(account_id, canonical_folder)
    exact_actor = _require_exact_text("actor", actor, max_length=128)
    exact_reason = _require_exact_text("reason", reason, max_length=512)
    return _canonical_digest(
        "cold-start.preview-payload.v1",
        {
            "v": 1,
            "account_id": account,
            "canonical_folder": folder,
            "actor": exact_actor,
            "reason": exact_reason,
        },
    )


def _approve_payload_digest(
    *,
    plan_id: UUID,
    actor: str,
    reason: str,
) -> str:
    plan = _require_uuid("plan_id", plan_id)
    exact_actor = _require_exact_text("actor", actor, max_length=128)
    exact_reason = _require_exact_text("reason", reason, max_length=512)
    return _canonical_digest(
        "cold-start.approve-payload.v1",
        {
            "v": 1,
            "plan_id": str(plan),
            "actor": exact_actor,
            "reason": exact_reason,
        },
    )


def _preview_result_digest(
    *,
    plan_id: UUID,
    account_id: int,
    canonical_folder: str,
    expected_cursor_status: SyncCursorStatus,
    expected_cursor_version: int,
    expected_cursor_hash: str | None,
    pipeline_name: str,
    generation: int,
    fencing_token: int,
    contract_fingerprint: str,
    folder_scope_config_hash: str,
    created_at: datetime,
    expires_at: datetime,
) -> str:
    plan = _require_uuid("plan_id", plan_id)
    account, folder = _require_digest_identity(account_id, canonical_folder)
    status, cursor_hash = _require_expected_cursor_binding(
        expected_cursor_status,
        expected_cursor_hash,
    )
    cursor_version = _require_exact_int(
        "expected_cursor_version",
        expected_cursor_version,
        minimum=0,
    )
    pipeline = _require_exact_text("pipeline_name", pipeline_name, max_length=64)
    exact_generation = _require_exact_int("generation", generation, minimum=1)
    exact_fence = _require_exact_int("fencing_token", fencing_token, minimum=1)
    contract_hash = _require_sha256(
        "contract_fingerprint",
        contract_fingerprint,
    )
    config_hash = _require_sha256(
        "folder_scope_config_hash",
        folder_scope_config_hash,
    )
    created = _format_digest_timestamp("created_at", created_at)
    expires = _format_digest_timestamp("expires_at", expires_at)
    if expires_at <= created_at:
        raise ValueError("expires_at must follow created_at")
    return _canonical_digest(
        "cold-start.preview-result.v1",
        {
            "v": 1,
            "plan_id": str(plan),
            "account_id": account,
            "canonical_folder": folder,
            "expected_cursor_status": status.value,
            "expected_cursor_version": cursor_version,
            "expected_cursor_hash": cursor_hash,
            "pipeline_name": pipeline,
            "generation": exact_generation,
            "fencing_token": exact_fence,
            "contract_fingerprint": contract_hash,
            "folder_scope_config_hash": config_hash,
            "created_at": created,
            "expires_at": expires,
        },
    )


def _approve_result_digest(
    *,
    plan_id: UUID,
    plan_hash: str,
    pipeline_name: str,
    generation: int,
    fencing_token: int,
    folder_scope_config_hash: str,
    approved_at: datetime,
) -> str:
    return _canonical_digest(
        "cold-start.approve-result.v1",
        {
            "v": 1,
            "plan_id": str(_require_uuid("plan_id", plan_id)),
            "plan_hash": _require_sha256("plan_hash", plan_hash),
            "pipeline_name": _require_exact_text(
                "pipeline_name",
                pipeline_name,
                max_length=64,
            ),
            "generation": _require_exact_int(
                "generation",
                generation,
                minimum=1,
            ),
            "fencing_token": _require_exact_int(
                "fencing_token",
                fencing_token,
                minimum=1,
            ),
            "folder_scope_config_hash": _require_sha256(
                "folder_scope_config_hash",
                folder_scope_config_hash,
            ),
            "approved_at": _format_digest_timestamp("approved_at", approved_at),
        },
    )


def _apply_page_payload_digest(
    *,
    account_id: int,
    canonical_folder: str,
    plan_id: UUID,
    plan_version: int,
    cursor_status: SyncCursorStatus,
    cursor_version: int,
    request_cursor_hash: str,
) -> str:
    account, folder = _require_digest_identity(account_id, canonical_folder)
    if type(cursor_status) is not SyncCursorStatus or cursor_status not in {
        SyncCursorStatus.COLD_START_PENDING,
        SyncCursorStatus.RESET_REQUIRED,
        SyncCursorStatus.COLD_START_APPLYING,
    }:
        raise ValueError("cursor_status is not cold-start eligible")
    return _canonical_digest(
        "cold-start.apply-page-payload.v1",
        {
            "v": 1,
            "command_name": "cold_start.apply_page",
            "account_id": account,
            "canonical_folder": folder,
            "plan_id": str(_require_uuid("plan_id", plan_id)),
            "plan_version": _require_exact_int(
                "plan_version",
                plan_version,
                minimum=0,
            ),
            "cursor_status": cursor_status.value,
            "cursor_version": _require_exact_int(
                "cursor_version",
                cursor_version,
                minimum=0,
            ),
            "request_cursor_hash": _require_sha256(
                "request_cursor_hash",
                request_cursor_hash,
            ),
        },
    )


def _apply_page_result_digest(batch_hash: str) -> str:
    return _canonical_digest(
        "cold-start.apply-page-result.v1",
        {"v": 1, "batch_hash": _require_sha256("batch_hash", batch_hash)},
    )


def _preview_rolling_digest(
    previous_hash: str | None,
    batch_hash: str,
) -> str:
    if previous_hash is None:
        previous = "0" * 64
    else:
        previous = _require_sha256("previous_hash", previous_hash)
    batch = _require_sha256("batch_hash", batch_hash)
    payload = (
        b"cold-start.preview-rolling.v1\x00"
        + previous.encode("ascii")
        + b"\x00"
        + batch.encode("ascii")
    )
    return hashlib.sha256(payload).hexdigest()


def _plan_digest(
    *,
    plan_id: UUID,
    account_id: int,
    canonical_folder: str,
    expected_cursor_status: SyncCursorStatus,
    expected_cursor_version: int,
    expected_cursor_hash: str | None,
    pipeline_name: str,
    generation: int,
    fencing_token: int,
    boundary_cursor_hash: str,
    boundary_cursor_version: int,
    rolling_hash: str,
    page_count: int,
    item_count: int,
    redacted_samples: tuple[ColdStartSample, ...],
    contract_fingerprint: str,
    folder_scope_config_hash: str,
    actor: str,
    reason: str,
    created_at: datetime,
    expires_at: datetime,
) -> str:
    account, folder = _require_digest_identity(account_id, canonical_folder)
    status, cursor_hash = _require_expected_cursor_binding(
        expected_cursor_status,
        expected_cursor_hash,
    )
    exact_page_count = _require_exact_int("page_count", page_count, minimum=1)
    exact_item_count = _require_exact_int("item_count", item_count, minimum=0)
    exact_boundary_version = _require_exact_int(
        "boundary_cursor_version",
        boundary_cursor_version,
        minimum=1,
    )
    if exact_boundary_version != exact_page_count:
        raise ValueError("boundary_cursor_version must equal page_count")
    if type(redacted_samples) is not tuple or len(redacted_samples) != min(
        exact_item_count, _MAX_REDACTED_SAMPLES
    ):
        raise ValueError("redacted_samples must be the complete bounded prefix")
    validated_samples: list[ColdStartSample] = []
    for sample in redacted_samples:
        if type(sample) is not ColdStartSample:
            raise ValueError("redacted_samples must contain exact samples")
        validated_samples.append(
            ColdStartSample(sample.kind, sample.external_email_id_hash)
        )
    samples = [
        {
            "kind": sample.kind.value,
            "external_email_id_hash": sample.external_email_id_hash,
        }
        for sample in validated_samples
    ]
    created = _format_digest_timestamp("created_at", created_at)
    expires = _format_digest_timestamp("expires_at", expires_at)
    if expires_at <= created_at:
        raise ValueError("expires_at must follow created_at")
    return _canonical_digest(
        "cold-start.plan.v1",
        {
            "v": 1,
            "plan_id": str(_require_uuid("plan_id", plan_id)),
            "account_id": account,
            "canonical_folder": folder,
            "expected_cursor_status": status.value,
            "expected_cursor_version": _require_exact_int(
                "expected_cursor_version",
                expected_cursor_version,
                minimum=0,
            ),
            "expected_cursor_hash": cursor_hash,
            "pipeline_name": _require_exact_text(
                "pipeline_name",
                pipeline_name,
                max_length=64,
            ),
            "generation": _require_exact_int(
                "generation",
                generation,
                minimum=1,
            ),
            "fencing_token": _require_exact_int(
                "fencing_token",
                fencing_token,
                minimum=1,
            ),
            "boundary_cursor_hash": _require_sha256(
                "boundary_cursor_hash",
                boundary_cursor_hash,
            ),
            "boundary_cursor_version": exact_boundary_version,
            "rolling_hash": _require_sha256("rolling_hash", rolling_hash),
            "page_count": exact_page_count,
            "item_count": exact_item_count,
            "redacted_samples": samples,
            "contract_fingerprint": _require_sha256(
                "contract_fingerprint",
                contract_fingerprint,
            ),
            "folder_scope_config_hash": _require_sha256(
                "folder_scope_config_hash",
                folder_scope_config_hash,
            ),
            "actor": _require_exact_text("actor", actor, max_length=128),
            "reason": _require_exact_text("reason", reason, max_length=512),
            "created_at": created,
            "expires_at": expires,
        },
    )


def _blocked_digest(
    *,
    account_id: int,
    canonical_folder: str,
    plan_id: UUID,
    safe_code: str,
) -> str:
    account, folder = _require_digest_identity(account_id, canonical_folder)
    exact_code = _require_safe_code("safe_code", safe_code)
    if exact_code not in _BLOCKED_REASON_CODES:
        raise ValueError("safe_code is not a frozen blocked reason")
    return _canonical_digest(
        "cold-start.blocked.v1",
        {
            "v": 1,
            "account_id": account,
            "canonical_folder": folder,
            "plan_id": str(_require_uuid("plan_id", plan_id)),
            "safe_code": exact_code,
        },
    )


def _audit_object_digest(plan_id: UUID) -> str:
    return _canonical_digest(
        "cold-start.audit-object.v1",
        {"v": 1, "plan_id": str(_require_uuid("plan_id", plan_id))},
    )


def _audit_event_digest(
    *,
    action: str,
    plan_id: UUID,
    plan_version: int,
) -> str:
    return _canonical_digest(
        "cold-start.audit-event.v1",
        {
            "v": 1,
            "action": _require_exact_text("action", action, max_length=128),
            "plan_id": str(_require_uuid("plan_id", plan_id)),
            "plan_version": _require_exact_int(
                "plan_version",
                plan_version,
                minimum=0,
            ),
        },
    )


_PLAN_VIEW_ROW_KEYS: Final = frozenset(
    {
        "plan_id",
        "account_id",
        "folder_key",
        "state",
        "boundary_cursor",
        "page_count",
        "item_count",
        "redacted_samples",
        "contract_fingerprint",
        "folder_scope_config_hash",
        "plan_hash",
        "blocked_reason_code",
        "blocked_fingerprint",
        "expires_at",
        "ready_at",
        "approved_at",
        "completed_at",
        "blocked_at",
        "created_at",
        "updated_at",
    }
)


def _plan_view_from_row(row: object) -> ColdStartPlanView:
    if (
        type(row) is not dict
        or any(type(key) is not str for key in row)
        or set(row) != _PLAN_VIEW_ROW_KEYS
    ):
        raise ValueError("cold-start plan row has an unexpected shape")

    raw_state = row["state"]
    if type(raw_state) is not str:
        raise ValueError("persisted cold-start state must be an exact string")
    try:
        state = ColdStartPlanState(raw_state)
    except ValueError:
        raise ValueError("persisted cold-start state is unknown") from None

    raw_samples = row["redacted_samples"]
    if type(raw_samples) is not list:
        raise ValueError("persisted redacted_samples must be an exact array")
    samples: list[ColdStartSample] = []
    for raw_sample in raw_samples:
        if (
            type(raw_sample) is not dict
            or any(type(key) is not str for key in raw_sample)
            or set(raw_sample) != {"kind", "external_email_id_hash"}
        ):
            raise ValueError("persisted cold-start sample has an unexpected shape")
        raw_kind = raw_sample["kind"]
        raw_hash = raw_sample["external_email_id_hash"]
        if type(raw_kind) is not str or type(raw_hash) is not str:
            raise ValueError(
                "persisted cold-start sample scalars must be exact strings"
            )
        try:
            kind = ChangeKind(raw_kind)
        except ValueError:
            raise ValueError("persisted cold-start sample kind is unknown") from None
        samples.append(ColdStartSample(kind, raw_hash))

    return ColdStartPlanView(
        plan_id=row["plan_id"],
        account_id=row["account_id"],
        canonical_folder=row["folder_key"],
        state=state,
        boundary_cursor=row["boundary_cursor"],
        page_count=row["page_count"],
        item_count=row["item_count"],
        redacted_samples=tuple(samples),
        contract_fingerprint=row["contract_fingerprint"],
        folder_scope_config_hash=row["folder_scope_config_hash"],
        plan_hash=row["plan_hash"],
        blocked_reason_code=row["blocked_reason_code"],
        blocked_fingerprint=row["blocked_fingerprint"],
        expires_at=_normalize_database_datetime("expires_at", row["expires_at"]),
        ready_at=_normalize_optional_database_datetime("ready_at", row["ready_at"]),
        approved_at=_normalize_optional_database_datetime(
            "approved_at",
            row["approved_at"],
        ),
        completed_at=_normalize_optional_database_datetime(
            "completed_at",
            row["completed_at"],
        ),
        blocked_at=_normalize_optional_database_datetime(
            "blocked_at",
            row["blocked_at"],
        ),
        created_at=_normalize_database_datetime("created_at", row["created_at"]),
        updated_at=_normalize_database_datetime("updated_at", row["updated_at"]),
    )


async def _fetch_origin_page(
    origin: ColdStartOriginPort,
    account_id: int,
    sync_folder: str,
    cursor: str | None,
    limit: int,
) -> SyncBatch:
    _validate_page_request(account_id, sync_folder, limit)
    if cursor is not None:
        _require_cursor("cold-start cursor", cursor)
    raw_batch = await origin.fetch_cold_start_page(
        account_id,
        sync_folder,
        cursor,
        limit,
    )
    return _rebuild_sync_batch(raw_batch, limit)


async def _fetch_ordinary_page(
    client: Any,
    account_id: int,
    sync_folder: str,
    cursor: str,
    limit: int,
) -> SyncBatch:
    _validate_page_request(account_id, sync_folder, limit)
    _require_cursor("ordinary sync cursor", cursor)
    raw_batch = await client.sync_emails(account_id, sync_folder, cursor, limit)
    return _rebuild_sync_batch(raw_batch, limit)


def _validate_page_request(
    account_id: object,
    sync_folder: object,
    limit: object,
) -> None:
    _require_exact_int("account_id", account_id, minimum=1)
    _require_exact_text("sync_folder", sync_folder, max_length=512)
    _require_exact_int(
        "limit",
        limit,
        minimum=1,
        maximum=MAX_SYNC_CHANGES_PER_BATCH,
    )


def _rebuild_sync_batch(value: object, limit: int) -> SyncBatch:
    try:
        return _strict_rebuild_sync_batch(value, limit)
    except ValueError:
        raise
    except Exception:
        raise ValueError("invalid SyncBatch response") from None


def _strict_rebuild_sync_batch(value: object, limit: object) -> SyncBatch:
    exact_limit = _require_exact_int(
        "limit",
        limit,
        minimum=1,
        maximum=MAX_SYNC_CHANGES_PER_BATCH,
    )
    if type(value) is not SyncBatch:
        raise ValueError("page client must return an exact SyncBatch")
    if (
        type(value.contract_version) is not str
        or value.contract_version != "exchange_sync_contract_v2"
        or type(value.includes_last) is not bool
        or type(value.changes) is not tuple
    ):
        raise ValueError("invalid SyncBatch outer shape")
    cursor = _require_cursor("batch.cursor", value.cursor)
    if len(value.changes) > exact_limit:
        raise ValueError("invalid SyncBatch: response exceeds requested limit")

    rebuilt_changes: list[SyncChange] = []
    identities: set[tuple[ChangeKind, str]] = set()
    for change in value.changes:
        if type(change) is not SyncChange:
            raise ValueError("invalid SyncBatch change shape")
        if type(change.kind) is not ChangeKind or change.kind not in {
            ChangeKind.CREATE,
            ChangeKind.UPDATE,
            ChangeKind.DELETE,
        }:
            raise ValueError("invalid SyncBatch change kind")
        external_email_id = _require_exact_text(
            "change.external_email_id",
            change.external_email_id,
            max_length=1024,
        )
        source_version = change.source_version
        if source_version is not None:
            source_version = _require_exact_text(
                "change.source_version",
                source_version,
                max_length=512,
            )

        if change.kind in {ChangeKind.CREATE, ChangeKind.UPDATE}:
            if type(change.item) is not MappingProxyType:
                raise ValueError("create/update change requires an immutable item")
            item = _materialize_frozen_json(change.item, active_ids=set())
            if type(item) is not dict:
                raise ValueError("change item must materialize to an exact object")
        else:
            if change.item is not None:
                raise ValueError("delete change must not contain an item")
            item = None

        identity = (change.kind, external_email_id)
        if identity in identities:
            raise ValueError("invalid SyncBatch duplicate change identity")
        identities.add(identity)
        rebuilt_changes.append(
            SyncChange(
                kind=change.kind,
                external_email_id=external_email_id,
                item=item,
                source_version=source_version,
            )
        )
    return SyncBatch(
        contract_version=value.contract_version,
        cursor=cursor,
        changes=tuple(rebuilt_changes),
        includes_last=value.includes_last,
    )


__all__ = [
    "ColdStartOriginPort",
    "ColdStartPlanNotFoundError",
    "ColdStartPlanState",
    "ColdStartPlanView",
    "ColdStartRunResult",
    "ColdStartRunStatus",
    "ColdStartSample",
    "ColdStartService",
    "ColdStartStateConflictError",
]
