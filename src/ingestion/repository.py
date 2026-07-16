"""Durable Inbox repository with fenced leases and bounded recovery."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from typing import Any, Final
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

from src.domain.email_state import PipelineGenerationState
from src.domain.errors import (
    DatabaseOperationError,
    ErrorKind,
    ManualReviewRequired,
    StaleFence,
)
from src.ingestion.email_events import (
    EmailEventApplication,
    EmailEventDecision,
    EmailEventDisposition,
    EmailEventReason,
    EmailStatus,
    decide_email_event,
)
from src.ingestion.models import (
    ChangeKind,
    InboxDisposition,
    InboxDispositionStatus,
    InboxLease,
    InboxStatus,
    InboxStats,
    IngressReceipt,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.ownership import ownership_advisory_lock_key
from src.ingestion.processing import (
    ProcessingCompletion,
    ProcessingCompletionRejected,
    ProcessingFinishResult,
    ProcessingReceiptConflict,
)


_DATABASE_EXCEPTIONS = (psycopg.Error, PoolTimeout)
_LOCK_TIMEOUT: Final = "5000ms"
_STATEMENT_TIMEOUT: Final = "15000ms"
_IDLE_TRANSACTION_TIMEOUT: Final = "15000ms"
_MAX_BATCH: Final = 500
_MAX_PIPELINES: Final = 64
_MAX_LEASE_SECONDS: Final = 3600
_MAX_RETRIES: Final = 5
_MAX_BACKOFF_SECONDS: Final = 900
_AUDIT_ACTOR: Final = "inbox_repository"
_PROCESSING_EXECUTION_EPOCH: Final = 0
_PROCESSING_ATTEMPT_ACTION: Final = "email.processing_attempt"
_PROCESSING_ATTEMPT_REASON: Final = "email.processing_attempt_authorized"
_PROCESSING_ATTEMPT_OBJECT_TYPE: Final = "email_processing_attempt"
_PROCESSING_ATTEMPT_RESULT: Final = "authorized"
_PROCESSING_RESULT_OBJECT_TYPE: Final = "email_processing_result"
_PROCESSING_COMPLETED_ACTION: Final = "email.processing_completed"
_PROCESSING_FAILED_ACTION: Final = "email.processing_failed"
_PROCESSING_COMPLETED_REASON: Final = "email.processing_completed"
_PROCESSING_VERSION_EXHAUSTED_CODE: Final = (
    "email.version_exhausted_before_nonterminal_completion"
)
_PROCESSING_VERSION_EXHAUSTED_SUMMARY: Final = "Email processing version is exhausted"

_CLAIMABLE_POLICIES = (
    ProcessingPolicy.FULL.value,
    ProcessingPolicy.ARCHIVE.value,
    ProcessingPolicy.METADATA_ONLY.value,
)
_SUPPRESSED_POLICIES = frozenset(
    {
        ProcessingPolicy.IGNORED,
        ProcessingPolicy.HISTORICAL_SUPPRESSED,
    }
)
_LEASE_OWNERSHIP_STATES = (
    PipelineGenerationState.CURRENT_INGRESS.value,
    PipelineGenerationState.QUIESCING.value,
    PipelineGenerationState.DRAINING.value,
)
_CLAIM_OWNERSHIP_STATES = (
    PipelineGenerationState.CURRENT_INGRESS.value,
    PipelineGenerationState.DRAINING.value,
)
_RETIRED_BLOCKING_EMAIL_STATUSES = frozenset(
    {
        EmailStatus.INGESTED,
        EmailStatus.PROCESSING,
        EmailStatus.RETRY_WAIT,
        EmailStatus.MANUAL_REVIEW,
        EmailStatus.WAITING_APPROVAL,
        EmailStatus.SEND_QUEUED,
        EmailStatus.SENDING,
        EmailStatus.ACCEPTED,
        EmailStatus.SEND_UNKNOWN,
        EmailStatus.DEAD_LETTER,
    }
)

_LEASE_COLUMNS = (
    "id",
    "account_id",
    "external_email_id",
    "folder_key",
    "source",
    "raw_event_type",
    "change_kind",
    "dedupe_key",
    "source_version",
    "source_event_at",
    "payload",
    "processing_policy",
    "pipeline_name",
    "generation",
    "fencing_token",
    "lease_owner",
    "lease_until",
    "attempts",
    "received_at",
)
_LEASE_RETURNING = sql.SQL(", ").join(
    sql.SQL("e.{} ").format(sql.Identifier(column)) for column in _LEASE_COLUMNS
)

_EMAIL_COLUMNS = (
    "id",
    "account_id",
    "external_email_id",
    "source_folder_key",
    "status",
    "version",
    "owner_generation",
    "owner_fencing_token",
    "processing_inbox_id",
    "create_seen_at",
    "processing_started_at",
    "source_deleted_at",
    "external_effects_started_at",
    "safe_error_code",
    "safe_error_summary",
    "is_read",
    "is_read_refresh_required",
    "updated_at",
)
_EMAIL_RETURNING = sql.SQL(", ").join(
    sql.SQL("e.{}").format(sql.Identifier(column)) for column in _EMAIL_COLUMNS
)


@dataclass(frozen=True, slots=True)
class _FailureDecision:
    status: InboxDispositionStatus
    safe_code: str
    safe_summary: str


@dataclass(frozen=True, slots=True)
class _EmailRow:
    id: str
    account_id: int
    external_email_id: str
    source_folder_key: str
    status: EmailStatus
    version: int
    owner_generation: int
    owner_fencing_token: int
    processing_inbox_id: str | None
    create_seen_at: object | None
    processing_started_at: object | None
    source_deleted_at: object | None
    external_effects_started_at: object | None
    safe_error_code: str | None
    safe_error_summary: str | None
    is_read: bool | None
    is_read_refresh_required: bool
    updated_at: object


@dataclass(frozen=True, slots=True)
class _ProcessingInboxRow:
    status: InboxStatus
    lease: InboxLease | _ExpiredLeaseCandidate | None
    effect_started_at: object | None
    attempts: int
    account_id: int
    external_email_id: str
    generation: int
    fencing_token: int
    lease_active: bool
    safe_error_code: str | None
    safe_error_summary: str | None


@dataclass(frozen=True, slots=True)
class _ExpiredLeaseCandidate:
    id: str
    account_id: int
    pipeline_name: str
    generation: int
    fencing_token: int
    lease_owner: str
    attempts: int
    event: NormalizedIngressEvent
    received_at: datetime
    lease_until: datetime


_LeaseToken = InboxLease | _ExpiredLeaseCandidate


@dataclass(frozen=True, slots=True)
class _ProcessingResolution:
    email_status: EmailStatus
    inbox_status: InboxStatus
    safe_code: str | None
    safe_summary: str | None
    attempts: int
    available_in_seconds: int


class _AuditInvariantError(DatabaseOperationError):
    """Private marker for an append-and-compare audit collision."""

    def __init__(self) -> None:
        super().__init__(
            operation="event_inbox_invariant",
            retryable=False,
            message="event inbox audit invariant failed",
        )


_FAILURE_DECISIONS: Final[dict[ErrorKind, _FailureDecision]] = {
    ErrorKind.TRANSIENT_DEPENDENCY: _FailureDecision(
        InboxDispositionStatus.RETRY_WAIT,
        "inbox.transient_dependency",
        "Transient dependency failure",
    ),
    ErrorKind.RATE_LIMITED: _FailureDecision(
        InboxDispositionStatus.RETRY_WAIT,
        "inbox.rate_limited",
        "Dependency rate limit reached",
    ),
    ErrorKind.AUTHENTICATION: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.authentication_failure",
        "Dependency authentication failed",
    ),
    ErrorKind.VALIDATION: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.validation_failure",
        "Inbox event validation failed",
    ),
    ErrorKind.PERMANENT_DEPENDENCY: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.permanent_dependency_failure",
        "Permanent dependency failure",
    ),
    ErrorKind.POLICY_REJECTED: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.policy_rejected",
        "Inbox processing policy rejected",
    ),
    ErrorKind.INTERNAL_INVARIANT: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.internal_invariant",
        "Inbox processing invariant failed",
    ),
    ErrorKind.SEND_UNKNOWN: _FailureDecision(
        InboxDispositionStatus.MANUAL_REVIEW,
        "inbox.send_unknown",
        "Remote send outcome requires review",
    ),
}
_EFFECT_UNKNOWN = _FailureDecision(
    InboxDispositionStatus.MANUAL_REVIEW,
    "inbox.effect_outcome_unknown",
    "Remote effect outcome requires review",
)
_MANUAL_REVIEW = _FailureDecision(
    InboxDispositionStatus.MANUAL_REVIEW,
    "inbox.manual_review_required",
    "Inbox event requires manual review",
)
_DATABASE_TRANSIENT = _FailureDecision(
    InboxDispositionStatus.RETRY_WAIT,
    "inbox.database_transient",
    "Transient database operation failure",
)
_DATABASE_FAILURE = _FailureDecision(
    InboxDispositionStatus.DEAD_LETTER,
    "inbox.database_failure",
    "Database operation failed",
)
_UNKNOWN_FAILURE = _FAILURE_DECISIONS[ErrorKind.INTERNAL_INVARIANT]
_LEASE_EXPIRED = _FailureDecision(
    InboxDispositionStatus.RETRY_WAIT,
    "inbox.lease_expired",
    "Inbox worker lease expired",
)
_STALE_OWNERSHIP = _FailureDecision(
    InboxDispositionStatus.MANUAL_REVIEW,
    "inbox.stale_ownership",
    "Inbox ownership requires review",
)


def _require_bigint(name: str, value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > POSTGRES_BIGINT_MAX
    ):
        raise ValueError(f"{name} must be a positive PostgreSQL BIGINT")
    return value


def _require_bounded_int(name: str, value: object, *, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > maximum
    ):
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def _require_exact_text(name: str, value: object, *, max_length: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > max_length
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise ValueError(f"{name} must be an exact bounded string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain valid UTF-8 text") from None
    return value


def _require_pipeline_names(values: object) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise ValueError("pipeline_names must be a bounded collection")
    try:
        candidates = tuple(islice(iter(values), _MAX_PIPELINES + 1))  # type: ignore[arg-type]
    except TypeError:
        raise ValueError("pipeline_names must be a bounded collection") from None
    if not candidates or len(candidates) > _MAX_PIPELINES:
        raise ValueError("pipeline_names must contain between 1 and 64 names")
    normalized = tuple(
        _require_exact_text("pipeline_name", value, max_length=64)
        for value in candidates
    )
    if len(set(normalized)) != len(normalized):
        normalized = tuple(dict.fromkeys(normalized))
    return tuple(sorted(normalized))


def _database_error(operation: str, error: Exception) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation=operation,
        retryable=isinstance(error, (psycopg.OperationalError, PoolTimeout)),
        message="event inbox database operation failed",
    )


def _invariant_error(message: str) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation="event_inbox_invariant",
        retryable=False,
        message=message,
    )


def _row_values(row: object, columns: tuple[str, ...]) -> tuple[object, ...]:
    try:
        if isinstance(row, Mapping):
            return tuple(row[column] for column in columns)
        if isinstance(row, (tuple, list)) and len(row) >= len(columns):
            return tuple(row[index] for index in range(len(columns)))
    except (KeyError, IndexError, TypeError):
        pass
    raise _invariant_error("event inbox database row is invalid")


def _lease_from_row(row: object) -> InboxLease:
    try:
        values = dict(
            zip(_LEASE_COLUMNS, _row_values(row, _LEASE_COLUMNS), strict=True)
        )
        payload = values["payload"]
        if not isinstance(payload, Mapping):
            raise ValueError
        event = NormalizedIngressEvent(
            account_id=values["account_id"],
            source=values["source"],
            raw_event_type=values["raw_event_type"],
            kind=values["change_kind"],
            external_email_id=values["external_email_id"],
            folder=values["folder_key"],
            source_version=values["source_version"],
            dedupe_key=str(values["dedupe_key"]),
            payload=payload,
            processing_policy=values["processing_policy"],
            source_event_at=values["source_event_at"],
        )
        return InboxLease(
            id=str(values["id"]),
            account_id=values["account_id"],
            pipeline_name=values["pipeline_name"],
            generation=values["generation"],
            fencing_token=values["fencing_token"],
            lease_owner=values["lease_owner"],
            attempts=values["attempts"],
            event=event,
            received_at=values["received_at"],
            lease_until=values["lease_until"],
        )
    except DatabaseOperationError:
        raise
    except (KeyError, TypeError, ValueError):
        raise _invariant_error("event inbox database row is invalid") from None


def _expired_candidate_from_row(row: object) -> _ExpiredLeaseCandidate:
    try:
        values = dict(
            zip(_LEASE_COLUMNS, _row_values(row, _LEASE_COLUMNS), strict=True)
        )
        payload = values["payload"]
        if not isinstance(payload, Mapping):
            raise ValueError
        event = NormalizedIngressEvent(
            account_id=values["account_id"],
            source=values["source"],
            raw_event_type=values["raw_event_type"],
            kind=values["change_kind"],
            external_email_id=values["external_email_id"],
            folder=values["folder_key"],
            source_version=values["source_version"],
            dedupe_key=str(values["dedupe_key"]),
            payload=payload,
            processing_policy=values["processing_policy"],
            source_event_at=values["source_event_at"],
        )
        candidate_id = str(UUID(str(values["id"])))
        account_id = _require_bigint("account_id", values["account_id"])
        pipeline_name = _require_exact_text(
            "pipeline_name",
            values["pipeline_name"],
            max_length=64,
        )
        generation = _require_bigint("generation", values["generation"])
        fencing_token = _require_bigint("fencing_token", values["fencing_token"])
        lease_owner = _require_exact_text(
            "lease_owner",
            values["lease_owner"],
            max_length=128,
        )
        attempts = values["attempts"]
        received_at = values["received_at"]
        lease_until = values["lease_until"]
        if (
            isinstance(attempts, bool)
            or not isinstance(attempts, int)
            or attempts < 0
            or attempts > POSTGRES_BIGINT_MAX
            or not isinstance(received_at, datetime)
            or received_at.tzinfo is None
            or not isinstance(lease_until, datetime)
            or lease_until.tzinfo is None
        ):
            raise ValueError
        return _ExpiredLeaseCandidate(
            id=candidate_id,
            account_id=account_id,
            pipeline_name=pipeline_name,
            generation=generation,
            fencing_token=fencing_token,
            lease_owner=lease_owner,
            attempts=attempts,
            event=event,
            received_at=received_at,
            lease_until=lease_until,
        )
    except DatabaseOperationError:
        raise
    except (KeyError, TypeError, ValueError):
        raise _invariant_error("expired inbox candidate row is invalid") from None


def _email_from_row(row: object) -> _EmailRow:
    try:
        values = dict(
            zip(_EMAIL_COLUMNS, _row_values(row, _EMAIL_COLUMNS), strict=True)
        )
        status = EmailStatus(values["status"])
        version = values["version"]
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version < 0
            or version > POSTGRES_BIGINT_MAX
        ):
            raise ValueError
        account_id = _require_bigint("account_id", values["account_id"])
        owner_generation = _require_bigint(
            "owner_generation",
            values["owner_generation"],
        )
        owner_fencing_token = _require_bigint(
            "owner_fencing_token",
            values["owner_fencing_token"],
        )
        is_read = values["is_read"]
        if is_read is not None and not isinstance(is_read, bool):
            raise ValueError
        refresh_required = values["is_read_refresh_required"]
        if not isinstance(refresh_required, bool):
            raise ValueError
        processing_inbox_id = values["processing_inbox_id"]
        return _EmailRow(
            id=str(values["id"]),
            account_id=account_id,
            external_email_id=str(values["external_email_id"]),
            source_folder_key=str(values["source_folder_key"]),
            status=status,
            version=version,
            owner_generation=owner_generation,
            owner_fencing_token=owner_fencing_token,
            processing_inbox_id=(
                str(processing_inbox_id) if processing_inbox_id is not None else None
            ),
            create_seen_at=values["create_seen_at"],
            processing_started_at=values["processing_started_at"],
            source_deleted_at=values["source_deleted_at"],
            external_effects_started_at=values["external_effects_started_at"],
            safe_error_code=(
                str(values["safe_error_code"])
                if values["safe_error_code"] is not None
                else None
            ),
            safe_error_summary=(
                str(values["safe_error_summary"])
                if values["safe_error_summary"] is not None
                else None
            ),
            is_read=is_read,
            is_read_refresh_required=refresh_required,
            updated_at=values["updated_at"],
        )
    except DatabaseOperationError:
        raise
    except (KeyError, TypeError, ValueError):
        raise _invariant_error("email aggregate database row is invalid") from None


def _source_is_read(event: NormalizedIngressEvent) -> bool | None:
    if event.kind is ChangeKind.READ:
        return True
    if event.source is IngressSource.SYNC:
        item = event.payload.get("item")
        if isinstance(item, Mapping):
            value = item.get("is_read")
            return value if isinstance(value, bool) else None
        return None
    if event.source is IngressSource.WEBHOOK:
        value = event.payload.get("is_read")
        return value if isinstance(value, bool) else None
    return None


def _json_values_equal(left: object, right: object) -> bool:
    """Compare values by JSON type, keeping booleans distinct from numbers."""

    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        return (
            isinstance(left, (int, float))
            and not isinstance(left, bool)
            and isinstance(right, (int, float))
            and not isinstance(right, bool)
            and left == right
        )
    if isinstance(left, str) or isinstance(right, str):
        return isinstance(left, str) and isinstance(right, str) and left == right
    if isinstance(left, Mapping) or isinstance(right, Mapping):
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            return False
        if left.keys() != right.keys():
            return False
        return all(_json_values_equal(left[key], right[key]) for key in left)
    if isinstance(left, (list, tuple)) or isinstance(right, (list, tuple)):
        if not isinstance(left, (list, tuple)) or not isinstance(right, (list, tuple)):
            return False
        return len(left) == len(right) and all(
            _json_values_equal(left_item, right_item)
            for left_item, right_item in zip(left, right, strict=True)
        )
    return False


def _leases_equal(persisted: _LeaseToken, supplied: _LeaseToken) -> bool:
    persisted_event = persisted.event
    supplied_event = supplied.event
    return (
        (
            persisted.id,
            persisted.account_id,
            persisted.pipeline_name,
            persisted.generation,
            persisted.fencing_token,
            persisted.lease_owner,
            persisted.attempts,
            persisted.received_at,
            persisted.lease_until,
        )
        == (
            supplied.id,
            supplied.account_id,
            supplied.pipeline_name,
            supplied.generation,
            supplied.fencing_token,
            supplied.lease_owner,
            supplied.attempts,
            supplied.received_at,
            supplied.lease_until,
        )
        and (
            persisted_event.account_id,
            persisted_event.source,
            persisted_event.raw_event_type,
            persisted_event.kind,
            persisted_event.external_email_id,
            persisted_event.folder,
            persisted_event.source_version,
            persisted_event.dedupe_key,
            persisted_event.processing_policy,
            persisted_event.source_event_at,
        )
        == (
            supplied_event.account_id,
            supplied_event.source,
            supplied_event.raw_event_type,
            supplied_event.kind,
            supplied_event.external_email_id,
            supplied_event.folder,
            supplied_event.source_version,
            supplied_event.dedupe_key,
            supplied_event.processing_policy,
            supplied_event.source_event_at,
        )
        and _json_values_equal(persisted_event.payload, supplied_event.payload)
    )


def _processing_attempt_event_key(inbox_id: str, attempts: int) -> str:
    payload = (
        b"email-processing-attempt-v1\x00"
        + inbox_id.encode("ascii")
        + b"\x00"
        + str(_PROCESSING_EXECUTION_EPOCH).encode("ascii")
        + b"\x00"
        + str(attempts).encode("ascii")
    )
    return hashlib.sha256(payload).hexdigest()


def _processing_attempt_fingerprint(email_id: str, inbox_id: str) -> str:
    payload = (
        b"email-processing-attempt-object-v1\x00"
        + email_id.encode("ascii")
        + b"\x00"
        + inbox_id.encode("ascii")
    )
    return hashlib.sha256(payload).hexdigest()


def _processing_result_event_key(
    inbox_id: str,
    attempts: int,
    operation: str,
) -> str:
    payload = (
        b"email-processing-result-v1\x00"
        + inbox_id.encode("ascii")
        + b"\x00"
        + str(_PROCESSING_EXECUTION_EPOCH).encode("ascii")
        + b"\x00"
        + str(attempts).encode("ascii")
        + b"\x00"
        + operation.encode("ascii")
    )
    return hashlib.sha256(payload).hexdigest()


def _processing_result_fingerprint(email_id: str, inbox_id: str) -> str:
    payload = (
        b"email-processing-result-object-v1\x00"
        + email_id.encode("ascii")
        + b"\x00"
        + inbox_id.encode("ascii")
    )
    return hashlib.sha256(payload).hexdigest()


def _lease_token_hash(lease: _LeaseToken) -> str:
    event = lease.event
    payload = {
        "id": lease.id,
        "account_id": lease.account_id,
        "pipeline_name": lease.pipeline_name,
        "generation": lease.generation,
        "fencing_token": lease.fencing_token,
        "lease_owner": lease.lease_owner,
        "attempts": lease.attempts,
        "received_at": lease.received_at.isoformat(),
        "lease_until": lease.lease_until.isoformat(),
        "event": {
            "account_id": event.account_id,
            "source": event.source.value,
            "raw_event_type": event.raw_event_type,
            "kind": event.kind.value,
            "external_email_id": event.external_email_id,
            "folder": event.folder,
            "source_version": event.source_version,
            "dedupe_key": event.dedupe_key,
            "payload": event.payload_for_storage(),
            "processing_policy": event.processing_policy.value,
            "source_event_at": (
                event.source_event_at.isoformat()
                if event.source_event_at is not None
                else None
            ),
        },
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _require_email_id(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("email_id must be a UUID string")
    try:
        normalized = str(UUID(value))
    except (AttributeError, ValueError):
        raise ValueError("email_id must be a UUID string") from None
    if normalized != value:
        raise ValueError("email_id must be a canonical UUID string")
    return normalized


def _require_email_version(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > POSTGRES_BIGINT_MAX
    ):
        raise ValueError("expected_email_version must be a PostgreSQL BIGINT")
    return value


def _audit_event_key(inbox_id: str, action: str, attempts: int) -> str:
    payload = f"event_inbox\x00{inbox_id}\x00{action}\x00{attempts}".encode()
    return hashlib.sha256(payload).hexdigest()


def _object_fingerprint(inbox_id: str) -> str:
    return hashlib.sha256(inbox_id.encode()).hexdigest()


def _next_attempts(attempts: int) -> int:
    return min(attempts + 1, POSTGRES_BIGINT_MAX)


def _failure_decision(error: BaseException) -> _FailureDecision:
    if isinstance(error, ManualReviewRequired):
        return _MANUAL_REVIEW
    if isinstance(error, DatabaseOperationError):
        return _DATABASE_TRANSIENT if error.retryable else _DATABASE_FAILURE
    try:
        kind = getattr(error, "kind", None)
    except Exception:
        return _UNKNOWN_FAILURE
    if isinstance(kind, ErrorKind):
        return _FAILURE_DECISIONS[kind]
    return _UNKNOWN_FAILURE


def _terminal_action(decision: _FailureDecision) -> str:
    if decision.safe_code == _EFFECT_UNKNOWN.safe_code:
        return "ingress.effect_unknown"
    if decision.status is InboxDispositionStatus.MANUAL_REVIEW:
        return "ingress.manual_review"
    return "ingress.dead_letter"


class InboxRepository:
    """The sole Phase-2 production mutation boundary for ``event_inbox``."""

    def __init__(self, pool: Any, *, target_schema: str = "public") -> None:
        self._pool = pool
        self._schema = _require_exact_text(
            "target_schema",
            target_schema,
            max_length=63,
        )

    def _table(self, name: str) -> sql.Identifier:
        return sql.Identifier(self._schema, name)

    async def _configure_transaction(
        self,
        connection: psycopg.AsyncConnection[Any],
    ) -> None:
        await connection.execute("SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
        await connection.execute(
            "SELECT "
            "pg_catalog.set_config('lock_timeout', %s, true), "
            "pg_catalog.set_config('statement_timeout', %s, true), "
            "pg_catalog.set_config("
            "'idle_in_transaction_session_timeout', %s, true)",
            (_LOCK_TIMEOUT, _STATEMENT_TIMEOUT, _IDLE_TRANSACTION_TIMEOUT),
        )

    async def _acquire_account_lock(
        self,
        connection: psycopg.AsyncConnection[Any],
        account_id: int,
    ) -> None:
        await connection.execute(
            "SELECT pg_catalog.pg_advisory_xact_lock_shared(%s)",
            (ownership_advisory_lock_key(account_id),),
        )

    def transaction(
        self,
        connection: psycopg.AsyncConnection[Any],
        *,
        for_key_share: bool = True,
    ) -> EmailEventTransaction:
        if type(for_key_share) is not bool:
            raise ValueError("for_key_share must be an exact boolean")
        return EmailEventTransaction(
            self,
            connection,
            for_key_share=for_key_share,
        )

    async def apply_email_event(
        self,
        lease: InboxLease,
    ) -> EmailEventApplication:
        lease = self._require_lease(lease)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    return await self.transaction(connection).apply_email_event(lease)
        except (
            StaleFence,
            ManualReviewRequired,
            DatabaseOperationError,
            ValueError,
            RuntimeError,
        ):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("apply_email_event", error) from None

    async def _append_audit(
        self,
        connection: psycopg.AsyncConnection[Any],
        *,
        inbox_id: str,
        account_id: int,
        action: str,
        result: str,
        reason: str,
        attempts: int,
        safe_metadata: Mapping[str, object],
    ) -> None:
        event_key = _audit_event_key(inbox_id, action, attempts)
        fingerprint = _object_fingerprint(inbox_id)
        metadata = dict(safe_metadata)
        insert = sql.SQL(
            "INSERT INTO {} ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata"
            ") VALUES (%s, %s, %s, NULL, 'event_inbox', %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (event_key) DO NOTHING"
        ).format(self._table("audit_events"))
        await connection.execute(
            insert,
            (
                str(uuid4()),
                event_key,
                account_id,
                fingerprint,
                action,
                result,
                _AUDIT_ACTOR,
                reason,
                Jsonb(metadata),
            ),
        )
        select = sql.SQL(
            "SELECT account_id, email_id, object_type, object_fingerprint, "
            "action, result, actor, reason, safe_metadata "
            "FROM {} WHERE event_key = %s"
        ).format(self._table("audit_events"))
        cursor = await connection.execute(select, (event_key,))
        row = await cursor.fetchone()
        expected = (
            account_id,
            None,
            "event_inbox",
            fingerprint,
            action,
            result,
            _AUDIT_ACTOR,
            reason,
            metadata,
        )
        if (
            row is None
            or _row_values(
                row,
                (
                    "account_id",
                    "email_id",
                    "object_type",
                    "object_fingerprint",
                    "action",
                    "result",
                    "actor",
                    "reason",
                    "safe_metadata",
                ),
            )
            != expected
        ):
            raise _AuditInvariantError()

    async def _lock_processing_email(
        self,
        connection: psycopg.AsyncConnection[Any],
        lease: _LeaseToken,
        email_id: str,
        *,
        skip_locked: bool = False,
    ) -> _EmailRow | None:
        query = sql.SQL(
            "SELECT {} FROM {} AS e WHERE e.account_id = %s AND e.id = %s FOR UPDATE{}"
        ).format(
            _EMAIL_RETURNING,
            self._table("emails"),
            sql.SQL(" SKIP LOCKED") if skip_locked else sql.SQL(""),
        )
        cursor = await connection.execute(query, (lease.account_id, email_id))
        row = await cursor.fetchone()
        if row is None:
            return None
        email = _email_from_row(row)
        if email.external_email_id != lease.event.external_email_id:
            raise ProcessingCompletionRejected()
        return email

    async def _lock_processing_ownership(
        self,
        connection: psycopg.AsyncConnection[Any],
        email: _EmailRow | None,
        lease: _LeaseToken,
        *,
        require_executable: bool,
    ) -> PipelineGenerationState:
        generations = sorted(
            {lease.generation}
            | ({email.owner_generation} if email is not None else set())
        )
        query = sql.SQL(
            "SELECT account_id, generation, pipeline_name, state, fencing_token "
            "FROM {} WHERE account_id = %s "
            "AND generation = ANY(%s::pg_catalog.int8[]) "
            "ORDER BY generation FOR SHARE"
        ).format(self._table("pipeline_ownership"))
        cursor = await connection.execute(query, (lease.account_id, generations))
        rows = await cursor.fetchall()
        locked: dict[int, tuple[object, ...]] = {}
        for row in rows:
            values = _row_values(
                row,
                (
                    "account_id",
                    "generation",
                    "pipeline_name",
                    "state",
                    "fencing_token",
                ),
            )
            generation = values[1]
            if isinstance(generation, bool) or not isinstance(generation, int):
                raise _invariant_error("email ownership row is invalid")
            locked[generation] = values
        if set(locked) != set(generations):
            raise ProcessingCompletionRejected()
        incoming = locked[lease.generation]
        if (
            incoming[0] != lease.account_id
            or incoming[2] != lease.pipeline_name
            or incoming[4] != lease.fencing_token
            or (require_executable and incoming[3] not in _LEASE_OWNERSHIP_STATES)
        ):
            raise ProcessingCompletionRejected()
        if email is not None:
            sticky = locked[email.owner_generation]
            if (
                sticky[0] != email.account_id
                or sticky[4] != email.owner_fencing_token
                or not isinstance(sticky[2], str)
            ):
                raise ProcessingCompletionRejected()
        try:
            return PipelineGenerationState(incoming[3])
        except (TypeError, ValueError):
            raise _invariant_error("email ownership state is invalid") from None

    async def _lock_processing_inbox(
        self,
        connection: psycopg.AsyncConnection[Any],
        lease: _LeaseToken,
        *,
        skip_locked: bool = False,
    ) -> _ProcessingInboxRow | None:
        query = sql.SQL(
            "SELECT {}, e.status AS inbox_status, e.effect_started_at, "
            "e.lease_until > pg_catalog.clock_timestamp() AS lease_active, "
            "e.safe_error_code, e.safe_error_summary "
            "FROM {} AS e WHERE e.id = %s FOR UPDATE{}"
        ).format(
            _LEASE_RETURNING,
            self._table("event_inbox"),
            sql.SQL(" SKIP LOCKED") if skip_locked else sql.SQL(""),
        )
        cursor = await connection.execute(query, (lease.id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        columns = _LEASE_COLUMNS + (
            "inbox_status",
            "effect_started_at",
            "lease_active",
            "safe_error_code",
            "safe_error_summary",
        )
        values = _row_values(row, columns)
        try:
            status = InboxStatus(values[-5])
        except (TypeError, ValueError):
            raise _invariant_error("event inbox status is invalid") from None
        attempts = values[_LEASE_COLUMNS.index("attempts")]
        account_id = values[_LEASE_COLUMNS.index("account_id")]
        external_email_id = values[_LEASE_COLUMNS.index("external_email_id")]
        generation = values[_LEASE_COLUMNS.index("generation")]
        fencing_token = values[_LEASE_COLUMNS.index("fencing_token")]
        if (
            isinstance(attempts, bool)
            or not isinstance(attempts, int)
            or attempts < 0
            or isinstance(account_id, bool)
            or not isinstance(account_id, int)
            or not isinstance(external_email_id, str)
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or isinstance(fencing_token, bool)
            or not isinstance(fencing_token, int)
        ):
            raise _invariant_error("event inbox processing row is invalid")
        persisted_lease = None
        if status is InboxStatus.LEASED:
            persisted_lease = _lease_from_row(values[: len(_LEASE_COLUMNS)])
        return _ProcessingInboxRow(
            status=status,
            lease=persisted_lease,
            effect_started_at=values[-4],
            attempts=attempts,
            account_id=account_id,
            external_email_id=external_email_id,
            generation=generation,
            fencing_token=fencing_token,
            lease_active=values[-3] is True,
            safe_error_code=(str(values[-2]) if values[-2] is not None else None),
            safe_error_summary=(str(values[-1]) if values[-1] is not None else None),
        )

    async def _lock_expired_inbox(
        self,
        connection: psycopg.AsyncConnection[Any],
        candidate: _ExpiredLeaseCandidate,
        *,
        skip_locked: bool,
    ) -> _ProcessingInboxRow | None:
        query = sql.SQL(
            "SELECT {}, e.status AS inbox_status, e.effect_started_at, "
            "e.lease_until > pg_catalog.clock_timestamp() AS lease_active, "
            "e.safe_error_code, e.safe_error_summary "
            "FROM {} AS e WHERE e.id = %s FOR UPDATE{}"
        ).format(
            _LEASE_RETURNING,
            self._table("event_inbox"),
            sql.SQL(" SKIP LOCKED") if skip_locked else sql.SQL(""),
        )
        cursor = await connection.execute(query, (candidate.id,))
        row = await cursor.fetchone()
        if row is None:
            return None
        columns = _LEASE_COLUMNS + (
            "inbox_status",
            "effect_started_at",
            "lease_active",
            "safe_error_code",
            "safe_error_summary",
        )
        values = _row_values(row, columns)
        try:
            status = InboxStatus(values[-5])
        except (TypeError, ValueError):
            raise _invariant_error("event inbox status is invalid") from None
        persisted = (
            _expired_candidate_from_row(values[: len(_LEASE_COLUMNS)])
            if status is InboxStatus.LEASED
            else None
        )
        attempts = values[_LEASE_COLUMNS.index("attempts")]
        account_id = values[_LEASE_COLUMNS.index("account_id")]
        external_email_id = values[_LEASE_COLUMNS.index("external_email_id")]
        generation = values[_LEASE_COLUMNS.index("generation")]
        fencing_token = values[_LEASE_COLUMNS.index("fencing_token")]
        if (
            isinstance(attempts, bool)
            or not isinstance(attempts, int)
            or isinstance(account_id, bool)
            or not isinstance(account_id, int)
            or not isinstance(external_email_id, str)
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or isinstance(fencing_token, bool)
            or not isinstance(fencing_token, int)
        ):
            raise _invariant_error("expired inbox lock row is invalid")
        return _ProcessingInboxRow(
            status=status,
            lease=persisted,
            effect_started_at=values[-4],
            attempts=attempts,
            account_id=account_id,
            external_email_id=external_email_id,
            generation=generation,
            fencing_token=fencing_token,
            lease_active=values[-3] is True,
            safe_error_code=(str(values[-2]) if values[-2] is not None else None),
            safe_error_summary=(str(values[-1]) if values[-1] is not None else None),
        )

    @staticmethod
    def _processing_receipt_metadata(
        *,
        lease: _LeaseToken,
        expected_email_version: int,
        operation: str,
        resolution: _ProcessingResolution,
        completion: ProcessingCompletion | None,
    ) -> dict[str, object]:
        metadata: dict[str, object] = {
            "execution_epoch": _PROCESSING_EXECUTION_EPOCH,
            "attempts": lease.attempts,
            "generation": lease.generation,
            "fencing_token": lease.fencing_token,
            "lease_token_hash": _lease_token_hash(lease),
            "expected_email_version": expected_email_version,
            "operation": operation,
            "email_status": resolution.email_status.value,
            "inbox_status": resolution.inbox_status.value,
            "email_version": min(expected_email_version + 1, POSTGRES_BIGINT_MAX),
            "safe_error_code": resolution.safe_code,
        }
        if completion is not None:
            metadata["requested_email_status"] = completion.target_status.value
            metadata["legacy_outcome"] = completion.legacy_outcome.value
            metadata["completion_safe_error_code"] = completion.safe_error_code
            metadata["completion_safe_error_summary"] = completion.safe_error_summary
        return metadata

    async def _load_processing_result_receipt(
        self,
        connection: psycopg.AsyncConnection[Any],
        *,
        lease: _LeaseToken,
        email_id: str,
        expected_email_version: int,
        operation: str,
        resolution: _ProcessingResolution,
        completion: ProcessingCompletion | None,
    ) -> bool:
        event_key = _processing_result_event_key(
            lease.id,
            lease.attempts,
            operation,
        )
        action = (
            _PROCESSING_COMPLETED_ACTION
            if operation == "success"
            else _PROCESSING_FAILED_ACTION
        )
        reason = resolution.safe_code or _PROCESSING_COMPLETED_REASON
        metadata = self._processing_receipt_metadata(
            lease=lease,
            expected_email_version=expected_email_version,
            operation=operation,
            resolution=resolution,
            completion=completion,
        )
        select = sql.SQL(
            "SELECT event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata "
            "FROM {} WHERE event_key = %s"
        ).format(self._table("audit_events"))
        cursor = await connection.execute(select, (event_key,))
        row = await cursor.fetchone()
        if row is None:
            return False
        actual = _row_values(
            row,
            (
                "event_key",
                "account_id",
                "email_id",
                "object_type",
                "object_fingerprint",
                "action",
                "result",
                "actor",
                "reason",
                "safe_metadata",
            ),
        )
        expected = (
            event_key,
            lease.account_id,
            UUID(email_id),
            _PROCESSING_RESULT_OBJECT_TYPE,
            _processing_result_fingerprint(email_id, lease.id),
            action,
            resolution.email_status.value,
            _AUDIT_ACTOR,
            reason,
        )
        if actual[:-1] != expected or not _json_values_equal(actual[-1], metadata):
            raise ProcessingReceiptConflict()
        return True

    async def _append_processing_result_receipt(
        self,
        connection: psycopg.AsyncConnection[Any],
        *,
        lease: _LeaseToken,
        email_id: str,
        expected_email_version: int,
        operation: str,
        resolution: _ProcessingResolution,
        completion: ProcessingCompletion | None,
    ) -> None:
        event_key = _processing_result_event_key(
            lease.id,
            lease.attempts,
            operation,
        )
        action = (
            _PROCESSING_COMPLETED_ACTION
            if operation == "success"
            else _PROCESSING_FAILED_ACTION
        )
        reason = resolution.safe_code or _PROCESSING_COMPLETED_REASON
        metadata = self._processing_receipt_metadata(
            lease=lease,
            expected_email_version=expected_email_version,
            operation=operation,
            resolution=resolution,
            completion=completion,
        )
        insert = sql.SQL(
            "INSERT INTO {} ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (event_key) DO NOTHING"
        ).format(self._table("audit_events"))
        await connection.execute(
            insert,
            (
                str(uuid4()),
                event_key,
                lease.account_id,
                email_id,
                _PROCESSING_RESULT_OBJECT_TYPE,
                _processing_result_fingerprint(email_id, lease.id),
                action,
                resolution.email_status.value,
                _AUDIT_ACTOR,
                reason,
                Jsonb(metadata),
            ),
        )
        if not await self._load_processing_result_receipt(
            connection,
            lease=lease,
            email_id=email_id,
            expected_email_version=expected_email_version,
            operation=operation,
            resolution=resolution,
            completion=completion,
        ):
            raise ProcessingReceiptConflict()

    @staticmethod
    def _assert_processing_replay_state(
        *,
        lease: _LeaseToken,
        email: _EmailRow,
        inbox: _ProcessingInboxRow,
        expected_email_version: int,
        operation: str,
        resolution: _ProcessingResolution,
    ) -> None:
        expected_link = (
            None if resolution.inbox_status is InboxStatus.COMPLETED else lease.id
        )
        expected_attempts = (
            resolution.attempts if operation == "failure" else lease.attempts
        )
        if (
            email.status is not resolution.email_status
            or email.version != expected_email_version + 1
            or email.processing_inbox_id != expected_link
            or email.safe_error_code != resolution.safe_code
            or email.safe_error_summary != resolution.safe_summary
            or inbox.status is not resolution.inbox_status
            or inbox.lease is not None
            or inbox.attempts != expected_attempts
            or inbox.safe_error_code != resolution.safe_code
            or inbox.safe_error_summary != resolution.safe_summary
        ):
            raise ProcessingReceiptConflict()

    @staticmethod
    def _resolution_for_failure_decision(
        *,
        decision: _FailureDecision,
        lease: _LeaseToken,
        expected_email_version: int,
    ) -> _ProcessingResolution:
        attempts = _next_attempts(lease.attempts)
        retry_has_capacity = (
            attempts <= _MAX_RETRIES
            and expected_email_version + 1 <= POSTGRES_BIGINT_MAX - 2
        )
        if (
            decision.status is InboxDispositionStatus.RETRY_WAIT
            and not retry_has_capacity
        ):
            decision = _FailureDecision(
                InboxDispositionStatus.DEAD_LETTER,
                decision.safe_code,
                decision.safe_summary,
            )
        return _ProcessingResolution(
            email_status=EmailStatus(decision.status.value),
            inbox_status=InboxStatus(decision.status.value),
            safe_code=decision.safe_code,
            safe_summary=decision.safe_summary,
            attempts=attempts,
            available_in_seconds=(
                min(5 * (2**lease.attempts), _MAX_BACKOFF_SECONDS)
                if decision.status is InboxDispositionStatus.RETRY_WAIT
                else 0
            ),
        )

    @classmethod
    def _failure_resolution(
        cls,
        *,
        error: BaseException,
        lease: InboxLease,
        email: _EmailRow,
        inbox: _ProcessingInboxRow,
        expected_email_version: int,
    ) -> _ProcessingResolution:
        effect_started = (
            inbox.effect_started_at is not None
            or email.external_effects_started_at is not None
        )
        decision = _EFFECT_UNKNOWN if effect_started else _failure_decision(error)
        return cls._resolution_for_failure_decision(
            decision=decision,
            lease=lease,
            expected_email_version=expected_email_version,
        )

    async def _require_authorized_processing_attempt(
        self,
        connection: psycopg.AsyncConnection[Any],
        *,
        lease: InboxLease,
        email: _EmailRow,
        inbox: _ProcessingInboxRow,
        expected_email_version: int,
        ownership_state: PipelineGenerationState,
        require_unexpired: bool,
    ) -> None:
        if (
            ownership_state.value not in _LEASE_OWNERSHIP_STATES
            or email.status is not EmailStatus.PROCESSING
            or email.version != expected_email_version
            or email.processing_inbox_id != lease.id
            or email.source_deleted_at is not None
            or email.owner_generation != lease.generation
            or email.owner_fencing_token != lease.fencing_token
            or inbox.status is not InboxStatus.LEASED
            or inbox.lease is None
            or not _leases_equal(inbox.lease, lease)
            or (require_unexpired and not inbox.lease_active)
            or inbox.account_id != lease.account_id
            or inbox.external_email_id != lease.event.external_email_id
            or inbox.generation != lease.generation
            or inbox.fencing_token != lease.fencing_token
        ):
            raise ProcessingCompletionRejected()
        transaction = self.transaction(connection)
        if not await transaction._load_processing_attempt(email, lease):
            raise ProcessingCompletionRejected()

    async def _duplicate_receipt(
        self,
        connection: psycopg.AsyncConnection[Any],
        event: NormalizedIngressEvent,
    ) -> IngressReceipt | None:
        query = sql.SQL(
            "SELECT id, account_id, source, raw_event_type, change_kind, "
            "external_email_id, folder_key, source_version, source_event_at "
            "FROM {} WHERE dedupe_key = %s"
        ).format(self._table("event_inbox"))
        cursor = await connection.execute(query, (event.dedupe_key,))
        row = await cursor.fetchone()
        if row is None:
            return None
        values = _row_values(
            row,
            (
                "id",
                "account_id",
                "source",
                "raw_event_type",
                "change_kind",
                "external_email_id",
                "folder_key",
                "source_version",
                "source_event_at",
            ),
        )
        persisted_identity = values[1:8]
        expected_identity = (
            event.account_id,
            event.source.value,
            event.raw_event_type,
            event.kind.value,
            event.external_email_id,
            event.folder,
            event.source_version,
        )
        if persisted_identity != expected_identity:
            raise DatabaseOperationError(
                operation="insert_event_inbox",
                retryable=False,
                message="event inbox dedupe identity conflict",
            )
        if event.source_version is None and values[8] != event.source_event_at:
            raise DatabaseOperationError(
                operation="insert_event_inbox",
                retryable=False,
                message="event inbox dedupe identity conflict",
            )
        return IngressReceipt(inbox_id=str(values[0]), duplicate=True)

    async def insert(
        self,
        event: NormalizedIngressEvent,
        generation: int,
        fencing_token: int,
    ) -> IngressReceipt:
        self._validate_insert_inputs(event, generation, fencing_token)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    return await self.transaction(connection).insert(
                        event,
                        generation,
                        fencing_token,
                    )
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None

    @staticmethod
    def _validate_insert_inputs(
        event: object,
        generation: object,
        fencing_token: object,
    ) -> tuple[NormalizedIngressEvent, int, int]:
        if type(event) is not NormalizedIngressEvent:
            raise ValueError("event must be an exact NormalizedIngressEvent")
        generation = _require_bigint("generation", generation)
        fencing_token = _require_bigint("fencing_token", fencing_token)
        return event, generation, fencing_token

    async def claim_batch(
        self,
        worker_id: str,
        pipeline_names: Iterable[str],
        limit: int,
        lease_seconds: int,
    ) -> list[InboxLease]:
        worker_id = _require_exact_text("worker_id", worker_id, max_length=128)
        pipelines = _require_pipeline_names(pipeline_names)
        limit = _require_bounded_int("limit", limit, maximum=_MAX_BATCH)
        lease_seconds = _require_bounded_int(
            "lease_seconds",
            lease_seconds,
            maximum=_MAX_LEASE_SECONDS,
        )
        candidate_window = min(_MAX_BATCH, max(limit, limit * 4))
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    candidates = sql.SQL(
                        "SELECT candidate.account_id FROM ("
                        "SELECT e.account_id, e.received_at, e.id "
                        "FROM {} AS e JOIN {} AS p "
                        "ON p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "WHERE e.status IN ('pending', 'retry_wait') "
                        "AND e.available_at <= pg_catalog.statement_timestamp() "
                        "AND e.processing_policy = ANY(%s::pg_catalog.text[]) "
                        "AND e.pipeline_name = ANY(%s::pg_catalog.text[]) "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "ORDER BY e.received_at, e.id LIMIT %s"
                        ") AS candidate GROUP BY candidate.account_id "
                        "ORDER BY candidate.account_id"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    candidate_cursor = await connection.execute(
                        candidates,
                        (
                            list(_CLAIMABLE_POLICIES),
                            list(pipelines),
                            list(_CLAIM_OWNERSHIP_STATES),
                            candidate_window,
                        ),
                    )
                    candidate_rows = await candidate_cursor.fetchall()
                    account_ids: list[int] = []
                    for row in candidate_rows:
                        value = _row_values(row, ("account_id",))[0]
                        account_ids.append(_require_bigint("account_id", value))
                    for account_id in sorted(set(account_ids)):
                        await self._acquire_account_lock(connection, account_id)
                    if not account_ids:
                        return []

                    claim = sql.SQL(
                        "WITH claimable AS ("
                        "SELECT e.id FROM {} AS e JOIN {} AS p "
                        "ON p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "WHERE e.status IN ('pending', 'retry_wait') "
                        "AND e.available_at <= pg_catalog.statement_timestamp() "
                        "AND e.processing_policy = ANY(%s::pg_catalog.text[]) "
                        "AND e.pipeline_name = ANY(%s::pg_catalog.text[]) "
                        "AND e.account_id = ANY(%s::pg_catalog.int8[]) "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "ORDER BY e.received_at, e.id "
                        "FOR UPDATE OF e SKIP LOCKED LIMIT %s"
                        ") UPDATE {} AS e SET "
                        "status = 'leased', lease_owner = %s, "
                        "lease_until = pg_catalog.statement_timestamp() "
                        "+ pg_catalog.make_interval(secs => %s), "
                        "processing_started_at = COALESCE("
                        "e.processing_started_at, pg_catalog.statement_timestamp()), "
                        "safe_error_code = NULL, safe_error_summary = NULL, "
                        "updated_at = pg_catalog.statement_timestamp() "
                        "FROM claimable AS c WHERE e.id = c.id RETURNING {}"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                        self._table("event_inbox"),
                        _LEASE_RETURNING,
                    )
                    claimed_cursor = await connection.execute(
                        claim,
                        (
                            list(_CLAIMABLE_POLICIES),
                            list(pipelines),
                            sorted(set(account_ids)),
                            list(_CLAIM_OWNERSHIP_STATES),
                            limit,
                            worker_id,
                            lease_seconds,
                        ),
                    )
                    leases = [
                        _lease_from_row(row) for row in await claimed_cursor.fetchall()
                    ]
                    leases.sort(key=lambda item: (item.received_at, item.id))
                    return leases
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("claim_event_inbox", error) from None

    async def renew(
        self,
        lease: InboxLease,
        lease_seconds: int,
    ) -> InboxLease | None:
        lease = self._require_lease(lease)
        lease_seconds = _require_bounded_int(
            "lease_seconds",
            lease_seconds,
            maximum=_MAX_LEASE_SECONDS,
        )
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    query = sql.SQL(
                        "UPDATE {} AS e SET lease_until = GREATEST("
                        "e.lease_until + INTERVAL '1 microsecond', "
                        "pg_catalog.clock_timestamp() "
                        "+ pg_catalog.make_interval(secs => %s)), "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) RETURNING {}"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                        _LEASE_RETURNING,
                    )
                    cursor = await connection.execute(
                        query,
                        (
                            lease_seconds,
                            lease.id,
                            lease.account_id,
                            lease.pipeline_name,
                            lease.generation,
                            lease.fencing_token,
                            lease.lease_owner,
                            lease.attempts,
                            lease.lease_until,
                            list(_LEASE_OWNERSHIP_STATES),
                        ),
                    )
                    row = await cursor.fetchone()
                    return _lease_from_row(row) if row is not None else None
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("renew_event_inbox_lease", error) from None

    async def begin_effect(self, lease: InboxLease) -> bool:
        lease = self._require_lease(lease)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    query = sql.SQL(
                        "UPDATE {} AS e SET effect_started_at = COALESCE("
                        "e.effect_started_at, pg_catalog.clock_timestamp()), "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) RETURNING e.id"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    cursor = await connection.execute(
                        query,
                        (*self._lease_params(lease), list(_LEASE_OWNERSHIP_STATES)),
                    )
                    return await cursor.fetchone() is not None
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("begin_event_inbox_effect", error) from None

    async def begin_processing_effect(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
    ) -> bool:
        lease = self._require_lease(lease)
        email_id = _require_email_id(email_id)
        expected_email_version = _require_email_version(expected_email_version)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    email = await self._lock_processing_email(
                        connection,
                        lease,
                        email_id,
                    )
                    if email is None:
                        raise ProcessingCompletionRejected()
                    ownership_state = await self._lock_processing_ownership(
                        connection,
                        email,
                        lease,
                        require_executable=True,
                    )
                    inbox = await self._lock_processing_inbox(connection, lease)
                    if inbox is None:
                        raise ProcessingCompletionRejected()
                    await self._require_authorized_processing_attempt(
                        connection,
                        lease=lease,
                        email=email,
                        inbox=inbox,
                        expected_email_version=expected_email_version,
                        ownership_state=ownership_state,
                        require_unexpired=True,
                    )
                    email_marked = email.external_effects_started_at is not None
                    inbox_marked = inbox.effect_started_at is not None
                    if email_marked is not inbox_marked:
                        raise ProcessingCompletionRejected()
                    if email_marked:
                        return True
                    timestamp_cursor = await connection.execute(
                        "SELECT pg_catalog.clock_timestamp() AS effect_started_at"
                    )
                    timestamp_row = await timestamp_cursor.fetchone()
                    if timestamp_row is None:
                        raise _invariant_error("effect timestamp is unavailable")
                    effect_started_at = _row_values(
                        timestamp_row,
                        ("effect_started_at",),
                    )[0]
                    email_update = sql.SQL(
                        "UPDATE {} SET external_effects_started_at = COALESCE("
                        "external_effects_started_at, %s), updated_at = %s "
                        "WHERE id = %s AND account_id = %s AND status = 'processing' "
                        "AND version = %s AND processing_inbox_id = %s "
                        "AND source_deleted_at IS NULL AND owner_generation = %s "
                        "AND owner_fencing_token = %s RETURNING id"
                    ).format(self._table("emails"))
                    email_cursor = await connection.execute(
                        email_update,
                        (
                            effect_started_at,
                            effect_started_at,
                            email_id,
                            lease.account_id,
                            expected_email_version,
                            lease.id,
                            lease.generation,
                            lease.fencing_token,
                        ),
                    )
                    if await email_cursor.fetchone() is None:
                        raise ProcessingCompletionRejected()
                    inbox_update = sql.SQL(
                        "UPDATE {} SET effect_started_at = COALESCE("
                        "effect_started_at, %s), updated_at = %s "
                        "WHERE id = %s AND account_id = %s AND pipeline_name = %s "
                        "AND generation = %s AND fencing_token = %s "
                        "AND lease_owner = %s AND attempts = %s "
                        "AND lease_until = %s AND status = 'leased' "
                        "AND lease_until > pg_catalog.clock_timestamp() RETURNING id"
                    ).format(self._table("event_inbox"))
                    inbox_cursor = await connection.execute(
                        inbox_update,
                        (
                            effect_started_at,
                            effect_started_at,
                            *self._lease_params(lease),
                        ),
                    )
                    if await inbox_cursor.fetchone() is None:
                        raise ProcessingCompletionRejected()
                    return True
        except ProcessingCompletionRejected:
            return False
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("begin_email_processing_effect", error) from None

    async def finish_email_processing(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
        completion: ProcessingCompletion,
    ) -> ProcessingFinishResult:
        lease = self._require_lease(lease)
        email_id = _require_email_id(email_id)
        expected_email_version = _require_email_version(expected_email_version)
        if type(completion) is not ProcessingCompletion:
            raise ValueError("completion must be an exact ProcessingCompletion")
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    email = await self._lock_processing_email(
                        connection,
                        lease,
                        email_id,
                    )
                    if email is None:
                        raise ProcessingCompletionRejected()
                    ownership_state = await self._lock_processing_ownership(
                        connection,
                        email,
                        lease,
                        require_executable=False,
                    )
                    inbox = await self._lock_processing_inbox(connection, lease)
                    if inbox is None:
                        raise ProcessingCompletionRejected()
                    effect_started = (
                        inbox.effect_started_at is not None
                        or email.external_effects_started_at is not None
                    )
                    if (
                        completion.target_status is EmailStatus.WAITING_APPROVAL
                        and expected_email_version >= POSTGRES_BIGINT_MAX - 1
                    ):
                        if effect_started:
                            resolution = _ProcessingResolution(
                                email_status=EmailStatus.MANUAL_REVIEW,
                                inbox_status=InboxStatus.MANUAL_REVIEW,
                                safe_code=_EFFECT_UNKNOWN.safe_code,
                                safe_summary=_EFFECT_UNKNOWN.safe_summary,
                                attempts=lease.attempts,
                                available_in_seconds=0,
                            )
                        else:
                            resolution = _ProcessingResolution(
                                email_status=EmailStatus.DEAD_LETTER,
                                inbox_status=InboxStatus.DEAD_LETTER,
                                safe_code=_PROCESSING_VERSION_EXHAUSTED_CODE,
                                safe_summary=_PROCESSING_VERSION_EXHAUSTED_SUMMARY,
                                attempts=lease.attempts,
                                available_in_seconds=0,
                            )
                    else:
                        resolution = _ProcessingResolution(
                            email_status=completion.target_status,
                            inbox_status=InboxStatus.COMPLETED,
                            safe_code=None,
                            safe_summary=None,
                            attempts=lease.attempts,
                            available_in_seconds=0,
                        )
                    if await self._load_processing_result_receipt(
                        connection,
                        lease=lease,
                        email_id=email_id,
                        expected_email_version=expected_email_version,
                        operation="success",
                        resolution=resolution,
                        completion=completion,
                    ):
                        self._assert_processing_replay_state(
                            lease=lease,
                            email=email,
                            inbox=inbox,
                            expected_email_version=expected_email_version,
                            operation="success",
                            resolution=resolution,
                        )
                        return ProcessingFinishResult(
                            email_status=resolution.email_status,
                            inbox_status=resolution.inbox_status,
                            replayed=True,
                        )
                    if expected_email_version >= POSTGRES_BIGINT_MAX:
                        raise ProcessingCompletionRejected()
                    await self._require_authorized_processing_attempt(
                        connection,
                        lease=lease,
                        email=email,
                        inbox=inbox,
                        expected_email_version=expected_email_version,
                        ownership_state=ownership_state,
                        require_unexpired=True,
                    )
                    terminal_failure = (
                        resolution.inbox_status is not InboxStatus.COMPLETED
                    )
                    email_update = sql.SQL(
                        "UPDATE {} SET status = %s, version = version + 1, "
                        "processing_inbox_id = CASE WHEN %s THEN processing_inbox_id "
                        "ELSE NULL END, safe_error_code = %s, "
                        "safe_error_summary = %s, updated_at = "
                        "pg_catalog.clock_timestamp() WHERE id = %s "
                        "AND account_id = %s AND status = 'processing' "
                        "AND version = %s AND processing_inbox_id = %s "
                        "AND source_deleted_at IS NULL AND version < %s "
                        "RETURNING id"
                    ).format(self._table("emails"))
                    email_cursor = await connection.execute(
                        email_update,
                        (
                            resolution.email_status.value,
                            terminal_failure,
                            resolution.safe_code,
                            resolution.safe_summary,
                            email_id,
                            lease.account_id,
                            expected_email_version,
                            lease.id,
                            POSTGRES_BIGINT_MAX,
                        ),
                    )
                    if await email_cursor.fetchone() is None:
                        raise ProcessingCompletionRejected()
                    inbox_update = sql.SQL(
                        "UPDATE {} SET status = %s, lease_owner = NULL, "
                        "lease_until = NULL, safe_error_code = %s, "
                        "safe_error_summary = %s, updated_at = "
                        "pg_catalog.clock_timestamp() WHERE id = %s "
                        "AND account_id = %s AND pipeline_name = %s "
                        "AND generation = %s AND fencing_token = %s "
                        "AND lease_owner = %s AND attempts = %s "
                        "AND lease_until = %s AND status = 'leased' RETURNING id"
                    ).format(self._table("event_inbox"))
                    inbox_cursor = await connection.execute(
                        inbox_update,
                        (
                            resolution.inbox_status.value,
                            resolution.safe_code,
                            resolution.safe_summary,
                            *self._lease_params(lease),
                        ),
                    )
                    if await inbox_cursor.fetchone() is None:
                        raise ProcessingCompletionRejected()
                    await self._append_processing_result_receipt(
                        connection,
                        lease=lease,
                        email_id=email_id,
                        expected_email_version=expected_email_version,
                        operation="success",
                        resolution=resolution,
                        completion=completion,
                    )
                    return ProcessingFinishResult(
                        email_status=resolution.email_status,
                        inbox_status=resolution.inbox_status,
                        replayed=False,
                    )
        except (
            ProcessingCompletionRejected,
            ProcessingReceiptConflict,
            DatabaseOperationError,
            ValueError,
        ):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("finish_email_processing", error) from None

    async def finish_email_processing_failure(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
        error: BaseException,
    ) -> ProcessingFinishResult:
        if not isinstance(error, BaseException):
            raise ValueError("error must be an exception")
        if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            raise error
        if not isinstance(error, Exception):
            raise error
        lease = self._require_lease(lease)
        email_id = _require_email_id(email_id)
        expected_email_version = _require_email_version(expected_email_version)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    email = await self._lock_processing_email(
                        connection,
                        lease,
                        email_id,
                    )
                    if email is None:
                        raise ProcessingCompletionRejected()
                    ownership_state = await self._lock_processing_ownership(
                        connection,
                        email,
                        lease,
                        require_executable=False,
                    )
                    inbox = await self._lock_processing_inbox(connection, lease)
                    if inbox is None:
                        raise ProcessingCompletionRejected()
                    resolution = self._failure_resolution(
                        error=error,
                        lease=lease,
                        email=email,
                        inbox=inbox,
                        expected_email_version=expected_email_version,
                    )
                    if await self._load_processing_result_receipt(
                        connection,
                        lease=lease,
                        email_id=email_id,
                        expected_email_version=expected_email_version,
                        operation="failure",
                        resolution=resolution,
                        completion=None,
                    ):
                        self._assert_processing_replay_state(
                            lease=lease,
                            email=email,
                            inbox=inbox,
                            expected_email_version=expected_email_version,
                            operation="failure",
                            resolution=resolution,
                        )
                        return ProcessingFinishResult(
                            email_status=resolution.email_status,
                            inbox_status=resolution.inbox_status,
                            replayed=True,
                        )
                    if expected_email_version >= POSTGRES_BIGINT_MAX:
                        raise ProcessingCompletionRejected()
                    await self._require_authorized_processing_attempt(
                        connection,
                        lease=lease,
                        email=email,
                        inbox=inbox,
                        expected_email_version=expected_email_version,
                        ownership_state=ownership_state,
                        require_unexpired=True,
                    )
                    email_update = sql.SQL(
                        "UPDATE {} SET status = %s, version = version + 1, "
                        "safe_error_code = %s, safe_error_summary = %s, "
                        "updated_at = pg_catalog.clock_timestamp() WHERE id = %s "
                        "AND account_id = %s AND status = 'processing' "
                        "AND version = %s AND processing_inbox_id = %s "
                        "AND source_deleted_at IS NULL AND version < %s "
                        "RETURNING id"
                    ).format(self._table("emails"))
                    email_cursor = await connection.execute(
                        email_update,
                        (
                            resolution.email_status.value,
                            resolution.safe_code,
                            resolution.safe_summary,
                            email_id,
                            lease.account_id,
                            expected_email_version,
                            lease.id,
                            POSTGRES_BIGINT_MAX,
                        ),
                    )
                    if await email_cursor.fetchone() is None:
                        raise ProcessingCompletionRejected()
                    inbox_update = sql.SQL(
                        "UPDATE {} SET status = %s, lease_owner = NULL, "
                        "lease_until = NULL, attempts = %s, available_at = CASE "
                        "WHEN %s = 'retry_wait' THEN pg_catalog.clock_timestamp() "
                        "+ pg_catalog.make_interval(secs => %s) ELSE available_at END, "
                        "safe_error_code = %s, safe_error_summary = %s, "
                        "updated_at = pg_catalog.clock_timestamp() WHERE id = %s "
                        "AND account_id = %s AND pipeline_name = %s "
                        "AND generation = %s AND fencing_token = %s "
                        "AND lease_owner = %s AND attempts = %s "
                        "AND lease_until = %s AND status = 'leased' RETURNING id"
                    ).format(self._table("event_inbox"))
                    inbox_cursor = await connection.execute(
                        inbox_update,
                        (
                            resolution.inbox_status.value,
                            resolution.attempts,
                            resolution.inbox_status.value,
                            resolution.available_in_seconds,
                            resolution.safe_code,
                            resolution.safe_summary,
                            *self._lease_params(lease),
                        ),
                    )
                    if await inbox_cursor.fetchone() is None:
                        raise ProcessingCompletionRejected()
                    await self._append_processing_result_receipt(
                        connection,
                        lease=lease,
                        email_id=email_id,
                        expected_email_version=expected_email_version,
                        operation="failure",
                        resolution=resolution,
                        completion=None,
                    )
                    return ProcessingFinishResult(
                        email_status=resolution.email_status,
                        inbox_status=resolution.inbox_status,
                        replayed=False,
                    )
        except (
            ProcessingCompletionRejected,
            ProcessingReceiptConflict,
            DatabaseOperationError,
            ValueError,
        ):
            raise
        except _DATABASE_EXCEPTIONS as database_error:
            raise _database_error(
                "finish_email_processing_failure",
                database_error,
            ) from None

    async def complete(self, lease: InboxLease) -> bool:
        lease = self._require_lease(lease)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    query = sql.SQL(
                        "UPDATE {} AS e SET status = 'completed', "
                        "lease_owner = NULL, lease_until = NULL, "
                        "safe_error_code = NULL, safe_error_summary = NULL, "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "RETURNING e.account_id, e.attempts"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    cursor = await connection.execute(
                        query,
                        (*self._lease_params(lease), list(_LEASE_OWNERSHIP_STATES)),
                    )
                    row = await cursor.fetchone()
                    if row is None:
                        return False
                    account_id, attempts = _row_values(
                        row,
                        ("account_id", "attempts"),
                    )
                    await self._append_audit(
                        connection,
                        inbox_id=lease.id,
                        account_id=account_id,  # type: ignore[arg-type]
                        action="ingress.completed",
                        result="completed",
                        reason="inbox.completed",
                        attempts=attempts,  # type: ignore[arg-type]
                        safe_metadata={
                            "attempts": attempts,
                            "status": "completed",
                        },
                    )
                    return True
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("complete_event_inbox", error) from None

    async def fail(
        self,
        lease: InboxLease,
        error: BaseException,
    ) -> InboxDisposition:
        if isinstance(error, asyncio.CancelledError):
            raise error
        if not isinstance(error, BaseException):
            raise ValueError("error must be an exception")
        if not isinstance(error, Exception):
            raise error
        lease = self._require_lease(lease)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    select = sql.SQL(
                        "SELECT e.effect_started_at, e.attempts "
                        "FROM {} AS e JOIN {} AS p "
                        "ON p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "FOR UPDATE OF e"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    selected_cursor = await connection.execute(
                        select,
                        (*self._lease_params(lease), list(_LEASE_OWNERSHIP_STATES)),
                    )
                    selected = await selected_cursor.fetchone()
                    if selected is None:
                        raise StaleFence()
                    effect_started_at, attempts = _row_values(
                        selected,
                        ("effect_started_at", "attempts"),
                    )
                    if not isinstance(attempts, int) or isinstance(attempts, bool):
                        raise _invariant_error("event inbox attempts are invalid")
                    new_attempts = _next_attempts(attempts)
                    decision = (
                        _EFFECT_UNKNOWN
                        if effect_started_at is not None
                        else _failure_decision(error)
                    )
                    if (
                        decision.status is InboxDispositionStatus.RETRY_WAIT
                        and new_attempts > _MAX_RETRIES
                    ):
                        decision = _FailureDecision(
                            InboxDispositionStatus.DEAD_LETTER,
                            decision.safe_code,
                            decision.safe_summary,
                        )
                    backoff = (
                        min(5 * (2**attempts), _MAX_BACKOFF_SECONDS)
                        if decision.status is InboxDispositionStatus.RETRY_WAIT
                        else 0
                    )
                    update = sql.SQL(
                        "UPDATE {} AS e SET status = %s, lease_owner = NULL, "
                        "lease_until = NULL, attempts = %s, "
                        "available_at = CASE WHEN %s = 'retry_wait' "
                        "THEN pg_catalog.clock_timestamp() "
                        "+ pg_catalog.make_interval(secs => %s) "
                        "ELSE e.available_at END, safe_error_code = %s, "
                        "safe_error_summary = %s, "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.effect_started_at IS NOT DISTINCT FROM %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "RETURNING e.attempts, e.available_at"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    updated_cursor = await connection.execute(
                        update,
                        (
                            decision.status.value,
                            new_attempts,
                            decision.status.value,
                            backoff,
                            decision.safe_code,
                            decision.safe_summary,
                            *self._lease_params(lease),
                            effect_started_at,
                            list(_LEASE_OWNERSHIP_STATES),
                        ),
                    )
                    updated = await updated_cursor.fetchone()
                    if updated is None:
                        raise StaleFence()
                    persisted_attempts, available_at = _row_values(
                        updated,
                        ("attempts", "available_at"),
                    )
                    if decision.status is not InboxDispositionStatus.RETRY_WAIT:
                        action = _terminal_action(decision)
                        await self._append_audit(
                            connection,
                            inbox_id=lease.id,
                            account_id=lease.account_id,
                            action=action,
                            result=decision.status.value,
                            reason=decision.safe_code,
                            attempts=new_attempts,
                            safe_metadata={
                                "attempts": new_attempts,
                                "safe_error_code": decision.safe_code,
                                "status": decision.status.value,
                            },
                        )
                    return InboxDisposition(
                        status=decision.status,
                        attempts=persisted_attempts,  # type: ignore[arg-type]
                        available_at=(
                            available_at
                            if decision.status is InboxDispositionStatus.RETRY_WAIT
                            else None
                        ),
                        safe_error_code=decision.safe_code,
                    )
        except (StaleFence, DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as database_error:
            raise _database_error("fail_event_inbox", database_error) from None

    async def _find_email_relation(
        self,
        connection: psycopg.AsyncConnection[Any],
        lease: _LeaseToken,
    ) -> tuple[str, str | None] | None:
        query = sql.SQL(
            "SELECT id, processing_inbox_id FROM {} WHERE account_id = %s "
            "AND external_email_id = %s"
        ).format(self._table("emails"))
        cursor = await connection.execute(
            query,
            (lease.account_id, lease.event.external_email_id),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        email_id, processing_inbox_id = _row_values(
            row,
            ("id", "processing_inbox_id"),
        )
        try:
            normalized_email_id = str(UUID(str(email_id)))
            normalized_inbox_id = (
                str(UUID(str(processing_inbox_id)))
                if processing_inbox_id is not None
                else None
            )
        except (TypeError, ValueError):
            raise _invariant_error("email processing relation is invalid") from None
        return normalized_email_id, normalized_inbox_id

    async def _recover_linked_expired_lease(
        self,
        connection: psycopg.AsyncConnection[Any],
        lease: _ExpiredLeaseCandidate,
        email_id: str,
    ) -> bool:
        email = await self._lock_processing_email(
            connection,
            lease,
            email_id,
            skip_locked=True,
        )
        if email is None:
            return False
        ownership_state = await self._lock_processing_ownership(
            connection,
            email,
            lease,
            require_executable=False,
        )
        inbox = await self._lock_expired_inbox(
            connection,
            lease,
            skip_locked=True,
        )
        if (
            inbox is None
            or inbox.status is not InboxStatus.LEASED
            or inbox.lease is None
            or not _leases_equal(inbox.lease, lease)
            or inbox.lease_active
            or email.processing_inbox_id != lease.id
        ):
            return False
        if email.version >= POSTGRES_BIGINT_MAX:
            raise _invariant_error("expired email processing version is exhausted")
        attempt_exists = await self.transaction(connection)._load_processing_attempt(
            email,
            lease,
        )
        effect_started = (
            inbox.effect_started_at is not None
            or email.external_effects_started_at is not None
        )
        attempt_state_is_valid = (
            email.status is EmailStatus.PROCESSING and attempt_exists
        ) or (email.status is EmailStatus.RETRY_WAIT and not attempt_exists)
        if effect_started:
            decision = _EFFECT_UNKNOWN
        elif (
            ownership_state.value not in _LEASE_OWNERSHIP_STATES
            or not attempt_state_is_valid
            or email.source_deleted_at is not None
            or email.owner_generation != lease.generation
            or email.owner_fencing_token != lease.fencing_token
        ):
            decision = _STALE_OWNERSHIP
        else:
            decision = _LEASE_EXPIRED
        resolution = self._resolution_for_failure_decision(
            decision=decision,
            lease=lease,
            expected_email_version=email.version,
        )
        if await self._load_processing_result_receipt(
            connection,
            lease=lease,
            email_id=email.id,
            expected_email_version=email.version,
            operation="failure",
            resolution=resolution,
            completion=None,
        ):
            self._assert_processing_replay_state(
                lease=lease,
                email=email,
                inbox=inbox,
                expected_email_version=email.version,
                operation="failure",
                resolution=resolution,
            )
            return False
        email_update = sql.SQL(
            "UPDATE {} SET status = %s, version = version + 1, "
            "safe_error_code = %s, safe_error_summary = %s, "
            "updated_at = pg_catalog.clock_timestamp() WHERE id = %s "
            "AND account_id = %s AND version = %s "
            "AND processing_inbox_id = %s AND version < %s RETURNING id"
        ).format(self._table("emails"))
        email_cursor = await connection.execute(
            email_update,
            (
                resolution.email_status.value,
                resolution.safe_code,
                resolution.safe_summary,
                email.id,
                lease.account_id,
                email.version,
                lease.id,
                POSTGRES_BIGINT_MAX,
            ),
        )
        if await email_cursor.fetchone() is None:
            raise ProcessingCompletionRejected()
        inbox_update = sql.SQL(
            "UPDATE {} SET status = %s, lease_owner = NULL, lease_until = NULL, "
            "attempts = %s, available_at = CASE WHEN %s = 'retry_wait' "
            "THEN pg_catalog.clock_timestamp() + "
            "pg_catalog.make_interval(secs => %s) ELSE available_at END, "
            "safe_error_code = %s, safe_error_summary = %s, "
            "updated_at = pg_catalog.clock_timestamp() WHERE id = %s "
            "AND account_id = %s AND pipeline_name = %s AND generation = %s "
            "AND fencing_token = %s AND lease_owner = %s AND attempts = %s "
            "AND lease_until = %s AND status = 'leased' AND lease_until <= "
            "pg_catalog.clock_timestamp() RETURNING id"
        ).format(self._table("event_inbox"))
        inbox_cursor = await connection.execute(
            inbox_update,
            (
                resolution.inbox_status.value,
                resolution.attempts,
                resolution.inbox_status.value,
                resolution.available_in_seconds,
                resolution.safe_code,
                resolution.safe_summary,
                *self._lease_params(lease),
            ),
        )
        if await inbox_cursor.fetchone() is None:
            raise ProcessingCompletionRejected()
        await self._append_processing_result_receipt(
            connection,
            lease=lease,
            email_id=email.id,
            expected_email_version=email.version,
            operation="failure",
            resolution=resolution,
            completion=None,
        )
        return True

    async def _recover_unlinked_expired_lease(
        self,
        connection: psycopg.AsyncConnection[Any],
        lease: _ExpiredLeaseCandidate,
    ) -> bool:
        ownership_state = await self._lock_processing_ownership(
            connection,
            None,
            lease,
            require_executable=False,
        )
        inbox = await self._lock_expired_inbox(
            connection,
            lease,
            skip_locked=True,
        )
        if (
            inbox is None
            or inbox.status is not InboxStatus.LEASED
            or inbox.lease is None
            or not _leases_equal(inbox.lease, lease)
            or inbox.lease_active
        ):
            return False
        relation = await self._find_email_relation(connection, lease)
        if relation is not None:
            return False
        new_attempts = _next_attempts(lease.attempts)
        if ownership_state.value not in _LEASE_OWNERSHIP_STATES:
            decision = _STALE_OWNERSHIP
        elif inbox.effect_started_at is not None:
            decision = _EFFECT_UNKNOWN
        elif new_attempts <= _MAX_RETRIES:
            decision = _LEASE_EXPIRED
        else:
            decision = _FailureDecision(
                InboxDispositionStatus.DEAD_LETTER,
                _LEASE_EXPIRED.safe_code,
                _LEASE_EXPIRED.safe_summary,
            )
        backoff = (
            min(5 * (2**lease.attempts), _MAX_BACKOFF_SECONDS)
            if decision.status is InboxDispositionStatus.RETRY_WAIT
            else 0
        )
        update = sql.SQL(
            "UPDATE {} SET status = %s, lease_owner = NULL, lease_until = NULL, "
            "attempts = %s, available_at = CASE WHEN %s = 'retry_wait' "
            "THEN pg_catalog.clock_timestamp() + "
            "pg_catalog.make_interval(secs => %s) ELSE available_at END, "
            "safe_error_code = %s, safe_error_summary = %s, "
            "updated_at = pg_catalog.clock_timestamp() WHERE id = %s "
            "AND account_id = %s AND pipeline_name = %s AND generation = %s "
            "AND fencing_token = %s AND lease_owner = %s AND attempts = %s "
            "AND lease_until = %s AND status = 'leased' AND lease_until <= "
            "pg_catalog.clock_timestamp() RETURNING id"
        ).format(self._table("event_inbox"))
        cursor = await connection.execute(
            update,
            (
                decision.status.value,
                new_attempts,
                decision.status.value,
                backoff,
                decision.safe_code,
                decision.safe_summary,
                *self._lease_params(lease),
            ),
        )
        if await cursor.fetchone() is None:
            return False
        if decision.status is not InboxDispositionStatus.RETRY_WAIT:
            await self._append_audit(
                connection,
                inbox_id=lease.id,
                account_id=lease.account_id,
                action=_terminal_action(decision),
                result=decision.status.value,
                reason=decision.safe_code,
                attempts=new_attempts,
                safe_metadata={
                    "attempts": new_attempts,
                    "safe_error_code": decision.safe_code,
                    "status": decision.status.value,
                },
            )
        return True

    async def recover_expired_leases(self, limit: int) -> int:
        limit = _require_bounded_int("limit", limit, maximum=_MAX_BATCH)
        candidate_error: BaseException | None = None
        recovered = 0
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    candidate_query = sql.SQL(
                        "SELECT {}, e.effect_started_at FROM {} AS e "
                        "WHERE e.status = 'leased' "
                        "AND e.lease_until <= pg_catalog.clock_timestamp() "
                        "ORDER BY e.lease_until, e.id LIMIT %s"
                    ).format(
                        _LEASE_RETURNING,
                        self._table("event_inbox"),
                    )
                    candidate_cursor = await connection.execute(
                        candidate_query,
                        (_MAX_BATCH,),
                    )
                    candidates = [
                        _expired_candidate_from_row(
                            _row_values(
                                row,
                                _LEASE_COLUMNS + ("effect_started_at",),
                            )[: len(_LEASE_COLUMNS)]
                        )
                        for row in await candidate_cursor.fetchall()
                    ]
                    for account_id in sorted(
                        {candidate.account_id for candidate in candidates}
                    ):
                        await self._acquire_account_lock(connection, account_id)
                    for candidate in candidates:
                        if recovered >= limit:
                            break
                        updated = False
                        try:
                            async with connection.transaction():
                                relation = await self._find_email_relation(
                                    connection,
                                    candidate,
                                )
                                if relation is not None and relation[1] == candidate.id:
                                    updated = await self._recover_linked_expired_lease(
                                        connection,
                                        candidate,
                                        relation[0],
                                    )
                                else:
                                    updated = (
                                        await self._recover_unlinked_expired_lease(
                                            connection,
                                            candidate,
                                        )
                                    )
                        except (
                            _AuditInvariantError,
                            ProcessingReceiptConflict,
                            ProcessingCompletionRejected,
                        ) as error:
                            if candidate_error is None:
                                candidate_error = error
                            continue
                        if updated:
                            recovered += 1
            if candidate_error is not None:
                raise candidate_error
            return recovered
        except (
            ProcessingReceiptConflict,
            ProcessingCompletionRejected,
            DatabaseOperationError,
            ValueError,
        ):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("recover_event_inbox_leases", error) from None

    async def stats(self) -> InboxStats:
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    query = sql.SQL(
                        "SELECT "
                        "pg_catalog.count(*) FILTER (WHERE status = 'pending') "
                        "AS pending, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'retry_wait') "
                        "AS retry_wait, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'leased') "
                        "AS leased, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'dead_letter') "
                        "AS dead_letter, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'manual_review') "
                        "AS manual_review, "
                        "COALESCE(GREATEST("
                        "EXTRACT(epoch FROM ("
                        "pg_catalog.clock_timestamp() - ("
                        "pg_catalog.min(received_at) FILTER ("
                        "WHERE status IN ('pending', 'retry_wait'))))), 0), 0) "
                        "AS oldest_pending_seconds FROM {}"
                    ).format(self._table("event_inbox"))
                    cursor = await connection.execute(query)
                    row = await cursor.fetchone()
                    values = _row_values(
                        row,
                        (
                            "pending",
                            "retry_wait",
                            "leased",
                            "dead_letter",
                            "manual_review",
                            "oldest_pending_seconds",
                        ),
                    )
                    return InboxStats(
                        pending=values[0],  # type: ignore[arg-type]
                        retry_wait=values[1],  # type: ignore[arg-type]
                        leased=values[2],  # type: ignore[arg-type]
                        dead_letter=values[3],  # type: ignore[arg-type]
                        manual_review=values[4],  # type: ignore[arg-type]
                        oldest_pending_seconds=float(values[5]),
                    )
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("read_event_inbox_stats", error) from None

    @staticmethod
    def _require_lease(value: object) -> InboxLease:
        if not isinstance(value, InboxLease):
            raise ValueError("lease must be an InboxLease")
        return value

    @staticmethod
    def _lease_params(lease: _LeaseToken) -> tuple[object, ...]:
        return (
            lease.id,
            lease.account_id,
            lease.pipeline_name,
            lease.generation,
            lease.fencing_token,
            lease.lease_owner,
            lease.attempts,
            lease.lease_until,
        )


class EmailEventTransaction:
    """Email aggregate primitive bound to one caller-owned transaction."""

    def __init__(
        self,
        repository: InboxRepository,
        connection: psycopg.AsyncConnection[Any],
        *,
        for_key_share: bool,
    ) -> None:
        if type(for_key_share) is not bool:
            raise ValueError("for_key_share must be an exact boolean")
        self._repository = repository
        self._connection = connection
        self._for_key_share = for_key_share
        self._transaction_id: str | None = None

    def _require_transaction(self) -> None:
        if self._connection.info.transaction_status is not TransactionStatus.INTRANS:
            raise RuntimeError("email event transaction is required")

    async def _assert_transaction_identity(self) -> None:
        self._require_transaction()
        cursor = await self._connection.execute(
            "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text "
            "AS transaction_id, "
            "pg_catalog.current_setting('transaction_isolation') "
            "AS transaction_isolation"
        )
        row = await cursor.fetchone()
        if row is None:
            raise _invariant_error("email event transaction identity is invalid")
        transaction_id, transaction_isolation = _row_values(
            row,
            ("transaction_id", "transaction_isolation"),
        )
        if (
            not isinstance(transaction_id, str)
            or not transaction_id.isascii()
            or not transaction_id.isdigit()
            or len(transaction_id) > 32
        ):
            raise _invariant_error("email event transaction identity is invalid")
        if transaction_isolation != "read committed":
            raise RuntimeError("email event transaction requires READ COMMITTED")
        if self._transaction_id is None:
            self._transaction_id = transaction_id
        elif self._transaction_id != transaction_id:
            raise StaleFence()

    async def insert(
        self,
        event: NormalizedIngressEvent,
        generation: int,
        fencing_token: int,
    ) -> IngressReceipt:
        for_key_share = self._for_key_share
        if type(for_key_share) is not bool:
            raise ValueError("for_key_share must be an exact boolean")
        event, generation, fencing_token = self._repository._validate_insert_inputs(
            event,
            generation,
            fencing_token,
        )
        payload = event.payload_for_storage()
        try:
            await self._assert_transaction_identity()
            await self._repository._acquire_account_lock(
                self._connection,
                event.account_id,
            )
            ownership_query = sql.SQL(
                "SELECT pipeline_name FROM {} "
                "WHERE account_id = %s AND generation = %s "
                "AND fencing_token = %s AND state = 'current_ingress'{}"
            ).format(
                self._repository._table("pipeline_ownership"),
                sql.SQL(" FOR KEY SHARE") if for_key_share else sql.SQL(""),
            )
            ownership_cursor = await self._connection.execute(
                ownership_query,
                (event.account_id, generation, fencing_token),
            )
            ownership_row = await ownership_cursor.fetchone()
            if ownership_row is None:
                raise StaleFence()
            pipeline_name = _row_values(
                ownership_row,
                ("pipeline_name",),
            )[0]
            if not isinstance(pipeline_name, str):
                raise _invariant_error("event inbox ownership row is invalid")

            duplicate = await self._repository._duplicate_receipt(
                self._connection,
                event,
            )
            if duplicate is not None:
                return duplicate

            suppressed = event.processing_policy in _SUPPRESSED_POLICIES
            status = "completed" if suppressed else "pending"
            inbox_id = str(uuid4())
            insert = sql.SQL(
                "INSERT INTO {} ("
                "id, account_id, external_email_id, folder_key, source, "
                "raw_event_type, change_kind, dedupe_key, source_version, "
                "source_event_at, payload, processing_policy, pipeline_name, "
                "generation, fencing_token, status, available_at"
                ") VALUES ("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, pg_catalog.clock_timestamp()"
                ") ON CONFLICT (dedupe_key) DO NOTHING RETURNING id"
            ).format(self._repository._table("event_inbox"))
            inserted_cursor = await self._connection.execute(
                insert,
                (
                    inbox_id,
                    event.account_id,
                    event.external_email_id,
                    event.folder,
                    event.source.value,
                    event.raw_event_type,
                    event.kind.value,
                    event.dedupe_key,
                    event.source_version,
                    event.source_event_at,
                    Jsonb(payload),
                    event.processing_policy.value,
                    pipeline_name,
                    generation,
                    fencing_token,
                    status,
                ),
            )
            inserted_row = await inserted_cursor.fetchone()
            if inserted_row is None:
                winner = await self._repository._duplicate_receipt(
                    self._connection,
                    event,
                )
                if winner is None:
                    raise _invariant_error("event inbox dedupe winner is unavailable")
                return winner
            inserted_id = str(_row_values(inserted_row, ("id",))[0])
            if inserted_id != inbox_id:
                raise _invariant_error("event inbox insert row is invalid")
            if suppressed:
                await self._repository._append_audit(
                    self._connection,
                    inbox_id=inbox_id,
                    account_id=event.account_id,
                    action="ingress.policy_suppressed",
                    result=status,
                    reason="inbox.policy_suppressed",
                    attempts=0,
                    safe_metadata={
                        "attempts": 0,
                        "processing_policy": event.processing_policy.value,
                        "status": status,
                    },
                )
            return IngressReceipt(inbox_id=inbox_id, duplicate=False)
        except (StaleFence, DatabaseOperationError, ValueError, RuntimeError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None

    async def _insert_neutral_shell(
        self,
        lease: InboxLease,
    ) -> tuple[bool, str]:
        email_id = str(uuid4())
        source_is_read = _source_is_read(lease.event)
        insert = sql.SQL(
            "INSERT INTO {} ("
            "id, account_id, external_email_id, source_folder_key, status, "
            "owner_generation, owner_fencing_token, processing_inbox_id, "
            "create_seen_at, processing_started_at, source_deleted_at, "
            "external_effects_started_at, safe_error_code, safe_error_summary, "
            "content_ref, is_read, is_read_refresh_required"
            ") VALUES ("
            "%s, %s, %s, %s, 'ingested', %s, %s, NULL, NULL, NULL, NULL, "
            "NULL, NULL, NULL, NULL, %s, %s"
            ") ON CONFLICT (account_id, external_email_id) DO NOTHING RETURNING id"
        ).format(self._repository._table("emails"))
        try:
            cursor = await self._connection.execute(
                insert,
                (
                    email_id,
                    lease.account_id,
                    lease.event.external_email_id,
                    lease.event.folder,
                    lease.generation,
                    lease.fencing_token,
                    source_is_read,
                    source_is_read is None,
                ),
            )
        except psycopg.errors.ForeignKeyViolation as error:
            if error.diag.constraint_name == "fk_emails_pipeline_ownership":
                raise StaleFence() from None
            raise
        row = await cursor.fetchone()
        if row is None:
            return False, email_id
        inserted_id = str(_row_values(row, ("id",))[0])
        if inserted_id != email_id:
            raise _invariant_error("email aggregate insert row is invalid")
        return True, email_id

    async def _lock_email(self, lease: InboxLease) -> _EmailRow:
        query = sql.SQL(
            "SELECT {} FROM {} AS e WHERE e.account_id = %s "
            "AND e.external_email_id = %s FOR UPDATE"
        ).format(
            _EMAIL_RETURNING,
            self._repository._table("emails"),
        )
        cursor = await self._connection.execute(
            query,
            (lease.account_id, lease.event.external_email_id),
        )
        row = await cursor.fetchone()
        if row is None:
            raise _invariant_error("email aggregate lock row is unavailable")
        return _email_from_row(row)

    async def _lock_ownership(
        self,
        email: _EmailRow,
        lease: InboxLease,
    ) -> PipelineGenerationState:
        generations = sorted({email.owner_generation, lease.generation})
        query = sql.SQL(
            "SELECT account_id, generation, pipeline_name, state, fencing_token "
            "FROM {} WHERE account_id = %s "
            "AND generation = ANY(%s::pg_catalog.int8[]) "
            "ORDER BY generation FOR SHARE"
        ).format(self._repository._table("pipeline_ownership"))
        cursor = await self._connection.execute(query, (lease.account_id, generations))
        rows = await cursor.fetchall()
        locked: dict[int, tuple[object, ...]] = {}
        for row in rows:
            values = _row_values(
                row,
                (
                    "account_id",
                    "generation",
                    "pipeline_name",
                    "state",
                    "fencing_token",
                ),
            )
            generation = values[1]
            if isinstance(generation, bool) or not isinstance(generation, int):
                raise _invariant_error("email ownership row is invalid")
            locked[generation] = values
        if set(locked) != set(generations):
            raise StaleFence()

        incoming = locked[lease.generation]
        if (
            incoming[0] != lease.account_id
            or incoming[2] != lease.pipeline_name
            or incoming[3] not in _LEASE_OWNERSHIP_STATES
            or incoming[4] != lease.fencing_token
        ):
            raise StaleFence()
        sticky = locked[email.owner_generation]
        if (
            sticky[0] != email.account_id
            or sticky[4] != email.owner_fencing_token
            or not isinstance(sticky[2], str)
        ):
            raise StaleFence()
        try:
            return PipelineGenerationState(sticky[3])
        except (TypeError, ValueError):
            raise _invariant_error("email ownership state is invalid") from None

    async def _lock_exact_lease(self, lease: InboxLease) -> None:
        query = sql.SQL(
            "SELECT {}, e.status AS inbox_status, "
            "e.lease_until > pg_catalog.clock_timestamp() AS lease_active "
            "FROM {} AS e WHERE e.id = %s FOR UPDATE"
        ).format(
            _LEASE_RETURNING,
            self._repository._table("event_inbox"),
        )
        cursor = await self._connection.execute(query, (lease.id,))
        row = await cursor.fetchone()
        if row is None:
            raise StaleFence()
        lock_columns = _LEASE_COLUMNS + ("inbox_status", "lease_active")
        locked_values = _row_values(row, lock_columns)
        inbox_status, lease_active = locked_values[-2:]
        if inbox_status != "leased" or lease_active is not True:
            raise StaleFence()
        persisted = _lease_from_row(locked_values[: len(_LEASE_COLUMNS)])
        if not _leases_equal(persisted, lease):
            raise StaleFence()

    async def _load_processing_attempt(
        self,
        email: _EmailRow,
        lease: InboxLease,
    ) -> bool:
        event_key = _processing_attempt_event_key(lease.id, lease.attempts)
        fingerprint = _processing_attempt_fingerprint(email.id, lease.id)
        metadata = {
            "execution_epoch": _PROCESSING_EXECUTION_EPOCH,
            "attempts": lease.attempts,
            "generation": lease.generation,
            "fencing_token": lease.fencing_token,
        }
        select = sql.SQL(
            "SELECT event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata "
            "FROM {} WHERE event_key = %s"
        ).format(self._repository._table("audit_events"))
        selected_cursor = await self._connection.execute(select, (event_key,))
        row = await selected_cursor.fetchone()
        if row is None:
            return False
        actual = _row_values(
            row,
            (
                "event_key",
                "account_id",
                "email_id",
                "object_type",
                "object_fingerprint",
                "action",
                "result",
                "actor",
                "reason",
                "safe_metadata",
            ),
        )
        expected = (
            event_key,
            email.account_id,
            UUID(email.id),
            _PROCESSING_ATTEMPT_OBJECT_TYPE,
            fingerprint,
            _PROCESSING_ATTEMPT_ACTION,
            _PROCESSING_ATTEMPT_RESULT,
            _AUDIT_ACTOR,
            _PROCESSING_ATTEMPT_REASON,
        )
        if actual[:-1] != expected or not _json_values_equal(actual[-1], metadata):
            raise _invariant_error("email processing receipt invariant failed")
        return True

    async def _elect_processing_attempt(
        self,
        email: _EmailRow,
        lease: InboxLease,
    ) -> bool:
        event_key = _processing_attempt_event_key(lease.id, lease.attempts)
        fingerprint = _processing_attempt_fingerprint(email.id, lease.id)
        metadata = {
            "execution_epoch": _PROCESSING_EXECUTION_EPOCH,
            "attempts": lease.attempts,
            "generation": lease.generation,
            "fencing_token": lease.fencing_token,
        }
        insert = sql.SQL(
            "INSERT INTO {} ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata"
            ") VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
            ") ON CONFLICT (event_key) DO NOTHING RETURNING id"
        ).format(self._repository._table("audit_events"))
        cursor = await self._connection.execute(
            insert,
            (
                str(uuid4()),
                event_key,
                email.account_id,
                email.id,
                _PROCESSING_ATTEMPT_OBJECT_TYPE,
                fingerprint,
                _PROCESSING_ATTEMPT_ACTION,
                _PROCESSING_ATTEMPT_RESULT,
                _AUDIT_ACTOR,
                _PROCESSING_ATTEMPT_REASON,
                Jsonb(metadata),
            ),
        )
        fresh = await cursor.fetchone() is not None
        if not await self._load_processing_attempt(email, lease):
            raise _invariant_error("email processing receipt invariant failed")
        return fresh

    async def _enter_processing(
        self,
        email: _EmailRow,
        lease: InboxLease,
        *,
        inserted: bool,
    ) -> _EmailRow:
        source_folder_key = email.source_folder_key
        is_read = email.is_read
        refresh_required = email.is_read_refresh_required
        if not inserted:
            incoming_read = _source_is_read(lease.event)
            if lease.event.folder != email.source_folder_key:
                refresh_required = True
            if incoming_read is None:
                refresh_required = True
            elif email.is_read is None:
                is_read = incoming_read
            elif incoming_read != email.is_read:
                refresh_required = True
        update = sql.SQL(
            "UPDATE {} AS e SET status = 'processing', version = e.version + 1, "
            "processing_inbox_id = COALESCE(e.processing_inbox_id, %s), "
            "create_seen_at = COALESCE(e.create_seen_at, "
            "pg_catalog.clock_timestamp()), "
            "processing_started_at = COALESCE(e.processing_started_at, "
            "pg_catalog.clock_timestamp()), safe_error_code = NULL, "
            "safe_error_summary = NULL, source_folder_key = %s, is_read = %s, "
            "is_read_refresh_required = %s, "
            "updated_at = pg_catalog.clock_timestamp() "
            "WHERE e.id = %s AND e.version = %s "
            "AND e.version < %s RETURNING {}"
        ).format(
            self._repository._table("emails"),
            _EMAIL_RETURNING,
        )
        cursor = await self._connection.execute(
            update,
            (
                lease.id,
                source_folder_key,
                is_read,
                refresh_required,
                email.id,
                email.version,
                POSTGRES_BIGINT_MAX,
            ),
        )
        row = await cursor.fetchone()
        if row is None:
            raise _invariant_error("email processing version CAS failed")
        return _email_from_row(row)

    async def _record_source_delete(
        self,
        email: _EmailRow,
        decision: EmailEventDecision,
    ) -> _EmailRow:
        if email.version >= POSTGRES_BIGINT_MAX:
            raise DatabaseOperationError(
                operation="email.version_exhausted",
                retryable=False,
                message="Email aggregate version is exhausted",
            )
        clears_processing = decision.new_status is EmailStatus.CANCELLED
        update = sql.SQL(
            "UPDATE {} AS e SET status = %s, version = e.version + 1, "
            "source_deleted_at = COALESCE(e.source_deleted_at, "
            "pg_catalog.clock_timestamp()), processing_inbox_id = CASE "
            "WHEN %s THEN NULL ELSE e.processing_inbox_id END, "
            "safe_error_code = CASE WHEN %s THEN NULL ELSE e.safe_error_code END, "
            "safe_error_summary = CASE WHEN %s THEN NULL "
            "ELSE e.safe_error_summary END, updated_at = pg_catalog.clock_timestamp() "
            "WHERE e.id = %s AND e.version = %s AND e.version < %s RETURNING {}"
        ).format(
            self._repository._table("emails"),
            _EMAIL_RETURNING,
        )
        cursor = await self._connection.execute(
            update,
            (
                decision.new_status.value,
                clears_processing,
                clears_processing,
                clears_processing,
                email.id,
                email.version,
                POSTGRES_BIGINT_MAX,
            ),
        )
        row = await cursor.fetchone()
        if row is None:
            raise _invariant_error("email deletion version CAS failed")
        return _email_from_row(row)

    async def _apply_metadata_projection(
        self,
        email: _EmailRow,
        event: NormalizedIngressEvent,
    ) -> tuple[_EmailRow, bool]:
        incoming_read = _source_is_read(event)
        projected_read = email.is_read
        refresh_required = email.is_read_refresh_required
        if incoming_read is None:
            refresh_required = True
        elif incoming_read:
            projected_read = True
        elif email.is_read is True:
            projected_read = True
            refresh_required = True
        else:
            projected_read = False

        changed = (
            event.folder != email.source_folder_key
            or projected_read is not email.is_read
            or refresh_required is not email.is_read_refresh_required
        )
        if not changed:
            return email, False
        update = sql.SQL(
            "UPDATE {} AS e SET source_folder_key = %s, is_read = %s, "
            "is_read_refresh_required = %s, "
            "updated_at = pg_catalog.clock_timestamp() "
            "WHERE e.id = %s AND e.version = %s RETURNING {}"
        ).format(
            self._repository._table("emails"),
            _EMAIL_RETURNING,
        )
        cursor = await self._connection.execute(
            update,
            (
                event.folder,
                projected_read,
                refresh_required,
                email.id,
                email.version,
            ),
        )
        row = await cursor.fetchone()
        if row is None:
            raise _invariant_error("email metadata projection CAS failed")
        return _email_from_row(row), True

    @staticmethod
    def _application(
        *,
        decision: EmailEventDecision,
        email: _EmailRow,
        disposition: EmailEventDisposition,
        may_complete_without_processing: bool,
    ) -> EmailEventApplication:
        return EmailEventApplication(
            decision=decision,
            email_id=email.id,
            persisted_status=email.status,
            version=email.version,
            disposition=disposition,
            may_complete_without_processing=may_complete_without_processing,
        )

    async def apply_email_event(
        self,
        lease: InboxLease,
    ) -> EmailEventApplication:
        lease = self._repository._require_lease(lease)
        try:
            await self._assert_transaction_identity()
            await self._repository._acquire_account_lock(
                self._connection,
                lease.account_id,
            )
            inserted, _candidate_id = await self._insert_neutral_shell(lease)
            email = await self._lock_email(lease)
            sticky_state = await self._lock_ownership(email, lease)
            await self._lock_exact_lease(lease)

            cross_generation = (
                email.owner_generation != lease.generation
                or email.owner_fencing_token != lease.fencing_token
            )
            if (
                cross_generation
                and lease.event.kind is ChangeKind.CREATE
                and email.create_seen_at is None
                and email.source_deleted_at is None
                and email.status in _RETIRED_BLOCKING_EMAIL_STATUSES
            ):
                raise ManualReviewRequired(
                    reason="email.sticky_owner_mismatch",
                    safe_summary="Email ownership requires review",
                )
            if (
                cross_generation
                and lease.event.kind is ChangeKind.DELETE
                and sticky_state is PipelineGenerationState.RETIRED
                and email.status in _RETIRED_BLOCKING_EMAIL_STATUSES
            ):
                raise ManualReviewRequired(
                    reason="email.retired_owner_unresolved",
                    safe_summary="Email ownership requires review",
                )

            if not inserted and lease.event.kind is ChangeKind.CREATE:
                receipt_exists = await self._load_processing_attempt(email, lease)
                if receipt_exists:
                    if (
                        email.status is not EmailStatus.PROCESSING
                        or email.processing_inbox_id != lease.id
                    ):
                        raise _invariant_error(
                            "email processing receipt conflicts with aggregate state"
                        )
                    duplicate_decision = EmailEventDecision(
                        should_process=False,
                        should_cancel=False,
                        new_status=EmailStatus.PROCESSING,
                        cancel_pending_side_effects=False,
                        create_seen=True,
                        reason=EmailEventReason.PROCESSING_ATTEMPT_ALREADY_ELECTED,
                    )
                    return self._application(
                        decision=duplicate_decision,
                        email=email,
                        disposition=(EmailEventDisposition.PROCESSING_ALREADY_ELECTED),
                        may_complete_without_processing=False,
                    )

            decision = decide_email_event(
                current_status=None if inserted else email.status,
                create_seen=False if inserted else email.create_seen_at is not None,
                kind=lease.event.kind,
                source_is_read=_source_is_read(lease.event),
                processing_owner_matches=(
                    not inserted and email.processing_inbox_id == lease.id
                ),
                external_effects_started=(
                    not inserted and email.external_effects_started_at is not None
                ),
                source_deleted=(not inserted and email.source_deleted_at is not None),
            )

            if decision.should_process:
                if email.version > POSTGRES_BIGINT_MAX - 2:
                    raise DatabaseOperationError(
                        operation="email.processing_version_exhausted",
                        retryable=False,
                        message="Email processing version is exhausted",
                    )
                fresh = await self._elect_processing_attempt(email, lease)
                if not fresh:
                    if email.status is not EmailStatus.PROCESSING:
                        raise _invariant_error(
                            "email processing receipt conflicts with aggregate state"
                        )
                    duplicate_decision = EmailEventDecision(
                        should_process=False,
                        should_cancel=False,
                        new_status=EmailStatus.PROCESSING,
                        cancel_pending_side_effects=False,
                        create_seen=True,
                        reason=(EmailEventReason.PROCESSING_ATTEMPT_ALREADY_ELECTED),
                    )
                    return self._application(
                        decision=duplicate_decision,
                        email=email,
                        disposition=(EmailEventDisposition.PROCESSING_ALREADY_ELECTED),
                        may_complete_without_processing=False,
                    )
                email = await self._enter_processing(
                    email,
                    lease,
                    inserted=inserted,
                )
                disposition = (
                    EmailEventDisposition.CREATOR_ELECTED
                    if decision.reason is EmailEventReason.FIRST_CREATE
                    else EmailEventDisposition.PROCESSING_RESUMED
                )
                return self._application(
                    decision=decision,
                    email=email,
                    disposition=disposition,
                    may_complete_without_processing=False,
                )

            if lease.event.kind is ChangeKind.DELETE:
                if email.source_deleted_at is None:
                    email = await self._record_source_delete(email, decision)
                    disposition = (
                        EmailEventDisposition.TOMBSTONE_CREATED
                        if inserted
                        else EmailEventDisposition.AGGREGATE_UPDATED
                    )
                else:
                    disposition = EmailEventDisposition.AGGREGATE_NOOP
                return self._application(
                    decision=decision,
                    email=email,
                    disposition=disposition,
                    may_complete_without_processing=True,
                )

            if (
                not inserted
                and lease.event.kind in {ChangeKind.UPDATE, ChangeKind.READ}
                and email.source_deleted_at is None
            ):
                email, changed = await self._apply_metadata_projection(
                    email,
                    lease.event,
                )
                return self._application(
                    decision=decision,
                    email=email,
                    disposition=(
                        EmailEventDisposition.AGGREGATE_UPDATED
                        if changed
                        else EmailEventDisposition.AGGREGATE_NOOP
                    ),
                    may_complete_without_processing=True,
                )

            if inserted:
                disposition = (
                    EmailEventDisposition.METADATA_SHELL_CREATED
                    if lease.event.kind in {ChangeKind.UPDATE, ChangeKind.READ}
                    else EmailEventDisposition.TOMBSTONE_CREATED
                )
            else:
                disposition = EmailEventDisposition.AGGREGATE_NOOP
            return self._application(
                decision=decision,
                email=email,
                disposition=disposition,
                may_complete_without_processing=True,
            )
        except (
            StaleFence,
            ManualReviewRequired,
            DatabaseOperationError,
            ValueError,
            RuntimeError,
        ):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("apply_email_event", error) from None


__all__ = ["EmailEventTransaction", "InboxRepository"]
