"""Pure monotonic transitions for the durable email aggregate."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final, TypeVar
from uuid import UUID

from src.ingestion.models import ChangeKind, POSTGRES_BIGINT_MAX


class EmailStatus(StrEnum):
    INGESTED = "ingested"
    PROCESSING = "processing"
    RETRY_WAIT = "retry_wait"
    MANUAL_REVIEW = "manual_review"
    WAITING_APPROVAL = "waiting_approval"
    NOTIFIED_READONLY = "notified_readonly"
    SEND_QUEUED = "send_queued"
    SENDING = "sending"
    ACCEPTED = "accepted"
    SENT = "sent"
    SEND_FAILED = "send_failed"
    DELIVERY_FAILED = "delivery_failed"
    SEND_UNKNOWN = "send_unknown"
    NO_ACTION = "no_action"
    ARCHIVED = "archived"
    REJECTED = "rejected"
    DRAFT_SAVED = "draft_saved"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


class EmailEventReason(StrEnum):
    FIRST_CREATE = "first_create"
    PROCESSING_RESUMED = "processing_resumed"
    PROCESSING_ATTEMPT_ALREADY_ELECTED = "processing_attempt_already_elected"
    METADATA_EVENT = "metadata_event"
    DUPLICATE_CREATE = "duplicate_create"
    SOURCE_TOMBSTONE = "source_tombstone"
    SOURCE_DELETE_CANCELLED = "source_delete_cancelled"
    SOURCE_DELETE_RECORDED = "source_delete_recorded"
    SOURCE_DELETED_PRESERVED = "source_deleted_preserved"
    STATUS_PRESERVED = "status_preserved"


class EmailEventDisposition(StrEnum):
    CREATOR_ELECTED = "creator_elected"
    PROCESSING_RESUMED = "processing_resumed"
    PROCESSING_ALREADY_ELECTED = "processing_already_elected"
    METADATA_SHELL_CREATED = "metadata_shell_created"
    TOMBSTONE_CREATED = "tombstone_created"
    AGGREGATE_UPDATED = "aggregate_updated"
    AGGREGATE_NOOP = "aggregate_noop"


@dataclass(frozen=True, slots=True)
class _EmailStatusTransition:
    cancel_on_delete: bool
    may_resume_processing: bool = False


# This is intentionally an explicit row-per-status manifest.  The unit contract
# compares its keys directly with the migration CHECK vocabulary.
EMAIL_STATUS_TRANSITIONS: Final[Mapping[EmailStatus, _EmailStatusTransition]] = (
    MappingProxyType(
        {
            EmailStatus.INGESTED: _EmailStatusTransition(cancel_on_delete=True),
            EmailStatus.PROCESSING: _EmailStatusTransition(
                cancel_on_delete=True,
                may_resume_processing=True,
            ),
            EmailStatus.RETRY_WAIT: _EmailStatusTransition(
                cancel_on_delete=True,
                may_resume_processing=True,
            ),
            EmailStatus.MANUAL_REVIEW: _EmailStatusTransition(cancel_on_delete=True),
            EmailStatus.WAITING_APPROVAL: _EmailStatusTransition(cancel_on_delete=True),
            EmailStatus.NOTIFIED_READONLY: _EmailStatusTransition(
                cancel_on_delete=False
            ),
            EmailStatus.SEND_QUEUED: _EmailStatusTransition(cancel_on_delete=True),
            EmailStatus.SENDING: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.ACCEPTED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.SENT: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.SEND_FAILED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.DELIVERY_FAILED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.SEND_UNKNOWN: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.NO_ACTION: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.ARCHIVED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.REJECTED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.DRAFT_SAVED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.EXPIRED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.CANCELLED: _EmailStatusTransition(cancel_on_delete=False),
            EmailStatus.DEAD_LETTER: _EmailStatusTransition(cancel_on_delete=False),
        }
    )
)


_EnumT = TypeVar("_EnumT", bound=StrEnum)


def _require_enum(name: str, value: object, enum_type: type[_EnumT]) -> _EnumT:
    if not isinstance(value, (str, enum_type)):
        raise ValueError(f"{name} must be a valid {enum_type.__name__}")
    try:
        return enum_type(value)
    except ValueError:
        raise ValueError(f"{name} must be a valid {enum_type.__name__}") from None


def _require_bool(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _require_uuid(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a UUID string")
    try:
        return str(UUID(value))
    except (AttributeError, ValueError):
        raise ValueError(f"{name} must be a UUID string") from None


def _require_version(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > POSTGRES_BIGINT_MAX
    ):
        raise ValueError(
            f"version must be an integer between 0 and {POSTGRES_BIGINT_MAX}"
        )
    return value


@dataclass(frozen=True, slots=True)
class EmailEventDecision:
    should_process: bool
    should_cancel: bool
    new_status: EmailStatus
    cancel_pending_side_effects: bool
    create_seen: bool
    reason: EmailEventReason

    def __post_init__(self) -> None:
        _require_bool("should_process", self.should_process)
        _require_bool("should_cancel", self.should_cancel)
        object.__setattr__(
            self,
            "new_status",
            _require_enum("new_status", self.new_status, EmailStatus),
        )
        _require_bool(
            "cancel_pending_side_effects",
            self.cancel_pending_side_effects,
        )
        _require_bool("create_seen", self.create_seen)
        object.__setattr__(
            self,
            "reason",
            _require_enum("reason", self.reason, EmailEventReason),
        )
        if self.should_process and self.should_cancel:
            raise ValueError("processing and cancellation are mutually exclusive")
        if self.cancel_pending_side_effects and not self.should_cancel:
            raise ValueError("cancelling pending effects requires should_cancel")
        if self.should_process and self.new_status is not EmailStatus.PROCESSING:
            raise ValueError("processing decisions require processing status")
        if self.should_process and not self.create_seen:
            raise ValueError("processing decisions require create_seen")
        if self.should_cancel and self.new_status is not EmailStatus.CANCELLED:
            raise ValueError("cancellation decisions require cancelled status")


@dataclass(frozen=True, slots=True)
class _EmailEventApplicationManifestRow:
    disposition: EmailEventDisposition
    reason: EmailEventReason
    persisted_statuses: frozenset[EmailStatus]
    should_process: bool
    should_cancel: bool
    cancel_pending_side_effects: bool
    create_seen: bool
    may_complete_without_processing: bool


_ALL_EMAIL_STATUSES: Final[frozenset[EmailStatus]] = frozenset(EmailStatus)
_PROCESSING_STATUS: Final[frozenset[EmailStatus]] = frozenset({EmailStatus.PROCESSING})
_INGESTED_STATUS: Final[frozenset[EmailStatus]] = frozenset({EmailStatus.INGESTED})
_CANCELLED_STATUS: Final[frozenset[EmailStatus]] = frozenset({EmailStatus.CANCELLED})
_NON_INGESTED_EMAIL_STATUSES: Final[frozenset[EmailStatus]] = frozenset(
    _ALL_EMAIL_STATUSES - _INGESTED_STATUS
)


# Every legal application result is an explicit row.  Some event reasons have
# two rows because an existing aggregate may or may not have observed CREATE.
_EMAIL_EVENT_APPLICATION_MANIFEST: Final[
    tuple[_EmailEventApplicationManifestRow, ...]
] = (
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.CREATOR_ELECTED,
        EmailEventReason.FIRST_CREATE,
        _PROCESSING_STATUS,
        True,
        False,
        False,
        True,
        False,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.PROCESSING_RESUMED,
        EmailEventReason.PROCESSING_RESUMED,
        _PROCESSING_STATUS,
        True,
        False,
        False,
        True,
        False,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.PROCESSING_ALREADY_ELECTED,
        EmailEventReason.PROCESSING_ATTEMPT_ALREADY_ELECTED,
        _PROCESSING_STATUS,
        False,
        False,
        False,
        True,
        False,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.METADATA_SHELL_CREATED,
        EmailEventReason.METADATA_EVENT,
        _INGESTED_STATUS,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.TOMBSTONE_CREATED,
        EmailEventReason.SOURCE_TOMBSTONE,
        _CANCELLED_STATUS,
        False,
        True,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.METADATA_EVENT,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.METADATA_EVENT,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        True,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.SOURCE_DELETE_CANCELLED,
        _CANCELLED_STATUS,
        False,
        True,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.SOURCE_DELETE_CANCELLED,
        _CANCELLED_STATUS,
        False,
        True,
        True,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.SOURCE_DELETE_CANCELLED,
        _CANCELLED_STATUS,
        False,
        True,
        True,
        True,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.SOURCE_DELETE_RECORDED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.SOURCE_DELETE_RECORDED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        True,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.STATUS_PRESERVED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_UPDATED,
        EmailEventReason.STATUS_PRESERVED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        True,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.DUPLICATE_CREATE,
        _NON_INGESTED_EMAIL_STATUSES,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.DUPLICATE_CREATE,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        True,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.SOURCE_DELETED_PRESERVED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.SOURCE_DELETED_PRESERVED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        True,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.STATUS_PRESERVED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.STATUS_PRESERVED,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        True,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.METADATA_EVENT,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        False,
        True,
    ),
    _EmailEventApplicationManifestRow(
        EmailEventDisposition.AGGREGATE_NOOP,
        EmailEventReason.METADATA_EVENT,
        _ALL_EMAIL_STATUSES,
        False,
        False,
        False,
        True,
        True,
    ),
)


@dataclass(frozen=True, slots=True)
class EmailEventApplication:
    decision: EmailEventDecision
    email_id: str
    persisted_status: EmailStatus
    version: int
    disposition: EmailEventDisposition
    may_complete_without_processing: bool

    def __post_init__(self) -> None:
        if not isinstance(self.decision, EmailEventDecision):
            raise ValueError("decision must be an EmailEventDecision")
        object.__setattr__(self, "email_id", _require_uuid("email_id", self.email_id))
        object.__setattr__(
            self,
            "persisted_status",
            _require_enum("persisted_status", self.persisted_status, EmailStatus),
        )
        _require_version(self.version)
        object.__setattr__(
            self,
            "disposition",
            _require_enum("disposition", self.disposition, EmailEventDisposition),
        )
        _require_bool(
            "may_complete_without_processing",
            self.may_complete_without_processing,
        )
        if self.persisted_status is not self.decision.new_status:
            raise ValueError("persisted_status must equal decision.new_status")
        if not any(
            row.disposition is self.disposition
            and row.reason is self.decision.reason
            and self.persisted_status in row.persisted_statuses
            and row.should_process is self.decision.should_process
            and row.should_cancel is self.decision.should_cancel
            and (
                row.cancel_pending_side_effects
                is self.decision.cancel_pending_side_effects
            )
            and row.create_seen is self.decision.create_seen
            and (
                row.may_complete_without_processing
                is self.may_complete_without_processing
            )
            for row in _EMAIL_EVENT_APPLICATION_MANIFEST
        ):
            raise ValueError("email event application manifest invariant failed")

    @property
    def should_process(self) -> bool:
        return self.decision.should_process

    @property
    def should_cancel(self) -> bool:
        return self.decision.should_cancel

    @property
    def cancel_pending_side_effects(self) -> bool:
        return self.decision.cancel_pending_side_effects


def _decision(
    *,
    should_process: bool,
    should_cancel: bool,
    new_status: EmailStatus,
    cancel_pending_side_effects: bool,
    create_seen: bool,
    reason: EmailEventReason,
) -> EmailEventDecision:
    return EmailEventDecision(
        should_process=should_process,
        should_cancel=should_cancel,
        new_status=new_status,
        cancel_pending_side_effects=cancel_pending_side_effects,
        create_seen=create_seen,
        reason=reason,
    )


def decide_email_event(
    *,
    current_status: EmailStatus | str | None,
    create_seen: bool,
    kind: ChangeKind | str,
    source_is_read: bool | None,
    processing_owner_matches: bool = False,
    external_effects_started: bool = False,
    source_deleted: bool = False,
) -> EmailEventDecision:
    """Return the monotonic authority decision without performing I/O."""

    status = (
        None
        if current_status is None
        else _require_enum("current_status", current_status, EmailStatus)
    )
    _require_bool("create_seen", create_seen)
    event_kind = _require_enum("kind", kind, ChangeKind)
    if source_is_read is not None:
        _require_bool("source_is_read", source_is_read)
    _require_bool("processing_owner_matches", processing_owner_matches)
    _require_bool("external_effects_started", external_effects_started)
    _require_bool("source_deleted", source_deleted)

    if status is None:
        if event_kind is ChangeKind.CREATE:
            return _decision(
                should_process=True,
                should_cancel=False,
                new_status=EmailStatus.PROCESSING,
                cancel_pending_side_effects=False,
                create_seen=True,
                reason=EmailEventReason.FIRST_CREATE,
            )
        if event_kind in {ChangeKind.UPDATE, ChangeKind.READ}:
            return _decision(
                should_process=False,
                should_cancel=False,
                new_status=EmailStatus.INGESTED,
                cancel_pending_side_effects=False,
                create_seen=False,
                reason=EmailEventReason.METADATA_EVENT,
            )
        return _decision(
            should_process=False,
            should_cancel=True,
            new_status=EmailStatus.CANCELLED,
            cancel_pending_side_effects=False,
            create_seen=False,
            reason=EmailEventReason.SOURCE_TOMBSTONE,
        )

    if source_deleted:
        return _decision(
            should_process=False,
            should_cancel=False,
            new_status=status,
            cancel_pending_side_effects=False,
            create_seen=create_seen,
            reason=EmailEventReason.SOURCE_DELETED_PRESERVED,
        )

    if event_kind is ChangeKind.CREATE:
        if status is EmailStatus.INGESTED and not create_seen:
            return _decision(
                should_process=True,
                should_cancel=False,
                new_status=EmailStatus.PROCESSING,
                cancel_pending_side_effects=False,
                create_seen=True,
                reason=EmailEventReason.FIRST_CREATE,
            )
        rule = EMAIL_STATUS_TRANSITIONS[status]
        if (
            rule.may_resume_processing
            and create_seen
            and processing_owner_matches
            and not external_effects_started
        ):
            return _decision(
                should_process=True,
                should_cancel=False,
                new_status=EmailStatus.PROCESSING,
                cancel_pending_side_effects=False,
                create_seen=True,
                reason=EmailEventReason.PROCESSING_RESUMED,
            )
        return _decision(
            should_process=False,
            should_cancel=False,
            new_status=status,
            cancel_pending_side_effects=False,
            create_seen=create_seen,
            reason=EmailEventReason.DUPLICATE_CREATE,
        )

    if event_kind in {ChangeKind.UPDATE, ChangeKind.READ}:
        return _decision(
            should_process=False,
            should_cancel=False,
            new_status=status,
            cancel_pending_side_effects=False,
            create_seen=create_seen,
            reason=EmailEventReason.METADATA_EVENT,
        )

    if external_effects_started:
        return _decision(
            should_process=False,
            should_cancel=False,
            new_status=status,
            cancel_pending_side_effects=False,
            create_seen=create_seen,
            reason=EmailEventReason.SOURCE_DELETE_RECORDED,
        )

    if EMAIL_STATUS_TRANSITIONS[status].cancel_on_delete:
        return _decision(
            should_process=False,
            should_cancel=True,
            new_status=EmailStatus.CANCELLED,
            cancel_pending_side_effects=not (
                status is EmailStatus.INGESTED and not create_seen
            ),
            create_seen=create_seen,
            reason=EmailEventReason.SOURCE_DELETE_CANCELLED,
        )

    return _decision(
        should_process=False,
        should_cancel=False,
        new_status=status,
        cancel_pending_side_effects=False,
        create_seen=create_seen,
        reason=EmailEventReason.SOURCE_DELETE_RECORDED,
    )


__all__ = [
    "EMAIL_STATUS_TRANSITIONS",
    "EmailEventApplication",
    "EmailEventDecision",
    "EmailEventDisposition",
    "EmailEventReason",
    "EmailStatus",
    "decide_email_event",
]
