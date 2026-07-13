"""Immutable value objects shared by durable-ingestion components.

These objects cross intake, repository, worker, and reconciliation boundaries.
They therefore validate their public inputs and copy mutable containers before
storing them.  No database or network I/O belongs in this module.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, TypeVar
from uuid import UUID

from src.domain.email_state import PipelineGenerationState


MAX_INBOX_PAYLOAD_BYTES: Final = 256 * 1024

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_ERROR_CODE_PATTERN = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}\Z")


class ChangeKind(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    READ = "read"
    DELETE = "delete"


class IngressSource(StrEnum):
    WEBHOOK = "webhook"
    SYNC = "sync"
    BACKFILL = "backfill"


class ProcessingPolicy(StrEnum):
    FULL = "full"
    ARCHIVE = "archive"
    METADATA_ONLY = "metadata_only"
    HISTORICAL_SUPPRESSED = "historical_suppressed"


class InboxStatus(StrEnum):
    PENDING = "pending"
    RETRY_WAIT = "retry_wait"
    LEASED = "leased"
    COMPLETED = "completed"
    DEAD_LETTER = "dead_letter"
    MANUAL_REVIEW = "manual_review"


class InboxDispositionStatus(StrEnum):
    RETRY_WAIT = "retry_wait"
    DEAD_LETTER = "dead_letter"


class SyncCursorStatus(StrEnum):
    ACTIVE = "active"
    RESET_REQUIRED = "reset_required"
    COLD_START_PENDING = "cold_start_pending"
    BLOCKED_CONTRACT = "blocked_contract"


_EnumT = TypeVar("_EnumT", bound=StrEnum)


def _require_enum(name: str, value: object, enum_type: type[_EnumT]) -> _EnumT:
    if not isinstance(value, (str, enum_type)):
        raise ValueError(f"{name} must be a valid {enum_type.__name__}")
    try:
        return enum_type(value)
    except ValueError:
        raise ValueError(f"{name} must be a valid {enum_type.__name__}") from None


def _require_int(name: str, value: object, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _require_bool(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _require_text(name: str, value: object, *, max_length: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise ValueError(f"{name} must be a non-empty string <= {max_length} chars")
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise ValueError(f"{name} contains forbidden control characters")
    return value


def _require_optional_text(
    name: str,
    value: object,
    *,
    max_length: int,
) -> str | None:
    if value is None:
        return None
    return _require_text(name, value, max_length=max_length)


def _require_sha256(name: str, value: object) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _require_uuid(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a UUID string")
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError):
        raise ValueError(f"{name} must be a UUID string") from None
    return str(parsed)


def _as_utc(name: str, value: object) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include timezone information")
    return value.astimezone(UTC)


def _as_optional_utc(name: str, value: object) -> datetime | None:
    if value is None:
        return None
    return _as_utc(name, value)


def _freeze_json_value(
    name: str,
    value: object,
    *,
    active_container_ids: set[int],
) -> tuple[object, object]:
    """Return an immutable value and a plain value for canonical sizing."""

    if value is None or isinstance(value, (bool, str, int)):
        return value, value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{name} must not contain non-finite numbers")
        return value, value

    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active_container_ids:
            raise ValueError(f"{name} must not contain a container cycle")
        active_container_ids.add(identity)
        try:
            frozen: dict[str, object] = {}
            plain: dict[str, object] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError(f"{name} must contain only string object keys")
                frozen_item, plain_item = _freeze_json_value(
                    name,
                    item,
                    active_container_ids=active_container_ids,
                )
                frozen[key] = frozen_item
                plain[key] = plain_item
            return MappingProxyType(frozen), plain
        finally:
            active_container_ids.remove(identity)

    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in active_container_ids:
            raise ValueError(f"{name} must not contain a container cycle")
        active_container_ids.add(identity)
        try:
            pairs = tuple(
                _freeze_json_value(
                    name,
                    item,
                    active_container_ids=active_container_ids,
                )
                for item in value
            )
            return tuple(frozen for frozen, _ in pairs), [plain for _, plain in pairs]
        finally:
            active_container_ids.remove(identity)

    raise ValueError(f"{name} must contain JSON-compatible values")


def _freeze_json_object(
    name: str,
    value: object,
    *,
    max_bytes: int = MAX_INBOX_PAYLOAD_BYTES,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    frozen, plain = _freeze_json_value(
        name,
        value,
        active_container_ids=set(),
    )
    try:
        encoded = json.dumps(
            plain,
            ensure_ascii=False,
            sort_keys=True,
            # Match PostgreSQL jsonb::text separator width so an object that
            # passes the DTO byte gate also fits the database CHECK boundary.
            separators=(", ", ": "),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError(f"{name} must contain valid UTF-8 JSON") from None
    if len(encoded) > max_bytes:
        raise ValueError(f"{name} exceeds the {max_bytes}-byte limit")
    if not isinstance(frozen, Mapping):  # Defensive assertion for type checkers.
        raise ValueError(f"{name} must be a JSON object")
    return frozen


@dataclass(frozen=True, slots=True)
class NormalizedIngressEvent:
    account_id: int
    source: IngressSource
    raw_event_type: str
    kind: ChangeKind
    external_email_id: str
    folder: str
    source_version: str | None
    dedupe_key: str
    payload: Mapping[str, Any]
    source_event_at: datetime | None = None
    processing_policy: ProcessingPolicy = ProcessingPolicy.FULL

    def __post_init__(self) -> None:
        _require_int("account_id", self.account_id, minimum=1)
        object.__setattr__(
            self,
            "source",
            _require_enum("source", self.source, IngressSource),
        )
        _require_text("raw_event_type", self.raw_event_type, max_length=128)
        object.__setattr__(
            self,
            "kind",
            _require_enum("kind", self.kind, ChangeKind),
        )
        _require_text(
            "external_email_id",
            self.external_email_id,
            max_length=1024,
        )
        _require_text("folder", self.folder, max_length=512)
        _require_optional_text(
            "source_version",
            self.source_version,
            max_length=512,
        )
        _require_sha256("dedupe_key", self.dedupe_key)
        object.__setattr__(
            self,
            "payload",
            _freeze_json_object("payload", self.payload),
        )
        object.__setattr__(
            self,
            "source_event_at",
            _as_optional_utc("source_event_at", self.source_event_at),
        )
        object.__setattr__(
            self,
            "processing_policy",
            _require_enum(
                "processing_policy",
                self.processing_policy,
                ProcessingPolicy,
            ),
        )


@dataclass(frozen=True, slots=True)
class PipelineGeneration:
    account_id: int
    generation: int
    pipeline_name: str
    state: PipelineGenerationState
    fencing_token: int

    def __post_init__(self) -> None:
        _require_int("account_id", self.account_id, minimum=1)
        _require_int("generation", self.generation, minimum=1)
        _require_text("pipeline_name", self.pipeline_name, max_length=64)
        object.__setattr__(
            self,
            "state",
            _require_enum("state", self.state, PipelineGenerationState),
        )
        _require_int("fencing_token", self.fencing_token, minimum=1)


@dataclass(frozen=True, slots=True)
class SyncChange:
    kind: ChangeKind
    external_email_id: str
    item: Mapping[str, Any] | None
    source_version: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "kind",
            _require_enum("kind", self.kind, ChangeKind),
        )
        _require_text(
            "external_email_id",
            self.external_email_id,
            max_length=1024,
        )
        if self.item is not None:
            object.__setattr__(
                self,
                "item",
                _freeze_json_object("item", self.item),
            )
        _require_optional_text(
            "source_version",
            self.source_version,
            max_length=512,
        )


@dataclass(frozen=True, slots=True)
class SyncBatch:
    cursor: str
    changes: Sequence[SyncChange]
    is_full: bool

    def __post_init__(self) -> None:
        _require_text("cursor", self.cursor, max_length=8192)
        if isinstance(self.changes, (str, bytes, bytearray)):
            raise ValueError("changes must be a sequence of SyncChange")
        try:
            changes = tuple(self.changes)
        except TypeError:
            raise ValueError("changes must be a sequence of SyncChange") from None
        if any(not isinstance(change, SyncChange) for change in changes):
            raise ValueError("changes must contain only SyncChange values")
        object.__setattr__(self, "changes", changes)
        _require_bool("is_full", self.is_full)


@dataclass(frozen=True, slots=True)
class InboxLease:
    id: str
    account_id: int
    generation: int
    fencing_token: int
    lease_owner: str
    attempts: int
    event: NormalizedIngressEvent
    received_at: datetime
    lease_until: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _require_uuid("id", self.id))
        _require_int("account_id", self.account_id, minimum=1)
        _require_int("generation", self.generation, minimum=1)
        _require_int("fencing_token", self.fencing_token, minimum=1)
        _require_text("lease_owner", self.lease_owner, max_length=128)
        _require_int("attempts", self.attempts, minimum=0)
        if not isinstance(self.event, NormalizedIngressEvent):
            raise ValueError("event must be a NormalizedIngressEvent")
        if self.event.account_id != self.account_id:
            raise ValueError("lease account_id must match event account_id")
        received_at = _as_utc("received_at", self.received_at)
        lease_until = _as_utc("lease_until", self.lease_until)
        if lease_until <= received_at:
            raise ValueError("lease_until must be later than received_at")
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "lease_until", lease_until)


@dataclass(frozen=True, slots=True)
class InboxStats:
    pending: int
    retry_wait: int
    leased: int
    dead_letter: int
    oldest_pending_seconds: float

    def __post_init__(self) -> None:
        for name in ("pending", "retry_wait", "leased", "dead_letter"):
            _require_int(name, getattr(self, name), minimum=0)
        value = self.oldest_pending_seconds
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("oldest_pending_seconds must be finite and nonnegative")
        try:
            normalized = float(value)
        except OverflowError:
            raise ValueError(
                "oldest_pending_seconds must be finite and nonnegative"
            ) from None
        if not math.isfinite(normalized) or normalized < 0:
            raise ValueError("oldest_pending_seconds must be finite and nonnegative")
        object.__setattr__(self, "oldest_pending_seconds", normalized)


@dataclass(frozen=True, slots=True)
class InboxDisposition:
    status: InboxDispositionStatus
    attempts: int
    available_at: datetime | None
    safe_error_code: str

    def __post_init__(self) -> None:
        status = _require_enum(
            "status",
            self.status,
            InboxDispositionStatus,
        )
        object.__setattr__(self, "status", status)
        _require_int("attempts", self.attempts, minimum=0)
        available_at = _as_optional_utc("available_at", self.available_at)
        if status is InboxDispositionStatus.RETRY_WAIT and available_at is None:
            raise ValueError("retry_wait requires available_at")
        if status is InboxDispositionStatus.DEAD_LETTER and available_at is not None:
            raise ValueError("dead_letter must not retain available_at")
        object.__setattr__(self, "available_at", available_at)
        if (
            not isinstance(self.safe_error_code, str)
            or _SAFE_ERROR_CODE_PATTERN.fullmatch(self.safe_error_code) is None
        ):
            raise ValueError("safe_error_code must be a safe bounded error token")


@dataclass(frozen=True, slots=True)
class IngressReceipt:
    inbox_id: str
    duplicate: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "inbox_id",
            _require_uuid("inbox_id", self.inbox_id),
        )
        _require_bool("duplicate", self.duplicate)


__all__ = [
    "ChangeKind",
    "InboxDisposition",
    "InboxDispositionStatus",
    "InboxLease",
    "InboxStats",
    "InboxStatus",
    "IngressReceipt",
    "IngressSource",
    "MAX_INBOX_PAYLOAD_BYTES",
    "NormalizedIngressEvent",
    "PipelineGeneration",
    "PipelineGenerationState",
    "ProcessingPolicy",
    "SyncBatch",
    "SyncChange",
    "SyncCursorStatus",
]
