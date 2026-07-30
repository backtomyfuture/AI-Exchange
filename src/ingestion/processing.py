"""Pure processing DTOs, effect authorization ports and stamped adapter routing."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Protocol, TypeAlias, TypeVar, runtime_checkable
from uuid import UUID

from src.domain.email_state import PipelineGenerationState, ProcessingOutcome
from src.domain.errors import ErrorKind
from src.ingestion.email_events import EmailEventApplication, EmailStatus
from src.ingestion.models import (
    InboxLease,
    InboxStatus,
    POSTGRES_BIGINT_MAX,
    PipelineGeneration,
)
from src.ingestion.runtime_authority import GREENFIELD_PIPELINE_NAME


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EXECUTABLE_STATES: Final = frozenset(
    {
        PipelineGenerationState.CURRENT_INGRESS,
        PipelineGenerationState.QUIESCING,
        PipelineGenerationState.DRAINING,
    }
)
_SUCCESS_PAIRS: Final = frozenset(
    {
        (EmailStatus.WAITING_APPROVAL, ProcessingOutcome.PROCESSED),
        (EmailStatus.NOTIFIED_READONLY, ProcessingOutcome.PROCESSED),
        (EmailStatus.NO_ACTION, ProcessingOutcome.PROCESSED),
        (EmailStatus.ARCHIVED, ProcessingOutcome.ARCHIVED),
        (EmailStatus.MANUAL_REVIEW, ProcessingOutcome.MANUAL_REVIEW),
    }
)
_FINISH_PAIRS: Final = frozenset(
    {
        (EmailStatus.WAITING_APPROVAL, InboxStatus.COMPLETED),
        (EmailStatus.NOTIFIED_READONLY, InboxStatus.COMPLETED),
        (EmailStatus.NO_ACTION, InboxStatus.COMPLETED),
        (EmailStatus.ARCHIVED, InboxStatus.COMPLETED),
        (EmailStatus.RETRY_WAIT, InboxStatus.RETRY_WAIT),
        (EmailStatus.MANUAL_REVIEW, InboxStatus.MANUAL_REVIEW),
        (EmailStatus.DEAD_LETTER, InboxStatus.DEAD_LETTER),
    }
)


class ExternalEffectKind(StrEnum):
    DETAIL = "detail"
    CONTENT = "content"
    MODEL = "model"
    FEISHU = "feishu"
    EXCHANGE_MUTATION = "exchange_mutation"
    QDRANT = "qdrant"


class ProcessingAdapterUnavailable(RuntimeError):
    """A stamp has no exact executable adapter in the immutable registry."""

    kind = ErrorKind.INTERNAL_INVARIANT
    safe_code = "processing.adapter_unavailable"
    safe_summary = "Processing adapter is unavailable"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"ProcessingAdapterUnavailable(safe_code={self.safe_code!r})"


class ProcessingPolicyRejected(RuntimeError):
    """The selected path attempted work outside its exact policy ceiling."""

    kind = ErrorKind.POLICY_REJECTED
    safe_code = "processing.policy_rejected"
    safe_summary = "Processing policy rejected the operation"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"ProcessingPolicyRejected(safe_code={self.safe_code!r})"


class ExternalEffectAuthorizationError(RuntimeError):
    """The latest fenced lease could not authorize an external operation."""

    kind = ErrorKind.INTERNAL_INVARIANT
    safe_code = "processing.effect_not_authorized"
    safe_summary = "External effect is not authorized"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"ExternalEffectAuthorizationError(safe_code={self.safe_code!r})"


class GuardedExternalEffectFailed(RuntimeError):
    """An authorized external call failed or returned an unsafe outcome."""

    kind = ErrorKind.SEND_UNKNOWN
    safe_code = "processing.external_effect_failed"
    safe_summary = "External effect completion is uncertain"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"GuardedExternalEffectFailed(safe_code={self.safe_code!r})"


class ReplaySafeExternalEffectFailed(RuntimeError):
    """An authorized, replay-safe external call failed before later effects."""

    kind = ErrorKind.TRANSIENT_DEPENDENCY
    safe_code = "processing.replay_safe_effect_failed"
    safe_summary = "Replay-safe external effect failed"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"ReplaySafeExternalEffectFailed(safe_code={self.safe_code!r})"


class ProcessingCompletionRejected(RuntimeError):
    """A completion did not match the currently fenced processing attempt."""

    kind = ErrorKind.INTERNAL_INVARIANT
    safe_code = "processing.completion_rejected"
    safe_summary = "Processing completion was rejected"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"ProcessingCompletionRejected(safe_code={self.safe_code!r})"


class ProcessingReceiptConflict(RuntimeError):
    """A replay attempted to replace an immutable processing receipt."""

    kind = ErrorKind.INTERNAL_INVARIANT
    safe_code = "processing.receipt_conflict"
    safe_summary = "Processing receipt conflicts with completion"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"ProcessingReceiptConflict(safe_code={self.safe_code!r})"


_EnumT = TypeVar("_EnumT", bound=StrEnum)


def _require_enum(name: str, value: object, enum_type: type[_EnumT]) -> _EnumT:
    if not isinstance(value, (str, enum_type)):
        raise ValueError(f"{name} must be a valid {enum_type.__name__}")
    try:
        return enum_type(value)
    except ValueError:
        raise ValueError(f"{name} must be a valid {enum_type.__name__}") from None


def _require_bigint(name: str, value: object, *, minimum: int = 0) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > POSTGRES_BIGINT_MAX
    ):
        raise ValueError(f"{name} must be a bounded PostgreSQL BIGINT")
    return value


def _require_uuid(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a UUID string")
    try:
        return str(UUID(value))
    except (AttributeError, ValueError):
        raise ValueError(f"{name} must be a UUID string") from None


def _require_text(name: str, value: object, *, max_length: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > max_length
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise ValueError(f"{name} must be exact bounded text")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain valid UTF-8 text") from None
    return value


def _is_async_callable(value: object) -> bool:
    if not callable(value):
        return False
    return inspect.iscoroutinefunction(value) or inspect.iscoroutinefunction(
        getattr(value, "__call__", None)
    )


@dataclass(frozen=True, slots=True)
class ProcessingCompletion:
    target_status: EmailStatus
    legacy_outcome: ProcessingOutcome
    safe_error_code: str | None = None
    safe_error_summary: str | None = None

    def __post_init__(self) -> None:
        if type(self.target_status) is not EmailStatus:
            raise ValueError("target_status must be an exact EmailStatus")
        if type(self.legacy_outcome) is not ProcessingOutcome:
            raise ValueError("legacy_outcome must be an exact ProcessingOutcome")
        target = self.target_status
        outcome = self.legacy_outcome
        if (target, outcome) not in _SUCCESS_PAIRS:
            raise ValueError("processing completion has an invalid legacy mapping")
        if target is EmailStatus.MANUAL_REVIEW:
            object.__setattr__(
                self,
                "safe_error_code",
                _require_text(
                    "safe_error_code",
                    self.safe_error_code,
                    max_length=128,
                ),
            )
            object.__setattr__(
                self,
                "safe_error_summary",
                _require_text(
                    "safe_error_summary",
                    self.safe_error_summary,
                    max_length=256,
                ),
            )
        elif self.safe_error_code is not None or self.safe_error_summary is not None:
            raise ValueError("successful processing completion cannot contain an error")

    @classmethod
    def waiting_approval(cls) -> ProcessingCompletion:
        return cls(EmailStatus.WAITING_APPROVAL, ProcessingOutcome.PROCESSED)

    @classmethod
    def notified_readonly(cls) -> ProcessingCompletion:
        return cls(EmailStatus.NOTIFIED_READONLY, ProcessingOutcome.PROCESSED)

    @classmethod
    def no_action(cls) -> ProcessingCompletion:
        return cls(EmailStatus.NO_ACTION, ProcessingOutcome.PROCESSED)

    @classmethod
    def archived(cls) -> ProcessingCompletion:
        return cls(EmailStatus.ARCHIVED, ProcessingOutcome.ARCHIVED)

    @classmethod
    def manual_review(cls) -> ProcessingCompletion:
        return cls(
            EmailStatus.MANUAL_REVIEW,
            ProcessingOutcome.MANUAL_REVIEW,
            safe_error_code="processing.manual_review",
            safe_error_summary="Processing requires manual review",
        )


@dataclass(frozen=True, slots=True)
class ProcessingFinishResult:
    email_status: EmailStatus
    inbox_status: InboxStatus
    replayed: bool

    def __post_init__(self) -> None:
        if type(self.email_status) is not EmailStatus:
            raise ValueError("email_status must be an exact EmailStatus")
        if type(self.inbox_status) is not InboxStatus:
            raise ValueError("inbox_status must be an exact InboxStatus")
        email_status = self.email_status
        inbox_status = self.inbox_status
        if not isinstance(self.replayed, bool):
            raise ValueError("replayed must be a boolean")
        if (email_status, inbox_status) not in _FINISH_PAIRS:
            raise ValueError(
                "processing finish result must represent one atomic pairing"
            )
        object.__setattr__(self, "email_status", email_status)
        object.__setattr__(self, "inbox_status", inbox_status)


@dataclass(frozen=True, slots=True)
class LegacyEffectScope:
    account_id: int
    inbox_id: str
    generation: int
    fencing_token: int
    attempts: int
    email_id: str
    expected_email_version: int
    event_dedupe_key: str
    external_email_id: str

    def __post_init__(self) -> None:
        _require_bigint("account_id", self.account_id, minimum=1)
        object.__setattr__(self, "inbox_id", _require_uuid("inbox_id", self.inbox_id))
        _require_bigint("generation", self.generation, minimum=1)
        _require_bigint("fencing_token", self.fencing_token, minimum=1)
        _require_bigint("attempts", self.attempts)
        object.__setattr__(self, "email_id", _require_uuid("email_id", self.email_id))
        _require_bigint("expected_email_version", self.expected_email_version)
        if (
            not isinstance(self.event_dedupe_key, str)
            or _SHA256.fullmatch(self.event_dedupe_key) is None
        ):
            raise ValueError("event_dedupe_key must be a lowercase SHA-256 digest")
        _require_text(
            "external_email_id",
            self.external_email_id,
            max_length=1024,
        )

    @classmethod
    def from_processing(
        cls,
        lease: InboxLease,
        application: EmailEventApplication,
    ) -> LegacyEffectScope:
        if not isinstance(lease, InboxLease):
            raise ValueError("lease must be an InboxLease")
        if not isinstance(application, EmailEventApplication):
            raise ValueError("application must be an EmailEventApplication")
        return cls(
            account_id=lease.account_id,
            inbox_id=lease.id,
            generation=lease.generation,
            fencing_token=lease.fencing_token,
            attempts=lease.attempts,
            email_id=application.email_id,
            expected_email_version=application.version,
            event_dedupe_key=lease.event.dedupe_key,
            external_email_id=lease.event.external_email_id,
        )

    def target_hash(
        self,
        kind: ExternalEffectKind | str,
        ordinal: int,
        target: object,
    ) -> str:
        resolved_kind = _require_enum("kind", kind, ExternalEffectKind)
        _require_bigint("ordinal", ordinal)
        canonical = {
            "schema_version": 1,
            "account_id": self.account_id,
            "inbox_id": self.inbox_id,
            "generation": self.generation,
            "fencing_token": self.fencing_token,
            "attempts": self.attempts,
            "email_id": self.email_id,
            "expected_email_version": self.expected_email_version,
            "event_dedupe_key": self.event_dedupe_key,
            "external_email_id": self.external_email_id,
            "kind": resolved_kind.value,
            "ordinal": ordinal,
            "target": target,
        }
        try:
            encoded = json.dumps(
                canonical,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise ValueError("effect target must be canonical JSON") from None
        return hashlib.sha256(encoded).hexdigest()


BeforeExternalEffect: TypeAlias = Callable[[str, int, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ExternalEffectBoundary:
    scope: LegacyEffectScope
    callback: BeforeExternalEffect

    def __post_init__(self) -> None:
        if not isinstance(self.scope, LegacyEffectScope):
            raise ValueError("scope must be a LegacyEffectScope")
        if not _is_async_callable(self.callback):
            raise ValueError("before_external_effect must be an async callable")

    async def before(
        self,
        kind: ExternalEffectKind | str,
        ordinal: int,
        target: object,
    ) -> str:
        resolved_kind = _require_enum("kind", kind, ExternalEffectKind)
        target_hash = self.scope.target_hash(resolved_kind, ordinal, target)
        authorized = await self.callback(resolved_kind.value, ordinal, target_hash)
        if authorized is not None:
            raise ExternalEffectAuthorizationError()
        return target_hash


@runtime_checkable
class ProcessingAdapter(Protocol):
    pipeline_name: str

    async def process(
        self,
        lease: InboxLease,
        application: EmailEventApplication,
        *,
        before_external_effect: BeforeExternalEffect,
    ) -> ProcessingCompletion: ...


class ProcessingAdapterRouter:
    """Pure exact-stamp selector over the sole production pipeline adapter."""

    __slots__ = ("_registry",)

    def __init__(self, registry: Mapping[str, ProcessingAdapter]) -> None:
        if not isinstance(registry, Mapping):
            raise ValueError("registry must be a mapping")
        copied = dict(registry)
        if frozenset(copied) != frozenset({GREENFIELD_PIPELINE_NAME}):
            raise ValueError(
                f"registry must contain only {GREENFIELD_PIPELINE_NAME}"
            )
        adapter = copied[GREENFIELD_PIPELINE_NAME]
        if getattr(
            adapter, "pipeline_name", None
        ) != GREENFIELD_PIPELINE_NAME or not _is_async_callable(
            getattr(adapter, "process", None)
        ):
            raise ValueError(f"{GREENFIELD_PIPELINE_NAME} adapter is invalid")
        self._registry = MappingProxyType(copied)

    @property
    def registry(self) -> Mapping[str, ProcessingAdapter]:
        return self._registry

    def select(
        self,
        stamped_lease: InboxLease,
        authority: PipelineGeneration,
    ) -> ProcessingAdapter:
        if (
            type(stamped_lease) is not InboxLease
            or type(authority) is not PipelineGeneration
        ):
            raise ProcessingAdapterUnavailable()
        exact = (
            authority.account_id == stamped_lease.account_id
            and authority.generation == stamped_lease.generation
            and authority.pipeline_name == stamped_lease.pipeline_name
            and authority.fencing_token == stamped_lease.fencing_token
            and authority.state in _EXECUTABLE_STATES
        )
        if not exact or stamped_lease.pipeline_name != GREENFIELD_PIPELINE_NAME:
            raise ProcessingAdapterUnavailable()
        try:
            return self._registry[stamped_lease.pipeline_name]
        except KeyError:
            raise ProcessingAdapterUnavailable() from None


__all__ = [
    "BeforeExternalEffect",
    "ExternalEffectAuthorizationError",
    "ExternalEffectBoundary",
    "ExternalEffectKind",
    "GuardedExternalEffectFailed",
    "LegacyEffectScope",
    "ProcessingAdapter",
    "ProcessingAdapterRouter",
    "ProcessingAdapterUnavailable",
    "ProcessingCompletion",
    "ProcessingCompletionRejected",
    "ProcessingFinishResult",
    "ProcessingPolicyRejected",
    "ProcessingReceiptConflict",
    "ReplaySafeExternalEffectFailed",
]
