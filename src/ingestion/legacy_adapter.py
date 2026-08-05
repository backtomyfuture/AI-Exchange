"""Fail-closed bridge from durable Inbox attempts to the legacy processor."""

from __future__ import annotations

import inspect
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final

from src.domain.email_state import ProcessingOutcome
from src.domain.errors import (
    ErrorKind,
    ExchangeDetailTransientError,
    ManualReviewRequired,
)
from src.exchange_service import process_and_archive_email_guarded
from src.ingestion.email_events import (
    EmailEventApplication,
    EmailEventDisposition,
    EmailStatus,
)
from src.ingestion.models import InboxLease, ProcessingPolicy
from src.ingestion.models import POSTGRES_BIGINT_MAX
from src.ingestion.processing import (
    BeforeExternalEffect,
    ExternalEffectAuthorizationError,
    ExternalEffectBoundary,
    ExternalEffectKind,
    LegacyEffectScope,
    ProcessingCompletion,
    ProcessingPolicyRejected,
    ReplaySafeExternalEffectFailed,
)
from src.ingestion.runtime_authority import GREENFIELD_PIPELINE_NAME


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FULL_EFFECTS: Final = frozenset(ExternalEffectKind)
_ARCHIVE_EFFECTS: Final = frozenset(
    {
        ExternalEffectKind.DETAIL,
        ExternalEffectKind.CONTENT,
        ExternalEffectKind.QDRANT,
    }
)
_EXECUTABLE_DISPOSITIONS: Final = frozenset(
    {
        EmailEventDisposition.CREATOR_ELECTED,
        EmailEventDisposition.PROCESSING_RESUMED,
    }
)
logger = logging.getLogger(__name__)


def _log_stage_failure(stage: str, error: BaseException) -> None:
    """Log only fixed stage metadata and an exception class, never its value."""

    logger.error(
        "Legacy processing stage failed: stage=%s error_type=%s",
        stage,
        type(error).__name__,
    )


class LegacyProcessingFailed(RuntimeError):
    """Fixed, privacy-safe failure emitted by the compatibility adapter."""

    kind = ErrorKind.TRANSIENT_DEPENDENCY
    safe_code = "legacy.processing_failed"
    safe_summary = "Legacy email processing failed"

    def __init__(self) -> None:
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"LegacyProcessingFailed(safe_code={self.safe_code!r})"


def _is_async_callable(value: object) -> bool:
    if not callable(value):
        return False
    return inspect.iscoroutinefunction(value) or inspect.iscoroutinefunction(
        getattr(value, "__call__", None)
    )


class _PolicyBoundEffectPort:
    __slots__ = ("_allowed", "_delegate")

    def __init__(
        self,
        delegate: BeforeExternalEffect,
        allowed: frozenset[ExternalEffectKind],
    ) -> None:
        if not _is_async_callable(delegate):
            raise ValueError("before_external_effect must be an async callable")
        if type(allowed) is not frozenset or not allowed:
            raise ValueError("effect policy must be an exact non-empty frozenset")
        self._delegate = delegate
        self._allowed = allowed

    async def __call__(self, kind: str, ordinal: int, target_hash: str) -> None:
        if type(kind) is not str:
            raise ProcessingPolicyRejected()
        try:
            effect_kind = ExternalEffectKind(kind)
        except ValueError:
            raise ProcessingPolicyRejected() from None
        if effect_kind not in self._allowed:
            raise ProcessingPolicyRejected()
        if (
            isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
            or ordinal < 0
            or type(target_hash) is not str
            or _SHA256.fullmatch(target_hash) is None
        ):
            raise ProcessingPolicyRejected()
        authorized = await self._delegate(effect_kind.value, ordinal, target_hash)
        if authorized is not None:
            raise ExternalEffectAuthorizationError()


GuardedProcessor = Callable[..., Awaitable[ProcessingOutcome]]


class LegacyProcessingAdapter:
    """Existing processor bridge selected only by the production pipeline stamp."""

    pipeline_name = GREENFIELD_PIPELINE_NAME

    __slots__ = ("_ctx", "_guarded_processor", "_legacy_account_id")

    def __init__(
        self,
        ctx: Any,
        *,
        legacy_account_id: int,
        guarded_processor: GuardedProcessor = process_and_archive_email_guarded,
    ) -> None:
        if ctx is None:
            raise ValueError("ctx is required")
        if (
            type(legacy_account_id) is not int
            or legacy_account_id <= 0
            or legacy_account_id > POSTGRES_BIGINT_MAX
        ):
            raise ValueError("legacy_account_id must be a positive PostgreSQL BIGINT")
        if not _is_async_callable(guarded_processor):
            raise ValueError("guarded_processor must be an async callable")
        object.__setattr__(self, "_ctx", ctx)
        object.__setattr__(self, "_legacy_account_id", legacy_account_id)
        object.__setattr__(self, "_guarded_processor", guarded_processor)

    def __setattr__(self, _name: str, _value: object) -> None:
        raise AttributeError("LegacyProcessingAdapter is immutable")

    def __delattr__(self, _name: str) -> None:
        raise AttributeError("LegacyProcessingAdapter is immutable")

    @property
    def legacy_account_id(self) -> int:
        return self._legacy_account_id

    async def process(
        self,
        lease: InboxLease,
        application: EmailEventApplication,
        *,
        before_external_effect: BeforeExternalEffect,
    ) -> ProcessingCompletion:
        policy = self._validate_attempt(
            lease,
            application,
            legacy_account_id=self._legacy_account_id,
        )
        allowed = self._allowed_effects(policy)
        scope = LegacyEffectScope.from_processing(lease, application)

        # Constructing the boundary validates the injected port before any I/O.
        ExternalEffectBoundary(scope, before_external_effect)
        policy_port = _PolicyBoundEffectPort(before_external_effect, allowed)
        detail_boundary = ExternalEffectBoundary(scope, policy_port)
        try:
            await detail_boundary.before(
                ExternalEffectKind.DETAIL,
                0,
                {
                    "operation": "get_email",
                    "external_email_id": lease.event.external_email_id,
                    "source_version": lease.event.source_version,
                },
            )
        except Exception as error:
            _log_stage_failure("detail_authorization", error)
            raise

        try:
            details = await self._ctx.exchange_client.get_email(
                lease.event.external_email_id
            )
        except ExchangeDetailTransientError as error:
            _log_stage_failure("detail_fetch", error)
            raise ReplaySafeExternalEffectFailed(
                retry_after_seconds=error.retry_after_seconds
            ) from None
        except Exception as error:
            _log_stage_failure("detail_fetch", error)
            raise ReplaySafeExternalEffectFailed() from None
        if details is None:
            error = ReplaySafeExternalEffectFailed()
            _log_stage_failure("detail_fetch", error)
            raise error

        try:
            email_data = self._project_detail(lease, details)
        except Exception as error:
            _log_stage_failure("detail_projection", error)
            raise

        try:
            outcome = await self._guarded_processor(
                email_data,
                self._ctx,
                skip_analysis=policy is ProcessingPolicy.ARCHIVE,
                force_reprocess=(
                    application.disposition
                    is EmailEventDisposition.PROCESSING_RESUMED
                ),
                before_external_effect=policy_port,
                effect_scope=scope,
            )
        except Exception as error:
            _log_stage_failure("guarded_processing", error)
            raise
        if type(outcome) is not ProcessingOutcome:
            error = self._manual_review()
            _log_stage_failure("completion_projection", error)
            raise error

        try:
            legacy_status = await self._ctx.db_manager.get_email_status(
                lease.event.external_email_id
            )
        except Exception as error:
            _log_stage_failure("completion_readback", error)
            raise
        try:
            return self._map_completion(policy, outcome, legacy_status)
        except Exception as error:
            _log_stage_failure("completion_projection", error)
            raise

    @staticmethod
    def _validate_attempt(
        lease: InboxLease,
        application: EmailEventApplication,
        *,
        legacy_account_id: int,
    ) -> ProcessingPolicy:
        if (
            type(lease) is not InboxLease
            or type(application) is not EmailEventApplication
        ):
            raise ProcessingPolicyRejected()
        if (
            lease.account_id != legacy_account_id
            or lease.pipeline_name != LegacyProcessingAdapter.pipeline_name
            or application.disposition not in _EXECUTABLE_DISPOSITIONS
            or application.should_process is not True
            or application.persisted_status is not EmailStatus.PROCESSING
        ):
            raise ProcessingPolicyRejected()
        policy = lease.event.processing_policy
        if type(policy) is not ProcessingPolicy:
            raise ProcessingPolicyRejected()
        return policy

    @staticmethod
    def _allowed_effects(
        policy: ProcessingPolicy,
    ) -> frozenset[ExternalEffectKind]:
        if policy is ProcessingPolicy.FULL:
            return _FULL_EFFECTS
        if policy is ProcessingPolicy.ARCHIVE:
            return _ARCHIVE_EFFECTS
        raise ProcessingPolicyRejected()

    @staticmethod
    def _project_detail(lease: InboxLease, details: object) -> dict[str, Any]:
        if details is None:
            raise ReplaySafeExternalEffectFailed()
        if not isinstance(details, Mapping):
            raise LegacyProcessingAdapter._manual_review()
        email_data = dict(details)
        detail_id = email_data.get("id")
        if type(detail_id) is not str or detail_id != lease.event.external_email_id:
            raise ManualReviewRequired(
                reason="legacy_detail_identity_mismatch",
                safe_summary="Legacy email detail requires manual review",
            )
        email_data["id"] = lease.event.external_email_id
        email_data.setdefault("subject", "")
        email_data.setdefault("sender", "")
        email_data.setdefault(
            "received_at",
            email_data.get("received_time", ""),
        )
        # NormalizedIngressEvent recursively freezes nested payload objects with
        # MappingProxyType.  Never leak that immutable transport representation
        # into the JSON-backed content envelope; ``folder`` is the already
        # validated authoritative parent-folder ID.
        email_data["_parent_folder_id"] = lease.event.folder
        email_data["_parent_folder_name"] = lease.event.folder
        email_data["_event_type"] = lease.event.raw_event_type
        return email_data

    @staticmethod
    def _map_completion(
        policy: ProcessingPolicy,
        outcome: ProcessingOutcome,
        legacy_status: object,
    ) -> ProcessingCompletion:
        if outcome is ProcessingOutcome.FAILED:
            raise LegacyProcessingFailed()
        if (
            outcome is ProcessingOutcome.MANUAL_REVIEW
            and type(legacy_status) is str
            and legacy_status == "manual_review"
        ):
            return ProcessingCompletion.manual_review()
        if policy is ProcessingPolicy.ARCHIVE:
            if (
                outcome is ProcessingOutcome.ARCHIVED
                and type(legacy_status) is str
                and legacy_status == "archived"
            ):
                return ProcessingCompletion.archived()
            raise LegacyProcessingAdapter._manual_review()
        if outcome is ProcessingOutcome.PROCESSED and type(legacy_status) is str:
            if legacy_status == "waiting_approval":
                return ProcessingCompletion.waiting_approval()
            if legacy_status == "notified_readonly":
                return ProcessingCompletion.notified_readonly()
            if legacy_status in {"skipped", "no_action"}:
                return ProcessingCompletion.no_action()
        raise LegacyProcessingAdapter._manual_review()

    @staticmethod
    def _manual_review() -> ManualReviewRequired:
        return ManualReviewRequired(
            reason="legacy_projection_unmapped",
            safe_summary="Legacy processing result requires manual review",
        )


__all__ = ["LegacyProcessingAdapter", "LegacyProcessingFailed"]
