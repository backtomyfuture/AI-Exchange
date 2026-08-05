from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.domain.email_state import PipelineGenerationState, ProcessingOutcome
from src.ingestion.email_events import (
    EmailEventApplication,
    EmailEventDecision,
    EmailEventDisposition,
    EmailEventReason,
    EmailStatus,
)
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    InboxStatus,
    IngressSource,
    NormalizedIngressEvent,
    PipelineGeneration,
    ProcessingPolicy,
)
from src.ingestion.processing import (
    ExternalEffectAuthorizationError,
    ExternalEffectBoundary,
    ExternalEffectKind,
    GuardedExternalEffectFailed,
    LegacyEffectScope,
    ProcessingAdapterRouter,
    ProcessingAdapterUnavailable,
    ProcessingCompletion,
    ProcessingCompletionRejected,
    ProcessingFinishResult,
    ProcessingPolicyRejected,
    ProcessingReceiptConflict,
)
from src.ingestion.runtime_authority import GREENFIELD_PIPELINE_NAME


def _lease(*, pipeline_name: str = GREENFIELD_PIPELINE_NAME) -> InboxLease:
    now = datetime.now(UTC)
    event = NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id="message-1",
        folder="INBOX",
        source_version="version-1",
        dedupe_key="a" * 64,
        payload={"id": "message-1"},
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=now,
    )
    return InboxLease(
        id=str(uuid4()),
        account_id=8,
        pipeline_name=pipeline_name,
        generation=3,
        fencing_token=7,
        execution_epoch=0,
        authority_epoch=1,
        capability_hash="a" * 64,
        lease_session_id="00000000-0000-4000-8000-000000000002",
        lease_owner="worker-1",
        attempts=1,
        event=event,
        received_at=now,
        lease_until=now + timedelta(minutes=5),
    )


def _authority(
    lease: InboxLease,
    *,
    state: PipelineGenerationState = PipelineGenerationState.CURRENT_INGRESS,
) -> PipelineGeneration:
    return PipelineGeneration(
        account_id=lease.account_id,
        generation=lease.generation,
        pipeline_name=lease.pipeline_name,
        state=state,
        fencing_token=lease.fencing_token,
    )


def _application() -> EmailEventApplication:
    return EmailEventApplication(
        decision=EmailEventDecision(
            should_process=True,
            should_cancel=False,
            new_status=EmailStatus.PROCESSING,
            cancel_pending_side_effects=False,
            create_seen=True,
            reason=EmailEventReason.FIRST_CREATE,
        ),
        email_id=str(uuid4()),
        persisted_status=EmailStatus.PROCESSING,
        version=4,
        disposition=EmailEventDisposition.CREATOR_ELECTED,
        may_complete_without_processing=False,
    )


@pytest.mark.parametrize(
    ("factory", "status", "legacy_outcome"),
    [
        (
            ProcessingCompletion.waiting_approval,
            EmailStatus.WAITING_APPROVAL,
            ProcessingOutcome.PROCESSED,
        ),
        (
            ProcessingCompletion.notified_readonly,
            EmailStatus.NOTIFIED_READONLY,
            ProcessingOutcome.PROCESSED,
        ),
        (
            ProcessingCompletion.no_action,
            EmailStatus.NO_ACTION,
            ProcessingOutcome.PROCESSED,
        ),
        (
            ProcessingCompletion.archived,
            EmailStatus.ARCHIVED,
            ProcessingOutcome.ARCHIVED,
        ),
    ],
)
def test_processing_completion_factories_are_exact_and_immutable(
    factory, status, legacy_outcome
) -> None:
    completion = factory()

    assert completion.target_status is status
    assert completion.legacy_outcome is legacy_outcome
    assert completion.safe_error_code is None
    assert completion.safe_error_summary is None
    with pytest.raises((AttributeError, TypeError)):
        completion.target_status = EmailStatus.ARCHIVED  # type: ignore[misc]


def test_manual_review_completion_is_an_explicit_terminal_outcome() -> None:
    completion = ProcessingCompletion.manual_review()

    assert completion.target_status is EmailStatus.MANUAL_REVIEW
    assert completion.legacy_outcome is ProcessingOutcome.MANUAL_REVIEW
    assert completion.safe_error_code == "processing.manual_review"
    assert completion.safe_error_summary == "Processing requires manual review"


@pytest.mark.parametrize(
    "values",
    [
        {
            "target_status": EmailStatus.PROCESSING,
            "legacy_outcome": ProcessingOutcome.PROCESSED,
        },
        {
            "target_status": EmailStatus.WAITING_APPROVAL,
            "legacy_outcome": ProcessingOutcome.ARCHIVED,
        },
        {
            "target_status": EmailStatus.NO_ACTION,
            "legacy_outcome": ProcessingOutcome.PROCESSED,
            "safe_error_code": "legacy.failure",
            "safe_error_summary": "Safe",
        },
        {
            "target_status": EmailStatus.NO_ACTION,
            "legacy_outcome": ProcessingOutcome.PROCESSED,
            "safe_error_code": "legacy.failure",
        },
        {
            "target_status": EmailStatus.NO_ACTION.value,
            "legacy_outcome": ProcessingOutcome.PROCESSED,
        },
        {
            "target_status": EmailStatus.NO_ACTION,
            "legacy_outcome": ProcessingOutcome.PROCESSED.value,
        },
    ],
)
def test_processing_completion_rejects_invalid_cross_field_states(values) -> None:
    with pytest.raises(ValueError):
        ProcessingCompletion(**values)


@pytest.mark.parametrize(
    ("email_status", "inbox_status"),
    [
        (EmailStatus.WAITING_APPROVAL, InboxStatus.COMPLETED),
        (EmailStatus.NOTIFIED_READONLY, InboxStatus.COMPLETED),
        (EmailStatus.NO_ACTION, InboxStatus.COMPLETED),
        (EmailStatus.ARCHIVED, InboxStatus.COMPLETED),
        (EmailStatus.RETRY_WAIT, InboxStatus.RETRY_WAIT),
        (EmailStatus.MANUAL_REVIEW, InboxStatus.MANUAL_REVIEW),
        (EmailStatus.DEAD_LETTER, InboxStatus.DEAD_LETTER),
    ],
)
def test_processing_finish_result_accepts_only_atomic_pairings(
    email_status, inbox_status
) -> None:
    result = ProcessingFinishResult(
        email_status=email_status,
        inbox_status=inbox_status,
        replayed=False,
    )
    assert result.email_status is email_status
    assert result.inbox_status is inbox_status


def test_processing_finish_result_rejects_split_aggregate_state() -> None:
    with pytest.raises(ValueError, match="atomic"):
        ProcessingFinishResult(
            email_status=EmailStatus.PROCESSING,
            inbox_status=InboxStatus.COMPLETED,
            replayed=False,
        )


@pytest.mark.parametrize(
    ("email_status", "inbox_status"),
    [
        (EmailStatus.NO_ACTION.value, InboxStatus.COMPLETED),
        (EmailStatus.NO_ACTION, InboxStatus.COMPLETED.value),
    ],
)
def test_processing_finish_result_rejects_non_enum_values(
    email_status, inbox_status
) -> None:
    with pytest.raises(ValueError, match="exact"):
        ProcessingFinishResult(
            email_status=email_status,
            inbox_status=inbox_status,
            replayed=False,
        )


def test_legacy_effect_scope_hash_is_stable_bounded_and_stamp_bound() -> None:
    lease = _lease()
    application = _application()
    scope = LegacyEffectScope.from_processing(lease, application)

    first = scope.target_hash(
        ExternalEffectKind.DETAIL,
        0,
        {"operation": "get_email", "external_email_id": "message-1"},
    )
    second = scope.target_hash(
        ExternalEffectKind.DETAIL,
        0,
        {"external_email_id": "message-1", "operation": "get_email"},
    )
    changed = scope.target_hash(
        ExternalEffectKind.DETAIL,
        1,
        {"operation": "get_email", "external_email_id": "message-1"},
    )

    assert first == second
    assert len(first) == 64
    assert first.isascii() and first.islower()
    assert changed != first


@pytest.mark.asyncio
async def test_external_effect_boundary_calls_required_async_port_with_hash() -> None:
    scope = LegacyEffectScope.from_processing(_lease(), _application())
    callback = AsyncMock(return_value=None)
    boundary = ExternalEffectBoundary(scope, callback)

    target_hash = await boundary.before(
        ExternalEffectKind.QDRANT,
        0,
        {"operation": "ingest"},
    )

    callback.assert_awaited_once_with("qdrant", 0, target_hash)
    assert len(target_hash) == 64


def test_external_effect_boundary_rejects_missing_or_sync_ports() -> None:
    scope = LegacyEffectScope.from_processing(_lease(), _application())

    with pytest.raises(ValueError, match="async"):
        ExternalEffectBoundary(scope, None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="async"):
        ExternalEffectBoundary(scope, lambda *_args: None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_external_effect_boundary_does_not_swallow_authority_loss() -> None:
    scope = LegacyEffectScope.from_processing(_lease(), _application())
    failure = ExternalEffectAuthorizationError()
    callback = AsyncMock(side_effect=failure)
    boundary = ExternalEffectBoundary(scope, callback)

    with pytest.raises(ExternalEffectAuthorizationError) as caught:
        await boundary.before(ExternalEffectKind.MODEL, 0, {"operation": "graph"})
    assert caught.value is failure


@pytest.mark.asyncio
async def test_external_effect_boundary_rejects_non_none_authorization_receipt() -> (
    None
):
    scope = LegacyEffectScope.from_processing(_lease(), _application())
    boundary = ExternalEffectBoundary(scope, AsyncMock(return_value=True))

    with pytest.raises(ExternalEffectAuthorizationError):
        await boundary.before(
            ExternalEffectKind.CONTENT,
            0,
            {"operation": "put_email_content"},
        )


class _Adapter:
    pipeline_name = GREENFIELD_PIPELINE_NAME

    async def process(self, *_args, **_kwargs):
        return ProcessingCompletion.no_action()


class _RetiredLegacyAdapter(_Adapter):
    pipeline_name = "legacy_compat"


def test_processing_adapter_router_freezes_exact_production_registry() -> None:
    adapter = _Adapter()
    source = {GREENFIELD_PIPELINE_NAME: adapter}
    router = ProcessingAdapterRouter(source)
    lease = _lease()
    source.clear()

    assert router.registry == MappingProxyType({GREENFIELD_PIPELINE_NAME: adapter})
    assert router.select(lease, _authority(lease)) is adapter


@pytest.mark.parametrize(
    "state",
    [
        PipelineGenerationState.CURRENT_INGRESS,
        PipelineGenerationState.QUIESCING,
        PipelineGenerationState.DRAINING,
    ],
)
def test_processing_adapter_router_selects_only_exact_executable_stamp(state) -> None:
    adapter = _Adapter()
    router = ProcessingAdapterRouter({GREENFIELD_PIPELINE_NAME: adapter})
    lease = _lease()

    assert router.select(lease, _authority(lease, state=state)) is adapter


@pytest.mark.parametrize(
    "mutate",
    [
        lambda generation: PipelineGeneration(
            account_id=9,
            generation=generation.generation,
            pipeline_name=generation.pipeline_name,
            state=generation.state,
            fencing_token=generation.fencing_token,
        ),
        lambda generation: PipelineGeneration(
            account_id=generation.account_id,
            generation=generation.generation + 1,
            pipeline_name=generation.pipeline_name,
            state=generation.state,
            fencing_token=generation.fencing_token,
        ),
        lambda generation: PipelineGeneration(
            account_id=generation.account_id,
            generation=generation.generation,
            pipeline_name=generation.pipeline_name,
            state=PipelineGenerationState.RETIRED,
            fencing_token=generation.fencing_token,
        ),
        lambda generation: PipelineGeneration(
            account_id=generation.account_id,
            generation=generation.generation,
            pipeline_name=generation.pipeline_name,
            state=generation.state,
            fencing_token=generation.fencing_token + 1,
        ),
    ],
)
def test_processing_adapter_router_fails_closed_on_nonexact_authority(mutate) -> None:
    lease = _lease()
    router = ProcessingAdapterRouter({GREENFIELD_PIPELINE_NAME: _Adapter()})

    with pytest.raises(ProcessingAdapterUnavailable):
        router.select(lease, mutate(_authority(lease)))


def test_retired_legacy_stamp_never_routes_to_production_adapter() -> None:
    lease = _lease(pipeline_name="legacy_compat")
    router = ProcessingAdapterRouter({GREENFIELD_PIPELINE_NAME: _Adapter()})

    with pytest.raises(ProcessingAdapterUnavailable):
        router.select(lease, _authority(lease))


@pytest.mark.parametrize(
    "registry",
    [
        {},
        {"legacy_compat": _Adapter()},
        {GREENFIELD_PIPELINE_NAME: _RetiredLegacyAdapter()},
        {GREENFIELD_PIPELINE_NAME: object()},
    ],
)
def test_router_rejects_nonproduction_or_invalid_registries(registry) -> None:
    with pytest.raises(ValueError):
        ProcessingAdapterRouter(registry)


@pytest.mark.parametrize(
    ("error_type", "safe_code"),
    [
        (ProcessingAdapterUnavailable, "processing.adapter_unavailable"),
        (ProcessingPolicyRejected, "processing.policy_rejected"),
        (ExternalEffectAuthorizationError, "processing.effect_not_authorized"),
        (GuardedExternalEffectFailed, "processing.external_effect_failed"),
        (ProcessingCompletionRejected, "processing.completion_rejected"),
        (ProcessingReceiptConflict, "processing.receipt_conflict"),
    ],
)
def test_processing_control_errors_expose_only_fixed_safe_repr(
    error_type: type[RuntimeError],
    safe_code: str,
) -> None:
    error = error_type()

    assert repr(error) == f"{error_type.__name__}(safe_code={safe_code!r})"
    assert "message-1" not in repr(error)


def test_processing_finish_result_rejects_truthy_non_boolean_replay_marker() -> None:
    with pytest.raises(ValueError, match="boolean"):
        ProcessingFinishResult(
            email_status=EmailStatus.NO_ACTION,
            inbox_status=InboxStatus.COMPLETED,
            replayed=1,  # type: ignore[arg-type]
        )


def _scope_values() -> dict[str, object]:
    return {
        "account_id": 8,
        "inbox_id": str(uuid4()),
        "generation": 3,
        "fencing_token": 7,
        "attempts": 1,
        "email_id": str(uuid4()),
        "expected_email_version": 4,
        "event_dedupe_key": "a" * 64,
        "external_email_id": "message-1",
    }


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("account_id", True, "BIGINT"),
        ("attempts", -1, "BIGINT"),
        ("inbox_id", 1, "UUID string"),
        ("email_id", "not-a-uuid", "UUID string"),
        ("event_dedupe_key", "A" * 64, "lowercase SHA-256"),
        ("external_email_id", " message-1", "exact bounded text"),
        ("external_email_id", "bad\nmessage", "exact bounded text"),
        ("external_email_id", "\ud800", "valid UTF-8"),
    ],
)
def test_legacy_effect_scope_rejects_ambiguous_or_unpersistable_identifiers(
    field: str,
    value: object,
    message: str,
) -> None:
    values = _scope_values()
    values[field] = value

    with pytest.raises(ValueError, match=message):
        LegacyEffectScope(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("lease", "application", "message"),
    [
        (object(), _application(), "InboxLease"),
        (_lease(), object(), "EmailEventApplication"),
    ],
)
def test_legacy_effect_scope_factory_rejects_unstamped_inputs(
    lease: object,
    application: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        LegacyEffectScope.from_processing(lease, application)  # type: ignore[arg-type]


@pytest.mark.parametrize("kind", [object(), "unknown-effect"])
def test_effect_hash_rejects_noncanonical_effect_kind(kind: object) -> None:
    scope = LegacyEffectScope.from_processing(_lease(), _application())

    with pytest.raises(ValueError, match="valid ExternalEffectKind"):
        scope.target_hash(kind, 0, {"operation": "safe"})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "target",
    [
        {"not_json": object()},
        {"not_finite": float("nan")},
        {"invalid_text": "\ud800"},
    ],
)
def test_effect_hash_rejects_targets_without_one_canonical_json_encoding(
    target: object,
) -> None:
    scope = LegacyEffectScope.from_processing(_lease(), _application())

    with pytest.raises(ValueError, match="canonical JSON"):
        scope.target_hash(ExternalEffectKind.CONTENT, 0, target)


def test_external_effect_boundary_rejects_scope_like_objects() -> None:
    with pytest.raises(ValueError, match="LegacyEffectScope"):
        ExternalEffectBoundary(object(), AsyncMock(return_value=None))  # type: ignore[arg-type]


def test_router_rejects_non_mapping_registry_before_copying() -> None:
    with pytest.raises(ValueError, match="mapping"):
        ProcessingAdapterRouter(  # type: ignore[arg-type]
            [(GREENFIELD_PIPELINE_NAME, _Adapter())]
        )


@pytest.mark.parametrize(
    ("lease", "authority"),
    [
        (object(), _authority(_lease())),
        (_lease(), object()),
    ],
)
def test_processing_adapter_router_rejects_stamp_like_objects(
    lease: object,
    authority: object,
) -> None:
    router = ProcessingAdapterRouter({GREENFIELD_PIPELINE_NAME: _Adapter()})

    with pytest.raises(ProcessingAdapterUnavailable):
        router.select(lease, authority)  # type: ignore[arg-type]
