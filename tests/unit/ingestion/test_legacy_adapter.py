from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.domain.email_state import ProcessingOutcome
from src.domain.errors import ErrorKind, ManualReviewRequired
from src.ingestion.email_events import (
    EmailEventApplication,
    EmailEventDecision,
    EmailEventDisposition,
    EmailEventReason,
    EmailStatus,
)
from src.ingestion.legacy_adapter import (
    LegacyProcessingAdapter,
    LegacyProcessingFailed,
    _FULL_EFFECTS,
    _PolicyBoundEffectPort,
)
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.processing import (
    ExternalEffectAuthorizationError,
    ExternalEffectKind,
    ProcessingCompletion,
    ProcessingPolicyRejected,
    ReplaySafeExternalEffectFailed,
)
from src.ingestion.runtime_authority import GREENFIELD_PIPELINE_NAME
from src.storage.content_store import serialize_email_envelope


def _lease(
    *,
    policy: ProcessingPolicy = ProcessingPolicy.FULL,
) -> InboxLease:
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
        processing_policy=policy,
        source_event_at=now,
    )
    return InboxLease(
        id=str(uuid4()),
        account_id=8,
        pipeline_name=GREENFIELD_PIPELINE_NAME,
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


def _application(
    *,
    disposition: EmailEventDisposition = EmailEventDisposition.CREATOR_ELECTED,
) -> EmailEventApplication:
    reason = (
        EmailEventReason.PROCESSING_RESUMED
        if disposition is EmailEventDisposition.PROCESSING_RESUMED
        else EmailEventReason.FIRST_CREATE
    )
    return EmailEventApplication(
        decision=EmailEventDecision(
            should_process=True,
            should_cancel=False,
            new_status=EmailStatus.PROCESSING,
            cancel_pending_side_effects=False,
            create_seen=True,
            reason=reason,
        ),
        email_id=str(uuid4()),
        persisted_status=EmailStatus.PROCESSING,
        version=4,
        disposition=disposition,
        may_complete_without_processing=False,
    )


def _ctx(*, legacy_status: str | None):
    return SimpleNamespace(
        exchange_client=SimpleNamespace(
            get_email=AsyncMock(
                return_value={
                    "id": "message-1",
                    "subject": "Subject",
                    "sender": "sender@example.test",
                    "body": "Body",
                    "received_at": "2026-07-16T08:00:00+00:00",
                }
            )
        ),
        db_manager=SimpleNamespace(
            get_email_status=AsyncMock(return_value=legacy_status)
        ),
    )


@pytest.mark.parametrize(
    "legacy_account_id",
    [True, 0, -1, POSTGRES_BIGINT_MAX + 1],
)
def test_adapter_requires_an_exact_bounded_legacy_account(
    legacy_account_id: object,
) -> None:
    with pytest.raises(ValueError, match="legacy_account_id"):
        LegacyProcessingAdapter(
            _ctx(legacy_status="skipped"),
            legacy_account_id=legacy_account_id,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_adapter_rejects_cross_account_attempt_before_any_effect_or_io() -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )
    lease = _lease()
    cross_account_lease = replace(
        lease,
        account_id=9,
        event=replace(lease.event, account_id=9),
    )

    with pytest.raises(ProcessingPolicyRejected):
        await adapter.process(
            cross_account_lease,
            _application(),
            before_external_effect=before,
        )

    before.assert_not_awaited()
    ctx.exchange_client.get_email.assert_not_awaited()
    ctx.db_manager.get_email_status.assert_not_awaited()
    processor.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("legacy_status", "expected_status"),
    [
        ("waiting_approval", EmailStatus.WAITING_APPROVAL),
        ("notified_readonly", EmailStatus.NOTIFIED_READONLY),
        ("skipped", EmailStatus.NO_ACTION),
        ("no_action", EmailStatus.NO_ACTION),
    ],
)
async def test_full_maps_only_exact_persisted_legacy_projection(
    legacy_status: str,
    expected_status: EmailStatus,
) -> None:
    ctx = _ctx(legacy_status=legacy_status)
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )
    lease = _lease()
    application = _application()

    completion = await adapter.process(
        lease,
        application,
        before_external_effect=before,
    )

    assert completion.target_status is expected_status
    assert completion.legacy_outcome is ProcessingOutcome.PROCESSED
    ctx.exchange_client.get_email.assert_awaited_once_with("message-1")
    assert before.await_args_list[0].args[0:2] == ("detail", 0)
    assert len(before.await_args_list[0].args[2]) == 64
    kwargs = processor.await_args.kwargs
    assert kwargs["skip_analysis"] is False
    assert kwargs["force_reprocess"] is False
    assert kwargs["effect_scope"].email_id == application.email_id
    assert callable(kwargs["before_external_effect"])
    ctx.db_manager.get_email_status.assert_awaited_once_with("message-1")


@pytest.mark.asyncio
async def test_archive_fetches_detail_uses_exact_old_path_and_maps_archived() -> None:
    ctx = _ctx(legacy_status="archived")
    processor = AsyncMock(return_value=ProcessingOutcome.ARCHIVED)
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    completion = await adapter.process(
        _lease(policy=ProcessingPolicy.ARCHIVE),
        _application(),
        before_external_effect=before,
    )

    assert completion.target_status is EmailStatus.ARCHIVED
    assert completion.legacy_outcome is ProcessingOutcome.ARCHIVED
    assert processor.await_args.kwargs["skip_analysis"] is True


@pytest.mark.asyncio
async def test_nested_folder_payload_is_materialized_for_content_storage() -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )
    lease = _lease()
    lease = replace(
        lease,
        event=replace(
            lease.event,
            payload={
                "id": "message-1",
                "parent_folder_id": {
                    "id": "INBOX",
                    "changekey": "opaque-version",
                },
            },
        ),
    )

    await adapter.process(
        lease,
        _application(),
        before_external_effect=AsyncMock(return_value=None),
    )

    projected = processor.await_args.args[0]
    assert projected["_parent_folder_id"] == "INBOX"
    assert serialize_email_envelope("message-1", projected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "legacy_status"),
    [
        (ProcessingOutcome.PROCESSED, None),
        (ProcessingOutcome.PROCESSED, "pending"),
        (ProcessingOutcome.PROCESSED, "sent"),
        (ProcessingOutcome.ARCHIVED, "waiting_approval"),
        (ProcessingOutcome.DUPLICATE, "waiting_approval"),
    ],
)
async def test_unknown_duplicate_or_manual_legacy_results_require_review(
    outcome: ProcessingOutcome,
    legacy_status: str | None,
) -> None:
    ctx = _ctx(legacy_status=legacy_status)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=AsyncMock(return_value=outcome),
    )

    with pytest.raises(ManualReviewRequired):
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )


@pytest.mark.asyncio
async def test_manual_legacy_result_maps_to_terminal_manual_completion() -> None:
    ctx = _ctx(legacy_status="manual_review")
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=AsyncMock(return_value=ProcessingOutcome.MANUAL_REVIEW),
    )

    completion = await adapter.process(
        _lease(),
        _application(),
        before_external_effect=AsyncMock(return_value=None),
    )

    assert completion == ProcessingCompletion.manual_review()


@pytest.mark.asyncio
async def test_failed_legacy_outcome_becomes_fixed_typed_failure() -> None:
    ctx = _ctx(legacy_status="error")
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=AsyncMock(return_value=ProcessingOutcome.FAILED),
    )

    with pytest.raises(LegacyProcessingFailed) as caught:
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )
    assert str(caught.value) == "Legacy email processing failed"
    assert "message-1" not in repr(caught.value)


@pytest.mark.asyncio
async def test_only_resumed_processing_enables_force_reprocess() -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    await adapter.process(
        _lease(),
        _application(disposition=EmailEventDisposition.PROCESSING_RESUMED),
        before_external_effect=AsyncMock(return_value=None),
    )

    assert processor.await_args.kwargs["force_reprocess"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy",
    [
        ProcessingPolicy.METADATA_ONLY,
        ProcessingPolicy.IGNORED,
        ProcessingPolicy.HISTORICAL_SUPPRESSED,
    ],
)
async def test_non_executable_policies_make_zero_external_calls(
    policy: ProcessingPolicy,
) -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ProcessingPolicyRejected):
        await adapter.process(
            _lease(policy=policy),
            _application(),
            before_external_effect=before,
        )

    before.assert_not_awaited()
    ctx.exchange_client.get_email.assert_not_awaited()
    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_archive_effect_ceiling_rejects_model_before_underlying_port() -> None:
    async def processor(_email_data, _ctx, **kwargs):
        await kwargs["before_external_effect"]("qdrant", 0, "a" * 64)
        await kwargs["before_external_effect"]("model", 0, "b" * 64)
        raise AssertionError("forbidden effect did not stop processing")

    ctx = _ctx(legacy_status="archived")
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ProcessingPolicyRejected):
        await adapter.process(
            _lease(policy=ProcessingPolicy.ARCHIVE),
            _application(),
            before_external_effect=before,
        )

    assert [call.args[0] for call in before.await_args_list] == ["detail", "qdrant"]


@pytest.mark.asyncio
async def test_full_forwards_all_six_effect_kinds_through_required_port() -> None:
    async def processor(_email_data, _ctx, **kwargs):
        for ordinal, kind in enumerate(
            ("content", "model", "feishu", "exchange_mutation", "qdrant")
        ):
            await kwargs["before_external_effect"](kind, ordinal, "a" * 64)
        return ProcessingOutcome.PROCESSED

    ctx = _ctx(legacy_status="skipped")
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    await adapter.process(
        _lease(),
        _application(),
        before_external_effect=before,
    )

    assert {call.args[0] for call in before.await_args_list} == {
        "detail",
        "content",
        "model",
        "feishu",
        "exchange_mutation",
        "qdrant",
    }


@pytest.mark.asyncio
async def test_authority_loss_before_detail_stops_fetch_and_legacy_path() -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )
    failure = ExternalEffectAuthorizationError()

    with pytest.raises(ExternalEffectAuthorizationError) as caught:
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(side_effect=failure),
        )

    assert caught.value is failure
    ctx.exchange_client.get_email.assert_not_awaited()
    processor.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("detail_id", [None, "different-message"])
async def test_detail_identity_mismatch_fails_closed_before_legacy_path(
    detail_id: str | None,
) -> None:
    ctx = _ctx(legacy_status="skipped")
    ctx.exchange_client.get_email.return_value = (
        {} if detail_id is None else {"id": detail_id}
    )
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ManualReviewRequired):
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )
    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_detail_is_retryable_instead_of_manual_review(caplog) -> None:
    ctx = _ctx(legacy_status="skipped")
    ctx.exchange_client.get_email.return_value = None
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ReplaySafeExternalEffectFailed) as caught:
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )

    assert caught.value.kind is ErrorKind.TRANSIENT_DEPENDENCY
    assert "stage=detail_fetch" in caplog.text
    assert "error_type=ReplaySafeExternalEffectFailed" in caplog.text
    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_detail_exception_is_redacted_and_wrapped_as_replay_safe(caplog) -> None:
    ctx = _ctx(legacy_status="skipped")
    ctx.exchange_client.get_email.side_effect = RuntimeError(
        "PRIVATE-EXCHANGE-DETAIL"
    )
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ReplaySafeExternalEffectFailed) as caught:
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )

    assert caught.value.__cause__ is None
    assert "stage=detail_fetch" in caplog.text
    assert "error_type=RuntimeError" in caplog.text
    assert "PRIVATE-EXCHANGE-DETAIL" not in caplog.text
    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_guarded_processing_failure_logs_only_stage_and_type(caplog) -> None:
    ctx = _ctx(legacy_status="skipped")
    private_failure = RuntimeError("PRIVATE-GUARDED-DETAIL")
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=AsyncMock(side_effect=private_failure),
    )

    with pytest.raises(RuntimeError) as caught:
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )

    assert caught.value is private_failure
    assert "stage=guarded_processing" in caplog.text
    assert "error_type=RuntimeError" in caplog.text
    assert "PRIVATE-GUARDED-DETAIL" not in caplog.text


@pytest.mark.asyncio
async def test_adapter_rejects_application_that_did_not_elect_processing() -> None:
    ctx = _ctx(legacy_status="skipped")
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=AsyncMock(return_value=ProcessingOutcome.PROCESSED),
    )
    elected = _application()
    invalid = object.__new__(EmailEventApplication)
    for field, value in (
        ("decision", replace(elected.decision, should_process=False)),
        ("email_id", elected.email_id),
        ("persisted_status", elected.persisted_status),
        ("version", elected.version),
        ("disposition", elected.disposition),
        ("may_complete_without_processing", elected.may_complete_without_processing),
    ):
        object.__setattr__(invalid, field, value)

    with pytest.raises(ProcessingPolicyRejected):
        await adapter.process(
            _lease(),
            invalid,
            before_external_effect=AsyncMock(return_value=None),
        )


def test_adapter_requires_context_and_async_guarded_processor() -> None:
    with pytest.raises(ValueError, match="ctx is required"):
        LegacyProcessingAdapter(None, legacy_account_id=8)
    with pytest.raises(ValueError, match="guarded_processor"):
        LegacyProcessingAdapter(
            _ctx(legacy_status="skipped"),
            legacy_account_id=8,
            guarded_processor=lambda *_args, **_kwargs: ProcessingOutcome.PROCESSED,
        )


def test_adapter_is_immutable_and_exposes_only_frozen_account_scope() -> None:
    adapter = LegacyProcessingAdapter(
        _ctx(legacy_status="skipped"),
        legacy_account_id=8,
        guarded_processor=AsyncMock(return_value=ProcessingOutcome.PROCESSED),
    )

    assert adapter.legacy_account_id == 8
    assert adapter.pipeline_name == GREENFIELD_PIPELINE_NAME
    with pytest.raises(AttributeError, match="immutable"):
        adapter.legacy_account_id = 9  # type: ignore[misc]
    with pytest.raises(AttributeError, match="immutable"):
        del adapter.legacy_account_id


class _AsyncProcessorObject:
    async def __call__(self, *_args, **_kwargs) -> ProcessingOutcome:
        return ProcessingOutcome.PROCESSED


def test_adapter_accepts_async_callable_objects_without_invoking_them() -> None:
    adapter = LegacyProcessingAdapter(
        _ctx(legacy_status="skipped"),
        legacy_account_id=8,
        guarded_processor=_AsyncProcessorObject(),
    )

    assert adapter.legacy_account_id == 8


@pytest.mark.asyncio
async def test_adapter_rejects_non_async_effect_port_before_detail_fetch() -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ValueError, match="async callable"):
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=lambda *_args: None,  # type: ignore[arg-type]
        )

    ctx.exchange_client.get_email.assert_not_awaited()
    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_policy_effect_port_rejects_malformed_calls_without_delegation() -> None:
    delegate = AsyncMock(return_value=None)
    port = _PolicyBoundEffectPort(delegate, _FULL_EFFECTS)

    malformed_calls = [
        (ExternalEffectKind.MODEL, 0, "a" * 64),
        ("unknown", 0, "a" * 64),
        ("model", True, "a" * 64),
        ("model", 0, "not-a-digest"),
    ]
    for kind, ordinal, target_hash in malformed_calls:
        with pytest.raises(ProcessingPolicyRejected):
            await port(kind, ordinal, target_hash)  # type: ignore[arg-type]

    delegate.assert_not_awaited()


def test_policy_effect_port_requires_exact_nonempty_policy_and_async_delegate() -> None:
    with pytest.raises(ValueError, match="async callable"):
        _PolicyBoundEffectPort(None, _FULL_EFFECTS)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="async callable"):
        _PolicyBoundEffectPort(lambda *_args: None, _FULL_EFFECTS)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="exact non-empty frozenset"):
        _PolicyBoundEffectPort(AsyncMock(return_value=None), frozenset())
    with pytest.raises(ValueError, match="exact non-empty frozenset"):
        _PolicyBoundEffectPort(
            AsyncMock(return_value=None),
            {ExternalEffectKind.MODEL},  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_policy_effect_port_rejects_non_none_authorization_receipt() -> None:
    delegate = AsyncMock(return_value="unexpected-receipt")
    port = _PolicyBoundEffectPort(delegate, _FULL_EFFECTS)

    with pytest.raises(ExternalEffectAuthorizationError):
        await port("model", 0, "a" * 64)

    delegate.assert_awaited_once_with("model", 0, "a" * 64)


@pytest.mark.asyncio
async def test_adapter_rejects_non_application_input_before_any_effect() -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ProcessingPolicyRejected):
        await adapter.process(
            _lease(),
            object(),  # type: ignore[arg-type]
            before_external_effect=before,
        )

    before.assert_not_awaited()
    ctx.exchange_client.get_email.assert_not_awaited()
    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_adapter_fails_closed_if_an_in_memory_lease_policy_is_corrupted() -> None:
    ctx = _ctx(legacy_status="skipped")
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    before = AsyncMock(return_value=None)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )
    lease = _lease()
    # This branch is defense in depth: a valid NormalizedIngressEvent cannot carry
    # a non-enum policy, but a compromised in-memory object must still make zero I/O.
    object.__setattr__(lease.event, "processing_policy", "full")

    with pytest.raises(ProcessingPolicyRejected):
        await adapter.process(
            lease,
            _application(),
            before_external_effect=before,
        )

    before.assert_not_awaited()
    ctx.exchange_client.get_email.assert_not_awaited()
    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_mapping_detail_requires_review_before_legacy_processor() -> None:
    ctx = _ctx(legacy_status="skipped")
    ctx.exchange_client.get_email.return_value = ["message-1"]
    processor = AsyncMock(return_value=ProcessingOutcome.PROCESSED)
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=processor,
    )

    with pytest.raises(ManualReviewRequired):
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )

    processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_enum_processor_result_requires_review() -> None:
    ctx = _ctx(legacy_status="skipped")
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=AsyncMock(return_value="processed"),
    )

    with pytest.raises(ManualReviewRequired):
        await adapter.process(
            _lease(),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )

    ctx.db_manager.get_email_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_archive_result_must_match_exact_persisted_archive_projection() -> None:
    ctx = _ctx(legacy_status="waiting_approval")
    adapter = LegacyProcessingAdapter(
        ctx,
        legacy_account_id=8,
        guarded_processor=AsyncMock(return_value=ProcessingOutcome.ARCHIVED),
    )

    with pytest.raises(ManualReviewRequired):
        await adapter.process(
            _lease(policy=ProcessingPolicy.ARCHIVE),
            _application(),
            before_external_effect=AsyncMock(return_value=None),
        )
