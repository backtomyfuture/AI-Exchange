import asyncio
import logging
from copy import deepcopy
from dataclasses import dataclass
from collections.abc import Mapping
from src.config import get_settings
from src.email_feishu_delivery import (
    ApprovalRequest,
    EmailDeliveryDisposition,
    EmailDeliveryOutcome,
    EmailDeliverySideEffectCommittedError,
    ManualReviewNotificationRequest,
    ReadNotificationRequest,
)
from src.domain.email_state import (
    SAFE_DUPLICATE_READ_STATUSES,
    InitialEmailWriteResult,
    ProcessingOutcome,
)
from src.domain.errors import DatabaseOperationError, StaleFence
from src.graph.state_factory import (
    MAX_ID_BYTES,
    MAX_TOKENS,
    build_initial_graph_state,
    cap_identifier_list,
    require_owned_content_ref,
    require_owned_draft_id,
    sanitize_graph_delta,
)
from src.graph.resource_locks import get_graph_resource_lock
from src.safety.input_limits import input_limits_from_settings, validate_email_input
from src.safety.manual_review import (
    build_manual_review_delta,
    normalize_manual_review_code,
)
from src.security.redaction import fingerprint_identifier
from src.utils.notification_policy import decide_notification_kind
from src.storage import ContentRef
from src.ingestion.processing import (
    BeforeExternalEffect,
    ExternalEffectAuthorizationError,
    ExternalEffectBoundary,
    ExternalEffectKind,
    GuardedExternalEffectFailed,
    ProcessingEffectScope,
    ProcessingPolicyRejected,
)

logger = logging.getLogger("ExchangeService")
@dataclass(frozen=True)
class CleanupHandleSnapshot:
    attachment_tokens: tuple[str, ...] = ()
    pdf_token: str | None = None


async def _authorize_external_effect(
    boundary: ExternalEffectBoundary | None,
    kind: ExternalEffectKind,
    ordinal: int,
    target: object,
) -> None:
    """Authorize one external call on the guarded path; legacy calls stay unchanged."""
    if boundary is not None:
        await boundary.before(kind, ordinal, target)


def _content_ref_effect_target(operation: str, ref: ContentRef) -> dict[str, object]:
    return {
        "operation": operation,
        "account_id": ref.account_id,
        "object_id": ref.object_id,
        "key_version": ref.key_version,
        "sha256": ref.sha256,
    }


def _effect_boundary_kwargs(
    boundary: ExternalEffectBoundary | None,
) -> dict[str, ExternalEffectBoundary]:
    return {} if boundary is None else {"_effect_boundary": boundary}


async def _ingest_to_qdrant(
    email_id: str,
    email_data: dict,
    ctx,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> None:
    """Ingest email into Qdrant vector store (sync call wrapped in thread)."""
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.QDRANT,
        0,
        {"operation": "ingest_email", "email_id": email_id},
    )
    try:
        processed = await asyncio.to_thread(
            ctx.email_processor.process_email, email_data
        )
        if _effect_boundary is not None and processed is not True:
            raise GuardedExternalEffectFailed()
        logger.info(
            "Email ingested to Qdrant: email=%s",
            fingerprint_identifier(email_id, namespace="email"),
        )
        await ctx.db_manager.update_status(
            email_id,
            "ingested",
            error_message=None,
        )
    except DatabaseOperationError:
        raise
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.error("Qdrant ingest failed: error_type=%s", type(exc).__name__)


def _require_owned_ref(ref: object) -> ContentRef:
    return require_owned_content_ref(
        ref,
        expected_account_id=get_settings().EXCHANGE_ACCOUNT_ID,
    )


async def _run_ai_pipeline(
    email_id: str,
    ctx,
    config: dict,
    *,
    attachment_tokens: list[str] | None = None,
    preserved_attachment_tokens: list[str] | None = None,
    preserved_pdf_token: str | None = None,
    _state_lock_held: bool = False,
    _effect_boundary: ExternalEffectBoundary | None = None,
):
    """Rebuild slim State from durable refs and return a transient edge projection."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            return await _run_ai_pipeline(
                email_id,
                ctx,
                config,
                attachment_tokens=attachment_tokens,
                preserved_attachment_tokens=preserved_attachment_tokens,
                preserved_pdf_token=preserved_pdf_token,
                _state_lock_held=True,
                _effect_boundary=_effect_boundary,
            )
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.CONTENT,
        1,
        {"operation": "load_email_content", "email_id": email_id},
    )
    try:
        ref = _require_owned_ref(await ctx.db_manager.get_content_ref(email_id))
        email_data = await ctx.content_store.load_email(ref)
        initial_state = build_initial_graph_state(email_data, ref)
        resource_tokens = list(
            dict.fromkeys(
                [
                    *(preserved_attachment_tokens or []),
                    *(attachment_tokens or []),
                ]
            )
        )
        if resource_tokens or preserved_pdf_token is not None:
            initial_state.update(
                sanitize_graph_delta(
                    initial_state,
                    {
                        "attachment_tokens": resource_tokens,
                        "pdf_token": preserved_pdf_token,
                    },
                )
            )

        graph_ordinal = 0

        async def consume(graph_input) -> None:
            nonlocal graph_ordinal
            await _authorize_external_effect(
                _effect_boundary,
                ExternalEffectKind.MODEL,
                graph_ordinal,
                {
                    "operation": "graph_astream",
                    "email_id": email_id,
                    "resume": graph_input is None,
                },
            )
            graph_ordinal += 1
            async for event in ctx.graph.astream(graph_input, config=config):
                if "categorizer" in event:
                    classification = event["categorizer"].get("classification", {})
                    await ctx.db_manager.update_status(
                        email_id,
                        "analyzed",
                        classification=classification,
                    )
                if "drafter" in event:
                    await ctx.db_manager.update_status(email_id, "drafted")

        await consume(initial_state)
        state = await ctx.graph.aget_state(config)
        for _rewrite in range(2):
            if state.values.get("next_step") != "drafter":
                break
            await consume(None)
            state = await ctx.graph.aget_state(config)
        if state.values.get("next_step") == "drafter":
            update = build_manual_review_delta(
                state.values,
                "graph_rewrite_limit",
                review_result={
                    "passed": False,
                    "issues": "graph_rewrite_limit",
                },
            )
            await ctx.graph.aupdate_state(config, update)
            state = await ctx.graph.aget_state(config)

        state_values = state.values
        draft_id = state_values.get("draft_id")
        is_manual_review = (
            state_values.get("next_step") == "manual_review"
            or state_values.get("approval_status") == "manual_review"
        )
        draft = (
            await ctx.db_manager.load_draft(
                require_owned_draft_id(state_values, draft_id)
            )
            if draft_id is not None and not is_manual_review
            else ""
        )
        projection_email = deepcopy(dict(email_data))
        projection_email["draft_to"] = list(state_values.get("draft_to") or [])
        projection_email["draft_cc"] = list(state_values.get("draft_cc") or [])
        return {
            "classification": state_values.get("classification", {}),
            "draft": draft,
            "context": state_values.get("context_summaries", []),
            "email": projection_email,
            "routing_log": state_values.get("routing_log", []),
            "route_decision": state_values.get("route_decision"),
            "approval_status": state_values.get("approval_status", ""),
            "next_step": state_values.get("next_step", ""),
            "safe_error_summary": state_values.get("safe_error_summary"),
        }
    except (
        ExternalEffectAuthorizationError,
        StaleFence,
        GuardedExternalEffectFailed,
    ):
        raise
    except DatabaseOperationError:
        raise
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.error(
            "Graph pipeline failed: error_type=%s",
            type(exc).__name__,
        )
        return None


async def _persist_canonical_route_decision(
    pipeline_result: dict,
    ctx,
    *,
    _effect_boundary: ExternalEffectBoundary | None,
) -> None:
    """Persist the final route before the first user-visible side effect."""
    if _effect_boundary is None:
        return
    raw = pipeline_result.get("route_decision")
    scope = _effect_boundary.scope
    if raw is None or not callable(
        getattr(ctx.db_manager, "persist_route_decision", None)
    ):
        raise ProcessingPolicyRejected()
    await ctx.db_manager.persist_route_decision(
        inbox_id=scope.inbox_id,
        account_id=scope.account_id,
        external_email_id=scope.external_email_id,
        decision_raw=raw,
    )


async def _advance_canonical_handoff(
    ctx,
    *,
    _effect_boundary: ExternalEffectBoundary | None,
    expected_state: str,
    next_state: str,
) -> None:
    if _effect_boundary is None:
        return
    transition = getattr(ctx.db_manager, "advance_handoff_execution", None)
    if not callable(transition):
        raise ProcessingPolicyRejected()
    await transition(
        inbox_id=_effect_boundary.scope.inbox_id,
        expected_state=expected_state,
        next_state=next_state,
    )


async def _deliver_pipeline_result(
    email_id: str,
    pipeline_result: Mapping[str, object],
    ctx,
    *,
    _effect_boundary: ExternalEffectBoundary | None,
) -> EmailDeliveryOutcome | None:
    """Project completed AI work into the one typed Email Feishu Delivery seam.

    ``None`` is the deliberate no-notification route. It is not a failed card
    and remains owned by the Exchange orchestration because it has no Delivery
    Resources to manage.
    """
    classification = pipeline_result.get("classification")
    if not isinstance(classification, Mapping):
        classification = {}
    email_data = pipeline_result.get("email")
    if not isinstance(email_data, Mapping):
        email_data = {}
    routing_log = pipeline_result.get("routing_log")
    if not isinstance(routing_log, (list, tuple)):
        routing_log = ()
    context = pipeline_result.get("context")
    if not isinstance(context, (list, tuple)):
        context = ()
    context_items = tuple(item for item in context if isinstance(item, Mapping))

    await _persist_canonical_route_decision(
        dict(pipeline_result),
        ctx,
        _effect_boundary=_effect_boundary,
    )
    is_manual_review = (
        pipeline_result.get("next_step") == "manual_review"
        or pipeline_result.get("approval_status") == "manual_review"
    )
    if is_manual_review:
        request = ManualReviewNotificationRequest(
            email_id=email_id,
            email_data=dict(email_data),
            classification=dict(classification),
            reason=normalize_manual_review_code(pipeline_result.get("safe_error_summary")),
            context=context_items,
            routing_log=tuple(routing_log),
        )
    else:
        await ctx.db_manager.update_status(
            email_id,
            None,
            routing_log=list(routing_log),
            original_draft=str(pipeline_result.get("draft") or ""),
        )
        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.QDRANT,
            1,
            {"operation": "update_email_labels", "email_id": email_id},
        )
        try:
            labels_updated = await asyncio.to_thread(
                ctx.email_processor.update_email_labels,
                email_id,
                pipeline_result.get("route_decision"),
                classification.get("priority", "P3"),
                classification.get("intent", "Unknown"),
                classification.get("need_reply"),
            )
            if _effect_boundary is not None and labels_updated is not True:
                raise GuardedExternalEffectFailed()
        except (ExternalEffectAuthorizationError, StaleFence, GuardedExternalEffectFailed):
            raise
        except Exception as exc:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            logger.warning("update_email_labels failed: error_type=%s", type(exc).__name__)
        kind = decide_notification_kind(dict(classification), dict(email_data))
        if kind == "skipped":
            await ctx.db_manager.update_status(email_id, "skipped")
            try:
                from src.observability.metrics import record_card_dispatch

                record_card_dispatch("skipped", True)
            except Exception:
                pass
            return None
        if kind == "approval":
            request = ApprovalRequest(
                email_id=email_id,
                email_data=dict(email_data),
                classification=dict(classification),
                draft=str(pipeline_result.get("draft") or ""),
                context=context_items,
                routing_log=tuple(routing_log),
            )
        elif kind == "read_only":
            request = ReadNotificationRequest(
                email_id=email_id,
                email_data=dict(email_data),
                classification=dict(classification),
                context=context_items,
                routing_log=tuple(routing_log),
            )
        else:
            raise ProcessingPolicyRejected()

    delivery = getattr(ctx, "email_feishu_delivery", None)
    deliver = getattr(delivery, "deliver", None)
    if not callable(deliver):
        raise ProcessingPolicyRejected()
    return await deliver(request, _effect_boundary)


async def _mark_email_read(
    email_id: str,
    ctx,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> None:
    """Mark email as read on Exchange server."""
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.EXCHANGE_MUTATION,
        0,
        {"operation": "mark_as_read", "email_id": email_id, "is_read": True},
    )
    try:
        success = await ctx.exchange_client.mark_as_read(email_id, is_read=True)
        if _effect_boundary is not None and success is not True:
            raise GuardedExternalEffectFailed()
        if success:
            logger.info(
                "Email marked read on Exchange: email=%s",
                fingerprint_identifier(email_id, namespace="email"),
            )
        else:
            logger.warning(
                "Exchange mark-read returned failure: email=%s",
                fingerprint_identifier(email_id, namespace="email"),
            )
    except Exception as exc:
        if _effect_boundary is not None:
            if isinstance(exc, GuardedExternalEffectFailed):
                raise
            raise GuardedExternalEffectFailed() from None
        logger.error("Mark-as-read failed: error_type=%s", type(exc).__name__)


async def process_and_archive_email(
    email_data,
    ctx,
    skip_analysis: bool = False,
    force_reprocess: bool = False,
) -> ProcessingOutcome:
    """
    Process a single email based on route decision.

    - skip_analysis=False: ingest -> AI -> conditional upload -> notify -> mark_read
    - skip_analysis=True: ingest only -> mark archived (no upload/AI/notify/mark_read)
    - force_reprocess=True: proceed even if email already exists in DB
    """
    return await _process_email_entry(
        email_data,
        ctx,
        skip_analysis,
        force_reprocess,
        effect_boundary=None,
    )


async def process_and_archive_email_guarded(
    email_data,
    ctx,
    skip_analysis: bool = False,
    force_reprocess: bool = False,
    *,
    before_external_effect: BeforeExternalEffect,
    effect_scope: ProcessingEffectScope,
) -> ProcessingOutcome:
    """Run the email processor with a mandatory fenced external-effect port."""
    settings = get_settings()
    if (
        type(effect_scope) is not ProcessingEffectScope
        or type(settings.EXCHANGE_ACCOUNT_ID) is not int
        or settings.EXCHANGE_ACCOUNT_ID <= 0
        or effect_scope.account_id != settings.EXCHANGE_ACCOUNT_ID
        or type(email_data) is not dict
        or email_data.get("id") != effect_scope.external_email_id
    ):
        raise ProcessingPolicyRejected()
    boundary = ExternalEffectBoundary(effect_scope, before_external_effect)
    return await _process_email_entry(
        email_data,
        ctx,
        skip_analysis,
        force_reprocess,
        effect_boundary=boundary,
    )


async def _process_email_entry(
    email_data,
    ctx,
    skip_analysis: bool,
    force_reprocess: bool,
    *,
    effect_boundary: ExternalEffectBoundary | None,
) -> ProcessingOutcome:
    validate_email_input(
        email_data,
        input_limits_from_settings(get_settings()),
        require_graph_metadata=True,
    )
    thread_id = email_data["id"]
    config = {"configurable": {"thread_id": thread_id}}

    from src.utils.logging_setup import log_email_context

    with log_email_context(thread_id):
        return await _process_and_archive_email_inner(
            email_data,
            ctx,
            skip_analysis,
            force_reprocess,
            thread_id,
            config,
            **_effect_boundary_kwargs(effect_boundary),
        )


async def _process_and_archive_email_inner(
    email_data,
    ctx,
    skip_analysis,
    force_reprocess,
    thread_id,
    config,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> ProcessingOutcome:
    event_type = email_data.get("_event_type", "unknown")
    logger.info(
        "Starting email processing: email=%s event=%s skip_analysis=%s force=%s",
        fingerprint_identifier(thread_id, namespace="email"),
        event_type,
        skip_analysis,
        force_reprocess,
    )

    # Initialize Draft Recipients (Reply Logic)
    if "draft_to" not in email_data:
        email_data["draft_to"] = (
            [email_data.get("sender")] if email_data.get("sender") else []
        )

    if "draft_cc" not in email_data:
        email_data["draft_cc"] = email_data.get("cc", [])

    initial_write = await ctx.db_manager.log_initial_email(email_data)
    if initial_write is InitialEmailWriteResult.DUPLICATE and not force_reprocess:
        logger.info(
            "Email already exists in database: email=%s",
            fingerprint_identifier(thread_id, namespace="email"),
        )
        if not skip_analysis and _effect_boundary is None:
            status = await ctx.db_manager.get_email_status(thread_id)
            if status in SAFE_DUPLICATE_READ_STATUSES:
                await _mark_email_read(thread_id, ctx)
        return ProcessingOutcome.DUPLICATE

    await _ensure_durable_content_ref(
        thread_id,
        email_data,
        ctx,
        reuse_existing=(
            force_reprocess and initial_write is InitialEmailWriteResult.DUPLICATE
        ),
        **_effect_boundary_kwargs(_effect_boundary),
    )

    logger.info(
        "Email logged as pending: email=%s",
        fingerprint_identifier(thread_id, namespace="email"),
    )

    if skip_analysis:
        await _archive_only(
            thread_id,
            email_data,
            ctx,
            event_type,
            **_effect_boundary_kwargs(_effect_boundary),
        )
        return ProcessingOutcome.ARCHIVED

    return await _run_ai_path(
        thread_id,
        email_data,
        ctx,
        config,
        **_effect_boundary_kwargs(_effect_boundary),
    )


async def _delete_unclaimed_content_candidate(
    ref: ContentRef,
    ctx,
    *,
    reason: str,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> None:
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.CONTENT,
        2,
        _content_ref_effect_target("delete_unclaimed_content", ref),
    )
    try:
        await ctx.content_store.delete(ref)
    except asyncio.CancelledError:
        if _effect_boundary is not None:
            raise
        logger.error("Unclaimed content cleanup was cancelled: reason=%s", reason)
    except Exception as cleanup_exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.error(
            "Unclaimed content cleanup failed: reason=%s error_type=%s",
            reason,
            type(cleanup_exc).__name__,
        )


def _log_content_persistence_failure(stage: str, error: BaseException) -> None:
    """Emit bounded diagnostics without content, identifiers, refs, or values."""

    logger.error(
        "Content persistence stage failed: stage=%s error_type=%s",
        stage,
        type(error).__name__,
    )


async def _ensure_durable_content_ref(
    email_id: str,
    email_data: dict,
    ctx,
    *,
    reuse_existing: bool,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> ContentRef:
    """Persist content and its typed DB ref before any downstream operation."""
    if reuse_existing:
        try:
            existing = await ctx.db_manager.get_content_ref(email_id)
        except asyncio.CancelledError:
            raise
        except Exception as read_exc:
            _log_content_persistence_failure("content_ref_readback", read_exc)
            raise
        if existing is not None:
            try:
                return _require_owned_ref(existing)
            except Exception as validation_exc:
                _log_content_persistence_failure(
                    "content_ref_validation",
                    validation_exc,
                )
                raise

    settings = get_settings()
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.CONTENT,
        0,
        {
            "operation": "put_email_content",
            "account_id": settings.EXCHANGE_ACCOUNT_ID,
            "email_id": email_id,
        },
    )
    try:
        ref = await ctx.content_store.put_email(
            settings.EXCHANGE_ACCOUNT_ID,
            email_id,
            email_data,
        )
    except asyncio.CancelledError:
        raise
    except Exception as put_exc:
        _log_content_persistence_failure("content_put", put_exc)
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        raise
    try:
        ref = _require_owned_ref(ref)
    except Exception as validation_exc:
        _log_content_persistence_failure(
            "content_ref_validation",
            validation_exc,
        )
        raise
    try:
        claimed = await ctx.db_manager.set_content_ref_if_absent(email_id, ref)
    except asyncio.CancelledError as cancel_exc:
        if _effect_boundary is not None:
            raise
        # The CAS may have committed before cancellation was observed.  Read
        # back before deciding whether this attempt's object is unclaimed.
        try:
            persisted_ref = await ctx.db_manager.get_content_ref(email_id)
        except asyncio.CancelledError:
            logger.error("Content reference cancellation read-back was cancelled")
            raise cancel_exc from None
        except Exception as read_exc:
            _log_content_persistence_failure("content_ref_readback", read_exc)
            raise cancel_exc from None

        if persisted_ref is not None:
            try:
                persisted_ref = _require_owned_ref(persisted_ref)
            except Exception as validation_exc:
                if isinstance(persisted_ref, ContentRef) and persisted_ref != ref:
                    await _delete_unclaimed_content_candidate(
                        ref,
                        ctx,
                        reason="cancelled_foreign_winner",
                        _effect_boundary=_effect_boundary,
                    )
                logger.error(
                    "Content reference cancellation winner invalid: error_type=%s",
                    type(validation_exc).__name__,
                )
                raise cancel_exc from None

        if persisted_ref is None or persisted_ref != ref:
            await _delete_unclaimed_content_candidate(
                ref,
                ctx,
                reason="cancelled_unclaimed_candidate",
                _effect_boundary=_effect_boundary,
            )
        raise cancel_exc from None
    except Exception as write_exc:
        _log_content_persistence_failure("content_ref_cas", write_exc)
        if _effect_boundary is not None:
            raise
        try:
            persisted_ref = await ctx.db_manager.get_content_ref(email_id)
        except Exception as read_exc:
            _log_content_persistence_failure("content_ref_readback", read_exc)
            raise write_exc from None

        if persisted_ref is not None:
            try:
                persisted_ref = _require_owned_ref(persisted_ref)
            except Exception:
                await _delete_unclaimed_content_candidate(
                    ref,
                    ctx,
                    reason="ambiguous_foreign_winner",
                    _effect_boundary=_effect_boundary,
                )
                raise
            if persisted_ref == ref:
                logger.warning("Content reference commit confirmed by read-back")
                return ref
            await _delete_unclaimed_content_candidate(
                ref,
                ctx,
                reason="ambiguous_concurrent_winner",
                _effect_boundary=_effect_boundary,
            )
            return persisted_ref

        await _delete_unclaimed_content_candidate(
            ref,
            ctx,
            reason="ambiguous_unclaimed_candidate",
            _effect_boundary=_effect_boundary,
        )
        raise write_exc from None

    if claimed:
        return ref

    try:
        persisted_ref = await ctx.db_manager.get_content_ref(email_id)
    except asyncio.CancelledError as cancel_exc:
        if _effect_boundary is not None:
            raise
        # CAS=False proves this candidate was never claimed, so it is safe to
        # delete even though reading the concurrent winner was cancelled.
        await _delete_unclaimed_content_candidate(
            ref,
            ctx,
            reason="false_claim_cancelled_readback",
            _effect_boundary=_effect_boundary,
        )
        raise cancel_exc from None
    except Exception as read_exc:
        _log_content_persistence_failure("content_ref_readback", read_exc)
        if _effect_boundary is not None:
            raise
        # A False CAS result proves this candidate was not claimed.  It is safe
        # to remove even though reading the concurrent winner failed.
        await _delete_unclaimed_content_candidate(
            ref,
            ctx,
            reason="false_claim_failed_readback",
            _effect_boundary=_effect_boundary,
        )
        raise read_exc from None
    if persisted_ref is None:
        await _delete_unclaimed_content_candidate(
            ref,
            ctx,
            reason="false_claim_unresolved",
            _effect_boundary=_effect_boundary,
        )
        unresolved = DatabaseOperationError(
            operation="set_content_ref_if_absent",
            retryable=True,
            message="content reference claim unresolved",
        )
        _log_content_persistence_failure("content_ref_readback", unresolved)
        raise unresolved
    try:
        persisted_ref = _require_owned_ref(persisted_ref)
    except Exception:
        await _delete_unclaimed_content_candidate(
            ref,
            ctx,
            reason="false_claim_foreign_winner",
            _effect_boundary=_effect_boundary,
        )
        raise
    if persisted_ref == ref:
        return ref
    await _delete_unclaimed_content_candidate(
        ref,
        ctx,
        reason="false_claim_concurrent_winner",
        _effect_boundary=_effect_boundary,
    )
    return persisted_ref


async def _archive_only(
    thread_id: str,
    email_data: dict,
    ctx,
    event_type: str,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> None:
    """Archive-folder route: ingest into Qdrant only; never touch mark_as_read."""
    await _ingest_to_qdrant(
        thread_id,
        email_data,
        ctx,
        _effect_boundary=_effect_boundary,
    )
    await ctx.db_manager.update_status(thread_id, "archived")
    logger.info(
        "Email archived to Qdrant: email=%s event=%s",
        fingerprint_identifier(thread_id, namespace="email"),
        event_type,
    )


async def _snapshot_cleanup_handles(
    email_id: str,
    ctx,
) -> CleanupHandleSnapshot:
    config = {"configurable": {"thread_id": email_id}}
    state = await ctx.graph.aget_state(config)
    values = getattr(state, "values", None)
    if not isinstance(values, Mapping) or not values:
        return CleanupHandleSnapshot()

    attachment_tokens = cap_identifier_list(
        values.get("attachment_tokens") or [],
        field="attachment_token",
        max_items=MAX_TOKENS,
        max_item_bytes=MAX_ID_BYTES,
        reject_excess=True,
    )
    pdf_token = values.get("pdf_token")
    if pdf_token is not None:
        pdf_token = cap_identifier_list(
            [pdf_token],
            field="pdf_token",
            max_items=1,
            max_item_bytes=MAX_ID_BYTES,
            reject_excess=True,
        )[0]
    return CleanupHandleSnapshot(
        attachment_tokens=tuple(attachment_tokens),
        pdf_token=pdf_token,
    )


async def _checkpoint_ai_path_resources(
    email_id: str,
    email_data: Mapping[str, object],
    ref: ContentRef,
    ctx,
    config: dict,
    *,
    attachment_tokens: list[str],
    pdf_token: str | None,
    _state_lock_held: bool = False,
) -> CleanupHandleSnapshot:
    """Create and read back a restartable slim cleanup checkpoint."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            return await _checkpoint_ai_path_resources(
                email_id,
                email_data,
                ref,
                ctx,
                config,
                attachment_tokens=attachment_tokens,
                pdf_token=pdf_token,
                _state_lock_held=True,
            )
    current = await _snapshot_cleanup_handles(email_id, ctx)
    requested_tokens = cap_identifier_list(
        attachment_tokens,
        field="attachment_token",
        max_items=MAX_TOKENS,
        max_item_bytes=MAX_ID_BYTES,
        reject_excess=True,
    )
    merged_tokens = list(dict.fromkeys([*current.attachment_tokens, *requested_tokens]))
    merged_tokens = cap_identifier_list(
        merged_tokens,
        field="attachment_token",
        max_items=MAX_TOKENS,
        max_item_bytes=MAX_ID_BYTES,
        reject_excess=True,
    )
    retained_pdf_token = current.pdf_token or pdf_token
    state = build_initial_graph_state(email_data, ref)
    state.update(
        sanitize_graph_delta(
            state,
            {
                "attachment_tokens": merged_tokens,
                "pdf_token": retained_pdf_token,
            },
        )
    )
    write_error: Exception | None = None
    try:
        await ctx.graph.aupdate_state(
            config,
            state,
            as_node="__start__",
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        write_error = exc

    try:
        confirmed_state = await ctx.graph.aget_state(config)
        confirmed_values = getattr(confirmed_state, "values", None)
        if not isinstance(confirmed_values, Mapping):
            raise ValueError("invalid_cleanup_checkpoint")
        confirmed_tokens = cap_identifier_list(
            confirmed_values.get("attachment_tokens") or [],
            field="attachment_token",
            max_items=MAX_TOKENS,
            max_item_bytes=MAX_ID_BYTES,
            reject_excess=True,
        )
        confirmed_pdf_token = confirmed_values.get("pdf_token")
    except Exception:
        if write_error is not None:
            raise write_error from None
        raise
    checkpoint_confirmed = (
        bool(confirmed_values)
        and confirmed_values.get("email_id") == email_id
        and confirmed_values.get("content_ref") == state["content_ref"]
    )
    tokens_confirmed = set(merged_tokens).issubset(confirmed_tokens)
    pdf_confirmed = (
        retained_pdf_token is None or confirmed_pdf_token == retained_pdf_token
    )
    if checkpoint_confirmed and tokens_confirmed and pdf_confirmed:
        return CleanupHandleSnapshot(
            attachment_tokens=tuple(confirmed_tokens),
            pdf_token=(
                confirmed_pdf_token if isinstance(confirmed_pdf_token, str) else None
            ),
        )
    if write_error is not None:
        raise write_error from None
    raise DatabaseOperationError(
        operation="checkpoint_cleanup_handles",
        retryable=True,
        message="cleanup handle checkpoint not confirmed",
    )


async def _run_ai_path(
    thread_id: str,
    email_data: dict,
    ctx,
    config: dict,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> ProcessingOutcome:
    """
    Inbox route: ingest -> AI -> conditional attachment upload -> notify.

    Mark-as-read is only fired AFTER user-facing delivery (Lark card or explicit
    skip) is confirmed. On dispatch failure the email stays unread on Exchange
    so durable recovery or a human can retry without losing visibility.
    """
    baseline = CleanupHandleSnapshot()
    try:
        baseline = await _snapshot_cleanup_handles(thread_id, ctx)
        ref = _require_owned_ref(await ctx.db_manager.get_content_ref(thread_id))
        baseline = await _checkpoint_ai_path_resources(
            thread_id,
            email_data,
            ref,
            ctx,
            config,
            attachment_tokens=list(baseline.attachment_tokens),
            pdf_token=baseline.pdf_token,
        )

        await _ingest_to_qdrant(
            thread_id,
            email_data,
            ctx,
            **_effect_boundary_kwargs(_effect_boundary),
        )
        pipeline_result = await _run_ai_pipeline(
            thread_id,
            ctx,
            config,
            attachment_tokens=[],
            preserved_attachment_tokens=list(baseline.attachment_tokens),
            preserved_pdf_token=baseline.pdf_token,
            **_effect_boundary_kwargs(_effect_boundary),
        )
        if pipeline_result is None:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed()
            await ctx.db_manager.update_status(thread_id, "error")
            return ProcessingOutcome.FAILED

        delivery_outcome = await _deliver_pipeline_result(
            thread_id,
            pipeline_result,
            ctx,
            _effect_boundary=_effect_boundary,
        )
        if delivery_outcome is None:
            await _mark_email_read(
                thread_id,
                ctx,
                **_effect_boundary_kwargs(_effect_boundary),
            )
            await _advance_canonical_handoff(
                ctx,
                _effect_boundary=_effect_boundary,
                expected_state="planned",
                next_state="completed",
            )
            return ProcessingOutcome.PROCESSED

        if delivery_outcome.disposition is EmailDeliveryDisposition.KNOWN_FAILURE:
            logger.warning(
                "Skipping mark-read after known delivery failure: email=%s kind=%s",
                fingerprint_identifier(thread_id, namespace="email"),
                delivery_outcome.kind,
            )
            return ProcessingOutcome.FAILED

        if delivery_outcome.kind.value == "manual_review":
            await _advance_canonical_handoff(
                ctx,
                _effect_boundary=_effect_boundary,
                expected_state="effect_committed",
                next_state="completed",
            )
            return ProcessingOutcome.MANUAL_REVIEW

        if delivery_outcome.disposition is EmailDeliveryDisposition.UNKNOWN:
            await _advance_canonical_handoff(
                ctx,
                _effect_boundary=_effect_boundary,
                expected_state="effect_committed",
                next_state="completed",
            )
            return ProcessingOutcome.MANUAL_REVIEW

        await _mark_email_read(
            thread_id,
            ctx,
            **_effect_boundary_kwargs(_effect_boundary),
        )
        await _advance_canonical_handoff(
            ctx,
            _effect_boundary=_effect_boundary,
            expected_state="effect_committed",
            next_state="completed",
        )
        return ProcessingOutcome.PROCESSED
    except EmailDeliverySideEffectCommittedError as committed:
        logger.error(
            "Email delivery state persistence failed after card acceptance: "
            "kind=%s error_type=%s",
            committed.kind,
            type(committed.cause).__name__,
        )
        raise committed.cause from None
    except asyncio.CancelledError:
        raise
    except DatabaseOperationError:
        raise
    except (ExternalEffectAuthorizationError, StaleFence, GuardedExternalEffectFailed):
        raise
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.error(
            "Pipeline failed; leaving unread: error_type=%s",
            type(exc).__name__,
        )
        await ctx.db_manager.update_status(thread_id, "error")
        return ProcessingOutcome.FAILED
