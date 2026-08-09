import asyncio
import hashlib
import logging
from copy import deepcopy
from dataclasses import dataclass
from collections.abc import Awaitable, Callable, Mapping
from src.config import get_settings
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
from src.safety.attachments import AttachmentPolicy
from src.safety.input_limits import input_limits_from_settings, validate_email_input
from src.safety.manual_review import (
    build_manual_review_delta,
    normalize_manual_review_code,
)
from src.security.redaction import fingerprint_identifier
from src.utils import lark_app
from src.utils.email_attachments import select_business_attachments
from src.utils.lark_pdf_flow import PdfFlowOutcome
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
MANUAL_REVIEW_SOURCE_STATUSES = frozenset(
    {
        "pending",
        "recovering",
        "ingested",
        "analyzed",
        "drafted",
        "error",
        "delivery_failed",
    }
)


@dataclass(frozen=True)
class AttachmentUploadProjection:
    tokens: tuple[str, ...]
    links: tuple[dict[str, str], ...]


def _apply_attachment_upload_links(
    email_data: object,
    links: tuple[dict[str, str], ...] | list[dict[str, str]],
) -> None:
    """Decorate a transient email projection with uploaded business attachments."""
    if not isinstance(email_data, dict):
        return
    attachments = email_data.get("attachments")
    if not isinstance(attachments, list):
        return

    remaining_links = [dict(link) for link in links]
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        for index, link in enumerate(remaining_links):
            if link.get("name") == str(attachment.get("name", "unknown")):
                attachment["lark_file_url"] = link.get("lark_file_url", "")
                remaining_links.pop(index)
                break


@dataclass(frozen=True)
class CleanupHandleSnapshot:
    attachment_tokens: tuple[str, ...] = ()
    pdf_token: str | None = None


@dataclass(frozen=True)
class NotificationPdfStage:
    """Result of reconciling a notification PDF with slim Graph state."""

    ready: bool
    url: str | None = None
    old_token: str | None = None
    new_token: str | None = None
    error_code: str | None = None


class NotificationSideEffectCommittedError(RuntimeError):
    """Internal signal that a card was sent before local persistence failed."""

    def __init__(self, *, kind: str, cause: BaseException):
        super().__init__("notification side effect committed")
        self.kind = kind
        self.cause = cause


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


def _identifier_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="surrogatepass")).hexdigest()


def _effect_boundary_kwargs(
    boundary: ExternalEffectBoundary | None,
) -> dict[str, ExternalEffectBoundary]:
    return {} if boundary is None else {"_effect_boundary": boundary}


async def _upload_attachments_to_lark(
    email_data: dict,
    *,
    max_uploads: int = MAX_TOKENS,
    acknowledge_token: Callable[[str], Awaitable[None]] | None = None,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> AttachmentUploadProjection:
    """Upload attachments, durably ACKing each token before the next upload."""
    if (
        isinstance(max_uploads, bool)
        or not isinstance(max_uploads, int)
        or not 0 <= max_uploads <= MAX_TOKENS
    ):
        raise ValueError("invalid_attachment_upload_capacity")
    attachments = email_data.get("attachments", [])
    if not attachments or max_uploads == 0:
        return AttachmentUploadProjection(tokens=(), links=())
    business_attachments = select_business_attachments(email_data)
    if not business_attachments:
        return AttachmentUploadProjection(tokens=(), links=())
    logger.info(
        "Email attachment projection: total=%d business=%d",
        len(attachments),
        len(business_attachments),
    )
    tokens: list[str] = []
    links: list[dict[str, str]] = []
    attachment_policy = AttachmentPolicy(
        max_bytes=input_limits_from_settings(
            get_settings()
        ).attachment_single_bytes
    )
    attempted_uploads = 0

    for ordinal, att in enumerate(business_attachments):
        if attempted_uploads >= max_uploads:
            break
        decision = attachment_policy.assess(att)
        if not decision.allowed or decision.content is None:
            logger.warning(
                "Attachment withheld from Lark Drive: reason=%s",
                decision.reason or "attachment_policy_rejected",
            )
            continue
        content_bytes = decision.content
        attempted_uploads += 1
        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.FEISHU,
            ordinal,
            {
                "operation": "upload_attachment",
                "content_sha256": hashlib.sha256(content_bytes).hexdigest(),
                "size": len(content_bytes),
            },
        )
        try:
            res = await asyncio.to_thread(
                lark_app.upload_file_to_drive,
                decision.name,
                content_bytes,
                len(content_bytes),
            )
        except Exception as exc:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            logger.error(
                "Attachment upload failed: error_type=%s",
                type(exc).__name__,
            )
            break
        token = res.get("file_token") if res else None
        if _effect_boundary is not None and not (isinstance(token, str) and token):
            raise GuardedExternalEffectFailed()
        if isinstance(token, str) and token:
            tokens.append(token)
            if acknowledge_token is not None:
                await acknowledge_token(token)
            url = res.get("url")
            if isinstance(url, str) and url:
                links.append(
                    {
                        "name": decision.name,
                        "lark_file_url": url,
                    }
                )
            logger.info("Attachment uploaded to Lark Drive")
    return AttachmentUploadProjection(tokens=tuple(tokens), links=tuple(links))


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
    attachment_links: list[dict[str, str]] | None = None,
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
                attachment_links=attachment_links,
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
        if attachment_links:
            _apply_attachment_upload_links(projection_email, attachment_links)
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


async def _stage_notification_pdf(
    email_id: str,
    ctx,
    pdf_result: object,
    *,
    _state_lock_held: bool = False,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> NotificationPdfStage:
    """Persist a PDF token, reconciling ambiguous writes before any card send."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            return await _stage_notification_pdf(
                email_id,
                ctx,
                pdf_result,
                _state_lock_held=True,
                _effect_boundary=_effect_boundary,
            )
    if pdf_result is None:
        return NotificationPdfStage(ready=True)
    if isinstance(pdf_result, PdfFlowOutcome):
        tracked = True
        for token in pdf_result.cleanup_tokens:
            if not await _retain_cleanup_token(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
            ):
                tracked = False
        if pdf_result.protected_tokens:
            try:
                state = await ctx.graph.aget_state(
                    {"configurable": {"thread_id": email_id}}
                )
                values = state.values
                known_tokens = set(values.get("attachment_tokens") or [])
                pdf_token = values.get("pdf_token")
                if isinstance(pdf_token, str):
                    known_tokens.add(pdf_token)
                tracked = tracked and all(
                    token in known_tokens for token in pdf_result.protected_tokens
                )
            except Exception as exc:
                logger.error(
                    "Protected PDF handle reconciliation failed: error_type=%s",
                    type(exc).__name__,
                )
                tracked = False
        logger.error(
            "Notification PDF generation requires reconciliation: status=%s",
            pdf_result.status,
        )
        return NotificationPdfStage(
            ready=tracked,
            error_code=(None if tracked else "pdf_cleanup_handle_untracked"),
        )
    if not isinstance(pdf_result, Mapping):
        logger.error(
            "Notification PDF generation requires reconciliation: result_type=%s",
            type(pdf_result).__name__,
        )
        return NotificationPdfStage(
            ready=False,
            error_code="pdf_generation_reconciliation_required",
        )
    token = pdf_result.get("file_token")
    url = pdf_result.get("url")
    valid_token = (
        isinstance(token, str) and bool(token) and len(token.encode("utf-8")) <= 512
    )
    valid_url = isinstance(url, str) and bool(url) and len(url.encode("utf-8")) <= 2_048
    if not valid_token or not valid_url:
        if isinstance(token, str) and token:
            safely_reconciled = await _delete_drive_token_or_retain(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
                _effect_boundary=_effect_boundary,
            )
            if not safely_reconciled:
                return NotificationPdfStage(
                    ready=False,
                    error_code="invalid_pdf_cleanup_untracked",
                )
        return NotificationPdfStage(ready=True)

    config = {"configurable": {"thread_id": email_id}}
    old_token = None
    try:
        state = await ctx.graph.aget_state(config)
        values = state.values
        old_token = values.get("pdf_token")
        cleanup_tokens = list(values.get("attachment_tokens") or [])
        should_track_old = (
            isinstance(old_token, str)
            and bool(old_token)
            and old_token != token
            and old_token not in cleanup_tokens
        )
        if should_track_old and len(cleanup_tokens) >= MAX_TOKENS:
            safely_reconciled = await _delete_drive_token_or_retain(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
                _effect_boundary=_effect_boundary,
            )
            return NotificationPdfStage(
                ready=safely_reconciled,
                error_code=(
                    None if safely_reconciled else "pdf_replacement_capacity_exhausted"
                ),
            )
        delta: dict[str, object] = {"pdf_token": token}
        if should_track_old:
            delta["attachment_tokens"] = [*cleanup_tokens, old_token]
        update = sanitize_graph_delta(values, delta)
        await ctx.graph.aupdate_state(config, update)
    except (
        ExternalEffectAuthorizationError,
        StaleFence,
        GuardedExternalEffectFailed,
    ):
        raise
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.error(
            "Notification PDF token persistence failed: error_type=%s",
            type(exc).__name__,
        )
        try:
            current = await ctx.graph.aget_state(config)
            current_values = current.values
            current_token = current_values.get("pdf_token")
        except Exception as read_exc:
            logger.error(
                "Notification PDF state reconciliation failed: error_type=%s",
                type(read_exc).__name__,
            )
            return NotificationPdfStage(
                ready=False,
                error_code="pdf_state_write_ambiguous",
            )

        if current_token == token:
            if (
                isinstance(old_token, str)
                and old_token
                and old_token != token
                and not await _retain_cleanup_token(
                    email_id,
                    ctx,
                    old_token,
                    _state_lock_held=True,
                )
            ):
                return NotificationPdfStage(
                    ready=False,
                    error_code="pdf_replacement_handle_untracked",
                )
            return NotificationPdfStage(
                ready=True,
                url=url,
                old_token=old_token if isinstance(old_token, str) else None,
                new_token=token,
            )
        if current_token != old_token:
            safely_reconciled = await _delete_drive_token_or_retain(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
                _effect_boundary=_effect_boundary,
            )
            return NotificationPdfStage(
                ready=safely_reconciled,
                error_code=(
                    None
                    if safely_reconciled
                    else "pdf_state_write_conflict_cleanup_untracked"
                ),
            )
        safely_reconciled = await _delete_drive_token_or_retain(
            email_id,
            ctx,
            token,
            _state_lock_held=True,
            _effect_boundary=_effect_boundary,
        )
        return NotificationPdfStage(
            ready=safely_reconciled,
            error_code=(
                None if safely_reconciled else "pdf_state_write_cleanup_untracked"
            ),
        )
    return NotificationPdfStage(
        ready=True,
        url=url,
        old_token=old_token if isinstance(old_token, str) else None,
        new_token=token,
    )


async def _retain_cleanup_token(
    email_id: str,
    ctx,
    token: str,
    *,
    _state_lock_held: bool = False,
) -> bool:
    """Keep a bounded cleanup handle when a remote Drive deletion is inconclusive."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            return await _retain_cleanup_token(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
            )
    config = {"configurable": {"thread_id": email_id}}
    try:
        state = await ctx.graph.aget_state(config)
        values = state.values
        tokens = list(values.get("attachment_tokens") or [])
        if token in tokens:
            return True
        tokens.append(token)
        update = sanitize_graph_delta(values, {"attachment_tokens": tokens})
        await ctx.graph.aupdate_state(config, update)
        current = await ctx.graph.aget_state(config)
        return token in (current.values.get("attachment_tokens") or [])
    except Exception as exc:
        logger.error(
            "Remote cleanup handle persistence failed: error_type=%s",
            type(exc).__name__,
        )
        try:
            current = await ctx.graph.aget_state(config)
            return token in (current.values.get("attachment_tokens") or [])
        except Exception as read_exc:
            logger.error(
                "Remote cleanup handle reconciliation failed: error_type=%s",
                type(read_exc).__name__,
            )
            return False


async def _delete_drive_token_or_retain(
    email_id: str,
    ctx,
    token: str,
    *,
    _state_lock_held: bool = False,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> bool:
    """Return true only when a Drive token is deleted or durably tracked."""
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.FEISHU,
        40,
        {
            "operation": "delete_drive_file",
            "token_sha256": _identifier_digest(token),
        },
    )
    try:
        deleted = await asyncio.to_thread(lark_app.delete_file_from_drive, token)
        if _effect_boundary is not None and deleted is not True:
            raise GuardedExternalEffectFailed()
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.error(
            "Drive token cleanup failed: error_type=%s",
            type(exc).__name__,
        )
        deleted = False
    if deleted:
        return True
    return await _retain_cleanup_token(
        email_id,
        ctx,
        token,
        _state_lock_held=_state_lock_held,
    )


async def _remove_cleanup_token(
    email_id: str,
    ctx,
    token: str,
    *,
    _state_lock_held: bool = False,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> None:
    """Best-effort removal of a stale handle after confirmed remote deletion."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            await _remove_cleanup_token(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
                _effect_boundary=_effect_boundary,
            )
        return
    config = {"configurable": {"thread_id": email_id}}
    try:
        state = await ctx.graph.aget_state(config)
        values = state.values
        tokens = [
            item for item in (values.get("attachment_tokens") or []) if item != token
        ]
        if tokens == list(values.get("attachment_tokens") or []):
            return
        update = sanitize_graph_delta(values, {"attachment_tokens": tokens})
        await ctx.graph.aupdate_state(config, update)
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.warning(
            "Cleanup handle removal failed: error_type=%s",
            type(exc).__name__,
        )


async def _delete_replaced_pdf(
    email_id: str,
    ctx,
    old_token: str | None,
    new_token: str | None,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> bool:
    if not old_token:
        return True
    if old_token == new_token:
        return True
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.FEISHU,
        34,
        {
            "operation": "delete_replaced_pdf",
            "token_sha256": _identifier_digest(old_token),
        },
    )
    try:
        deleted = await asyncio.to_thread(
            lark_app.delete_file_from_drive,
            old_token,
        )
        if _effect_boundary is not None and deleted is not True:
            raise GuardedExternalEffectFailed()
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.error(
            "Replaced PDF cleanup failed: error_type=%s",
            type(exc).__name__,
        )
        deleted = False
    if deleted:
        await _remove_cleanup_token(
            email_id,
            ctx,
            old_token,
            _effect_boundary=_effect_boundary,
        )
        return True
    if _effect_boundary is not None:
        raise GuardedExternalEffectFailed()
    reconciled = await _retain_cleanup_token(email_id, ctx, old_token)
    if not reconciled:
        logger.error("Replaced PDF cleanup handle is untracked")
    return reconciled


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


async def _dispatch_manual_review_notification(
    email_id: str,
    pipeline_result: dict,
    ctx,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> dict:
    """Surface a fail-closed result without acknowledging the Exchange email.

    Returns ``{"delivered": bool, "pdf_token": str | None}``. ``pdf_token`` is
    the Drive handle the sent card links to (if any) so the caller preserves
    it during later best-effort cleanup instead of deleting it out from under
    the card that was just delivered.
    """
    safe_code = normalize_manual_review_code(
        pipeline_result.get("safe_error_summary")
    )
    email_data = pipeline_result.get("email")
    if not isinstance(email_data, dict):
        email_data = {}
    classification = pipeline_result.get("classification")
    if not isinstance(classification, dict):
        classification = {}
    routing_log = pipeline_result.get("routing_log", [])
    await _persist_canonical_route_decision(
        pipeline_result,
        ctx,
        _effect_boundary=_effect_boundary,
    )
    logger.info(
        "Sending Lark manual-review request: email=%s reason=%s",
        fingerprint_identifier(email_id, namespace="email"),
        safe_code,
    )
    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.FEISHU,
        32,
        {"operation": "generate_notification_pdf", "email_id": email_id},
    )
    try:
        pdf_result = await lark_app.generate_and_upload_pdf(email_id)
    except Exception:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        raise
    pdf_stage = await _stage_notification_pdf(
        email_id,
        ctx,
        pdf_result,
        _effect_boundary=_effect_boundary,
    )
    if _effect_boundary is not None and (
        not pdf_stage.ready or pdf_stage.new_token is None or pdf_stage.url is None
    ):
        raise GuardedExternalEffectFailed()
    if not pdf_stage.ready:
        logger.error(
            "Manual-review notification PDF staging failed: code=%s",
            pdf_stage.error_code,
        )
        return {"delivered": False, "pdf_token": None}

    await _authorize_external_effect(
        _effect_boundary,
        ExternalEffectKind.FEISHU,
        33,
        {
            "operation": "send_manual_review_card",
            "email_id": email_id,
            "reason": safe_code,
        },
    )
    try:
        delivery_result = await asyncio.to_thread(
            lark_app.send_manual_review_card,
            email_id=email_id,
            email_data=email_data,
            reason=safe_code,
            classification=classification,
            pdf_url=pdf_stage.url,
            routing_log=routing_log,
        )
    except Exception:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        raise
    if _effect_boundary is not None and delivery_result is not True:
        raise GuardedExternalEffectFailed()
    delivered = bool(delivery_result)
    if not delivered:
        logger.error(
            "Manual-review card delivery failed; leaving Exchange unread: email=%s",
            fingerprint_identifier(email_id, namespace="email"),
        )
        try:
            from src.observability.metrics import record_card_dispatch

            record_card_dispatch("manual_review", False)
        except Exception:
            pass
        return {"delivered": False, "pdf_token": pdf_stage.new_token}
    try:
        await _delete_replaced_pdf(
            email_id,
            ctx,
            pdf_stage.old_token,
            pdf_stage.new_token,
            _effect_boundary=_effect_boundary,
        )
    except asyncio.CancelledError as exc:
        raise NotificationSideEffectCommittedError(
            kind="manual_review",
            cause=exc,
        ) from None
    except Exception as exc:
        raise NotificationSideEffectCommittedError(
            kind="manual_review",
            cause=exc,
        ) from None
    try:
        from src.observability.metrics import record_card_dispatch

        record_card_dispatch("manual_review", True)
    except Exception:
        pass
    return {"delivered": True, "pdf_token": pdf_stage.new_token}


async def _dispatch_notification(
    email_id: str,
    pipeline_result: dict,
    ctx,
    config: dict,
    *,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> dict:
    """
    Send Lark card based on classification result.

    Returns a dispatch outcome dict so the caller can decide whether to
    irreversibly mark the email as read on Exchange. Shape::

        {"delivered": bool, "kind": "approval" | "read_only" | "skipped"}

    - ``delivered=True`` means the email is safe to mark-as-read on the server,
      because the user has either received an actionable card or the rule
      explicitly classifies the email as not worth surfacing.
    - ``delivered=False`` means card delivery failed and the email is still
      unread on Exchange so durable recovery or the next manual retry can
      retry without losing the email.
    """
    classification = pipeline_result.get("classification", {})
    priority = classification.get("priority", "P3")
    intent = classification.get("intent", "Unknown")
    routing_log = pipeline_result.get("routing_log", [])
    email_data = pipeline_result.get("email", {})
    kind = decide_notification_kind(classification, email_data)

    await _persist_canonical_route_decision(
        pipeline_result,
        ctx,
        _effect_boundary=_effect_boundary,
    )

    await ctx.db_manager.update_status(
        email_id,
        None,
        routing_log=routing_log,
        original_draft=pipeline_result.get("draft", ""),
    )

    # Tier 2 substrate: write classification/skill labels back into Qdrant
    # so future similar emails can vote on these skills via semantic retrieval.
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
            priority,
            intent,
            classification.get("need_reply"),
        )
        if _effect_boundary is not None and labels_updated is not True:
            raise GuardedExternalEffectFailed()
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        # Best-effort enrichment; never block notification on label writes.
        logger.warning(
            "update_email_labels failed: error_type=%s",
            type(exc).__name__,
        )

    if kind == "approval":
        logger.info(
            "Sending Lark approval request: email=%s",
            fingerprint_identifier(email_id, namespace="email"),
        )
        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.FEISHU,
            32,
            {"operation": "generate_notification_pdf", "email_id": email_id},
        )
        try:
            pdf_result = await lark_app.generate_and_upload_pdf(email_id)
        except Exception:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            raise
        pdf_stage = await _stage_notification_pdf(
            email_id,
            ctx,
            pdf_result,
            _effect_boundary=_effect_boundary,
        )
        if _effect_boundary is not None and (
            not pdf_stage.ready or pdf_stage.new_token is None or pdf_stage.url is None
        ):
            raise GuardedExternalEffectFailed()
        if not pdf_stage.ready:
            logger.error(
                "Approval notification PDF staging failed: code=%s",
                pdf_stage.error_code,
            )
            await ctx.db_manager.update_status(
                email_id,
                "delivery_failed",
                error_message="notification_pdf_stage_failed",
            )
            return {"delivered": False, "kind": "approval"}

        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.FEISHU,
            33,
            {"operation": "send_approval_card", "email_id": email_id},
        )
        try:
            delivery_result = await asyncio.to_thread(
                lark_app.send_approval_card,
                email_id=email_id,
                draft=pipeline_result.get("draft", ""),
                context=pipeline_result.get("context", []),
                email_data=pipeline_result.get("email", {}),
                classification=classification,
                pdf_url=pdf_stage.url,
                routing_log=routing_log,
            )
        except Exception:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            raise
        if _effect_boundary is not None and delivery_result is not True:
            raise GuardedExternalEffectFailed()
        delivered = bool(delivery_result)
        if delivered:
            try:
                await _delete_replaced_pdf(
                    email_id,
                    ctx,
                    pdf_stage.old_token,
                    pdf_stage.new_token,
                    _effect_boundary=_effect_boundary,
                )
                await ctx.db_manager.update_status(email_id, "waiting_approval")
            except asyncio.CancelledError as exc:
                raise NotificationSideEffectCommittedError(
                    kind="approval",
                    cause=exc,
                ) from None
            except Exception as exc:
                raise NotificationSideEffectCommittedError(
                    kind="approval",
                    cause=exc,
                ) from None
        else:
            logger.error(
                "Approval card delivery failed; leaving Exchange unread: email=%s",
                fingerprint_identifier(email_id, namespace="email"),
            )
            await ctx.db_manager.update_status(
                email_id,
                "delivery_failed",
                error_message="Approval card send returned failure",
            )
        try:
            from src.observability.metrics import record_card_dispatch

            record_card_dispatch("approval", delivered)
        except Exception:
            pass
        return {"delivered": delivered, "kind": "approval"}

    if kind == "read_only":
        logger.info(
            "Sending read-only Lark card: email=%s priority=%s intent=%s",
            fingerprint_identifier(email_id, namespace="email"),
            priority,
            intent,
        )
        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.FEISHU,
            32,
            {"operation": "generate_notification_pdf", "email_id": email_id},
        )
        try:
            pdf_result = await lark_app.generate_and_upload_pdf(email_id)
        except Exception:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            raise
        pdf_stage = await _stage_notification_pdf(
            email_id,
            ctx,
            pdf_result,
            _effect_boundary=_effect_boundary,
        )
        if _effect_boundary is not None and (
            not pdf_stage.ready or pdf_stage.new_token is None or pdf_stage.url is None
        ):
            raise GuardedExternalEffectFailed()
        if not pdf_stage.ready:
            logger.error(
                "Read-only notification PDF staging failed: code=%s",
                pdf_stage.error_code,
            )
            await ctx.db_manager.update_status(
                email_id,
                "delivery_failed",
                error_message="notification_pdf_stage_failed",
            )
            return {"delivered": False, "kind": "read_only"}

        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.FEISHU,
            33,
            {"operation": "send_read_only_card", "email_id": email_id},
        )
        try:
            delivery_result = await asyncio.to_thread(
                lark_app.send_read_only_card,
                email_id=email_id,
                context=pipeline_result.get("context", []),
                email_data=pipeline_result.get("email", {}),
                classification=classification,
                pdf_url=pdf_stage.url,
                routing_log=routing_log,
            )
        except Exception:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            raise
        if _effect_boundary is not None and delivery_result is not True:
            raise GuardedExternalEffectFailed()
        delivered = bool(delivery_result)
        if delivered:
            try:
                await _delete_replaced_pdf(
                    email_id,
                    ctx,
                    pdf_stage.old_token,
                    pdf_stage.new_token,
                    _effect_boundary=_effect_boundary,
                )
                await ctx.db_manager.update_status(email_id, "notified_readonly")
            except asyncio.CancelledError as exc:
                raise NotificationSideEffectCommittedError(
                    kind="read_only",
                    cause=exc,
                ) from None
            except Exception as exc:
                raise NotificationSideEffectCommittedError(
                    kind="read_only",
                    cause=exc,
                ) from None
        else:
            logger.error(
                "Read-only card delivery failed; leaving Exchange unread: email=%s",
                fingerprint_identifier(email_id, namespace="email"),
            )
            await ctx.db_manager.update_status(
                email_id,
                "delivery_failed",
                error_message="Read-only card send returned failure",
            )
        try:
            from src.observability.metrics import record_card_dispatch

            record_card_dispatch("read_only", delivered)
        except Exception:
            pass
        return {"delivered": delivered, "kind": "read_only"}

    logger.info(
        "Email requires no notification: email=%s",
        fingerprint_identifier(email_id, namespace="email"),
    )
    await ctx.db_manager.update_status(email_id, "skipped")
    try:
        from src.observability.metrics import record_card_dispatch

        record_card_dispatch("skipped", True)
    except Exception:
        pass
    # An intentional skip is a successful "delivery" (user does not need to see it).
    return {"delivered": True, "kind": "skipped"}


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


async def _cleanup_graph_drive_files(
    email_id: str,
    ctx,
    *,
    fallback_attachment_tokens: list[str],
    preserve_attachment_tokens: list[str] | None = None,
    preserve_pdf_token: str | None = None,
    _state_lock_held: bool = False,
    _effect_boundary: ExternalEffectBoundary | None = None,
) -> None:
    """Best-effort remote cleanup while retaining failed handles in slim State."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            await _cleanup_graph_drive_files(
                email_id,
                ctx,
                fallback_attachment_tokens=fallback_attachment_tokens,
                preserve_attachment_tokens=preserve_attachment_tokens,
                preserve_pdf_token=preserve_pdf_token,
                _state_lock_held=True,
                _effect_boundary=_effect_boundary,
            )
        return
    config = {"configurable": {"thread_id": email_id}}
    state = None
    values: Mapping[str, object] = {}
    try:
        state = await ctx.graph.aget_state(config)
        if state is not None and isinstance(getattr(state, "values", None), Mapping):
            values = state.values
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.warning(
            "Cleanup state lookup failed: error_type=%s",
            type(exc).__name__,
        )
    if _effect_boundary is not None and (state is None or not values):
        raise GuardedExternalEffectFailed()

    state_attachment_tokens = [
        token
        for token in (values.get("attachment_tokens") or [])
        if isinstance(token, str) and token
    ]
    all_attachment_tokens = list(
        dict.fromkeys([*state_attachment_tokens, *fallback_attachment_tokens])
    )
    preserved_attachment_tokens = list(dict.fromkeys(preserve_attachment_tokens or []))
    preserved_attachment_set = set(preserved_attachment_tokens)
    pdf_token = values.get("pdf_token")

    failed_attachment_tokens: list[str] = []
    for ordinal, token in enumerate(all_attachment_tokens):
        if token in preserved_attachment_set or token == preserve_pdf_token:
            continue
        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.FEISHU,
            64 + ordinal,
            {
                "operation": "cleanup_attachment",
                "token_sha256": _identifier_digest(token),
            },
        )
        try:
            deleted = await asyncio.to_thread(lark_app.delete_file_from_drive, token)
            if _effect_boundary is not None and deleted is not True:
                raise GuardedExternalEffectFailed()
        except Exception as exc:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            logger.error(
                "Drive cleanup failed: error_type=%s",
                type(exc).__name__,
            )
            deleted = False
        if not deleted:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed()
            failed_attachment_tokens.append(token)

    retained_pdf_token = preserve_pdf_token
    if isinstance(pdf_token, str) and pdf_token and pdf_token != preserve_pdf_token:
        await _authorize_external_effect(
            _effect_boundary,
            ExternalEffectKind.FEISHU,
            96,
            {
                "operation": "cleanup_pdf",
                "token_sha256": _identifier_digest(pdf_token),
            },
        )
        try:
            deleted = await asyncio.to_thread(
                lark_app.delete_file_from_drive,
                pdf_token,
            )
            if _effect_boundary is not None and deleted is not True:
                raise GuardedExternalEffectFailed()
        except Exception as exc:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed() from None
            logger.error(
                "PDF cleanup failed: error_type=%s",
                type(exc).__name__,
            )
            deleted = False
        if not deleted:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed()
            if preserve_pdf_token is None:
                retained_pdf_token = pdf_token
            else:
                failed_attachment_tokens.append(pdf_token)

    if state is None or not values:
        return
    retained_state_tokens = list(
        dict.fromkeys([*preserved_attachment_tokens, *failed_attachment_tokens])
    )
    try:
        update = sanitize_graph_delta(
            values,
            {
                "attachment_tokens": retained_state_tokens,
                "pdf_token": retained_pdf_token,
            },
        )
        update_kwargs = {}
        if tuple(getattr(state, "next", ())) == ("categorizer",):
            update_kwargs["as_node"] = "__start__"
        await ctx.graph.aupdate_state(config, update, **update_kwargs)
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.warning(
            "Cleanup state update failed: error_type=%s",
            type(exc).__name__,
        )
        return

    try:
        confirmed = await _snapshot_cleanup_handles(email_id, ctx)
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        logger.warning(
            "Cleanup state read-back failed: error_type=%s",
            type(exc).__name__,
        )
        return
    tokens_confirmed = set(retained_state_tokens).issubset(confirmed.attachment_tokens)
    pdf_confirmed = (
        retained_pdf_token is None or confirmed.pdf_token == retained_pdf_token
    )
    if not tokens_confirmed or not pdf_confirmed:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed()
        logger.warning("Cleanup state update was not confirmed")


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
    attachment_tokens: list[str] = []
    notification_committed = False
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
            attachment_links=[],
            **_effect_boundary_kwargs(_effect_boundary),
        )
        if pipeline_result is None:
            if _effect_boundary is not None:
                raise GuardedExternalEffectFailed()
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                preserve_attachment_tokens=list(baseline.attachment_tokens),
                preserve_pdf_token=baseline.pdf_token,
                **_effect_boundary_kwargs(_effect_boundary),
            )
            await ctx.db_manager.update_status(thread_id, "error")
            return ProcessingOutcome.FAILED

        if (
            pipeline_result.get("next_step") == "manual_review"
            or pipeline_result.get("approval_status") == "manual_review"
        ):
            safe_code = normalize_manual_review_code(
                pipeline_result.get("safe_error_summary")
            )
            manual_dispatch = await _dispatch_manual_review_notification(
                thread_id,
                pipeline_result,
                ctx,
                _effect_boundary=_effect_boundary,
            )
            manual_delivered = manual_dispatch.get("delivered", False)
            manual_pdf_token = manual_dispatch.get("pdf_token")
            if not manual_delivered:
                await _cleanup_graph_drive_files(
                    thread_id,
                    ctx,
                    fallback_attachment_tokens=attachment_tokens,
                    preserve_attachment_tokens=list(baseline.attachment_tokens),
                    preserve_pdf_token=baseline.pdf_token,
                    **_effect_boundary_kwargs(_effect_boundary),
                )
                await ctx.db_manager.update_status(
                    thread_id,
                    "delivery_failed",
                    error_message="manual_review_card_delivery_failed",
                )
                return ProcessingOutcome.FAILED
            notification_committed = True
            await _advance_canonical_handoff(
                ctx,
                _effect_boundary=_effect_boundary,
                expected_state="planned",
                next_state="effect_committed",
            )
            try:
                manual_persisted = await ctx.db_manager.compare_and_set_manual_review(
                    thread_id,
                    expected=MANUAL_REVIEW_SOURCE_STATUSES,
                    error_code=safe_code,
                )
            except DatabaseOperationError as claim_exc:
                logger.error(
                    "Manual-review persistence is ambiguous: error_type=%s",
                    type(claim_exc).__name__,
                )
                try:
                    manual_persisted = (
                        await ctx.db_manager.get_email_status(thread_id)
                        == "manual_review"
                    )
                except Exception as read_exc:
                    logger.error(
                        "Manual-review readback failed: error_type=%s",
                        type(read_exc).__name__,
                    )
                    raise NotificationSideEffectCommittedError(
                        kind="manual_review",
                        cause=read_exc,
                    ) from None
                if not manual_persisted:
                    raise NotificationSideEffectCommittedError(
                        kind="manual_review",
                        cause=claim_exc,
                    ) from None
            if manual_persisted is not True:
                failure = DatabaseOperationError(
                    operation="compare_and_set_manual_review",
                    retryable=False,
                    message="manual-review transition was not confirmed",
                )
                raise NotificationSideEffectCommittedError(
                    kind="manual_review",
                    cause=failure,
                )
            await _advance_canonical_handoff(
                ctx,
                _effect_boundary=_effect_boundary,
                expected_state="effect_committed",
                next_state="completed",
            )
            try:
                await _cleanup_graph_drive_files(
                    thread_id,
                    ctx,
                    fallback_attachment_tokens=attachment_tokens,
                    preserve_attachment_tokens=list(baseline.attachment_tokens),
                    preserve_pdf_token=manual_pdf_token,
                    **_effect_boundary_kwargs(_effect_boundary),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Manual-review cleanup failed: error_type=%s",
                    type(exc).__name__,
                )
            return ProcessingOutcome.MANUAL_REVIEW

        delivery_kind = decide_notification_kind(
            pipeline_result.get("classification") or {},
            pipeline_result.get("email") or {},
        )
        if delivery_kind in {"approval", "read_only"}:
            async def acknowledge_attachment_token(token: str) -> None:
                if token not in attachment_tokens:
                    attachment_tokens.append(token)
                retained = await _retain_cleanup_token(thread_id, ctx, token)
                if not retained:
                    raise DatabaseOperationError(
                        operation="track_attachment_upload",
                        retryable=True,
                        message="attachment cleanup handle not confirmed",
                    )

            attachment_uploads = await _upload_attachments_to_lark(
                email_data,
                max_uploads=MAX_TOKENS - len(baseline.attachment_tokens),
                acknowledge_token=acknowledge_attachment_token,
                **_effect_boundary_kwargs(_effect_boundary),
            )
            for token in attachment_uploads.tokens:
                if token not in attachment_tokens:
                    await acknowledge_attachment_token(token)
            _apply_attachment_upload_links(
                pipeline_result.get("email"),
                attachment_uploads.links,
            )

        dispatch_result = await _dispatch_notification(
            thread_id,
            pipeline_result,
            ctx,
            config,
            **_effect_boundary_kwargs(_effect_boundary),
        )
        notification_committed = bool(
            dispatch_result.get("delivered")
            and dispatch_result.get("kind") in {"approval", "read_only"}
        )
        if notification_committed:
            await _advance_canonical_handoff(
                ctx,
                _effect_boundary=_effect_boundary,
                expected_state="planned",
                next_state="effect_committed",
            )
        if not dispatch_result.get("delivered"):
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                preserve_attachment_tokens=list(baseline.attachment_tokens),
                preserve_pdf_token=baseline.pdf_token,
                **_effect_boundary_kwargs(_effect_boundary),
            )
        elif dispatch_result.get("kind") == "skipped":
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                **_effect_boundary_kwargs(_effect_boundary),
            )
        if dispatch_result.get("delivered"):
            await _mark_email_read(
                thread_id,
                ctx,
                **_effect_boundary_kwargs(_effect_boundary),
            )
            if dispatch_result.get("kind") in {"approval", "read_only"}:
                await _advance_canonical_handoff(
                    ctx,
                    _effect_boundary=_effect_boundary,
                    expected_state="effect_committed",
                    next_state="completed",
                )
            else:
                await _advance_canonical_handoff(
                    ctx,
                    _effect_boundary=_effect_boundary,
                    expected_state="planned",
                    next_state="completed",
                )
        else:
            logger.warning(
                "Skipping mark-read after delivery failure: email=%s kind=%s",
                fingerprint_identifier(thread_id, namespace="email"),
                dispatch_result.get("kind"),
            )
            return ProcessingOutcome.FAILED
        return ProcessingOutcome.PROCESSED
    except NotificationSideEffectCommittedError as committed:
        # The card already references the current attachment/PDF handles.
        # Preserve them even though the local status write must be retried.
        logger.error(
            "Notification status persistence failed after delivery: "
            "kind=%s error_type=%s",
            committed.kind,
            type(committed.cause).__name__,
        )
        raise committed.cause from None
    except asyncio.CancelledError:
        if _effect_boundary is None and not notification_committed:
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                preserve_attachment_tokens=list(baseline.attachment_tokens),
                preserve_pdf_token=baseline.pdf_token,
            )
        raise
    except DatabaseOperationError:
        if _effect_boundary is None:
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                preserve_attachment_tokens=list(baseline.attachment_tokens),
                preserve_pdf_token=baseline.pdf_token,
            )
        raise
    except (ExternalEffectAuthorizationError, StaleFence, GuardedExternalEffectFailed):
        raise
    except Exception as exc:
        if _effect_boundary is not None:
            raise GuardedExternalEffectFailed() from None
        await _cleanup_graph_drive_files(
            thread_id,
            ctx,
            fallback_attachment_tokens=attachment_tokens,
            preserve_attachment_tokens=list(baseline.attachment_tokens),
            preserve_pdf_token=baseline.pdf_token,
        )
        logger.error(
            "Pipeline failed; leaving unread: error_type=%s",
            type(exc).__name__,
        )
        await ctx.db_manager.update_status(thread_id, "error")
        return ProcessingOutcome.FAILED
