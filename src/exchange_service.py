import asyncio
import logging
import re
from copy import deepcopy
from dataclasses import dataclass
from collections.abc import Awaitable, Callable, Mapping
from src.config import get_settings
from src.domain.email_state import (
    SAFE_DUPLICATE_READ_STATUSES,
    InitialEmailWriteResult,
    ProcessingOutcome,
)
from src.domain.errors import DatabaseOperationError
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
from src.init_app import get_app_context
from src.safety.input_limits import input_limits_from_settings, validate_email_input
from src.utils import lark_app
from src.utils.lark_pdf_flow import PdfFlowOutcome
from src.utils.notification_policy import decide_notification_kind
from src.storage import ContentRef

logger = logging.getLogger("ExchangeService")
WORKER_CONCURRENCY = 3
WEBHOOK_QUEUE_MAXSIZE = 500


@dataclass(frozen=True)
class AttachmentUploadProjection:
    tokens: tuple[str, ...]
    links: tuple[dict[str, str], ...]


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


async def _upload_attachments_to_lark(
    email_data: dict,
    *,
    max_uploads: int = MAX_TOKENS,
    acknowledge_token: Callable[[str], Awaitable[None]] | None = None,
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
    logger.info("Email has %d attachments", len(attachments))
    tokens: list[str] = []
    links: list[dict[str, str]] = []
    import base64

    for att in attachments[:max_uploads]:
        if att.get("content"):
            try:
                content_bytes = base64.b64decode(att["content"], validate=True)
                res = lark_app.upload_file_to_drive(
                    att.get("name", "unknown"), content_bytes, len(content_bytes)
                )
            except Exception as exc:
                logger.error(
                    "Attachment upload failed: error_type=%s",
                    type(exc).__name__,
                )
                break
            token = res.get("file_token") if res else None
            if isinstance(token, str) and token:
                tokens.append(token)
                if acknowledge_token is not None:
                    await acknowledge_token(token)
                url = res.get("url")
                if isinstance(url, str) and url:
                    links.append(
                        {
                            "name": str(att.get("name", "unknown")),
                            "lark_file_url": url,
                        }
                    )
                logger.info("Attachment uploaded to Lark Drive")
    return AttachmentUploadProjection(tokens=tuple(tokens), links=tuple(links))


async def _ingest_to_qdrant(email_id: str, email_data: dict, ctx) -> None:
    """Ingest email into Qdrant vector store (sync call wrapped in thread)."""
    try:
        await asyncio.to_thread(ctx.email_processor.process_email, email_data)
        logger.info(f"Email {email_id} ingested to Qdrant.")
        await ctx.db_manager.update_status(email_id, "ingested")
    except DatabaseOperationError:
        raise
    except Exception as exc:
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

        async def consume(graph_input) -> None:
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
            raise RuntimeError("review_rewrite_limit_exceeded")

        state_values = state.values
        draft_id = state_values.get("draft_id")
        draft = (
            await ctx.db_manager.load_draft(
                require_owned_draft_id(state_values, draft_id)
            )
            if draft_id is not None
            else ""
        )
        projection_email = deepcopy(dict(email_data))
        projection_email["draft_to"] = list(state_values.get("draft_to") or [])
        projection_email["draft_cc"] = list(state_values.get("draft_cc") or [])
        if attachment_links and isinstance(projection_email.get("attachments"), list):
            remaining_links = [dict(link) for link in attachment_links]
            for attachment in projection_email["attachments"]:
                if not isinstance(attachment, dict):
                    continue
                for index, link in enumerate(remaining_links):
                    if link.get("name") == str(attachment.get("name", "unknown")):
                        attachment["lark_file_url"] = link.get("lark_file_url", "")
                        remaining_links.pop(index)
                        break
        return {
            "classification": state_values.get("classification", {}),
            "draft": draft,
            "context": state_values.get("context_summaries", []),
            "email": projection_email,
            "routing_log": state_values.get("routing_log", []),
            "active_skills": state_values.get("active_skills", []),
        }
    except DatabaseOperationError:
        raise
    except Exception as exc:
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
) -> NotificationPdfStage:
    """Persist a PDF token, reconciling ambiguous writes before any card send."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            return await _stage_notification_pdf(
                email_id,
                ctx,
                pdf_result,
                _state_lock_held=True,
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
        isinstance(token, str)
        and bool(token)
        and len(token.encode("utf-8")) <= 512
    )
    valid_url = (
        isinstance(url, str)
        and bool(url)
        and len(url.encode("utf-8")) <= 2_048
    )
    if not valid_token or not valid_url:
        if isinstance(token, str) and token:
            safely_reconciled = await _delete_drive_token_or_retain(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
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
            )
            return NotificationPdfStage(
                ready=safely_reconciled,
                error_code=(
                    None
                    if safely_reconciled
                    else "pdf_replacement_capacity_exhausted"
                ),
            )
        delta: dict[str, object] = {"pdf_token": token}
        if should_track_old:
            delta["attachment_tokens"] = [*cleanup_tokens, old_token]
        update = sanitize_graph_delta(values, delta)
        await ctx.graph.aupdate_state(config, update)
    except Exception as exc:
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
) -> bool:
    """Return true only when a Drive token is deleted or durably tracked."""
    try:
        deleted = await asyncio.to_thread(lark_app.delete_file_from_drive, token)
    except Exception as exc:
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
) -> None:
    """Best-effort removal of a stale handle after confirmed remote deletion."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            await _remove_cleanup_token(
                email_id,
                ctx,
                token,
                _state_lock_held=True,
            )
        return
    config = {"configurable": {"thread_id": email_id}}
    try:
        state = await ctx.graph.aget_state(config)
        values = state.values
        tokens = [
            item
            for item in (values.get("attachment_tokens") or [])
            if item != token
        ]
        if tokens == list(values.get("attachment_tokens") or []):
            return
        update = sanitize_graph_delta(values, {"attachment_tokens": tokens})
        await ctx.graph.aupdate_state(config, update)
    except Exception as exc:
        logger.warning(
            "Cleanup handle removal failed: error_type=%s",
            type(exc).__name__,
        )


async def _delete_replaced_pdf(
    email_id: str,
    ctx,
    old_token: str | None,
    new_token: str | None,
) -> bool:
    if not old_token:
        return True
    if old_token == new_token:
        return True
    try:
        deleted = await asyncio.to_thread(
            lark_app.delete_file_from_drive,
            old_token,
        )
    except Exception as exc:
        logger.error(
            "Replaced PDF cleanup failed: error_type=%s",
            type(exc).__name__,
        )
        deleted = False
    if deleted:
        await _remove_cleanup_token(email_id, ctx, old_token)
        return True
    reconciled = await _retain_cleanup_token(email_id, ctx, old_token)
    if not reconciled:
        logger.error("Replaced PDF cleanup handle is untracked")
    return reconciled


async def _dispatch_notification(email_id: str, pipeline_result: dict, ctx, config: dict) -> dict:
    """
    Send Lark card based on classification result.

    Returns a dispatch outcome dict so the caller can decide whether to
    irreversibly mark the email as read on Exchange. Shape::

        {"delivered": bool, "kind": "approval" | "read_only" | "skipped"}

    - ``delivered=True`` means the email is safe to mark-as-read on the server,
      because the user has either received an actionable card or the rule
      explicitly classifies the email as not worth surfacing.
    - ``delivered=False`` means card delivery failed and the email is still
      unread on Exchange so the SelfHealer (or the next manual retry) can
      retry without losing the email.
    """
    classification = pipeline_result.get("classification", {})
    priority = classification.get("priority", "P3")
    intent = classification.get("intent", "Unknown")
    routing_log = pipeline_result.get("routing_log", [])
    active_skills = pipeline_result.get("active_skills", [])
    email_data = pipeline_result.get("email", {})
    kind = decide_notification_kind(classification, email_data)

    await ctx.db_manager.update_status(
        email_id, None,
        routing_log=routing_log,
        active_skills=active_skills,
        original_draft=pipeline_result.get("draft", ""),
    )

    # Tier 2 substrate: write classification/skill labels back into Qdrant
    # so future similar emails can vote on these skills via semantic retrieval.
    try:
        await asyncio.to_thread(
            ctx.email_processor.update_email_labels,
            email_id,
            active_skills,
            priority,
            intent,
            classification.get("need_reply"),
        )
    except Exception as exc:
        # Best-effort enrichment; never block notification on label writes.
        logger.warning(
            "update_email_labels failed: error_type=%s",
            type(exc).__name__,
        )

    if kind == "approval":
        logger.info(f"Email requires reply. Sending Lark approval request: {email_id}")
        pdf_result = await lark_app.generate_and_upload_pdf(email_id)
        pdf_stage = await _stage_notification_pdf(
            email_id,
            ctx,
            pdf_result,
        )
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

        delivered = bool(lark_app.send_approval_card(
            email_id=email_id,
            draft=pipeline_result.get("draft", ""),
            context=pipeline_result.get("context", []),
            email_data=pipeline_result.get("email", {}),
            classification=classification,
            pdf_url=pdf_stage.url,
            routing_log=routing_log,
            active_skills=active_skills,
        ))
        if delivered:
            try:
                await _delete_replaced_pdf(
                    email_id,
                    ctx,
                    pdf_stage.old_token,
                    pdf_stage.new_token,
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
            logger.error("Approval card delivery failed for %s; leaving on Exchange unread.", email_id)
            await ctx.db_manager.update_status(
                email_id, "delivery_failed",
                error_message="Approval card send returned failure",
            )
        try:
            from src.observability.metrics import record_card_dispatch
            record_card_dispatch("approval", delivered)
        except Exception:
            pass
        return {"delivered": delivered, "kind": "approval"}

    if kind == "read_only":
        logger.info(f"Email is read-worthy ({priority}/{intent}) but no reply needed. Sending Read-Only card: {email_id}")
        pdf_result = await lark_app.generate_and_upload_pdf(email_id)
        pdf_stage = await _stage_notification_pdf(
            email_id,
            ctx,
            pdf_result,
        )
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

        delivered = bool(lark_app.send_read_only_card(
            email_id=email_id,
            context=pipeline_result.get("context", []),
            email_data=pipeline_result.get("email", {}),
            classification=classification,
            pdf_url=pdf_stage.url,
            routing_log=routing_log,
            active_skills=active_skills,
        ))
        if delivered:
            try:
                await _delete_replaced_pdf(
                    email_id,
                    ctx,
                    pdf_stage.old_token,
                    pdf_stage.new_token,
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
            logger.error("Read-only card delivery failed for %s; leaving on Exchange unread.", email_id)
            await ctx.db_manager.update_status(
                email_id, "delivery_failed",
                error_message="Read-only card send returned failure",
            )
        try:
            from src.observability.metrics import record_card_dispatch
            record_card_dispatch("read_only", delivered)
        except Exception:
            pass
        return {"delivered": delivered, "kind": "read_only"}

    logger.info(f"No reply needed for email: {email_id}")
    await ctx.db_manager.update_status(email_id, "skipped")
    try:
        from src.observability.metrics import record_card_dispatch
        record_card_dispatch("skipped", True)
    except Exception:
        pass
    # An intentional skip is a successful "delivery" (user does not need to see it).
    return {"delivered": True, "kind": "skipped"}


async def _mark_email_read(email_id: str, ctx) -> None:
    """Mark email as read on Exchange server."""
    try:
        success = await ctx.exchange_client.mark_as_read(email_id, is_read=True)
        if success:
            logger.info(f"Email {email_id} marked as read on server.")
        else:
            logger.warning(f"Failed to mark {email_id} as read.")
    except Exception as exc:
        logger.error("Mark-as-read failed: error_type=%s", type(exc).__name__)


async def process_and_archive_email(
    email_data,
    ctx,
    skip_analysis: bool = False,
    force_reprocess: bool = False,
) -> ProcessingOutcome:
    """
    Process a single email based on route decision.

    - skip_analysis=False: upload -> ingest -> AI -> notify -> mark_read
    - skip_analysis=True: ingest only -> mark archived (no upload/AI/notify/mark_read)
    - force_reprocess=True: proceed even if email already exists in DB
    """
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
            email_data, ctx, skip_analysis, force_reprocess, thread_id, config
        )


async def _process_and_archive_email_inner(
    email_data, ctx, skip_analysis, force_reprocess, thread_id, config
) -> ProcessingOutcome:
    event_type = email_data.get("_event_type", "unknown")
    folder_name = email_data.get("_parent_folder_name", "unknown")
    logger.info(
        "Starting processing for email: %s - %s (event=%s, folder=%s, skip_analysis=%s, force=%s)",
        thread_id,
        email_data.get("subject"),
        event_type,
        folder_name,
        skip_analysis,
        force_reprocess,
    )

    # Initialize Draft Recipients (Reply Logic)
    if "draft_to" not in email_data:
        email_data["draft_to"] = [email_data.get("sender")] if email_data.get("sender") else []
    
    if "draft_cc" not in email_data:
        email_data["draft_cc"] = email_data.get("cc", [])

    initial_write = await ctx.db_manager.log_initial_email(email_data)
    if initial_write is InitialEmailWriteResult.DUPLICATE and not force_reprocess:
        logger.info("Email %s already exists in DB.", thread_id)
        if not skip_analysis:
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
    )

    logger.info("Email %s logged to DB as 'pending'.", thread_id)

    if skip_analysis:
        await _archive_only(thread_id, email_data, ctx, event_type)
        return ProcessingOutcome.ARCHIVED

    await _run_ai_path(thread_id, email_data, ctx, config)
    return ProcessingOutcome.PROCESSED


async def _delete_unclaimed_content_candidate(ref: ContentRef, ctx, *, reason: str) -> None:
    try:
        await ctx.content_store.delete(ref)
    except asyncio.CancelledError:
        logger.error("Unclaimed content cleanup was cancelled: reason=%s", reason)
    except Exception as cleanup_exc:
        logger.error(
            "Unclaimed content cleanup failed: reason=%s error_type=%s",
            reason,
            type(cleanup_exc).__name__,
        )


async def _ensure_durable_content_ref(
    email_id: str,
    email_data: dict,
    ctx,
    *,
    reuse_existing: bool,
) -> ContentRef:
    """Persist content and its typed DB ref before any downstream operation."""
    if reuse_existing:
        existing = await ctx.db_manager.get_content_ref(email_id)
        if existing is not None:
            return _require_owned_ref(existing)

    settings = get_settings()
    ref = await ctx.content_store.put_email(
        settings.EXCHANGE_ACCOUNT_ID,
        email_id,
        email_data,
    )
    ref = _require_owned_ref(ref)
    try:
        claimed = await ctx.db_manager.set_content_ref_if_absent(email_id, ref)
    except asyncio.CancelledError as cancel_exc:
        # The CAS may have committed before cancellation was observed.  Read
        # back before deciding whether this attempt's object is unclaimed.
        try:
            persisted_ref = await ctx.db_manager.get_content_ref(email_id)
        except asyncio.CancelledError:
            logger.error("Content reference cancellation read-back was cancelled")
            raise cancel_exc from None
        except Exception as read_exc:
            logger.error(
                "Content reference cancellation outcome unknown: "
                "read_error_type=%s",
                type(read_exc).__name__,
            )
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
                    )
                logger.error(
                    "Content reference cancellation winner invalid: error_type=%s",
                    type(validation_exc).__name__,
                )
                raise cancel_exc from None

        if persisted_ref is None or persisted_ref != ref:
            try:
                await ctx.content_store.delete(ref)
            except asyncio.CancelledError:
                logger.error("Cancelled content cleanup was interrupted")
            except Exception as cleanup_exc:
                logger.error(
                    "Cancelled content cleanup failed: error_type=%s",
                    type(cleanup_exc).__name__,
                )
        raise cancel_exc from None
    except Exception as write_exc:
        try:
            persisted_ref = await ctx.db_manager.get_content_ref(email_id)
        except Exception as read_exc:
            logger.error(
                "Content reference commit outcome unknown: write_error_type=%s "
                "read_error_type=%s",
                type(write_exc).__name__,
                type(read_exc).__name__,
            )
            raise write_exc from None

        if persisted_ref is not None:
            try:
                persisted_ref = _require_owned_ref(persisted_ref)
            except Exception:
                await _delete_unclaimed_content_candidate(
                    ref,
                    ctx,
                    reason="ambiguous_foreign_winner",
                )
                raise
            if persisted_ref == ref:
                logger.warning("Content reference commit confirmed by read-back")
                return ref
            try:
                await ctx.content_store.delete(ref)
            except Exception as cleanup_exc:
                logger.error(
                    "Unclaimed content cleanup failed: error_type=%s",
                    type(cleanup_exc).__name__,
                )
            return persisted_ref

        try:
            await ctx.content_store.delete(ref)
        except Exception as cleanup_exc:
            logger.error(
                "Content cleanup failed: error_type=%s",
                type(cleanup_exc).__name__,
            )
        raise write_exc from None

    if claimed:
        return ref

    try:
        persisted_ref = await ctx.db_manager.get_content_ref(email_id)
    except asyncio.CancelledError as cancel_exc:
        # CAS=False proves this candidate was never claimed, so it is safe to
        # delete even though reading the concurrent winner was cancelled.
        try:
            await ctx.content_store.delete(ref)
        except asyncio.CancelledError:
            logger.error("Unclaimed content cleanup was cancelled")
        except Exception as cleanup_exc:
            logger.error(
                "Unclaimed content cleanup after cancellation failed: "
                "error_type=%s",
                type(cleanup_exc).__name__,
            )
        raise cancel_exc from None
    except Exception as read_exc:
        # A False CAS result proves this candidate was not claimed.  It is safe
        # to remove even though reading the concurrent winner failed.
        try:
            await ctx.content_store.delete(ref)
        except Exception as cleanup_exc:
            logger.error(
                "Unclaimed content cleanup after read-back failure failed: "
                "error_type=%s",
                type(cleanup_exc).__name__,
            )
        raise read_exc from None
    if persisted_ref is None:
        try:
            await ctx.content_store.delete(ref)
        except Exception as cleanup_exc:
            logger.error(
                "Unresolved content cleanup failed: error_type=%s",
                type(cleanup_exc).__name__,
            )
        raise DatabaseOperationError(
            operation="set_content_ref_if_absent",
            retryable=True,
            message="content reference claim unresolved",
        )
    try:
        persisted_ref = _require_owned_ref(persisted_ref)
    except Exception:
        await _delete_unclaimed_content_candidate(
            ref,
            ctx,
            reason="false_claim_foreign_winner",
        )
        raise
    if persisted_ref == ref:
        return ref
    try:
        await ctx.content_store.delete(ref)
    except Exception as cleanup_exc:
        logger.error(
            "Concurrent content cleanup failed: error_type=%s",
            type(cleanup_exc).__name__,
        )
    return persisted_ref


async def _archive_only(thread_id: str, email_data: dict, ctx, event_type: str) -> None:
    """Archive-folder route: ingest into Qdrant only; never touch mark_as_read."""
    await _ingest_to_qdrant(thread_id, email_data, ctx)
    await ctx.db_manager.update_status(thread_id, "archived")
    logger.info("Email %s archived (Qdrant only, event=%s).", thread_id, event_type)


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
    merged_tokens = list(
        dict.fromkeys([*current.attachment_tokens, *requested_tokens])
    )
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
        retained_pdf_token is None
        or confirmed_pdf_token == retained_pdf_token
    )
    if checkpoint_confirmed and tokens_confirmed and pdf_confirmed:
        return CleanupHandleSnapshot(
            attachment_tokens=tuple(confirmed_tokens),
            pdf_token=(
                confirmed_pdf_token
                if isinstance(confirmed_pdf_token, str)
                else None
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
        logger.warning(
            "Cleanup state lookup failed: error_type=%s",
            type(exc).__name__,
        )

    state_attachment_tokens = [
        token
        for token in (values.get("attachment_tokens") or [])
        if isinstance(token, str) and token
    ]
    all_attachment_tokens = list(
        dict.fromkeys([*state_attachment_tokens, *fallback_attachment_tokens])
    )
    preserved_attachment_tokens = list(
        dict.fromkeys(preserve_attachment_tokens or [])
    )
    preserved_attachment_set = set(preserved_attachment_tokens)
    pdf_token = values.get("pdf_token")

    failed_attachment_tokens: list[str] = []
    for token in all_attachment_tokens:
        if token in preserved_attachment_set or token == preserve_pdf_token:
            continue
        try:
            deleted = await asyncio.to_thread(lark_app.delete_file_from_drive, token)
        except Exception as exc:
            logger.error(
                "Drive cleanup failed: error_type=%s",
                type(exc).__name__,
            )
            deleted = False
        if not deleted:
            failed_attachment_tokens.append(token)

    retained_pdf_token = preserve_pdf_token
    if (
        isinstance(pdf_token, str)
        and pdf_token
        and pdf_token != preserve_pdf_token
    ):
        try:
            deleted = await asyncio.to_thread(
                lark_app.delete_file_from_drive,
                pdf_token,
            )
        except Exception as exc:
            logger.error(
                "PDF cleanup failed: error_type=%s",
                type(exc).__name__,
            )
            deleted = False
        if not deleted:
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
        logger.warning(
            "Cleanup state update failed: error_type=%s",
            type(exc).__name__,
        )
        return

    try:
        confirmed = await _snapshot_cleanup_handles(email_id, ctx)
    except Exception as exc:
        logger.warning(
            "Cleanup state read-back failed: error_type=%s",
            type(exc).__name__,
        )
        return
    tokens_confirmed = set(retained_state_tokens).issubset(
        confirmed.attachment_tokens
    )
    pdf_confirmed = (
        retained_pdf_token is None
        or confirmed.pdf_token == retained_pdf_token
    )
    if not tokens_confirmed or not pdf_confirmed:
        logger.warning("Cleanup state update was not confirmed")


async def _run_ai_path(thread_id: str, email_data: dict, ctx, config: dict) -> None:
    """
    Inbox route: upload -> ingest -> AI -> notify, with two-phase mark_as_read.

    Mark-as-read is only fired AFTER user-facing delivery (Lark card or explicit
    skip) is confirmed. On dispatch failure the email stays unread on Exchange
    so SelfHealer / human can retry without losing visibility.
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

        async def acknowledge_attachment_token(token: str) -> None:
            if token not in attachment_tokens:
                attachment_tokens.append(token)
            await _checkpoint_ai_path_resources(
                thread_id,
                email_data,
                ref,
                ctx,
                config,
                attachment_tokens=[
                    *baseline.attachment_tokens,
                    *attachment_tokens,
                ],
                pdf_token=baseline.pdf_token,
            )

        attachment_uploads = await _upload_attachments_to_lark(
            email_data,
            max_uploads=MAX_TOKENS - len(baseline.attachment_tokens),
            acknowledge_token=acknowledge_attachment_token,
        )
        for token in attachment_uploads.tokens:
            if token not in attachment_tokens:
                await acknowledge_attachment_token(token)
        await _ingest_to_qdrant(thread_id, email_data, ctx)
        pipeline_result = await _run_ai_pipeline(
            thread_id,
            ctx,
            config,
            attachment_tokens=attachment_tokens,
            preserved_attachment_tokens=list(baseline.attachment_tokens),
            preserved_pdf_token=baseline.pdf_token,
            attachment_links=[dict(link) for link in attachment_uploads.links],
        )
        if pipeline_result is None:
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                preserve_attachment_tokens=list(baseline.attachment_tokens),
                preserve_pdf_token=baseline.pdf_token,
            )
            await ctx.db_manager.update_status(thread_id, "error")
            return

        dispatch_result = await _dispatch_notification(thread_id, pipeline_result, ctx, config)
        notification_committed = bool(
            dispatch_result.get("delivered")
            and dispatch_result.get("kind") in {"approval", "read_only"}
        )
        if not dispatch_result.get("delivered"):
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                preserve_attachment_tokens=list(baseline.attachment_tokens),
                preserve_pdf_token=baseline.pdf_token,
            )
        elif dispatch_result.get("kind") == "skipped":
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
            )
        if dispatch_result.get("delivered"):
            await _mark_email_read(thread_id, ctx)
        else:
            logger.warning(
                "Skipping mark_as_read for %s: delivery_failed (kind=%s).",
                thread_id,
                dispatch_result.get("kind"),
            )
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
        if not notification_committed:
            await _cleanup_graph_drive_files(
                thread_id,
                ctx,
                fallback_attachment_tokens=attachment_tokens,
                preserve_attachment_tokens=list(baseline.attachment_tokens),
                preserve_pdf_token=baseline.pdf_token,
            )
        raise
    except DatabaseOperationError:
        await _cleanup_graph_drive_files(
            thread_id,
            ctx,
            fallback_attachment_tokens=attachment_tokens,
            preserve_attachment_tokens=list(baseline.attachment_tokens),
            preserve_pdf_token=baseline.pdf_token,
        )
        raise
    except Exception as exc:
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


def _extract_id(raw) -> str | None:
    """Safely extract ID from nested EWS objects or plain strings."""
    if isinstance(raw, dict):
        return raw.get("id")
    if isinstance(raw, str):
        raw_str = raw.strip()
        if not raw_str:
            return None
        match = re.search(r"\bid\s*=\s*['\"]([^'\"]+)['\"]", raw_str)
        if match:
            return match.group(1)
        return raw_str
    return None


# ---------------------------------------------------------------------------
# Shared enqueue implementation (used by both WebhookWorker and legacy API)
# ---------------------------------------------------------------------------

async def _enqueue_event_impl(queue: asyncio.Queue, ctx, payload: dict, header_event: str | None = None) -> dict:
    """Core routing + enqueue logic, parameterised on *queue* and *ctx*."""
    if queue is None:
        raise RuntimeError("Exchange worker is not running")

    event_type = header_event or payload.get("event_type") or payload.get("event")
    if event_type not in {"NewMailEvent", "CreatedEvent"}:
        return {"queued": False, "reason": "unsupported_event", "event_type": event_type}

    email_id = _extract_id(payload.get("item_id")) or _extract_id(payload.get("id"))
    parent_folder_id = _extract_id(payload.get("parent_folder_id"))
    if not email_id:
        logger.debug("Ignoring event %s: no item_id", event_type)
        return {"queued": False, "reason": "no_item_id", "event_type": event_type}

    exchange_client = ctx.exchange_client if ctx else None
    folder_name = exchange_client.get_folder_name(parent_folder_id) if exchange_client else None

    route = None
    skip_analysis = False

    if event_type == "NewMailEvent":
        folder_policies = getattr(exchange_client, "_folder_policies", None) if exchange_client else None
        if folder_policies:
            policy = exchange_client.get_folder_policy(parent_folder_id)
        else:
            policy = "full"
            logger.warning("Folder policies not loaded; defaulting %s to full pipeline", email_id)

        if policy == "full":
            route = "full"
            skip_analysis = False
        elif policy == "archive":
            route = "archive"
            skip_analysis = True
        else:
            return {
                "queued": False,
                "reason": "folder_not_in_whitelist",
                "folder": folder_name or parent_folder_id,
            }
    else:  # CreatedEvent
        if exchange_client and parent_folder_id == exchange_client.sentitems_folder_id:
            route = "archive"
            skip_analysis = True
        elif exchange_client and parent_folder_id == exchange_client.drafts_folder_id:
            return {"queued": False, "reason": "drafts_ignored"}
        else:
            return {
                "queued": False,
                "reason": "created_other_ignored",
                "folder": folder_name or parent_folder_id,
            }

    email_data = payload.get("item") if isinstance(payload.get("item"), dict) else {}
    email_data.setdefault("id", email_id)
    email_data.setdefault("subject", payload.get("subject", ""))
    email_data.setdefault("sender", payload.get("sender", ""))
    email_data.setdefault("received_at", payload.get("received_time", ""))
    email_data["_parent_folder_id"] = parent_folder_id
    email_data["_parent_folder_name"] = folder_name
    email_data["_event_type"] = event_type

    try:
        queue.put_nowait((email_data, skip_analysis))
        try:
            from src.observability.metrics import webhook_queue_depth
            webhook_queue_depth.set(queue.qsize())
        except Exception:
            pass
    except asyncio.QueueFull:
        logger.error(
            "Webhook queue full (max=%s); rejecting %s id=%s. Sender should retry.",
            getattr(queue, "maxsize", "?"),
            event_type,
            email_id,
        )
        return {
            "queued": False,
            "reason": "queue_full",
            "email_id": email_id,
            "queue_size": queue.qsize(),
        }
    logger.info(
        "Enqueued %s [%s]: %s (folder=%s, skip_analysis=%s)",
        event_type,
        route,
        email_id,
        folder_name or "unknown",
        skip_analysis,
    )
    return {
        "queued": True,
        "email_id": email_id,
        "route": route,
        "folder": folder_name,
        "queue_size": queue.qsize(),
    }


# ---------------------------------------------------------------------------
# WebhookWorker class
# ---------------------------------------------------------------------------

class WebhookWorker:
    """Process webhook events with a fixed set of queue consumers."""

    def __init__(
        self,
        ctx,
        *,
        queue_maxsize: int = WEBHOOK_QUEUE_MAXSIZE,
        concurrency: int = WORKER_CONCURRENCY,
    ):
        for name, value in (
            ("queue_maxsize", queue_maxsize),
            ("concurrency", concurrency),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

        self._ctx = ctx
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self.concurrency = concurrency
        self._consumer_tasks: list[asyncio.Task] = []
        self._accepting = True
        self._shutdown_cancelled_count = 0
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)

    @property
    def queue(self) -> asyncio.Queue:
        return self._queue

    @property
    def consumer_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(self._consumer_tasks)

    @property
    def shutdown_cancelled_count(self) -> int:
        """Return queued events cancelled after incomplete shutdown drains."""
        return self._shutdown_cancelled_count

    async def start(self):
        """Start the fixed set of background consumers once."""
        if self._consumer_tasks or not self._accepting:
            return
        self._consumer_tasks = [
            asyncio.create_task(
                self._consume(),
                name=f"exchange-webhook-consumer-{index}",
            )
            for index in range(self.concurrency)
        ]
        logger.info("Exchange webhook worker started (concurrency=%d).", self.concurrency)

    async def stop(self, drain_timeout: float = 30.0):
        """Close intake, drain queued work, then collect all consumers."""
        self._accepting = False
        if not self._consumer_tasks:
            return

        tasks = tuple(self._consumer_tasks)
        drain_completed = False
        try:
            async with asyncio.timeout(drain_timeout):
                await self._queue.join()
            drain_completed = True
        except TimeoutError:
            logger.warning(
                "Timed out draining Exchange webhook queue after %.1f seconds; "
                "cancelling consumers.",
                drain_timeout,
            )
        finally:
            cleanup_task = asyncio.create_task(
                self._finish_stop(tasks, cancel_queued=not drain_completed),
                name="exchange-webhook-stop-cleanup",
            )
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
                raise

    async def _finish_stop(
        self,
        tasks: tuple[asyncio.Task, ...],
        *,
        cancel_queued: bool,
    ) -> None:
        """Collect consumers and account for work cancelled by shutdown."""
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._consumer_tasks.clear()

        if cancel_queued:
            cancelled = 0
            while True:
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._queue.task_done()
                cancelled += 1

            self._shutdown_cancelled_count += cancelled
            if cancelled:
                logger.warning(
                    "Webhook shutdown cancelled queued events: shutdown_cancelled=%d",
                    cancelled,
                )
            try:
                from src.observability.metrics import webhook_queue_depth

                webhook_queue_depth.set(self._queue.qsize())
            except Exception:
                pass
        logger.info("Exchange webhook worker stopped.")

    async def enqueue_event(self, payload: dict, header_event: str | None = None) -> dict:
        """Route and enqueue a webhook event."""
        if not self._accepting:
            raise RuntimeError("Exchange webhook worker is not accepting events")
        return await _enqueue_event_impl(self._queue, self._ctx, payload, header_event)

    async def _consume(self):
        """Process queue items inline so task completion tracks real work."""
        while True:
            email_data, skip_analysis = await self._queue.get()
            try:
                await self._process_one(email_data, skip_analysis)
            finally:
                self._queue.task_done()

    async def _process_one(self, email_data, skip_analysis):
        """Process one webhook event inside its fixed consumer."""
        try:
            if "body" not in email_data:
                email_id = email_data.get("id")
                logger.info(f"Fetching details for {email_id}...")
                full_details = await self._ctx.exchange_client.get_email(email_id)
                if full_details:
                    email_data.update(full_details)
                else:
                    logger.warning(
                        "Skip webhook event because detail fetch failed (id=%s, event=%s).",
                        email_id,
                        email_data.get("_event_type", "unknown"),
                    )
                    return
            await process_and_archive_email(email_data, self._ctx, skip_analysis)
        except Exception as exc:
            logger.error("Worker processing failed: error_type=%s", type(exc).__name__)


# ---------------------------------------------------------------------------
# Module-level singleton and backward-compatible globals / functions
# ---------------------------------------------------------------------------

_worker: WebhookWorker | None = None
_webhook_queue: asyncio.Queue | None = None
_worker_ctx = None
_worker_semaphore: asyncio.Semaphore | None = None


async def start_worker(ctx=None):
    """Start webhook worker (backward-compatible module-level function)."""
    global _worker, _webhook_queue, _worker_ctx, _worker_semaphore
    if _worker is not None and any(not task.done() for task in _worker.consumer_tasks):
        return
    ctx = ctx or get_app_context()
    _worker = WebhookWorker(ctx)
    _webhook_queue = _worker.queue
    _worker_ctx = ctx
    _worker_semaphore = _worker._semaphore
    await _worker.start()


async def stop_worker():
    """Stop webhook worker (backward-compatible module-level function)."""
    global _worker, _webhook_queue, _worker_ctx, _worker_semaphore
    worker = _worker
    if worker is None:
        return
    try:
        await worker.stop()
    finally:
        if _worker is worker:
            _worker = None
            _webhook_queue = None
            _worker_ctx = None
            _worker_semaphore = None


async def enqueue_webhook_event(payload: dict, header_event: str | None = None) -> dict:
    """Enqueue a webhook event (backward-compatible module-level function)."""
    if _worker is not None:
        return await _worker.enqueue_event(payload, header_event)
    # Legacy fallback: tests may patch _webhook_queue and _worker_ctx directly
    return await _enqueue_event_impl(_webhook_queue, _worker_ctx, payload, header_event)
