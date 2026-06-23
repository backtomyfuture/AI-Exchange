import asyncio
import time
import logging
import re
from src.init_app import get_app_context
from src.utils import lark_app

logger = logging.getLogger("ExchangeService")
WORKER_CONCURRENCY = 3
WEBHOOK_QUEUE_MAXSIZE = 500


async def _upload_attachments_to_lark(email_data: dict) -> None:
    """Upload attachments to Lark Drive and append tokens/urls."""
    attachments = email_data.get("attachments", [])
    if not attachments:
        return
    logger.info(f"Email has {len(attachments)} attachments.")
    try:
        import base64

        for att in attachments:
            if att.get("content"):
                content_bytes = base64.b64decode(att["content"])
                res = lark_app.upload_file_to_drive(
                    att.get("name", "unknown"), content_bytes, len(content_bytes)
                )
                if res:
                    att["lark_file_token"] = res["file_token"]
                    att["lark_file_url"] = res["url"]
                    logger.info(f"Uploaded {att.get('name')} to Lark Drive: {res['url']}")
    except Exception as e:
        logger.error(f"Error uploading attachments to Lark: {e}")


async def _ingest_to_qdrant(email_id: str, email_data: dict, ctx) -> None:
    """Ingest email into Qdrant vector store (sync call wrapped in thread)."""
    try:
        await asyncio.to_thread(ctx.email_processor.process_email, email_data)
        logger.info(f"Email {email_id} ingested to Qdrant.")
        await ctx.db_manager.update_status(email_id, "ingested")
    except Exception as e:
        logger.error(f"Failed to ingest email {email_id}: {e}")


async def _run_ai_pipeline(email_id: str, email_data: dict, ctx, config: dict):
    """Run LangGraph pipeline and return final classification dict or None."""
    initial_state = {
        "email": email_data,
        "classification": {},
        "context": [],
        "draft": "",
        "approval_status": "pending",
        "next_step": "",
    }
    try:
        async for event in ctx.graph.astream(initial_state, config=config):
            if "categorizer" in event:
                classification = event["categorizer"].get("classification", {})
                await ctx.db_manager.update_status(email_id, "analyzed", classification=classification)
            if "drafter" in event:
                draft = event["drafter"].get("draft", "")
                await ctx.db_manager.update_status(email_id, "drafted", draft_content=draft)

        state = await ctx.graph.aget_state(config)
        return {
            "classification": state.values.get("classification", {}),
            "draft": state.values.get("draft", ""),
            "context": state.values.get("context", []),
            "email": state.values.get("email", {}),
            "routing_log": state.values.get("routing_log", []),
            "active_skills": state.values.get("active_skills", []),
        }
    except Exception as e:
        logger.exception(f"Error executing graph for {email_id}: {e}")
        return None


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
    except Exception as e:
        # Best-effort enrichment; never block notification on label writes.
        logger.warning("update_email_labels failed for %s: %s", email_id, e)

    if classification.get("need_reply"):
        logger.info(f"Email requires reply. Sending Lark approval request: {email_id}")
        pdf_result = await lark_app.generate_and_upload_pdf(email_id, pipeline_result.get("email", {}))
        pdf_url = pdf_result.get("url") if pdf_result else None
        pdf_token = pdf_result.get("file_token") if pdf_result else None

        if pdf_token:
            config = {"configurable": {"thread_id": email_id}}
            await ctx.graph.aupdate_state(config, {"pdf_token": pdf_token})

        delivered = bool(lark_app.send_approval_card(
            email_id=email_id,
            draft=pipeline_result.get("draft", ""),
            context=pipeline_result.get("context", []),
            email_data=pipeline_result.get("email", {}),
            classification=classification,
            pdf_url=pdf_url,
            routing_log=routing_log,
            active_skills=active_skills,
        ))
        if delivered:
            await ctx.db_manager.update_status(email_id, "waiting_approval")
        else:
            logger.error("Approval card delivery failed for %s; leaving on Exchange unread.", email_id)
            await ctx.db_manager.update_status(
                email_id, "delivery_failed",
                error_message="Approval card send returned failure",
            )
        return {"delivered": delivered, "kind": "approval"}

    if priority == "P1" or intent == "通知":
        logger.info(f"Email is important ({priority}/{intent}) but no reply needed. Sending Read-Only card: {email_id}")
        pdf_result = await lark_app.generate_and_upload_pdf(email_id, pipeline_result.get("email", {}))
        pdf_url = pdf_result.get("url") if pdf_result else None
        pdf_token = pdf_result.get("file_token") if pdf_result else None

        if pdf_token:
            config = {"configurable": {"thread_id": email_id}}
            await ctx.graph.aupdate_state(config, {"pdf_token": pdf_token})

        delivered = bool(lark_app.send_read_only_card(
            email_id=email_id,
            context=pipeline_result.get("context", []),
            email_data=pipeline_result.get("email", {}),
            classification=classification,
            pdf_url=pdf_url,
            routing_log=routing_log,
            active_skills=active_skills,
        ))
        if delivered:
            await ctx.db_manager.update_status(email_id, "notified_readonly")
        else:
            logger.error("Read-only card delivery failed for %s; leaving on Exchange unread.", email_id)
            await ctx.db_manager.update_status(
                email_id, "delivery_failed",
                error_message="Read-only card send returned failure",
            )
        return {"delivered": delivered, "kind": "read_only"}

    logger.info(f"No reply needed for email: {email_id}")
    await ctx.db_manager.update_status(email_id, "skipped")
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
    except Exception as e:
        logger.error(f"Exception marking email {email_id} as read: {e}")


async def process_and_archive_email(email_data, ctx, skip_analysis: bool = False, force_reprocess: bool = False):
    """
    Process a single email based on route decision.

    - skip_analysis=False: upload -> ingest -> AI -> notify -> mark_read
    - skip_analysis=True: ingest only -> mark archived (no upload/AI/notify/mark_read)
    - force_reprocess=True: proceed even if email already exists in DB
    """
    thread_id = email_data.get("id", str(time.time()))
    config = {"configurable": {"thread_id": thread_id}}
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

    is_new = await ctx.db_manager.log_initial_email(email_data)
    if not is_new and not force_reprocess:
        logger.info("Email %s already exists in DB.", thread_id)
        if not skip_analysis:
            await _mark_email_read(thread_id, ctx)
        return

    logger.info("Email %s logged to DB as 'pending'.", thread_id)

    if skip_analysis:
        await _archive_only(thread_id, email_data, ctx, event_type)
        return

    await _run_ai_path(thread_id, email_data, ctx, config)


async def _archive_only(thread_id: str, email_data: dict, ctx, event_type: str) -> None:
    """Archive-folder route: ingest into Qdrant only; never touch mark_as_read."""
    await _ingest_to_qdrant(thread_id, email_data, ctx)
    await ctx.db_manager.update_status(thread_id, "archived")
    logger.info("Email %s archived (Qdrant only, event=%s).", thread_id, event_type)


async def _run_ai_path(thread_id: str, email_data: dict, ctx, config: dict) -> None:
    """
    Inbox route: upload -> ingest -> AI -> notify, with two-phase mark_as_read.

    Mark-as-read is only fired AFTER user-facing delivery (Lark card or explicit
    skip) is confirmed. On dispatch failure the email stays unread on Exchange
    so SelfHealer / human can retry without losing visibility.
    """
    await _upload_attachments_to_lark(email_data)
    await _ingest_to_qdrant(thread_id, email_data, ctx)
    try:
        pipeline_result = await _run_ai_pipeline(thread_id, email_data, ctx, config)
        if pipeline_result is None:
            await ctx.db_manager.update_status(thread_id, "error")
            return

        dispatch_result = await _dispatch_notification(thread_id, pipeline_result, ctx, config)
        if dispatch_result.get("delivered"):
            await _mark_email_read(thread_id, ctx)
        else:
            logger.warning(
                "Skipping mark_as_read for %s: delivery_failed (kind=%s).",
                thread_id,
                dispatch_result.get("kind"),
            )
    except Exception as e:
        logger.error("Pipeline failed for %s, leaving unread for retry: %s", thread_id, e)
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
    """Encapsulates webhook worker state: queue, task, context, semaphore."""

    def __init__(self, ctx, queue_maxsize: int = WEBHOOK_QUEUE_MAXSIZE):
        self._ctx = ctx
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._task: asyncio.Task | None = None
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)

    @property
    def queue(self) -> asyncio.Queue:
        return self._queue

    async def start(self):
        """Start the background worker loop."""
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._worker_loop())

    async def stop(self):
        """Cancel the worker task and clean up."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            logger.info("Exchange webhook worker cancelled.")
        self._task = None

    async def enqueue_event(self, payload: dict, header_event: str | None = None) -> dict:
        """Route and enqueue a webhook event."""
        return await _enqueue_event_impl(self._queue, self._ctx, payload, header_event)

    async def _worker_loop(self):
        """Background loop: dequeue and process with concurrency control."""
        logger.info("Exchange webhook worker started (concurrency=%d).", WORKER_CONCURRENCY)
        while True:
            email_data, skip_analysis = await self._queue.get()
            asyncio.create_task(self._process_one(email_data, skip_analysis))
            self._queue.task_done()

    async def _process_one(self, email_data, skip_analysis):
        """Process a single webhook event with semaphore-based concurrency."""
        async with self._semaphore:
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
            except Exception as e:
                logger.error(f"Worker processing error: {e}")


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
    if _worker is not None and _worker._task and not _worker._task.done():
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
    if _worker is None:
        return
    await _worker.stop()
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
