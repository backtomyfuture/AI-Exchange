import asyncio
import time
import logging
import re
from src.init_app import get_app_context
from src.utils import lark_app

logger = logging.getLogger("ExchangeService")
WORKER_CONCURRENCY = 3
_webhook_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None
_worker_ctx = None
_worker_semaphore: asyncio.Semaphore | None = None

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
        }
    except Exception as e:
        logger.exception(f"Error executing graph for {email_id}: {e}")
        return None


async def _dispatch_notification(email_id: str, pipeline_result: dict, ctx, config: dict) -> None:
    """Send Lark card based on classification result."""
    classification = pipeline_result.get("classification", {})
    priority = classification.get("priority", "P3")
    intent = classification.get("intent", "Unknown")

    if classification.get("need_reply"):
        logger.info(f"Email requires reply. Sending Lark approval request: {email_id}")
        pdf_url = await lark_app.generate_and_upload_pdf(email_id, pipeline_result.get("email", {}))
        lark_app.send_approval_card(
            email_id=email_id,
            draft=pipeline_result.get("draft", ""),
            context=pipeline_result.get("context", []),
            email_data=pipeline_result.get("email", {}),
            classification=classification,
            pdf_url=pdf_url,
        )
        await ctx.db_manager.update_status(email_id, "waiting_approval")
    elif priority == "P1" or intent == "通知":
        logger.info(f"Email is important ({priority}/{intent}) but no reply needed. Sending Read-Only card: {email_id}")
        pdf_url = await lark_app.generate_and_upload_pdf(email_id, pipeline_result.get("email", {}))
        lark_app.send_read_only_card(
            email_id=email_id,
            context=pipeline_result.get("context", []),
            email_data=pipeline_result.get("email", {}),
            classification=classification,
            pdf_url=pdf_url,
        )
        await ctx.db_manager.update_status(email_id, "notified_readonly")
    else:
        logger.info(f"No reply needed for email: {email_id}")
        await ctx.db_manager.update_status(email_id, "skipped")


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

    is_new = await ctx.db_manager.log_initial_email(email_data)
    if not is_new and not force_reprocess:
        logger.info("Email %s already exists in DB.", thread_id)
        if not skip_analysis:
            await _mark_email_read(thread_id, ctx)
        return

    logger.info("Email %s logged to DB as 'pending'.", thread_id)

    if skip_analysis:
        await _ingest_to_qdrant(thread_id, email_data, ctx)
        await ctx.db_manager.update_status(thread_id, "archived")
        logger.info("Email %s archived (Qdrant only, event=%s).", thread_id, event_type)
    else:
        await _upload_attachments_to_lark(email_data)
        await _ingest_to_qdrant(thread_id, email_data, ctx)
        try:
            pipeline_result = await _run_ai_pipeline(thread_id, email_data, ctx, config)
            if pipeline_result is not None:
                await _dispatch_notification(thread_id, pipeline_result, ctx, config)
            await _mark_email_read(thread_id, ctx)
        except Exception as e:
            logger.error("Pipeline failed for %s, leaving unread for retry: %s", thread_id, e)
            await ctx.db_manager.update_status(thread_id, "error")

async def _worker_loop():
    """Webhook-driven background worker with concurrency control."""
    global _webhook_queue, _worker_ctx, _worker_semaphore
    _worker_semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
    logger.info("Exchange webhook worker started (concurrency=%d).", WORKER_CONCURRENCY)

    async def _process_one(email_data, skip_analysis):
        async with _worker_semaphore:
            try:
                if "body" not in email_data:
                    email_id = email_data.get("id")
                    logger.info(f"Fetching details for {email_id}...")
                    full_details = await _worker_ctx.exchange_client.get_email(email_id)
                    if full_details:
                        email_data.update(full_details)
                    else:
                        logger.warning(
                            "Skip webhook event because detail fetch failed (id=%s, event=%s).",
                            email_id,
                            email_data.get("_event_type", "unknown"),
                        )
                        return
                await process_and_archive_email(email_data, _worker_ctx, skip_analysis)
            except Exception as e:
                logger.error(f"Worker processing error: {e}")

    while True:
        email_data, skip_analysis = await _webhook_queue.get()
        asyncio.create_task(_process_one(email_data, skip_analysis))
        _webhook_queue.task_done()


async def start_worker(ctx=None):
    """Start webhook worker once."""
    global _webhook_queue, _worker_task, _worker_ctx
    if _worker_task and not _worker_task.done():
        return
    _worker_ctx = ctx or get_app_context()
    _webhook_queue = asyncio.Queue()
    _worker_task = asyncio.create_task(_worker_loop())


async def stop_worker():
    """Stop webhook worker and cleanup task."""
    global _worker_task, _webhook_queue
    if _worker_task is None:
        return
    _worker_task.cancel()
    try:
        await _worker_task
    except asyncio.CancelledError:
        logger.info("Exchange webhook worker cancelled.")
    _worker_task = None
    _webhook_queue = None


def _extract_id(raw) -> str | None:
    """Safely extract ID from nested EWS objects or plain strings."""
    if isinstance(raw, dict):
        return raw.get("id")
    if isinstance(raw, str):
        raw_str = raw.strip()
        if not raw_str:
            return None
        # Handle object-like string repr:
        # ItemId(id='xxx', changekey='...') / ParentFolderId(id='xxx', ...)
        match = re.search(r"\bid\s*=\s*['\"]([^'\"]+)['\"]", raw_str)
        if match:
            return match.group(1)
        return raw_str
    return None


async def enqueue_webhook_event(payload: dict, header_event: str | None = None) -> dict:
    """
    Route webhook event by event type + folder policy and enqueue.
    """
    if _webhook_queue is None:
        raise RuntimeError("Exchange worker is not running")

    event_type = header_event or payload.get("event_type") or payload.get("event")
    if event_type not in {"NewMailEvent", "CreatedEvent"}:
        return {"queued": False, "reason": "unsupported_event", "event_type": event_type}

    email_id = _extract_id(payload.get("item_id")) or _extract_id(payload.get("id"))
    parent_folder_id = _extract_id(payload.get("parent_folder_id"))
    if not email_id:
        logger.debug("Ignoring event %s: no item_id", event_type)
        return {"queued": False, "reason": "no_item_id", "event_type": event_type}

    exchange_client = _worker_ctx.exchange_client if _worker_ctx else None
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

    await _webhook_queue.put((email_data, skip_analysis))
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
        "queue_size": _webhook_queue.qsize(),
    }
