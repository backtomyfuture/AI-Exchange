import asyncio
import time
import logging
from src.init_app import get_app_context
from src.utils import lark_app

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ExchangeService")
_webhook_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None
_worker_ctx = None

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
    """Ingest email into Qdrant vector store."""
    try:
        ctx.email_processor.process_email(email_data)
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


async def process_and_archive_email(email_data, ctx, skip_analysis: bool = False):
    """Process a single email: Ingest -> Analyze -> Notify -> Archive."""
    thread_id = email_data.get("id", str(time.time()))
    config = {"configurable": {"thread_id": thread_id}}
    logger.info(f"Starting processing for email: {thread_id} - {email_data.get('subject')} (skip_analysis={skip_analysis})")
    await _upload_attachments_to_lark(email_data)

    is_new = await ctx.db_manager.log_initial_email(email_data)
    if not is_new:
        logger.info(f"Email {thread_id} already exists in DB.")
        await _mark_email_read(thread_id, ctx)
        return

    logger.info(f"Email {thread_id} logged to DB as 'pending'.")
    await _ingest_to_qdrant(thread_id, email_data, ctx)

    if skip_analysis:
        logger.info(f"Skipping AI analysis for email: {thread_id}")
        await ctx.db_manager.update_status(thread_id, "archived")
    else:
        pipeline_result = await _run_ai_pipeline(thread_id, email_data, ctx, config)
        if pipeline_result is not None:
            await _dispatch_notification(thread_id, pipeline_result, ctx, config)

    await _mark_email_read(thread_id, ctx)

async def _worker_loop():
    """Webhook-driven background worker. No polling/sync logic."""
    global _webhook_queue, _worker_ctx
    logger.info("Exchange webhook worker started.")
    while True:
        email_data, skip_analysis = await _webhook_queue.get()
        try:
            # Fetch full details if body missing
            if "body" not in email_data:
                email_id = email_data.get("id")
                logger.info(f"Fetching details for {email_id}...")
                full_details = await _worker_ctx.exchange_client.get_email(email_id)
                if full_details:
                    email_data.update(full_details)

            await process_and_archive_email(email_data, _worker_ctx, skip_analysis)
        except Exception as e:
            logger.error(f"Worker processing error: {e}")
        finally:
            _webhook_queue.task_done()
            await asyncio.sleep(1)


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


async def enqueue_webhook_event(payload: dict, header_event: str | None = None) -> dict:
    """
    Convert webhook payload to queue task and enqueue.
    """
    if _webhook_queue is None:
        raise RuntimeError("Exchange worker is not running")

    event_type = header_event or payload.get("event_type") or payload.get("event")
    if event_type and event_type not in {"NewMailEvent", "CreatedEvent"}:
        return {"queued": False, "ignored": True, "event_type": event_type}

    email_id = payload.get("item_id") or payload.get("id")
    if not email_id:
        raise ValueError("Missing item_id in webhook payload")

    email_data = payload.get("item") if isinstance(payload.get("item"), dict) else {}
    email_data.setdefault("id", email_id)
    email_data.setdefault("subject", payload.get("subject", ""))
    email_data.setdefault("sender", payload.get("sender", ""))
    email_data.setdefault("received_at", payload.get("received_time", ""))

    await _webhook_queue.put((email_data, False))
    return {"queued": True, "email_id": email_id, "queue_size": _webhook_queue.qsize()}
