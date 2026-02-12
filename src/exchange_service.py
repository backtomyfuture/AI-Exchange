import asyncio
import time
import os
import logging
from src.init_app import get_app_context
from src.utils import lark_app

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ExchangeService")

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

async def main_loop():
    logger.info("Starting Exchange Service...")
    
    ctx = get_app_context()
    await ctx.setup_async()
    
    # Initialize Lark App (for API usage only, no WS)
    # Pass the running loop to ensure thread-safe operations from Lark WS thread
    lark_app.init_lark_app(ctx.db_manager, ctx.graph, ctx.exchange_client, worker_loop_arg=asyncio.get_running_loop())
    logger.info("Lark App Initialized (API Mode).")
    
    queue = asyncio.Queue()

    async def worker():
        logger.info("Worker started.")
        while True:
            email_task = await queue.get()
            email_data, skip_analysis = email_task
            try:
                # Fetch full details if body missing
                if 'body' not in email_data:
                    email_id = email_data.get('id')
                    logger.info(f"Fetching details for {email_id}...")
                    full_details = await ctx.exchange_client.get_email(email_id)
                    if full_details:
                        email_data.update(full_details)
                
                await process_and_archive_email(email_data, ctx, skip_analysis)
            except Exception as e:
                logger.error(f"Worker processing error: {e}")
            finally:
                queue.task_done()
                await asyncio.sleep(1)

    # Start Worker
    worker_task = asyncio.create_task(worker())

    # Sync Configuration
    current_account_id = str(ctx.exchange_client.account_id)
    ai_folders = [f.strip() for f in os.getenv("EXCHANGE_AI_FOLDERS", "INBOX").split(",") if f.strip()]
    archive_folders = [f.strip() for f in os.getenv("EXCHANGE_ARCHIVE_FOLDERS", "").split(",") if f.strip()]
    all_folders = list(set(ai_folders + archive_folders))
    
    sync_states = {}
    for folder in all_folders:
        state = await ctx.db_manager.get_sync_state(current_account_id, folder)
        sync_states[folder] = state
        logger.info(f"Initial sync state for {folder}: {'None' if state is None else 'Present'}")

    try:
        while True:
            for folder in all_folders:
                try:
                    current_sync_state = sync_states.get(folder)
                    skip_analysis = folder in archive_folders and folder not in ai_folders
                    
                    limit = 5 if current_sync_state is None else 50
                    sync_data = await ctx.exchange_client.sync_emails(sync_state=current_sync_state, folder=folder, limit=limit)
                    
                    if 'sync_state' in sync_data:
                        new_sync_state = sync_data['sync_state']
                        items = sync_data.get('items', [])
                        
                        if items:
                            logger.info(f"Sync: Folder '{folder}' has {len(items)} changes.")
                            if current_sync_state is None and len(items) > 10:
                                items = items[-10:]
                            
                            for item in items:
                                if item.get('change_type') == 'create':
                                    email_content = item.get('item', {})
                                    if email_content:
                                        email_content['id'] = item.get('id')
                                        await queue.put((email_content, skip_analysis))
                        
                        if new_sync_state != current_sync_state:
                            await ctx.db_manager.save_sync_state(current_account_id, new_sync_state, folder=folder)
                            sync_states[folder] = new_sync_state
                    
                except Exception as e:
                    logger.error(f"Error syncing folder {folder}: {e}")
                
            logger.info(f"Sync cycle complete. Queue: {queue.qsize()}. Sleep 60s.")
            await asyncio.sleep(60)

    except KeyboardInterrupt:
        logger.info("Stopping...")
    except Exception as e:
        logger.critical(f"Critical error: {e}")
    finally:
        worker_task.cancel()
        await ctx.close()

if __name__ == "__main__":
    asyncio.run(main_loop())
