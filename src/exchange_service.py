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

async def process_and_archive_email(email_data, ctx, skip_analysis: bool = False):
    """
    Process a single email: Ingest -> Analyze -> Reply -> Archive/Delete.
    """
    thread_id = email_data.get("id", str(time.time()))
    # Use config with thread_id for Graph Checkpointing
    config = {"configurable": {"thread_id": thread_id}}

    logger.info(f"Starting processing for email: {thread_id} - {email_data.get('subject')} (skip_analysis={skip_analysis})")
    
    if 'attachments' in email_data:
        logger.info(f"Email has {len(email_data['attachments'])} attachments.")
    
    # 1. Log to PostgreSQL (Initial Audit)
    is_new = await ctx.db_manager.log_initial_email(email_data)
    
    if is_new:
        logger.info(f"Email {thread_id} logged to DB as 'pending'.")

        # 2. Ingest to Vector DB
        try:
            ctx.email_processor.process_email(email_data)
            logger.info(f"Email {thread_id} ingested to Qdrant.")
            await ctx.db_manager.update_status(thread_id, "ingested")
        except Exception as e:
            logger.error(f"Failed to ingest email {thread_id}: {e}")
        
        if not skip_analysis:
            # 3. Run LangGraph Workflow
            initial_state = {
                "email": email_data,
                "classification": {},
                "context": [],
                "draft": "",
                "approval_status": "pending",
                "next_step": ""
            }

            try:
                # Run the graph until interrupt or end
                async for event in ctx.graph.astream(initial_state, config=config):
                    # Intercept categorization
                    if "categorizer" in event:
                        classification = event["categorizer"].get("classification", {})
                        await ctx.db_manager.update_status(thread_id, "analyzed", classification=classification)
                    # Intercept drafting
                    if "drafter" in event:
                        draft = event["drafter"].get("draft", "")
                        await ctx.db_manager.update_status(thread_id, "drafted", draft_content=draft)
                
                # Retrieve final (or current paused) state
                state = await ctx.graph.aget_state(config)
                
                # 4. Handle Approval Request (if paused at drafter)
                # The graph pauses at "drafter" if a reply is needed and flow goes to route_after_approval?
                # Actually, route_after_approval is condition logic. 
                # interrupt_after=["drafter"] means it stops AFTER drafter runs.
                
                # If we are here, the graph is either END or PAUSED.
                # Check if we need reply based on classification
                classification = state.values.get("classification", {})
                if classification.get("need_reply"):
                    # Check if we are waiting for approval (i.e. not yet approved/sent)
                    # If the graph finished completely (e.g. no reply needed), next would be END.
                    # But if we paused, next is empty or pointing to next node?
                    
                    logger.info(f"Email requires reply. Sending Lark approval request: {thread_id}")
                    lark_app.send_approval_card(
                        email_id=thread_id,
                        draft=state.values.get("draft", ""),
                        context=state.values.get("context", []),
                        email_data=state.values.get("email", {}),
                        classification=classification
                    )
                    await ctx.db_manager.update_status(thread_id, "waiting_approval")
                else:
                    logger.info(f"No reply needed for email: {thread_id}")
                    await ctx.db_manager.update_status(thread_id, "skipped")

            except Exception as e:
                logger.exception(f"Error executing graph for {thread_id}: {e}")
        else:
            logger.info(f"Skipping AI analysis for email: {thread_id}")
            await ctx.db_manager.update_status(thread_id, "archived")
    else:
        logger.info(f"Email {thread_id} already exists in DB.")

    # 5. Mark as processed (Read) on Server
    try:
        success = await ctx.exchange_client.mark_as_read(thread_id, is_read=True)
        if success:
            logger.info(f"Email {thread_id} marked as read on server.")
        else:
            logger.warning(f"Failed to mark {thread_id} as read.")
    except Exception as e:
        logger.error(f"Exception marking email {thread_id} as read: {e}")

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
