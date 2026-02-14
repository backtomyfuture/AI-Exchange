import asyncio
import logging
from src.exchange_service import process_and_archive_email

logger = logging.getLogger("PollingScheduler")

async def run_polling_loop(ctx, interval: int, startup_delay: int = 10):
    """
    Periodic polling loop to fetch recent emails as a backup/catch-up mechanism.
    This runs alongside the webhook system.
    """
    logger.info(f"Polling scheduler started. Interval: {interval} seconds, Startup Delay: {startup_delay}s")
    
    # Intentionally sleep a bit on startup to avoid racing with initial webhook storms or startup logic
    if startup_delay > 0:
        await asyncio.sleep(startup_delay)

    while True:
        try:
            logger.info("Polling recent emails from Exchange (Catch-up)...")
            # Limit 20 to avoid overwhelming, this is just for catch-up
            # exclude_ids is empty because we rely on DB deduplication
            recent_emails = await ctx.exchange_client.get_recent_emails(limit=20, exclude_ids=[])
            
            for email_data in recent_emails:
                # process_and_archive_email handles deduplication internally via db_manager.log_initial_email
                # If it returns False (exists), logic stops.
                await process_and_archive_email(email_data, ctx, skip_analysis=False)
            
            logger.debug(f"Polling cycle checked {len(recent_emails)} items.")

        except asyncio.CancelledError:
            logger.info("Polling scheduler cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in polling loop: {e}")
        
        await asyncio.sleep(interval)
