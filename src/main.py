import asyncio
import logging
import signal
import sys
from src.init_app import get_app_context
from src.utils import lark_app
from src.exchange_service import main_loop as exchange_loop

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MainService")

async def main():
    """
    Unified Entry Point for AI Assistant Service.
    Runs:
      1. Lark WebSocket Client (Threaded)
      2. Exchange Email Polling Loop (Async)
    """
    logger.info("Starting AI Assistant Unified Service...")

    # 1. Initialize Shared Context
    ctx = get_app_context()
    await ctx.setup_async()
    
    # 2. Initialize Lark App (API & WS)
    # Lark WS runs in a separate thread but calls back into this asyncio loop
    # We pass the current running loop to ensure thread-safety for graph/db calls
    worker_loop = asyncio.get_running_loop()
    lark_app.init_lark_app(ctx.db_manager, ctx.graph, ctx.exchange_client, worker_loop_arg=worker_loop)
    
    # Start Lark WS in a background thread
    lark_app.start_lark_ws()
    
    # 3. Setup Shutdown Signals
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received shutdown signal: {sig}")
        # Schedule the stop event set in the loop
        worker_loop.call_soon_threadsafe(stop_event.set)

    # Register handlers for SIGINT and SIGTERM
    # Note: loop.add_signal_handler is safer for asyncio than signal.signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            worker_loop.add_signal_handler(sig, lambda s=sig: signal_handler(s, None))
        except NotImplementedError:
            # Fallback for Windows or non-main thread execution
            signal.signal(sig, signal_handler)

    # 4. Run Exchange Loop concurrently
    # The exchange loop was designed as 'run_forever', so we wrap it to be cancellable or modification
    # Actually src.exchange_service.main_loop runs `while True`. 
    # We should ideally modify it to accept a stop event, but for now we can run it as a task and cancel it.
    
    exchange_task = asyncio.create_task(exchange_loop())
    
    logger.info("Service is fully operational.")
    
    try:
        # Wait until a signal is received
        await stop_event.wait()
        logger.info("Stopping services...")
    finally:
        # Cancel Exchange Loop
        exchange_task.cancel()
        try:
            await exchange_task
        except asyncio.CancelledError:
            logger.info("Exchange loop cancelled.")
            
        # Cleanup Resources
        await ctx.close()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Fallback if signal handler doesn't catch it cleanly in some envs
        pass
