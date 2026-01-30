import time
import signal
import logging
from src.init_app import get_app_context
from src.utils import lark_app

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LarkService")

import asyncio
import threading

def main():
    """
    Main Entry point.
    Because Lark SDK is synchronous and runs in its own threads, 
    but we need to interact with AsyncGraph/AsyncPostgres,
    we act as the Bridge.
    
    1. We start a dedicated AsyncIO Worker Loop in a separate thread.
    2. We pass this loop to lark_app.
    3. lark_app uses run_coroutine_threadsafe to submit tasks to this loop.
    """
    logger.info("Starting Lark Service...")
    
    # 1. Create a dedicated asyncio loop for background tasks
    work_loop = asyncio.new_event_loop()
    
    def run_worker_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
        
    worker_thread = threading.Thread(target=run_worker_loop, args=(work_loop,), daemon=True)
    worker_thread.start()
    logger.info("Async Worker Thread started.")
    
    # 2. Initialize Shared App Context
    # We must do async setup INSIDE the worker loop via run_coroutine_threadsafe
    ctx = get_app_context()
    
    async def init_async_components():
        # Setup pool inside the worker loop
        await ctx.setup_async()
        
    future = asyncio.run_coroutine_threadsafe(init_async_components(), work_loop)
    future.result(timeout=10) # Wait for init to complete
    
    # 3. Initialize Lark App
    # Inject the worker loop so it can submit tasks later
    lark_app.init_lark_app(ctx.db_manager, ctx.graph, ctx.exchange_client, worker_loop_arg=work_loop)
    
    # 4. Start WebSocket Client (Internal thread)
    lark_app.start_lark_ws()
    
    logger.info("Lark Service is running and listening for events.")
    
    # 5. Keep Process Alive
    stop_signal = False
    
    def signal_handler(sig, frame):
        nonlocal stop_signal
        logger.info("Shutdown signal received.")
        stop_signal = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not stop_signal:
            time.sleep(60)
            logger.info("Heartbeat: Lark Service is running...")
    except Exception as e:
        logger.error(f"LarkService Loop Error: {e}")
    finally:
        logger.info("Lark Service shutting down...")
        # Clean shutdown of worker loop
        work_loop.call_soon_threadsafe(work_loop.stop)
        worker_thread.join(timeout=5)
        ctx.close()

if __name__ == "__main__":
    main()
