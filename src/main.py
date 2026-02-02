import asyncio
import logging
import signal
import sys
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.init_app import get_app_context
from src.utils import lark_app
from src.exchange_service import main_loop as exchange_loop
from src.server import app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MainService")

# Global task references to prevent garbage collection
background_tasks = set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for the FastAPI app.
    Startup: Initialize Context, Start Lark WS, Start Exchange Loop.
    Shutdown: Stop Exchange Loop, Cleanup Context.
    """
    logger.info("Starting AI Assistant Unified Service (Web + Worker)...")

    # 1. Initialize Shared Context
    ctx = get_app_context()
    await ctx.setup_async()
    
    # 2. Initialize Lark App (API & WS)
    # We pass the current running loop (uvicorn's loop)
    worker_loop = asyncio.get_running_loop()
    lark_app.init_lark_app(ctx.db_manager, ctx.graph, ctx.exchange_client, worker_loop_arg=worker_loop)
    
    # Start Lark WS in a background thread
    lark_app.start_lark_ws()
    
    # 3. Run Exchange Loop as a background Task
    exchange_task = asyncio.create_task(exchange_loop())
    background_tasks.add(exchange_task)
    exchange_task.add_done_callback(background_tasks.discard)
    
    logger.info("Service is fully operational (Web Server running).")
    
    yield # Server runs here
    
    logger.info("Stopping services...")
    
    # Shutdown logic
    exchange_task.cancel()
    try:
        await exchange_task
    except asyncio.CancelledError:
        logger.info("Exchange loop cancelled.")
        
    await ctx.close()
    logger.info("Shutdown complete.")

app.router.lifespan_context = lifespan

if __name__ == "__main__":
    # Run uvicorn server
    uvicorn.run(app, host="0.0.0.0", port=8000)

