
import asyncio
import logging
import signal
import sys
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.config import get_settings
from src.utils.logging_setup import setup_logging

from src.init_app import get_app_context
from src.utils import lark_app
from src.exchange_service import start_worker as exchange_start_worker
from src.exchange_service import stop_worker as exchange_stop_worker
from src.utils.self_healing import SelfHealer
from src.scheduler.daily_summary import init_scheduler, run_scheduler
from src.scheduler.polling import run_polling_loop
from src.server import app

logger = logging.getLogger("MainService")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for the FastAPI app.
    Startup: Initialize Context, Start Lark WS, Start Exchange Loop.
    Shutdown: Stop Exchange Loop, Cleanup Context.
    """
    setup_logging(get_settings().LOG_LEVEL)
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
    
    # 3. Start webhook-driven Exchange worker
    await exchange_start_worker(ctx)

    # 4. Start Self-Healing worker
    self_healer = SelfHealer(ctx=ctx, interval_seconds=900)
    healing_task = asyncio.create_task(self_healer.start())

    # 5. Start Daily Summary scheduler
    init_scheduler(ctx.db_manager, lark_app)
    summary_task = asyncio.create_task(run_scheduler())

    # 6. Start Hybrid Polling scheduler (Catch-up)
    polling_interval = get_settings().POLLING_INTERVAL
    polling_task = asyncio.create_task(
        run_polling_loop(ctx, interval=polling_interval, startup_delay=polling_interval)
    )

    logger.info("Service is fully operational (Web Server running).")
    
    yield # Server runs here
    
    logger.info("Stopping services...")
    
    # Shutdown logic
    self_healer.stop()
    healing_task.cancel()
    summary_task.cancel()
    polling_task.cancel()
    try:
        await asyncio.gather(healing_task, summary_task, polling_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    await exchange_stop_worker()
    await ctx.close()
    logger.info("Shutdown complete.")

app.router.lifespan_context = lifespan

async def main():
    """Entrypoint for tests: init components and run background loop."""
    ctx = get_app_context()
    await ctx.setup_async()
    worker_loop = asyncio.get_running_loop()
    lark_app.init_lark_app(ctx.db_manager, ctx.graph, ctx.exchange_client, worker_loop_arg=worker_loop)
    lark_app.start_lark_ws()

    try:
        await exchange_start_worker(ctx)
        await asyncio.Future()
    except asyncio.CancelledError:
        await exchange_stop_worker()
        await ctx.close()
        raise


def run_server():
    """Entrypoint for CLI runtime."""
    setup_logging(get_settings().LOG_LEVEL)
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    run_server()
