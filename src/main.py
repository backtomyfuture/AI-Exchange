import asyncio
import logging
import os
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.config import get_settings
from src.security.auth import validate_runtime_security
from src.utils.logging_setup import setup_logging

from src.db.schema import require_runtime_database
from src.db.maintenance_fence import RuntimeCheckpointMaintenanceFence
from src.db.runtime_boundary import require_runtime_database_boundary
from src.init_app import get_app_context
from src.utils import lark_app
from src.exchange_service import start_worker as exchange_start_worker
from src.exchange_service import stop_worker as exchange_stop_worker
from src.utils.self_healing import SelfHealer
from src.scheduler.daily_summary import init_scheduler, run_scheduler
from src.scheduler.polling import run_polling_loop
from src.server import app

logger = logging.getLogger("MainService")
_BACKGROUND_TASK_SHUTDOWN_SECONDS = 5.0
_EXCHANGE_WORKER_SHUTDOWN_SECONDS = 32.0
_LARK_SHUTDOWN_DRAIN_SECONDS = 30.0
_LARK_ACTION_SHUTDOWN_SECONDS = 32.0
_LARK_WS_JOIN_SECONDS = 5.0
_LARK_WS_SHUTDOWN_SECONDS = 11.0
_CONTEXT_SHUTDOWN_SECONDS = 10.0


def _fail_stop_after_maintenance_fence_loss(_reason: str) -> None:
    """Close async intake and terminate if the lock-holding DB session is lost."""

    logger.critical("Checkpoint maintenance lifecycle fence was lost")
    lark_app.disable_lark_intake()
    os._exit(70)


async def _shutdown_runtime_components(
    *,
    ctx,
    lark_initialized: bool,
    exchange_worker_start_attempted: bool,
    self_healer,
    background_tasks: list[asyncio.Task],
) -> None:
    """Attempt every shutdown stage before allowing the fence to be released."""

    failed_stages: list[str] = []

    def attempt_sync(stage: str, operation) -> None:
        try:
            operation()
        except BaseException:
            failed_stages.append(stage)

    def consume_task_result(task: asyncio.Task) -> None:
        try:
            task.result()
        except BaseException:
            pass

    async def attempt_async(
        stage: str,
        operation,
        *,
        timeout_seconds: float,
    ) -> None:
        try:
            task = asyncio.create_task(
                operation(),
                name=f"runtime-shutdown-{stage}",
            )
        except BaseException:
            failed_stages.append(stage)
            return

        done, _pending = await asyncio.wait(
            {task},
            timeout=max(0.0, timeout_seconds),
        )
        if not done:
            failed_stages.append(stage)
            task.cancel()
            task.add_done_callback(consume_task_result)
            return
        try:
            task.result()
        except BaseException:
            failed_stages.append(stage)

    try:
        if lark_initialized:
            attempt_sync("lark_intake_disable", lark_app.disable_lark_intake)
        if self_healer is not None:
            attempt_sync("self_healer_stop", self_healer.stop)

        for task in background_tasks:
            task.cancel()
        if background_tasks:
            done, pending = await asyncio.wait(
                background_tasks,
                timeout=_BACKGROUND_TASK_SHUTDOWN_SECONDS,
            )
            background_failed = bool(pending)
            for task in done:
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except BaseException:
                    background_failed = True
            for task in pending:
                task.add_done_callback(consume_task_result)
            if background_failed:
                failed_stages.append("background_tasks_stop")

        if exchange_worker_start_attempted:
            await attempt_async(
                "exchange_worker_stop",
                exchange_stop_worker,
                timeout_seconds=_EXCHANGE_WORKER_SHUTDOWN_SECONDS,
            )
        if lark_initialized:
            await attempt_async(
                "lark_actions_drain",
                lambda: lark_app.stop_lark_intake(
                    timeout_seconds=_LARK_SHUTDOWN_DRAIN_SECONDS
                ),
                timeout_seconds=_LARK_ACTION_SHUTDOWN_SECONDS,
            )
            await attempt_async(
                "lark_ws_stop",
                lambda: asyncio.to_thread(
                    lark_app.stop_lark_ws,
                    timeout_seconds=_LARK_WS_JOIN_SECONDS,
                ),
                timeout_seconds=_LARK_WS_SHUTDOWN_SECONDS,
            )
        await attempt_async(
            "context_close",
            ctx.close,
            timeout_seconds=_CONTEXT_SHUTDOWN_SECONDS,
        )

        if failed_stages:
            logger.critical(
                "Runtime shutdown failed closed: stages=%s",
                ",".join(failed_stages),
            )
            os._exit(70)
            raise RuntimeError("runtime_shutdown_incomplete")
    except asyncio.CancelledError:
        os._exit(70)
        raise RuntimeError("runtime_shutdown_incomplete") from None


async def _require_runtime_database_boundary(settings) -> None:
    """Apply the same read-only database boundary to every runtime entrypoint."""
    await require_runtime_database_boundary(
        settings,
        require_database=require_runtime_database,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for the FastAPI app.
    Startup: Initialize Context, Start Lark WS, Start Exchange Loop.
    Shutdown: Stop Exchange Loop, Cleanup Context.
    """
    settings = get_settings()
    validate_runtime_security(settings)
    setup_logging(settings.LOG_LEVEL)
    logger.info("Starting AI Assistant Unified Service (Web + Worker)...")

    # 0. Runtime startup is read-only. Deployment must run the explicit
    # bootstrap command before the service can become ready.
    try:
        await _require_runtime_database_boundary(settings)
    except Exception as exc:
        logger.error(
            "Database preflight failed at startup: error_type=%s",
            type(exc).__name__,
        )
        raise

    fence = RuntimeCheckpointMaintenanceFence(
        settings.database_url,
        fail_stop=_fail_stop_after_maintenance_fence_loss,
    )
    async with fence:
        ctx = get_app_context()
        lark_initialized = False
        exchange_worker_start_attempted = False
        self_healer = None
        background_tasks: list[asyncio.Task] = []
        try:
            # 1. Initialize Shared Context only after the shared maintenance
            # fence is held. No DB-writing runtime component may precede it.
            ctx.bind_checkpoint_write_guard(fence.assert_held)
            await ctx.setup_async()
            recovered_actions = (
                await ctx.db_manager.recover_incomplete_approval_states()
            )
            if recovered_actions:
                logger.warning(
                    "Moved incomplete approval/send actions to manual review: count=%d",
                    recovered_actions,
                )

            # 2. Initialize Lark App (API & WS).
            worker_loop = asyncio.get_running_loop()
            lark_app.init_lark_app(
                ctx.db_manager,
                ctx.graph,
                ctx.exchange_client,
                worker_loop_arg=worker_loop,
                dependencies=ctx.graph_dependencies,
            )
            lark_initialized = True
            lark_app.start_lark_ws()

            # 3. Start webhook-driven Exchange worker.
            exchange_worker_start_attempted = True
            await exchange_start_worker(ctx)

            # 4. Start Self-Healing worker.
            self_healer = SelfHealer(ctx=ctx, interval_seconds=900)
            background_tasks.append(asyncio.create_task(self_healer.start()))

            # 5. Start Daily Summary scheduler.
            init_scheduler(ctx.db_manager, lark_app)
            background_tasks.append(asyncio.create_task(run_scheduler()))

            # 5b. Start Memory Consolidation (runs alongside daily summary).
            from src.memory.consolidator import MemoryConsolidator

            consolidator = MemoryConsolidator(
                db_manager=ctx.db_manager,
                email_processor=ctx.email_processor,
            )

            async def _consolidation_loop():
                await asyncio.sleep(7200)
                while True:
                    try:
                        await consolidator.consolidate(days=7, min_records=10)
                        logger.info("Memory consolidation completed")
                    except asyncio.CancelledError:
                        break
                    except Exception as exc:
                        logger.error(
                            "Memory consolidation failed: error_type=%s",
                            type(exc).__name__,
                        )
                    await asyncio.sleep(86400)

            background_tasks.append(asyncio.create_task(_consolidation_loop()))

            # 6. Start Hybrid Polling scheduler (Catch-up).
            polling_interval = settings.POLLING_INTERVAL
            background_tasks.append(
                asyncio.create_task(
                    run_polling_loop(
                        ctx,
                        interval=polling_interval,
                        startup_delay=polling_interval,
                    )
                )
            )

            logger.info("Service is fully operational (Web Server running).")
            yield
        finally:
            logger.info("Stopping services...")
            await _shutdown_runtime_components(
                ctx=ctx,
                lark_initialized=lark_initialized,
                exchange_worker_start_attempted=exchange_worker_start_attempted,
                self_healer=self_healer,
                background_tasks=background_tasks,
            )
            logger.info("Shutdown complete.")


app.router.lifespan_context = lifespan


async def main():
    """Entrypoint for tests: init components and run background loop."""
    settings = get_settings()
    validate_runtime_security(settings)
    await _require_runtime_database_boundary(settings)
    fence = RuntimeCheckpointMaintenanceFence(
        settings.database_url,
        fail_stop=_fail_stop_after_maintenance_fence_loss,
    )
    async with fence:
        ctx = get_app_context()
        lark_initialized = False
        exchange_worker_start_attempted = False
        try:
            ctx.bind_checkpoint_write_guard(fence.assert_held)
            await ctx.setup_async()
            recovered_actions = (
                await ctx.db_manager.recover_incomplete_approval_states()
            )
            if recovered_actions:
                logger.warning(
                    "Moved incomplete approval/send actions to manual review: count=%d",
                    recovered_actions,
                )
            worker_loop = asyncio.get_running_loop()
            lark_app.init_lark_app(
                ctx.db_manager,
                ctx.graph,
                ctx.exchange_client,
                worker_loop_arg=worker_loop,
                dependencies=ctx.graph_dependencies,
            )
            lark_initialized = True
            lark_app.start_lark_ws()

            exchange_worker_start_attempted = True
            await exchange_start_worker(ctx)
            await asyncio.Future()
        finally:
            await _shutdown_runtime_components(
                ctx=ctx,
                lark_initialized=lark_initialized,
                exchange_worker_start_attempted=exchange_worker_start_attempted,
                self_healer=None,
                background_tasks=[],
            )


def run_server():
    """Entrypoint for CLI runtime."""
    settings = get_settings()
    validate_runtime_security(settings)
    setup_logging(settings.LOG_LEVEL)
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False)


if __name__ == "__main__":
    run_server()
