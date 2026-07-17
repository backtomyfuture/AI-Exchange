#!/usr/bin/env python3
"""
邮件恢复脚本 - 重新处理卡在 ingested/analyzed/error 状态的邮件。

用法:
    # 通过邮件 ID 重新处理
    python scripts/reprocess_email.py <email_id>

    # 列出所有卡住的邮件
    python scripts/reprocess_email.py --list-stuck

    # 重新处理所有卡住的邮件
    python scripts/reprocess_email.py --all-stuck
"""

import os
import sys
import asyncio
import argparse
import logging
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from src.config import get_settings  # noqa: E402
from src.db.maintenance_fence import RuntimeCheckpointMaintenanceFence  # noqa: E402
from src.db.runtime_boundary import (  # noqa: E402
    require_runtime_database_boundary,
)
from src.security.auth import validate_runtime_security  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("Reprocess")

STUCK_STATUSES = ("ingested", "analyzed", "error", "pending")
_CONTEXT_SHUTDOWN_SECONDS = 10.0


def _fail_stop_after_maintenance_fence_loss(_reason: str) -> None:
    logger.critical("Checkpoint maintenance lifecycle fence was lost")
    os._exit(70)


async def _close_fenced_context(ctx, fence) -> None:
    """Close the pool before releasing the dedicated maintenance fence."""

    if ctx is None:
        await fence.close()
        return

    def consume_task_result(task: asyncio.Task) -> None:
        try:
            task.result()
        except BaseException:
            pass

    try:
        close_task = asyncio.create_task(
            ctx.close(),
            name="reprocess-context-close",
        )
    except BaseException:
        logger.critical("Reprocess shutdown failed before checkpoint pool close")
        os._exit(70)
        raise RuntimeError("reprocess_shutdown_incomplete") from None

    try:
        done, _pending = await asyncio.wait(
            {close_task},
            timeout=max(0.0, _CONTEXT_SHUTDOWN_SECONDS),
        )
    except BaseException:
        close_task.cancel()
        close_task.add_done_callback(consume_task_result)
        logger.critical("Reprocess shutdown was interrupted before pool close")
        os._exit(70)
        raise RuntimeError("reprocess_shutdown_incomplete") from None

    if not done:
        close_task.cancel()
        close_task.add_done_callback(consume_task_result)
        logger.critical("Reprocess context shutdown exceeded its fixed deadline")
        os._exit(70)
        raise RuntimeError("reprocess_shutdown_incomplete")

    try:
        close_task.result()
    except BaseException:
        logger.critical("Reprocess shutdown failed before checkpoint pool close")
        os._exit(70)
        raise RuntimeError("reprocess_shutdown_incomplete") from None
    await fence.close()


async def list_stuck_emails(db_manager):
    """List emails that are stuck in intermediate states."""
    print("\n📋 查找卡住的邮件...")

    stuck_emails = []
    try:
        async with db_manager.get_connection() as conn:
            async with conn.cursor() as cur:
                placeholders = ", ".join(["%s"] * len(STUCK_STATUSES))
                await cur.execute(
                    f"SELECT id, subject, sender, status, updated_at FROM emails_log WHERE status IN ({placeholders}) ORDER BY updated_at DESC",
                    STUCK_STATUSES,
                )
                stuck_emails = await cur.fetchall()
    except Exception as exc:
        logger.error(
            "Failed to query stuck emails: error_type=%s",
            type(exc).__name__,
        )
        return []

    if not stuck_emails:
        print("  ✅ 没有卡住的邮件")
        return []

    print(f"\n  找到 {len(stuck_emails)} 封卡住的邮件:\n")
    print(f"  {'Status':12s} | {'Subject':40s} | ID")
    print(f"  {'-' * 12} | {'-' * 40} | {'-' * 30}")
    for email in stuck_emails:
        status = email.get("status", "N/A")
        subject = (email.get("subject", "N/A") or "N/A")[:40]
        eid = str(email.get("id", "N/A"))[:60]
        print(f"  {status:12s} | {subject:40s} | {eid}")

    return stuck_emails


async def reprocess_single(email_id: str, ctx):
    """Reprocess a single email by ID."""
    from src.domain.email_state import (
        SAFE_DUPLICATE_READ_STATUSES,
        ProcessingOutcome,
    )
    from src.exchange_service import process_and_archive_email

    logger.info(f"Reprocessing email: {email_id}")

    # Step 1: Try to get email data from Exchange API
    try:
        email_data = await ctx.exchange_client.get_email(email_id)
        if not email_data:
            logger.error(f"Could not fetch email {email_id} from Exchange API")
            return False

        email_data["id"] = email_id
        logger.info(f"Fetched email: {email_data.get('subject', 'N/A')}")
    except Exception as exc:
        logger.error(
            "Error fetching email: error_type=%s",
            type(exc).__name__,
        )
        return False

    # Preserve the row/content_ref/draft and use the production force contract.
    try:
        outcome = await process_and_archive_email(
            email_data,
            ctx,
            force_reprocess=True,
        )
        if outcome is ProcessingOutcome.PROCESSED:
            final_status = await ctx.db_manager.get_email_status(email_id)
            if final_status in SAFE_DUPLICATE_READ_STATUSES:
                logger.info("Email reprocessed successfully")
                return True
            logger.error(
                "Reprocessing did not reach a safe terminal status: status=%s",
                final_status or "missing",
            )
            return False
        logger.error("Reprocessing returned unexpected outcome: %s", outcome.value)
        return False
    except Exception as exc:
        logger.error(
            "Reprocessing failed: error_type=%s",
            type(exc).__name__,
        )
        return False


async def main():
    parser = argparse.ArgumentParser(description="Reprocess stuck emails")
    parser.add_argument("email_id", nargs="?", help="Email ID to reprocess")
    parser.add_argument(
        "--list-stuck", action="store_true", help="List all stuck emails"
    )
    parser.add_argument(
        "--all-stuck", action="store_true", help="Reprocess all stuck emails"
    )
    args = parser.parse_args()

    if not args.email_id and not args.list_stuck and not args.all_stuck:
        parser.print_help()
        return

    # Initialize app context behind the same dual checkpoint fence as main.
    from src.init_app import get_app_context
    from src.utils import lark_app

    settings = get_settings()
    validate_runtime_security(settings)
    await require_runtime_database_boundary(settings)
    fence = RuntimeCheckpointMaintenanceFence(
        settings.database_url,
        fail_stop=_fail_stop_after_maintenance_fence_loss,
    )
    ctx = None
    await fence.start()
    try:
        ctx = get_app_context()
        ctx.bind_checkpoint_write_guard(fence.assert_held)
        await ctx.setup_async()
        lark_app.init_lark_app(
            ctx.db_manager,
            ctx.graph,
            ctx.exchange_client,
            worker_loop_arg=asyncio.get_running_loop(),
            dependencies=ctx.graph_dependencies,
        )
        logger.info("App context initialized.")

        if args.list_stuck:
            await list_stuck_emails(ctx.db_manager)
        elif args.all_stuck:
            stuck = await list_stuck_emails(ctx.db_manager)
            if stuck:
                print(f"\n🔄 开始重新处理 {len(stuck)} 封邮件...")
                success = 0
                for email in stuck:
                    eid = email.get("id")
                    if eid:
                        if await reprocess_single(eid, ctx):
                            success += 1
                        await asyncio.sleep(2)  # Rate limit between emails
                print(f"\n📊 完成: {success}/{len(stuck)} 封邮件成功重新处理")
        elif args.email_id:
            await reprocess_single(args.email_id, ctx)
    finally:
        await _close_fenced_context(ctx, fence)


if __name__ == "__main__":
    asyncio.run(main())
