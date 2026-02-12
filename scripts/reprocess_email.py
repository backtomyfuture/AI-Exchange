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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Reprocess")

STUCK_STATUSES = ("ingested", "analyzed", "error", "pending")


async def list_stuck_emails(db_manager):
    """List emails that are stuck in intermediate states."""
    print("\n📋 查找卡住的邮件...")
    
    stuck_emails = []
    try:
        conn = await db_manager.get_connection()
        async with conn.cursor() as cur:
            placeholders = ', '.join(['%s'] * len(STUCK_STATUSES))
            await cur.execute(
                f"SELECT id, subject, sender, status, updated_at FROM emails_log WHERE status IN ({placeholders}) ORDER BY updated_at DESC",
                STUCK_STATUSES
            )
            stuck_emails = await cur.fetchall()
    except Exception as e:
        logger.error(f"Failed to query stuck emails: {e}")
        return []
    
    if not stuck_emails:
        print("  ✅ 没有卡住的邮件")
        return []
    
    print(f"\n  找到 {len(stuck_emails)} 封卡住的邮件:\n")
    print(f"  {'Status':12s} | {'Subject':40s} | ID")
    print(f"  {'-'*12} | {'-'*40} | {'-'*30}")
    for email in stuck_emails:
        status = email.get('status', 'N/A')
        subject = (email.get('subject', 'N/A') or 'N/A')[:40]
        eid = str(email.get('id', 'N/A'))[:60]
        print(f"  {status:12s} | {subject:40s} | {eid}")
    
    return stuck_emails


async def reprocess_single(email_id: str, ctx):
    """Reprocess a single email by ID."""
    from src.exchange_service import process_and_archive_email
    from src.utils import lark_app
    
    logger.info(f"Reprocessing email: {email_id}")
    
    # Step 1: Try to get email data from Exchange API
    try:
        email_data = await ctx.exchange_client.get_email(email_id)
        if not email_data:
            logger.error(f"Could not fetch email {email_id} from Exchange API")
            return False
        
        email_data['id'] = email_id
        logger.info(f"Fetched email: {email_data.get('subject', 'N/A')}")
    except Exception as e:
        logger.error(f"Error fetching email {email_id}: {e}")
        return False
    
    # Step 2: Delete old DB record so process_and_archive_email treats it as new
    try:
        conn = await ctx.db_manager.get_connection()
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM emails_log WHERE id = %s", (email_id,))
        logger.info(f"Cleared old DB record for {email_id}")
    except Exception as e:
        logger.warning(f"Could not delete old record (may be fine): {e}")
    
    # Step 3: Run the full processing pipeline
    try:
        await process_and_archive_email(email_data, ctx)
        logger.info(f"✅ Successfully reprocessed {email_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Reprocessing failed for {email_id}: {e}")
        return False


async def main():
    parser = argparse.ArgumentParser(description="Reprocess stuck emails")
    parser.add_argument("email_id", nargs="?", help="Email ID to reprocess")
    parser.add_argument("--list-stuck", action="store_true", help="List all stuck emails")
    parser.add_argument("--all-stuck", action="store_true", help="Reprocess all stuck emails")
    args = parser.parse_args()
    
    if not args.email_id and not args.list_stuck and not args.all_stuck:
        parser.print_help()
        return
    
    # Initialize app context
    from src.init_app import get_app_context
    from src.utils import lark_app
    
    ctx = get_app_context()
    await ctx.setup_async()
    lark_app.init_lark_app(ctx.db_manager, ctx.graph, ctx.exchange_client, worker_loop_arg=asyncio.get_running_loop())
    logger.info("App context initialized.")
    
    try:
        if args.list_stuck:
            await list_stuck_emails(ctx.db_manager)
        elif args.all_stuck:
            stuck = await list_stuck_emails(ctx.db_manager)
            if stuck:
                print(f"\n🔄 开始重新处理 {len(stuck)} 封邮件...")
                success = 0
                for email in stuck:
                    eid = email.get('id')
                    if eid:
                        if await reprocess_single(eid, ctx):
                            success += 1
                        await asyncio.sleep(2)  # Rate limit between emails
                print(f"\n📊 完成: {success}/{len(stuck)} 封邮件成功重新处理")
        elif args.email_id:
            await reprocess_single(args.email_id, ctx)
    finally:
        await ctx.close()


if __name__ == "__main__":
    asyncio.run(main())
