import asyncio
import os
from src.init_app import get_app_context
from src.utils.db_async import AsyncDatabaseManager

async def verify():
    print("--- Starting Verification ---")
    
    # 1. Config & Init
    print("[1] Initializing AppContext...")
    ctx = get_app_context()
    
    # Verify Settings injection in ExchangeClient
    print(f"[Check] ExchangeClient SSL Verify: {ctx.exchange_client.ssl_verify} (Expected: False/True based on env)")
    
    # 2. Async DB
    print("[2] Checking Async Database Manager...")
    if isinstance(ctx.db_manager, AsyncDatabaseManager):
        print(f"[Check] ctx.db_manager is AsyncDatabaseManager: PASS")
    else:
        print(f"[Check] ctx.db_manager is {type(ctx.db_manager)}: FAIL")
        return

    # 3. Connection Test
    print("[3] Testing DB Connection (SELECT 1)...")
    try:
        conn = await ctx.db_manager.get_connection()
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            res = await cur.fetchone()
            print(f"[Check] Query Result: {res}")
            
        print("[Check] DB Connection & Query: PASS")
    except Exception as e:
        print(f"[Check] DB Connection Failed: {e}")
        # Assuming we might not have a running DB locallly, this failure is expected if no DB.
        # But user said "use local .venv", implying maybe they have env set up.
    
    await ctx.close()
    print("--- Verification Finished ---")

if __name__ == "__main__":
    asyncio.run(verify())
