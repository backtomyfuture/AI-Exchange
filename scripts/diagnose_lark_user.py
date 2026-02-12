#!/usr/bin/env python3
"""
Script to diagnose Lark user lookup issues.
Tests different user_id formats to see which ones work.
"""
import os
import sys

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import lark_oapi
from lark_oapi.api.contact.v3 import GetUserRequest

def main():
    # Initialize client
    app_id = os.getenv("LARK_APP_ID")
    app_secret = os.getenv("LARK_APP_SECRET")

    if not app_id or not app_secret:
        print("ERROR: LARK_APP_ID or LARK_APP_SECRET not set")
        return

    print(f"Using App ID: {app_id[:8]}...")

    client = lark_oapi.Client.builder() \
        .app_id(app_id) \
        .app_secret(app_secret) \
        .log_level(lark_oapi.LogLevel.ERROR) \
        .build()

    # Test user IDs to query
    test_users = ["zhang-xia", "yy-zhang1", "zhib_li", "q-fu"]

    print("\n=== Testing user_id lookup ===\n")

    for user_id in test_users:
        req = GetUserRequest.builder() \
            .user_id(user_id) \
            .user_id_type("user_id") \
            .build()
        
        resp = client.contact.v3.user.get(req)
        
        if resp.success():
            user = resp.data.user
            print(f"✅ {user_id}: {user.name} (open_id: {user.open_id[:20]}...)")
        else:
            print(f"❌ {user_id}: code={resp.code}, msg={resp.msg}")

if __name__ == "__main__":
    main()
