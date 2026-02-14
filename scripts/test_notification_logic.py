
import asyncio
import logging
from unittest.mock import MagicMock, AsyncMock

# Mock environment setup
import sys
import os
sys.path.append(os.getcwd())

from src.exchange_service import _dispatch_notification

# Mock logger
logging.basicConfig(level=logging.INFO)

async def test_notification_logic():
    print("Testing Notification Logic...")

    # Mock Context and Lark App
    ctx = MagicMock()
    ctx.db_manager.update_status = AsyncMock()
    
    # Mock lark_app module
    import src.utils.lark_app as lark_app
    lark_app.generate_and_upload_pdf = AsyncMock(return_value="http://mock.pdf.url")
    lark_app.send_read_only_card = MagicMock()
    lark_app.send_approval_card = MagicMock()

    # Case 1: Priority P1, Need Reply False -> Should send Read-Only
    print("\n[Case 1] High Priority (P1), No Reply Needed")
    pipeline_result_p1 = {
        "classification": {"priority": "P1", "intent": "通知", "need_reply": False},
        "email": {"subject": "Urgent Notice"},
        "draft": "",
        "context": []
    }
    await _dispatch_notification("email_p1", pipeline_result_p1, ctx, {})
    
    if lark_app.send_read_only_card.called:
        print("✅ SUCCESS: send_read_only_card called for P1 email.")
    else:
        print("❌ FAILED: send_read_only_card NOT called for P1 email.")

    # Reset mocks
    lark_app.send_read_only_card.reset_mock()
    lark_app.send_approval_card.reset_mock()

    # Case 2: Intent Notification, Need Reply False -> Should send Read-Only
    print("\n[Case 2] Intent '通知', No Reply Needed")
    pipeline_result_notify = {
        "classification": {"priority": "P2", "intent": "通知", "need_reply": False},
        "email": {"subject": "General Notification"},
    }
    await _dispatch_notification("email_notify", pipeline_result_notify, ctx, {})

    if lark_app.send_read_only_card.called:
        print("✅ SUCCESS: send_read_only_card called for Notification email.")
    else:
        print("❌ FAILED: send_read_only_card NOT called for Notification email.")

    # Reset mocks
    lark_app.send_read_only_card.reset_mock()

    # Case 3: Low Priority, No Reply Needed -> Should SKIP
    print("\n[Case 3] Low Priority (P3), No Reply Needed")
    pipeline_result_low = {
        "classification": {"priority": "P3", "intent": "广告", "need_reply": False},
        "email": {"subject": "Spam"},
    }
    await _dispatch_notification("email_low", pipeline_result_low, ctx, {})

    if not lark_app.send_read_only_card.called and not lark_app.send_approval_card.called:
        print("✅ SUCCESS: No card sent for P3 email.")
    else:
        print("❌ FAILED: Card sent for P3 email unexpectedly.")

if __name__ == "__main__":
    asyncio.run(test_notification_logic())
