
import sys
import os
import unittest
import json
from unittest.mock import MagicMock

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils import lark_app
from src.utils.lark_app import handle_card_action

class TestViewOriginal(unittest.TestCase):
    def setUp(self):
        # Setup mock data in _mock_store
        self.email_id = "test_push_VERIFY"
        
        # Populate mock store with data similar to push_test_card.py
        lark_app._mock_store[self.email_id] = MagicMock()
        lark_app._mock_store[self.email_id].values = {
            "email": {
                "subject": "TEST: Verify Outlook Headers",
                "sender": "name='System', email_address='sender@test.com'",
                "to": ["name='Receiver', email_address='receiver@test.com'"],
                "cc": ["name='CC User', email_address='cc@test.com'"],
                "received_at": "2023-11-01 10:00:00",
                "body": "<h1>Original Body Content</h1>"
            },
            "classification": {"reasoning": "Test"}
        }
        
        # Mock Graph (needed because logic calls it before checking mock store)
        lark_app.graph = MagicMock()
        async def mock_aget_state(*args, **kwargs):
            return None
        lark_app.graph.aget_state.side_effect = mock_aget_state

        # Mock Lark API Client
        lark_app.lark_api_client = MagicMock()
        
        self.captured_content = None
        def side_effect_create(req):
            # Capture content while file is open
            f = req.body.file
            f.seek(0)
            self.captured_content = f.read().decode('utf-8')
            
            resp = MagicMock()
            resp.success.return_value = True
            resp.data.file_key = "mock_file_key"
            return resp
            
        lark_app.lark_api_client.im.v1.file.create.side_effect = side_effect_create

    def test_view_original_headers(self):
        # Construct a mock event
        mock_event = MagicMock()
        mock_event.event.action.value = {"action": "view_original", "id": self.email_id}
        mock_event.event.operator.open_id = "user_123"
        # Mock message_id in context
        mock_event.event.context.open_message_id = "msg_thread_root_123"
        
        # Run handling
        handle_card_action(mock_event)
        
        # Verify NO file upload was called
        self.assertFalse(lark_app.lark_api_client.im.v1.file.create.called)
        
        # Check reply message calls
        reply_msg_call = lark_app.lark_api_client.im.v1.message.reply.call_args
        self.assertIsNotNone(reply_msg_call, "Reply was not called")
        
        # Inspect argument
        req_msg = reply_msg_call[0][0]
        self.assertEqual(req_msg.message_id, "msg_thread_root_123")
        
        # Check content has URL
        content_json = json.loads(req_msg.request_body.content)
        print(f"Content JSON: {content_json}")
        
        # Elements structure: headers, elements list. Button is in elements list.
        # Find button
        found_url = False
        elements = content_json.get("elements", [])
        for el in elements:
            if el.get("tag") == "action":
                for action in el.get("actions", []):
                    if action.get("url", "").endswith(f"/email/{self.email_id}"):
                        found_url = True
        
        self.assertTrue(found_url, "H5 URL not found in card")

if __name__ == '__main__':
    unittest.main()
