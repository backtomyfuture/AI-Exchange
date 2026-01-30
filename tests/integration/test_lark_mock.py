import unittest
import json
import sys
import os
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Mock lark_oapi before importing lark_app
sys.modules["lark_oapi"] = MagicMock()
sys.modules["lark_oapi.api.im.v1"] = MagicMock()
sys.modules["lark_oapi.adapter.flask"] = MagicMock()
sys.modules["lark_oapi.ws"] = MagicMock()

from src.utils import lark_app

class TestLarkApp(unittest.TestCase):
    def test_build_approval_card(self):
        email_id = "123"
        draft = "Hello World"
        context = [{"body": "Previous context"}]
        email_data = {"subject": "Test Email", "sender": "test@example.com"}
        classification = {"reasoning": "Generic"}
        
        card = lark_app.build_approval_card(email_id, draft, context, email_data, classification)
        
        # Verify structure
        self.assertIn("header", card)
        self.assertIn("elements", card)
        self.assertEqual(card["header"]["title"]["content"], "📨 拟稿审批: Test Email")
        
        # Verify draft content in View Mode
        found_draft = False
        for el in card["elements"]:
            if "text" in el and "content" in el["text"] and "Hello World" in el["text"]["content"]:
                found_draft = True
        self.assertTrue(found_draft)

    def test_build_approval_card_edit_mode(self):
        email_id = "123"
        draft = "Draft"
        
        card = lark_app.build_approval_card(email_id, draft, [], {}, {}, is_edit_mode=True, feedback_value="Edited Draft")
        
        # Verify input element exists
        found_input = False
        for el in card["elements"]:
            if el.get("tag") == "input":
                self.assertEqual(el["default_value"], "Edited Draft")
                found_input = True
        self.assertTrue(found_input)

if __name__ == "__main__":
    unittest.main()
