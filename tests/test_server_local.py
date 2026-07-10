import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import os
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi.testclient import TestClient
from src.server import app

class TestServer(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.email_id = "test_email_123"
        
        # Mock Graph State
        self.mock_email_data = {
            "id": self.email_id,
            "subject": "TEST: H5 View",
            "sender": "Sender <sender@example.com>",
            "to": ["Receiver <receiver@example.com>"],
            "cc": [],
            "body": "<h1>Content</h1><p>Body</p>",
            "received_at": "2023-11-01 10:00:00"
        }
        
    @patch("src.server.get_app_context")
    def test_view_email_render(self, mock_get_ctx):
        # Setup Mock Context
        mock_ctx = MagicMock()
        mock_graph = MagicMock()
        mock_ctx.graph = mock_graph
        mock_get_ctx.return_value = mock_ctx
        
        # Async Mock for aget_state
        async def mock_aget_state(config):
            state = MagicMock()
            state.values = {"email": self.mock_email_data}
            return state
            
        mock_graph.aget_state = AsyncMock(side_effect=mock_aget_state)
        
        # Test Endpoint
        response = self.client.get(f"/email/{self.email_id}")
        
        # Assertions
        self.assertEqual(response.status_code, 200)
        content = response.text
        
        self.assertIn("TEST: H5 View", content)
        self.assertIn("Sender &lt;sender@example.com&gt;", content)
        self.assertIn("Receiver &lt;receiver@example.com&gt;", content)
        self.assertIn("Content", content)
        self.assertIn("viewport", content) # Default viewport meta

if __name__ == '__main__':
    unittest.main()
