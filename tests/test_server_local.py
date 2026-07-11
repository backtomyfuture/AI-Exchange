import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import os
import sys
from types import SimpleNamespace

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi.testclient import TestClient
from src.server import app
from src.storage import ContentRef
from src.storage import ContentStoreNotFoundError
from src.utils import lark_app

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
        mock_ctx.graph = MagicMock()
        mock_ctx.db_manager = MagicMock()
        mock_ctx.content_store = MagicMock()
        mock_get_ctx.return_value = mock_ctx
        ref = ContentRef(
            account_id=8,
            object_id="00000000-0000-4000-8000-000000000087",
            key_version="v1",
            sha256="8" * 64,
        )
        mock_ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
        mock_ctx.content_store.load_email = AsyncMock(
            return_value=self.mock_email_data
        )
        
        # Test Endpoint
        with patch("src.server.get_settings") as settings:
            settings.return_value.EXCHANGE_ACCOUNT_ID = 8
            response = self.client.get(f"/email/{self.email_id}")
        
        # Assertions
        self.assertEqual(response.status_code, 200)
        content = response.text
        
        self.assertIn("TEST: H5 View", content)
        self.assertIn("Sender &lt;sender@example.com&gt;", content)
        self.assertIn("Receiver &lt;receiver@example.com&gt;", content)
        self.assertIn("Content", content)
        self.assertIn("viewport", content) # Default viewport meta
        mock_ctx.db_manager.get_content_ref.assert_awaited_once_with(self.email_id)
        mock_ctx.content_store.load_email.assert_awaited_once_with(
            ref,
            include_attachments=True,
        )
        mock_ctx.graph.aget_state.assert_not_called()

    @patch("src.server.get_app_context")
    def test_mock_original_view_never_initializes_production_context(self, mock_get_ctx):
        email_id = "test_push_server_boundary"
        state = MagicMock()
        state.values = {"email": self.mock_email_data}
        lark_app._mock_store[email_id] = state
        try:
            with patch(
                "src.server.get_settings",
                return_value=SimpleNamespace(DEBUG=True),
            ):
                response = self.client.get(f"/email/{email_id}")
        finally:
            lark_app._mock_store.pop(email_id, None)

        self.assertEqual(response.status_code, 200)
        mock_get_ctx.assert_not_called()

    @patch("src.server.get_app_context")
    def test_debug_false_seeded_mock_uses_production_content_boundary(
        self,
        mock_get_ctx,
    ):
        email_id = "test_push_seeded_but_disabled"
        state = MagicMock()
        state.values = {"email": {**self.mock_email_data, "body": "MOCK-BODY"}}
        lark_app._mock_store[email_id] = state
        ref = ContentRef(
            account_id=8,
            object_id="00000000-0000-4000-8000-000000000127",
            key_version="v1",
            sha256="c" * 64,
        )
        ctx = MagicMock()
        ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
        ctx.content_store.load_email = AsyncMock(
            return_value={**self.mock_email_data, "id": email_id, "body": "PRODUCTION-BODY"}
        )
        mock_get_ctx.return_value = ctx
        try:
            with patch(
                "src.server.get_settings",
                return_value=SimpleNamespace(DEBUG=False, EXCHANGE_ACCOUNT_ID=8),
            ):
                response = self.client.get(f"/email/{email_id}")
        finally:
            lark_app._mock_store.pop(email_id, None)

        self.assertEqual(response.status_code, 200)
        self.assertIn("PRODUCTION-BODY", response.text)
        self.assertNotIn("MOCK-BODY", response.text)
        ctx.db_manager.get_content_ref.assert_awaited_once_with(email_id)
        ctx.content_store.load_email.assert_awaited_once_with(
            ref,
            include_attachments=True,
        )

    @patch("src.server.get_app_context")
    def test_debug_true_non_test_seed_cannot_shadow_production_content(
        self,
        mock_get_ctx,
    ):
        email_id = "REAL-EXCHANGE-ID"
        state = MagicMock()
        state.values = {"email": {**self.mock_email_data, "body": "MOCK-BODY"}}
        lark_app._mock_store[email_id] = state
        ref = ContentRef(
            account_id=8,
            object_id="00000000-0000-4000-8000-000000000147",
            key_version="v1",
            sha256="e" * 64,
        )
        ctx = MagicMock()
        ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
        ctx.content_store.load_email = AsyncMock(
            return_value={**self.mock_email_data, "id": email_id, "body": "PRODUCTION-BODY"}
        )
        mock_get_ctx.return_value = ctx
        try:
            with patch(
                "src.server.get_settings",
                return_value=SimpleNamespace(DEBUG=True, EXCHANGE_ACCOUNT_ID=8),
            ):
                response = self.client.get(f"/email/{email_id}")
        finally:
            lark_app._mock_store.pop(email_id, None)

        self.assertEqual(response.status_code, 200)
        self.assertIn("PRODUCTION-BODY", response.text)
        self.assertNotIn("MOCK-BODY", response.text)
        ctx.db_manager.get_content_ref.assert_awaited_once_with(email_id)

    @patch("src.server.get_app_context")
    def test_debug_prefix_without_seed_uses_production_content_boundary(
        self,
        mock_get_ctx,
    ):
        email_id = "test_push_unseeded_server"
        lark_app._mock_store.pop(email_id, None)
        ref = ContentRef(
            account_id=8,
            object_id="00000000-0000-4000-8000-000000000137",
            key_version="v1",
            sha256="d" * 64,
        )
        ctx = MagicMock()
        ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
        ctx.content_store.load_email = AsyncMock(
            return_value={**self.mock_email_data, "id": email_id}
        )
        mock_get_ctx.return_value = ctx

        with patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(DEBUG=True, EXCHANGE_ACCOUNT_ID=8),
        ):
            response = self.client.get(f"/email/{email_id}")

        self.assertEqual(response.status_code, 200)
        ctx.db_manager.get_content_ref.assert_awaited_once_with(email_id)
        ctx.content_store.load_email.assert_awaited_once_with(
            ref,
            include_attachments=True,
        )

    def test_debug_injection_seeds_complete_explicit_test_state(self):
        email_id = "test_push_debug_seed"
        lark_app._mock_store.pop(email_id, None)
        try:
            with patch(
                "src.server.get_settings",
                return_value=SimpleNamespace(DEBUG=True),
            ):
                response = self.client.post(
                    "/debug/inject_email",
                    json={
                        "id": email_id,
                        "subject": "Debug seed",
                        "sender": "sender@example.com",
                        "to": ["recipient@example.com"],
                        "cc": [],
                        "body": "DEBUG-BODY",
                        "received_at": "2026-07-11T00:00:00Z",
                        "attachments": [],
                        "draft": "DEBUG-DRAFT",
                        "context": [{"id": "context-1"}],
                        "classification": {"need_reply": True},
                        "attachment_tokens": ["attachment-token"],
                        "pdf_token": "pdf-token",
                        "recipient_candidates": {
                            "to": ["candidate-1"],
                            "cc": [],
                        },
                    },
                )

            self.assertEqual(response.status_code, 200)
            values = lark_app._mock_store[email_id].values
            self.assertEqual(values["draft"], "DEBUG-DRAFT")
            self.assertEqual(values["context"], [{"id": "context-1"}])
            self.assertEqual(values["classification"], {"need_reply": True})
            self.assertEqual(values["attachment_tokens"], ["attachment-token"])
            self.assertEqual(values["pdf_token"], "pdf-token")
            self.assertEqual(values["email"]["draft_to"], ["recipient@example.com"])
            self.assertEqual(values["email"]["draft_cc"], [])
            self.assertEqual(
                values["recipient_candidates"],
                {"to": ["candidate-1"], "cc": []},
            )
        finally:
            lark_app._mock_store.pop(email_id, None)

    def test_debug_injection_rejects_non_test_namespace(self):
        email_id = "REAL-EXCHANGE-ID"
        lark_app._mock_store.pop(email_id, None)
        with patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(DEBUG=True),
        ):
            response = self.client.post(
                "/debug/inject_email",
                json={
                    "id": email_id,
                    "subject": "must reject",
                    "sender": "sender@example.com",
                    "to": ["recipient@example.com"],
                    "body": "MOCK-BODY",
                    "received_at": "2026-07-11T00:00:00Z",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertNotIn(email_id, lark_app._mock_store)

    def test_debug_injection_delete_removes_only_explicit_test_state(self):
        email_id = "test_push_debug_delete"
        lark_app._mock_store[email_id] = MagicMock()
        try:
            with patch(
                "src.server.get_settings",
                return_value=SimpleNamespace(DEBUG=True),
            ):
                response = self.client.delete(
                    f"/debug/inject_email/{email_id}"
                )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.json(),
                {"status": "ok", "id": email_id, "removed": True},
            )
            self.assertNotIn(email_id, lark_app._mock_store)
        finally:
            lark_app._mock_store.pop(email_id, None)

    def test_debug_injection_delete_rejects_non_test_namespace(self):
        email_id = "REAL-EXCHANGE-ID"
        lark_app._mock_store[email_id] = MagicMock()
        try:
            with patch(
                "src.server.get_settings",
                return_value=SimpleNamespace(DEBUG=True),
            ):
                response = self.client.delete(
                    f"/debug/inject_email/{email_id}"
                )

            self.assertEqual(response.status_code, 400)
            self.assertIn(email_id, lark_app._mock_store)
        finally:
            lark_app._mock_store.pop(email_id, None)

    @patch("src.server.get_app_context")
    def test_missing_content_ref_returns_safe_404_without_graph(self, mock_get_ctx):
        ctx = MagicMock()
        ctx.db_manager.get_content_ref = AsyncMock(return_value=None)
        mock_get_ctx.return_value = ctx

        response = self.client.get(f"/email/{self.email_id}")

        self.assertEqual(response.status_code, 404)
        ctx.graph.aget_state.assert_not_called()
        ctx.content_store.load_email.assert_not_called()

    @patch("src.server.get_app_context")
    def test_wrong_account_ref_returns_safe_404_before_store_load(self, mock_get_ctx):
        ctx = MagicMock()
        ref = ContentRef(
            account_id=9,
            object_id="00000000-0000-4000-8000-000000000097",
            key_version="v1",
            sha256="9" * 64,
        )
        ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
        mock_get_ctx.return_value = ctx

        with patch("src.server.get_settings") as settings:
            settings.return_value.EXCHANGE_ACCOUNT_ID = 8
            response = self.client.get(f"/email/{self.email_id}")

        self.assertEqual(response.status_code, 404)
        ctx.content_store.load_email.assert_not_called()
        ctx.graph.aget_state.assert_not_called()

    @patch("src.server.get_app_context")
    def test_store_not_found_and_email_id_mismatch_return_safe_404(self, mock_get_ctx):
        ctx = MagicMock()
        ref = ContentRef(
            account_id=8,
            object_id="00000000-0000-4000-8000-000000000107",
            key_version="v1",
            sha256="a" * 64,
        )
        ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
        ctx.content_store.load_email = AsyncMock(
            side_effect=ContentStoreNotFoundError("content_not_found")
        )
        mock_get_ctx.return_value = ctx

        with patch("src.server.get_settings") as settings:
            settings.return_value.EXCHANGE_ACCOUNT_ID = 8
            missing = self.client.get(f"/email/{self.email_id}")
            ctx.content_store.load_email.side_effect = None
            ctx.content_store.load_email.return_value = {
                **self.mock_email_data,
                "id": "different-email",
            }
            mismatch = self.client.get(f"/email/{self.email_id}")

        self.assertEqual(missing.status_code, 404)
        self.assertEqual(mismatch.status_code, 404)
        ctx.graph.aget_state.assert_not_called()

if __name__ == '__main__':
    unittest.main()
