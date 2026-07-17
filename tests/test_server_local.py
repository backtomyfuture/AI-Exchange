from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.server import app
from src.utils import lark_app


client = TestClient(app)


def test_production_email_preview_returns_404_before_any_context_lookup():
    with patch(
        "src.server.get_app_context",
        side_effect=AssertionError("production_preview_must_not_load_context"),
    ), patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(DEBUG=False, APP_ENV="production"),
    ):
        response = client.get("/email/private-exchange-id")

    assert response.status_code == 404


def test_production_debug_misconfiguration_cannot_read_seeded_preview_state():
    email_id = "test_push_production_debug_misconfiguration"

    class TrackingStore(dict):
        reads = 0

        def __contains__(self, key):
            self.reads += 1
            return super().__contains__(key)

        def __getitem__(self, key):
            self.reads += 1
            return super().__getitem__(key)

    store = TrackingStore({email_id: MagicMock()})
    with patch.object(lark_app, "_mock_store", store), patch(
        "src.server.get_app_context",
        side_effect=AssertionError("production_preview_must_not_load_context"),
    ), patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(DEBUG=True, APP_ENV="production"),
    ), patch(
        "src.utils.email_renderer.render_email_html",
        side_effect=AssertionError("production_preview_must_not_render"),
    ):
        response = client.get(f"/email/{email_id}")

    assert response.status_code == 404
    assert store.reads == 0


def test_debug_false_seeded_mock_returns_404_without_production_fallback():
    email_id = "test_push_seeded_but_disabled"
    lark_app._mock_store[email_id] = MagicMock()
    try:
        with patch(
            "src.server.get_app_context",
            side_effect=AssertionError("disabled_preview_must_not_load_context"),
        ), patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(DEBUG=False, APP_ENV="development"),
        ):
            response = client.get(f"/email/{email_id}")
    finally:
        lark_app._mock_store.pop(email_id, None)

    assert response.status_code == 404


def test_debug_non_test_or_unseeded_id_returns_404_without_context():
    with patch(
        "src.server.get_app_context",
        side_effect=AssertionError("debug_preview_must_not_fall_through"),
    ), patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(DEBUG=True, APP_ENV="development"),
    ):
        real_id = client.get("/email/REAL-EXCHANGE-ID")
        unseeded_id = client.get("/email/test_push_unseeded")

    assert real_id.status_code == 404
    assert unseeded_id.status_code == 404


def test_explicit_seeded_debug_preview_never_initializes_production_context():
    email_id = "test_push_server_boundary"
    state = MagicMock()
    state.values = {
        "email": {
            "id": email_id,
            "subject": "Debug subject",
            "sender": "Sender <sender@example.com>",
            "to": ["Receiver <receiver@example.com>"],
            "cc": [],
            "body": "<h1>DEBUG-BODY</h1>",
            "received_at": "2026-07-12T00:00:00Z",
        }
    }
    lark_app._mock_store[email_id] = state
    try:
        with patch(
            "src.server.get_app_context",
            side_effect=AssertionError("debug_preview_must_not_load_context"),
        ), patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(DEBUG=True, APP_ENV="development"),
        ):
            response = client.get(f"/email/{email_id}")
    finally:
        lark_app._mock_store.pop(email_id, None)

    assert response.status_code == 200
    assert "Debug subject" in response.text
    assert "DEBUG-BODY" in response.text


def test_debug_injection_seeds_and_deletes_only_test_namespace():
    email_id = "test_push_debug_seed"
    lark_app._mock_store.pop(email_id, None)
    settings = SimpleNamespace(DEBUG=True, APP_ENV="development")
    payload = {
        "id": email_id,
        "subject": "Debug seed",
        "sender": "sender@example.com",
        "to": ["recipient@example.com"],
        "cc": [],
        "body": "DEBUG-BODY",
        "received_at": "2026-07-12T00:00:00Z",
        "attachments": [],
        "draft": "DEBUG-DRAFT",
        "context": [{"id": "context-1"}],
        "classification": {"need_reply": True},
        "attachment_tokens": ["attachment-token"],
        "pdf_token": "pdf-token",
        "recipient_candidates": {"to": ["candidate-1"], "cc": []},
    }
    try:
        with patch("src.server.get_settings", return_value=settings):
            created = client.post("/debug/inject_email", json=payload)
            values = lark_app._mock_store[email_id].values
            deleted = client.delete(f"/debug/inject_email/{email_id}")

        assert created.status_code == 200
        assert values["draft"] == "DEBUG-DRAFT"
        assert values["context"] == [{"id": "context-1"}]
        assert values["classification"] == {"need_reply": True}
        assert values["attachment_tokens"] == ["attachment-token"]
        assert values["pdf_token"] == "pdf-token"
        assert values["recipient_candidates"] == {
            "to": ["candidate-1"],
            "cc": [],
        }
        assert values["email"]["draft_to"] == ["recipient@example.com"]
        assert values["email"]["draft_cc"] == []
        assert deleted.status_code == 200
        assert deleted.json()["removed"] is True
    finally:
        lark_app._mock_store.pop(email_id, None)


def test_debug_injection_rejects_non_test_namespace():
    with patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(DEBUG=True, APP_ENV="development"),
    ):
        response = client.post(
            "/debug/inject_email",
            json={
                "id": "REAL-EXCHANGE-ID",
                "subject": "must reject",
                "sender": "sender@example.com",
                "to": ["recipient@example.com"],
                "body": "MOCK-BODY",
                "received_at": "2026-07-12T00:00:00Z",
            },
        )

    assert response.status_code == 400


def test_debug_delete_rejects_non_test_namespace_without_removing_state():
    email_id = "REAL-EXCHANGE-ID"
    state = MagicMock()
    lark_app._mock_store[email_id] = state
    try:
        with patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(DEBUG=True, APP_ENV="development"),
        ):
            response = client.delete(f"/debug/inject_email/{email_id}")

        assert response.status_code == 400
        assert lark_app._mock_store[email_id] is state
    finally:
        lark_app._mock_store.pop(email_id, None)
