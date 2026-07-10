import hashlib
import hmac
import json
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from src.server import app


def _build_signed_body(payload: dict, secret: str) -> tuple[bytes, str]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return body, signature


def test_exchange_webhook_missing_signature_returns_400():
    client = TestClient(app)
    payload = {"event_type": "NewMailEvent", "item_id": {"id": "AAMkAGQ"}}

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(EXCHANGE_WEBHOOK_SECRET="test-secret")
        response = client.post("/webhooks/exchange", json=payload)

    assert response.status_code == 400
    assert "Missing signature" in response.text


def test_exchange_webhook_invalid_signature_returns_403():
    client = TestClient(app)
    payload = {"event_type": "NewMailEvent", "item_id": {"id": "AAMkAGQ"}}
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(EXCHANGE_WEBHOOK_SECRET="test-secret")
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Signature": "invalid-signature",
            },
        )

    assert response.status_code == 403
    assert "Invalid signature" in response.text


def test_exchange_webhook_valid_signature_enqueues_event():
    client = TestClient(app)
    payload = {
        "event": "NewMailEvent",
        "event_type": "NewMailEvent",
        "account_id": 1,
        "item_id": {"id": "AAMkAGQ", "changekey": "CQAAABYAAAB"},
        "parent_folder_id": {"id": "INBOX", "changekey": "AQAAABYAAAB"},
        "subject": "测试邮件",
        "sender": "sender@example.com",
        "received_time": "2023-10-27T10:00:00",
    }
    body, signature = _build_signed_body(payload, "test-secret")

    with patch("src.server.get_settings") as mock_settings, patch(
        "src.server.enqueue_exchange_webhook",
        new_callable=AsyncMock,
    ) as mock_enqueue:
        mock_settings.return_value = MagicMock(EXCHANGE_WEBHOOK_SECRET="test-secret")
        mock_enqueue.return_value = {"queued": True, "email_id": "AAMkAGQ", "queue_size": 1}

        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Event": "NewMailEvent",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["queued"] is True
    mock_enqueue.assert_awaited_once_with(payload, header_event="NewMailEvent")


def test_exchange_webhook_queue_full_returns_503():
    client = TestClient(app)
    payload = {
        "event_type": "NewMailEvent",
        "item_id": {"id": "AAMkAGQ"},
        "parent_folder_id": {"id": "INBOX"},
    }
    body, signature = _build_signed_body(payload, "test-secret")

    with patch("src.server.get_settings") as mock_settings, patch(
        "src.server.enqueue_exchange_webhook",
        new_callable=AsyncMock,
    ) as mock_enqueue:
        mock_settings.return_value = MagicMock(EXCHANGE_WEBHOOK_SECRET="test-secret")
        mock_enqueue.return_value = {
            "queued": False,
            "reason": "queue_full",
            "email_id": "AAMkAGQ",
            "queue_size": 500,
        }

        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Event": "NewMailEvent",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["reason"] == "queue_full"
