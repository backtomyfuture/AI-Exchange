import hashlib
import hmac
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from src.server import app, exchange_webhook


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


@pytest.mark.asyncio
async def test_exchange_webhook_aborts_stream_before_hmac_or_later_chunks():
    chunks = [
        {"type": "http.request", "body": b"1234", "more_body": True},
        {"type": "http.request", "body": b"56", "more_body": True},
        {"type": "http.request", "body": b"never-read", "more_body": False},
    ]
    receive_calls = 0

    async def receive():
        nonlocal receive_calls
        message = chunks[receive_calls]
        receive_calls += 1
        return message

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhooks/exchange",
            "headers": [(b"x-exchange-signature", b"not-used")],
        },
        receive,
    )

    with patch("src.server.get_settings") as mock_settings, patch(
        "src.server.hmac.new"
    ) as mock_hmac, patch("src.server.logger.info") as mock_log:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=5,
        )

        with pytest.raises(HTTPException) as caught:
            await exchange_webhook(request)

    assert caught.value.status_code == 413
    assert receive_calls == 2
    mock_hmac.assert_not_called()
    mock_log.assert_not_called()


@pytest.mark.asyncio
async def test_exchange_webhook_missing_signature_does_not_consume_stream():
    async def receive():
        raise AssertionError("unauthenticated webhook body must not be consumed")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhooks/exchange",
            "headers": [],
        },
        receive,
    )

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=5,
        )

        with pytest.raises(HTTPException) as caught:
            await exchange_webhook(request)

    assert caught.value.status_code == 400


@pytest.mark.asyncio
async def test_exchange_webhook_missing_secret_does_not_consume_stream():
    async def receive():
        raise AssertionError("unconfigured webhook body must not be consumed")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhooks/exchange",
            "headers": [(b"x-exchange-signature", b"present")],
        },
        receive,
    )

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="",
            WEBHOOK_MAX_BYTES=5,
        )

        with pytest.raises(HTTPException) as caught:
            await exchange_webhook(request)

    assert caught.value.status_code == 503


def test_exchange_webhook_allows_exact_limit_and_hashes_exact_raw_bytes():
    client = TestClient(app)
    payload = {"event_type": "NewMailEvent", "item_id": {"id": "精确字节"}}
    body, signature = _build_signed_body(payload, "test-secret")

    with patch("src.server.get_settings") as mock_settings, patch(
        "src.server.enqueue_exchange_webhook",
        new_callable=AsyncMock,
    ) as mock_enqueue:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=len(body),
        )
        mock_enqueue.return_value = {"queued": True}

        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={"X-Exchange-Signature": signature},
        )

    assert response.status_code == 200
    mock_enqueue.assert_awaited_once_with(payload, header_event=None)


def test_exchange_webhook_does_not_log_headers_signature_or_raw_body(caplog):
    client = TestClient(app)
    payload = {
        "event_type": "NewMailEvent",
        "item_id": {"id": "AAMkAGQ"},
        "private_field": "raw-body-secret",
    }
    body, signature = _build_signed_body(payload, "test-secret")
    caplog.set_level(logging.INFO, logger="WebServer")

    with patch("src.server.get_settings") as mock_settings, patch(
        "src.server.enqueue_exchange_webhook",
        new_callable=AsyncMock,
    ) as mock_enqueue:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        )
        mock_enqueue.return_value = {"queued": True}

        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "X-Exchange-Signature": signature,
                "X-Private-Header": "header-secret",
            },
        )

    assert response.status_code == 200
    rendered_logs = caplog.text
    assert signature not in rendered_logs
    assert "header-secret" not in rendered_logs
    assert "raw-body-secret" not in rendered_logs


def test_exchange_webhook_invalid_json_with_valid_signature_is_redacted(caplog):
    client = TestClient(app)
    body = b'{"private":"invalid-json-secret"'
    signature = hmac.new(b"test-secret", body, hashlib.sha256).hexdigest()
    caplog.set_level(logging.INFO, logger="WebServer")

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        )
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={"X-Exchange-Signature": signature},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid JSON payload"
    assert "invalid-json-secret" not in caplog.text
    assert signature not in caplog.text
