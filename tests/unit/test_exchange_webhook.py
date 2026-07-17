import asyncio
import hashlib
import hmac
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from src.domain.errors import IngressValidationCode, IngressValidationError
from src.ingestion.models import IngressReceipt
from src.ingestion.webhook import (
    TestWebhookReceipt as _TestWebhookReceipt,
    WebhookIngressUnavailable,
)
from src.server import app, exchange_webhook


def _build_signed_body(payload: dict, secret: str) -> tuple[bytes, str]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return body, signature


@pytest.fixture
def webhook_ingress_service(monkeypatch):
    service = SimpleNamespace(accept=AsyncMock())
    monkeypatch.setattr(
        app.state,
        "webhook_ingress_service",
        service,
        raising=False,
    )
    return service


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
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )

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


def test_exchange_webhook_overlong_json_integer_returns_fixed_400_not_500():
    client = TestClient(app, raise_server_exceptions=False)
    body = b'{"account_id":' + (b"9" * 5000) + b"}"
    signature = hmac.new(b"test-secret", body, hashlib.sha256).hexdigest()

    with patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        ),
    ):
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid JSON payload"}


@pytest.mark.asyncio
async def test_non_ascii_signature_is_rejected_before_body_consumption():
    async def receive():
        raise AssertionError("invalid signature must not consume request body")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhooks/exchange",
            "headers": [(b"x-exchange-signature", b"\xff")],
        },
        receive,
    )

    with (
        patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(
                EXCHANGE_WEBHOOK_SECRET="test-secret",
                WEBHOOK_MAX_BYTES=1_048_576,
            ),
        ),
        pytest.raises(HTTPException) as caught,
    ):
        await exchange_webhook(request)

    assert caught.value.status_code == 403
    assert caught.value.detail == "Invalid signature"


def test_exchange_webhook_valid_signature_returns_202_after_durable_acceptance(
    webhook_ingress_service,
):
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

    webhook_ingress_service.accept.return_value = IngressReceipt(
        inbox_id=str(uuid4()),
        duplicate=False,
    )

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(EXCHANGE_WEBHOOK_SECRET="test-secret")

        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Event": "NewMailEvent",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}
    webhook_ingress_service.accept.assert_awaited_once_with(
        raw_body=body,
        payload=payload,
        header_event="NewMailEvent",
    )


def test_exchange_webhook_durable_ingress_unavailable_returns_fixed_503(
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {
        "event_type": "NewMailEvent",
        "item_id": {"id": "AAMkAGQ"},
        "parent_folder_id": {"id": "INBOX"},
    }
    body, signature = _build_signed_body(payload, "test-secret")

    webhook_ingress_service.accept.side_effect = WebhookIngressUnavailable()

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(EXCHANGE_WEBHOOK_SECRET="test-secret")

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
    assert response.json() == {"detail": "Webhook ingress unavailable"}
    webhook_ingress_service.accept.assert_awaited_once_with(
        raw_body=body,
        payload=payload,
        header_event="NewMailEvent",
    )


def test_exchange_webhook_test_receipt_returns_200_without_durable_metadata(
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {
        "event": "TestEvent",
        "timestamp": 1_721_111_111,
        "account_id": 8,
        "message": "This is a test event from Exchange Gateway.",
    }
    body, signature = _build_signed_body(payload, "test-secret")
    webhook_ingress_service.accept.return_value = _TestWebhookReceipt()

    with patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        ),
    ):
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Event": "TestEvent",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "test": True}
    webhook_ingress_service.accept.assert_awaited_once_with(
        raw_body=body,
        payload=payload,
        header_event="TestEvent",
    )


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
            "headers": [
                (b"content-type", b"application/json"),
                (b"x-exchange-signature", b"0" * 64),
            ],
        },
        receive,
    )

    with (
        patch("src.server.get_settings") as mock_settings,
        patch("src.server.hmac.new") as mock_hmac,
        patch("src.server.logger.info") as mock_log,
    ):
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


def test_exchange_webhook_allows_exact_limit_and_passes_exact_signed_bytes(
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {"event_type": "NewMailEvent", "item_id": {"id": "精确字节"}}
    body, signature = _build_signed_body(payload, "test-secret")

    webhook_ingress_service.accept.return_value = IngressReceipt(
        inbox_id=str(uuid4()),
        duplicate=False,
    )

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=len(body),
        )

        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "X-Exchange-Signature": f"sha256={signature}",
            },
        )

    assert response.status_code == 202
    webhook_ingress_service.accept.assert_awaited_once_with(
        raw_body=body,
        payload=payload,
        header_event=None,
    )


def test_exchange_webhook_does_not_log_headers_signature_or_raw_body(
    caplog,
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {
        "event_type": "NewMailEvent",
        "item_id": {"id": "AAMkAGQ"},
        "private_field": "raw-body-secret",
    }
    body, signature = _build_signed_body(payload, "test-secret")
    caplog.set_level(logging.INFO, logger="WebServer")

    webhook_ingress_service.accept.return_value = IngressReceipt(
        inbox_id=str(uuid4()),
        duplicate=False,
    )

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        )

        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Signature": signature,
                "X-Private-Header": "header-secret",
            },
        )

    assert response.status_code == 202
    rendered_logs = caplog.text
    assert signature not in rendered_logs
    assert "header-secret" not in rendered_logs
    assert "raw-body-secret" not in rendered_logs


def test_exchange_webhook_rejects_non_json_media_type_before_body_consumption():
    client = TestClient(app)

    with (
        patch("src.server.get_settings") as mock_settings,
        patch("src.server.hmac.new") as mock_hmac,
    ):
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        )
        response = client.post(
            "/webhooks/exchange",
            content=b"private-body-must-not-be-parsed",
            headers={
                "Content-Type": "text/plain",
                "X-Exchange-Signature": "0" * 64,
            },
        )

    assert response.status_code == 415
    mock_hmac.assert_not_called()


def test_ingress_validation_error_returns_fixed_400(
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {
        "event_type": "NewMailEvent",
        "item_id": {"id": "AAMkAGQ"},
    }
    body, signature = _build_signed_body(payload, "test-secret")

    webhook_ingress_service.accept.side_effect = IngressValidationError(
        IngressValidationCode.HEADER_EVENT_MISMATCH
    )

    with patch("src.server.get_settings") as mock_settings:
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        )
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Signature": signature,
                "X-Exchange-Event": "CreatedEvent",
            },
        )

    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid webhook event"}
    webhook_ingress_service.accept.assert_awaited_once_with(
        raw_body=body,
        payload=payload,
        header_event="CreatedEvent",
    )


def test_webhook_response_and_logs_omit_internal_receipt_metadata(
    caplog,
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {
        "event_type": "NewMailEvent",
        "item_id": {"id": "private-email-id-sentinel"},
        "account_id": 8_888_887,
        "parent_folder_id": {"id": "private-folder-sentinel"},
    }
    body, signature = _build_signed_body(payload, "test-secret")
    private_inbox_id = str(uuid4())
    webhook_ingress_service.accept.return_value = IngressReceipt(
        inbox_id=private_inbox_id,
        duplicate=True,
    )

    with (
        patch("src.server.get_settings") as mock_settings,
        caplog.at_level(
            logging.INFO,
            logger="WebServer",
        ),
    ):
        mock_settings.return_value = SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        )
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}
    assert "private-email-id-sentinel" not in caplog.text
    assert "8888887" not in response.text
    assert "8888887" not in caplog.text
    assert "private-folder-sentinel" not in response.text
    assert "private-folder-sentinel" not in caplog.text
    assert private_inbox_id not in response.text
    assert private_inbox_id not in caplog.text


def test_exchange_webhook_invalid_receipt_returns_fixed_503(
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {"event_type": "NewMailEvent", "item_id": {"id": "AAMkAGQ"}}
    body, signature = _build_signed_body(payload, "test-secret")
    webhook_ingress_service.accept.return_value = object()

    with patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(
            EXCHANGE_WEBHOOK_SECRET="test-secret",
            WEBHOOK_MAX_BYTES=1_048_576,
        ),
    ):
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 503
    assert response.json() == {"detail": "Webhook ingress unavailable"}


@pytest.mark.asyncio
async def test_exchange_webhook_does_not_swallow_cancellation(
    webhook_ingress_service,
):
    payload = {"event_type": "NewMailEvent", "item_id": {"id": "AAMkAGQ"}}
    body, signature = _build_signed_body(payload, "test-secret")
    messages = iter(({"type": "http.request", "body": body, "more_body": False},))

    async def receive():
        return next(messages)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhooks/exchange",
            "app": app,
            "headers": [
                (b"content-type", b"application/json"),
                (b"x-exchange-signature", signature.encode("ascii")),
            ],
        },
        receive,
    )
    webhook_ingress_service.accept.side_effect = asyncio.CancelledError()

    with (
        patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(
                EXCHANGE_WEBHOOK_SECRET="test-secret",
                WEBHOOK_MAX_BYTES=1_048_576,
            ),
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await exchange_webhook(request)


def test_request_boundary_never_calls_legacy_or_external_effects_across_outcomes(
    webhook_ingress_service,
):
    client = TestClient(app)
    payload = {
        "event": "NewMailEvent",
        "account_id": 8,
        "item_id": {"id": "request-boundary-spy", "changekey": "v1"},
        "parent_folder_id": {"id": "INBOX"},
    }
    body, signature = _build_signed_body(payload, "test-secret")
    headers = {
        "Content-Type": "application/json",
        "X-Exchange-Event": "NewMailEvent",
        "X-Exchange-Signature": signature,
    }

    with (
        patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(
                EXCHANGE_WEBHOOK_SECRET="test-secret",
                WEBHOOK_MAX_BYTES=1_048_576,
            ),
        ),
        patch(
            "src.exchange_service.enqueue_webhook_event",
            new_callable=AsyncMock,
        ) as legacy_enqueue,
        patch(
            "src.exchange_service.process_and_archive_email",
            new_callable=AsyncMock,
        ) as legacy_processor,
        patch(
            "src.utils.exchange_api.ExchangeClient.get_email",
            new_callable=AsyncMock,
        ) as exchange_detail,
        patch(
            "src.utils.exchange_api.ExchangeClient.get_recent_emails",
            new_callable=AsyncMock,
        ) as exchange_list,
        patch(
            "src.utils.exchange_api.ExchangeClient.mark_as_read",
            new_callable=AsyncMock,
        ) as mailbox_mutation,
        patch("src.utils.lark_app.send_approval_card") as lark_send,
        patch("src.graph.builder.build_graph") as graph_build,
        patch(
            "langchain_openai.ChatOpenAI.ainvoke",
            new_callable=AsyncMock,
        ) as model_invoke,
        patch("src.utils.email_processor.QdrantClient.upsert") as qdrant_upsert,
    ):
        outcomes = (
            (_TestWebhookReceipt(), 200),
            (IngressReceipt(inbox_id=str(uuid4()), duplicate=False), 202),
            (IngressReceipt(inbox_id=str(uuid4()), duplicate=True), 202),
        )
        for outcome, expected_status in outcomes:
            webhook_ingress_service.accept.reset_mock(side_effect=True)
            webhook_ingress_service.accept.return_value = outcome
            response = client.post(
                "/webhooks/exchange",
                content=body,
                headers=headers,
            )
            assert response.status_code == expected_status

        webhook_ingress_service.accept.reset_mock(side_effect=True)
        webhook_ingress_service.accept.side_effect = IngressValidationError(
            IngressValidationCode.EVENT_UNSUPPORTED
        )
        response = client.post(
            "/webhooks/exchange",
            content=body,
            headers=headers,
        )
        assert response.status_code == 400

    for external_spy in (
        legacy_enqueue,
        legacy_processor,
        exchange_detail,
        exchange_list,
        mailbox_mutation,
        lark_send,
        graph_build,
        model_invoke,
        qdrant_upsert,
    ):
        external_spy.assert_not_called()


def test_missing_durable_service_returns_503_without_legacy_fallback(monkeypatch):
    client = TestClient(app)
    payload = {
        "event": "NewMailEvent",
        "account_id": 8,
        "item_id": {"id": "no-fallback", "changekey": "v1"},
        "parent_folder_id": {"id": "INBOX"},
    }
    body, signature = _build_signed_body(payload, "test-secret")
    monkeypatch.delattr(app.state, "webhook_ingress_service", raising=False)

    with (
        patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(
                EXCHANGE_WEBHOOK_SECRET="test-secret",
                WEBHOOK_MAX_BYTES=1_048_576,
            ),
        ),
        patch(
            "src.exchange_service.enqueue_webhook_event",
            new_callable=AsyncMock,
        ) as legacy_enqueue,
    ):
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
    assert response.json() == {"detail": "Webhook ingress unavailable"}
    legacy_enqueue.assert_not_called()


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
            headers={
                "Content-Type": "application/json",
                "X-Exchange-Signature": signature,
            },
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid JSON payload"
    assert "invalid-json-secret" not in caplog.text
    assert signature not in caplog.text
