from __future__ import annotations

import hashlib
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from src.security.redaction import exception_type, fingerprint_identifier


def test_identifier_fingerprint_is_stable_namespaced_and_single_line():
    raw = "ou_private-identifier\nlog-injection-sentinel"

    first = fingerprint_identifier(raw, namespace="actor")
    second = fingerprint_identifier(raw, namespace="actor")
    other = fingerprint_identifier(raw, namespace="email")
    different_value = fingerprint_identifier("another-value", namespace="actor")
    expected_digest = hashlib.sha256(b"actor\0" + raw.encode("utf-8")).hexdigest()[:16]

    assert first == second
    assert first != other
    assert first != different_value
    assert first == f"actor:{expected_digest}"
    assert len(first) == len("actor:") + 16
    assert raw not in first
    assert "\n" not in first and "\r" not in first
    assert fingerprint_identifier(None)
    assert fingerprint_identifier("")


def test_safe_log_metadata_allows_only_bounded_explicit_values():
    from src.security import redaction

    allowed = {"accepted", "unavailable"}

    assert redaction.safe_log_metadata("accepted", allowed_values=allowed) == "accepted"
    assert (
        redaction.safe_log_metadata("private-reason", allowed_values=allowed) == "other"
    )
    assert (
        redaction.safe_log_metadata("accepted\nsecret", allowed_values=allowed)
        == "other"
    )
    assert redaction.safe_log_metadata("x" * 80, allowed_values={"x" * 80}) == "other"


def test_security_logging_hardens_url_bearing_third_party_loggers():
    from src.utils.logging_setup import harden_third_party_loggers

    logger_names = ("httpx", "httpcore", "fontTools", "weasyprint", "Lark")
    original_levels = {
        logger_name: logging.getLogger(logger_name).level
        for logger_name in logger_names
    }
    try:
        harden_third_party_loggers()

        assert logging.getLogger("httpx").level >= logging.WARNING
        assert logging.getLogger("httpcore").level >= logging.WARNING
        assert logging.getLogger("fontTools").level >= logging.ERROR
        assert logging.getLogger("weasyprint").level >= logging.ERROR
        assert logging.getLogger("Lark").level > logging.CRITICAL
    finally:
        for logger_name, level in original_levels.items():
            logging.getLogger(logger_name).setLevel(level)


def test_console_logging_is_plain_readable_and_filters_fonttools_noise(capsys):
    from src.utils.logging_setup import setup_logging

    root = logging.getLogger()
    fonttools = logging.getLogger("fontTools")
    fonttools_subset = logging.getLogger("fontTools.subset")
    original_handlers = root.handlers[:]
    original_root_level = root.level
    original_fonttools_level = fonttools.level
    original_subset_level = fonttools_subset.level
    try:
        setup_logging("INFO")

        # Simulate WeasyPrint's capture_logs(), which temporarily changes the
        # fontTools parent logger to DEBUG during every PDF render.
        fonttools.setLevel(logging.DEBUG)
        fonttools_subset.info("Glyph IDs: [1, 2, 3]")
        fonttools_subset.error("Font subset failed")
        logging.getLogger("test.readability").info("Email processing completed")

        rendered = capsys.readouterr().err
        assert "Glyph IDs" not in rendered
        assert "Font subset failed" in rendered
        assert "Email processing completed" in rendered
        assert "test.readability" in rendered
        assert "[info" in rendered
        assert "\x1b[" not in rendered
    finally:
        root.handlers[:] = original_handlers
        root.setLevel(original_root_level)
        fonttools.setLevel(original_fonttools_level)
        fonttools_subset.setLevel(original_subset_level)


def test_lark_api_and_websocket_clients_never_enable_verbose_sdk_logs(monkeypatch):
    from src.utils import lark_app

    settings = SimpleNamespace(
        LARK_APP_ID="cli_safe_runtime",
        LARK_APP_SECRET="private-lark-secret",
        LARK_CHAT_ID="",
    )
    api_builder = MagicMock()
    api_builder.app_id.return_value = api_builder
    api_builder.app_secret.return_value = api_builder
    api_builder.log_level.return_value = api_builder
    api_builder.build.return_value = MagicMock()
    event_builder = MagicMock()
    event_builder.register_p2_card_action_trigger.return_value = event_builder
    event_builder.register_p2_im_message_receive_v1.return_value = event_builder
    event_builder.build.return_value = MagicMock()
    ws_client = MagicMock()
    thread = MagicMock()

    originals = (
        lark_app.lark_api_client,
        lark_app.lark_ws_client,
        lark_app.card_builder,
    )
    try:
        monkeypatch.setattr(lark_app, "get_settings", lambda: settings)
        monkeypatch.setattr(lark_app.lark_oapi.Client, "builder", lambda: api_builder)
        monkeypatch.setattr(
            lark_app.lark_oapi.EventDispatcherHandler,
            "builder",
            lambda *_args: event_builder,
        )
        ws_constructor = MagicMock(return_value=ws_client)
        monkeypatch.setattr(lark_app.lark_oapi.ws, "Client", ws_constructor)
        monkeypatch.setattr(lark_app, "LarkCardBuilder", MagicMock())
        monkeypatch.setattr(lark_app, "init_commands", MagicMock())
        monkeypatch.setattr(lark_app, "_register_builtin_commands", MagicMock())
        monkeypatch.setattr("threading.Thread", MagicMock(return_value=thread))

        lark_app.init_lark_app(MagicMock(), MagicMock(), MagicMock())
        lark_app.start_lark_ws()

        api_builder.log_level.assert_called_once_with(
            lark_app.lark_oapi.LogLevel.CRITICAL
        )
        assert ws_constructor.call_args.kwargs["log_level"] == (
            lark_app.lark_oapi.LogLevel.CRITICAL
        )
    finally:
        (
            lark_app.lark_api_client,
            lark_app.lark_ws_client,
            lark_app.card_builder,
        ) = originals


def test_exception_type_never_uses_exception_message():
    secret = "private-exception-sentinel"
    exc = RuntimeError(secret)

    rendered = exception_type(exc)

    assert rendered == "RuntimeError"
    assert secret not in rendered


def test_lark_message_success_log_omits_email_and_message_identifiers(caplog):
    from src.utils.lark_messaging import send_read_only_card

    raw_email_id = "private-email-id-log-sentinel"
    raw_message_id = "private-message-id-log-sentinel"
    response = MagicMock()
    response.success.return_value = True
    response.data.message_id = raw_message_id
    create = MagicMock(return_value=response)
    client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(create=create)))
    )
    builder = MagicMock()
    builder.build_read_only_card.return_value = {"elements": []}

    with (
        patch(
            "src.utils.lark_messaging.get_settings",
            return_value=SimpleNamespace(LARK_CHAT_ID="private-chat-id-sentinel"),
        ),
        caplog.at_level(logging.INFO, logger="src.utils.lark_messaging"),
    ):
        result = send_read_only_card(
            raw_email_id,
            [],
            {"subject": "private-subject-sentinel"},
            {},
            lark_api_client=client,
            card_builder=builder,
        )

    assert result is True
    assert raw_email_id not in caplog.text
    assert raw_message_id not in caplog.text
    assert "private-chat-id-sentinel" not in caplog.text


def test_card_builder_logs_counts_not_recipient_subject_or_pdf(caplog):
    from src.utils.card_builder import LarkCardBuilder

    private_values = (
        "private-sender@example.test",
        "private-recipient@example.test",
        "private-subject-log-sentinel",
        "https://private.example.test/file-token-sentinel",
    )
    builder = LarkCardBuilder(None, exchange_client=None)

    with (
        patch(
            "src.utils.card_builder.get_settings",
            return_value=SimpleNamespace(EXCHANGE_ACCOUNT_EMAIL=""),
        ),
        caplog.at_level(logging.INFO, logger="src.utils.card_builder"),
    ):
        builder.build_read_only_card(
            "private-email-id-sentinel",
            [],
            {
                "subject": private_values[2],
                "sender": private_values[0],
                "to": [private_values[1]],
                "cc": [],
                "body": "bounded body",
                "attachments": [],
            },
            {"reasoning": "bounded", "priority": "P1"},
            pdf_url=private_values[3],
        )

    for private_value in private_values:
        assert private_value not in caplog.text


@pytest.mark.asyncio
async def test_exchange_client_exception_log_omits_remote_exception_text(caplog):
    from src.config import Settings
    from src.utils.exchange_api import ExchangeClient

    secret = "private-exchange-exception-sentinel"
    client = ExchangeClient(
        Settings(
            _env_file=None,
            EXCHANGE_API_URL="https://exchange.internal.company/api/emails",
        )
    )
    failing_http = MagicMock()
    failing_http.is_closed = False
    failing_http.get = AsyncMock(side_effect=RuntimeError(secret))
    client._http_client = failing_http

    with caplog.at_level(logging.ERROR, logger="ExchangeClient"):
        result = await client.get_all_folders()

    assert result == {}
    assert secret not in caplog.text
    assert "RuntimeError" in caplog.text


def test_email_log_context_binds_only_fingerprint():
    from src.utils.logging_setup import log_email_context

    raw_email_id = "private-context-email-id-sentinel"
    context = MagicMock()
    context.__enter__.return_value = None
    context.__exit__.return_value = False

    with patch(
        "src.utils.logging_setup.structlog.contextvars.bound_contextvars",
        return_value=context,
    ) as bound:
        with log_email_context(raw_email_id):
            pass

    bound_value = bound.call_args.kwargs["email_id"]
    assert bound_value != raw_email_id
    assert raw_email_id not in bound_value
    assert "\n" not in bound_value


def test_circuit_breaker_stores_and_logs_only_error_type(caplog):
    from src.utils.circuit_breaker import CircuitBreaker

    secret = "private-circuit-breaker-error-sentinel"
    breaker = CircuitBreaker(failure_threshold=1)

    with caplog.at_level(logging.CRITICAL, logger="src.utils.circuit_breaker"):
        opened = breaker.report_failure(RuntimeError(secret))

    assert opened is True
    assert breaker.last_error == "RuntimeError"
    assert secret not in caplog.text
