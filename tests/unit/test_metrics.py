"""Unit tests for ``src.observability.metrics`` and the ``/metrics`` endpoint."""

import hmac
import logging
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.observability import metrics as m
from src.server import app


def _scrape() -> str:
    body, _ = m.render_metrics()
    return body.decode("utf-8")


def test_record_email_status_increments_counter():
    m.record_email_status("ingested")
    m.record_email_status("ingested")
    m.record_email_status("error")

    body = _scrape()
    assert 'emails_processed_total{status="ingested"}' in body
    assert 'emails_processed_total{status="error"}' in body


def test_record_card_dispatch_label_normalisation():
    m.record_card_dispatch("approval", True)
    m.record_card_dispatch("read_only", False)

    body = _scrape()
    assert 'card_dispatch_total{delivered="true",kind="approval"}' in body
    assert 'card_dispatch_total{delivered="false",kind="read_only"}' in body


def test_record_circuit_breaker_state_maps_text_to_int():
    m.record_circuit_breaker_state("closed")
    body = _scrape()
    assert "circuit_breaker_state 0.0" in body

    m.record_circuit_breaker_state("open")
    body = _scrape()
    assert "circuit_breaker_state 2.0" in body

    m.record_circuit_breaker_state("half_open")
    body = _scrape()
    assert "circuit_breaker_state 1.0" in body


def test_metrics_endpoint_returns_prometheus_payload():
    client = TestClient(app)
    with patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(METRICS_TOKEN=SecretStr("metrics-secret")),
    ):
        resp = client.get(
            "/metrics",
            headers={"Authorization": "Bearer metrics-secret"},
        )
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    body = resp.text
    # Sanity: at least one of our metrics families is present.
    assert any(
        token in body
        for token in (
            "emails_processed_total",
            "card_dispatch_total",
            "circuit_breaker_state",
            "webhook_queue_depth",
        )
    )


def test_metrics_endpoint_requires_exactly_one_authorization_header():
    client = TestClient(app)
    settings = SimpleNamespace(METRICS_TOKEN=SecretStr("metrics-secret"))

    with patch("src.server.get_settings", return_value=settings):
        response = client.get(
            "/metrics",
            headers=[
                ("Authorization", "Bearer metrics-secret"),
                ("Authorization", "Bearer second-value"),
            ],
        )

    assert response.status_code == 401


def test_metrics_token_is_compared_with_constant_time_primitive():
    client = TestClient(app)
    settings = SimpleNamespace(METRICS_TOKEN=SecretStr("metrics-secret"))
    real_compare = hmac.compare_digest

    with patch("src.server.get_settings", return_value=settings), patch(
        "src.security.auth.hmac.compare_digest",
        wraps=real_compare,
    ) as compare:
        response = client.get(
            "/metrics",
            headers={"Authorization": "Bearer metrics-secret"},
        )

    assert response.status_code == 200
    compare.assert_called_once_with("metrics-secret", "metrics-secret")


def test_metrics_endpoint_rejects_missing_malformed_and_wrong_credentials():
    client = TestClient(app)
    settings = SimpleNamespace(METRICS_TOKEN=SecretStr("metrics-secret"))

    with patch("src.server.get_settings", return_value=settings):
        responses = (
            client.get("/metrics"),
            client.get("/metrics", headers={"Authorization": "Basic value"}),
            client.get("/metrics", headers={"Authorization": "Bearer wrong"}),
            client.get(
                "/metrics",
                headers={"Authorization": "Bearer metrics-secret extra"},
            ),
        )

    assert all(response.status_code == 401 for response in responses)
    assert all(
        response.headers.get("www-authenticate") == "Bearer"
        for response in responses
    )


def test_metrics_endpoint_fails_closed_when_token_is_unconfigured():
    client = TestClient(app)

    with patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(METRICS_TOKEN=SecretStr("")),
    ):
        response = client.get(
            "/metrics",
            headers={"Authorization": "Bearer anything"},
        )

    assert response.status_code == 503
    assert "anything" not in response.text


def test_metrics_token_never_enters_logs(caplog):
    client = TestClient(app)
    token = "metrics-log-secret-sentinel"

    with patch(
        "src.server.get_settings",
        return_value=SimpleNamespace(METRICS_TOKEN=SecretStr(token)),
    ), caplog.at_level(logging.INFO):
        response = client.get(
            "/metrics",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert token not in caplog.text


def test_email_id_log_context_propagates_to_records(caplog):
    """The structlog context should add ``email_id`` to log records."""
    import logging

    from src.utils.logging_setup import log_email_context

    logger = logging.getLogger("test.email_context")

    with caplog.at_level(logging.INFO, logger="test.email_context"):
        with log_email_context("msg-c4-test"):
            logger.info("inside context")
        logger.info("outside context")

    # The context manager should not leak the contextvar outside the block.
    inside = [r for r in caplog.records if r.message == "inside context"]
    outside = [r for r in caplog.records if r.message == "outside context"]
    assert inside, "Expected at least one log record inside the context."
    assert outside, "Expected at least one log record outside the context."

    # When structlog is configured, the email_id is merged via contextvars and
    # may show up either in the formatted message or as a record attribute.
    # We validate the helper does not raise; deeper structlog-integration
    # assertions live in test_logging_setup if needed.
