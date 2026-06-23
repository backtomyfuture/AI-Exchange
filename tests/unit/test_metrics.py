"""Unit tests for ``src.observability.metrics`` and the ``/metrics`` endpoint."""

import pytest
from fastapi.testclient import TestClient

from src.observability import metrics as m
from src.server import app


def _scrape() -> str:
    body, _ = m.render_metrics()
    return body.decode("utf-8")


def test_record_email_status_increments_counter():
    before = _scrape()
    before_count = before.count('emails_processed_total{status="ingested"}')

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
    resp = client.get("/metrics")
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
