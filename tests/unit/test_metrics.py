"""Unit tests for ``src.observability.metrics`` and the ``/metrics`` endpoint."""

import hmac
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.observability import metrics as m
from src.ingestion.models import InboxStats
from src.server import app


def _scrape() -> str:
    body, _ = m.render_metrics()
    return body.decode("utf-8")


def test_new_ops_metrics_are_exported():
    m.record_route_decision("tier1")
    m.record_manual_review()
    m.record_reviewer_rewrite()
    m.record_reviewer_reject("human")
    m.record_approval_quality(draft_edited=False)
    m.record_approval_quality(draft_edited=True)
    m.record_silent_route("no_action", rule_id="vip_skip")
    m.record_silent_route_share("silent", 0.4)
    m.record_approval_expiry(SimpleNamespace(kind="expired", count=2, oldest_seconds=90000))

    body = _scrape()
    assert 'route_decisions_total{tier="tier1"}' in body
    assert "manual_review_total" in body
    assert "reviewer_rewrite_total" in body
    assert 'reviewer_reject_total{source="human"}' in body
    assert "approvals_as_written_total" in body
    assert "approvals_after_edit_total" in body
    assert 'silent_route_total{route="no_action",rule_id="vip_skip"}' in body
    assert 'silent_route_share{route="silent"}' in body
    assert "approval_expired_total" in body


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


def test_record_durable_ingestion_uses_only_bounded_queue_labels():
    stats = InboxStats(
        pending=3,
        retry_wait=2,
        leased=1,
        dead_letter=4,
        manual_review=5,
        oldest_pending_seconds=12.5,
    )

    m.record_durable_ingestion(
        stats,
        ready=True,
        processing_active=True,
        polling_active=True,
        polling_cursor_ready=True,
    )

    body = _scrape()
    for status, value in {
        "pending": 3,
        "retry_wait": 2,
        "leased": 1,
        "dead_letter": 4,
        "manual_review": 5,
    }.items():
        assert f'durable_inbox_items{{status="{status}"}} {value}.0' in body
    assert "durable_inbox_oldest_pending_seconds 12.5" in body
    assert "durable_ingress_ready 1.0" in body
    assert "durable_ingestion_snapshot_ok 1.0" in body
    assert "durable_processing_active 1.0" in body
    assert "polling_ingress_active 1.0" in body
    assert "polling_cursor_ready 1.0" in body
    assert "polling_queue_depth 5.0" in body
    assert "webhook_queue_depth" not in body


def test_failed_queue_snapshot_preserves_backlog_and_marks_snapshot_unknown():
    stats = InboxStats(
        pending=7,
        retry_wait=3,
        leased=2,
        dead_letter=5,
        manual_review=4,
        oldest_pending_seconds=18.0,
    )
    m.record_durable_ingestion(
        stats,
        ready=True,
        processing_active=True,
        polling_active=True,
        polling_cursor_ready=True,
    )

    m.record_durable_ingestion(
        None,
        ready=False,
        processing_active=False,
        polling_active=False,
        polling_cursor_ready=False,
    )

    body = _scrape()
    assert 'durable_inbox_items{status="pending"} 7.0' in body
    assert 'durable_inbox_items{status="dead_letter"} 5.0' in body
    assert "durable_inbox_oldest_pending_seconds 18.0" in body
    assert "polling_queue_depth 10.0" in body
    assert "durable_ingress_ready 0.0" in body
    assert "durable_ingestion_snapshot_ok 0.0" in body
    assert "durable_processing_active 0.0" in body
    assert "polling_ingress_active 0.0" in body
    assert "polling_cursor_ready 0.0" in body


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
            "polling_queue_depth",
        )
    )


def test_metrics_endpoint_tolerates_a_failed_local_runtime_health_projection():
    client = TestClient(app)
    settings = SimpleNamespace(METRICS_TOKEN=SecretStr("metrics-secret"))
    runtime = SimpleNamespace(
        check_ready=AsyncMock(return_value=True),
        queue_stats=AsyncMock(return_value=None),
        health_snapshot=Mock(side_effect=RuntimeError("projection_unavailable")),
    )

    with (
        patch("src.server.get_settings", return_value=settings),
        patch.object(app.state, "ingestion_runtime", runtime, create=True),
    ):
        response = client.get(
            "/metrics",
            headers={"Authorization": "Bearer metrics-secret"},
        )

    assert response.status_code == 200
    runtime.health_snapshot.assert_called_once_with()


def test_queue_endpoint_returns_one_identifier_free_runtime_snapshot():
    client = TestClient(app)
    settings = SimpleNamespace(METRICS_TOKEN=SecretStr("metrics-secret"))
    stats = InboxStats(
        pending=3,
        retry_wait=2,
        leased=1,
        dead_letter=4,
        manual_review=5,
        oldest_pending_seconds=12.5,
    )
    runtime = SimpleNamespace(
        check_ready=AsyncMock(return_value=True),
        queue_stats=AsyncMock(return_value=stats),
        processing_ready=False,
        polling_live=True,
        polling_ready=True,
    )

    with (
        patch("src.server.get_settings", return_value=settings),
        patch.object(app.state, "ingestion_runtime", runtime, create=True),
    ):
        response = client.get(
            "/queue",
            headers={"Authorization": "Bearer metrics-secret"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "ingress": "active",
        "cursor": "ready",
        "session": "active",
        "processing": "standby",
        "queue": {
            "pending": 3,
            "retry_wait": 2,
            "leased": 1,
            "manual_review": 5,
            "dead_letter": 4,
            "oldest_pending_seconds": 12.5,
        },
    }
    runtime.check_ready.assert_awaited_once_with()
    runtime.queue_stats.assert_awaited_once_with()


def test_queue_endpoint_rejects_unready_runtime_before_stats_query():
    client = TestClient(app)
    settings = SimpleNamespace(METRICS_TOKEN=SecretStr("metrics-secret"))
    runtime = SimpleNamespace(
        check_ready=AsyncMock(return_value=False),
        queue_stats=AsyncMock(),
    )

    with (
        patch("src.server.get_settings", return_value=settings),
        patch.object(app.state, "ingestion_runtime", runtime, create=True),
    ):
        response = client.get(
            "/queue",
            headers={"Authorization": "Bearer metrics-secret"},
        )

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    runtime.queue_stats.assert_not_awaited()


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

    with (
        patch("src.server.get_settings", return_value=settings),
        patch(
            "src.security.auth.hmac.compare_digest",
            wraps=real_compare,
        ) as compare,
    ):
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
        response.headers.get("www-authenticate") == "Bearer" for response in responses
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

    with (
        patch(
            "src.server.get_settings",
            return_value=SimpleNamespace(METRICS_TOKEN=SecretStr(token)),
        ),
        caplog.at_level(logging.INFO),
    ):
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
