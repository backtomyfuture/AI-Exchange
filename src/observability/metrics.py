"""
Prometheus metrics for AI Email Assistant.

Exposes counters / histograms / gauges that production needs to operate this
service: throughput, end-to-end latency, LLM call quality, queue depth, and
circuit-breaker visibility. The metrics module is import-side-effect free
beyond declaring metric objects on the global ``REGISTRY``; it is safe to
import from tests as long as those tests don't mutate the registry.

Conventions
-----------
- Counters end in ``_total``.
- Histograms end in ``_seconds``.
- Labels are kept low-cardinality (e.g. ``status``, ``kind``) to avoid blowing
  up the time-series count.
"""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Use the default global registry so prometheus_client's helpers like
# generate_latest() with no argument keep working in case other modules also
# emit metrics.
from prometheus_client import REGISTRY as _DEFAULT_REGISTRY  # noqa: F401


# ---------------------------------------------------------------------------
# Email pipeline
# ---------------------------------------------------------------------------

emails_processed_total = Counter(
    "emails_processed_total",
    "Total number of emails processed by the AI pipeline.",
    labelnames=("status",),
)
"""``status`` is one of ``ingested``, ``analyzed``, ``drafted``,
``waiting_approval``, ``notified_readonly``, ``skipped``, ``archived``,
``error``, ``delivery_failed``, ``approved``, ``rejected``, ``modified``,
``draft_saved``, ``read``."""

email_pipeline_duration_seconds = Histogram(
    "email_pipeline_duration_seconds",
    "Time spent processing a single email through the full pipeline.",
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300),
)


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

llm_calls_total = Counter(
    "llm_calls_total",
    "LLM API call attempts.",
    labelnames=("node", "outcome"),
)
"""``node`` is the high-level caller (``categorizer``/``drafter``/...).
``outcome`` is one of ``success``/``rate_limited``/``error``."""

llm_call_duration_seconds = Histogram(
    "llm_call_duration_seconds",
    "End-to-end LLM call duration including retries.",
    labelnames=("node",),
    buckets=(0.5, 1, 2, 4, 8, 16, 32, 64, 120),
)


# ---------------------------------------------------------------------------
# Card delivery
# ---------------------------------------------------------------------------

card_dispatch_total = Counter(
    "card_dispatch_total",
    "Lark card dispatch outcomes.",
    labelnames=("kind", "delivered"),
)
"""``kind`` in (``approval``, ``read_only``, ``skipped``);
``delivered`` is the string ``"true"``/``"false"``."""


# ---------------------------------------------------------------------------
# Polling queue / circuit-breaker visibility
# ---------------------------------------------------------------------------

polling_queue_depth = Gauge(
    "polling_queue_depth",
    "Current number of pending or retryable items from Exchange polling.",
)

durable_inbox_items = Gauge(
    "durable_inbox_items",
    "Current durable Inbox items by bounded status.",
    labelnames=("status",),
)

durable_inbox_oldest_pending_seconds = Gauge(
    "durable_inbox_oldest_pending_seconds",
    "Age of the oldest pending or retryable durable Inbox item.",
)

durable_ingress_ready = Gauge(
    "durable_ingress_ready",
    "Whether the schema, policy, authority, session and polling cursor are ready.",
)

durable_ingestion_snapshot_ok = Gauge(
    "durable_ingestion_snapshot_ok",
    "Whether the latest durable queue snapshot was read successfully.",
)

durable_processing_active = Gauge(
    "durable_processing_active",
    "Whether the configured durable processing worker and recovery loop are active.",
)

polling_ingress_active = Gauge(
    "polling_ingress_active",
    "Whether the single Exchange polling scheduler is live.",
)

polling_cursor_ready_gauge = Gauge(
    "polling_cursor_ready",
    "Whether polling completed baseline activation and has a usable cursor.",
)

circuit_breaker_state = Gauge(
    "circuit_breaker_state",
    "Circuit breaker state: 0=closed, 1=half_open, 2=open.",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CB_STATE_MAP = {"closed": 0, "half_open": 1, "open": 2}


def record_circuit_breaker_state(state: str) -> None:
    """Translate a textual circuit breaker state into the gauge's encoding."""
    circuit_breaker_state.set(_CB_STATE_MAP.get(state.lower(), 0))


def record_card_dispatch(kind: str, delivered: bool) -> None:
    """Increment the card dispatch counter with a normalised label."""
    card_dispatch_total.labels(
        kind=kind or "unknown",
        delivered="true" if delivered else "false",
    ).inc()


approval_pending_oldest_seconds = Gauge(
    "approval_pending_oldest_seconds",
    "Age of the oldest waiting_approval email.",
)

approval_expired_total = Counter(
    "approval_expired_total",
    "Approvals expired by the 24h SLA and moved to manual_review.",
)

route_decisions_total = Counter(
    "route_decisions_total",
    "Final route decisions by tier.",
    labelnames=("tier",),
)

manual_review_total = Counter(
    "manual_review_total",
    "Emails sent to manual review.",
)

reviewer_rewrite_total = Counter(
    "reviewer_rewrite_total",
    "Drafts sent back by the reviewer.",
)

reviewer_reject_total = Counter(
    "reviewer_reject_total",
    "Human or model rejections of a draft.",
    labelnames=("source",),
)

approval_latency_seconds = Histogram(
    "approval_latency_seconds",
    "Time from waiting_approval to a human decision.",
    buckets=(60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400),
)

approvals_as_written_total = Counter(
    "approvals_as_written_total",
    "Approvals sent without a human draft edit.",
)

approvals_after_edit_total = Counter(
    "approvals_after_edit_total",
    "Approvals sent after a human edited the draft.",
)

silent_route_total = Counter(
    "silent_route_total",
    "Emails ended as no_action or read_only.",
    labelnames=("route", "rule_id"),
)

silent_route_share = Gauge(
    "silent_route_share",
    "Seven-day share of silent routes versus the rolling baseline.",
    labelnames=("route",),
)


def record_email_status(status: str) -> None:
    """Increment ``emails_processed_total`` for the given status."""
    if not status:
        return
    emails_processed_total.labels(status=status).inc()


def record_approval_expiry(event) -> None:
    kind = getattr(event, "kind", "")
    if kind == "expired":
        approval_expired_total.inc(getattr(event, "count", 1) or 1)
    oldest = float(getattr(event, "oldest_seconds", 0.0) or 0.0)
    if oldest:
        approval_pending_oldest_seconds.set(max(0.0, oldest))


def record_route_decision(tier: str) -> None:
    route_decisions_total.labels(tier=tier or "unknown").inc()


def record_manual_review() -> None:
    manual_review_total.inc()


def record_reviewer_rewrite() -> None:
    reviewer_rewrite_total.inc()


def record_reviewer_reject(source: str = "human") -> None:
    reviewer_reject_total.labels(source=source or "human").inc()


def record_approval_latency(seconds: float) -> None:
    approval_latency_seconds.observe(max(0.0, float(seconds)))


def record_approval_quality(*, draft_edited: bool) -> None:
    if draft_edited:
        approvals_after_edit_total.inc()
    else:
        approvals_as_written_total.inc()


def record_silent_route(route: str, *, rule_id: str = "none") -> None:
    silent_route_total.labels(route=route or "unknown", rule_id=rule_id or "none").inc()


def record_silent_route_share(route: str, share: float) -> None:
    silent_route_share.labels(route=route or "unknown").set(max(0.0, float(share)))


def record_durable_ingestion(
    stats,
    *,
    ready: bool,
    processing_active: bool,
    polling_active: bool,
    polling_cursor_ready: bool,
) -> None:
    """Update the bounded polling queue and runtime health gauges."""

    durable_ingress_ready.set(1 if ready else 0)
    durable_processing_active.set(1 if processing_active else 0)
    polling_ingress_active.set(1 if polling_active else 0)
    polling_cursor_ready_gauge.set(1 if polling_cursor_ready else 0)
    if stats is None:
        durable_ingestion_snapshot_ok.set(0)
        return

    durable_ingestion_snapshot_ok.set(1)
    values = {
        "pending": int(getattr(stats, "pending", 0)),
        "retry_wait": int(getattr(stats, "retry_wait", 0)),
        "leased": int(getattr(stats, "leased", 0)),
        "manual_review": int(getattr(stats, "manual_review", 0)),
        "dead_letter": int(getattr(stats, "dead_letter", 0)),
    }
    for status, count in values.items():
        durable_inbox_items.labels(status=status).set(max(0, count))
    oldest = float(getattr(stats, "oldest_pending_seconds", 0.0))
    durable_inbox_oldest_pending_seconds.set(max(0.0, oldest))
    polling_queue_depth.set(values["pending"] + values["retry_wait"])


def render_metrics() -> tuple[bytes, str]:
    """Serialize the current registry for the ``/metrics`` HTTP endpoint."""
    return generate_latest(), CONTENT_TYPE_LATEST


__all__ = [
    "emails_processed_total",
    "email_pipeline_duration_seconds",
    "llm_calls_total",
    "llm_call_duration_seconds",
    "card_dispatch_total",
    "polling_queue_depth",
    "durable_inbox_items",
    "durable_inbox_oldest_pending_seconds",
    "durable_ingress_ready",
    "durable_ingestion_snapshot_ok",
    "durable_processing_active",
    "polling_ingress_active",
    "polling_cursor_ready_gauge",
    "circuit_breaker_state",
    "approval_pending_oldest_seconds",
    "approval_expired_total",
    "route_decisions_total",
    "manual_review_total",
    "reviewer_rewrite_total",
    "reviewer_reject_total",
    "approval_latency_seconds",
    "approvals_as_written_total",
    "approvals_after_edit_total",
    "silent_route_total",
    "silent_route_share",
    "record_card_dispatch",
    "record_circuit_breaker_state",
    "record_email_status",
    "record_approval_expiry",
    "record_route_decision",
    "record_manual_review",
    "record_reviewer_rewrite",
    "record_reviewer_reject",
    "record_approval_latency",
    "record_approval_quality",
    "record_silent_route",
    "record_silent_route_share",
    "record_durable_ingestion",
    "render_metrics",
    "CONTENT_TYPE_LATEST",
    "generate_latest",
    "CollectorRegistry",
]
