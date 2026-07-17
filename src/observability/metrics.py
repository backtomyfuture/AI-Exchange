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
# Webhook queue / circuit-breaker visibility
# ---------------------------------------------------------------------------

webhook_queue_depth = Gauge(
    "webhook_queue_depth",
    "Current length of the Exchange webhook ingest queue.",
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
    "Whether the schema, policy, authority and Web session are ready.",
)

durable_ingestion_snapshot_ok = Gauge(
    "durable_ingestion_snapshot_ok",
    "Whether the latest durable queue snapshot was read successfully.",
)

durable_processing_active = Gauge(
    "durable_processing_active",
    "Whether final durable processing is active; Phase 2 remains standby.",
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


def record_email_status(status: str) -> None:
    """Increment ``emails_processed_total`` for the given status."""
    if not status:
        return
    emails_processed_total.labels(status=status).inc()


def record_durable_ingestion(stats, *, ready: bool) -> None:
    """Update the bounded Phase-2 queue and runtime health gauges."""

    durable_ingress_ready.set(1 if ready else 0)
    durable_processing_active.set(0)
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
    webhook_queue_depth.set(values["pending"] + values["retry_wait"])


def render_metrics() -> tuple[bytes, str]:
    """Serialize the current registry for the ``/metrics`` HTTP endpoint."""
    return generate_latest(), CONTENT_TYPE_LATEST


__all__ = [
    "emails_processed_total",
    "email_pipeline_duration_seconds",
    "llm_calls_total",
    "llm_call_duration_seconds",
    "card_dispatch_total",
    "webhook_queue_depth",
    "durable_inbox_items",
    "durable_inbox_oldest_pending_seconds",
    "durable_ingress_ready",
    "durable_ingestion_snapshot_ok",
    "durable_processing_active",
    "circuit_breaker_state",
    "record_card_dispatch",
    "record_circuit_breaker_state",
    "record_email_status",
    "record_durable_ingestion",
    "render_metrics",
    "CONTENT_TYPE_LATEST",
    "generate_latest",
    "CollectorRegistry",
]
