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
    "circuit_breaker_state",
    "record_card_dispatch",
    "record_circuit_breaker_state",
    "record_email_status",
    "render_metrics",
    "CONTENT_TYPE_LATEST",
    "generate_latest",
    "CollectorRegistry",
]
