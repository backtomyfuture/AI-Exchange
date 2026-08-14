"""Write human approval outcomes back into the routing-sample pool."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from src.handoff.labels import LabelOutcome, eligible_for_tier2, label_source_for
from src.observability.metrics import (
    record_approval_latency,
    record_approval_quality,
    record_reviewer_reject,
)
from src.router.decision import RouteDecision


logger = logging.getLogger(__name__)


def drafts_differ(original: object, final: object) -> bool:
    left = " ".join(str(original or "").split())
    right = " ".join(str(final or "").split())
    return bool(left) and bool(right) and left != right


def record_human_route_outcome(
    processor: object,
    *,
    email_id: str,
    route_decision: object,
    classification: Mapping[str, Any] | None = None,
    outcome: LabelOutcome,
    original_draft: object = None,
    final_draft: object = None,
    waiting_since: datetime | None = None,
    decided_at: datetime | None = None,
) -> bool:
    """Persist a human decision as a routing label and record quality metrics."""

    if not email_id or not callable(getattr(processor, "update_email_labels", None)):
        return False
    parsed: RouteDecision | None
    try:
        parsed = RouteDecision.model_validate(route_decision) if route_decision is not None else None
    except Exception:
        parsed = None
    draft_edited = outcome == "approved" and drafts_differ(original_draft, final_draft)
    human_verified = outcome == "approved"
    classification = classification or {}
    try:
        updated = processor.update_email_labels(
            email_id,
            parsed.model_dump(mode="json") if parsed is not None else route_decision,
            classification.get("priority"),
            classification.get("intent"),
            classification.get("need_reply"),
            human_verified=human_verified,
            draft_edited=draft_edited,
            label_source=label_source_for(parsed, outcome=outcome),
            eligible_for_tier2=eligible_for_tier2(
                decision=parsed,
                human_verified=human_verified,
                draft_edited=draft_edited,
                outcome=outcome,
            ),
        )
    except Exception as exc:
        logger.warning(
            "Human route label write failed: error_type=%s",
            type(exc).__name__,
        )
        return False
    if outcome == "approved":
        record_approval_quality(draft_edited=draft_edited)
        if waiting_since is not None:
            instant = decided_at or datetime.now(UTC)
            if waiting_since.tzinfo is None:
                waiting_since = waiting_since.replace(tzinfo=UTC)
            if instant.tzinfo is None:
                instant = instant.replace(tzinfo=UTC)
            record_approval_latency((instant - waiting_since).total_seconds())
    elif outcome in {"rejected", "expired"}:
        record_reviewer_reject(source="human" if outcome == "rejected" else "sla")
    return bool(updated)


__all__ = ["drafts_differ", "record_human_route_outcome"]
