"""Single writer for Qdrant routing-label eligibility."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from src.router.decision import RouteDecision, RouteTier
from src.router.tier1.schema import CanonicalRoute


LabelOutcome = Literal["auto", "approved", "rejected", "expired", "manual_review"]

_EXCLUDED_TIERS = frozenset({RouteTier.TIER3, RouteTier.HISTORICAL_INFERRED})
_WRITING_ROUTES = frozenset({CanonicalRoute.REPLY, CanonicalRoute.FORWARD})


def eligible_for_tier2(
    *,
    decision: RouteDecision | Mapping[str, Any] | None,
    human_verified: bool = False,
    draft_edited: bool = False,
    outcome: LabelOutcome = "auto",
) -> bool:
    """Return whether this label may vote in Historical Route Consensus."""

    parsed = _as_decision(decision)
    if parsed is None:
        return False
    if parsed.provenance.tier in _EXCLUDED_TIERS:
        return False
    if draft_edited or outcome in {"rejected", "expired", "manual_review"}:
        return False
    if parsed.route in _WRITING_ROUTES and not human_verified:
        return False
    return parsed.outcome.value == "matched" and parsed.route is not None


def label_source_for(
    decision: RouteDecision | Mapping[str, Any] | None,
    *,
    outcome: LabelOutcome = "auto",
) -> str:
    if outcome == "expired":
        return "approval_expired"
    if outcome == "rejected":
        return "human_rejected"
    if outcome == "approved":
        return "human_approved"
    if outcome == "manual_review":
        return "manual_review"
    parsed = _as_decision(decision)
    if parsed is None:
        return "unknown"
    return parsed.provenance.tier.value


def _as_decision(decision: RouteDecision | Mapping[str, Any] | None) -> RouteDecision | None:
    if decision is None:
        return None
    if isinstance(decision, RouteDecision):
        return decision
    try:
        return RouteDecision.model_validate(decision)
    except Exception:
        return None


__all__ = ["LabelOutcome", "eligible_for_tier2", "label_source_for"]
