"""Bounded, non-authoritative routing evaluation records.

The canonical route decision remains owned by ``tier1_decisions``.  This
module only validates the small audit projection used by the local Operations
Console.  It intentionally rejects content-shaped keys so a trace can never
become a second email-content store.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


RouteEvaluationTier = Literal["tier1", "tier2", "tier3"]
RouteEvaluationOutcome = Literal[
    "matched",
    "abstain",
    "conflict",
    "error",
    "partial",
    "unavailable",
    "skipped",
    "unknown",
]

_FORBIDDEN_KEY_PARTS = (
    "attachment",
    "body",
    "content",
    "draft",
    "html",
    "prompt",
    "snippet",
    "text",
)


def _contains_forbidden_key(value: object, *, depth: int = 0) -> bool:
    if depth > 5:
        return False
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).casefold()
            if normalized == "content_ref" or any(
                part in normalized for part in _FORBIDDEN_KEY_PARTS
            ):
                return True
            if _contains_forbidden_key(item, depth=depth + 1):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_forbidden_key(item, depth=depth + 1) for item in value[:32])
    return False


class RouteEvaluationTrace(BaseModel):
    """One immutable, bounded observation of a routing tier."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    inbox_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(gt=0, le=16)
    tier: RouteEvaluationTier
    outcome: RouteEvaluationOutcome
    matched_rule_ids: list[dict[str, Any]] = Field(default_factory=list, max_length=32)
    candidate_routes: list[dict[str, Any]] = Field(default_factory=list, max_length=32)
    evidence_refs: list[dict[str, Any]] = Field(default_factory=list, max_length=32)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    continue_reason: str | None = Field(default=None, max_length=512)
    safe_reason: str | None = Field(default=None, max_length=512)
    started_at: datetime
    finished_at: datetime
    safe_detail_json: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _bounded_projection(self) -> "RouteEvaluationTrace":
        try:
            serialized = json.dumps(self.safe_detail_json, ensure_ascii=False)
            serialized_lists = (
                json.dumps(self.matched_rule_ids, ensure_ascii=False),
                json.dumps(self.candidate_routes, ensure_ascii=False),
                json.dumps(self.evidence_refs, ensure_ascii=False),
            )
        except (TypeError, ValueError):
            raise ValueError("route_evaluation_projection_not_json") from None
        if len(serialized.encode("utf-8")) > 16_384 or any(
            len(item.encode("utf-8")) > 16_384 for item in serialized_lists
        ):
            raise ValueError("route_evaluation_detail_too_large")
        if _contains_forbidden_key(self.safe_detail_json):
            raise ValueError("route_evaluation_content_forbidden")
        if _contains_forbidden_key(self.matched_rule_ids):
            raise ValueError("route_evaluation_rule_content_forbidden")
        if _contains_forbidden_key(self.candidate_routes):
            raise ValueError("route_evaluation_candidate_content_forbidden")
        if _contains_forbidden_key(self.evidence_refs):
            raise ValueError("route_evaluation_evidence_content_forbidden")
        if self.finished_at < self.started_at:
            raise ValueError("route_evaluation_time_order_invalid")
        return self


def validate_route_evaluation(
    evaluation: object,
    *,
    inbox_id: str,
    sequence: int,
) -> RouteEvaluationTrace:
    """Validate a runtime-produced record and bind it to the durable inbox."""

    if not isinstance(evaluation, dict):
        raise ValueError("route_evaluation_not_mapping")
    payload = dict(evaluation)
    payload["inbox_id"] = inbox_id
    payload["sequence"] = sequence
    return RouteEvaluationTrace.model_validate(payload)


__all__ = ["RouteEvaluationTrace", "validate_route_evaluation"]
