from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.graph.state_factory import sanitize_graph_delta


MANUAL_REVIEW_CODES = frozenset(
    {
        "approval_handoff_failed",
        "approval_handoff_incomplete",
        "categorizer_input_too_large",
        "categorizer_model_failed",
        "content_guard_failed",
        "content_guard_rejected",
        "drafter_empty_response",
        "drafter_input_too_large",
        "drafter_model_failed",
        "draft_save_outcome_unknown",
        "empty_draft",
        "graph_rewrite_limit",
        "invalid_classification",
        "recipient_resolution_failed",
        "reviewer_input_too_large",
        "reviewer_model_failed",
        "reviewer_rewrite_limit",
        "reviewer_schema_invalid",
        "router_input_too_large",
        "router_execution_failed",
        "router_model_failed",
        "router_skill_failed",
        "send_completion_unconfirmed",
        "send_outcome_unknown",
        "self_healing_interrupted",
        "startup_ambiguous_send",
        "startup_incomplete_approval",
        "summary_input_too_large",
        "summary_model_failed",
    }
)
DEFAULT_MANUAL_REVIEW_CODE = "invalid_classification"


def normalize_manual_review_code(code: object) -> str:
    return code if isinstance(code, str) and code in MANUAL_REVIEW_CODES else (
        DEFAULT_MANUAL_REVIEW_CODE
    )


def manual_review_classification(code: object) -> dict[str, Any]:
    """Return a bounded classification that never implies no action."""
    safe_code = normalize_manual_review_code(code)
    return {
        "priority": "P1",
        "intent": "审批",
        "summary": "需要人工审核",
        "reasoning": safe_code,
        "confidence": 0.0,
    }


def build_manual_review_delta(
    state: Mapping[str, Any],
    code: object,
    *,
    classification: Mapping[str, Any] | None = None,
    review_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the sole fail-closed, checkpoint-safe manual-review delta."""
    safe_code = normalize_manual_review_code(code)
    delta: dict[str, Any] = {
        "approval_status": "manual_review",
        "next_step": "manual_review",
        "safe_error_summary": safe_code,
    }
    if classification is not None:
        delta["classification"] = dict(classification)
    if review_result is not None:
        delta["review_result"] = dict(review_result)
    return sanitize_graph_delta(state, delta)
