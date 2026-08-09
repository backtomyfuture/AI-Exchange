from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.graph.state_factory import sanitize_graph_delta
from src.router.decision import (
    DecisionOutcome,
    RouteDecision,
    RouteProvenance,
    RouteTier,
)
from src.router.tier1.schema import CanonicalRoute


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
        "feishu_delivery_outcome_unknown",
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

_REASON_LABELS: dict[str, str] = {
    "approval_handoff_failed": "审批流转失败，需人工核实",
    "approval_handoff_incomplete": "审批流转未完成，需人工核实",
    "categorizer_input_too_large": "邮件内容超出处理长度限制，需人工判断",
    "categorizer_model_failed": "邮件分类模型调用失败，需人工判断",
    "content_guard_failed": "内容安全检查未能完成，需人工核实",
    "content_guard_rejected": "AI 草稿疑似包含虚构内容（幻觉），需人工核实",
    "drafter_empty_response": "AI 未能生成回复草稿，需人工处理",
    "drafter_input_too_large": "邮件内容过长，AI 无法生成草稿，需人工处理",
    "drafter_model_failed": "草稿生成模型调用失败，需人工处理",
    "draft_save_outcome_unknown": "草稿保存结果未知，需人工核实",
    "empty_draft": "生成的草稿为空，需人工处理",
    "feishu_delivery_outcome_unknown": "飞书卡片送达结果未知，需人工核实",
    "graph_rewrite_limit": "AI 多次重写仍未通过审核，需人工处理",
    "invalid_classification": "邮件分类结果无效，需人工判断",
    "recipient_resolution_failed": "收件人解析失败，需人工核实",
    "reviewer_input_too_large": "内容过长导致审核未能完成，需人工判断",
    "reviewer_model_failed": "草稿审核模型调用失败，需人工判断",
    "reviewer_rewrite_limit": "草稿多次修改仍未通过审核，需人工处理",
    "reviewer_schema_invalid": "审核结果格式异常，需人工判断",
    "router_input_too_large": "邮件内容过长导致路由失败，需人工判断",
    "router_execution_failed": "路由执行失败，需人工判断",
    "router_model_failed": "路由模型调用失败，需人工判断",
    "router_skill_failed": "路由规则执行失败，需人工判断",
    "send_completion_unconfirmed": "发送结果未确认，需人工核实",
    "send_outcome_unknown": "发送结果未知，需人工核实",
    "self_healing_interrupted": "自愈流程被中断，需人工核实",
    "startup_ambiguous_send": "系统重启时发送状态不确定，需人工核实",
    "startup_incomplete_approval": "系统重启时审批流程未完成，需人工核实",
    "summary_input_too_large": "内容过长导致摘要生成失败，需人工判断",
    "summary_model_failed": "摘要生成模型调用失败，需人工判断",
}


def normalize_manual_review_code(code: object) -> str:
    return code if isinstance(code, str) and code in MANUAL_REVIEW_CODES else (
        DEFAULT_MANUAL_REVIEW_CODE
    )


def manual_review_reason_label(code: object) -> str:
    """Return a bounded, human-readable Chinese description for a review code."""
    safe_code = normalize_manual_review_code(code)
    return _REASON_LABELS.get(safe_code, _REASON_LABELS[DEFAULT_MANUAL_REVIEW_CODE])


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
    existing_decision = state.get("route_decision")
    if existing_decision is None:
        existing_decision = RouteDecision(
            outcome=DecisionOutcome.ERROR,
            route=CanonicalRoute.MANUAL_REVIEW,
            params={"reason_code": safe_code},
            provenance=RouteProvenance(
                tier=RouteTier.SYSTEM,
                source_version="manual-review-v1",
            ),
            reason_code=safe_code,
        ).model_dump(mode="json")
    delta["route_decision"] = existing_decision
    if classification is not None:
        delta["classification"] = dict(classification)
    if review_result is not None:
        delta["review_result"] = dict(review_result)
    return sanitize_graph_delta(state, delta)
