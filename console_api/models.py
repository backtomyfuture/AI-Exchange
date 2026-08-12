"""Stable JSON models exposed by the Operations Console."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ConsoleModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SenderInfo(ConsoleModel):
    name: str | None = None
    address: str | None = None


class EmailListItem(ConsoleModel):
    external_email_id: str
    inbox_id: str | None = None
    subject: str | None = None
    sender: SenderInfo | str | None = None
    received_at: datetime | None = None
    status: str
    route: str | None = None
    tier: str | None = None
    matched_rule_count: int | None = None
    requires_human: bool = False
    has_anomaly: bool = False
    updated_at: datetime | None = None


class EmailListResponse(ConsoleModel):
    items: list[EmailListItem]
    page: int
    page_size: int
    total: int


class TraceNode(ConsoleModel):
    id: str
    label: str
    kind: Literal[
        "ingestion",
        "intake_guard",
        "route_decision",
        "handoff",
        "draft",
        "approval",
        "send",
    ]
    status: Literal[
        "pending",
        "active",
        "waiting",
        "human_action",
        "completed",
        "not_triggered",
        "skipped",
        "failed",
        "unknown",
    ]
    timestamp: datetime | None = None
    summary: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    data_quality: Literal["ok", "missing", "inconsistent"] = "ok"
    safe_error_code: str | None = None
    business_detail: dict[str, Any] = Field(default_factory=dict)
    input_output: dict[str, Any] = Field(default_factory=dict)
    technical_detail: dict[str, Any] = Field(default_factory=dict)
    # Kept for one transition period so older local dashboard bundles can
    # continue reading the projection while the structured fields roll out.
    detail: dict[str, Any] = Field(default_factory=dict)


class TraceEdge(ConsoleModel):
    source: str
    target: str


class RouteEvaluationStep(ConsoleModel):
    tier: Literal["tier1", "tier2", "tier3"]
    status: Literal[
        "completed",
        "active",
        "waiting",
        "human_action",
        "not_triggered",
        "skipped",
        "failed",
        "unknown",
    ]
    summary: str
    continue_reason: str | None = None
    matched_rules: list[dict[str, Any]] = Field(default_factory=list)
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    model_result: dict[str, Any] | None = None
    safe_error_code: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    data_quality: Literal["ok", "missing", "inconsistent"] = "ok"


class RouteDecisionDetail(ConsoleModel):
    final_route: str | None = None
    final_tier: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    reason_code: str | None = None
    steps: list[RouteEvaluationStep] = Field(default_factory=list)
    decision_digest: str | None = None
    decision_data_quality: Literal["ok", "missing", "inconsistent"] = "missing"


class PipelineTrace(ConsoleModel):
    external_email_id: str
    inbox_id: str | None = None
    subject: str | None = None
    sender: SenderInfo | str | None = None
    current_status: str | None = None
    nodes: list[TraceNode]
    edges: list[TraceEdge]
    route_decision: RouteDecisionDetail | None = None
    updated_at: datetime | None = None


class RuleSummary(ConsoleModel):
    rule_id: str
    rule_version: int
    status: str
    route: str
    purpose: str | None = None
    owner: str | None = None
    filename: str


class RuleDetail(RuleSummary):
    manifest: dict[str, Any]


class RuleDraftRequest(ConsoleModel):
    rule_id: str | None = None
    manifest: dict[str, Any] | None = None
    raw_yaml: str | None = None

    @field_validator("raw_yaml")
    @classmethod
    def _bound_yaml_size(cls, value: str | None) -> str | None:
        if value is not None and len(value.encode("utf-8")) > 256 * 1024:
            raise ValueError("raw_yaml_too_large")
        return value


class RuleSaveResponse(ConsoleModel):
    rule: RuleDetail
    message: str
    written_path: str


class CompileIssueModel(ConsoleModel):
    rule_id: str | None = None
    code: str
    message: str


class RuleValidationResponse(ConsoleModel):
    valid: bool
    digest: str | None = None
    enabled_rule_count: int = 0
    errors: list[CompileIssueModel] = Field(default_factory=list)
    warnings: list[CompileIssueModel] = Field(default_factory=list)


class MatchTestRequest(ConsoleModel):
    external_email_id: str
    save_as: Literal["positive_cases", "negative_cases"] | None = None


class MatchTestResponse(ConsoleModel):
    rule_id: str
    external_email_id: str
    result: Literal["MATCHED", "NOT_MATCHED", "INDETERMINATE"]
    saved_as: Literal["positive_cases", "negative_cases"] | None = None
    case_id: str | None = None
