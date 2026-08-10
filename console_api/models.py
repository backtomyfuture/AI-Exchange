"""Stable JSON models exposed by the Operations Console."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ConsoleModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EmailListItem(ConsoleModel):
    external_email_id: str
    inbox_id: str | None = None
    subject: str | None = None
    sender: str | None = None
    received_at: datetime | None = None
    status: str
    route: str | None = None
    tier: str | None = None
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
    status: Literal["pending", "active", "completed", "failed", "skipped", "unknown"]
    timestamp: datetime | None = None
    safe_error_code: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class TraceEdge(ConsoleModel):
    source: str
    target: str


class PipelineTrace(ConsoleModel):
    external_email_id: str
    inbox_id: str | None = None
    subject: str | None = None
    sender: str | None = None
    current_status: str | None = None
    nodes: list[TraceNode]
    edges: list[TraceEdge]


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
