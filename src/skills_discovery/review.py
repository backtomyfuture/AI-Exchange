"""Candidate review, chronological replay, and safe promotion inputs.

Discovery is deliberately a read-only operation.  This module persists a local
review artifact so an operator can inspect and edit the proposed declarative
rule in a conversation before the separate *implementation API* writes it into
the production ``tier1_rules`` directory. It never loads, reloads, or sends
anything itself.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.router.tier1.dsl import EmailView, RuleEvalStatus, evaluate_match
from src.router.tier1.schema import AnchorGroup, ConditionNode
from src.skills_discovery.analyzer import DiscoveredPattern, EmailRecord, PatternAnalyzer
from src.skills_discovery.declarative import candidate_match

REVIEW_SCHEMA_VERSION = 1
VALID_PRIORITIES = frozenset({"P0", "P1", "P2", "P3"})
VALID_ACTIONS = frozenset({"forward"})
SUPPORTED_CONDITION_TYPES = frozenset(
    {"sender_match", "to_match", "cc_match", "subject_match", "body_match"}
)
SUPPORTED_CONDITION_OPERATORS = frozenset({"eq", "contains", "regex", "in"})
_SKILL_ID_RE = re.compile(r"^skill_[a-z0-9][a-z0-9_]{0,79}$")
EDITABLE_CANDIDATE_FIELDS = frozenset({
    "skill_id",
    "name",
    "description",
    "trigger_type",
    "conditions",
    "condition_logic",
    "suggested_priority",
    "suggested_need_reply",
    "suggested_tone",
    "suggested_action",
    "suggested_forward_to",
})


class CandidateReviewError(ValueError):
    """A persisted review artifact or selection is structurally invalid."""


class CandidateSelectionError(CandidateReviewError):
    """A requested conversational selection cannot be applied safely."""


def suggested_skill_id(name: str) -> str:
    """Create a stable, readable default target ID for a candidate.

    Earlier prototypes used Python's process-randomized ``hash()``, which
    made non-ASCII target IDs change across runs.  A short SHA-256 suffix gives
    repeatable collision resistance while leaving ASCII names recognizable.
    """
    normalized_name = str(name or "skill")
    ascii_parts = re.findall(r"[A-Za-z0-9]+", normalized_name)
    readable = "_".join(part.lower() for part in ascii_parts)[:48]
    digest = hashlib.sha256(normalized_name.encode("utf-8")).hexdigest()[:8]
    if readable:
        return f"skill_auto_{readable}_{digest}"
    return f"skill_auto_{digest}"


def is_valid_skill_id(skill_id: object) -> bool:
    return isinstance(skill_id, str) and bool(_SKILL_ID_RE.fullmatch(skill_id))


def _record_sort_key(record: EmailRecord) -> tuple[int, float | str, str]:
    value = (record.received_at or "").strip()
    if value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return (0, parsed.timestamp(), record.id)
        except ValueError:
            return (1, value, record.id)
    return (2, "", record.id)


def split_records_chronologically(
    records: Sequence[EmailRecord],
    *,
    training_fraction: float = 0.8,
) -> tuple[list[EmailRecord], list[EmailRecord]]:
    """Return earliest training records and newest held-out replay records."""
    if not 0 < training_fraction < 1:
        raise ValueError("training_fraction_must_be_between_zero_and_one")
    ordered = sorted(records, key=_record_sort_key)
    if len(ordered) < 2:
        return ordered, []
    split_at = int(len(ordered) * training_fraction)
    split_at = min(len(ordered) - 1, max(1, split_at))
    return ordered[:split_at], ordered[split_at:]


@dataclass
class ValidationExample:
    id: str
    received_at: str
    sender: str
    subject: str
    replied: bool


@dataclass
class TimeSplitValidation:
    held_out_records: int
    held_out_received: int
    matched_count: int
    observed_reply_rate: float | None
    examples: list[ValidationExample] = field(default_factory=list)


@dataclass
class CandidateReviewItem:
    """All production-effective fields plus their evidence for one candidate."""

    candidate_id: str
    skill_id: str
    name: str
    description: str
    trigger_type: str
    conditions: list[dict[str, Any]]
    condition_logic: str
    suggested_priority: str
    suggested_need_reply: bool
    suggested_tone: str
    suggested_action: str | None
    suggested_forward_to: list[str]
    discovery_reply_rate: float
    discovery_sample_count: int
    confidence: float
    example_subjects: list[str]
    example_senders: list[str]
    validation: TimeSplitValidation
    promotion_issues: list[str] = field(default_factory=list)

    @classmethod
    def from_pattern(
        cls,
        pattern: DiscoveredPattern,
        *,
        validation: TimeSplitValidation,
    ) -> "CandidateReviewItem":
        return cls(
            candidate_id=pattern.id,
            skill_id=suggested_skill_id(pattern.name),
            name=pattern.name,
            description=pattern.description,
            trigger_type=pattern.trigger_type,
            conditions=[dict(condition) for condition in pattern.conditions],
            condition_logic=pattern.condition_logic,
            suggested_priority=pattern.suggested_priority,
            suggested_need_reply=pattern.suggested_need_reply,
            suggested_tone=pattern.suggested_tone,
            suggested_action=pattern.suggested_action,
            suggested_forward_to=list(pattern.suggested_forward_to),
            discovery_reply_rate=pattern.reply_rate,
            discovery_sample_count=pattern.sample_count,
            confidence=pattern.confidence,
            example_subjects=list(pattern.example_subjects),
            example_senders=list(pattern.example_senders),
            validation=validation,
        )

    def to_pattern(self) -> DiscoveredPattern:
        return DiscoveredPattern(
            id=self.candidate_id,
            name=self.name,
            description=self.description,
            trigger_type=self.trigger_type,
            conditions=[dict(condition) for condition in self.conditions],
            reply_rate=self.discovery_reply_rate,
            sample_count=self.discovery_sample_count,
            suggested_priority=self.suggested_priority,
            suggested_need_reply=self.suggested_need_reply,
            suggested_tone=self.suggested_tone,
            example_subjects=list(self.example_subjects),
            example_senders=list(self.example_senders),
            confidence=self.confidence,
            condition_logic=self.condition_logic,
            suggested_action=self.suggested_action,
            suggested_forward_to=list(self.suggested_forward_to),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CandidateReviewItem":
        validation_raw = value.get("validation")
        if not isinstance(validation_raw, Mapping):
            raise CandidateReviewError("candidate_missing_validation")
        examples_raw = validation_raw.get("examples", [])
        if not isinstance(examples_raw, list):
            raise CandidateReviewError("candidate_invalid_validation_examples")
        try:
            validation = TimeSplitValidation(
                held_out_records=int(validation_raw["held_out_records"]),
                held_out_received=int(validation_raw["held_out_received"]),
                matched_count=int(validation_raw["matched_count"]),
                observed_reply_rate=(
                    float(validation_raw["observed_reply_rate"])
                    if validation_raw.get("observed_reply_rate") is not None
                    else None
                ),
                examples=[ValidationExample(**example) for example in examples_raw],
            )
            return cls(
                candidate_id=str(value["candidate_id"]),
                skill_id=str(value["skill_id"]),
                name=str(value["name"]),
                description=str(value["description"]),
                trigger_type=str(value["trigger_type"]),
                conditions=[dict(item) for item in value.get("conditions", [])],
                condition_logic=str(value.get("condition_logic", "and")),
                suggested_priority=str(value.get("suggested_priority", "P2")),
                suggested_need_reply=bool(value.get("suggested_need_reply", True)),
                suggested_tone=str(value.get("suggested_tone", "")),
                suggested_action=(
                    str(value["suggested_action"])
                    if value.get("suggested_action") is not None
                    else None
                ),
                suggested_forward_to=[str(item) for item in value.get("suggested_forward_to", [])],
                discovery_reply_rate=float(value.get("discovery_reply_rate", 0)),
                discovery_sample_count=int(value.get("discovery_sample_count", 0)),
                confidence=float(value.get("confidence", 0)),
                example_subjects=[str(item) for item in value.get("example_subjects", [])],
                example_senders=[str(item) for item in value.get("example_senders", [])],
                validation=validation,
                promotion_issues=[str(item) for item in value.get("promotion_issues", [])],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CandidateReviewError("candidate_invalid") from exc


@dataclass
class CandidateReview:
    schema_version: int
    created_at: str
    source: str
    my_email: str
    training_records: int
    held_out_records: int
    candidates: list[CandidateReviewItem]
    validation_records: list[EmailRecord] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "created_at": self.created_at,
            "source": self.source,
            "my_email": self.my_email,
            "training_records": self.training_records,
            "held_out_records": self.held_out_records,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            # Store the bounded replay snapshot so edited conversational fields
            # are revalidated before promotion without rereading a mutable mailbox.
            "validation_records": [asdict(record) for record in self.validation_records],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CandidateReview":
        if value.get("schema_version") != REVIEW_SCHEMA_VERSION:
            raise CandidateReviewError("unsupported_review_schema_version")
        candidates_raw = value.get("candidates", [])
        records_raw = value.get("validation_records", [])
        if not isinstance(candidates_raw, list) or not isinstance(records_raw, list):
            raise CandidateReviewError("review_invalid_collections")
        try:
            return cls(
                schema_version=REVIEW_SCHEMA_VERSION,
                created_at=str(value["created_at"]),
                source=str(value["source"]),
                my_email=str(value.get("my_email", "")),
                training_records=int(value["training_records"]),
                held_out_records=int(value["held_out_records"]),
                candidates=[CandidateReviewItem.from_dict(item) for item in candidates_raw],
                validation_records=[EmailRecord(**record) for record in records_raw],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CandidateReviewError("review_invalid") from exc


def _record_email(record: EmailRecord) -> dict[str, Any]:
    return {
        "sender": record.sender,
        "subject": record.subject,
        "body": record.body_preview,
        "to": record.to,
        "cc": record.cc,
    }


def replay_candidate(
    candidate: CandidateReviewItem,
    validation_records: Sequence[EmailRecord],
    *,
    my_email: str = "",
    max_examples: int = 3,
) -> TimeSplitValidation:
    """Replay a candidate over the newest held-out records using live matcher code."""
    analyzer = PatternAnalyzer(list(validation_records), my_email=my_email)
    received = [record for record in validation_records if record.message_type != "sent"]
    try:
        anchor_raw, conditions_raw = candidate_match(candidate)
        anchor = AnchorGroup.model_validate(anchor_raw)
        conditions = (
            ConditionNode.model_validate(conditions_raw)
            if conditions_raw is not None
            else None
        )
    except Exception:
        matches = []
    else:
        matches = []
        for record in received:
            view = EmailView(
                sender_address=record.sender,
                to_addresses=list(record.to),
                cc_addresses=list(record.cc),
                subject=record.subject,
                body_current_text=record.body_preview,
                body_full_text=record.body_preview,
            )
            if evaluate_match(
                anchor,
                conditions,
                view,
                me_email=my_email or None,
            ) is RuleEvalStatus.MATCHED:
                matches.append(record)
    replied_count = sum(1 for record in matches if analyzer.was_replied(record))
    return TimeSplitValidation(
        held_out_records=len(validation_records),
        held_out_received=len(received),
        matched_count=len(matches),
        observed_reply_rate=(replied_count / len(matches) if matches else None),
        examples=[
            ValidationExample(
                id=record.id,
                received_at=record.received_at,
                sender=record.sender,
                subject=record.subject,
                replied=analyzer.was_replied(record),
            )
            for record in matches[:max_examples]
        ],
    )


def _validate_condition(condition: object, index: int) -> list[str]:
    prefix = f"条件 {index + 1}"
    if not isinstance(condition, Mapping):
        return [f"{prefix} 必须是对象"]
    condition_type = condition.get("type")
    operator = condition.get("operator", "contains")
    value = condition.get("value")
    issues: list[str] = []
    if condition_type not in SUPPORTED_CONDITION_TYPES:
        issues.append(f"{prefix} 的 type 不受运行时支持: {condition_type!r}")
    if operator not in SUPPORTED_CONDITION_OPERATORS:
        issues.append(f"{prefix} 的 operator 不受运行时支持: {operator!r}")
    if operator == "in":
        if not isinstance(value, list) or not value or not all(
            isinstance(item, str) and item.strip() for item in value
        ):
            issues.append(f"{prefix} 的 in 值必须是非空字符串数组")
    elif not isinstance(value, str) or not value.strip():
        issues.append(f"{prefix} 的值必须是非空字符串")
    if operator == "regex" and isinstance(value, str):
        try:
            re.compile(value)
        except re.error as exc:
            issues.append(f"{prefix} 的正则无效: {exc.msg}")
    return issues


def validate_candidate_for_promotion(candidate: CandidateReviewItem) -> list[str]:
    """Return blocking configuration issues; replay statistics never block promotion."""
    issues: list[str] = []
    if not is_valid_skill_id(candidate.skill_id):
        issues.append("目标 Skill ID 必须匹配 skill_[a-z0-9_]")
    if not isinstance(candidate.name, str) or not candidate.name.strip():
        issues.append("名称不能为空")
    if not isinstance(candidate.description, str) or not candidate.description.strip():
        issues.append("描述不能为空")
    if not isinstance(candidate.suggested_priority, str) or candidate.suggested_priority not in VALID_PRIORITIES:
        issues.append("优先级必须为 P0、P1、P2 或 P3")
    if not isinstance(candidate.condition_logic, str) or candidate.condition_logic not in {"and", "or"}:
        issues.append("条件组合方式必须为 and 或 or")
    if not isinstance(candidate.conditions, list) or not candidate.conditions:
        issues.append("至少需要一个触发条件")
    else:
        for index, condition in enumerate(candidate.conditions):
            issues.extend(_validate_condition(condition, index))
    if not isinstance(candidate.suggested_need_reply, bool):
        issues.append("是否需要回复必须为布尔值")
    if not isinstance(candidate.suggested_tone, str):
        issues.append("语气指令必须为文本")
    elif len(candidate.suggested_tone) > 500:
        issues.append("语气指令不能超过 500 个字符")

    action = candidate.suggested_action
    if action is not None and not isinstance(action, str):
        issues.append("声明式动作必须是文本")
        action = None
    if action == "transfer":
        issues.append("动作请使用 forward，不要使用 transfer")
    elif action is not None and action not in VALID_ACTIONS:
        issues.append(f"不支持的声明式动作: {action!r}")
    forward_to = candidate.suggested_forward_to
    if not isinstance(forward_to, list):
        issues.append("固定收件人必须是数组")
        forward_to = []
    if action == "forward":
        if not candidate.suggested_need_reply:
            issues.append("forward 动作必须标记为需要回复，以生成可编辑审批计划")
        if not forward_to:
            issues.append("forward 动作必须提供至少一个固定收件人")
    elif forward_to:
        issues.append("只有 forward 动作可以设置固定收件人")

    if len(forward_to) > 10:
        issues.append("固定收件人不能超过 10 个")
    for recipient in forward_to:
        if (
            not isinstance(recipient, str)
            or not recipient.strip()
            or len(recipient.encode("utf-8")) > 320
        ):
            issues.append("固定收件人必须是长度不超过 320 字节的非空文本")
            break
        if "@" not in recipient or any(character in recipient for character in "*?"):
            issues.append("固定收件人必须是精确邮箱地址")
            break
    try:
        candidate_match(candidate)
    except ValueError as exc:
        code = str(exc)
        messages = {
            "candidate_anchor_required": "至少需要一个精确地址锚点",
            "candidate_mixed_or_unsupported": "地址锚点与正文条件不能使用 or 混合",
        }
        issues.append(messages.get(code, f"候选无法转换为 Tier1 v1: {code}"))
    return issues


def create_candidate_review(
    patterns: Sequence[DiscoveredPattern],
    *,
    training_records: Sequence[EmailRecord],
    validation_records: Sequence[EmailRecord],
    source: str,
    my_email: str = "",
) -> CandidateReview:
    """Create a durable, read-only review artifact for discovered patterns."""
    candidates = []
    for pattern in patterns:
        provisional = CandidateReviewItem.from_pattern(
            pattern,
            validation=TimeSplitValidation(0, 0, 0, None),
        )
        candidate = replace(
            provisional,
            validation=replay_candidate(
                provisional,
                validation_records,
                my_email=my_email,
            ),
        )
        candidates.append(replace(
            candidate,
            promotion_issues=validate_candidate_for_promotion(candidate),
        ))
    return CandidateReview(
        schema_version=REVIEW_SCHEMA_VERSION,
        created_at=datetime.now(timezone.utc).isoformat(),
        source=source,
        my_email=my_email,
        training_records=len(training_records),
        held_out_records=len(validation_records),
        candidates=candidates,
        validation_records=list(validation_records),
    )


def write_review(review: CandidateReview, path: str | Path) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(review.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def load_review(path: str | Path) -> CandidateReview:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CandidateReviewError("无法读取候选审阅文件") from exc
    if not isinstance(raw, Mapping):
        raise CandidateReviewError("候选审阅文件必须是对象")
    return CandidateReview.from_dict(raw)


def _selection_list(raw_selections: object) -> list[Mapping[str, Any]]:
    if isinstance(raw_selections, Mapping):
        raw_selections = raw_selections.get("selections")
    if not isinstance(raw_selections, list):
        raise CandidateSelectionError("选择文件必须包含 selections 数组")
    selections: list[Mapping[str, Any]] = []
    for raw_selection in raw_selections:
        if not isinstance(raw_selection, Mapping):
            raise CandidateSelectionError("每个选择必须是对象")
        selections.append(raw_selection)
    return selections


def apply_conversational_selections(
    review: CandidateReview,
    raw_selections: object,
) -> list[CandidateReviewItem]:
    """Apply explicit candidate selections and editable field overrides.

    This is the bridge used by a conversation agent after the user has reviewed
    candidates.  No default selection exists, and all modified triggers are
    replayed against the persisted held-out snapshot before any caller can write
    them to the registry.
    """
    by_id = {candidate.candidate_id: candidate for candidate in review.candidates}
    selected_ids: set[str] = set()
    selected: list[CandidateReviewItem] = []
    for selection in _selection_list(raw_selections):
        candidate_id = selection.get("candidate_id")
        if not isinstance(candidate_id, str) or candidate_id not in by_id:
            raise CandidateSelectionError(f"未知候选: {candidate_id!r}")
        if candidate_id in selected_ids:
            raise CandidateSelectionError(f"候选被重复选择: {candidate_id}")
        selected_ids.add(candidate_id)
        overrides = selection.get("overrides", {})
        if not isinstance(overrides, Mapping):
            raise CandidateSelectionError(f"候选 {candidate_id} 的 overrides 必须是对象")
        unknown_fields = set(overrides) - EDITABLE_CANDIDATE_FIELDS
        if unknown_fields:
            raise CandidateSelectionError(
                f"候选 {candidate_id} 包含不可编辑字段: {', '.join(sorted(unknown_fields))}"
            )
        candidate = replace(by_id[candidate_id], **dict(overrides))
        if candidate.suggested_action == "transfer":
            candidate = replace(candidate, suggested_action="forward")
        candidate = replace(
            candidate,
            validation=replay_candidate(
                candidate,
                review.validation_records,
                my_email=review.my_email,
            ),
        )
        candidate = replace(
            candidate,
            promotion_issues=validate_candidate_for_promotion(candidate),
        )
        selected.append(candidate)
    if not selected:
        raise CandidateSelectionError("至少需要选择一个候选")
    return selected


def render_review(review: CandidateReview) -> str:
    """Render every effective field for a compact, conversation-friendly review."""
    lines = [
        "历史邮件 Skill 候选审阅",
        f"来源：{review.source}",
        (
            "时间切分：最早 80% 用于发现 "
            f"（{review.training_records} 封），最新 20% 用于回放（{review.held_out_records} 封）。"
        ),
        "回放指标只供人工判断，不设自动通过阈值。",
    ]
    for candidate in review.candidates:
        validation = candidate.validation
        lines.extend([
            "",
            f"[{candidate.candidate_id}] {candidate.name}",
            f"目标 Skill ID：{candidate.skill_id}",
            f"说明：{candidate.description}",
            f"触发（{candidate.condition_logic}）：{json.dumps(candidate.conditions, ensure_ascii=False)}",
            (
                "建议结果："
                f"优先级={candidate.suggested_priority}，"
                f"需要回复={candidate.suggested_need_reply}，"
                f"语气={candidate.suggested_tone or '无'}，"
                f"动作={candidate.suggested_action or '无'}，"
                f"固定收件人={candidate.suggested_forward_to or '无'}"
            ),
            (
                "发现期："
                f"样本 {candidate.discovery_sample_count}，"
                f"回复率 {candidate.discovery_reply_rate:.0%}，"
                f"置信度 {candidate.confidence:.2f}"
            ),
            (
                "最新 20% 回放："
                f"{validation.matched_count}/{validation.held_out_received} 封收件命中，"
                + (
                    f"命中邮件的观察回复率 {validation.observed_reply_rate:.0%}"
                    if validation.observed_reply_rate is not None
                    else "无命中，暂无观察回复率"
                )
            ),
        ])
        if validation.examples:
            lines.append("回放示例：")
            lines.extend(
                f"- {'已回复' if example.replied else '未回复'} | "
                f"{example.received_at} | {example.sender} | {example.subject}"
                for example in validation.examples
            )
        if candidate.promotion_issues:
            lines.append("不可提升，需编辑：" + "；".join(candidate.promotion_issues))
        else:
            lines.append("可提升：是（仍需在对话中明确选择）。")
    return "\n".join(lines)
