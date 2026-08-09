"""Write reviewed discovery candidates as strict Tier 1 v1 YAML rules."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

import yaml

from src.router.tier1.schema import RuleStatus
from src.skills_discovery.analyzer import DiscoveredPattern
from src.skills_discovery.declarative import manifest_for_candidate
from src.skills_discovery.review import (
    CandidateReviewItem,
    TimeSplitValidation,
    is_valid_skill_id,
    suggested_skill_id,
    validate_candidate_for_promotion,
)


logger = logging.getLogger(__name__)


class SkillPromotionConflict(FileExistsError):
    def __init__(self, skill_ids: Sequence[str]):
        self.skill_ids = tuple(skill_ids)
        super().__init__("目标规则 ID 已存在: " + ", ".join(self.skill_ids))


class SkillPromotionValidationError(ValueError):
    pass


def _sanitize_skill_id(name: str) -> str:
    return suggested_skill_id(name)


def _as_candidate(pattern: DiscoveredPattern, skill_id: str) -> CandidateReviewItem:
    return CandidateReviewItem(
        candidate_id=pattern.id,
        skill_id=skill_id,
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
        validation=TimeSplitValidation(0, 0, 0, None),
    )


def generate_manifest(pattern: DiscoveredPattern, skill_id: str) -> dict:
    candidate = _as_candidate(pattern, skill_id)
    return manifest_for_candidate(candidate, status=RuleStatus.PROPOSED).model_dump(
        mode="json", by_alias=True, exclude_none=True
    )


def _validate(candidate: CandidateReviewItem) -> None:
    issues = validate_candidate_for_promotion(candidate)
    if issues:
        raise SkillPromotionValidationError("；".join(issues))
    try:
        manifest_for_candidate(candidate, status=RuleStatus.ENABLED)
    except Exception as exc:
        raise SkillPromotionValidationError(str(exc)) from None


def _write_manifest(candidate: CandidateReviewItem, directory: Path, *, status: RuleStatus) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{candidate.skill_id}.yaml"
    if target.exists() or target.is_symlink():
        raise SkillPromotionConflict([candidate.skill_id])
    manifest = manifest_for_candidate(candidate, status=status)
    target.write_text(
        yaml.safe_dump(
            manifest.model_dump(mode="json", by_alias=True, exclude_none=True),
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return str(target)


def write_skill(
    pattern: DiscoveredPattern,
    registry_path: str = "tier1_rules",
    *,
    skill_id: str | None = None,
) -> str:
    candidate = _as_candidate(pattern, skill_id or _sanitize_skill_id(pattern.name))
    _validate(candidate)
    return _write_manifest(candidate, Path(registry_path), status=RuleStatus.ENABLED)


def write_proposed_candidates(
    candidates: Sequence[CandidateReviewItem],
    candidate_path: str | Path,
) -> list[str]:
    output = Path(candidate_path)
    paths: list[str] = []
    for candidate in candidates:
        if not is_valid_skill_id(candidate.skill_id):
            raise SkillPromotionValidationError("目标规则 ID 无效")
        paths.append(_write_manifest(candidate, output, status=RuleStatus.PROPOSED))
    return paths


def promote_selected_candidates(
    candidates: Sequence[CandidateReviewItem],
    registry_path: str = "tier1_rules",
) -> list[str]:
    if not candidates:
        raise SkillPromotionValidationError("至少需要选择一个候选")
    for candidate in candidates:
        _validate(candidate)
    ids = [candidate.skill_id for candidate in candidates]
    if len(ids) != len(set(ids)):
        raise SkillPromotionValidationError("所选候选的目标规则 ID 不能重复")
    directory = Path(registry_path)
    conflicts = [item for item in ids if (directory / f"{item}.yaml").exists()]
    if conflicts:
        raise SkillPromotionConflict(conflicts)
    return [
        _write_manifest(candidate, directory, status=RuleStatus.ENABLED)
        for candidate in candidates
    ]


__all__ = [
    "SkillPromotionConflict",
    "SkillPromotionValidationError",
    "generate_manifest",
    "promote_selected_candidates",
    "write_proposed_candidates",
    "write_skill",
]
