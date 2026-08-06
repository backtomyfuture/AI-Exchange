"""Write manually selected discovered skills as declarative manifests only."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

import yaml

from src.skills_discovery.analyzer import DiscoveredPattern
from src.skills_discovery.review import (
    CandidateReviewItem,
    TimeSplitValidation,
    is_valid_skill_id,
    suggested_skill_id,
    validate_candidate_for_promotion,
)

logger = logging.getLogger(__name__)


class SkillPromotionConflict(FileExistsError):
    """Promotion targets an existing registry directory and must not overwrite it."""

    def __init__(self, skill_ids: Sequence[str]):
        self.skill_ids = tuple(skill_ids)
        super().__init__("目标 Skill ID 已存在: " + ", ".join(self.skill_ids))


class SkillPromotionValidationError(ValueError):
    """A manually selected candidate is not a safe declarative runtime rule."""


def _sanitize_skill_id(name: str) -> str:
    """Compatibility name for the stable candidate target-ID generator."""
    return suggested_skill_id(name)


def _priority_to_level(priority: str) -> int:
    return {"P0": 10, "P1": 8, "P2": 5, "P3": 2}.get(priority, 5)


def _card_type_from(priority: str, need_reply: bool) -> str:
    if need_reply:
        return "approval"
    if priority in ("P0", "P1"):
        return "read_only"
    return "none"


def generate_manifest(pattern: DiscoveredPattern, skill_id: str) -> dict:
    """Generate a bounded data-only manifest for a reviewed pattern.

    There is intentionally no ``handler.py`` output.  ``SkillManager`` loads
    the generic ``AutoOutcomeSkill`` for this manifest at the next service
    start, so a discovered rule can only prepare classification or an editable
    approval plan.
    """
    triggers: dict = {
        "priority": (
            90 if pattern.confidence >= 0.8 else 60 if pattern.confidence < 0.5 else 80
        ),
        "conditions": [dict(condition) for condition in pattern.conditions],
    }
    if pattern.condition_logic != "and":
        triggers["condition_logic"] = pattern.condition_logic

    auto_outcome: dict = {
        "priority": pattern.suggested_priority,
        "need_reply": pattern.suggested_need_reply,
        "card_type": _card_type_from(
            pattern.suggested_priority,
            pattern.suggested_need_reply,
        ),
        "priority_level": _priority_to_level(pattern.suggested_priority),
        # Runtime logs display rates in percent while discovery stores a ratio.
        "reply_rate": round(pattern.reply_rate * 100, 1),
    }
    if pattern.suggested_tone:
        auto_outcome["tone_instruction"] = pattern.suggested_tone
    if pattern.suggested_action:
        auto_outcome["action"] = pattern.suggested_action
    if pattern.suggested_forward_to:
        auto_outcome["forward_to"] = list(pattern.suggested_forward_to)

    return {
        "id": skill_id,
        "name": pattern.name,
        "description": pattern.description,
        "version": "1.0.0",
        "execution_mode": "modifier",
        "triggers": triggers,
        "auto_outcome": auto_outcome,
    }


def _as_candidate(pattern: DiscoveredPattern, skill_id: str) -> CandidateReviewItem:
    """Use the same schema validator for direct API callers and review promotion."""
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


def _raise_if_invalid(candidate: CandidateReviewItem) -> None:
    issues = validate_candidate_for_promotion(candidate)
    if issues:
        raise SkillPromotionValidationError("；".join(issues))


def write_skill(
    pattern: DiscoveredPattern,
    registry_path: str = "skills_registry",
    *,
    skill_id: str | None = None,
) -> str:
    """Create one declarative skill directory without overwriting any target.

    This is a low-level promotion primitive.  Discovery must not invoke it;
    callers first obtain an explicit selection through ``CandidateReview``.
    """
    target_id = skill_id or _sanitize_skill_id(pattern.name)
    if not is_valid_skill_id(target_id):
        raise SkillPromotionValidationError("目标 Skill ID 必须匹配 skill_[a-z0-9_]")
    _raise_if_invalid(_as_candidate(pattern, target_id))

    registry = Path(registry_path)
    registry.mkdir(parents=True, exist_ok=True)
    skill_dir = registry / target_id
    if skill_dir.exists() or skill_dir.is_symlink():
        raise SkillPromotionConflict([target_id])
    try:
        # mkdir is the collision-safe reservation.  Unlike a replace operation,
        # it cannot silently replace an existing rule in a concurrent run.
        skill_dir.mkdir()
    except FileExistsError as exc:
        raise SkillPromotionConflict([target_id]) from exc

    manifest = generate_manifest(pattern, target_id)
    manifest_path = skill_dir / "manifest.yaml"
    try:
        manifest_path.write_text(
            yaml.safe_dump(
                manifest,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
    except Exception:
        # Keep the empty reservation rather than deleting an unknown directory:
        # an operator can inspect it, and no existing rule is ever overwritten.
        logger.exception("Unable to write declarative skill manifest: %s", target_id)
        raise

    logger.info("Promoted declarative skill: %s -> %s", target_id, skill_dir)
    return str(skill_dir)


def promote_selected_candidates(
    candidates: Sequence[CandidateReviewItem],
    registry_path: str = "skills_registry",
) -> list[str]:
    """Validate a reviewed selection, reject all conflicts, then create rules.

    Existing target conflicts are checked for the whole selection before the
    first write so a conflict never causes a partial silent overwrite or merge.
    """
    if not candidates:
        raise SkillPromotionValidationError("至少需要选择一个候选")

    issues: list[str] = []
    target_ids: list[str] = []
    for candidate in candidates:
        candidate_issues = validate_candidate_for_promotion(candidate)
        if candidate_issues:
            issues.extend(
                f"{candidate.candidate_id}: {issue}" for issue in candidate_issues
            )
        target_ids.append(candidate.skill_id)
    if issues:
        raise SkillPromotionValidationError("；".join(issues))
    if len(set(target_ids)) != len(target_ids):
        issues.append("所选候选的目标 Skill ID 不能重复")
    if issues:
        raise SkillPromotionValidationError("；".join(issues))

    registry = Path(registry_path)
    conflicts = [
        target_id
        for target_id in target_ids
        if (registry / target_id).exists() or (registry / target_id).is_symlink()
    ]
    if conflicts:
        raise SkillPromotionConflict(conflicts)

    return [
        write_skill(
            candidate.to_pattern(),
            registry_path=registry_path,
            skill_id=candidate.skill_id,
        )
        for candidate in candidates
    ]
