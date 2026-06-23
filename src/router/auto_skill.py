"""
Generic data-driven skill: applies the manifest's ``auto_outcome`` block.

Drops 20+ duplicated ``handler.py`` files in ``skills_registry/skill_auto_*``
in favour of declarative YAML. Manifests that declare ``auto_outcome`` get this
class auto-installed by :class:`SkillManager` when no custom ``handler.py`` is
present.
"""

from typing import Any, Dict

from src.graph.state import AgentState
from src.router.base import BaseSkill


class AutoOutcomeSkill(BaseSkill):
    """Apply ``manifest.auto_outcome`` onto classification state."""

    async def execute(self, state: AgentState) -> Dict[str, Any]:
        outcome = self.manifest.auto_outcome
        if outcome is None:
            return {}

        classification = dict(state.get("classification") or {})

        # Always overwrite priority/need_reply/card_type when the rule fires:
        # the manifest is the authoritative outcome of an auto-discovered pattern.
        classification["priority"] = outcome.priority
        classification["need_reply"] = outcome.need_reply
        classification["card_type"] = outcome.card_type
        if outcome.intent:
            classification["intent"] = outcome.intent
        if outcome.action:
            classification["action"] = outcome.action

        # Reasoning is appended to preserve any upstream LLM/skill reasoning.
        rate_part = ""
        if outcome.reply_rate is not None:
            rate_part = f"，回复率 {outcome.reply_rate:.0f}%"
        classification["reasoning"] = (
            (classification.get("reasoning", "") or "")
            + f" [Auto-Skill: {self.manifest.name}] 匹配自动发现规则{rate_part}。"
        ).strip()

        return {
            "classification": classification,
            "priority_level": outcome.priority_level,
        }
