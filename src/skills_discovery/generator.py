"""
Skill Generator — 根据发现的模式生成 Skill 目录、manifest.yaml 和 handler.py。
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

from src.skills_discovery.analyzer import DiscoveredPattern

logger = logging.getLogger(__name__)

_HANDLER_TEMPLATE_MODIFIER = '''\
from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        {description}
        """
        classification = state.get("classification", {{}})
        classification.update({{
            "priority": "{priority}",
            "need_reply": {need_reply},
            "reasoning": (
                classification.get("reasoning", "")
                + " [Auto-Skill: {name}] {reason_suffix}"
            ).strip(),
            "card_type": "{card_type}",
        }})

        updates: Dict[str, Any] = {{
            "classification": classification,
            "priority_level": {priority_level},
        }}
{tone_block}
        return updates
'''

_TONE_BLOCK = '''
        modifier = (
            "【语气指令】{tone_instruction}"
        )
        updates["system_prompt_modifier"] = (
            (state.get("system_prompt_modifier") or "") + "\\n" + modifier
        )
'''


def _sanitize_skill_id(name: str) -> str:
    """Convert a pattern name to a valid skill directory name."""
    ascii_parts = re.findall(r'[a-zA-Z0-9]+', name)
    if ascii_parts:
        return "skill_auto_" + "_".join(ascii_parts).lower()
    pinyin_hash = abs(hash(name)) % 10000
    return f"skill_auto_{pinyin_hash}"


def _priority_to_level(priority: str) -> int:
    return {"P0": 10, "P1": 8, "P2": 5, "P3": 2}.get(priority, 5)


def _card_type_from(priority: str, need_reply: bool) -> str:
    if need_reply:
        return "approval"
    if priority in ("P0", "P1"):
        return "read_only"
    return "none"


def generate_manifest(pattern: DiscoveredPattern, skill_id: str) -> dict:
    """Generate a manifest.yaml dict for a pattern."""
    conditions = pattern.conditions
    if not conditions:
        if pattern.trigger_type == "sender_match" and pattern.example_senders:
            conditions = [{
                "type": "sender_match",
                "operator": "in",
                "value": pattern.example_senders,
            }]
        else:
            conditions = [{
                "type": "subject_match",
                "operator": "regex",
                "value": "|".join(
                    re.escape(s[:20]) for s in pattern.example_subjects[:3]
                ) if pattern.example_subjects else "placeholder",
            }]

    trigger_priority = 80
    if pattern.confidence >= 0.8:
        trigger_priority = 90
    elif pattern.confidence < 0.5:
        trigger_priority = 60

    triggers: dict = {
        "priority": trigger_priority,
        "conditions": conditions,
    }

    # 仅在非默认值（非 and）时输出 condition_logic，保持 manifest 简洁
    condition_logic = getattr(pattern, "condition_logic", "and")
    if condition_logic and condition_logic != "and":
        triggers["condition_logic"] = condition_logic

    return {
        "id": skill_id,
        "name": pattern.name,
        "description": pattern.description,
        "version": "1.0.0",
        "execution_mode": "modifier",
        "triggers": triggers,
    }


def generate_handler(pattern: DiscoveredPattern) -> str:
    """Generate handler.py source code for a pattern."""
    tone_block = ""
    if pattern.suggested_tone:
        tone_block = _TONE_BLOCK.format(tone_instruction=pattern.suggested_tone)

    return _HANDLER_TEMPLATE_MODIFIER.format(
        description=pattern.description,
        priority=pattern.suggested_priority,
        need_reply=pattern.suggested_need_reply,
        name=pattern.name,
        reason_suffix=f"匹配自动发现规则，回复率 {pattern.reply_rate:.0%}。",
        card_type=_card_type_from(pattern.suggested_priority, pattern.suggested_need_reply),
        priority_level=_priority_to_level(pattern.suggested_priority),
        tone_block=tone_block,
    )


def write_skill(
    pattern: DiscoveredPattern,
    registry_path: str = "skills_registry",
) -> str:
    """Write a complete skill directory with manifest.yaml and handler.py.

    Returns the skill directory path.
    """
    skill_id = _sanitize_skill_id(pattern.name)
    skill_dir = Path(registry_path) / skill_id
    skill_dir.mkdir(parents=True, exist_ok=True)

    manifest = generate_manifest(pattern, skill_id)
    manifest_path = skill_dir / "manifest.yaml"
    with open(manifest_path, "w", encoding="utf-8") as f:
        yaml.dump(manifest, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    handler_code = generate_handler(pattern)
    handler_path = skill_dir / "handler.py"
    with open(handler_path, "w", encoding="utf-8") as f:
        f.write(handler_code)

    logger.info("Generated skill: %s → %s", skill_id, skill_dir)
    return str(skill_dir)
