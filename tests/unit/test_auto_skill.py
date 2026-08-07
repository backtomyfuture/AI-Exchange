"""Tests for the data-driven AutoOutcomeSkill (replaces 21 duplicate handlers)."""

import os
import textwrap

import pytest

from src.router.auto_skill import AutoOutcomeSkill
from src.router.base import AutoOutcome, SkillManifest, SkillTrigger
from src.router.manager import SkillManager


def _make_manifest(**kwargs):
    base = dict(
        id="skill_auto_test",
        name="测试自动技能",
        description="单元测试自动发现的发件人规则",
        triggers=SkillTrigger(priority=50, conditions=[
            {"type": "sender_match", "operator": "contains", "value": "x@y.com"}
        ]),
        auto_outcome=AutoOutcome(
            priority="P3", need_reply=False, card_type="none",
            priority_level=2, reply_rate=0.0,
        ),
    )
    base.update(kwargs)
    return SkillManifest(**base)


@pytest.mark.asyncio
async def test_auto_outcome_applies_classification_fields():
    skill = AutoOutcomeSkill(manifest=_make_manifest())
    state = {"classification": {"reasoning": "prior"}}
    update = await skill.execute(state)
    cls = update["classification"]
    assert cls["priority"] == "P3"
    assert cls["need_reply"] is False
    assert cls["card_type"] == "none"
    assert "Auto-Skill: 测试自动技能" in cls["reasoning"]
    assert "回复率 0%" in cls["reasoning"]
    assert "prior" in cls["reasoning"]
    assert update["priority_level"] == 2


@pytest.mark.asyncio
async def test_auto_outcome_with_high_reply_rate_routes_to_approval():
    skill = AutoOutcomeSkill(manifest=_make_manifest(
        auto_outcome=AutoOutcome(
            priority="P1", need_reply=True, card_type="approval",
            priority_level=8, reply_rate=75.0,
        ),
    ))
    update = await skill.execute({"classification": {}})
    cls = update["classification"]
    assert cls["priority"] == "P1"
    assert cls["need_reply"] is True
    assert cls["card_type"] == "approval"
    assert "回复率 75%" in cls["reasoning"]
    assert update["priority_level"] == 8


@pytest.mark.asyncio
async def test_auto_outcome_no_reply_rate_omits_segment():
    skill = AutoOutcomeSkill(manifest=_make_manifest(
        auto_outcome=AutoOutcome(
            priority="P3", need_reply=False, card_type="none",
            priority_level=2, reply_rate=None,
        ),
    ))
    update = await skill.execute({"classification": {}})
    assert "回复率" not in update["classification"]["reasoning"]


@pytest.mark.asyncio
async def test_auto_outcome_supports_action_for_forward():
    skill = AutoOutcomeSkill(manifest=_make_manifest(
        auto_outcome=AutoOutcome(
            priority="P2", need_reply=True, card_type="approval",
            priority_level=5, action="forward", intent="转发",
        ),
    ))
    update = await skill.execute({"classification": {}})
    cls = update["classification"]
    assert cls["action"] == "forward"
    assert cls["intent"] == "转发"


@pytest.mark.asyncio
async def test_auto_outcome_forward_sets_editable_recipient_plan_without_sending():
    skill = AutoOutcomeSkill(manifest=_make_manifest(
        auto_outcome=AutoOutcome(
            priority="P2", need_reply=True, card_type="approval",
            priority_level=5, action="forward", forward_to=["open_id=leader"],
            tone_instruction="简洁呈阅",
        ),
    ))

    update = await skill.execute({
        "classification": {},
        "email": {"subject": "待呈阅", "draft_to": ["old@example.com"]},
    })

    assert update["classification"]["action"] == "forward"
    assert update["email"]["draft_to"] == ["open_id=leader"]
    assert update["email"]["draft_cc"] == []
    assert "简洁呈阅" in update["system_prompt_modifier"]


def test_skill_manager_loads_auto_outcome_without_handler(tmp_path):
    """Manager should auto-install AutoOutcomeSkill for handler-less manifests."""
    skill_dir = tmp_path / "skill_auto_unit"
    skill_dir.mkdir()
    (skill_dir / "manifest.yaml").write_text(textwrap.dedent("""
        id: skill_auto_unit
        name: unit auto skill
        description: tmp
        version: 1.0.0
        execution_mode: modifier
        triggers:
          priority: 50
          conditions:
          - type: sender_match
            operator: contains
            value: foo@bar.com
        auto_outcome:
          priority: P3
          need_reply: false
          card_type: none
          priority_level: 2
          reply_rate: 0
    """).strip())

    mgr = SkillManager(registry_path=str(tmp_path))
    skill = mgr.get_skill("skill_auto_unit")
    assert skill is not None
    assert type(skill).__name__ == "AutoOutcomeSkill"


def test_skill_manager_skips_skill_with_neither_handler_nor_outcome(tmp_path):
    skill_dir = tmp_path / "skill_orphan"
    skill_dir.mkdir()
    (skill_dir / "manifest.yaml").write_text(textwrap.dedent("""
        id: skill_orphan
        name: orphan
        description: orphan
        version: 1.0.0
        execution_mode: modifier
    """).strip())

    mgr = SkillManager(registry_path=str(tmp_path))
    assert mgr.get_skill("skill_orphan") is None


def test_real_skill_auto_lanjuan_retired_from_production_registry():
    """skill_auto_lanjuan was retired during the Tier 1 v1 migration: it
    duplicated skill_vip_handling and, due to last-write-wins skill ordering,
    silently clobbered vip_handling's direct-recipient-aware decision (see
    docs/tier1-migration-inventory.md RETIRE group). It now lives under
    skills_registry_retired/, not skills_registry/, so the live SkillManager
    must not load it."""
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    mgr = SkillManager(registry_path=os.path.join(root, "skills_registry"))
    assert mgr.get_skill("skill_auto_lanjuan") is None
    # Most other auto skills should still load normally, e.g. P3/no-reply.
    zx = mgr.get_skill("skill_auto_zhang_xia")
    assert zx.manifest.auto_outcome.priority == "P3"
    assert zx.manifest.auto_outcome.need_reply is False
