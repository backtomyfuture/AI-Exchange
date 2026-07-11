"""Tier 2 (semantic-layer routing) unit tests."""

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.router.engine import RoutingEngine, TIER2_MIN_HITS
from src.nodes.retriever_node import retrieve_context


@pytest.fixture
def engine_with_skill_pool():
    engine = RoutingEngine()
    skill_known = MagicMock()
    skill_known.manifest.description = "known"
    pool = {
        "skill_vip_handling": skill_known,
        "skill_project_tracker": skill_known,
        "skill_leadership_tone": skill_known,
    }
    with patch.object(engine.skill_manager, "get_all_skills", return_value=pool):
        yield engine, pool


def _hit(email_id, skills):
    return {"id": email_id, "active_skills": skills}


def test_tier2_activates_when_skill_majority_hits(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    hits = [
        _hit("e1", ["skill_vip_handling"]),
        _hit("e2", ["skill_vip_handling", "skill_project_tracker"]),
        _hit("e3", ["skill_vip_handling"]),
    ]

    chosen = engine._tier2_route(hits, existing_skills=[], skills=pool)
    assert chosen == ["skill_vip_handling"]  # 3/3 emails


def test_tier2_skips_below_min_hits(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    hits = [
        _hit("e1", ["skill_vip_handling"]),  # only 1 vote, < TIER2_MIN_HITS (=2)
    ]
    chosen = engine._tier2_route(hits, existing_skills=[], skills=pool)
    assert chosen == []


def test_tier2_skips_below_ratio(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    # 5 emails total, only 2 mention the skill -> ratio 0.4 < 0.5 default.
    hits = [
        _hit("e1", ["skill_project_tracker"]),
        _hit("e2", ["skill_project_tracker"]),
        _hit("e3", ["other_unknown"]),
        _hit("e4", ["other_unknown"]),
        _hit("e5", ["other_unknown"]),
    ]
    chosen = engine._tier2_route(hits, existing_skills=[], skills=pool)
    assert chosen == []


def test_tier2_dedups_same_email_chunks(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    # Same email_id appears across chunks - should count as one vote.
    hits = [
        _hit("e1", ["skill_vip_handling"]),
        _hit("e1", ["skill_vip_handling"]),
        _hit("e1", ["skill_vip_handling"]),
    ]
    chosen = engine._tier2_route(hits, existing_skills=[], skills=pool)
    # Only 1 unique email -> count=1 < TIER2_MIN_HITS -> skip.
    assert chosen == []
    assert TIER2_MIN_HITS == 2


def test_tier2_filters_unknown_skill_ids(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    hits = [
        _hit("e1", ["skill_was_deleted"]),
        _hit("e2", ["skill_was_deleted"]),
        _hit("e3", ["skill_was_deleted"]),
    ]
    chosen = engine._tier2_route(hits, existing_skills=[], skills=pool)
    assert chosen == []


def test_tier2_excludes_already_active(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    hits = [
        _hit("e1", ["skill_vip_handling"]),
        _hit("e2", ["skill_vip_handling"]),
    ]
    chosen = engine._tier2_route(
        hits,
        existing_skills=["skill_vip_handling"],
        skills=pool,
    )
    assert chosen == []


@pytest.mark.asyncio
async def test_apply_tier2_hits_returns_delta(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    hits = [
        _hit("e1", ["skill_leadership_tone"]),
        _hit("e2", ["skill_leadership_tone"]),
    ]
    state = {
        "email": {"id": "x", "subject": "s", "body": "b"},
        "active_skills": [],
        "routing_log": [],
    }

    fake_skill = MagicMock()
    fake_skill.execute = AsyncMock(return_value={
        "system_prompt_modifier": "be polite",
    })
    with patch.object(engine.skill_manager, "get_skill", return_value=fake_skill), \
         patch("src.router.dependency.resolve_skill_order",
               return_value=["skill_leadership_tone"]):
        delta = await engine.apply_tier2_hits(state, hits)

    assert delta["active_skills"] == ["skill_leadership_tone"]
    assert delta["routing_log"] == ["Tier 2 Match: ['skill_leadership_tone']"]
    assert delta["system_prompt_modifier"] == "be polite"


@pytest.mark.asyncio
async def test_apply_tier2_hits_no_match_returns_empty(engine_with_skill_pool):
    engine, pool = engine_with_skill_pool
    state = {"active_skills": [], "routing_log": []}
    delta = await engine.apply_tier2_hits(state, [])
    assert delta == {}


@pytest.mark.asyncio
async def test_retriever_node_integrates_tier2(monkeypatch, graph_node_harness):
    """retrieve_context must surface Tier 2 deltas alongside context."""
    from src.nodes import retriever_node

    fake_retriever = MagicMock()
    fake_retriever.search_by_thread.return_value = []
    fake_retriever.search.return_value = [
        _hit("h1", ["skill_vip_handling"]),
        _hit("h2", ["skill_vip_handling"]),
    ]
    monkeypatch.setattr(retriever_node, "get_retriever", lambda: fake_retriever)

    fake_engine = MagicMock()
    fake_engine.apply_tier2_hits = AsyncMock(return_value={
        "active_skills": ["skill_vip_handling"],
        "routing_log": ["Tier 2 Match: ['skill_vip_handling']"],
        "tool_calls": [{"id": "new-call", "name": "new-tool"}],
    })
    monkeypatch.setattr(retriever_node, "get_routing_engine", lambda: fake_engine)

    state = graph_node_harness.state(
        {
            "id": "tier-two",
            "subject": "s",
            "body": "b",
            "sender": "u@x.com",
        },
        classification={},
        context=[],
        active_skills=["skill_existing"],
        routing_log=["Tier 1 No match"],
        tool_calls=[{"id": "old-call", "name": "old-tool"}],
    )

    updates = await retrieve_context(state, graph_node_harness.dependencies)

    assert updates["active_skills"] == [
        "skill_existing",
        "skill_vip_handling",
    ]
    assert updates["routing_log"][0] == "Tier 1 No match"
    assert any("Tier 2 Match" in entry for entry in updates["routing_log"])
    assert [call["id"] for call in updates["tool_calls"]] == [
        "old-call",
        "new-call",
    ]
    assert updates["context_summaries"]
    fake_engine.apply_tier2_hits.assert_awaited_once()


@pytest.mark.asyncio
async def test_real_mutating_forward_skill_projects_recipients_and_draft_store(
    monkeypatch,
    graph_node_harness,
):
    from skills_registry.skill_forward_boss.handler import Skill as ForwardBossSkill
    from src.nodes import retriever_node

    engine = RoutingEngine()
    manifest = MagicMock()
    manifest.name = "Forward to Boss Verification"
    manifest.depends_on = None
    forward_skill = ForwardBossSkill(manifest)
    monkeypatch.setattr(
        engine.skill_manager,
        "get_all_skills",
        lambda: {"skill_forward_boss": forward_skill},
    )
    monkeypatch.setattr(
        engine.skill_manager,
        "get_skill",
        lambda skill_id: forward_skill if skill_id == "skill_forward_boss" else None,
    )
    monkeypatch.setattr(retriever_node, "get_routing_engine", lambda: engine)

    fake_retriever = MagicMock()
    fake_retriever.search.return_value = [
        _hit("old-1", ["skill_forward_boss"]),
        _hit("old-2", ["skill_forward_boss"]),
    ]
    monkeypatch.setattr(retriever_node, "get_retriever", lambda: fake_retriever)
    monkeypatch.setattr(
        retriever_node,
        "_retrieve_experience",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        retriever_node,
        "_retrieve_style_guidance",
        AsyncMock(return_value=""),
    )
    monkeypatch.setattr(
        retriever_node,
        "_retrieve_user_preferences",
        AsyncMock(return_value=[]),
    )

    state = graph_node_harness.state(
        {
            "id": "forward-tier-two",
            "subject": "important",
            "body": "body",
            "sender": "sender@example.com",
            "draft_to": ["sender@example.com"],
            "draft_cc": ["copy@example.com"],
        },
        classification={
            "priority": "P2",
            "need_reply": True,
            "intent": "咨询",
        },
        active_skills=["skill_existing"],
        routing_log=["Tier 1 No match"],
    )
    before = deepcopy(state)

    updates = await retrieve_context(state, graph_node_harness.dependencies)

    assert updates["classification"]["action"] == "forward"
    assert updates["draft_to"] == ["boss@company.com"]
    assert updates["draft_cc"] == []
    assert updates["draft_id"] == "forward-tier-two"
    assert graph_node_harness.draft_saves == [("forward-tier-two", "呈阅")]
    assert updates["active_skills"] == ["skill_existing", "skill_forward_boss"]
    assert "draft" not in updates
    assert "email" not in updates
    assert state == before
