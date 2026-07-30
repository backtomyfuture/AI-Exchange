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


def _forward_skill():
    skill = MagicMock()
    skill.manifest.name = "Test forward"
    skill.manifest.depends_on = None

    async def execute(state):
        classification = dict(state.get("classification") or {})
        classification.update(
            {
                "priority": "P0",
                "need_reply": True,
                "intent": "转发",
                "action": "forward",
                "reasoning": "test forward skill",
            }
        )
        email = dict(state.get("email") or {})
        email["draft_to"] = ["forward-target@example.com"]
        email["draft_cc"] = []
        return {"classification": classification, "email": email, "draft": "test forward"}

    skill.execute = AsyncMock(side_effect=execute)
    return skill


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
    fake_skill.execute = AsyncMock(
        return_value={
            "system_prompt_modifier": "be polite",
        }
    )
    with (
        patch.object(engine.skill_manager, "get_skill", return_value=fake_skill),
        patch(
            "src.router.dependency.resolve_skill_order",
            return_value=["skill_leadership_tone"],
        ),
    ):
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
async def test_tier1_miss_defers_tier3_until_retrieval(engine_with_skill_pool):
    engine, _pool = engine_with_skill_pool
    state = {
        "email": {"id": "mail-1", "subject": "s", "body": "b"},
        "active_skills": [],
        "routing_log": [],
    }

    with (
        patch.object(engine.t1_router, "route", return_value=[]),
        patch.object(engine, "_tier3_llm_route", new_callable=AsyncMock) as tier3,
    ):
        result = await engine.execute_router(state)

    tier3.assert_not_awaited()
    assert result["routing_stage"] == "pending"
    assert result["routing_log"] == ["Tier 1 No match, awaiting Tier 2"]


@pytest.mark.asyncio
async def test_retriever_uses_tier3_only_after_tier2_has_no_decision(
    monkeypatch,
    graph_node_harness,
):
    from src.nodes import retriever_node

    call_order: list[str] = []
    fake_retriever = MagicMock()
    fake_retriever.search_by_thread.return_value = []
    fake_retriever.search.return_value = []
    monkeypatch.setattr(retriever_node, "get_retriever", lambda: fake_retriever)

    async def tier2(*_args):
        call_order.append("tier2")
        return {}

    async def tier3(*_args):
        call_order.append("tier3")
        return {
            "active_skills": ["skill_project_tracker"],
            "routing_log": ["Tier 3 LLM Match: ['skill_project_tracker']"],
            "routing_stage": "tier3",
        }

    fake_engine = MagicMock()
    fake_engine.apply_tier2_hits = AsyncMock(side_effect=tier2)
    fake_engine.apply_tier3_fallback = AsyncMock(side_effect=tier3)
    monkeypatch.setattr(retriever_node, "get_routing_engine", lambda: fake_engine)
    monkeypatch.setattr(retriever_node, "_retrieve_experience", AsyncMock(return_value=[]))
    monkeypatch.setattr(retriever_node, "_retrieve_style_guidance", AsyncMock(return_value=""))
    monkeypatch.setattr(retriever_node, "_retrieve_user_preferences", AsyncMock(return_value=[]))

    state = graph_node_harness.state(
        {"id": "tier-order", "subject": "s", "body": "b", "sender": "u@x.com"},
        classification={"need_reply": True},
        routing_stage="pending",
    )
    updates = await retrieve_context(state, graph_node_harness.dependencies)

    assert call_order == ["tier2", "tier3"]
    assert updates["routing_stage"] == "tier3"
    assert updates["active_skills"] == ["skill_project_tracker"]


@pytest.mark.asyncio
async def test_retriever_does_not_reopen_tier2_or_tier3_after_tier1(
    monkeypatch,
    graph_node_harness,
):
    from src.nodes import retriever_node

    fake_retriever = MagicMock()
    fake_retriever.search_by_thread.return_value = []
    fake_retriever.search.return_value = []
    monkeypatch.setattr(retriever_node, "get_retriever", lambda: fake_retriever)
    fake_engine = MagicMock()
    fake_engine.apply_tier2_hits = AsyncMock()
    fake_engine.apply_tier3_fallback = AsyncMock()
    monkeypatch.setattr(retriever_node, "get_routing_engine", lambda: fake_engine)
    monkeypatch.setattr(retriever_node, "_retrieve_experience", AsyncMock(return_value=[]))
    monkeypatch.setattr(retriever_node, "_retrieve_style_guidance", AsyncMock(return_value=""))
    monkeypatch.setattr(retriever_node, "_retrieve_user_preferences", AsyncMock(return_value=[]))

    state = graph_node_harness.state(
        {"id": "tier-one", "subject": "s", "body": "b", "sender": "u@x.com"},
        classification={"need_reply": True},
        active_skills=["skill_vip_handling"],
        routing_stage="tier1",
    )
    updates = await retrieve_context(state, graph_node_harness.dependencies)

    fake_engine.apply_tier2_hits.assert_not_awaited()
    fake_engine.apply_tier3_fallback.assert_not_awaited()
    assert updates["routing_stage"] == "tier1"


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
    fake_engine.apply_tier2_hits = AsyncMock(
        return_value={
            "active_skills": ["skill_vip_handling"],
            "routing_log": ["Tier 2 Match: ['skill_vip_handling']"],
            "tool_calls": [{"id": "new-call", "name": "new-tool"}],
        }
    )
    monkeypatch.setattr(retriever_node, "get_routing_engine", lambda: fake_engine)
    monkeypatch.setattr(
        retriever_node,
        "_generate_thread_summary",
        AsyncMock(return_value="Thread summary"),
    )

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
    from src.nodes import retriever_node

    engine = RoutingEngine()
    forward_skill = _forward_skill()
    monkeypatch.setattr(
        engine.skill_manager,
        "get_all_skills",
        lambda: {"test_forward": forward_skill},
    )
    monkeypatch.setattr(
        engine.skill_manager,
        "get_skill",
        lambda skill_id: forward_skill if skill_id == "test_forward" else None,
    )
    monkeypatch.setattr(retriever_node, "get_routing_engine", lambda: engine)

    fake_retriever = MagicMock()
    fake_retriever.search.return_value = [
        _hit("old-1", ["test_forward"]),
        _hit("old-2", ["test_forward"]),
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
    monkeypatch.setattr(
        retriever_node,
        "_generate_thread_summary",
        AsyncMock(return_value="Thread summary"),
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
    assert updates["draft_to"] == ["forward-target@example.com"]
    assert updates["draft_cc"] == []
    assert updates["draft_id"] == "forward-tier-two"
    assert graph_node_harness.draft_saves == [("forward-tier-two", "test forward")]
    assert updates["active_skills"] == ["skill_existing", "test_forward"]
    assert "draft" not in updates
    assert "email" not in updates
    assert state == before
