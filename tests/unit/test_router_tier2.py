"""Tier 2 (semantic-layer routing) unit tests."""

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
async def test_retriever_node_integrates_tier2(monkeypatch):
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
    })
    monkeypatch.setattr(retriever_node, "get_routing_engine", lambda: fake_engine)

    state = {
        "email": {"subject": "s", "body": "b", "sender": "u@x.com"},
        "classification": {},
        "context": [],
        "active_skills": [],
        "routing_log": [],
    }

    updates = await retrieve_context(state)

    assert updates["active_skills"] == ["skill_vip_handling"]
    assert any("Tier 2 Match" in entry for entry in updates["routing_log"])
    assert updates["context"]
    fake_engine.apply_tier2_hits.assert_awaited_once()
