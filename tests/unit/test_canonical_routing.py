import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
from src.router.engine import RoutingEngine
from src.router.tier1.compiler import CompiledArtifact, compile_registry
from src.router.tier1.schema import CanonicalRoute


def _artifact() -> CompiledArtifact:
    result = compile_registry(
        "tier1_rules",
        internal_email_domains=("tianjin-air.com", "hnair.com", "hnaaviation.com"),
        me_email="q-fu@tianjin-air.com",
    )
    assert isinstance(result, CompiledArtifact)
    return result


@pytest.mark.asyncio
async def test_tier1_returns_one_canonical_decision_without_skill_handlers():
    engine = RoutingEngine(artifact=_artifact(), me_email="q-fu@tianjin-air.com")
    state = {
        "email": {
            "id": "mail-1",
            "sender": "hhsc@hnair.com",
            "to": ["q-fu@tianjin-air.com"],
            "cc": [],
            "subject": "marketing",
            "body": "notice",
        }
    }

    result = await engine.execute_router(state)

    decision = RouteDecision.model_validate(result["route_decision"])
    assert decision.route is CanonicalRoute.NO_ACTION
    assert decision.provenance.tier is RouteTier.TIER1
    assert decision.provenance.artifact_digest == _artifact().digest
    assert decision.params == {"reason_code": "marketing_spam"}
    assert result["classification"]["need_reply"] is False
    assert result["routing_stage"] == "tier1"


@pytest.mark.asyncio
async def test_tier2_votes_on_canonical_actions_not_legacy_skill_ids():
    engine = RoutingEngine(artifact=_artifact(), me_email="q-fu@tianjin-air.com")
    historical = RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route=CanonicalRoute.REPLY,
        params={"reply_mode": "sender_only"},
        provenance=RouteProvenance(
            tier=RouteTier.TIER3,
            source_version="router-model-v1",
            confidence=0.9,
        ),
        reason_code="historical_label",
    ).model_dump(mode="json")
    hits = [
        {"id": "h1", "route_decision": historical},
        {"id": "h2", "route_decision": historical},
    ]

    delta = await engine.apply_tier2_hits({}, hits)

    decision = RouteDecision.model_validate(delta["route_decision"])
    assert decision.route is CanonicalRoute.REPLY
    assert decision.provenance.tier is RouteTier.TIER2
    assert decision.provenance.evidence_ids == ["h1", "h2"]
    assert "active_skills" not in delta


@pytest.mark.asyncio
async def test_tier3_requires_strict_route_json_and_fails_closed():
    engine = RoutingEngine(artifact=_artifact(), me_email="q-fu@tianjin-air.com")
    model = AsyncMock()
    model.ainvoke.return_value = Mock(
        content=json.dumps(
            {
                "route": "read_only",
                "params": {},
                "confidence": 0.82,
                "reason_code": "informational_notice",
            }
        )
    )
    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        delta = await engine.apply_tier3_fallback(
            {"email": {"subject": "FYI", "body": "notice", "sender": "a@example.com"}}
        )
    decision = RouteDecision.model_validate(delta["route_decision"])
    assert decision.route is CanonicalRoute.READ_ONLY
    assert decision.provenance.tier is RouteTier.TIER3

    model.ainvoke.return_value = Mock(content='{"route":"unknown"}')
    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        failed = await engine.apply_tier3_fallback(
            {"email": {"subject": "x", "body": "y", "sender": "a@example.com"}}
        )
    failed_decision = RouteDecision.model_validate(failed["route_decision"])
    assert failed_decision.route is CanonicalRoute.MANUAL_REVIEW
    assert failed["next_step"] == "manual_review"


@pytest.mark.asyncio
async def test_tier3_accepts_json_code_fence_from_model():
    engine = RoutingEngine(artifact=_artifact(), me_email="q-fu@tianjin-air.com")
    model = AsyncMock()
    model.ainvoke.return_value = Mock(
        content=(
            "```json\n"
            '{"route":"read_only","params":{},"confidence":0.9,'
            '"reason_code":"informational_notice"}\n'
            "```"
        )
    )

    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        delta = await engine.apply_tier3_fallback(
            {"email": {"subject": "FYI", "body": "notice", "sender": "a@example.com"}}
        )

    decision = RouteDecision.model_validate(delta["route_decision"])
    assert decision.route is CanonicalRoute.READ_ONLY
    assert decision.provenance.tier is RouteTier.TIER3
