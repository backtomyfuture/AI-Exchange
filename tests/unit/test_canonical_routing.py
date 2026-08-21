import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.router.context import RecipientRelation, RoutingEvidenceBundle
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


def test_recipient_relation_normalizes_exchange_mailbox_mappings():
    relation = RecipientRelation.from_email(
        {
            "sender": {"email": "sender@example.com"},
            "to": [{"email": "colleague@example.com"}],
            "cc": [{"email_address": "me@example.com"}],
        },
        owner_email="me@example.com",
    )

    assert relation.relation == "cc_only"
    assert relation.owner_in_to is False
    assert relation.owner_in_cc is True


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
            tier=RouteTier.TIER1,
            source_version="tier1-artifact-v1",
            artifact_digest="a" * 64,
            confidence=1.0,
        ),
        reason_code="historical_label",
    ).model_dump(mode="json")
    hits = [
        {"id": "h1", "sender": "a@x.com", "thread_id": "t1", "score": 0.91, "route_decision": historical},
        {"id": "h2", "sender": "b@x.com", "thread_id": "t2", "score": 0.88, "route_decision": historical},
        {"id": "h3", "sender": "c@x.com", "thread_id": "t3", "score": 0.86, "route_decision": historical},
    ]

    delta = await engine.apply_tier2_hits({}, hits)

    decision = RouteDecision.model_validate(delta["route_decision"])
    assert decision.route is CanonicalRoute.REPLY
    assert decision.provenance.tier is RouteTier.TIER2
    assert decision.provenance.evidence_ids == ["h1", "h2", "h3"]
    assert "active_skills" not in delta


@pytest.mark.asyncio
async def test_tier2_consensus_remains_authoritative_over_tier3():
    engine = RoutingEngine(artifact=_artifact(), me_email="me@example.com")
    model = AsyncMock()
    historical = RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route=CanonicalRoute.READ_ONLY,
        params={},
        provenance=RouteProvenance(
            tier=RouteTier.TIER1,
            source_version="tier1-artifact-v1",
            artifact_digest="a" * 64,
            confidence=1.0,
        ),
        reason_code="historical_label",
    ).model_dump(mode="json")

    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        decision = await engine.resolve_route(
            {"email": {"subject": "FYI", "body": "notice", "sender": "a@example.com"}},
            RoutingEvidenceBundle.from_hits(
                [
                    {"id": "h1", "sender": "a@example.com", "thread_id": "t1", "score": 0.9, "route_decision": historical},
                    {"id": "h2", "sender": "b@example.com", "thread_id": "t2", "score": 0.9, "route_decision": historical},
                    {"id": "h3", "sender": "c@example.com", "thread_id": "t3", "score": 0.9, "route_decision": historical},
                ]
            ),
        )

    assert decision.route is CanonicalRoute.READ_ONLY
    model.ainvoke.assert_not_awaited()


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
                "priority": "P3",
                "intent": "通知",
                "summary": "FYI notice",
                "reasoning": "informational",
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
    assert delta["classification"]["priority"] == "P3"
    assert delta["classification"]["tier3_metadata_complete"] is True

    model.ainvoke.return_value = Mock(content='{"route":"unknown"}')
    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        failed = await engine.apply_tier3_fallback(
            {"email": {"subject": "x", "body": "y", "sender": "a@example.com"}}
        )
    failed_decision = RouteDecision.model_validate(failed["route_decision"])
    assert failed_decision.route is CanonicalRoute.MANUAL_REVIEW
    assert failed["next_step"] == "manual_review"


@pytest.mark.asyncio
async def test_tier3_keeps_valid_route_when_intent_metadata_is_long():
    engine = RoutingEngine(artifact=_artifact(), me_email="me@example.com")
    model = AsyncMock()
    model.ainvoke.return_value = Mock(
        content=json.dumps(
            {
                "route": "read_only",
                "params": {},
                "confidence": 0.88,
                "reason_code": "informational_notice",
                "explicit_current_action": False,
                "priority": "P3",
                "intent": "informational_update_" + ("x" * 40),
                "summary": "Informational update",
                "reasoning": "No current action is addressed to the mailbox owner.",
            }
        )
    )

    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        delta = await engine.apply_tier3_fallback(
            {
                "email": {
                    "sender": "sender@example.com",
                    "to": ["me@example.com"],
                    "cc": [],
                    "subject": "Update",
                    "body": "For your information.",
                }
            }
        )

    decision = RouteDecision.model_validate(delta["route_decision"])
    assert decision.route is CanonicalRoute.READ_ONLY
    assert delta["classification"]["intent"].startswith("informational_update_")
    assert delta["classification"]["tier3_metadata_complete"] is True


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


@pytest.mark.asyncio
async def test_tier3_receives_recipient_semantics_and_historical_routing_context():
    engine = RoutingEngine(artifact=_artifact(), me_email="me@example.com")
    model = AsyncMock()
    model.ainvoke.return_value = Mock(
        content=json.dumps(
            {
                "route": "read_only",
                "params": {},
                "confidence": 0.91,
                "reason_code": "historical_context_supports_read_only",
                "explicit_current_action": False,
            }
        )
    )
    historical = RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route=CanonicalRoute.READ_ONLY,
        params={},
        provenance=RouteProvenance(
            tier=RouteTier.TIER3,
            source_version="router-model-v1",
            confidence=0.9,
        ),
        reason_code="historical_label",
    ).model_dump(mode="json")
    evidence = RoutingEvidenceBundle.from_hits(
        [
            {
                "id": "thread-1",
                "sender": "me@example.com",
                "to": ["colleague@example.com"],
                "cc": [],
                "subject": "Earlier task",
                "body": "I asked the colleague to handle the task.",
                "route_decision": historical,
            }
        ]
    )

    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        delta = await engine.resolve_route(
            {
                "email": {
                    "sender": "sender@example.com",
                    "to": ["colleague@example.com"],
                    "cc": ["me@example.com"],
                    "subject": "Current update",
                    "body": "The task was handled.",
                }
            },
            evidence,
        )

    assert delta.route is CanonicalRoute.READ_ONLY
    prompt = model.ainvoke.call_args.args[0]
    assert "owner_relation: cc_only" in prompt
    assert "Tier 1 status: abstained" in prompt
    assert "Tier 2 status: no_consensus" in prompt
    assert "Tier 2 candidate routes: read_only (1 vote)" in prompt
    assert "Earlier task" in prompt
    assert "I asked the colleague to handle the task." in prompt


@pytest.mark.asyncio
async def test_tier3_downgrades_cc_only_writing_route_without_current_action():
    engine = RoutingEngine(artifact=_artifact(), me_email="me@example.com")
    model = AsyncMock()
    model.ainvoke.return_value = Mock(
        content=json.dumps(
            {
                "route": "forward",
                "params": {"fixed_recipients": ["admin@example.com"]},
                "confidence": 0.95,
                "reason_code": "request_requires_action_by_third_party",
                "explicit_current_action": False,
            }
        )
    )

    with patch("src.providers.factory.get_llm_for_role", return_value=model):
        delta = await engine.resolve_route(
            {
                "email": {
                    "sender": "sender@example.com",
                    "to": ["colleague@example.com"],
                    "cc": ["me@example.com"],
                    "subject": "Current update",
                    "body": "The task was handled.",
                }
            },
            RoutingEvidenceBundle.from_hits([]),
        )

    assert delta.route is CanonicalRoute.READ_ONLY
    assert delta.reason_code == "mailbox_owner_in_cc_no_explicit_action_request"
