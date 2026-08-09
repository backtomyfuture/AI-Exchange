from __future__ import annotations

from types import SimpleNamespace

import pytest
from langchain_core.runnables import RunnableLambda
from pydantic import ValidationError

from src.handoff.evidence import (
    EvidenceAdapterRegistry,
    EvidenceItem,
    EvidencePack,
    WritingEvidenceRetriever,
)
from src.handoff.history import HistoricalRouteConsensus
from src.handoff.models import HandoffPlan
from src.handoff.profiles import PROFILE_REGISTRY, get_handoff_profile
from src.nodes.drafter import generate_draft
from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
from src.router.engine import RoutingEngine
from src.router.tier1.compiler import CompilationFailure, compile_registry
from src.router.tier1.fingerprint import compute_action_fingerprint
from src.router.tier1.schema import CanonicalRoute, Decision


def _historical(profile: str | None = None) -> dict:
    params = {"reply_mode": "sender_only"}
    decision = RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route=CanonicalRoute.REPLY,
        params=params,
        provenance=RouteProvenance(tier=RouteTier.TIER3, source_version="router-model-v1"),
        handoff_profile_id=profile,
    )
    return decision.model_dump(mode="json")


def test_immutable_versioned_dtos_have_stable_canonical_digests():
    item = EvidenceItem(source="mail_thread", source_id="m1", content="Earlier answer")
    pack = EvidencePack(profile_id="generic_reply_v1", items=(item,))
    plan = HandoffPlan(profile_id="generic_reply_v1", required_sources=("mail_thread",))
    assert item.canonical_digest() == EvidenceItem.model_validate(item.model_dump()).canonical_digest()
    assert pack.canonical_digest() == EvidencePack.model_validate(pack.model_dump()).canonical_digest()
    assert plan.canonical_digest() == HandoffPlan.model_validate(plan.model_dump()).canonical_digest()
    with pytest.raises(ValidationError):
        item.content = "changed"


def test_registry_is_static_and_rejects_unknown_or_ad_hoc_adapter():
    assert {"generic_reply_v1", "generic_forward_v1", "vip_direct_reply_v1"} <= set(PROFILE_REGISTRY)
    with pytest.raises(KeyError):
        get_handoff_profile("https://evil.invalid/profile.py")
    adapters = EvidenceAdapterRegistry()
    with pytest.raises(ValueError, match="not registered"):
        adapters.resolve("arbitrary_python")


def test_registered_adapters_bind_existing_readonly_mail_history_api():
    class ExistingRetriever:
        def search_by_thread(self, **kwargs):
            assert kwargs["thread_id"] == "t1"
            return []

        def search(self, **kwargs):
            assert kwargs["query_text"] == "question"
            return []

    adapters = EvidenceAdapterRegistry.from_email_retriever(ExistingRetriever())
    assert adapters.resolve("mail_thread")({"thread_id": "t1"}, 5) == []
    assert adapters.resolve("semantic_history")({"body": "question"}, 5) == []


@pytest.mark.asyncio
async def test_history_ignores_invalid_and_does_not_mix_v1_v2():
    consensus = HistoricalRouteConsensus(min_hits=2, min_ratio=0.5)
    hits = [
        {"id": "bad", "route_decision": {"route": "reply"}},
        {"id": "none"},
        {"id": "v1a", "route_decision": _historical()},
        {"id": "v1b", "route_decision": _historical()},
        {"id": "v2a", "route_decision": _historical("generic_reply_v1")},
        {"id": "v2b", "route_decision": _historical("generic_reply_v1")},
    ]
    conflict = consensus.decide(hits[2:])
    assert conflict is not None
    assert conflict.outcome is DecisionOutcome.CONFLICT
    assert conflict.route is CanonicalRoute.MANUAL_REVIEW
    assert conflict.reason_code == "tier2_conflict"
    decision = consensus.decide(hits[2:4])
    assert decision is not None
    assert decision.handoff_profile_id is None
    assert decision.selected_action_fingerprint == compute_action_fingerprint(
        Decision(route="reply", params={"reply_mode": "sender_only"}),
        fingerprint_version=1,
    )


@pytest.mark.asyncio
async def test_writing_retriever_emits_evidence_only_and_vip_history_is_optional():
    adapters = EvidenceAdapterRegistry(
        exchange_contact=lambda request, limit: [],
        mail_thread=lambda request, limit: [{"id": "m1", "body": "approved wording"}],
        semantic_history=lambda request, limit: [],
    )
    retriever = WritingEvidenceRetriever(adapters)
    generic = await retriever.retrieve(
        HandoffPlan(profile_id="generic_reply_v1", optional_sources=("mail_thread",)),
        {"thread_id": "t1"},
    )
    dumped = generic.model_dump(mode="json")
    assert dumped["items"][0]["content"] == "approved wording"
    assert "route" not in str(dumped).lower()
    assert "recipient" not in str(dumped).lower()

    vip = get_handoff_profile("vip_direct_reply_v1").build_plan()
    vip_pack = await retriever.retrieve(vip, {"thread_id": "t1"})
    assert [item.source for item in vip_pack.items] == ["mail_thread"]


def test_vip_profile_has_real_readonly_history_plan():
    profile = get_handoff_profile("vip_direct_reply_v1")
    plan = profile.build_plan()
    assert plan.required_sources == ()
    assert plan.optional_sources == (
        "exchange_contact",
        "mail_thread",
        "semantic_history",
    )
    assert profile.readonly is True


def test_history_deduplicates_identical_evidence_votes():
    consensus = HistoricalRouteConsensus(min_hits=2, min_ratio=0.5)
    duplicate = {"id": "same", "route_decision": _historical()}

    assert consensus.decide([duplicate, duplicate]) is None


def test_history_fails_closed_when_one_evidence_identity_has_conflicting_labels():
    consensus = HistoricalRouteConsensus(min_hits=2, min_ratio=0.5)
    conflict = consensus.decide(
        [
            {"id": "same", "route_decision": _historical()},
            {
                "id": "same",
                "route_decision": _historical("generic_reply_v1"),
            },
        ]
    )

    assert conflict is not None
    assert conflict.outcome is DecisionOutcome.CONFLICT
    assert conflict.reason_code == "tier2_conflict"
    assert conflict.provenance.evidence_ids == ["same"]
    assert len(conflict.candidate_actions) == 2


def test_history_denominator_includes_unusable_retrieval_hits():
    consensus = HistoricalRouteConsensus(min_hits=2, min_ratio=0.5)
    hits = [
        {"id": "invalid-1"},
        {"id": "invalid-2", "route_decision": {"route": "reply"}},
        {"id": "valid-1", "route_decision": _historical()},
        {"id": "valid-2", "route_decision": _historical()},
        {"id": "invalid-3"},
    ]

    assert consensus.decide(hits) is None


def test_history_never_assigns_positional_vote_identity_to_unidentified_hits():
    consensus = HistoricalRouteConsensus(min_hits=2, min_ratio=0.5)
    label = _historical()

    assert consensus.decide(
        [
            {"route_decision": label},
            {"route_decision": label},
            {"id": 1, "route_decision": label},
            {"id": "", "route_decision": label},
        ]
    ) is None


def test_history_does_not_coerce_evidence_identity_types():
    consensus = HistoricalRouteConsensus(min_hits=2, min_ratio=0.5)
    label = _historical()

    assert consensus.decide(
        [
            {"id": 1, "route_decision": label},
            {"id": "1", "route_decision": label},
        ]
    ) is None


@pytest.mark.asyncio
async def test_source_authorization_failure_is_never_treated_as_optional_evidence():
    async def reject_authorization(_source):
        raise RuntimeError("stale_fence")

    retriever = WritingEvidenceRetriever(
        EvidenceAdapterRegistry(mail_thread=lambda _request, _limit: []),
        before_source=reject_authorization,
    )

    with pytest.raises(RuntimeError, match="stale_fence"):
        await retriever.retrieve(
            HandoffPlan(
                profile_id="generic_reply_v1",
                optional_sources=("mail_thread",),
            ),
            {},
        )


@pytest.mark.asyncio
async def test_vip_profile_invokes_typed_exchange_contact_adapter():
    class ExistingRetriever:
        def search_by_thread(self, **_kwargs):
            return []

        def search(self, **_kwargs):
            return []

    class ExchangeDirectory:
        async def resolve_contact(self, query):
            assert query == "vip@example.com"
            return "VIP Leader"

    adapters = EvidenceAdapterRegistry.from_runtime(
        retriever=ExistingRetriever(),
        exchange_client=ExchangeDirectory(),
    )
    pack = await WritingEvidenceRetriever(adapters).retrieve(
        get_handoff_profile("vip_direct_reply_v1").build_plan(),
        {"sender": "VIP Leader <vip@example.com>"},
    )

    assert [item.source for item in pack.items] == ["exchange_contact"]
    assert "VIP Leader" in pack.items[0].content
    assert "route" not in pack.model_dump_json().lower()


@pytest.mark.asyncio
async def test_vip_rule_profile_evidence_and_drafter_form_one_writing_flow(
    monkeypatch,
    graph_node_harness,
):
    mailbox = "q-fu@tianjin-air.com"
    artifact = compile_registry(
        "tier1_rules",
        internal_email_domains=("tianjin-air.com",),
        me_email=mailbox,
    )
    assert not isinstance(artifact, CompilationFailure)
    monkeypatch.setattr(
        "src.router.engine.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_EMAIL=mailbox),
    )
    decision = await RoutingEngine(
        artifact=artifact,
        me_email=mailbox,
    ).resolve_route(
        {
            "email": {
                "sender": "lanjuan@tianjin-air.com",
                "to": [mailbox],
                "cc": [],
                "subject": "请确认本周安排",
                "body": "请确认本周安排。",
            }
        },
        [],
    )
    assert decision.route is CanonicalRoute.REPLY
    assert decision.handoff_profile_id == "vip_direct_reply_v1"

    class ExistingRetriever:
        def search_by_thread(self, **_kwargs):
            return []

        def search(self, **_kwargs):
            return []

    class ExchangeDirectory:
        async def resolve_contact(self, query):
            assert query == "lanjuan@tianjin-air.com"
            return "兰娟"

    plan = get_handoff_profile(decision.handoff_profile_id).build_plan()
    pack = await WritingEvidenceRetriever(
        EvidenceAdapterRegistry.from_runtime(
            retriever=ExistingRetriever(),
            exchange_client=ExchangeDirectory(),
        )
    ).retrieve(
        plan,
        {"sender": "兰娟 <lanjuan@tianjin-air.com>"},
    )
    assert [item.source for item in pack.items] == ["exchange_contact"]

    captured_prompt = ""

    def draft_response(prompt):
        nonlocal captured_prompt
        captured_prompt = prompt.to_string()
        return SimpleNamespace(content="兰总，已收到。我将在本周内确认安排并反馈。")

    monkeypatch.setattr(
        "src.providers.factory.get_llm_for_role",
        lambda *_args, **_kwargs: RunnableLambda(draft_response),
    )
    monkeypatch.setattr("src.nodes.drafter.enforce_model_input_budget", lambda *_a, **_k: None)
    email = {
        "id": "vip-mail-1",
        "sender": "兰娟 <lanjuan@tianjin-air.com>",
        "to": [mailbox],
        "cc": [],
        "subject": "请确认本周安排",
        "body": "请确认本周安排。",
    }
    state = graph_node_harness.state(
        email,
        route_decision=decision.model_dump(mode="json"),
        handoff_plan=plan.model_dump(mode="json"),
        handoff_plan_digest=plan.canonical_digest(),
        evidence_pack_digest=pack.canonical_digest(),
        context=[
            {
                "id": item.source_id,
                "sender": item.sender,
                "subject": item.subject,
                "body": item.content,
            }
            for item in pack.items
        ],
    )
    result = await generate_draft(state, graph_node_harness.dependencies)

    assert result["draft_id"] == "vip-mail-1"
    assert graph_node_harness.drafts["vip-mail-1"].startswith("兰总")
    assert "VIP 直发写作合同" in captured_prompt
    assert "Exchange directory verified" in captured_prompt
