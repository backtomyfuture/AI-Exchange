from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.handoff.approval_feedback import drafts_differ, record_human_route_outcome
from src.handoff.labels import eligible_for_tier2
from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
from src.router.tier1.schema import CanonicalRoute


def _decision(tier=RouteTier.TIER1, route=CanonicalRoute.REPLY) -> RouteDecision:
    kwargs = {
        "outcome": DecisionOutcome.MATCHED,
        "route": route,
        "params": {"reply_mode": "sender_only"} if route is CanonicalRoute.REPLY else {"fixed_recipients": ["a@b.com"]},
        "provenance": RouteProvenance(
            tier=tier,
            source_version="test-v1",
            artifact_digest="a" * 64 if tier is RouteTier.TIER1 else None,
            confidence=1.0,
        ),
        "reason_code": "test",
        "handoff_profile_id": "generic_reply_v1" if route is CanonicalRoute.REPLY else "generic_forward_v1",
    }
    return RouteDecision(**kwargs)


def test_tier3_and_historical_labels_cannot_vote():
    assert eligible_for_tier2(decision=_decision(RouteTier.TIER3)) is False
    assert eligible_for_tier2(decision=_decision(RouteTier.HISTORICAL_INFERRED)) is False


def test_as_written_human_approval_can_vote():
    assert eligible_for_tier2(
        decision=_decision(),
        human_verified=True,
        draft_edited=False,
        outcome="approved",
    )


def test_edited_or_rejected_labels_cannot_vote():
    assert not eligible_for_tier2(
        decision=_decision(),
        human_verified=True,
        draft_edited=True,
        outcome="approved",
    )
    assert not eligible_for_tier2(decision=_decision(), outcome="rejected")


def test_record_human_route_outcome_writes_quality_flags():
    processor = SimpleNamespace(update_email_labels=MagicMock(return_value=True))
    assert drafts_differ("呈阅", "请审阅")
    ok = record_human_route_outcome(
        processor,
        email_id="mail-1",
        route_decision=_decision(),
        classification={"priority": "P1", "intent": "审批"},
        outcome="approved",
        original_draft="呈阅",
        final_draft="请审阅",
    )
    assert ok is True
    kwargs = processor.update_email_labels.call_args.kwargs
    assert kwargs["human_verified"] is True
    assert kwargs["draft_edited"] is True
    assert kwargs["eligible_for_tier2"] is False
