"""Tier 1 decision aggregation tests (docs/tier1-routing-design.md §2.2)."""
from datetime import UTC, datetime

from src.router.tier1.decision import DecisionOrigin, EvaluationOutcome, build_tier1_decision
from src.router.tier1.dsl import UNKNOWN, EmailView
from src.router.tier1.schema import CanonicalRoute, RuleManifest


def _rule(
    rule_id,
    sender,
    route="read_only",
    params=None,
    business_flow_id=None,
    validity=None,
):
    governance = {"positive_cases": [{"case_id": "p1", "email": {"sender": {"address": sender}}}]}
    validity = dict(validity or {})
    if route == "no_action":
        governance["negative_cases"] = [
            {"case_id": "n1", "email": {"sender": {"address": "other1@example.com"}}},
            {"case_id": "n2", "email": {"sender": {"address": "other2@example.com"}}},
        ]
        validity.setdefault("expires_at", "2099-01-01")
    payload = {
        "rule_id": rule_id,
        "rule_version": 1,
        "status": "enabled",
        "owner": "team-x",
        "match": {"anchor": {"any": [{"field": "sender.address", "op": "eq", "value": sender}]}},
        "decision": {
            "route": route,
            **({"params": params} if params else {}),
            **({"business_flow_id": business_flow_id} if business_flow_id else {}),
        },
        "governance": governance,
        **({"validity": validity} if validity else {}),
    }
    return RuleManifest.model_validate(payload)


def test_no_rule_matches_is_abstain():
    rule = _rule("RULE-D-001", "a@example.com")
    view = EmailView(sender_address="nobody@example.com")
    decision = build_tier1_decision([rule], view)
    assert decision.outcome is EvaluationOutcome.ABSTAIN
    assert decision.route is None


def test_single_matching_rule_is_rule_declared():
    rule = _rule("RULE-D-002", "a@example.com", route="forward", params={"fixed_recipients": ["ops@example.com"]})
    view = EmailView(sender_address="a@example.com")
    decision = build_tier1_decision([rule], view)
    assert decision.outcome is EvaluationOutcome.MATCHED
    assert decision.route is CanonicalRoute.FORWARD
    assert decision.decision_origin is DecisionOrigin.RULE_DECLARED
    assert decision.selected_action_fingerprint is not None
    assert [r.rule_id for r in decision.matched_rules] == ["RULE-D-002"]


def test_two_rules_same_action_merge_without_conflict():
    rule_a = _rule("RULE-D-003", "a@example.com", business_flow_id="flow-a")
    rule_b = _rule("RULE-D-004", "a@example.com", business_flow_id="flow-b")
    view = EmailView(sender_address="a@example.com")
    decision = build_tier1_decision([rule_a, rule_b], view)
    assert decision.outcome is EvaluationOutcome.MATCHED
    assert len(decision.candidate_actions) == 1
    assert sorted(decision.candidate_actions[0].rule_ids) == ["RULE-D-003", "RULE-D-004"]
    assert decision.business_flow_ids == ["flow-a", "flow-b"]


def test_two_rules_different_actions_is_conflict_forced_to_manual_review():
    rule_a = _rule("RULE-D-005", "a@example.com", route="read_only")
    rule_b = _rule("RULE-D-006", "a@example.com", route="no_action", params={"reason_code": "auto_reply_detected"})
    view = EmailView(sender_address="a@example.com")
    decision = build_tier1_decision([rule_a, rule_b], view)
    assert decision.outcome is EvaluationOutcome.CONFLICT
    assert decision.route is CanonicalRoute.MANUAL_REVIEW
    assert decision.decision_origin is DecisionOrigin.RUNTIME_CONFLICT
    assert decision.selected_action_fingerprint is None
    assert len(decision.candidate_actions) == 2


def test_indeterminate_rule_forces_error_manual_review():
    rule = _rule("RULE-D-007", "a@example.com")
    view = EmailView(sender_address=UNKNOWN)
    decision = build_tier1_decision([rule], view)
    assert decision.outcome is EvaluationOutcome.ERROR
    assert decision.route is CanonicalRoute.MANUAL_REVIEW
    assert decision.decision_origin is DecisionOrigin.RUNTIME_INDETERMINATE


def test_indeterminate_rule_takes_priority_over_a_matched_rule():
    matched_rule = _rule("RULE-D-008", "a@example.com")
    indeterminate_rule_payload = {
        "rule_id": "RULE-D-009",
        "rule_version": 1,
        "status": "enabled",
        "owner": "team-x",
        "match": {"anchor": {"any": [{"field": "to.addresses", "op": "has_any", "values": ["b@example.com"]}]}},
        "decision": {"route": "read_only"},
        "governance": {"positive_cases": [{"case_id": "p1", "email": {"to": ["b@example.com"]}}]},
    }
    indeterminate_rule = RuleManifest.model_validate(indeterminate_rule_payload)
    # matched_rule's anchor resolves (sender is known); indeterminate_rule's
    # anchor cannot (to_addresses resolution failed) -> overall outcome must
    # still be ERROR, not MATCHED, per the invariants table.
    view = EmailView(sender_address="a@example.com", to_addresses=UNKNOWN)
    decision = build_tier1_decision([matched_rule, indeterminate_rule], view)
    assert decision.outcome is EvaluationOutcome.ERROR


def test_me_placeholder_threaded_through_to_dsl():
    payload = {
        "rule_id": "RULE-D-010",
        "rule_version": 1,
        "status": "enabled",
        "owner": "team-x",
        "match": {
            "anchor": {"any": [{"field": "sender.address", "op": "eq", "value": "vip@example.com"}]},
            "conditions": {"all": [{"field": "to.addresses", "op": "has_any", "values": ["$ME"]}]},
        },
        "decision": {"route": "reply", "params": {"reply_mode": "sender_and_original_cc"}},
        "governance": {
            "positive_cases": [
                {"case_id": "p1", "email": {"sender": {"address": "vip@example.com"}, "to": ["me@example.com"]}}
            ],
            "negative_cases": [
                {"case_id": "n1", "email": {"sender": {"address": "vip@example.com"}, "to": ["other@example.com"]}}
            ],
        },
    }
    rule = RuleManifest.model_validate(payload)

    view_direct = EmailView(sender_address="vip@example.com", to_addresses=["me@example.com"])
    decision = build_tier1_decision([rule], view_direct, me_email="me@example.com")
    assert decision.outcome is EvaluationOutcome.MATCHED
    assert decision.route is CanonicalRoute.REPLY

    # Without me_email, the $ME leaf can't resolve -> INDETERMINATE -> ERROR,
    # not a silent NOT_MATCHED/ABSTAIN.
    decision_no_me = build_tier1_decision([rule], view_direct)
    assert decision_no_me.outcome is EvaluationOutcome.ERROR


def test_rule_is_not_active_before_effective_from():
    rule = _rule(
        "RULE-D-011",
        "future@example.com",
        validity={"effective_from": "2026-08-10T00:00:00Z"},
    )
    decision = build_tier1_decision(
        [rule],
        EmailView(sender_address="future@example.com"),
        decision_time=datetime(2026, 8, 9, 23, 59, tzinfo=UTC),
    )
    assert decision.outcome is EvaluationOutcome.ABSTAIN


def test_rule_stops_matching_at_expiry_without_reloading_rules():
    rule = _rule(
        "RULE-D-012",
        "expiring@example.com",
        validity={"expires_at": "2026-08-10T00:00:00Z"},
    )
    view = EmailView(sender_address="expiring@example.com")

    before_expiry = build_tier1_decision(
        [rule],
        view,
        decision_time=datetime(2026, 8, 9, 23, 59, 59, tzinfo=UTC),
    )
    at_expiry = build_tier1_decision(
        [rule],
        view,
        decision_time=datetime(2026, 8, 10, tzinfo=UTC),
    )

    assert before_expiry.outcome is EvaluationOutcome.MATCHED
    assert at_expiry.outcome is EvaluationOutcome.ABSTAIN
