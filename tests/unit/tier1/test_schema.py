"""Tier 1 v1 rule manifest schema tests (docs/tier1-routing-design.md).

Covers strict-schema rejection, anchor operator restrictions, route-specific
``decision.params`` validation, and the no_action/full_text governance gates.
"""
import pytest
from pydantic import ValidationError

from src.router.tier1.schema import (
    CanonicalRoute,
    RuleManifest,
    canonical_match_signature,
)

BASE_RULE = {
    "rule_id": "RULE-BASE-001",
    "rule_version": 1,
    "status": "enabled",
    "owner": "team-x",
    "match": {"anchor": {"any": [{"field": "sender.address", "op": "eq", "value": "a@example.com"}]}},
    "decision": {"route": "read_only"},
    "governance": {"positive_cases": [{"case_id": "p1", "email": {"sender": {"address": "a@example.com"}}}]},
}


def _rule(**overrides):
    payload = {**BASE_RULE, **overrides}
    return RuleManifest.model_validate(payload)


def test_minimal_valid_rule_parses():
    rule = _rule()
    assert rule.decision.route is CanonicalRoute.READ_ONLY


def test_unknown_top_level_field_is_rejected():
    with pytest.raises(ValidationError):
        _rule(need_reply=True)


def test_unknown_decision_params_field_is_rejected():
    with pytest.raises(ValidationError):
        _rule(decision={"route": "read_only", "params": {"card_type": "info"}})


@pytest.mark.parametrize("op", ["contains", "regex"])
def test_anchor_rejects_weak_operators(op):
    with pytest.raises(ValidationError):
        _rule(match={"anchor": {"any": [{"field": "sender.address", "op": op, "value": "a"}]}})


def test_anchor_eq_on_set_field_is_rejected():
    with pytest.raises(ValidationError):
        _rule(match={"anchor": {"any": [{"field": "to.addresses", "op": "eq", "value": "a@example.com"}]}})


def test_condition_leaf_allows_contains_and_regex():
    rule = _rule(
        match={
            "anchor": BASE_RULE["match"]["anchor"],
            "conditions": {"all": [{"field": "subject", "op": "contains", "value": "refund"}]},
        },
        governance={
            **BASE_RULE["governance"],
            "negative_cases": [{"case_id": "n1", "email": {"sender": {"address": "a@example.com"}, "subject": "x"}}],
        },
    )
    assert rule.match.conditions is not None


def test_condition_top_level_bare_not_is_rejected():
    with pytest.raises(ValidationError):
        _rule(
            match={
                "anchor": BASE_RULE["match"]["anchor"],
                "conditions": {"not": {"field": "subject", "op": "contains", "value": "x"}},
            },
            governance={
                **BASE_RULE["governance"],
                "negative_cases": [{"case_id": "n1", "email": {}}],
            },
        )


def test_nested_not_inside_all_is_allowed():
    rule = _rule(
        match={
            "anchor": BASE_RULE["match"]["anchor"],
            "conditions": {
                "all": [
                    {"not": {"field": "subject", "op": "contains", "value": "spam"}},
                ]
            },
        },
        governance={
            **BASE_RULE["governance"],
            "negative_cases": [{"case_id": "n1", "email": {"subject": "spam"}}],
        },
    )
    assert rule.match.conditions.all[0].not_.field == "subject"


def test_forward_requires_exact_addresses():
    with pytest.raises(ValidationError):
        _rule(decision={"route": "forward", "params": {"fixed_recipients": ["*@example.com"]}})


def test_forward_params_valid():
    rule = _rule(decision={"route": "forward", "params": {"fixed_recipients": ["ops@example.com"]}})
    assert rule.decision.typed_params.fixed_recipients == ["ops@example.com"]


def test_no_action_requires_owner_expiry_and_two_negatives():
    with pytest.raises(ValidationError):
        _rule(
            owner=None,
            decision={"route": "no_action", "params": {"reason_code": "auto_reply_detected"}},
        )


def test_no_action_with_full_governance_parses():
    rule = _rule(
        decision={"route": "no_action", "params": {"reason_code": "auto_reply_detected"}},
        validity={"expires_at": "2099-01-01"},
        governance={
            "positive_cases": [{"case_id": "p1", "email": {}}],
            "negative_cases": [
                {"case_id": "n1", "email": {}},
                {"case_id": "n2", "email": {}},
            ],
        },
    )
    assert rule.decision.route is CanonicalRoute.NO_ACTION


def test_full_text_match_requires_acknowledgement():
    with pytest.raises(ValidationError):
        _rule(
            match={
                "anchor": BASE_RULE["match"]["anchor"],
                "conditions": {"all": [{"field": "body.full_text", "op": "contains", "value": "refund"}]},
            },
            governance={
                **BASE_RULE["governance"],
                "negative_cases": [{"case_id": "n1", "email": {}}],
            },
        )


def test_full_text_match_with_acknowledgement_parses():
    rule = _rule(
        match={
            "anchor": BASE_RULE["match"]["anchor"],
            "conditions": {"all": [{"field": "body.full_text", "op": "contains", "value": "refund"}]},
        },
        governance={
            **BASE_RULE["governance"],
            "negative_cases": [{"case_id": "n1", "email": {}}],
            "full_text_match_acknowledged": True,
        },
    )
    assert rule.match.conditions is not None


def test_invalid_rule_id_is_rejected():
    with pytest.raises(ValidationError):
        _rule(rule_id="a")  # too short for the id pattern


def test_content_condition_without_negative_case_is_rejected():
    with pytest.raises(ValidationError):
        _rule(
            match={
                "anchor": BASE_RULE["match"]["anchor"],
                "conditions": {"all": [{"field": "subject", "op": "contains", "value": "refund"}]},
            },
        )


def test_canonical_match_signature_is_case_and_order_insensitive():
    left = _rule(
        match={"anchor": {"any": [
            {"field": "sender.address", "op": "in", "values": ["A@example.com", "b@example.com"]},
        ]}}
    )
    right = _rule(
        match={"anchor": {"any": [
            {"field": "sender.address", "op": "in", "values": ["b@EXAMPLE.com", "a@example.com"]},
        ]}}
    )
    assert canonical_match_signature(left.match) == canonical_match_signature(right.match)


def test_canonical_match_signature_differs_for_different_anchors():
    left = _rule()
    right = _rule(match={"anchor": {"any": [{"field": "sender.address", "op": "eq", "value": "z@example.com"}]}})
    assert canonical_match_signature(left.match) != canonical_match_signature(right.match)
