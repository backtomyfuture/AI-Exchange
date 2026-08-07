"""Tier 1 v1 three-valued matcher tests (docs/tier1-routing-design.md §4.2, §4.4)."""
import pytest

from src.router.tier1.dsl import (
    UNKNOWN,
    EmailView,
    RuleEvalStatus,
    TriState,
    UnsafeRegexError,
    compile_safe_regex,
    evaluate_match,
    tri_all,
    tri_any,
    tri_not,
)
from src.router.tier1.schema import RuleManifest

ANCHOR = {"any": [{"field": "sender.address", "op": "eq", "value": "a@example.com"}]}


def _match(conditions=None):
    payload = {
        "rule_id": "RULE-DSL-001",
        "rule_version": 1,
        "status": "enabled",
        "owner": "team-x",
        "match": {"anchor": ANCHOR, **({"conditions": conditions} if conditions else {})},
        "decision": {"route": "read_only"},
        "governance": {
            "positive_cases": [{"case_id": "p1", "email": {"sender": {"address": "a@example.com"}}}],
            **({"negative_cases": [{"case_id": "n1", "email": {}}]} if conditions else {}),
        },
    }
    return RuleManifest.model_validate(payload).match


def test_tri_not_table():
    assert tri_not(TriState.TRUE) is TriState.FALSE
    assert tri_not(TriState.FALSE) is TriState.TRUE
    assert tri_not(TriState.UNKNOWN) is TriState.UNKNOWN


def test_tri_all_table():
    assert tri_all([TriState.TRUE, TriState.TRUE]) is TriState.TRUE
    assert tri_all([TriState.TRUE, TriState.FALSE]) is TriState.FALSE
    assert tri_all([TriState.FALSE, TriState.UNKNOWN]) is TriState.FALSE  # FALSE dominates UNKNOWN
    assert tri_all([TriState.TRUE, TriState.UNKNOWN]) is TriState.UNKNOWN


def test_tri_any_table():
    assert tri_any([TriState.FALSE, TriState.FALSE]) is TriState.FALSE
    assert tri_any([TriState.TRUE, TriState.FALSE]) is TriState.TRUE
    assert tri_any([TriState.TRUE, TriState.UNKNOWN]) is TriState.TRUE  # TRUE dominates UNKNOWN
    assert tri_any([TriState.FALSE, TriState.UNKNOWN]) is TriState.UNKNOWN


def test_anchor_false_is_not_matched_even_with_unknown_conditions():
    match = _match({"all": [{"field": "subject", "op": "contains", "value": "refund"}]})
    view = EmailView(sender_address="other@example.com", subject=UNKNOWN)
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.NOT_MATCHED


def test_anchor_unknown_is_indeterminate_regardless_of_conditions():
    match = _match()
    view = EmailView(sender_address=UNKNOWN)
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.INDETERMINATE


def test_missing_field_is_empty_not_unknown():
    """A genuinely absent Cc list ([]) participates as FALSE, not UNKNOWN."""
    match = _match({"all": [{"field": "cc.addresses", "op": "has_any", "values": ["x@example.com"]}]})
    view = EmailView(sender_address="a@example.com", cc_addresses=[])
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.NOT_MATCHED


def test_condition_unknown_makes_rule_indeterminate():
    match = _match({"all": [{"field": "subject", "op": "contains", "value": "refund"}]})
    view = EmailView(sender_address="a@example.com", subject=UNKNOWN)
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.INDETERMINATE


def test_full_match():
    match = _match({"all": [{"field": "subject", "op": "contains", "value": "refund"}]})
    view = EmailView(sender_address="a@example.com", subject="refund please")
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.MATCHED


@pytest.mark.parametrize(
    "pattern",
    [
        "(?=foo)",  # lookahead
        "(?<=foo)",  # lookbehind
        r"(a\1)",  # backreference
        "(a+)+",  # classic catastrophic-backtracking shape
        "a" * 201,  # too long
    ],
)
def test_unsafe_regex_rejected(pattern):
    with pytest.raises(UnsafeRegexError):
        compile_safe_regex(pattern)


def test_safe_regex_compiles_and_matches():
    compiled = compile_safe_regex(r"^INV-\d{4,8}$")
    assert compiled.search("INV-12345") is not None
    assert compiled.search("not-an-invoice") is None


def test_regex_leaf_input_over_budget_is_unknown_not_error():
    match = _match({"all": [{"field": "subject", "op": "regex", "value": "refund"}]})
    view = EmailView(sender_address="a@example.com", subject="x" * 20_001)
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.INDETERMINATE


# ---------------------------------------------------------------------------
# $ME placeholder
# ---------------------------------------------------------------------------


def _me_match(anchor, conditions=None):
    payload = {
        "rule_id": "RULE-DSL-ME-001",
        "rule_version": 1,
        "status": "enabled",
        "owner": "team-x",
        "match": {"anchor": anchor, **({"conditions": conditions} if conditions else {})},
        "decision": {"route": "read_only"},
        "governance": {
            "positive_cases": [
                {"case_id": "p1", "email": {"to": ["me@example.com"], "sender": {"address": "vip@example.com"}}}
            ],
            "negative_cases": [
                {"case_id": "n1", "email": {"to": ["other@example.com"], "sender": {"address": "vip@example.com"}}}
            ],
        },
    }
    return RuleManifest.model_validate(payload).match


def test_me_placeholder_resolves_in_condition_value_when_supplied():
    match = _me_match(
        anchor={"any": [{"field": "sender.address", "op": "eq", "value": "vip@example.com"}]},
        conditions={"all": [{"field": "to.addresses", "op": "has_any", "values": ["$ME"]}]},
    )
    view = EmailView(sender_address="vip@example.com", to_addresses=["me@example.com"])
    assert (
        evaluate_match(match.anchor, match.conditions, view, me_email="me@example.com")
        is RuleEvalStatus.MATCHED
    )


def test_me_placeholder_not_matching_view_is_not_matched():
    match = _me_match(
        anchor={"any": [{"field": "sender.address", "op": "eq", "value": "vip@example.com"}]},
        conditions={"all": [{"field": "to.addresses", "op": "has_any", "values": ["$ME"]}]},
    )
    view = EmailView(sender_address="vip@example.com", to_addresses=["other@example.com"])
    assert (
        evaluate_match(match.anchor, match.conditions, view, me_email="me@example.com")
        is RuleEvalStatus.NOT_MATCHED
    )


def test_me_placeholder_negated_condition_flips_around_direct_recipient():
    match = _me_match(
        anchor={"any": [{"field": "sender.address", "op": "eq", "value": "vip@example.com"}]},
        conditions={"all": [{"not": {"field": "to.addresses", "op": "has_any", "values": ["$ME"]}}]},
    )
    direct = EmailView(sender_address="vip@example.com", to_addresses=["me@example.com"])
    cc_only = EmailView(sender_address="vip@example.com", to_addresses=["other@example.com"])
    assert (
        evaluate_match(match.anchor, match.conditions, direct, me_email="me@example.com")
        is RuleEvalStatus.NOT_MATCHED
    )
    assert (
        evaluate_match(match.anchor, match.conditions, cc_only, me_email="me@example.com")
        is RuleEvalStatus.MATCHED
    )


def test_me_placeholder_without_me_email_is_indeterminate_not_false():
    """A misconfigured/missing me_email must surface as manual_review, not a
    silent wrong route, whether $ME sits in the anchor or in conditions."""
    match = _me_match(
        anchor={"any": [{"field": "sender.address", "op": "eq", "value": "vip@example.com"}]},
        conditions={"all": [{"field": "to.addresses", "op": "has_any", "values": ["$ME"]}]},
    )
    view = EmailView(sender_address="vip@example.com", to_addresses=["me@example.com"])
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.INDETERMINATE


def test_me_placeholder_in_anchor_without_me_email_is_indeterminate():
    match = _me_match(anchor={"any": [{"field": "to.addresses", "op": "has_any", "values": ["$ME"]}]})
    view = EmailView(sender_address="vip@example.com", to_addresses=["me@example.com"])
    assert evaluate_match(match.anchor, match.conditions, view) is RuleEvalStatus.INDETERMINATE


def test_me_placeholder_in_anchor_resolves_when_supplied():
    match = _me_match(anchor={"any": [{"field": "to.addresses", "op": "has_any", "values": ["$ME"]}]})
    view = EmailView(sender_address="vip@example.com", to_addresses=["me@example.com"])
    assert (
        evaluate_match(match.anchor, match.conditions, view, me_email="me@example.com")
        is RuleEvalStatus.MATCHED
    )
