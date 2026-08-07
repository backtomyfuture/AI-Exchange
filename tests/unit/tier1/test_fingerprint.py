"""Action fingerprint tests (docs/tier1-routing-design.md §3).

``business_flow_id`` must never affect the fingerprint; default-expanded and
address-normalized params of otherwise-identical rules must fingerprint
identically, and any authoritative difference must fingerprint differently.
"""
from src.router.tier1.fingerprint import compute_action_fingerprint
from src.router.tier1.schema import RuleManifest

BASE = {
    "rule_id": "RULE-FP-001",
    "rule_version": 1,
    "status": "enabled",
    "owner": "team-x",
    "match": {"anchor": {"any": [{"field": "sender.address", "op": "eq", "value": "a@example.com"}]}},
    "governance": {"positive_cases": [{"case_id": "p1", "email": {"sender": {"address": "a@example.com"}}}]},
}


def _decision(decision_payload):
    payload = {**BASE, "decision": decision_payload}
    if decision_payload.get("route") == "no_action":
        payload = {
            **payload,
            "validity": {"expires_at": "2099-01-01"},
            "governance": {
                "positive_cases": BASE["governance"]["positive_cases"],
                "negative_cases": [
                    {"case_id": "n1", "email": {"sender": {"address": "other1@example.com"}}},
                    {"case_id": "n2", "email": {"sender": {"address": "other2@example.com"}}},
                ],
            },
        }
    return RuleManifest.model_validate(payload).decision


def test_business_flow_id_excluded_from_fingerprint():
    left = _decision({"route": "read_only", "business_flow_id": "flow-a"})
    right = _decision({"route": "read_only", "business_flow_id": "flow-b"})
    assert compute_action_fingerprint(left) == compute_action_fingerprint(right)


def test_default_expanded_reply_mode_matches_explicit_default():
    implicit = _decision({"route": "reply"})
    explicit = _decision({"route": "reply", "params": {"reply_mode": "sender_and_original_cc"}})
    assert compute_action_fingerprint(implicit) == compute_action_fingerprint(explicit)


def test_different_reply_mode_fingerprints_differently():
    a = _decision({"route": "reply", "params": {"reply_mode": "sender_only"}})
    b = _decision({"route": "reply", "params": {"reply_mode": "sender_and_original_cc"}})
    assert compute_action_fingerprint(a) != compute_action_fingerprint(b)


def test_forward_recipients_order_and_case_insensitive():
    a = _decision(
        {"route": "forward", "params": {"fixed_recipients": ["Ops@Example.com", "sales@example.com"]}}
    )
    b = _decision(
        {"route": "forward", "params": {"fixed_recipients": ["sales@example.com", "ops@example.com"]}}
    )
    assert compute_action_fingerprint(a) == compute_action_fingerprint(b)


def test_forward_extra_cc_fingerprints_differently():
    a = _decision({"route": "forward", "params": {"fixed_recipients": ["ops@example.com"]}})
    b = _decision(
        {"route": "forward", "params": {"fixed_recipients": ["ops@example.com"], "cc": ["cc@example.com"]}}
    )
    assert compute_action_fingerprint(a) != compute_action_fingerprint(b)


def test_no_action_reason_code_participates_in_fingerprint():
    a = _decision({"route": "no_action", "params": {"reason_code": "auto_reply_detected"}})
    b = _decision({"route": "no_action", "params": {"reason_code": "ndr_detected"}})
    assert compute_action_fingerprint(a) != compute_action_fingerprint(b)


def test_read_only_fingerprint_is_stable_and_route_scoped():
    a = _decision({"route": "read_only"})
    b = _decision({"route": "read_only"})
    assert compute_action_fingerprint(a) == compute_action_fingerprint(b)
    manual = _decision({"route": "manual_review", "params": {"reason_code": "escalation"}})
    assert compute_action_fingerprint(a) != compute_action_fingerprint(manual)
