from datetime import UTC, datetime, timedelta

import pytest

from src.router.observability import validate_route_evaluation


def _evaluation():
    now = datetime.now(UTC)
    return {
        "tier": "tier1",
        "outcome": "matched",
        "matched_rule_ids": [{"rule_id": "rule-1", "route": "reply"}],
        "candidate_routes": [{"route": "reply", "votes": 1}],
        "evidence_refs": [],
        "confidence": 1.0,
        "continue_reason": "Tier 1 已命中",
        "safe_reason": "rule_match",
        "started_at": now,
        "finished_at": now + timedelta(milliseconds=4),
        "safe_detail_json": {"artifact_digest": "abc", "matched_rule_count": 1},
    }


def test_route_evaluation_is_bound_to_inbox_and_sequence():
    trace = validate_route_evaluation(_evaluation(), inbox_id="inbox-1", sequence=1)

    assert trace.inbox_id == "inbox-1"
    assert trace.sequence == 1
    assert trace.safe_detail_json["matched_rule_count"] == 1


@pytest.mark.parametrize("forbidden_key", ["body", "content_ref", "attachment_name", "prompt"])
def test_route_evaluation_rejects_content_shaped_projection_keys(forbidden_key):
    payload = _evaluation()
    payload["safe_detail_json"] = {forbidden_key: "must not persist"}

    with pytest.raises(ValueError, match="route_evaluation_"):
        validate_route_evaluation(payload, inbox_id="inbox-1", sequence=1)


def test_route_evaluation_rejects_reversed_timestamps():
    payload = _evaluation()
    payload["finished_at"] = payload["started_at"] - timedelta(seconds=1)

    with pytest.raises(ValueError, match="route_evaluation_time_order_invalid"):
        validate_route_evaluation(payload, inbox_id="inbox-1", sequence=1)
