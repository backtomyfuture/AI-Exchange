"""Translate reviewed discovery candidates into strict Tier 1 v1 manifests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.router.tier1.schema import RuleManifest, RuleStatus


_ADDRESS_FIELDS = {
    "sender_match": "sender.address",
    "to_match": "to.addresses",
    "cc_match": "cc.addresses",
}
_CONTENT_FIELDS = {
    "subject_match": "subject",
    "body_match": "body.current_text",
}


def _values(condition: Mapping[str, Any]) -> list[str]:
    value = condition.get("value")
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value or "").strip() else []


def _address_leaf(condition: Mapping[str, Any]) -> dict[str, Any]:
    field = _ADDRESS_FIELDS[str(condition["type"])]
    values = _values(condition)
    if field == "sender.address":
        return (
            {"field": field, "op": "eq", "value": values[0]}
            if len(values) == 1
            else {"field": field, "op": "in", "values": values}
        )
    return {"field": field, "op": "has_any", "values": values}


def _content_leaf(condition: Mapping[str, Any]) -> dict[str, Any]:
    field = _CONTENT_FIELDS[str(condition["type"])]
    operator = str(condition.get("operator") or "contains")
    values = _values(condition)
    if operator == "in":
        operator = "contains_any"
    if operator in {"contains_any"}:
        return {"field": field, "op": operator, "values": values}
    return {"field": field, "op": operator, "value": values[0]}


def candidate_match(candidate: Any) -> tuple[dict[str, Any], dict[str, Any] | None]:
    anchors: list[dict[str, Any]] = []
    content: list[dict[str, Any]] = []
    for raw in candidate.conditions:
        if not isinstance(raw, Mapping):
            raise ValueError("candidate_condition_invalid")
        condition_type = raw.get("type")
        if condition_type in _ADDRESS_FIELDS:
            anchors.append(_address_leaf(raw))
        elif condition_type in _CONTENT_FIELDS:
            content.append(_content_leaf(raw))
        else:
            raise ValueError("candidate_condition_unsupported")
    if not anchors:
        raise ValueError("candidate_anchor_required")
    logic = str(candidate.condition_logic or "and")
    if logic not in {"and", "or"}:
        raise ValueError("candidate_condition_logic_invalid")
    if logic == "or" and content:
        raise ValueError("candidate_mixed_or_unsupported")
    anchor_key = "all" if logic == "and" else "any"
    conditions = None
    if content:
        conditions = {"all": content}
    return {anchor_key: anchors}, conditions


def _positive_fixture(anchor: dict[str, Any], conditions: dict[str, Any] | None) -> dict[str, Any]:
    email: dict[str, Any] = {"sender": {"address": "candidate@example.invalid"}}
    leaves = next(iter(anchor.values()))
    for leaf in leaves:
        values = leaf.get("values") or [leaf.get("value")]
        if leaf["field"] == "sender.address":
            email["sender"] = {"address": values[0]}
        elif leaf["field"] == "to.addresses":
            email["to"] = [values[0]]
        elif leaf["field"] == "cc.addresses":
            email["cc"] = [values[0]]
    if conditions:
        for leaf in conditions["all"]:
            value = leaf.get("value") or (leaf.get("values") or [""])[0]
            if leaf["field"] == "subject":
                email["subject"] = value
            else:
                email.setdefault("body", {})["current_text"] = value
    return {"case_id": "reviewed-candidate-positive", "email": email}


def manifest_for_candidate(
    candidate: Any,
    *,
    status: RuleStatus,
) -> RuleManifest:
    anchor, conditions = candidate_match(candidate)
    if candidate.suggested_action == "forward":
        route = "forward"
        params = {
            "fixed_recipients": list(candidate.suggested_forward_to),
            "allow_recipient_edit": True,
            "include_attachments": False,
        }
    elif candidate.suggested_need_reply:
        route = "reply"
        params = {"reply_mode": "sender_only"}
    else:
        # Discovery may suggest no reply, but only an owner-reviewed rule with
        # explicit hard negatives and expiry may suppress all action.
        route = "read_only"
        params = {}
    raw: dict[str, Any] = {
        "schema_version": 1,
        "rule_id": candidate.skill_id,
        "rule_version": 1,
        "status": status.value,
        "owner": "mailbox-owner" if status is RuleStatus.ENABLED else "pending-review",
        "purpose": candidate.description,
        "match": {"anchor": anchor},
        "decision": {
            "route": route,
            "business_flow_id": f"discovered:{candidate.candidate_id}",
            "params": params,
        },
        "governance": {
            "criticality": candidate.suggested_priority,
            "risk_notes": (
                f"discovery_confidence={candidate.confidence:.3f}; "
                f"sample_count={candidate.discovery_sample_count}; "
                f"observed_reply_rate={candidate.discovery_reply_rate:.3f}"
            ),
            "positive_cases": [_positive_fixture(anchor, conditions)],
        },
    }
    if conditions:
        raw["match"]["conditions"] = conditions
        negative = _positive_fixture(anchor, None)
        negative["case_id"] = "reviewed-candidate-hard-negative"
        raw["governance"]["negative_cases"] = [negative]
    return RuleManifest.model_validate(raw)


__all__ = ["candidate_match", "manifest_for_candidate"]
