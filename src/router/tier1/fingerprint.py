"""Action fingerprint: a versioned hash of the canonicalized route + its
default-expanded, normalized params (design doc §3).

``business_flow_id`` is a non-authoritative audit/grouping label and never
enters this computation — only fields that actually determine execution
semantics do. Two rules that produce the identical fingerprint are treated as
the same action and merged into one execution; any difference is a conflict.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from src.router.tier1.dsl import normalize_address
from src.router.tier1.schema import (
    CanonicalRoute,
    Decision,
    ForwardParams,
    ManualReviewParams,
    NoActionParams,
    ReplyMode,
    ReplyParams,
)

FINGERPRINT_VERSION = 2


def _canonical_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def canonicalize_params(route: CanonicalRoute, typed_params: Any) -> Dict[str, Any]:
    """Expand defaults and normalize into the plain dict that enters the fingerprint."""
    if route is CanonicalRoute.REPLY:
        assert isinstance(typed_params, ReplyParams)
        reply_mode = typed_params.reply_mode or ReplyMode.SENDER_AND_ORIGINAL_CC
        return {"reply_mode": reply_mode.value}

    if route is CanonicalRoute.FORWARD:
        assert isinstance(typed_params, ForwardParams)
        return {
            "fixed_recipients": sorted({normalize_address(a) for a in typed_params.fixed_recipients}),
            "cc": sorted({normalize_address(a) for a in typed_params.cc}),
            "allow_recipient_edit": bool(typed_params.allow_recipient_edit),
            "include_attachments": bool(typed_params.include_attachments),
        }

    if route is CanonicalRoute.READ_ONLY:
        return {}

    if route is CanonicalRoute.NO_ACTION:
        assert isinstance(typed_params, NoActionParams)
        return {"reason_code": typed_params.reason_code}

    if route is CanonicalRoute.MANUAL_REVIEW:
        assert isinstance(typed_params, ManualReviewParams)
        return {"reason_code": typed_params.reason_code}

    raise ValueError(f"unsupported route {route!r}")  # unreachable given the CanonicalRoute enum


def compute_action_fingerprint(
    decision: Decision,
    *,
    handoff_profile_id: str | None = None,
    fingerprint_version: int | None = None,
) -> str:
    normalized = canonicalize_params(decision.route, decision.typed_params)
    payload = {"route": decision.route.value, "params": normalized}
    profile_id = handoff_profile_id or decision.handoff_profile_id
    version = fingerprint_version or (2 if profile_id else 1)
    if version == 2:
        if not profile_id:
            raise ValueError("v2 action fingerprint requires handoff_profile_id")
        payload["handoff_profile_id"] = profile_id
    elif version != 1:
        raise ValueError(f"unsupported fingerprint version: {version}")
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"sha256:v2:{digest}" if version == 2 else f"sha256:{digest}"
