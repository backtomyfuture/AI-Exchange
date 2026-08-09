"""One canonical routing module for deterministic, historical and model tiers."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.config import get_settings
from src.graph.state import AgentState
from src.router.decision import (
    DecisionOutcome,
    RouteDecision,
    RouteProvenance,
    RouteTier,
)
from src.router.tier1.compiler import CompiledArtifact, CompilationFailure, compile_registry
from src.router.tier1.decision import EvaluationOutcome, build_tier1_decision
from src.router.tier1.dsl import EmailView
from src.router.tier1.fingerprint import compute_action_fingerprint
from src.router.tier1.schema import CanonicalRoute, Decision
from src.safety.model_budget import enforce_model_input_budget, token_budget_from_settings


logger = logging.getLogger(__name__)

TIER2_MIN_HITS = 2
TIER2_MIN_RATIO = 0.5
_MAILBOX_ADDRESS = re.compile(r"email_address='([^']+)'", re.IGNORECASE)


class _Tier3Result(BaseModel):
    model_config = ConfigDict(extra="forbid")

    route: CanonicalRoute
    params: dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(ge=0.0, le=1.0)
    reason_code: str = Field(min_length=1, max_length=128)


def _address(value: object) -> str:
    text = str(value or "").strip()
    match = _MAILBOX_ADDRESS.search(text)
    return (match.group(1) if match else text).strip()


def _addresses(value: object) -> list[str]:
    if isinstance(value, str):
        return [_address(value)] if value else []
    if not isinstance(value, Iterable) or isinstance(value, (bytes, bytearray, Mapping)):
        return []
    return [_address(item) for item in value if str(item or "").strip()]


def _email_view(email: Mapping[str, Any]) -> EmailView:
    body = str(email.get("body") or "")
    return EmailView(
        sender_address=_address(email.get("sender")),
        to_addresses=_addresses(email.get("to")),
        cc_addresses=_addresses(email.get("cc")),
        subject=str(email.get("subject") or ""),
        body_current_text=body,
        body_full_text=body,
    )


def _classification_for(decision: RouteDecision) -> dict[str, Any]:
    route = decision.route
    if route is CanonicalRoute.REPLY:
        return {"need_reply": True, "action": "reply"}
    if route is CanonicalRoute.FORWARD:
        return {"need_reply": True, "action": "forward"}
    if route is CanonicalRoute.READ_ONLY:
        return {"need_reply": False, "action": "read_only"}
    if route is CanonicalRoute.NO_ACTION:
        return {"need_reply": False, "action": "no_action"}
    if route is CanonicalRoute.MANUAL_REVIEW:
        return {"need_reply": False, "action": "manual_review"}
    return {}


def _decision_delta(decision: RouteDecision) -> AgentState:
    stage = decision.provenance.tier.value
    delta: AgentState = {
        "route_decision": decision.model_dump(mode="json"),
        "routing_stage": stage,
        "routing_log": [f"{stage} route={decision.route.value if decision.route else 'abstain'}"],
        "classification": _classification_for(decision),
    }
    if decision.route is CanonicalRoute.FORWARD:
        delta["draft_to"] = list(decision.params["fixed_recipients"])
        delta["draft_cc"] = list(decision.params.get("cc", []))
    if decision.route is CanonicalRoute.MANUAL_REVIEW:
        delta["next_step"] = "manual_review"
        delta["approval_status"] = "manual_review"
        delta["safe_error_summary"] = decision.reason_code
    return delta


def _default_artifact() -> CompiledArtifact:
    settings = get_settings()
    domains = tuple(
        item.strip()
        for item in str(settings.INTERNAL_EMAIL_DOMAINS or "").split(",")
        if item.strip()
    ) or ("tianjin-air.com", "hnair.com", "hnaaviation.com")
    result = compile_registry(
        "tier1_rules",
        internal_email_domains=domains,
        me_email=str(settings.EXCHANGE_ACCOUNT_EMAIL or "").strip()
        or "q-fu@tianjin-air.com",
    )
    if isinstance(result, CompilationFailure):
        raise RuntimeError("tier1_artifact_unavailable")
    return result


class RoutingEngine:
    """Deep module whose interface returns exactly one :class:`RouteDecision`."""

    def __init__(
        self,
        *,
        artifact: CompiledArtifact | None = None,
        me_email: str | None = None,
    ) -> None:
        self.artifact = artifact or _default_artifact()
        configured_me = str(get_settings().EXCHANGE_ACCOUNT_EMAIL or "").strip()
        self.me_email = me_email if me_email is not None else configured_me

    async def execute_router(self, state: AgentState) -> AgentState:
        email = state.get("email") or {}
        decision_time = datetime.now(UTC)
        tier1 = build_tier1_decision(
            [compiled.manifest for compiled in self.artifact.rules],
            _email_view(email),
            me_email=self.me_email or None,
            decision_time=decision_time,
        )
        provenance = RouteProvenance(
            tier=RouteTier.TIER1,
            source_version="tier1-artifact-v1",
            artifact_digest=self.artifact.digest,
            rule_ids=[ref.rule_id for ref in tier1.matched_rules],
            confidence=1.0 if tier1.outcome is EvaluationOutcome.MATCHED else None,
        )
        if tier1.outcome is EvaluationOutcome.ABSTAIN:
            decision = RouteDecision(
                outcome=DecisionOutcome.ABSTAIN,
                route=None,
                provenance=provenance,
            )
            return {
                **state,
                "route_decision": decision.model_dump(mode="json"),
                "routing_log": [*(state.get("routing_log") or []), "tier1 route=abstain"],
                "routing_stage": "pending",
            }

        if tier1.outcome is EvaluationOutcome.MATCHED:
            selected = next(
                compiled
                for compiled in self.artifact.rules
                if compiled.action_fingerprint == tier1.selected_action_fingerprint
            )
            route = selected.manifest.decision.route
            params = selected.manifest.decision.typed_params.model_dump(
                mode="json",
                exclude_none=True,
            )
            reason = (
                params.get("reason_code")
                or selected.manifest.decision.business_flow_id
                or selected.manifest.rule_id
            )
            outcome = DecisionOutcome.MATCHED
        else:
            route = CanonicalRoute.MANUAL_REVIEW
            params = {
                "reason_code": (
                    "tier1_conflict"
                    if tier1.outcome is EvaluationOutcome.CONFLICT
                    else "tier1_indeterminate"
                )
            }
            reason = params["reason_code"]
            outcome = (
                DecisionOutcome.CONFLICT
                if tier1.outcome is EvaluationOutcome.CONFLICT
                else DecisionOutcome.ERROR
            )
        decision = RouteDecision(
            outcome=outcome,
            route=route,
            params=params,
            provenance=provenance,
            reason_code=reason,
            selected_action_fingerprint=tier1.selected_action_fingerprint,
            candidate_actions=[
                {
                    "fingerprint": item.fingerprint,
                    "rule_ids": item.rule_ids,
                    "route": item.route.value,
                }
                for item in tier1.candidate_actions
            ],
        )
        delta = _decision_delta(decision)
        return {**state, **delta, "routing_log": [*(state.get("routing_log") or []), *delta["routing_log"]]}

    @staticmethod
    def _decision_from_hit(hit: Mapping[str, Any]) -> RouteDecision | None:
        raw = hit.get("route_decision")
        payload = hit.get("payload")
        if raw is None and isinstance(payload, Mapping):
            raw = payload.get("route_decision")
        try:
            decision = RouteDecision.model_validate(raw)
        except Exception:
            return None
        if decision.outcome is not DecisionOutcome.MATCHED or decision.route is None:
            return None
        return decision

    async def apply_tier2_hits(
        self,
        state: AgentState,
        hits: Iterable[dict[str, Any]],
    ) -> AgentState:
        by_digest: dict[str, RouteDecision] = {}
        evidence: dict[str, list[str]] = {}
        seen: set[tuple[str, str]] = set()
        total_ids: set[str] = set()
        for position, hit in enumerate(hits or []):
            if not isinstance(hit, Mapping):
                continue
            email_id = str(hit.get("id") or hit.get("email_id") or position)
            total_ids.add(email_id)
            decision = self._decision_from_hit(hit)
            if decision is None:
                continue
            action = Decision(route=decision.route, params=decision.params)
            fingerprint = compute_action_fingerprint(action)
            if (email_id, fingerprint) in seen:
                continue
            seen.add((email_id, fingerprint))
            by_digest[fingerprint] = decision
            evidence.setdefault(fingerprint, []).append(email_id)
        denominator = max(1, len(total_ids))
        eligible = {
            fingerprint: ids
            for fingerprint, ids in evidence.items()
            if len(ids) >= TIER2_MIN_HITS and len(ids) / denominator >= TIER2_MIN_RATIO
        }
        if not eligible:
            return {}
        if len(eligible) > 1:
            conflict = RouteDecision(
                outcome=DecisionOutcome.CONFLICT,
                route=CanonicalRoute.MANUAL_REVIEW,
                params={"reason_code": "tier2_conflict"},
                provenance=RouteProvenance(
                    tier=RouteTier.TIER2,
                    source_version="routing-label-v1",
                    evidence_ids=sorted({item for ids in eligible.values() for item in ids})[:16],
                ),
                reason_code="tier2_conflict",
                candidate_actions=[{"fingerprint": fp, "evidence_ids": ids} for fp, ids in eligible.items()],
            )
            return _decision_delta(conflict)
        fingerprint, ids = next(iter(eligible.items()))
        historical = by_digest[fingerprint]
        decision = RouteDecision(
            outcome=DecisionOutcome.MATCHED,
            route=historical.route,
            params=historical.params,
            provenance=RouteProvenance(
                tier=RouteTier.TIER2,
                source_version="routing-label-v1",
                evidence_ids=ids[:16],
                confidence=len(ids) / denominator,
            ),
            reason_code="historical_consensus",
            selected_action_fingerprint=fingerprint,
        )
        return _decision_delta(decision)

    async def apply_tier3_fallback(self, state: AgentState) -> AgentState:
        email = state.get("email") or {}
        prompt = (
            "Classify this email into exactly one route. Return strict JSON with keys "
            "route, params, confidence, reason_code. Routes: reply, forward, read_only, "
            "no_action, manual_review. Forward params require fixed_recipients. "
            f"Subject: {str(email.get('subject') or '')[:500]}\n"
            f"Sender: {_address(email.get('sender'))[:320]}\n"
            f"Body: {str(email.get('body') or '')[:2000]}"
        )
        try:
            settings = get_settings()
            enforce_model_input_budget(
                "router",
                prompt,
                budget=token_budget_from_settings(settings),
            )
            from src.providers.factory import get_llm_for_role

            response = await get_llm_for_role("router", temperature=0).ainvoke(prompt)
            content = getattr(response, "content", None)
            if not isinstance(content, str):
                raise ValueError("router_schema_invalid")
            parsed = _Tier3Result.model_validate(json.loads(content))
            Decision(route=parsed.route, params=parsed.params)
            decision = RouteDecision(
                outcome=DecisionOutcome.MATCHED,
                route=parsed.route,
                params=parsed.params,
                provenance=RouteProvenance(
                    tier=RouteTier.TIER3,
                    source_version="router-model-v1",
                    confidence=parsed.confidence,
                ),
                reason_code=parsed.reason_code,
            )
        except Exception as exc:
            logger.error("Tier 3 routing failed: error_type=%s", type(exc).__name__)
            decision = RouteDecision(
                outcome=DecisionOutcome.ERROR,
                route=CanonicalRoute.MANUAL_REVIEW,
                params={"reason_code": "router_model_failed"},
                provenance=RouteProvenance(
                    tier=RouteTier.TIER3,
                    source_version="router-model-v1",
                ),
                reason_code="router_model_failed",
            )
        return _decision_delta(decision)

    def dry_run(self, subject: str, sender: str, body: str = "") -> dict[str, Any]:
        decision_time = datetime.now(UTC)
        tier1 = build_tier1_decision(
            [compiled.manifest for compiled in self.artifact.rules],
            _email_view({"subject": subject, "sender": sender, "body": body}),
            me_email=self.me_email or None,
            decision_time=decision_time,
        )
        return {
            "artifact_digest": self.artifact.digest,
            "outcome": tier1.outcome.value,
            "route": tier1.route.value if tier1.route else None,
            "matched_rule_ids": [item.rule_id for item in tier1.matched_rules],
        }


_routing_engine: RoutingEngine | None = None


def configure_routing_engine(engine: RoutingEngine) -> None:
    global _routing_engine
    if _routing_engine is not None:
        raise RuntimeError("routing_engine_already_configured")
    _routing_engine = engine


def get_routing_engine() -> RoutingEngine:
    global _routing_engine
    if _routing_engine is None:
        _routing_engine = RoutingEngine()
    return _routing_engine


__all__ = [
    "RoutingEngine",
    "TIER2_MIN_HITS",
    "TIER2_MIN_RATIO",
    "configure_routing_engine",
    "get_routing_engine",
]
