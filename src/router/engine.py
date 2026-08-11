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
from src.router.tier1.schema import CanonicalRoute, Decision
from src.handoff.history import HistoricalRouteConsensus
from src.safety.model_budget import enforce_model_input_budget, token_budget_from_settings


logger = logging.getLogger(__name__)

TIER2_MIN_HITS = 2
TIER2_MIN_RATIO = 0.5
_MAILBOX_ADDRESS = re.compile(r"email_address='([^']+)'", re.IGNORECASE)
_JSON_CODE_FENCE = re.compile(
    r"```(?:json)?\s*(.*?)\s*```",
    re.IGNORECASE | re.DOTALL,
)


class _Tier3Result(BaseModel):
    model_config = ConfigDict(extra="forbid")

    route: CanonicalRoute
    params: dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(ge=0.0, le=1.0)
    reason_code: str = Field(min_length=1, max_length=128)


def _parse_tier3_json(content: str) -> Any:
    normalized = content.strip()
    fenced = _JSON_CODE_FENCE.fullmatch(normalized)
    if fenced:
        normalized = fenced.group(1).strip()
    return json.loads(normalized)


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


def classification_for_route(decision: RouteDecision) -> dict[str, Any]:
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
        "classification": classification_for_route(decision),
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
        """Evaluate Tier 1 only; abstention deliberately emits no RouteDecision."""
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
            return {
                **state,
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
            handoff_profile_id = selected.manifest.decision.handoff_profile_id
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
            handoff_profile_id = None
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
            handoff_profile_id=handoff_profile_id,
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
        decision = HistoricalRouteConsensus(
            min_hits=TIER2_MIN_HITS,
            min_ratio=TIER2_MIN_RATIO,
        ).decide(hit for hit in (hits or []) if isinstance(hit, Mapping))
        return {} if decision is None else _decision_delta(decision)

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
            parsed = _Tier3Result.model_validate(_parse_tier3_json(content))
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
                handoff_profile_id=(
                    "generic_reply_v1"
                    if parsed.route is CanonicalRoute.REPLY
                    else "generic_forward_v1"
                    if parsed.route is CanonicalRoute.FORWARD
                    else None
                ),
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

    async def resolve_route(
        self,
        state: AgentState,
        hits: Iterable[dict[str, Any]],
    ) -> RouteDecision:
        """Run the cascade and return exactly one final decision."""

        tier1_state = await self.execute_router(state)
        raw = tier1_state.get("route_decision")
        if raw is not None:
            return self.with_default_handoff_profile(RouteDecision.model_validate(raw))
        tier2_delta = await self.apply_tier2_hits(state, hits)
        raw = tier2_delta.get("route_decision")
        if raw is not None:
            return self.with_default_handoff_profile(RouteDecision.model_validate(raw))
        tier3_delta = await self.apply_tier3_fallback(state)
        return self.with_default_handoff_profile(
            RouteDecision.model_validate(tier3_delta.get("route_decision"))
        )

    @staticmethod
    def with_default_handoff_profile(decision: RouteDecision) -> RouteDecision:
        if decision.handoff_profile_id is not None:
            return decision
        profile_id = (
            "generic_reply_v1"
            if decision.route is CanonicalRoute.REPLY
            else "generic_forward_v1"
            if decision.route is CanonicalRoute.FORWARD
            else None
        )
        if profile_id is None:
            return decision
        return decision.model_copy(update={"handoff_profile_id": profile_id})

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
    "classification_for_route",
    "get_routing_engine",
]
