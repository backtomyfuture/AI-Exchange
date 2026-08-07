"""Aggregates per-rule evaluation into one immutable ``tier1_decision`` (design
doc §2). Deliberately excludes ``handoff_execution`` (the mutable downstream
execution-state machine owned by drafter/card/approval/send code, not part of
Tier 1) — see the design doc §2.2 for why the two must not be merged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from src.router.tier1.dsl import EmailView, RuleEvalStatus, evaluate_match
from src.router.tier1.fingerprint import compute_action_fingerprint
from src.router.tier1.schema import CanonicalRoute, RuleManifest


class EvaluationOutcome(str, Enum):
    MATCHED = "matched"
    ABSTAIN = "abstain"
    CONFLICT = "conflict"
    ERROR = "error"


class DecisionOrigin(str, Enum):
    RULE_DECLARED = "rule_declared"
    RUNTIME_CONFLICT = "runtime_conflict"
    RUNTIME_INDETERMINATE = "runtime_indeterminate"
    RUNTIME_ERROR = "runtime_error"


@dataclass(frozen=True)
class MatchedRuleRef:
    rule_id: str
    rule_version: int


@dataclass(frozen=True)
class CandidateAction:
    fingerprint: str
    rule_ids: List[str]
    route: CanonicalRoute


@dataclass(frozen=True)
class Tier1Decision:
    """Immutable Tier 1 output for one email. Persist before creating any handoff."""

    outcome: EvaluationOutcome
    route: Optional[CanonicalRoute]
    decision_origin: Optional[DecisionOrigin]
    matched_rules: List[MatchedRuleRef] = field(default_factory=list)
    candidate_actions: List[CandidateAction] = field(default_factory=list)
    selected_action_fingerprint: Optional[str] = None
    business_flow_ids: List[str] = field(default_factory=list)
    reason_codes: List[str] = field(default_factory=list)


def evaluate_rule(rule: RuleManifest, view: EmailView, *, me_email: Optional[str] = None) -> RuleEvalStatus:
    return evaluate_match(rule.match.anchor, rule.match.conditions, view, me_email=me_email)


def build_tier1_decision(
    rules: List[RuleManifest], view: EmailView, *, me_email: Optional[str] = None
) -> Tier1Decision:
    """Evaluate every rule in ``rules`` against ``view`` and aggregate per the
    invariants table in design doc §2.2.

    Callers must pass only the currently activated (already atomically
    compiled) ``enabled`` ruleset — this function performs no lifecycle or
    activation filtering of its own. ``me_email`` resolves any ``$ME``
    placeholder referenced by a rule's anchor/conditions (design doc's
    identity-dependent rules, e.g. "I am a direct recipient").
    """
    matched: List[RuleManifest] = []
    indeterminate: List[RuleManifest] = []
    for rule in rules:
        status = evaluate_rule(rule, view, me_email=me_email)
        if status is RuleEvalStatus.MATCHED:
            matched.append(rule)
        elif status is RuleEvalStatus.INDETERMINATE:
            indeterminate.append(rule)

    if indeterminate:
        return Tier1Decision(
            outcome=EvaluationOutcome.ERROR,
            route=CanonicalRoute.MANUAL_REVIEW,
            decision_origin=DecisionOrigin.RUNTIME_INDETERMINATE,
            matched_rules=[MatchedRuleRef(r.rule_id, r.rule_version) for r in indeterminate],
        )

    if not matched:
        return Tier1Decision(outcome=EvaluationOutcome.ABSTAIN, route=None, decision_origin=None)

    groups: Dict[str, List[RuleManifest]] = {}
    for rule in matched:
        fingerprint = compute_action_fingerprint(rule.decision)
        groups.setdefault(fingerprint, []).append(rule)

    matched_refs = [MatchedRuleRef(r.rule_id, r.rule_version) for r in matched]
    business_flow_ids = sorted({r.decision.business_flow_id for r in matched if r.decision.business_flow_id})
    reason_codes = sorted(
        {
            r.decision.typed_params.reason_code
            for r in matched
            if hasattr(r.decision.typed_params, "reason_code")
        }
    )

    if len(groups) == 1:
        (fingerprint, group_rules), = groups.items()
        route = group_rules[0].decision.route
        return Tier1Decision(
            outcome=EvaluationOutcome.MATCHED,
            route=route,
            decision_origin=DecisionOrigin.RULE_DECLARED,
            matched_rules=matched_refs,
            candidate_actions=[
                CandidateAction(fingerprint, sorted(r.rule_id for r in group_rules), route)
            ],
            selected_action_fingerprint=fingerprint,
            business_flow_ids=business_flow_ids,
            reason_codes=reason_codes,
        )

    candidate_actions = sorted(
        (
            CandidateAction(fp, sorted(r.rule_id for r in group_rules), group_rules[0].decision.route)
            for fp, group_rules in groups.items()
        ),
        key=lambda c: c.fingerprint,
    )
    return Tier1Decision(
        outcome=EvaluationOutcome.CONFLICT,
        route=CanonicalRoute.MANUAL_REVIEW,
        decision_origin=DecisionOrigin.RUNTIME_CONFLICT,
        matched_rules=matched_refs,
        candidate_actions=candidate_actions,
        selected_action_fingerprint=None,
        business_flow_ids=business_flow_ids,
        reason_codes=reason_codes,
    )
