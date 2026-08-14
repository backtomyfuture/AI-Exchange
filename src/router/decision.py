"""Canonical, immutable routing decision shared by Tier 1, Tier 2 and Tier 3."""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.router.tier1.schema import CanonicalRoute, Decision


class DecisionOutcome(StrEnum):
    MATCHED = "matched"
    ABSTAIN = "abstain"
    CONFLICT = "conflict"
    ERROR = "error"


class RouteTier(StrEnum):
    TIER1 = "tier1"
    TIER2 = "tier2"
    TIER3 = "tier3"
    SYSTEM = "system"
    HISTORICAL_INFERRED = "historical_inferred"


class RouteProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tier: RouteTier
    source_version: str = Field(min_length=1, max_length=128)
    artifact_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    rule_ids: list[str] = Field(default_factory=list, max_length=32)
    evidence_ids: list[str] = Field(default_factory=list, max_length=16)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _tier1_requires_artifact(self) -> "RouteProvenance":
        if self.tier is RouteTier.TIER1 and self.artifact_digest is None:
            raise ValueError("tier1 provenance requires artifact_digest")
        if self.tier is not RouteTier.TIER1 and self.artifact_digest is not None:
            raise ValueError("only tier1 provenance may contain artifact_digest")
        return self


class RouteDecision(BaseModel):
    """The final routing value downstream callers may treat as authoritative.

    Tier abstention is an evaluation result, not a route.  It must never enter
    this model or the durable decision table.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: DecisionOutcome
    route: CanonicalRoute | None
    params: dict[str, Any] = Field(default_factory=dict)
    provenance: RouteProvenance
    reason_code: str | None = Field(default=None, max_length=128)
    selected_action_fingerprint: str | None = Field(
        default=None,
        pattern=r"^sha256:(?:v2:)?[0-9a-f]{64}$",
    )
    handoff_profile_id: str | None = Field(
        default=None,
        pattern=r"^[a-z][a-z0-9_]*_v[1-9][0-9]*$",
    )
    candidate_actions: list[dict[str, Any]] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def _validate_route_and_params(self) -> "RouteDecision":
        if self.outcome is DecisionOutcome.ABSTAIN:
            raise ValueError("abstain is not a final route decision")
        if self.route is None:
            raise ValueError("non-abstain decision requires route")
        Decision(route=self.route, params=self.params)
        if self.outcome in {DecisionOutcome.CONFLICT, DecisionOutcome.ERROR}:
            if self.route is not CanonicalRoute.MANUAL_REVIEW:
                raise ValueError("conflict/error decisions require manual_review")
            if not self.reason_code:
                raise ValueError("conflict/error decisions require reason_code")
        # Profile-less writing decisions remain readable as v1 historical
        # labels. New finalization always expands them to a generic profile.
        if self.route not in {CanonicalRoute.REPLY, CanonicalRoute.FORWARD} and self.handoff_profile_id is not None:
            raise ValueError("non-writing routes cannot contain handoff_profile_id")
        return self

    def canonical_digest(self) -> str:
        payload = self.model_dump(mode="json")
        encoded = json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "DecisionOutcome",
    "RouteDecision",
    "RouteProvenance",
    "RouteTier",
]
