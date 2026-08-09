"""Deterministic validation for the immutable, human-approved send envelope."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.router.decision import RouteDecision
from src.router.tier1.schema import CanonicalRoute

_EMAIL = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


class ApprovedExecutionEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    inbox_id: str = Field(pattern=r"^[0-9a-f-]{36}$")
    account_id: int = Field(gt=0)
    email_id: str = Field(min_length=1, max_length=1_024)
    payload_revision: int = Field(gt=0)
    payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    route_decision: RouteDecision
    decision_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    draft_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    draft_content: str = Field(min_length=1, max_length=1_000_000)
    draft_ref: dict[str, Any] | None = None
    to: tuple[str, ...] = Field(min_length=1, max_length=100)
    cc: tuple[str, ...] = Field(default=(), max_length=100)
    attachment_refs: tuple[Any, ...] = Field(default=(), max_length=100)
    attachment_digests: tuple[str, ...] = Field(default=(), max_length=100)
    external_recipient_acknowledged: bool
    approver: str = Field(min_length=1, max_length=512)
    approved_at: datetime

    @field_validator("to", "cc")
    @classmethod
    def _resolved_addresses_only(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not _EMAIL.fullmatch(value) for value in values):
            raise ValueError("execution envelope contains unresolved recipient")
        if len(values) != len(set(value.casefold() for value in values)):
            raise ValueError("execution envelope contains duplicate recipient")
        return values

    @field_validator("attachment_digests")
    @classmethod
    def _attachment_digests_are_sha256(
        cls, values: tuple[str, ...]
    ) -> tuple[str, ...]:
        if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in values):
            raise ValueError("invalid attachment digest")
        return values

    @model_validator(mode="after")
    def _authority_is_self_consistent(self) -> "ApprovedExecutionEnvelope":
        if self.route_decision.route not in {
            CanonicalRoute.REPLY,
            CanonicalRoute.FORWARD,
        }:
            raise ValueError("approved_envelope_requires_writing_route")
        if not self.route_decision.handoff_profile_id:
            raise ValueError("approved_envelope_requires_handoff_profile")
        if self.route_decision.canonical_digest() != self.decision_digest:
            raise ValueError("decision_digest_mismatch")
        if hashlib.sha256(self.draft_content.encode("utf-8")).hexdigest() != self.draft_digest:
            raise ValueError("draft_digest_mismatch")
        canonical_payload = {
            "decision_digest": self.decision_digest,
            "plan_digest": self.plan_digest,
            "evidence_digest": self.evidence_digest,
            "draft_digest": self.draft_digest,
            "draft_content": self.draft_content,
            "draft_ref": self.draft_ref,
            "to": list(self.to),
            "cc": list(self.cc),
            "attachment_refs": list(self.attachment_refs),
            "attachment_digests": list(self.attachment_digests),
            "external_recipient_acknowledged": self.external_recipient_acknowledged,
        }
        payload_digest = hashlib.sha256(
            json.dumps(
                canonical_payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        if payload_digest != self.payload_digest:
            raise ValueError("payload_digest_mismatch")
        if len(self.attachment_refs) != len(self.attachment_digests):
            raise ValueError("attachment_manifest_mismatch")
        if self.route_decision.params.get("include_attachments", False):
            raise ValueError("unbound_forward_attachments")
        if not self.external_recipient_acknowledged:
            raise ValueError("recipient_acknowledgement_missing")
        if set(value.casefold() for value in self.to) & set(
            value.casefold() for value in self.cc
        ):
            raise ValueError("recipient_sets_overlap")
        return self

    def canonical_digest(self) -> str:
        encoded = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class ExecutionGate:
    """Validate the approved envelope and its append-only storage digest."""

    def validate(
        self,
        raw: object,
        *,
        expected_envelope_digest: str | None = None,
    ) -> ApprovedExecutionEnvelope:
        envelope = ApprovedExecutionEnvelope.model_validate(raw)
        if (
            expected_envelope_digest is not None
            and envelope.canonical_digest() != expected_envelope_digest
        ):
            raise ValueError("envelope_digest_mismatch")
        return envelope


def evaluate_execution_gate(raw: object) -> ApprovedExecutionEnvelope:
    """Return a validated frozen envelope or fail before any send claim."""
    return ExecutionGate().validate(raw)


__all__ = ["ApprovedExecutionEnvelope", "ExecutionGate", "evaluate_execution_gate"]
