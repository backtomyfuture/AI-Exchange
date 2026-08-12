"""Bounded context shared by Historical Route Consensus and Tier 3."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from src.router.decision import DecisionOutcome, RouteDecision
from src.utils.email_body_projection import project_email_body_for_model

_MAILBOX_ADDRESS = re.compile(r"email_address='([^']+)'", re.IGNORECASE)
_ADDRESS_KEYS = ("email_address", "email", "address", "value")
_MAX_EVIDENCE_ITEMS = 5
_MAX_EVIDENCE_SUBJECT_CHARS = 240
_MAX_EVIDENCE_BODY_CHARS = 900
_MAX_EVIDENCE_ADDRESS_CHARS = 320
_MAX_EVIDENCE_ID_CHARS = 96


def normalize_address(value: object) -> str:
    """Normalize Exchange strings and mapping-shaped mailbox values."""
    if isinstance(value, Mapping):
        for key in _ADDRESS_KEYS:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return normalize_address(candidate)
        for key in ("mailbox", "mailbox_address", "mailboxAddress"):
            if key in value:
                return normalize_address(value[key])
    else:
        for key in _ADDRESS_KEYS:
            candidate = getattr(value, key, None)
            if isinstance(candidate, str) and candidate.strip():
                return normalize_address(candidate)

    text = str(value or "").strip()
    match = _MAILBOX_ADDRESS.search(text)
    return (match.group(1) if match else text).strip()


def normalize_addresses(value: object) -> list[str]:
    if isinstance(value, str):
        address = normalize_address(value)
        return [address] if address else []
    if isinstance(value, Mapping):
        address = normalize_address(value)
        return [address] if address else []
    if not isinstance(value, Iterable) or isinstance(value, (bytes, bytearray)):
        return []
    return [
        address
        for item in value
        if (address := normalize_address(item))
    ]


@dataclass(frozen=True, slots=True)
class RecipientRelation:
    """The current mailbox owner's relation to the current message."""

    owner_email: str | None
    to_addresses: tuple[str, ...]
    cc_addresses: tuple[str, ...]
    owner_in_to: bool | None
    owner_in_cc: bool | None
    sender_is_owner: bool | None
    relation: Literal[
        "direct_to",
        "cc_only",
        "other_recipient",
        "unknown",
    ]

    @classmethod
    def from_email(
        cls,
        email: Mapping[str, Any],
        *,
        owner_email: str | None,
    ) -> "RecipientRelation":
        owner = normalize_address(owner_email).casefold() or None
        to_value = email.get("to") or email.get("to_recipients")
        cc_value = email.get("cc") or email.get("cc_recipients")
        to_addresses = tuple(normalize_addresses(to_value))
        cc_addresses = tuple(normalize_addresses(cc_value))
        to_known = "to" in email or "to_recipients" in email
        cc_known = "cc" in email or "cc_recipients" in email
        if not owner or not to_known or not cc_known:
            owner_in_to = owner_in_cc = None
            relation = "unknown"
        else:
            normalized_to = {address.casefold() for address in to_addresses}
            normalized_cc = {address.casefold() for address in cc_addresses}
            owner_in_to = owner.casefold() in normalized_to
            owner_in_cc = owner.casefold() in normalized_cc
            relation = (
                "direct_to"
                if owner_in_to
                else "cc_only"
                if owner_in_cc
                else "other_recipient"
            )
        sender = normalize_address(email.get("sender")).casefold()
        sender_is_owner = None if not owner else sender == owner
        return cls(
            owner_email=owner,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
            owner_in_to=owner_in_to,
            owner_in_cc=owner_in_cc,
            sender_is_owner=sender_is_owner,
            relation=relation,
        )

    def render(self) -> str:
        def addresses(values: tuple[str, ...]) -> str:
            return ", ".join(values)[:_MAX_EVIDENCE_ADDRESS_CHARS] or "(none)"

        return "\n".join(
            (
                "Recipient semantics:",
                f"- mailbox_owner_relation: {self.relation}",
                f"- owner_in_to: {self.owner_in_to}",
                f"- owner_in_cc: {self.owner_in_cc}",
                f"- sender_is_mailbox_owner: {self.sender_is_owner}",
                f"- To: {addresses(self.to_addresses)}",
                f"- Cc: {addresses(self.cc_addresses)}",
            )
        )


@dataclass(frozen=True, slots=True)
class RoutingEvidenceBundle:
    """One bounded retrieval result reused by Tier 2 and Tier 3."""

    hits: tuple[dict[str, Any], ...] = ()
    status: Literal["available", "partial", "unavailable"] = "available"

    @classmethod
    def from_hits(
        cls,
        hits: Iterable[Mapping[str, Any]] | None,
        *,
        status: Literal["available", "partial", "unavailable"] = "available",
    ) -> "RoutingEvidenceBundle":
        bounded = tuple(
            dict(hit)
            for hit in (hits or ())
            if isinstance(hit, Mapping)
        )[:_MAX_EVIDENCE_ITEMS]
        return cls(hits=bounded, status=status)

    @classmethod
    def unavailable(cls) -> "RoutingEvidenceBundle":
        return cls(status="unavailable")

    def __iter__(self):
        return iter(self.hits)

    def __len__(self) -> int:
        return len(self.hits)


@dataclass(frozen=True, slots=True)
class RoutingAssessment:
    """Advisory Tier 1/Tier 2 evidence supplied to the Tier 3 fallback."""

    recipient_relation: RecipientRelation
    tier1_status: Literal["abstained"]
    tier2_status: Literal["no_consensus", "partial", "unavailable"]
    tier2_candidate_routes: tuple[tuple[str, int], ...]
    tier2_evidence_ids: tuple[str, ...]
    evidence: RoutingEvidenceBundle

    @classmethod
    def for_tier3(
        cls,
        email: Mapping[str, Any],
        *,
        owner_email: str | None,
        evidence: RoutingEvidenceBundle,
    ) -> "RoutingAssessment":
        candidate_counts: dict[str, int] = {}
        evidence_ids: list[str] = []
        seen_ids: set[str] = set()
        counted_ids: set[str] = set()
        for hit in evidence:
            raw_id = hit.get("id") or hit.get("email_id")
            evidence_id = (
                raw_id.strip()
                if isinstance(raw_id, str)
                else ""
            )
            if evidence_id and evidence_id not in seen_ids:
                seen_ids.add(evidence_id)
                evidence_ids.append(evidence_id[:_MAX_EVIDENCE_ID_CHARS])

            decision = _historical_decision(hit)
            if decision is None or not evidence_id or evidence_id in counted_ids:
                continue
            counted_ids.add(evidence_id)
            candidate = decision.route.value
            candidate_counts[candidate] = candidate_counts.get(candidate, 0) + 1

        candidates = tuple(
            sorted(candidate_counts.items(), key=lambda item: (-item[1], item[0]))
        )
        return cls(
            recipient_relation=RecipientRelation.from_email(
                email,
                owner_email=owner_email,
            ),
            tier1_status="abstained",
            tier2_status=(
                "no_consensus"
                if evidence.status == "available"
                else evidence.status
            ),
            tier2_candidate_routes=candidates,
            tier2_evidence_ids=tuple(evidence_ids[:16]),
            evidence=evidence,
        )

    def render(self) -> str:
        if self.tier2_candidate_routes:
            candidates = ", ".join(
                f"{route} ({count} vote{'s' if count != 1 else ''})"
                for route, count in self.tier2_candidate_routes
            )
        else:
            candidates = "(none)"
        evidence_lines = [
            "Historical RAG evidence (advisory context only):"
        ]
        if self.evidence.status == "unavailable":
            evidence_lines.append("- retrieval_status: unavailable")
        elif self.evidence.status == "partial":
            evidence_lines.append("- retrieval_status: partial")
            evidence_lines.append(
                "- some historical sources were unavailable; do not treat the "
                "retrieved set as exhaustive"
            )
        if not self.evidence.hits:
            evidence_lines.append("- no historical evidence was retrieved")
        else:
            evidence_lines.append(
                f"- retrieved_items: {len(self.evidence.hits)}"
            )
            for index, hit in enumerate(self.evidence.hits, start=1):
                evidence_lines.extend(
                    _render_evidence_item(
                        index,
                        hit,
                        owner_email=self.recipient_relation.owner_email,
                    )
                )
        return "\n".join(
            (
                f"Tier 1 status: {self.tier1_status}",
                f"Tier 2 status: {self.tier2_status}",
                f"Tier 2 candidate routes: {candidates}",
                (
                    "Tier 2 evidence ids: "
                    + ", ".join(self.tier2_evidence_ids[:16])
                    if self.tier2_evidence_ids
                    else "Tier 2 evidence ids: (none)"
                ),
                *evidence_lines,
            )
        )


def _historical_decision(hit: Mapping[str, Any]) -> RouteDecision | None:
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


def _render_evidence_item(
    index: int,
    hit: Mapping[str, Any],
    *,
    owner_email: str | None,
) -> list[str]:
    payload = hit.get("payload")
    source = payload if isinstance(payload, Mapping) else hit

    def text(key: str, limit: int) -> str:
        value = source.get(key, hit.get(key, ""))
        return str(value or "").strip().replace("\x00", "")[:limit]

    raw_body = source.get("body") or source.get("chunk_text") or source.get("snippet")
    body = project_email_body_for_model(raw_body).text[:_MAX_EVIDENCE_BODY_CHARS]
    sender = normalize_address(source.get("sender") or hit.get("sender"))
    direction = (
        "mailbox_owner_sent"
        if owner_email
        and sender.casefold() == owner_email.casefold()
        else "other_sender"
        if sender
        else "unknown"
    )
    timestamp = text("received_at", 64) or text("sent_at", 64) or text("date", 64)
    return [
        f"- Evidence {index}:",
        f"  - direction: {direction}",
        f"  - timestamp: {timestamp or '(unknown)'}",
        f"  - sender: {text('sender', _MAX_EVIDENCE_ADDRESS_CHARS) or '(unknown)'}",
        f"  - To: {text('to', _MAX_EVIDENCE_ADDRESS_CHARS) or '(unknown)'}",
        f"  - Cc: {text('cc', _MAX_EVIDENCE_ADDRESS_CHARS) or '(unknown)'}",
        f"  - subject: {text('subject', _MAX_EVIDENCE_SUBJECT_CHARS) or '(none)'}",
        f"  - body: {body or '(empty)'}",
    ]


__all__ = [
    "RecipientRelation",
    "RoutingAssessment",
    "RoutingEvidenceBundle",
    "normalize_address",
    "normalize_addresses",
]
