from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from email.utils import parseaddr

from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
from src.router.tier1.fingerprint import compute_action_fingerprint
from src.router.tier1.schema import Decision
from src.utils.mailbox_text import parse_serialized_mailbox


TIER2_EXCLUDED_TIERS = frozenset({RouteTier.TIER3, RouteTier.HISTORICAL_INFERRED})
_MAILBOX_ADDRESS = re.compile(r"email_address='([^']+)'", re.IGNORECASE)


def _parse_received_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_sender(value: object) -> str:
    mailbox = parse_serialized_mailbox(value)
    if mailbox is not None and mailbox.address:
        return mailbox.address.casefold()
    text = str(value or "").strip()
    match = _MAILBOX_ADDRESS.search(text)
    address = match.group(1) if match else parseaddr(text)[1] or text
    return address.strip().casefold()


def _vote_source_key(hit: Mapping[str, object], evidence_id: str) -> str:
    sender = _normalize_sender(hit.get("sender"))
    thread_id = str(hit.get("thread_id") or hit.get("conversation_id") or "").strip()
    if sender and thread_id:
        return f"{sender}|{thread_id}"
    if sender:
        return f"sender:{sender}"
    if thread_id:
        return f"thread:{thread_id}"
    return evidence_id


def _hit_score(hit: Mapping[str, object]) -> float | None:
    for key in ("score", "similarity"):
        raw = hit.get(key)
        if isinstance(raw, bool):
            continue
        if isinstance(raw, (int, float)):
            return float(raw)
    return None


class HistoricalRouteConsensus:
    """Consensus from immutable historical ``route_decision`` labels only."""

    def __init__(
        self,
        *,
        min_hits: int = 3,
        min_ratio: float = 0.67,
        min_score: float = 0.75,
        received_before: object = None,
    ) -> None:
        self.min_hits = min_hits
        self.min_ratio = min_ratio
        self.min_score = min_score
        self.received_before = _parse_received_at(received_before)

    def decide(self, hits: Iterable[Mapping[str, object]]) -> RouteDecision | None:
        evidence_votes: dict[
            str,
            dict[tuple[int, str], RouteDecision],
        ] = defaultdict(dict)
        evidence_ids_by_source: dict[str, str] = {}
        evidence_order: list[str] = []
        seen_evidence: set[str] = set()
        unidentified_hits = 0
        for hit in hits:
            raw_evidence_id = hit.get("id") or hit.get("email_id")
            if (
                type(raw_evidence_id) is not str
                or not raw_evidence_id.strip()
                or len(raw_evidence_id.encode("utf-8")) > 512
            ):
                unidentified_hits += 1
                continue
            raw = hit.get("route_decision")
            payload = hit.get("payload")
            if raw is None and isinstance(payload, Mapping):
                raw = payload.get("route_decision")
            try:
                decision = RouteDecision.model_validate(raw)
            except Exception:
                evidence_id = raw_evidence_id
                if evidence_id not in seen_evidence:
                    seen_evidence.add(evidence_id)
                    evidence_order.append(evidence_id)
                continue
            if decision.outcome is not DecisionOutcome.MATCHED or decision.route is None:
                evidence_id = raw_evidence_id
                if evidence_id not in seen_evidence:
                    seen_evidence.add(evidence_id)
                    evidence_order.append(evidence_id)
                continue
            if decision.provenance.tier in TIER2_EXCLUDED_TIERS:
                continue
            if hit.get("eligible_for_tier2") is False:
                continue
            score = _hit_score(hit)
            if score is not None and score < self.min_score:
                evidence_id = raw_evidence_id
                if evidence_id not in seen_evidence:
                    seen_evidence.add(evidence_id)
                    evidence_order.append(evidence_id)
                continue
            hit_received_at = _parse_received_at(hit.get("received_at"))
            if (
                self.received_before is not None
                and hit_received_at is not None
                and hit_received_at >= self.received_before
            ):
                continue
            evidence_id = _vote_source_key(hit, raw_evidence_id)
            if evidence_id not in seen_evidence:
                seen_evidence.add(evidence_id)
                evidence_order.append(evidence_id)
            version = 2 if decision.handoff_profile_id else 1
            action = Decision(
                route=decision.route,
                params=decision.params,
                handoff_profile_id=decision.handoff_profile_id,
            )
            fingerprint = compute_action_fingerprint(
                action,
                handoff_profile_id=decision.handoff_profile_id,
                fingerprint_version=version,
            )
            evidence_votes[evidence_id][(version, fingerprint)] = decision
            evidence_ids_by_source[evidence_id] = raw_evidence_id
        corrupt_evidence = [
            evidence_id
            for evidence_id in evidence_order
            if len(evidence_votes.get(evidence_id, {})) > 1
        ]
        if corrupt_evidence:
            candidate_actions = []
            for evidence_id in corrupt_evidence:
                for (_version, fingerprint) in sorted(evidence_votes[evidence_id]):
                    candidate_actions.append(
                        {
                            "fingerprint": fingerprint,
                            "evidence_ids": [evidence_id],
                        }
                    )
            return self._conflict(
                evidence_ids=corrupt_evidence,
                candidate_actions=candidate_actions,
            )
        groups: dict[tuple[int, str], list[tuple[str, RouteDecision]]] = defaultdict(list)
        for evidence_id in evidence_order:
            votes = evidence_votes.get(evidence_id, {})
            # A single historical email or folded source is one vote.
            # Conflicting duplicate labels for that identity are corrupt.
            if len(votes) != 1:
                continue
            key, decision = next(iter(votes.items()))
            groups[key].append((evidence_id, decision))
        denominator = max(1, len(evidence_order) + unidentified_hits)
        eligible = [
            (key, rows) for key, rows in groups.items()
            if len(rows) >= self.min_hits and len(rows) / denominator >= self.min_ratio
        ]
        if not eligible:
            return None
        if len(eligible) > 1:
            evidence_ids = list(
                dict.fromkeys(
                    evidence_id
                    for _key, rows in eligible
                    for evidence_id, _decision in rows
                )
            )
            return self._conflict(
                evidence_ids=evidence_ids,
                candidate_actions=[
                    {
                        "fingerprint": fingerprint,
                        "evidence_ids": [row[0] for row in rows[:16]],
                    }
                    for (_version, fingerprint), rows in eligible
                ],
            )
        (_, fingerprint), rows = eligible[0]
        historical = rows[0][1]
        return RouteDecision(
            outcome=DecisionOutcome.MATCHED,
            route=historical.route,
            params=historical.params,
            provenance=RouteProvenance(
                tier=RouteTier.TIER2,
                source_version="historical-route-consensus-v1",
                evidence_ids=[
                    evidence_ids_by_source.get(row[0], row[0]) for row in rows[:16]
                ],
                confidence=len(rows) / denominator,
            ),
            reason_code="historical_consensus",
            selected_action_fingerprint=fingerprint,
            handoff_profile_id=historical.handoff_profile_id,
        )

    @staticmethod
    def _conflict(
        *,
        evidence_ids: list[str],
        candidate_actions: list[dict[str, object]],
    ) -> RouteDecision:
        return RouteDecision(
            outcome=DecisionOutcome.CONFLICT,
            route="manual_review",
            params={"reason_code": "tier2_conflict"},
            provenance=RouteProvenance(
                tier=RouteTier.TIER2,
                source_version="historical-route-consensus-v1",
                evidence_ids=evidence_ids[:16],
            ),
            reason_code="tier2_conflict",
            candidate_actions=candidate_actions[:16],
        )


__all__ = ["HistoricalRouteConsensus"]
