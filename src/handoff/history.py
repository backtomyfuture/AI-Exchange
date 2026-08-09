from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping

from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
from src.router.tier1.fingerprint import compute_action_fingerprint
from src.router.tier1.schema import Decision


class HistoricalRouteConsensus:
    """Consensus from immutable historical ``route_decision`` labels only."""

    def __init__(self, *, min_hits: int = 2, min_ratio: float = 0.5) -> None:
        self.min_hits = min_hits
        self.min_ratio = min_ratio

    def decide(self, hits: Iterable[Mapping[str, object]]) -> RouteDecision | None:
        evidence_votes: dict[
            str,
            dict[tuple[int, str], RouteDecision],
        ] = defaultdict(dict)
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
            evidence_id = raw_evidence_id
            if evidence_id not in seen_evidence:
                seen_evidence.add(evidence_id)
                evidence_order.append(evidence_id)
            raw = hit.get("route_decision")
            payload = hit.get("payload")
            if raw is None and isinstance(payload, Mapping):
                raw = payload.get("route_decision")
            try:
                decision = RouteDecision.model_validate(raw)
            except Exception:
                continue
            if decision.outcome is not DecisionOutcome.MATCHED or decision.route is None:
                continue
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
            # A single historical email is one vote. Conflicting duplicate
            # labels for that identity are corrupt evidence, not two votes.
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
                evidence_ids=[row[0] for row in rows[:16]],
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
