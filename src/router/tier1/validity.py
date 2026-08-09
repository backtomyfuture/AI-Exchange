"""Runtime eligibility for an activated Tier 1 rule."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Optional

from src.router.tier1.schema import RuleManifest


def parse_validity_timestamp(
    value: Optional[str],
    *,
    field_name: str,
) -> Optional[datetime]:
    """Parse a manifest timestamp into aware UTC, failing with a stable code."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ValueError(f"{field_name}_invalid") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def is_rule_active(rule: RuleManifest, decision_time: datetime) -> bool:
    """Return whether ``rule`` is eligible at one routing decision time.

    Compilation and artifact loading validate the manifest before activation;
    this runtime check is still required because a long-lived process can cross
    ``effective_from`` or ``expires_at`` without reloading its immutable
    artifact. Invalid timestamps fail closed defensively.
    """
    try:
        effective_from = parse_validity_timestamp(
            rule.validity.effective_from,
            field_name="effective_from",
        )
        expires_at = parse_validity_timestamp(
            rule.validity.expires_at,
            field_name="expires_at",
        )
    except ValueError:
        return False

    current = _as_utc(decision_time)
    return (
        (effective_from is None or effective_from <= current)
        and (expires_at is None or current < expires_at)
    )


__all__ = ["is_rule_active", "parse_validity_timestamp"]
