"""Detect silent-route share drifting away from a 7-day baseline."""

from __future__ import annotations

from collections.abc import Iterable

from src.observability.metrics import record_silent_route_share


SILENT_STATUSES = frozenset({"no_action", "skipped", "notified_readonly"})
_MIN_TODAY_SILENT = 3
_MIN_BASELINE_TOTAL = 10
_SHARE_RATIO = 1.5
_SHARE_DELTA = 0.15


def silent_share(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return max(0.0, min(1.0, count / total))


def silent_share_alert(*, today_silent: int, today_total: int, baseline_silent: int, baseline_total: int) -> bool:
    if today_silent < _MIN_TODAY_SILENT or today_total <= 0:
        return False
    if baseline_total < _MIN_BASELINE_TOTAL:
        return False
    today = silent_share(today_silent, today_total)
    baseline = silent_share(baseline_silent, baseline_total)
    return today > max(baseline * _SHARE_RATIO, baseline + _SHARE_DELTA)


def count_silent(statuses: Iterable[object]) -> tuple[int, int]:
    total = 0
    silent = 0
    for status in statuses:
        if not isinstance(status, str) or not status.strip():
            continue
        total += 1
        if status.strip().lower() in SILENT_STATUSES:
            silent += 1
    return silent, total


def publish_silent_share(route: str, share: float) -> None:
    record_silent_route_share(route, share)


__all__ = [
    "SILENT_STATUSES",
    "count_silent",
    "publish_silent_share",
    "silent_share",
    "silent_share_alert",
]
