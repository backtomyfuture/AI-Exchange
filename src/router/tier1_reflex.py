"""Deterministic matching for declarative Tier 1 skill triggers.

The pure helpers in this module are deliberately shared by the runtime router
and historical-candidate replay.  A candidate must therefore be validated with
the exact same matching semantics it will use after a planned restart.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any

from src.router.manager import get_skill_manager

logger = logging.getLogger(__name__)


TEXT_CONDITION_TYPES = frozenset({"sender_match", "subject_match", "body_match"})
RECIPIENT_CONDITION_TYPES = frozenset({"to_match", "cc_match"})
SUPPORTED_CONDITION_TYPES = TEXT_CONDITION_TYPES | RECIPIENT_CONDITION_TYPES
SUPPORTED_CONDITION_OPERATORS = frozenset({"eq", "contains", "regex", "in"})


def _as_string_list(value: Any) -> list[str]:
    """Coerce a recipient collection into usable strings without raising."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if isinstance(item, str)]
    return []


def _needs_me_resolution(value: Any) -> bool:
    if isinstance(value, str):
        return "$ME" in value
    if isinstance(value, list):
        return any(isinstance(item, str) and "$ME" in item for item in value)
    return False


def _resolve_me_placeholder(value: Any, me_email: str | None) -> Any:
    """Resolve ``$ME`` lazily so pure replay never needs application settings."""
    if not _needs_me_resolution(value):
        return value
    if me_email is None:
        from src.config import get_settings

        me_email = get_settings().EXCHANGE_ACCOUNT_EMAIL
    replacement = str(me_email or "")
    if isinstance(value, str):
        return value.replace("$ME", replacement)
    if isinstance(value, list):
        return [
            item.replace("$ME", replacement) if isinstance(item, str) else item
            for item in value
        ]
    return value


def _match_text(target: str, operator: str, value: Any) -> bool:
    if operator == "eq":
        return isinstance(value, str) and target == value
    if operator == "contains":
        return isinstance(value, str) and value.lower() in target.lower()
    if operator == "regex":
        if not isinstance(value, str):
            return False
        try:
            return bool(re.search(value, target, re.IGNORECASE))
        except re.error:
            return False
    if operator == "in":
        values = _as_string_list(value)
        return target.lower() in {item.lower() for item in values}
    return False


def _match_recipients(recipients: list[str], operator: str, value: Any) -> bool:
    if operator == "eq":
        return isinstance(value, str) and any(
            value.lower() == recipient.lower() for recipient in recipients
        )
    if operator == "contains":
        return isinstance(value, str) and any(
            value.lower() in recipient.lower() for recipient in recipients
        )
    if operator == "regex":
        if not isinstance(value, str):
            return False
        try:
            return any(re.search(value, recipient, re.IGNORECASE) for recipient in recipients)
        except re.error:
            return False
    if operator == "in":
        values = {item.lower() for item in _as_string_list(value)}
        return any(recipient.lower() in values for recipient in recipients)
    return False


def condition_matches_email(
    email: Mapping[str, Any],
    condition: Mapping[str, Any],
    *,
    me_email: str | None = None,
) -> bool:
    """Return whether one supported declarative condition matches ``email``.

    Invalid or unsupported conditions intentionally evaluate to ``False``.  The
    promotion path rejects them before writing a rule, while this behaviour keeps
    a malformed legacy manifest from taking down routing for an inbound email.
    """
    condition_type = condition.get("type")
    operator = condition.get("operator", "contains")
    if condition_type not in SUPPORTED_CONDITION_TYPES:
        return False
    if operator not in SUPPORTED_CONDITION_OPERATORS:
        return False

    value = _resolve_me_placeholder(condition.get("value"), me_email)
    if condition_type == "sender_match":
        return _match_text(str(email.get("sender") or ""), operator, value)
    if condition_type == "subject_match":
        return _match_text(str(email.get("subject") or ""), operator, value)
    if condition_type == "body_match":
        return _match_text(str(email.get("body") or ""), operator, value)
    if condition_type == "to_match":
        return _match_recipients(_as_string_list(email.get("to") or []), operator, value)
    if condition_type == "cc_match":
        return _match_recipients(_as_string_list(email.get("cc") or []), operator, value)
    return False


def conditions_match_email(
    email: Mapping[str, Any],
    conditions: Sequence[Mapping[str, Any]] | None,
    condition_logic: str = "and",
    *,
    me_email: str | None = None,
) -> bool:
    """Apply a complete Tier 1 trigger using runtime-equivalent semantics."""
    if not conditions:
        return False
    checks = [
        condition_matches_email(email, condition, me_email=me_email)
        for condition in conditions
        if isinstance(condition, Mapping)
    ]
    if not checks:
        return False
    if condition_logic == "or":
        return any(checks)
    return all(checks)


class Tier1ReflexRouter:
    """Tier 1 反射路由器：执行 Skill Manifest 中的声明式规则。"""

    def __init__(self):
        self.manager = get_skill_manager()

    def route(self, email: dict[str, Any]) -> list[str]:
        """根据邮件内容，返回匹配的 Skill ID 列表。"""
        matched_skills = []
        for trigger in self.manager.get_tier1_triggers():
            skill_id = trigger["skill_id"]
            if conditions_match_email(
                email,
                trigger.get("conditions"),
                trigger.get("condition_logic", "and"),
            ):
                logger.info("Tier 1 Match found: %s", skill_id)
                matched_skills.append(skill_id)
        return matched_skills

    def _check_condition(
        self,
        cond: dict[str, Any],
        subject: str,
        body: str,
        sender: str,
        to_list: list[str],
        cc_list: list[str] | None = None,
    ) -> bool:
        """Compatibility wrapper retained for callers of the old private seam."""
        return condition_matches_email(
            {
                "subject": subject,
                "body": body,
                "sender": sender,
                "to": to_list,
                "cc": cc_list or [],
            },
            cond,
        )
