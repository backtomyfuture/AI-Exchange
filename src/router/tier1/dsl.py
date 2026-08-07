"""Tier 1 v1 three-valued matcher (design doc §4.2, §4.4).

A missing field (e.g. no Cc) is EMPTY, not UNKNOWN: it participates in the
operator normally and usually yields ``FALSE``. Only a genuine resolution
failure (body projection error, address parse failure, ...) is UNKNOWN,
signalled by the :data:`UNKNOWN` sentinel on :class:`EmailView`. Callers that
build an ``EmailView`` from a real email should leave a field at its natural
empty value (``""``/``[]``) when it is simply absent, and set it to
:data:`UNKNOWN` only when resolution itself failed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, List, Optional, Union

from src.router.tier1.schema import AnchorGroup, ConditionNode


class TriState(str, Enum):
    TRUE = "TRUE"
    FALSE = "FALSE"
    UNKNOWN = "UNKNOWN"


class _UnknownType:
    """Sentinel: field resolution genuinely failed (parse/projection error)."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "UNKNOWN"

    def __bool__(self) -> bool:
        raise TypeError("UNKNOWN sentinel has no truthiness; compare with 'is UNKNOWN' instead")


UNKNOWN = _UnknownType()

ScalarField = Union[str, _UnknownType]
SetField = Union[List[str], _UnknownType]


@dataclass(frozen=True)
class EmailView:
    """Normalized, projected view of one email used for Tier 1 matching."""

    sender_address: ScalarField = ""
    to_addresses: SetField = field(default_factory=list)
    cc_addresses: SetField = field(default_factory=list)
    subject: ScalarField = ""
    body_current_text: ScalarField = ""
    body_full_text: ScalarField = ""


def normalize_text(value: str) -> str:
    return value.strip().casefold()


def normalize_address(address: str) -> str:
    return normalize_text(address)


_FIELD_MAP = {
    "sender.address": "sender_address",
    "to.addresses": "to_addresses",
    "cc.addresses": "cc_addresses",
    "subject": "subject",
    "body.current_text": "body_current_text",
    "body.full_text": "body_full_text",
}


def _resolve(view: EmailView, field_name: str) -> Union[str, List[str], _UnknownType]:
    attr = _FIELD_MAP.get(field_name)
    if attr is None:
        raise ValueError(f"unsupported field {field_name!r}")  # unreachable given schema validation
    return getattr(view, attr)


# ---------------------------------------------------------------------------
# $ME placeholder: an address anchor/condition value can be the literal string
# "$ME", resolved lazily against the caller-supplied ``me_email`` at match
# time (mirrors the legacy ``src/router/tier1_reflex.py`` behaviour). This
# keeps ``EmailView``/rule manifests portable and config-independent; only the
# evaluation entry points below accept ``me_email``. A rule that references
# "$ME" while no ``me_email`` was supplied resolves to UNKNOWN, not a silent
# False/True, so a misconfiguration surfaces as ``manual_review`` rather than
# a wrong route.
# ---------------------------------------------------------------------------

ME_PLACEHOLDER = "$ME"


def _resolve_me_value(value: str, me_email: Optional[str]) -> Union[str, _UnknownType]:
    if value != ME_PLACEHOLDER:
        return value
    return me_email if me_email else UNKNOWN


def _resolve_me_values(values: List[str], me_email: Optional[str]) -> Union[List[str], _UnknownType]:
    resolved: List[str] = []
    for item in values:
        if item != ME_PLACEHOLDER:
            resolved.append(item)
            continue
        if not me_email:
            return UNKNOWN
        resolved.append(me_email)
    return resolved


# ---------------------------------------------------------------------------
# regex safety (design §4.4): allowlist-flavoured denylist + length caps.
# ---------------------------------------------------------------------------

MAX_REGEX_PATTERN_LENGTH = 200
MAX_REGEX_INPUT_LENGTH = 20_000

_UNSAFE_REGEX_RE = re.compile(
    r"\(\?[<=!]"  # lookahead / lookbehind
    r"|\\[1-9]"  # backreference
    r"|\([^)]*[+*]\)[+*]"  # nested quantifier on a group, e.g. (a+)+ or (ab*)*
)


class UnsafeRegexError(ValueError):
    """Raised when a Tier 1 regex pattern falls outside the safety subset."""


def compile_safe_regex(pattern: str) -> "re.Pattern[str]":
    """Compile ``pattern`` against the Tier 1 regex safety subset.

    Rejects lookaround, backreferences, and the classic ``(a+)+`` nested-
    quantifier shape that causes catastrophic backtracking in the stdlib ``re``
    engine, and caps pattern length. This is a conservative denylist over a
    small allowed feature set, not a formal linear-time proof; a construct
    found unsafe later should be added here rather than relaxed away. There is
    no dependency on ``re2`` in this project (see design doc §4.4).
    """
    if len(pattern) > MAX_REGEX_PATTERN_LENGTH:
        raise UnsafeRegexError(f"regex pattern exceeds {MAX_REGEX_PATTERN_LENGTH} characters")
    if _UNSAFE_REGEX_RE.search(pattern):
        raise UnsafeRegexError(f"regex pattern uses a disallowed construct: {pattern!r}")
    try:
        return re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        raise UnsafeRegexError(f"regex pattern does not compile: {exc}") from exc


# ---------------------------------------------------------------------------
# operators
# ---------------------------------------------------------------------------


def _scalar_eq(resolved: Union[str, _UnknownType], value: str) -> TriState:
    if resolved is UNKNOWN:
        return TriState.UNKNOWN
    return TriState.TRUE if normalize_text(str(resolved)) == normalize_text(value) else TriState.FALSE


def _scalar_in(resolved: Union[str, _UnknownType], values: List[str]) -> TriState:
    if resolved is UNKNOWN:
        return TriState.UNKNOWN
    normalized = {normalize_text(v) for v in values}
    return TriState.TRUE if normalize_text(str(resolved)) in normalized else TriState.FALSE


def _scalar_contains(resolved: Union[str, _UnknownType], value: str) -> TriState:
    if resolved is UNKNOWN:
        return TriState.UNKNOWN
    return TriState.TRUE if value.casefold() in str(resolved).casefold() else TriState.FALSE


def _scalar_contains_any(resolved: Union[str, _UnknownType], values: List[str]) -> TriState:
    if resolved is UNKNOWN:
        return TriState.UNKNOWN
    haystack = str(resolved).casefold()
    return TriState.TRUE if any(v.casefold() in haystack for v in values) else TriState.FALSE


def _scalar_regex(resolved: Union[str, _UnknownType], pattern: str) -> TriState:
    if resolved is UNKNOWN:
        return TriState.UNKNOWN
    text = str(resolved)
    if len(text) > MAX_REGEX_INPUT_LENGTH:
        return TriState.UNKNOWN
    compiled = compile_safe_regex(pattern)
    return TriState.TRUE if compiled.search(text) is not None else TriState.FALSE


def _set_has_any(resolved: Union[List[str], _UnknownType], values: List[str]) -> TriState:
    if resolved is UNKNOWN:
        return TriState.UNKNOWN
    targets = {normalize_address(v) for v in values}
    items = {normalize_address(item) for item in resolved}
    return TriState.TRUE if targets & items else TriState.FALSE


def _set_has_all(resolved: Union[List[str], _UnknownType], values: List[str]) -> TriState:
    if resolved is UNKNOWN:
        return TriState.UNKNOWN
    targets = {normalize_address(v) for v in values}
    items = {normalize_address(item) for item in resolved}
    return TriState.TRUE if targets <= items else TriState.FALSE


def evaluate_leaf(leaf, view: EmailView, *, me_email: Optional[str] = None) -> TriState:
    """Evaluate one leaf (an :class:`AnchorCondition` or leaf :class:`ConditionNode`).

    ``leaf.value``/``leaf.values`` are resolved for the ``$ME`` placeholder
    against ``me_email`` before the operator runs. Dispatches on ``leaf.op``
    with a plain if/elif chain rather than a dict-of-callables: the latter is
    an unresolved-reflection call site as far as
    ``tests/contracts/test_exchange_sync_contract.py``'s static dormant-
    boundary scan is concerned, even though this module has nothing to do
    with that contract's ``ExchangeClient.sync_emails`` boundary.
    """
    resolved = _resolve(view, leaf.field)

    value = leaf.value
    if value is not None:
        value = _resolve_me_value(value, me_email)
        if value is UNKNOWN:
            return TriState.UNKNOWN

    values = leaf.values
    if values is not None:
        values = _resolve_me_values(values, me_email)
        if values is UNKNOWN:
            return TriState.UNKNOWN

    if leaf.op == "eq":
        return _scalar_eq(resolved, value)
    if leaf.op == "in":
        return _scalar_in(resolved, values)
    if leaf.op == "contains":
        return _scalar_contains(resolved, value)
    if leaf.op == "contains_any":
        return _scalar_contains_any(resolved, values)
    if leaf.op == "regex":
        return _scalar_regex(resolved, value)
    if leaf.op == "has_any":
        return _set_has_any(resolved, values)
    if leaf.op == "has_all":
        return _set_has_all(resolved, values)
    raise ValueError(f"unsupported operator {leaf.op!r}")  # unreachable given schema validation


# ---------------------------------------------------------------------------
# three-valued combinators
# ---------------------------------------------------------------------------


def tri_not(value: TriState) -> TriState:
    if value is TriState.UNKNOWN:
        return TriState.UNKNOWN
    return TriState.FALSE if value is TriState.TRUE else TriState.TRUE


def tri_all(values: Iterable[TriState]) -> TriState:
    values = list(values)
    if any(v is TriState.FALSE for v in values):
        return TriState.FALSE
    if all(v is TriState.TRUE for v in values):
        return TriState.TRUE
    return TriState.UNKNOWN


def tri_any(values: Iterable[TriState]) -> TriState:
    values = list(values)
    if any(v is TriState.TRUE for v in values):
        return TriState.TRUE
    if all(v is TriState.FALSE for v in values):
        return TriState.FALSE
    return TriState.UNKNOWN


def evaluate_condition_node(
    node: Optional[ConditionNode], view: EmailView, *, me_email: Optional[str] = None
) -> TriState:
    if node is None:
        return TriState.TRUE  # no content condition beyond the anchor
    if node.field is not None:
        return evaluate_leaf(node, view, me_email=me_email)
    if node.all is not None:
        return tri_all(
            evaluate_condition_node(child, view, me_email=me_email) for child in node.all
        )
    if node.any is not None:
        return tri_any(
            evaluate_condition_node(child, view, me_email=me_email) for child in node.any
        )
    if node.not_ is not None:
        return tri_not(evaluate_condition_node(node.not_, view, me_email=me_email))
    raise ValueError("malformed condition node")  # unreachable given schema validation


def evaluate_anchor(anchor: AnchorGroup, view: EmailView, *, me_email: Optional[str] = None) -> TriState:
    conditions = anchor.any if anchor.any is not None else anchor.all
    results = [evaluate_leaf(condition, view, me_email=me_email) for condition in conditions]
    return tri_any(results) if anchor.any is not None else tri_all(results)


# ---------------------------------------------------------------------------
# per-rule evaluation (design §4.2's table, exactly)
# ---------------------------------------------------------------------------


class RuleEvalStatus(str, Enum):
    MATCHED = "MATCHED"
    NOT_MATCHED = "NOT_MATCHED"
    INDETERMINATE = "INDETERMINATE"


def evaluate_match(
    anchor: AnchorGroup,
    conditions: Optional[ConditionNode],
    view: EmailView,
    *,
    me_email: Optional[str] = None,
) -> RuleEvalStatus:
    anchor_result = evaluate_anchor(anchor, view, me_email=me_email)
    if anchor_result is TriState.FALSE:
        return RuleEvalStatus.NOT_MATCHED
    if anchor_result is TriState.UNKNOWN:
        return RuleEvalStatus.INDETERMINATE

    condition_result = evaluate_condition_node(conditions, view, me_email=me_email)
    if condition_result is TriState.TRUE:
        return RuleEvalStatus.MATCHED
    if condition_result is TriState.FALSE:
        return RuleEvalStatus.NOT_MATCHED
    return RuleEvalStatus.INDETERMINATE
