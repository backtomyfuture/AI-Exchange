"""Tier 1 v1 rule manifest schema.

Strict, versioned pydantic models for the declarative Tier 1 registry defined in
``docs/tier1-routing-design.md``. This schema is independent from the legacy
``src/router/base.py`` manifest (``SkillTrigger``/``AutoOutcome``) used by the
currently running production router; the two coexist until the 31 frozen
``skills_registry/`` candidates are individually migrated (see
``docs/tier1-routing-design.md`` §10).

``model_config = ConfigDict(extra="forbid")`` on every model enforces §9 of the
design doc: an unknown or legacy field (``need_reply``, ``card_type``,
``priority``, ``action``, ``forward_to``, ``tone_instruction``, ...) fails
validation loudly instead of being silently ignored.
"""
from __future__ import annotations

import json
import re
from enum import Enum
from typing import Any, Dict, Iterator, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

SCHEMA_VERSION = 1

_RULE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,63}$")

_SET_FIELDS = frozenset({"to.addresses", "cc.addresses"})
_SET_ONLY_OPERATORS = frozenset({"has_any", "has_all"})
_SCALAR_SINGLE_VALUE_OPERATORS = frozenset({"eq", "contains", "regex"})


class RuleStatus(str, Enum):
    PROPOSED = "proposed"
    ENABLED = "enabled"
    RETIRED = "retired"


class Criticality(str, Enum):
    """Non-authoritative descriptive metadata (``governance.criticality``).

    Never selects a route and never resolves a conflict; reporting/SLA use only.
    """

    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class CanonicalRoute(str, Enum):
    REPLY = "reply"
    FORWARD = "forward"
    READ_ONLY = "read_only"
    NO_ACTION = "no_action"
    MANUAL_REVIEW = "manual_review"


class ReplyMode(str, Enum):
    SENDER_ONLY = "sender_only"
    SENDER_AND_ORIGINAL_CC = "sender_and_original_cc"


# ---------------------------------------------------------------------------
# match.anchor
# ---------------------------------------------------------------------------


def _validate_leaf_shape(
    field_name: str, op: str, value: Optional[str], values: Optional[List[str]]
) -> None:
    is_set_field = field_name in _SET_FIELDS
    is_set_op = op in _SET_ONLY_OPERATORS
    if is_set_field != is_set_op:
        raise ValueError(
            f"op {op!r} is not valid on field {field_name!r}: address-set fields "
            "require 'has_any'/'has_all'; scalar fields require "
            "'eq'/'in'/'contains'/'contains_any'/'regex'"
        )
    if op in _SCALAR_SINGLE_VALUE_OPERATORS:
        if not isinstance(value, str) or not value.strip() or values is not None:
            raise ValueError(f"op {op!r} requires a non-empty 'value' and no 'values'")
    else:
        if not isinstance(values, list) or not values or value is not None:
            raise ValueError(f"op {op!r} requires a non-empty 'values' list and no 'value'")
        if any(not isinstance(v, str) or not v.strip() for v in values):
            raise ValueError("'values' entries must be non-empty strings")


class AnchorCondition(BaseModel):
    """A single, exact-match anchor leaf.

    Anchors may never use ``contains``/``regex``: a weak anchor is equivalent to
    no anchor at all (design doc §4.1).
    """

    model_config = ConfigDict(extra="forbid")

    field: Literal["sender.address", "to.addresses", "cc.addresses"]
    op: Literal["eq", "in", "has_any", "has_all"]
    value: Optional[str] = None
    values: Optional[List[str]] = None

    @model_validator(mode="after")
    def _check_value_shape(self) -> "AnchorCondition":
        _validate_leaf_shape(self.field, self.op, self.value, self.values)
        return self


class AnchorGroup(BaseModel):
    """``match.anchor``: exactly one of ``any``/``all`` over :class:`AnchorCondition`."""

    model_config = ConfigDict(extra="forbid")

    any: Optional[List[AnchorCondition]] = None
    all: Optional[List[AnchorCondition]] = None

    @model_validator(mode="after")
    def _exactly_one_combinator(self) -> "AnchorGroup":
        combinators = [name for name in ("any", "all") if getattr(self, name) is not None]
        if len(combinators) != 1:
            raise ValueError("anchor must specify exactly one of 'any' or 'all'")
        if not getattr(self, combinators[0]):
            raise ValueError("anchor combinator must not be empty")
        return self


# ---------------------------------------------------------------------------
# match.conditions (recursive all/any/not, leaf inline)
# ---------------------------------------------------------------------------


class ConditionNode(BaseModel):
    """One node of the ``match.conditions`` tree.

    Exactly one of ``all``/``any``/``not``/leaf (``field``+``op``) must be set.
    Leaves reuse the anchor's exact-match operators plus content operators
    (``contains``/``contains_any``/``regex``) not available to anchors.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    all: Optional[List["ConditionNode"]] = None
    any: Optional[List["ConditionNode"]] = None
    not_: Optional["ConditionNode"] = Field(default=None, alias="not")
    field: Optional[
        Literal[
            "sender.address",
            "to.addresses",
            "cc.addresses",
            "subject",
            "body.current_text",
            "body.full_text",
        ]
    ] = None
    op: Optional[
        Literal["eq", "in", "contains", "contains_any", "regex", "has_any", "has_all"]
    ] = None
    value: Optional[str] = None
    values: Optional[List[str]] = None

    @model_validator(mode="after")
    def _exactly_one_shape(self) -> "ConditionNode":
        shapes = [
            name
            for name, present in (("all", self.all), ("any", self.any), ("not", self.not_))
            if present is not None
        ]
        is_leaf = self.field is not None
        if is_leaf:
            shapes.append("leaf")
        if len(shapes) != 1:
            raise ValueError(f"condition node must be exactly one of all/any/not/leaf, got {shapes}")
        if is_leaf:
            if self.op is None:
                raise ValueError("leaf condition requires 'op'")
            _validate_leaf_shape(self.field, self.op, self.value, self.values)
        else:
            group = self.all if self.all is not None else self.any
            if group is not None and not group:
                raise ValueError("'all'/'any' must not be empty")
        return self


ConditionNode.model_rebuild()


def iter_condition_leaves(node: Optional[ConditionNode]) -> Iterator[ConditionNode]:
    """Depth-first walk yielding every leaf node under ``node``."""
    if node is None:
        return
    if node.field is not None:
        yield node
        return
    for key in ("all", "any"):
        group = getattr(node, key)
        if group:
            for child in group:
                yield from iter_condition_leaves(child)
    if node.not_ is not None:
        yield from iter_condition_leaves(node.not_)


# ---------------------------------------------------------------------------
# match
# ---------------------------------------------------------------------------


class Match(BaseModel):
    model_config = ConfigDict(extra="forbid")

    anchor: AnchorGroup
    conditions: Optional[ConditionNode] = None

    @model_validator(mode="after")
    def _conditions_not_bare_not(self) -> "Match":
        # ``_exactly_one_shape`` on ConditionNode already guarantees ``not_`` is
        # the sole populated shape when it is set, so this check is sufficient.
        if self.conditions is not None and self.conditions.not_ is not None:
            raise ValueError(
                "match.conditions must not be a bare 'not' at the top level; "
                "provide a positive anchor/condition"
            )
        return self


def match_has_content_condition(match: Match) -> bool:
    return match.conditions is not None


def match_uses_full_text(match: Match) -> bool:
    return any(leaf.field == "body.full_text" for leaf in iter_condition_leaves(match.conditions))


def _normalize_leaf_dump(leaf: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"field": leaf["field"], "op": leaf["op"]}
    if "value" in leaf:
        out["value"] = leaf["value"].strip().casefold()
    if "values" in leaf:
        out["values"] = sorted(v.strip().casefold() for v in leaf["values"])
    return out


def _normalize_condition_dump(node: Dict[str, Any]) -> Dict[str, Any]:
    if "field" in node:
        return _normalize_leaf_dump(node)
    out: Dict[str, Any] = {}
    for key in ("all", "any"):
        if key in node:
            out[key] = sorted(
                json.dumps(_normalize_condition_dump(child), sort_keys=True) for child in node[key]
            )
    if "not" in node:
        out["not"] = json.dumps(_normalize_condition_dump(node["not"]), sort_keys=True)
    return out


def canonical_match_signature(match: Match) -> str:
    """A best-effort, order/case-insensitive structural signature of ``match``.

    Used to detect *literal* duplicate matches with divergent actions (design
    §6 hard error). This is not a semantic-equivalence prover for arbitrary
    regex/contains trees — that is undecidable in general and is intentionally
    left to the warning-level static overlap check plus runtime conflict
    handling.
    """
    dumped = match.model_dump(mode="json", by_alias=True, exclude_none=True)
    anchor_dump = dumped.get("anchor", {})
    anchor_norm: Dict[str, Any] = {}
    for key in ("any", "all"):
        if key in anchor_dump:
            anchor_norm[key] = sorted(
                json.dumps(_normalize_leaf_dump(leaf), sort_keys=True) for leaf in anchor_dump[key]
            )
    conditions_norm = (
        _normalize_condition_dump(dumped["conditions"]) if dumped.get("conditions") else None
    )
    payload = {"anchor": anchor_norm, "conditions": conditions_norm}
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# decision.params (route-specific, strict)
# ---------------------------------------------------------------------------


class ReplyParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reply_mode: Optional[ReplyMode] = None


class ForwardParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fixed_recipients: List[str]
    cc: List[str] = Field(default_factory=list)
    allow_recipient_edit: bool = True
    include_attachments: bool = False

    @model_validator(mode="after")
    def _recipients_are_exact_addresses(self) -> "ForwardParams":
        if not self.fixed_recipients:
            raise ValueError("forward requires at least one fixed_recipients address")
        for addr in (*self.fixed_recipients, *self.cc):
            if "@" not in addr or any(ch in addr for ch in "*?"):
                raise ValueError(
                    f"fixed_recipients/cc must be exact addresses, no wildcards/domains: {addr!r}"
                )
        return self


class ReadOnlyParams(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoActionParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason_code: str


class ManualReviewParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason_code: str


_PARAMS_BY_ROUTE: Dict[CanonicalRoute, type[BaseModel]] = {
    CanonicalRoute.REPLY: ReplyParams,
    CanonicalRoute.FORWARD: ForwardParams,
    CanonicalRoute.READ_ONLY: ReadOnlyParams,
    CanonicalRoute.NO_ACTION: NoActionParams,
    CanonicalRoute.MANUAL_REVIEW: ManualReviewParams,
}


class Decision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    route: CanonicalRoute
    business_flow_id: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)

    _typed_params: BaseModel = PrivateAttr()

    @model_validator(mode="after")
    def _validate_params_for_route(self) -> "Decision":
        params_model = _PARAMS_BY_ROUTE[self.route]
        try:
            self._typed_params = params_model.model_validate(self.params)
        except Exception as exc:  # re-raised as a ValueError pydantic will wrap
            raise ValueError(f"decision.params invalid for route {self.route.value!r}: {exc}") from exc
        return self

    @property
    def typed_params(self) -> BaseModel:
        return self._typed_params


# ---------------------------------------------------------------------------
# rule-level governance / validity / fixtures
# ---------------------------------------------------------------------------


class FixtureCase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    case_id: str
    email: Dict[str, Any]


class Governance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    criticality: Optional[Criticality] = None
    risk_notes: Optional[str] = None
    positive_cases: List[FixtureCase] = Field(default_factory=list)
    negative_cases: List[FixtureCase] = Field(default_factory=list)
    external_recipient_acknowledged: bool = False
    full_text_match_acknowledged: bool = False
    owner_reviewed_at: Optional[str] = None


class Validity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    effective_from: Optional[str] = None
    expires_at: Optional[str] = None


# ---------------------------------------------------------------------------
# top-level rule manifest
# ---------------------------------------------------------------------------


class RuleManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    rule_id: str
    rule_version: int = Field(ge=1)
    status: RuleStatus
    owner: Optional[str] = None
    purpose: Optional[str] = None

    validity: Validity = Field(default_factory=Validity)
    match: Match
    decision: Decision
    governance: Governance = Field(default_factory=Governance)

    @model_validator(mode="after")
    def _validate_rule_id(self) -> "RuleManifest":
        if not _RULE_ID_RE.match(self.rule_id):
            raise ValueError(f"rule_id {self.rule_id!r} must match {_RULE_ID_RE.pattern}")
        return self

    @model_validator(mode="after")
    def _validate_no_action_governance(self) -> "RuleManifest":
        if self.decision.route is CanonicalRoute.NO_ACTION:
            if not self.owner:
                raise ValueError("route=no_action requires a top-level 'owner'")
            if self.validity.expires_at is None:
                raise ValueError("route=no_action requires 'validity.expires_at'")
            if len(self.governance.positive_cases) < 1:
                raise ValueError("route=no_action requires at least 1 governance.positive_cases")
            if len(self.governance.negative_cases) < 2:
                raise ValueError("route=no_action requires at least 2 governance.negative_cases")
        return self

    @model_validator(mode="after")
    def _validate_fixture_minimums(self) -> "RuleManifest":
        if len(self.governance.positive_cases) < 1:
            raise ValueError("every rule requires at least 1 governance.positive_cases")
        if match_has_content_condition(self.match) and len(self.governance.negative_cases) < 1:
            raise ValueError(
                "content-driven rules (match.conditions set) require at least 1 "
                "governance.negative_cases (hard negative)"
            )
        return self

    @model_validator(mode="after")
    def _validate_full_text_ack(self) -> "RuleManifest":
        if match_uses_full_text(self.match) and not self.governance.full_text_match_acknowledged:
            raise ValueError(
                "rules matching body.full_text require "
                "governance.full_text_match_acknowledged=true"
            )
        return self
