"""Tier 1 v1 registry compiler (design doc §7).

Validates a directory of rule YAML files as one atomic unit and, on success,
produces an immutable, digest-addressed :class:`CompiledArtifact`. Any single
failing ``enabled`` rule blocks that artifact's creation; the compiler never
partially loads a ruleset and never touches a previously written artifact
(that is the caller's job via :func:`write_artifact`'s atomic pointer switch).
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

import yaml
from pydantic import ValidationError

from src.router.tier1.dsl import (
    EmailView,
    RuleEvalStatus,
    UnsafeRegexError,
    compile_safe_regex,
    evaluate_match,
    normalize_address,
)
from src.router.tier1.fingerprint import FINGERPRINT_VERSION, compute_action_fingerprint
from src.router.tier1.schema import (
    SCHEMA_VERSION,
    AnchorGroup,
    CanonicalRoute,
    RuleManifest,
    RuleStatus,
    canonical_match_signature,
    iter_condition_leaves,
)

RULE_FILE_SUFFIXES = (".yaml", ".yml")


@dataclass(frozen=True)
class CompileIssue:
    rule_id: Optional[str]
    code: str
    message: str


@dataclass(frozen=True)
class CompiledRule:
    manifest: RuleManifest
    action_fingerprint: str


@dataclass(frozen=True)
class CompiledArtifact:
    schema_version: int
    fingerprint_version: int
    digest: str
    rules: List[CompiledRule] = field(default_factory=list)
    warnings: List[CompileIssue] = field(default_factory=list)

    def to_json_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "fingerprint_version": self.fingerprint_version,
            "digest": self.digest,
            "rules": [
                {
                    "rule_id": c.manifest.rule_id,
                    "rule_version": c.manifest.rule_version,
                    "route": c.manifest.decision.route.value,
                    "action_fingerprint": c.action_fingerprint,
                }
                for c in self.rules
            ],
        }


@dataclass(frozen=True)
class CompilationFailure:
    errors: List[CompileIssue]
    warnings: List[CompileIssue] = field(default_factory=list)


def _canonical_json(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def _fixture_to_view(raw: Dict) -> EmailView:
    """Build an :class:`EmailView` from a ``governance.positive_cases``/
    ``negative_cases`` fixture's ``email`` payload.

    Accepts either ``{"to": ["a@b.com"]}`` or ``{"to": {"addresses": [...]}}``
    so fixtures can stay close to the design doc's YAML examples.
    """

    def _addresses(value) -> List[str]:
        if isinstance(value, dict):
            return list(value.get("addresses", []))
        if isinstance(value, list):
            return list(value)
        return []

    sender = raw.get("sender") or {}
    sender_address = sender.get("address", "") if isinstance(sender, dict) else str(sender)
    body = raw.get("body") or {}
    current_text = body.get("current_text", "") if isinstance(body, dict) else str(body)
    full_text = body.get("full_text", current_text) if isinstance(body, dict) else current_text

    return EmailView(
        sender_address=sender_address,
        to_addresses=_addresses(raw.get("to")),
        cc_addresses=_addresses(raw.get("cc")),
        subject=raw.get("subject", ""),
        body_current_text=current_text,
        body_full_text=full_text,
    )


def _load_rule_files(rule_dir: Path) -> List[Path]:
    return sorted(p for p in rule_dir.iterdir() if p.suffix in RULE_FILE_SUFFIXES)


def _parse_one_file(path: Path) -> tuple[Optional[RuleManifest], List[CompileIssue]]:
    """Parse and schema-validate one rule file.

    A schema failure on a rule that is not (probably) ``enabled`` is downgraded
    to a warning so a broken candidate can't block activation of an otherwise-
    valid production ruleset (design §7 point 1 / candidate-stage isolation). A
    rule whose status can't be determined defaults to being treated as
    ``enabled`` (fail closed).
    """
    text = path.read_text(encoding="utf-8")
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return None, [CompileIssue(None, "yaml_parse_error", f"{path.name}: {exc}")]
    if not isinstance(raw, dict):
        return None, [CompileIssue(None, "yaml_not_a_mapping", f"{path.name}: rule file must be a YAML mapping")]

    probed_status = raw.get("status") if isinstance(raw.get("status"), str) else None
    rule_id_hint = raw.get("rule_id") if isinstance(raw.get("rule_id"), str) else path.stem

    try:
        manifest = RuleManifest.model_validate(raw)
    except ValidationError as exc:
        message = f"{path.name}: {exc}"
        if probed_status in ("proposed", "retired"):
            return None, [CompileIssue(rule_id_hint, "schema_invalid_non_enabled", message)]
        return None, [CompileIssue(rule_id_hint, "schema_invalid", message)]
    return manifest, []


def _check_regex_safety(rule: RuleManifest) -> List[CompileIssue]:
    issues: List[CompileIssue] = []
    for leaf in iter_condition_leaves(rule.match.conditions):
        if leaf.op != "regex":
            continue
        try:
            compile_safe_regex(leaf.value)
        except UnsafeRegexError as exc:
            issues.append(CompileIssue(rule.rule_id, "unsafe_regex", str(exc)))
    return issues


def _check_external_recipients(rule: RuleManifest, internal_domains: Sequence[str]) -> List[CompileIssue]:
    if rule.decision.route is not CanonicalRoute.FORWARD:
        return []
    params = rule.decision.typed_params
    domains = {d.strip().casefold() for d in internal_domains if d.strip()}
    external = sorted(
        {
            addr
            for addr in (*params.fixed_recipients, *params.cc)
            if "@" in addr and addr.rsplit("@", 1)[1].strip().casefold() not in domains
        }
    )
    if external and not rule.governance.external_recipient_acknowledged:
        return [
            CompileIssue(
                rule.rule_id,
                "external_recipient_not_acknowledged",
                f"forward targets external address(es) {external!r} without "
                "governance.external_recipient_acknowledged=true",
            )
        ]
    return []


def _run_fixtures(rule: RuleManifest, *, me_email: Optional[str] = None) -> List[CompileIssue]:
    issues: List[CompileIssue] = []
    for case in rule.governance.positive_cases:
        view = _fixture_to_view(case.email)
        status = evaluate_match(rule.match.anchor, rule.match.conditions, view, me_email=me_email)
        if status is not RuleEvalStatus.MATCHED:
            issues.append(
                CompileIssue(
                    rule.rule_id,
                    "fixture_positive_failed",
                    f"positive_case {case.case_id!r} did not MATCH (got {status.value})",
                )
            )
    for case in rule.governance.negative_cases:
        view = _fixture_to_view(case.email)
        status = evaluate_match(rule.match.anchor, rule.match.conditions, view, me_email=me_email)
        if status is not RuleEvalStatus.NOT_MATCHED:
            issues.append(
                CompileIssue(
                    rule.rule_id,
                    "fixture_negative_failed",
                    f"negative_case {case.case_id!r} did not NO_MATCH (got {status.value})",
                )
            )
    return issues


def _check_duplicate_ids(rules: List[RuleManifest]) -> List[CompileIssue]:
    seen: Dict[str, RuleManifest] = {}
    issues: List[CompileIssue] = []
    for rule in rules:
        if rule.rule_id in seen:
            issues.append(
                CompileIssue(rule.rule_id, "duplicate_rule_id", f"rule_id {rule.rule_id!r} declared more than once")
            )
        else:
            seen[rule.rule_id] = rule
    return issues


def _literal_anchor_addresses(anchor: AnchorGroup) -> set:
    """Collect literal anchor address values for static overlap detection.

    Deliberately does not resolve the ``$ME`` placeholder (compile time has no
    identity context): a rule anchored on ``$ME`` is treated as an opaque
    literal here, so it is only flagged as overlapping another rule that
    anchors on the literal string ``"$ME"`` itself, never on the real address
    it resolves to at match time. Runtime conflict handling remains the final
    authority for identity-dependent overlap.
    """
    conditions = anchor.any if anchor.any is not None else anchor.all
    addresses: set = set()
    for condition in conditions:
        if condition.value is not None:
            addresses.add(normalize_address(condition.value))
        if condition.values:
            addresses.update(normalize_address(v) for v in condition.values)
    return addresses


def _check_static_overlap(compiled: List[CompiledRule]) -> tuple[List[CompileIssue], List[CompileIssue]]:
    """Hard errors for mechanically decidable duplicate-match cases; warnings
    for everything else that is merely *possible* overlap (design §6). This
    does not attempt to prove regex/contains non-overlap in general — that is
    undecidable; runtime conflict handling is the final authority there.
    """
    errors: List[CompileIssue] = []
    warnings: List[CompileIssue] = []

    by_signature: Dict[str, List[CompiledRule]] = {}
    for compiled_rule in compiled:
        signature = canonical_match_signature(compiled_rule.manifest.match)
        by_signature.setdefault(signature, []).append(compiled_rule)
    for group in by_signature.values():
        if len(group) < 2:
            continue
        fingerprints = {c.action_fingerprint for c in group}
        if len(fingerprints) > 1:
            rule_ids = sorted(c.manifest.rule_id for c in group)
            errors.append(
                CompileIssue(
                    None,
                    "duplicate_match_divergent_action",
                    f"rules {rule_ids} share an identical normalized match but produce "
                    f"different actions {sorted(fingerprints)}",
                )
            )

    anchor_addresses: Dict[str, set] = {
        c.manifest.rule_id: _literal_anchor_addresses(c.manifest.match.anchor) for c in compiled
    }
    by_id = {c.manifest.rule_id: c for c in compiled}
    ids = sorted(anchor_addresses)
    for i, left_id in enumerate(ids):
        for right_id in ids[i + 1 :]:
            shared = anchor_addresses[left_id] & anchor_addresses[right_id]
            if not shared:
                continue
            left, right = by_id[left_id], by_id[right_id]
            if left.action_fingerprint != right.action_fingerprint:
                warnings.append(
                    CompileIssue(
                        None,
                        "possible_anchor_overlap",
                        f"rules {left_id!r} and {right_id!r} share anchor address(es) "
                        f"{sorted(shared)} with different actions; runtime conflict handling applies",
                    )
                )
    return errors, warnings


def compile_registry(
    rule_dir: Union[Path, str],
    *,
    internal_email_domains: Sequence[str] = (),
    me_email: Optional[str] = None,
) -> Union[CompiledArtifact, CompilationFailure]:
    """Validate every rule file under ``rule_dir`` as one atomic unit.

    Only ``status: enabled`` rules enter the activation-eligible set and are
    subject to the full pipeline (duplicate-ID, regex safety, external-
    recipient acknowledgement, fixture replay, static overlap). Schema
    failures on ``proposed``/``retired`` rules are downgraded to warnings so a
    broken candidate never blocks production activation. ``me_email`` is used
    only to replay fixtures for rules that reference the ``$ME`` placeholder;
    it has no effect on schema, regex-safety, or static-overlap checks.
    """
    rule_dir = Path(rule_dir)
    errors: List[CompileIssue] = []
    warnings: List[CompileIssue] = []
    enabled_rules: List[RuleManifest] = []

    for path in _load_rule_files(rule_dir):
        manifest, issues = _parse_one_file(path)
        for issue in issues:
            if issue.code == "schema_invalid_non_enabled":
                warnings.append(issue)
            else:
                errors.append(issue)
        if manifest is not None and manifest.status is RuleStatus.ENABLED:
            enabled_rules.append(manifest)

    if errors:
        return CompilationFailure(errors=errors, warnings=warnings)

    errors.extend(_check_duplicate_ids(enabled_rules))
    for rule in enabled_rules:
        regex_issues = _check_regex_safety(rule)
        errors.extend(regex_issues)
        errors.extend(_check_external_recipients(rule, internal_email_domains))
        if regex_issues:
            # An unsafe/uncompilable regex would make fixture replay itself
            # raise; the rule is already rejected, so skip evaluating it.
            continue
        errors.extend(_run_fixtures(rule, me_email=me_email))

    if errors:
        return CompilationFailure(errors=errors, warnings=warnings)

    compiled_rules = [
        CompiledRule(manifest=rule, action_fingerprint=compute_action_fingerprint(rule.decision))
        for rule in sorted(enabled_rules, key=lambda r: r.rule_id)
    ]

    overlap_errors, overlap_warnings = _check_static_overlap(compiled_rules)
    errors.extend(overlap_errors)
    warnings.extend(overlap_warnings)
    if errors:
        return CompilationFailure(errors=errors, warnings=warnings)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "fingerprint_version": FINGERPRINT_VERSION,
        "rules": [
            {
                "rule_id": c.manifest.rule_id,
                "rule_version": c.manifest.rule_version,
                "route": c.manifest.decision.route.value,
                "action_fingerprint": c.action_fingerprint,
            }
            for c in compiled_rules
        ],
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()

    return CompiledArtifact(
        schema_version=SCHEMA_VERSION,
        fingerprint_version=FINGERPRINT_VERSION,
        digest=digest,
        rules=compiled_rules,
        warnings=warnings,
    )


def _atomic_write(path: Path, content: str) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_name, path)
    except Exception:
        os.unlink(tmp_name)
        raise


def write_artifact(artifact: CompiledArtifact, output_dir: Union[Path, str]) -> Path:
    """Atomically publish ``artifact`` and switch the ``current.json`` pointer.

    Writes ``<output_dir>/<digest>.json`` and only then atomically replaces
    ``<output_dir>/current.json`` to point at it. The pointer switch — not the
    artifact write — is what makes activation atomic (design §7): a reader
    always sees either the old complete artifact or the new complete one,
    never a partial write.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    artifact_path = output_dir / f"{artifact.digest}.json"
    _atomic_write(artifact_path, _canonical_json(artifact.to_json_dict()))

    pointer_path = output_dir / "current.json"
    _atomic_write(pointer_path, _canonical_json({"digest": artifact.digest, "path": artifact_path.name}))
    return artifact_path
