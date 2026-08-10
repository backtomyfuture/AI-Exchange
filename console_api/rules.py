"""Rule Draft storage, validation, compilation, and matcher sandbox."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from console_api.database import ConsoleDatabase
from console_api.models import (
    CompileIssueModel,
    MatchTestResponse,
    RuleDetail,
    RuleDraftRequest,
    RuleSummary,
    RuleValidationResponse,
)
from console_api.settings import ConsoleSettings
from src.router.tier1.compiler import (
    CompilationFailure,
    CompiledArtifact,
    compile_registry,
    write_artifact,
)
from src.router.tier1.dsl import RuleEvalStatus, evaluate_match
from src.router.tier1.schema import RuleManifest, RuleStatus


_RULE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,63}$")
_RULE_SUFFIXES = (".yaml", ".yml")


class RuleStoreError(RuntimeError):
    """Safe operator-facing failure from the local rule store."""

    def __init__(self, code: str, *, status_code: int = 400):
        super().__init__(code)
        self.code = code
        self.status_code = status_code


def _issue_models(issues) -> list[CompileIssueModel]:
    return [
        CompileIssueModel(rule_id=issue.rule_id, code=issue.code, message=issue.message)
        for issue in issues
    ]


def _domains(settings: ConsoleSettings) -> tuple[str, ...]:
    return tuple(
        value.strip()
        for value in settings.internal_email_domains.split(",")
        if value.strip()
    )


class RuleStore:
    """Own the local ``tier1_rules`` seam and keep compiler semantics centralized."""

    def __init__(self, settings: ConsoleSettings):
        self._settings = settings
        self._rules_dir = settings.rules_dir.resolve()
        self._artifact_dir = settings.artifact_dir.resolve()

    def _paths(self) -> list[Path]:
        if not self._rules_dir.is_dir():
            return []
        return sorted(
            path
            for path in self._rules_dir.iterdir()
            if path.is_file() and path.suffix in _RULE_SUFFIXES
        )

    def _path_for_rule(self, rule_id: str) -> Path:
        if _RULE_ID_RE.fullmatch(rule_id) is None:
            raise RuleStoreError("rule_id_invalid")
        for path in self._paths():
            if path.stem == rule_id:
                return path
        return self._rules_dir / f"{rule_id}.yaml"

    def _read_raw(self, path: Path) -> dict[str, Any]:
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            raise RuleStoreError("rule_file_unreadable") from exc
        if not isinstance(raw, Mapping):
            raise RuleStoreError("rule_manifest_not_mapping")
        return dict(raw)

    def _summary(self, path: Path, raw: Mapping[str, Any]) -> RuleSummary:
        decision = raw.get("decision")
        route = decision.get("route") if isinstance(decision, Mapping) else None
        return RuleSummary(
            rule_id=str(raw.get("rule_id") or path.stem),
            rule_version=int(raw.get("rule_version") or 1),
            status=str(raw.get("status") or "proposed"),
            route=str(route or "unknown"),
            purpose=str(raw["purpose"]) if raw.get("purpose") is not None else None,
            owner=str(raw["owner"]) if raw.get("owner") is not None else None,
            filename=path.name,
        )

    def list_rules(self) -> list[RuleSummary]:
        return [self._summary(path, self._read_raw(path)) for path in self._paths()]

    def get_rule(self, rule_id: str) -> RuleDetail | None:
        path = self._path_for_rule(rule_id)
        if not path.exists():
            return None
        raw = self._read_raw(path)
        return RuleDetail(**self._summary(path, raw).model_dump(), manifest=raw)

    def _candidate_manifest(self, request: RuleDraftRequest) -> tuple[str, dict[str, Any]]:
        if request.manifest is not None and request.raw_yaml is not None:
            raise RuleStoreError("rule_payload_ambiguous")
        if request.raw_yaml is not None:
            try:
                raw = yaml.safe_load(request.raw_yaml)
            except yaml.YAMLError as exc:
                raise RuleStoreError("rule_yaml_invalid") from exc
        else:
            raw = request.manifest
        if raw is None:
            raw = {}
        if not isinstance(raw, Mapping):
            raise RuleStoreError("rule_manifest_not_mapping")
        candidate = dict(raw)
        requested_id = request.rule_id or candidate.get("rule_id")
        if not isinstance(requested_id, str) or _RULE_ID_RE.fullmatch(requested_id) is None:
            raise RuleStoreError("rule_id_invalid")
        if candidate.get("rule_id") not in {None, requested_id}:
            raise RuleStoreError("rule_id_mismatch")
        candidate["rule_id"] = requested_id
        candidate.setdefault("rule_version", 1)
        candidate.setdefault("status", RuleStatus.PROPOSED.value)
        status = candidate["status"]
        allowed_statuses = {item.value for item in RuleStatus}
        if not isinstance(status, str) or status not in allowed_statuses:
            raise RuleStoreError("rule_status_invalid")
        return requested_id, candidate

    def _compile_dir(self, directory: Path):
        return compile_registry(
            directory,
            internal_email_domains=_domains(self._settings),
            me_email=self._settings.me_email.strip() or None,
        )

    def _with_candidate(self, candidate: dict[str, Any]) -> tuple[tempfile.TemporaryDirectory, Path]:
        temporary = tempfile.TemporaryDirectory(prefix=".operations-console-rules-")
        directory = Path(temporary.name)
        for path in self._paths():
            if path.stem == str(candidate["rule_id"]):
                continue
            shutil.copy2(path, directory / path.name)
        path = directory / f"{candidate['rule_id']}.yaml"
        path.write_text(
            yaml.safe_dump(candidate, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return temporary, path

    def validate_candidate(self, request: RuleDraftRequest) -> RuleValidationResponse:
        _rule_id, candidate = self._candidate_manifest(request)
        temporary, _ = self._with_candidate(candidate)
        try:
            result = self._compile_dir(Path(temporary.name))
        finally:
            temporary.cleanup()
        return _validation_response(result)

    def validate_registry(self) -> RuleValidationResponse:
        result = self._compile_dir(self._rules_dir)
        return _validation_response(result)

    def save(self, request: RuleDraftRequest) -> RuleDetail:
        rule_id, candidate = self._candidate_manifest(request)
        status = str(candidate.get("status", RuleStatus.PROPOSED.value))
        if status == RuleStatus.ENABLED.value:
            validation = self.validate_candidate(request)
            if not validation.valid:
                raise RuleStoreError("enabled_rule_requires_valid_registry", status_code=422)
        path = self._path_for_rule(rule_id)
        self._rules_dir.mkdir(parents=True, exist_ok=True)
        self._atomic_write_yaml(path, candidate)
        return RuleDetail(
            **self._summary(path, candidate).model_dump(),
            manifest=candidate,
        )

    def compile_artifact(self) -> tuple[CompiledArtifact, Path]:
        result = self._compile_dir(self._rules_dir)
        if isinstance(result, CompilationFailure):
            raise RuleStoreError("tier1_compile_failed", status_code=422)
        return result, write_artifact(result, self._artifact_dir)

    async def test_match(
        self,
        database: ConsoleDatabase,
        rule_id: str,
        external_email_id: str,
        save_as: str | None = None,
    ) -> MatchTestResponse:
        detail = self.get_rule(rule_id)
        if detail is None:
            raise RuleStoreError("rule_not_found", status_code=404)
        try:
            manifest = RuleManifest.model_validate(detail.manifest)
        except ValidationError as exc:
            raise RuleStoreError("rule_not_testable") from exc
        view = await database.historical_email_view(external_email_id)
        if view is None:
            raise RuleStoreError("historical_email_not_found", status_code=404)
        result = evaluate_match(
            manifest.match.anchor,
            manifest.match.conditions,
            view,
            me_email=self._settings.me_email.strip() or None,
        )
        saved_as: str | None = None
        case_id: str | None = None
        if save_as is not None:
            if save_as not in {"positive_cases", "negative_cases"}:
                raise RuleStoreError("fixture_bucket_invalid")
            expected = (
                RuleEvalStatus.MATCHED
                if save_as == "positive_cases"
                else RuleEvalStatus.NOT_MATCHED
            )
            if result is not expected:
                raise RuleStoreError("fixture_result_does_not_match_bucket", status_code=422)
            case_id = f"console-{external_email_id[:24]}-{os.urandom(3).hex()}"
            email = _fixture_email(view)
            bucket = list(manifest.governance.model_dump(mode="json").get(save_as, []))
            bucket.append({"case_id": case_id, "email": email})
            raw = manifest.model_dump(mode="json", by_alias=True, exclude_none=True)
            raw["governance"][save_as] = bucket
            self.save(RuleDraftRequest(rule_id=rule_id, manifest=raw))
            saved_as = save_as
        return MatchTestResponse(
            rule_id=rule_id,
            external_email_id=external_email_id,
            result=result.value,
            saved_as=saved_as,
            case_id=case_id,
        )

    @staticmethod
    def _atomic_write_yaml(path: Path, candidate: Mapping[str, Any]) -> None:
        temporary = path.with_name(f".{path.name}.tmp")
        try:
            temporary.write_text(
                yaml.safe_dump(dict(candidate), allow_unicode=True, sort_keys=False),
                encoding="utf-8",
            )
            os.replace(temporary, path)
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise RuleStoreError("rule_write_failed", status_code=500) from exc


def _validation_response(result) -> RuleValidationResponse:
    if isinstance(result, CompilationFailure):
        return RuleValidationResponse(
            valid=False,
            errors=_issue_models(result.errors),
            warnings=_issue_models(result.warnings),
        )
    return RuleValidationResponse(
        valid=True,
        digest=result.digest,
        enabled_rule_count=len(result.rules),
        warnings=_issue_models(result.warnings),
    )


def _fixture_email(view) -> dict[str, Any]:
    return {
        "sender": {"address": view.sender_address},
        "to": {"addresses": list(view.to_addresses) if isinstance(view.to_addresses, list) else []},
        "cc": {"addresses": list(view.cc_addresses) if isinstance(view.cc_addresses, list) else []},
        "subject": view.subject,
        "body": {
            "current_text": view.body_current_text if isinstance(view.body_current_text, str) else "",
            "full_text": view.body_full_text if isinstance(view.body_full_text, str) else "",
        },
    }
