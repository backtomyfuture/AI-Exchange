#!/usr/bin/env python3
"""Prepare deterministic Phase 4-Lite greenfield initialization manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.models import (  # noqa: E402
    ChangeKind,
    IngressSource,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.policy import FolderScope, PolicySnapshot  # noqa: E402
from src.ingestion.runtime_authority import (  # noqa: E402
    GREENFIELD_PIPELINE_NAME,
    RuntimeContract,
    canonical_policy_manifest,
)
from src.ingestion.runtime_capability import (  # noqa: E402
    CAPABILITY_CHAIN_ROOT_HASH,
    PHASE2_SCHEMA_REVISION,
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
)


_MAX_JSON_BYTES: Final = 1_048_576
_PROTOCOL_VERSION: Final = 1
_CANONICAL_FOLDER_KEY: Final = "INBOX"
_BUILD_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}\Z", re.ASCII)
_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_PLACEHOLDER_WEBHOOK_IDS: Final = frozenset(
    {
        "inbox",
        "your_inbox_id",
        "your-inbox-id",
        "placeholder",
        "replace_me",
        "replace-me",
    }
)

_POLICY_ENTRIES: Final = (
    (IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE, ProcessingPolicy.FULL),
    (
        IngressSource.WEBHOOK,
        "CreatedEvent",
        ChangeKind.CREATE,
        ProcessingPolicy.IGNORED,
    ),
    (
        IngressSource.WEBHOOK,
        "ModifiedEvent",
        ChangeKind.UPDATE,
        ProcessingPolicy.METADATA_ONLY,
    ),
    (
        IngressSource.WEBHOOK,
        "DeletedEvent",
        ChangeKind.DELETE,
        ProcessingPolicy.METADATA_ONLY,
    ),
    (IngressSource.SYNC, "create", ChangeKind.CREATE, ProcessingPolicy.FULL),
    (
        IngressSource.SYNC,
        "update",
        ChangeKind.UPDATE,
        ProcessingPolicy.METADATA_ONLY,
    ),
    (
        IngressSource.SYNC,
        "delete",
        ChangeKind.DELETE,
        ProcessingPolicy.METADATA_ONLY,
    ),
)

_SCHEMA_HASH_DOMAIN: Final = b"ai-exchange-phase4-lite-schema-source-v1\x00"
_ADAPTER_HASH_DOMAIN: Final = b"ai-exchange-phase4-lite-adapter-source-v1\x00"
_CONFIG_HASH_DOMAIN: Final = b"ai-exchange-phase4-lite-runtime-config-v1\x00"
_EVIDENCE_HASH_DOMAIN: Final = b"ai-exchange-phase4-lite-release-evidence-v1\x00"


class ManifestPreparationError(RuntimeError):
    """A privacy-safe manifest preparation failure."""


class _DuplicateJsonKey(ValueError):
    pass


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")


def _domain_hash(domain: bytes, value: object) -> str:
    return hashlib.sha256(domain + _canonical_bytes(value)).hexdigest()


def _require_text(name: str, value: object, *, maximum: int) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or _CONTROL_CHARACTER.search(value) is not None
    ):
        raise ManifestPreparationError(f"{name}_invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        raise ManifestPreparationError(f"{name}_invalid") from None
    if len(encoded) > maximum:
        raise ManifestPreparationError(f"{name}_invalid")
    return value


def _require_account_id(value: object) -> int:
    if type(value) is not int or not 1 <= value <= POSTGRES_BIGINT_MAX:
        raise ManifestPreparationError("account_id_invalid")
    return value


def _require_build_id(value: object) -> str:
    if type(value) is not str or _BUILD_ID.fullmatch(value) is None:
        raise ManifestPreparationError("build_id_invalid")
    return value


def _require_webhook_id(value: object) -> str:
    exact = _require_text("webhook_inbox_id", value, maximum=512)
    if len(exact.encode("utf-8")) < 8 or exact.casefold() in _PLACEHOLDER_WEBHOOK_IDS:
        raise ManifestPreparationError("webhook_inbox_id_invalid")
    return exact


def _read_regular_file(path: Path, *, maximum: int, error: str) -> bytes:
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= maximum:
            raise ManifestPreparationError(error)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_dev != metadata.st_dev
                or opened.st_ino != metadata.st_ino
                or opened.st_size != metadata.st_size
            ):
                raise ManifestPreparationError(error)
            chunks: list[bytes] = []
            remaining = opened.st_size
            while remaining:
                chunk = os.read(descriptor, min(remaining, 65_536))
                if not chunk:
                    raise ManifestPreparationError(error)
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise ManifestPreparationError(error)
            return b"".join(chunks)
        finally:
            os.close(descriptor)
    except ManifestPreparationError:
        raise
    except OSError:
        raise ManifestPreparationError(error) from None


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey
        value[key] = item
    return value


def _load_release_evidence(path: Path, *, build_id: str) -> dict[str, Any]:
    raw = _read_regular_file(
        path,
        maximum=_MAX_JSON_BYTES,
        error="release_evidence_invalid",
    )
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeError, json.JSONDecodeError, ValueError, _DuplicateJsonKey):
        raise ManifestPreparationError("release_evidence_invalid") from None
    expected_keys = {
        "schema_version",
        "build_id",
        "source_revision",
        "checks",
        "artifacts",
        "accepted_residual_risks",
    }
    if type(value) is not dict or set(value) != expected_keys:
        raise ManifestPreparationError("release_evidence_invalid")
    if value["schema_version"] != 1 or type(value["schema_version"]) is not int:
        raise ManifestPreparationError("release_evidence_invalid")
    if value["build_id"] != build_id or type(value["build_id"]) is not str:
        raise ManifestPreparationError("release_evidence_build_mismatch")
    source_revision = value["source_revision"]
    if (
        type(source_revision) is not str
        or _GIT_OBJECT_ID.fullmatch(source_revision) is None
    ):
        raise ManifestPreparationError("release_evidence_invalid")

    checks = value["checks"]
    if type(checks) is not list or not 1 <= len(checks) <= 64:
        raise ManifestPreparationError("release_evidence_invalid")
    check_names: set[str] = set()
    for check in checks:
        if type(check) is not dict or set(check) != {"name", "command", "exit_code"}:
            raise ManifestPreparationError("release_evidence_invalid")
        name = _require_text("release_evidence", check["name"], maximum=128)
        _require_text("release_evidence", check["command"], maximum=4_096)
        if name in check_names or type(check["exit_code"]) is not int:
            raise ManifestPreparationError("release_evidence_invalid")
        if check["exit_code"] != 0:
            raise ManifestPreparationError("release_evidence_not_green")
        check_names.add(name)

    artifacts = value["artifacts"]
    if type(artifacts) is not list or not 1 <= len(artifacts) <= 64:
        raise ManifestPreparationError("release_evidence_invalid")
    artifact_names: set[str] = set()
    for artifact in artifacts:
        if type(artifact) is not dict or set(artifact) != {"name", "sha256"}:
            raise ManifestPreparationError("release_evidence_invalid")
        name = _require_text("release_evidence", artifact["name"], maximum=128)
        digest = artifact["sha256"]
        if (
            name in artifact_names
            or type(digest) is not str
            or _SHA256.fullmatch(digest) is None
        ):
            raise ManifestPreparationError("release_evidence_invalid")
        artifact_names.add(name)

    risks = value["accepted_residual_risks"]
    if type(risks) is not list or len(risks) > 64:
        raise ManifestPreparationError("release_evidence_invalid")
    exact_risks = [
        _require_text("release_evidence", risk, maximum=1_024) for risk in risks
    ]
    if len(exact_risks) != len(set(exact_risks)):
        raise ManifestPreparationError("release_evidence_invalid")
    return value


def _git_output(project_root: Path, *arguments: str, timeout: int = 10) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(project_root), *arguments],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        raise ManifestPreparationError("reviewed_source_revision_unavailable") from None
    return result.stdout.strip()


def _require_clean_git_tree(project_root: Path) -> dict[str, str]:
    try:
        expected_root = project_root.resolve(strict=True)
        discovered_root = Path(
            _git_output(project_root, "rev-parse", "--show-toplevel", timeout=5)
        ).resolve(strict=True)
    except (OSError, RuntimeError):
        raise ManifestPreparationError("reviewed_source_revision_unavailable") from None
    if discovered_root != expected_root:
        raise ManifestPreparationError("reviewed_source_revision_unavailable")

    status_arguments = (
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--ignore-submodules=none",
    )
    if _git_output(project_root, *status_arguments):
        raise ManifestPreparationError("reviewed_source_not_clean")

    revision = _git_output(project_root, "rev-parse", "--verify", "HEAD", timeout=5)
    tree = _git_output(
        project_root,
        "rev-parse",
        "--verify",
        f"{revision}^{{tree}}",
        timeout=5,
    )
    if (
        _GIT_OBJECT_ID.fullmatch(revision) is None
        or _GIT_OBJECT_ID.fullmatch(tree) is None
    ):
        raise ManifestPreparationError("reviewed_source_revision_unavailable")

    if (
        _git_output(project_root, *status_arguments)
        or _git_output(project_root, "rev-parse", "--verify", "HEAD", timeout=5)
        != revision
    ):
        raise ManifestPreparationError("reviewed_source_not_clean")
    return {"revision": revision, "tree": tree}


def _committed_tree_manifest(
    identity: Mapping[str, str],
    *,
    kind: str,
    fixed: Mapping[str, object],
) -> dict[str, object]:
    if (
        type(identity) is not dict
        or set(identity) != {"revision", "tree"}
        or any(
            type(value) is not str or _GIT_OBJECT_ID.fullmatch(value) is None
            for value in identity.values()
        )
    ):
        raise ManifestPreparationError("reviewed_source_manifest_invalid")
    return {
        "schema_version": 1,
        "kind": kind,
        "fixed": dict(fixed),
        "git": dict(identity),
    }


def _policy_payload(
    webhook_inbox_id: str, sync_folder: str
) -> tuple[dict[str, object], PolicySnapshot]:
    matrix = {
        (source, event_type, kind): policy
        for source, event_type, kind, policy in _POLICY_ENTRIES
    }
    scope = FolderScope.configured(
        canonical_key=_CANONICAL_FOLDER_KEY,
        webhook_ids=(webhook_inbox_id,),
        sync_folder=sync_folder,
        event_policy_matrix=matrix,
    )
    snapshot = PolicySnapshot(scopes=(scope,))
    canonical_policy_manifest(snapshot)
    payload: dict[str, object] = {
        "schema_version": 1,
        "scopes": [
            {
                "canonical_key": _CANONICAL_FOLDER_KEY,
                "webhook_ids": [webhook_inbox_id],
                "sync_folder": sync_folder,
                "event_policy_matrix": [
                    {
                        "source": source.value,
                        "raw_event_type": event_type,
                        "change_kind": kind.value,
                        "processing_policy": policy.value,
                    }
                    for source, event_type, kind, policy in _POLICY_ENTRIES
                ],
            }
        ],
    }
    return payload, snapshot


def _prepare_payloads(
    *,
    account_id: int,
    webhook_inbox_id: str,
    sync_folder: str,
    build_id: str,
    release_evidence_file: Path,
    project_root: Path = PROJECT_ROOT,
) -> tuple[dict[str, object], dict[str, object]]:
    exact_account_id = _require_account_id(account_id)
    exact_webhook_id = _require_webhook_id(webhook_inbox_id)
    exact_sync_folder = _require_text("sync_folder", sync_folder, maximum=512)
    exact_build_id = _require_build_id(build_id)
    evidence = _load_release_evidence(
        release_evidence_file,
        build_id=exact_build_id,
    )
    git_identity = _require_clean_git_tree(project_root)
    if evidence["source_revision"] != git_identity["revision"]:
        raise ManifestPreparationError("release_evidence_source_mismatch")

    policy, snapshot = _policy_payload(exact_webhook_id, exact_sync_folder)
    policy_hash = canonical_policy_manifest(snapshot).hash
    schema_manifest = _committed_tree_manifest(
        git_identity,
        kind="greenfield_schema",
        fixed={
            "schema_revision": PHASE2_SCHEMA_REVISION,
            "protocol_version": _PROTOCOL_VERSION,
        },
    )
    adapter_manifest = _committed_tree_manifest(
        git_identity,
        kind="durable_processing_adapter",
        fixed={
            "pipeline_name": GREENFIELD_PIPELINE_NAME,
            "protocol_version": _PROTOCOL_VERSION,
        },
    )
    fixed_config = {
        "schema_version": 1,
        "account_id": exact_account_id,
        "folder_scope": {
            "canonical_key": _CANONICAL_FOLDER_KEY,
            "sync_folder": exact_sync_folder,
            "webhook_ids": [exact_webhook_id],
        },
        "features": {
            "durable_inbox_enabled": True,
            "ingestion_shadow_enabled": False,
            "polling_enabled": False,
            "sync_reconciliation_enabled": False,
            "webhook_primary": True,
        },
        "topology": {
            "exchange_accounts": 1,
            "feishu_accounts": 1,
            "processes": 1,
            "workers": 1,
        },
    }
    contract: dict[str, object] = {
        "schema_digest": _domain_hash(_SCHEMA_HASH_DOMAIN, schema_manifest),
        "protocol_version": _PROTOCOL_VERSION,
        "build_id": exact_build_id,
        "config_hash": _domain_hash(_CONFIG_HASH_DOMAIN, fixed_config),
        "adapter_hash": _domain_hash(_ADAPTER_HASH_DOMAIN, adapter_manifest),
        "evidence_manifest_hash": _domain_hash(_EVIDENCE_HASH_DOMAIN, evidence),
    }
    capability = RuntimeCapabilityManifest(
        stage=RuntimeCapabilityStage.PHASE2_INGESTION,
        schema_revision=PHASE2_SCHEMA_REVISION,
        schema_digest=contract["schema_digest"],  # type: ignore[arg-type]
        protocol_version=_PROTOCOL_VERSION,
        minimum_build_id=exact_build_id,
        config_hash=contract["config_hash"],  # type: ignore[arg-type]
        adapter_hash=contract["adapter_hash"],  # type: ignore[arg-type]
        policy_manifest_hash=policy_hash,
        evidence_manifest_hash=contract["evidence_manifest_hash"],  # type: ignore[arg-type]
        predecessor_hash=CAPABILITY_CHAIN_ROOT_HASH,
    )
    RuntimeContract(
        schema_revision=PHASE2_SCHEMA_REVISION,
        schema_digest=capability.schema_digest,
        protocol_version=capability.protocol_version,
        build_id=capability.minimum_build_id,
        config_hash=capability.config_hash,
        capability_manifest=capability,
    )
    return policy, contract


def _json_document(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _write_new_file(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_output_directory(
    output_dir: Path,
    *,
    policy: Mapping[str, object],
    contract: Mapping[str, object],
) -> None:
    if os.path.lexists(output_dir):
        raise ManifestPreparationError("output_dir_exists")
    parent = output_dir.parent
    try:
        parent_metadata = parent.lstat()
        if not stat.S_ISDIR(parent_metadata.st_mode):
            raise ManifestPreparationError("output_parent_invalid")
        output_dir.mkdir(mode=0o700)
        os.chmod(output_dir, 0o700)
    except ManifestPreparationError:
        raise
    except OSError:
        raise ManifestPreparationError("output_dir_create_failed") from None

    created: list[Path] = []
    try:
        for name, payload in (
            ("POLICY.json", _json_document(policy)),
            ("CONTRACT.json", _json_document(contract)),
        ):
            path = output_dir / name
            created.append(path)
            _write_new_file(path, payload)
    except OSError:
        for path in reversed(created):
            try:
                path.unlink()
            except OSError:
                pass
        try:
            output_dir.rmdir()
        except OSError:
            pass
        raise ManifestPreparationError("output_write_failed") from None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", required=True, type=int)
    parser.add_argument("--webhook-inbox-id", required=True)
    parser.add_argument("--sync-folder", required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--release-evidence-file", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser


def run(args: argparse.Namespace) -> dict[str, object]:
    policy, contract = _prepare_payloads(
        account_id=args.account_id,
        webhook_inbox_id=args.webhook_inbox_id,
        sync_folder=args.sync_folder,
        build_id=args.build_id,
        release_evidence_file=args.release_evidence_file,
    )
    _write_output_directory(args.output_dir, policy=policy, contract=contract)
    return {
        "account_id": args.account_id,
        "build_id": args.build_id,
        "files": ["CONTRACT.json", "POLICY.json"],
        "status": "prepared",
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(build_parser().parse_args(argv))
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except ManifestPreparationError as exc:
        print(
            json.dumps(
                {"error": str(exc), "status": "failed"},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2
    except (OSError, TypeError, ValueError):
        print(
            json.dumps(
                {"error": "manifest_preparation_failed", "status": "failed"},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
