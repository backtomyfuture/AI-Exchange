#!/usr/bin/env python3
"""Explicit operator commands for the one greenfield ingestion authority."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.migration_settings import _read_secret_file  # noqa: E402
from src.db.roles import require_maintenance_database_role  # noqa: E402
from src.ingestion.models import ChangeKind, IngressSource, ProcessingPolicy  # noqa: E402
from src.ingestion.policy import FolderScope, PolicySnapshot  # noqa: E402
from src.ingestion.recovery import InboxRecoveryService, RequeueCommand  # noqa: E402
from src.ingestion.runtime_authority import (  # noqa: E402
    GreenfieldInitializer,
    RuntimeAuthorityRepository,
    RuntimeContract,
    canonical_policy_manifest,
)
from src.ingestion.runtime_capability import (  # noqa: E402
    CAPABILITY_CHAIN_ROOT_HASH,
    PHASE2_SCHEMA_REVISION,
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
)


_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z", re.ASCII)
_MAX_OPERATOR_JSON_BYTES = 1_048_576
_OPERATOR_DSN_ENV = "INGESTION_MAINTENANCE_DATABASE_URL_FILE"


class OperatorCommandError(RuntimeError):
    """Safe, non-secret operator command failure."""


@dataclass(frozen=True, slots=True)
class _OperatorDatabaseSettings:
    dsn: str
    runtime_role: str
    migration_role: str
    maintenance_role: str
    auditor_role: str
    target_schema: str


def _safe_fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _print_json(payload: Mapping[str, object]) -> None:
    print(
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )


def _load_json_object(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    try:
        metadata = path.stat()
        if not path.is_file() or not 0 < metadata.st_size <= _MAX_OPERATOR_JSON_BYTES:
            raise OperatorCommandError("operator_file_invalid")
        payload = path.read_bytes()
        if len(payload) != metadata.st_size:
            raise OperatorCommandError("operator_file_invalid")
        value = json.loads(payload.decode("utf-8"))
    except OperatorCommandError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise OperatorCommandError("operator_file_invalid") from None
    if type(value) is not dict:
        raise OperatorCommandError("operator_file_invalid")
    return value


def _policy_snapshot(path_value: str) -> PolicySnapshot:
    value = _load_json_object(path_value)
    if set(value) != {"schema_version", "scopes"} or value["schema_version"] != 1:
        raise OperatorCommandError("policy_manifest_invalid")
    raw_scopes = value["scopes"]
    if type(raw_scopes) is not list or not raw_scopes:
        raise OperatorCommandError("policy_manifest_invalid")
    scopes: list[FolderScope] = []
    try:
        for raw_scope in raw_scopes:
            if type(raw_scope) is not dict or set(raw_scope) != {
                "canonical_key",
                "webhook_ids",
                "sync_folder",
                "event_policy_matrix",
            }:
                raise ValueError
            raw_matrix = raw_scope["event_policy_matrix"]
            if type(raw_matrix) is not list:
                raise ValueError
            matrix: dict[tuple[IngressSource, str, ChangeKind], ProcessingPolicy] = {}
            for entry in raw_matrix:
                if type(entry) is not dict or set(entry) != {
                    "source",
                    "raw_event_type",
                    "change_kind",
                    "processing_policy",
                }:
                    raise ValueError
                key = (
                    IngressSource(entry["source"]),
                    entry["raw_event_type"],
                    ChangeKind(entry["change_kind"]),
                )
                if key in matrix:
                    raise ValueError
                matrix[key] = ProcessingPolicy(entry["processing_policy"])
            scopes.append(
                FolderScope.configured(
                    canonical_key=raw_scope["canonical_key"],
                    webhook_ids=raw_scope["webhook_ids"],
                    sync_folder=raw_scope["sync_folder"],
                    event_policy_matrix=matrix,
                )
            )
        snapshot = PolicySnapshot(scopes=tuple(scopes))
        canonical_policy_manifest(snapshot)
        return snapshot
    except (KeyError, TypeError, ValueError):
        raise OperatorCommandError("policy_manifest_invalid") from None


def _runtime_contract(
    path_value: str,
    snapshot: PolicySnapshot,
) -> RuntimeContract:
    value = _load_json_object(path_value)
    if set(value) != {
        "schema_digest",
        "protocol_version",
        "build_id",
        "config_hash",
        "adapter_hash",
        "evidence_manifest_hash",
    }:
        raise OperatorCommandError("runtime_contract_invalid")
    policy_hash = canonical_policy_manifest(snapshot).hash
    try:
        capability = RuntimeCapabilityManifest(
            stage=RuntimeCapabilityStage.PHASE2_INGESTION,
            schema_revision=PHASE2_SCHEMA_REVISION,
            schema_digest=value["schema_digest"],
            protocol_version=value["protocol_version"],
            minimum_build_id=value["build_id"],
            config_hash=value["config_hash"],
            adapter_hash=value["adapter_hash"],
            policy_manifest_hash=policy_hash,
            evidence_manifest_hash=value["evidence_manifest_hash"],
            predecessor_hash=CAPABILITY_CHAIN_ROOT_HASH,
        )
        return RuntimeContract(
            schema_revision=capability.schema_revision,
            schema_digest=capability.schema_digest,
            protocol_version=capability.protocol_version,
            build_id=capability.minimum_build_id,
            config_hash=capability.config_hash,
            capability_manifest=capability,
        )
    except (TypeError, ValueError):
        raise OperatorCommandError("runtime_contract_invalid") from None


def _identifier(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise OperatorCommandError("operator_database_settings_invalid")
    return value


def _operator_database_settings(
    environment: Mapping[str, str] | None = None,
) -> _OperatorDatabaseSettings:
    values = os.environ if environment is None else environment
    try:
        dsn = _read_secret_file(values.get(_OPERATOR_DSN_ENV, ""))
        settings = _OperatorDatabaseSettings(
            dsn=dsn,
            runtime_role=_identifier(values, "POSTGRES_RUNTIME_ROLE"),
            migration_role=_identifier(values, "POSTGRES_MIGRATION_OWNER_ROLE"),
            maintenance_role=_identifier(values, "POSTGRES_MAINTENANCE_ROLE"),
            auditor_role=_identifier(values, "POSTGRES_CHECKPOINT_AUDITOR_ROLE"),
            target_schema=_identifier(values, "POSTGRES_SCHEMA"),
        )
        if (
            len(
                {
                    settings.runtime_role,
                    settings.migration_role,
                    settings.maintenance_role,
                    settings.auditor_role,
                }
            )
            != 4
        ):
            raise OperatorCommandError("operator_database_settings_invalid")
        return settings
    except OperatorCommandError:
        raise
    except Exception:
        raise OperatorCommandError("operator_database_settings_invalid") from None


async def _open_operator_pool() -> AsyncConnectionPool:
    settings = _operator_database_settings()
    await require_maintenance_database_role(
        settings.dsn,
        expected_maintenance_role=settings.maintenance_role,
        expected_runtime_role=settings.runtime_role,
        expected_migration_role=settings.migration_role,
        expected_auditor_role=settings.auditor_role,
        target_schema=settings.target_schema,
    )
    pool = AsyncConnectionPool(
        conninfo=settings.dsn,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    return pool


def _transition_payload(receipt) -> dict[str, object]:
    return {
        "account_id": receipt.authority.account_id,
        "authority_epoch": receipt.authority.authority_epoch,
        "command_receipt": _safe_fingerprint(receipt.command_receipt_id),
        "replayed": receipt.replayed,
        "state": receipt.authority.state.value,
        "version": receipt.authority.version,
    }


async def _initialize(args: argparse.Namespace) -> None:
    snapshot = _policy_snapshot(args.policy_file)
    contract = _runtime_contract(args.contract_file, snapshot)
    policy = canonical_policy_manifest(snapshot)
    preview = {
        "account_id": args.account_id,
        "build_id": contract.build_id,
        "capability_hash": contract.capability_manifest.capability_hash,
        "dry_run": bool(args.dry_run),
        "policy_manifest_hash": policy.hash,
        "schema_revision": contract.schema_revision,
        "scope_count": policy.scope_count,
    }
    if args.dry_run:
        _print_json(preview)
        return
    pool = await _open_operator_pool()
    try:
        receipt = await GreenfieldInitializer(pool).initialize(
            args.account_id,
            contract,
            snapshot,
            args.actor,
            args.reason,
            args.idempotency_key,
        )
    finally:
        await pool.close()
    _print_json(
        {
            **preview,
            "dry_run": False,
            "initialization": _safe_fingerprint(receipt.initialization_id),
            "replayed": receipt.replayed,
        }
    )


async def _status(args: argparse.Namespace) -> None:
    pool = await _open_operator_pool()
    try:
        authority = await RuntimeAuthorityRepository(pool).get(args.account_id)
    finally:
        await pool.close()
    if authority is None:
        _print_json({"account_id": args.account_id, "state": "uninitialized"})
        return
    _print_json(
        {
            "account_id": authority.account_id,
            "authority_epoch": authority.authority_epoch,
            "build_id": authority.build_id,
            "capability_hash": authority.capability_hash,
            "generation": authority.generation,
            "policy_manifest_hash": authority.policy_manifest_hash,
            "schema_revision": authority.schema_revision,
            "state": authority.state.value,
            "version": authority.version,
        }
    )


async def _transition(args: argparse.Namespace, *, resume: bool) -> None:
    pool = await _open_operator_pool()
    try:
        repository = RuntimeAuthorityRepository(pool)
        authority = await repository.get(args.account_id)
        if authority is None:
            raise OperatorCommandError("runtime_authority_unavailable")
        if resume:
            receipt = await repository.resume_ingress(
                authority,
                actor=args.actor,
                reason=args.reason,
                idempotency_key=args.idempotency_key,
            )
        else:
            receipt = await repository.pause(
                authority,
                actor=args.actor,
                reason=args.reason,
                idempotency_key=args.idempotency_key,
            )
    finally:
        await pool.close()
    _print_json(_transition_payload(receipt))


async def _requeue(args: argparse.Namespace) -> None:
    command = RequeueCommand(
        account_id=args.account_id,
        inbox_id=args.inbox_id,
        expected_execution_epoch=args.expected_execution_epoch,
        expected_email_version=args.expected_email_version,
        actor=args.actor,
        reason=args.reason,
        idempotency_key=args.idempotency_key,
    )
    pool = await _open_operator_pool()
    try:
        receipt = await InboxRecoveryService(pool).requeue(command)
    finally:
        await pool.close()
    _print_json(
        {
            "account_id": args.account_id,
            "command_receipt": _safe_fingerprint(receipt.command_receipt_id),
            "execution_epoch": receipt.execution_epoch,
            "inbox": _safe_fingerprint(receipt.inbox_id),
            "replayed": receipt.replayed,
            "status": receipt.status,
        }
    )


def _add_operator_identity(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--idempotency-key", required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    initialize = subparsers.add_parser("initialize")
    initialize.add_argument("--account-id", required=True, type=int)
    initialize.add_argument("--policy-file", required=True)
    initialize.add_argument("--contract-file", required=True)
    initialize.add_argument("--dry-run", action="store_true")
    _add_operator_identity(initialize)

    status = subparsers.add_parser("status")
    status.add_argument("--account-id", required=True, type=int)

    for command in ("pause", "resume-ingress"):
        transition = subparsers.add_parser(command)
        transition.add_argument("--account-id", required=True, type=int)
        _add_operator_identity(transition)

    requeue = subparsers.add_parser("requeue")
    requeue.add_argument("--account-id", required=True, type=int)
    requeue.add_argument("--inbox-id", required=True)
    requeue.add_argument("--expected-execution-epoch", required=True, type=int)
    requeue.add_argument("--expected-email-version", required=True, type=int)
    _add_operator_identity(requeue)
    return parser


async def run(args: argparse.Namespace) -> None:
    if args.command == "initialize":
        await _initialize(args)
    elif args.command == "status":
        await _status(args)
    elif args.command == "pause":
        await _transition(args, resume=False)
    elif args.command == "resume-ingress":
        await _transition(args, resume=True)
    elif args.command == "requeue":
        await _requeue(args)
    else:  # pragma: no cover - argparse enforces the exact command set.
        raise OperatorCommandError("operator_command_invalid")


def main(argv: list[str] | None = None) -> int:
    try:
        asyncio.run(run(build_parser().parse_args(argv)))
        return 0
    except (OperatorCommandError, RuntimeError, ValueError) as exc:
        print(
            json.dumps(
                {"error": type(exc).__name__, "status": "failed"},
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
