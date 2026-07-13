#!/usr/bin/env python3
"""Create or execute a guarded checkpoint-cleanup plan.

The Phase 1 production gate invokes only the ``plan`` subcommand. It creates an
immutable plan and immediately revalidates it as a dry-run; no delete path is
entered.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import stat
import sys
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.db.auditor import require_checkpoint_auditor_database_role  # noqa: E402
from src.db.maintenance_settings import (  # noqa: E402
    CheckpointPlanSettings,
    MaintenanceSettings,
    load_checkpoint_plan_settings,
    load_maintenance_settings,
)
from src.db.migration_settings import (  # noqa: E402
    MigrationSettingsError,
    _read_secret_file,
)
from src.db.roles import require_maintenance_database_role  # noqa: E402
from src.maintenance.checkpoint_cleanup import (  # noqa: E402
    CleanupAuthorizationError,
    CleanupPlanError,
    CheckpointCleaner,
)
from src.maintenance.cleanup_backup import (  # noqa: E402
    MAX_BACKUP_RECEIPT_BYTES,
    BackupReceiptError,
    Ed25519BackupReceiptVerifier,
)


RECEIPT_PUBLIC_KEY_FILE_ENV = "CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE"
_CLI_ERROR_CODES = frozenset(
    {
        "plan_confirmation_mismatch",
        "backup_receipt_file_unsafe",
        "backup_receipt_file_too_large",
        "backup_receipt_key_unavailable",
        "backup_receipt_key_invalid",
        "checkpoint_cleanup_failed",
    }
)


class CliSafetyError(ValueError):
    """A fixed-code local CLI boundary failure."""

    def __init__(self, code: str) -> None:
        safe_code = code if code in _CLI_ERROR_CODES else "checkpoint_cleanup_failed"
        self.code = safe_code
        super().__init__(safe_code)


def _default_state_dir() -> Path:
    state_home = os.environ.get("XDG_STATE_HOME")
    if state_home:
        return Path(state_home).expanduser() / "ai-exchange" / "checkpoint-cleanup"
    return Path.home() / ".local" / "state" / "ai-exchange" / "checkpoint-cleanup"


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected a positive integer") from None
    if parsed < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Guarded LangGraph checkpoint cleanup",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser(
        "plan",
        help="create an immutable plan and run a read-only revalidation",
    )
    plan.add_argument(
        "--older-than-hours",
        type=_positive_int,
        default=24,
    )
    plan.add_argument("--limit", type=_positive_int, default=100)
    plan.add_argument(
        "--state-dir",
        type=Path,
        default=_default_state_dir(),
    )

    execute = subparsers.add_parser(
        "execute",
        help=(
            "execute one exact plan after backup verification and an operator "
            "quiescence attestation"
        ),
    )
    execute.add_argument("--plan-id", required=True)
    execute.add_argument("--confirm-plan-id", required=True)
    execute.add_argument("--backup-id", required=True)
    execute.add_argument("--backup-receipt", type=Path, required=True)
    execute.add_argument(
        "--operator-attests-service-quiesced",
        dest="service_quiesced",
        action="store_true",
        required=True,
    )
    execute.add_argument("--limit", type=_positive_int, required=True)
    execute.add_argument(
        "--state-dir",
        type=Path,
        default=_default_state_dir(),
    )
    return parser


def _read_private_receipt(path: Path) -> bytes:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow == 0:
        raise CliSafetyError("backup_receipt_file_unsafe")
    flags = (
        os.O_RDONLY
        | nofollow
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except (OSError, TypeError, ValueError):
        raise CliSafetyError("backup_receipt_file_unsafe") from None

    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise CliSafetyError("backup_receipt_file_unsafe")
        if metadata.st_size < 1 or metadata.st_size > MAX_BACKUP_RECEIPT_BYTES:
            raise CliSafetyError("backup_receipt_file_too_large")
        raw = os.read(descriptor, MAX_BACKUP_RECEIPT_BYTES + 1)
    except OSError:
        raise CliSafetyError("backup_receipt_file_unsafe") from None
    finally:
        os.close(descriptor)

    if not raw or len(raw) > MAX_BACKUP_RECEIPT_BYTES:
        raise CliSafetyError("backup_receipt_file_too_large")
    return raw


def _load_receipt_public_key(
    environment: Mapping[str, str] | None = None,
) -> bytes:
    values = os.environ if environment is None else environment
    key_path = values.get(RECEIPT_PUBLIC_KEY_FILE_ENV, "")
    if not isinstance(key_path, str) or not key_path:
        raise CliSafetyError("backup_receipt_key_unavailable")
    try:
        encoded = _read_secret_file(key_path)
    except MigrationSettingsError:
        raise CliSafetyError("backup_receipt_key_invalid") from None
    try:
        public_key = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise CliSafetyError("backup_receipt_key_invalid") from None
    if len(public_key) != 32:
        raise CliSafetyError("backup_receipt_key_invalid")
    return public_key


def _build_cleaner(
    *,
    state_dir: Path,
    require_backup_verifier: bool,
    maintenance_settings: MaintenanceSettings | CheckpointPlanSettings | None = None,
) -> CheckpointCleaner:
    # Imports remain local so argument/confirmation failures cannot initialize
    # a database path, and this command never initializes AppContext.
    from src.maintenance.checkpoint_repository import (
        PostgresCheckpointRepository,
    )
    from src.maintenance.cleanup_artifacts import PlanArtifactStore

    if maintenance_settings is None:
        maintenance_settings = load_maintenance_settings()
    verifier = None
    if require_backup_verifier:
        verifier = Ed25519BackupReceiptVerifier(_load_receipt_public_key())
    return CheckpointCleaner(
        repository=PostgresCheckpointRepository(
            maintenance_settings.database_url.get_secret_value()
        ),
        artifact_store=PlanArtifactStore(state_dir),
        backup_verifier=verifier,
    )


async def _build_preflighted_cleaner(
    *,
    state_dir: Path,
    require_backup_verifier: bool,
) -> CheckpointCleaner:
    if not require_backup_verifier:
        plan_settings = load_checkpoint_plan_settings()
        plan_dsn = plan_settings.database_url.get_secret_value()
        await require_checkpoint_auditor_database_role(
            plan_dsn,
            expected_auditor_role=plan_settings.expected_auditor_role,
            expected_maintenance_role=plan_settings.expected_maintenance_role,
            expected_runtime_role=plan_settings.expected_runtime_role,
            expected_migration_role=plan_settings.expected_migration_role,
            target_schema=plan_settings.target_schema,
        )
        return _build_cleaner(
            state_dir=state_dir,
            require_backup_verifier=False,
            maintenance_settings=plan_settings,
        )

    maintenance_settings = load_maintenance_settings()
    dsn = maintenance_settings.database_url.get_secret_value()
    await require_maintenance_database_role(
        dsn,
        expected_maintenance_role=(maintenance_settings.expected_maintenance_role),
        expected_runtime_role=maintenance_settings.expected_runtime_role,
        expected_migration_role=maintenance_settings.expected_migration_role,
        expected_auditor_role=maintenance_settings.expected_auditor_role,
        target_schema=maintenance_settings.target_schema,
    )
    return _build_cleaner(
        state_dir=state_dir,
        require_backup_verifier=require_backup_verifier,
        maintenance_settings=maintenance_settings,
    )


def _emit(payload: dict[str, object]) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _public_summary(value: object) -> dict[str, object]:
    summary = getattr(value, "public_summary", None)
    if not callable(summary):
        raise CliSafetyError("checkpoint_cleanup_failed")
    result = summary()
    if not isinstance(result, dict):
        raise CliSafetyError("checkpoint_cleanup_failed")
    return result


def _safe_error_code(exc: BaseException) -> str:
    code = getattr(exc, "code", None)
    return _normalize_error_code(code)


def _normalize_error_code(code: object) -> str:
    if isinstance(code, str) and code and len(code) <= 128:
        if all(
            character.islower() or character.isdigit() or character == "_"
            for character in code
        ):
            return code
    return "checkpoint_cleanup_failed"


async def async_main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.command == "execute" and args.plan_id != args.confirm_plan_id:
        _emit({"ok": False, "error_code": "plan_confirmation_mismatch"})
        return 2

    try:
        if args.command == "plan":
            cleaner = await _build_preflighted_cleaner(
                state_dir=args.state_dir,
                require_backup_verifier=False,
            )
            cutoff = datetime.now(UTC) - timedelta(hours=args.older_than_hours)
            plan = await cleaner.plan(older_than=cutoff, limit=args.limit)
            report = await cleaner.run(
                plan.plan_id,
                dry_run=True,
                backup_id=None,
                limit=args.limit,
            )
            report_summary = _public_summary(report)
            error_code = report_summary.get("error_code")
            payload = {
                "ok": error_code is None,
                "operation": "dry_run",
                "plan": _public_summary(plan),
                "report": report_summary,
            }
            if error_code is not None:
                payload["error_code"] = _normalize_error_code(error_code)
            _emit(payload)
            return 0 if error_code is None else 1

        receipt = _read_private_receipt(args.backup_receipt)
        cleaner = await _build_preflighted_cleaner(
            state_dir=args.state_dir,
            require_backup_verifier=True,
        )
        report = await cleaner.run(
            args.plan_id,
            dry_run=False,
            backup_id=args.backup_id,
            backup_receipt=receipt,
            service_quiesced=args.service_quiesced,
            limit=args.limit,
        )
        report_summary = _public_summary(report)
        error_code = report_summary.get("error_code")
        payload = {
            "ok": error_code is None,
            "operation": "execute",
            "report": report_summary,
        }
        if error_code is not None:
            payload["error_code"] = _normalize_error_code(error_code)
        _emit(payload)
        return 0 if error_code is None else 1
    except (
        BackupReceiptError,
        CleanupAuthorizationError,
        CleanupPlanError,
        CliSafetyError,
    ) as exc:
        _emit({"ok": False, "error_code": _safe_error_code(exc)})
        return 2
    except Exception as exc:
        # Artifact/repository errors expose only a fixed .code; all other
        # exception text is deliberately discarded.
        _emit({"ok": False, "error_code": _safe_error_code(exc)})
        return 1


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
