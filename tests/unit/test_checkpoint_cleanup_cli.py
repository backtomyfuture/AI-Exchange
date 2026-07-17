from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from scripts import checkpoint_cleanup
from src.maintenance.checkpoint_cleanup import CleanupPlanError


PLAN_ID = "a" * 64
PRIVATE_SENTINEL = "private-thread-id-never-print"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class _Cleaner:
    def __init__(self) -> None:
        self.plan_calls: list[dict[str, object]] = []
        self.run_calls: list[tuple[str, dict[str, object]]] = []
        self.plan_result = SimpleNamespace(
            plan_id=PLAN_ID,
            public_summary=lambda: {
                "plan_id": PLAN_ID,
                "candidate_count": 1,
                "estimated_logical_bytes": 123,
            },
        )
        self.report_result = SimpleNamespace(
            public_summary=lambda: {
                "plan_id": PLAN_ID,
                "dry_run": True,
                "deleted_thread_count": 0,
                "candidate_count": 1,
            },
        )

    async def plan(self, **kwargs):
        self.plan_calls.append(kwargs)
        return self.plan_result

    async def run(self, plan_id: str, **kwargs):
        self.run_calls.append((plan_id, kwargs))
        return self.report_result


def _json_stdout(capsys) -> dict[str, object]:
    output = capsys.readouterr().out.strip()
    assert output
    return json.loads(output)


async def _async_value(value: object) -> object:
    return value


def test_cli_requires_an_explicit_safe_subcommand() -> None:
    parser = checkpoint_cleanup._build_parser()

    with pytest.raises(SystemExit) as caught:
        parser.parse_args([])

    assert caught.value.code == 2


def test_cli_direct_script_entrypoint_can_resolve_project_imports(
    tmp_path: Path,
) -> None:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "checkpoint_cleanup.py"),
            "--help",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "checkpoint cleanup" in result.stdout.lower()


@pytest.mark.asyncio
async def test_plan_subcommand_always_runs_dry_run_and_prints_public_summary(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    cleaner = _Cleaner()
    monkeypatch.setattr(
        checkpoint_cleanup,
        "_build_preflighted_cleaner",
        lambda **_kwargs: _async_value(cleaner),
    )

    exit_code = await checkpoint_cleanup.async_main(
        [
            "plan",
            "--older-than-hours",
            "24",
            "--limit",
            "7",
            "--state-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 0
    assert len(cleaner.plan_calls) == 1
    assert cleaner.plan_calls[0]["limit"] == 7
    assert cleaner.run_calls == [
        (
            PLAN_ID,
            {
                "dry_run": True,
                "backup_id": None,
                "limit": 7,
            },
        )
    ]
    payload = _json_stdout(capsys)
    assert payload["ok"] is True
    assert payload["operation"] == "dry_run"
    assert payload["plan"]["plan_id"] == PLAN_ID
    assert payload["report"]["deleted_thread_count"] == 0
    assert PRIVATE_SENTINEL not in json.dumps(payload)


@pytest.mark.asyncio
async def test_execute_confirmation_mismatch_fails_before_building_cleaner(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    built = False

    async def fail_build(**_kwargs):
        nonlocal built
        built = True
        raise AssertionError

    monkeypatch.setattr(
        checkpoint_cleanup,
        "_build_preflighted_cleaner",
        fail_build,
    )
    receipt = tmp_path / "receipt.json"
    receipt.write_bytes(b"{}")
    receipt.chmod(0o600)

    exit_code = await checkpoint_cleanup.async_main(
        [
            "execute",
            "--plan-id",
            PLAN_ID,
            "--confirm-plan-id",
            "b" * 64,
            "--backup-id",
            "backup-1",
            "--backup-receipt",
            str(receipt),
            "--operator-attests-service-quiesced",
            "--limit",
            "1",
            "--state-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 2
    assert built is False
    assert _json_stdout(capsys) == {
        "ok": False,
        "error_code": "plan_confirmation_mismatch",
    }


def test_execute_requires_explicit_operator_quiescence_attestation(
    tmp_path: Path,
) -> None:
    parser = checkpoint_cleanup._build_parser()
    receipt = tmp_path / "receipt.json"

    with pytest.raises(SystemExit) as caught:
        parser.parse_args(
            [
                "execute",
                "--plan-id",
                PLAN_ID,
                "--confirm-plan-id",
                PLAN_ID,
                "--backup-id",
                "backup-1",
                "--backup-receipt",
                str(receipt),
                "--limit",
                "1",
            ]
        )

    assert caught.value.code == 2


@pytest.mark.asyncio
async def test_execute_passes_private_receipt_and_never_prints_it(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    cleaner = _Cleaner()
    cleaner.report_result = SimpleNamespace(
        public_summary=lambda: {
            "plan_id": PLAN_ID,
            "dry_run": False,
            "deleted_thread_count": 1,
        }
    )
    monkeypatch.setattr(
        checkpoint_cleanup,
        "_build_preflighted_cleaner",
        lambda **_kwargs: _async_value(cleaner),
    )
    receipt_bytes = b'{"private":"receipt-sentinel"}'
    receipt = tmp_path / "receipt.json"
    receipt.write_bytes(receipt_bytes)
    receipt.chmod(0o600)

    exit_code = await checkpoint_cleanup.async_main(
        [
            "execute",
            "--plan-id",
            PLAN_ID,
            "--confirm-plan-id",
            PLAN_ID,
            "--backup-id",
            "backup-1",
            "--backup-receipt",
            str(receipt),
            "--operator-attests-service-quiesced",
            "--limit",
            "1",
            "--state-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 0
    assert cleaner.plan_calls == []
    assert cleaner.run_calls == [
        (
            PLAN_ID,
            {
                "dry_run": False,
                "backup_id": "backup-1",
                "backup_receipt": receipt_bytes,
                "service_quiesced": True,
                "limit": 1,
            },
        )
    ]
    output = capsys.readouterr().out
    assert "receipt-sentinel" not in output
    assert str(receipt) not in output


@pytest.mark.asyncio
async def test_cli_converts_domain_failures_to_fixed_codes(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    class FailingCleaner(_Cleaner):
        async def plan(self, **kwargs):
            raise CleanupPlanError("retention_period_too_short")

    monkeypatch.setattr(
        checkpoint_cleanup,
        "_build_preflighted_cleaner",
        lambda **_kwargs: _async_value(FailingCleaner()),
    )

    exit_code = await checkpoint_cleanup.async_main(
        ["plan", "--state-dir", str(tmp_path)]
    )

    assert exit_code == 2
    assert _json_stdout(capsys) == {
        "ok": False,
        "error_code": "retention_period_too_short",
    }


@pytest.mark.asyncio
async def test_cli_returns_failure_when_execution_report_stops_on_error(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    cleaner = _Cleaner()
    cleaner.report_result = SimpleNamespace(
        error_code="cleanup_delete_failed",
        public_summary=lambda: {
            "plan_id": PLAN_ID,
            "dry_run": False,
            "deleted_thread_count": 0,
            "error_code": "cleanup_delete_failed",
        },
    )
    monkeypatch.setattr(
        checkpoint_cleanup,
        "_build_preflighted_cleaner",
        lambda **_kwargs: _async_value(cleaner),
    )
    receipt = tmp_path / "receipt.json"
    receipt.write_bytes(b"{}")
    receipt.chmod(0o600)

    exit_code = await checkpoint_cleanup.async_main(
        [
            "execute",
            "--plan-id",
            PLAN_ID,
            "--confirm-plan-id",
            PLAN_ID,
            "--backup-id",
            "backup-1",
            "--backup-receipt",
            str(receipt),
            "--operator-attests-service-quiesced",
            "--limit",
            "1",
            "--state-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 1
    payload = _json_stdout(capsys)
    assert payload["ok"] is False
    assert payload["error_code"] == "cleanup_delete_failed"
    assert payload["report"]["deleted_thread_count"] == 0


@pytest.mark.parametrize("mode", [0o644, 0o400, 0o666])
def test_receipt_reader_rejects_unsafe_file_modes(
    tmp_path: Path,
    mode: int,
) -> None:
    receipt = tmp_path / "receipt.json"
    receipt.write_bytes(b"{}")
    receipt.chmod(mode)

    with pytest.raises(checkpoint_cleanup.CliSafetyError) as caught:
        checkpoint_cleanup._read_private_receipt(receipt)

    assert caught.value.code == "backup_receipt_file_unsafe"


def test_receipt_reader_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_bytes(b"{}")
    target.chmod(0o600)
    link = tmp_path / "receipt.json"
    os.symlink(target, link)

    with pytest.raises(checkpoint_cleanup.CliSafetyError) as caught:
        checkpoint_cleanup._read_private_receipt(link)

    assert caught.value.code == "backup_receipt_file_unsafe"


def test_receipt_reader_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    receipt = tmp_path / "receipt.pipe"
    os.mkfifo(receipt, mode=0o600)

    with pytest.raises(checkpoint_cleanup.CliSafetyError) as caught:
        checkpoint_cleanup._read_private_receipt(receipt)

    assert caught.value.code == "backup_receipt_file_unsafe"


def test_execute_public_key_is_loaded_only_from_control_plane_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    encoded_key = base64.b64encode(b"k" * 32).decode("ascii")
    key_file = tmp_path / "maintenance-receipt-key"
    key_file.write_text(encoded_key, encoding="utf-8")
    key_file.chmod(0o400)
    environment = {
        "CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE": str(key_file),
        # The legacy symmetric signing secret must never win over the public
        # verification key file.
        "CHECKPOINT_CLEANUP_RECEIPT_HMAC_KEY_B64": base64.b64encode(
            b"wrong" * 8
        ).decode("ascii"),
    }

    key = checkpoint_cleanup._load_receipt_public_key(environment)

    assert key == b"k" * 32


@pytest.mark.parametrize("mode", [0o644, 0o440, 0o666])
def test_execute_public_key_rejects_unsafe_file_modes(
    tmp_path: Path,
    mode: int,
) -> None:
    key_file = tmp_path / "maintenance-receipt-key"
    key_file.write_text(
        base64.b64encode(b"k" * 32).decode("ascii"),
        encoding="utf-8",
    )
    key_file.chmod(mode)

    with pytest.raises(checkpoint_cleanup.CliSafetyError) as caught:
        checkpoint_cleanup._load_receipt_public_key(
            {"CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE": str(key_file)}
        )

    assert caught.value.code == "backup_receipt_key_invalid"


def test_execute_public_key_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "maintenance-receipt-key-target"
    target.write_text(
        base64.b64encode(b"k" * 32).decode("ascii"),
        encoding="utf-8",
    )
    target.chmod(0o400)
    link = tmp_path / "maintenance-receipt-key"
    link.symlink_to(target)

    with pytest.raises(checkpoint_cleanup.CliSafetyError) as caught:
        checkpoint_cleanup._load_receipt_public_key(
            {"CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE": str(link)}
        )

    assert caught.value.code == "backup_receipt_key_invalid"


def test_execute_public_key_never_falls_back_to_symmetric_runtime_secret() -> None:
    encoded_key = base64.b64encode(b"k" * 32).decode("ascii")

    with pytest.raises(checkpoint_cleanup.CliSafetyError) as caught:
        checkpoint_cleanup._load_receipt_public_key(
            {"CHECKPOINT_CLEANUP_RECEIPT_HMAC_KEY_B64": encoded_key}
        )

    assert caught.value.code == "backup_receipt_key_unavailable"


@pytest.mark.asyncio
async def test_build_cleaner_preflights_exact_maintenance_identity_before_repository(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from src.maintenance import checkpoint_repository

    maintenance_settings = SimpleNamespace(
        database_url=SecretStr("postgresql://maintenance:private@postgres/db"),
        expected_maintenance_role="maintenance",
        expected_runtime_role="runtime",
        expected_migration_role="migration",
        expected_auditor_role="auditor",
        target_schema="public",
    )
    events: list[object] = []

    async def preflight(dsn: str, **identity: str) -> None:
        events.append(("preflight", dsn, identity))

    class Repository:
        def __init__(self, dsn: str) -> None:
            assert events and events[0][0] == "preflight"
            self._dsn = dsn
            events.append(("repository", dsn))

    monkeypatch.setattr(
        checkpoint_cleanup,
        "load_maintenance_settings",
        lambda: maintenance_settings,
    )
    monkeypatch.setattr(
        checkpoint_cleanup,
        "require_maintenance_database_role",
        preflight,
    )
    monkeypatch.setattr(
        checkpoint_cleanup,
        "_load_receipt_public_key",
        lambda: b"k" * 32,
    )
    monkeypatch.setattr(
        checkpoint_repository,
        "PostgresCheckpointRepository",
        Repository,
    )

    cleaner = await checkpoint_cleanup._build_preflighted_cleaner(
        state_dir=tmp_path / "artifacts",
        require_backup_verifier=True,
    )

    assert cleaner._repository._dsn == (  # type: ignore[attr-defined]
        "postgresql://maintenance:private@postgres/db"
    )
    assert events == [
        (
            "preflight",
            "postgresql://maintenance:private@postgres/db",
            {
                "expected_maintenance_role": "maintenance",
                "expected_runtime_role": "runtime",
                "expected_migration_role": "migration",
                "expected_auditor_role": "auditor",
                "target_schema": "public",
            },
        ),
        ("repository", "postgresql://maintenance:private@postgres/db"),
    ]


@pytest.mark.asyncio
async def test_plan_preflights_read_only_auditor_before_repository(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from src.maintenance import checkpoint_repository

    plan_settings = SimpleNamespace(
        database_url=SecretStr("postgresql://auditor:private@postgres/db"),
        expected_auditor_role="auditor",
        expected_maintenance_role="maintenance",
        expected_runtime_role="runtime",
        expected_migration_role="migration",
        target_schema="public",
    )
    events: list[object] = []

    async def preflight(dsn: str, **identity: str) -> None:
        events.append(("preflight", dsn, identity))

    class Repository:
        def __init__(self, dsn: str) -> None:
            assert events and events[0][0] == "preflight"
            self._dsn = dsn
            events.append(("repository", dsn))

    monkeypatch.setattr(
        checkpoint_cleanup,
        "load_checkpoint_plan_settings",
        lambda: plan_settings,
    )
    monkeypatch.setattr(
        checkpoint_cleanup,
        "require_checkpoint_auditor_database_role",
        preflight,
    )
    monkeypatch.setattr(
        checkpoint_repository,
        "PostgresCheckpointRepository",
        Repository,
    )

    cleaner = await checkpoint_cleanup._build_preflighted_cleaner(
        state_dir=tmp_path / "artifacts",
        require_backup_verifier=False,
    )

    assert cleaner._repository._dsn == (  # type: ignore[attr-defined]
        "postgresql://auditor:private@postgres/db"
    )
    assert events == [
        (
            "preflight",
            "postgresql://auditor:private@postgres/db",
            {
                "expected_auditor_role": "auditor",
                "expected_maintenance_role": "maintenance",
                "expected_runtime_role": "runtime",
                "expected_migration_role": "migration",
                "target_schema": "public",
            },
        ),
        ("repository", "postgresql://auditor:private@postgres/db"),
    ]


@pytest.mark.asyncio
async def test_build_cleaner_does_not_construct_repository_when_preflight_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from src.maintenance import checkpoint_repository

    maintenance_settings = SimpleNamespace(
        database_url=SecretStr("postgresql://maintenance:private@postgres/db"),
        expected_maintenance_role="maintenance",
        expected_runtime_role="runtime",
        expected_migration_role="migration",
        expected_auditor_role="auditor",
        target_schema="public",
    )
    repository_constructed = False

    async def fail_preflight(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("database_role_preflight_failed")

    class Repository:
        def __init__(self, _dsn: str) -> None:
            nonlocal repository_constructed
            repository_constructed = True

    monkeypatch.setattr(
        checkpoint_cleanup,
        "load_maintenance_settings",
        lambda: maintenance_settings,
    )
    monkeypatch.setattr(
        checkpoint_cleanup,
        "require_maintenance_database_role",
        fail_preflight,
    )
    monkeypatch.setattr(
        checkpoint_repository,
        "PostgresCheckpointRepository",
        Repository,
    )

    with pytest.raises(RuntimeError, match="database_role_preflight_failed"):
        await checkpoint_cleanup._build_preflighted_cleaner(
            state_dir=tmp_path / "artifacts",
            require_backup_verifier=True,
        )

    assert repository_constructed is False
