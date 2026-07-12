from __future__ import annotations

import json
import os
import base64
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import checkpoint_cleanup
from src.config import Settings
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
        "_build_cleaner",
        lambda **_kwargs: cleaner,
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

    def fail_build(**_kwargs):
        nonlocal built
        built = True
        raise AssertionError

    monkeypatch.setattr(checkpoint_cleanup, "_build_cleaner", fail_build)
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
            "--service-quiesced",
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


def test_execute_requires_explicit_quiescence_flag(tmp_path: Path) -> None:
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
        "_build_cleaner",
        lambda **_kwargs: cleaner,
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
            "--service-quiesced",
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
        "_build_cleaner",
        lambda **_kwargs: FailingCleaner(),
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
        "_build_cleaner",
        lambda **_kwargs: cleaner,
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
            "--service-quiesced",
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


def test_execute_key_can_be_loaded_from_settings_env_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    encoded_key = base64.b64encode(b"k" * 32).decode("ascii")
    env_file = tmp_path / ".env"
    env_file.write_text(
        f"CHECKPOINT_CLEANUP_RECEIPT_HMAC_KEY_B64={encoded_key}\n",
        encoding="utf-8",
    )
    settings = Settings(_env_file=env_file)
    monkeypatch.delenv(
        checkpoint_cleanup.RECEIPT_KEY_ENV,
        raising=False,
    )
    monkeypatch.setattr(checkpoint_cleanup, "get_settings", lambda: settings)

    cleaner = checkpoint_cleanup._build_cleaner(
        state_dir=tmp_path / "artifacts",
        require_backup_verifier=True,
    )

    assert cleaner._backup_verifier is not None  # type: ignore[attr-defined]
