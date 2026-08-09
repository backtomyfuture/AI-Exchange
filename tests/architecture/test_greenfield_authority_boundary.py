"""Authority boundaries retained by the greenfield polling baseline."""

from __future__ import annotations

import re
from pathlib import Path

from src.db.access_contract import (
    MAINTENANCE_ROUTINE_EXECUTE,
    RUNTIME_ROUTINE_EXECUTE,
    SECURITY_DEFINER_ROUTINES,
)


ROOT = Path(__file__).resolve().parents[2]
BASELINE_SQL = ROOT / "alembic" / "versions" / "20260808_0001_polling_baseline.sql"
RUNTIME_AUTHORITY = ROOT / "src" / "ingestion" / "runtime_authority.py"
RECOVERY = ROOT / "src" / "ingestion" / "recovery.py"
RUNTIME = ROOT / "src" / "ingestion" / "runtime.py"


def _routine_names(source: str) -> set[str]:
    return set(re.findall(r"public\.(greenfield_[a-z_]+)", source))


def test_baseline_owns_the_exact_security_definer_routine_set() -> None:
    source = BASELINE_SQL.read_text(encoding="utf-8")
    expected = {routine.name for routine in SECURITY_DEFINER_ROUTINES}

    assert expected <= _routine_names(source)
    assert "greenfield_insert_webhook_event" not in source
    assert source.count("SECURITY DEFINER") == len(expected)


def test_authority_and_recovery_use_only_authorized_routines() -> None:
    source = "\n".join(
        (
            RUNTIME_AUTHORITY.read_text(encoding="utf-8"),
            RECOVERY.read_text(encoding="utf-8"),
        )
    )
    allowed = {
        *(routine.name for routine in RUNTIME_ROUTINE_EXECUTE),
        *(routine.name for routine in MAINTENANCE_ROUTINE_EXECUTE),
    }

    assert _routine_names(source) <= allowed
    assert "greenfield_insert_webhook_event" not in source


def test_runtime_authority_retains_explicit_operator_commands() -> None:
    source = RUNTIME_AUTHORITY.read_text(encoding="utf-8")

    assert "runtime.pause" in source
    assert "runtime.resume_ingress" in source
    assert "greenfield_initialize_runtime" in source
    assert "pipeline_command_receipts" not in source


def test_recovery_is_limited_to_the_fixed_requeue_function() -> None:
    source = RECOVERY.read_text(encoding="utf-8")

    assert _routine_names(source) == {"greenfield_requeue_inbox"}
    assert "INSERT INTO" not in source
    assert "UPDATE " not in source
    assert "DELETE FROM" not in source


def test_removed_command_receipt_adapter_is_not_a_live_python_module() -> None:
    assert not (ROOT / "src" / "ingestion" / "command_receipts.py").exists()


def test_email_processing_pipeline_is_the_runtime_bridge() -> None:
    runtime_source = RUNTIME.read_text(encoding="utf-8")

    assert "EmailProcessingAdapter" in runtime_source
    assert (ROOT / "src" / "ingestion" / "email_pipeline.py").is_file()
