"""Current Exchange polling contract boundaries.

Detailed HTTP parsing and retry cases live beside ``ExchangeClient``.  This
module protects the architectural fact that only the polling ingress consumes
Gateway sync pages.
"""

from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from src.ingestion.models import ChangeKind, SyncBatch, SyncChange
from src.utils.exchange_api import ExchangeClient


ROOT = Path(__file__).resolve().parents[2]


def _sync_polling_callers() -> set[Path]:
    callers: set[Path] = set()
    for path in (ROOT / "src").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "sync_polling":
                    callers.add(path.relative_to(ROOT))
    return callers


def test_sync_batch_exposes_only_the_frozen_v2_page_contract() -> None:
    change = SyncChange(ChangeKind.CREATE, "message-1", {"id": "message-1"})
    batch = SyncBatch("exchange_sync_contract_v2", "cursor-1", (change,), True)

    assert batch.contract_version == "exchange_sync_contract_v2"
    assert batch.cursor == "cursor-1"
    assert batch.changes == (change,)
    assert batch.includes_last is True
    assert not hasattr(batch, "__dict__")
    with pytest.raises(FrozenInstanceError):
        batch.cursor = "other"


def test_exchange_client_exposes_the_polling_adapter_without_database_coupling() -> None:
    source = (ROOT / "src" / "utils" / "exchange_api.py").read_text(
        encoding="utf-8"
    )

    assert callable(getattr(ExchangeClient, "sync_polling", None))
    assert "src.db" not in source
    assert "src.ingestion.repository" not in source
    assert "sync_cold_start_plans" not in source


def test_only_polling_ingress_calls_the_gateway_polling_method() -> None:
    assert _sync_polling_callers() == {Path("src/ingestion/polling.py")}


def test_retired_sync_coordinator_and_cold_start_callers_are_not_shipped() -> None:
    assert not (ROOT / "src" / "ingestion" / "sync.py").exists()
    assert not (ROOT / "src" / "ingestion" / "cold_start.py").exists()
