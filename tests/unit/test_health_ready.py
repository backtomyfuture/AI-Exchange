from __future__ import annotations

from datetime import datetime
import logging
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src import server as server_module


app = server_module.app


def test_health_returns_only_liveness_metadata_without_app_context():
    client = TestClient(app)

    with patch(
        "src.server.get_app_context",
        side_effect=AssertionError("health_must_not_initialize_app_context"),
    ):
        response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"status", "version", "time"}
    assert payload["status"] == "ok"
    assert isinstance(payload["version"], str) and payload["version"]
    assert datetime.fromisoformat(payload["time"]).tzinfo is not None
    assert "last_error" not in response.text


def test_ready_checks_database_revision_and_returns_minimal_success():
    client = TestClient(app)
    settings = SimpleNamespace(
        database_url="postgresql://private-dsn-sentinel",
        DURABLE_INBOX_ENABLED=False,
        INGESTION_SHADOW_ENABLED=True,
        SYNC_RECONCILIATION_ENABLED=False,
    )
    runtime_gate = AsyncMock()
    legacy_gate = AsyncMock(side_effect=AssertionError("legacy_gate_was_called"))

    with patch.object(server_module, "get_settings", return_value=settings), patch.object(
        server_module,
        "require_runtime_database",
        new=runtime_gate,
        create=True,
    ) as require_database, patch.object(
        server_module,
        "require_current_database",
        new=legacy_gate,
        create=True,
    ):
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    require_database.assert_awaited_once_with(
        settings.database_url,
        durable_inbox_enabled=False,
        ingestion_shadow_enabled=True,
        sync_reconciliation_enabled=False,
    )
    legacy_gate.assert_not_awaited()


def test_ready_failure_is_generic_and_never_logs_or_returns_exception_text(caplog):
    client = TestClient(app)
    secret = "postgresql://user:dsn-secret@db/private-sql-sentinel"
    settings = SimpleNamespace(database_url="postgresql://bounded-placeholder")
    runtime_gate = AsyncMock(side_effect=RuntimeError(secret))
    legacy_gate = AsyncMock(side_effect=AssertionError("legacy_gate_was_called"))

    with patch.object(server_module, "get_settings", return_value=settings), patch.object(
        server_module,
        "require_runtime_database",
        new=runtime_gate,
        create=True,
    ), patch.object(
        server_module,
        "require_current_database",
        new=legacy_gate,
        create=True,
    ):
        with caplog.at_level(logging.WARNING, logger="WebServer"):
            response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    assert secret not in response.text
    assert secret not in caplog.text
    assert "RuntimeError" in caplog.text
    runtime_gate.assert_awaited_once_with(
        settings.database_url,
        durable_inbox_enabled=False,
        ingestion_shadow_enabled=False,
        sync_reconciliation_enabled=False,
    )
    legacy_gate.assert_not_awaited()


def test_production_route_inventory_disables_interactive_api_docs():
    project_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["APP_ENV"] = "production"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from src.server import app; "
                "print('|'.join(sorted(route.path for route in app.routes)))"
            ),
        ],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    paths = set(result.stdout.strip().split("|"))
    assert "/docs" not in paths
    assert "/redoc" not in paths
    assert "/openapi.json" not in paths
