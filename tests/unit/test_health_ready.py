from __future__ import annotations

import asyncio
from datetime import datetime
import logging
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src import server as server_module


app = server_module.app


def test_health_returns_only_liveness_metadata_without_app_context():
    client = TestClient(app)

    with patch(
        "src.server.initialize_app_context",
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
    runtime = SimpleNamespace(check_ready=AsyncMock(return_value=True))
    settings = SimpleNamespace(
        database_url="postgresql://private-dsn-sentinel",
        DURABLE_INBOX_ENABLED=False,
        INGESTION_SHADOW_ENABLED=True,
        SYNC_RECONCILIATION_ENABLED=False,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="public",
    )
    cached_gate = AsyncMock()
    runtime_gate = AsyncMock(
        side_effect=AssertionError("endpoint_bypassed_cached_preflight")
    )
    legacy_gate = AsyncMock(side_effect=AssertionError("legacy_gate_was_called"))

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(
            server_module,
            "_require_cached_runtime_database",
            new=cached_gate,
        ),
        patch.object(
            server_module,
            "require_runtime_database",
            new=runtime_gate,
            create=True,
        ) as require_database,
        patch.object(
            server_module,
            "require_current_database",
            new=legacy_gate,
            create=True,
        ),
        patch.object(app.state, "ingestion_runtime", runtime, create=True),
    ):
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "processing": "standby"}
    cached_gate.assert_awaited_once_with(settings)
    runtime.check_ready.assert_awaited_once_with()
    require_database.assert_not_awaited()
    legacy_gate.assert_not_awaited()


def test_ready_rejects_missing_or_unready_runtime_without_mutating_session():
    client = TestClient(app)
    settings = SimpleNamespace(database_url="postgresql://private-dsn-sentinel")
    cached_gate = AsyncMock()
    runtime = SimpleNamespace(check_ready=AsyncMock(return_value=False))

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(server_module, "validate_runtime_security"),
        patch.object(server_module, "_require_cached_runtime_database", cached_gate),
        patch.object(app.state, "ingestion_runtime", runtime, create=True),
    ):
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    runtime.check_ready.assert_awaited_once_with()
    assert not hasattr(runtime, "heartbeat")


def test_ready_failure_is_generic_and_never_logs_or_returns_exception_text(caplog):
    client = TestClient(app)
    secret = "postgresql://user:dsn-secret@db/private-sql-sentinel"
    settings = SimpleNamespace(
        database_url="postgresql://bounded-placeholder",
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="public",
    )
    runtime_gate = AsyncMock(side_effect=RuntimeError(secret))
    legacy_gate = AsyncMock(side_effect=AssertionError("legacy_gate_was_called"))

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(
            server_module,
            "require_runtime_database",
            new=runtime_gate,
            create=True,
        ),
        patch.object(
            server_module,
            "require_current_database",
            new=legacy_gate,
            create=True,
        ),
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
        role_separation_required=True,
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )
    legacy_gate.assert_not_awaited()


def test_ready_runs_security_validation_before_database_preflight():
    client = TestClient(app)
    settings = SimpleNamespace(
        APP_ENV="production",
        database_url="postgresql://private-dsn-sentinel",
        DATABASE_ROLE_SEPARATION_REQUIRED=False,
        DURABLE_INBOX_ENABLED=False,
        INGESTION_SHADOW_ENABLED=False,
        SYNC_RECONCILIATION_ENABLED=False,
    )
    runtime_gate = AsyncMock()
    security_gate = MagicMock(side_effect=RuntimeError("unsafe_runtime_settings"))

    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(
            server_module,
            "validate_runtime_security",
            new=security_gate,
        ),
        patch.object(
            server_module,
            "require_runtime_database",
            new=runtime_gate,
        ),
    ):
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    security_gate.assert_called_once_with(settings)
    runtime_gate.assert_not_awaited()


@pytest.mark.asyncio
async def test_ready_database_preflight_coalesces_concurrent_requests():
    settings = SimpleNamespace(
        database_url="postgresql://runtime/private",
        DURABLE_INBOX_ENABLED=False,
        INGESTION_SHADOW_ENABLED=False,
        SYNC_RECONCILIATION_ENABLED=False,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="public",
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    async def database_gate(*_args, **_kwargs):
        entered.set()
        await release.wait()

    runtime_gate = AsyncMock(side_effect=database_gate)
    server_module._READINESS_STATES.clear()
    try:
        with patch.object(
            server_module,
            "require_runtime_database",
            new=runtime_gate,
        ):
            tasks = [
                asyncio.create_task(
                    server_module._require_cached_runtime_database(settings)
                )
                for _ in range(25)
            ]
            await entered.wait()
            await asyncio.sleep(0)
            release.set()
            await asyncio.gather(*tasks)
            await server_module._require_cached_runtime_database(settings)
    finally:
        server_module._READINESS_STATES.clear()

    assert runtime_gate.await_count == 1
    runtime_gate.assert_awaited_once_with(
        settings.database_url,
        durable_inbox_enabled=False,
        ingestion_shadow_enabled=False,
        sync_reconciliation_enabled=False,
        role_separation_required=True,
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )


@pytest.mark.asyncio
async def test_ready_database_preflight_caches_failure_and_has_timeout(
    monkeypatch,
):
    settings = SimpleNamespace(
        database_url="postgresql://runtime/private",
        DURABLE_INBOX_ENABLED=False,
        INGESTION_SHADOW_ENABLED=False,
        SYNC_RECONCILIATION_ENABLED=False,
        DATABASE_ROLE_SEPARATION_REQUIRED=True,
        POSTGRES_USER="runtime_user",
        POSTGRES_MIGRATION_OWNER_ROLE="migration_owner",
        POSTGRES_MAINTENANCE_ROLE="maintenance_user",
        POSTGRES_CHECKPOINT_AUDITOR_ROLE="checkpoint_auditor",
        POSTGRES_SCHEMA="public",
    )

    async def slow_gate(*_args, **_kwargs):
        await asyncio.sleep(1)

    runtime_gate = AsyncMock(side_effect=slow_gate)
    monkeypatch.setattr(server_module, "_READINESS_DATABASE_TIMEOUT_SECONDS", 0.001)
    server_module._READINESS_STATES.clear()
    try:
        with patch.object(
            server_module,
            "require_runtime_database",
            new=runtime_gate,
        ):
            with pytest.raises(TimeoutError):
                await server_module._require_cached_runtime_database(settings)
            with pytest.raises(server_module.ReadinessPreflightError):
                await server_module._require_cached_runtime_database(settings)
    finally:
        server_module._READINESS_STATES.clear()

    assert runtime_gate.await_count == 1


@pytest.mark.asyncio
async def test_ready_failure_logging_is_rate_limited_and_redacted(caplog):
    secret = "postgresql://runtime:private-password@db/private"
    server_module._READINESS_STATES.clear()
    try:
        with caplog.at_level(logging.WARNING, logger="WebServer"):
            for _ in range(50):
                server_module._log_readiness_failure_once(RuntimeError(secret))
    finally:
        server_module._READINESS_STATES.clear()

    assert caplog.text.count("Readiness check failed") == 1
    assert "RuntimeError" in caplog.text
    assert secret not in caplog.text
    assert "private-password" not in caplog.text


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
