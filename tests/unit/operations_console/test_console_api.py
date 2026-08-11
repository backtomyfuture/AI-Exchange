from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from console_api.database import ConsoleDatabaseError, _safe_projection
from console_api.main import _database, create_app
from console_api.models import PipelineTrace, TraceNode
from console_api.rules import RuleStore, RuleStoreError
from console_api.settings import ConsoleSettings


def _settings(tmp_path):
    return ConsoleSettings(
        database_url="postgresql://console:test@localhost/email_agent",
        rules_dir=tmp_path / "tier1_rules",
        artifact_dir=tmp_path / "artifacts",
        internal_email_domains="example.com",
        me_email="operator@example.com",
    )


def test_console_settings_can_allow_private_docker_gateway_only_when_enabled(tmp_path):
    settings = _settings(tmp_path)

    assert settings.client_host_allowed("192.168.117.1") is False

    settings.allow_private_client_hosts = True
    assert settings.client_host_allowed("192.168.117.1") is True
    assert settings.client_host_allowed("8.8.8.8") is False
    assert settings.client_host_allowed("not-an-ip") is False


def _manifest(status: str = "proposed") -> dict:
    return {
        "schema_version": 1,
        "rule_id": "console-test-001",
        "rule_version": 1,
        "status": status,
        "owner": "operator",
        "purpose": "Route a deterministic test sender.",
        "match": {
            "anchor": {
                "any": [
                    {
                        "field": "sender.address",
                        "op": "eq",
                        "value": "sender@example.com",
                    }
                ]
            }
        },
        "decision": {
            "route": "reply",
            "params": {"reply_mode": "sender_only"},
        },
        "governance": {
            "positive_cases": [
                {
                    "case_id": "positive-1",
                    "email": {"sender": {"address": "sender@example.com"}},
                }
            ],
            "negative_cases": [],
        },
    }


def test_rule_store_saves_incomplete_proposed_draft_without_compiling(tmp_path):
    store = RuleStore(_settings(tmp_path))

    result = store.save(
        SimpleNamespace(
            rule_id="console-test-001",
            manifest={"rule_id": "console-test-001", "status": "proposed"},
            raw_yaml=None,
        )
    )

    assert result.status == "proposed"
    assert (tmp_path / "tier1_rules" / "console-test-001.yaml").exists()


def test_rule_store_validation_uses_real_compiler_and_returns_digest(tmp_path):
    store = RuleStore(_settings(tmp_path))
    store.save(
        SimpleNamespace(
            rule_id="console-test-001",
            manifest=_manifest(status="enabled"),
            raw_yaml=None,
        )
    )

    result = store.validate_registry()

    assert result.valid is True
    assert len(result.digest) == 64
    assert result.enabled_rule_count == 1


def test_rule_store_blocks_invalid_enabled_draft(tmp_path):
    store = RuleStore(_settings(tmp_path))

    with pytest.raises(RuleStoreError, match="enabled_rule_requires_valid_registry"):
        store.save(
            SimpleNamespace(
                rule_id="console-test-001",
                manifest=_manifest(status="enabled")
                | {"governance": {"positive_cases": [], "negative_cases": []}},
                raw_yaml=None,
            )
        )


def test_rule_store_rejects_unknown_status(tmp_path):
    store = RuleStore(_settings(tmp_path))

    with pytest.raises(RuleStoreError, match="rule_status_invalid"):
        store.save(
            SimpleNamespace(
                rule_id="console-test-001",
                manifest={"rule_id": "console-test-001", "status": "active"},
                raw_yaml=None,
            )
        )


def test_trace_projection_excludes_content_and_bounds_metadata():
    projection = _safe_projection(
        {
            "body": "private body",
            "attachment_name": "private.pdf",
            "safe": "visible",
            "nested": {"content": "private nested content", "stage": "router"},
            "items": list(range(40)),
        }
    )

    assert projection == {
        "safe": "visible",
        "nested": {"stage": "router"},
        "items": list(range(32)),
    }


def test_trace_endpoint_uses_business_stage_projection():
    app = create_app()
    trace = PipelineTrace(
        external_email_id="mail-001",
        inbox_id="00000000-0000-4000-8000-000000000001",
        subject="Quarterly review",
        sender="sender@example.com",
        current_status="waiting_approval",
        nodes=[
            TraceNode(
                id="ingestion",
                label="Ingestion",
                kind="ingestion",
                status="completed",
            )
        ],
        edges=[],
    )

    class FakeDatabase:
        async def trace(self, external_email_id):
            assert external_email_id == "mail-001"
            return trace

    app.dependency_overrides[_database] = lambda: FakeDatabase()
    try:
        response = TestClient(app).get("/api/emails/mail-001/trace")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["nodes"][0]["id"] == "ingestion"


def test_trace_endpoint_maps_database_unavailability_to_safe_error():
    app = create_app()

    class FailingDatabase:
        async def trace(self, _external_email_id):
            raise ConsoleDatabaseError("database_private_detail")

    app.dependency_overrides[_database] = lambda: FailingDatabase()
    try:
        response = TestClient(app).get("/api/emails/mail-001/trace")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == {"detail": "console_database_unavailable"}
