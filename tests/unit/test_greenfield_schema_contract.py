"""Shape checks for the clean polling schema contract."""

from __future__ import annotations

from src.db import schema_contract


def test_schema_contract_accepts_only_the_single_greenfield_revision() -> None:
    assert schema_contract.GREENFIELD_DATABASE_REVISION == "20260808_0001"


def test_required_business_shape_contains_polling_and_checkpoint_boundaries() -> None:
    required = schema_contract._BUSINESS_RELATIONS
    columns = schema_contract._REQUIRED_COLUMN_TYPES

    assert {"event_inbox", "sync_cursors", "pipeline_runtime_authority"} <= set(required)
    assert {
        "checkpoints",
        "checkpoint_blobs",
        "checkpoint_writes",
        "checkpoint_migrations",
    } == set(schema_contract._CHECKPOINT_RELATIONS)
    assert ("event_inbox", "source") in columns
    assert ("sync_cursors", "cursor") in columns


def test_retired_ingress_objects_are_explicitly_rejected() -> None:
    assert schema_contract._RETIRED_RELATIONS == {
        "cold_start_command_receipts",
        "sync_cold_start_plans",
    }
    assert ("pipeline_folder_scopes", "webhook_ids") in schema_contract._RETIRED_COLUMNS
    assert schema_contract._RETIRED_ROUTINES == {"greenfield_insert_webhook_event"}


def test_required_routines_include_sync_commit_and_exclude_webhook_ingress() -> None:
    routines = schema_contract._REQUIRED_ROUTINES

    assert "greenfield_commit_sync_page" in routines
    assert "greenfield_initialize_runtime" in routines
    assert "greenfield_insert_webhook_event" not in routines
