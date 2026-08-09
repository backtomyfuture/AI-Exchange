"""Contract tests for the one supported polling database baseline."""

from __future__ import annotations

from src.db import access_contract


def test_every_revision_indexed_manifest_has_only_the_greenfield_revision() -> None:
    revision = access_contract.DATABASE_REVISION
    manifests = (
        access_contract.POLLING_RELATIONS_BY_REVISION,
        access_contract.POLLING_VIEW_SPECS_BY_REVISION,
        access_contract.RUNTIME_RELATION_ACCESS_BY_REVISION,
        access_contract.MAINTENANCE_RELATION_ACCESS_BY_REVISION,
        access_contract.AUDITOR_RELATION_ACCESS_BY_REVISION,
        access_contract.RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
        access_contract.MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION,
        access_contract.AUDITOR_ROUTINE_EXECUTE_BY_REVISION,
        access_contract.SECURITY_DEFINER_ROUTINES_BY_REVISION,
        access_contract.FOREIGN_KEY_SPECS_BY_REVISION,
        access_contract.TRIGGER_SPECS_BY_REVISION,
        access_contract.TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION,
        access_contract.TRIGGER_FUNCTION_SEARCH_PATH_BY_REVISION,
        access_contract.TRIGGER_FUNCTIONS_BY_REVISION,
    )

    assert all(set(manifest) == {revision} for manifest in manifests)
    assert access_contract.DATABASE_REVISION == revision


def test_runtime_contract_exposes_only_the_polling_ingress_writer() -> None:
    routines = {
        (routine.name, routine.identity_arguments)
        for routine in access_contract.RUNTIME_ROUTINE_EXECUTE
    }

    assert any(name == "greenfield_commit_sync_page" for name, _ in routines)
    assert not any("webhook" in name or "cold_start" in name for name, _ in routines)


def test_relation_and_trigger_contracts_have_no_retired_ingress_shape() -> None:
    relations = set(access_contract.POLLING_RELATIONS)
    trigger_functions = set(access_contract.TRIGGER_FUNCTIONS)
    foreign_keys = {spec.name for spec in access_contract.FOREIGN_KEY_SPECS}

    assert {"event_inbox", "sync_cursors", "pipeline_folder_scopes"} <= relations
    assert not {"cold_start_command_receipts", "sync_cold_start_plans"} & relations
    assert not any("webhook" in name or "cold_start" in name for name in trigger_functions)
    assert "reject_tier1_decisions_mutation" in trigger_functions
    assert {
        "fk_tier1_decisions_inbox",
        "fk_handoff_executions_decision",
    } <= foreign_keys
    assert set(access_contract.TRIGGER_FUNCTION_SOURCE_SHA256) == set(
        access_contract.TRIGGER_FUNCTION_SEARCH_PATH
    )


def test_security_definer_contract_is_derived_from_execute_manifests() -> None:
    executable = {
        (routine.name, routine.identity_arguments)
        for routine in (
            *access_contract.RUNTIME_ROUTINE_EXECUTE,
            *access_contract.MAINTENANCE_ROUTINE_EXECUTE,
        )
    }
    declared = {
        (routine.name, routine.identity_arguments)
        for routine in access_contract.SECURITY_DEFINER_ROUTINES
    }

    assert declared == executable
