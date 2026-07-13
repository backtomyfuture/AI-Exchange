"""Unit coverage for the PostgreSQL role catalog preflight."""

from __future__ import annotations

import importlib
from dataclasses import fields, replace
from urllib.parse import parse_qs, urlsplit
from unittest.mock import AsyncMock, patch

import psycopg
import pytest


RUNTIME_DSN = "postgresql://runtime_user:runtime-secret@db/email_agent"
MIGRATION_DSN = "postgresql://migration_owner:migration-secret@db/email_agent"
MAINTENANCE_DSN = "postgresql://maintenance_user:maintenance-secret@db/email_agent"
AUDITOR_ROLE = "checkpoint_auditor"


def _module():
    return importlib.import_module("src.db.roles")


def _valid_snapshot(snapshot_type):
    return snapshot_type(**{field.name: True for field in fields(snapshot_type)})


def test_role_snapshots_prove_all_identities_have_no_role_memberships():
    module = _module()

    assert "no_role_memberships" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "runtime_restricted_attributes" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "runtime_no_role_memberships" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "maintenance_no_role_memberships" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "auditor_access_contract_exact" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "auditor_access_contract_reconcilable" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "extended_objects_owned_by_migration" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "default_privileges_exclusive" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "trigger_semantics_safe" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "runtime_counterpart_privileges_safe" in {
        field.name for field in fields(module.MigrationRoleSnapshot)
    }
    assert "no_role_memberships" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "migration_restricted_attributes" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "migration_no_role_memberships" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "maintenance_no_role_memberships" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "auditor_access_contract_exact" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "extended_objects_owned_by_migration" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "default_privileges_exclusive" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "routines_execute_denied" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "trigger_semantics_safe" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "migration_counterpart_privileges_safe" in {
        field.name for field in fields(module.RuntimeRoleSnapshot)
    }
    assert "counterpart_roles_have_no_memberships" in {
        field.name for field in fields(module.MaintenanceRoleSnapshot)
    }
    assert "maintenance_privileges_safe" in {
        field.name for field in fields(module.MaintenanceRoleSnapshot)
    }
    assert "auditor_access_contract_exact" in {
        field.name for field in fields(module.MaintenanceRoleSnapshot)
    }


def test_role_test_dsn_preserves_admin_transport_parameters():
    from tests.integration.conftest import PostgresDatabaseFactory

    factory = PostgresDatabaseFactory(
        "postgresql://admin:secret@db.example/postgres"
        "?sslmode=verify-full&sslrootcert=%2Fprivate%2Froot.pem"
        "&channel_binding=require&application_name=role_test"
    )

    dsn = factory._role_database_url(
        database_name="isolated_database",
        role="runtime_user",
        password="private-password",
        search_path="pg_catalog,public",
    )

    query = parse_qs(urlsplit(dsn).query)
    assert query == {
        "application_name": ["role_test"],
        "channel_binding": ["require"],
        "options": ["-csearch_path=pg_catalog,public"],
        "sslmode": ["verify-full"],
        "sslrootcert": ["/private/root.pem"],
    }


@pytest.mark.asyncio
async def test_migration_role_preflight_accepts_only_a_complete_snapshot():
    module = _module()
    snapshot = _valid_snapshot(module.MigrationRoleSnapshot)
    reader = AsyncMock(return_value=snapshot)

    with patch.object(module, "_read_migration_role_snapshot", new=reader):
        await module.require_migration_database_role(
            MIGRATION_DSN,
            expected_migration_role="migration_owner",
            expected_runtime_role="runtime_user",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )

    reader.assert_awaited_once_with(
        MIGRATION_DSN,
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role=AUDITOR_ROLE,
        target_schema="public",
    )


@pytest.mark.asyncio
async def test_migration_recovery_allows_only_safe_missing_managed_acl():
    module = _module()
    snapshot = replace(
        _valid_snapshot(module.MigrationRoleSnapshot),
        runtime_access_contract_exact=False,
        maintenance_counterpart_privileges_safe=False,
        auditor_access_contract_exact=False,
    )
    reader = AsyncMock(return_value=snapshot)

    with patch.object(module, "_read_migration_role_snapshot", new=reader):
        await module.require_migration_database_role(
            MIGRATION_DSN,
            expected_migration_role="migration_owner",
            expected_runtime_role="runtime_user",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
            allow_acl_reconciliation=True,
        )


@pytest.mark.asyncio
async def test_migration_recovery_rejects_unreconcilable_acl():
    module = _module()
    snapshot = replace(
        _valid_snapshot(module.MigrationRoleSnapshot),
        runtime_access_contract_exact=False,
        runtime_access_contract_reconcilable=False,
    )

    with (
        patch.object(
            module,
            "_read_migration_role_snapshot",
            new=AsyncMock(return_value=snapshot),
        ),
        pytest.raises(module.DatabaseRoleError),
    ):
        await module.require_migration_database_role(
            MIGRATION_DSN,
            expected_migration_role="migration_owner",
            expected_runtime_role="runtime_user",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
            allow_acl_reconciliation=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_field",
    [
        "direct_session",
        "expected_identity",
        "restricted_attributes",
        "no_role_memberships",
        "runtime_role_exists",
        "runtime_restricted_attributes",
        "runtime_no_role_memberships",
        "maintenance_role_exists",
        "maintenance_restricted_attributes",
        "maintenance_no_role_memberships",
        "roles_distinct",
        "roles_all_distinct",
        "runtime_not_member",
        "database_owned_by_migration",
        "schema_owned_by_migration",
        "relations_owned_by_migration",
        "routines_owned_by_migration",
        "types_owned_by_migration",
        "extended_objects_owned_by_migration",
        "default_privileges_exclusive",
        "schema_create_exclusive",
        "trigger_semantics_safe",
        "session_security_settings_safe",
        "large_object_creation_denied",
        "target_execution_hooks_denied",
        "unexpected_direct_grants_denied",
        "unexpected_object_ownership_denied",
        "other_schema_create_denied",
        "other_database_connect_denied",
        "other_user_schema_usage_denied",
        "target_acl_exclusive",
        "auditor_access_contract_exact",
        "auditor_access_contract_reconcilable",
        "runtime_access_contract_exact",
        "runtime_access_contract_reconcilable",
        "system_public_acl_unchanged",
        "runtime_counterpart_privileges_safe",
        "maintenance_counterpart_privileges_safe",
        "maintenance_counterpart_privileges_reconcilable",
        "search_path_matches",
    ],
)
async def test_migration_role_preflight_fails_closed_for_each_invariant(
    failed_field: str,
):
    module = _module()
    valid = _valid_snapshot(module.MigrationRoleSnapshot)
    snapshot = replace(valid, **{failed_field: False})

    with (
        patch.object(
            module,
            "_read_migration_role_snapshot",
            new=AsyncMock(return_value=snapshot),
        ),
        pytest.raises(module.DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await module.require_migration_database_role(
            MIGRATION_DSN,
            expected_migration_role="migration_owner",
            expected_runtime_role="runtime_user",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )


@pytest.mark.asyncio
async def test_runtime_role_preflight_accepts_only_a_complete_snapshot():
    module = _module()
    snapshot = _valid_snapshot(module.RuntimeRoleSnapshot)
    reader = AsyncMock(return_value=snapshot)

    with patch.object(module, "_read_runtime_role_snapshot", new=reader):
        await module.require_runtime_database_role(
            RUNTIME_DSN,
            expected_runtime_role="runtime_user",
            expected_migration_role="migration_owner",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )

    reader.assert_awaited_once_with(
        RUNTIME_DSN,
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role=AUDITOR_ROLE,
        target_schema="public",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_field",
    [
        "direct_session",
        "expected_identity",
        "restricted_attributes",
        "no_role_memberships",
        "migration_role_exists",
        "migration_restricted_attributes",
        "migration_no_role_memberships",
        "maintenance_role_exists",
        "maintenance_restricted_attributes",
        "maintenance_no_role_memberships",
        "roles_distinct",
        "roles_all_distinct",
        "runtime_not_member",
        "database_owned_by_migration",
        "schema_owned_by_migration",
        "relations_owned_by_migration",
        "routines_owned_by_migration",
        "types_owned_by_migration",
        "extended_objects_owned_by_migration",
        "default_privileges_exclusive",
        "schema_create_exclusive",
        "routines_execute_denied",
        "trigger_semantics_safe",
        "session_security_settings_safe",
        "large_object_creation_denied",
        "target_execution_hooks_denied",
        "unexpected_direct_grants_denied",
        "unexpected_object_ownership_denied",
        "other_schema_create_denied",
        "other_database_connect_denied",
        "other_user_schema_usage_denied",
        "target_acl_exclusive",
        "auditor_access_contract_exact",
        "runtime_access_contract_exact",
        "delegation_privileges_denied",
        "system_public_acl_unchanged",
        "migration_counterpart_privileges_safe",
        "maintenance_counterpart_privileges_safe",
        "database_connect_allowed",
        "database_create_denied",
        "database_temp_denied",
        "schema_usage_allowed",
        "schema_create_denied",
        "dangerous_relation_privileges_denied",
        "sequence_update_denied",
        "audit_permissions_valid",
        "search_path_matches",
    ],
)
async def test_runtime_role_preflight_fails_closed_for_each_invariant(
    failed_field: str,
):
    module = _module()
    valid = _valid_snapshot(module.RuntimeRoleSnapshot)
    snapshot = replace(valid, **{failed_field: False})

    with (
        patch.object(
            module,
            "_read_runtime_role_snapshot",
            new=AsyncMock(return_value=snapshot),
        ),
        pytest.raises(module.DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await module.require_runtime_database_role(
            RUNTIME_DSN,
            expected_runtime_role="runtime_user",
            expected_migration_role="migration_owner",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )


@pytest.mark.asyncio
async def test_maintenance_role_preflight_accepts_only_a_complete_snapshot():
    module = _module()
    snapshot = _valid_snapshot(module.MaintenanceRoleSnapshot)
    reader = AsyncMock(return_value=snapshot)

    with patch.object(module, "_read_maintenance_role_snapshot", new=reader):
        await module.require_maintenance_database_role(
            MAINTENANCE_DSN,
            expected_maintenance_role="maintenance_user",
            expected_runtime_role="runtime_user",
            expected_migration_role="migration_owner",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )

    reader.assert_awaited_once_with(
        MAINTENANCE_DSN,
        expected_maintenance_role="maintenance_user",
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_auditor_role=AUDITOR_ROLE,
        target_schema="public",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_field",
    [
        "direct_session",
        "expected_identity",
        "restricted_attributes",
        "no_role_memberships",
        "counterpart_roles_exist",
        "counterpart_roles_restricted",
        "counterpart_roles_have_no_memberships",
        "roles_all_distinct",
        "database_owned_by_migration",
        "schema_owned_by_migration",
        "target_objects_owned_by_migration",
        "trigger_semantics_safe",
        "session_security_settings_safe",
        "target_execution_hooks_denied",
        "target_acl_exclusive",
        "auditor_access_contract_exact",
        "runtime_access_contract_exact",
        "maintenance_privileges_safe",
        "search_path_matches",
    ],
)
async def test_maintenance_role_preflight_fails_closed_for_each_invariant(
    failed_field: str,
):
    module = _module()
    valid = _valid_snapshot(module.MaintenanceRoleSnapshot)
    snapshot = replace(valid, **{failed_field: False})

    with (
        patch.object(
            module,
            "_read_maintenance_role_snapshot",
            new=AsyncMock(return_value=snapshot),
        ),
        pytest.raises(module.DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await module.require_maintenance_database_role(
            MAINTENANCE_DSN,
            expected_maintenance_role="maintenance_user",
            expected_runtime_role="runtime_user",
            expected_migration_role="migration_owner",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )


@pytest.mark.asyncio
async def test_role_preflight_logs_only_fixed_failed_invariant_names(caplog):
    module = _module()
    snapshot = replace(
        _valid_snapshot(module.RuntimeRoleSnapshot),
        other_database_connect_denied=False,
    )
    private_role = "private_runtime_user"
    private_dsn = "postgresql://private-user:private-password@db/private"

    with (
        patch.object(
            module,
            "_read_runtime_role_snapshot",
            new=AsyncMock(return_value=snapshot),
        ),
        caplog.at_level("ERROR", logger="src.db.roles"),
        pytest.raises(
            module.DatabaseRoleError,
            match="database_role_preflight_failed",
        ),
    ):
        await module.require_runtime_database_role(
            private_dsn,
            expected_runtime_role=private_role,
            expected_migration_role="migration_owner",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )

    assert "identity_plane=runtime" in caplog.text
    assert "other_database_connect_denied" in caplog.text
    assert private_role not in caplog.text
    assert private_dsn not in caplog.text
    assert "private-password" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["migration", "runtime", "maintenance"])
async def test_role_preflight_cuts_off_reader_errors_and_private_values(kind: str):
    module = _module()
    private = f"private-{kind}-catalog-error {MIGRATION_DSN}"
    if kind == "migration":
        function = module.require_migration_database_role
        reader_name = "_read_migration_role_snapshot"
        kwargs = {
            "expected_migration_role": "migration_owner",
            "expected_runtime_role": "runtime_user",
            "expected_maintenance_role": "maintenance_user",
            "expected_auditor_role": AUDITOR_ROLE,
            "target_schema": "public",
        }
        dsn = MIGRATION_DSN
    elif kind == "runtime":
        function = module.require_runtime_database_role
        reader_name = "_read_runtime_role_snapshot"
        kwargs = {
            "expected_runtime_role": "runtime_user",
            "expected_migration_role": "migration_owner",
            "expected_maintenance_role": "maintenance_user",
            "expected_auditor_role": AUDITOR_ROLE,
            "target_schema": "public",
        }
        dsn = RUNTIME_DSN
    else:
        function = module.require_maintenance_database_role
        reader_name = "_read_maintenance_role_snapshot"
        kwargs = {
            "expected_maintenance_role": "maintenance_user",
            "expected_runtime_role": "runtime_user",
            "expected_migration_role": "migration_owner",
            "expected_auditor_role": AUDITOR_ROLE,
            "target_schema": "public",
        }
        dsn = MAINTENANCE_DSN

    with (
        patch.object(
            module,
            reader_name,
            new=AsyncMock(side_effect=RuntimeError(private)),
        ),
        pytest.raises(module.DatabaseRoleError) as caught,
    ):
        await function(dsn, **kwargs)

    assert str(caught.value) == "database_role_preflight_failed"
    assert private not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.asyncio
async def test_checkpoint_auditor_rejects_unsafe_identity_before_connecting():
    module = importlib.import_module("src.db.auditor")
    connect = AsyncMock()
    private_role = "unsafe-auditor-private-value"
    private_dsn = "postgresql://auditor:private-password@db/email_agent"

    with (
        patch.object(module.psycopg.AsyncConnection, "connect", new=connect),
        pytest.raises(module.CheckpointAuditorRoleError) as caught,
    ):
        await module.require_checkpoint_auditor_database_role(
            private_dsn,
            expected_auditor_role=private_role,
            expected_runtime_role="runtime_user",
            expected_migration_role="migration_owner",
            expected_maintenance_role="maintenance_user",
            target_schema="public",
        )

    connect.assert_not_awaited()
    assert str(caught.value) == "checkpoint_auditor_role_preflight_failed"
    assert private_role not in str(caught.value)
    assert private_dsn not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.asyncio
async def test_checkpoint_auditor_cuts_off_driver_error_and_private_values(caplog):
    module = importlib.import_module("src.db.auditor")
    private = "private-driver-error private-password"
    private_dsn = "postgresql://auditor:private-password@db/email_agent"
    connect = AsyncMock(side_effect=psycopg.OperationalError(private))

    with (
        patch.object(module.psycopg.AsyncConnection, "connect", new=connect),
        caplog.at_level("ERROR", logger="src.db.auditor"),
        pytest.raises(module.CheckpointAuditorRoleError) as caught,
    ):
        await module.require_checkpoint_auditor_database_role(
            private_dsn,
            expected_auditor_role="checkpoint_auditor",
            expected_runtime_role="runtime_user",
            expected_migration_role="migration_owner",
            expected_maintenance_role="maintenance_user",
            target_schema="public",
        )

    connect.assert_awaited_once_with(
        private_dsn,
        autocommit=True,
        prepare_threshold=0,
    )
    assert str(caught.value) == "checkpoint_auditor_role_preflight_failed"
    assert private not in str(caught.value)
    assert private_dsn not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    assert "error_type=OperationalError" in caplog.text
    assert private not in caplog.text
    assert private_dsn not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["migration", "runtime", "maintenance"])
@pytest.mark.parametrize(
    (
        "expected_runtime_role",
        "expected_migration_role",
        "expected_maintenance_role",
        "expected_auditor_role",
        "target_schema",
    ),
    [
        ("same_role", "same_role", "maintenance_user", AUDITOR_ROLE, "public"),
        ("same_role", "migration_owner", "same_role", AUDITOR_ROLE, "public"),
        ("runtime_user", "same_role", "same_role", AUDITOR_ROLE, "public"),
        ("same_role", "migration_owner", "maintenance_user", "same_role", "public"),
        ("runtime_user", "same_role", "maintenance_user", "same_role", "public"),
        ("runtime_user", "migration_owner", "same_role", "same_role", "public"),
        ("bad-role!", "migration_owner", "maintenance_user", AUDITOR_ROLE, "public"),
        ("runtime_user", "bad-role!", "maintenance_user", AUDITOR_ROLE, "public"),
        ("runtime_user", "migration_owner", "bad-role!", AUDITOR_ROLE, "public"),
        ("runtime_user", "migration_owner", "maintenance_user", "bad-role!", "public"),
        (
            "runtime_user",
            "migration_owner",
            "maintenance_user",
            AUDITOR_ROLE,
            "bad-schema!",
        ),
        (None, "migration_owner", "maintenance_user", AUDITOR_ROLE, "public"),
        ("runtime_user", 1, "maintenance_user", AUDITOR_ROLE, "public"),
        ("runtime_user", "migration_owner", None, AUDITOR_ROLE, "public"),
        ("runtime_user", "migration_owner", "maintenance_user", None, "public"),
        ("runtime_user", "migration_owner", "maintenance_user", 1, "public"),
        (
            "runtime_user",
            "migration_owner",
            "maintenance_user",
            AUDITOR_ROLE,
            None,
        ),
    ],
)
async def test_role_preflight_rejects_invalid_contract_before_database_access(
    kind: str,
    expected_runtime_role: object,
    expected_migration_role: object,
    expected_maintenance_role: object,
    expected_auditor_role: object,
    target_schema: object,
):
    module = _module()
    migration_reader = AsyncMock()
    runtime_reader = AsyncMock()
    maintenance_reader = AsyncMock()
    if kind == "migration":
        function = module.require_migration_database_role
        dsn = MIGRATION_DSN
    elif kind == "runtime":
        function = module.require_runtime_database_role
        dsn = RUNTIME_DSN
    else:
        function = module.require_maintenance_database_role
        dsn = MAINTENANCE_DSN

    with (
        patch.object(module, "_read_migration_role_snapshot", new=migration_reader),
        patch.object(module, "_read_runtime_role_snapshot", new=runtime_reader),
        patch.object(
            module,
            "_read_maintenance_role_snapshot",
            new=maintenance_reader,
        ),
        pytest.raises(module.DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await function(
            dsn,
            expected_runtime_role=expected_runtime_role,
            expected_migration_role=expected_migration_role,
            expected_maintenance_role=expected_maintenance_role,
            expected_auditor_role=expected_auditor_role,
            target_schema=target_schema,
        )

    migration_reader.assert_not_awaited()
    runtime_reader.assert_not_awaited()
    maintenance_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_bootstrap_checks_migration_role_before_any_schema_write():
    from src.db import bootstrap as bootstrap_module

    module = _module()
    role_error = module.DatabaseRoleError("database_role_preflight_failed")
    role_gate = AsyncMock(side_effect=role_error)

    with (
        patch.object(
            bootstrap_module,
            "require_migration_database_role",
            new=role_gate,
            create=True,
        ),
        patch.object(
            bootstrap_module,
            "_upgrade_business_schema",
            side_effect=AssertionError("schema write reached before role gate"),
        ) as upgrade,
        pytest.raises(module.DatabaseRoleError),
    ):
        await bootstrap_module.bootstrap_database(
            MIGRATION_DSN,
            expected_migration_role="migration_owner",
            expected_runtime_role="runtime_user",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )

    role_gate.assert_awaited_once_with(
        MIGRATION_DSN,
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role=AUDITOR_ROLE,
        target_schema="public",
        allow_acl_reconciliation=True,
    )
    upgrade.assert_not_called()


@pytest.mark.asyncio
async def test_bootstrap_rechecks_role_boundary_after_all_schema_writes():
    from src.db import bootstrap as bootstrap_module

    module = _module()
    postflight_error = module.DatabaseRoleError("database_role_preflight_failed")
    role_gate = AsyncMock(side_effect=[None, postflight_error])
    schema_contract = AsyncMock()
    access_contract = AsyncMock()
    empty_inbox_gate = AsyncMock()

    with (
        patch.object(
            bootstrap_module,
            "require_migration_database_role",
            new=role_gate,
        ),
        patch.object(bootstrap_module, "_upgrade_business_schema"),
        patch.object(
            bootstrap_module,
            "get_current_database_revision",
            new=AsyncMock(side_effect=["20260710_0003", "20260713_0004"]),
        ),
        patch.object(
            bootstrap_module,
            "_apply_checkpoint_migrations",
            new=AsyncMock(return_value=0),
        ) as checkpoint_migrations,
        patch.object(
            bootstrap_module,
            "_apply_database_access_contract",
            new=access_contract,
        ),
        patch.object(
            bootstrap_module,
            "require_database_schema_contract",
            new=schema_contract,
        ),
        patch.object(
            bootstrap_module,
            "_require_empty_event_inbox_for_0004",
            new=empty_inbox_gate,
        ),
        pytest.raises(module.DatabaseRoleError, match="database_role_preflight_failed"),
    ):
        await bootstrap_module.bootstrap_database(
            MIGRATION_DSN,
            expected_migration_role="migration_owner",
            expected_runtime_role="runtime_user",
            expected_maintenance_role="maintenance_user",
            expected_auditor_role=AUDITOR_ROLE,
            target_schema="public",
        )

    assert role_gate.await_count == 2
    empty_inbox_gate.assert_awaited_once_with(
        MIGRATION_DSN,
        target_schema="public",
    )
    checkpoint_migrations.assert_awaited_once_with(MIGRATION_DSN, "public")
    access_contract.assert_awaited_once_with(
        MIGRATION_DSN,
        target_schema="public",
        runtime_role="runtime_user",
        maintenance_role="maintenance_user",
        auditor_role=AUDITOR_ROLE,
    )
    assert schema_contract.await_args_list == [
        (
            (MIGRATION_DSN,),
            {
                "target_schema": "public",
                "require_complete": False,
                "require_business_complete": True,
                "expected_revision": "20260710_0003",
            },
        ),
        (
            (MIGRATION_DSN,),
            {
                "target_schema": "public",
                "require_complete": True,
                "expected_revision": "20260710_0003",
            },
        ),
        (
            (MIGRATION_DSN,),
            {
                "target_schema": "public",
                "require_complete": True,
                "expected_revision": "20260713_0004",
            },
        ),
    ]
