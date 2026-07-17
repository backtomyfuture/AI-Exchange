from __future__ import annotations

import inspect

import pytest
from psycopg import sql

from src.db import access_contract, bootstrap, roles, schema_contract


REVISION = "20260716_0006"
GOVERNED_RELATIONS = frozenset(
    {
        "audit_events",
        "emails",
        "event_inbox",
        "pipeline_command_receipts",
        "pipeline_folder_scopes",
        "pipeline_initializations",
        "pipeline_ownership",
        "pipeline_runtime_authority",
        "pipeline_runtime_capabilities",
        "pipeline_runtime_instances",
        "sync_cold_start_plans",
        "sync_cursors",
    }
)
RUNTIME_ROUTINES = frozenset(
    {
        "greenfield_get_runtime_authority",
        "greenfield_register_web_instance",
        "greenfield_heartbeat_web_instance",
        "greenfield_drain_web_instance",
        "greenfield_insert_webhook_event",
        "greenfield_claim_inbox",
        "greenfield_renew_inbox",
        "greenfield_apply_email_event",
        "greenfield_begin_inbox_effect",
        "greenfield_finish_inbox",
        "greenfield_fail_inbox",
        "greenfield_reap_inbox",
    }
)
MAINTENANCE_ROUTINES = frozenset(
    {
        "greenfield_initialize_runtime",
        "greenfield_get_runtime_authority",
        "greenfield_pause_runtime",
        "greenfield_resume_ingress",
        "greenfield_requeue_inbox",
    }
)
RUNTIME_ROUTINE_IDENTITIES = {
    "greenfield_get_runtime_authority": "p_account_id bigint",
    "greenfield_register_web_instance": (
        "p_account_id bigint, p_instance_id text, p_session_id uuid, "
        "p_expected_authority_epoch bigint, p_expected_authority_version bigint, "
        "p_schema_revision text, p_protocol_version bigint, p_build_id text, "
        "p_config_hash text, p_capability_hash text, p_lease_seconds bigint"
    ),
    "greenfield_heartbeat_web_instance": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_expected_authority_epoch bigint, p_expected_capability_hash text, "
        "p_accepted_count bigint, p_rejected_count bigint, p_lease_seconds bigint"
    ),
    "greenfield_drain_web_instance": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_expected_authority_epoch bigint, p_expected_capability_hash text"
    ),
    "greenfield_insert_webhook_event": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_external_email_id text, p_folder_key text, p_raw_event_type text, "
        "p_change_kind text, p_dedupe_key text, p_source_version text, "
        "p_source_event_at timestamp with time zone, p_payload jsonb, "
        "p_processing_policy text"
    ),
    "greenfield_claim_inbox": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_lease_owner text, p_limit bigint, p_lease_seconds bigint"
    ),
    "greenfield_renew_inbox": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_lease_owner text, "
        "p_attempts bigint, p_lease_seconds bigint"
    ),
    "greenfield_apply_email_event": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, "
        "p_expected_email_version bigint"
    ),
    "greenfield_begin_inbox_effect": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint"
    ),
    "greenfield_finish_inbox": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint, "
        "p_completion jsonb"
    ),
    "greenfield_fail_inbox": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint, "
        "p_safe_error_code text, p_safe_error_summary text"
    ),
    "greenfield_reap_inbox": (
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_limit bigint"
    ),
}
MAINTENANCE_ROUTINE_IDENTITIES = {
    "greenfield_initialize_runtime": (
        "p_account_id bigint, p_capability_hash text, p_predecessor_hash text, "
        "p_capability_stage text, p_schema_revision text, p_schema_digest text, "
        "p_protocol_version bigint, p_minimum_build_id text, p_config_hash text, "
        "p_adapter_hash text, p_policy_manifest_hash text, "
        "p_evidence_manifest_hash text, p_policy_manifest_json text, "
        "p_policy_scope_count bigint, p_actor text, p_reason text, "
        "p_idempotency_key text, p_canonical_payload_hash text"
    ),
    "greenfield_get_runtime_authority": "p_account_id bigint",
    "greenfield_pause_runtime": (
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text"
    ),
    "greenfield_resume_ingress": (
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text"
    ),
    "greenfield_requeue_inbox": (
        "p_account_id bigint, p_inbox_id uuid, p_expected_execution_epoch bigint, "
        "p_expected_email_version bigint, p_actor text, p_reason text, "
        "p_idempotency_key text, p_canonical_payload_hash text"
    ),
}


class _RoutineCursor:
    def __init__(self, rows: list[tuple[str, str]] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[tuple[str, object]] = []

    async def __aenter__(self) -> _RoutineCursor:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, statement: object, parameters: object = None) -> None:
        rendered = (
            statement.as_string()
            if isinstance(statement, sql.Composable)
            else statement
        )
        assert isinstance(rendered, str)
        self.executed.append((rendered, parameters))

    async def fetchall(self) -> list[tuple[str, str]]:
        return list(self.rows)


class _RoutineConnection:
    def __init__(self, rows: list[tuple[str, str]] | None = None) -> None:
        self.routine_cursor = _RoutineCursor(rows)

    def cursor(self) -> _RoutineCursor:
        return self.routine_cursor


def _assert_select_only(access: access_contract.RelationAccess) -> None:
    assert set(access.table_privileges) <= {"SELECT"}
    assert access.insert_columns == ()
    assert access.update_columns == ()
    assert access.delete is False


def test_greenfield_relation_acl_limits_runtime_worker_to_exact_columns() -> None:
    runtime = access_contract.RUNTIME_RELATION_ACCESS_BY_REVISION[REVISION]
    maintenance = access_contract.MAINTENANCE_RELATION_ACCESS_BY_REVISION[REVISION]

    assert GOVERNED_RELATIONS <= set(runtime)
    assert GOVERNED_RELATIONS <= set(maintenance)
    assert "pipeline_shadow_comparisons" not in runtime
    assert "pipeline_shadow_comparisons" not in maintenance
    for relation in GOVERNED_RELATIONS - {"audit_events", "emails", "event_inbox"}:
        _assert_select_only(runtime[relation])
    for relation in GOVERNED_RELATIONS:
        _assert_select_only(maintenance[relation])

    assert runtime["event_inbox"] == access_contract.RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "account_id",
            "external_email_id",
            "folder_key",
            "source",
            "raw_event_type",
            "change_kind",
            "dedupe_key",
            "source_version",
            "source_event_at",
            "payload",
            "processing_policy",
            "pipeline_name",
            "generation",
            "fencing_token",
            "status",
            "available_at",
        ),
        update_columns=(
            "status",
            "lease_owner",
            "lease_session_id",
            "lease_until",
            "attempts",
            "available_at",
            "processing_started_at",
            "effect_started_at",
            "safe_error_code",
            "safe_error_summary",
            "updated_at",
        ),
    )
    assert runtime["emails"] == access_contract.RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "account_id",
            "external_email_id",
            "source_folder_key",
            "status",
            "owner_generation",
            "owner_fencing_token",
            "owner_authority_epoch",
            "owner_capability_hash",
            "processing_inbox_id",
            "processing_execution_epoch",
            "create_seen_at",
            "processing_started_at",
            "source_deleted_at",
            "external_effects_started_at",
            "safe_error_code",
            "safe_error_summary",
            "content_ref",
            "is_read",
            "is_read_refresh_required",
        ),
        update_columns=(
            "source_folder_key",
            "status",
            "version",
            "processing_inbox_id",
            "processing_execution_epoch",
            "create_seen_at",
            "processing_started_at",
            "source_deleted_at",
            "external_effects_started_at",
            "safe_error_code",
            "safe_error_summary",
            "content_ref",
            "is_read",
            "is_read_refresh_required",
            "updated_at",
        ),
    )
    assert runtime["audit_events"] == access_contract.RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "event_key",
            "account_id",
            "email_id",
            "object_type",
            "object_fingerprint",
            "action",
            "result",
            "actor",
            "reason",
            "safe_metadata",
        ),
    )
    for relation in ("audit_events", "emails", "event_inbox"):
        assert set(runtime[relation].table_privileges) == {"SELECT"}
        assert runtime[relation].delete is False

    assert runtime["emails_log"] == access_contract.RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "subject",
            "sender",
            "received_at",
            "status",
        ),
        update_columns=(
            "status",
            "classification",
            "draft_content",
            "updated_at",
            "routing_log",
            "active_skills",
            "original_draft",
            "final_draft",
            "approver_user_id",
            "rejection_reason",
            "error_message",
            "content_ref",
        ),
    )
    assert {"draft_diff", "version"}.isdisjoint(
        runtime["emails_log"].update_columns
    )
    assert runtime["checkpoints"].insert_columns
    assert runtime["checkpoints"].update_columns
    assert runtime["checkpoint_blobs"].insert_columns
    assert runtime["checkpoint_writes"].insert_columns
    assert maintenance["checkpoints"].delete is True
    assert maintenance["checkpoint_blobs"].delete is True
    assert maintenance["checkpoint_writes"].delete is True


def test_greenfield_fk_and_trigger_manifests_match_schema_contract() -> None:
    foreign_keys = {
        (
            spec.name,
            spec.child_relation,
            spec.child_columns,
            spec.parent_relation,
            spec.parent_columns,
            spec.match_type,
            spec.update_action,
            spec.delete_action,
            spec.deferrable,
            spec.initially_deferred,
            spec.validated,
        )
        for spec in access_contract.FOREIGN_KEY_SPECS_BY_REVISION[REVISION]
    }
    triggers = {
        (
            spec.name,
            spec.relation,
            spec.function,
            spec.trigger_type,
            spec.is_constraint,
            "O",
            True,
            True,
            True,
            True,
            True,
            spec.is_deferrable,
            spec.is_initially_deferred,
            len(spec.arguments),
            " ".join(str(value) for value in spec.update_attribute_numbers),
            b"".join(
                argument.encode("utf-8") + b"\x00" for argument in spec.arguments
            ).hex(),
            spec.when_clause_sha256,
            spec.old_transition_table,
            spec.new_transition_table,
            True,
        )
        for spec in access_contract.TRIGGER_SPECS_BY_REVISION[REVISION]
    }

    assert foreign_keys == schema_contract._GREENFIELD_FOREIGN_KEYS
    assert triggers == schema_contract._GREENFIELD_TRIGGERS


def test_greenfield_auditor_manifest_is_select_only_and_redacted() -> None:
    auditor = access_contract.AUDITOR_RELATION_ACCESS_BY_REVISION[REVISION]

    for access in auditor.values():
        _assert_select_only(access)
    assert "sync_cursors" not in auditor
    assert "sync_cold_start_plans" not in auditor
    assert auditor["pipeline_folder_scopes"].select_columns == (
        "initialization_id",
        "account_id",
        "canonical_key",
        "scope_hash",
        "policy_manifest_hash",
        "created_at",
    )
    assert {
        "external_email_id",
        "folder_key",
        "source_version",
        "payload",
        "safe_error_summary",
    }.isdisjoint(auditor["event_inbox"].select_columns)
    assert {
        "external_email_id",
        "source_folder_key",
        "content_ref",
        "safe_error_summary",
    }.isdisjoint(auditor["emails"].select_columns)
    for relation in (
        "pipeline_runtime_capabilities",
        "pipeline_initializations",
        "pipeline_runtime_authority",
        "pipeline_runtime_instances",
        "pipeline_ownership",
        "pipeline_command_receipts",
        "audit_events",
    ):
        assert auditor[relation] == access_contract.RelationAccess(
            table_privileges=("SELECT",)
        )


def test_greenfield_routine_execute_manifests_are_exact_identity_pairs() -> None:
    runtime = access_contract.RUNTIME_ROUTINE_EXECUTE_BY_REVISION[REVISION]
    maintenance = access_contract.MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION[REVISION]
    auditor = access_contract.AUDITOR_ROUTINE_EXECUTE_BY_REVISION[REVISION]

    assert {spec.name for spec in runtime} == RUNTIME_ROUTINES
    assert {spec.name for spec in maintenance} == MAINTENANCE_ROUTINES
    assert {spec.name: spec.identity_arguments for spec in runtime} == (
        RUNTIME_ROUTINE_IDENTITIES
    )
    assert {spec.name: spec.identity_arguments for spec in maintenance} == (
        MAINTENANCE_ROUTINE_IDENTITIES
    )
    assert auditor == ()
    for manifest in (runtime, maintenance):
        identities = {(spec.name, spec.identity_arguments) for spec in manifest}
        assert len(identities) == len(manifest)
        assert all(spec.identity_arguments for spec in manifest)
        assert all("pending" not in spec.identity_arguments for spec in manifest)
        assert all("*" not in spec.name and "%" not in spec.name for spec in manifest)
        bootstrap._validate_routine_manifest(manifest)


def test_role_preflight_uses_exact_name_and_identity_argument_set_equality() -> None:
    runtime_sql = roles._routine_execute_contract_sql(
        "schema_oid",
        "runtime_oid",
        access_contract.RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
    )
    maintenance_sql = roles._routine_execute_contract_sql(
        "schema_oid",
        "maintenance_oid",
        access_contract.MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION,
    )

    for statement, expected_names in (
        (runtime_sql, RUNTIME_ROUTINES),
        (maintenance_sql, MAINTENANCE_ROUTINES),
    ):
        lowered = statement.casefold()
        assert "pg_get_function_identity_arguments" in lowered
        assert lowered.count(" except ") >= 2
        assert "pg_catalog.aclexplode" in lowered
        assert "grantee = 0" in lowered
        assert "is_grantable" in lowered
        assert " like " not in lowered
        assert all(name in statement for name in expected_names)


def test_role_preflight_allows_only_fixed_migration_owned_security_definers() -> None:
    statement = roles._security_definer_contract_sql(
        "schema_oid",
        "migration_oid",
    )
    lowered = statement.casefold()

    assert "pg_get_function_identity_arguments" in lowered
    assert "routine.prosecdef" in lowered
    assert "routine.proowner is distinct from migration_oid" in lowered
    assert "routine.prokind <> 'f'" in lowered
    assert "array['search_path=pg_catalog']" in lowered
    assert lowered.count("except") >= 2
    assert " like " not in lowered
    assert all(name in statement for name in RUNTIME_ROUTINES | MAINTENANCE_ROUTINES)


def test_bootstrap_revokes_public_and_grants_only_exact_routine_signatures() -> None:
    source = inspect.getsource(bootstrap)

    assert "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA" in source
    assert "FROM PUBLIC" in source
    assert "pg_get_function_identity_arguments" in source
    assert "GRANT EXECUTE ON FUNCTION" in source
    assert "GRANT EXECUTE ON ALL FUNCTIONS" not in source
    assert "LIKE 'greenfield_%'" not in source


def test_bootstrap_rejects_ambiguous_or_injectable_routine_manifests() -> None:
    valid = access_contract.RoutineAccess(
        "greenfield_fixed",
        "p_account_id bigint, p_session_id uuid",
    )
    bootstrap._validate_routine_manifest((valid,))

    invalid = (
        access_contract.RoutineAccess("greenfield_*", "p_account_id bigint"),
        access_contract.RoutineAccess("greenfield_fixed", "bigint) TO PUBLIC"),
        access_contract.RoutineAccess("greenfield_fixed", "bigint; SELECT 1"),
    )
    for spec in invalid:
        with pytest.raises(
            RuntimeError,
            match="Database routine access contract is invalid",
        ):
            bootstrap._validate_routine_manifest((spec,))
    with pytest.raises(
        RuntimeError,
        match="Database routine access contract is invalid",
    ):
        bootstrap._validate_routine_manifest((valid, valid))
    with pytest.raises(
        RuntimeError,
        match="Database routine access contract is invalid",
    ):
        bootstrap._validate_routine_manifest((object(),))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_bootstrap_grants_only_catalog_proven_exact_overload() -> None:
    spec = access_contract.RoutineAccess(
        "greenfield_fixed",
        "p_account_id bigint, p_session_id uuid",
    )
    connection = _RoutineConnection([(spec.name, spec.identity_arguments)])

    await bootstrap._grant_routine_access(
        connection,  # type: ignore[arg-type]
        target_schema="private_schema",
        role="runtime_role",
        manifest=(spec,),
    )

    assert connection.routine_cursor.executed[0][1] == (
        "private_schema",
        ["greenfield_fixed"],
    )
    assert connection.routine_cursor.executed[1] == (
        'GRANT EXECUTE ON FUNCTION "private_schema"."greenfield_fixed"('
        'p_account_id bigint, p_session_id uuid) TO "runtime_role"',
        None,
    )


@pytest.mark.asyncio
async def test_bootstrap_rejects_missing_or_extra_routine_overload_before_grant() -> (
    None
):
    spec = access_contract.RoutineAccess(
        "greenfield_fixed",
        "p_account_id bigint",
    )
    connection = _RoutineConnection(
        [
            (spec.name, spec.identity_arguments),
            (spec.name, "p_account_id uuid"),
        ]
    )

    with pytest.raises(
        RuntimeError,
        match="Database routine access contract is unavailable",
    ):
        await bootstrap._grant_routine_access(
            connection,  # type: ignore[arg-type]
            target_schema="public",
            role="runtime_role",
            manifest=(spec,),
        )

    assert len(connection.routine_cursor.executed) == 1


@pytest.mark.asyncio
async def test_bootstrap_revokes_public_and_every_managed_role_before_grant() -> None:
    connection = _RoutineConnection()

    await bootstrap._revoke_routine_access(
        connection,  # type: ignore[arg-type]
        target_schema="public",
        roles=("runtime_role", "maintenance_role", "auditor_role"),
    )

    assert [statement for statement, _params in connection.routine_cursor.executed] == [
        'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA "public" FROM PUBLIC',
        'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA "public" FROM "runtime_role"',
        'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA "public" '
        'FROM "maintenance_role"',
        'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA "public" FROM "auditor_role"',
    ]
