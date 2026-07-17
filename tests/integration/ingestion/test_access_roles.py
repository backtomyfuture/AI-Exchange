from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import sql
from psycopg.conninfo import make_conninfo

from src.db.auditor import (
    CheckpointAuditorRoleError,
    require_checkpoint_auditor_database_role,
)
from src.db.bootstrap import (
    _apply_checkpoint_migrations,
    _apply_database_access_contract,
    bootstrap_database,
)
from src.db.roles import (
    DatabaseRoleError,
    _security_definer_contract_sql,
    _target_foreign_keys_exact_sql,
    _target_trigger_contract_exact_sql,
    require_maintenance_database_role,
    require_migration_database_role,
    require_runtime_database_role,
)
from src.db.schema import DatabaseRevisionError, require_runtime_database
from src.domain.email_state import InitialEmailWriteResult
from src.maintenance.checkpoint_cleanup import CheckpointCleaner
from src.maintenance.checkpoint_repository import PostgresCheckpointRepository
from src.maintenance.cleanup_artifacts import PlanArtifactStore
from src.maintenance.cleanup_backup import (
    Ed25519BackupReceiptVerifier,
    create_ed25519_signed_backup_receipt,
)
from src.utils.db_async import AsyncDatabaseManager


RECEIPT_PRIVATE_SEED = b"\x22" * 32
RECEIPT_PUBLIC_KEY = (
    Ed25519PrivateKey.from_private_bytes(RECEIPT_PRIVATE_SEED)
    .public_key()
    .public_bytes_raw()
)

_RUNTIME_TABLE_PRIVILEGES = {
    relation: ("SELECT",)
    for relation in (
        "alembic_version",
        "emails_log",
        "checkpoints",
        "checkpoint_blobs",
        "checkpoint_writes",
        "pipeline_ownership",
        "event_inbox",
        "sync_cursors",
        "emails",
        "audit_events",
        "pipeline_shadow_comparisons",
    )
}
_RUNTIME_INSERT_COLUMNS = {
    "emails_log": (
        "id",
        "subject",
        "sender",
        "received_at",
        "status",
    ),
    "checkpoints": (
        "thread_id",
        "checkpoint_ns",
        "checkpoint_id",
        "parent_checkpoint_id",
        "checkpoint",
        "metadata",
    ),
    "checkpoint_blobs": (
        "thread_id",
        "checkpoint_ns",
        "channel",
        "version",
        "type",
        "blob",
    ),
    "checkpoint_writes": (
        "thread_id",
        "checkpoint_ns",
        "checkpoint_id",
        "task_id",
        "task_path",
        "idx",
        "channel",
        "type",
        "blob",
    ),
    "pipeline_ownership": (
        "account_id",
        "generation",
        "pipeline_name",
        "state",
        "fencing_token",
        "created_by",
        "reason",
    ),
    "event_inbox": (
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
        "received_at",
    ),
    "sync_cursors": (
        "account_id",
        "folder_key",
        "cursor",
        "status",
        "blocked_reason_code",
        "contract_fingerprint",
        "blocked_at",
        "last_success_at",
        "last_attempt_at",
    ),
    "emails": (
        "id",
        "account_id",
        "external_email_id",
        "source_folder_key",
        "status",
        "owner_generation",
        "owner_fencing_token",
        "processing_inbox_id",
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
    "audit_events": (
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
    "pipeline_shadow_comparisons": (
        "id",
        "account_id",
        "generation",
        "fencing_token",
        "pipeline_name",
        "candidate_pipeline_name",
        "candidate_build_id",
        "candidate_config_hash",
        "event_key",
        "input_hash",
        "legacy_status",
        "shadow_status",
        "comparison_status",
        "legacy_decision_hash",
        "legacy_failure_code",
        "shadow_decision_hash",
        "shadow_failure_code",
        "safe_metadata",
    ),
}
_RUNTIME_UPDATE_COLUMNS = {
    "emails_log": (
        "status",
        "classification",
        "draft_content",
        "updated_at",
        "routing_log",
        "active_skills",
        "original_draft",
        "final_draft",
        "draft_diff",
        "approver_user_id",
        "rejection_reason",
        "error_message",
        "content_ref",
        "version",
    ),
    "checkpoints": ("checkpoint", "metadata"),
    "checkpoint_writes": ("channel", "type", "blob"),
    "pipeline_ownership": ("state", "reason", "updated_at"),
    "event_inbox": (
        "status",
        "lease_owner",
        "lease_until",
        "attempts",
        "available_at",
        "processing_started_at",
        "effect_started_at",
        "safe_error_code",
        "safe_error_summary",
        "updated_at",
    ),
    "sync_cursors": (
        "cursor",
        "status",
        "blocked_reason_code",
        "contract_fingerprint",
        "blocked_at",
        "version",
        "last_success_at",
        "last_attempt_at",
        "updated_at",
    ),
    "emails": (
        "source_folder_key",
        "status",
        "version",
        "processing_inbox_id",
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
    "pipeline_shadow_comparisons": (
        "legacy_status",
        "shadow_status",
        "comparison_status",
        "legacy_decision_hash",
        "legacy_failure_code",
        "shadow_decision_hash",
        "shadow_failure_code",
        "safe_metadata",
        "updated_at",
    ),
}


def _expected_runtime_acl() -> set[tuple[str, str, str, str, bool]]:
    expected = {
        ("table", relation, "", privilege, False)
        for relation, privileges in _RUNTIME_TABLE_PRIVILEGES.items()
        for privilege in privileges
    }
    for privilege, manifest in (
        ("INSERT", _RUNTIME_INSERT_COLUMNS),
        ("UPDATE", _RUNTIME_UPDATE_COLUMNS),
    ):
        expected.update(
            ("column", relation, column, privilege, False)
            for relation, columns in manifest.items()
            for column in columns
        )
    return expected


_EXPECTED_MAINTENANCE_ACL = {
    ("table", "alembic_version", "", "SELECT", False),
    ("table", "checkpoint_migrations", "", "SELECT", False),
    ("table", "emails_log", "", "SELECT", False),
    ("table", "checkpoints", "", "SELECT", False),
    ("table", "checkpoints", "", "DELETE", False),
    ("table", "checkpoint_blobs", "", "SELECT", False),
    ("table", "checkpoint_blobs", "", "DELETE", False),
    ("table", "checkpoint_writes", "", "SELECT", False),
    ("table", "checkpoint_writes", "", "DELETE", False),
}

_AUDITOR_SELECT_COLUMNS = {
    "alembic_version": ("version_num",),
    "checkpoint_migrations": ("v",),
    "emails_log": ("id", "status", "updated_at"),
    "checkpoints": (
        "thread_id",
        "checkpoint_ns",
        "checkpoint_id",
        "parent_checkpoint_id",
        "type",
        "checkpoint",
        "metadata",
    ),
    "checkpoint_blobs": (
        "thread_id",
        "checkpoint_ns",
        "channel",
        "version",
        "type",
        "blob",
    ),
    "checkpoint_writes": (
        "thread_id",
        "checkpoint_ns",
        "checkpoint_id",
        "task_id",
        "idx",
        "channel",
        "type",
        "blob",
        "task_path",
    ),
}
_EXPECTED_AUDITOR_ACL = {
    ("column", relation, column, "SELECT", False)
    for relation, columns in _AUDITOR_SELECT_COLUMNS.items()
    for column in columns
}

_RUNTIME_0005_INSERT_COLUMNS = {
    **_RUNTIME_INSERT_COLUMNS,
    "sync_cursors": (
        *_RUNTIME_INSERT_COLUMNS["sync_cursors"],
        "transient_failures",
        "retry_after_at",
    ),
}
_RUNTIME_0005_UPDATE_COLUMNS = {
    **_RUNTIME_UPDATE_COLUMNS,
    "sync_cursors": (
        *_RUNTIME_UPDATE_COLUMNS["sync_cursors"],
        "transient_failures",
        "retry_after_at",
    ),
}


def _expected_runtime_acl_0005() -> set[tuple[str, str, str, str, bool]]:
    expected = {
        ("table", relation, "", privilege, False)
        for relation, privileges in _RUNTIME_TABLE_PRIVILEGES.items()
        for privilege in privileges
    }
    for privilege, manifest in (
        ("INSERT", _RUNTIME_0005_INSERT_COLUMNS),
        ("UPDATE", _RUNTIME_0005_UPDATE_COLUMNS),
    ):
        expected.update(
            ("column", relation, column, privilege, False)
            for relation, columns in manifest.items()
            for column in columns
        )
    return expected


_MAINTENANCE_0005_TABLE_PRIVILEGES = {
    **{
        relation: privileges
        for relation, privileges in {
            "alembic_version": ("SELECT",),
            "checkpoint_migrations": ("SELECT",),
            "emails_log": ("SELECT",),
            "checkpoints": ("SELECT", "DELETE"),
            "checkpoint_blobs": ("SELECT", "DELETE"),
            "checkpoint_writes": ("SELECT", "DELETE"),
        }.items()
    },
    "pipeline_ownership": ("SELECT",),
    "event_inbox": ("SELECT",),
    "sync_cursors": ("SELECT",),
    "audit_events": ("SELECT",),
    "sync_cold_start_plans": ("SELECT",),
    "cold_start_command_receipts": ("SELECT", "INSERT"),
}
_MAINTENANCE_0005_INSERT_COLUMNS = {
    "event_inbox": (
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
    "sync_cursors": (
        *_RUNTIME_0005_INSERT_COLUMNS["sync_cursors"],
        "cold_start_plan_id",
        "cold_start_plan_state",
    ),
    "audit_events": _RUNTIME_INSERT_COLUMNS["audit_events"],
    "sync_cold_start_plans": (
        "plan_id",
        "account_id",
        "folder_key",
        "expected_cursor_status",
        "expected_cursor",
        "expected_cursor_version",
        "pipeline_name",
        "generation",
        "fencing_token",
        "state",
        "version",
        "preview_cursor",
        "preview_cursor_version",
        "boundary_cursor",
        "boundary_cursor_version",
        "apply_cursor",
        "apply_cursor_version",
        "rolling_hash",
        "page_count",
        "item_count",
        "redacted_samples",
        "contract_fingerprint",
        "folder_scope_config_hash",
        "plan_hash",
        "actor",
        "reason",
        "blocked_reason_code",
        "blocked_fingerprint",
        "expires_at",
        "ready_at",
        "approved_at",
        "completed_at",
        "blocked_at",
        "created_at",
        "updated_at",
    ),
}
_MAINTENANCE_0005_UPDATE_COLUMNS = {
    "sync_cursors": (
        *_RUNTIME_0005_UPDATE_COLUMNS["sync_cursors"],
        "cold_start_plan_id",
        "cold_start_plan_state",
    ),
    "sync_cold_start_plans": (
        "state",
        "version",
        "preview_cursor",
        "preview_cursor_version",
        "boundary_cursor",
        "boundary_cursor_version",
        "apply_cursor",
        "apply_cursor_version",
        "rolling_hash",
        "page_count",
        "item_count",
        "redacted_samples",
        "plan_hash",
        "blocked_reason_code",
        "blocked_fingerprint",
        "ready_at",
        "approved_at",
        "completed_at",
        "blocked_at",
        "updated_at",
    ),
}


def _expected_maintenance_acl_0005() -> set[tuple[str, str, str, str, bool]]:
    expected = {
        ("table", relation, "", privilege, False)
        for relation, privileges in _MAINTENANCE_0005_TABLE_PRIVILEGES.items()
        for privilege in privileges
    }
    for privilege, manifest in (
        ("INSERT", _MAINTENANCE_0005_INSERT_COLUMNS),
        ("UPDATE", _MAINTENANCE_0005_UPDATE_COLUMNS),
    ):
        expected.update(
            ("column", relation, column, privilege, False)
            for relation, columns in manifest.items()
            for column in columns
        )
    return expected


_EXPECTED_AUDITOR_ACL_0005 = _EXPECTED_AUDITOR_ACL | {
    ("table", "pipeline_command_receipts", "", "SELECT", False)
}
_MIGRATION_0005_RELATIONS = {
    "alembic_version",
    "app_kv_store",
    "audit_events",
    "checkpoint_blobs",
    "checkpoint_migrations",
    "checkpoint_writes",
    "checkpoints",
    "cold_start_command_receipts",
    "emails",
    "emails_log",
    "event_inbox",
    "pipeline_command_receipts",
    "pipeline_ownership",
    "pipeline_shadow_comparisons",
    "processed_emails",
    "sync_cold_start_plans",
    "sync_cursors",
}
_EXPECTED_MIGRATION_ACL_0005 = {
    ("table", relation, "", privilege, False)
    for relation in _MIGRATION_0005_RELATIONS
    for privilege in (
        "DELETE",
        "INSERT",
        "REFERENCES",
        "SELECT",
        "TRIGGER",
        "TRUNCATE",
        "UPDATE",
    )
}

_GREENFIELD_SELECT_ONLY_RELATIONS = (
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
)
_EXPECTED_RUNTIME_ACL_0006 = {
    (
        "table",
        relation,
        "",
        "SELECT",
        False,
    )
    for relation in (
        "alembic_version",
        "emails_log",
        "checkpoints",
        "checkpoint_blobs",
        "checkpoint_writes",
        *_GREENFIELD_SELECT_ONLY_RELATIONS,
    )
}
_RUNTIME_0006_INSERT_COLUMNS = {
    "emails_log": (
        "id",
        "subject",
        "sender",
        "received_at",
        "status",
    ),
    "checkpoints": _RUNTIME_INSERT_COLUMNS["checkpoints"],
    "checkpoint_blobs": _RUNTIME_INSERT_COLUMNS["checkpoint_blobs"],
    "checkpoint_writes": _RUNTIME_INSERT_COLUMNS["checkpoint_writes"],
    "event_inbox": tuple(
        column
        for column in _RUNTIME_INSERT_COLUMNS["event_inbox"]
        if column != "received_at"
    ),
    "emails": (
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
    "audit_events": _RUNTIME_INSERT_COLUMNS["audit_events"],
}
_RUNTIME_0006_UPDATE_COLUMNS = {
    "emails_log": (
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
    "checkpoints": _RUNTIME_UPDATE_COLUMNS["checkpoints"],
    "checkpoint_writes": _RUNTIME_UPDATE_COLUMNS["checkpoint_writes"],
    "event_inbox": (
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
    "emails": (
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
}
for _privilege, _columns_by_relation in (
    ("INSERT", _RUNTIME_0006_INSERT_COLUMNS),
    ("UPDATE", _RUNTIME_0006_UPDATE_COLUMNS),
):
    _EXPECTED_RUNTIME_ACL_0006.update(
        (
            "column",
            relation,
            column,
            _privilege,
            False,
        )
        for relation, columns in _columns_by_relation.items()
        for column in columns
    )

_EXPECTED_MAINTENANCE_ACL_0006 = {
    ("table", relation, "", "SELECT", False)
    for relation in (
        "alembic_version",
        "checkpoint_migrations",
        "emails_log",
        "checkpoints",
        "checkpoint_blobs",
        "checkpoint_writes",
        *_GREENFIELD_SELECT_ONLY_RELATIONS,
    )
} | {
    ("table", relation, "", "DELETE", False)
    for relation in ("checkpoints", "checkpoint_blobs", "checkpoint_writes")
}

_AUDITOR_0006_FULL_SELECT_RELATIONS = (
    "audit_events",
    "pipeline_command_receipts",
    "pipeline_initializations",
    "pipeline_ownership",
    "pipeline_runtime_authority",
    "pipeline_runtime_capabilities",
    "pipeline_runtime_instances",
)
_AUDITOR_0006_SELECT_COLUMNS = {
    "pipeline_folder_scopes": (
        "initialization_id",
        "account_id",
        "canonical_key",
        "scope_hash",
        "policy_manifest_hash",
        "created_at",
    ),
    "event_inbox": (
        "id",
        "account_id",
        "source",
        "raw_event_type",
        "change_kind",
        "dedupe_key",
        "source_event_at",
        "processing_policy",
        "pipeline_name",
        "generation",
        "fencing_token",
        "execution_epoch",
        "authority_epoch",
        "capability_hash",
        "status",
        "lease_owner",
        "lease_session_id",
        "lease_until",
        "attempts",
        "available_at",
        "processing_started_at",
        "effect_started_at",
        "safe_error_code",
        "received_at",
        "updated_at",
    ),
    "emails": (
        "id",
        "account_id",
        "status",
        "version",
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
        "is_read",
        "is_read_refresh_required",
        "created_at",
        "updated_at",
    ),
}
_EXPECTED_AUDITOR_ACL_0006 = (
    _EXPECTED_AUDITOR_ACL
    | {
        ("table", relation, "", "SELECT", False)
        for relation in _AUDITOR_0006_FULL_SELECT_RELATIONS
    }
    | {
        ("column", relation, column, "SELECT", False)
        for relation, columns in _AUDITOR_0006_SELECT_COLUMNS.items()
        for column in columns
    }
)

_EXPECTED_RUNTIME_ROUTINES_0006 = {
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
_EXPECTED_MAINTENANCE_ROUTINES_0006 = {
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


async def _prepare_revision(schema, alembic_runner, revision: str) -> None:
    if revision not in {
        "20260710_0002",
        "20260710_0003",
        "20260713_0004",
        "20260713_0005",
        "20260716_0006",
    }:
        raise AssertionError("unsupported test revision")
    alembic_runner.upgrade(schema, revision)
    await _apply_checkpoint_migrations(schema.dsn, "public")
    await _apply_database_access_contract(
        schema.dsn,
        target_schema="public",
        runtime_role=schema.runtime_role,
        maintenance_role=schema.maintenance_role,
        auditor_role=schema.auditor_role,
        business_revision=revision,
    )


def _direct_relation_acl(schema, role_name: str):
    with psycopg.connect(schema.dsn, autocommit=True) as conn:
        return {
            tuple(row)
            for row in conn.execute(
                "SELECT 'table'::pg_catalog.text, "
                "relation.relname::pg_catalog.text, ''::pg_catalog.text, "
                "grant_acl.privilege_type::pg_catalog.text, "
                "grant_acl.is_grantable "
                "FROM pg_catalog.pg_class AS relation "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) "
                "AS grant_acl "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "JOIN pg_catalog.pg_roles AS role "
                "ON role.oid = grant_acl.grantee "
                "WHERE relation_schema.nspname = 'public' "
                "AND role.rolname = %s "
                "UNION ALL "
                "SELECT 'column'::pg_catalog.text, "
                "relation.relname::pg_catalog.text, "
                "attribute.attname::pg_catalog.text, "
                "grant_acl.privilege_type::pg_catalog.text, "
                "grant_acl.is_grantable "
                "FROM pg_catalog.pg_attribute AS attribute "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = attribute.attrelid "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) "
                "AS grant_acl "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "JOIN pg_catalog.pg_roles AS role "
                "ON role.oid = grant_acl.grantee "
                "WHERE relation_schema.nspname = 'public' "
                "AND role.rolname = %s",
                (role_name, role_name),
            ).fetchall()
        }


def _direct_routine_acl(
    schema,
    role_name: str,
) -> set[tuple[str, str, str, bool]]:
    with psycopg.connect(schema.dsn, autocommit=True) as conn:
        return {
            tuple(row)
            for row in conn.execute(
                "SELECT routine.proname::pg_catalog.text, "
                "pg_catalog.pg_get_function_identity_arguments(routine.oid), "
                "grant_acl.privilege_type::pg_catalog.text, "
                "grant_acl.is_grantable "
                "FROM pg_catalog.pg_proc AS routine "
                "JOIN pg_catalog.pg_namespace AS routine_schema "
                "ON routine_schema.oid = routine.pronamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(routine.proacl) "
                "AS grant_acl "
                "JOIN pg_catalog.pg_roles AS role "
                "ON role.oid = grant_acl.grantee "
                "WHERE routine_schema.nspname = 'public' "
                "AND role.rolname = %s",
                (role_name,),
            ).fetchall()
        }


def _public_routine_acl(schema) -> set[tuple[str, str, str, bool]]:
    with psycopg.connect(schema.dsn, autocommit=True) as conn:
        return {
            tuple(row)
            for row in conn.execute(
                "SELECT routine.proname::pg_catalog.text, "
                "pg_catalog.pg_get_function_identity_arguments(routine.oid), "
                "grant_acl.privilege_type::pg_catalog.text, "
                "grant_acl.is_grantable "
                "FROM pg_catalog.pg_proc AS routine "
                "JOIN pg_catalog.pg_namespace AS routine_schema "
                "ON routine_schema.oid = routine.pronamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode("
                "COALESCE(routine.proacl, "
                "pg_catalog.acldefault('f', routine.proowner))) AS grant_acl "
                "WHERE routine_schema.nspname = 'public' "
                "AND grant_acl.grantee = 0"
            ).fetchall()
        }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0004_managed_role_acls_match_independent_manifests(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260713_0004")

    assert _direct_relation_acl(schema, schema.runtime_role) == (
        _expected_runtime_acl()
    )
    assert _direct_relation_acl(schema, schema.maintenance_role) == (
        _EXPECTED_MAINTENANCE_ACL
    )
    assert _direct_relation_acl(schema, schema.auditor_role) == _EXPECTED_AUDITOR_ACL


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0005_managed_role_acls_match_independent_manifests(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260713_0005")

    runtime_acl = _direct_relation_acl(schema, schema.runtime_role)
    maintenance_acl = _direct_relation_acl(schema, schema.maintenance_role)
    auditor_acl = _direct_relation_acl(schema, schema.auditor_role)
    migration_acl = _direct_relation_acl(schema, schema.migration_role)

    assert runtime_acl == _expected_runtime_acl_0005()
    assert len(runtime_acl) == 185
    assert maintenance_acl == _expected_maintenance_acl_0005()
    assert len(maintenance_acl) == 125
    assert auditor_acl == _EXPECTED_AUDITOR_ACL_0005
    assert len(auditor_acl) == 28
    assert migration_acl == _EXPECTED_MIGRATION_ACL_0005
    assert len(migration_acl) == 119
    owner_rows = schema.scalar(
        "SELECT pg_catalog.jsonb_object_agg("
        "relation.relname, owner.rolname) "
        "FROM pg_catalog.pg_class AS relation "
        "JOIN pg_catalog.pg_namespace AS relation_schema "
        "ON relation_schema.oid = relation.relnamespace "
        "JOIN pg_catalog.pg_roles AS owner ON owner.oid = relation.relowner "
        "WHERE relation_schema.nspname = 'public' "
        "AND relation.relname IN ("
        "'sync_cold_start_plans', 'pipeline_command_receipts', "
        "'cold_start_command_receipts')"
    )
    assert owner_rows == {
        "sync_cold_start_plans": schema.migration_role,
        "pipeline_command_receipts": schema.migration_role,
        "cold_start_command_receipts": schema.migration_role,
    }
    await require_migration_database_role(
        schema.dsn,
        **schema.bootstrap_identity,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0006_managed_role_acls_and_routines_match_independent_manifests(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260716_0006")

    assert _direct_relation_acl(schema, schema.runtime_role) == (
        _EXPECTED_RUNTIME_ACL_0006
    )
    assert _direct_relation_acl(schema, schema.maintenance_role) == (
        _EXPECTED_MAINTENANCE_ACL_0006
    )
    assert _direct_relation_acl(schema, schema.auditor_role) == (
        _EXPECTED_AUDITOR_ACL_0006
    )

    runtime_routines = _direct_routine_acl(schema, schema.runtime_role)
    maintenance_routines = _direct_routine_acl(schema, schema.maintenance_role)
    assert runtime_routines == {
        (name, identity_arguments, "EXECUTE", False)
        for name, identity_arguments in _EXPECTED_RUNTIME_ROUTINES_0006.items()
    }
    assert maintenance_routines == {
        (name, identity_arguments, "EXECUTE", False)
        for name, identity_arguments in _EXPECTED_MAINTENANCE_ROUTINES_0006.items()
    }
    for routine_acl in (runtime_routines, maintenance_routines):
        assert all(
            identity_arguments and privilege == "EXECUTE" and grantable is False
            for _name, identity_arguments, privilege, grantable in routine_acl
        )
    assert _direct_routine_acl(schema, schema.auditor_role) == set()
    assert _public_routine_acl(schema) == set()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0006_all_role_gates_accept_exact_relation_and_routine_acls(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260716_0006")

    schema_oid = "'public'::pg_catalog.regnamespace"
    migration_oid = "(SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)"
    hook_contract = schema.scalar(
        "SELECT pg_catalog.jsonb_build_object("
        f"'foreign_keys', {_target_foreign_keys_exact_sql(schema_oid)}, "
        "'triggers', "
        f"{_target_trigger_contract_exact_sql(schema_oid, migration_oid)}, "
        "'security_definers', "
        f"{_security_definer_contract_sql(schema_oid, migration_oid)})"
    )
    assert hook_contract == {
        "foreign_keys": True,
        "triggers": True,
        "security_definers": True,
    }

    for gate, dsn, identity in (
        (
            require_migration_database_role,
            schema.dsn,
            schema.bootstrap_identity,
        ),
        (
            require_runtime_database_role,
            schema.runtime_dsn,
            schema.runtime_identity,
        ),
        (
            require_maintenance_database_role,
            schema.maintenance_dsn,
            schema.maintenance_identity,
        ),
    ):
        await gate(dsn, **identity)
    await require_checkpoint_auditor_database_role(
        schema.auditor_dsn,
        expected_auditor_role=schema.auditor_role,
        expected_runtime_role=schema.runtime_role,
        expected_migration_role=schema.migration_role,
        expected_maintenance_role=schema.maintenance_role,
        target_schema="public",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0006_runtime_role_executes_bounded_legacy_email_writes(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260716_0006")
    manager = AsyncDatabaseManager(SimpleNamespace(database_url=schema.runtime_dsn))
    email_id = "phase4-runtime-legacy-write"

    try:
        assert await manager.recover_incomplete_approval_states() == 0
        assert (
            await manager.log_initial_email(
                {
                    "id": email_id,
                    "subject": "Phase 4 runtime grant",
                    "sender": "sender@example.test",
                    "received_at": "2026-07-17T08:00:00+00:00",
                }
            )
            is InitialEmailWriteResult.CREATED
        )
        assert await manager.save_draft(email_id, "bounded draft") == email_id
        await manager.update_status(
            email_id,
            "waiting_approval",
            classification={"category": "integration"},
            routing_log={"route": "runtime"},
            active_skills=["draft"],
            original_draft="original draft",
            final_draft="final draft",
            approver_user_id="approver",
            rejection_reason="not rejected",
            error_message="no error",
        )
    finally:
        await manager.close()

    with psycopg.connect(schema.dsn, autocommit=True) as connection:
        row = connection.execute(
            "SELECT status, draft_content, classification, routing_log, "
            "active_skills, original_draft, final_draft, approver_user_id, "
            "rejection_reason, error_message FROM emails_log WHERE id = %s",
            (email_id,),
        ).fetchone()
    assert row == (
        "waiting_approval",
        "bounded draft",
        {"category": "integration"},
        {"route": "runtime"},
        ["draft"],
        "original draft",
        "final draft",
        "approver",
        "not rejected",
        "no error",
    )

    for statement in (
        "INSERT INTO emails_log (processed_at) "
        "VALUES (pg_catalog.clock_timestamp())",
        "UPDATE emails_log SET draft_diff = 'forbidden' WHERE false",
        "UPDATE emails_log SET version = version WHERE false",
        "DELETE FROM emails_log",
        "TRUNCATE emails_log",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            schema.runtime_execute(statement)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0006_governed_relations_expose_only_bounded_runtime_worker_columns(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260716_0006")

    with psycopg.connect(schema.dsn, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT role.rolname, relation.relname, "
            "has_table_privilege(role.oid, relation.oid, 'INSERT'), "
            "has_table_privilege(role.oid, relation.oid, 'UPDATE'), "
            "has_table_privilege(role.oid, relation.oid, 'DELETE'), "
            "has_table_privilege(role.oid, relation.oid, 'TRUNCATE'), "
            "has_table_privilege(role.oid, relation.oid, 'TRIGGER'), "
            "has_any_column_privilege(role.oid, relation.oid, 'INSERT'), "
            "has_any_column_privilege(role.oid, relation.oid, 'UPDATE') "
            "FROM pg_catalog.pg_roles AS role "
            "CROSS JOIN pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS relation_schema "
            "ON relation_schema.oid = relation.relnamespace "
            "WHERE role.rolname = ANY(%s::pg_catalog.text[]) "
            "AND relation_schema.nspname = 'public' "
            "AND relation.relname = ANY(%s::pg_catalog.text[])",
            (
                [schema.runtime_role, schema.maintenance_role],
                list(_GREENFIELD_SELECT_ONLY_RELATIONS),
            ),
        ).fetchall()
    assert len(rows) == 2 * len(_GREENFIELD_SELECT_ONLY_RELATIONS)
    expected_runtime_column_insert = {"audit_events", "emails", "event_inbox"}
    expected_runtime_column_update = {"emails", "event_inbox"}
    for row in rows:
        role_name, relation_name = row[:2]
        assert not any(row[2:7])
        if role_name == schema.runtime_role:
            assert row[7] is (relation_name in expected_runtime_column_insert)
            assert row[8] is (relation_name in expected_runtime_column_update)
        else:
            assert row[7:] == (False, False)

    schema.runtime_execute(
        "UPDATE event_inbox SET status = status, "
        "lease_session_id = lease_session_id WHERE false"
    )

    for execute, statement in (
        (
            schema.runtime_execute,
            "INSERT INTO audit_events (created_at) "
            "VALUES (pg_catalog.clock_timestamp())",
        ),
        (
            schema.runtime_execute,
            "INSERT INTO event_inbox (received_at) "
            "VALUES (pg_catalog.clock_timestamp())",
        ),
        (
            schema.maintenance_execute,
            "UPDATE event_inbox SET status = status",
        ),
        (
            schema.runtime_execute,
            "UPDATE event_inbox SET execution_epoch = execution_epoch "
            "WHERE false",
        ),
        (
            schema.runtime_execute,
            "UPDATE emails SET owner_authority_epoch = owner_authority_epoch "
            "WHERE false",
        ),
        (schema.runtime_execute, "DELETE FROM emails"),
        (schema.maintenance_execute, "TRUNCATE pipeline_ownership"),
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            execute(statement)

    for dsn in (schema.runtime_dsn, schema.maintenance_dsn):
        with psycopg.connect(dsn, autocommit=True) as connection:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute("SELECT * FROM cold_start_command_receipts LIMIT 0")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with connection.cursor().copy("COPY audit_events FROM STDIN"):
                    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0006_auditor_sees_only_redacted_operational_columns(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260716_0006")

    with psycopg.connect(schema.auditor_dsn, autocommit=True) as connection:
        assert (
            connection.execute(
                "SELECT account_id, status, safe_error_code FROM event_inbox LIMIT 0"
            ).description
            is not None
        )
        assert (
            connection.execute(
                "SELECT account_id, status, version FROM emails LIMIT 0"
            ).description
            is not None
        )
        for statement in (
            "SELECT external_email_id FROM event_inbox LIMIT 0",
            "SELECT payload FROM event_inbox LIMIT 0",
            "SELECT content_ref FROM emails LIMIT 0",
            "SELECT * FROM sync_cursors LIMIT 0",
            "SELECT * FROM sync_cold_start_plans LIMIT 0",
            "SELECT * FROM cold_start_command_receipts LIMIT 0",
            "SELECT * FROM public.greenfield_get_runtime_authority(8)",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(statement)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0005_adjacent_role_capabilities_are_denied(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260713_0005")

    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        schema.maintenance_execute(
            "INSERT INTO event_inbox ("
            "id, account_id, external_email_id, folder_key, source, "
            "raw_event_type, change_kind, dedupe_key, payload, "
            "processing_policy, pipeline_name, generation, fencing_token, "
            "status, available_at, received_at"
            ") VALUES ("
            f"'{uuid4()}', 8, 'forged-received-at', 'INBOX', 'sync', "
            "'create', 'create', '1'::pg_catalog.bpchar(64), "
            "'{}'::pg_catalog.jsonb, 'full', 'durable', 1, 1, 'pending', "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        )

    for statement in (
        "UPDATE cold_start_command_receipts SET outcome = 'succeeded'",
        "DELETE FROM cold_start_command_receipts",
        "SELECT id FROM pipeline_command_receipts",
        f"INSERT INTO pipeline_command_receipts (id) VALUES ('{uuid4()}')",
        "DELETE FROM pipeline_command_receipts",
        "UPDATE event_inbox SET status = 'pending'",
        "DELETE FROM event_inbox",
        "UPDATE audit_events SET action = 'forged'",
        "DELETE FROM audit_events",
        "DELETE FROM sync_cold_start_plans",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            schema.maintenance_execute(statement)
    with pytest.raises(
        (psycopg.errors.InsufficientPrivilege, psycopg.errors.WrongObjectType)
    ):
        schema.maintenance_execute("TRUNCATE cold_start_command_receipts")

    for statement in (
        "SELECT plan_id FROM sync_cold_start_plans",
        "SELECT id FROM cold_start_command_receipts",
        "SELECT id FROM pipeline_command_receipts",
        f"INSERT INTO sync_cold_start_plans (plan_id) VALUES ('{uuid4()}')",
        "UPDATE sync_cold_start_plans SET state = 'blocked'",
        f"INSERT INTO cold_start_command_receipts (id) VALUES ('{uuid4()}')",
        "UPDATE cold_start_command_receipts SET outcome = 'succeeded'",
        "DELETE FROM sync_cold_start_plans",
        "DELETE FROM cold_start_command_receipts",
        "DELETE FROM pipeline_command_receipts",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            schema.runtime_execute(statement)

    with pytest.raises(
        (psycopg.errors.InsufficientPrivilege, psycopg.errors.GeneratedAlways)
    ):
        schema.maintenance_execute(
            "UPDATE sync_cold_start_plans SET cursor_binding_plan_id = NULL"
        )

    for statement in (
        "UPDATE pipeline_command_receipts SET outcome = 'succeeded'",
        "DELETE FROM pipeline_command_receipts",
        "TRUNCATE pipeline_command_receipts",
    ):
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(schema.auditor_dsn, autocommit=True) as connection:
                connection.execute(statement)


async def _put_runtime_checkpoint(schema, thread_id: str, updated_at: datetime) -> None:
    schema.execute(
        "INSERT INTO emails_log (id, status, updated_at) VALUES ("
        "%s, 'sent', %s::pg_catalog.timestamptz "
        "AT TIME ZONE pg_catalog.current_setting('TimeZone'))",
        (thread_id, updated_at),
    )
    async with await psycopg.AsyncConnection.connect(
        schema.runtime_dsn,
        autocommit=True,
    ) as conn:
        checkpoint = empty_checkpoint()
        values: dict[str, object] = {
            "email_id": thread_id,
            "content_ref": {
                "account_id": 8,
                "object_id": "00000000-0000-4000-8000-000000000127",
                "key_version": "v1",
                "sha256": "c" * 64,
            },
            "attachment_tokens": [],
            "pdf_token": None,
        }
        versions = {
            channel: f"{checkpoint['id']}:{index}"
            for index, channel in enumerate(values)
        }
        checkpoint["channel_values"] = values
        checkpoint["channel_versions"] = versions
        checkpoint["updated_channels"] = list(values)
        saver = AsyncPostgresSaver(conn)
        saved_config = await saver.aput(
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                }
            },
            checkpoint,
            {},
            versions,
        )
        await saver.aput(
            saved_config,
            checkpoint,
            {"source": "loop", "step": 1, "parents": {}},
            versions,
        )
        await saver.aput_writes(
            saved_config,
            [("write_probe", {"version": 1})],
            task_id="runtime-write-probe",
        )
        await saver.aput_writes(
            saved_config,
            [("write_probe", {"version": 2})],
            task_id="runtime-write-probe",
        )
        assert await saver.aget(saved_config) is not None
        assert len([item async for item in saver.alist(saved_config, limit=2)]) == 1
        row = await (
            await conn.execute(
                "SELECT pg_catalog.count(*) FROM checkpoints WHERE thread_id = %s",
                (thread_id,),
            )
        ).fetchone()
        assert row is not None and row[0] == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_checkpointer_crud_works_without_delete_capability(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    await _put_runtime_checkpoint(
        schema,
        "runtime-checkpointer-thread",
        datetime.now(UTC) - timedelta(hours=25),
    )

    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        schema.runtime_execute(
            "DELETE FROM checkpoints WHERE thread_id = 'runtime-checkpointer-thread'"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0006_maintenance_role_can_cleanup_checkpoints_without_mutating_governed_data(
    postgres_database_factory,
    tmp_path,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    now = datetime.now(UTC).replace(microsecond=0)
    thread_id = "maintenance-cleanup-thread"
    await _put_runtime_checkpoint(schema, thread_id, now - timedelta(hours=25))
    await require_maintenance_database_role(
        schema.maintenance_dsn,
        **schema.maintenance_identity,
    )

    clock = [now]
    cleaner = CheckpointCleaner(
        repository=PostgresCheckpointRepository(schema.maintenance_dsn),
        artifact_store=PlanArtifactStore(tmp_path / "maintenance-artifacts"),
        backup_verifier=Ed25519BackupReceiptVerifier(RECEIPT_PUBLIC_KEY),
        now=lambda: clock[0],
    )
    plan = await cleaner.plan(older_than=now - timedelta(hours=24), limit=1)
    assert [candidate.thread_id for candidate in plan.candidates] == [thread_id]

    clock[0] = now + timedelta(minutes=1)
    backup_id = "phase2-maintenance-backup"
    receipt = create_ed25519_signed_backup_receipt(
        private_seed=RECEIPT_PRIVATE_SEED,
        plan_id=plan.plan_id,
        database_fingerprint=plan.database_fingerprint,
        alembic_revision=plan.alembic_revision,
        checkpoint_revision=plan.checkpoint_revision,
        backup_id=backup_id,
        completed_at=clock[0],
        manifest_sha256="d" * 64,
    )
    report = await cleaner.run(
        plan.plan_id,
        dry_run=False,
        backup_id=backup_id,
        backup_receipt=receipt,
        service_quiesced=True,
        limit=1,
    )

    assert report.error_code is None
    assert report.deleted_thread_count == 1
    for relation in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
        assert (
            schema.scalar(
                f"SELECT pg_catalog.count(*) FROM {relation} WHERE thread_id = %s",
                (thread_id,),
            )
            == 0
        )
    schema.maintenance_execute("SELECT * FROM event_inbox LIMIT 0")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        schema.maintenance_execute(
            "UPDATE emails_log SET status = 'sent' WHERE id = "
            "'maintenance-cleanup-thread'"
        )
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        schema.maintenance_execute(
            "DELETE FROM emails_log WHERE id = 'maintenance-cleanup-thread'"
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "acl_drift",
    (
        "missing_checkpoint_select",
        "missing_checkpoint_delete",
        "unexpected_emails_update",
        "unexpected_event_inbox_update",
        "unexpected_event_inbox_delete",
        "select_grant_option",
    ),
)
async def test_0006_role_gates_reject_maintenance_acl_drift(
    postgres_database_factory,
    alembic_runner,
    acl_drift,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, "20260716_0006")
    maintenance = sql.Identifier(schema.maintenance_role)

    if acl_drift == "missing_checkpoint_select":
        schema.execute(
            sql.SQL("REVOKE SELECT ON checkpoints FROM {}").format(maintenance)
        )
    elif acl_drift == "missing_checkpoint_delete":
        schema.execute(
            sql.SQL("REVOKE DELETE ON checkpoints FROM {}").format(maintenance)
        )
    elif acl_drift == "unexpected_emails_update":
        schema.execute(sql.SQL("GRANT UPDATE ON emails_log TO {}").format(maintenance))
    elif acl_drift.startswith("unexpected_event_inbox_"):
        privilege = acl_drift.removeprefix("unexpected_event_inbox_").upper()
        schema.execute(
            sql.SQL("GRANT {} ON event_inbox TO {}").format(
                sql.SQL(privilege),
                maintenance,
            )
        )
    else:
        schema.execute(
            sql.SQL("GRANT SELECT ON emails_log TO {} WITH GRANT OPTION").format(
                maintenance
            )
        )

    for gate, dsn, identity in (
        (
            require_maintenance_database_role,
            schema.maintenance_dsn,
            schema.maintenance_identity,
        ),
        (
            require_migration_database_role,
            schema.dsn,
            schema.bootstrap_identity,
        ),
        (
            require_runtime_database_role,
            schema.runtime_dsn,
            schema.runtime_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "revision",
    (
        "20260710_0002",
        "20260710_0003",
        "20260713_0004",
        "20260713_0005",
    ),
)
async def test_legacy_runtime_revisions_fail_closed(
    postgres_database_factory,
    alembic_runner,
    revision,
):
    schema = postgres_database_factory()
    await _prepare_revision(schema, alembic_runner, revision)

    with pytest.raises(
        DatabaseRevisionError,
        match=r"expected one of \[20260716_0006\]",
    ):
        await require_runtime_database(
            schema.runtime_dsn,
            durable_inbox_enabled=False,
            ingestion_shadow_enabled=False,
            sync_reconciliation_enabled=False,
            role_separation_required=True,
            expected_runtime_role=schema.runtime_role,
            expected_migration_role=schema.migration_role,
            expected_maintenance_role=schema.maintenance_role,
            expected_auditor_role=schema.auditor_role,
            target_schema="public",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_checkpoint_plan_auditor_is_effectively_read_only(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    identity = {
        "expected_auditor_role": schema.auditor_role,
        "expected_runtime_role": schema.runtime_role,
        "expected_migration_role": schema.migration_role,
        "expected_maintenance_role": schema.maintenance_role,
        "target_schema": "public",
    }

    try:
        for gate, dsn, gate_identity in (
            (
                require_migration_database_role,
                schema.dsn,
                schema.bootstrap_identity,
            ),
            (
                require_runtime_database_role,
                schema.runtime_dsn,
                schema.runtime_identity,
            ),
            (
                require_maintenance_database_role,
                schema.maintenance_dsn,
                schema.maintenance_identity,
            ),
        ):
            await gate(dsn, **gate_identity)
        await require_checkpoint_auditor_database_role(schema.auditor_dsn, **identity)
        schema.admin_execute(
            "ALTER TABLE emails_log ADD COLUMN auditor_future_probe pg_catalog.text"
        )
        with psycopg.connect(schema.auditor_dsn, autocommit=True) as conn:
            for relation, columns in _AUDITOR_SELECT_COLUMNS.items():
                assert (
                    conn.execute(
                        sql.SQL("SELECT {} FROM {} LIMIT 0").format(
                            sql.SQL(", ").join(map(sql.Identifier, columns)),
                            sql.Identifier(relation),
                        )
                    ).description
                    is not None
                )
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute("SELECT subject FROM emails_log LIMIT 0")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute("SELECT auditor_future_probe FROM emails_log LIMIT 0")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute("SELECT * FROM emails_log LIMIT 0")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute("DELETE FROM checkpoints")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute("SELECT * FROM event_inbox")

        schema.admin_execute(
            sql.SQL("GRANT UPDATE ON emails_log TO {}").format(
                sql.Identifier(schema.auditor_role)
            )
        )
        for gate, dsn, gate_identity, error in (
            (
                require_migration_database_role,
                schema.dsn,
                schema.bootstrap_identity,
                DatabaseRoleError,
            ),
            (
                require_runtime_database_role,
                schema.runtime_dsn,
                schema.runtime_identity,
                DatabaseRoleError,
            ),
            (
                require_maintenance_database_role,
                schema.maintenance_dsn,
                schema.maintenance_identity,
                DatabaseRoleError,
            ),
            (
                require_checkpoint_auditor_database_role,
                schema.auditor_dsn,
                identity,
                CheckpointAuditorRoleError,
            ),
        ):
            with pytest.raises(error):
                await gate(dsn, **gate_identity)
    finally:
        schema.admin_execute(
            sql.SQL("REVOKE UPDATE ON emails_log FROM {}").format(
                sql.Identifier(schema.auditor_role)
            )
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_configured_role_gates_reject_second_exact_auditor(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    second_role = f"ai_exchange_test_s_{uuid4().hex}"
    second_password = f"SecondAuditor-{uuid4().hex}"
    second = sql.Identifier(second_role)
    database = sql.Identifier(schema.database_name)
    configured_identity = {
        "expected_auditor_role": schema.auditor_role,
        "expected_runtime_role": schema.runtime_role,
        "expected_migration_role": schema.migration_role,
        "expected_maintenance_role": schema.maintenance_role,
        "target_schema": "public",
    }

    schema.admin_execute(
        sql.SQL(
            "CREATE ROLE {} WITH LOGIN PASSWORD {} "
            "NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS NOINHERIT"
        ).format(second, sql.Literal(second_password))
    )
    schema.admin_execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, second)
    )
    schema.admin_execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(second))
    for relation, columns in _AUDITOR_SELECT_COLUMNS.items():
        schema.admin_execute(
            sql.SQL("GRANT SELECT ({}) ON TABLE {} TO {}").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.Identifier("public", relation),
                second,
            )
        )
    second_dsn = make_conninfo(
        schema.auditor_dsn,
        user=second_role,
        password=second_password,
    )

    try:
        with psycopg.connect(second_dsn, autocommit=True) as conn:
            assert (
                conn.execute("SELECT thread_id FROM checkpoints LIMIT 0").description
                is not None
            )

        for gate, dsn, identity, error in (
            (
                require_migration_database_role,
                schema.dsn,
                schema.bootstrap_identity,
                DatabaseRoleError,
            ),
            (
                require_runtime_database_role,
                schema.runtime_dsn,
                schema.runtime_identity,
                DatabaseRoleError,
            ),
            (
                require_maintenance_database_role,
                schema.maintenance_dsn,
                schema.maintenance_identity,
                DatabaseRoleError,
            ),
            (
                require_checkpoint_auditor_database_role,
                second_dsn,
                configured_identity,
                CheckpointAuditorRoleError,
            ),
        ):
            with pytest.raises(error):
                await gate(dsn, **identity)

        with pytest.raises(
            CheckpointAuditorRoleError,
            match="checkpoint_auditor_role_preflight_failed",
        ):
            await require_checkpoint_auditor_database_role(
                schema.auditor_dsn,
                **configured_identity,
            )
    finally:
        schema.admin_execute(
            sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(database, second)
        )
        schema.admin_execute(sql.SQL("DROP OWNED BY {}").format(second))
        schema.admin_execute(sql.SQL("DROP ROLE IF EXISTS {}").format(second))


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("unexpected_grantee", ["outsider", "public"])
async def test_checkpoint_auditor_gate_rejects_other_target_acl_grantees(
    postgres_database_factory,
    unexpected_grantee,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    outsider_role = f"ai_exchange_test_o_{uuid4().hex}"
    grantee = (
        sql.SQL("PUBLIC")
        if unexpected_grantee == "public"
        else sql.Identifier(outsider_role)
    )
    if unexpected_grantee == "outsider":
        schema.admin_execute(
            sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(grantee)
        )
    schema.admin_execute(
        sql.SQL("GRANT SELECT (id) ON TABLE emails_log TO {}").format(grantee)
    )

    try:
        with pytest.raises(
            CheckpointAuditorRoleError,
            match="checkpoint_auditor_role_preflight_failed",
        ):
            await require_checkpoint_auditor_database_role(
                schema.auditor_dsn,
                expected_auditor_role=schema.auditor_role,
                expected_runtime_role=schema.runtime_role,
                expected_migration_role=schema.migration_role,
                expected_maintenance_role=schema.maintenance_role,
                target_schema="public",
            )
    finally:
        schema.admin_execute(
            sql.SQL("REVOKE SELECT (id) ON TABLE emails_log FROM {}").format(grantee)
        )
        if unexpected_grantee == "outsider":
            schema.admin_execute(sql.SQL("DROP ROLE IF EXISTS {}").format(grantee))


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "managed_role_attribute",
    ["migration_role", "runtime_role", "maintenance_role"],
)
async def test_checkpoint_auditor_gate_rejects_inheriting_managed_role_delegate(
    postgres_database_factory,
    managed_role_attribute,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    managed_role = sql.Identifier(getattr(schema, managed_role_attribute))
    delegate_role = f"ai_exchange_test_d_{uuid4().hex}"
    delegate = sql.Identifier(delegate_role)
    schema.admin_execute(
        sql.SQL(
            "CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS INHERIT"
        ).format(delegate)
    )
    schema.admin_execute(sql.SQL("GRANT {} TO {}").format(managed_role, delegate))

    try:
        with pytest.raises(
            CheckpointAuditorRoleError,
            match="checkpoint_auditor_role_preflight_failed",
        ):
            await require_checkpoint_auditor_database_role(
                schema.auditor_dsn,
                expected_auditor_role=schema.auditor_role,
                expected_runtime_role=schema.runtime_role,
                expected_migration_role=schema.migration_role,
                expected_maintenance_role=schema.maintenance_role,
                target_schema="public",
            )
    finally:
        schema.admin_execute(
            sql.SQL("REVOKE {} FROM {}").format(managed_role, delegate)
        )
        schema.admin_execute(sql.SQL("DROP ROLE IF EXISTS {}").format(delegate))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_checkpoint_auditor_gate_rejects_added_public_system_acl(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    schema.admin_execute(
        "GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_file(text) TO PUBLIC"
    )

    try:
        with pytest.raises(
            CheckpointAuditorRoleError,
            match="checkpoint_auditor_role_preflight_failed",
        ):
            await require_checkpoint_auditor_database_role(
                schema.auditor_dsn,
                expected_auditor_role=schema.auditor_role,
                expected_runtime_role=schema.runtime_role,
                expected_migration_role=schema.migration_role,
                expected_maintenance_role=schema.maintenance_role,
                target_schema="public",
            )
    finally:
        schema.admin_execute(
            "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_read_file(text) FROM PUBLIC"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_all_role_gates_reject_public_create_on_peer_schema_before_write(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    peer_schema_name = f"peer_create_{uuid4().hex}"
    peer_schema = sql.Identifier(peer_schema_name)
    schema.admin_execute(sql.SQL("CREATE SCHEMA {}").format(peer_schema))
    schema.admin_execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON SCHEMA {} FROM PUBLIC").format(peer_schema)
    )
    schema.admin_execute(
        sql.SQL("GRANT CREATE ON SCHEMA {} TO PUBLIC").format(peer_schema)
    )
    auditor_identity = {
        "expected_auditor_role": schema.auditor_role,
        "expected_runtime_role": schema.runtime_role,
        "expected_migration_role": schema.migration_role,
        "expected_maintenance_role": schema.maintenance_role,
        "target_schema": "public",
    }

    try:
        with psycopg.connect(schema.auditor_dsn, autocommit=True) as conn:
            assert conn.execute(
                "SELECT pg_catalog.has_schema_privilege(%s, 'CREATE'), "
                "pg_catalog.has_schema_privilege(%s, 'USAGE')",
                (peer_schema_name, peer_schema_name),
            ).fetchone() == (True, False)

        for gate, dsn, identity, error in (
            (
                require_migration_database_role,
                schema.dsn,
                schema.bootstrap_identity,
                DatabaseRoleError,
            ),
            (
                require_runtime_database_role,
                schema.runtime_dsn,
                schema.runtime_identity,
                DatabaseRoleError,
            ),
            (
                require_maintenance_database_role,
                schema.maintenance_dsn,
                schema.maintenance_identity,
                DatabaseRoleError,
            ),
            (
                require_checkpoint_auditor_database_role,
                schema.auditor_dsn,
                auditor_identity,
                CheckpointAuditorRoleError,
            ),
        ):
            with pytest.raises(error):
                await gate(dsn, **identity)

        with psycopg.connect(schema.admin_dsn, autocommit=True) as conn:
            assert conn.execute(
                "SELECT NOT EXISTS ("
                "SELECT 1 FROM pg_catalog.pg_class AS relation "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "WHERE relation_schema.nspname = %s "
                "AND relation.relname = 'guarded_write_probe')",
                (peer_schema_name,),
            ).fetchone() == (True,)
    finally:
        schema.admin_execute(sql.SQL("DROP SCHEMA {} CASCADE").format(peer_schema))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_generic_outsider_cannot_connect_to_managed_database(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    outsider_role = f"ai_exchange_test_o_{uuid4().hex}"
    outsider_password = f"Outsider-{uuid4().hex}"
    outsider = sql.Identifier(outsider_role)
    schema.admin_execute(
        sql.SQL(
            "CREATE ROLE {} WITH LOGIN PASSWORD {} "
            "NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS NOINHERIT"
        ).format(outsider, sql.Literal(outsider_password))
    )
    outsider_dsn = make_conninfo(
        schema.runtime_dsn,
        user=outsider_role,
        password=outsider_password,
    )

    try:
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(outsider_dsn)
    finally:
        schema.admin_execute(sql.SQL("DROP ROLE IF EXISTS {}").format(outsider))


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe_capability",
    ["extra_membership", "connect_grant_option"],
)
async def test_all_role_gates_reject_unsafe_auditor_capability(
    postgres_database_factory,
    unsafe_capability,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    extra_role = f"ai_exchange_test_x_{uuid4().hex}"
    auditor = sql.Identifier(schema.auditor_role)
    extra = sql.Identifier(extra_role)
    database = sql.Identifier(schema.database_name)

    if unsafe_capability == "extra_membership":
        schema.admin_execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(extra))
        schema.admin_execute(sql.SQL("GRANT {} TO {}").format(extra, auditor))
    else:
        schema.admin_execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {} WITH GRANT OPTION").format(
                database, auditor
            )
        )

    try:
        for gate, dsn, identity in (
            (
                require_migration_database_role,
                schema.dsn,
                schema.bootstrap_identity,
            ),
            (
                require_runtime_database_role,
                schema.runtime_dsn,
                schema.runtime_identity,
            ),
            (
                require_maintenance_database_role,
                schema.maintenance_dsn,
                schema.maintenance_identity,
            ),
            (
                require_checkpoint_auditor_database_role,
                schema.auditor_dsn,
                {
                    "expected_auditor_role": schema.auditor_role,
                    "expected_runtime_role": schema.runtime_role,
                    "expected_migration_role": schema.migration_role,
                    "expected_maintenance_role": schema.maintenance_role,
                    "target_schema": "public",
                },
            ),
        ):
            with pytest.raises((DatabaseRoleError, CheckpointAuditorRoleError)):
                await gate(dsn, **identity)
    finally:
        if unsafe_capability == "extra_membership":
            schema.admin_execute(sql.SQL("REVOKE {} FROM {}").format(extra, auditor))
        else:
            schema.admin_execute(
                sql.SQL(
                    "REVOKE GRANT OPTION FOR CONNECT ON DATABASE {} FROM {}"
                ).format(
                    database,
                    auditor,
                )
            )
        if unsafe_capability == "extra_membership":
            schema.admin_execute(sql.SQL("DROP ROLE IF EXISTS {}").format(extra))


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unexpected_acl",
    [
        "column_outside_target",
        "large_object",
        "language",
        "tablespace",
    ],
)
async def test_all_role_gates_reject_auditor_direct_acl_outside_manifest(
    postgres_database_factory,
    unexpected_acl,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    auditor = sql.Identifier(schema.auditor_role)
    probe_schema_name = f"auditor_probe_{uuid4().hex}"
    probe_schema = sql.Identifier(probe_schema_name)
    large_object_oid = None

    if unexpected_acl == "column_outside_target":
        schema.admin_execute(sql.SQL("CREATE SCHEMA {}").format(probe_schema))
        schema.admin_execute(
            sql.SQL("CREATE TABLE {}.probe (value pg_catalog.text)").format(
                probe_schema
            )
        )
        schema.admin_execute(
            sql.SQL("GRANT SELECT (value) ON {}.probe TO {}").format(
                probe_schema,
                auditor,
            )
        )
    elif unexpected_acl == "large_object":
        with psycopg.connect(schema.admin_dsn, autocommit=True) as conn:
            row = conn.execute("SELECT pg_catalog.lo_create(0)").fetchone()
            assert row is not None
            large_object_oid = row[0]
            conn.execute(
                sql.SQL("GRANT SELECT ON LARGE OBJECT {} TO {}").format(
                    sql.Literal(large_object_oid),
                    auditor,
                )
            )
    elif unexpected_acl == "language":
        schema.admin_execute(
            sql.SQL("GRANT USAGE ON LANGUAGE plpgsql TO {}").format(auditor)
        )
    else:
        schema.admin_execute(
            sql.SQL("GRANT CREATE ON TABLESPACE pg_default TO {}").format(auditor)
        )

    auditor_identity = {
        "expected_auditor_role": schema.auditor_role,
        "expected_runtime_role": schema.runtime_role,
        "expected_migration_role": schema.migration_role,
        "expected_maintenance_role": schema.maintenance_role,
        "target_schema": "public",
    }
    try:
        for gate, dsn, identity in (
            (
                require_migration_database_role,
                schema.dsn,
                schema.bootstrap_identity,
            ),
            (
                require_runtime_database_role,
                schema.runtime_dsn,
                schema.runtime_identity,
            ),
            (
                require_maintenance_database_role,
                schema.maintenance_dsn,
                schema.maintenance_identity,
            ),
            (
                require_checkpoint_auditor_database_role,
                schema.auditor_dsn,
                auditor_identity,
            ),
        ):
            with pytest.raises((DatabaseRoleError, CheckpointAuditorRoleError)):
                await gate(dsn, **identity)
    finally:
        if unexpected_acl == "column_outside_target":
            schema.admin_execute(sql.SQL("DROP SCHEMA {} CASCADE").format(probe_schema))
        elif unexpected_acl == "large_object":
            assert large_object_oid is not None
            with psycopg.connect(schema.admin_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("REVOKE SELECT ON LARGE OBJECT {} FROM {}").format(
                        sql.Literal(large_object_oid),
                        auditor,
                    )
                )
                conn.execute(
                    "SELECT pg_catalog.lo_unlink(%s)",
                    (large_object_oid,),
                )
        elif unexpected_acl == "language":
            schema.admin_execute(
                sql.SQL("REVOKE USAGE ON LANGUAGE plpgsql FROM {}").format(auditor)
            )
        else:
            schema.admin_execute(
                sql.SQL("REVOKE CREATE ON TABLESPACE pg_default FROM {}").format(
                    auditor
                )
            )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe_capability",
    ["inbound_member", "reader_admin_option"],
)
async def test_all_role_gates_reject_delegable_auditor_capability(
    postgres_database_factory,
    unsafe_capability,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    delegate_role = f"ai_exchange_test_d_{uuid4().hex}"
    delegate_password = f"Delegate-{uuid4().hex}"
    auditor = sql.Identifier(schema.auditor_role)
    delegate = sql.Identifier(delegate_role)

    schema.admin_execute(
        sql.SQL(
            "CREATE ROLE {} WITH LOGIN PASSWORD {} "
            "NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS INHERIT"
        ).format(delegate, sql.Literal(delegate_password))
    )
    if unsafe_capability == "inbound_member":
        schema.admin_execute(sql.SQL("GRANT {} TO {}").format(auditor, delegate))
        delegate_dsn = make_conninfo(
            schema.runtime_dsn,
            user=delegate_role,
            password=delegate_password,
        )
        with psycopg.connect(delegate_dsn, autocommit=True) as conn:
            assert conn.execute("SELECT count(*) FROM checkpoints").fetchone() == (0,)
    else:
        schema.admin_execute(
            sql.SQL("GRANT pg_read_all_data TO {} WITH ADMIN OPTION").format(auditor)
        )

    auditor_identity = {
        "expected_auditor_role": schema.auditor_role,
        "expected_runtime_role": schema.runtime_role,
        "expected_migration_role": schema.migration_role,
        "expected_maintenance_role": schema.maintenance_role,
        "target_schema": "public",
    }

    try:
        for gate, dsn, identity in (
            (
                require_migration_database_role,
                schema.dsn,
                schema.bootstrap_identity,
            ),
            (
                require_runtime_database_role,
                schema.runtime_dsn,
                schema.runtime_identity,
            ),
            (
                require_maintenance_database_role,
                schema.maintenance_dsn,
                schema.maintenance_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
        with pytest.raises(
            CheckpointAuditorRoleError,
            match="checkpoint_auditor_role_preflight_failed",
        ):
            await require_checkpoint_auditor_database_role(
                schema.auditor_dsn,
                **auditor_identity,
            )
    finally:
        if unsafe_capability == "inbound_member":
            schema.admin_execute(sql.SQL("REVOKE {} FROM {}").format(auditor, delegate))
        else:
            schema.admin_execute(
                sql.SQL("REVOKE pg_read_all_data FROM {}").format(auditor)
            )
        schema.admin_execute(sql.SQL("DROP OWNED BY {}").format(delegate))
        schema.admin_execute(sql.SQL("DROP ROLE IF EXISTS {}").format(delegate))
