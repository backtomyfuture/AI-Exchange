"""Read-only provenance checks for application and checkpoint column types."""

from __future__ import annotations

import hashlib
from typing import Final

import psycopg

from src.db.access_contract import (
    GREENFIELD_DATABASE_REVISION,
    PHASE2_CHECK_CONSTRAINT_SHA256,  # noqa: F401 - compatibility export
    PHASE2_CHECK_CONSTRAINT_SHA256_BY_REVISION,
    PHASE2_CHECK_CONSTRAINT_SHA256_OVERRIDES_BY_REVISION,  # noqa: F401
    PHASE2_DEFAULT_EXPRESSIONS,  # noqa: F401 - compatibility export
    PHASE2_DEFAULT_EXPRESSIONS_BY_REVISION,
    PHASE2_GENERATED_EXPRESSION_SHA256_BY_REVISION,
    PHASE2_INDEX_SPECS,  # noqa: F401 - compatibility export
    PHASE2_INDEX_SPECS_BY_REVISION,
    PHASE2_RELATIONS,
    PHASE2_RELATIONS_BY_REVISION,
    PHASE2_UNIQUE_CONSTRAINTS,  # noqa: F401 - compatibility export
    PHASE2_UNIQUE_CONSTRAINTS_BY_REVISION,
    PHASE2_VIEW_SPECS_BY_REVISION,
    SYNC_RECONCILIATION_DATABASE_REVISION,
)


class DatabaseSchemaContractError(RuntimeError):
    """Raised when deployed columns do not match the trusted type contract."""


_CHECKPOINT_RELATIONS: Final = frozenset(
    {
        "checkpoint_migrations",
        "checkpoints",
        "checkpoint_blobs",
        "checkpoint_writes",
    }
)


_EXPECTED_COLUMN_TYPES: Final[dict[tuple[str, str], str]] = {
    ("alembic_version", "version_num"): "varchar",
    ("emails_log", "id"): "text",
    ("emails_log", "subject"): "text",
    ("emails_log", "sender"): "text",
    ("emails_log", "received_at"): "timestamp",
    ("emails_log", "status"): "text",
    ("emails_log", "classification"): "jsonb",
    ("emails_log", "draft_content"): "text",
    ("emails_log", "processed_at"): "timestamp",
    ("emails_log", "updated_at"): "timestamp",
    ("emails_log", "routing_log"): "jsonb",
    ("emails_log", "active_skills"): "jsonb",
    ("emails_log", "original_draft"): "text",
    ("emails_log", "final_draft"): "text",
    ("emails_log", "draft_diff"): "text",
    ("emails_log", "approver_user_id"): "text",
    ("emails_log", "rejection_reason"): "text",
    ("emails_log", "error_message"): "text",
    ("emails_log", "content_ref"): "jsonb",
    ("emails_log", "version"): "int8",
    ("app_kv_store", "key"): "text",
    ("app_kv_store", "value"): "text",
    ("app_kv_store", "updated_at"): "timestamp",
    ("pipeline_ownership", "account_id"): "int8",
    ("pipeline_ownership", "generation"): "int8",
    ("pipeline_ownership", "pipeline_name"): "text",
    ("pipeline_ownership", "state"): "text",
    ("pipeline_ownership", "fencing_token"): "int8",
    ("pipeline_ownership", "created_by"): "text",
    ("pipeline_ownership", "reason"): "text",
    ("pipeline_ownership", "created_at"): "timestamptz",
    ("pipeline_ownership", "updated_at"): "timestamptz",
    ("event_inbox", "id"): "uuid",
    ("event_inbox", "account_id"): "int8",
    ("event_inbox", "external_email_id"): "text",
    ("event_inbox", "folder_key"): "text",
    ("event_inbox", "source"): "text",
    ("event_inbox", "raw_event_type"): "text",
    ("event_inbox", "change_kind"): "text",
    ("event_inbox", "dedupe_key"): "bpchar",
    ("event_inbox", "source_version"): "text",
    ("event_inbox", "source_event_at"): "timestamptz",
    ("event_inbox", "payload"): "jsonb",
    ("event_inbox", "processing_policy"): "text",
    ("event_inbox", "pipeline_name"): "text",
    ("event_inbox", "generation"): "int8",
    ("event_inbox", "fencing_token"): "int8",
    ("event_inbox", "status"): "text",
    ("event_inbox", "lease_owner"): "text",
    ("event_inbox", "lease_until"): "timestamptz",
    ("event_inbox", "attempts"): "int8",
    ("event_inbox", "available_at"): "timestamptz",
    ("event_inbox", "processing_started_at"): "timestamptz",
    ("event_inbox", "effect_started_at"): "timestamptz",
    ("event_inbox", "safe_error_code"): "text",
    ("event_inbox", "safe_error_summary"): "text",
    ("event_inbox", "received_at"): "timestamptz",
    ("event_inbox", "updated_at"): "timestamptz",
    ("sync_cursors", "account_id"): "int8",
    ("sync_cursors", "folder_key"): "text",
    ("sync_cursors", "cursor"): "text",
    ("sync_cursors", "status"): "text",
    ("sync_cursors", "blocked_reason_code"): "text",
    ("sync_cursors", "contract_fingerprint"): "bpchar",
    ("sync_cursors", "blocked_at"): "timestamptz",
    ("sync_cursors", "version"): "int8",
    ("sync_cursors", "last_success_at"): "timestamptz",
    ("sync_cursors", "last_attempt_at"): "timestamptz",
    ("sync_cursors", "created_at"): "timestamptz",
    ("sync_cursors", "updated_at"): "timestamptz",
    ("emails", "id"): "uuid",
    ("emails", "account_id"): "int8",
    ("emails", "external_email_id"): "text",
    ("emails", "source_folder_key"): "text",
    ("emails", "status"): "text",
    ("emails", "version"): "int8",
    ("emails", "owner_generation"): "int8",
    ("emails", "owner_fencing_token"): "int8",
    ("emails", "processing_inbox_id"): "uuid",
    ("emails", "create_seen_at"): "timestamptz",
    ("emails", "processing_started_at"): "timestamptz",
    ("emails", "source_deleted_at"): "timestamptz",
    ("emails", "external_effects_started_at"): "timestamptz",
    ("emails", "safe_error_code"): "text",
    ("emails", "safe_error_summary"): "text",
    ("emails", "content_ref"): "jsonb",
    ("emails", "is_read"): "bool",
    ("emails", "is_read_refresh_required"): "bool",
    ("emails", "created_at"): "timestamptz",
    ("emails", "updated_at"): "timestamptz",
    ("audit_events", "id"): "uuid",
    ("audit_events", "event_key"): "bpchar",
    ("audit_events", "account_id"): "int8",
    ("audit_events", "email_id"): "uuid",
    ("audit_events", "object_type"): "text",
    ("audit_events", "object_fingerprint"): "bpchar",
    ("audit_events", "action"): "text",
    ("audit_events", "result"): "text",
    ("audit_events", "actor"): "text",
    ("audit_events", "reason"): "text",
    ("audit_events", "safe_metadata"): "jsonb",
    ("audit_events", "created_at"): "timestamptz",
    ("pipeline_shadow_comparisons", "id"): "uuid",
    ("pipeline_shadow_comparisons", "account_id"): "int8",
    ("pipeline_shadow_comparisons", "generation"): "int8",
    ("pipeline_shadow_comparisons", "fencing_token"): "int8",
    ("pipeline_shadow_comparisons", "pipeline_name"): "text",
    ("pipeline_shadow_comparisons", "candidate_pipeline_name"): "text",
    ("pipeline_shadow_comparisons", "candidate_build_id"): "text",
    ("pipeline_shadow_comparisons", "candidate_config_hash"): "bpchar",
    ("pipeline_shadow_comparisons", "event_key"): "bpchar",
    ("pipeline_shadow_comparisons", "input_hash"): "bpchar",
    ("pipeline_shadow_comparisons", "legacy_status"): "text",
    ("pipeline_shadow_comparisons", "shadow_status"): "text",
    ("pipeline_shadow_comparisons", "comparison_status"): "text",
    ("pipeline_shadow_comparisons", "legacy_decision_hash"): "bpchar",
    ("pipeline_shadow_comparisons", "legacy_failure_code"): "text",
    ("pipeline_shadow_comparisons", "shadow_decision_hash"): "bpchar",
    ("pipeline_shadow_comparisons", "shadow_failure_code"): "text",
    ("pipeline_shadow_comparisons", "safe_metadata"): "jsonb",
    ("pipeline_shadow_comparisons", "created_at"): "timestamptz",
    ("pipeline_shadow_comparisons", "updated_at"): "timestamptz",
    ("processed_emails", "id"): "text",
    ("processed_emails", "processed_at"): "timestamp",
    ("checkpoint_migrations", "v"): "int4",
    ("checkpoints", "thread_id"): "text",
    ("checkpoints", "checkpoint_ns"): "text",
    ("checkpoints", "checkpoint_id"): "text",
    ("checkpoints", "parent_checkpoint_id"): "text",
    ("checkpoints", "type"): "text",
    ("checkpoints", "checkpoint"): "jsonb",
    ("checkpoints", "metadata"): "jsonb",
    ("checkpoint_blobs", "thread_id"): "text",
    ("checkpoint_blobs", "checkpoint_ns"): "text",
    ("checkpoint_blobs", "channel"): "text",
    ("checkpoint_blobs", "version"): "text",
    ("checkpoint_blobs", "type"): "text",
    ("checkpoint_blobs", "blob"): "bytea",
    ("checkpoint_writes", "thread_id"): "text",
    ("checkpoint_writes", "checkpoint_ns"): "text",
    ("checkpoint_writes", "checkpoint_id"): "text",
    ("checkpoint_writes", "task_id"): "text",
    ("checkpoint_writes", "idx"): "int4",
    ("checkpoint_writes", "channel"): "text",
    ("checkpoint_writes", "type"): "text",
    ("checkpoint_writes", "blob"): "bytea",
    ("checkpoint_writes", "task_path"): "text",
}

_SYNC_RECONCILIATION_COLUMN_TYPES: Final[dict[tuple[str, str], str]] = {
    ("sync_cursors", "transient_failures"): "int8",
    ("sync_cursors", "retry_after_at"): "timestamptz",
    ("sync_cursors", "cold_start_plan_id"): "uuid",
    ("sync_cursors", "cold_start_plan_state"): "text",
    ("sync_cold_start_plans", "plan_id"): "uuid",
    ("sync_cold_start_plans", "account_id"): "int8",
    ("sync_cold_start_plans", "folder_key"): "text",
    ("sync_cold_start_plans", "expected_cursor_status"): "text",
    ("sync_cold_start_plans", "expected_cursor"): "text",
    ("sync_cold_start_plans", "expected_cursor_version"): "int8",
    ("sync_cold_start_plans", "pipeline_name"): "text",
    ("sync_cold_start_plans", "generation"): "int8",
    ("sync_cold_start_plans", "fencing_token"): "int8",
    ("sync_cold_start_plans", "state"): "text",
    ("sync_cold_start_plans", "version"): "int8",
    ("sync_cold_start_plans", "preview_cursor"): "text",
    ("sync_cold_start_plans", "preview_cursor_version"): "int8",
    ("sync_cold_start_plans", "boundary_cursor"): "text",
    ("sync_cold_start_plans", "boundary_cursor_version"): "int8",
    ("sync_cold_start_plans", "apply_cursor"): "text",
    ("sync_cold_start_plans", "apply_cursor_version"): "int8",
    ("sync_cold_start_plans", "cursor_binding_plan_id"): "uuid",
    ("sync_cold_start_plans", "rolling_hash"): "bpchar",
    ("sync_cold_start_plans", "page_count"): "int8",
    ("sync_cold_start_plans", "item_count"): "int8",
    ("sync_cold_start_plans", "redacted_samples"): "jsonb",
    ("sync_cold_start_plans", "contract_fingerprint"): "bpchar",
    ("sync_cold_start_plans", "folder_scope_config_hash"): "bpchar",
    ("sync_cold_start_plans", "plan_hash"): "bpchar",
    ("sync_cold_start_plans", "actor"): "text",
    ("sync_cold_start_plans", "reason"): "text",
    ("sync_cold_start_plans", "blocked_reason_code"): "text",
    ("sync_cold_start_plans", "blocked_fingerprint"): "bpchar",
    ("sync_cold_start_plans", "expires_at"): "timestamptz",
    ("sync_cold_start_plans", "ready_at"): "timestamptz",
    ("sync_cold_start_plans", "approved_at"): "timestamptz",
    ("sync_cold_start_plans", "completed_at"): "timestamptz",
    ("sync_cold_start_plans", "blocked_at"): "timestamptz",
    ("sync_cold_start_plans", "created_at"): "timestamptz",
    ("sync_cold_start_plans", "updated_at"): "timestamptz",
    ("pipeline_command_receipts", "id"): "uuid",
    ("pipeline_command_receipts", "account_id"): "int8",
    ("pipeline_command_receipts", "command_name"): "text",
    ("pipeline_command_receipts", "idempotency_key_hash"): "bpchar",
    ("pipeline_command_receipts", "canonical_payload_hash"): "bpchar",
    ("pipeline_command_receipts", "outcome"): "text",
    ("pipeline_command_receipts", "result_type"): "text",
    ("pipeline_command_receipts", "result_id"): "text",
    ("pipeline_command_receipts", "result_hash"): "bpchar",
    ("pipeline_command_receipts", "authority_epoch"): "int8",
    ("pipeline_command_receipts", "created_at"): "timestamptz",
}

_GREENFIELD_COLUMN_TYPES: Final[dict[tuple[str, str], str]] = {
    ("emails", "owner_authority_epoch"): "int8",
    ("emails", "owner_capability_hash"): "bpchar",
    ("emails", "processing_execution_epoch"): "int8",
    ("event_inbox", "authority_epoch"): "int8",
    ("event_inbox", "capability_hash"): "bpchar",
    ("event_inbox", "execution_epoch"): "int8",
    ("event_inbox", "lease_session_id"): "uuid",
    ("pipeline_folder_scopes", "account_id"): "int8",
    ("pipeline_folder_scopes", "canonical_key"): "text",
    ("pipeline_folder_scopes", "created_at"): "timestamptz",
    ("pipeline_folder_scopes", "event_policy_matrix"): "jsonb",
    ("pipeline_folder_scopes", "initialization_id"): "uuid",
    ("pipeline_folder_scopes", "policy_manifest_hash"): "bpchar",
    ("pipeline_folder_scopes", "scope_hash"): "bpchar",
    ("pipeline_folder_scopes", "sync_folder"): "text",
    ("pipeline_folder_scopes", "webhook_ids"): "jsonb",
    ("pipeline_initializations", "account_id"): "int8",
    ("pipeline_initializations", "actor"): "text",
    ("pipeline_initializations", "authority_epoch"): "int8",
    ("pipeline_initializations", "authority_version"): "int8",
    ("pipeline_initializations", "capability_hash"): "bpchar",
    ("pipeline_initializations", "capability_stage_ordinal"): "int2",
    ("pipeline_initializations", "command_receipt_id"): "uuid",
    ("pipeline_initializations", "created_at"): "timestamptz",
    ("pipeline_initializations", "fencing_token"): "int8",
    ("pipeline_initializations", "generation"): "int8",
    ("pipeline_initializations", "initialization_id"): "uuid",
    ("pipeline_initializations", "pipeline_name"): "text",
    ("pipeline_initializations", "policy_manifest_hash"): "bpchar",
    ("pipeline_initializations", "reason"): "text",
    ("pipeline_initializations", "receipt_command_name"): "text",
    ("pipeline_initializations", "transaction_id"): "text",
    ("pipeline_runtime_authority", "account_id"): "int8",
    ("pipeline_runtime_authority", "authority_epoch"): "int8",
    ("pipeline_runtime_authority", "build_id"): "text",
    ("pipeline_runtime_authority", "capability_hash"): "bpchar",
    ("pipeline_runtime_authority", "capability_stage_ordinal"): "int2",
    ("pipeline_runtime_authority", "config_hash"): "bpchar",
    ("pipeline_runtime_authority", "created_at"): "timestamptz",
    ("pipeline_runtime_authority", "fencing_token"): "int8",
    ("pipeline_runtime_authority", "generation"): "int8",
    ("pipeline_runtime_authority", "initialization_id"): "uuid",
    ("pipeline_runtime_authority", "pipeline_name"): "text",
    ("pipeline_runtime_authority", "policy_manifest_hash"): "bpchar",
    ("pipeline_runtime_authority", "protocol_version"): "int8",
    ("pipeline_runtime_authority", "schema_revision"): "text",
    ("pipeline_runtime_authority", "state"): "text",
    ("pipeline_runtime_authority", "updated_at"): "timestamptz",
    ("pipeline_runtime_authority", "version"): "int8",
    ("pipeline_runtime_capabilities", "adapter_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "capability_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "config_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "created_at"): "timestamptz",
    ("pipeline_runtime_capabilities", "evidence_manifest_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "minimum_build_id"): "text",
    ("pipeline_runtime_capabilities", "policy_manifest_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "predecessor_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "predecessor_stage_ordinal"): "int2",
    ("pipeline_runtime_capabilities", "protocol_version"): "int8",
    ("pipeline_runtime_capabilities", "schema_digest"): "bpchar",
    ("pipeline_runtime_capabilities", "schema_revision"): "text",
    ("pipeline_runtime_capabilities", "stage"): "text",
    ("pipeline_runtime_capabilities", "stage_ordinal"): "int2",
    ("pipeline_runtime_instances", "accepted_count"): "int8",
    ("pipeline_runtime_instances", "account_id"): "int8",
    ("pipeline_runtime_instances", "authority_epoch"): "int8",
    ("pipeline_runtime_instances", "build_id"): "text",
    ("pipeline_runtime_instances", "capability_hash"): "bpchar",
    ("pipeline_runtime_instances", "capability_stage_ordinal"): "int2",
    ("pipeline_runtime_instances", "config_hash"): "bpchar",
    ("pipeline_runtime_instances", "fencing_token"): "int8",
    ("pipeline_runtime_instances", "generation"): "int8",
    ("pipeline_runtime_instances", "heartbeat_at"): "timestamptz",
    ("pipeline_runtime_instances", "instance_id"): "text",
    ("pipeline_runtime_instances", "lease_until"): "timestamptz",
    ("pipeline_runtime_instances", "lease_version"): "int8",
    ("pipeline_runtime_instances", "lifecycle"): "text",
    ("pipeline_runtime_instances", "protocol_version"): "int8",
    ("pipeline_runtime_instances", "registered_at"): "timestamptz",
    ("pipeline_runtime_instances", "rejected_count"): "int8",
    ("pipeline_runtime_instances", "schema_revision"): "text",
    ("pipeline_runtime_instances", "session_id"): "uuid",
    ("pipeline_runtime_instances", "updated_at"): "timestamptz",
    ("pipeline_runtime_instances", "workload"): "text",
}

_PHASE2_COLUMN_TYPES_BY_REVISION: Final[dict[str, dict[tuple[str, str], str]]] = {
    revision: {
        column: type_name
        for column, type_name in _EXPECTED_COLUMN_TYPES.items()
        if column[0] in relations
    }
    for revision, relations in PHASE2_RELATIONS_BY_REVISION.items()
}
_PHASE2_COLUMN_TYPES_BY_REVISION[SYNC_RECONCILIATION_DATABASE_REVISION] = {
    **_PHASE2_COLUMN_TYPES_BY_REVISION[SYNC_RECONCILIATION_DATABASE_REVISION],
    **_SYNC_RECONCILIATION_COLUMN_TYPES,
}
_PHASE2_COLUMN_TYPES_BY_REVISION[GREENFIELD_DATABASE_REVISION] = {
    column: type_name
    for column, type_name in {
        **_PHASE2_COLUMN_TYPES_BY_REVISION[SYNC_RECONCILIATION_DATABASE_REVISION],
        **_GREENFIELD_COLUMN_TYPES,
    }.items()
    if column[0] in PHASE2_RELATIONS_BY_REVISION[GREENFIELD_DATABASE_REVISION]
}

_PHASE2_NULLABLE_COLUMNS: Final = frozenset(
    {
        ("pipeline_ownership", "reason"),
        ("event_inbox", "source_version"),
        ("event_inbox", "source_event_at"),
        ("event_inbox", "lease_owner"),
        ("event_inbox", "lease_until"),
        ("event_inbox", "processing_started_at"),
        ("event_inbox", "effect_started_at"),
        ("event_inbox", "safe_error_code"),
        ("event_inbox", "safe_error_summary"),
        ("sync_cursors", "cursor"),
        ("sync_cursors", "blocked_reason_code"),
        ("sync_cursors", "contract_fingerprint"),
        ("sync_cursors", "blocked_at"),
        ("sync_cursors", "last_success_at"),
        ("sync_cursors", "last_attempt_at"),
        ("emails", "processing_inbox_id"),
        ("emails", "create_seen_at"),
        ("emails", "processing_started_at"),
        ("emails", "source_deleted_at"),
        ("emails", "external_effects_started_at"),
        ("emails", "safe_error_code"),
        ("emails", "safe_error_summary"),
        ("emails", "content_ref"),
        ("emails", "is_read"),
        ("audit_events", "email_id"),
        ("audit_events", "reason"),
        ("pipeline_shadow_comparisons", "legacy_decision_hash"),
        ("pipeline_shadow_comparisons", "legacy_failure_code"),
        ("pipeline_shadow_comparisons", "shadow_decision_hash"),
        ("pipeline_shadow_comparisons", "shadow_failure_code"),
    }
)

_PHASE2_DEFAULTED_COLUMNS: Final = frozenset(
    {
        ("pipeline_ownership", "created_at"),
        ("pipeline_ownership", "updated_at"),
        ("event_inbox", "payload"),
        ("event_inbox", "attempts"),
        ("event_inbox", "available_at"),
        ("event_inbox", "received_at"),
        ("event_inbox", "updated_at"),
        ("sync_cursors", "version"),
        ("sync_cursors", "created_at"),
        ("sync_cursors", "updated_at"),
        ("emails", "version"),
        ("emails", "is_read_refresh_required"),
        ("emails", "created_at"),
        ("emails", "updated_at"),
        ("audit_events", "safe_metadata"),
        ("audit_events", "created_at"),
        ("pipeline_shadow_comparisons", "safe_metadata"),
        ("pipeline_shadow_comparisons", "created_at"),
        ("pipeline_shadow_comparisons", "updated_at"),
    }
)

_SYNC_RECONCILIATION_NULLABLE_COLUMNS: Final = frozenset(
    {
        ("sync_cursors", "retry_after_at"),
        ("sync_cursors", "cold_start_plan_id"),
        ("sync_cursors", "cold_start_plan_state"),
        ("sync_cold_start_plans", "expected_cursor"),
        ("sync_cold_start_plans", "preview_cursor"),
        ("sync_cold_start_plans", "boundary_cursor"),
        ("sync_cold_start_plans", "boundary_cursor_version"),
        ("sync_cold_start_plans", "apply_cursor"),
        ("sync_cold_start_plans", "apply_cursor_version"),
        ("sync_cold_start_plans", "cursor_binding_plan_id"),
        ("sync_cold_start_plans", "rolling_hash"),
        ("sync_cold_start_plans", "plan_hash"),
        ("sync_cold_start_plans", "blocked_reason_code"),
        ("sync_cold_start_plans", "blocked_fingerprint"),
        ("sync_cold_start_plans", "ready_at"),
        ("sync_cold_start_plans", "approved_at"),
        ("sync_cold_start_plans", "completed_at"),
        ("sync_cold_start_plans", "blocked_at"),
    }
)

_SYNC_RECONCILIATION_DEFAULTED_COLUMNS: Final = frozenset(
    {
        ("sync_cursors", "transient_failures"),
        ("sync_cold_start_plans", "version"),
        ("sync_cold_start_plans", "preview_cursor_version"),
        ("sync_cold_start_plans", "page_count"),
        ("sync_cold_start_plans", "item_count"),
        ("sync_cold_start_plans", "redacted_samples"),
        ("sync_cold_start_plans", "created_at"),
        ("sync_cold_start_plans", "updated_at"),
        ("pipeline_command_receipts", "created_at"),
    }
)

_PHASE2_NULLABLE_COLUMNS_BY_REVISION: Final = {
    "20260710_0002": frozenset(),
    "20260710_0003": _PHASE2_NULLABLE_COLUMNS,
    "20260713_0004": _PHASE2_NULLABLE_COLUMNS,
    SYNC_RECONCILIATION_DATABASE_REVISION: (
        _PHASE2_NULLABLE_COLUMNS | _SYNC_RECONCILIATION_NULLABLE_COLUMNS
    ),
}

_PHASE2_DEFAULTED_COLUMNS_BY_REVISION: Final = {
    "20260710_0002": frozenset(),
    "20260710_0003": _PHASE2_DEFAULTED_COLUMNS,
    "20260713_0004": _PHASE2_DEFAULTED_COLUMNS,
    SYNC_RECONCILIATION_DATABASE_REVISION: (
        _PHASE2_DEFAULTED_COLUMNS | _SYNC_RECONCILIATION_DEFAULTED_COLUMNS
    ),
}

_GREENFIELD_RELATIONS: Final = frozenset(
    PHASE2_RELATIONS_BY_REVISION[GREENFIELD_DATABASE_REVISION]
)
_GREENFIELD_NULLABLE_COLUMNS: Final = frozenset(
    {
        column
        for column in _PHASE2_NULLABLE_COLUMNS_BY_REVISION[
            SYNC_RECONCILIATION_DATABASE_REVISION
        ]
        if column[0] in _GREENFIELD_RELATIONS
    }
    | {
        ("emails", "processing_execution_epoch"),
        ("event_inbox", "lease_session_id"),
        ("pipeline_initializations", "receipt_command_name"),
        ("pipeline_runtime_capabilities", "predecessor_stage_ordinal"),
        ("pipeline_runtime_capabilities", "stage_ordinal"),
    }
)
_GREENFIELD_DEFAULT_EXPRESSIONS: Final = {
    **{
        column: expression
        for column, expression in PHASE2_DEFAULT_EXPRESSIONS_BY_REVISION[
            SYNC_RECONCILIATION_DATABASE_REVISION
        ].items()
        if column[0] in _GREENFIELD_RELATIONS
    },
    ("event_inbox", "execution_epoch"): "0",
    ("pipeline_folder_scopes", "created_at"): "CURRENT_TIMESTAMP",
    ("pipeline_initializations", "capability_stage_ordinal"): "1",
    ("pipeline_initializations", "created_at"): "CURRENT_TIMESTAMP",
    ("pipeline_runtime_authority", "capability_stage_ordinal"): "1",
    ("pipeline_runtime_authority", "created_at"): "CURRENT_TIMESTAMP",
    ("pipeline_runtime_authority", "updated_at"): "CURRENT_TIMESTAMP",
    ("pipeline_runtime_capabilities", "created_at"): "CURRENT_TIMESTAMP",
    ("pipeline_runtime_instances", "accepted_count"): "0",
    ("pipeline_runtime_instances", "capability_stage_ordinal"): "1",
    ("pipeline_runtime_instances", "registered_at"): "CURRENT_TIMESTAMP",
    ("pipeline_runtime_instances", "rejected_count"): "0",
    ("pipeline_runtime_instances", "updated_at"): "CURRENT_TIMESTAMP",
}
_GREENFIELD_GENERATED_EXPRESSION_SHA256: Final = {
    **PHASE2_GENERATED_EXPRESSION_SHA256_BY_REVISION[
        SYNC_RECONCILIATION_DATABASE_REVISION
    ],
    ("pipeline_initializations", "receipt_command_name"): (
        "0440b0ae6fb921bb9d055d8f1afcfed9cb26ae4f128adec283ecf17420ad5df5"
    ),
    ("pipeline_runtime_capabilities", "predecessor_stage_ordinal"): (
        "ba2ac13d05d8de3a592d785d7154d56a8f1972fb0296e015c7f1884a705c3443"
    ),
    ("pipeline_runtime_capabilities", "stage_ordinal"): (
        "3eddd7c1d28b0ed2305ac6657ecae9f665138fcabb088fa39444d977f37910c6"
    ),
}
_PHASE2_NULLABLE_COLUMNS_BY_REVISION[GREENFIELD_DATABASE_REVISION] = (
    _GREENFIELD_NULLABLE_COLUMNS
)
_PHASE2_DEFAULTED_COLUMNS_BY_REVISION[GREENFIELD_DATABASE_REVISION] = frozenset(
    _GREENFIELD_DEFAULT_EXPRESSIONS
)
_DEFAULT_EXPRESSIONS_BY_REVISION: Final = {
    **PHASE2_DEFAULT_EXPRESSIONS_BY_REVISION,
    GREENFIELD_DATABASE_REVISION: _GREENFIELD_DEFAULT_EXPRESSIONS,
}
_GENERATED_EXPRESSION_SHA256_BY_REVISION: Final = {
    **PHASE2_GENERATED_EXPRESSION_SHA256_BY_REVISION,
    GREENFIELD_DATABASE_REVISION: _GREENFIELD_GENERATED_EXPRESSION_SHA256,
}
_GREENFIELD_CHECK_CONSTRAINT_SHA256: Final = {
    **{
        key: digest
        for key, digest in PHASE2_CHECK_CONSTRAINT_SHA256_BY_REVISION[
            SYNC_RECONCILIATION_DATABASE_REVISION
        ].items()
        if key[0] in _GREENFIELD_RELATIONS
    },
    ("emails", "ck_emails_processing_runtime_identity"): (
        "22130b66db8ee7ddf735a8b679e2c147ef245af3ba5d790432274566a5c63e68"
    ),
    ("emails", "ck_emails_runtime_ownership"): (
        "0947764d2e4907631a94e909953343fa7f4a8a77a52d744058d5fea9e455b952"
    ),
    ("event_inbox", "ck_event_inbox_execution_epoch"): (
        "1b98f5c2ba457ebdb6061bf85a9ceba221de16d553e2451fec9d89c09a43806c"
    ),
    ("event_inbox", "ck_event_inbox_lease"): (
        "c67dac3fdc72ad1bb915cd8a8c56c04c052fc3fa1b9f969cd32ab89b386e6feb"
    ),
    ("event_inbox", "ck_event_inbox_runtime_authority"): (
        "46e6eec3ff6ac4ecc8fae7a5985a217f4b892e2c2440e10413df749b079a4f49"
    ),
    ("pipeline_command_receipts", "ck_pipeline_command_receipts_authority_epoch"): (
        "604d688589abfbf7834635e731050dfd2151fb9232e229852d653e12d10bd7f3"
    ),
    ("pipeline_command_receipts", "ck_pipeline_command_receipts_command_name"): (
        "1d9b1d8c031b575a7fc259023c39982d43e940a91841f6457255e371551bb3b4"
    ),
    ("pipeline_command_receipts", "ck_pipeline_command_receipts_result"): (
        "e78f474b27ee0b8d3291c2befcf6add63bcdf96bbf5f95e3dcca00d30124cf18"
    ),
    ("pipeline_folder_scopes", "ck_pipeline_folder_scopes_event_policy_matrix"): (
        "a2f254f5416da36e8e47b5e94a9616d17e7979417a679440da1f134470a6ed67"
    ),
    ("pipeline_folder_scopes", "ck_pipeline_folder_scopes_hashes"): (
        "adfd35a18eafb43be02058474d6c99f2a2fd66eaa21a3b2ceb4505e2d45bd9fb"
    ),
    ("pipeline_folder_scopes", "ck_pipeline_folder_scopes_identity"): (
        "95e6091b2100f469c110c5be760797fa27c34c0bc7b57c187360437dcbd722ba"
    ),
    ("pipeline_folder_scopes", "ck_pipeline_folder_scopes_sync_folder"): (
        "8e4f94fbe173c21d40e2f8d612bb9f7451763fdee8be5f4d1b543dffcb847331"
    ),
    ("pipeline_folder_scopes", "ck_pipeline_folder_scopes_webhook_ids"): (
        "83c3507faa090ac4dcd305bb3ac3999fd589713ae4f19503a2e25fe09e6dcaaa"
    ),
    (
        "pipeline_initializations",
        "ck_pipeline_initializations_greenfield_identity",
    ): "851626454562ece09589c3bc5ef41bb6f6345002017e93609dbdc43fb82a99cd",
    ("pipeline_initializations", "ck_pipeline_initializations_hashes"): (
        "fb6c6339e1efe6ec4c11b035568a9adc069626c0666ed4614ac7096e6f37d104"
    ),
    ("pipeline_initializations", "ck_pipeline_initializations_operator"): (
        "b5b8600fa9d4622182db78be598537d41662ea6682bb0e29aa24aff92c8b0c4e"
    ),
    ("pipeline_initializations", "ck_pipeline_initializations_transaction"): (
        "b450a7bdfc435c095f0effdd04fdc27e3f0d9314283ee9724b33c9ba7511c1de"
    ),
    ("pipeline_runtime_authority", "ck_pipeline_runtime_authority_contract"): (
        "5f76229d41fc74952e7da5791444dfe79c558552fb93bbad2c4166cf4eb1239e"
    ),
    ("pipeline_runtime_authority", "ck_pipeline_runtime_authority_identity"): (
        "d05c0413c3ee7f2e8c3d61a4b276510b48e7ca6b2e8b42b4ef03247bb4d4eab0"
    ),
    ("pipeline_runtime_authority", "ck_pipeline_runtime_authority_state"): (
        "397cb29f454f9d64836c10173ad1408039d29d26fc666d3bbcec61a21cffdf04"
    ),
    ("pipeline_runtime_authority", "ck_pipeline_runtime_authority_versions"): (
        "8ef2ac3be3560ceb0f676ee0fe9a7ceeffd978c574aa4a5007b06ffc97ebbfe4"
    ),
    ("pipeline_runtime_capabilities", "ck_pipeline_runtime_capabilities_build"): (
        "2f2bbe2f4fcc7f42fd3c8352eb6d0c5cd236ff013906f36d83172be1155c39e6"
    ),
    (
        "pipeline_runtime_capabilities",
        "ck_pipeline_runtime_capabilities_hashes",
    ): "d299724157bd9d3a7e94a11ae2cef34018eeb397b93d9e711d4ff1afdbb720b4",
    (
        "pipeline_runtime_capabilities",
        "ck_pipeline_runtime_capabilities_predecessor",
    ): "9720f3889507f175780770a4f35b8a78b404933e1a0610c114b1181b68201dd1",
    (
        "pipeline_runtime_capabilities",
        "ck_pipeline_runtime_capabilities_protocol",
    ): "05d52c0da5c02e8f46ae247a0917262cbbdebccaeaa1e5fed9a6cecd3f4eb08e",
    (
        "pipeline_runtime_capabilities",
        "ck_pipeline_runtime_capabilities_schema",
    ): "2c4abec881ce32c590a17ec4b7356c61dabf5830de9f1ffac8ebc1c24e2392e4",
    (
        "pipeline_runtime_capabilities",
        "ck_pipeline_runtime_capabilities_stage",
    ): "c681e4ce3ff4a5041711352cf34d50c3d8be5684659cda5c490d70267ce16cf6",
    ("pipeline_runtime_instances", "ck_pipeline_runtime_instances_contract"): (
        "f4f7b3107b9590c065798f04900584cbc50cedf3ffdfe51563ce68277a65265b"
    ),
    ("pipeline_runtime_instances", "ck_pipeline_runtime_instances_counters"): (
        "a4f01e8aaf10faba38a88e00756f5ba683600a5e64abadc178f0ceeb20295e71"
    ),
    ("pipeline_runtime_instances", "ck_pipeline_runtime_instances_identity"): (
        "ccbdeb12dbc4b0af86483e52d6bd870f48571d49d76dab2fb5d8eabf4dc2fcb8"
    ),
    ("pipeline_runtime_instances", "ck_pipeline_runtime_instances_lease"): (
        "995bf2b27b997c2c884d8efa4619b51e36aeb8dddea979077100be020ec9e2a9"
    ),
    ("pipeline_runtime_instances", "ck_pipeline_runtime_instances_lifecycle"): (
        "0595344a75ad3137cbefd0072a67c61059282a525fddbb082b1f258d48942634"
    ),
    ("pipeline_runtime_instances", "ck_pipeline_runtime_instances_workload"): (
        "9b8f1b8f6a2c94ff52e5f06c50ce618cdcece9b9af535829c094da6d7b886116"
    ),
}
_CHECK_CONSTRAINT_SHA256_BY_REVISION: Final = {
    **PHASE2_CHECK_CONSTRAINT_SHA256_BY_REVISION,
    GREENFIELD_DATABASE_REVISION: _GREENFIELD_CHECK_CONSTRAINT_SHA256,
}


def _plain_unique_contract(
    relation: str,
    name: str,
    columns: tuple[str, ...],
    *,
    constraint_type: str = "u",
) -> tuple[object, ...]:
    return (
        relation,
        name,
        name,
        constraint_type,
        columns,
        (0,) * len(columns),
        None,
        False,
        False,
        False,
        True,
        True,
        True,
        "btree",
        True,
        True,
        True,
        True,
    )


_GREENFIELD_UNIQUE_CONSTRAINTS: Final = frozenset(
    {
        (
            spec.relation,
            spec.name,
            spec.name,
            spec.constraint_type,
            spec.columns,
            spec.index_options,
            None,
            spec.nulls_not_distinct,
            spec.deferrable,
            spec.initially_deferred,
            spec.validated,
            spec.index_valid,
            spec.index_ready,
            spec.access_method,
            spec.has_no_included_columns,
            spec.has_only_plain_columns,
            spec.uses_default_operator_classes,
            spec.uses_default_collations,
        )
        for spec in PHASE2_UNIQUE_CONSTRAINTS_BY_REVISION[
            SYNC_RECONCILIATION_DATABASE_REVISION
        ]
        if spec.relation in _GREENFIELD_RELATIONS
        and (spec.relation, spec.name)
        not in {
            ("emails", "uq_emails_outbox_identity"),
            ("event_inbox", "uq_event_inbox_processing_identity"),
        }
    }
    | {
        _plain_unique_contract(
            "emails",
            "uq_emails_outbox_identity",
            (
                "id",
                "account_id",
                "owner_generation",
                "owner_fencing_token",
                "owner_authority_epoch",
                "owner_capability_hash",
            ),
        ),
        _plain_unique_contract(
            "event_inbox",
            "uq_event_inbox_processing_identity",
            (
                "id",
                "account_id",
                "external_email_id",
                "generation",
                "fencing_token",
                "execution_epoch",
                "authority_epoch",
                "capability_hash",
            ),
        ),
        _plain_unique_contract(
            "pipeline_command_receipts",
            "uq_pipeline_command_receipts_runtime_binding",
            ("id", "account_id", "command_name", "authority_epoch"),
        ),
        _plain_unique_contract(
            "pipeline_folder_scopes",
            "pk_pipeline_folder_scopes",
            ("initialization_id", "canonical_key"),
            constraint_type="p",
        ),
        _plain_unique_contract(
            "pipeline_folder_scopes",
            "uq_pipeline_folder_scopes_hash",
            ("initialization_id", "scope_hash"),
        ),
        _plain_unique_contract(
            "pipeline_folder_scopes",
            "uq_pipeline_folder_scopes_sync_folder",
            ("initialization_id", "sync_folder"),
        ),
        _plain_unique_contract(
            "pipeline_initializations",
            "pk_pipeline_initializations",
            ("initialization_id",),
            constraint_type="p",
        ),
        _plain_unique_contract(
            "pipeline_initializations",
            "uq_pipeline_initializations_account",
            ("account_id",),
        ),
        _plain_unique_contract(
            "pipeline_initializations",
            "uq_pipeline_initializations_authority_binding",
            (
                "initialization_id",
                "account_id",
                "generation",
                "fencing_token",
                "pipeline_name",
                "policy_manifest_hash",
            ),
        ),
        _plain_unique_contract(
            "pipeline_initializations",
            "uq_pipeline_initializations_policy_binding",
            ("initialization_id", "account_id", "policy_manifest_hash"),
        ),
        _plain_unique_contract(
            "pipeline_initializations",
            "uq_pipeline_initializations_receipt",
            ("command_receipt_id",),
        ),
        _plain_unique_contract(
            "pipeline_runtime_authority",
            "pk_pipeline_runtime_authority",
            ("account_id",),
            constraint_type="p",
        ),
        _plain_unique_contract(
            "pipeline_runtime_authority",
            "uq_pipeline_runtime_authority_stamp",
            (
                "account_id",
                "generation",
                "fencing_token",
                "pipeline_name",
                "authority_epoch",
                "capability_hash",
            ),
        ),
        _plain_unique_contract(
            "pipeline_runtime_capabilities",
            "pk_pipeline_runtime_capabilities",
            ("capability_hash",),
            constraint_type="p",
        ),
        _plain_unique_contract(
            "pipeline_runtime_capabilities",
            "uq_pipeline_runtime_capabilities_instance_contract",
            (
                "capability_hash",
                "stage_ordinal",
                "schema_revision",
                "protocol_version",
                "minimum_build_id",
                "config_hash",
            ),
        ),
        _plain_unique_contract(
            "pipeline_runtime_capabilities",
            "uq_pipeline_runtime_capabilities_policy_identity",
            ("capability_hash", "stage_ordinal", "policy_manifest_hash"),
        ),
        _plain_unique_contract(
            "pipeline_runtime_capabilities",
            "uq_pipeline_runtime_capabilities_runtime_contract",
            (
                "capability_hash",
                "stage_ordinal",
                "schema_revision",
                "protocol_version",
                "minimum_build_id",
                "config_hash",
                "policy_manifest_hash",
            ),
        ),
        _plain_unique_contract(
            "pipeline_runtime_capabilities",
            "uq_pipeline_runtime_capabilities_stage_identity",
            ("capability_hash", "stage_ordinal"),
        ),
        _plain_unique_contract(
            "pipeline_runtime_instances",
            "pk_pipeline_runtime_instances",
            ("session_id",),
            constraint_type="p",
        ),
        _plain_unique_contract(
            "pipeline_runtime_instances",
            "uq_pipeline_runtime_instances_lease_identity",
            (
                "session_id",
                "account_id",
                "generation",
                "fencing_token",
                "authority_epoch",
                "capability_hash",
            ),
        ),
        _plain_unique_contract(
            "pipeline_runtime_instances",
            "uq_pipeline_runtime_instances_session",
            ("account_id", "session_id"),
        ),
    }
)


def _plain_index_contract(
    relation: str,
    name: str,
    columns: tuple[str, ...],
    *,
    unique: bool = False,
    predicate_sha256: str | None = None,
) -> tuple[object, ...]:
    return (
        relation,
        name,
        unique,
        True,
        True,
        True,
        True,
        True,
        "btree",
        columns,
        (0,) * len(columns),
        predicate_sha256,
    )


_GREENFIELD_INDEXES: Final = frozenset(
    {
        (
            spec.relation,
            spec.name,
            spec.unique,
            True,
            True,
            True,
            True,
            True,
            "btree",
            spec.columns,
            spec.options,
            spec.predicate_sha256,
        )
        for spec in PHASE2_INDEX_SPECS_BY_REVISION[
            SYNC_RECONCILIATION_DATABASE_REVISION
        ]
        if spec.relation in _GREENFIELD_RELATIONS
        and (spec.relation, spec.name)
        not in {
            ("emails", "ix_emails_owner_status"),
            ("event_inbox", "ix_event_inbox_expired_lease"),
        }
    }
    | {
        _plain_index_contract(
            "emails",
            "ix_emails_owner_status",
            (
                "account_id",
                "owner_generation",
                "owner_fencing_token",
                "owner_authority_epoch",
                "owner_capability_hash",
                "status",
            ),
        ),
        _plain_index_contract(
            "event_inbox",
            "ix_event_inbox_expired_lease",
            (
                "lease_until",
                "execution_epoch",
                "authority_epoch",
                "capability_hash",
                "lease_session_id",
                "id",
            ),
            predicate_sha256=(
                "0d8854756b3b05c4ae1bb96a0493add03c0099df50cfb267871e9b5368f7c54c"
            ),
        ),
        _plain_index_contract(
            "pipeline_folder_scopes",
            "ix_pipeline_folder_scopes_account",
            ("account_id", "canonical_key"),
        ),
        _plain_index_contract(
            "pipeline_runtime_authority",
            "ix_pipeline_runtime_authority_state",
            ("state", "account_id"),
        ),
        _plain_index_contract(
            "pipeline_runtime_capabilities",
            "ix_pipeline_runtime_capabilities_stage",
            ("stage_ordinal", "created_at", "capability_hash"),
        ),
        _plain_index_contract(
            "pipeline_runtime_instances",
            "ix_pipeline_runtime_instances_authority",
            (
                "account_id",
                "generation",
                "fencing_token",
                "authority_epoch",
                "capability_hash",
                "lifecycle",
            ),
        ),
        _plain_index_contract(
            "pipeline_runtime_instances",
            "ix_pipeline_runtime_instances_lease",
            ("lease_until", "session_id"),
            predicate_sha256=(
                "2f952cb9388375627f98891cc0eca31021b80e760cab62e84730a898221049f2"
            ),
        ),
        _plain_index_contract(
            "pipeline_runtime_instances",
            "uq_pipeline_runtime_instances_live_identity",
            ("account_id", "workload", "instance_id"),
            unique=True,
            predicate_sha256=(
                "2f952cb9388375627f98891cc0eca31021b80e760cab62e84730a898221049f2"
            ),
        ),
    }
)


def _foreign_key_contract(
    name: str,
    child_relation: str,
    child_columns: tuple[str, ...],
    parent_relation: str,
    parent_columns: tuple[str, ...],
    match_type: str,
    *,
    update_action: str = "r",
    deferrable: bool = False,
    initially_deferred: bool = False,
) -> tuple[object, ...]:
    return (
        name,
        child_relation,
        child_columns,
        parent_relation,
        parent_columns,
        match_type,
        update_action,
        "r",
        deferrable,
        initially_deferred,
        True,
    )


_GREENFIELD_FOREIGN_KEYS: Final = frozenset(
    {
        _foreign_key_contract(
            "fk_audit_events_email",
            "audit_events",
            ("account_id", "email_id"),
            "emails",
            ("account_id", "id"),
            "s",
        ),
        _foreign_key_contract(
            "fk_emails_pipeline_ownership",
            "emails",
            ("account_id", "owner_generation", "owner_fencing_token"),
            "pipeline_ownership",
            ("account_id", "generation", "fencing_token"),
            "f",
        ),
        _foreign_key_contract(
            "fk_emails_processing_inbox",
            "emails",
            (
                "processing_inbox_id",
                "account_id",
                "external_email_id",
                "owner_generation",
                "owner_fencing_token",
                "processing_execution_epoch",
                "owner_authority_epoch",
                "owner_capability_hash",
            ),
            "event_inbox",
            (
                "id",
                "account_id",
                "external_email_id",
                "generation",
                "fencing_token",
                "execution_epoch",
                "authority_epoch",
                "capability_hash",
            ),
            "s",
            update_action="a",
            deferrable=True,
            initially_deferred=True,
        ),
        _foreign_key_contract(
            "fk_emails_runtime_capability",
            "emails",
            ("owner_capability_hash",),
            "pipeline_runtime_capabilities",
            ("capability_hash",),
            "f",
        ),
        _foreign_key_contract(
            "fk_event_inbox_lease_session",
            "event_inbox",
            (
                "lease_session_id",
                "account_id",
                "generation",
                "fencing_token",
                "authority_epoch",
                "capability_hash",
            ),
            "pipeline_runtime_instances",
            (
                "session_id",
                "account_id",
                "generation",
                "fencing_token",
                "authority_epoch",
                "capability_hash",
            ),
            "s",
        ),
        _foreign_key_contract(
            "fk_event_inbox_pipeline_ownership",
            "event_inbox",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "pipeline_ownership",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "f",
        ),
        _foreign_key_contract(
            "fk_event_inbox_runtime_capability",
            "event_inbox",
            ("capability_hash",),
            "pipeline_runtime_capabilities",
            ("capability_hash",),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_folder_scopes_initialization",
            "pipeline_folder_scopes",
            ("initialization_id", "account_id", "policy_manifest_hash"),
            "pipeline_initializations",
            ("initialization_id", "account_id", "policy_manifest_hash"),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_initializations_capability",
            "pipeline_initializations",
            (
                "capability_hash",
                "capability_stage_ordinal",
                "policy_manifest_hash",
            ),
            "pipeline_runtime_capabilities",
            ("capability_hash", "stage_ordinal", "policy_manifest_hash"),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_initializations_ownership",
            "pipeline_initializations",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "pipeline_ownership",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_initializations_receipt",
            "pipeline_initializations",
            (
                "command_receipt_id",
                "account_id",
                "receipt_command_name",
                "authority_epoch",
            ),
            "pipeline_command_receipts",
            ("id", "account_id", "command_name", "authority_epoch"),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_runtime_authority_capability",
            "pipeline_runtime_authority",
            (
                "capability_hash",
                "capability_stage_ordinal",
                "schema_revision",
                "protocol_version",
                "build_id",
                "config_hash",
                "policy_manifest_hash",
            ),
            "pipeline_runtime_capabilities",
            (
                "capability_hash",
                "stage_ordinal",
                "schema_revision",
                "protocol_version",
                "minimum_build_id",
                "config_hash",
                "policy_manifest_hash",
            ),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_runtime_authority_initialization",
            "pipeline_runtime_authority",
            (
                "initialization_id",
                "account_id",
                "generation",
                "fencing_token",
                "pipeline_name",
                "policy_manifest_hash",
            ),
            "pipeline_initializations",
            (
                "initialization_id",
                "account_id",
                "generation",
                "fencing_token",
                "pipeline_name",
                "policy_manifest_hash",
            ),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_runtime_authority_ownership",
            "pipeline_runtime_authority",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "pipeline_ownership",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "f",
        ),
        _foreign_key_contract(
            "fk_pipeline_runtime_capabilities_predecessor",
            "pipeline_runtime_capabilities",
            ("predecessor_hash", "predecessor_stage_ordinal"),
            "pipeline_runtime_capabilities",
            ("capability_hash", "stage_ordinal"),
            "s",
        ),
        _foreign_key_contract(
            "fk_pipeline_runtime_instances_capability",
            "pipeline_runtime_instances",
            (
                "capability_hash",
                "capability_stage_ordinal",
                "schema_revision",
                "protocol_version",
                "build_id",
                "config_hash",
            ),
            "pipeline_runtime_capabilities",
            (
                "capability_hash",
                "stage_ordinal",
                "schema_revision",
                "protocol_version",
                "minimum_build_id",
                "config_hash",
            ),
            "f",
        ),
        _foreign_key_contract(
            "fk_sync_cold_start_plan_active_cursor",
            "sync_cold_start_plans",
            (
                "cursor_binding_plan_id",
                "account_id",
                "folder_key",
                "apply_cursor",
                "apply_cursor_version",
                "state",
            ),
            "sync_cursors",
            (
                "cold_start_plan_id",
                "account_id",
                "folder_key",
                "cursor",
                "version",
                "cold_start_plan_state",
            ),
            "s",
            update_action="a",
            deferrable=True,
            initially_deferred=True,
        ),
        _foreign_key_contract(
            "fk_sync_cold_start_plan_ownership",
            "sync_cold_start_plans",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "pipeline_ownership",
            ("account_id", "generation", "fencing_token", "pipeline_name"),
            "f",
        ),
        _foreign_key_contract(
            "fk_sync_cursors_cold_start_plan",
            "sync_cursors",
            (
                "cold_start_plan_id",
                "account_id",
                "folder_key",
                "cursor",
                "version",
                "cold_start_plan_state",
            ),
            "sync_cold_start_plans",
            (
                "plan_id",
                "account_id",
                "folder_key",
                "apply_cursor",
                "apply_cursor_version",
                "state",
            ),
            "s",
            update_action="a",
            deferrable=True,
            initially_deferred=True,
        ),
    }
)


def _trigger_contract(
    name: str,
    relation: str,
    function_name: str,
    trigger_type: int,
    *,
    constraint: bool = False,
    deferrable: bool = False,
    initially_deferred: bool = False,
) -> tuple[object, ...]:
    return (
        name,
        relation,
        function_name,
        trigger_type,
        constraint,
        "O",
        True,
        True,
        True,
        True,
        True,
        deferrable,
        initially_deferred,
        0,
        "",
        "",
        None,
        None,
        None,
        True,
    )


_GREENFIELD_TRIGGERS: Final = frozenset(
    {
        _trigger_contract(
            "trg_audit_events_guard_row",
            "audit_events",
            "reject_audit_events_mutation",
            27,
        ),
        _trigger_contract(
            "trg_audit_events_guard_truncate",
            "audit_events",
            "reject_audit_events_mutation",
            34,
        ),
        _trigger_contract(
            "trg_emails_runtime_identity",
            "emails",
            "guard_emails_runtime_identity",
            21,
            constraint=True,
            deferrable=True,
            initially_deferred=True,
        ),
        _trigger_contract(
            "trg_event_inbox_runtime_identity",
            "event_inbox",
            "guard_event_inbox_runtime_identity",
            23,
        ),
        _trigger_contract(
            "trg_pipeline_command_receipts_guard_row",
            "pipeline_command_receipts",
            "reject_pipeline_command_receipts_mutation",
            27,
        ),
        _trigger_contract(
            "trg_pipeline_command_receipts_guard_truncate",
            "pipeline_command_receipts",
            "reject_pipeline_command_receipts_mutation",
            34,
        ),
        _trigger_contract(
            "trg_pipeline_folder_scopes_guard_row",
            "pipeline_folder_scopes",
            "reject_pipeline_folder_scopes_mutation",
            31,
        ),
        _trigger_contract(
            "trg_pipeline_folder_scopes_guard_truncate",
            "pipeline_folder_scopes",
            "reject_pipeline_folder_scopes_mutation",
            34,
        ),
        _trigger_contract(
            "trg_pipeline_initializations_guard_row",
            "pipeline_initializations",
            "reject_pipeline_initializations_mutation",
            31,
        ),
        _trigger_contract(
            "trg_pipeline_initializations_guard_truncate",
            "pipeline_initializations",
            "reject_pipeline_initializations_mutation",
            34,
        ),
        _trigger_contract(
            "trg_pipeline_ownership_guard_row",
            "pipeline_ownership",
            "guard_pipeline_ownership",
            31,
        ),
        _trigger_contract(
            "trg_pipeline_ownership_guard_truncate",
            "pipeline_ownership",
            "guard_pipeline_ownership",
            34,
        ),
        _trigger_contract(
            "trg_pipeline_runtime_authority_guard_row",
            "pipeline_runtime_authority",
            "guard_pipeline_runtime_authority",
            31,
        ),
        _trigger_contract(
            "trg_pipeline_runtime_authority_guard_truncate",
            "pipeline_runtime_authority",
            "guard_pipeline_runtime_authority",
            34,
        ),
        _trigger_contract(
            "trg_pipeline_runtime_capabilities_guard_row",
            "pipeline_runtime_capabilities",
            "reject_pipeline_runtime_capabilities_mutation",
            27,
        ),
        _trigger_contract(
            "trg_pipeline_runtime_capabilities_guard_truncate",
            "pipeline_runtime_capabilities",
            "reject_pipeline_runtime_capabilities_mutation",
            34,
        ),
        _trigger_contract(
            "trg_pipeline_runtime_instances_guard_row",
            "pipeline_runtime_instances",
            "guard_pipeline_runtime_instances",
            31,
        ),
        _trigger_contract(
            "trg_pipeline_runtime_instances_guard_truncate",
            "pipeline_runtime_instances",
            "guard_pipeline_runtime_instances",
            34,
        ),
    }
)

_GREENFIELD_INSTANCE_RESULT: Final = (
    "TABLE(account_id bigint, workload text, instance_id text, session_id uuid, "
    "generation bigint, fencing_token bigint, authority_epoch bigint, "
    "capability_hash text, schema_revision text, protocol_version bigint, "
    "build_id text, config_hash text, lifecycle text, lease_version bigint, "
    "accepted_count bigint, rejected_count bigint, heartbeat_at timestamp with "
    "time zone, lease_until timestamp with time zone)"
)
_GREENFIELD_TRANSITION_RESULT: Final = (
    "TABLE(command_receipt_id uuid, command_name text, previous_state text, "
    "previous_authority_epoch bigint, previous_version bigint, transaction_id "
    "text, replayed boolean, receipt_created_at timestamp with time zone, "
    "account_id bigint, state text, generation bigint, fencing_token bigint, "
    "pipeline_name text, authority_epoch bigint, version bigint, "
    "schema_revision text, protocol_version bigint, build_id text, config_hash "
    "text, capability_hash text, policy_manifest_hash text, initialization_id "
    "uuid, updated_at timestamp with time zone)"
)


def _routine_contract(
    name: str,
    identity_arguments: str,
    result: str,
    *,
    language: str = "plpgsql",
    security_definer: bool = True,
    volatility: str = "v",
    parallel: str = "u",
    returns_set: bool | None = None,
    search_path: str = "pg_catalog",
) -> tuple[object, ...]:
    return (
        name,
        identity_arguments,
        result,
        language,
        "f",
        security_definer,
        volatility,
        False,
        False,
        parallel,
        result.startswith("TABLE(") if returns_set is None else returns_set,
        0,
        (f"search_path={search_path}",),
        True,
        True,
    )


_GREENFIELD_ROUTINES: Final = frozenset(
    {
        _routine_contract(
            "greenfield_apply_email_event",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_expected_email_version bigint",
            "jsonb",
        ),
        _routine_contract(
            "greenfield_begin_inbox_effect",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_attempts bigint",
            "jsonb",
        ),
        _routine_contract(
            "greenfield_claim_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_lease_owner text, "
            "p_limit bigint, p_lease_seconds bigint",
            "jsonb",
        ),
        _routine_contract(
            "greenfield_drain_web_instance",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_expected_authority_epoch "
            "bigint, p_expected_capability_hash text",
            _GREENFIELD_INSTANCE_RESULT,
        ),
        _routine_contract(
            "greenfield_fail_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_attempts bigint, "
            "p_safe_error_code text, p_safe_error_summary text",
            "jsonb",
        ),
        _routine_contract(
            "greenfield_finish_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_attempts bigint, p_completion jsonb",
            "jsonb",
        ),
        _routine_contract(
            "greenfield_get_runtime_authority",
            "p_account_id bigint",
            "TABLE(account_id bigint, state text, generation bigint, "
            "fencing_token bigint, pipeline_name text, authority_epoch bigint, "
            "version bigint, schema_revision text, protocol_version bigint, "
            "build_id text, config_hash text, capability_hash text, "
            "policy_manifest_hash text, initialization_id uuid, updated_at "
            "timestamp with time zone)",
            language="sql",
            volatility="s",
        ),
        _routine_contract(
            "greenfield_heartbeat_web_instance",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_expected_authority_epoch "
            "bigint, p_expected_capability_hash text, p_accepted_count bigint, "
            "p_rejected_count bigint, p_lease_seconds bigint",
            _GREENFIELD_INSTANCE_RESULT,
        ),
        _routine_contract(
            "greenfield_initialize_runtime",
            "p_account_id bigint, p_capability_hash text, p_predecessor_hash "
            "text, p_capability_stage text, p_schema_revision text, "
            "p_schema_digest text, p_protocol_version bigint, "
            "p_minimum_build_id text, p_config_hash text, p_adapter_hash text, "
            "p_policy_manifest_hash text, p_evidence_manifest_hash text, "
            "p_policy_manifest_json text, p_policy_scope_count bigint, "
            "p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
            "TABLE(initialization_id uuid, command_receipt_id uuid, account_id "
            "bigint, generation bigint, fencing_token bigint, pipeline_name "
            "text, authority_epoch bigint, authority_version bigint, "
            "capability_hash text, policy_manifest_hash text, transaction_id "
            "text, replayed boolean, created_at timestamp with time zone)",
        ),
        _routine_contract(
            "greenfield_insert_webhook_event",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_external_email_id text, "
            "p_folder_key text, p_raw_event_type text, p_change_kind text, "
            "p_dedupe_key text, p_source_version text, p_source_event_at "
            "timestamp with time zone, p_payload jsonb, p_processing_policy text",
            "TABLE(inbox_id uuid, duplicate boolean)",
        ),
        _routine_contract(
            "greenfield_pause_runtime",
            "p_account_id bigint, p_expected_authority_epoch bigint, "
            "p_expected_version bigint, p_expected_capability_hash text, "
            "p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
            _GREENFIELD_TRANSITION_RESULT,
        ),
        _routine_contract(
            "greenfield_reap_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_limit bigint",
            "jsonb",
        ),
        _routine_contract(
            "greenfield_register_web_instance",
            "p_account_id bigint, p_instance_id text, p_session_id uuid, "
            "p_expected_authority_epoch bigint, p_expected_authority_version "
            "bigint, p_schema_revision text, p_protocol_version bigint, "
            "p_build_id text, p_config_hash text, p_capability_hash text, "
            "p_lease_seconds bigint",
            _GREENFIELD_INSTANCE_RESULT,
        ),
        _routine_contract(
            "greenfield_renew_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_lease_owner text, p_attempts bigint, "
            "p_lease_seconds bigint",
            "jsonb",
        ),
        _routine_contract(
            "greenfield_requeue_inbox",
            "p_account_id bigint, p_inbox_id uuid, "
            "p_expected_execution_epoch bigint, p_expected_email_version "
            "bigint, p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
            "TABLE(command_receipt_id uuid, inbox_id uuid, email_id uuid, "
            "previous_execution_epoch bigint, execution_epoch bigint, "
            "email_version bigint, status text, transaction_id text, replayed "
            "boolean, created_at timestamp with time zone)",
        ),
        _routine_contract(
            "greenfield_resume_ingress",
            "p_account_id bigint, p_expected_authority_epoch bigint, "
            "p_expected_version bigint, p_expected_capability_hash text, "
            "p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
            _GREENFIELD_TRANSITION_RESULT,
        ),
        _routine_contract(
            "guard_emails_runtime_identity",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
        ),
        _routine_contract(
            "guard_event_inbox_runtime_identity",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
        ),
        _routine_contract(
            "guard_pipeline_ownership",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
            search_path="public",
        ),
        _routine_contract(
            "guard_pipeline_runtime_authority",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
        ),
        _routine_contract(
            "guard_pipeline_runtime_instances",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
        ),
        _routine_contract(
            "reject_audit_events_mutation",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
            search_path="public",
        ),
        _routine_contract(
            "reject_pipeline_command_receipts_mutation",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
            search_path="public",
        ),
        _routine_contract(
            "reject_pipeline_folder_scopes_mutation",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
        ),
        _routine_contract(
            "reject_pipeline_initializations_mutation",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
        ),
        _routine_contract(
            "reject_pipeline_runtime_capabilities_mutation",
            "",
            "trigger",
            security_definer=False,
            returns_set=False,
        ),
    }
)

_GREENFIELD_ROUTINE_SOURCE_SHA256: Final[dict[tuple[str, str], str]] = {
    (
        "greenfield_apply_email_event",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_expected_email_version bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_begin_inbox_effect",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_attempts bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_claim_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_lease_owner text, "
        "p_limit bigint, p_lease_seconds bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_drain_web_instance",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_expected_authority_epoch bigint, "
        "p_expected_capability_hash text",
    ): "f251ab3b82db21ff53e71b8520a444c54a4948e3139d2b497744f4993727c07a",
    (
        "greenfield_fail_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_attempts bigint, "
        "p_safe_error_code text, p_safe_error_summary text",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_finish_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_attempts bigint, p_completion jsonb",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_get_runtime_authority",
        "p_account_id bigint",
    ): "83c7710802ebe87c789541d335ae00b3b443099326721ae5c276b81289aa1e14",
    (
        "greenfield_heartbeat_web_instance",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_expected_authority_epoch bigint, "
        "p_expected_capability_hash text, p_accepted_count bigint, "
        "p_rejected_count bigint, p_lease_seconds bigint",
    ): "b8b514a4eefdaea8cc0b04d8e91f4abbd1c10a3e909fd22bf8b1006f598b6f6a",
    (
        "greenfield_initialize_runtime",
        "p_account_id bigint, p_capability_hash text, p_predecessor_hash text, "
        "p_capability_stage text, p_schema_revision text, "
        "p_schema_digest text, p_protocol_version bigint, "
        "p_minimum_build_id text, p_config_hash text, p_adapter_hash text, "
        "p_policy_manifest_hash text, p_evidence_manifest_hash text, "
        "p_policy_manifest_json text, p_policy_scope_count bigint, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "629db55e4a4a727c87f29ede0aa905e0852840ce330779080fa02e4316c12b7a",
    (
        "greenfield_insert_webhook_event",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_external_email_id text, "
        "p_folder_key text, p_raw_event_type text, p_change_kind text, "
        "p_dedupe_key text, p_source_version text, "
        "p_source_event_at timestamp with time zone, p_payload jsonb, "
        "p_processing_policy text",
    ): "90d1856349da36b686fbbbf9b9064b795dd819dcb2946a2731e7c7566f59f48b",
    (
        "greenfield_pause_runtime",
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "859dd32a2d4588383c0d6cb8e8be9448d9b11ced2cdf719bdc57a6bf8a6a1702",
    (
        "greenfield_reap_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_limit bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_register_web_instance",
        "p_account_id bigint, p_instance_id text, p_session_id uuid, "
        "p_expected_authority_epoch bigint, p_expected_authority_version "
        "bigint, p_schema_revision text, p_protocol_version bigint, "
        "p_build_id text, p_config_hash text, p_capability_hash text, "
        "p_lease_seconds bigint",
    ): "9ca45fe19a5fc3f071aa3ef9e4b015bfff6a5195b5c172ddb3b0a4434627a005",
    (
        "greenfield_renew_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_lease_owner text, p_attempts bigint, "
        "p_lease_seconds bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_requeue_inbox",
        "p_account_id bigint, p_inbox_id uuid, "
        "p_expected_execution_epoch bigint, p_expected_email_version bigint, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "c2212b40235a5ec862c4f775256b899b220c5ea60b1b7537cd2ad861e440fe8d",
    (
        "greenfield_resume_ingress",
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "9bc48d2dc92a03bd2919c7d68a134ef68c002a80a38a2d80212a1fde9af2511c",
    ("guard_emails_runtime_identity", ""): (
        "7bc574d299fa3bc6f2ad10d027776f53e24473d77a2edac35cc587d69d5452e1"
    ),
    ("guard_event_inbox_runtime_identity", ""): (
        "f314df8f1cdd5d1c67160c14243e7da906f84749f17a6a59eeba7ba76e42f576"
    ),
    ("guard_pipeline_ownership", ""): (
        "c898a988c2bfca60837cda5ce37ef8cdb00fd12312c312b3ceecd90b3356fc5c"
    ),
    ("guard_pipeline_runtime_authority", ""): (
        "9ce80deea362439e39fe9f02739145042b40d553f06ca689dd79e41ecb2fe059"
    ),
    ("guard_pipeline_runtime_instances", ""): (
        "20feb0f127036518702fd6d3ea4d13575a36fca70942109796643a1431b34f6d"
    ),
    ("reject_audit_events_mutation", ""): (
        "5ba2612faea4adf49b92395f87102f166df17b65aa64bf3f42ab5172bf375c5b"
    ),
    ("reject_pipeline_command_receipts_mutation", ""): (
        "2a5ebd74102b1adf35afc2bf49d0a2317b867c5f57c7d0080361826f28b97f16"
    ),
    ("reject_pipeline_folder_scopes_mutation", ""): (
        "4f0c3e20e0f837d713b3bc89b536b4bf4421b736ebe81961daa4245dd5e1e044"
    ),
    ("reject_pipeline_initializations_mutation", ""): (
        "d74a5146da3ed09d01bde3725343ee536f64d6ca151b79fa0be802fd30d5bbea"
    ),
    ("reject_pipeline_runtime_capabilities_mutation", ""): (
        "4f451f9f20e5538a7bd18117b7cd207474350055e2baa20f9595f38f11b20461"
    ),
}
_GREENFIELD_ROUTINE_DIGEST_IDENTITIES: Final = frozenset(
    (routine[0], routine[1]) for routine in _GREENFIELD_ROUTINES
)
_GREENFIELD_ROUTINE_PENDING_SOURCE_IDENTITIES: Final = (
    _GREENFIELD_ROUTINE_DIGEST_IDENTITIES - frozenset(_GREENFIELD_ROUTINE_SOURCE_SHA256)
)

_BASE_RELATION_KINDS: Final = {
    "alembic_version": "r",
    "emails_log": "r",
    "app_kv_store": "r",
    "processed_emails": "v",
    "checkpoint_migrations": "r",
    "checkpoints": "r",
    "checkpoint_blobs": "r",
    "checkpoint_writes": "r",
}
_PHASE2_RELATION_KINDS: Final = {
    relation_name: "r" for relation_name in PHASE2_RELATIONS
}
_PHASE2_RELATION_KINDS_BY_REVISION: Final = {
    revision: {
        **{relation_name: "r" for relation_name in relations},
        **{
            view.name: view.relation_kind
            for view in PHASE2_VIEW_SPECS_BY_REVISION[revision]
        },
    }
    for revision, relations in PHASE2_RELATIONS_BY_REVISION.items()
}

_RELATION_KIND_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    relation.relkind::pg_catalog.text,
    relation.relpersistence::pg_catalog.text,
    relation.relrowsecurity,
    relation.relforcerowsecurity,
    relation.relowner = relation_schema.nspowner,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_policy AS policy
        WHERE policy.polrelid = relation.oid
    ),
    access_method.amname::pg_catalog.text
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
LEFT JOIN pg_catalog.pg_am AS access_method
  ON access_method.oid = relation.relam
WHERE relation_schema.nspname = %s
  AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
ORDER BY relation.relname
"""

_VIEW_CONTRACT_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    relation.relkind::pg_catalog.text,
    pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(
                pg_catalog.pg_get_viewdef(relation.oid, false),
                'UTF8'
            )
        ),
        'hex'
    )::pg_catalog.text,
    COALESCE(relation.reloptions, ARRAY[]::pg_catalog.text[])
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
WHERE relation_schema.nspname = %s
  AND relation.relname = ANY(%s::pg_catalog.text[])
ORDER BY relation.relname
"""

_COLUMN_TYPE_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    attribute.attname::pg_catalog.text,
    type_schema.nspname::pg_catalog.text,
    column_type.typname::pg_catalog.text,
    NOT attribute.attnotnull,
    default_value.adbin IS NOT NULL,
    attribute.atttypmod,
    pg_catalog.pg_get_expr(
        default_value.adbin,
        default_value.adrelid,
        true
    )::pg_catalog.text,
    attribute.attidentity::pg_catalog.text,
    attribute.attgenerated::pg_catalog.text,
    attribute.attcollation = column_type.typcollation,
    attribute.attstorage = column_type.typstorage,
    attribute.attcompression::pg_catalog.text
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
JOIN pg_catalog.pg_attribute AS attribute
  ON attribute.attrelid = relation.oid
JOIN pg_catalog.pg_type AS column_type
  ON column_type.oid = attribute.atttypid
JOIN pg_catalog.pg_namespace AS type_schema
  ON type_schema.oid = column_type.typnamespace
LEFT JOIN pg_catalog.pg_attrdef AS default_value
  ON default_value.adrelid = relation.oid
 AND default_value.adnum = attribute.attnum
WHERE relation_schema.nspname = %s
  AND relation.relname = ANY(%s::pg_catalog.text[])
  AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND attribute.attnum > 0
  AND NOT attribute.attisdropped
"""

_CHECK_CONSTRAINT_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    constraint_record.conname::pg_catalog.text,
    pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(
                pg_catalog.pg_get_expr(
                    constraint_record.conbin,
                    constraint_record.conrelid,
                    true
                ),
                'UTF8'
            )
        ),
        'hex'
    )::pg_catalog.text,
    constraint_record.convalidated,
    constraint_record.connoinherit
FROM pg_catalog.pg_constraint AS constraint_record
JOIN pg_catalog.pg_class AS relation
  ON relation.oid = constraint_record.conrelid
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
WHERE relation_schema.nspname = %s
  AND relation.relname = ANY(%s::pg_catalog.text[])
  AND constraint_record.contype = 'c'
ORDER BY relation.relname, constraint_record.conname
"""

_UNIQUE_CONSTRAINT_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    index_relation.relname::pg_catalog.text,
    backing_constraint.conname::pg_catalog.text,
    backing_constraint.contype::pg_catalog.text,
    ARRAY(
        SELECT attribute.attname::pg_catalog.text
        FROM pg_catalog.unnest(index_metadata.indkey::pg_catalog.int2[])
             WITH ORDINALITY AS key_column(attnum, position)
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = relation.oid
         AND attribute.attnum = key_column.attnum
        WHERE key_column.position <= index_metadata.indnkeyatts
        ORDER BY key_column.position
    ),
    index_metadata.indoption::pg_catalog.int2[],
    CASE
        WHEN index_metadata.indpred IS NULL THEN NULL
        ELSE pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    pg_catalog.pg_get_expr(
                        index_metadata.indpred,
                        index_metadata.indrelid,
                        true
                    ),
                    'UTF8'
                )
            ),
            'hex'
        )::pg_catalog.text
    END,
    index_metadata.indnullsnotdistinct,
    backing_constraint.condeferrable,
    backing_constraint.condeferred,
    backing_constraint.convalidated,
    index_metadata.indisvalid,
    index_metadata.indisready,
    access_method.amname::pg_catalog.text,
    index_metadata.indnatts = index_metadata.indnkeyatts,
    index_metadata.indnkeyatts = (
        SELECT pg_catalog.count(*)
        FROM pg_catalog.unnest(index_metadata.indkey::pg_catalog.int2[])
             WITH ORDINALITY AS key_column(attnum, position)
        WHERE key_column.position <= index_metadata.indnkeyatts
          AND key_column.attnum > 0
    ),
    (
        SELECT pg_catalog.bool_and(operator_class.opcdefault)
        FROM pg_catalog.unnest(index_metadata.indclass::pg_catalog.oid[])
             WITH ORDINALITY AS indexed_opclass(opclass_oid, position)
        JOIN pg_catalog.pg_opclass AS operator_class
          ON operator_class.oid = indexed_opclass.opclass_oid
        WHERE indexed_opclass.position <= index_metadata.indnkeyatts
    ),
    (
        SELECT pg_catalog.bool_and(
            indexed_collation.collation_oid = attribute.attcollation
        )
        FROM pg_catalog.unnest(index_metadata.indcollation::pg_catalog.oid[])
             WITH ORDINALITY AS indexed_collation(collation_oid, position)
        JOIN pg_catalog.unnest(index_metadata.indkey::pg_catalog.int2[])
             WITH ORDINALITY AS key_column(attnum, position)
          ON key_column.position = indexed_collation.position
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = relation.oid
         AND attribute.attnum = key_column.attnum
        WHERE indexed_collation.position <= index_metadata.indnkeyatts
    )
FROM pg_catalog.pg_index AS index_metadata
JOIN pg_catalog.pg_class AS index_relation
  ON index_relation.oid = index_metadata.indexrelid
JOIN pg_catalog.pg_class AS relation
  ON relation.oid = index_metadata.indrelid
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
JOIN pg_catalog.pg_am AS access_method
  ON access_method.oid = index_relation.relam
JOIN pg_catalog.pg_constraint AS backing_constraint
  ON backing_constraint.conindid = index_relation.oid
 AND backing_constraint.contype IN ('p', 'u')
WHERE relation_schema.nspname = %s
  AND relation.relname = ANY(%s::pg_catalog.text[])
  AND index_metadata.indisunique
ORDER BY relation.relname, backing_constraint.conname
"""

_INDEX_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    index_relation.relname::pg_catalog.text,
    index_metadata.indisunique,
    index_metadata.indisvalid,
    index_metadata.indisready,
    index_metadata.indnatts = index_metadata.indnkeyatts,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.generate_series(
            0, index_metadata.indnkeyatts - 1
        ) AS key_position(position)
        JOIN pg_catalog.pg_opclass AS operator_class
          ON operator_class.oid = index_metadata.indclass[key_position.position]
        WHERE NOT operator_class.opcdefault
    ),
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.generate_series(
            0, index_metadata.indnkeyatts - 1
        ) AS key_position(position)
        JOIN pg_catalog.pg_attribute AS key_attribute
          ON key_attribute.attrelid = relation.oid
         AND key_attribute.attnum = index_metadata.indkey[key_position.position]
        WHERE index_metadata.indcollation[key_position.position] <> 0
          AND index_metadata.indcollation[key_position.position]
              IS DISTINCT FROM key_attribute.attcollation
    ),
    access_method.amname::pg_catalog.text,
    ARRAY(
        SELECT attribute.attname::pg_catalog.text
        FROM pg_catalog.unnest(index_metadata.indkey::pg_catalog.int2[])
             WITH ORDINALITY AS key_column(attnum, position)
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = relation.oid
         AND attribute.attnum = key_column.attnum
        WHERE key_column.position <= index_metadata.indnkeyatts
        ORDER BY key_column.position
    ),
    index_metadata.indoption::pg_catalog.int2[],
    CASE
        WHEN index_metadata.indpred IS NULL THEN NULL
        ELSE pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    pg_catalog.pg_get_expr(
                        index_metadata.indpred,
                        index_metadata.indrelid,
                        true
                    ),
                    'UTF8'
                )
            ),
            'hex'
        )::pg_catalog.text
    END
FROM pg_catalog.pg_index AS index_metadata
JOIN pg_catalog.pg_class AS index_relation
  ON index_relation.oid = index_metadata.indexrelid
JOIN pg_catalog.pg_class AS relation
  ON relation.oid = index_metadata.indrelid
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
JOIN pg_catalog.pg_am AS access_method
  ON access_method.oid = index_relation.relam
LEFT JOIN pg_catalog.pg_constraint AS backing_constraint
  ON backing_constraint.conindid = index_relation.oid
 AND backing_constraint.contype IN ('p', 'u')
WHERE relation_schema.nspname = %s
  AND relation.relname = ANY(%s::pg_catalog.text[])
  AND backing_constraint.oid IS NULL
ORDER BY relation.relname, index_relation.relname
"""

_FOREIGN_KEY_QUERY: Final = """
SELECT
    foreign_key.conname::pg_catalog.text,
    child.relname::pg_catalog.text,
    ARRAY(
        SELECT attribute.attname::pg_catalog.text
        FROM pg_catalog.unnest(foreign_key.conkey) WITH ORDINALITY
             AS key_column(attnum, position)
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = child.oid
         AND attribute.attnum = key_column.attnum
        ORDER BY key_column.position
    ),
    parent.relname::pg_catalog.text,
    ARRAY(
        SELECT attribute.attname::pg_catalog.text
        FROM pg_catalog.unnest(foreign_key.confkey) WITH ORDINALITY
             AS key_column(attnum, position)
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = parent.oid
         AND attribute.attnum = key_column.attnum
        ORDER BY key_column.position
    ),
    foreign_key.confmatchtype::pg_catalog.text,
    foreign_key.confupdtype::pg_catalog.text,
    foreign_key.confdeltype::pg_catalog.text,
    foreign_key.condeferrable,
    foreign_key.condeferred,
    foreign_key.convalidated
FROM pg_catalog.pg_constraint AS foreign_key
JOIN pg_catalog.pg_class AS child
  ON child.oid = foreign_key.conrelid
JOIN pg_catalog.pg_class AS parent
  ON parent.oid = foreign_key.confrelid
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = child.relnamespace
WHERE relation_schema.nspname = %s
  AND foreign_key.contype = 'f'
ORDER BY foreign_key.conname
"""

_TRIGGER_QUERY: Final = """
SELECT
    trigger.tgname::pg_catalog.text,
    relation.relname::pg_catalog.text,
    routine.proname::pg_catalog.text,
    trigger.tgtype,
    trigger.tgconstraint <> 0,
    trigger.tgenabled::pg_catalog.text,
    routine_schema.nspname = %s,
    routine.proowner = relation_schema.nspowner,
    trigger.tgparentid = 0,
    trigger.tgconstrrelid = 0,
    trigger.tgconstrindid = 0,
    trigger.tgdeferrable,
    trigger.tginitdeferred,
    trigger.tgnargs,
    trigger.tgattr::pg_catalog.text,
    pg_catalog.encode(trigger.tgargs, 'hex')::pg_catalog.text,
    CASE
        WHEN trigger.tgqual IS NULL THEN NULL::pg_catalog.text
        ELSE pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    pg_catalog.pg_get_expr(
                        trigger.tgqual,
                        trigger.tgrelid,
                        false
                    ),
                    'UTF8'
                )
            ),
            'hex'
        )::pg_catalog.text
    END,
    trigger.tgoldtable::pg_catalog.text,
    trigger.tgnewtable::pg_catalog.text,
    CASE
        WHEN trigger.tgconstraint = 0 THEN true
        ELSE (
            trigger_constraint.oid IS NOT NULL
            AND trigger_constraint.contype = 't'
            AND trigger_constraint.conname = trigger.tgname
            AND trigger_constraint.conrelid = trigger.tgrelid
            AND trigger_constraint.condeferrable = trigger.tgdeferrable
            AND trigger_constraint.condeferred = trigger.tginitdeferred
            AND trigger_constraint.convalidated
            AND trigger_constraint.connoinherit
            AND trigger_constraint.conparentid = 0
            AND trigger_constraint.coninhcount = 0
            AND trigger_constraint.conislocal
        )
    END
FROM pg_catalog.pg_trigger AS trigger
JOIN pg_catalog.pg_class AS relation
  ON relation.oid = trigger.tgrelid
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
JOIN pg_catalog.pg_proc AS routine
  ON routine.oid = trigger.tgfoid
JOIN pg_catalog.pg_namespace AS routine_schema
  ON routine_schema.oid = routine.pronamespace
LEFT JOIN pg_catalog.pg_constraint AS trigger_constraint
  ON trigger_constraint.oid = trigger.tgconstraint
WHERE relation_schema.nspname = %s
  AND NOT trigger.tgisinternal
ORDER BY relation.relname, trigger.tgname
"""

_ROUTINE_QUERY: Final = """
SELECT
    routine.proname::pg_catalog.text,
    pg_catalog.pg_get_function_identity_arguments(routine.oid)::pg_catalog.text,
    pg_catalog.pg_get_function_result(routine.oid)::pg_catalog.text,
    language.lanname::pg_catalog.text,
    routine.prokind::pg_catalog.text,
    routine.prosecdef,
    routine.provolatile::pg_catalog.text,
    routine.proisstrict,
    routine.proleakproof,
    routine.proparallel::pg_catalog.text,
    routine.proretset,
    routine.pronargdefaults,
    COALESCE(routine.proconfig, ARRAY[]::pg_catalog.text[]),
    routine.proowner = routine_schema.nspowner,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_depend AS dependency
        WHERE dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
          AND dependency.objid = routine.oid
          AND dependency.deptype = 'e'
    ),
    pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(routine.prosrc, 'UTF8')
        ),
        'hex'
    )::pg_catalog.text
FROM pg_catalog.pg_proc AS routine
JOIN pg_catalog.pg_namespace AS routine_schema
  ON routine_schema.oid = routine.pronamespace
JOIN pg_catalog.pg_language AS language
  ON language.oid = routine.prolang
WHERE routine_schema.nspname = %s
ORDER BY routine.proname,
         pg_catalog.pg_get_function_identity_arguments(routine.oid)
"""


def _invalid_contract() -> DatabaseSchemaContractError:
    return DatabaseSchemaContractError("database_schema_contract_invalid")


async def require_database_schema_contract(
    dsn: str,
    *,
    target_schema: str,
    require_complete: bool,
    require_business_complete: bool = False,
    expected_revision: str | None = None,
) -> None:
    """Reject physical drift while allowing bootstrap to repair checkpoints."""

    all_phase2_relations = frozenset(
        relation_name
        for relations in PHASE2_RELATIONS_BY_REVISION.values()
        for relation_name in relations
    )
    all_phase2_views = tuple(
        sorted(
            {
                view.name
                for views in PHASE2_VIEW_SPECS_BY_REVISION.values()
                for view in views
            }
        )
    )
    all_column_types = {
        **_EXPECTED_COLUMN_TYPES,
        **_SYNC_RECONCILIATION_COLUMN_TYPES,
        **_GREENFIELD_COLUMN_TYPES,
    }
    relation_names = sorted({key[0] for key in all_column_types})
    view_contract_rows: list[tuple[object, ...]] = []
    foreign_key_rows: list[tuple[object, ...]] = []
    trigger_rows: list[tuple[object, ...]] = []
    routine_rows: list[tuple[object, ...]] = []
    try:
        async with await psycopg.AsyncConnection.connect(
            dsn,
            autocommit=True,
        ) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SET search_path TO pg_catalog")
                await cursor.execute(
                    _RELATION_KIND_QUERY,
                    (target_schema,),
                )
                relation_kind_rows = await cursor.fetchall()
                deployed_greenfield = any(
                    row[0] == "pipeline_runtime_authority" for row in relation_kind_rows
                )
                await cursor.execute(
                    _COLUMN_TYPE_QUERY,
                    (target_schema, relation_names),
                )
                rows = await cursor.fetchall()
                await cursor.execute(
                    _CHECK_CONSTRAINT_QUERY,
                    (target_schema, list(all_phase2_relations)),
                )
                check_constraint_rows = await cursor.fetchall()
                await cursor.execute(
                    _UNIQUE_CONSTRAINT_QUERY,
                    (target_schema, list(all_phase2_relations)),
                )
                unique_constraint_rows = await cursor.fetchall()
                await cursor.execute(
                    _INDEX_QUERY,
                    (target_schema, list(all_phase2_relations)),
                )
                index_rows = await cursor.fetchall()
                deployed_view_names = {
                    row[0] for row in relation_kind_rows if row[0] in all_phase2_views
                }
                if deployed_view_names:
                    await cursor.execute(
                        _VIEW_CONTRACT_QUERY,
                        (target_schema, list(all_phase2_views)),
                    )
                    view_contract_rows = await cursor.fetchall()
                if deployed_greenfield:
                    await cursor.execute(
                        _FOREIGN_KEY_QUERY,
                        (target_schema,),
                    )
                    foreign_key_rows = await cursor.fetchall()
                    await cursor.execute(
                        _TRIGGER_QUERY,
                        (target_schema, target_schema),
                    )
                    trigger_rows = await cursor.fetchall()
                    await cursor.execute(
                        _ROUTINE_QUERY,
                        (target_schema,),
                    )
                    routine_rows = await cursor.fetchall()
    except Exception:
        raise _invalid_contract() from None

    actual = {
        (relation_name, column_name): (
            type_schema,
            type_name,
            is_nullable,
            has_default,
            type_modifier,
            default_expression,
            identity_kind,
            generated_kind,
            uses_default_collation,
            uses_default_storage,
            compression,
        )
        for (
            relation_name,
            column_name,
            type_schema,
            type_name,
            is_nullable,
            has_default,
            type_modifier,
            default_expression,
            identity_kind,
            generated_kind,
            uses_default_collation,
            uses_default_storage,
            compression,
        ) in rows
    }
    deployed_phase2_relation_kinds = {
        relation_name: relation_kind
        for relation_name, relation_kind, *_rest in relation_kind_rows
        if relation_name in all_phase2_relations or relation_name in all_phase2_views
    }
    if expected_revision is None:
        revision_candidates = tuple(
            revision
            for revision, relation_kinds in (_PHASE2_RELATION_KINDS_BY_REVISION.items())
            if deployed_phase2_relation_kinds == relation_kinds
        )
    elif expected_revision in PHASE2_RELATIONS_BY_REVISION:
        revision_candidates = (expected_revision,)
    else:
        raise _invalid_contract()
    if not revision_candidates:
        raise _invalid_contract()
    structural_revision = revision_candidates[0]
    if (
        deployed_phase2_relation_kinds
        != _PHASE2_RELATION_KINDS_BY_REVISION[structural_revision]
    ):
        raise _invalid_contract()
    expected_relation_kinds = dict(_BASE_RELATION_KINDS)
    expected_relation_kinds.update(
        _PHASE2_RELATION_KINDS_BY_REVISION[structural_revision]
    )
    expected_relation_contract = {
        name: (
            relation_kind,
            "p",
            False,
            False,
            True,
            True,
            "heap" if relation_kind in {"r", "p"} else None,
        )
        for name, relation_kind in expected_relation_kinds.items()
    }
    actual_relation_contract = {
        name: (
            relation_kind,
            persistence,
            row_security_enabled,
            row_security_forced,
            owner_matches_schema_owner,
            has_no_policies,
            access_method,
        )
        for (
            name,
            relation_kind,
            persistence,
            row_security_enabled,
            row_security_forced,
            owner_matches_schema_owner,
            has_no_policies,
            access_method,
        ) in relation_kind_rows
    }
    if require_complete:
        if actual_relation_contract != expected_relation_contract:
            raise _invalid_contract()
    elif require_business_complete:
        expected_business_relations = {
            name: contract
            for name, contract in expected_relation_contract.items()
            if name not in _CHECKPOINT_RELATIONS
        }
        actual_business_relations = {
            name: contract
            for name, contract in actual_relation_contract.items()
            if name not in _CHECKPOINT_RELATIONS
        }
        if actual_business_relations != expected_business_relations:
            raise _invalid_contract()
        if any(
            expected_relation_contract.get(name) != relation_contract
            for name, relation_contract in actual_relation_contract.items()
        ):
            raise _invalid_contract()
    elif any(
        expected_relation_contract.get(name) != relation_contract
        for name, relation_contract in actual_relation_contract.items()
    ):
        raise _invalid_contract()
    base_column_types = {
        column: type_name
        for column, type_name in _EXPECTED_COLUMN_TYPES.items()
        if column[0] not in all_phase2_relations
    }
    expected_column_types = {
        **base_column_types,
        **_PHASE2_COLUMN_TYPES_BY_REVISION[structural_revision],
    }
    expected_columns = set(expected_column_types)
    if require_complete:
        if set(actual) != expected_columns:
            raise _invalid_contract()
    elif require_business_complete:
        expected_business_columns = {
            column
            for column in expected_columns
            if column[0] not in _CHECKPOINT_RELATIONS
        }
        actual_business_columns = {
            column for column in actual if column[0] not in _CHECKPOINT_RELATIONS
        }
        if actual_business_columns != expected_business_columns:
            raise _invalid_contract()
    for column, expected_type in expected_column_types.items():
        if column not in expected_columns:
            continue
        deployed_type = actual.get(column)
        if deployed_type is None:
            if require_complete or (
                require_business_complete and column[0] not in _CHECKPOINT_RELATIONS
            ):
                raise _invalid_contract()
            continue
        if deployed_type[:2] != ("pg_catalog", expected_type):
            raise _invalid_contract()
        relation_name, _ = column
        if relation_name not in all_phase2_relations:
            continue
        (
            _,
            _,
            is_nullable,
            has_default,
            type_modifier,
            default_expression,
            identity_kind,
            generated_kind,
            uses_default_collation,
            uses_default_storage,
            compression,
        ) = deployed_type
        nullable_columns = _PHASE2_NULLABLE_COLUMNS_BY_REVISION[structural_revision]
        defaulted_columns = _PHASE2_DEFAULTED_COLUMNS_BY_REVISION[structural_revision]
        default_expressions = _DEFAULT_EXPRESSIONS_BY_REVISION.get(
            structural_revision,
            {},
        )
        generated_expressions = _GENERATED_EXPRESSION_SHA256_BY_REVISION.get(
            structural_revision,
            {},
        )
        generated_expression_sha256 = generated_expressions.get(column)
        if is_nullable is not (column in nullable_columns):
            raise _invalid_contract()
        if generated_expression_sha256 is None:
            if has_default is not (column in defaulted_columns):
                raise _invalid_contract()
            if default_expression != default_expressions.get(column):
                raise _invalid_contract()
            if generated_kind:
                raise _invalid_contract()
        else:
            if (
                has_default is not True
                or type(default_expression) is not str
                or generated_kind != "s"
                or hashlib.sha256(default_expression.encode("utf-8")).hexdigest()
                != generated_expression_sha256
            ):
                raise _invalid_contract()
        if identity_kind:
            raise _invalid_contract()
        if uses_default_collation is not True:
            raise _invalid_contract()
        if uses_default_storage is not True:
            raise _invalid_contract()
        if compression:
            raise _invalid_contract()
        if expected_type == "bpchar" and type_modifier != 68:
            raise _invalid_contract()

    actual_checks = {
        (relation_name, constraint_name): (
            source_sha256,
            is_validated,
            is_inheritable,
        )
        for (
            relation_name,
            constraint_name,
            source_sha256,
            is_validated,
            is_inheritable,
        ) in check_constraint_rows
    }
    matching_revisions = tuple(
        revision
        for revision in revision_candidates
        if actual_checks
        == {
            key: (source_sha256, True, False)
            for key, source_sha256 in (
                _CHECK_CONSTRAINT_SHA256_BY_REVISION.get(
                    revision,
                    {},
                ).items()
            )
        }
    )
    if len(matching_revisions) != 1:
        raise _invalid_contract()
    selected_revision = matching_revisions[0]

    actual_views = {
        (relation_name, relation_kind, definition_sha256, tuple(relation_options))
        for (
            relation_name,
            relation_kind,
            definition_sha256,
            relation_options,
        ) in view_contract_rows
    }
    expected_views = {
        (
            view.name,
            view.relation_kind,
            view.definition_sha256,
            (f"check_option={view.check_option.lower()}",),
        )
        for view in PHASE2_VIEW_SPECS_BY_REVISION[selected_revision]
    }
    if actual_views != expected_views:
        raise _invalid_contract()

    if selected_revision == GREENFIELD_DATABASE_REVISION:
        expected_unique = _GREENFIELD_UNIQUE_CONSTRAINTS
    else:
        expected_unique = {
            (
                spec.relation,
                spec.name,
                spec.name,
                spec.constraint_type,
                spec.columns,
                spec.index_options,
                None,
                spec.nulls_not_distinct,
                spec.deferrable,
                spec.initially_deferred,
                spec.validated,
                spec.index_valid,
                spec.index_ready,
                spec.access_method,
                spec.has_no_included_columns,
                spec.has_only_plain_columns,
                spec.uses_default_operator_classes,
                spec.uses_default_collations,
            )
            for spec in PHASE2_UNIQUE_CONSTRAINTS_BY_REVISION.get(
                selected_revision,
                (),
            )
        }
    actual_unique = {
        (
            relation_name,
            index_name,
            constraint_name,
            constraint_type,
            tuple(columns),
            tuple(index_options),
            predicate_sha256,
            nulls_not_distinct,
            is_deferrable,
            is_initially_deferred,
            is_constraint_validated,
            is_index_valid,
            is_index_ready,
            access_method,
            has_no_included_columns,
            has_only_plain_columns,
            uses_default_operator_classes,
            uses_default_collations,
        )
        for (
            relation_name,
            index_name,
            constraint_name,
            constraint_type,
            columns,
            index_options,
            predicate_sha256,
            nulls_not_distinct,
            is_deferrable,
            is_initially_deferred,
            is_constraint_validated,
            is_index_valid,
            is_index_ready,
            access_method,
            has_no_included_columns,
            has_only_plain_columns,
            uses_default_operator_classes,
            uses_default_collations,
        ) in unique_constraint_rows
    }
    if actual_unique != expected_unique:
        raise _invalid_contract()

    if selected_revision == GREENFIELD_DATABASE_REVISION:
        expected_indexes = _GREENFIELD_INDEXES
    else:
        expected_indexes = {
            (
                spec.relation,
                spec.name,
                spec.unique,
                True,
                True,
                True,
                True,
                True,
                "btree",
                spec.columns,
                spec.options,
                spec.predicate_sha256,
            )
            for spec in PHASE2_INDEX_SPECS_BY_REVISION.get(
                selected_revision,
                (),
            )
        }
    actual_indexes = {
        (
            relation_name,
            index_name,
            is_unique,
            is_valid,
            is_ready,
            has_no_included_columns,
            uses_default_operator_classes,
            uses_column_collations,
            access_method,
            tuple(columns),
            tuple(options),
            predicate_sha256,
        )
        for (
            relation_name,
            index_name,
            is_unique,
            is_valid,
            is_ready,
            has_no_included_columns,
            uses_default_operator_classes,
            uses_column_collations,
            access_method,
            columns,
            options,
            predicate_sha256,
        ) in index_rows
    }
    if actual_indexes != expected_indexes:
        raise _invalid_contract()

    if selected_revision != GREENFIELD_DATABASE_REVISION:
        return

    actual_foreign_keys = {
        (
            name,
            child_relation,
            tuple(child_columns),
            parent_relation,
            tuple(parent_columns),
            match_type,
            update_action,
            delete_action,
            deferrable,
            initially_deferred,
            validated,
        )
        for (
            name,
            child_relation,
            child_columns,
            parent_relation,
            parent_columns,
            match_type,
            update_action,
            delete_action,
            deferrable,
            initially_deferred,
            validated,
        ) in foreign_key_rows
    }
    if actual_foreign_keys != _GREENFIELD_FOREIGN_KEYS:
        raise _invalid_contract()

    if {tuple(row) for row in trigger_rows} != _GREENFIELD_TRIGGERS:
        raise _invalid_contract()

    actual_routines = {
        (
            name,
            identity_arguments,
            result,
            language,
            routine_kind,
            security_definer,
            volatility,
            strict,
            leakproof,
            parallel,
            returns_set,
            argument_defaults,
            tuple(configuration),
            owner_matches_schema_owner,
            extension_free,
        )
        for (
            name,
            identity_arguments,
            result,
            language,
            routine_kind,
            security_definer,
            volatility,
            strict,
            leakproof,
            parallel,
            returns_set,
            argument_defaults,
            configuration,
            owner_matches_schema_owner,
            extension_free,
            _source_sha256,
        ) in routine_rows
    }
    if actual_routines != _GREENFIELD_ROUTINES:
        raise _invalid_contract()

    actual_routine_digests = {(row[0], row[1]): row[-1] for row in routine_rows}
    if any(
        actual_routine_digests.get(identity) != expected_digest
        for identity, expected_digest in _GREENFIELD_ROUTINE_SOURCE_SHA256.items()
    ):
        raise _invalid_contract()
