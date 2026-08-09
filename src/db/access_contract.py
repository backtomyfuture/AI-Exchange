"""The single, greenfield PostgreSQL access contract.

There is intentionally no historical revision matrix here.  A deployment owns
an empty application database and creates exactly the polling baseline below.
Schema shape is checked in :mod:`src.db.schema_contract`; this module records
the narrow role grants and executable database hooks that remain valid for
that one shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


DATABASE_REVISION: Final = "20260808_0001"
# The role verifier currently consumes revision-indexed manifests.  Every map
# below has exactly one entry and must not be extended for an in-place upgrade.


@dataclass(frozen=True)
class RelationAccess:
    table_privileges: tuple[str, ...] = ()
    select_columns: tuple[str, ...] = ()
    insert_columns: tuple[str, ...] = ()
    update_columns: tuple[str, ...] = ()
    delete: bool = False


@dataclass(frozen=True)
class RoutineAccess:
    """One exact ``pg_get_function_identity_arguments`` EXECUTE grant."""

    name: str
    identity_arguments: str


@dataclass(frozen=True)
class ViewSpec:
    name: str
    relation_kind: str
    check_option: str = "NONE"
    definition_sha256: str = ""


@dataclass(frozen=True)
class ForeignKeySpec:
    name: str
    child_relation: str
    child_columns: tuple[str, ...]
    parent_relation: str
    parent_columns: tuple[str, ...]
    match_type: str
    update_action: str = "r"
    delete_action: str = "r"
    deferrable: bool = False
    initially_deferred: bool = False
    validated: bool = True


@dataclass(frozen=True)
class TriggerSpec:
    name: str
    relation: str
    function: str
    trigger_type: int
    is_constraint: bool = False
    is_deferrable: bool = False
    is_initially_deferred: bool = False
    arguments: tuple[str, ...] = ()
    update_attribute_numbers: tuple[int, ...] = ()
    when_clause_sha256: str | None = None
    old_transition_table: str | None = None
    new_transition_table: str | None = None


def _access(
    *,
    table: tuple[str, ...] = (),
    select: tuple[str, ...] = (),
    insert: tuple[str, ...] = (),
    update: tuple[str, ...] = (),
    delete: bool = False,
) -> RelationAccess:
    return RelationAccess(table, select, insert, update, delete)


def _routine(name: str, arguments: str) -> RoutineAccess:
    return RoutineAccess(name, " ".join(arguments.split()))


POLLING_RELATIONS: Final = (
    "audit_events",
    "daily_digest_executions",
    "emails",
    "event_inbox",
    "handoff_executions",
    "pipeline_command_receipts",
    "pipeline_folder_scopes",
    "pipeline_initializations",
    "pipeline_ownership",
    "pipeline_runtime_authority",
    "pipeline_runtime_capabilities",
    "pipeline_runtime_instances",
    "sync_cursors",
    "tier1_decisions",
)
POLLING_RELATIONS_BY_REVISION: Final = {DATABASE_REVISION: POLLING_RELATIONS}
POLLING_VIEW_SPECS_BY_REVISION: Final = {DATABASE_REVISION: ()}


RUNTIME_RELATION_ACCESS: Final = {
    "alembic_version": _access(table=("SELECT",)),
    "emails_log": _access(
        table=("SELECT",),
        insert=("id", "subject", "sender", "received_at", "status"),
        update=(
            "status",
            "classification",
            "draft_content",
            "updated_at",
            "routing_log",
            "original_draft",
            "final_draft",
            "approver_user_id",
            "rejection_reason",
            "error_message",
            "content_ref",
        ),
    ),
    "checkpoints": _access(
        table=("SELECT",),
        insert=(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "parent_checkpoint_id",
            "checkpoint",
            "metadata",
        ),
        update=("checkpoint", "metadata"),
    ),
    "checkpoint_blobs": _access(
        table=("SELECT",),
        insert=("thread_id", "checkpoint_ns", "channel", "version", "type", "blob"),
    ),
    "checkpoint_writes": _access(
        table=("SELECT",),
        insert=(
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
        update=("channel", "type", "blob"),
    ),
    "pipeline_ownership": _access(table=("SELECT",)),
    "event_inbox": _access(
        table=("SELECT",),
        insert=(
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
        update=(
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
    ),
    "sync_cursors": _access(table=("SELECT",)),
    "emails": _access(
        table=("SELECT",),
        insert=(
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
        update=(
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
    ),
    "audit_events": _access(
        table=("SELECT",),
        insert=(
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
    ),
    "pipeline_command_receipts": _access(table=("SELECT",)),
    "pipeline_runtime_capabilities": _access(table=("SELECT",)),
    "pipeline_initializations": _access(table=("SELECT",)),
    "pipeline_folder_scopes": _access(table=("SELECT",)),
    "pipeline_runtime_authority": _access(table=("SELECT",)),
    "pipeline_runtime_instances": _access(table=("SELECT",)),
    "daily_digest_executions": _access(
        table=("SELECT",),
        insert=(
            "account_id",
            "delivery_scope_hash",
            "window_start",
            "window_end",
            "state",
            "is_backfill",
            "delivery_parts",
        ),
        update=(
            "state",
            "delivery_parts",
            "attempt_count",
            "last_attempt_at",
            "last_error_code",
            "confirmed_at",
            "missed_at",
            "missed_reported_at",
            "updated_at",
        ),
    ),
    "tier1_decisions": _access(
        table=("SELECT",),
        insert=(
            "inbox_id",
            "account_id",
            "external_email_id",
            "decision_digest",
            "decision_json",
            "outcome",
            "route",
            "tier",
            "artifact_digest",
        ),
    ),
    "handoff_executions": _access(
        table=("SELECT",),
        insert=("inbox_id", "decision_digest", "state"),
        update=("state", "version", "safe_error_code", "updated_at"),
    ),
}
RUNTIME_RELATION_ACCESS_BY_REVISION: Final = {DATABASE_REVISION: RUNTIME_RELATION_ACCESS}


MAINTENANCE_RELATION_ACCESS: Final = {
    "alembic_version": _access(table=("SELECT",)),
    "checkpoint_migrations": _access(table=("SELECT",)),
    "emails_log": _access(table=("SELECT",)),
    "checkpoints": _access(table=("SELECT",), delete=True),
    "checkpoint_blobs": _access(table=("SELECT",), delete=True),
    "checkpoint_writes": _access(table=("SELECT",), delete=True),
    "pipeline_ownership": _access(table=("SELECT",)),
    "event_inbox": _access(table=("SELECT",)),
    "sync_cursors": _access(table=("SELECT",)),
    "emails": _access(table=("SELECT",)),
    "audit_events": _access(table=("SELECT",)),
    "pipeline_command_receipts": _access(table=("SELECT",)),
    "pipeline_runtime_capabilities": _access(table=("SELECT",)),
    "pipeline_initializations": _access(table=("SELECT",)),
    "pipeline_folder_scopes": _access(table=("SELECT",)),
    "pipeline_runtime_authority": _access(table=("SELECT",)),
    "pipeline_runtime_instances": _access(table=("SELECT",)),
    "tier1_decisions": _access(table=("SELECT",)),
    "handoff_executions": _access(table=("SELECT",)),
}
MAINTENANCE_RELATION_ACCESS_BY_REVISION: Final = {
    DATABASE_REVISION: MAINTENANCE_RELATION_ACCESS
}


AUDITOR_RELATION_ACCESS: Final = {
    "alembic_version": _access(select=("version_num",)),
    "checkpoint_migrations": _access(select=("v",)),
    "emails_log": _access(select=("id", "status", "updated_at")),
    "checkpoints": _access(
        select=(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "parent_checkpoint_id",
            "type",
            "checkpoint",
            "metadata",
        )
    ),
    "checkpoint_blobs": _access(
        select=("thread_id", "checkpoint_ns", "channel", "version", "type", "blob")
    ),
    "checkpoint_writes": _access(
        select=(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "task_id",
            "idx",
            "channel",
            "type",
            "blob",
            "task_path",
        )
    ),
    "pipeline_runtime_capabilities": _access(table=("SELECT",)),
    "pipeline_initializations": _access(table=("SELECT",)),
    "pipeline_runtime_authority": _access(table=("SELECT",)),
    "pipeline_runtime_instances": _access(table=("SELECT",)),
    "pipeline_ownership": _access(table=("SELECT",)),
    "pipeline_command_receipts": _access(table=("SELECT",)),
    "audit_events": _access(table=("SELECT",)),
    "tier1_decisions": _access(table=("SELECT",)),
    "handoff_executions": _access(table=("SELECT",)),
    "pipeline_folder_scopes": _access(
        select=(
            "initialization_id",
            "account_id",
            "canonical_key",
            "scope_hash",
            "policy_manifest_hash",
            "created_at",
        )
    ),
    "event_inbox": _access(
        select=(
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
        )
    ),
    "emails": _access(
        select=(
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
        )
    ),
}
AUDITOR_RELATION_ACCESS_BY_REVISION: Final = {DATABASE_REVISION: AUDITOR_RELATION_ACCESS}


RUNTIME_ROUTINE_EXECUTE: Final = (
    _routine("greenfield_get_runtime_authority", "p_account_id bigint"),
    _routine(
        "greenfield_register_web_instance",
        "p_account_id bigint, p_instance_id text, p_session_id uuid, "
        "p_expected_authority_epoch bigint, p_expected_authority_version bigint, "
        "p_schema_revision text, p_protocol_version bigint, p_build_id text, "
        "p_config_hash text, p_capability_hash text, p_lease_seconds bigint",
    ),
    _routine(
        "greenfield_heartbeat_web_instance",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_expected_authority_epoch bigint, p_expected_capability_hash text, "
        "p_accepted_count bigint, p_rejected_count bigint, p_lease_seconds bigint",
    ),
    _routine(
        "greenfield_drain_web_instance",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_expected_authority_epoch bigint, p_expected_capability_hash text",
    ),
    _routine(
        "greenfield_claim_inbox",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_lease_owner text, p_limit bigint, p_lease_seconds bigint",
    ),
    _routine(
        "greenfield_renew_inbox",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_lease_owner text, "
        "p_attempts bigint, p_lease_seconds bigint",
    ),
    _routine(
        "greenfield_apply_email_event",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_expected_email_version bigint",
    ),
    _routine(
        "greenfield_begin_inbox_effect",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint",
    ),
    _routine(
        "greenfield_finish_inbox",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint, p_completion jsonb",
    ),
    _routine(
        "greenfield_fail_inbox",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint, "
        "p_safe_error_code text, p_safe_error_summary text",
    ),
    _routine(
        "greenfield_reap_inbox",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_limit bigint",
    ),
    _routine(
        "greenfield_commit_sync_page",
        "p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, "
        "p_folder_key text, p_expected_cursor text, p_expected_cursor_version bigint, "
        "p_next_cursor text, p_events jsonb, p_activation boolean",
    ),
)
MAINTENANCE_ROUTINE_EXECUTE: Final = (
    _routine(
        "greenfield_initialize_runtime",
        "p_account_id bigint, p_capability_hash text, p_predecessor_hash text, "
        "p_capability_stage text, p_schema_revision text, p_schema_digest text, "
        "p_protocol_version bigint, p_minimum_build_id text, p_config_hash text, "
        "p_adapter_hash text, p_policy_manifest_hash text, p_evidence_manifest_hash text, "
        "p_policy_manifest_json text, p_policy_scope_count bigint, p_actor text, "
        "p_reason text, p_idempotency_key text, p_canonical_payload_hash text",
    ),
    _routine("greenfield_get_runtime_authority", "p_account_id bigint"),
    _routine(
        "greenfield_pause_runtime",
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, p_actor text, "
        "p_reason text, p_idempotency_key text, p_canonical_payload_hash text",
    ),
    _routine(
        "greenfield_resume_ingress",
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, p_actor text, "
        "p_reason text, p_idempotency_key text, p_canonical_payload_hash text",
    ),
    _routine(
        "greenfield_requeue_inbox",
        "p_account_id bigint, p_inbox_id uuid, p_expected_execution_epoch bigint, "
        "p_expected_email_version bigint, p_actor text, p_reason text, "
        "p_idempotency_key text, p_canonical_payload_hash text",
    ),
)
AUDITOR_ROUTINE_EXECUTE: Final = ()
RUNTIME_ROUTINE_EXECUTE_BY_REVISION: Final = {DATABASE_REVISION: RUNTIME_ROUTINE_EXECUTE}
MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION: Final = {
    DATABASE_REVISION: MAINTENANCE_ROUTINE_EXECUTE
}
AUDITOR_ROUTINE_EXECUTE_BY_REVISION: Final = {DATABASE_REVISION: AUDITOR_ROUTINE_EXECUTE}
SECURITY_DEFINER_ROUTINES: Final = tuple(
    routine
    for _identity, routine in sorted(
        {
            (routine.name, routine.identity_arguments): routine
            for routine in (*RUNTIME_ROUTINE_EXECUTE, *MAINTENANCE_ROUTINE_EXECUTE)
        }.items()
    )
)
SECURITY_DEFINER_ROUTINES_BY_REVISION: Final = {
    DATABASE_REVISION: SECURITY_DEFINER_ROUTINES
}


FOREIGN_KEY_SPECS: Final = (
    ForeignKeySpec(
        "fk_event_inbox_pipeline_ownership",
        "event_inbox",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
    ForeignKeySpec(
        "fk_emails_pipeline_ownership",
        "emails",
        ("account_id", "owner_generation", "owner_fencing_token"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token"),
        "f",
    ),
    ForeignKeySpec(
        "fk_audit_events_email",
        "audit_events",
        ("account_id", "email_id"),
        "emails",
        ("account_id", "id"),
        "s",
    ),
    ForeignKeySpec(
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
    ForeignKeySpec(
        "fk_emails_runtime_capability",
        "emails",
        ("owner_capability_hash",),
        "pipeline_runtime_capabilities",
        ("capability_hash",),
        "f",
    ),
    ForeignKeySpec(
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
    ForeignKeySpec(
        "fk_event_inbox_runtime_capability",
        "event_inbox",
        ("capability_hash",),
        "pipeline_runtime_capabilities",
        ("capability_hash",),
        "f",
    ),
    ForeignKeySpec(
        "fk_tier1_decisions_inbox",
        "tier1_decisions",
        ("inbox_id",),
        "event_inbox",
        ("id",),
        "s",
    ),
    ForeignKeySpec(
        "fk_handoff_executions_decision",
        "handoff_executions",
        ("inbox_id",),
        "tier1_decisions",
        ("inbox_id",),
        "s",
    ),
    ForeignKeySpec(
        "fk_pipeline_folder_scopes_initialization",
        "pipeline_folder_scopes",
        ("initialization_id", "account_id", "policy_manifest_hash"),
        "pipeline_initializations",
        ("initialization_id", "account_id", "policy_manifest_hash"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_initializations_capability",
        "pipeline_initializations",
        ("capability_hash", "capability_stage_ordinal", "policy_manifest_hash"),
        "pipeline_runtime_capabilities",
        ("capability_hash", "stage_ordinal", "policy_manifest_hash"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_initializations_ownership",
        "pipeline_initializations",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_initializations_receipt",
        "pipeline_initializations",
        ("command_receipt_id", "account_id", "receipt_command_name", "authority_epoch"),
        "pipeline_command_receipts",
        ("id", "account_id", "command_name", "authority_epoch"),
        "f",
    ),
    ForeignKeySpec(
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
    ForeignKeySpec(
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
    ForeignKeySpec(
        "fk_pipeline_runtime_authority_ownership",
        "pipeline_runtime_authority",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_runtime_capabilities_predecessor",
        "pipeline_runtime_capabilities",
        ("predecessor_hash", "predecessor_stage_ordinal"),
        "pipeline_runtime_capabilities",
        ("capability_hash", "stage_ordinal"),
        "s",
    ),
    ForeignKeySpec(
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
)
FOREIGN_KEY_SPECS_BY_REVISION: Final = {DATABASE_REVISION: FOREIGN_KEY_SPECS}


TRIGGER_SPECS: Final = (
    TriggerSpec("trg_pipeline_ownership_guard_row", "pipeline_ownership", "guard_pipeline_ownership", 31),
    TriggerSpec("trg_pipeline_ownership_guard_truncate", "pipeline_ownership", "guard_pipeline_ownership", 34),
    TriggerSpec("trg_audit_events_guard_row", "audit_events", "reject_audit_events_mutation", 27),
    TriggerSpec("trg_audit_events_guard_truncate", "audit_events", "reject_audit_events_mutation", 34),
    TriggerSpec("trg_pipeline_command_receipts_guard_row", "pipeline_command_receipts", "reject_pipeline_command_receipts_mutation", 27),
    TriggerSpec("trg_pipeline_command_receipts_guard_truncate", "pipeline_command_receipts", "reject_pipeline_command_receipts_mutation", 34),
    TriggerSpec("trg_emails_runtime_identity", "emails", "guard_emails_runtime_identity", 21, is_constraint=True, is_deferrable=True, is_initially_deferred=True),
    TriggerSpec("trg_event_inbox_runtime_identity", "event_inbox", "guard_event_inbox_runtime_identity", 23),
    TriggerSpec("trg_pipeline_folder_scopes_guard_row", "pipeline_folder_scopes", "reject_pipeline_folder_scopes_mutation", 31),
    TriggerSpec("trg_pipeline_folder_scopes_guard_truncate", "pipeline_folder_scopes", "reject_pipeline_folder_scopes_mutation", 34),
    TriggerSpec("trg_pipeline_initializations_guard_row", "pipeline_initializations", "reject_pipeline_initializations_mutation", 31),
    TriggerSpec("trg_pipeline_initializations_guard_truncate", "pipeline_initializations", "reject_pipeline_initializations_mutation", 34),
    TriggerSpec("trg_pipeline_runtime_authority_guard_row", "pipeline_runtime_authority", "guard_pipeline_runtime_authority", 31),
    TriggerSpec("trg_pipeline_runtime_authority_guard_truncate", "pipeline_runtime_authority", "guard_pipeline_runtime_authority", 34),
    TriggerSpec("trg_pipeline_runtime_capabilities_guard_row", "pipeline_runtime_capabilities", "reject_pipeline_runtime_capabilities_mutation", 27),
    TriggerSpec("trg_pipeline_runtime_capabilities_guard_truncate", "pipeline_runtime_capabilities", "reject_pipeline_runtime_capabilities_mutation", 34),
    TriggerSpec("trg_pipeline_runtime_instances_guard_row", "pipeline_runtime_instances", "guard_pipeline_runtime_instances", 31),
    TriggerSpec("trg_pipeline_runtime_instances_guard_truncate", "pipeline_runtime_instances", "guard_pipeline_runtime_instances", 34),
    TriggerSpec("trg_tier1_decisions_guard_row", "tier1_decisions", "reject_tier1_decisions_mutation", 27),
    TriggerSpec("trg_tier1_decisions_guard_truncate", "tier1_decisions", "reject_tier1_decisions_mutation", 34),
)
TRIGGER_SPECS_BY_REVISION: Final = {DATABASE_REVISION: TRIGGER_SPECS}
TRIGGER_FUNCTION_SOURCE_SHA256: Final = {
    "guard_emails_runtime_identity": "0f3ec494fb8d70649dcf48c6d088a5ecac59e5c0b13a353366e55a884fcab6c9",
    "guard_event_inbox_runtime_identity": "790cf9c0196540c7fa11e7c509564bf5111d751890e7b1aa285d83285a82d238",
    "guard_pipeline_ownership": "65d2b3e6182dbae58b6f627117c41843c8b7715b5e836ff1e4bb7bd2655f85be",
    "guard_pipeline_runtime_authority": "9245f105304dce25bde6977e4f62eb6e88a1d98b0cff5fdb059fe8a6ff9a4588",
    "guard_pipeline_runtime_instances": "bb446aafcd431b409f1038c2da1bfcf8479baed577d12734ab1cbaf36e924d47",
    "reject_audit_events_mutation": "5ba2612faea4adf49b92395f87102f166df17b65aa64bf3f42ab5172bf375c5b",
    "reject_pipeline_command_receipts_mutation": "2a5ebd74102b1adf35afc2bf49d0a2317b867c5f57c7d0080361826f28b97f16",
    "reject_pipeline_folder_scopes_mutation": "c5e8b59c56e648a32713943ea36e8e7d2aca7a04916aebe04cf19a959eeda706",
    "reject_pipeline_initializations_mutation": "d74a5146da3ed09d01bde3725343ee536f64d6ca151b79fa0be802fd30d5bbea",
    "reject_pipeline_runtime_capabilities_mutation": "4f451f9f20e5538a7bd18117b7cd207474350055e2baa20f9595f38f11b20461",
    "reject_tier1_decisions_mutation": "a9b3145a55a04ab0e51e70ebaae7fe1c90d3ca72f2ca38d4b9f41999f9dfd357",
}
TRIGGER_FUNCTION_SEARCH_PATH: Final = {
    "guard_emails_runtime_identity": "pg_catalog",
    "guard_event_inbox_runtime_identity": "pg_catalog",
    "guard_pipeline_ownership": "target_schema",
    "guard_pipeline_runtime_authority": "pg_catalog",
    "guard_pipeline_runtime_instances": "pg_catalog",
    "reject_audit_events_mutation": "target_schema",
    "reject_pipeline_command_receipts_mutation": "target_schema",
    "reject_pipeline_folder_scopes_mutation": "pg_catalog",
    "reject_pipeline_initializations_mutation": "pg_catalog",
    "reject_pipeline_runtime_capabilities_mutation": "pg_catalog",
    "reject_tier1_decisions_mutation": "target_schema",
}
TRIGGER_FUNCTIONS: Final = tuple(sorted(TRIGGER_FUNCTION_SOURCE_SHA256))
TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION: Final = {
    DATABASE_REVISION: TRIGGER_FUNCTION_SOURCE_SHA256
}
TRIGGER_FUNCTION_SEARCH_PATH_BY_REVISION: Final = {
    DATABASE_REVISION: TRIGGER_FUNCTION_SEARCH_PATH
}
TRIGGER_FUNCTIONS_BY_REVISION: Final = {DATABASE_REVISION: TRIGGER_FUNCTIONS}
