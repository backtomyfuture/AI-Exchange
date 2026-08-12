"""Fail-closed catalog checks for the one greenfield database baseline.

The former contract encoded every intermediate migration as a separate public
runtime profile.  This project deliberately no longer upgrades historical
application databases, so the runtime only accepts the clean polling schema.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

import psycopg
from psycopg import sql


GREENFIELD_DATABASE_REVISION: Final = "20260808_0001"


class DatabaseSchemaContractError(RuntimeError):
    """Raised for an incomplete, drifted, or historical application schema."""


_BUSINESS_RELATIONS: Final[dict[str, str]] = {
    "alembic_version": "r",
    "app_kv_store": "r",
    "audit_events": "r",
    "daily_digest_executions": "r",
    "emails": "r",
    "emails_log": "r",
    "event_inbox": "r",
    "intake_decisions": "r",
    "intake_releases": "r",
    "handoff_runs": "r",
    "execution_payload_revisions": "r",
    "approved_execution_envelopes": "r",
    "handoff_executions": "r",
    "pipeline_command_receipts": "r",
    "pipeline_folder_scopes": "r",
    "pipeline_initializations": "r",
    "pipeline_ownership": "r",
    "pipeline_runtime_authority": "r",
    "pipeline_runtime_capabilities": "r",
    "pipeline_runtime_instances": "r",
    "processed_emails": "v",
    "route_evaluation_traces": "r",
    "sync_cursors": "r",
    "tier1_decisions": "r",
}
_CHECKPOINT_RELATIONS: Final[dict[str, str]] = {
    "checkpoint_blobs": "r",
    "checkpoint_migrations": "r",
    "checkpoint_writes": "r",
    "checkpoints": "r",
}
_RETIRED_RELATIONS: Final = frozenset({"cold_start_command_receipts", "sync_cold_start_plans"})
_RETIRED_COLUMNS: Final = frozenset(
    {
        ("pipeline_folder_scopes", "webhook_ids"),
        ("sync_cursors", "cold_start_plan_id"),
        ("sync_cursors", "cold_start_plan_state"),
        ("sync_cursors", "retry_after_at"),
        ("sync_cursors", "transient_failures"),
    }
)
_RETIRED_ROUTINES: Final = frozenset({"greenfield_insert_webhook_event"})
_REQUIRED_ROUTINES: Final = frozenset(
    {
        "greenfield_apply_email_event",
        "greenfield_begin_inbox_effect",
        "greenfield_claim_inbox",
        "greenfield_commit_sync_page",
        "greenfield_drain_web_instance",
        "greenfield_fail_inbox",
        "greenfield_finish_inbox",
        "greenfield_get_runtime_authority",
        "greenfield_heartbeat_web_instance",
        "greenfield_initialize_runtime",
        "greenfield_pause_runtime",
        "greenfield_reap_inbox",
        "greenfield_register_web_instance",
        "greenfield_renew_inbox",
        "greenfield_requeue_inbox",
        "greenfield_resume_ingress",
    }
)

# These are the fields the application reads or writes directly.  Function-only
# data-plane mutations are additionally guarded by the role contract.
_REQUIRED_COLUMN_TYPES: Final[dict[tuple[str, str], str]] = {
    ("alembic_version", "version_num"): "varchar",
    ("app_kv_store", "key"): "text",
    ("app_kv_store", "value"): "text",
    ("audit_events", "account_id"): "int8",
    ("audit_events", "event_key"): "bpchar",
    ("audit_events", "safe_metadata"): "jsonb",
    ("checkpoint_migrations", "v"): "int4",
    ("daily_digest_executions", "account_id"): "int8",
    ("daily_digest_executions", "delivery_parts"): "jsonb",
    ("daily_digest_executions", "delivery_scope_hash"): "bpchar",
    ("daily_digest_executions", "state"): "text",
    ("daily_digest_executions", "window_end"): "timestamptz",
    ("daily_digest_executions", "window_start"): "timestamptz",
    ("emails", "account_id"): "int8",
    ("emails", "content_ref"): "jsonb",
    ("emails", "external_email_id"): "text",
    ("emails", "id"): "uuid",
    ("emails", "processing_inbox_id"): "uuid",
    ("emails", "status"): "text",
    ("emails", "version"): "int8",
    ("emails_log", "id"): "text",
    ("emails_log", "status"): "text",
    ("emails_log", "updated_at"): "timestamp",
    ("event_inbox", "account_id"): "int8",
    ("event_inbox", "authority_epoch"): "int8",
    ("event_inbox", "capability_hash"): "bpchar",
    ("event_inbox", "change_kind"): "text",
    ("event_inbox", "dedupe_key"): "bpchar",
    ("event_inbox", "execution_epoch"): "int8",
    ("event_inbox", "external_email_id"): "text",
    ("event_inbox", "id"): "uuid",
    ("event_inbox", "payload"): "jsonb",
    ("event_inbox", "processing_policy"): "text",
    ("event_inbox", "source"): "text",
    ("event_inbox", "status"): "text",
    ("intake_decisions", "inbox_id"): "uuid",
    ("intake_decisions", "execution_epoch"): "int8",
    ("intake_decisions", "decision_json"): "jsonb",
    ("intake_decisions", "decision_digest"): "bpchar",
    ("intake_decisions", "disposition"): "text",
    ("intake_decisions", "snapshot_digest"): "bpchar",
    ("intake_releases", "inbox_id"): "uuid",
    ("intake_releases", "new_execution_epoch"): "int8",
    ("handoff_runs", "inbox_id"): "uuid",
    ("handoff_runs", "decision_digest"): "bpchar",
    ("handoff_runs", "plan_json"): "jsonb",
    ("handoff_runs", "plan_digest"): "bpchar",
    ("handoff_runs", "evidence_json"): "jsonb",
    ("handoff_runs", "evidence_digest"): "bpchar",
    ("handoff_runs", "state"): "text",
    ("handoff_runs", "payload_revision"): "int8",
    ("handoff_runs", "version"): "int8",
    ("execution_payload_revisions", "inbox_id"): "uuid",
    ("execution_payload_revisions", "revision"): "int8",
    ("execution_payload_revisions", "payload_digest"): "bpchar",
    ("execution_payload_revisions", "draft_digest"): "bpchar",
    ("execution_payload_revisions", "draft_ref"): "jsonb",
    ("approved_execution_envelopes", "inbox_id"): "uuid",
    ("approved_execution_envelopes", "payload_revision"): "int8",
    ("approved_execution_envelopes", "payload_digest"): "bpchar",
    ("approved_execution_envelopes", "envelope_json"): "jsonb",
    ("approved_execution_envelopes", "envelope_digest"): "bpchar",
    ("handoff_executions", "decision_digest"): "bpchar",
    ("handoff_executions", "inbox_id"): "uuid",
    ("handoff_executions", "state"): "text",
    ("handoff_executions", "version"): "int8",
    ("pipeline_command_receipts", "authority_epoch"): "int8",
    ("pipeline_command_receipts", "canonical_payload_hash"): "bpchar",
    ("pipeline_command_receipts", "command_name"): "text",
    ("pipeline_command_receipts", "id"): "uuid",
    ("pipeline_folder_scopes", "event_policy_matrix"): "jsonb",
    ("pipeline_folder_scopes", "initialization_id"): "uuid",
    ("pipeline_folder_scopes", "scope_hash"): "bpchar",
    ("pipeline_folder_scopes", "sync_folder"): "text",
    ("pipeline_initializations", "capability_hash"): "bpchar",
    ("pipeline_initializations", "initialization_id"): "uuid",
    ("pipeline_ownership", "account_id"): "int8",
    ("pipeline_ownership", "fencing_token"): "int8",
    ("pipeline_ownership", "generation"): "int8",
    ("pipeline_runtime_authority", "account_id"): "int8",
    ("pipeline_runtime_authority", "capability_hash"): "bpchar",
    ("pipeline_runtime_authority", "initialization_id"): "uuid",
    ("pipeline_runtime_authority", "state"): "text",
    ("pipeline_runtime_capabilities", "capability_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "policy_manifest_hash"): "bpchar",
    ("pipeline_runtime_capabilities", "schema_revision"): "text",
    ("pipeline_runtime_instances", "lease_until"): "timestamptz",
    ("pipeline_runtime_instances", "session_id"): "uuid",
    ("pipeline_runtime_instances", "workload"): "text",
    ("route_evaluation_traces", "candidate_routes"): "jsonb",
    ("route_evaluation_traces", "confidence"): "float8",
    ("route_evaluation_traces", "continue_reason"): "text",
    ("route_evaluation_traces", "created_at"): "timestamptz",
    ("route_evaluation_traces", "evidence_refs"): "jsonb",
    ("route_evaluation_traces", "finished_at"): "timestamptz",
    ("route_evaluation_traces", "inbox_id"): "uuid",
    ("route_evaluation_traces", "matched_rule_ids"): "jsonb",
    ("route_evaluation_traces", "outcome"): "text",
    ("route_evaluation_traces", "safe_detail_json"): "jsonb",
    ("route_evaluation_traces", "safe_reason"): "text",
    ("route_evaluation_traces", "sequence"): "int8",
    ("route_evaluation_traces", "started_at"): "timestamptz",
    ("route_evaluation_traces", "tier"): "text",
    ("sync_cursors", "account_id"): "int8",
    ("sync_cursors", "cursor"): "text",
    ("sync_cursors", "folder_key"): "text",
    ("sync_cursors", "status"): "text",
    ("sync_cursors", "version"): "int8",
    ("tier1_decisions", "decision_digest"): "bpchar",
    ("tier1_decisions", "decision_json"): "jsonb",
    ("tier1_decisions", "inbox_id"): "uuid",
    ("tier1_decisions", "route"): "text",
}


def _invalid_contract() -> DatabaseSchemaContractError:
    return DatabaseSchemaContractError("database_schema_contract_invalid")


async def _rows(
    cursor: psycopg.AsyncCursor,
    statement: str | sql.Composed,
    params: Iterable[object] = (),
) -> list[tuple[object, ...]]:
    await cursor.execute(statement, tuple(params))
    return list(await cursor.fetchall())


async def require_database_schema_contract(
    dsn: str,
    *,
    target_schema: str,
    require_complete: bool,
    require_business_complete: bool = False,
    expected_revision: str | None = None,
) -> None:
    """Accept only an empty database or the complete polling-only baseline."""

    if expected_revision not in {None, GREENFIELD_DATABASE_REVISION}:
        raise _invalid_contract()
    try:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            async with conn.cursor() as cursor:
                relations = await _rows(
                    cursor,
                    "SELECT relation.relname::text, relation.relkind::text "
                    "FROM pg_catalog.pg_class AS relation "
                    "JOIN pg_catalog.pg_namespace AS namespace "
                    "ON namespace.oid = relation.relnamespace "
                    "WHERE namespace.nspname = %s "
                    "AND relation.relkind IN ('r', 'v')",
                    (target_schema,),
                )
                relation_kinds = {str(name): str(kind) for name, kind in relations}
                # psycopg adapts tuples as composite records, not PostgreSQL
                # arrays.  This query explicitly requests ``text[]``.
                names = sorted({relation for relation, _column in _REQUIRED_COLUMN_TYPES})
                columns = await _rows(
                    cursor,
                    "SELECT relation.relname::text, attribute.attname::text, "
                    "type_namespace.nspname::text, type_relation.typname::text "
                    "FROM pg_catalog.pg_attribute AS attribute "
                    "JOIN pg_catalog.pg_class AS relation "
                    "ON relation.oid = attribute.attrelid "
                    "JOIN pg_catalog.pg_namespace AS namespace "
                    "ON namespace.oid = relation.relnamespace "
                    "JOIN pg_catalog.pg_type AS type_relation "
                    "ON type_relation.oid = attribute.atttypid "
                    "JOIN pg_catalog.pg_namespace AS type_namespace "
                    "ON type_namespace.oid = type_relation.typnamespace "
                    "WHERE namespace.nspname = %s "
                    "AND relation.relname = ANY(%s::text[]) "
                    "AND attribute.attnum > 0 AND NOT attribute.attisdropped",
                    (target_schema, names),
                )
                routines = await _rows(
                    cursor,
                    "SELECT routine.proname::text "
                    "FROM pg_catalog.pg_proc AS routine "
                    "JOIN pg_catalog.pg_namespace AS namespace "
                    "ON namespace.oid = routine.pronamespace "
                    "WHERE namespace.nspname = %s",
                    (target_schema,),
                )
                revision_rows: list[tuple[object, ...]] = []
                if relation_kinds.get("alembic_version") == "r":
                    revision_rows = await _rows(
                        cursor,
                        sql.SQL("SELECT version_num::text FROM {}.alembic_version").format(
                            sql.Identifier(target_schema)
                        ),
                    )
    except Exception:
        raise _invalid_contract() from None

    business_actual = {
        name: kind for name, kind in relation_kinds.items() if name in _BUSINESS_RELATIONS
    }
    business_complete = business_actual == _BUSINESS_RELATIONS
    if business_actual and not business_complete:
        raise _invalid_contract()
    if (require_business_complete or require_complete) and not business_complete:
        raise _invalid_contract()
    if require_complete:
        expected_all = {**_BUSINESS_RELATIONS, **_CHECKPOINT_RELATIONS}
        actual_all = {
            name: kind
            for name, kind in relation_kinds.items()
            if name in expected_all
        }
        if actual_all != expected_all:
            raise _invalid_contract()
    if any(name in relation_kinds for name in _RETIRED_RELATIONS):
        raise _invalid_contract()

    revision = {str(row[0]) for row in revision_rows}
    if business_complete and revision != {GREENFIELD_DATABASE_REVISION}:
        raise _invalid_contract()
    if expected_revision is not None and revision != {expected_revision}:
        raise _invalid_contract()
    if not business_complete:
        if relation_kinds or routines:
            raise _invalid_contract()
        return

    actual_columns = {
        (str(relation), str(column)): (str(schema), str(type_name))
        for relation, column, schema, type_name in columns
    }
    for field, expected_type in _REQUIRED_COLUMN_TYPES.items():
        if field[0] not in relation_kinds:
            continue
        if actual_columns.get(field) != ("pg_catalog", expected_type):
            raise _invalid_contract()
    if any(field in actual_columns for field in _RETIRED_COLUMNS):
        raise _invalid_contract()
    routine_names = {str(row[0]) for row in routines}
    if not _REQUIRED_ROUTINES.issubset(routine_names):
        raise _invalid_contract()
    if _RETIRED_ROUTINES & routine_names:
        raise _invalid_contract()
