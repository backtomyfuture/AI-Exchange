"""Read-only provenance checks for application and checkpoint column types."""

from __future__ import annotations

from typing import Final

import psycopg

from src.db.access_contract import (
    PHASE2_CHECK_CONSTRAINT_SHA256,
    PHASE2_CHECK_CONSTRAINT_SHA256_OVERRIDES_BY_REVISION,
    PHASE2_DEFAULT_EXPRESSIONS,
    PHASE2_INDEX_SPECS,
    PHASE2_RELATIONS,
    PHASE2_UNIQUE_CONSTRAINTS,
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

_RELATION_KIND_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    relation.relkind::pg_catalog.text,
    relation.relpersistence::pg_catalog.text,
    relation.relrowsecurity,
    relation.relforcerowsecurity,
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

    relation_names = sorted({key[0] for key in _EXPECTED_COLUMN_TYPES})
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
                await cursor.execute(
                    _COLUMN_TYPE_QUERY,
                    (target_schema, relation_names),
                )
                rows = await cursor.fetchall()
                await cursor.execute(
                    _CHECK_CONSTRAINT_QUERY,
                    (target_schema, list(PHASE2_RELATIONS)),
                )
                check_constraint_rows = await cursor.fetchall()
                await cursor.execute(
                    _UNIQUE_CONSTRAINT_QUERY,
                    (target_schema, list(PHASE2_RELATIONS)),
                )
                unique_constraint_rows = await cursor.fetchall()
                await cursor.execute(
                    _INDEX_QUERY,
                    (target_schema, list(PHASE2_RELATIONS)),
                )
                index_rows = await cursor.fetchall()
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
    deployed_phase2_relations = {
        relation_name
        for relation_name, _ in actual
        if relation_name in PHASE2_RELATIONS
    }
    if expected_revision == "20260710_0002":
        expected_phase2_relations: set[str] = set()
    elif expected_revision in {"20260710_0003", "20260713_0004"}:
        expected_phase2_relations = set(PHASE2_RELATIONS)
    elif expected_revision is None:
        if deployed_phase2_relations not in (set(), set(PHASE2_RELATIONS)):
            raise _invalid_contract()
        expected_phase2_relations = deployed_phase2_relations
    else:
        raise _invalid_contract()
    if deployed_phase2_relations != expected_phase2_relations:
        raise _invalid_contract()
    expected_relation_kinds = dict(_BASE_RELATION_KINDS)
    if expected_phase2_relations:
        expected_relation_kinds.update(_PHASE2_RELATION_KINDS)
    expected_relation_contract = {
        name: (
            relation_kind,
            "p",
            False,
            False,
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
            has_no_policies,
            access_method,
        )
        for (
            name,
            relation_kind,
            persistence,
            row_security_enabled,
            row_security_forced,
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
    expected_columns = {
        column
        for column in _EXPECTED_COLUMN_TYPES
        if expected_phase2_relations or column[0] not in PHASE2_RELATIONS
    }
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
    for column, expected_type in _EXPECTED_COLUMN_TYPES.items():
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
        if relation_name not in PHASE2_RELATIONS:
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
        if is_nullable is not (column in _PHASE2_NULLABLE_COLUMNS):
            raise _invalid_contract()
        if has_default is not (column in _PHASE2_DEFAULTED_COLUMNS):
            raise _invalid_contract()
        if default_expression != PHASE2_DEFAULT_EXPRESSIONS.get(column):
            raise _invalid_contract()
        if identity_kind or generated_kind:
            raise _invalid_contract()
        if uses_default_collation is not True:
            raise _invalid_contract()
        if uses_default_storage is not True:
            raise _invalid_contract()
        if compression:
            raise _invalid_contract()
        if expected_type == "bpchar" and type_modifier != 68:
            raise _invalid_contract()

    expected_checks = dict(PHASE2_CHECK_CONSTRAINT_SHA256)
    expected_checks.update(
        PHASE2_CHECK_CONSTRAINT_SHA256_OVERRIDES_BY_REVISION.get(
            expected_revision or "",
            {},
        )
    )
    if not expected_phase2_relations:
        expected_checks = {}
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
    expected_check_contract = {
        key: (source_sha256, True, False)
        for key, source_sha256 in expected_checks.items()
    }
    if actual_checks != expected_check_contract:
        raise _invalid_contract()

    expected_unique = (
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
            for spec in PHASE2_UNIQUE_CONSTRAINTS
        }
        if expected_phase2_relations
        else set()
    )
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

    expected_indexes = (
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
            for spec in PHASE2_INDEX_SPECS
        }
        if expected_phase2_relations
        else set()
    )
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
