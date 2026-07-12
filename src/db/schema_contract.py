"""Read-only provenance checks for application and checkpoint column types."""

from __future__ import annotations

from typing import Final

import psycopg


class DatabaseSchemaContractError(RuntimeError):
    """Raised when deployed columns do not match the trusted type contract."""


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

_COLUMN_TYPE_QUERY: Final = """
SELECT
    relation.relname::pg_catalog.text,
    attribute.attname::pg_catalog.text,
    type_schema.nspname::pg_catalog.text,
    column_type.typname::pg_catalog.text
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS relation_schema
  ON relation_schema.oid = relation.relnamespace
JOIN pg_catalog.pg_attribute AS attribute
  ON attribute.attrelid = relation.oid
JOIN pg_catalog.pg_type AS column_type
  ON column_type.oid = attribute.atttypid
JOIN pg_catalog.pg_namespace AS type_schema
  ON type_schema.oid = column_type.typnamespace
WHERE relation_schema.nspname = %s
  AND relation.relname = ANY(%s::pg_catalog.text[])
  AND attribute.attnum > 0
  AND NOT attribute.attisdropped
"""


def _invalid_contract() -> DatabaseSchemaContractError:
    return DatabaseSchemaContractError("database_schema_contract_invalid")


async def require_database_schema_contract(
    dsn: str,
    *,
    target_schema: str,
    require_complete: bool,
) -> None:
    """Reject shadowed built-in types and, after DDL, missing known columns."""

    relation_names = sorted({key[0] for key in _EXPECTED_COLUMN_TYPES})
    try:
        async with await psycopg.AsyncConnection.connect(
            dsn,
            autocommit=True,
        ) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SET search_path TO pg_catalog")
                await cursor.execute(
                    _COLUMN_TYPE_QUERY,
                    (target_schema, relation_names),
                )
                rows = await cursor.fetchall()
    except Exception:
        raise _invalid_contract() from None

    actual = {
        (relation_name, column_name): (type_schema, type_name)
        for relation_name, column_name, type_schema, type_name in rows
    }
    for column, expected_type in _EXPECTED_COLUMN_TYPES.items():
        deployed_type = actual.get(column)
        if deployed_type is None:
            if require_complete:
                raise _invalid_contract()
            continue
        if deployed_type != ("pg_catalog", expected_type):
            raise _invalid_contract()
