"""Read-only PostgreSQL identity and privilege preflight checks."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, fields
from typing import Final, TypeVar

import psycopg
from psycopg.rows import dict_row

from src.db.access_contract import (
    AUDITOR_ROUTINE_EXECUTE_BY_REVISION,
    AUDITOR_RELATION_ACCESS,
    AUDITOR_RELATION_ACCESS_BY_REVISION,
    DATABASE_REVISION,
    FOREIGN_KEY_SPECS_BY_REVISION,
    MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION,
    MAINTENANCE_RELATION_ACCESS,
    MAINTENANCE_RELATION_ACCESS_BY_REVISION,
    POLLING_RELATIONS,
    POLLING_RELATIONS_BY_REVISION,
    POLLING_VIEW_SPECS_BY_REVISION,
    RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
    RUNTIME_RELATION_ACCESS,
    RUNTIME_RELATION_ACCESS_BY_REVISION,
    RelationAccess,
    RoutineAccess,
    SECURITY_DEFINER_ROUTINES_BY_REVISION,
    TRIGGER_FUNCTIONS_BY_REVISION,
    TRIGGER_FUNCTION_SEARCH_PATH_BY_REVISION,
    TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION,
    TRIGGER_SPECS_BY_REVISION,
)


_IDENTIFIER: Final = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")
logger = logging.getLogger(__name__)


def _unexpected_direct_grants_sql(
    role_oid: str,
    schema_oid: str,
    database_oid: str,
) -> str:
    """Build a fixed catalog predicate denying grants outside the allowlist."""

    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.relacl) AS grant_acl
        WHERE object.relnamespace IS DISTINCT FROM {schema_oid}
          AND grant_acl.grantee = {role_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_attribute AS attribute
        JOIN pg_catalog.pg_class AS object
          ON object.oid = attribute.attrelid
        CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) AS grant_acl
        WHERE object.relnamespace IS DISTINCT FROM {schema_oid}
          AND grant_acl.grantee = {role_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_proc AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.proacl) AS grant_acl
        WHERE object.pronamespace IS DISTINCT FROM {schema_oid}
          AND grant_acl.grantee = {role_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_type AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.typacl) AS grant_acl
        WHERE object.typnamespace IS DISTINCT FROM {schema_oid}
          AND grant_acl.grantee = {role_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_namespace AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.nspacl) AS grant_acl
        WHERE object.oid IS DISTINCT FROM {schema_oid}
          AND grant_acl.grantee = {role_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_database AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.datacl) AS grant_acl
        WHERE object.oid IS DISTINCT FROM {database_oid}
          AND grant_acl.grantee = {role_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_default_acl AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.defaclacl) AS grant_acl
        WHERE object.defaclrole IS DISTINCT FROM {role_oid}
          AND grant_acl.grantee = {role_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_language AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.lanacl) AS grant_acl
        WHERE grant_acl.grantee IN (0, {role_oid})
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_tablespace AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.spcacl) AS grant_acl
        WHERE grant_acl.grantee IN (0, {role_oid})
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_foreign_data_wrapper AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.fdwacl) AS grant_acl
        WHERE grant_acl.grantee IN (0, {role_oid})
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_foreign_server AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.srvacl) AS grant_acl
        WHERE grant_acl.grantee IN (0, {role_oid})
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_largeobject_metadata AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.lomacl) AS grant_acl
        WHERE grant_acl.grantee IN (0, {role_oid})
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_parameter_acl AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.paracl) AS grant_acl
        WHERE grant_acl.grantee IN (0, {role_oid})
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_user_mappings AS object
        WHERE object.umuser IN (0, {role_oid})
    )"""


def _unexpected_ownership_sql(
    role_oid: str,
    schema_oid: str,
    database_oid: str,
) -> str:
    """Build a predicate denying objects owned outside the target boundary."""

    return f"""NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace AS object
        WHERE object.nspowner = {role_oid}
          AND object.oid IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_class AS object
        WHERE object.relowner = {role_oid}
          AND object.relnamespace IS DISTINCT FROM {schema_oid}
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_namespace AS internal_schema
              WHERE internal_schema.oid = object.relnamespace
                AND internal_schema.nspname LIKE 'pg_toast%%'
          )
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_proc AS object
        WHERE object.proowner = {role_oid}
          AND object.pronamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_type AS object
        WHERE object.typowner = {role_oid}
          AND object.typnamespace IS DISTINCT FROM {schema_oid}
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_namespace AS internal_schema
              WHERE internal_schema.oid = object.typnamespace
                AND internal_schema.nspname LIKE 'pg_toast%%'
          )
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_operator AS object
        WHERE object.oprowner = {role_oid}
          AND object.oprnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_opclass AS object
        WHERE object.opcowner = {role_oid}
          AND object.opcnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_opfamily AS object
        WHERE object.opfowner = {role_oid}
          AND object.opfnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_collation AS object
        WHERE object.collowner = {role_oid}
          AND object.collnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_conversion AS object
        WHERE object.conowner = {role_oid}
          AND object.connamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_ts_config AS object
        WHERE object.cfgowner = {role_oid}
          AND object.cfgnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_ts_dict AS object
        WHERE object.dictowner = {role_oid}
          AND object.dictnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_statistic_ext AS object
        WHERE object.stxowner = {role_oid}
          AND object.stxnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_extension AS object
        WHERE object.extowner = {role_oid}
          AND object.extnamespace IS DISTINCT FROM {schema_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_database AS object
        WHERE object.datdba = {role_oid}
          AND object.oid IS DISTINCT FROM {database_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_language AS object
        WHERE object.lanowner = {role_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_tablespace AS object
        WHERE object.spcowner = {role_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_foreign_data_wrapper AS object
        WHERE object.fdwowner = {role_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_foreign_server AS object
        WHERE object.srvowner = {role_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_largeobject_metadata AS object
        WHERE object.lomowner = {role_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_publication AS object
        WHERE object.pubowner = {role_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_subscription AS object
        WHERE object.subowner = {role_oid}
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_event_trigger AS object
        WHERE object.evtowner = {role_oid}
    )"""


def _other_schema_create_denied_sql(role_oid: str, schema_oid: str) -> str:
    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace AS other_schema
        WHERE other_schema.oid IS DISTINCT FROM {schema_oid}
          AND pg_catalog.has_schema_privilege(
              {role_oid}, other_schema.oid, 'CREATE'
          )
    )"""


def _other_database_connect_denied_sql(
    role_oid: str,
    database_oid: str,
) -> str:
    """Confine each managed credential to the current database."""

    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_database AS other_database
        WHERE other_database.oid IS DISTINCT FROM {database_oid}
          AND other_database.datallowconn
          AND pg_catalog.has_database_privilege(
              {role_oid}, other_database.oid, 'CONNECT'
          )
    )"""


def _other_user_schema_usage_denied_sql(
    role_oid: str,
    schema_oid: str,
) -> str:
    """Deny effective access through PUBLIC to non-system peer schemas."""

    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace AS other_schema
        WHERE other_schema.oid IS DISTINCT FROM {schema_oid}
          AND other_schema.nspname <> 'information_schema'
          AND other_schema.nspname NOT LIKE 'pg_%%'
          AND pg_catalog.has_schema_privilege(
              {role_oid}, other_schema.oid, 'USAGE'
          )
    )"""


def _target_acl_exclusive_sql(
    migration_oid: str,
    runtime_oid: str,
    maintenance_oid: str,
    auditor_oid: str,
    schema_oid: str,
    database_oid: str,
) -> str:
    """Allow target ACLs only for managed roles and the configured auditor."""

    allowed = f"({migration_oid}, {runtime_oid}, {maintenance_oid})"
    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_database AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                object.datacl,
                pg_catalog.acldefault('d', object.datdba)
            )
        ) AS target_acl
        WHERE object.oid = {database_oid}
          AND target_acl.grantee NOT IN {allowed}
          AND target_acl.grantee IS DISTINCT FROM {auditor_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_namespace AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                object.nspacl,
                pg_catalog.acldefault('n', object.nspowner)
            )
        ) AS target_acl
        WHERE object.oid = {schema_oid}
          AND target_acl.grantee NOT IN {allowed}
          AND target_acl.grantee IS DISTINCT FROM {auditor_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_class AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                object.relacl,
                pg_catalog.acldefault(
                    CASE
                        WHEN object.relkind = 'S' THEN 'S'
                        ELSE 'r'
                    END::pg_catalog."char",
                    object.relowner
                )
            )
        ) AS target_acl
        WHERE object.relnamespace = {schema_oid}
          AND target_acl.grantee NOT IN {allowed}
          AND target_acl.grantee IS DISTINCT FROM {auditor_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_attribute AS attribute
        JOIN pg_catalog.pg_class AS object
          ON object.oid = attribute.attrelid
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            attribute.attacl
        ) AS target_acl
        WHERE object.relnamespace = {schema_oid}
          AND target_acl.grantee NOT IN {allowed}
          AND target_acl.grantee IS DISTINCT FROM {auditor_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_proc AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                object.proacl,
                pg_catalog.acldefault('f', object.proowner)
            )
        ) AS target_acl
        WHERE object.pronamespace = {schema_oid}
          AND target_acl.grantee NOT IN {allowed}
          AND target_acl.grantee IS DISTINCT FROM {auditor_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_type AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                object.typacl,
                pg_catalog.acldefault('T', object.typowner)
            )
        ) AS target_acl
        WHERE object.typnamespace = {schema_oid}
          AND object.typrelid = 0
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_type AS element_type
              WHERE element_type.typarray = object.oid
          )
          AND target_acl.grantee NOT IN {allowed}
          AND target_acl.grantee IS DISTINCT FROM {auditor_oid}
    )"""


def _delegation_denied_sql(
    role_oid: str,
    schema_oid: str,
    database_oid: str,
) -> str:
    """Deny every grant option inside the target runtime allowlist."""

    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.relacl) AS grant_acl
        WHERE object.relnamespace = {schema_oid}
          AND grant_acl.grantee = {role_oid}
          AND grant_acl.is_grantable
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_attribute AS attribute
        JOIN pg_catalog.pg_class AS object
          ON object.oid = attribute.attrelid
        CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) AS grant_acl
        WHERE object.relnamespace = {schema_oid}
          AND grant_acl.grantee = {role_oid}
          AND grant_acl.is_grantable
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_proc AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.proacl) AS grant_acl
        WHERE object.pronamespace = {schema_oid}
          AND grant_acl.grantee = {role_oid}
          AND grant_acl.is_grantable
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_type AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.typacl) AS grant_acl
        WHERE object.typnamespace = {schema_oid}
          AND grant_acl.grantee = {role_oid}
          AND grant_acl.is_grantable
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_namespace AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.nspacl) AS grant_acl
        WHERE object.oid = {schema_oid}
          AND grant_acl.grantee = {role_oid}
          AND grant_acl.is_grantable
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_database AS object
        CROSS JOIN LATERAL pg_catalog.aclexplode(object.datacl) AS grant_acl
        WHERE object.oid = {database_oid}
          AND grant_acl.grantee = {role_oid}
          AND grant_acl.is_grantable
    )"""


def _sequence_update_denied_sql(role_oid: str, schema_oid: str) -> str:
    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS sequence
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                sequence.relacl,
                pg_catalog.acldefault('S', sequence.relowner)
            )
        ) AS sequence_acl
        WHERE sequence.relnamespace = {schema_oid}
          AND sequence.relkind = 'S'
          AND sequence_acl.privilege_type = 'UPDATE'
          AND sequence_acl.grantee IN (0, {role_oid})
    )"""


def _large_object_creation_denied_sql(role_oid: str) -> str:
    """Require revocation of every built-in large-object creation entrypoint."""

    return f"""(
        NOT pg_catalog.has_function_privilege(
            {role_oid},
            'pg_catalog.lo_creat(pg_catalog.int4)'::pg_catalog.regprocedure,
            'EXECUTE'
        )
        AND NOT pg_catalog.has_function_privilege(
            {role_oid},
            'pg_catalog.lo_create(pg_catalog.oid)'::pg_catalog.regprocedure,
            'EXECUTE'
        )
        AND NOT pg_catalog.has_function_privilege(
            {role_oid},
            'pg_catalog.lo_from_bytea(pg_catalog.oid,pg_catalog.bytea)'
                ::pg_catalog.regprocedure,
            'EXECUTE'
        )
        AND NOT pg_catalog.has_function_privilege(
            {role_oid},
            'pg_catalog.lo_import(pg_catalog.text)'::pg_catalog.regprocedure,
            'EXECUTE'
        )
        AND NOT pg_catalog.has_function_privilege(
            {role_oid},
            'pg_catalog.lo_import(pg_catalog.text,pg_catalog.oid)'
                ::pg_catalog.regprocedure,
            'EXECUTE'
        )
    )"""


def _sql_text_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_nullable_text_literal(value: str | None) -> str:
    if value is None:
        return "NULL::pg_catalog.text"
    return _sql_text_literal(value)


def _sql_text_array(values: tuple[str, ...]) -> str:
    members = ", ".join(_sql_text_literal(value) for value in values)
    return f"ARRAY[{members}]::pg_catalog.text[]"


def _baseline_relation_count_sql(schema_oid: str) -> str:
    relation_names = ", ".join(map(_sql_text_literal, POLLING_RELATIONS))
    return f"""(
        SELECT pg_catalog.count(*)
        FROM pg_catalog.pg_class AS business_relation
        WHERE business_relation.relnamespace = {schema_oid}
          AND business_relation.relname IN ({relation_names})
          AND business_relation.relkind = 'r'
    )"""


def _baseline_profile_matches_sql(schema_oid: str) -> str:
    """Match the one business relation set produced by the baseline."""

    actual_filter = ", ".join(map(_sql_text_literal, POLLING_RELATIONS))
    expected_rows = ", ".join(
        f"({_sql_text_literal(name)}, 'r')" for name in sorted(POLLING_RELATIONS)
    )
    return f"""(
        WITH actual_managed_relations(relation_name, relation_kind) AS (
            SELECT
                managed_relation.relname::pg_catalog.text,
                managed_relation.relkind::pg_catalog.text
            FROM pg_catalog.pg_class AS managed_relation
            WHERE managed_relation.relnamespace = {schema_oid}
              AND managed_relation.relname IN ({actual_filter})
        ),
        expected_managed_relations(relation_name, relation_kind) AS (
            VALUES {expected_rows}
        )
        SELECT NOT EXISTS (
            SELECT * FROM actual_managed_relations
            EXCEPT
            SELECT * FROM expected_managed_relations
        ) AND NOT EXISTS (
            SELECT * FROM expected_managed_relations
            EXCEPT
            SELECT * FROM actual_managed_relations
        )
    )"""


def _empty_baseline_profile_sql(schema_oid: str) -> str:
    relation_names = ", ".join(map(_sql_text_literal, POLLING_RELATIONS))
    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS business_relation
        WHERE business_relation.relnamespace = {schema_oid}
          AND business_relation.relname IN ({relation_names})
    )"""


_POLLING_SYNC_PAGE_IDENTITY_ARGUMENTS: Final = (
    "p_account_id bigint, p_session_id uuid, "
    "p_expected_lease_version bigint, p_folder_key text, "
    "p_expected_cursor text, p_expected_cursor_version bigint, "
    "p_next_cursor text, p_events jsonb, p_activation boolean"
)


def _polling_sync_function_exists_sql(schema_oid: str) -> str:
    """Prove the polling boundary without trusting runtime settings."""

    return f"""EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS routine
        WHERE routine.pronamespace = {schema_oid}
          AND routine.proname = 'greenfield_commit_sync_page'
          AND routine.prokind = 'f'
          AND pg_catalog.pg_get_function_identity_arguments(routine.oid)
              = {_sql_text_literal(_POLLING_SYNC_PAGE_IDENTITY_ARGUMENTS)}
    )"""


def _reviewed_baseline_matches_sql(schema_oid: str) -> str:
    """Match the complete polling database shape used by all identities."""

    return (
        "(" + _baseline_profile_matches_sql(schema_oid) + " AND "
        + _polling_sync_function_exists_sql(schema_oid) + ")"
    )


# The generic catalog predicates below remain revision-indexed so that their
# SQL is easy to audit against ``alembic_version``.  There is only one allowed
# revision; these narrow adapters deliberately do not reintroduce any old
# schema profile.
def _polling_relation_count_sql(schema_oid: str) -> str:
    return _baseline_relation_count_sql(schema_oid)


def _polling_profile_matches_sql(schema_oid: str, revision: str) -> str:
    if revision != DATABASE_REVISION:
        return "false"
    return (
        "(" + _empty_baseline_profile_sql(schema_oid) + " OR "
        + _baseline_profile_matches_sql(schema_oid) + ")"
    )


def _reviewed_profile_matches_sql(schema_oid: str, revision: str) -> str:
    if revision != DATABASE_REVISION:
        return "false"
    return _reviewed_baseline_matches_sql(schema_oid)


def _relation_access_contract_sql(
    schema_oid: str,
    role_oid: str,
    manifest: dict[str, RelationAccess],
    *,
    allow_missing: bool = False,
    manifests_by_revision: dict[str, dict[str, RelationAccess]] | None = None,
) -> str:
    def expected_values(access_manifest: dict[str, RelationAccess]) -> str:
        expected_rows: list[str] = []
        for relation_name, access in access_manifest.items():
            for privilege in access.table_privileges:
                expected_rows.append(
                    "("
                    + ", ".join(
                        (
                            "'table'",
                            _sql_text_literal(relation_name),
                            "''",
                            _sql_text_literal(privilege),
                            "false",
                        )
                    )
                    + ")"
                )
            if access.delete:
                expected_rows.append(
                    "("
                    + ", ".join(
                        (
                            "'table'",
                            _sql_text_literal(relation_name),
                            "''",
                            "'DELETE'",
                            "false",
                        )
                    )
                    + ")"
                )
            for privilege, columns in (
                ("SELECT", access.select_columns),
                ("INSERT", access.insert_columns),
                ("UPDATE", access.update_columns),
            ):
                expected_rows.extend(
                    "("
                    + ", ".join(
                        (
                            "'column'",
                            _sql_text_literal(relation_name),
                            _sql_text_literal(column),
                            _sql_text_literal(privilege),
                            "false",
                        )
                    )
                    + ")"
                    for column in columns
                )
        return ",\n".join(expected_rows)

    if manifests_by_revision is None:
        base_manifest = {
            name: access
            for name, access in manifest.items()
            if name not in POLLING_RELATIONS
        }
        legacy_manifest = manifest
        latest_manifest = manifest
        greenfield_manifest = manifest
        daily_digest_manifest = manifest
    else:
        base_manifest = manifests_by_revision[DATABASE_REVISION]
        legacy_manifest = manifests_by_revision[DATABASE_REVISION]
        latest_manifest = manifests_by_revision[DATABASE_REVISION]
        greenfield_manifest = manifests_by_revision.get(
            DATABASE_REVISION,
            manifest,
        )
        daily_digest_manifest = manifests_by_revision.get(
            DATABASE_REVISION,
            greenfield_manifest,
        )
    selected_difference = "unexpected" if allow_missing else "difference"
    return f"""(
        WITH actual_access(
            access_kind,
            relation_name,
            column_name,
            privilege_type,
            is_grantable
        ) AS (
            SELECT
                'table'::pg_catalog.text,
                relation.relname::pg_catalog.text,
                ''::pg_catalog.text,
                grant_acl.privilege_type::pg_catalog.text,
                grant_acl.is_grantable
            FROM pg_catalog.pg_class AS relation
            CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl)
                AS grant_acl
            WHERE relation.relnamespace = {schema_oid}
              AND grant_acl.grantee = {role_oid}
            UNION ALL
            SELECT
                'column'::pg_catalog.text,
                relation.relname::pg_catalog.text,
                attribute.attname::pg_catalog.text,
                grant_acl.privilege_type::pg_catalog.text,
                grant_acl.is_grantable
            FROM pg_catalog.pg_attribute AS attribute
            JOIN pg_catalog.pg_class AS relation
              ON relation.oid = attribute.attrelid
            CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl)
                AS grant_acl
            WHERE relation.relnamespace = {schema_oid}
              AND grant_acl.grantee = {role_oid}
        ),
        expected_access_0002(
            access_kind,
            relation_name,
            column_name,
            privilege_type,
            is_grantable
        ) AS (
            VALUES {expected_values(base_manifest)}
        ),
        expected_access_0004(
            access_kind,
            relation_name,
            column_name,
            privilege_type,
            is_grantable
        ) AS (
            VALUES {expected_values(legacy_manifest)}
        ),
        expected_access_0005(
            access_kind,
            relation_name,
            column_name,
            privilege_type,
            is_grantable
        ) AS (
            VALUES {expected_values(latest_manifest)}
        ),
        expected_access_0006(
            access_kind,
            relation_name,
            column_name,
            privilege_type,
            is_grantable
        ) AS (
            VALUES {expected_values(greenfield_manifest)}
        ),
        expected_access_0008(
            access_kind,
            relation_name,
            column_name,
            privilege_type,
            is_grantable
        ) AS (
            VALUES {expected_values(daily_digest_manifest)}
        ),
        difference_0002 AS (
            SELECT * FROM (
                SELECT * FROM actual_access
                EXCEPT
                SELECT * FROM expected_access_0002
            ) AS unexpected_access
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_access_0002
                EXCEPT
                SELECT * FROM actual_access
            ) AS missing_access
        ),
        unexpected_0002 AS (
            SELECT * FROM actual_access
            EXCEPT
            SELECT * FROM expected_access_0002
        ),
        difference_0004 AS (
            SELECT * FROM (
                SELECT * FROM actual_access
                EXCEPT
                SELECT * FROM expected_access_0004
            ) AS unexpected_access
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_access_0004
                EXCEPT
                SELECT * FROM actual_access
            ) AS missing_access
        ),
        unexpected_0004 AS (
            SELECT * FROM actual_access
            EXCEPT
            SELECT * FROM expected_access_0004
        ),
        difference_0005 AS (
            SELECT * FROM (
                SELECT * FROM actual_access
                EXCEPT
                SELECT * FROM expected_access_0005
            ) AS unexpected_access
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_access_0005
                EXCEPT
                SELECT * FROM actual_access
            ) AS missing_access
        ),
        unexpected_0005 AS (
            SELECT * FROM actual_access
            EXCEPT
            SELECT * FROM expected_access_0005
        ),
        difference_0006 AS (
            SELECT * FROM (
                SELECT * FROM actual_access
                EXCEPT
                SELECT * FROM expected_access_0006
            ) AS unexpected_access
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_access_0006
                EXCEPT
                SELECT * FROM actual_access
            ) AS missing_access
        ),
        unexpected_0006 AS (
            SELECT * FROM actual_access
            EXCEPT
            SELECT * FROM expected_access_0006
        ),
        difference_0008 AS (
            SELECT * FROM (
                SELECT * FROM actual_access
                EXCEPT
                SELECT * FROM expected_access_0008
            ) AS unexpected_access
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_access_0008
                EXCEPT
                SELECT * FROM actual_access
            ) AS missing_access
        ),
        unexpected_0008 AS (
            SELECT * FROM actual_access
            EXCEPT
            SELECT * FROM expected_access_0008
        )
        SELECT CASE
            WHEN {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
             AND NOT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class AS revision_relation
                 WHERE revision_relation.relnamespace = {schema_oid}
                   AND revision_relation.relname = 'alembic_version'
                   AND revision_relation.relkind = 'r'
             ) THEN NOT EXISTS (SELECT 1 FROM actual_access)
            WHEN {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
             AND EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class AS revision_relation
                 WHERE revision_relation.relnamespace = {schema_oid}
                   AND revision_relation.relname = 'alembic_version'
                   AND revision_relation.relkind = 'r'
             ) THEN NOT EXISTS (SELECT 1 FROM {selected_difference}_0002)
            WHEN {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
                THEN NOT EXISTS (SELECT 1 FROM {selected_difference}_0004)
            WHEN {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
                THEN NOT EXISTS (SELECT 1 FROM {selected_difference}_0005)
            WHEN {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
                THEN NOT EXISTS (SELECT 1 FROM {selected_difference}_0006)
            WHEN {_reviewed_profile_matches_sql(schema_oid, DATABASE_REVISION)}
                THEN NOT EXISTS (SELECT 1 FROM {selected_difference}_0008)
            ELSE false
        END
    )"""


def _selected_access_revision_sql(
    schema_oid: str,
    revisions: tuple[str, ...],
) -> str:
    """Resolve only one reviewed structural profile without trusting settings."""

    clauses = "\n".join(
        "WHEN "
        + _reviewed_profile_matches_sql(schema_oid, revision)
        + " THEN "
        + _sql_text_literal(revision)
        + "::pg_catalog.text"
        for revision in sorted(set(revisions), reverse=True)
        if revision in POLLING_RELATIONS_BY_REVISION
        and revision in POLLING_VIEW_SPECS_BY_REVISION
    )
    return f"CASE\n{clauses}\nELSE NULL::pg_catalog.text\nEND"


def _routine_manifest_rows_sql(
    manifests_by_revision: dict[str, tuple[RoutineAccess, ...]],
) -> str:
    rows = [
        "("
        + ", ".join(
            (
                _sql_text_literal(revision),
                _sql_text_literal(spec.name),
                _sql_text_literal(spec.identity_arguments),
            )
        )
        + ")"
        for revision, manifest in manifests_by_revision.items()
        for spec in manifest
    ]
    if rows:
        return "VALUES " + ",\n".join(rows)
    return (
        "SELECT NULL::pg_catalog.text, NULL::pg_catalog.text, "
        "NULL::pg_catalog.text WHERE false"
    )


def _routine_execute_contract_sql(
    schema_oid: str,
    role_oid: str,
    manifests_by_revision: dict[str, tuple[RoutineAccess, ...]],
    *,
    allow_missing: bool = False,
) -> str:
    """Require one exact effective EXECUTE set keyed by identity arguments."""

    selected_difference = "unexpected_access" if allow_missing else "difference"
    return f"""(
        WITH selected_revision(revision) AS (
            SELECT {
        _selected_access_revision_sql(
            schema_oid,
            tuple(manifests_by_revision),
        )
    }
        ),
        expected_by_revision(
            revision,
            routine_name,
            identity_arguments
        ) AS (
            {_routine_manifest_rows_sql(manifests_by_revision)}
        ),
        expected_access(routine_name, identity_arguments) AS (
            SELECT routine_name, identity_arguments
            FROM expected_by_revision
            WHERE revision = (SELECT revision FROM selected_revision)
        ),
        actual_access(routine_name, identity_arguments) AS (
            SELECT
                routine.proname::pg_catalog.text,
                pg_catalog.pg_get_function_identity_arguments(routine.oid)
                    ::pg_catalog.text
            FROM pg_catalog.pg_proc AS routine
            WHERE routine.pronamespace = {schema_oid}
              AND pg_catalog.has_function_privilege(
                  {role_oid}, routine.oid, 'EXECUTE'
              )
        ),
        difference AS (
            SELECT * FROM (
                SELECT * FROM actual_access EXCEPT SELECT * FROM expected_access
            ) AS unexpected_execute
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_access EXCEPT SELECT * FROM actual_access
            ) AS missing_execute
        ),
        unexpected_access AS (
            SELECT * FROM actual_access
            EXCEPT
            SELECT * FROM expected_access
        )
        SELECT (
            (
                (SELECT revision IS NOT NULL FROM selected_revision)
                AND NOT EXISTS (SELECT 1 FROM {selected_difference})
            )
            OR (
                {_empty_baseline_profile_sql(schema_oid)}
                AND NOT EXISTS (SELECT 1 FROM actual_access)
            )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS routine
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    routine.proacl,
                    pg_catalog.acldefault('f', routine.proowner)
                )
            ) AS routine_acl
            WHERE routine.pronamespace = {schema_oid}
              AND routine_acl.privilege_type = 'EXECUTE'
              AND (
                  routine_acl.grantee = 0
                  OR (
                      routine_acl.grantee = {role_oid}
                      AND routine_acl.is_grantable
                  )
              )
        )
    )"""


def _security_definer_contract_sql(
    schema_oid: str,
    migration_oid: str,
) -> str:
    """Allow only the revision's fixed migration-owned greenfield routines."""

    manifests = SECURITY_DEFINER_ROUTINES_BY_REVISION

    return f"""(
        WITH selected_revision(revision) AS (
            SELECT {
        _selected_access_revision_sql(
            schema_oid,
            tuple(manifests),
        )
    }
        ),
        expected_by_revision(
            revision,
            routine_name,
            identity_arguments
        ) AS (
            {_routine_manifest_rows_sql(manifests)}
        ),
        expected_functions(routine_name, identity_arguments) AS (
            SELECT routine_name, identity_arguments
            FROM expected_by_revision
            WHERE revision = (SELECT revision FROM selected_revision)
        ),
        actual_functions(routine_name, identity_arguments) AS (
            SELECT
                routine.proname::pg_catalog.text,
                pg_catalog.pg_get_function_identity_arguments(routine.oid)
                    ::pg_catalog.text
            FROM pg_catalog.pg_proc AS routine
            WHERE routine.pronamespace = {schema_oid}
              AND routine.prosecdef
        ),
        function_difference AS (
            SELECT * FROM (
                SELECT * FROM actual_functions
                EXCEPT
                SELECT * FROM expected_functions
            ) AS unexpected_function
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_functions
                EXCEPT
                SELECT * FROM actual_functions
            ) AS missing_function
        )
        SELECT (
            (
                (SELECT revision IS NOT NULL FROM selected_revision)
                AND NOT EXISTS (SELECT 1 FROM function_difference)
            )
            OR (
                {_empty_baseline_profile_sql(schema_oid)}
                AND NOT EXISTS (SELECT 1 FROM actual_functions)
            )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS routine
            WHERE routine.pronamespace = {schema_oid}
              AND routine.prosecdef
              AND (
                  routine.proowner IS DISTINCT FROM {migration_oid}
                  OR routine.prokind <> 'f'
                  OR routine.proconfig IS DISTINCT FROM
                     ARRAY['search_path=pg_catalog']::pg_catalog.text[]
              )
        )
    )"""


def _checkpoint_auditor_access_contract_sql(
    auditor_oid: str,
    schema_oid: str,
    database_oid: str,
    *,
    allow_missing: bool = False,
) -> str:
    """Require one non-delegable auditor with only plan-required column grants."""

    required_direct_access = (
        "true"
        if allow_missing
        else f"""(
        EXISTS (
            SELECT 1
            FROM pg_catalog.pg_database AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.datacl) AS grant_acl
            WHERE object.oid = {database_oid}
              AND grant_acl.grantee = {auditor_oid}
              AND grant_acl.privilege_type = 'CONNECT'
              AND NOT grant_acl.is_grantable
        )
        AND EXISTS (
            SELECT 1
            FROM pg_catalog.pg_namespace AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.nspacl) AS grant_acl
            WHERE object.oid = {schema_oid}
              AND grant_acl.grantee = {auditor_oid}
              AND grant_acl.privilege_type = 'USAGE'
              AND NOT grant_acl.is_grantable
        )
    )"""
    )
    return f"""(
        {auditor_oid} IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM pg_catalog.pg_roles AS auditor
            WHERE auditor.oid = {auditor_oid}
              AND auditor.rolcanlogin
              AND NOT auditor.rolsuper
              AND NOT auditor.rolcreatedb
              AND NOT auditor.rolcreaterole
              AND NOT auditor.rolreplication
              AND NOT auditor.rolbypassrls
              AND NOT auditor.rolinherit
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_auth_members AS membership
            WHERE membership.member = {auditor_oid}
               OR membership.roleid = {auditor_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_database AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.datacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
              AND NOT (
                  object.oid = {database_oid}
                  AND grant_acl.privilege_type = 'CONNECT'
                  AND NOT grant_acl.is_grantable
              )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_namespace AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.nspacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
              AND NOT (
                  object.oid = {schema_oid}
                  AND grant_acl.privilege_type = 'USAGE'
                  AND NOT grant_acl.is_grantable
              )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.relacl) AS grant_acl
            WHERE object.relnamespace IS DISTINCT FROM {schema_oid}
              AND grant_acl.grantee = {auditor_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_attribute AS attribute
            JOIN pg_catalog.pg_class AS object
              ON object.oid = attribute.attrelid
            CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) AS grant_acl
            WHERE object.relnamespace IS DISTINCT FROM {schema_oid}
              AND grant_acl.grantee = {auditor_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.proacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
            UNION ALL
            SELECT 1
            FROM pg_catalog.pg_type AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.typacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_largeobject_metadata AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.lomacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
            UNION ALL
            SELECT 1
            FROM pg_catalog.pg_language AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.lanacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
            UNION ALL
            SELECT 1
            FROM pg_catalog.pg_tablespace AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.spcacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
            UNION ALL
            SELECT 1
            FROM pg_catalog.pg_foreign_data_wrapper AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.fdwacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
            UNION ALL
            SELECT 1
            FROM pg_catalog.pg_foreign_server AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.srvacl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
            UNION ALL
            SELECT 1
            FROM pg_catalog.pg_parameter_acl AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.paracl) AS grant_acl
            WHERE grant_acl.grantee = {auditor_oid}
            UNION ALL
            SELECT 1
            FROM pg_catalog.pg_user_mappings AS object
            WHERE object.umuser = {auditor_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_default_acl AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.defaclacl) AS grant_acl
            WHERE object.defaclrole = {auditor_oid}
               OR grant_acl.grantee = {auditor_oid}
        )
        AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_namespace AS object
            WHERE object.nspowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_class AS object
            WHERE object.relowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_proc AS object
            WHERE object.proowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_type AS object
            WHERE object.typowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_database AS object
            WHERE object.datdba = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_operator AS object
            WHERE object.oprowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_opclass AS object
            WHERE object.opcowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_opfamily AS object
            WHERE object.opfowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_collation AS object
            WHERE object.collowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_conversion AS object
            WHERE object.conowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_ts_config AS object
            WHERE object.cfgowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_ts_dict AS object
            WHERE object.dictowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_statistic_ext AS object
            WHERE object.stxowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_extension AS object
            WHERE object.extowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_language AS object
            WHERE object.lanowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_tablespace AS object
            WHERE object.spcowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_foreign_data_wrapper AS object
            WHERE object.fdwowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_foreign_server AS object
            WHERE object.srvowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_largeobject_metadata AS object
            WHERE object.lomowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_publication AS object
            WHERE object.pubowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_subscription AS object
            WHERE object.subowner = {auditor_oid}
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_event_trigger AS object
            WHERE object.evtowner = {auditor_oid}
        )
        AND {_other_database_connect_denied_sql(auditor_oid, database_oid)}
        AND {_other_schema_create_denied_sql(auditor_oid, schema_oid)}
        AND {_other_user_schema_usage_denied_sql(auditor_oid, schema_oid)}
        AND {_role_and_database_settings_absent_sql(auditor_oid, database_oid)}
        AND {_large_object_creation_denied_sql(auditor_oid)}
        AND NOT pg_catalog.has_database_privilege(
            {auditor_oid}, {database_oid}, 'CREATE'
        )
        AND NOT pg_catalog.has_database_privilege(
            {auditor_oid}, {database_oid}, 'TEMPORARY'
        )
        AND NOT pg_catalog.has_schema_privilege(
            {auditor_oid}, {schema_oid}, 'CREATE'
        )
        AND {required_direct_access}
        AND {
        _relation_access_contract_sql(
            schema_oid,
            auditor_oid,
            AUDITOR_RELATION_ACCESS,
            allow_missing=allow_missing,
            manifests_by_revision=AUDITOR_RELATION_ACCESS_BY_REVISION,
        )
    }
        AND {
        _routine_execute_contract_sql(
            schema_oid,
            auditor_oid,
            AUDITOR_ROUTINE_EXECUTE_BY_REVISION,
            allow_missing=allow_missing,
        )
    }
    )"""


def _target_foreign_keys_exact_for_specs_sql(
    schema_oid: str,
    specs: tuple,
) -> str:
    expected_rows = ",\n".join(
        "("
        + ", ".join(
            (
                _sql_text_literal(spec.name),
                f"{schema_oid}::pg_catalog.oid",
                _sql_text_literal(spec.child_relation),
                _sql_text_array(spec.child_columns),
                f"{schema_oid}::pg_catalog.oid",
                _sql_text_literal(spec.parent_relation),
                _sql_text_array(spec.parent_columns),
                _sql_text_literal(spec.match_type),
                _sql_text_literal(spec.update_action),
                _sql_text_literal(spec.delete_action),
                "true" if spec.deferrable else "false",
                "true" if spec.initially_deferred else "false",
                "true" if spec.validated else "false",
                "0::pg_catalog.oid",
            )
        )
        + ")"
        for spec in specs
    )
    return f"""(
        WITH actual_foreign_keys AS (
            SELECT
                foreign_key.oid,
                foreign_key.conname::pg_catalog.text AS constraint_name,
                child.relnamespace AS child_schema_oid,
                child.relname::pg_catalog.text AS child_relation,
                ARRAY(
                    SELECT attribute.attname::pg_catalog.text
                    FROM pg_catalog.unnest(foreign_key.conkey)
                         WITH ORDINALITY AS key_column(attnum, position)
                    JOIN pg_catalog.pg_attribute AS attribute
                      ON attribute.attrelid = child.oid
                     AND attribute.attnum = key_column.attnum
                    ORDER BY key_column.position
                ) AS child_columns,
                parent.relnamespace AS parent_schema_oid,
                parent.relname::pg_catalog.text AS parent_relation,
                ARRAY(
                    SELECT attribute.attname::pg_catalog.text
                    FROM pg_catalog.unnest(foreign_key.confkey)
                         WITH ORDINALITY AS key_column(attnum, position)
                    JOIN pg_catalog.pg_attribute AS attribute
                      ON attribute.attrelid = parent.oid
                     AND attribute.attnum = key_column.attnum
                    ORDER BY key_column.position
                ) AS parent_columns,
                foreign_key.confmatchtype::pg_catalog.text AS match_type,
                foreign_key.confupdtype::pg_catalog.text AS update_action,
                foreign_key.confdeltype::pg_catalog.text AS delete_action,
                foreign_key.condeferrable AS is_deferrable,
                foreign_key.condeferred AS is_deferred,
                foreign_key.convalidated AS is_validated,
                foreign_key.conparentid AS parent_constraint_oid
            FROM pg_catalog.pg_constraint AS foreign_key
            JOIN pg_catalog.pg_class AS child
              ON child.oid = foreign_key.conrelid
            JOIN pg_catalog.pg_class AS parent
              ON parent.oid = foreign_key.confrelid
            WHERE foreign_key.contype = 'f'
              AND (
                  child.relnamespace = {schema_oid}
                  OR parent.relnamespace = {schema_oid}
              )
        ),
        expected_foreign_keys(
            constraint_name,
            child_schema_oid,
            child_relation,
            child_columns,
            parent_schema_oid,
            parent_relation,
            parent_columns,
            match_type,
            update_action,
            delete_action,
            is_deferrable,
            is_deferred,
            is_validated,
            parent_constraint_oid
        ) AS (
            VALUES {expected_rows}
        ),
        difference AS (
            SELECT * FROM (
                SELECT
                    constraint_name, child_schema_oid,
                    child_relation, child_columns, parent_schema_oid,
                    parent_relation, parent_columns, match_type,
                    update_action, delete_action, is_deferrable,
                    is_deferred, is_validated, parent_constraint_oid
                FROM actual_foreign_keys
                EXCEPT
                SELECT * FROM expected_foreign_keys
            ) AS unexpected_foreign_keys
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_foreign_keys
                EXCEPT
                SELECT
                    constraint_name, child_schema_oid,
                    child_relation, child_columns, parent_schema_oid,
                    parent_relation, parent_columns, match_type,
                    update_action, delete_action, is_deferrable,
                    is_deferred, is_validated, parent_constraint_oid
                FROM actual_foreign_keys
            ) AS missing_foreign_keys
        )
        SELECT NOT EXISTS (SELECT 1 FROM difference)
    )"""


def _target_foreign_keys_exact_sql(schema_oid: str) -> str:
    no_foreign_keys = f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint AS foreign_key
        JOIN pg_catalog.pg_class AS child
          ON child.oid = foreign_key.conrelid
        JOIN pg_catalog.pg_class AS parent
          ON parent.oid = foreign_key.confrelid
        WHERE foreign_key.contype = 'f'
          AND (
              child.relnamespace = {schema_oid}
              OR parent.relnamespace = {schema_oid}
          )
    )"""
    revision_contracts = [
        f"""(
            {_polling_profile_matches_sql(schema_oid, revision)}
            AND {
            _target_foreign_keys_exact_for_specs_sql(
                schema_oid,
                specs,
            )
        }
        )"""
        for revision, specs in FOREIGN_KEY_SPECS_BY_REVISION.items()
        if revision in POLLING_RELATIONS_BY_REVISION
        and revision in POLLING_VIEW_SPECS_BY_REVISION
    ]
    no_foreign_key_contract = f"""(
        {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
        AND {no_foreign_keys}
    )"""
    return "(" + " OR ".join((no_foreign_key_contract, *revision_contracts)) + ")"


def _target_trigger_contract_exact_for_revision_sql(
    schema_oid: str,
    migration_oid: str,
    revision: str,
) -> str:
    trigger_specs = TRIGGER_SPECS_BY_REVISION[revision]
    trigger_functions = TRIGGER_FUNCTIONS_BY_REVISION[revision]
    trigger_function_digests = TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION[revision]
    trigger_function_search_paths = TRIGGER_FUNCTION_SEARCH_PATH_BY_REVISION[revision]
    if (
        set(trigger_function_digests) != set(trigger_functions)
        or set(trigger_function_search_paths) != set(trigger_functions)
        or not set(trigger_function_search_paths.values())
        <= {"target_schema", "pg_catalog"}
    ):
        raise RuntimeError("Trigger function contract manifest is invalid")
    expected_trigger_rows = ",\n".join(
        "("
        + ", ".join(
            (
                _sql_text_literal(spec.name),
                _sql_text_literal(spec.relation),
                _sql_text_literal(spec.function),
                f"{spec.trigger_type}::pg_catalog.int2",
                "true" if spec.is_constraint else "false",
                "'O'::pg_catalog.\"char\"",
                "true",
                "true",
                "0::pg_catalog.oid",
                "0::pg_catalog.oid",
                "0::pg_catalog.oid",
                "true" if spec.is_deferrable else "false",
                "true" if spec.is_initially_deferred else "false",
                f"{len(spec.arguments)}::pg_catalog.int2",
                _sql_text_literal(
                    " ".join(str(value) for value in spec.update_attribute_numbers)
                ),
                _sql_text_literal(
                    b"".join(
                        argument.encode("utf-8") + b"\x00"
                        for argument in spec.arguments
                    ).hex()
                ),
                _sql_nullable_text_literal(spec.when_clause_sha256),
                _sql_nullable_text_literal(spec.old_transition_table),
                _sql_nullable_text_literal(spec.new_transition_table),
                "true",
            )
        )
        + ")"
        for spec in trigger_specs
    )
    expected_function_rows = ",\n".join(
        "("
        + ", ".join(
            (
                _sql_text_literal(function_name),
                _sql_text_literal(trigger_function_digests[function_name]),
                (
                    "ARRAY['search_path=' || (SELECT schema.nspname "
                    "FROM pg_catalog.pg_namespace AS schema WHERE schema.oid = "
                    f"{schema_oid})]::pg_catalog.text[]"
                    if trigger_function_search_paths[function_name] == "target_schema"
                    else "ARRAY['search_path=pg_catalog']::pg_catalog.text[]"
                ),
            )
        )
        + ")"
        for function_name in trigger_functions
    )
    return f"""(
        WITH actual_user_triggers AS (
            SELECT
                trigger.tgname::pg_catalog.text AS trigger_name,
                relation.relname::pg_catalog.text AS relation_name,
                routine.proname::pg_catalog.text AS function_name,
                trigger.tgtype,
                trigger.tgconstraint <> 0 AS is_constraint,
                trigger.tgenabled,
                routine.pronamespace = {schema_oid}
                    AS function_in_target_schema,
                routine.proowner = {migration_oid}
                    AS function_owned_by_migration,
                trigger.tgparentid AS parent_trigger_oid,
                trigger.tgconstrrelid AS constraint_relation_oid,
                trigger.tgconstrindid AS constraint_index_oid,
                trigger.tgdeferrable AS is_deferrable,
                trigger.tginitdeferred AS is_initially_deferred,
                trigger.tgnargs AS argument_count,
                trigger.tgattr::pg_catalog.text AS update_attribute_numbers,
                pg_catalog.encode(trigger.tgargs, 'hex')::pg_catalog.text
                    AS arguments_hex,
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
                END AS when_clause_sha256,
                trigger.tgoldtable::pg_catalog.text AS old_transition_table,
                trigger.tgnewtable::pg_catalog.text AS new_transition_table,
                CASE
                    WHEN trigger.tgconstraint = 0 THEN true
                    ELSE (
                        trigger_constraint.oid IS NOT NULL
                        AND trigger_constraint.contype = 't'
                        AND trigger_constraint.conname = trigger.tgname
                        AND trigger_constraint.conrelid = trigger.tgrelid
                        AND trigger_constraint.condeferrable =
                            trigger.tgdeferrable
                        AND trigger_constraint.condeferred =
                            trigger.tginitdeferred
                        AND trigger_constraint.convalidated
                        AND trigger_constraint.connoinherit
                        AND trigger_constraint.conparentid = 0
                        AND trigger_constraint.coninhcount = 0
                        AND trigger_constraint.conislocal
                    )
                END AS constraint_metadata_exact
            FROM pg_catalog.pg_trigger AS trigger
            JOIN pg_catalog.pg_class AS relation
              ON relation.oid = trigger.tgrelid
            JOIN pg_catalog.pg_proc AS routine
              ON routine.oid = trigger.tgfoid
            LEFT JOIN pg_catalog.pg_constraint AS trigger_constraint
              ON trigger_constraint.oid = trigger.tgconstraint
            WHERE relation.relnamespace = {schema_oid}
              AND NOT trigger.tgisinternal
        ),
        expected_user_triggers(
            trigger_name,
            relation_name,
            function_name,
            tgtype,
            is_constraint,
            tgenabled,
            function_in_target_schema,
            function_owned_by_migration,
            parent_trigger_oid,
            constraint_relation_oid,
            constraint_index_oid,
            is_deferrable,
            is_initially_deferred,
            argument_count,
            update_attribute_numbers,
            arguments_hex,
            when_clause_sha256,
            old_transition_table,
            new_transition_table,
            constraint_metadata_exact
        ) AS (
            VALUES {expected_trigger_rows}
        ),
        trigger_difference AS (
            SELECT * FROM (
                SELECT * FROM actual_user_triggers
                EXCEPT
                SELECT * FROM expected_user_triggers
            ) AS unexpected_user_triggers
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_user_triggers
                EXCEPT
                SELECT * FROM actual_user_triggers
            ) AS missing_user_triggers
        ),
        actual_trigger_functions AS (
            SELECT
                routine.proname::pg_catalog.text AS function_name,
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(routine.prosrc, 'UTF8')
                    ),
                    'hex'
                )::pg_catalog.text AS source_sha256,
                routine.proconfig AS configuration
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_language AS language
              ON language.oid = routine.prolang
            WHERE routine.pronamespace = {schema_oid}
              AND routine.prorettype =
                  'pg_catalog.trigger'::pg_catalog.regtype
              AND routine.proowner = {migration_oid}
              AND language.lanname = 'plpgsql'
              AND NOT routine.prosecdef
              AND pg_catalog.pg_get_function_identity_arguments(routine.oid) = ''
        ),
        expected_trigger_functions(
            function_name,
            source_sha256,
            configuration
        ) AS (
            VALUES {expected_function_rows}
        ),
        unapproved_trigger_functions AS (
            SELECT 1
            FROM pg_catalog.pg_proc AS routine
            WHERE routine.pronamespace = {schema_oid}
              AND routine.prorettype =
                  'pg_catalog.trigger'::pg_catalog.regtype
              AND (
                  routine.proname NOT IN (
                      {", ".join(_sql_text_literal(name) for name in trigger_functions)}
                  )
                  OR pg_catalog.pg_get_function_identity_arguments(
                      routine.oid
                  ) <> ''
              )
        ),
        function_difference AS (
            SELECT * FROM (
                SELECT * FROM actual_trigger_functions
                EXCEPT
                SELECT * FROM expected_trigger_functions
            ) AS unexpected_trigger_functions
            UNION ALL
            SELECT * FROM (
                SELECT * FROM expected_trigger_functions
                EXCEPT
                SELECT * FROM actual_trigger_functions
            ) AS missing_trigger_functions
        )
        SELECT
            NOT EXISTS (SELECT 1 FROM trigger_difference)
            AND NOT EXISTS (SELECT 1 FROM function_difference)
            AND NOT EXISTS (SELECT 1 FROM unapproved_trigger_functions)
            AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_trigger AS internal_trigger
                JOIN pg_catalog.pg_class AS relation
                  ON relation.oid = internal_trigger.tgrelid
                LEFT JOIN pg_catalog.pg_constraint AS trigger_constraint
                  ON trigger_constraint.oid = internal_trigger.tgconstraint
                WHERE relation.relnamespace = {schema_oid}
                  AND internal_trigger.tgisinternal
                  AND (
                      trigger_constraint.contype IS DISTINCT FROM 'f'
                      OR trigger_constraint.conname NOT IN (
                          {", ".join(_sql_text_literal(spec.name) for spec in FOREIGN_KEY_SPECS_BY_REVISION[revision])}
                      )
                      OR internal_trigger.tgenabled <> 'O'
                  )
            )
    )"""


def _target_trigger_contract_exact_sql(
    schema_oid: str,
    migration_oid: str,
) -> str:
    no_trigger_hooks = f"""(
        NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_trigger AS trigger
            JOIN pg_catalog.pg_class AS relation
              ON relation.oid = trigger.tgrelid
            WHERE relation.relnamespace = {schema_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS routine
            WHERE routine.pronamespace = {schema_oid}
              AND routine.prorettype =
                  'pg_catalog.trigger'::pg_catalog.regtype
        )
    )"""
    revisions = (
        set(TRIGGER_SPECS_BY_REVISION)
        & set(TRIGGER_FUNCTIONS_BY_REVISION)
        & set(TRIGGER_FUNCTION_SEARCH_PATH_BY_REVISION)
        & set(TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION)
        & set(FOREIGN_KEY_SPECS_BY_REVISION)
        & set(POLLING_RELATIONS_BY_REVISION)
        & set(POLLING_VIEW_SPECS_BY_REVISION)
    )
    revision_contracts = [
        f"""(
            {_polling_profile_matches_sql(schema_oid, revision)}
            AND {
            _target_trigger_contract_exact_for_revision_sql(
                schema_oid,
                migration_oid,
                revision,
            )
        }
        )"""
        for revision in sorted(revisions)
    ]
    no_trigger_contract = f"""(
        {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
        AND {no_trigger_hooks}
    )"""
    return "(" + " OR ".join((no_trigger_contract, *revision_contracts)) + ")"


def _runtime_relation_capability_sql(role_oid: str, relation_oid: str) -> str:
    """Return whether a role can exercise any data capability on a relation."""

    return f"""(
        pg_catalog.has_table_privilege(
            {role_oid}, {relation_oid}, 'SELECT'
        )
        OR pg_catalog.has_table_privilege(
            {role_oid}, {relation_oid}, 'INSERT'
        )
        OR pg_catalog.has_table_privilege(
            {role_oid}, {relation_oid}, 'UPDATE'
        )
        OR pg_catalog.has_table_privilege(
            {role_oid}, {relation_oid}, 'DELETE'
        )
        OR pg_catalog.has_any_column_privilege(
            {role_oid}, {relation_oid}, 'SELECT'
        )
        OR pg_catalog.has_any_column_privilege(
            {role_oid}, {relation_oid}, 'INSERT'
        )
        OR pg_catalog.has_any_column_privilege(
            {role_oid}, {relation_oid}, 'UPDATE'
        )
        OR pg_catalog.has_any_column_privilege(
            {role_oid}, {relation_oid}, 'REFERENCES'
        )
    )"""


def _runtime_audit_permissions_sql(role_oid: str, schema_oid: str) -> str:
    """Allow only append-only, column-bounded runtime audit writes."""

    grant_option_denied = f"""(
        NOT pg_catalog.has_table_privilege(
            {role_oid}, audit_relation.oid, 'SELECT WITH GRANT OPTION'
        )
        AND NOT pg_catalog.has_table_privilege(
            {role_oid}, audit_relation.oid, 'INSERT WITH GRANT OPTION'
        )
        AND NOT pg_catalog.has_table_privilege(
            {role_oid}, audit_relation.oid, 'UPDATE WITH GRANT OPTION'
        )
        AND NOT pg_catalog.has_any_column_privilege(
            {role_oid}, audit_relation.oid, 'SELECT WITH GRANT OPTION'
        )
        AND NOT pg_catalog.has_any_column_privilege(
            {role_oid}, audit_relation.oid, 'INSERT WITH GRANT OPTION'
        )
        AND NOT pg_catalog.has_any_column_privilege(
            {role_oid}, audit_relation.oid, 'UPDATE WITH GRANT OPTION'
        )
    )"""
    greenfield_append_only = f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS audit_relation
        WHERE audit_relation.relnamespace = {schema_oid}
          AND audit_relation.relname = 'audit_events'
          AND (
              audit_relation.relkind <> 'r'
              OR NOT pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'SELECT'
              )
              OR NOT {grant_option_denied}
              OR NOT pg_catalog.has_any_column_privilege(
                  {role_oid}, audit_relation.oid, 'INSERT'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'UPDATE'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'DELETE'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'TRUNCATE'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'REFERENCES'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'TRIGGER'
              )
              OR pg_catalog.has_any_column_privilege(
                  {role_oid}, audit_relation.oid, 'UPDATE'
              )
              OR pg_catalog.has_any_column_privilege(
                  {role_oid}, audit_relation.oid, 'REFERENCES'
              )
          )
    )"""
    legacy_insert = f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS audit_relation
        WHERE audit_relation.relnamespace = {schema_oid}
          AND audit_relation.relname = 'audit_events'
          AND (
              audit_relation.relkind <> 'r'
              OR NOT pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'SELECT'
              )
              OR NOT pg_catalog.has_any_column_privilege(
                  {role_oid}, audit_relation.oid, 'INSERT'
              )
              OR NOT {grant_option_denied}
              OR pg_catalog.has_any_column_privilege(
                  {role_oid}, audit_relation.oid, 'UPDATE'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'UPDATE'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'DELETE'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'TRUNCATE'
              )
              OR pg_catalog.has_table_privilege(
                  {role_oid}, audit_relation.oid, 'TRIGGER'
              )
          )
    )"""
    return f"""(
        CASE
            WHEN (
                {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
                OR {_polling_profile_matches_sql(schema_oid, DATABASE_REVISION)}
            )
                THEN {greenfield_append_only}
            ELSE {legacy_insert}
        END
    )"""


def _target_execution_hooks_denied_sql(
    schema_oid: str,
    runtime_oid: str,
    migration_oid: str,
) -> str:
    """Allow only the revisioned FK/trigger set and deny other hidden paths."""

    return f"""(
    {_target_foreign_keys_exact_sql(schema_oid)}
    AND {_target_trigger_contract_exact_sql(schema_oid, migration_oid)}
    AND {_security_definer_contract_sql(schema_oid, migration_oid)}
    AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_inherits AS inheritance
        JOIN pg_catalog.pg_class AS child_relation
          ON child_relation.oid = inheritance.inhrelid
        JOIN pg_catalog.pg_class AS parent_relation
          ON parent_relation.oid = inheritance.inhparent
        WHERE child_relation.relnamespace = {schema_oid}
           OR parent_relation.relnamespace = {schema_oid}
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_rewrite AS rewrite_rule
        JOIN pg_catalog.pg_class AS relation
          ON relation.oid = rewrite_rule.ev_class
        WHERE relation.relnamespace = {schema_oid}
          AND NOT (
              rewrite_rule.rulename = '_RETURN'
              AND relation.relkind = 'v'
              AND (
                  COALESCE(
                      relation.reloptions,
                      '{{}}'::pg_catalog.text[]
                  ) OPERATOR(pg_catalog.@>)
                      ARRAY['security_invoker=true']::pg_catalog.text[]
                  OR NOT {
        _runtime_relation_capability_sql(
            runtime_oid,
            "relation.oid",
        )
    }
              )
          )
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_class AS view_relation
        WHERE view_relation.relnamespace = {schema_oid}
          AND view_relation.relkind = 'v'
          AND NOT (
              COALESCE(
                  view_relation.reloptions,
                  '{{}}'::pg_catalog.text[]
              ) OPERATOR(pg_catalog.@>)
              ARRAY['security_invoker=true']::pg_catalog.text[]
          )
          AND {
        _runtime_relation_capability_sql(
            runtime_oid,
            "view_relation.oid",
        )
    }
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_event_trigger AS event_trigger
        WHERE event_trigger.evtenabled <> 'D'
    ))"""


def _system_initial_acl_unchanged_sql(role_oid: str, schema_oid: str) -> str:
    """Reject system-object grants beyond the immutable initdb baseline."""

    return f"""NOT EXISTS (
        WITH actual_system_grants AS (
            SELECT
                'pg_catalog.pg_class'::pg_catalog.regclass AS classoid,
                object.oid AS objoid,
                0::pg_catalog.int4 AS objsubid,
                CASE
                    WHEN object.relkind = 'S' THEN 'S'
                    ELSE 'r'
                END::pg_catalog."char" AS acl_kind,
                object.relowner AS owner_oid,
                actual_grant.grantee,
                actual_grant.privilege_type,
                actual_grant.is_grantable
            FROM pg_catalog.pg_class AS object
            JOIN pg_catalog.pg_namespace AS object_schema
              ON object_schema.oid = object.relnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    object.relacl,
                    pg_catalog.acldefault(
                        CASE
                            WHEN object.relkind = 'S' THEN 'S'
                            ELSE 'r'
                        END::pg_catalog."char",
                        object.relowner
                    )
                )
            ) AS actual_grant
            WHERE (
                object_schema.nspname = 'information_schema'
                OR object_schema.nspname LIKE 'pg_%%'
            )
              AND object_schema.nspname OPERATOR(pg_catalog.!~)
                  '^pg_(toast_)?temp_[0-9]+$'
            UNION ALL
            SELECT
                'pg_catalog.pg_class'::pg_catalog.regclass,
                object.oid,
                attribute.attnum,
                NULL::pg_catalog."char",
                object.relowner,
                actual_grant.grantee,
                actual_grant.privilege_type,
                actual_grant.is_grantable
            FROM pg_catalog.pg_attribute AS attribute
            JOIN pg_catalog.pg_class AS object
              ON object.oid = attribute.attrelid
            JOIN pg_catalog.pg_namespace AS object_schema
              ON object_schema.oid = object.relnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                attribute.attacl
            ) AS actual_grant
            WHERE (
                object_schema.nspname = 'information_schema'
                OR object_schema.nspname LIKE 'pg_%%'
            )
              AND object_schema.nspname OPERATOR(pg_catalog.!~)
                  '^pg_(toast_)?temp_[0-9]+$'
            UNION ALL
            SELECT
                'pg_catalog.pg_proc'::pg_catalog.regclass,
                object.oid,
                0::pg_catalog.int4,
                'f'::pg_catalog."char",
                object.proowner,
                actual_grant.grantee,
                actual_grant.privilege_type,
                actual_grant.is_grantable
            FROM pg_catalog.pg_proc AS object
            JOIN pg_catalog.pg_namespace AS object_schema
              ON object_schema.oid = object.pronamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    object.proacl,
                    pg_catalog.acldefault('f', object.proowner)
                )
            ) AS actual_grant
            WHERE (
                object_schema.nspname = 'information_schema'
                OR object_schema.nspname LIKE 'pg_%%'
            )
              AND object_schema.nspname OPERATOR(pg_catalog.!~)
                  '^pg_(toast_)?temp_[0-9]+$'
            UNION ALL
            SELECT
                'pg_catalog.pg_type'::pg_catalog.regclass,
                object.oid,
                0::pg_catalog.int4,
                'T'::pg_catalog."char",
                object.typowner,
                actual_grant.grantee,
                actual_grant.privilege_type,
                actual_grant.is_grantable
            FROM pg_catalog.pg_type AS object
            JOIN pg_catalog.pg_namespace AS object_schema
              ON object_schema.oid = object.typnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    object.typacl,
                    pg_catalog.acldefault('T', object.typowner)
                )
            ) AS actual_grant
            WHERE (
                object_schema.nspname = 'information_schema'
                OR object_schema.nspname LIKE 'pg_%%'
            )
              AND object_schema.nspname OPERATOR(pg_catalog.!~)
                  '^pg_(toast_)?temp_[0-9]+$'
            UNION ALL
            SELECT
                'pg_catalog.pg_namespace'::pg_catalog.regclass,
                object.oid,
                0::pg_catalog.int4,
                'n'::pg_catalog."char",
                object.nspowner,
                actual_grant.grantee,
                actual_grant.privilege_type,
                actual_grant.is_grantable
            FROM pg_catalog.pg_namespace AS object
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    object.nspacl,
                    pg_catalog.acldefault('n', object.nspowner)
                )
            ) AS actual_grant
            WHERE object.oid IS DISTINCT FROM {schema_oid}
              AND (
                  object.nspname = 'information_schema'
                  OR object.nspname LIKE 'pg_%%'
              )
              AND object.nspname OPERATOR(pg_catalog.!~)
                  '^pg_(toast_)?temp_[0-9]+$'
        )
        SELECT 1
        FROM actual_system_grants AS actual_grant
        WHERE actual_grant.grantee IN (0, {role_oid})
          AND NOT (
              actual_grant.objoid < 16384
              AND actual_grant.grantee = 0
              AND NOT actual_grant.is_grantable
              AND (
                  (
                      actual_grant.classoid =
                          'pg_catalog.pg_class'::pg_catalog.regclass
                      AND actual_grant.privilege_type = 'SELECT'
                      AND EXISTS (
                          SELECT 1
                          FROM pg_catalog.pg_class AS baseline_relation
                          JOIN pg_catalog.pg_namespace AS baseline_schema
                            ON baseline_schema.oid =
                               baseline_relation.relnamespace
                          WHERE baseline_relation.oid = actual_grant.objoid
                            AND baseline_schema.nspname =
                                'information_schema'
                      )
                  ) OR (
                      actual_grant.classoid =
                          'pg_catalog.pg_namespace'::pg_catalog.regclass
                      AND actual_grant.privilege_type = 'USAGE'
                      AND EXISTS (
                          SELECT 1
                          FROM pg_catalog.pg_namespace AS baseline_schema
                          WHERE baseline_schema.oid = actual_grant.objoid
                            AND baseline_schema.nspname =
                                'information_schema'
                      )
                  )
              )
          )
          AND (
              (
                  actual_grant.objoid >= 16384
                  AND actual_grant.grantee = 0
              ) OR (
                  actual_grant.objoid < 16384
                  AND NOT EXISTS (
                  SELECT 1
                  FROM pg_catalog.aclexplode(
                      COALESCE(
                          (
                              SELECT initial_acl.initprivs
                              FROM pg_catalog.pg_init_privs AS initial_acl
                              WHERE initial_acl.classoid = actual_grant.classoid
                                AND initial_acl.objoid = actual_grant.objoid
                                AND initial_acl.objsubid = actual_grant.objsubid
                                AND initial_acl.privtype = 'i'
                          ),
                          CASE
                              WHEN actual_grant.acl_kind IS NULL
                              THEN '{{}}'::pg_catalog.aclitem[]
                              ELSE pg_catalog.acldefault(
                                  actual_grant.acl_kind,
                                  actual_grant.owner_oid
                              )
                          END
                      )
                  ) AS base_grant
                  WHERE base_grant.grantee = actual_grant.grantee
                    AND base_grant.privilege_type =
                        actual_grant.privilege_type
                    AND base_grant.is_grantable =
                        actual_grant.is_grantable
                  )
              )
          )
    )"""


def _role_and_database_settings_absent_sql(
    role_oid: str,
    database_oid: str,
) -> str:
    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_db_role_setting AS role_setting
        WHERE (
            role_setting.setrole = {role_oid}
            AND role_setting.setdatabase IN (0, {database_oid})
        ) OR (
            role_setting.setrole = 0
            AND role_setting.setdatabase = {database_oid}
        )
    )"""


def _runtime_counterpart_privileges_sql(
    role_oid: str,
    schema_oid: str,
    database_oid: str,
) -> str:
    """Prove runtime safety from the migration connection before any DDL."""

    direct_grants = _unexpected_direct_grants_sql(
        role_oid,
        schema_oid,
        database_oid,
    )
    ownership = _unexpected_ownership_sql(role_oid, schema_oid, database_oid)
    other_schema = _other_schema_create_denied_sql(role_oid, schema_oid)
    other_database = _other_database_connect_denied_sql(
        role_oid,
        database_oid,
    )
    other_schema_usage = _other_user_schema_usage_denied_sql(
        role_oid,
        schema_oid,
    )
    delegation = _delegation_denied_sql(role_oid, schema_oid, database_oid)
    role_setting = _role_and_database_settings_absent_sql(
        role_oid,
        database_oid,
    )
    sequence_update = _sequence_update_denied_sql(role_oid, schema_oid)
    large_object_creation = _large_object_creation_denied_sql(role_oid)
    return f"""(
        {direct_grants}
        AND {ownership}
        AND {other_schema}
        AND {other_database}
        AND {other_schema_usage}
        AND {delegation}
        AND {role_setting}
        AND {sequence_update}
        AND {large_object_creation}
        AND pg_catalog.has_database_privilege(
            {role_oid}, {database_oid}, 'CONNECT'
        )
        AND NOT pg_catalog.has_database_privilege(
            {role_oid}, {database_oid}, 'CREATE'
        )
        AND NOT pg_catalog.has_database_privilege(
            {role_oid}, {database_oid}, 'TEMPORARY'
        )
        AND pg_catalog.has_schema_privilege(
            {role_oid}, {schema_oid}, 'USAGE'
        )
        AND NOT pg_catalog.has_schema_privilege(
            {role_oid}, {schema_oid}, 'CREATE'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS relation
            WHERE relation.relnamespace = {schema_oid}
              AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND (
                  pg_catalog.has_table_privilege(
                      {role_oid}, relation.oid, 'DELETE'
                  )
                  OR pg_catalog.has_table_privilege(
                      {role_oid}, relation.oid, 'TRUNCATE'
                  )
                  OR pg_catalog.has_table_privilege(
                      {role_oid}, relation.oid, 'TRIGGER'
                  )
              )
        )
    )"""


def _migration_counterpart_privileges_sql(
    role_oid: str,
    schema_oid: str,
    database_oid: str,
) -> str:
    direct_grants = _unexpected_direct_grants_sql(
        role_oid,
        schema_oid,
        database_oid,
    )
    ownership = _unexpected_ownership_sql(role_oid, schema_oid, database_oid)
    other_schema = _other_schema_create_denied_sql(role_oid, schema_oid)
    other_database = _other_database_connect_denied_sql(
        role_oid,
        database_oid,
    )
    other_schema_usage = _other_user_schema_usage_denied_sql(
        role_oid,
        schema_oid,
    )
    role_setting = _role_and_database_settings_absent_sql(
        role_oid,
        database_oid,
    )
    large_object_creation = _large_object_creation_denied_sql(role_oid)
    return f"""(
        {direct_grants}
        AND {ownership}
        AND {other_schema}
        AND {other_database}
        AND {other_schema_usage}
        AND {role_setting}
        AND {large_object_creation}
    )"""


def _maintenance_counterpart_privileges_sql(
    role_oid: str,
    schema_oid: str,
    database_oid: str,
    *,
    allow_missing_access: bool = False,
) -> str:
    """Prove the cleanup identity has only the revisioned delete boundary."""

    return f"""(
        {_unexpected_direct_grants_sql(role_oid, schema_oid, database_oid)}
        AND {_unexpected_ownership_sql(role_oid, schema_oid, database_oid)}
        AND {_other_schema_create_denied_sql(role_oid, schema_oid)}
        AND {_other_database_connect_denied_sql(role_oid, database_oid)}
        AND {_other_user_schema_usage_denied_sql(role_oid, schema_oid)}
        AND {_delegation_denied_sql(role_oid, schema_oid, database_oid)}
        AND {_role_and_database_settings_absent_sql(role_oid, database_oid)}
        AND {_large_object_creation_denied_sql(role_oid)}
        AND {_system_initial_acl_unchanged_sql(role_oid, schema_oid)}
        AND {
        _relation_access_contract_sql(
            schema_oid,
            role_oid,
            MAINTENANCE_RELATION_ACCESS,
            allow_missing=allow_missing_access,
            manifests_by_revision=MAINTENANCE_RELATION_ACCESS_BY_REVISION,
        )
    }
        AND pg_catalog.has_database_privilege(
            {role_oid}, {database_oid}, 'CONNECT'
        )
        AND NOT pg_catalog.has_database_privilege(
            {role_oid}, {database_oid}, 'CREATE'
        )
        AND NOT pg_catalog.has_database_privilege(
            {role_oid}, {database_oid}, 'TEMPORARY'
        )
        AND pg_catalog.has_schema_privilege(
            {role_oid}, {schema_oid}, 'USAGE'
        )
        AND NOT pg_catalog.has_schema_privilege(
            {role_oid}, {schema_oid}, 'CREATE'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS object
            WHERE object.relnamespace = {schema_oid}
              AND object.relowner = {role_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS routine
            WHERE routine.pronamespace = {schema_oid}
              AND routine.proowner = {role_oid}
        )
        AND {
        _routine_execute_contract_sql(
            schema_oid,
            role_oid,
            MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION,
            allow_missing=allow_missing_access,
        )
    }
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_type AS object_type
            WHERE object_type.typnamespace = {schema_oid}
              AND object_type.typowner = {role_oid}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_type AS object_type
            CROSS JOIN LATERAL pg_catalog.aclexplode(object_type.typacl)
                AS type_acl
            WHERE object_type.typnamespace = {schema_oid}
              AND type_acl.grantee = {role_oid}
        )
    )"""


class DatabaseRoleError(RuntimeError):
    """Safe failure raised when a database identity boundary is unproven."""


@dataclass(frozen=True)
class MigrationRoleSnapshot:
    direct_session: bool
    expected_identity: bool
    restricted_attributes: bool
    no_role_memberships: bool
    runtime_role_exists: bool
    runtime_restricted_attributes: bool
    runtime_no_role_memberships: bool
    maintenance_role_exists: bool
    maintenance_restricted_attributes: bool
    maintenance_no_role_memberships: bool
    roles_distinct: bool
    roles_all_distinct: bool
    runtime_not_member: bool
    database_owned_by_migration: bool
    schema_owned_by_migration: bool
    relations_owned_by_migration: bool
    routines_owned_by_migration: bool
    types_owned_by_migration: bool
    extended_objects_owned_by_migration: bool
    default_privileges_exclusive: bool
    schema_create_exclusive: bool
    trigger_semantics_safe: bool
    session_security_settings_safe: bool
    large_object_creation_denied: bool
    target_execution_hooks_denied: bool
    unexpected_direct_grants_denied: bool
    unexpected_object_ownership_denied: bool
    other_schema_create_denied: bool
    other_database_connect_denied: bool
    other_user_schema_usage_denied: bool
    target_acl_exclusive: bool
    auditor_access_contract_exact: bool
    auditor_access_contract_reconcilable: bool
    runtime_access_contract_exact: bool
    runtime_access_contract_reconcilable: bool
    system_public_acl_unchanged: bool
    runtime_counterpart_privileges_safe: bool
    maintenance_counterpart_privileges_safe: bool
    maintenance_counterpart_privileges_reconcilable: bool
    search_path_matches: bool


@dataclass(frozen=True)
class RuntimeRoleSnapshot:
    direct_session: bool
    expected_identity: bool
    restricted_attributes: bool
    no_role_memberships: bool
    migration_role_exists: bool
    migration_restricted_attributes: bool
    migration_no_role_memberships: bool
    maintenance_role_exists: bool
    maintenance_restricted_attributes: bool
    maintenance_no_role_memberships: bool
    roles_distinct: bool
    roles_all_distinct: bool
    runtime_not_member: bool
    database_owned_by_migration: bool
    schema_owned_by_migration: bool
    relations_owned_by_migration: bool
    routines_owned_by_migration: bool
    types_owned_by_migration: bool
    extended_objects_owned_by_migration: bool
    default_privileges_exclusive: bool
    schema_create_exclusive: bool
    routines_execute_denied: bool
    trigger_semantics_safe: bool
    session_security_settings_safe: bool
    large_object_creation_denied: bool
    target_execution_hooks_denied: bool
    unexpected_direct_grants_denied: bool
    unexpected_object_ownership_denied: bool
    other_schema_create_denied: bool
    other_database_connect_denied: bool
    other_user_schema_usage_denied: bool
    target_acl_exclusive: bool
    auditor_access_contract_exact: bool
    runtime_access_contract_exact: bool
    delegation_privileges_denied: bool
    system_public_acl_unchanged: bool
    migration_counterpart_privileges_safe: bool
    maintenance_counterpart_privileges_safe: bool
    database_connect_allowed: bool
    database_create_denied: bool
    database_temp_denied: bool
    schema_usage_allowed: bool
    schema_create_denied: bool
    dangerous_relation_privileges_denied: bool
    sequence_update_denied: bool
    audit_permissions_valid: bool
    search_path_matches: bool


@dataclass(frozen=True)
class MaintenanceRoleSnapshot:
    direct_session: bool
    expected_identity: bool
    restricted_attributes: bool
    no_role_memberships: bool
    counterpart_roles_exist: bool
    counterpart_roles_restricted: bool
    counterpart_roles_have_no_memberships: bool
    roles_all_distinct: bool
    database_owned_by_migration: bool
    schema_owned_by_migration: bool
    target_objects_owned_by_migration: bool
    trigger_semantics_safe: bool
    session_security_settings_safe: bool
    target_execution_hooks_denied: bool
    target_acl_exclusive: bool
    auditor_access_contract_exact: bool
    runtime_access_contract_exact: bool
    maintenance_privileges_safe: bool
    search_path_matches: bool


_MIGRATION_ROLE_QUERY: Final = f"""
WITH role_context AS (
    SELECT
        role.oid AS current_oid,
        runtime_role.oid AS runtime_oid,
        maintenance_role.oid AS maintenance_oid,
        auditor_role.oid AS auditor_oid,
        database.oid AS database_oid,
        database.datdba,
        namespace.oid AS schema_oid,
        namespace.nspowner,
        namespace.nspacl,
        role.rolsuper,
        role.rolcreatedb,
        role.rolcreaterole,
        role.rolreplication,
        role.rolbypassrls,
        role.rolcanlogin,
        role.rolinherit,
        runtime_role.rolsuper AS runtime_rolsuper,
        runtime_role.rolcreatedb AS runtime_rolcreatedb,
        runtime_role.rolcreaterole AS runtime_rolcreaterole,
        runtime_role.rolreplication AS runtime_rolreplication,
        runtime_role.rolbypassrls AS runtime_rolbypassrls,
        runtime_role.rolcanlogin AS runtime_rolcanlogin,
        runtime_role.rolinherit AS runtime_rolinherit,
        maintenance_role.rolsuper AS maintenance_rolsuper,
        maintenance_role.rolcreatedb AS maintenance_rolcreatedb,
        maintenance_role.rolcreaterole AS maintenance_rolcreaterole,
        maintenance_role.rolreplication AS maintenance_rolreplication,
        maintenance_role.rolbypassrls AS maintenance_rolbypassrls,
        maintenance_role.rolcanlogin AS maintenance_rolcanlogin,
        maintenance_role.rolinherit AS maintenance_rolinherit
    FROM pg_catalog.pg_roles AS role
    JOIN pg_catalog.pg_database AS database
      ON database.datname = pg_catalog.current_database()
    LEFT JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.nspname = %s
    LEFT JOIN pg_catalog.pg_roles AS runtime_role
      ON runtime_role.rolname = %s
    LEFT JOIN pg_catalog.pg_roles AS maintenance_role
      ON maintenance_role.rolname = %s
    LEFT JOIN pg_catalog.pg_roles AS auditor_role
      ON auditor_role.rolname = %s
    WHERE role.rolname = current_user
)
SELECT
    session_user = current_user AS direct_session,
    current_user = %s AS expected_identity,
    role.rolcanlogin
      AND NOT role.rolsuper
      AND NOT role.rolcreatedb
      AND NOT role.rolcreaterole
      AND NOT role.rolreplication
      AND NOT role.rolbypassrls
      AND NOT role.rolinherit AS restricted_attributes,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        WHERE membership.member = role.current_oid
           OR membership.roleid = role.current_oid
    ) AS no_role_memberships,
    role.runtime_oid IS NOT NULL AS runtime_role_exists,
    role.runtime_oid IS NOT NULL
      AND role.runtime_rolcanlogin
      AND NOT role.runtime_rolsuper
      AND NOT role.runtime_rolcreatedb
      AND NOT role.runtime_rolcreaterole
      AND NOT role.runtime_rolreplication
      AND NOT role.runtime_rolbypassrls
      AND NOT role.runtime_rolinherit AS runtime_restricted_attributes,
    role.runtime_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        WHERE membership.member = role.runtime_oid
           OR membership.roleid = role.runtime_oid
    ) AS runtime_no_role_memberships,
    role.maintenance_oid IS NOT NULL AS maintenance_role_exists,
    role.maintenance_oid IS NOT NULL
      AND role.maintenance_rolcanlogin
      AND NOT role.maintenance_rolsuper
      AND NOT role.maintenance_rolcreatedb
      AND NOT role.maintenance_rolcreaterole
      AND NOT role.maintenance_rolreplication
      AND NOT role.maintenance_rolbypassrls
      AND NOT role.maintenance_rolinherit AS maintenance_restricted_attributes,
    role.maintenance_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        WHERE membership.member = role.maintenance_oid
           OR membership.roleid = role.maintenance_oid
    ) AS maintenance_no_role_memberships,
    role.current_oid IS DISTINCT FROM role.runtime_oid AS roles_distinct,
    role.maintenance_oid IS NOT NULL
      AND role.current_oid IS DISTINCT FROM role.maintenance_oid
      AND role.runtime_oid IS DISTINCT FROM role.maintenance_oid
      AS roles_all_distinct,
    role.runtime_oid IS NOT NULL
      AND NOT pg_catalog.pg_has_role(
          role.runtime_oid, role.current_oid, 'MEMBER'
      )
      AS runtime_not_member,
    role.datdba = role.current_oid AS database_owned_by_migration,
    role.nspowner = role.current_oid AS schema_owned_by_migration,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS relation
        WHERE relation.relnamespace = role.schema_oid
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'i', 'I')
          AND relation.relowner IS DISTINCT FROM role.current_oid
    ) AS relations_owned_by_migration,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS routine
        WHERE routine.pronamespace = role.schema_oid
          AND routine.proowner IS DISTINCT FROM role.current_oid
    ) AS routines_owned_by_migration,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_type AS owned_type
        WHERE owned_type.typnamespace = role.schema_oid
          AND owned_type.typowner IS DISTINCT FROM role.current_oid
    ) AS types_owned_by_migration,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_operator AS schema_object
        WHERE schema_object.oprnamespace = role.schema_oid
          AND schema_object.oprowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_opclass AS schema_object
        WHERE schema_object.opcnamespace = role.schema_oid
          AND schema_object.opcowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_opfamily AS schema_object
        WHERE schema_object.opfnamespace = role.schema_oid
          AND schema_object.opfowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_collation AS schema_object
        WHERE schema_object.collnamespace = role.schema_oid
          AND schema_object.collowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_conversion AS schema_object
        WHERE schema_object.connamespace = role.schema_oid
          AND schema_object.conowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_config AS schema_object
        WHERE schema_object.cfgnamespace = role.schema_oid
          AND schema_object.cfgowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_dict AS schema_object
        WHERE schema_object.dictnamespace = role.schema_oid
          AND schema_object.dictowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_statistic_ext AS schema_object
        WHERE schema_object.stxnamespace = role.schema_oid
          AND schema_object.stxowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_extension AS schema_object
        WHERE schema_object.extnamespace = role.schema_oid
          AND schema_object.extowner IS DISTINCT FROM role.current_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_parser AS schema_object
        WHERE schema_object.prsnamespace = role.schema_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_template AS schema_object
        WHERE schema_object.tmplnamespace = role.schema_oid
    ) AS extended_objects_owned_by_migration,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_default_acl AS default_acl
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            default_acl.defaclacl
        ) AS default_grant
        WHERE default_acl.defaclrole = role.current_oid
          AND default_acl.defaclnamespace IN (0, role.schema_oid)
          AND default_grant.grantee IS DISTINCT FROM role.current_oid
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.aclexplode(
            COALESCE(
                (
                    SELECT default_acl.defaclacl
                    FROM pg_catalog.pg_default_acl AS default_acl
                    WHERE default_acl.defaclrole = role.current_oid
                      AND default_acl.defaclnamespace = 0
                      AND default_acl.defaclobjtype = 'f'
                ),
                pg_catalog.acldefault('f', role.current_oid)
            )
        ) AS default_grant
        WHERE default_grant.grantee IS DISTINCT FROM role.current_oid
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.aclexplode(
            COALESCE(
                (
                    SELECT default_acl.defaclacl
                    FROM pg_catalog.pg_default_acl AS default_acl
                    WHERE default_acl.defaclrole = role.current_oid
                      AND default_acl.defaclnamespace = 0
                      AND default_acl.defaclobjtype = 'T'
                ),
                pg_catalog.acldefault('T', role.current_oid)
            )
        ) AS default_grant
        WHERE default_grant.grantee IS DISTINCT FROM role.current_oid
    ) AS default_privileges_exclusive,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.aclexplode(
            COALESCE(
                role.nspacl,
                pg_catalog.acldefault('n', role.nspowner)
            )
        ) AS schema_acl
        WHERE schema_acl.privilege_type = 'CREATE'
          AND schema_acl.grantee IS DISTINCT FROM role.current_oid
    ) AS schema_create_exclusive,
    pg_catalog.current_setting('session_replication_role') = 'origin'
      AND NOT pg_catalog.has_parameter_privilege(
          role.current_oid, 'session_replication_role', 'SET'
      )
      AND NOT pg_catalog.has_parameter_privilege(
          role.current_oid, 'session_replication_role', 'ALTER SYSTEM'
      ) AS trigger_semantics_safe,
    pg_catalog.current_setting('lo_compat_privileges') = 'off'
      AND pg_catalog.current_setting('row_security') = 'on'
      AND pg_catalog.current_setting('local_preload_libraries') = ''
      AND {
    _role_and_database_settings_absent_sql("role.current_oid", "role.database_oid")
}
      AS session_security_settings_safe,
    {_large_object_creation_denied_sql("role.current_oid")}
      AS large_object_creation_denied,
    {
    _target_execution_hooks_denied_sql(
        "role.schema_oid",
        "role.runtime_oid",
        "role.current_oid",
    )
}
      AS target_execution_hooks_denied,
    {
    _unexpected_direct_grants_sql(
        "role.current_oid", "role.schema_oid", "role.database_oid"
    )
}
      AS unexpected_direct_grants_denied,
    {
    _unexpected_ownership_sql(
        "role.current_oid", "role.schema_oid", "role.database_oid"
    )
}
      AS unexpected_object_ownership_denied,
    {_other_schema_create_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_schema_create_denied,
    {_other_database_connect_denied_sql("role.current_oid", "role.database_oid")}
      AS other_database_connect_denied,
    {_other_user_schema_usage_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_user_schema_usage_denied,
    role.runtime_oid IS NOT NULL
      AND role.maintenance_oid IS NOT NULL AND
      {
    _target_acl_exclusive_sql(
        "role.current_oid",
        "role.runtime_oid",
        "role.maintenance_oid",
        "role.auditor_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS target_acl_exclusive,
    {
    _checkpoint_auditor_access_contract_sql(
        "role.auditor_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS auditor_access_contract_exact,
    {
    _checkpoint_auditor_access_contract_sql(
        "role.auditor_oid",
        "role.schema_oid",
        "role.database_oid",
        allow_missing=True,
    )
}
      AS auditor_access_contract_reconcilable,
    role.runtime_oid IS NOT NULL AND
      {
    _relation_access_contract_sql(
        "role.schema_oid",
        "role.runtime_oid",
        RUNTIME_RELATION_ACCESS,
        manifests_by_revision=RUNTIME_RELATION_ACCESS_BY_REVISION,
    )
}
      AND {
    _routine_execute_contract_sql(
        "role.schema_oid",
        "role.runtime_oid",
        RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
    )
}
      AS runtime_access_contract_exact,
    role.runtime_oid IS NOT NULL AND
      {
    _relation_access_contract_sql(
        "role.schema_oid",
        "role.runtime_oid",
        RUNTIME_RELATION_ACCESS,
        allow_missing=True,
        manifests_by_revision=RUNTIME_RELATION_ACCESS_BY_REVISION,
    )
}
      AND {
    _routine_execute_contract_sql(
        "role.schema_oid",
        "role.runtime_oid",
        RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
        allow_missing=True,
    )
}
      AS runtime_access_contract_reconcilable,
    {_system_initial_acl_unchanged_sql("role.current_oid", "role.schema_oid")}
      AS system_public_acl_unchanged,
    role.runtime_oid IS NOT NULL AND
      {
    _runtime_counterpart_privileges_sql(
        "role.runtime_oid", "role.schema_oid", "role.database_oid"
    )
}
      AS runtime_counterpart_privileges_safe,
    role.maintenance_oid IS NOT NULL AND
      {
    _maintenance_counterpart_privileges_sql(
        "role.maintenance_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS maintenance_counterpart_privileges_safe,
    role.maintenance_oid IS NOT NULL AND
      {
    _maintenance_counterpart_privileges_sql(
        "role.maintenance_oid",
        "role.schema_oid",
        "role.database_oid",
        allow_missing_access=True,
    )
}
      AS maintenance_counterpart_privileges_reconcilable
FROM role_context AS role
"""


_RUNTIME_ROLE_QUERY: Final = f"""
WITH role_context AS (
    SELECT
        role.oid AS current_oid,
        migration_role.oid AS migration_oid,
        maintenance_role.oid AS maintenance_oid,
        auditor_role.oid AS auditor_oid,
        database.oid AS database_oid,
        database.datdba,
        namespace.oid AS schema_oid,
        namespace.nspowner,
        namespace.nspacl,
        role.rolsuper,
        role.rolcreatedb,
        role.rolcreaterole,
        role.rolreplication,
        role.rolbypassrls,
        role.rolcanlogin,
        role.rolinherit,
        migration_role.rolsuper AS migration_rolsuper,
        migration_role.rolcreatedb AS migration_rolcreatedb,
        migration_role.rolcreaterole AS migration_rolcreaterole,
        migration_role.rolreplication AS migration_rolreplication,
        migration_role.rolbypassrls AS migration_rolbypassrls,
        migration_role.rolcanlogin AS migration_rolcanlogin,
        migration_role.rolinherit AS migration_rolinherit,
        maintenance_role.rolsuper AS maintenance_rolsuper,
        maintenance_role.rolcreatedb AS maintenance_rolcreatedb,
        maintenance_role.rolcreaterole AS maintenance_rolcreaterole,
        maintenance_role.rolreplication AS maintenance_rolreplication,
        maintenance_role.rolbypassrls AS maintenance_rolbypassrls,
        maintenance_role.rolcanlogin AS maintenance_rolcanlogin,
        maintenance_role.rolinherit AS maintenance_rolinherit
    FROM pg_catalog.pg_roles AS role
    JOIN pg_catalog.pg_database AS database
      ON database.datname = pg_catalog.current_database()
    LEFT JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.nspname = %s
    LEFT JOIN pg_catalog.pg_roles AS migration_role
      ON migration_role.rolname = %s
    LEFT JOIN pg_catalog.pg_roles AS maintenance_role
      ON maintenance_role.rolname = %s
    LEFT JOIN pg_catalog.pg_roles AS auditor_role
      ON auditor_role.rolname = %s
    WHERE role.rolname = current_user
)
SELECT
    session_user = current_user AS direct_session,
    current_user = %s AS expected_identity,
    role.rolcanlogin
      AND NOT role.rolsuper
      AND NOT role.rolcreatedb
      AND NOT role.rolcreaterole
      AND NOT role.rolreplication
      AND NOT role.rolbypassrls
      AND NOT role.rolinherit AS restricted_attributes,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        WHERE membership.member = role.current_oid
           OR membership.roleid = role.current_oid
    ) AS no_role_memberships,
    role.migration_oid IS NOT NULL AS migration_role_exists,
    role.migration_oid IS NOT NULL
      AND role.migration_rolcanlogin
      AND NOT role.migration_rolsuper
      AND NOT role.migration_rolcreatedb
      AND NOT role.migration_rolcreaterole
      AND NOT role.migration_rolreplication
      AND NOT role.migration_rolbypassrls
      AND NOT role.migration_rolinherit AS migration_restricted_attributes,
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        WHERE membership.member = role.migration_oid
           OR membership.roleid = role.migration_oid
    ) AS migration_no_role_memberships,
    role.maintenance_oid IS NOT NULL AS maintenance_role_exists,
    role.maintenance_oid IS NOT NULL
      AND role.maintenance_rolcanlogin
      AND NOT role.maintenance_rolsuper
      AND NOT role.maintenance_rolcreatedb
      AND NOT role.maintenance_rolcreaterole
      AND NOT role.maintenance_rolreplication
      AND NOT role.maintenance_rolbypassrls
      AND NOT role.maintenance_rolinherit AS maintenance_restricted_attributes,
    role.maintenance_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        WHERE membership.member = role.maintenance_oid
           OR membership.roleid = role.maintenance_oid
    ) AS maintenance_no_role_memberships,
    role.current_oid IS DISTINCT FROM role.migration_oid AS roles_distinct,
    role.maintenance_oid IS NOT NULL
      AND role.current_oid IS DISTINCT FROM role.maintenance_oid
      AND role.migration_oid IS DISTINCT FROM role.maintenance_oid
      AS roles_all_distinct,
    role.migration_oid IS NOT NULL
      AND NOT pg_catalog.pg_has_role(
          role.current_oid, role.migration_oid, 'MEMBER'
      )
      AS runtime_not_member,
    role.migration_oid IS NOT NULL
      AND role.datdba = role.migration_oid AS database_owned_by_migration,
    role.migration_oid IS NOT NULL
      AND role.nspowner = role.migration_oid AS schema_owned_by_migration,
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS relation
        WHERE relation.relnamespace = role.schema_oid
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'i', 'I')
          AND relation.relowner IS DISTINCT FROM role.migration_oid
    ) AS relations_owned_by_migration,
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS routine
        WHERE routine.pronamespace = role.schema_oid
          AND routine.proowner IS DISTINCT FROM role.migration_oid
    ) AS routines_owned_by_migration,
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_type AS owned_type
        WHERE owned_type.typnamespace = role.schema_oid
          AND owned_type.typowner IS DISTINCT FROM role.migration_oid
    ) AS types_owned_by_migration,
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_operator AS schema_object
        WHERE schema_object.oprnamespace = role.schema_oid
          AND schema_object.oprowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_opclass AS schema_object
        WHERE schema_object.opcnamespace = role.schema_oid
          AND schema_object.opcowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_opfamily AS schema_object
        WHERE schema_object.opfnamespace = role.schema_oid
          AND schema_object.opfowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_collation AS schema_object
        WHERE schema_object.collnamespace = role.schema_oid
          AND schema_object.collowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_conversion AS schema_object
        WHERE schema_object.connamespace = role.schema_oid
          AND schema_object.conowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_config AS schema_object
        WHERE schema_object.cfgnamespace = role.schema_oid
          AND schema_object.cfgowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_dict AS schema_object
        WHERE schema_object.dictnamespace = role.schema_oid
          AND schema_object.dictowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_statistic_ext AS schema_object
        WHERE schema_object.stxnamespace = role.schema_oid
          AND schema_object.stxowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_extension AS schema_object
        WHERE schema_object.extnamespace = role.schema_oid
          AND schema_object.extowner IS DISTINCT FROM role.migration_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_parser AS schema_object
        WHERE schema_object.prsnamespace = role.schema_oid
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_ts_template AS schema_object
        WHERE schema_object.tmplnamespace = role.schema_oid
    ) AS extended_objects_owned_by_migration,
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_default_acl AS default_acl
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            default_acl.defaclacl
        ) AS default_grant
        WHERE default_acl.defaclrole = role.migration_oid
          AND default_acl.defaclnamespace IN (0, role.schema_oid)
          AND default_grant.grantee IS DISTINCT FROM role.migration_oid
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.aclexplode(
            COALESCE(
                (
                    SELECT default_acl.defaclacl
                    FROM pg_catalog.pg_default_acl AS default_acl
                    WHERE default_acl.defaclrole = role.migration_oid
                      AND default_acl.defaclnamespace = 0
                      AND default_acl.defaclobjtype = 'f'
                ),
                pg_catalog.acldefault('f', role.migration_oid)
            )
        ) AS default_grant
        WHERE default_grant.grantee IS DISTINCT FROM role.migration_oid
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.aclexplode(
            COALESCE(
                (
                    SELECT default_acl.defaclacl
                    FROM pg_catalog.pg_default_acl AS default_acl
                    WHERE default_acl.defaclrole = role.migration_oid
                      AND default_acl.defaclnamespace = 0
                      AND default_acl.defaclobjtype = 'T'
                ),
                pg_catalog.acldefault('T', role.migration_oid)
            )
        ) AS default_grant
        WHERE default_grant.grantee IS DISTINCT FROM role.migration_oid
    ) AS default_privileges_exclusive,
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.aclexplode(
            COALESCE(
                role.nspacl,
                pg_catalog.acldefault('n', role.nspowner)
            )
        ) AS schema_acl
        WHERE schema_acl.privilege_type = 'CREATE'
          AND schema_acl.grantee IS DISTINCT FROM role.migration_oid
    ) AS schema_create_exclusive,
    role.migration_oid IS NOT NULL AND
      {
    _routine_execute_contract_sql(
        "role.schema_oid",
        "role.current_oid",
        RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
    )
}
      AS routines_execute_denied,
    pg_catalog.current_setting('session_replication_role') = 'origin'
      AND NOT pg_catalog.has_parameter_privilege(
          role.current_oid, 'session_replication_role', 'SET'
      )
      AND NOT pg_catalog.has_parameter_privilege(
          role.current_oid, 'session_replication_role', 'ALTER SYSTEM'
      ) AS trigger_semantics_safe,
    pg_catalog.current_setting('lo_compat_privileges') = 'off'
      AND pg_catalog.current_setting('row_security') = 'on'
      AND pg_catalog.current_setting('local_preload_libraries') = ''
      AND {
    _role_and_database_settings_absent_sql("role.current_oid", "role.database_oid")
}
      AS session_security_settings_safe,
    {_large_object_creation_denied_sql("role.current_oid")}
      AS large_object_creation_denied,
    {
    _target_execution_hooks_denied_sql(
        "role.schema_oid",
        "role.current_oid",
        "role.migration_oid",
    )
}
      AS target_execution_hooks_denied,
    {
    _unexpected_direct_grants_sql(
        "role.current_oid", "role.schema_oid", "role.database_oid"
    )
}
      AS unexpected_direct_grants_denied,
    {
    _unexpected_ownership_sql(
        "role.current_oid", "role.schema_oid", "role.database_oid"
    )
}
      AS unexpected_object_ownership_denied,
    {_other_schema_create_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_schema_create_denied,
    {_other_database_connect_denied_sql("role.current_oid", "role.database_oid")}
      AS other_database_connect_denied,
    {_other_user_schema_usage_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_user_schema_usage_denied,
    role.migration_oid IS NOT NULL
      AND role.maintenance_oid IS NOT NULL AND
      {
    _target_acl_exclusive_sql(
        "role.migration_oid",
        "role.current_oid",
        "role.maintenance_oid",
        "role.auditor_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS target_acl_exclusive,
    {
    _checkpoint_auditor_access_contract_sql(
        "role.auditor_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS auditor_access_contract_exact,
    {
    _relation_access_contract_sql(
        "role.schema_oid",
        "role.current_oid",
        RUNTIME_RELATION_ACCESS,
        manifests_by_revision=RUNTIME_RELATION_ACCESS_BY_REVISION,
    )
}
      AS runtime_access_contract_exact,
    {_delegation_denied_sql("role.current_oid", "role.schema_oid", "role.database_oid")}
      AS delegation_privileges_denied,
    {_system_initial_acl_unchanged_sql("role.current_oid", "role.schema_oid")}
      AS system_public_acl_unchanged,
    role.migration_oid IS NOT NULL AND
      {
    _migration_counterpart_privileges_sql(
        "role.migration_oid", "role.schema_oid", "role.database_oid"
    )
}
      AS migration_counterpart_privileges_safe,
    role.maintenance_oid IS NOT NULL AND
      {
    _maintenance_counterpart_privileges_sql(
        "role.maintenance_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS maintenance_counterpart_privileges_safe,
    pg_catalog.has_database_privilege(
        role.current_oid, role.database_oid, 'CONNECT'
    ) AS database_connect_allowed,
    NOT pg_catalog.has_database_privilege(
        role.current_oid, role.database_oid, 'CREATE'
    ) AS database_create_denied,
    NOT pg_catalog.has_database_privilege(
        role.current_oid, role.database_oid, 'TEMPORARY'
    ) AS database_temp_denied,
    pg_catalog.has_schema_privilege(
        role.current_oid, role.schema_oid, 'USAGE'
    ) AS schema_usage_allowed,
    NOT pg_catalog.has_schema_privilege(
        role.current_oid, role.schema_oid, 'CREATE'
    ) AS schema_create_denied,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS relation
        WHERE relation.relnamespace = role.schema_oid
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND (
              pg_catalog.has_table_privilege(
                  role.current_oid, relation.oid, 'DELETE'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid, relation.oid, 'TRUNCATE'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid, relation.oid, 'TRIGGER'
              )
          )
    ) AS dangerous_relation_privileges_denied,
    {_sequence_update_denied_sql("role.current_oid", "role.schema_oid")}
      AS sequence_update_denied,
    {_runtime_audit_permissions_sql("role.current_oid", "role.schema_oid")}
      AS audit_permissions_valid
FROM role_context AS role
"""


_MAINTENANCE_ROLE_QUERY: Final = f"""
WITH role_context AS (
    SELECT
        role.oid AS current_oid,
        migration_role.oid AS migration_oid,
        runtime_role.oid AS runtime_oid,
        auditor_role.oid AS auditor_oid,
        database.oid AS database_oid,
        database.datdba,
        namespace.oid AS schema_oid,
        namespace.nspowner,
        role.rolsuper,
        role.rolcreatedb,
        role.rolcreaterole,
        role.rolreplication,
        role.rolbypassrls,
        role.rolcanlogin,
        role.rolinherit,
        migration_role.rolsuper AS migration_rolsuper,
        migration_role.rolcreatedb AS migration_rolcreatedb,
        migration_role.rolcreaterole AS migration_rolcreaterole,
        migration_role.rolreplication AS migration_rolreplication,
        migration_role.rolbypassrls AS migration_rolbypassrls,
        migration_role.rolcanlogin AS migration_rolcanlogin,
        migration_role.rolinherit AS migration_rolinherit,
        runtime_role.rolsuper AS runtime_rolsuper,
        runtime_role.rolcreatedb AS runtime_rolcreatedb,
        runtime_role.rolcreaterole AS runtime_rolcreaterole,
        runtime_role.rolreplication AS runtime_rolreplication,
        runtime_role.rolbypassrls AS runtime_rolbypassrls,
        runtime_role.rolcanlogin AS runtime_rolcanlogin,
        runtime_role.rolinherit AS runtime_rolinherit
    FROM pg_catalog.pg_roles AS role
    JOIN pg_catalog.pg_database AS database
      ON database.datname = pg_catalog.current_database()
    LEFT JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.nspname = %s
    LEFT JOIN pg_catalog.pg_roles AS migration_role
      ON migration_role.rolname = %s
    LEFT JOIN pg_catalog.pg_roles AS runtime_role
      ON runtime_role.rolname = %s
    LEFT JOIN pg_catalog.pg_roles AS auditor_role
      ON auditor_role.rolname = %s
    WHERE role.rolname = current_user
)
SELECT
    session_user = current_user AS direct_session,
    current_user = %s AS expected_identity,
    role.rolcanlogin
      AND NOT role.rolsuper
      AND NOT role.rolcreatedb
      AND NOT role.rolcreaterole
      AND NOT role.rolreplication
      AND NOT role.rolbypassrls
      AND NOT role.rolinherit AS restricted_attributes,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        WHERE membership.member = role.current_oid
           OR membership.roleid = role.current_oid
    ) AS no_role_memberships,
    role.migration_oid IS NOT NULL
      AND role.runtime_oid IS NOT NULL AS counterpart_roles_exist,
    role.migration_oid IS NOT NULL
      AND role.migration_rolcanlogin
      AND NOT role.migration_rolsuper
      AND NOT role.migration_rolcreatedb
      AND NOT role.migration_rolcreaterole
      AND NOT role.migration_rolreplication
      AND NOT role.migration_rolbypassrls
      AND NOT role.migration_rolinherit
      AND role.runtime_oid IS NOT NULL
      AND role.runtime_rolcanlogin
      AND NOT role.runtime_rolsuper
      AND NOT role.runtime_rolcreatedb
      AND NOT role.runtime_rolcreaterole
      AND NOT role.runtime_rolreplication
      AND NOT role.runtime_rolbypassrls
      AND NOT role.runtime_rolinherit AS counterpart_roles_restricted,
    role.migration_oid IS NOT NULL
      AND role.runtime_oid IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_auth_members AS membership
          WHERE membership.member IN (role.migration_oid, role.runtime_oid)
             OR membership.roleid IN (role.migration_oid, role.runtime_oid)
      ) AS counterpart_roles_have_no_memberships,
    role.migration_oid IS NOT NULL
      AND role.runtime_oid IS NOT NULL
      AND role.current_oid IS DISTINCT FROM role.migration_oid
      AND role.current_oid IS DISTINCT FROM role.runtime_oid
      AND role.migration_oid IS DISTINCT FROM role.runtime_oid
      AS roles_all_distinct,
    role.migration_oid IS NOT NULL
      AND role.datdba = role.migration_oid AS database_owned_by_migration,
    role.migration_oid IS NOT NULL
      AND role.nspowner = role.migration_oid AS schema_owned_by_migration,
    role.migration_oid IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_class AS relation
          WHERE relation.relnamespace = role.schema_oid
            AND relation.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'i', 'I')
            AND relation.relowner IS DISTINCT FROM role.migration_oid
          UNION ALL
          SELECT 1
          FROM pg_catalog.pg_proc AS routine
          WHERE routine.pronamespace = role.schema_oid
            AND routine.proowner IS DISTINCT FROM role.migration_oid
          UNION ALL
          SELECT 1
          FROM pg_catalog.pg_type AS owned_type
          WHERE owned_type.typnamespace = role.schema_oid
            AND owned_type.typowner IS DISTINCT FROM role.migration_oid
      ) AS target_objects_owned_by_migration,
    pg_catalog.current_setting('session_replication_role') = 'origin'
      AND NOT pg_catalog.has_parameter_privilege(
          role.current_oid, 'session_replication_role', 'SET'
      )
      AND NOT pg_catalog.has_parameter_privilege(
          role.current_oid, 'session_replication_role', 'ALTER SYSTEM'
      ) AS trigger_semantics_safe,
    pg_catalog.current_setting('lo_compat_privileges') = 'off'
      AND pg_catalog.current_setting('row_security') = 'on'
      AND pg_catalog.current_setting('local_preload_libraries') = ''
      AND {
    _role_and_database_settings_absent_sql(
        "role.current_oid",
        "role.database_oid",
    )
}
      AS session_security_settings_safe,
    {
    _target_execution_hooks_denied_sql(
        "role.schema_oid",
        "role.runtime_oid",
        "role.migration_oid",
    )
}
      AS target_execution_hooks_denied,
    role.migration_oid IS NOT NULL
      AND role.runtime_oid IS NOT NULL
      AND {
    _target_acl_exclusive_sql(
        "role.migration_oid",
        "role.runtime_oid",
        "role.current_oid",
        "role.auditor_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS target_acl_exclusive,
    {
    _checkpoint_auditor_access_contract_sql(
        "role.auditor_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS auditor_access_contract_exact,
    {
    _relation_access_contract_sql(
        "role.schema_oid",
        "role.runtime_oid",
        RUNTIME_RELATION_ACCESS,
        manifests_by_revision=RUNTIME_RELATION_ACCESS_BY_REVISION,
    )
}
      AND {
    _routine_execute_contract_sql(
        "role.schema_oid",
        "role.runtime_oid",
        RUNTIME_ROUTINE_EXECUTE_BY_REVISION,
    )
}
      AS runtime_access_contract_exact,
    {
    _maintenance_counterpart_privileges_sql(
        "role.current_oid",
        "role.schema_oid",
        "role.database_oid",
    )
}
      AS maintenance_privileges_safe
FROM role_context AS role
"""


SnapshotT = TypeVar(
    "SnapshotT",
    MigrationRoleSnapshot,
    RuntimeRoleSnapshot,
    MaintenanceRoleSnapshot,
)


def _safe_failure() -> DatabaseRoleError:
    return DatabaseRoleError("database_role_preflight_failed")


def _valid_contract(
    expected_runtime_role: object,
    expected_migration_role: object,
    expected_maintenance_role: object,
    expected_auditor_role: object,
    target_schema: object,
) -> bool:
    return (
        isinstance(expected_runtime_role, str)
        and isinstance(expected_migration_role, str)
        and isinstance(expected_maintenance_role, str)
        and isinstance(expected_auditor_role, str)
        and isinstance(target_schema, str)
        and bool(_IDENTIFIER.fullmatch(expected_runtime_role))
        and bool(_IDENTIFIER.fullmatch(expected_migration_role))
        and bool(_IDENTIFIER.fullmatch(expected_maintenance_role))
        and bool(_IDENTIFIER.fullmatch(expected_auditor_role))
        and bool(_IDENTIFIER.fullmatch(target_schema))
        and len(
            {
                expected_runtime_role,
                expected_migration_role,
                expected_maintenance_role,
                expected_auditor_role,
            }
        )
        == 4
    )


def _failed_invariants(
    snapshot: object,
    snapshot_type: type[SnapshotT],
) -> tuple[str, ...]:
    if not isinstance(snapshot, snapshot_type):
        return ("snapshot_type_invalid",)
    return tuple(
        field.name
        for field in fields(snapshot_type)
        if getattr(snapshot, field.name) is not True
    )


def _reject_failed_invariants(
    snapshot: object,
    snapshot_type: type[SnapshotT],
    identity_plane: str,
) -> None:
    failed = _failed_invariants(snapshot, snapshot_type)
    if not failed:
        return
    logger.error(
        "Database role preflight rejected: identity_plane=%s failed_invariants=%s",
        identity_plane,
        ",".join(failed),
    )
    raise _safe_failure()


async def _fetch_snapshot(
    dsn: str,
    query: str,
    params: tuple[str, ...],
    snapshot_type: type[SnapshotT],
    *,
    expected_search_path: str,
) -> SnapshotT:
    try:
        connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        async with connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    "SELECT pg_catalog.current_setting('search_path') "
                    "AS configured_search_path"
                )
                search_path_rows = await cursor.fetchall()
                if len(search_path_rows) != 1:
                    raise _safe_failure()
                configured_search_path = search_path_rows[0].get(
                    "configured_search_path"
                )
                if not isinstance(configured_search_path, str):
                    raise _safe_failure()

                # The migration connection uses only the target schema so
                # PostgreSQL resolves pg_catalog implicitly first while CREATE
                # still lands in the target. Catalog verification pins it
                # explicitly and never trusts the caller's resolution order.
                await cursor.execute("SET search_path TO pg_catalog")
                await cursor.execute(query, params)
                rows = await cursor.fetchall()
        if len(rows) != 1:
            raise _safe_failure()
        row = dict(rows[0])
        row["search_path_matches"] = (
            configured_search_path.replace(" ", "") == expected_search_path
        )
        return snapshot_type(
            **{
                field.name: row.get(field.name) is True
                for field in fields(snapshot_type)
            }
        )
    except DatabaseRoleError:
        raise
    except Exception:
        raise _safe_failure() from None


async def _read_migration_role_snapshot(
    dsn: str,
    *,
    expected_migration_role: str,
    expected_runtime_role: str,
    expected_maintenance_role: str,
    expected_auditor_role: str,
    target_schema: str,
) -> MigrationRoleSnapshot:
    return await _fetch_snapshot(
        dsn,
        _MIGRATION_ROLE_QUERY,
        (
            target_schema,
            expected_runtime_role,
            expected_maintenance_role,
            expected_auditor_role,
            expected_migration_role,
        ),
        MigrationRoleSnapshot,
        expected_search_path=target_schema,
    )


async def _read_runtime_role_snapshot(
    dsn: str,
    *,
    expected_runtime_role: str,
    expected_migration_role: str,
    expected_maintenance_role: str,
    expected_auditor_role: str,
    target_schema: str,
) -> RuntimeRoleSnapshot:
    return await _fetch_snapshot(
        dsn,
        _RUNTIME_ROLE_QUERY,
        (
            target_schema,
            expected_migration_role,
            expected_maintenance_role,
            expected_auditor_role,
            expected_runtime_role,
        ),
        RuntimeRoleSnapshot,
        expected_search_path=f"pg_catalog,{target_schema}",
    )


async def _read_maintenance_role_snapshot(
    dsn: str,
    *,
    expected_maintenance_role: str,
    expected_runtime_role: str,
    expected_migration_role: str,
    expected_auditor_role: str,
    target_schema: str,
) -> MaintenanceRoleSnapshot:
    return await _fetch_snapshot(
        dsn,
        _MAINTENANCE_ROLE_QUERY,
        (
            target_schema,
            expected_migration_role,
            expected_runtime_role,
            expected_auditor_role,
            expected_maintenance_role,
        ),
        MaintenanceRoleSnapshot,
        expected_search_path=f"pg_catalog,{target_schema}",
    )


async def require_migration_database_role(
    dsn: str,
    *,
    expected_migration_role: str,
    expected_runtime_role: str,
    expected_maintenance_role: str,
    expected_auditor_role: str,
    target_schema: str,
    allow_acl_reconciliation: bool = False,
) -> None:
    """Fail before DDL unless the connection is the restricted schema owner."""

    if not _valid_contract(
        expected_runtime_role,
        expected_migration_role,
        expected_maintenance_role,
        expected_auditor_role,
        target_schema,
    ):
        raise _safe_failure()
    try:
        snapshot = await _read_migration_role_snapshot(
            dsn,
            expected_migration_role=expected_migration_role,
            expected_runtime_role=expected_runtime_role,
            expected_maintenance_role=expected_maintenance_role,
            expected_auditor_role=expected_auditor_role,
            target_schema=target_schema,
        )
    except Exception:
        raise _safe_failure() from None
    failed = _failed_invariants(snapshot, MigrationRoleSnapshot)
    if allow_acl_reconciliation:
        failed = tuple(
            invariant
            for invariant in failed
            if invariant
            not in {
                "runtime_access_contract_exact",
                "maintenance_counterpart_privileges_safe",
                "auditor_access_contract_exact",
            }
        )
    if failed:
        logger.error(
            "Database role preflight rejected: identity_plane=%s failed_invariants=%s",
            "migration",
            ",".join(failed),
        )
        raise _safe_failure()


async def require_runtime_database_role(
    dsn: str,
    *,
    expected_runtime_role: str,
    expected_migration_role: str,
    expected_maintenance_role: str,
    expected_auditor_role: str,
    target_schema: str,
) -> None:
    """Fail unless the runtime session is restricted and cannot perform DDL."""

    if not _valid_contract(
        expected_runtime_role,
        expected_migration_role,
        expected_maintenance_role,
        expected_auditor_role,
        target_schema,
    ):
        raise _safe_failure()
    try:
        snapshot = await _read_runtime_role_snapshot(
            dsn,
            expected_runtime_role=expected_runtime_role,
            expected_migration_role=expected_migration_role,
            expected_maintenance_role=expected_maintenance_role,
            expected_auditor_role=expected_auditor_role,
            target_schema=target_schema,
        )
    except Exception:
        raise _safe_failure() from None
    _reject_failed_invariants(
        snapshot,
        RuntimeRoleSnapshot,
        "runtime",
    )


async def require_maintenance_database_role(
    dsn: str,
    *,
    expected_maintenance_role: str,
    expected_runtime_role: str,
    expected_migration_role: str,
    expected_auditor_role: str,
    target_schema: str,
) -> None:
    """Fail unless checkpoint cleanup uses its exact restricted identity."""

    if not _valid_contract(
        expected_runtime_role,
        expected_migration_role,
        expected_maintenance_role,
        expected_auditor_role,
        target_schema,
    ):
        raise _safe_failure()
    try:
        snapshot = await _read_maintenance_role_snapshot(
            dsn,
            expected_maintenance_role=expected_maintenance_role,
            expected_runtime_role=expected_runtime_role,
            expected_migration_role=expected_migration_role,
            expected_auditor_role=expected_auditor_role,
            target_schema=target_schema,
        )
    except Exception:
        raise _safe_failure() from None
    _reject_failed_invariants(
        snapshot,
        MaintenanceRoleSnapshot,
        "maintenance",
    )
