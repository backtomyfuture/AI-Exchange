"""Read-only PostgreSQL identity and privilege preflight checks."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, fields
from typing import Final, TypeVar

import psycopg
from psycopg.rows import dict_row


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
    schema_oid: str,
    database_oid: str,
) -> str:
    """Allow target data-plane ACLs only for the two managed identities."""

    allowed = f"({migration_oid}, {runtime_oid})"
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


def _target_execution_hooks_denied_sql(
    schema_oid: str,
    runtime_oid: str,
) -> str:
    """Deny hidden target-schema execution paths until explicitly allowlisted."""

    return f"""NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS routine
        WHERE routine.pronamespace = {schema_oid}
          AND routine.prosecdef
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_trigger AS trigger
        JOIN pg_catalog.pg_class AS relation
          ON relation.oid = trigger.tgrelid
        WHERE relation.relnamespace = {schema_oid}
          AND NOT trigger.tgisinternal
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_constraint AS foreign_key
        JOIN pg_catalog.pg_class AS referencing_relation
          ON referencing_relation.oid = foreign_key.conrelid
        JOIN pg_catalog.pg_class AS referenced_relation
          ON referenced_relation.oid = foreign_key.confrelid
        WHERE foreign_key.contype = 'f'
          AND (
              referencing_relation.relnamespace = {schema_oid}
              OR referenced_relation.relnamespace = {schema_oid}
          )
        UNION ALL
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
    )"""


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
        AND NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_proc AS routine
            WHERE routine.pronamespace = {schema_oid}
              AND pg_catalog.has_function_privilege(
                  {role_oid}, routine.oid, 'EXECUTE'
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
    roles_distinct: bool
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
    system_public_acl_unchanged: bool
    runtime_counterpart_privileges_safe: bool
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
    roles_distinct: bool
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
    delegation_privileges_denied: bool
    system_public_acl_unchanged: bool
    migration_counterpart_privileges_safe: bool
    database_connect_allowed: bool
    database_create_denied: bool
    database_temp_denied: bool
    schema_usage_allowed: bool
    schema_create_denied: bool
    dangerous_relation_privileges_denied: bool
    sequence_update_denied: bool
    audit_permissions_valid: bool
    search_path_matches: bool


_MIGRATION_ROLE_QUERY: Final = f"""
WITH role_context AS (
    SELECT
        role.oid AS current_oid,
        runtime_role.oid AS runtime_oid,
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
        runtime_role.rolinherit AS runtime_rolinherit
    FROM pg_catalog.pg_roles AS role
    JOIN pg_catalog.pg_database AS database
      ON database.datname = pg_catalog.current_database()
    LEFT JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.nspname = %s
    LEFT JOIN pg_catalog.pg_roles AS runtime_role
      ON runtime_role.rolname = %s
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
    role.current_oid IS DISTINCT FROM role.runtime_oid AS roles_distinct,
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
      AND {_role_and_database_settings_absent_sql("role.current_oid", "role.database_oid")}
      AS session_security_settings_safe,
    {_large_object_creation_denied_sql("role.current_oid")}
      AS large_object_creation_denied,
    {_target_execution_hooks_denied_sql("role.schema_oid", "role.runtime_oid")}
      AS target_execution_hooks_denied,
    {_unexpected_direct_grants_sql("role.current_oid", "role.schema_oid", "role.database_oid")}
      AS unexpected_direct_grants_denied,
    {_unexpected_ownership_sql("role.current_oid", "role.schema_oid", "role.database_oid")}
      AS unexpected_object_ownership_denied,
    {_other_schema_create_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_schema_create_denied,
    {_other_database_connect_denied_sql("role.current_oid", "role.database_oid")}
      AS other_database_connect_denied,
    {_other_user_schema_usage_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_user_schema_usage_denied,
    role.runtime_oid IS NOT NULL AND
      {_target_acl_exclusive_sql("role.current_oid", "role.runtime_oid", "role.schema_oid", "role.database_oid")}
      AS target_acl_exclusive,
    {_system_initial_acl_unchanged_sql("role.current_oid", "role.schema_oid")}
      AS system_public_acl_unchanged,
    role.runtime_oid IS NOT NULL AND
      {_runtime_counterpart_privileges_sql("role.runtime_oid", "role.schema_oid", "role.database_oid")}
      AS runtime_counterpart_privileges_safe
FROM role_context AS role
"""


_RUNTIME_ROLE_QUERY: Final = f"""
WITH role_context AS (
    SELECT
        role.oid AS current_oid,
        migration_role.oid AS migration_oid,
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
        migration_role.rolinherit AS migration_rolinherit
    FROM pg_catalog.pg_roles AS role
    JOIN pg_catalog.pg_database AS database
      ON database.datname = pg_catalog.current_database()
    LEFT JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.nspname = %s
    LEFT JOIN pg_catalog.pg_roles AS migration_role
      ON migration_role.rolname = %s
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
    role.current_oid IS DISTINCT FROM role.migration_oid AS roles_distinct,
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
    role.migration_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS routine
        WHERE routine.pronamespace = role.schema_oid
          AND pg_catalog.has_function_privilege(
              role.current_oid, routine.oid, 'EXECUTE'
          )
    ) AS routines_execute_denied,
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
      AND {_role_and_database_settings_absent_sql("role.current_oid", "role.database_oid")}
      AS session_security_settings_safe,
    {_large_object_creation_denied_sql("role.current_oid")}
      AS large_object_creation_denied,
    {_target_execution_hooks_denied_sql("role.schema_oid", "role.current_oid")}
      AS target_execution_hooks_denied,
    {_unexpected_direct_grants_sql("role.current_oid", "role.schema_oid", "role.database_oid")}
      AS unexpected_direct_grants_denied,
    {_unexpected_ownership_sql("role.current_oid", "role.schema_oid", "role.database_oid")}
      AS unexpected_object_ownership_denied,
    {_other_schema_create_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_schema_create_denied,
    {_other_database_connect_denied_sql("role.current_oid", "role.database_oid")}
      AS other_database_connect_denied,
    {_other_user_schema_usage_denied_sql("role.current_oid", "role.schema_oid")}
      AS other_user_schema_usage_denied,
    role.migration_oid IS NOT NULL AND
      {_target_acl_exclusive_sql("role.migration_oid", "role.current_oid", "role.schema_oid", "role.database_oid")}
      AS target_acl_exclusive,
    {_delegation_denied_sql("role.current_oid", "role.schema_oid", "role.database_oid")}
      AS delegation_privileges_denied,
    {_system_initial_acl_unchanged_sql("role.current_oid", "role.schema_oid")}
      AS system_public_acl_unchanged,
    role.migration_oid IS NOT NULL AND
      {_migration_counterpart_privileges_sql("role.migration_oid", "role.schema_oid", "role.database_oid")}
      AS migration_counterpart_privileges_safe,
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
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS audit_relation
        WHERE audit_relation.relnamespace = role.schema_oid
          AND audit_relation.relname = 'audit_events'
          AND (
              audit_relation.relkind NOT IN ('r', 'p')
              OR NOT pg_catalog.has_table_privilege(
                  role.current_oid, audit_relation.oid, 'SELECT'
              )
              OR NOT pg_catalog.has_table_privilege(
                  role.current_oid, audit_relation.oid, 'INSERT'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid,
                  audit_relation.oid,
                  'SELECT WITH GRANT OPTION'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid,
                  audit_relation.oid,
                  'INSERT WITH GRANT OPTION'
              )
              OR pg_catalog.has_any_column_privilege(
                  role.current_oid,
                  audit_relation.oid,
                  'SELECT WITH GRANT OPTION'
              )
              OR pg_catalog.has_any_column_privilege(
                  role.current_oid,
                  audit_relation.oid,
                  'INSERT WITH GRANT OPTION'
              )
              OR pg_catalog.has_any_column_privilege(
                  role.current_oid, audit_relation.oid, 'UPDATE'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid, audit_relation.oid, 'UPDATE'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid, audit_relation.oid, 'DELETE'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid, audit_relation.oid, 'TRUNCATE'
              )
              OR pg_catalog.has_table_privilege(
                  role.current_oid, audit_relation.oid, 'TRIGGER'
              )
          )
    ) AS audit_permissions_valid
FROM role_context AS role
"""


SnapshotT = TypeVar("SnapshotT", MigrationRoleSnapshot, RuntimeRoleSnapshot)


def _safe_failure() -> DatabaseRoleError:
    return DatabaseRoleError("database_role_preflight_failed")


def _valid_contract(
    expected_runtime_role: object,
    expected_migration_role: object,
    target_schema: object,
) -> bool:
    return (
        isinstance(expected_runtime_role, str)
        and isinstance(expected_migration_role, str)
        and isinstance(target_schema, str)
        and bool(_IDENTIFIER.fullmatch(expected_runtime_role))
        and bool(_IDENTIFIER.fullmatch(expected_migration_role))
        and bool(_IDENTIFIER.fullmatch(target_schema))
        and expected_runtime_role != expected_migration_role
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
    target_schema: str,
) -> MigrationRoleSnapshot:
    return await _fetch_snapshot(
        dsn,
        _MIGRATION_ROLE_QUERY,
        (
            target_schema,
            expected_runtime_role,
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
    target_schema: str,
) -> RuntimeRoleSnapshot:
    return await _fetch_snapshot(
        dsn,
        _RUNTIME_ROLE_QUERY,
        (
            target_schema,
            expected_migration_role,
            expected_runtime_role,
        ),
        RuntimeRoleSnapshot,
        expected_search_path=f"pg_catalog,{target_schema}",
    )


async def require_migration_database_role(
    dsn: str,
    *,
    expected_migration_role: str,
    expected_runtime_role: str,
    target_schema: str,
) -> None:
    """Fail before DDL unless the connection is the restricted schema owner."""

    if not _valid_contract(
        expected_runtime_role, expected_migration_role, target_schema
    ):
        raise _safe_failure()
    try:
        snapshot = await _read_migration_role_snapshot(
            dsn,
            expected_migration_role=expected_migration_role,
            expected_runtime_role=expected_runtime_role,
            target_schema=target_schema,
        )
    except Exception:
        raise _safe_failure() from None
    _reject_failed_invariants(
        snapshot,
        MigrationRoleSnapshot,
        "migration",
    )


async def require_runtime_database_role(
    dsn: str,
    *,
    expected_runtime_role: str,
    expected_migration_role: str,
    target_schema: str,
) -> None:
    """Fail unless the runtime session is restricted and cannot perform DDL."""

    if not _valid_contract(
        expected_runtime_role, expected_migration_role, target_schema
    ):
        raise _safe_failure()
    try:
        snapshot = await _read_runtime_role_snapshot(
            dsn,
            expected_runtime_role=expected_runtime_role,
            expected_migration_role=expected_migration_role,
            target_schema=target_schema,
        )
    except Exception:
        raise _safe_failure() from None
    _reject_failed_invariants(
        snapshot,
        RuntimeRoleSnapshot,
        "runtime",
    )
