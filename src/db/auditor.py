"""Fail-closed preflight for the read-only checkpoint-plan identity."""

from __future__ import annotations

import logging
import re
from typing import Final

import psycopg

from src.db.roles import (
    _checkpoint_auditor_access_contract_sql,
    _system_initial_acl_unchanged_sql,
    _target_acl_exclusive_sql,
)


logger = logging.getLogger(__name__)
_IDENTIFIER: Final = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")


class CheckpointAuditorRoleError(RuntimeError):
    """Fixed-text role error that never exposes identifiers or credentials."""

    def __init__(self) -> None:
        super().__init__("checkpoint_auditor_role_preflight_failed")


def _reject() -> CheckpointAuditorRoleError:
    return CheckpointAuditorRoleError()


async def require_checkpoint_auditor_database_role(
    dsn: str,
    *,
    expected_auditor_role: str,
    expected_runtime_role: str,
    expected_migration_role: str,
    expected_maintenance_role: str,
    target_schema: str,
) -> None:
    """Require one NOINHERIT login with only the plan's direct column grants."""

    identities = {
        expected_auditor_role,
        expected_runtime_role,
        expected_migration_role,
        expected_maintenance_role,
    }
    if (
        len(identities) != 4
        or any(not _IDENTIFIER.fullmatch(value) for value in identities)
        or not _IDENTIFIER.fullmatch(target_schema)
    ):
        raise _reject()

    try:
        async with await psycopg.AsyncConnection.connect(
            dsn,
            autocommit=True,
            prepare_threshold=0,
        ) as conn:
            row = await (
                await conn.execute(
                    f"""
                    WITH identity AS (
                        SELECT
                            auditor_role.oid AS current_oid,
                            runtime_role.oid AS runtime_oid,
                            migration_role.oid AS migration_oid,
                            maintenance_role.oid AS maintenance_oid,
                            database.oid AS database_oid,
                            target_schema.oid AS schema_oid,
                            runtime_role.rolcanlogin
                              AND NOT runtime_role.rolsuper
                              AND NOT runtime_role.rolcreatedb
                              AND NOT runtime_role.rolcreaterole
                              AND NOT runtime_role.rolreplication
                              AND NOT runtime_role.rolbypassrls
                              AND NOT runtime_role.rolinherit
                              AS runtime_restricted,
                            migration_role.rolcanlogin
                              AND NOT migration_role.rolsuper
                              AND NOT migration_role.rolcreatedb
                              AND NOT migration_role.rolcreaterole
                              AND NOT migration_role.rolreplication
                              AND NOT migration_role.rolbypassrls
                              AND NOT migration_role.rolinherit
                              AS migration_restricted,
                            maintenance_role.rolcanlogin
                              AND NOT maintenance_role.rolsuper
                              AND NOT maintenance_role.rolcreatedb
                              AND NOT maintenance_role.rolcreaterole
                              AND NOT maintenance_role.rolreplication
                              AND NOT maintenance_role.rolbypassrls
                              AND NOT maintenance_role.rolinherit
                              AS maintenance_restricted
                        FROM pg_catalog.pg_roles AS auditor_role
                        LEFT JOIN pg_catalog.pg_roles AS runtime_role
                          ON runtime_role.rolname = %s
                        LEFT JOIN pg_catalog.pg_roles AS migration_role
                          ON migration_role.rolname = %s
                        LEFT JOIN pg_catalog.pg_roles AS maintenance_role
                          ON maintenance_role.rolname = %s
                        JOIN pg_catalog.pg_database AS database
                          ON database.datname = pg_catalog.current_database()
                        LEFT JOIN pg_catalog.pg_namespace AS target_schema
                          ON target_schema.nspname = %s
                        WHERE auditor_role.rolname = %s
                    )
                    SELECT
                        session_user = current_user
                          AND current_user = %s,
                        identity.schema_oid IS NOT NULL,
                        identity.runtime_oid IS NOT NULL
                          AND identity.migration_oid IS NOT NULL
                          AND identity.maintenance_oid IS NOT NULL,
                        identity.runtime_restricted
                          AND identity.migration_restricted
                          AND identity.maintenance_restricted,
                        identity.runtime_oid IS NOT NULL
                          AND identity.migration_oid IS NOT NULL
                          AND identity.maintenance_oid IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1
                              FROM pg_catalog.pg_auth_members AS membership
                              WHERE membership.member IN (
                                  identity.runtime_oid,
                                  identity.migration_oid,
                                  identity.maintenance_oid
                              ) OR membership.roleid IN (
                                  identity.runtime_oid,
                                  identity.migration_oid,
                                  identity.maintenance_oid
                              )
                          ),
                        identity.current_oid IS DISTINCT FROM identity.runtime_oid
                          AND identity.current_oid IS DISTINCT FROM identity.migration_oid
                          AND identity.current_oid
                              IS DISTINCT FROM identity.maintenance_oid
                          AND identity.runtime_oid
                              IS DISTINCT FROM identity.migration_oid
                          AND identity.runtime_oid
                              IS DISTINCT FROM identity.maintenance_oid
                          AND identity.migration_oid
                              IS DISTINCT FROM identity.maintenance_oid,
                        pg_catalog.current_schemas(false)::pg_catalog.text[]
                          = ARRAY['pg_catalog', %s]::pg_catalog.text[],
                        {
                        _target_acl_exclusive_sql(
                            "identity.migration_oid",
                            "identity.runtime_oid",
                            "identity.maintenance_oid",
                            "identity.current_oid",
                            "identity.schema_oid",
                            "identity.database_oid",
                        )
                    },
                        {
                        _system_initial_acl_unchanged_sql(
                            "identity.current_oid",
                            "identity.schema_oid",
                        )
                    },
                        {
                        _checkpoint_auditor_access_contract_sql(
                            "identity.current_oid",
                            "identity.schema_oid",
                            "identity.database_oid",
                        )
                    }
                    FROM identity
                    """,
                    (
                        expected_runtime_role,
                        expected_migration_role,
                        expected_maintenance_role,
                        target_schema,
                        expected_auditor_role,
                        expected_auditor_role,
                        target_schema,
                    ),
                )
            ).fetchone()
    except (psycopg.Error, OSError, TypeError, ValueError) as exc:
        logger.error(
            "Checkpoint auditor role snapshot failed: error_type=%s",
            type(exc).__name__,
        )
        raise _reject() from None

    invariant_names = (
        "direct_expected_session",
        "target_schema_exists",
        "counterpart_roles_exist",
        "counterpart_roles_restricted",
        "counterpart_roles_have_no_memberships",
        "roles_all_distinct",
        "search_path_exact",
        "target_acl_exclusive",
        "system_public_acl_unchanged",
        "auditor_access_contract_exact",
    )
    if row is None or not all(value is True for value in row):
        failed = (
            "snapshot_unavailable"
            if row is None
            else ",".join(
                name
                for name, value in zip(invariant_names, row, strict=True)
                if value is not True
            )
        )
        logger.error(
            "Checkpoint auditor role preflight rejected: failed_invariants=%s",
            failed,
        )
        raise _reject()
