"""One-shot PostgreSQL cluster provisioning for a fresh polling deployment.

This is deliberately separate from schema bootstrap.  It is the only container
that receives the PostgreSQL administrator credential and it refuses to operate
once the target database contains user objects.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pydantic import SecretStr

from src.db.migration_settings import _read_secret_file
from src.db.roles import require_migration_database_role


logger = logging.getLogger(__name__)

_IDENTIFIER: Final = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z", re.ASCII)
_TARGET_SCHEMA: Final = "public"
_SYSTEM_DATABASES: Final = frozenset({"postgres", "template0", "template1"})
_PROVISION_LOCK_KEY: Final = 4_149_584_368_895_813
_PASSWORD_MIN_LENGTH: Final = 16
_PASSWORD_MAX_LENGTH: Final = 1024
_PG15_BASELINE_MEMBERSHIPS: Final = (
    ("pg_read_all_settings", "pg_monitor"),
    ("pg_read_all_stats", "pg_monitor"),
    ("pg_stat_scan_tables", "pg_monitor"),
)
_ROLE_PASSWORD_FILE_ENV: Final = (
    ("POSTGRES_MIGRATION_OWNER_ROLE", "POSTGRES_MIGRATION_PASSWORD_FILE"),
    ("POSTGRES_RUNTIME_ROLE", "POSTGRES_RUNTIME_PASSWORD_FILE"),
    ("POSTGRES_MAINTENANCE_ROLE", "POSTGRES_MAINTENANCE_PASSWORD_FILE"),
    (
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE",
        "POSTGRES_CHECKPOINT_AUDITOR_PASSWORD_FILE",
    ),
)
_LARGE_OBJECT_CREATION_ROUTINES: Final = (
    ("lo_creat", ("int4",)),
    ("lo_create", ("oid",)),
    ("lo_from_bytea", ("oid", "bytea")),
    ("lo_import", ("text",)),
    ("lo_import", ("text", "oid")),
)


class DatabaseProvisionError(RuntimeError):
    """Non-secret failure raised by the administrator-only one-shot."""


@dataclass(frozen=True, slots=True)
class ManagedRole:
    name: str
    password: SecretStr


@dataclass(frozen=True, slots=True)
class ProvisionSettings:
    admin_database_url: SecretStr
    database_name: str
    target_schema: str
    migration: ManagedRole
    runtime: ManagedRole
    maintenance: ManagedRole
    auditor: ManagedRole

    @property
    def roles(self) -> tuple[ManagedRole, ...]:
        return (self.migration, self.runtime, self.maintenance, self.auditor)


def _reject() -> DatabaseProvisionError:
    return DatabaseProvisionError("database_provision_invalid")


def _identifier(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "")
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise _reject()
    return value


def _password_from_file(values: Mapping[str, str], name: str) -> SecretStr:
    password = _read_secret_file(values.get(name, ""))
    if not _PASSWORD_MIN_LENGTH <= len(password) <= _PASSWORD_MAX_LENGTH or any(
        ord(character) < 32 or ord(character) == 127 for character in password
    ):
        raise _reject()
    return SecretStr(password)


def load_provision_settings(
    environment: Mapping[str, str] | None = None,
) -> ProvisionSettings:
    """Load only private file-backed credentials for the fresh provisioner."""

    values = os.environ if environment is None else environment
    try:
        database_name = _identifier(values, "POSTGRES_DB")
        target_schema = _identifier(values, "POSTGRES_SCHEMA")
        if target_schema != _TARGET_SCHEMA or database_name in _SYSTEM_DATABASES:
            raise _reject()

        managed_roles = tuple(
            ManagedRole(
                name=_identifier(values, role_env),
                password=_password_from_file(values, password_file_env),
            )
            for role_env, password_file_env in _ROLE_PASSWORD_FILE_ENV
        )
        role_names = tuple(role.name for role in managed_roles)
        passwords = tuple(role.password.get_secret_value() for role in managed_roles)
        if len(set(role_names)) != len(role_names) or len(set(passwords)) != len(
            passwords
        ):
            raise _reject()

        admin_url = _read_secret_file(
            values.get("DATABASE_PROVISION_ADMIN_URL_FILE", "")
        )
        parsed = conninfo_to_dict(admin_url)
        admin_user = parsed.get("user", "")
        admin_password = parsed.get("password", "")
        if (
            not admin_user
            or admin_user in role_names
            or not admin_password
            or admin_password in passwords
            or not parsed.get("host")
            or parsed.get("dbname") != database_name
            or parsed.get("options") not in {None, ""}
            or parsed.get("service")
            or parsed.get("passfile")
        ):
            raise _reject()
    except DatabaseProvisionError:
        raise
    except Exception:
        raise _reject() from None

    return ProvisionSettings(
        admin_database_url=SecretStr(admin_url),
        database_name=database_name,
        target_schema=target_schema,
        migration=managed_roles[0],
        runtime=managed_roles[1],
        maintenance=managed_roles[2],
        auditor=managed_roles[3],
    )


def _require_true_row(
    cursor: Any, statement: str, parameters: tuple[object, ...]
) -> None:
    cursor.execute(statement, parameters)
    row = cursor.fetchone()
    if row != (True,):
        raise _reject()


def _require_admin_boundary(cursor: Any, settings: ProvisionSettings) -> None:
    cursor.execute(
        "SELECT "
        "  pg_catalog.current_database() = %s, "
        "  session_user = current_user, "
        "  administrator.rolsuper, "
        "  NOT database.datistemplate, "
        "  database.datallowconn, "
        "  namespace.oid IS NOT NULL "
        "FROM pg_catalog.pg_roles AS administrator "
        "JOIN pg_catalog.pg_database AS database "
        "  ON database.datname = pg_catalog.current_database() "
        "LEFT JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = %s "
        "WHERE administrator.rolname = current_user",
        (settings.database_name, settings.target_schema),
    )
    row = cursor.fetchone()
    if row != (True, True, True, True, True, True):
        raise _reject()


def _require_dedicated_cluster(cursor: Any, settings: ProvisionSettings) -> None:
    """Accept only the exact database set created by the official Compose image."""

    if settings.database_name in _SYSTEM_DATABASES:
        raise _reject()
    cursor.execute(
        "SELECT database.datname::pg_catalog.text, "
        "       database.datistemplate, database.datallowconn "
        "FROM pg_catalog.pg_database AS database "
        "ORDER BY database.datname"
    )
    actual = {
        str(name): (bool(is_template), bool(allows_connections))
        for name, is_template, allows_connections in cursor.fetchall()
    }
    expected = {
        settings.database_name: (False, True),
        "postgres": (False, True),
        "template0": (True, False),
        "template1": (True, True),
    }
    if actual != expected:
        raise _reject()


def _require_fresh_target(
    cursor: Any,
    settings: ProvisionSettings,
    *,
    allow_provision_defaults: bool,
) -> None:
    """Reject databases that have crossed the empty greenfield boundary."""

    _require_true_row(
        cursor,
        "SELECT "
        "NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_namespace AS namespace "
        "  WHERE namespace.nspname <> %s "
        "    AND namespace.nspname <> 'information_schema' "
        "    AND namespace.nspname NOT LIKE 'pg\\_%%' ESCAPE '\\'"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_class AS object "
        "  JOIN pg_catalog.pg_namespace AS namespace "
        "    ON namespace.oid = object.relnamespace "
        "  WHERE namespace.nspname = %s"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_proc AS object "
        "  JOIN pg_catalog.pg_namespace AS namespace "
        "    ON namespace.oid = object.pronamespace "
        "  WHERE namespace.nspname = %s"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_type AS object "
        "  JOIN pg_catalog.pg_namespace AS namespace "
        "    ON namespace.oid = object.typnamespace "
        "  WHERE namespace.nspname = %s"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_extension AS object "
        "  JOIN pg_catalog.pg_namespace AS namespace "
        "    ON namespace.oid = object.extnamespace "
        "  WHERE namespace.nspname = %s"
        ") AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_largeobject_metadata) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_foreign_server) "
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_user_mapping) "
        "AND (%s OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_default_acl))",
        (
            settings.target_schema,
            settings.target_schema,
            settings.target_schema,
            settings.target_schema,
            settings.target_schema,
            allow_provision_defaults,
        ),
    )


def _existing_role_names(cursor: Any, settings: ProvisionSettings) -> set[str]:
    names = [role.name for role in settings.roles]
    cursor.execute(
        "SELECT role.rolname::pg_catalog.text "
        "FROM pg_catalog.pg_roles AS role "
        "WHERE role.rolname = ANY(%s::pg_catalog.text[]) "
        "ORDER BY role.rolname",
        (names,),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def _require_cluster_authority_baseline(
    cursor: Any,
    settings: ProvisionSettings,
) -> None:
    """Reject cluster authority that an official fresh PostgreSQL 15 lacks."""

    role_names = [role.name for role in settings.roles]
    membership_roles = [pair[0] for pair in _PG15_BASELINE_MEMBERSHIPS]
    membership_members = [pair[1] for pair in _PG15_BASELINE_MEMBERSHIPS]
    _require_true_row(
        cursor,
        "WITH expected_membership AS ("
        "  SELECT roles.role_name, members.member_name "
        "  FROM pg_catalog.unnest(%s::pg_catalog.text[]) WITH ORDINALITY "
        "    AS roles(role_name, position) "
        "  JOIN pg_catalog.unnest(%s::pg_catalog.text[]) WITH ORDINALITY "
        "    AS members(member_name, position) USING (position)"
        "), actual_membership AS ("
        "  SELECT parent.rolname::pg_catalog.text AS role_name, "
        "         member.rolname::pg_catalog.text AS member_name, "
        "         grantor.rolname::pg_catalog.text AS grantor_name, "
        "         membership.admin_option "
        "  FROM pg_catalog.pg_auth_members AS membership "
        "  JOIN pg_catalog.pg_roles AS parent ON parent.oid = membership.roleid "
        "  JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member "
        "  JOIN pg_catalog.pg_roles AS grantor ON grantor.oid = membership.grantor"
        ") "
        "SELECT "
        "NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_roles AS role "
        "  WHERE role.rolname NOT LIKE 'pg\\_%%' ESCAPE '\\' "
        "    AND role.rolname <> current_user "
        "    AND role.rolname <> ALL(%s::pg_catalog.text[])"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM actual_membership AS actual "
        "  LEFT JOIN expected_membership AS expected "
        "    ON expected.role_name = actual.role_name "
        "   AND expected.member_name = actual.member_name "
        "  WHERE expected.role_name IS NULL "
        "     OR actual.grantor_name <> current_user "
        "     OR actual.admin_option"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM expected_membership AS expected "
        "  LEFT JOIN actual_membership AS actual "
        "    ON actual.role_name = expected.role_name "
        "   AND actual.member_name = expected.member_name "
        "  WHERE actual.role_name IS NULL"
        ") AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_db_role_setting)",
        (membership_roles, membership_members, role_names),
    )


def _require_target_authority_baseline(
    cursor: Any,
    settings: ProvisionSettings,
    *,
    provisioned_retry: bool,
) -> None:
    """Accept exactly the official PG15 fresh or completed-provision ACL shape."""

    role_names = [role.name for role in settings.roles]
    _require_true_row(
        cursor,
        "WITH managed AS ("
        "  SELECT role.oid, role.rolname "
        "  FROM pg_catalog.pg_roles AS role "
        "  WHERE role.rolname = ANY(%s::pg_catalog.text[])"
        "), target AS ("
        "  SELECT database.datdba, database.datacl, "
        "         namespace.nspowner, namespace.nspacl "
        "  FROM pg_catalog.pg_database AS database "
        "  JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = %s "
        "  WHERE database.datname = pg_catalog.current_database()"
        "), database_acl AS ("
        "  SELECT acl.* FROM target "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(target.datacl) AS acl"
        "), schema_acl AS ("
        "  SELECT acl.* FROM target "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(target.nspacl) AS acl"
        ") "
        "SELECT CASE WHEN %s THEN ("
        "  target.datdba = ("
        "    SELECT oid FROM managed WHERE rolname = %s"
        "  ) AND pg_catalog.cardinality(target.datacl) = 4 "
        "  AND ("
        "    SELECT pg_catalog.count(*) = 6 AND pg_catalog.bool_and("
        "      NOT acl.is_grantable "
        "      AND acl.grantor = ("
        "        SELECT oid FROM managed WHERE rolname = %s"
        "      ) AND ("
        "        (acl.grantee = ("
        "          SELECT oid FROM managed WHERE rolname = %s"
        "        ) AND acl.privilege_type IN ('CONNECT', 'CREATE', 'TEMPORARY')) "
        "        OR (acl.grantee IN ("
        "          SELECT oid FROM managed WHERE rolname <> %s"
        "        ) AND acl.privilege_type = 'CONNECT')"
        "      )"
        "    ) FROM database_acl AS acl"
        "  ) AND target.nspowner = ("
        "    SELECT oid FROM managed WHERE rolname = %s"
        "  ) AND pg_catalog.cardinality(target.nspacl) = 3 "
        "  AND ("
        "    SELECT pg_catalog.count(*) = 4 AND pg_catalog.bool_and("
        "      NOT acl.is_grantable "
        "      AND acl.grantor = ("
        "        SELECT oid FROM managed WHERE rolname = %s"
        "      ) AND ("
        "        (acl.grantee = ("
        "          SELECT oid FROM managed WHERE rolname = %s"
        "        ) AND acl.privilege_type IN ('CREATE', 'USAGE')) "
        "        OR (acl.grantee IN ("
        "          SELECT oid FROM managed "
        "          WHERE rolname = ANY(%s::pg_catalog.text[])"
        "        ) AND acl.privilege_type = 'USAGE')"
        "      )"
        "    ) FROM schema_acl AS acl"
        "  )"
        ") ELSE ("
        "  target.datdba = ("
        "    SELECT oid FROM pg_catalog.pg_roles "
        "    WHERE rolname = current_user"
        "  ) AND target.datacl IS NULL "
        "  AND target.nspowner = ("
        "    SELECT oid FROM pg_catalog.pg_roles "
        "    WHERE rolname = 'pg_database_owner'"
        "  ) AND pg_catalog.cardinality(target.nspacl) = 2 "
        "  AND ("
        "    SELECT pg_catalog.count(*) = 3 AND pg_catalog.bool_and("
        "      NOT acl.is_grantable AND acl.grantor = target.nspowner AND ("
        "        (acl.grantee = target.nspowner "
        "         AND acl.privilege_type IN ('CREATE', 'USAGE')) "
        "        OR (acl.grantee = 0 AND acl.privilege_type = 'USAGE')"
        "      )"
        "    ) FROM schema_acl AS acl"
        "  )"
        ") END FROM target",
        (
            role_names,
            settings.target_schema,
            provisioned_retry,
            settings.migration.name,
            settings.migration.name,
            settings.migration.name,
            settings.migration.name,
            settings.migration.name,
            settings.migration.name,
            settings.migration.name,
            [settings.runtime.name, settings.maintenance.name],
        ),
    )


def _require_existing_roles_safe(
    cursor: Any,
    settings: ProvisionSettings,
    existing: set[str],
) -> None:
    """Allow a retry, but never adopt roles already used outside this empty DB."""

    if not existing:
        return
    expected_names = {role.name for role in settings.roles}
    if existing != expected_names:
        raise _reject()
    role_names = sorted(existing)
    _require_true_row(
        cursor,
        "WITH managed AS ("
        "  SELECT role.oid, role.rolname "
        "  FROM pg_catalog.pg_roles AS role "
        "  WHERE role.rolname = ANY(%s::pg_catalog.text[])"
        "), target AS ("
        "  SELECT database.oid AS database_oid, namespace.oid AS schema_oid "
        "  FROM pg_catalog.pg_database AS database "
        "  JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = %s "
        "  WHERE database.datname = pg_catalog.current_database()"
        ") "
        "SELECT "
        "NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_auth_members AS membership "
        "  WHERE membership.member IN (SELECT oid FROM managed) "
        "     OR membership.roleid IN (SELECT oid FROM managed)"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_db_role_setting AS setting "
        "  WHERE setting.setrole IN (SELECT oid FROM managed)"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_database AS database "
        "  WHERE database.datdba IN (SELECT oid FROM managed) "
        "    AND NOT ("
        "      database.datname = pg_catalog.current_database() "
        "      AND database.datdba = ("
        "        SELECT oid FROM managed WHERE rolname = %s"
        "      )"
        "    )"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_shdepend AS dependency "
        "  CROSS JOIN target "
        "  WHERE dependency.refclassid = 'pg_catalog.pg_authid'::pg_catalog.regclass "
        "    AND dependency.refobjid IN (SELECT oid FROM managed) "
        "    AND NOT ("
        "      dependency.dbid = 0 "
        "      AND dependency.classid = "
        "          'pg_catalog.pg_database'::pg_catalog.regclass"
        "      AND dependency.objid = target.database_oid"
        "    ) "
        "    AND NOT ("
        "      dependency.dbid = target.database_oid "
        "      AND dependency.classid = "
        "          'pg_catalog.pg_namespace'::pg_catalog.regclass "
        "      AND dependency.objid = target.schema_oid"
        "    ) "
        "    AND NOT ("
        "      dependency.dbid = target.database_oid "
        "      AND dependency.classid = "
        "          'pg_catalog.pg_default_acl'::pg_catalog.regclass "
        "      AND dependency.refobjid = ("
        "        SELECT oid FROM managed WHERE rolname = %s"
        "      )"
        "    )"
        ")",
        (
            role_names,
            settings.target_schema,
            settings.migration.name,
            settings.migration.name,
        ),
    )


def _ensure_roles(
    cursor: Any,
    settings: ProvisionSettings,
    existing: set[str],
) -> None:
    # PostgreSQL utility grammar does not accept protocol parameters in a
    # PASSWORD clause. Keep both identity and password as bound parameters and
    # perform the one utility statement inside a transaction-local pg_temp
    # helper using format(%I, %L); the secret never becomes client SQL text.
    cursor.execute(
        "CREATE FUNCTION pg_temp.ai_exchange_apply_role("
        "  role_name pg_catalog.text, "
        "  role_password pg_catalog.text, "
        "  role_exists pg_catalog.bool"
        ") RETURNS pg_catalog.void "
        "LANGUAGE plpgsql "
        "SET search_path TO pg_catalog "
        "AS $provision$ "
        "DECLARE role_statement pg_catalog.text; "
        "BEGIN "
        "  IF role_exists THEN "
        "    role_statement := pg_catalog.format("
        "      'ALTER ROLE %I WITH LOGIN NOINHERIT NOSUPERUSER NOCREATEDB '"
        "      || 'NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1 '"
        "      || 'PASSWORD %L VALID UNTIL %L', "
        "      role_name, role_password, 'infinity'"
        "    ); "
        "  ELSE "
        "    role_statement := pg_catalog.format("
        "      'CREATE ROLE %I WITH LOGIN NOINHERIT NOSUPERUSER NOCREATEDB '"
        "      || 'NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1 '"
        "      || 'PASSWORD %L VALID UNTIL %L', "
        "      role_name, role_password, 'infinity'"
        "    ); "
        "  END IF; "
        "  EXECUTE role_statement; "
        "END "
        "$provision$"
    )
    for role in settings.roles:
        identifier = sql.Identifier(role.name)
        cursor.execute(
            "SELECT pg_temp.ai_exchange_apply_role(%s, %s, %s)",
            (
                role.name,
                role.password.get_secret_value(),
                role.name in existing,
            ),
        )
        cursor.execute(sql.SQL("ALTER ROLE {} RESET ALL").format(identifier))
        cursor.execute(
            sql.SQL("ALTER ROLE {} IN DATABASE {} RESET ALL").format(
                identifier,
                sql.Identifier(settings.database_name),
            )
        )


def _revoke_role_database_access(
    cursor: Any,
    *,
    database_name: str,
    roles: tuple[ManagedRole, ...],
    all_privileges: bool,
) -> None:
    privileges = (
        sql.SQL("ALL PRIVILEGES") if all_privileges else sql.SQL("CONNECT, TEMPORARY")
    )
    database = sql.Identifier(database_name)
    cursor.execute(
        sql.SQL("REVOKE {} ON DATABASE {} FROM PUBLIC").format(
            privileges,
            database,
        )
    )
    for role in roles:
        cursor.execute(
            sql.SQL("REVOKE {} ON DATABASE {} FROM {}").format(
                privileges,
                database,
                sql.Identifier(role.name),
            )
        )


def _apply_database_boundary(cursor: Any, settings: ProvisionSettings) -> None:
    database = sql.Identifier(settings.database_name)
    migration = sql.Identifier(settings.migration.name)
    schema = sql.Identifier(settings.target_schema)

    cursor.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(database, migration))
    cursor.execute(sql.SQL("ALTER DATABASE {} RESET ALL").format(database))

    cursor.execute(
        "SELECT database.datname::pg_catalog.text "
        "FROM pg_catalog.pg_database AS database "
        "WHERE database.datallowconn "
        "ORDER BY database.datname"
    )
    connectable_databases = [str(row[0]) for row in cursor.fetchall()]
    if settings.database_name not in connectable_databases:
        raise _reject()
    for database_name in connectable_databases:
        _revoke_role_database_access(
            cursor,
            database_name=database_name,
            roles=(
                settings.roles[1:]
                if database_name == settings.database_name
                else settings.roles
            ),
            all_privileges=database_name == settings.database_name,
        )

    for role in settings.roles:
        cursor.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                database,
                sql.Identifier(role.name),
            )
        )

    cursor.execute(sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(schema, migration))
    cursor.execute(
        sql.SQL("REVOKE ALL PRIVILEGES ON SCHEMA {} FROM PUBLIC").format(schema)
    )
    # Do not revoke the migration owner's own schema ACL: PostgreSQL permits an
    # owner to revoke its effective USAGE/CREATE while retaining ownership.
    for role in (settings.runtime, settings.maintenance, settings.auditor):
        cursor.execute(
            sql.SQL("REVOKE ALL PRIVILEGES ON SCHEMA {} FROM {}").format(
                schema,
                sql.Identifier(role.name),
            )
        )
    for role in (settings.runtime, settings.maintenance):
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                schema,
                sql.Identifier(role.name),
            )
        )


def _apply_default_and_large_object_boundary(
    cursor: Any,
    settings: ProvisionSettings,
) -> None:
    migration = sql.Identifier(settings.migration.name)
    schema = sql.Identifier(settings.target_schema)
    for schema_clause in (
        sql.SQL(""),
        sql.SQL(" IN SCHEMA {} ").format(schema),
    ):
        cursor.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {}{} ").format(
                migration,
                schema_clause,
            )
            + sql.SQL("REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC")
        )
        cursor.execute(
            sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {}{} ").format(
                migration,
                schema_clause,
            )
            + sql.SQL("REVOKE USAGE ON TYPES FROM PUBLIC")
        )

    grantees = sql.SQL(", ").join(
        (sql.SQL("PUBLIC"), *(sql.Identifier(role.name) for role in settings.roles))
    )
    for routine_name, argument_types in _LARGE_OBJECT_CREATION_ROUTINES:
        signature = sql.SQL("{}.{}({})").format(
            sql.Identifier("pg_catalog"),
            sql.Identifier(routine_name),
            sql.SQL(", ").join(
                sql.Identifier("pg_catalog", argument_type)
                for argument_type in argument_types
            ),
        )
        cursor.execute(
            sql.SQL("REVOKE EXECUTE ON FUNCTION {} FROM {}").format(
                signature,
                grantees,
            )
        )


def _verify_restricted_privilege_postconditions(
    cursor: Any,
    settings: ProvisionSettings,
) -> None:
    role_names = [role.name for role in settings.roles]
    _require_true_row(
        cursor,
        "WITH managed AS ("
        "  SELECT role.oid, role.rolname FROM pg_catalog.pg_roles AS role "
        "  WHERE role.rolname = ANY(%s::pg_catalog.text[])"
        "), target AS ("
        "  SELECT database.oid AS database_oid, namespace.oid AS schema_oid "
        "  FROM pg_catalog.pg_database AS database "
        "  JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = %s "
        "  WHERE database.datname = pg_catalog.current_database()"
        "), migration AS ("
        "  SELECT oid FROM managed WHERE rolname = %s"
        ") "
        "SELECT "
        "NOT EXISTS ("
        "  SELECT 1 FROM managed CROSS JOIN target "
        "  WHERE managed.rolname <> %s "
        "    AND ("
        "      pg_catalog.has_database_privilege("
        "        managed.oid, target.database_oid, 'CREATE'"
        "      ) OR pg_catalog.has_database_privilege("
        "        managed.oid, target.database_oid, 'TEMPORARY'"
        "      )"
        "    )"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_database AS database "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(database.datacl) AS acl "
        "  CROSS JOIN target "
        "  WHERE database.oid = target.database_oid "
        "    AND acl.grantee IN (SELECT oid FROM managed) "
        "    AND acl.is_grantable"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_namespace AS namespace "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(namespace.nspacl) AS acl "
        "  CROSS JOIN target "
        "  WHERE namespace.oid = target.schema_oid "
        "    AND acl.grantee IN (SELECT oid FROM managed) "
        "    AND acl.is_grantable"
        ") AND ("
        "  SELECT pg_catalog.count(*) = 2 "
        "    AND pg_catalog.count(*) FILTER ("
        "      WHERE defaults.defaclnamespace = 0 "
        "        AND defaults.defaclobjtype = 'f'"
        "    ) = 1 "
        "    AND pg_catalog.count(*) FILTER ("
        "      WHERE defaults.defaclnamespace = 0 "
        "        AND defaults.defaclobjtype = 'T'"
        "    ) = 1 "
        "  FROM pg_catalog.pg_default_acl AS defaults "
        "  WHERE defaults.defaclrole = (SELECT oid FROM migration)"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_default_acl AS defaults "
        "  WHERE defaults.defaclrole <> (SELECT oid FROM migration)"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_default_acl AS defaults "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(defaults.defaclacl) AS acl "
        "  WHERE acl.grantee <> (SELECT oid FROM migration)"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_proc AS routine "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode("
        "    COALESCE(routine.proacl, pg_catalog.acldefault('f', routine.proowner))"
        "  ) AS acl "
        "  WHERE routine.oid = ANY(ARRAY["
        "    'pg_catalog.lo_creat(pg_catalog.int4)'::pg_catalog.regprocedure,"
        "    'pg_catalog.lo_create(pg_catalog.oid)'::pg_catalog.regprocedure,"
        "    'pg_catalog.lo_from_bytea(pg_catalog.oid,pg_catalog.bytea)'"
        "      ::pg_catalog.regprocedure,"
        "    'pg_catalog.lo_import(pg_catalog.text)'::pg_catalog.regprocedure,"
        "    'pg_catalog.lo_import(pg_catalog.text,pg_catalog.oid)'"
        "      ::pg_catalog.regprocedure"
        "  ]) "
        "    AND acl.privilege_type = 'EXECUTE' "
        "    AND (acl.grantee = 0 OR acl.grantee IN (SELECT oid FROM managed))"
        ")",
        (
            role_names,
            settings.target_schema,
            settings.migration.name,
            settings.migration.name,
        ),
    )


def _verify_atomic_postconditions(cursor: Any, settings: ProvisionSettings) -> None:
    role_names = [role.name for role in settings.roles]
    cursor.execute(
        "SELECT "
        "  pg_catalog.count(*) = 4 "
        "  AND pg_catalog.bool_and("
        "    role.rolcanlogin AND NOT role.rolinherit "
        "    AND NOT role.rolsuper AND NOT role.rolcreatedb "
        "    AND NOT role.rolcreaterole AND NOT role.rolreplication "
        "    AND NOT role.rolbypassrls AND role.rolconnlimit = -1 "
        "    AND role.rolvaliduntil = 'infinity'::pg_catalog.timestamptz "
        "    AND role.rolpassword IS NOT NULL"
        "  ) "
        "FROM pg_catalog.pg_authid AS role "
        "WHERE role.rolname = ANY(%s::pg_catalog.text[])",
        (role_names,),
    )
    if cursor.fetchone() != (True,):
        raise _reject()

    _require_true_row(
        cursor,
        "WITH managed AS ("
        "  SELECT role.oid, role.rolname FROM pg_catalog.pg_roles AS role "
        "  WHERE role.rolname = ANY(%s::pg_catalog.text[])"
        "), target AS ("
        "  SELECT database.oid AS database_oid, database.datdba, database.datacl, "
        "         namespace.oid AS schema_oid, namespace.nspowner, namespace.nspacl "
        "  FROM pg_catalog.pg_database AS database "
        "  JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = %s "
        "  WHERE database.datname = pg_catalog.current_database()"
        ") "
        "SELECT "
        "(SELECT datdba FROM target) = "
        "  (SELECT oid FROM managed WHERE rolname = %s) "
        "AND (SELECT nspowner FROM target) = "
        "  (SELECT oid FROM managed WHERE rolname = %s) "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM target "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(target.datacl) AS acl "
        "  WHERE acl.grantee = 0"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM target "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(target.nspacl) AS acl "
        "  WHERE acl.grantee = 0"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_auth_members AS membership "
        "  WHERE membership.member IN (SELECT oid FROM managed) "
        "     OR membership.roleid IN (SELECT oid FROM managed)"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_db_role_setting AS setting "
        "  CROSS JOIN target "
        "  WHERE setting.setrole IN (SELECT oid FROM managed) "
        "     OR (setting.setrole = 0 AND setting.setdatabase = target.database_oid)"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_database AS other_database "
        "  CROSS JOIN managed "
        "  CROSS JOIN target "
        "  WHERE other_database.oid <> target.database_oid "
        "    AND other_database.datallowconn "
        "    AND ("
        "      pg_catalog.has_database_privilege("
        "        managed.oid, other_database.oid, 'CONNECT'"
        "      ) OR pg_catalog.has_database_privilege("
        "        managed.oid, other_database.oid, 'TEMPORARY'"
        "      )"
        "    )"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_database AS other_database "
        "  CROSS JOIN LATERAL pg_catalog.aclexplode(other_database.datacl) AS acl "
        "  CROSS JOIN target "
        "  WHERE other_database.oid <> target.database_oid "
        "    AND other_database.datallowconn "
        "    AND acl.grantee = 0 "
        "    AND acl.privilege_type IN ('CONNECT', 'TEMPORARY')"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM managed CROSS JOIN target "
        "  WHERE NOT pg_catalog.has_database_privilege("
        "    managed.oid, target.database_oid, 'CONNECT'"
        "  )"
        ") AND pg_catalog.has_schema_privilege("
        "  (SELECT oid FROM managed WHERE rolname = %s), "
        "  (SELECT schema_oid FROM target), 'USAGE'"
        ") AND pg_catalog.has_schema_privilege("
        "  (SELECT oid FROM managed WHERE rolname = %s), "
        "  (SELECT schema_oid FROM target), 'USAGE'"
        ") AND NOT pg_catalog.has_schema_privilege("
        "  (SELECT oid FROM managed WHERE rolname = %s), "
        "  (SELECT schema_oid FROM target), 'USAGE'"
        ")",
        (
            role_names,
            settings.target_schema,
            settings.migration.name,
            settings.migration.name,
            settings.runtime.name,
            settings.maintenance.name,
            settings.auditor.name,
        ),
    )
    _verify_restricted_privilege_postconditions(cursor, settings)


def _migration_dsn(settings: ProvisionSettings) -> str:
    parsed = conninfo_to_dict(settings.admin_database_url.get_secret_value())
    parsed["user"] = settings.migration.name
    parsed["password"] = settings.migration.password.get_secret_value()
    parsed["dbname"] = settings.database_name
    parsed["options"] = f"-csearch_path={settings.target_schema}"
    parsed.pop("service", None)
    parsed.pop("passfile", None)
    return make_conninfo(**parsed)


def provision_database(settings: ProvisionSettings) -> None:
    """Provision the empty database and prove the bootstrap identity gate."""

    try:
        with psycopg.connect(
            settings.admin_database_url.get_secret_value(),
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_catalog.pg_advisory_xact_lock(%s)",
                    (_PROVISION_LOCK_KEY,),
                )
                _require_admin_boundary(cursor, settings)
                _require_dedicated_cluster(cursor, settings)
                existing = _existing_role_names(cursor, settings)
                _require_cluster_authority_baseline(cursor, settings)
                _require_target_authority_baseline(
                    cursor,
                    settings,
                    provisioned_retry=bool(existing),
                )
                _require_fresh_target(
                    cursor,
                    settings,
                    allow_provision_defaults=bool(existing),
                )
                _require_existing_roles_safe(cursor, settings, existing)
                if existing:
                    _verify_atomic_postconditions(cursor, settings)
                _ensure_roles(cursor, settings, existing)
                _apply_database_boundary(cursor, settings)
                _apply_default_and_large_object_boundary(cursor, settings)
                _verify_atomic_postconditions(cursor, settings)

        asyncio.run(
            require_migration_database_role(
                _migration_dsn(settings),
                expected_migration_role=settings.migration.name,
                expected_runtime_role=settings.runtime.name,
                expected_maintenance_role=settings.maintenance.name,
                expected_auditor_role=settings.auditor.name,
                target_schema=settings.target_schema,
                allow_acl_reconciliation=True,
            )
        )
    except DatabaseProvisionError:
        raise
    except Exception:
        raise _reject() from None

    logger.info("Fresh database role provisioning complete")


def main() -> None:
    try:
        provision_database(load_provision_settings())
    except Exception:
        raise RuntimeError("database_provision_failed") from None


if __name__ == "__main__":
    main()
