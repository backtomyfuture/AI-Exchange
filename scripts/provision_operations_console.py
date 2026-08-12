"""Provision the local Operations Console's read-only PostgreSQL role.

This is an explicit operator command.  It is intentionally not part of the
production Compose provisioning container because the console runs outside the
application and its credential must never enter an application container.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import tempfile
from pathlib import Path
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


ROLE_DEFAULT = "ai_exchange_operations_console"
SCHEMA_DEFAULT = "public"
TABLES = (
    "event_inbox",
    "intake_decisions",
    "emails",
    "tier1_decisions",
    "handoff_runs",
    "handoff_executions",
    "execution_payload_revisions",
    "approved_execution_envelopes",
    "route_evaluation_traces",
    "audit_events",
    "emails_log",
)
IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z", re.ASCII)
PASSWORD_MIN_LENGTH = 16


class ProvisionOperationsConsoleError(RuntimeError):
    """Safe operator-facing failure without secret or database detail."""


def _read_private(path: Path) -> str:
    try:
        if path.is_symlink() or (path.stat().st_mode & 0o777) not in {0o400, 0o600}:
            raise ValueError
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError, ValueError):
        raise ProvisionOperationsConsoleError("console_secret_file_invalid") from None
    if not value or "\x00" in value or "\n" in value or "\r" in value:
        raise ProvisionOperationsConsoleError("console_secret_file_invalid")
    return value


def _identifier(value: str, code: str) -> str:
    if IDENTIFIER.fullmatch(value) is None:
        raise ProvisionOperationsConsoleError(code)
    return value


def _write_private(path: Path, value: str) -> None:
    if "\x00" in value or "\r" in value or "\n" in value:
        raise ProvisionOperationsConsoleError("console_secret_file_invalid")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = -1
            stream.write(value + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
        os.chmod(path, 0o600)
    except Exception:
        if fd >= 0:
            os.close(fd)
        Path(temporary_name).unlink(missing_ok=True)
        raise ProvisionOperationsConsoleError("console_secret_write_failed") from None


def _role_password(path: Path | None) -> str:
    if path is None:
        return secrets.token_urlsafe(32)
    value = _read_private(path)
    if len(value) < PASSWORD_MIN_LENGTH:
        raise ProvisionOperationsConsoleError("console_role_password_invalid")
    return value


def _require_admin(cursor) -> None:
    cursor.execute(
        "SELECT session_user = current_user AND rolsuper "
        "FROM pg_catalog.pg_roles WHERE rolname = current_user"
    )
    row = cursor.fetchone()
    if row != (True,):
        raise ProvisionOperationsConsoleError("console_admin_identity_invalid")


def _ensure_role(cursor, role: str, password: str) -> None:
    cursor.execute(
        "CREATE FUNCTION pg_temp.operations_console_apply_role("
        "role_name text, role_password text, role_exists boolean) "
        "RETURNS void LANGUAGE plpgsql SET search_path TO pg_catalog AS $body$ "
        "DECLARE statement text; BEGIN "
        "IF role_exists THEN "
        "statement := format('ALTER ROLE %I WITH LOGIN NOINHERIT NOSUPERUSER "
        "NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1 "
        "PASSWORD %L VALID UNTIL %L', role_name, role_password, 'infinity'); "
        "ELSE "
        "statement := format('CREATE ROLE %I WITH LOGIN NOINHERIT NOSUPERUSER "
        "NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1 "
        "PASSWORD %L VALID UNTIL %L', role_name, role_password, 'infinity'); "
        "END IF; EXECUTE statement; END $body$"
    )
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s)",
        (role,),
    )
    exists = bool(cursor.fetchone()[0])
    cursor.execute(
        "SELECT pg_temp.operations_console_apply_role(%s, %s, %s)",
        (role, password, exists),
    )


def _apply_grants(cursor, *, role: str, database: str, schema: str) -> None:
    role_id = sql.Identifier(role)
    database_id = sql.Identifier(database)
    schema_id = sql.Identifier(schema)
    cursor.execute(sql.SQL("ALTER ROLE {} RESET ALL").format(role_id))
    cursor.execute(sql.SQL("ALTER ROLE {} IN DATABASE {} RESET ALL").format(role_id, database_id))
    cursor.execute(sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(database_id, role_id))
    cursor.execute(sql.SQL("REVOKE ALL PRIVILEGES ON SCHEMA {} FROM {}").format(schema_id, role_id))
    cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database_id, role_id))
    cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema_id, role_id))
    cursor.execute(sql.SQL("REVOKE CREATE ON DATABASE {} FROM {}").format(database_id, role_id))
    cursor.execute(sql.SQL("REVOKE TEMPORARY ON DATABASE {} FROM {}").format(database_id, role_id))
    cursor.execute(sql.SQL("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} FROM {}").format(schema_id, role_id))
    for table in TABLES:
        cursor.execute(
            sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                schema_id,
                sql.Identifier(table),
                role_id,
            )
        )


def _verify(cursor, *, role: str, database: str, schema: str) -> None:
    cursor.execute(
        """
        SELECT rolcanlogin AND NOT rolinherit AND NOT rolsuper
               AND NOT rolcreatedb AND NOT rolcreaterole
               AND NOT rolreplication AND NOT rolbypassrls
        FROM pg_catalog.pg_roles
        WHERE rolname = %s
        """,
        (role,),
    )
    if cursor.fetchone() != (True,):
        raise ProvisionOperationsConsoleError("console_role_postcondition_failed")
    cursor.execute(
        "SELECT 1 FROM pg_catalog.pg_auth_members AS member "
        "JOIN pg_catalog.pg_roles AS role ON role.oid = member.member "
        "WHERE role.rolname = %s LIMIT 1",
        (role,),
    )
    if cursor.fetchone() is not None:
        raise ProvisionOperationsConsoleError("console_role_membership_detected")
    cursor.execute(
        """
        SELECT relation.relname
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = %s
          AND relation.relkind = 'r'
          AND pg_catalog.has_table_privilege(%s, relation.oid, 'SELECT')
          AND relation.relname <> ALL(%s::text[])
        LIMIT 1
        """,
        (schema, role, list(TABLES)),
    )
    if cursor.fetchone() is not None:
        raise ProvisionOperationsConsoleError("console_role_scope_too_wide")
    cursor.execute(
        """
        SELECT relation.relname, privilege
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN pg_catalog.unnest(
            ARRAY['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER']::text[]
        ) AS requested(privilege)
        WHERE namespace.nspname = %s
          AND relation.relkind = 'r'
          AND relation.relname = ANY(%s::text[])
          AND pg_catalog.has_table_privilege(%s, relation.oid, requested.privilege)
        LIMIT 1
        """,
        (schema, list(TABLES), role),
    )
    if cursor.fetchone() is not None:
        raise ProvisionOperationsConsoleError("console_role_write_scope_detected")
    cursor.execute(
        "SELECT pg_catalog.has_database_privilege(%s, %s, 'CONNECT'), "
        "pg_catalog.has_schema_privilege(%s, %s, 'USAGE'), "
        "NOT pg_catalog.has_schema_privilege(%s, %s, 'CREATE'), "
        "NOT pg_catalog.has_database_privilege(%s, %s, 'CREATE'), "
        "NOT pg_catalog.has_database_privilege(%s, %s, 'TEMPORARY')",
        (role, database, role, schema, role, schema, role, database, role, database),
    )
    if cursor.fetchone() != (True, True, True, True, True):
        raise ProvisionOperationsConsoleError("console_role_boundary_invalid")


def _console_dsn(admin_dsn: str, *, role: str, password: str, schema: str) -> str:
    try:
        parsed = conninfo_to_dict(admin_dsn)
        parsed["user"] = role
        parsed["password"] = password
        parsed["options"] = f"-csearch_path=pg_catalog,{schema}"
        parsed.pop("service", None)
        parsed.pop("passfile", None)
        return make_conninfo(**parsed)
    except Exception:
        raise ProvisionOperationsConsoleError("console_dsn_invalid") from None


def provision(
    *,
    admin_dsn_file: Path,
    role_password_file: Path | None,
    dsn_output: Path,
    role: str = ROLE_DEFAULT,
    schema: str = SCHEMA_DEFAULT,
) -> None:
    role = _identifier(role, "console_role_invalid")
    schema = _identifier(schema, "console_schema_invalid")
    admin_dsn = _read_private(admin_dsn_file)
    password = _role_password(role_password_file)
    try:
        parsed = conninfo_to_dict(admin_dsn)
        database = _identifier(str(parsed.get("dbname") or ""), "console_database_invalid")
        with psycopg.connect(admin_dsn, autocommit=False) as connection:
            with connection.cursor() as cursor:
                _require_admin(cursor)
                _ensure_role(cursor, role, password)
                _apply_grants(cursor, role=role, database=database, schema=schema)
                _verify(cursor, role=role, database=database, schema=schema)
            connection.commit()
    except ProvisionOperationsConsoleError:
        raise
    except Exception:
        raise ProvisionOperationsConsoleError("console_role_provision_failed") from None
    _write_private(dsn_output, _console_dsn(admin_dsn, role=role, password=password, schema=schema))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-dsn-file", type=Path, required=True)
    parser.add_argument("--role-password-file", type=Path)
    parser.add_argument("--dsn-output", type=Path, required=True)
    parser.add_argument("--role", default=ROLE_DEFAULT)
    parser.add_argument("--schema", default=SCHEMA_DEFAULT)
    arguments = parser.parse_args(argv)
    try:
        provision(
            admin_dsn_file=arguments.admin_dsn_file,
            role_password_file=arguments.role_password_file,
            dsn_output=arguments.dsn_output,
            role=arguments.role,
            schema=arguments.schema,
        )
    except ProvisionOperationsConsoleError as exc:
        print(f"Operations Console role provisioning failed: {exc}")
        return 1
    print(f"Operations Console role ready: {arguments.role}")
    print(f"DSN written to: {arguments.dsn_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
