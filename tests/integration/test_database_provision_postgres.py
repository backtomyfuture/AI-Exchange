"""PostgreSQL-backed integration coverage for greenfield provisioning."""

from __future__ import annotations

import asyncio
import os
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from pydantic import SecretStr

from src.db.bootstrap import bootstrap_database
from src.db.provision import (
    DatabaseProvisionError,
    ManagedRole,
    ProvisionSettings,
    provision_database,
)


def _role_url(
    admin_url: str,
    *,
    database_name: str,
    role_name: str,
    password: str,
) -> str:
    parsed = urlsplit(admin_url)
    assert parsed.hostname
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parsed.port}" if parsed.port else ""
    query = [
        (name, value)
        for name, value in parse_qsl(parsed.query, keep_blank_values=True)
        if name != "options"
    ]
    query.append(("options", "-csearch_path=public"))
    return urlunsplit(
        (
            parsed.scheme,
            f"{quote(role_name, safe='')}:{quote(password, safe='')}@{host}{port}",
            f"/{database_name}",
            urlencode(query),
            parsed.fragment,
        )
    )


def _database_url(admin_url: str, database_name: str) -> str:
    parsed = urlsplit(admin_url)
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            f"/{database_name}",
            parsed.query,
            parsed.fragment,
        )
    )


def _provision_mutation_snapshot(settings: ProvisionSettings) -> tuple[object, ...]:
    with psycopg.connect(
        settings.admin_database_url.get_secret_value(),
        autocommit=True,
    ) as admin:
        row = admin.execute(
            "SELECT pg_catalog.pg_get_userbyid(database.datdba), "
            "       database.datacl::pg_catalog.text, "
            "       pg_catalog.pg_get_userbyid(namespace.nspowner), "
            "       namespace.nspacl::pg_catalog.text, "
            "       COALESCE(("
            "         SELECT pg_catalog.array_agg(role.rolname ORDER BY role.rolname) "
            "         FROM pg_catalog.pg_roles AS role "
            "         WHERE role.rolname = ANY(%s::pg_catalog.text[])"
            "       ), ARRAY[]::pg_catalog.name[]) "
            "FROM pg_catalog.pg_database AS database "
            "JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = %s "
            "WHERE database.datname = pg_catalog.current_database()",
            ([role.name for role in settings.roles], settings.target_schema),
        ).fetchone()
    assert row is not None
    return row


def _assert_preflight_rejects_without_mutation(settings: ProvisionSettings) -> None:
    before = _provision_mutation_snapshot(settings)
    with pytest.raises(DatabaseProvisionError, match="database_provision_invalid"):
        provision_database(settings)
    assert _provision_mutation_snapshot(settings) == before
    assert before[-1] == []


def _recreate_fresh_target(admin_url: str, database_name: str) -> None:
    parsed = conninfo_to_dict(admin_url)
    admin_role = parsed["user"]
    with psycopg.connect(
        _database_url(admin_url, "postgres"),
        autocommit=True,
    ) as admin:
        admin.execute(
            "SELECT pg_catalog.pg_terminate_backend(activity.pid) "
            "FROM pg_catalog.pg_stat_activity AS activity "
            "WHERE activity.datname = %s "
            "  AND activity.pid <> pg_catalog.pg_backend_pid()",
            (database_name,),
        )
        admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database_name)))
        admin.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(database_name),
                sql.Identifier(admin_role),
            )
        )


@pytest.mark.integration
def test_fresh_provision_is_retryable_and_enables_existing_bootstrap(
    postgres_admin_url: str,
):
    if os.getenv("TEST_POSTGRES_DEDICATED_PROVISION") != "1":
        pytest.skip("TEST_POSTGRES_DEDICATED_PROVISION=1 requires a disposable cluster")
    token = uuid4().hex
    database_name = conninfo_to_dict(postgres_admin_url)["dbname"]
    assert database_name not in {"postgres", "template0", "template1"}
    peer_database_name = f"aixp_peer_{token}"
    role_names = (
        f"aixp_m_{token}",
        f"aixp_r_{token}",
        f"aixp_k_{token}",
        f"aixp_a_{token}",
    )
    passwords = (
        f"Migration-{token}",
        f"Runtime-{token}",
        f"Maintenance-{token}",
        f"Auditor-{token}",
    )
    settings = ProvisionSettings(
        admin_database_url=SecretStr(postgres_admin_url),
        database_name=database_name,
        target_schema="public",
        migration=ManagedRole(role_names[0], SecretStr(passwords[0])),
        runtime=ManagedRole(role_names[1], SecretStr(passwords[1])),
        maintenance=ManagedRole(role_names[2], SecretStr(passwords[2])),
        auditor=ManagedRole(role_names[3], SecretStr(passwords[3])),
    )

    unexpected_role = f"aixp_unexpected_{token}"
    with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
        admin.execute(
            sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(unexpected_role))
        )
    try:
        _assert_preflight_rejects_without_mutation(settings)
    finally:
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP ROLE {}").format(sql.Identifier(unexpected_role))
            )

    admin_role = conninfo_to_dict(postgres_admin_url)["user"]
    with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
        admin.execute(
            sql.SQL("ALTER ROLE {} SET application_name TO 'unexpected'").format(
                sql.Identifier(admin_role)
            )
        )
    try:
        _assert_preflight_rejects_without_mutation(settings)
    finally:
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("ALTER ROLE {} RESET ALL").format(sql.Identifier(admin_role))
            )

    with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
        admin.execute(
            sql.SQL("GRANT pg_monitor TO {}").format(sql.Identifier(admin_role))
        )
    try:
        _assert_preflight_rejects_without_mutation(settings)
    finally:
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("REVOKE pg_monitor FROM {}").format(sql.Identifier(admin_role))
            )

    target_authority_anomalies = (
        sql.SQL("GRANT CONNECT ON DATABASE {} TO pg_monitor").format(
            sql.Identifier(database_name)
        ),
        sql.SQL("ALTER DATABASE {} OWNER TO pg_monitor").format(
            sql.Identifier(database_name)
        ),
        sql.SQL("GRANT CREATE ON SCHEMA public TO pg_monitor"),
        sql.SQL("ALTER SCHEMA public OWNER TO pg_monitor"),
    )
    for anomaly in target_authority_anomalies:
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            admin.execute(anomaly)
        try:
            _assert_preflight_rejects_without_mutation(settings)
        finally:
            _recreate_fresh_target(postgres_admin_url, database_name)

    with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
        initial_boundary = admin.execute(
            "SELECT pg_catalog.pg_get_userbyid(database.datdba), "
            "       pg_catalog.pg_get_userbyid(namespace.nspowner) "
            "FROM pg_catalog.pg_database AS database "
            "JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = 'public' "
            "WHERE database.datname = pg_catalog.current_database()"
        ).fetchone()
        admin.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(peer_database_name))
        )

    try:
        with pytest.raises(DatabaseProvisionError, match="database_provision_invalid"):
            provision_database(settings)
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            assert (
                admin.execute(
                    "SELECT pg_catalog.pg_get_userbyid(database.datdba), "
                    "       pg_catalog.pg_get_userbyid(namespace.nspowner) "
                    "FROM pg_catalog.pg_database AS database "
                    "JOIN pg_catalog.pg_namespace AS namespace "
                    "  ON namespace.nspname = 'public' "
                    "WHERE database.datname = pg_catalog.current_database()"
                ).fetchone()
                == initial_boundary
            )
            assert admin.execute(
                "SELECT pg_catalog.has_database_privilege('public', %s, 'CONNECT')",
                (peer_database_name,),
            ).fetchone() == (True,)
            admin.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(peer_database_name))
            )
            for role_name, password in zip(role_names, passwords, strict=True):
                attributes = (
                    sql.SQL("NOLOGIN CREATEDB")
                    if role_name == role_names[1]
                    else sql.SQL("LOGIN NOCREATEDB")
                )
                admin.execute(
                    sql.SQL("CREATE ROLE {} {} PASSWORD {}").format(
                        sql.Identifier(role_name),
                        attributes,
                        sql.Literal(password),
                    )
                )
            admin.execute(
                sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                    sql.Identifier(database_name),
                    sql.Identifier(role_names[1]),
                )
            )

        with pytest.raises(DatabaseProvisionError, match="database_provision_invalid"):
            provision_database(settings)
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            assert admin.execute(
                "SELECT role.rolcanlogin, role.rolcreatedb "
                "FROM pg_catalog.pg_roles AS role WHERE role.rolname = %s",
                (role_names[1],),
            ).fetchone() == (False, True)
            assert (
                admin.execute(
                    "SELECT pg_catalog.pg_get_userbyid(database.datdba), "
                    "       pg_catalog.pg_get_userbyid(namespace.nspowner) "
                    "FROM pg_catalog.pg_database AS database "
                    "JOIN pg_catalog.pg_namespace AS namespace "
                    "  ON namespace.nspname = 'public' "
                    "WHERE database.datname = pg_catalog.current_database()"
                ).fetchone()
                == initial_boundary
            )
            for role_name in reversed(role_names):
                admin.execute(
                    sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role_name))
                )
                admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role_name)))

        _recreate_fresh_target(postgres_admin_url, database_name)
        provision_database(settings)
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            peer_acl = admin.execute(
                "SELECT "
                "NOT EXISTS ("
                "  SELECT 1 "
                "  FROM pg_catalog.pg_database AS database "
                "  CROSS JOIN LATERAL pg_catalog.aclexplode("
                "    COALESCE(database.datacl, "
                "             pg_catalog.acldefault('d', database.datdba))"
                "  ) AS acl "
                "  WHERE database.datname <> pg_catalog.current_database() "
                "    AND database.datallowconn "
                "    AND acl.grantee = 0 "
                "    AND acl.privilege_type IN ('CONNECT', 'TEMPORARY')"
                ") AND NOT EXISTS ("
                "  SELECT 1 "
                "  FROM pg_catalog.pg_database AS database "
                "  CROSS JOIN pg_catalog.pg_roles AS role "
                "  WHERE database.datname <> pg_catalog.current_database() "
                "    AND database.datallowconn "
                "    AND role.rolname = ANY(%s::pg_catalog.text[]) "
                "    AND ("
                "      pg_catalog.has_database_privilege("
                "        role.oid, database.oid, 'CONNECT'"
                "      ) OR pg_catalog.has_database_privilege("
                "        role.oid, database.oid, 'TEMPORARY'"
                "      )"
                "    )"
                ")",
                (list(role_names),),
            ).fetchone()
            assert peer_acl == (True,)
        provision_database(settings)
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("GRANT pg_monitor TO {}").format(sql.Identifier(role_names[1]))
            )
        with pytest.raises(
            DatabaseProvisionError,
            match="database_provision_invalid",
        ):
            provision_database(settings)
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("REVOKE pg_monitor FROM {}").format(
                    sql.Identifier(role_names[1])
                )
            )

        asyncio.run(
            bootstrap_database(
                _role_url(
                    postgres_admin_url,
                    database_name=database_name,
                    role_name=role_names[0],
                    password=passwords[0],
                ),
                expected_migration_role=role_names[0],
                expected_runtime_role=role_names[1],
                expected_maintenance_role=role_names[2],
                expected_auditor_role=role_names[3],
                target_schema="public",
            )
        )

        with pytest.raises(
            DatabaseProvisionError,
            match="database_provision_invalid",
        ):
            provision_database(settings)
    finally:
        with psycopg.connect(postgres_admin_url, autocommit=True) as admin:
            admin.execute(
                "SELECT pg_catalog.pg_terminate_backend(pid) "
                "FROM pg_catalog.pg_stat_activity "
                "WHERE datname = ANY(%s::pg_catalog.text[]) "
                "AND pid <> pg_catalog.pg_backend_pid()",
                ([peer_database_name],),
            )
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(peer_database_name)
                )
            )
