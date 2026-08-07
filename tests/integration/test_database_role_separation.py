from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from src.db.bootstrap import bootstrap_database
from src.db.roles import (
    DatabaseRoleError,
    require_migration_database_role,
    require_runtime_database_role,
)
from src.db.schema import EXPECTED_DATABASE_REVISION
from src.db.schema_contract import (
    DatabaseSchemaContractError,
    require_database_schema_contract,
)
from tests.integration.conftest import PostgresDatabaseFactory


@pytest.mark.integration
def test_factory_cleanup_tolerates_setup_failure_before_roles_exist(
    postgres_admin_url,
):
    factory = PostgresDatabaseFactory(postgres_admin_url)
    token = uuid4().hex
    factory._resources.append(
        (
            f"ai_exchange_test_{token}",
            f"ai_exchange_test_r_{token}",
            f"ai_exchange_test_k_{token}",
            f"ai_exchange_test_a_{token}",
            f"ai_exchange_test_m_{token}",
        )
    )

    factory.close()


@pytest.fixture
async def separated_schema(empty_schema):
    await bootstrap_database(empty_schema.dsn, **empty_schema.bootstrap_identity)
    empty_schema.grant_runtime_readiness()
    return empty_schema


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migration_owner_bootstraps_and_restricted_runtime_passes(
    separated_schema,
):
    await require_migration_database_role(
        separated_schema.dsn,
        **separated_schema.bootstrap_identity,
    )
    await require_runtime_database_role(
        separated_schema.runtime_dsn,
        **separated_schema.runtime_identity,
    )

    assert separated_schema.scalar("SELECT version_num FROM alembic_version")
    with psycopg.connect(separated_schema.runtime_dsn, autocommit=True) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_cannot_bootstrap_or_create_schema_objects(empty_schema):
    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await bootstrap_database(
            empty_schema.runtime_dsn,
            **empty_schema.bootstrap_identity,
        )

    assert not empty_schema.table_exists("alembic_version")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migration_owner_cannot_impersonate_runtime(separated_schema):
    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.dsn,
            **separated_schema.runtime_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        "CREATE SCHEMA runtime_forbidden",
        "CREATE TEMP TABLE runtime_forbidden_temp (id integer)",
        "CREATE TABLE public.runtime_forbidden (id integer)",
        "ALTER TABLE public.emails_log ADD COLUMN runtime_forbidden integer",
    ],
)
async def test_runtime_ddl_is_denied_by_postgres(separated_schema, statement: str):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        separated_schema.runtime_execute(statement)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_cannot_set_role_to_migration_owner(separated_schema):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        separated_schema.runtime_execute(
            sql.SQL("SET ROLE {}").format(
                sql.Identifier(separated_schema.migration_role)
            )
        )


def _apply_mutation(schema, mutation: str) -> None:
    if mutation == "superuser":
        schema.admin_execute(
            sql.SQL("ALTER ROLE {} SUPERUSER").format(
                sql.Identifier(schema.runtime_role)
            )
        )
    elif mutation in {
        "createdb",
        "createrole",
        "replication",
        "bypassrls",
    }:
        schema.admin_execute(
            sql.SQL("ALTER ROLE {} {}").format(
                sql.Identifier(schema.runtime_role),
                sql.SQL(mutation.upper()),
            )
        )
    elif mutation == "migration_membership":
        schema.admin_execute(
            sql.SQL("GRANT {} TO {}").format(
                sql.Identifier(schema.migration_role),
                sql.Identifier(schema.runtime_role),
            )
        )
    elif mutation == "database_create":
        schema.admin_execute(
            sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(schema.database_name),
                sql.Identifier(schema.runtime_role),
            )
        )
    elif mutation == "database_temp":
        schema.admin_execute(
            sql.SQL("GRANT TEMPORARY ON DATABASE {} TO {}").format(
                sql.Identifier(schema.database_name),
                sql.Identifier(schema.runtime_role),
            )
        )
    elif mutation == "schema_create":
        schema.admin_execute(
            sql.SQL("GRANT CREATE ON SCHEMA public TO {}").format(
                sql.Identifier(schema.runtime_role)
            )
        )
    elif mutation == "schema_owner":
        schema.admin_execute(
            sql.SQL("ALTER SCHEMA public OWNER TO {}").format(
                sql.Identifier(schema.runtime_role)
            )
        )
    elif mutation == "role_membership":
        schema.admin_execute(
            sql.SQL("GRANT pg_read_all_data TO {}").format(
                sql.Identifier(schema.runtime_role)
            )
        )
    elif mutation == "role_member":
        schema.admin_execute(
            sql.SQL("GRANT {} TO pg_monitor").format(
                sql.Identifier(schema.runtime_role)
            )
        )
    elif mutation == "inherit":
        schema.admin_execute(
            sql.SQL("ALTER ROLE {} INHERIT").format(sql.Identifier(schema.runtime_role))
        )
    elif mutation == "replica_session_config":
        schema.admin_execute(
            sql.SQL("ALTER ROLE {} SET session_replication_role = replica").format(
                sql.Identifier(schema.runtime_role)
            )
        )
    elif mutation in {
        "replication_parameter_set",
        "replication_parameter_alter_system",
    }:
        privilege = "SET" if mutation == "replication_parameter_set" else "ALTER SYSTEM"
        schema.admin_execute(
            sql.SQL("GRANT {} ON PARAMETER session_replication_role TO {}").format(
                sql.SQL(privilege),
                sql.Identifier(schema.runtime_role),
            )
        )
    elif mutation == "relation_owner":
        schema.admin_execute(
            sql.SQL("ALTER TABLE public.emails_log OWNER TO {}").format(
                sql.Identifier(schema.runtime_role)
            )
        )
    elif mutation == "database_owner":
        with psycopg.connect(
            schema.admin_dsn.replace(f"/{schema.database_name}", "/postgres"),
            autocommit=True,
        ) as conn:
            conn.execute(
                sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                    sql.Identifier(schema.database_name),
                    sql.Identifier(schema.runtime_role),
                )
            )
    else:  # pragma: no cover - test table is exhaustive
        raise AssertionError(mutation)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "superuser",
        "createdb",
        "createrole",
        "replication",
        "bypassrls",
        "migration_membership",
        "database_create",
        "database_temp",
        "schema_create",
        "schema_owner",
        "role_membership",
        "role_member",
        "inherit",
        "replica_session_config",
        "replication_parameter_set",
        "replication_parameter_alter_system",
        "relation_owner",
        "database_owner",
    ],
)
async def test_runtime_gate_rejects_each_escalation(separated_schema, mutation: str):
    _apply_mutation(separated_schema, mutation)

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )
    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            separated_schema.dsn,
            **separated_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "superuser",
        "createdb",
        "createrole",
        "replication",
        "bypassrls",
        "role_membership",
        "role_member",
        "inherit",
        "replica_session_config",
        "replication_parameter_set",
        "replication_parameter_alter_system",
    ],
)
async def test_migration_gate_rejects_extra_role_capability(empty_schema, mutation):
    if mutation == "role_membership":
        empty_schema.admin_execute(
            sql.SQL("GRANT pg_read_all_data TO {}").format(
                sql.Identifier(empty_schema.migration_role)
            )
        )
    elif mutation == "role_member":
        empty_schema.admin_execute(
            sql.SQL("GRANT {} TO pg_monitor").format(
                sql.Identifier(empty_schema.migration_role)
            )
        )
    elif mutation == "inherit":
        empty_schema.admin_execute(
            sql.SQL("ALTER ROLE {} INHERIT").format(
                sql.Identifier(empty_schema.migration_role)
            )
        )
    elif mutation == "replica_session_config":
        empty_schema.admin_execute(
            sql.SQL("ALTER ROLE {} SET session_replication_role = replica").format(
                sql.Identifier(empty_schema.migration_role)
            )
        )
    elif mutation in {
        "replication_parameter_set",
        "replication_parameter_alter_system",
    }:
        privilege = "SET" if mutation == "replication_parameter_set" else "ALTER SYSTEM"
        empty_schema.admin_execute(
            sql.SQL("GRANT {} ON PARAMETER session_replication_role TO {}").format(
                sql.SQL(privilege),
                sql.Identifier(empty_schema.migration_role),
            )
        )
    else:
        empty_schema.admin_execute(
            sql.SQL("ALTER ROLE {} {}").format(
                sql.Identifier(empty_schema.migration_role),
                sql.SQL(mutation.upper()),
            )
        )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migration_gate_cannot_be_tricked_by_public_catalog_shadow(empty_schema):
    empty_schema.execute(
        "CREATE FUNCTION public.current_setting(text) "
        "RETURNS text LANGUAGE sql IMMUTABLE "
        "AS 'SELECT ''public,pg_catalog''::text'"
    )
    wrong_search_path_dsn = psycopg.conninfo.make_conninfo(
        empty_schema.dsn,
        options="-csearch_path=public,pg_catalog",
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            wrong_search_path_dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("grantee", ["PUBLIC", "pg_monitor"])
async def test_migration_gate_requires_exclusive_schema_create_acl(
    empty_schema,
    grantee,
):
    if grantee == "PUBLIC":
        empty_schema.admin_execute("GRANT CREATE ON SCHEMA public TO PUBLIC")
    else:
        empty_schema.admin_execute(
            sql.SQL("GRANT CREATE ON SCHEMA public TO {}").format(
                sql.Identifier(grantee)
            )
        )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migration_gate_rejects_privileged_runtime_counterpart(empty_schema):
    empty_schema.admin_execute(
        sql.SQL("ALTER ROLE {} SUPERUSER").format(
            sql.Identifier(empty_schema.runtime_role)
        )
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_gate_rejects_privileged_migration_counterpart(
    separated_schema,
):
    separated_schema.admin_execute(
        sql.SQL("ALTER ROLE {} SUPERUSER").format(
            sql.Identifier(separated_schema.migration_role)
        )
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_gate_rejects_wrong_search_path(separated_schema):
    wrong_search_path_dsn = psycopg.conninfo.make_conninfo(
        separated_schema.runtime_dsn,
        options="-csearch_path=public,pg_catalog",
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            wrong_search_path_dsn,
            **separated_schema.runtime_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("gate_kind", ["migration", "runtime"])
@pytest.mark.parametrize("capability", ["catalog_passwords", "server_file"])
async def test_role_gate_rejects_direct_system_acl(
    separated_schema,
    gate_kind,
    capability,
):
    if gate_kind == "migration":
        role = separated_schema.migration_role
        dsn = separated_schema.dsn
        gates = (
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
        )
    else:
        role = separated_schema.runtime_role
        dsn = separated_schema.runtime_dsn
        gates = (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        )

    if capability == "catalog_passwords":
        separated_schema.admin_execute(
            sql.SQL("GRANT SELECT ON pg_catalog.pg_authid TO {}").format(
                sql.Identifier(role)
            )
        )
        proof_query = "SELECT rolpassword FROM pg_catalog.pg_authid LIMIT 1"
    else:
        separated_schema.admin_execute(
            sql.SQL(
                "GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_file(text) TO {}"
            ).format(sql.Identifier(role))
        )
        proof_query = "SELECT pg_catalog.pg_read_file('PG_VERSION')"

    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute(proof_query).fetchone() is not None

    for gate, gate_dsn, identity in gates:
        with pytest.raises(
            DatabaseRoleError,
            match="database_role_preflight_failed",
        ):
            await gate(gate_dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_gate_rejects_added_public_system_function_acl(
    separated_schema,
):
    separated_schema.admin_execute(
        "GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_file(text) TO PUBLIC"
    )
    try:
        with psycopg.connect(
            separated_schema.runtime_dsn,
            autocommit=True,
        ) as conn:
            assert conn.execute(
                "SELECT pg_catalog.pg_read_file('PG_VERSION')"
            ).fetchone()

        with pytest.raises(
            DatabaseRoleError,
            match="database_role_preflight_failed",
        ):
            await require_runtime_database_role(
                separated_schema.runtime_dsn,
                **separated_schema.runtime_identity,
            )
    finally:
        separated_schema.admin_execute(
            "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_read_file(text) FROM PUBLIC"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_added_public_system_column_acl(
    separated_schema,
):
    separated_schema.admin_execute(
        "GRANT SELECT (rolpassword) ON pg_catalog.pg_authid TO PUBLIC"
    )
    try:
        with psycopg.connect(
            separated_schema.runtime_dsn,
            autocommit=True,
        ) as conn:
            assert conn.execute(
                "SELECT rolpassword FROM pg_catalog.pg_authid LIMIT 1"
            ).fetchone()

        for gate, dsn, identity in (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
    finally:
        separated_schema.admin_execute(
            "REVOKE SELECT (rolpassword) ON pg_catalog.pg_authid FROM PUBLIC"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_new_public_system_security_definer(
    separated_schema,
):
    function_name = "ai_exchange_password_hash_count"
    separated_schema.admin_execute(
        f"CREATE FUNCTION pg_catalog.{function_name}() "
        "RETURNS bigint LANGUAGE sql SECURITY DEFINER "
        "SET search_path TO pg_catalog "
        "AS 'SELECT count(*) FROM pg_catalog.pg_authid "
        "WHERE rolpassword IS NOT NULL'"
    )
    try:
        with psycopg.connect(
            separated_schema.runtime_dsn,
            autocommit=True,
        ) as conn:
            assert (
                conn.execute(f"SELECT pg_catalog.{function_name}()").fetchone()[0] > 0
            )

        for gate, dsn, identity in (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
    finally:
        separated_schema.admin_execute(f"DROP FUNCTION pg_catalog.{function_name}()")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_ignore_other_session_temporary_schema(
    separated_schema,
):
    with psycopg.connect(
        separated_schema.admin_dsn,
        autocommit=True,
    ) as admin_connection:
        admin_connection.execute("CREATE TEMP TABLE temp_probe (id integer)")

        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )
        await require_migration_database_role(
            separated_schema.dsn,
            **separated_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_public_large_object_read(separated_schema):
    with psycopg.connect(separated_schema.admin_dsn, autocommit=True) as conn:
        large_object_oid = conn.execute("SELECT pg_catalog.lo_create(0)").fetchone()[0]
        conn.execute(
            "SELECT pg_catalog.lo_put(%s, 0, %s)",
            (large_object_oid, b"private-large-object"),
        )
        conn.execute(
            sql.SQL("GRANT SELECT ON LARGE OBJECT {} TO PUBLIC").format(
                sql.Literal(large_object_oid)
            )
        )
    try:
        with psycopg.connect(
            separated_schema.runtime_dsn,
            autocommit=True,
        ) as conn:
            assert (
                conn.execute(
                    "SELECT pg_catalog.lo_get(%s)",
                    (large_object_oid,),
                ).fetchone()[0]
                == b"private-large-object"
            )

        for gate, dsn, identity in (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
    finally:
        separated_schema.admin_execute(
            "SELECT pg_catalog.lo_unlink(%s)",
            (large_object_oid,),
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("signature", "statement"),
    [
        ("pg_catalog.lo_creat(pg_catalog.int4)", "SELECT pg_catalog.lo_creat(0)"),
        ("pg_catalog.lo_create(pg_catalog.oid)", "SELECT pg_catalog.lo_create(0)"),
        (
            "pg_catalog.lo_from_bytea(pg_catalog.oid, pg_catalog.bytea)",
            "SELECT pg_catalog.lo_from_bytea(0, 'probe'::pg_catalog.bytea)",
        ),
    ],
)
async def test_runtime_cannot_create_large_objects(
    separated_schema,
    signature,
    statement,
):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        separated_schema.runtime_execute(statement)

    separated_schema.admin_execute(
        sql.SQL("GRANT EXECUTE ON FUNCTION {} TO PUBLIC").format(sql.SQL(signature))
    )
    try:
        with psycopg.connect(
            separated_schema.runtime_dsn,
            autocommit=True,
        ) as conn:
            large_object_oid = conn.execute(statement).fetchone()[0]
        separated_schema.admin_execute(
            "SELECT pg_catalog.lo_unlink(%s)",
            (large_object_oid,),
        )

        for gate, dsn, identity in (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
    finally:
        separated_schema.admin_execute(
            sql.SQL("REVOKE EXECUTE ON FUNCTION {} FROM PUBLIC").format(
                sql.SQL(signature)
            )
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_gate_rejects_public_security_definer_in_other_schema(
    separated_schema,
):
    separated_schema.admin_execute("CREATE SCHEMA external_auth")
    separated_schema.admin_execute("GRANT USAGE ON SCHEMA external_auth TO PUBLIC")
    separated_schema.admin_execute(
        "CREATE FUNCTION external_auth.password_hash_count() "
        "RETURNS bigint LANGUAGE sql SECURITY DEFINER "
        "SET search_path TO pg_catalog "
        "AS 'SELECT count(*) FROM pg_catalog.pg_authid "
        "WHERE rolpassword IS NOT NULL'"
    )

    with psycopg.connect(
        separated_schema.runtime_dsn,
        autocommit=True,
    ) as conn:
        assert (
            conn.execute("SELECT external_auth.password_hash_count()").fetchone()[0] > 0
        )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            separated_schema.runtime_dsn,
            separated_schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            separated_schema.dsn,
            separated_schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migration_gate_rejects_unsafe_database_replication_setting(
    empty_schema,
):
    database_name = empty_schema.database_name
    empty_schema.admin_execute(
        sql.SQL("ALTER DATABASE {} SET session_replication_role = replica").format(
            sql.Identifier(database_name)
        )
    )
    empty_schema.admin_execute(
        sql.SQL(
            "ALTER ROLE {} IN DATABASE {} SET session_replication_role = origin"
        ).format(
            sql.Identifier(empty_schema.migration_role),
            sql.Identifier(database_name),
        )
    )

    with psycopg.connect(empty_schema.dsn, autocommit=True) as conn:
        assert (
            conn.execute(
                "SELECT pg_catalog.current_setting('session_replication_role')"
            ).fetchone()[0]
            == "origin"
        )
    with psycopg.connect(empty_schema.runtime_dsn, autocommit=True) as conn:
        assert (
            conn.execute(
                "SELECT pg_catalog.current_setting('session_replication_role')"
            ).fetchone()[0]
            == "replica"
        )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_runtime_large_object_compatibility_mode(
    separated_schema,
):
    with psycopg.connect(separated_schema.admin_dsn, autocommit=True) as conn:
        large_object_oid = conn.execute("SELECT pg_catalog.lo_create(0)").fetchone()[0]
        conn.execute(
            "SELECT pg_catalog.lo_put(%s, 0, %s)",
            (large_object_oid, b"private-without-acl"),
        )
        conn.execute(
            sql.SQL("ALTER ROLE {} SET lo_compat_privileges = on").format(
                sql.Identifier(separated_schema.runtime_role)
            )
        )
    try:
        with psycopg.connect(
            separated_schema.runtime_dsn,
            autocommit=True,
        ) as conn:
            assert (
                conn.execute(
                    "SELECT pg_catalog.lo_get(%s)",
                    (large_object_oid,),
                ).fetchone()[0]
                == b"private-without-acl"
            )

        for gate, dsn, identity in (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
    finally:
        separated_schema.admin_execute(
            sql.SQL("ALTER ROLE {} RESET lo_compat_privileges").format(
                sql.Identifier(separated_schema.runtime_role)
            )
        )
        separated_schema.admin_execute(
            "SELECT pg_catalog.lo_unlink(%s)",
            (large_object_oid,),
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["database", "schema", "relation"])
async def test_role_gates_reject_public_target_data_acl(separated_schema, scope):
    if scope == "database":
        separated_schema.admin_execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO PUBLIC").format(
                sql.Identifier(separated_schema.database_name)
            )
        )
    elif scope == "schema":
        separated_schema.admin_execute("GRANT USAGE ON SCHEMA public TO PUBLIC")
    else:
        separated_schema.admin_execute(
            "GRANT SELECT ON TABLE public.emails_log TO PUBLIC"
        )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            separated_schema.runtime_dsn,
            separated_schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            separated_schema.dsn,
            separated_schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("identity_plane", ["runtime", "migration"])
async def test_role_gates_confine_managed_roles_to_current_database(
    separated_schema,
    identity_plane,
):
    with psycopg.connect(
        separated_schema.admin_dsn,
        autocommit=True,
    ) as admin_connection:
        peer_database = admin_connection.execute(
            "SELECT datname FROM pg_catalog.pg_database "
            "WHERE datallowconn AND datname <> pg_catalog.current_database() "
            "ORDER BY datname LIMIT 1"
        ).fetchone()[0]

    if identity_plane == "runtime":
        managed_role = separated_schema.runtime_role
        managed_dsn = separated_schema.runtime_dsn
    else:
        managed_role = separated_schema.migration_role
        managed_dsn = separated_schema.dsn
    peer_managed_dsn = psycopg.conninfo.make_conninfo(
        managed_dsn,
        dbname=peer_database,
    )
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(peer_managed_dsn, autocommit=True)

    separated_schema.admin_execute(
        sql.SQL("GRANT CONNECT, TEMPORARY ON DATABASE {} TO {}").format(
            sql.Identifier(peer_database),
            sql.Identifier(managed_role),
        )
    )
    try:
        with psycopg.connect(peer_managed_dsn, autocommit=True) as conn:
            conn.execute("CREATE TEMP TABLE peer_escape (id integer)")

        for gate, dsn, identity in (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
    finally:
        separated_schema.admin_execute(
            sql.SQL("REVOKE CONNECT, TEMPORARY ON DATABASE {} FROM {}").format(
                sql.Identifier(peer_database),
                sql.Identifier(managed_role),
            )
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_new_peer_database_public_defaults(
    separated_schema,
):
    peer_database = f"ai_exchange_peer_{uuid4().hex}"
    with psycopg.connect(separated_schema.admin_dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(peer_database))
        )

    try:
        with psycopg.connect(separated_schema.admin_dsn, autocommit=True) as conn:
            public_privilege_count = conn.execute(
                "SELECT count(*) "
                "FROM pg_catalog.pg_database AS database "
                "CROSS JOIN LATERAL pg_catalog.aclexplode("
                "COALESCE(database.datacl, "
                "pg_catalog.acldefault('d', database.datdba))) AS acl "
                "WHERE database.datname = %s "
                "AND acl.grantee = 0 "
                "AND acl.privilege_type IN ('CONNECT', 'TEMPORARY')",
                (peer_database,),
            ).fetchone()[0]
        assert public_privilege_count == 2

        for managed_dsn in (
            separated_schema.runtime_dsn,
            separated_schema.dsn,
        ):
            peer_managed_dsn = psycopg.conninfo.make_conninfo(
                managed_dsn,
                dbname=peer_database,
            )
            with psycopg.connect(peer_managed_dsn, autocommit=True) as conn:
                assert conn.execute(
                    "SELECT pg_catalog.current_database()"
                ).fetchone() == (peer_database,)

        for gate, dsn, identity in (
            (
                require_runtime_database_role,
                separated_schema.runtime_dsn,
                separated_schema.runtime_identity,
            ),
            (
                require_migration_database_role,
                separated_schema.dsn,
                separated_schema.bootstrap_identity,
            ),
        ):
            with pytest.raises(
                DatabaseRoleError,
                match="database_role_preflight_failed",
            ):
                await gate(dsn, **identity)
    finally:
        with psycopg.connect(separated_schema.admin_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(peer_database)
                )
            )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["owner", "create_grant"])
async def test_runtime_gate_rejects_ddl_in_other_schema(
    separated_schema,
    capability,
):
    if capability == "owner":
        separated_schema.admin_execute(
            sql.SQL("CREATE SCHEMA runtime_escape AUTHORIZATION {}").format(
                sql.Identifier(separated_schema.runtime_role)
            )
        )
    else:
        separated_schema.admin_execute("CREATE SCHEMA runtime_escape")
        separated_schema.admin_execute(
            sql.SQL("GRANT CREATE ON SCHEMA runtime_escape TO {}").format(
                sql.Identifier(separated_schema.runtime_role)
            )
        )
    separated_schema.runtime_execute("CREATE TABLE runtime_escape.proof (id integer)")

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )
    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            separated_schema.dsn,
            **separated_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("privilege", ["CREATE", "TEMPORARY"])
async def test_migration_gate_rejects_runtime_database_privilege_before_ddl(
    empty_schema,
    privilege,
):
    empty_schema.admin_execute(
        sql.SQL("GRANT {} ON DATABASE {} TO {}").format(
            sql.SQL(privilege),
            sql.Identifier(empty_schema.database_name),
            sql.Identifier(empty_schema.runtime_role),
        )
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_gate_rejects_target_grant_option(separated_schema):
    separated_schema.execute(
        sql.SQL("GRANT SELECT ON public.emails_log TO {} WITH GRANT OPTION").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_both_gates_reject_runtime_sequence_setval_capability(
    separated_schema,
):
    separated_schema.execute("CREATE SEQUENCE public.runtime_forbidden_sequence")
    separated_schema.execute(
        sql.SQL(
            "GRANT UPDATE ON SEQUENCE public.runtime_forbidden_sequence TO {}"
        ).format(sql.Identifier(separated_schema.runtime_role))
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )
    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            separated_schema.dsn,
            **separated_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_rejects_target_schema_catalog_shadow_before_ddl(empty_schema):
    empty_schema.execute(
        "CREATE FUNCTION public.current_schema() RETURNS name "
        "LANGUAGE sql IMMUTABLE AS 'SELECT ''pg_catalog''::name'"
    )
    empty_schema.execute(
        "CREATE VIEW public.pg_class AS SELECT 0::pg_catalog.oid AS oid"
    )

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await bootstrap_database(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )

    assert not empty_schema.table_exists("alembic_version")
    assert not empty_schema.table_exists("checkpoint_migrations")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_resolves_builtin_types_before_target_domains(empty_schema):
    domain_types = {
        "text": "pg_catalog.text",
        "jsonb": "pg_catalog.jsonb",
        "timestamp": "pg_catalog.timestamp",
        "integer": "pg_catalog.int4",
        "bigint": "pg_catalog.int8",
        "bytea": "pg_catalog.bytea",
    }
    for domain_name, base_type in domain_types.items():
        empty_schema.execute(
            sql.SQL("CREATE DOMAIN public.{} AS {}").format(
                sql.Identifier(domain_name),
                sql.SQL(base_type),
            )
        )

    summary = await bootstrap_database(
        empty_schema.dsn,
        **empty_schema.bootstrap_identity,
    )

    assert summary["alembic"] == EXPECTED_DATABASE_REVISION
    shadowed_columns = empty_schema.scalar(
        "SELECT count(*) "
        "FROM pg_catalog.pg_attribute AS attribute "
        "JOIN pg_catalog.pg_class AS relation "
        "  ON relation.oid = attribute.attrelid "
        "JOIN pg_catalog.pg_namespace AS relation_schema "
        "  ON relation_schema.oid = relation.relnamespace "
        "JOIN pg_catalog.pg_type AS column_type "
        "  ON column_type.oid = attribute.atttypid "
        "WHERE relation_schema.nspname = 'public' "
        "  AND relation.relname IN ("
        "      'emails_log', 'app_kv_store', 'checkpoint_migrations', "
        "      'checkpoints', 'checkpoint_blobs', 'checkpoint_writes'"
        "  ) "
        "  AND column_type.typnamespace = "
        "      'public'::pg_catalog.regnamespace "
        "  AND column_type.typname = ANY(%s)",
        (list(domain_types),),
    )
    assert shadowed_columns == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_rejects_preexisting_shadowed_column_type(empty_schema):
    empty_schema.execute("CREATE DOMAIN public.text AS pg_catalog.text")
    unsafe_dsn = psycopg.conninfo.make_conninfo(
        empty_schema.dsn,
        options="-csearch_path=public,pg_catalog",
    )
    with psycopg.connect(unsafe_dsn, autocommit=True) as conn:
        conn.execute("CREATE TABLE public.emails_log (id text PRIMARY KEY)")

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await bootstrap_database(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )

    assert not empty_schema.table_exists("alembic_version")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_schema_contract_rejects_post_bootstrap_domain_column(
    separated_schema,
):
    separated_schema.execute(
        "CREATE DOMAIN public.checkpoint_version AS pg_catalog.int4"
    )
    separated_schema.execute(
        "ALTER TABLE public.checkpoint_migrations "
        "ALTER COLUMN v TYPE public.checkpoint_version "
        "USING v::pg_catalog.int4::public.checkpoint_version"
    )

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            separated_schema.runtime_dsn,
            target_schema="public",
            require_complete=True,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migration_gate_rejects_unowned_extended_schema_object(empty_schema):
    empty_schema.admin_execute(
        "CREATE OPERATOR public.=== ("
        "FUNCTION = pg_catalog.int4eq, LEFTARG = integer, RIGHTARG = integer)"
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_migration_gate_rejects_third_party_default_privileges(empty_schema):
    empty_schema.admin_execute(
        sql.SQL(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public "
            "GRANT SELECT ON TABLES TO pg_monitor"
        ).format(sql.Identifier(empty_schema.migration_role))
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("object_kind", ["functions", "types"])
async def test_migration_gate_rejects_builtin_public_default_privilege(
    empty_schema,
    object_kind,
):
    if object_kind == "functions":
        privilege = "EXECUTE"
        object_clause = "FUNCTIONS"
    else:
        privilege = "USAGE"
        object_clause = "TYPES"
    empty_schema.admin_execute(
        sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {} GRANT {} ON {} TO PUBLIC").format(
            sql.Identifier(empty_schema.migration_role),
            sql.SQL(privilege),
            sql.SQL(object_clause),
        )
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_migration_database_role(
            empty_schema.dsn,
            **empty_schema.bootstrap_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runtime_gate_rejects_explicit_public_execute_on_schema_routine(
    separated_schema,
):
    separated_schema.execute(
        "CREATE FUNCTION public.preflight_probe() RETURNS integer "
        "LANGUAGE sql IMMUTABLE AS 'SELECT 1'"
    )
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        separated_schema.runtime_execute("SELECT public.preflight_probe()")

    separated_schema.execute(
        "GRANT EXECUTE ON FUNCTION public.preflight_probe() TO PUBLIC"
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_security_definer_trigger_path(separated_schema):
    separated_schema.execute(
        "CREATE TABLE public.protected_trigger_effect (value pg_catalog.text)"
    )
    separated_schema.execute(
        "CREATE FUNCTION public.capture_runtime_insert() "
        "RETURNS pg_catalog.trigger "
        "LANGUAGE plpgsql SECURITY DEFINER "
        "SET search_path TO pg_catalog "
        "AS 'BEGIN "
        "INSERT INTO public.protected_trigger_effect(value) "
        "VALUES (NEW.id); RETURN NEW; END'"
    )
    separated_schema.execute(
        "CREATE TRIGGER capture_runtime_insert "
        "AFTER INSERT ON public.emails_log "
        "FOR EACH ROW EXECUTE FUNCTION public.capture_runtime_insert()"
    )
    separated_schema.execute(
        sql.SQL("GRANT INSERT ON public.emails_log TO {}").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    )

    separated_schema.runtime_execute(
        "INSERT INTO public.emails_log(id, status) VALUES ('trigger-proof', 'pending')"
    )
    assert (
        separated_schema.scalar(
            "SELECT count(*) FROM public.protected_trigger_effect "
            "WHERE value = 'trigger-proof'"
        )
        == 1
    )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            separated_schema.runtime_dsn,
            separated_schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            separated_schema.dsn,
            separated_schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "execution_hook",
    [
        "security_definer",
        "user_trigger",
        "orphan_trigger_function",
        "rewrite_rule",
        "event_trigger",
    ],
)
async def test_role_gates_reject_each_hidden_execution_hook(
    separated_schema,
    execution_hook,
):
    if execution_hook == "security_definer":
        separated_schema.execute(
            "CREATE FUNCTION public.hidden_execution_probe() "
            "RETURNS pg_catalog.int4 LANGUAGE sql SECURITY DEFINER "
            "AS 'SELECT 1'"
        )
    elif execution_hook == "user_trigger":
        separated_schema.execute(
            "CREATE FUNCTION public.hidden_trigger_probe() "
            "RETURNS pg_catalog.trigger LANGUAGE plpgsql "
            "AS 'BEGIN RETURN NEW; END'"
        )
        separated_schema.execute(
            "CREATE TRIGGER hidden_trigger_probe "
            "BEFORE INSERT ON public.emails_log "
            "FOR EACH ROW EXECUTE FUNCTION public.hidden_trigger_probe()"
        )
    elif execution_hook == "orphan_trigger_function":
        separated_schema.execute(
            "CREATE FUNCTION public.orphan_trigger_probe() "
            "RETURNS pg_catalog.trigger LANGUAGE plpgsql "
            "AS 'BEGIN RETURN NEW; END'"
        )
    elif execution_hook == "rewrite_rule":
        separated_schema.execute(
            "CREATE RULE hidden_rewrite_probe AS "
            "ON INSERT TO public.emails_log DO ALSO "
            "INSERT INTO public.app_kv_store(key, value) "
            "VALUES ('rewrite-proof', NEW.id)"
        )
    else:
        separated_schema.execute(
            "CREATE FUNCTION public.hidden_event_probe() "
            "RETURNS pg_catalog.event_trigger LANGUAGE plpgsql "
            "AS 'BEGIN END'"
        )
        separated_schema.admin_execute(
            "CREATE EVENT TRIGGER hidden_event_probe "
            "ON ddl_command_start "
            "EXECUTE FUNCTION public.hidden_event_probe()"
        )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            separated_schema.runtime_dsn,
            separated_schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            separated_schema.dsn,
            separated_schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_foreign_key_internal_actions(separated_schema):
    separated_schema.execute(
        "CREATE TABLE public.fk_parent (id pg_catalog.int4 PRIMARY KEY)"
    )
    separated_schema.execute(
        "CREATE TABLE public.fk_child ("
        "id pg_catalog.int4 PRIMARY KEY, "
        "parent_id pg_catalog.int4 REFERENCES public.fk_parent(id) "
        "ON UPDATE CASCADE)"
    )
    separated_schema.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON public.fk_parent TO {}").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    )
    separated_schema.execute(
        sql.SQL("GRANT SELECT, INSERT ON public.fk_child TO {}").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    )

    separated_schema.runtime_execute("INSERT INTO public.fk_parent VALUES (1)")
    separated_schema.runtime_execute("INSERT INTO public.fk_child VALUES (1, 1)")
    separated_schema.runtime_execute("UPDATE public.fk_parent SET id = 2 WHERE id = 1")
    assert (
        separated_schema.scalar("SELECT parent_id FROM public.fk_child WHERE id = 1")
        == 2
    )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            separated_schema.runtime_dsn,
            separated_schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            separated_schema.dsn,
            separated_schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_target_relation_inheritance(separated_schema):
    separated_schema.execute(
        "CREATE TABLE public.runtime_inheritance_probe ("
        "id pg_catalog.int4 PRIMARY KEY, payload pg_catalog.text)"
    )
    separated_schema.execute(
        "CREATE TABLE public.runtime_inheritance_probe_child () "
        "INHERITS (public.runtime_inheritance_probe)"
    )
    separated_schema.execute(
        sql.SQL(
            "GRANT SELECT, INSERT ON public.runtime_inheritance_probe TO {}"
        ).format(sql.Identifier(separated_schema.runtime_role))
    )
    separated_schema.execute(
        sql.SQL(
            "GRANT SELECT, INSERT, UPDATE ON "
            "public.runtime_inheritance_probe_child TO {}"
        ).format(sql.Identifier(separated_schema.runtime_role))
    )

    separated_schema.runtime_execute(
        "INSERT INTO public.runtime_inheritance_probe_child(id, payload) "
        "VALUES (1, 'original')"
    )
    separated_schema.runtime_execute(
        "UPDATE public.runtime_inheritance_probe_child "
        "SET payload = 'changed' WHERE id = 1"
    )
    assert (
        separated_schema.scalar(
            "SELECT payload FROM public.runtime_inheritance_probe_child WHERE id = 1"
        )
        == "changed"
    )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            separated_schema.runtime_dsn,
            separated_schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            separated_schema.dsn,
            separated_schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_allow_disabled_event_trigger(separated_schema):
    separated_schema.execute(
        "CREATE FUNCTION public.disabled_event_probe() "
        "RETURNS pg_catalog.event_trigger LANGUAGE plpgsql "
        "AS 'BEGIN END'"
    )
    separated_schema.admin_execute(
        "CREATE EVENT TRIGGER disabled_event_probe "
        "ON ddl_command_start "
        "EXECUTE FUNCTION public.disabled_event_probe()"
    )
    separated_schema.admin_execute("ALTER EVENT TRIGGER disabled_event_probe DISABLE")

    await require_runtime_database_role(
        separated_schema.runtime_dsn,
        **separated_schema.runtime_identity,
    )
    await require_migration_database_role(
        separated_schema.dsn,
        **separated_schema.bootstrap_identity,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_owner_rights_updatable_view(separated_schema):
    separated_schema.execute(
        "CREATE TABLE public.runtime_owner_probe ("
        "id pg_catalog.int4 PRIMARY KEY, payload pg_catalog.text)"
    )
    separated_schema.execute(
        sql.SQL("GRANT SELECT, INSERT ON public.runtime_owner_probe TO {}").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    )
    separated_schema.execute(
        "CREATE VIEW public.runtime_owner_probe_view AS "
        "SELECT id, payload FROM public.runtime_owner_probe"
    )
    separated_schema.execute(
        sql.SQL("GRANT SELECT, UPDATE ON public.runtime_owner_probe_view TO {}").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    )

    separated_schema.runtime_execute(
        "INSERT INTO public.runtime_owner_probe(id, payload) VALUES (1, 'original')"
    )
    separated_schema.runtime_execute(
        "UPDATE public.runtime_owner_probe_view "
        "SET payload = 'owner-rights-bypass' WHERE id = 1"
    )
    assert (
        separated_schema.scalar(
            "SELECT payload FROM public.runtime_owner_probe WHERE id = 1"
        )
        == "owner-rights-bypass"
    )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            separated_schema.runtime_dsn,
            separated_schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            separated_schema.dsn,
            separated_schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_allow_security_invoker_view(separated_schema):
    separated_schema.execute(
        "CREATE VIEW public.safe_runtime_view "
        "WITH (security_invoker = true) AS "
        "SELECT id, status FROM public.emails_log"
    )

    await require_runtime_database_role(
        separated_schema.runtime_dsn,
        **separated_schema.runtime_identity,
    )
    await require_migration_database_role(
        separated_schema.dsn,
        **separated_schema.bootstrap_identity,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_allow_inaccessible_legacy_owner_view(separated_schema):
    separated_schema.execute(
        "CREATE VIEW public.legacy_inaccessible_view AS "
        "SELECT id, status FROM public.emails_log"
    )
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        separated_schema.runtime_execute(
            "SELECT * FROM public.legacy_inaccessible_view"
        )

    await require_runtime_database_role(
        separated_schema.runtime_dsn,
        **separated_schema.runtime_identity,
    )
    await require_migration_database_role(
        separated_schema.dsn,
        **separated_schema.bootstrap_identity,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_accepts_inaccessible_legacy_processed_view(
    separated_schema,
):
    separated_schema.execute(
        "ALTER VIEW public.processed_emails SET (security_invoker = false)"
    )
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        separated_schema.runtime_execute("SELECT * FROM public.processed_emails")

    summary = await bootstrap_database(
        separated_schema.dsn,
        **separated_schema.bootstrap_identity,
    )

    assert summary["alembic"] == EXPECTED_DATABASE_REVISION
    await require_runtime_database_role(
        separated_schema.runtime_dsn,
        **separated_schema.runtime_identity,
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("privilege", ["DELETE", "TRUNCATE", "TRIGGER"])
async def test_runtime_gate_rejects_dangerous_relation_privilege(
    separated_schema,
    privilege,
):
    separated_schema.execute(
        sql.SQL("GRANT {} ON public.emails_log TO {}").format(
            sql.SQL(privilege),
            sql.Identifier(separated_schema.runtime_role),
        )
    )

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "capability",
    [
        "missing_select",
        "table_update",
        "column_update",
        "select_grant_option",
        "insert_grant_option",
    ],
)
async def test_runtime_gate_rejects_audit_mutation_or_delegation(
    separated_schema,
    capability,
):
    if capability == "missing_select":
        mutation = sql.SQL("REVOKE SELECT ON public.audit_events FROM {}").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    elif capability == "table_update":
        mutation = sql.SQL("GRANT UPDATE ON public.audit_events TO {}").format(
            sql.Identifier(separated_schema.runtime_role)
        )
    elif capability == "column_update":
        mutation = sql.SQL(
            "GRANT UPDATE (safe_metadata) ON public.audit_events TO {}"
        ).format(sql.Identifier(separated_schema.runtime_role))
    elif capability == "select_grant_option":
        mutation = sql.SQL(
            "GRANT SELECT ON public.audit_events TO {} WITH GRANT OPTION"
        ).format(sql.Identifier(separated_schema.runtime_role))
    else:
        mutation = sql.SQL(
            "GRANT INSERT ON public.audit_events TO {} WITH GRANT OPTION"
        ).format(sql.Identifier(separated_schema.runtime_role))
    separated_schema.execute(mutation)

    with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
        await require_runtime_database_role(
            separated_schema.runtime_dsn,
            **separated_schema.runtime_identity,
        )
