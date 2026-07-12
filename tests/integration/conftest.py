from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg_pool import AsyncConnectionPool


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _database_url(admin_url: str, database_name: str) -> str:
    parsed = urlsplit(admin_url)
    if not parsed.scheme or not parsed.netloc:
        return psycopg.conninfo.make_conninfo(admin_url, dbname=database_name)
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            f"/{database_name}",
            parsed.query,
            parsed.fragment,
        )
    )


@dataclass(frozen=True)
class SchemaProbe:
    """Small synchronous probe around one isolated PostgreSQL database."""

    dsn: str
    admin_dsn: str
    runtime_dsn: str
    database_name: str
    migration_role: str
    runtime_role: str

    @property
    def bootstrap_identity(self) -> dict[str, str]:
        return {
            "expected_migration_role": self.migration_role,
            "expected_runtime_role": self.runtime_role,
            "target_schema": "public",
        }

    @property
    def runtime_identity(self) -> dict[str, str]:
        return {
            "expected_runtime_role": self.runtime_role,
            "expected_migration_role": self.migration_role,
            "target_schema": "public",
        }

    def execute(self, statement: str, params=None) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(statement, params)

    def scalar(self, statement: str, params=None):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(statement, params).fetchone()
        return row[0] if row else None

    def admin_execute(self, statement, params=None) -> None:
        with psycopg.connect(self.admin_dsn, autocommit=True) as conn:
            conn.execute(statement, params)

    def runtime_execute(self, statement, params=None) -> None:
        with psycopg.connect(self.runtime_dsn, autocommit=True) as conn:
            conn.execute(statement, params)

    def grant_runtime_readiness(self) -> None:
        with psycopg.connect(self.admin_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                    sql.Identifier("public"),
                    sql.Identifier("alembic_version"),
                    sql.Identifier(self.runtime_role),
                )
            )

    def table_exists(self, table_name: str) -> bool:
        return bool(
            self.scalar(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s"
                ")",
                ("public", table_name),
            )
        )

    def column_exists(self, table_name: str, column_name: str) -> bool:
        return bool(
            self.scalar(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = %s "
                "AND table_name = %s AND column_name = %s"
                ")",
                ("public", table_name, column_name),
            )
        )


class MigrationHarness:
    """Run Alembic against an isolated database without importing it at collection."""

    def __init__(self, config_path: Path = PROJECT_ROOT / "alembic.ini"):
        self.config_path = config_path

    def upgrade(self, schema: SchemaProbe, revision: str) -> None:
        from alembic import command
        from alembic.config import Config

        config = Config(str(self.config_path))
        config.set_main_option("sqlalchemy.url", schema.dsn.replace("%", "%%"))
        command.upgrade(config, revision)


class PostgresDatabaseFactory:
    def __init__(self, admin_url: str):
        self.admin_url = admin_url
        self._resources: list[tuple[str, str, str]] = []

    def _role_database_url(
        self,
        *,
        database_name: str,
        role: str,
        password: str,
        search_path: str,
    ) -> str:
        parsed = urlsplit(self.admin_url)
        if not parsed.scheme or not parsed.hostname:
            raise RuntimeError("role-separated tests require a PostgreSQL URL")
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port else ""
        netloc = f"{quote(role, safe='')}:{quote(password, safe='')}@{host}{port}"
        query_items = [
            (name, value)
            for name, value in parse_qsl(
                parsed.query,
                keep_blank_values=True,
            )
            if name != "options"
        ]
        query_items.append(("options", f"-csearch_path={search_path}"))
        query = urlencode(query_items)
        return urlunsplit(
            (parsed.scheme, netloc, f"/{database_name}", query, parsed.fragment)
        )

    def create(self) -> SchemaProbe:
        token = uuid4().hex
        database_name = f"ai_exchange_test_{token}"
        migration_role = f"ai_exchange_test_m_{token}"
        runtime_role = f"ai_exchange_test_r_{token}"
        migration_password = f"Migration-{token}"
        runtime_password = f"Runtime-{token}"
        self._resources.append((database_name, runtime_role, migration_role))
        with psycopg.connect(self.admin_url, autocommit=True) as conn:
            conn.execute(
                sql.SQL(
                    "CREATE ROLE {} WITH LOGIN PASSWORD {} "
                    "NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS NOINHERIT"
                ).format(
                    sql.Identifier(migration_role),
                    sql.Literal(migration_password),
                )
            )
            conn.execute(
                sql.SQL(
                    "CREATE ROLE {} WITH LOGIN PASSWORD {} "
                    "NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS NOINHERIT"
                ).format(
                    sql.Identifier(runtime_role),
                    sql.Literal(runtime_password),
                )
            )
            conn.execute(
                sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    sql.Identifier(database_name),
                    sql.Identifier(migration_role),
                )
            )
            conn.execute(
                sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(
                    sql.Identifier(database_name)
                )
            )
            conn.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}").format(
                    sql.Identifier(database_name),
                    sql.Identifier(migration_role),
                    sql.Identifier(runtime_role),
                )
            )

        admin_dsn = _database_url(self.admin_url, database_name)
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                    sql.Identifier("public"), sql.Identifier(migration_role)
                )
            )
            conn.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
            conn.execute(
                sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            conn.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} "
                    "REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"
                ).format(sql.Identifier(migration_role))
            )
            conn.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} "
                    "REVOKE USAGE ON TYPES FROM PUBLIC"
                ).format(sql.Identifier(migration_role))
            )
            conn.execute(
                "REVOKE EXECUTE ON FUNCTION "
                "pg_catalog.lo_creat(pg_catalog.int4) FROM PUBLIC"
            )
            conn.execute(
                "REVOKE EXECUTE ON FUNCTION "
                "pg_catalog.lo_create(pg_catalog.oid) FROM PUBLIC"
            )
            conn.execute(
                "REVOKE EXECUTE ON FUNCTION "
                "pg_catalog.lo_from_bytea(pg_catalog.oid, pg_catalog.bytea) "
                "FROM PUBLIC"
            )

        return SchemaProbe(
            dsn=self._role_database_url(
                database_name=database_name,
                role=migration_role,
                password=migration_password,
                search_path="public",
            ),
            admin_dsn=admin_dsn,
            runtime_dsn=self._role_database_url(
                database_name=database_name,
                role=runtime_role,
                password=runtime_password,
                search_path="pg_catalog,public",
            ),
            database_name=database_name,
            migration_role=migration_role,
            runtime_role=runtime_role,
        )

    def close(self) -> None:
        with psycopg.connect(self.admin_url, autocommit=True) as conn:
            for database_name, runtime_role, migration_role in reversed(
                self._resources
            ):
                if not database_name.startswith("ai_exchange_test_"):
                    raise RuntimeError("unsafe test database cleanup identifier")
                if not runtime_role.startswith("ai_exchange_test_r_"):
                    raise RuntimeError("unsafe test runtime-role cleanup identifier")
                if not migration_role.startswith("ai_exchange_test_m_"):
                    raise RuntimeError("unsafe test migration-role cleanup identifier")
                conn.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (database_name,),
                )
                conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        sql.Identifier(database_name)
                    )
                )
                existing_roles = {
                    row[0]
                    for row in conn.execute(
                        "SELECT rolname FROM pg_catalog.pg_roles "
                        "WHERE rolname IN (%s, %s)",
                        (runtime_role, migration_role),
                    ).fetchall()
                }
                for role_name in (runtime_role, migration_role):
                    if role_name not in existing_roles:
                        continue
                    conn.execute(
                        sql.SQL(
                            "REVOKE ALL ON PARAMETER session_replication_role FROM {}"
                        ).format(sql.Identifier(role_name))
                    )
                    conn.execute(
                        sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role_name))
                    )
                if runtime_role in existing_roles:
                    conn.execute(
                        sql.SQL("DROP ROLE {}").format(sql.Identifier(runtime_role))
                    )
                if migration_role in existing_roles:
                    conn.execute(
                        sql.SQL("DROP ROLE {}").format(sql.Identifier(migration_role))
                    )
        self._resources.clear()


@pytest.fixture(scope="session")
def postgres_admin_url() -> str:
    admin_url = os.getenv("TEST_POSTGRES_ADMIN_URL")
    if not admin_url:
        pytest.skip(
            "TEST_POSTGRES_ADMIN_URL is required for PostgreSQL integration tests"
        )
    if os.getenv("TEST_POSTGRES_ROLE_DDL") != "1":
        pytest.skip("TEST_POSTGRES_ROLE_DDL=1 is required for role-DDL tests")
    return admin_url


@pytest.fixture(scope="session")
def peer_database_acl_boundary(postgres_admin_url: str) -> Iterator[None]:
    """Temporarily confine disposable test roles to their own database."""

    lock_key = 4_149_584_368_895_812
    with psycopg.connect(postgres_admin_url, autocommit=True) as conn:
        conn.execute("SELECT pg_catalog.pg_advisory_lock(%s)", (lock_key,))
        original_public_privileges: dict[str, list[tuple[str, bool]]] = {}
        database_names = [
            row[0]
            for row in conn.execute(
                "SELECT datname FROM pg_catalog.pg_database "
                "WHERE datallowconn ORDER BY datname"
            ).fetchall()
        ]
        for database_name, privilege_type, is_grantable in conn.execute(
            "SELECT database.datname, database_acl.privilege_type, "
            "database_acl.is_grantable "
            "FROM pg_catalog.pg_database AS database "
            "CROSS JOIN LATERAL pg_catalog.aclexplode("
            "  COALESCE(database.datacl, "
            "           pg_catalog.acldefault('d', database.datdba))"
            ") AS database_acl "
            "WHERE database.datallowconn "
            "  AND database_acl.grantee = 0 "
            "  AND database_acl.privilege_type IN ('CONNECT', 'TEMPORARY')"
        ):
            original_public_privileges.setdefault(database_name, []).append(
                (privilege_type, is_grantable)
            )
        try:
            for database_name in database_names:
                conn.execute(
                    sql.SQL(
                        "REVOKE CONNECT, TEMPORARY ON DATABASE {} FROM PUBLIC"
                    ).format(sql.Identifier(database_name))
                )
            yield
        finally:
            existing_databases = {
                row[0]
                for row in conn.execute(
                    "SELECT datname FROM pg_catalog.pg_database"
                ).fetchall()
            }
            for database_name in database_names:
                if database_name not in existing_databases:
                    continue
                conn.execute(
                    sql.SQL(
                        "REVOKE CONNECT, TEMPORARY ON DATABASE {} FROM PUBLIC"
                    ).format(sql.Identifier(database_name))
                )
                for privilege_type, is_grantable in original_public_privileges.get(
                    database_name,
                    [],
                ):
                    grant_option = (
                        sql.SQL(" WITH GRANT OPTION") if is_grantable else sql.SQL("")
                    )
                    conn.execute(
                        sql.SQL("GRANT {} ON DATABASE {} TO PUBLIC{}").format(
                            sql.SQL(privilege_type),
                            sql.Identifier(database_name),
                            grant_option,
                        )
                    )
            conn.execute("SELECT pg_catalog.pg_advisory_unlock(%s)", (lock_key,))


@pytest.fixture
def postgres_database_factory(
    postgres_admin_url: str,
    peer_database_acl_boundary: None,
) -> Iterator[Callable[[], SchemaProbe]]:
    admin_url = postgres_admin_url

    factory = PostgresDatabaseFactory(admin_url)
    try:
        yield factory.create
    finally:
        factory.close()


@pytest.fixture
def alembic_runner() -> MigrationHarness:
    return MigrationHarness()


@pytest.fixture
def empty_schema(postgres_database_factory) -> SchemaProbe:
    return postgres_database_factory()


@pytest.fixture
def legacy_schema(postgres_database_factory) -> SchemaProbe:
    schema = postgres_database_factory()
    schema.admin_execute(
        """
        CREATE TABLE emails_log (
            id TEXT PRIMARY KEY,
            subject TEXT,
            sender TEXT,
            received_at TIMESTAMP,
            status TEXT DEFAULT 'pending',
            classification JSONB,
            draft_content TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            routing_log JSONB,
            active_skills JSONB,
            original_draft TEXT,
            final_draft TEXT,
            draft_diff TEXT,
            approver_user_id TEXT,
            rejection_reason TEXT
        )
        """
    )
    schema.admin_execute(
        "INSERT INTO emails_log (id, subject, status) VALUES "
        "('legacy-1', 'First legacy email', 'waiting_approval'), "
        "('legacy-2', 'Second legacy email', 'sent')"
    )
    schema.admin_execute(
        sql.SQL("ALTER TABLE emails_log OWNER TO {}").format(
            sql.Identifier(schema.migration_role)
        )
    )
    return schema


@pytest.fixture
async def migrated_postgres_pool(
    postgres_database_factory, alembic_runner
) -> AsyncConnectionPool:
    schema = postgres_database_factory()
    alembic_runner.upgrade(schema, "head")
    pool = AsyncConnectionPool(conninfo=schema.dsn, open=False)
    await pool.open()
    try:
        yield pool
    finally:
        await pool.close()
