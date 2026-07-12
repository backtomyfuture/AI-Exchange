from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
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
        (parsed.scheme, parsed.netloc, f"/{database_name}", parsed.query, parsed.fragment)
    )


@dataclass(frozen=True)
class SchemaProbe:
    """Small synchronous probe around one isolated PostgreSQL database."""

    dsn: str

    @property
    def bootstrap_identity(self) -> dict[str, str]:
        return {
            "expected_migration_role": str(self.scalar("SELECT current_user")),
            "expected_runtime_role": "ai_exchange_test_runtime",
            "target_schema": str(self.scalar("SELECT current_schema()")),
        }

    def execute(self, statement: str, params=None) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(statement, params)

    def scalar(self, statement: str, params=None):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(statement, params).fetchone()
        return row[0] if row else None

    def table_exists(self, table_name: str) -> bool:
        return bool(
            self.scalar(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = current_schema() AND table_name = %s"
                ")",
                (table_name,),
            )
        )

    def column_exists(self, table_name: str, column_name: str) -> bool:
        return bool(
            self.scalar(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = current_schema() "
                "AND table_name = %s AND column_name = %s"
                ")",
                (table_name, column_name),
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
        self._database_names: list[str] = []

    def create(self) -> SchemaProbe:
        database_name = f"ai_exchange_test_{uuid4().hex}"
        with psycopg.connect(self.admin_url, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
            )
        self._database_names.append(database_name)
        return SchemaProbe(_database_url(self.admin_url, database_name))

    def close(self) -> None:
        with psycopg.connect(self.admin_url, autocommit=True) as conn:
            for database_name in reversed(self._database_names):
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
        self._database_names.clear()


@pytest.fixture
def postgres_database_factory() -> Iterator[Callable[[], SchemaProbe]]:
    admin_url = os.getenv("TEST_POSTGRES_ADMIN_URL")
    if not admin_url:
        pytest.skip("TEST_POSTGRES_ADMIN_URL is required for PostgreSQL integration tests")

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
    schema.execute(
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
    schema.execute(
        "INSERT INTO emails_log (id, subject, status) VALUES "
        "('legacy-1', 'First legacy email', 'waiting_approval'), "
        "('legacy-2', 'Second legacy email', 'sent')"
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
