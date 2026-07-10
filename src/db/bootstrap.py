"""Explicit deployment-time bootstrap for business and checkpoint schemas."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from src.config import get_settings
from src.db.schema import EXPECTED_DATABASE_REVISION


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_CONFIG_PATH = PROJECT_ROOT / "alembic.ini"


def _upgrade_business_schema(dsn: str) -> None:
    config = Config(str(ALEMBIC_CONFIG_PATH))
    config.set_main_option("sqlalchemy.url", dsn.replace("%", "%%"))
    command.upgrade(config, "head")


async def _apply_checkpoint_migrations(dsn: str) -> int:
    migrations = AsyncPostgresSaver.MIGRATIONS
    async with await psycopg.AsyncConnection.connect(
        dsn,
        autocommit=True,
        prepare_threshold=0,
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute(migrations[0])
            await cur.execute("SELECT v FROM checkpoint_migrations")
            applied = {row[0] for row in await cur.fetchall()}

        applied_count = 0
        for version, migration in enumerate(migrations):
            if version == 0 or version in applied:
                continue
            async with conn.cursor() as cur:
                await cur.execute(migration)
                await cur.execute(
                    "INSERT INTO checkpoint_migrations (v) VALUES (%s) "
                    "ON CONFLICT (v) DO NOTHING",
                    (version,),
                )
            applied_count += 1

    return applied_count


async def bootstrap_database(dsn: str) -> dict[str, str | int]:
    """Upgrade both schemas; this is the only supported schema-writing entrypoint."""
    await asyncio.to_thread(_upgrade_business_schema, dsn)
    checkpoint_count = await _apply_checkpoint_migrations(dsn)
    summary: dict[str, str | int] = {
        "alembic": EXPECTED_DATABASE_REVISION,
        "checkpoint": checkpoint_count,
    }
    logger.info("Database bootstrap complete: %s", summary)
    return summary


def main() -> None:
    settings = get_settings()
    asyncio.run(bootstrap_database(settings.database_url))


if __name__ == "__main__":
    main()
