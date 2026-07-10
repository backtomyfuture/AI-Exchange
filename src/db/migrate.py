"""Compatibility wrapper for the explicit database bootstrap command."""

from __future__ import annotations

import logging

from src.db.bootstrap import bootstrap_database


logger = logging.getLogger(__name__)


async def run_migrations(
    dsn: str,
    migrations_dir: str | None = None,
    *,
    apply_checkpoint_migrations: bool = True,
) -> dict[str, str | int]:
    """Delegate legacy callers to bootstrap without reading filesystem SQL."""
    if migrations_dir is not None or not apply_checkpoint_migrations:
        logger.warning(
            "Legacy migration options are ignored; explicit bootstrap always "
            "upgrades both schemas."
        )
    return await bootstrap_database(dsn)
