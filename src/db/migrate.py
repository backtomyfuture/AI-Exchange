"""Compatibility wrapper for the explicit database bootstrap command."""

from __future__ import annotations

import logging

from src.db.bootstrap import bootstrap_database
from src.db.migration_settings import load_migration_settings


logger = logging.getLogger(__name__)


async def run_migrations(
    *,
    migrations_dir: str | None = None,
    apply_checkpoint_migrations: bool = True,
) -> dict[str, str | int]:
    """Use the dedicated migration secret; caller-supplied DSNs are forbidden."""
    if migrations_dir is not None or not apply_checkpoint_migrations:
        logger.warning(
            "Legacy migration options are ignored; explicit bootstrap always "
            "upgrades both schemas."
        )
    try:
        settings = load_migration_settings()
        return await bootstrap_database(
            settings.database_url.get_secret_value(),
            expected_migration_role=settings.expected_migration_role,
            expected_runtime_role=settings.expected_runtime_role,
            expected_maintenance_role=settings.expected_maintenance_role,
            expected_auditor_role=settings.expected_auditor_role,
            target_schema=settings.target_schema,
        )
    except Exception:
        raise RuntimeError("database_bootstrap_failed") from None
