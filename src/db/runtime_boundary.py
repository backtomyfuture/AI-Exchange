"""Shared runtime database preflight for every checkpoint-writing entrypoint."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from src.db.schema import require_runtime_database


RuntimeDatabaseGate = Callable[..., Awaitable[None]]


async def require_runtime_database_boundary(
    settings: Any,
    *,
    require_database: RuntimeDatabaseGate = require_runtime_database,
) -> None:
    """Validate revision, schema and four-role identity without mutating the DB."""

    await require_database(
        settings.database_url,
        durable_inbox_enabled=bool(getattr(settings, "DURABLE_INBOX_ENABLED", False)),
        role_separation_required=bool(
            getattr(settings, "DATABASE_ROLE_SEPARATION_REQUIRED", False)
        ),
        expected_runtime_role=str(getattr(settings, "POSTGRES_USER", "")),
        expected_migration_role=str(
            getattr(settings, "POSTGRES_MIGRATION_OWNER_ROLE", "")
        ),
        expected_maintenance_role=str(
            getattr(settings, "POSTGRES_MAINTENANCE_ROLE", "")
        ),
        expected_auditor_role=str(
            getattr(settings, "POSTGRES_CHECKPOINT_AUDITOR_ROLE", "")
        ),
        target_schema=str(getattr(settings, "POSTGRES_SCHEMA", "public")),
    )
