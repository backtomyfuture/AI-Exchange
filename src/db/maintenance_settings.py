"""Credentials used only by the explicit checkpoint maintenance process."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

from psycopg.conninfo import conninfo_to_dict
from pydantic import SecretStr

from src.db.migration_settings import _read_secret_file


_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")


class MaintenanceSettingsError(RuntimeError):
    """Safe, non-secret error raised for an invalid maintenance boundary."""


@dataclass(frozen=True)
class MaintenanceSettings:
    database_url: SecretStr
    expected_maintenance_role: str
    expected_runtime_role: str
    expected_migration_role: str
    expected_auditor_role: str
    target_schema: str


@dataclass(frozen=True)
class CheckpointPlanSettings:
    database_url: SecretStr
    expected_auditor_role: str
    expected_maintenance_role: str
    expected_runtime_role: str
    expected_migration_role: str
    target_schema: str


def _reject() -> MaintenanceSettingsError:
    return MaintenanceSettingsError("maintenance_settings_invalid")


def _read_identifier(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise _reject()
    return value


def load_maintenance_settings(
    environment: Mapping[str, str] | None = None,
) -> MaintenanceSettings:
    """Load one private maintenance DSN without any runtime fallback."""

    values = os.environ if environment is None else environment
    try:
        database_url = _read_secret_file(
            values.get("CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE", "")
        )
        maintenance_role = _read_identifier(
            values,
            "POSTGRES_MAINTENANCE_ROLE",
        )
        runtime_role = _read_identifier(values, "POSTGRES_RUNTIME_ROLE")
        migration_role = _read_identifier(
            values,
            "POSTGRES_MIGRATION_OWNER_ROLE",
        )
        auditor_role = _read_identifier(
            values,
            "POSTGRES_CHECKPOINT_AUDITOR_ROLE",
        )
        target_schema = _read_identifier(values, "POSTGRES_SCHEMA")
        if len({maintenance_role, runtime_role, migration_role, auditor_role}) != 4:
            raise _reject()

        parsed = conninfo_to_dict(database_url)
        if (
            parsed.get("user") != maintenance_role
            or not parsed.get("password")
            or not parsed.get("host")
            or not parsed.get("dbname")
            or parsed.get("options") != f"-csearch_path=pg_catalog,{target_schema}"
        ):
            raise _reject()
    except MaintenanceSettingsError:
        raise
    except Exception:
        raise _reject() from None

    return MaintenanceSettings(
        database_url=SecretStr(database_url),
        expected_maintenance_role=maintenance_role,
        expected_runtime_role=runtime_role,
        expected_migration_role=migration_role,
        expected_auditor_role=auditor_role,
        target_schema=target_schema,
    )


def load_checkpoint_plan_settings(
    environment: Mapping[str, str] | None = None,
) -> CheckpointPlanSettings:
    """Load the read-only plan identity; it never falls back to execute DSN."""

    values = os.environ if environment is None else environment
    try:
        database_url = _read_secret_file(
            values.get("CHECKPOINT_AUDITOR_DATABASE_URL_FILE", "")
        )
        auditor_role = _read_identifier(
            values,
            "POSTGRES_CHECKPOINT_AUDITOR_ROLE",
        )
        maintenance_role = _read_identifier(
            values,
            "POSTGRES_MAINTENANCE_ROLE",
        )
        runtime_role = _read_identifier(values, "POSTGRES_RUNTIME_ROLE")
        migration_role = _read_identifier(
            values,
            "POSTGRES_MIGRATION_OWNER_ROLE",
        )
        target_schema = _read_identifier(values, "POSTGRES_SCHEMA")
        if len({auditor_role, maintenance_role, runtime_role, migration_role}) != 4:
            raise _reject()

        parsed = conninfo_to_dict(database_url)
        if (
            parsed.get("user") != auditor_role
            or not parsed.get("password")
            or not parsed.get("host")
            or not parsed.get("dbname")
            or parsed.get("options") != f"-csearch_path=pg_catalog,{target_schema}"
        ):
            raise _reject()
    except MaintenanceSettingsError:
        raise
    except Exception:
        raise _reject() from None

    return CheckpointPlanSettings(
        database_url=SecretStr(database_url),
        expected_auditor_role=auditor_role,
        expected_maintenance_role=maintenance_role,
        expected_runtime_role=runtime_role,
        expected_migration_role=migration_role,
        target_schema=target_schema,
    )
