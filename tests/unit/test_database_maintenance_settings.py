from __future__ import annotations

import base64
import importlib
import inspect
from pathlib import Path

import pytest
from pydantic import SecretStr

from scripts import checkpoint_cleanup


MAINTENANCE_DSN = (
    "postgresql://checkpoint_maintenance:Maintenance9Q2w7V4m@postgres:5432/"
    "email_agent?options=-csearch_path%3Dpg_catalog%2Cpublic"
)
AUDITOR_DSN = (
    "postgresql://checkpoint_auditor:Auditor9Q2w7V4m@postgres:5432/"
    "email_agent?options=-csearch_path%3Dpg_catalog%2Cpublic"
)


def _module():
    return importlib.import_module("src.db.maintenance_settings")


def _private_secret(
    path: Path,
    content: str = MAINTENANCE_DSN,
    *,
    mode: int = 0o600,
) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(mode)
    return path


def _environment(secret_path: Path) -> dict[str, str]:
    return {
        "CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE": str(secret_path),
        "POSTGRES_MAINTENANCE_ROLE": "checkpoint_maintenance",
        "POSTGRES_RUNTIME_ROLE": "runtime_user",
        "POSTGRES_MIGRATION_OWNER_ROLE": "migration_owner",
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE": "checkpoint_auditor",
        "POSTGRES_SCHEMA": "public",
    }


def _auditor_environment(secret_path: Path) -> dict[str, str]:
    return {
        "CHECKPOINT_AUDITOR_DATABASE_URL_FILE": str(secret_path),
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE": "checkpoint_auditor",
        "POSTGRES_MAINTENANCE_ROLE": "checkpoint_maintenance",
        "POSTGRES_RUNTIME_ROLE": "runtime_user",
        "POSTGRES_MIGRATION_OWNER_ROLE": "migration_owner",
        "POSTGRES_SCHEMA": "public",
    }


@pytest.mark.parametrize("mode", [0o400, 0o600])
def test_checkpoint_plan_settings_loads_only_distinct_auditor_dsn(
    tmp_path: Path,
    mode: int,
) -> None:
    module = _module()
    secret_path = _private_secret(
        tmp_path / "auditor-dsn",
        AUDITOR_DSN,
        mode=mode,
    )

    settings = module.load_checkpoint_plan_settings(_auditor_environment(secret_path))

    assert settings.database_url.get_secret_value() == AUDITOR_DSN
    assert settings.expected_auditor_role == "checkpoint_auditor"
    assert settings.expected_maintenance_role == "checkpoint_maintenance"
    assert AUDITOR_DSN not in repr(settings)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "checkpoint_maintenance"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "not-valid!"),
        ("POSTGRES_MAINTENANCE_ROLE", "checkpoint_auditor"),
    ],
)
def test_checkpoint_plan_settings_rejects_non_distinct_or_invalid_roles(
    tmp_path: Path,
    field_name: str,
    value: str,
) -> None:
    module = _module()
    secret_path = _private_secret(tmp_path / "auditor-dsn", AUDITOR_DSN)
    environment = _auditor_environment(secret_path)
    environment[field_name] = value

    with pytest.raises(
        module.MaintenanceSettingsError,
        match="maintenance_settings_invalid",
    ):
        module.load_checkpoint_plan_settings(environment)


@pytest.mark.parametrize("mode", [0o400, 0o600])
def test_maintenance_settings_loads_only_a_private_secret_file(
    tmp_path: Path,
    mode: int,
) -> None:
    module = _module()
    secret_path = _private_secret(tmp_path / "maintenance-dsn", mode=mode)

    settings = module.load_maintenance_settings(_environment(secret_path))

    assert isinstance(settings.database_url, SecretStr)
    assert settings.database_url.get_secret_value() == MAINTENANCE_DSN
    assert settings.expected_maintenance_role == "checkpoint_maintenance"
    assert settings.expected_runtime_role == "runtime_user"
    assert settings.expected_migration_role == "migration_owner"
    assert settings.expected_auditor_role == "checkpoint_auditor"
    assert settings.target_schema == "public"
    rendered = repr(settings)
    assert MAINTENANCE_DSN not in rendered
    assert "Maintenance9Q2w7V4m" not in rendered
    assert str(secret_path) not in rendered


@pytest.mark.parametrize(
    ("kind", "mode"),
    [
        ("missing", 0o600),
        ("directory", 0o600),
        ("group-readable", 0o640),
        ("world-readable", 0o604),
    ],
)
def test_maintenance_settings_rejects_unsafe_secret_sources_without_leaking(
    tmp_path: Path,
    kind: str,
    mode: int,
) -> None:
    module = _module()
    secret_path = tmp_path / f"maintenance-{kind}"
    if kind == "directory":
        secret_path.mkdir()
    elif kind != "missing":
        _private_secret(secret_path, mode=mode)

    with pytest.raises(module.MaintenanceSettingsError) as caught:
        module.load_maintenance_settings(_environment(secret_path))

    assert str(caught.value) == "maintenance_settings_invalid"
    assert MAINTENANCE_DSN not in str(caught.value)
    assert "Maintenance9Q2w7V4m" not in str(caught.value)
    assert str(secret_path) not in str(caught.value)
    assert caught.value.__cause__ is None


def test_maintenance_settings_rejects_symbolic_link(tmp_path: Path) -> None:
    module = _module()
    target = _private_secret(tmp_path / "target")
    link = tmp_path / "maintenance-link"
    link.symlink_to(target)

    with pytest.raises(module.MaintenanceSettingsError) as caught:
        module.load_maintenance_settings(_environment(link))

    assert str(caught.value) == "maintenance_settings_invalid"
    assert str(link) not in str(caught.value)
    assert str(target) not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("POSTGRES_MAINTENANCE_ROLE", "not-valid-role!"),
        ("POSTGRES_MAINTENANCE_ROLE", "runtime_user"),
        ("POSTGRES_MAINTENANCE_ROLE", "migration_owner"),
        ("POSTGRES_RUNTIME_ROLE", "not-valid-role!"),
        ("POSTGRES_RUNTIME_ROLE", "checkpoint_maintenance"),
        ("POSTGRES_RUNTIME_ROLE", "migration_owner"),
        ("POSTGRES_MIGRATION_OWNER_ROLE", "not-valid-role!"),
        ("POSTGRES_MIGRATION_OWNER_ROLE", "checkpoint_maintenance"),
        ("POSTGRES_MIGRATION_OWNER_ROLE", "runtime_user"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "not-valid-role!"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "checkpoint_maintenance"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "runtime_user"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "migration_owner"),
        ("POSTGRES_SCHEMA", "not-valid-schema!"),
    ],
)
def test_maintenance_settings_rejects_invalid_identifiers(
    tmp_path: Path,
    field_name: str,
    value: str,
) -> None:
    module = _module()
    secret_path = _private_secret(tmp_path / "maintenance-dsn")
    environment = _environment(secret_path)
    environment[field_name] = value

    with pytest.raises(
        module.MaintenanceSettingsError,
        match="maintenance_settings_invalid",
    ):
        module.load_maintenance_settings(environment)


@pytest.mark.parametrize(
    "database_url",
    [
        (
            "postgresql://wrong_role:private@postgres:5432/email_agent"
            "?options=-csearch_path%3Dpg_catalog%2Cpublic"
        ),
        (
            "postgresql://checkpoint_maintenance@postgres:5432/email_agent"
            "?options=-csearch_path%3Dpg_catalog%2Cpublic"
        ),
        (
            "postgresql://checkpoint_maintenance:private@postgres:5432/email_agent"
            "?options=-csearch_path%3Dpublic"
        ),
        (
            "postgresql://checkpoint_maintenance:private@postgres:5432/email_agent"
            "?options=-csearch_path%3Dpublic%2Cpg_catalog"
        ),
    ],
)
def test_maintenance_settings_requires_exact_identity_and_search_path(
    tmp_path: Path,
    database_url: str,
) -> None:
    module = _module()
    secret_path = _private_secret(tmp_path / "maintenance-dsn", database_url)

    with pytest.raises(
        module.MaintenanceSettingsError,
        match="maintenance_settings_invalid",
    ):
        module.load_maintenance_settings(_environment(secret_path))


def test_maintenance_settings_never_falls_back_to_runtime_database_values(
    tmp_path: Path,
) -> None:
    module = _module()
    environment = {
        "DATABASE_URL": "postgresql://runtime:runtime-password-sentinel@runtime/db",
        "POSTGRES_HOST": "runtime-db",
        "POSTGRES_USER": "runtime-user",
        "POSTGRES_PASSWORD": "runtime-password-sentinel",
        "POSTGRES_DB": "runtime-db-name",
        "POSTGRES_MAINTENANCE_ROLE": "checkpoint_maintenance",
        "POSTGRES_RUNTIME_ROLE": "runtime_user",
        "POSTGRES_MIGRATION_OWNER_ROLE": "migration_owner",
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE": "checkpoint_auditor",
        "POSTGRES_SCHEMA": "public",
    }

    with pytest.raises(module.MaintenanceSettingsError) as caught:
        module.load_maintenance_settings(environment)

    assert str(caught.value) == "maintenance_settings_invalid"
    assert "runtime-password-sentinel" not in str(caught.value)
    assert caught.value.__cause__ is None


def test_cleanup_cli_repository_uses_only_maintenance_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _module()
    maintenance_settings = module.MaintenanceSettings(
        database_url=SecretStr(MAINTENANCE_DSN),
        expected_maintenance_role="checkpoint_maintenance",
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )

    monkeypatch.setattr(
        checkpoint_cleanup,
        "load_maintenance_settings",
        lambda: maintenance_settings,
    )

    cleaner = checkpoint_cleanup._build_cleaner(
        state_dir=tmp_path / "artifacts",
        require_backup_verifier=False,
    )

    assert cleaner._repository._dsn == MAINTENANCE_DSN  # type: ignore[attr-defined]


def test_cleanup_execute_uses_control_plane_public_key_without_runtime_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _module()
    from src.maintenance.cleanup_backup import Ed25519BackupReceiptVerifier

    maintenance_settings = module.MaintenanceSettings(
        database_url=SecretStr(MAINTENANCE_DSN),
        expected_maintenance_role="checkpoint_maintenance",
        expected_runtime_role="runtime_user",
        expected_migration_role="migration_owner",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )
    encoded_key = base64.b64encode(b"k" * 32).decode("ascii")
    key_file = _private_secret(
        tmp_path / "maintenance-receipt-key",
        encoded_key,
        mode=0o400,
    )

    monkeypatch.setattr(
        checkpoint_cleanup,
        "load_maintenance_settings",
        lambda: maintenance_settings,
    )
    monkeypatch.setenv(
        checkpoint_cleanup.RECEIPT_PUBLIC_KEY_FILE_ENV,
        str(key_file),
    )

    cleaner = checkpoint_cleanup._build_cleaner(
        state_dir=tmp_path / "artifacts",
        require_backup_verifier=True,
    )

    assert cleaner._repository._dsn == MAINTENANCE_DSN  # type: ignore[attr-defined]
    assert isinstance(  # type: ignore[attr-defined]
        cleaner._backup_verifier,
        Ed25519BackupReceiptVerifier,
    )


def test_runtime_settings_model_never_contains_maintenance_database_secret() -> None:
    from src.config import Settings

    assert "CHECKPOINT_MAINTENANCE_DATABASE_URL" not in Settings.model_fields
    assert "CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE" not in Settings.model_fields
    assert (
        "CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE"
        not in Settings.model_fields
    )
    assert "POSTGRES_MAINTENANCE_PASSWORD" not in Settings.model_fields
    assert "CHECKPOINT_CLEANUP_RECEIPT_HMAC_KEY_B64" not in Settings.model_fields
    assert "get_settings" not in inspect.getsource(checkpoint_cleanup)
