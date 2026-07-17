from __future__ import annotations

import importlib
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import SecretStr


MIGRATION_DSN = (
    "postgresql://migration_owner:Migration9Q2w7V4m@postgres:5432/email_agent"
    "?options=-csearch_path%3Dpublic"
)


def _module():
    return importlib.import_module("src.db.migration_settings")


def _private_secret(path: Path, content: str = MIGRATION_DSN) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)
    return path


def _environment(secret_path: Path) -> dict[str, str]:
    return {
        "MIGRATION_DATABASE_URL_FILE": str(secret_path),
        "POSTGRES_MIGRATION_OWNER_ROLE": "migration_owner",
        "POSTGRES_RUNTIME_ROLE": "runtime_user",
        "POSTGRES_MAINTENANCE_ROLE": "maintenance_user",
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE": "checkpoint_auditor",
        "POSTGRES_SCHEMA": "public",
    }


def test_migration_settings_loads_only_a_private_secret_file(tmp_path: Path):
    module = _module()
    secret_path = _private_secret(tmp_path / "migration-dsn")

    settings = module.load_migration_settings(_environment(secret_path))

    assert isinstance(settings.database_url, SecretStr)
    assert settings.database_url.get_secret_value() == MIGRATION_DSN
    assert settings.expected_migration_role == "migration_owner"
    assert settings.expected_runtime_role == "runtime_user"
    assert settings.expected_maintenance_role == "maintenance_user"
    assert settings.expected_auditor_role == "checkpoint_auditor"
    assert settings.target_schema == "public"
    rendered = repr(settings)
    assert MIGRATION_DSN not in rendered
    assert "Migration9Q2w7V4m" not in rendered
    assert str(secret_path) not in rendered


def test_migration_settings_reads_until_eof_when_os_returns_short_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    module = _module()
    secret_path = _private_secret(tmp_path / "migration-dsn")
    real_read = module.os.read

    def short_read(fd: int, size: int) -> bytes:
        return real_read(fd, min(size, 7))

    monkeypatch.setattr(module.os, "read", short_read)

    settings = module.load_migration_settings(_environment(secret_path))

    assert settings.database_url.get_secret_value() == MIGRATION_DSN


def test_migration_settings_rejects_metadata_change_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    module = _module()
    secret_path = _private_secret(tmp_path / "migration-dsn")
    fd = os.open(secret_path, os.O_RDONLY)
    try:
        real = os.fstat(fd)
    finally:
        os.close(fd)
    changed = SimpleNamespace(
        st_mode=real.st_mode,
        st_uid=real.st_uid,
        st_dev=real.st_dev,
        st_ino=real.st_ino,
        st_size=real.st_size,
        st_mtime_ns=real.st_mtime_ns + 1,
        st_ctime_ns=real.st_ctime_ns,
    )
    real_fstat = module.os.fstat
    calls = 0

    def changing_fstat(open_fd: int):
        nonlocal calls
        calls += 1
        if calls == 2:
            return changed
        return real_fstat(open_fd)

    monkeypatch.setattr(module.os, "fstat", changing_fstat)

    with pytest.raises(
        module.MigrationSettingsError, match="migration_settings_invalid"
    ):
        module.load_migration_settings(_environment(secret_path))


@pytest.mark.parametrize("mode", [0o700, 0o500, 0o300])
def test_migration_settings_rejects_non_secret_owner_modes(
    tmp_path: Path,
    mode: int,
):
    module = _module()
    secret_path = _private_secret(tmp_path / "migration-dsn")
    secret_path.chmod(mode)

    with pytest.raises(
        module.MigrationSettingsError, match="migration_settings_invalid"
    ):
        module.load_migration_settings(_environment(secret_path))


@pytest.mark.parametrize(
    ("kind", "content", "mode"),
    [
        ("missing", MIGRATION_DSN, 0o600),
        ("directory", MIGRATION_DSN, 0o600),
        ("empty", "", 0o600),
        ("multiline", f"{MIGRATION_DSN}\npostgresql://second", 0o600),
        ("nul", f"{MIGRATION_DSN}\x00tail", 0o600),
        ("oversized", "x" * 8193, 0o600),
        ("group-readable", MIGRATION_DSN, 0o640),
        ("world-readable", MIGRATION_DSN, 0o604),
    ],
)
def test_migration_settings_rejects_unsafe_secret_sources_without_leaking(
    tmp_path: Path,
    kind: str,
    content: str,
    mode: int,
):
    module = _module()
    secret_path = tmp_path / f"migration-{kind}"
    if kind == "directory":
        secret_path.mkdir()
    elif kind != "missing":
        secret_path.write_bytes(content.encode("utf-8"))
        secret_path.chmod(mode)

    with pytest.raises(module.MigrationSettingsError) as caught:
        module.load_migration_settings(_environment(secret_path))

    message = str(caught.value)
    assert "migration_settings_invalid" in message
    assert MIGRATION_DSN not in message
    assert "Migration9Q2w7V4m" not in message
    assert str(secret_path) not in message
    assert caught.value.__cause__ is None


def test_migration_settings_rejects_symbolic_link(tmp_path: Path):
    module = _module()
    target = _private_secret(tmp_path / "target")
    link = tmp_path / "migration-link"
    link.symlink_to(target)

    with pytest.raises(module.MigrationSettingsError) as caught:
        module.load_migration_settings(_environment(link))

    assert str(link) not in str(caught.value)
    assert str(target) not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO requires POSIX")
def test_migration_settings_rejects_fifo_without_blocking(tmp_path: Path):
    module = _module()
    fifo = tmp_path / "migration-fifo"
    os.mkfifo(fifo, mode=0o600)

    with pytest.raises(
        module.MigrationSettingsError, match="migration_settings_invalid"
    ):
        module.load_migration_settings(_environment(fifo))


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("POSTGRES_MIGRATION_OWNER_ROLE", "runtime_user"),
        ("POSTGRES_MIGRATION_OWNER_ROLE", "maintenance_user"),
        ("POSTGRES_MIGRATION_OWNER_ROLE", "not-valid-role!"),
        ("POSTGRES_RUNTIME_ROLE", "maintenance_user"),
        ("POSTGRES_RUNTIME_ROLE", "not-valid-role!"),
        ("POSTGRES_MAINTENANCE_ROLE", "migration_owner"),
        ("POSTGRES_MAINTENANCE_ROLE", "runtime_user"),
        ("POSTGRES_MAINTENANCE_ROLE", "not-valid-role!"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "migration_owner"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "runtime_user"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "maintenance_user"),
        ("POSTGRES_CHECKPOINT_AUDITOR_ROLE", "not-valid-role!"),
        ("POSTGRES_SCHEMA", "not-valid-schema!"),
    ],
)
def test_migration_settings_rejects_invalid_identity_contract(
    tmp_path: Path,
    field_name: str,
    value: str,
):
    module = _module()
    secret_path = _private_secret(tmp_path / "migration-dsn")
    environment = _environment(secret_path)
    environment[field_name] = value

    with pytest.raises(
        module.MigrationSettingsError, match="migration_settings_invalid"
    ):
        module.load_migration_settings(environment)


def test_runtime_settings_model_never_contains_admin_or_migration_secrets():
    from src.config import Settings

    forbidden = {
        "MIGRATION_DATABASE_URL",
        "MIGRATION_DATABASE_URL_FILE",
        "POSTGRES_ADMIN_PASSWORD",
        "POSTGRES_MIGRATION_PASSWORD",
    }

    assert forbidden.isdisjoint(Settings.model_fields)
    assert Settings.model_config["env_file"] == ".env.runtime"
    assert Settings(_env_file=None).LEADER_SENDERS == ""


def test_bootstrap_cli_uses_only_the_dedicated_migration_settings(tmp_path: Path):
    from src.db import bootstrap as bootstrap_module

    module = _module()
    settings = module.MigrationSettings(
        database_url=SecretStr(MIGRATION_DSN),
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )
    bootstrap = AsyncMock(return_value={"alembic": "20260710_0002", "checkpoint": 9})

    with (
        patch.object(
            bootstrap_module,
            "load_migration_settings",
            return_value=settings,
            create=True,
        ) as loader,
        patch.object(
            bootstrap_module,
            "get_settings",
            side_effect=AssertionError("runtime settings must not be loaded"),
            create=True,
        ),
        patch.object(bootstrap_module, "bootstrap_database", new=bootstrap),
    ):
        bootstrap_module.main()

    loader.assert_called_once_with()
    bootstrap.assert_awaited_once_with(
        MIGRATION_DSN,
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )


def test_bootstrap_cli_cuts_off_downstream_secret_bearing_exception(
    tmp_path: Path,
):
    from src.db import bootstrap as bootstrap_module

    module = _module()
    settings = module.MigrationSettings(
        database_url=SecretStr(MIGRATION_DSN),
        expected_migration_role="migration_owner",
        expected_runtime_role="runtime_user",
        expected_maintenance_role="maintenance_user",
        expected_auditor_role="checkpoint_auditor",
        target_schema="public",
    )
    downstream = RuntimeError(f"connection failed: {MIGRATION_DSN}")

    with (
        patch.object(
            bootstrap_module,
            "load_migration_settings",
            return_value=settings,
        ),
        patch.object(
            bootstrap_module,
            "bootstrap_database",
            new=AsyncMock(side_effect=downstream),
        ),
        pytest.raises(RuntimeError) as caught,
    ):
        bootstrap_module.main()

    assert str(caught.value) == "database_bootstrap_failed"
    assert MIGRATION_DSN not in str(caught.value)
    assert "Migration9Q2w7V4m" not in str(caught.value)
    assert caught.value.__cause__ is None


def test_application_context_has_no_dotenv_import_side_effect():
    source = (Path(__file__).resolve().parents[2] / "src" / "init_app.py").read_text(
        encoding="utf-8"
    )

    assert "load_dotenv" not in source
    assert "from dotenv" not in source


def test_migration_settings_does_not_fall_back_to_runtime_environment(
    tmp_path: Path,
):
    module = _module()
    environment = {
        "POSTGRES_HOST": "runtime-db",
        "POSTGRES_USER": "runtime-user",
        "POSTGRES_PASSWORD": "runtime-password-sentinel",
        "POSTGRES_DB": "runtime-db-name",
        "POSTGRES_MIGRATION_OWNER_ROLE": "migration_owner",
        "POSTGRES_RUNTIME_ROLE": "runtime_user",
        "POSTGRES_MAINTENANCE_ROLE": "maintenance_user",
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE": "checkpoint_auditor",
        "POSTGRES_SCHEMA": "public",
    }

    with pytest.raises(module.MigrationSettingsError) as caught:
        module.load_migration_settings(environment)

    assert "runtime-password-sentinel" not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql://migration_owner:private@postgres/email_agent",
        (
            "postgresql://migration_owner:private@postgres/email_agent"
            "?options=-csearch_path%3Dpg_catalog%2Cpublic"
        ),
        (
            "postgresql://migration_owner:private@postgres/email_agent"
            "?options=-csearch_path%3Dpublic%2Cpg_catalog"
        ),
    ],
)
def test_migration_settings_requires_exact_migration_search_path(
    tmp_path: Path,
    database_url: str,
):
    module = _module()
    secret_path = _private_secret(tmp_path / "migration-dsn", database_url)

    with pytest.raises(
        module.MigrationSettingsError, match="migration_settings_invalid"
    ):
        module.load_migration_settings(_environment(secret_path))


def test_migration_settings_rejects_libpq_keyword_dsn_before_alembic(
    tmp_path: Path,
):
    module = _module()
    secret_path = _private_secret(
        tmp_path / "migration-dsn",
        "host=postgres dbname=email_agent user=migration_owner "
        "password=Migration9Q2w7V4m options=-csearch_path=public",
    )

    with pytest.raises(
        module.MigrationSettingsError, match="migration_settings_invalid"
    ) as caught:
        module.load_migration_settings(_environment(secret_path))

    assert "Migration9Q2w7V4m" not in str(caught.value)
    assert caught.value.__cause__ is None


def test_runtime_database_url_fixes_catalog_first_search_path():
    from psycopg.conninfo import conninfo_to_dict

    from src.config import Settings

    settings = Settings(
        _env_file=None,
        POSTGRES_HOST="postgres",
        POSTGRES_DB="email_agent",
        POSTGRES_USER="runtime_user",
        POSTGRES_PASSWORD=SecretStr("runtime-password"),
        POSTGRES_SCHEMA="public",
    )

    parsed = conninfo_to_dict(settings.database_url)

    assert parsed["options"] == "-csearch_path=pg_catalog,public"
