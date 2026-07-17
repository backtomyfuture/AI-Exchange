from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from psycopg import sql
from pydantic import SecretStr

from src.db import provision


ADMIN_DSN = "postgresql://cluster_admin:AdminOnly9Q2w7V4m@postgres:5432/email_agent"
PASSWORDS = {
    "POSTGRES_MIGRATION_PASSWORD_FILE": "Migration9Q2w7V4m",
    "POSTGRES_RUNTIME_PASSWORD_FILE": "Runtime8R3x6W5n2",
    "POSTGRES_MAINTENANCE_PASSWORD_FILE": "Maintenance7T4y8X6p",
    "POSTGRES_CHECKPOINT_AUDITOR_PASSWORD_FILE": "Auditor6U5z9Y7q3",
}


def _private_file(path: Path, value: str) -> Path:
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


def _environment(tmp_path: Path) -> dict[str, str]:
    values = {
        "DATABASE_PROVISION_ADMIN_URL_FILE": str(
            _private_file(tmp_path / "admin-url", ADMIN_DSN)
        ),
        "POSTGRES_DB": "email_agent",
        "POSTGRES_SCHEMA": "public",
        "POSTGRES_MIGRATION_OWNER_ROLE": "migration_owner",
        "POSTGRES_RUNTIME_ROLE": "runtime_user",
        "POSTGRES_MAINTENANCE_ROLE": "maintenance_user",
        "POSTGRES_CHECKPOINT_AUDITOR_ROLE": "checkpoint_auditor",
    }
    for name, password in PASSWORDS.items():
        values[name] = str(_private_file(tmp_path / name.casefold(), password))
    return values


def test_provision_settings_loads_five_private_files_without_rendering_secrets(
    tmp_path: Path,
):
    settings = provision.load_provision_settings(_environment(tmp_path))

    assert isinstance(settings.admin_database_url, SecretStr)
    assert settings.database_name == "email_agent"
    assert settings.target_schema == "public"
    assert tuple(role.name for role in settings.roles) == (
        "migration_owner",
        "runtime_user",
        "maintenance_user",
        "checkpoint_auditor",
    )
    rendered = repr(settings)
    assert ADMIN_DSN not in rendered
    assert "AdminOnly9Q2w7V4m" not in rendered
    for password in PASSWORDS.values():
        assert password not in rendered


@pytest.mark.parametrize(
    ("mutation", "value"),
    [
        ("POSTGRES_SCHEMA", "tenant"),
        ("POSTGRES_RUNTIME_ROLE", "migration_owner"),
        ("POSTGRES_DB", "email-agent"),
        ("POSTGRES_DB", "postgres"),
        ("POSTGRES_DB", "template1"),
    ],
)
def test_provision_settings_rejects_non_greenfield_identity_contract(
    tmp_path: Path,
    mutation: str,
    value: str,
):
    environment = _environment(tmp_path)
    environment[mutation] = value

    with pytest.raises(
        provision.DatabaseProvisionError,
        match="database_provision_invalid",
    ) as caught:
        provision.load_provision_settings(environment)

    assert caught.value.__cause__ is None
    assert ADMIN_DSN not in str(caught.value)


@pytest.mark.parametrize(
    "admin_dsn",
    [
        "postgresql://migration_owner:AdminOnly9Q2w7V4m@postgres/email_agent",
        "postgresql://cluster_admin:AdminOnly9Q2w7V4m@postgres/other_database",
        (
            "postgresql://cluster_admin:AdminOnly9Q2w7V4m@postgres/email_agent"
            "?options=-csearch_path%3Dpublic"
        ),
    ],
)
def test_provision_settings_rejects_admin_boundary_confusion(
    tmp_path: Path,
    admin_dsn: str,
):
    environment = _environment(tmp_path)
    _private_file(Path(environment["DATABASE_PROVISION_ADMIN_URL_FILE"]), admin_dsn)

    with pytest.raises(provision.DatabaseProvisionError) as caught:
        provision.load_provision_settings(environment)

    assert caught.value.__cause__ is None
    assert admin_dsn not in str(caught.value)


def test_provision_settings_rejects_non_private_password_file(tmp_path: Path):
    environment = _environment(tmp_path)
    password_path = Path(environment["POSTGRES_RUNTIME_PASSWORD_FILE"])
    password_path.chmod(0o644)

    with pytest.raises(provision.DatabaseProvisionError) as caught:
        provision.load_provision_settings(environment)

    assert caught.value.__cause__ is None
    assert str(password_path) not in str(caught.value)


class _RecordingCursor:
    def __init__(
        self,
        rows: list[tuple[object, ...]] | None = None,
        one: tuple[object, ...] | None = None,
    ):
        self.rows = rows or []
        self.one = one
        self.statements: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(
        self,
        statement: str | sql.Composable,
        parameters: tuple[object, ...] | None = None,
    ) -> None:
        rendered = (
            statement.as_string(None)
            if isinstance(statement, sql.Composable)
            else statement
        )
        self.statements.append((rendered, parameters))

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    def fetchone(self) -> tuple[object, ...] | None:
        return self.one


def test_dedicated_cluster_accepts_only_target_and_official_system_databases(
    tmp_path: Path,
):
    settings = provision.load_provision_settings(_environment(tmp_path))
    exact = _RecordingCursor(
        rows=[
            ("email_agent", False, True),
            ("postgres", False, True),
            ("template0", True, False),
            ("template1", True, True),
        ]
    )

    provision._require_dedicated_cluster(exact, settings)

    with_peer = _RecordingCursor(
        rows=[
            ("email_agent", False, True),
            ("other_business_database", False, True),
            ("postgres", False, True),
            ("template0", True, False),
            ("template1", True, True),
        ]
    )
    with pytest.raises(provision.DatabaseProvisionError):
        provision._require_dedicated_cluster(with_peer, settings)


def test_partial_preexisting_role_set_is_rejected_before_any_sql(tmp_path: Path):
    settings = provision.load_provision_settings(_environment(tmp_path))
    cursor = _RecordingCursor()

    with pytest.raises(provision.DatabaseProvisionError):
        provision._require_existing_roles_safe(
            cursor,
            settings,
            {settings.runtime.name},
        )

    assert cursor.statements == []


def test_cluster_authority_baseline_is_exact_and_fails_closed(tmp_path: Path):
    settings = provision.load_provision_settings(_environment(tmp_path))
    accepted = _RecordingCursor(one=(True,))

    provision._require_cluster_authority_baseline(accepted, settings)

    statement, parameters = accepted.statements[0]
    assert "pg_catalog.pg_auth_members" in statement
    assert "pg_catalog.pg_db_role_setting" in statement
    assert "role.rolname NOT LIKE 'pg\\_%%'" in statement
    assert parameters == (
        ["pg_read_all_settings", "pg_read_all_stats", "pg_stat_scan_tables"],
        ["pg_monitor", "pg_monitor", "pg_monitor"],
        [role.name for role in settings.roles],
    )

    with pytest.raises(provision.DatabaseProvisionError):
        provision._require_cluster_authority_baseline(
            _RecordingCursor(one=(False,)),
            settings,
        )


@pytest.mark.parametrize("provisioned_retry", [False, True])
def test_target_authority_baseline_is_state_specific_and_fails_closed(
    tmp_path: Path,
    provisioned_retry: bool,
):
    settings = provision.load_provision_settings(_environment(tmp_path))
    accepted = _RecordingCursor(one=(True,))

    provision._require_target_authority_baseline(
        accepted,
        settings,
        provisioned_retry=provisioned_retry,
    )

    statement, parameters = accepted.statements[0]
    assert "target.datacl IS NULL" in statement
    assert "pg_catalog.cardinality(target.datacl) = 4" in statement
    assert "pg_catalog.cardinality(target.nspacl) = 2" in statement
    assert "pg_catalog.cardinality(target.nspacl) = 3" in statement
    assert parameters is not None
    assert parameters[2] is provisioned_retry

    with pytest.raises(provision.DatabaseProvisionError):
        provision._require_target_authority_baseline(
            _RecordingCursor(one=(False,)),
            settings,
            provisioned_retry=provisioned_retry,
        )


def test_every_catalog_preflight_precedes_first_persistent_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    settings = provision.load_provision_settings(_environment(tmp_path))
    cursor = _RecordingCursor()
    events: list[str] = []

    class _Context:
        def __init__(self, value: object):
            self.value = value

        def __enter__(self) -> object:
            return self.value

        def __exit__(self, *_args: object) -> None:
            return None

    class _Connection:
        def cursor(self) -> _Context:
            return _Context(cursor)

    monkeypatch.setattr(
        provision.psycopg,
        "connect",
        lambda *_args, **_kwargs: _Context(_Connection()),
    )
    monkeypatch.setattr(
        provision,
        "_require_admin_boundary",
        lambda *_args: events.append("admin"),
    )
    monkeypatch.setattr(
        provision,
        "_require_dedicated_cluster",
        lambda *_args: events.append("dedicated"),
    )

    def no_existing_roles(*_args: object) -> set[str]:
        events.append("existing")
        return set()

    monkeypatch.setattr(provision, "_existing_role_names", no_existing_roles)
    monkeypatch.setattr(
        provision,
        "_require_cluster_authority_baseline",
        lambda *_args: events.append("cluster_authority"),
    )
    monkeypatch.setattr(
        provision,
        "_require_target_authority_baseline",
        lambda *_args, **_kwargs: events.append("target_authority"),
    )
    monkeypatch.setattr(
        provision,
        "_require_fresh_target",
        lambda *_args, **_kwargs: events.append("fresh_target"),
    )
    monkeypatch.setattr(
        provision,
        "_require_existing_roles_safe",
        lambda *_args: events.append("existing_roles_safe"),
    )

    def stop_at_first_mutation(*_args: object) -> None:
        events.append("ensure_roles")
        raise provision.DatabaseProvisionError("database_provision_invalid")

    monkeypatch.setattr(provision, "_ensure_roles", stop_at_first_mutation)

    with pytest.raises(provision.DatabaseProvisionError):
        provision.provision_database(settings)

    assert events == [
        "admin",
        "dedicated",
        "existing",
        "cluster_authority",
        "target_authority",
        "fresh_target",
        "existing_roles_safe",
        "ensure_roles",
    ]
    assert cursor.statements == [
        (
            "SELECT pg_catalog.pg_advisory_xact_lock(%s)",
            (provision._PROVISION_LOCK_KEY,),
        )
    ]


def test_role_passwords_are_bound_parameters_not_interpolated_into_sql(
    tmp_path: Path,
):
    settings = provision.load_provision_settings(_environment(tmp_path))
    cursor = _RecordingCursor()

    provision._ensure_roles(cursor, settings, {settings.runtime.name})

    rendered = "\n".join(statement for statement, _parameters in cursor.statements)
    assert "CREATE FUNCTION pg_temp.ai_exchange_apply_role" in rendered
    assert "SELECT pg_temp.ai_exchange_apply_role(%s, %s, %s)" in rendered
    for password in PASSWORDS.values():
        assert password not in rendered
    password_parameters = [
        parameters
        for statement, parameters in cursor.statements
        if statement == "SELECT pg_temp.ai_exchange_apply_role(%s, %s, %s)"
    ]
    assert password_parameters == [
        (
            role.name,
            role.password.get_secret_value(),
            role.name == settings.runtime.name,
        )
        for role in settings.roles
    ]


def test_database_boundary_revokes_peer_access_before_target_connect_grants(
    tmp_path: Path,
):
    settings = provision.load_provision_settings(_environment(tmp_path))
    cursor = _RecordingCursor(rows=[("email_agent",), ("postgres",), ("template1",)])

    provision._apply_database_boundary(cursor, settings)

    rendered = "\n".join(statement for statement, _parameters in cursor.statements)
    assert 'ALTER DATABASE "email_agent" OWNER TO "migration_owner"' in rendered
    assert 'REVOKE CONNECT, TEMPORARY ON DATABASE "postgres" FROM PUBLIC' in rendered
    assert (
        'REVOKE CONNECT, TEMPORARY ON DATABASE "template1" FROM "runtime_user"'
        in rendered
    )
    assert 'GRANT CONNECT ON DATABASE "email_agent" TO "checkpoint_auditor"' in rendered
    assert 'ALTER SCHEMA "public" OWNER TO "migration_owner"' in rendered
    assert 'GRANT USAGE ON SCHEMA "public" TO "runtime_user"' in rendered
    assert 'GRANT USAGE ON SCHEMA "public" TO "maintenance_user"' in rendered
    assert 'GRANT USAGE ON SCHEMA "public" TO "checkpoint_auditor"' not in rendered


def test_provision_failure_is_generic_and_does_not_leak_admin_dsn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    settings = provision.load_provision_settings(_environment(tmp_path))

    def fail_connect(*_args: Any, **_kwargs: Any):
        raise RuntimeError(ADMIN_DSN)

    monkeypatch.setattr(provision.psycopg, "connect", fail_connect)

    with pytest.raises(provision.DatabaseProvisionError) as caught:
        provision.provision_database(settings)

    assert str(caught.value) == "database_provision_invalid"
    assert caught.value.__cause__ is None
    assert ADMIN_DSN not in str(caught.value)
