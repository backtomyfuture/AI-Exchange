"""RED tests for the minimum production runtime-security boundary."""

from __future__ import annotations

import base64
import importlib
import os
import subprocess
import sys
from typing import Any, Callable
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import SecretStr

from src.config import Settings, resolve_secret


_SAFE_SECRET_VALUES = {
    "POSTGRES_PASSWORD": "Db9!F4m2Q7w8Z1x6",
    "EXCHANGE_API_KEY": "Exch8Q2w7V4m9N6k3P1z",
    "EXCHANGE_WEBHOOK_SECRET": "Hook7M4x9K2v8Q6p3N1w",
    "LARK_APP_SECRET": "Lark6V9m2Q4x8N1k7P3w",
    "LARK_ENCRYPT_KEY": "Encrypt4N8v2K7m9Q1x6P3w",
    "CONTENT_STORE_KEY": base64.b64encode(b"k" * 32).decode("ascii"),
    "METRICS_TOKEN": "Metrics9Q2w7V4m8N1k6P3x",
    "OPENAI_API_KEY": "Model8N2v7K4m9Q1x6P3w",
    "EMBEDDING_API_KEY": "Embed7Q4m9V2x8N1k6P3w",
}


def _load_runtime_validator() -> Callable[[Settings], None]:
    """Load the planned interface at test execution time.

    Importing here keeps the rest of the security tests collectable while the
    production module is intentionally absent during the RED checkpoint.
    """

    module = importlib.import_module("src.security.auth")
    return module.validate_runtime_security


def _secure_production_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "APP_ENV": "production",
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": 5432,
        "POSTGRES_DB": "email_agent",
        "POSTGRES_USER": "email_agent_runtime",
        "POSTGRES_PASSWORD": SecretStr(_SAFE_SECRET_VALUES["POSTGRES_PASSWORD"]),
        "POSTGRES_SCHEMA": "public",
        "POSTGRES_MIGRATION_OWNER_ROLE": "ai_exchange_migration_owner",
        "DATABASE_ROLE_SEPARATION_REQUIRED": True,
        "DURABLE_INBOX_ENABLED": True,
        "INGESTION_SHADOW_ENABLED": False,
        "SYNC_RECONCILIATION_ENABLED": False,
        "INGESTION_INSTANCE_ID": "ai-exchange-web",
        "EXCHANGE_API_URL": "https://exchange.internal.company/api/v1/exchange/emails",
        "EXCHANGE_API_KEY": SecretStr(_SAFE_SECRET_VALUES["EXCHANGE_API_KEY"]),
        "EXCHANGE_ACCOUNT_ID": 8,
        "EXCHANGE_SSL_VERIFY": True,
        "EXCHANGE_CA_FILE": "",
        "EXCHANGE_WEBHOOK_SECRET": SecretStr(
            _SAFE_SECRET_VALUES["EXCHANGE_WEBHOOK_SECRET"]
        ),
        "LARK_APP_ID": "cli_runtime_app",
        "LARK_APP_SECRET": SecretStr(_SAFE_SECRET_VALUES["LARK_APP_SECRET"]),
        "LARK_ENCRYPT_KEY": SecretStr(_SAFE_SECRET_VALUES["LARK_ENCRYPT_KEY"]),
        "LARK_CHAT_ID": "oc_runtime_chat",
        "LARK_ALLOWED_OPEN_IDS": "ou_runtime_operator",
        "CONTENT_STORE_KEY": SecretStr(_SAFE_SECRET_VALUES["CONTENT_STORE_KEY"]),
        "METRICS_TOKEN": SecretStr(_SAFE_SECRET_VALUES["METRICS_TOKEN"]),
        "EXTERNAL_URL": "https://ai-exchange.internal.company",
        "OPENAI_API_KEY": SecretStr(_SAFE_SECRET_VALUES["OPENAI_API_KEY"]),
        "EMBEDDING_API_KEY": SecretStr(_SAFE_SECRET_VALUES["EMBEDDING_API_KEY"]),
        "DEBUG": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_exchange_tls_verification_defaults_to_true(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("EXCHANGE_SSL_VERIFY", raising=False)

    settings = Settings(_env_file=None)

    assert settings.EXCHANGE_SSL_VERIFY is True


def test_phase_2_ingestion_flags_default_to_disabled(
    monkeypatch: pytest.MonkeyPatch,
):
    for field_name in (
        "DURABLE_INBOX_ENABLED",
        "INGESTION_SHADOW_ENABLED",
        "SYNC_RECONCILIATION_ENABLED",
    ):
        monkeypatch.delenv(field_name, raising=False)

    settings = Settings(_env_file=None)

    assert settings.DURABLE_INBOX_ENABLED is False
    assert settings.INGESTION_SHADOW_ENABLED is False
    assert settings.SYNC_RECONCILIATION_ENABLED is False


def test_secure_production_baseline_is_accepted():
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings()

    result = validate_runtime_security(settings)

    assert result is settings


@pytest.mark.parametrize(
    ("field_name", "unsafe_value"),
    [
        ("DURABLE_INBOX_ENABLED", False),
        ("INGESTION_SHADOW_ENABLED", True),
        ("SYNC_RECONCILIATION_ENABLED", True),
    ],
)
def test_production_accepts_only_the_phase4_lite_runtime_mode(
    field_name: str,
    unsafe_value: bool,
) -> None:
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(**{field_name: unsafe_value})

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    assert field_name in str(caught.value)


@pytest.mark.parametrize("instance_id", ["", "ai-exchange-blue", "worker-2"])
def test_production_rejects_non_singleton_ingestion_identity(
    instance_id: str,
) -> None:
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(INGESTION_INSTANCE_ID=instance_id)

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    assert "INGESTION_INSTANCE_ID" in str(caught.value)
    if instance_id:
        assert instance_id not in str(caught.value)


@pytest.mark.parametrize(
    ("field_name", "unsafe_value"),
    [
        ("DATABASE_ROLE_SEPARATION_REQUIRED", False),
        ("POSTGRES_SCHEMA", "Public"),
        ("POSTGRES_SCHEMA", "public;drop_schema"),
        ("POSTGRES_USER", "Runtime-User"),
        ("POSTGRES_MIGRATION_OWNER_ROLE", "Migration-Owner"),
        ("POSTGRES_MIGRATION_OWNER_ROLE", "email_agent_runtime"),
    ],
)
def test_production_rejects_unsafe_database_role_boundary(
    field_name: str,
    unsafe_value: object,
):
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(**{field_name: unsafe_value})

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    message = str(caught.value)
    assert field_name in message
    assert str(unsafe_value) not in message


@pytest.mark.parametrize(
    ("field_name", "unsafe_value"),
    [
        ("POSTGRES_USER", "user"),
        ("POSTGRES_PASSWORD", SecretStr("password")),
        ("EXCHANGE_API_KEY", SecretStr("your_api_key")),
        ("EXCHANGE_WEBHOOK_SECRET", SecretStr("")),
        ("LARK_APP_SECRET", SecretStr("y5...")),
        ("LARK_ENCRYPT_KEY", SecretStr("your_encrypt_key")),
        ("CONTENT_STORE_KEY", SecretStr("")),
        ("METRICS_TOKEN", SecretStr("")),
        ("LARK_ALLOWED_OPEN_IDS", ""),
        ("EXCHANGE_SSL_VERIFY", False),
    ],
)
def test_production_rejects_each_unsafe_security_field_without_leaking_value(
    field_name: str,
    unsafe_value: Any,
):
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(**{field_name: unsafe_value})

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    message = str(caught.value)
    assert field_name in message
    raw_value = resolve_secret(unsafe_value)
    if raw_value:
        assert raw_value not in message


def test_production_rejects_insecure_exchange_url_without_leaking_url():
    validate_runtime_security = _load_runtime_validator()
    insecure_url = "http://exchange.internal.example/api/v1/exchange/emails"
    settings = _secure_production_settings(EXCHANGE_API_URL=insecure_url)

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    message = str(caught.value)
    assert "EXCHANGE_API_URL" in message
    assert insecure_url not in message


@pytest.mark.parametrize(
    ("field_name", "unsafe_value"),
    [
        ("POSTGRES_USER", "postgres"),
        ("EXCHANGE_API_URL", "https://example.com/api/v1/exchange/emails"),
        ("EXCHANGE_API_URL", "https://exchange.example.com/api"),
        ("EXTERNAL_URL", "https://example.invalid"),
        ("LARK_APP_ID", "cli_example"),
        ("LARK_CHAT_ID", "oc_example"),
        ("LARK_APP_ID", "cli_your_app_id"),
        ("LARK_CHAT_ID", "oc_your_chat_id"),
        ("LARK_ALLOWED_OPEN_IDS", "ou_your_open_id"),
        (
            "LARK_ALLOWED_OPEN_IDS",
            "ou_runtime_operator,ou_your_open_id",
        ),
    ],
)
def test_production_rejects_structured_placeholder_values(
    field_name: str,
    unsafe_value: str,
):
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(**{field_name: unsafe_value})

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    assert field_name in str(caught.value)
    assert unsafe_value not in str(caught.value)


@pytest.mark.parametrize(
    "field_name",
    [
        "POSTGRES_PASSWORD",
        "EXCHANGE_API_KEY",
        "EXCHANGE_WEBHOOK_SECRET",
        "LARK_APP_SECRET",
        "LARK_ENCRYPT_KEY",
        "METRICS_TOKEN",
    ],
)
def test_production_rejects_weak_boundary_secrets(field_name: str):
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(**{field_name: SecretStr("short-secret")})

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    assert field_name in str(caught.value)
    assert "short-secret" not in str(caught.value)


def test_production_rejects_missing_exchange_ca_bundle_without_leaking_path(tmp_path):
    validate_runtime_security = _load_runtime_validator()
    missing = tmp_path / "private-ca-path-sentinel.pem"
    settings = _secure_production_settings(EXCHANGE_CA_FILE=str(missing))

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    assert "EXCHANGE_CA_FILE" in str(caught.value)
    assert str(missing) not in str(caught.value)


@pytest.mark.parametrize(
    ("field_name", "unsafe_value"),
    [
        ("DEBUG", True),
        ("LARK_APP_ID", ""),
        ("LARK_CHAT_ID", ""),
        ("CONTENT_STORE_KEY_VERSION", ""),
        ("EXTERNAL_URL", "http://ai-exchange.internal.company"),
    ],
)
def test_production_rejects_additional_unsafe_runtime_fields(
    field_name: str,
    unsafe_value: object,
):
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(**{field_name: unsafe_value})

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    assert field_name in str(caught.value)


def test_production_validation_reports_all_invalid_field_names_at_once():
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(
        POSTGRES_PASSWORD=SecretStr("password"),
        EXCHANGE_WEBHOOK_SECRET=SecretStr(""),
        EXCHANGE_SSL_VERIFY=False,
        LARK_ALLOWED_OPEN_IDS="",
    )

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    message = str(caught.value)
    assert "POSTGRES_PASSWORD" in message
    assert "EXCHANGE_WEBHOOK_SECRET" in message
    assert "EXCHANGE_SSL_VERIFY" in message
    assert "LARK_ALLOWED_OPEN_IDS" in message
    assert "password" not in message


def test_validation_error_never_contains_other_configured_secrets():
    validate_runtime_security = _load_runtime_validator()
    settings = _secure_production_settings(EXCHANGE_SSL_VERIFY=False)

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(settings)

    message = str(caught.value)
    for secret in _SAFE_SECRET_VALUES.values():
        assert secret not in message


def test_development_mode_does_not_apply_production_only_rejections():
    validate_runtime_security = _load_runtime_validator()
    settings = Settings(
        _env_file=None,
        APP_ENV="development",
        POSTGRES_USER="user",
        POSTGRES_PASSWORD=SecretStr("password"),
        EXCHANGE_SSL_VERIFY=False,
        EXCHANGE_WEBHOOK_SECRET=SecretStr(""),
        CONTENT_STORE_KEY=SecretStr(""),
        LARK_ALLOWED_OPEN_IDS="",
        METRICS_TOKEN=SecretStr(""),
    )

    result = validate_runtime_security(settings)

    assert result is settings


@pytest.mark.parametrize(
    "field_name",
    [
        "MIGRATION_DATABASE_URL",
        "MIGRATION_DATABASE_URL_FILE",
        "INGESTION_MAINTENANCE_DATABASE_URL_FILE",
        "CHECKPOINT_MAINTENANCE_DATABASE_URL",
        "CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE",
        "CHECKPOINT_AUDITOR_DATABASE_URL",
        "CHECKPOINT_AUDITOR_DATABASE_URL_FILE",
        "CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE",
        "CHECKPOINT_MAINTENANCE_RECEIPT_HMAC_KEY_FILE",
        "CHECKPOINT_MAINTENANCE_RECEIPT_HMAC_KEY_B64",
        "CHECKPOINT_CLEANUP_RECEIPT_HMAC_KEY_B64",
        "POSTGRES_ADMIN_USER",
        "POSTGRES_ADMIN_PASSWORD",
        "POSTGRES_MIGRATION_USER",
        "POSTGRES_MIGRATION_PASSWORD",
        "POSTGRES_MAINTENANCE_PASSWORD",
        "POSTGRES_CHECKPOINT_AUDITOR_PASSWORD",
    ],
)
def test_production_rejects_admin_or_migration_credentials_in_runtime_process(
    monkeypatch: pytest.MonkeyPatch,
    field_name: str,
):
    validate_runtime_security = _load_runtime_validator()
    secret = f"private-{field_name.casefold()}-sentinel"
    monkeypatch.setenv(field_name, secret)

    with pytest.raises(RuntimeError) as caught:
        validate_runtime_security(_secure_production_settings())

    message = str(caught.value)
    assert field_name in message
    assert secret not in message


def test_development_does_not_require_production_credential_plane_guard(
    monkeypatch: pytest.MonkeyPatch,
):
    validate_runtime_security = _load_runtime_validator()
    monkeypatch.setenv("MIGRATION_DATABASE_URL_FILE", "/private/migration-sentinel")
    settings = Settings(_env_file=None, APP_ENV="development")

    assert validate_runtime_security(settings) is settings


def test_direct_server_app_rejects_unsafe_production_lifespan():
    env = os.environ.copy()
    env["APP_ENV"] = "production"
    env["EXCHANGE_SSL_VERIFY"] = "false"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import asyncio\n"
                "from src.server import app\n"
                "async def probe():\n"
                "    async with app.router.lifespan_context(app):\n"
                "        pass\n"
                "asyncio.run(probe())"
            ),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode != 0
    assert "EXCHANGE_SSL_VERIFY" in result.stderr


def test_security_credentials_are_secretstr_fields():
    settings = _secure_production_settings()
    secret_fields = (
        "POSTGRES_PASSWORD",
        "EXCHANGE_API_KEY",
        "EXCHANGE_WEBHOOK_SECRET",
        "LARK_APP_SECRET",
        "LARK_ENCRYPT_KEY",
        "CONTENT_STORE_KEY",
        "METRICS_TOKEN",
        "OPENAI_API_KEY",
        "EMBEDDING_API_KEY",
    )

    for field_name in secret_fields:
        assert isinstance(getattr(settings, field_name), SecretStr), field_name

    rendered = repr(settings)
    for secret in _SAFE_SECRET_VALUES.values():
        assert secret not in rendered


@pytest.mark.asyncio
async def test_lifespan_validates_runtime_before_database_or_context():
    from src import server as server_module

    settings = _secure_production_settings(EXCHANGE_SSL_VERIFY=False)
    runtime_database_check = AsyncMock(
        side_effect=AssertionError("database_reached_before_security_validation")
    )
    with (
        patch.object(server_module, "get_settings", return_value=settings),
        patch.object(
            server_module,
            "require_runtime_database_boundary",
            new=runtime_database_check,
        ) as database_check,
        patch.object(
            server_module,
            "get_runtime_app_context",
            side_effect=AssertionError("context_reached_before_security_validation"),
        ),
    ):
        with pytest.raises(RuntimeError, match="EXCHANGE_SSL_VERIFY"):
            async with server_module.application_lifespan(server_module.app):
                pass

    database_check.assert_not_awaited()


def test_run_server_validates_runtime_before_uvicorn_bind():
    from src import main as main_module

    settings = _secure_production_settings(POSTGRES_PASSWORD=SecretStr("password"))
    with (
        patch.object(main_module, "get_settings", return_value=settings),
        patch.object(
            main_module.uvicorn,
            "run",
            side_effect=AssertionError("uvicorn_bound_before_security_validation"),
        ) as uvicorn_run,
        pytest.raises(RuntimeError, match="POSTGRES_PASSWORD"),
    ):
        main_module.run_server()

    uvicorn_run.assert_not_called()


def test_run_server_disables_unsanitized_uvicorn_access_log():
    from src import main as main_module

    settings = Settings(_env_file=None, APP_ENV="development")
    with (
        patch.object(main_module, "get_settings", return_value=settings),
        patch.object(
            main_module,
            "setup_logging",
        ),
        patch.object(main_module.uvicorn, "run") as uvicorn_run,
    ):
        main_module.run_server()

    uvicorn_run.assert_called_once_with(
        main_module.app,
        host="0.0.0.0",
        port=8000,
        access_log=False,
    )
