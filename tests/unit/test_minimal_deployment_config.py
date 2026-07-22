from __future__ import annotations

import base64
import stat
from pathlib import Path

import pytest

from scripts import deploy_system
from src.config import Settings, resolve_secret
from src.deployment.configuration import (
    USER_ENV_KEYS,
    configure_deployment,
    read_env_file,
)


def _private(path: Path, value: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


def _minimal_user_values() -> dict[str, str]:
    return {
        "EXTERNAL_URL": "https://assistant.example.test",
        "EXCHANGE_API_URL": "https://exchange.example.test/api/v1/exchange/emails",
        "EXCHANGE_API_KEY": "exchange-api-key-value",
        "EXCHANGE_ACCOUNT_ID": "8",
        "EXCHANGE_ACCOUNT_EMAIL": "owner@example.test",
        "EXCHANGE_WEBHOOK_SECRET": "exchange-webhook-value",
        "LARK_APP_ID": "lark-app-id",
        "LARK_APP_SECRET": "lark-app-secret-value",
        "LARK_ENCRYPT_KEY": "lark-encrypt-key-value",
        "LARK_CHAT_ID": "lark-chat-id",
        "LARK_ALLOWED_OPEN_IDS": "lark-open-id",
        "OPENAI_API_KEY": "model-api-key-value",
        "OPENAI_API_BASE": "https://models.example.test/v1",
        "LLM_MODEL": "model-name",
        "EMBEDDING_API_KEY": "embedding-api-key-value",
        "EMBEDDING_BASE_URL": "https://embeddings.example.test/v1",
        "EMBEDDING_MODEL": "embedding-model",
    }


def _isolated_deployment_script(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _private(tmp_path / ".env", "")
    monkeypatch.setattr(deploy_system, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(deploy_system, "USER_ENV_KEYS", ())
    monkeypatch.setattr(deploy_system, "_REQUIRED_SECRETS", ())
    monkeypatch.setattr(
        deploy_system.subprocess,
        "check_output",
        lambda *args, **kwargs: "abc123def456",
    )


def test_user_environment_contract_is_exactly_the_recommended_seventeen_keys():
    assert USER_ENV_KEYS == tuple(_minimal_user_values())


def test_settings_load_generated_runtime_secrets_from_private_files(tmp_path: Path):
    content_key = base64.b64encode(b"k" * 32).decode("ascii")
    settings = Settings(
        _env_file=None,
        POSTGRES_PASSWORD_FILE=str(
            _private(tmp_path / "postgres-password", "database-secret-value")
        ),
        METRICS_TOKEN_FILE=str(
            _private(tmp_path / "metrics-token", "metrics-secret-value")
        ),
        CONTENT_STORE_KEY_FILE=str(
            _private(tmp_path / "content-key", content_key)
        ),
    )

    assert resolve_secret(settings.POSTGRES_PASSWORD) == "database-secret-value"
    assert resolve_secret(settings.METRICS_TOKEN) == "metrics-secret-value"
    assert resolve_secret(settings.CONTENT_STORE_KEY) == content_key


def test_legacy_environment_migrates_without_losing_advanced_behavior(
    tmp_path: Path,
):
    values = _minimal_user_values()
    values.update(
        {
            "POSTGRES_ADMIN_USER": "ai_exchange_admin",
            "POSTGRES_ADMIN_PASSWORD": "admin-database-secret-value",
            "POSTGRES_RUNTIME_USER": "ai_exchange_runtime",
            "POSTGRES_RUNTIME_PASSWORD": "runtime-database-secret-value",
            "POSTGRES_DB": "email_agent",
            "POSTGRES_SCHEMA": "public",
            "POSTGRES_MIGRATION_OWNER_ROLE": "ai_exchange_migration_owner",
            "POSTGRES_MAINTENANCE_ROLE": "ai_exchange_checkpoint_maintenance",
            "POSTGRES_CHECKPOINT_AUDITOR_ROLE": "ai_exchange_checkpoint_auditor",
            "METRICS_TOKEN": "metrics-token-secret-value",
            "CONTENT_STORE_KEY": base64.b64encode(b"c" * 32).decode("ascii"),
            "EXCHANGE_SSL_VERIFY": "true",
            "EXCHANGE_TLS_HOSTNAME": "exchange.example.test",
            "EXCHANGE_TLS_IP": "192.0.2.20",
            "EXCHANGE_CA_FILE_HOST": "/private/exchange-ca.pem",
            "EXCHANGE_FOLDERS_ARCHIVE": "Archive",
            "LARK_DRIVE_FOLDER_TOKEN": "drive-folder-token",
        }
    )
    env_payload = "# project=existing-project build_id=test\n" + "".join(
        f"{key}={value}\n" for key, value in values.items()
    )
    _private(tmp_path / ".env", env_payload)

    first = configure_deployment(tmp_path)
    second = configure_deployment(tmp_path)

    migrated, _ = read_env_file(tmp_path / ".env")
    assert tuple(migrated) == USER_ENV_KEYS
    assert migrated == _minimal_user_values()
    assert first.user_key_count == 17
    assert second.generated_secret_count == 0
    assert first.project_name == "existing-project"
    assert stat.S_IMODE((tmp_path / ".env").stat().st_mode) == 0o600

    advanced, _ = read_env_file(tmp_path / "secrets" / "deployment.env")
    assert advanced == {
        "LARK_DRIVE_FOLDER_TOKEN": "drive-folder-token",
        "EXCHANGE_FOLDERS_ARCHIVE": "Archive",
        "EXCHANGE_TLS_HOSTNAME": "exchange.example.test",
        "EXCHANGE_TLS_IP": "192.0.2.20",
        "EXCHANGE_CA_FILE_HOST": "/private/exchange-ca.pem",
    }
    assert stat.S_IMODE(
        (tmp_path / "secrets" / "deployment.env").stat().st_mode
    ) == 0o600
    assert (
        tmp_path / "secrets" / "postgres_admin_password"
    ).read_text(encoding="utf-8") == "admin-database-secret-value"
    assert (
        tmp_path / "secrets" / "postgres_runtime_password"
    ).read_text(encoding="utf-8") == "runtime-database-secret-value"
    assert (
        tmp_path / "secrets" / "metrics_token"
    ).read_text(encoding="utf-8") == "metrics-token-secret-value"


def test_polling_setting_is_not_part_of_runtime_configuration():
    assert "POLLING_INTERVAL" not in Settings.model_fields
    assert Settings.model_config["env_file"] == ".env"


def test_python_dotenv_disabled_skips_local_env_file(tmp_path: Path, monkeypatch):
    _private(
        tmp_path / ".env",
        "EXCHANGE_ACCOUNT_EMAIL=dotenv-account@example.test\n",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("EXCHANGE_ACCOUNT_EMAIL", raising=False)
    monkeypatch.setenv("PYTHON_DOTENV_DISABLED", "1")

    assert Settings().EXCHANGE_ACCOUNT_EMAIL == ""


def test_deployment_automatically_adds_complete_webhook_tls_overlay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _isolated_deployment_script(tmp_path, monkeypatch)
    _private(
        tmp_path / "secrets" / "webhook_tls_fullchain.pem",
        "-----BEGIN CERTIFICATE-----\ncertificate\n-----END CERTIFICATE-----\n",
    )
    _private(
        tmp_path / "secrets" / "webhook_tls_key.pem",
        "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----\n",
    )

    compose, _, _, _, webhook_tls_enabled = deploy_system._deployment_context(
        "test-project"
    )

    assert webhook_tls_enabled is True
    assert str(tmp_path / "docker-compose.webhook-tls.yml") in compose


def test_deployment_rejects_partial_webhook_tls_material(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _isolated_deployment_script(tmp_path, monkeypatch)
    _private(
        tmp_path / "secrets" / "webhook_tls_fullchain.pem",
        "certificate",
    )

    with pytest.raises(
        deploy_system.DeploymentError,
        match="webhook_tls_material_incomplete",
    ):
        deploy_system._deployment_context("test-project")


def test_deployment_rejects_symlinked_webhook_tls_material(tmp_path: Path):
    target = _private(tmp_path / "private-key", "key")
    link = tmp_path / "webhook_tls_key.pem"
    link.symlink_to(target)

    with pytest.raises(
        deploy_system.DeploymentError,
        match="private_file_invalid:webhook_tls_key.pem",
    ):
        deploy_system._private_material_file(link)
