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


def test_user_environment_contract_is_exactly_the_recommended_sixteen_keys():
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
    assert first.user_key_count == 16
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


def test_explicit_greenfield_project_name_replaces_a_previous_local_project(
    tmp_path: Path,
) -> None:
    _private(
        tmp_path / ".env",
        "".join(f"{key}={value}\n" for key, value in _minimal_user_values().items()),
    )

    configured = configure_deployment(
        tmp_path,
        project_name="ai-exchange-greenfield-20260808",
    )
    repeated = configure_deployment(tmp_path)

    assert configured.project_name == "ai-exchange-greenfield-20260808"
    assert repeated.project_name == "ai-exchange-greenfield-20260808"
    assert (tmp_path / "secrets" / "compose_project_name").read_text(
        encoding="utf-8"
    ) == "ai-exchange-greenfield-20260808"


def test_polling_settings_default_to_an_explicitly_disabled_bounded_schedule():
    assert Settings.model_fields["POLLING_ENABLED"].default is False
    assert Settings.model_fields["POLLING_INTERVAL_SECONDS"].default == 60
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


def test_deployment_context_uses_only_the_polling_compose_stack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _isolated_deployment_script(tmp_path, monkeypatch)
    compose, _environment, project_name, port = deploy_system._deployment_context(
        "test-project"
    )

    assert project_name == "test-project"
    assert port == 8000
    assert all("webhook" not in item for item in compose)
    assert str(tmp_path / "docker-compose.dev.yml") not in compose


def test_deployment_context_adds_the_development_overlay_only_on_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _isolated_deployment_script(tmp_path, monkeypatch)

    compose, _environment, project_name, port = deploy_system._deployment_context(
        "test-project",
        development=True,
    )

    assert project_name == "test-project"
    assert port == 8000
    assert str(tmp_path / "docker-compose.dev.yml") in compose
