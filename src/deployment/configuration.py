"""Generate internal deployment state while keeping the user `.env` minimal."""

from __future__ import annotations

import base64
import os
import re
import secrets
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlencode

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from psycopg.conninfo import conninfo_to_dict

from src.db.migration_settings import _read_secret_file


USER_ENV_KEYS = (
    "EXTERNAL_URL",
    "EXCHANGE_API_URL",
    "EXCHANGE_API_KEY",
    "EXCHANGE_ACCOUNT_ID",
    "EXCHANGE_ACCOUNT_EMAIL",
    "LARK_APP_ID",
    "LARK_APP_SECRET",
    "LARK_ENCRYPT_KEY",
    "LARK_CHAT_ID",
    "LARK_ALLOWED_OPEN_IDS",
    "OPENAI_API_KEY",
    "OPENAI_API_BASE",
    "LLM_MODEL",
    "EMBEDDING_API_KEY",
    "EMBEDDING_BASE_URL",
    "EMBEDDING_MODEL",
)

_USER_ENV_SECTIONS = (
    ("Service", USER_ENV_KEYS[0:1]),
    ("Exchange", USER_ENV_KEYS[1:5]),
    ("Lark", USER_ENV_KEYS[5:10]),
    ("LLM", USER_ENV_KEYS[10:13]),
    ("Embedding", USER_ENV_KEYS[13:16]),
)

_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")
_PROJECT_NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}\Z")
_PROJECT_COMMENT = re.compile(r"(?:^|\s)project=([a-z0-9][a-z0-9_-]{0,62})(?:\s|$)")

_DATABASE_NAME = "email_agent"
_DATABASE_SCHEMA = "public"
_ADMIN_ROLE = "ai_exchange_admin"
_MIGRATION_ROLE = "ai_exchange_migration_owner"
_RUNTIME_ROLE = "ai_exchange_runtime"
_MAINTENANCE_ROLE = "ai_exchange_checkpoint_maintenance"
_AUDITOR_ROLE = "ai_exchange_checkpoint_auditor"

_ROLE_FILES = {
    _MIGRATION_ROLE: "postgres_migration_password",
    _RUNTIME_ROLE: "postgres_runtime_password",
    _MAINTENANCE_ROLE: "postgres_maintenance_password",
    _AUDITOR_ROLE: "postgres_checkpoint_auditor_password",
}

_LEGACY_ROLE_FIELDS = {
    "POSTGRES_ADMIN_USER": _ADMIN_ROLE,
    "POSTGRES_RUNTIME_USER": _RUNTIME_ROLE,
    "POSTGRES_MIGRATION_OWNER_ROLE": _MIGRATION_ROLE,
    "POSTGRES_MAINTENANCE_ROLE": _MAINTENANCE_ROLE,
    "POSTGRES_CHECKPOINT_AUDITOR_ROLE": _AUDITOR_ROLE,
    "POSTGRES_DB": _DATABASE_NAME,
    "POSTGRES_SCHEMA": _DATABASE_SCHEMA,
}

_ADVANCED_DEFAULTS = {
    "APP_PORT": "8000",
    "LOG_LEVEL": "INFO",
    "LARK_DRIVE_FOLDER_TOKEN": "",
    "EXCHANGE_FOLDERS_FULL": "收件箱",
    "EXCHANGE_FOLDERS_ARCHIVE": "",
    "EXCHANGE_FOLDER_SENTITEMS": "Sent Items",
    "EXCHANGE_FOLDER_DRAFTS": "Drafts",
    "LEADER_SENDERS": "",
    "EXCHANGE_TLS_HOSTNAME": "",
    "EXCHANGE_TLS_IP": "",
    "EXCHANGE_CA_FILE_HOST": "",
}


class DeploymentConfigurationError(RuntimeError):
    """Safe error raised when local deployment state cannot be migrated."""


@dataclass(frozen=True, slots=True)
class ConfigurationResult:
    user_key_count: int
    generated_secret_count: int
    advanced_key_count: int
    project_name: str


def _reject(code: str) -> DeploymentConfigurationError:
    return DeploymentConfigurationError(code)


def read_env_file(path: Path) -> tuple[dict[str, str], str]:
    """Read a strict dotenv file without expanding or logging values."""

    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise _reject("deployment_env_unreadable") from None

    values: dict[str, str] = {}
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in raw_line:
            raise _reject("deployment_env_invalid")
        key, value = raw_line.split("=", 1)
        key = key.strip()
        if _IDENTIFIER.fullmatch(key.casefold()) is None or key != key.upper():
            raise _reject("deployment_env_invalid")
        if key in values or "\x00" in value or "\r" in value or "\n" in value:
            raise _reject("deployment_env_invalid")
        values[key] = value
    return values, raw


def _write_private(path: Path, content: str, *, allow_newlines: bool = False) -> None:
    if "\x00" in content or "\r" in content or (
        not allow_newlines and "\n" in content
    ):
        raise _reject("deployment_secret_invalid")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=True) as stream:
            fd = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except Exception:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)
        raise


def _existing_private(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return _read_secret_file(str(path))
    except Exception:
        raise _reject("deployment_secret_invalid") from None


def _ensure_private(path: Path, desired: str | None = None) -> tuple[str, bool]:
    existing = _existing_private(path)
    if existing is not None:
        if desired and not secrets.compare_digest(existing, desired):
            raise _reject("deployment_secret_conflict")
        return existing, False
    value = desired or secrets.token_urlsafe(48)
    if not value:
        raise _reject("deployment_secret_invalid")
    _write_private(path, value)
    return value, True


def _postgres_url(user: str, password: str, *, options: str | None = None) -> str:
    authority = f"{quote(user, safe='')}:{quote(password, safe='')}@postgres:5432"
    query = f"?{urlencode({'options': options})}" if options else ""
    return f"postgresql://{authority}/{_DATABASE_NAME}{query}"


def _validate_dsn(
    value: str,
    *,
    user: str,
    password: str,
    options: str | None,
) -> None:
    try:
        parsed = conninfo_to_dict(value)
    except Exception:
        raise _reject("deployment_database_secret_invalid") from None
    if (
        parsed.get("host") != "postgres"
        or parsed.get("port") != "5432"
        or parsed.get("dbname") != _DATABASE_NAME
        or parsed.get("user") != user
        or parsed.get("password") != password
        or parsed.get("options") != options
    ):
        raise _reject("deployment_database_secret_invalid")


def _ensure_dsn(
    path: Path,
    *,
    user: str,
    password: str,
    options: str | None,
) -> bool:
    desired = _postgres_url(user, password, options=options)
    value = _existing_private(path)
    created = value is None
    if value is None:
        _write_private(path, desired)
        value = desired
    _validate_dsn(value, user=user, password=password, options=options)
    return created


def _ensure_receipt_keypair(secrets_dir: Path) -> int:
    public_path = secrets_dir / "checkpoint_maintenance_receipt_ed25519_public_key"
    if _existing_private(public_path) is not None:
        return 0

    private_key = Ed25519PrivateKey.generate()
    private_seed = private_key.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    _write_private(
        secrets_dir / "checkpoint_maintenance_receipt_ed25519_private_key",
        base64.b64encode(private_seed).decode("ascii"),
    )
    _write_private(public_path, base64.b64encode(public_key).decode("ascii"))
    return 2


def _render_user_env(values: dict[str, str]) -> str:
    lines = [
        "# AI Exchange user configuration",
        "# Keep only these 16 integration and model settings.",
        "# Internal credentials and runtime controls are generated automatically.",
        "",
    ]
    for section, keys in _USER_ENV_SECTIONS:
        lines.append(f"# {section}")
        lines.extend(f"{key}={values.get(key, '')}" for key in keys)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _advanced_values(legacy: dict[str, str], existing: dict[str, str]) -> dict[str, str]:
    advanced = dict(existing)
    unknown = set(advanced) - set(_ADVANCED_DEFAULTS)
    if unknown:
        raise _reject("deployment_advanced_env_invalid")
    for key, default in _ADVANCED_DEFAULTS.items():
        value = legacy.get(key)
        if value is not None and value != default:
            advanced[key] = value
    return {key: advanced[key] for key in _ADVANCED_DEFAULTS if advanced.get(key)}


def _render_env(values: dict[str, str]) -> str:
    return "".join(f"{key}={value}\n" for key, value in values.items())


def configure_deployment(
    root: Path,
    *,
    project_name: str | None = None,
) -> ConfigurationResult:
    """Migrate a legacy `.env` and create all non-user-managed deployment state."""

    root = root.resolve()
    env_path = root / ".env"
    legacy, raw_env = read_env_file(env_path)
    missing = set(USER_ENV_KEYS) - set(legacy)
    if missing:
        raise _reject("deployment_user_env_missing_keys")

    for field, expected in _LEGACY_ROLE_FIELDS.items():
        configured = legacy.get(field)
        if configured and configured != expected:
            raise _reject("deployment_database_identity_incompatible")
    if legacy.get("EXCHANGE_SSL_VERIFY", "true").casefold() not in {"true", "1"}:
        raise _reject("deployment_tls_verification_required")

    secrets_dir = root / "secrets"
    secrets_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(secrets_dir, 0o700)
    generated = 0

    admin_dsn_path = secrets_dir / "database_provision_admin_url"
    admin_dsn = _existing_private(admin_dsn_path)
    admin_password = legacy.get("POSTGRES_ADMIN_PASSWORD", "")
    if not admin_password and admin_dsn:
        try:
            admin_password = conninfo_to_dict(admin_dsn).get("password", "")
        except Exception:
            raise _reject("deployment_database_secret_invalid") from None
    admin_password, created = _ensure_private(
        secrets_dir / "postgres_admin_password",
        admin_password or None,
    )
    generated += int(created)

    role_passwords: dict[str, str] = {}
    for role, filename in _ROLE_FILES.items():
        desired = None
        if role == _RUNTIME_ROLE:
            desired = legacy.get("POSTGRES_RUNTIME_PASSWORD") or legacy.get(
                "POSTGRES_PASSWORD"
            )
        role_passwords[role], created = _ensure_private(
            secrets_dir / filename,
            desired or None,
        )
        generated += int(created)

    generated += int(
        _ensure_dsn(
            admin_dsn_path,
            user=_ADMIN_ROLE,
            password=admin_password,
            options=None,
        )
    )
    generated += int(
        _ensure_dsn(
            secrets_dir / "migration_database_url",
            user=_MIGRATION_ROLE,
            password=role_passwords[_MIGRATION_ROLE],
            options=f"-csearch_path={_DATABASE_SCHEMA}",
        )
    )
    for filename, role in (
        ("checkpoint_auditor_database_url", _AUDITOR_ROLE),
        ("checkpoint_maintenance_database_url", _MAINTENANCE_ROLE),
        ("ingestion_maintenance_database_url", _MAINTENANCE_ROLE),
    ):
        generated += int(
            _ensure_dsn(
                secrets_dir / filename,
                user=role,
                password=role_passwords[role],
                options=f"-csearch_path=pg_catalog,{_DATABASE_SCHEMA}",
            )
        )

    metrics_token, created = _ensure_private(
        secrets_dir / "metrics_token",
        legacy.get("METRICS_TOKEN") or None,
    )
    generated += int(created)
    if len(metrics_token) < 16:
        raise _reject("deployment_metrics_secret_invalid")

    content_key_path = secrets_dir / "content_store_key"
    content_key = legacy.get("CONTENT_STORE_KEY") or _existing_private(
        content_key_path
    )
    if content_key is None:
        content_key = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
    resolved_content_key, created = _ensure_private(
        content_key_path,
        content_key,
    )
    generated += int(created)
    try:
        if len(base64.b64decode(resolved_content_key, validate=True)) != 32:
            raise ValueError
    except ValueError:
        raise _reject("deployment_content_secret_invalid") from None

    generated += _ensure_receipt_keypair(secrets_dir)

    advanced_path = secrets_dir / "deployment.env"
    existing_advanced: dict[str, str] = {}
    if advanced_path.exists():
        existing_advanced, _ = read_env_file(advanced_path)
    advanced = _advanced_values(legacy, existing_advanced)
    if advanced:
        _write_private(
            advanced_path,
            _render_env(advanced).rstrip("\n"),
            allow_newlines=True,
        )
    else:
        advanced_path.unlink(missing_ok=True)

    project_path = secrets_dir / "compose_project_name"
    existing_project_name = _existing_private(project_path)
    if project_name is not None:
        if _PROJECT_NAME.fullmatch(project_name) is None:
            raise _reject("deployment_project_name_invalid")
        if existing_project_name != project_name:
            _write_private(project_path, project_name)
    elif existing_project_name is None:
        match = _PROJECT_COMMENT.search(raw_env)
        project_name = match.group(1) if match else "ai-exchange-greenfield"
        if _PROJECT_NAME.fullmatch(project_name) is None:
            raise _reject("deployment_project_name_invalid")
        _write_private(project_path, project_name)
        generated += 1
    elif _PROJECT_NAME.fullmatch(existing_project_name) is None:
        raise _reject("deployment_project_name_invalid")
    else:
        project_name = existing_project_name

    user_values = {key: legacy.get(key, "") for key in USER_ENV_KEYS}
    _write_private(
        env_path,
        _render_user_env(user_values).rstrip("\n"),
        allow_newlines=True,
    )

    return ConfigurationResult(
        user_key_count=len(USER_ENV_KEYS),
        generated_secret_count=generated,
        advanced_key_count=len(advanced),
        project_name=project_name,
    )
