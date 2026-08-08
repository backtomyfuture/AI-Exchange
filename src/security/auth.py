"""Fail-closed authentication and production configuration validation."""

from __future__ import annotations

import base64
import binascii
import hmac
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import HTTPException, Request, status

from src.config import get_settings, resolve_secret


_PLACEHOLDER_EXACT = frozenset(
    {
        "",
        "...",
        "example",
        "password",
        "postgres",
        "root",
        "secret",
        "test",
        "user",
        "change-me",
        "changeme",
        "replace-me",
    }
)
_PLACEHOLDER_MARKER = re.compile(
    r"(?:^|[._-])(?:dummy|example|placeholder|sample|your)(?:$|[._-])"
)
_POSTGRES_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z")
_RESERVED_HOST_SUFFIXES = (".example", ".invalid", ".localhost", ".test")
_PLACEHOLDER_PREFIXES = (
    "change_",
    "change-",
    "example_",
    "example-",
    "replace_",
    "replace-",
    "test_",
    "test-",
    "your_",
    "your-",
)
_MIN_BOUNDARY_SECRET_LENGTH = 16
_POLLING_INSTANCE_ID = "ai-exchange-web"
_BOUNDARY_SECRET_FIELDS = frozenset(
    {
        "POSTGRES_PASSWORD",
        "EXCHANGE_API_KEY",
        "LARK_APP_SECRET",
        "LARK_ENCRYPT_KEY",
        "METRICS_TOKEN",
    }
)
_FORBIDDEN_PRODUCTION_RUNTIME_ENVIRONMENT = frozenset(
    {
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
    }
)


def _setting(settings: Any, name: str, default: Any = "") -> Any:
    return getattr(settings, name, default)


def _plain(settings: Any, name: str) -> str:
    return resolve_secret(_setting(settings, name, "")).strip()


def _is_placeholder(value: Any) -> bool:
    normalized = resolve_secret(value).strip().casefold()
    return (
        normalized in _PLACEHOLDER_EXACT
        or normalized.startswith(_PLACEHOLDER_PREFIXES)
        or normalized.endswith("...")
        or bool(_PLACEHOLDER_MARKER.search(normalized))
    )


def _is_https_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    hostname = (parsed.hostname or "").rstrip(".").casefold()
    hostname_labels = hostname.split(".") if hostname else []
    is_placeholder_host = (
        not hostname
        or "example" in hostname_labels
        or hostname == "localhost"
        or hostname.endswith(_RESERVED_HOST_SUFFIXES)
    )
    return (
        parsed.scheme.casefold() == "https"
        and bool(parsed.netloc)
        and not parsed.username
        and not parsed.password
        and not is_placeholder_host
    )


def _content_key_is_valid(value: str) -> bool:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return False
    return len(decoded) == 32


def _configured_lark_operators(settings: Any) -> tuple[str, ...]:
    raw = _setting(settings, "LARK_ALLOWED_OPEN_IDS", "")
    if not isinstance(raw, str):
        return ()
    values = tuple(
        dict.fromkeys(item.strip() for item in raw.split(",") if item.strip())
    )
    if "*" in values:
        return ()
    return values


def is_lark_operator_allowed(open_id: object, settings: Any | None = None) -> bool:
    """Return whether an exact, case-sensitive Lark ``open_id`` is allowlisted."""

    if not isinstance(open_id, str) or not open_id or open_id != open_id.strip():
        return False
    configured = _configured_lark_operators(settings or get_settings())
    if not configured:
        return False

    allowed = False
    for candidate in configured:
        allowed = hmac.compare_digest(open_id, candidate) or allowed
    return allowed


def validate_runtime_security(settings: Any | None = None) -> Any:
    """Reject unsafe production settings without including their values."""

    settings = settings or get_settings()
    if (
        str(_setting(settings, "APP_ENV", "development")).strip().casefold()
        != "production"
    ):
        return settings

    invalid: set[str] = set()

    for field_name in _FORBIDDEN_PRODUCTION_RUNTIME_ENVIRONMENT:
        if field_name in os.environ:
            invalid.add(field_name)

    placeholder_fields = (
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "EXCHANGE_API_KEY",
        "LARK_APP_ID",
        "LARK_APP_SECRET",
        "LARK_ENCRYPT_KEY",
        "LARK_CHAT_ID",
        "METRICS_TOKEN",
    )
    for field_name in placeholder_fields:
        value = _setting(settings, field_name, "")
        if _is_placeholder(value) or (
            field_name in _BOUNDARY_SECRET_FIELDS
            and len(resolve_secret(value).strip()) < _MIN_BOUNDARY_SECRET_LENGTH
        ):
            invalid.add(field_name)

    if _setting(settings, "DATABASE_ROLE_SEPARATION_REQUIRED", False) is not True:
        invalid.add("DATABASE_ROLE_SEPARATION_REQUIRED")
    if _setting(settings, "DURABLE_INBOX_ENABLED", False) is not True:
        invalid.add("DURABLE_INBOX_ENABLED")
    if _setting(settings, "POLLING_ENABLED", False) is not True:
        invalid.add("POLLING_ENABLED")
    if _plain(settings, "INGESTION_INSTANCE_ID") != _POLLING_INSTANCE_ID:
        invalid.add("INGESTION_INSTANCE_ID")

    target_schema = _plain(settings, "POSTGRES_SCHEMA")
    if not _POSTGRES_IDENTIFIER.fullmatch(target_schema):
        invalid.add("POSTGRES_SCHEMA")

    migration_role = _plain(settings, "POSTGRES_MIGRATION_OWNER_ROLE")
    maintenance_role = _plain(settings, "POSTGRES_MAINTENANCE_ROLE")
    runtime_role = _plain(settings, "POSTGRES_USER")
    if not _POSTGRES_IDENTIFIER.fullmatch(runtime_role):
        invalid.add("POSTGRES_USER")
    if not _POSTGRES_IDENTIFIER.fullmatch(migration_role):
        invalid.add("POSTGRES_MIGRATION_OWNER_ROLE")
    if not _POSTGRES_IDENTIFIER.fullmatch(maintenance_role):
        invalid.add("POSTGRES_MAINTENANCE_ROLE")
    if len({runtime_role, migration_role, maintenance_role}) != 3:
        invalid.update(
            {
                "POSTGRES_USER",
                "POSTGRES_MIGRATION_OWNER_ROLE",
                "POSTGRES_MAINTENANCE_ROLE",
            }
        )

    exchange_url = _plain(settings, "EXCHANGE_API_URL")
    if _is_placeholder(exchange_url) or not _is_https_url(exchange_url):
        invalid.add("EXCHANGE_API_URL")

    external_url = _plain(settings, "EXTERNAL_URL")
    if _is_placeholder(external_url) or not _is_https_url(external_url):
        invalid.add("EXTERNAL_URL")

    if _setting(settings, "DEBUG", False) is not False:
        invalid.add("DEBUG")
    if _setting(settings, "EXCHANGE_SSL_VERIFY", True) is not True:
        invalid.add("EXCHANGE_SSL_VERIFY")
    configured_operators = _configured_lark_operators(settings)
    if not configured_operators or any(
        _is_placeholder(operator) for operator in configured_operators
    ):
        invalid.add("LARK_ALLOWED_OPEN_IDS")

    content_key = _plain(settings, "CONTENT_STORE_KEY")
    if _is_placeholder(content_key) or not _content_key_is_valid(content_key):
        invalid.add("CONTENT_STORE_KEY")

    key_version = _plain(settings, "CONTENT_STORE_KEY_VERSION")
    if _is_placeholder(key_version):
        invalid.add("CONTENT_STORE_KEY_VERSION")

    ca_file = _plain(settings, "EXCHANGE_CA_FILE")
    if ca_file:
        path = Path(ca_file)
        if not path.is_file() or not os.access(path, os.R_OK):
            invalid.add("EXCHANGE_CA_FILE")

    if invalid:
        fields = ", ".join(sorted(invalid))
        raise RuntimeError(f"unsafe production settings: {fields}")

    return settings


def require_metrics_auth(request: Request, settings: Any | None = None) -> None:
    """Require a configured Bearer token for the metrics endpoint."""

    settings = settings or get_settings()
    expected = _plain(settings, "METRICS_TOKEN")
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metrics authentication is not configured",
        )

    authorization_values = request.headers.getlist("Authorization")
    if len(authorization_values) != 1:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid metrics credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    authorization = authorization_values[0]
    scheme, separator, credential = authorization.partition(" ")
    valid_format = (
        separator == " "
        and scheme.casefold() == "bearer"
        and bool(credential)
        and credential == credential.strip()
        and " " not in credential
    )
    if not valid_format or not hmac.compare_digest(credential, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid metrics credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
