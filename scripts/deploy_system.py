#!/usr/bin/env python3
# ruff: noqa: E402
"""Validate, rebuild, and redeploy AI Exchange without exposing internal config."""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.migration_settings import _read_secret_file  # noqa: E402
from src.deployment.configuration import USER_ENV_KEYS, read_env_file  # noqa: E402


_REQUIRED_SECRETS = (
    "postgres_admin_password",
    "database_provision_admin_url",
    "postgres_migration_password",
    "postgres_runtime_password",
    "postgres_maintenance_password",
    "postgres_checkpoint_auditor_password",
    "migration_database_url",
    "checkpoint_auditor_database_url",
    "checkpoint_maintenance_database_url",
    "checkpoint_maintenance_receipt_ed25519_public_key",
    "ingestion_maintenance_database_url",
    "metrics_token",
    "content_store_key",
)
_TLS_KEYS = (
    "EXCHANGE_TLS_HOSTNAME",
    "EXCHANGE_TLS_IP",
    "EXCHANGE_CA_FILE_HOST",
)
_WEBHOOK_TLS_FILES = (
    "webhook_tls_fullchain.pem",
    "webhook_tls_key.pem",
)


class DeploymentError(RuntimeError):
    pass


def _run(arguments: list[str], *, environment: dict[str, str]) -> None:
    result = subprocess.run(
        arguments,
        cwd=PROJECT_ROOT,
        env=environment,
        check=False,
    )
    if result.returncode != 0:
        raise DeploymentError(f"command_failed:{arguments[-1]}")


def _private_file(path: Path) -> str:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode not in {0o400, 0o600}:
            raise DeploymentError(f"private_file_mode_invalid:{path.name}")
        return _read_secret_file(str(path))
    except DeploymentError:
        raise
    except Exception:
        raise DeploymentError(f"private_file_invalid:{path.name}") from None


def _private_material_file(path: Path) -> None:
    """Validate private multiline material without parsing or exposing it."""

    try:
        details = path.lstat()
        if not stat.S_ISREG(details.st_mode):
            raise DeploymentError(f"private_file_invalid:{path.name}")
        if stat.S_IMODE(details.st_mode) not in {0o400, 0o600}:
            raise DeploymentError(f"private_file_mode_invalid:{path.name}")
        if not 1 <= details.st_size <= 64 * 1024:
            raise DeploymentError(f"private_file_invalid:{path.name}")
        with path.open("rb") as stream:
            if not stream.read(1):
                raise DeploymentError(f"private_file_invalid:{path.name}")
    except DeploymentError:
        raise
    except Exception:
        raise DeploymentError(f"private_file_invalid:{path.name}") from None


def _deployment_context(
    project_override: str | None,
) -> tuple[list[str], dict[str, str], str, int, bool]:
    user_values, _ = read_env_file(PROJECT_ROOT / ".env")
    actual_keys = set(user_values)
    expected_keys = set(USER_ENV_KEYS)
    if actual_keys != expected_keys:
        raise DeploymentError("user_env_contract_invalid")
    if any(not user_values[key].strip() for key in USER_ENV_KEYS):
        raise DeploymentError("user_env_value_missing")

    secrets_dir = PROJECT_ROOT / "secrets"
    for filename in _REQUIRED_SECRETS:
        _private_file(secrets_dir / filename)

    advanced: dict[str, str] = {}
    advanced_path = secrets_dir / "deployment.env"
    env_files: list[Path] = []
    if advanced_path.exists():
        advanced, _ = read_env_file(advanced_path)
        if stat.S_IMODE(advanced_path.stat().st_mode) not in {0o400, 0o600}:
            raise DeploymentError("advanced_env_mode_invalid")
        env_files.append(advanced_path)
    env_files.append(PROJECT_ROOT / ".env")

    tls_values = tuple(bool(advanced.get(key, "")) for key in _TLS_KEYS)
    if any(tls_values) and not all(tls_values):
        raise DeploymentError("exchange_tls_overlay_incomplete")
    compose_files = [PROJECT_ROOT / "docker-compose.yml"]
    if all(tls_values):
        ca_path = Path(advanced["EXCHANGE_CA_FILE_HOST"])
        if not ca_path.is_absolute():
            ca_path = PROJECT_ROOT / ca_path
        if not ca_path.is_file():
            raise DeploymentError("exchange_ca_file_missing")
        compose_files.append(PROJECT_ROOT / "docker-compose.exchange-tls.yml")

    webhook_tls_paths = tuple(
        secrets_dir / filename for filename in _WEBHOOK_TLS_FILES
    )
    webhook_tls_presence = tuple(os.path.lexists(path) for path in webhook_tls_paths)
    if any(webhook_tls_presence) and not all(webhook_tls_presence):
        raise DeploymentError("webhook_tls_material_incomplete")
    webhook_tls_enabled = all(webhook_tls_presence)
    if webhook_tls_enabled:
        for path in webhook_tls_paths:
            _private_material_file(path)
        compose_files.append(PROJECT_ROOT / "docker-compose.webhook-tls.yml")

    project_name = project_override or _private_file(
        secrets_dir / "compose_project_name"
    )
    if not project_name or any(
        character not in "abcdefghijklmnopqrstuvwxyz0123456789_-"
        for character in project_name
    ):
        raise DeploymentError("compose_project_name_invalid")

    try:
        head = subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        raise DeploymentError("git_revision_unavailable") from None
    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    environment = dict(os.environ)
    environment["AI_EXCHANGE_IMAGE"] = f"ai-exchange:local-{timestamp}-{head}"

    compose = ["docker", "compose"]
    for env_file in env_files:
        compose.extend(("--env-file", str(env_file)))
    compose.extend(("--project-name", project_name))
    for compose_file in compose_files:
        compose.extend(("--file", str(compose_file)))

    try:
        port = int(advanced.get("APP_PORT", "8000"))
    except ValueError:
        raise DeploymentError("app_port_invalid") from None
    if not 1 <= port <= 65535:
        raise DeploymentError("app_port_invalid")
    return compose, environment, project_name, port, webhook_tls_enabled


def _wait_ready(port: int, timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    url = f"http://127.0.0.1:{port}/ready"
    last_status = "unreachable"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                payload = json.load(response)
            last_status = str(payload.get("status", "unknown"))
            if (
                payload.get("status") == "ready"
                and payload.get("processing") == "active"
            ):
                return
        except (OSError, ValueError, urllib.error.URLError):
            last_status = "unreachable"
        time.sleep(2)
    raise DeploymentError(f"readiness_timeout:{last_status}")


def check(
    project_name: str | None = None,
) -> tuple[list[str], dict[str, str], str, int, bool]:
    compose, environment, resolved_project, port, webhook_tls_enabled = (
        _deployment_context(project_name)
    )
    _run([*compose, "config", "--quiet"], environment=environment)
    print(
        f"Deployment configuration valid: project={resolved_project} "
        f"user_keys={len(USER_ENV_KEYS)} webhook_tls={webhook_tls_enabled}"
    )
    return compose, environment, resolved_project, port, webhook_tls_enabled


def redeploy(project_name: str | None = None) -> None:
    compose, environment, resolved_project, port, webhook_tls_enabled = check(
        project_name
    )
    application_services = ["ai-assistant-service"]
    if webhook_tls_enabled:
        application_services.append("webhook-tls-ingress")
    print("Building the canonical application image...")
    _run(
        [*compose, "build", "--pull", "ai-assistant-service"],
        environment=environment,
    )
    print("Stopping the application while data services remain available...")
    _run(
        [*compose, "stop", *application_services],
        environment=environment,
    )
    print("Refreshing data services with existing volumes...")
    _run(
        [*compose, "up", "-d", "--no-build", "postgres", "qdrant"],
        environment=environment,
    )
    print("Starting the rebuilt application and optional TLS ingress...")
    _run(
        [
            *compose,
            "up",
            "-d",
            "--no-build",
            *application_services,
        ],
        environment=environment,
    )
    _wait_ready(port)
    _run([*compose, "ps"], environment=environment)
    print(f"Deployment ready: project={resolved_project}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("check", "redeploy"))
    parser.add_argument("--project-name")
    arguments = parser.parse_args()
    try:
        if arguments.command == "check":
            check(arguments.project_name)
        else:
            redeploy(arguments.project_name)
    except DeploymentError as exc:
        print(f"Deployment failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
