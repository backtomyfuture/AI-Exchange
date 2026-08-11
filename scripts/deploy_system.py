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
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.migration_settings import _read_secret_file  # noqa: E402
from src.deployment.configuration import USER_ENV_KEYS, read_env_file  # noqa: E402
from src.router.tier1.compiler import (  # noqa: E402
    CompilationFailure,
    compile_registry,
    write_artifact,
)


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


class DeploymentError(RuntimeError):
    pass


_COMPOSE_SERVICES = frozenset(
    {
        "qdrant",
        "postgres",
        "database-provision",
        "database-bootstrap",
        "ingestion-maintenance",
        "checkpoint-maintenance",
        "checkpoint-maintenance-execute",
        "ai-assistant-service",
    }
)
_COMPOSE_VOLUMES = frozenset(
    {"postgres_data", "qdrant_data", "content_data", "checkpoint_maintenance_state"}
)


def _prepare_tier1_artifact(
    root: Path,
    *,
    me_email: str,
    internal_domains: tuple[str, ...],
) -> str:
    result = compile_registry(
        root / "tier1_rules",
        internal_email_domains=internal_domains,
        me_email=me_email,
    )
    if isinstance(result, CompilationFailure):
        codes = sorted({issue.code for issue in result.errors})
        raise DeploymentError("tier1_compile_failed:" + ",".join(codes))
    write_artifact(result, root / "artifacts" / "tier1")
    return result.digest


def _run(arguments: list[str], *, environment: dict[str, str]) -> None:
    result = subprocess.run(
        arguments,
        cwd=PROJECT_ROOT,
        env=environment,
        check=False,
    )
    if result.returncode != 0:
        raise DeploymentError(f"command_failed:{arguments[-1]}")


def _docker_json(arguments: list[str], *, environment: dict[str, str]) -> Any:
    try:
        raw = subprocess.check_output(
            arguments,
            cwd=PROJECT_ROOT,
            env=environment,
            text=True,
        )
        return json.loads(raw)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
        raise DeploymentError("docker_resource_inspection_failed") from None


def _verify_project_resources(
    project_name: str,
    *,
    environment: dict[str, str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Resolve and validate only exact Compose-labelled resources."""
    try:
        container_ids = subprocess.check_output(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                f"label=com.docker.compose.project={project_name}",
            ],
            cwd=PROJECT_ROOT,
            env=environment,
            text=True,
        ).split()
        volume_names = subprocess.check_output(
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project_name}",
            ],
            cwd=PROJECT_ROOT,
            env=environment,
            text=True,
        ).split()
    except (OSError, subprocess.CalledProcessError):
        raise DeploymentError("docker_resource_inspection_failed") from None
    if not container_ids or not volume_names:
        raise DeploymentError("project_resources_not_found")
    containers = _docker_json(
        ["docker", "inspect", *container_ids],
        environment=environment,
    )
    volumes = _docker_json(
        ["docker", "volume", "inspect", *volume_names],
        environment=environment,
    )
    for item in containers:
        labels = (item.get("Config") or {}).get("Labels") or {}
        if (
            labels.get("com.docker.compose.project") != project_name
            or labels.get("com.docker.compose.service") not in _COMPOSE_SERVICES
        ):
            raise DeploymentError("container_label_boundary_violation")
    seen_volume_keys: set[str] = set()
    for item in volumes:
        labels = item.get("Labels") or {}
        volume_key = labels.get("com.docker.compose.volume")
        if (
            labels.get("com.docker.compose.project") != project_name
            or volume_key not in _COMPOSE_VOLUMES
        ):
            raise DeploymentError("volume_label_boundary_violation")
        seen_volume_keys.add(volume_key)
    if not {"postgres_data", "qdrant_data", "content_data"} <= seen_volume_keys:
        raise DeploymentError("required_data_volumes_not_found")
    return tuple(container_ids), tuple(volume_names)


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


def _deployment_context(
    project_override: str | None,
    *,
    development: bool = False,
) -> tuple[list[str], dict[str, str], str, int]:
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
    environment = dict(os.environ)
    if development:
        environment["APP_ENV"] = "development"
        environment["APP_BIND_HOST"] = "127.0.0.1"
    if all(tls_values):
        environment["EXCHANGE_CA_FILE"] = "/run/ai-exchange/exchange-ca.pem"
    environment["AI_EXCHANGE_IMAGE"] = f"ai-exchange:local-{head}"
    rules_dir = PROJECT_ROOT / "tier1_rules"
    if rules_dir.is_dir():
        environment["TIER1_ARTIFACT_DIGEST"] = _prepare_tier1_artifact(
            PROJECT_ROOT,
            me_email=user_values.get("EXCHANGE_ACCOUNT_EMAIL", ""),
            internal_domains=("tianjin-air.com", "hnair.com", "hnaaviation.com"),
        )

    compose = ["docker", "compose"]
    for env_file in env_files:
        compose.extend(("--env-file", str(env_file)))
    compose.extend(("--project-name", project_name))
    for compose_file in compose_files:
        compose.extend(("--file", str(compose_file)))
    if development:
        compose.extend(("--profile", "operations-console"))

    try:
        port = int(advanced.get("APP_PORT", "8000"))
    except ValueError:
        raise DeploymentError("app_port_invalid") from None
    if not 1 <= port <= 65535:
        raise DeploymentError("app_port_invalid")
    return compose, environment, project_name, port


def _wait_ready(port: int, timeout_seconds: int = 900) -> None:
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
    *,
    development: bool = False,
) -> tuple[list[str], dict[str, str], str, int]:
    compose, environment, resolved_project, port = _deployment_context(
        project_name,
        development=development,
    )
    _run([*compose, "config", "--quiet"], environment=environment)
    print(
        f"Deployment configuration valid: project={resolved_project} "
        f"user_keys={len(USER_ENV_KEYS)} ingress=polling-only "
        f"mode={'development' if development else 'production'}"
    )
    return compose, environment, resolved_project, port


def redeploy(
    project_name: str | None = None,
    *,
    development: bool = False,
) -> None:
    compose, environment, resolved_project, port = check(
        project_name,
        development=development,
    )
    application_services = ["ai-assistant-service"]
    compose_reconcile_options: list[str] = []
    if development:
        application_services.append("operations-console-api")
        application_services.append("operations-dashboard")
        compose_reconcile_options = ["--force-recreate", "--remove-orphans"]
    print("Building the canonical application image...")
    build_services = ["ai-assistant-service"]
    if development:
        build_services.append("operations-dashboard")
    _run(
        [*compose, "build", "--pull", *build_services],
        environment=environment,
    )
    print("Stopping the application while data services remain available...")
    _run(
        [*compose, "stop", *application_services],
        environment=environment,
    )
    print("Refreshing data services with existing volumes...")
    _run(
        [
            *compose,
            "up",
            "-d",
            "--no-build",
            *compose_reconcile_options,
            "postgres",
            "qdrant",
        ],
        environment=environment,
    )
    print("Starting the rebuilt polling application...")
    _run(
        [
            *compose,
            "up",
            "-d",
            "--no-build",
            *compose_reconcile_options,
            *application_services,
        ],
        environment=environment,
    )
    _wait_ready(port)
    _run([*compose, "ps"], environment=environment)
    print(f"Deployment ready: project={resolved_project}")


def greenfield_reset(
    project_name: str | None,
    *,
    manifest_dir: Path,
    build_id: str,
    actor: str,
) -> None:
    """Permanently erase the exact project volumes and bootstrap one empty system."""
    compose, environment, resolved_project, port = check(project_name)
    policy = manifest_dir / "POLICY.json"
    contract = manifest_dir / "CONTRACT.json"
    if not manifest_dir.is_dir() or not policy.is_file() or not contract.is_file():
        raise DeploymentError("greenfield_manifest_missing")
    account_id = read_env_file(PROJECT_ROOT / ".env")[0].get(
        "EXCHANGE_ACCOUNT_ID", ""
    )
    if not account_id.isdecimal() or int(account_id) <= 0:
        raise DeploymentError("exchange_account_id_invalid")

    print("Building and validating the canonical image before destructive cutover...")
    _run([*compose, "build", "--pull", "ai-assistant-service"], environment=environment)
    containers, volumes = _verify_project_resources(
        resolved_project,
        environment=environment,
    )
    print(
        "Verified destructive boundary: "
        f"project={resolved_project} containers={len(containers)} volumes={len(volumes)}"
    )
    _run(
        [*compose, "down", "--volumes", "--remove-orphans"],
        environment=environment,
    )
    print("Project containers and named volumes permanently removed; bootstrapping empty data.")
    _run([*compose, "up", "-d", "--no-build", "postgres", "qdrant"], environment=environment)
    _run(
        [*compose, "--profile", "database-provision", "run", "--rm", "database-provision"],
        environment=environment,
    )
    _run(
        [*compose, "--profile", "migration", "run", "--rm", "database-bootstrap"],
        environment=environment,
    )
    manifest_mount = f"{manifest_dir.resolve()}:/run/manifest:ro"
    initialize = [
        *compose,
        "--profile",
        "ingestion-maintenance",
        "run",
        "--rm",
        "--volume",
        manifest_mount,
        "ingestion-maintenance",
        "initialize",
        "--account-id",
        account_id,
        "--policy-file",
        "/run/manifest/POLICY.json",
        "--contract-file",
        "/run/manifest/CONTRACT.json",
        "--actor",
        actor,
        "--reason",
        "authorized-greenfield-reset",
        "--idempotency-key",
        build_id,
    ]
    _run([*initialize, "--dry-run"], environment=environment)
    _run(initialize, environment=environment)
    _run(
        [*compose, "up", "-d", "--no-build", "ai-assistant-service"],
        environment=environment,
    )
    _wait_ready(port)
    _run([*compose, "ps"], environment=environment)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("check", "redeploy", "greenfield-reset"))
    parser.add_argument("--project-name")
    parser.add_argument(
        "--development",
        action="store_true",
        help="使用 docker-compose.dev.yml；仅适用于本机或受控开发环境。",
    )
    parser.add_argument("--manifest-dir", type=Path)
    parser.add_argument("--build-id")
    parser.add_argument("--actor")
    arguments = parser.parse_args()
    try:
        if arguments.command == "check":
            check(arguments.project_name, development=arguments.development)
        elif arguments.command == "redeploy":
            redeploy(arguments.project_name, development=arguments.development)
        else:
            if arguments.development:
                raise DeploymentError("greenfield_reset_production_only")
            if not arguments.manifest_dir or not arguments.build_id or not arguments.actor:
                raise DeploymentError("greenfield_reset_arguments_missing")
            greenfield_reset(
                arguments.project_name,
                manifest_dir=arguments.manifest_dir,
                build_id=arguments.build_id,
                actor=arguments.actor,
            )
    except DeploymentError as exc:
        print(f"Deployment failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
