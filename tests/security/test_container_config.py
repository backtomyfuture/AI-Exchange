"""Static RED tests for production and development deployment boundaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_COMPOSE = PROJECT_ROOT / "docker-compose.yml"
EXCHANGE_TLS_COMPOSE = PROJECT_ROOT / "docker-compose.exchange-tls.yml"
DEVELOPMENT_COMPOSE = PROJECT_ROOT / "docker-compose.dev.yml"
DOCKERFILE = PROJECT_ROOT / "Dockerfile"
BOOTSTRAP_REQUIREMENTS = PROJECT_ROOT / "requirements.bootstrap.txt"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
RUNTIME_ENV_EXAMPLE = PROJECT_ROOT / ".env.runtime.example"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"
GITIGNORE = PROJECT_ROOT / ".gitignore"
POSTGRES_PEER_ACL_INIT = (
    PROJECT_ROOT / "docker" / "postgres" / "010-peer-database-acl.sql"
)
PINNED_PYTHON_IMAGE = (
    "python:3.12-slim@"
    "sha256:423ed6ab25b1921a477529254bfeeabf5855151dc2c3141699a1bfc852199fbf"
)
PINNED_POSTGRES_IMAGE = (
    "postgres:15@"
    "sha256:f30e3de0ac9cc938dac627ef2231099867c694b5f949fadb924c8c977428c399"
)
PINNED_QDRANT_IMAGE = (
    "qdrant/qdrant:v1.17.0@"
    "sha256:f1c7272cdac52b38c1a0e89313922d940ba50afd90d593a1605dbbc214e66ffb"
)
PINNED_UV_WHEEL_HASHES = {
    "041e4b80bebc58d7142ac9394370cacd73185fd8d066d6675d14707d83408f6d",
    "49fe42df9f42056037473f3876adec1615709b57d3470ed39178ff420f3afb9f",
}
PINNED_DEBIAN_SNAPSHOT = "https://snapshot.debian.org/archive/debian/20260623T000000Z"
PINNED_DEBIAN_SECURITY_SNAPSHOT = (
    "https://snapshot.debian.org/archive/debian-security/20260623T000000Z"
)


class _ComposeLoader(yaml.SafeLoader):
    pass


def _construct_override(loader: yaml.SafeLoader, node: yaml.Node):
    return loader.construct_sequence(node)


_ComposeLoader.add_constructor("!override", _construct_override)


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.load(path.read_text(encoding="utf-8"), Loader=_ComposeLoader)
    assert isinstance(payload, dict), f"{path.name} must contain a YAML mapping"
    return payload


def _ports(service: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(port) for port in service.get("ports", ()))


def _network_names(service: dict[str, Any]) -> set[str]:
    networks = service.get("networks", ())
    if isinstance(networks, dict):
        return set(networks)
    return {str(network) for network in networks}


def _read_env_example() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def test_production_application_base_image_is_digest_pinned():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert dockerfile.splitlines()[0] == f"FROM {PINNED_PYTHON_IMAGE}"


def test_production_data_service_images_are_digest_pinned():
    compose = _load_yaml(PRODUCTION_COMPOSE)

    assert compose["services"]["postgres"]["image"] == PINNED_POSTGRES_IMAGE
    assert compose["services"]["qdrant"]["image"] == PINNED_QDRANT_IMAGE


def test_production_container_inputs_do_not_use_latest_tags():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    compose = PRODUCTION_COMPOSE.read_text(encoding="utf-8")

    assert ":latest" not in dockerfile
    assert ":latest" not in compose


def test_all_application_one_shots_share_one_explicit_release_image():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    expected = "${AI_EXCHANGE_IMAGE:?AI_EXCHANGE_IMAGE is required}"

    for service_name in (
        "database-provision",
        "database-bootstrap",
        "ingestion-maintenance",
        "checkpoint-maintenance",
        "checkpoint-maintenance-execute",
        "ai-assistant-service",
    ):
        service = compose["services"][service_name]
        assert service["build"] == "."
        assert service["image"] == expected


def test_docker_bootstrap_does_not_upgrade_unlocked_build_tools():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "--upgrade" not in dockerfile
    assert "pip setuptools wheel Cython" not in dockerfile


def test_uv_bootstrap_requirement_is_hash_locked_for_supported_linux_architectures():
    assert BOOTSTRAP_REQUIREMENTS.is_file()
    requirements = BOOTSTRAP_REQUIREMENTS.read_text(encoding="utf-8")
    hashes = {
        token.removeprefix("--hash=sha256:")
        for token in requirements.replace("\\", " ").split()
        if token.startswith("--hash=sha256:")
    }

    assert "uv==0.11.28" in requirements
    assert hashes == PINNED_UV_WHEEL_HASHES


def test_docker_installs_uv_only_from_hash_locked_bootstrap():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "COPY pyproject.toml uv.lock requirements.bootstrap.txt ./" in dockerfile
    assert "--only-binary=:all: --require-hashes --no-deps" in dockerfile
    assert "-r requirements.bootstrap.txt" in dockerfile
    assert "uv==0.11.28" not in dockerfile


def test_docker_installs_runtime_dependencies_from_hashed_wheels_only():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert dockerfile.count("--only-binary=:all:") == 2
    assert (
        "pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple "
        "--only-binary=:all: --require-hashes -r /tmp/requirements.lock"
    ) in " ".join(dockerfile.split())


def test_docker_resolves_debian_packages_from_the_pinned_snapshot_only():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert PINNED_DEBIAN_SNAPSHOT in dockerfile
    assert PINNED_DEBIAN_SECURITY_SNAPSHOT in dockerfile
    assert "Check-Valid-Until: no" in dockerfile
    assert "Acquire::Check-Valid-Until" not in dockerfile
    assert dockerfile.index(PINNED_DEBIAN_SNAPSHOT) < dockerfile.index("apt-get update")
    assert dockerfile.index(PINNED_DEBIAN_SECURITY_SNAPSHOT) < dockerfile.index(
        "apt-get update"
    )


@pytest.mark.parametrize("service_name", ["postgres", "qdrant"])
def test_production_data_services_do_not_publish_host_ports(service_name: str):
    compose = _load_yaml(PRODUCTION_COMPOSE)

    service = compose["services"][service_name]

    assert "ports" not in service


@pytest.mark.parametrize(
    ("service_name", "field_name", "source_name"),
    [
        ("postgres", "POSTGRES_USER", "POSTGRES_ADMIN_USER"),
        ("postgres", "POSTGRES_PASSWORD", "POSTGRES_ADMIN_PASSWORD"),
        ("postgres", "POSTGRES_DB", "POSTGRES_DB"),
        ("ai-assistant-service", "POSTGRES_USER", "POSTGRES_RUNTIME_USER"),
        (
            "ai-assistant-service",
            "POSTGRES_PASSWORD",
            "POSTGRES_RUNTIME_PASSWORD",
        ),
        ("ai-assistant-service", "POSTGRES_DB", "POSTGRES_DB"),
        ("ai-assistant-service", "EXTERNAL_URL", "EXTERNAL_URL"),
    ],
)
def test_production_boundary_values_are_required_from_environment(
    service_name: str,
    field_name: str,
    source_name: str,
):
    compose = _load_yaml(PRODUCTION_COMPOSE)
    environment = compose["services"][service_name]["environment"]

    assert isinstance(environment, dict)
    configured = environment[field_name]
    assert isinstance(configured, str)
    assert f"${{{source_name}:?" in configured


def test_production_backend_network_is_internal_and_contains_only_data_services():
    compose = _load_yaml(PRODUCTION_COMPOSE)

    assert compose["networks"]["backend"]["internal"] is True
    assert _network_names(compose["services"]["postgres"]) == {"backend"}
    assert _network_names(compose["services"]["qdrant"]) == {"backend"}
    assert "backend" in _network_names(compose["services"]["ai-assistant-service"])
    assert "edge" in _network_names(compose["services"]["ai-assistant-service"])
    assert compose["networks"].get("edge") is not None


def test_production_does_not_publish_legacy_port_15000():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    ports = _ports(compose["services"]["ai-assistant-service"])

    assert all("15000" not in port for port in ports)


def test_production_application_publishes_one_webhook_port():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    ports = _ports(compose["services"]["ai-assistant-service"])

    assert ports == ("${APP_PORT:-8000}:8000",)


def test_production_shutdown_budget_covers_bounded_ingestion_drain():
    compose = _load_yaml(PRODUCTION_COMPOSE)

    assert compose["services"]["ai-assistant-service"]["stop_grace_period"] == "150s"


def test_production_healthchecks_use_session_aware_readiness():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service_check = compose["services"]["ai-assistant-service"]["healthcheck"]["test"]
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert service_check == ["CMD", "curl", "-f", "http://localhost:8000/ready"]
    assert "CMD curl -f http://localhost:8000/ready || exit 1" in dockerfile
    assert "CMD curl -f http://localhost:8000/health || exit 1" not in dockerfile


def test_production_does_not_expose_host_gateway_to_application():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["ai-assistant-service"]

    assert "extra_hosts" not in service


def test_optional_exchange_tls_overlay_pins_dns_alias_and_read_only_ca():
    compose = _load_yaml(EXCHANGE_TLS_COMPOSE)
    service = compose["services"]["ai-assistant-service"]

    assert service["environment"] == {
        "EXCHANGE_CA_FILE": "/run/ai-exchange/exchange-ca.pem"
    }
    assert service["extra_hosts"] == [
        "${EXCHANGE_TLS_HOSTNAME:?EXCHANGE_TLS_HOSTNAME is required}:"
        "${EXCHANGE_TLS_IP:?EXCHANGE_TLS_IP is required}"
    ]
    assert service["volumes"] == [
        {
            "type": "bind",
            "source": "${EXCHANGE_CA_FILE_HOST:?EXCHANGE_CA_FILE_HOST is required}",
            "target": "/run/ai-exchange/exchange-ca.pem",
            "read_only": True,
        }
    ]
    assert "host-gateway" not in EXCHANGE_TLS_COMPOSE.read_text(encoding="utf-8")


def test_production_application_has_no_source_or_test_bind_mounts():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    volumes = tuple(compose["services"]["ai-assistant-service"].get("volumes", ()))

    assert all(not str(volume).startswith("./src:") for volume in volumes)
    assert all(not str(volume).startswith("./tests:") for volume in volumes)


def test_production_database_credentials_use_three_distinct_planes():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    postgres_environment = compose["services"]["postgres"]["environment"]
    runtime_environment = compose["services"]["ai-assistant-service"]["environment"]

    assert "${POSTGRES_ADMIN_USER:" in postgres_environment["POSTGRES_USER"]
    assert "${POSTGRES_ADMIN_PASSWORD:" in postgres_environment["POSTGRES_PASSWORD"]
    assert "${POSTGRES_RUNTIME_USER:" in runtime_environment["POSTGRES_USER"]
    assert "${POSTGRES_RUNTIME_PASSWORD:" in runtime_environment["POSTGRES_PASSWORD"]
    assert postgres_environment["POSTGRES_USER"] != runtime_environment["POSTGRES_USER"]
    assert (
        postgres_environment["POSTGRES_PASSWORD"]
        != runtime_environment["POSTGRES_PASSWORD"]
    )


def test_fresh_postgres_volume_revokes_public_peer_database_access():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    postgres_volumes = compose["services"]["postgres"]["volumes"]

    assert (
        "./docker/postgres/010-peer-database-acl.sql:"
        "/docker-entrypoint-initdb.d/010-peer-database-acl.sql:ro"
    ) in postgres_volumes
    init_sql = POSTGRES_PEER_ACL_INIT.read_text(encoding="utf-8")
    assert "REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC;" in init_sql
    assert "REVOKE CONNECT, TEMPORARY ON DATABASE template1 FROM PUBLIC;" in init_sql


def test_production_application_uses_an_explicit_runtime_allowlist_only():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["ai-assistant-service"]

    assert "env_file" not in service
    assert "secrets" not in service
    forbidden_fragments = (
        "MIGRATION_DATABASE",
        "POSTGRES_ADMIN",
        "MIGRATION_PASSWORD",
        "MAINTENANCE_DATABASE",
        "MAINTENANCE_RECEIPT",
    )
    for key, value in service["environment"].items():
        rendered = f"{key}={value}"
        assert not any(fragment in rendered for fragment in forbidden_fragments)


def test_database_bootstrap_is_manual_one_shot_with_only_migration_secret():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["database-bootstrap"]

    assert service["profiles"] == ["migration"]
    assert service["restart"] == "no"
    assert "ports" not in service
    assert _network_names(service) == {"backend"}
    assert service["command"] == ["python", "-m", "src.db.bootstrap"]
    assert service["user"] == "0:0"
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["cap_add"] == ["DAC_READ_SEARCH"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["secrets"] == [
        {
            "source": "migration_database_url",
            "target": "migration_database_url",
        }
    ]
    assert service["environment"]["MIGRATION_DATABASE_URL_FILE"] == (
        "/run/secrets/migration_database_url"
    )
    assert "env_file" not in service
    assert not {
        "POSTGRES_ADMIN_PASSWORD",
        "POSTGRES_RUNTIME_PASSWORD",
        "POSTGRES_PASSWORD",
    }.intersection(service["environment"])
    assert set(compose["secrets"]) == {
        "database_provision_admin_url",
        "postgres_migration_password",
        "postgres_runtime_password",
        "postgres_maintenance_password",
        "postgres_checkpoint_auditor_password",
        "migration_database_url",
        "ingestion_maintenance_database_url",
        "checkpoint_auditor_database_url",
        "checkpoint_maintenance_database_url",
        "checkpoint_maintenance_receipt_ed25519_public_key",
    }


def test_database_provision_is_isolated_greenfield_admin_one_shot():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["database-provision"]

    assert service["profiles"] == ["database-provision"]
    assert service["restart"] == "no"
    assert service["command"] == ["python", "-m", "src.db.provision"]
    assert "ports" not in service
    assert _network_names(service) == {"backend"}
    assert service["user"] == "0:0"
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["cap_add"] == ["DAC_READ_SEARCH"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["secrets"] == [
        {
            "source": "database_provision_admin_url",
            "target": "database_provision_admin_url",
        },
        {
            "source": "postgres_migration_password",
            "target": "postgres_migration_password",
        },
        {
            "source": "postgres_runtime_password",
            "target": "postgres_runtime_password",
        },
        {
            "source": "postgres_maintenance_password",
            "target": "postgres_maintenance_password",
        },
        {
            "source": "postgres_checkpoint_auditor_password",
            "target": "postgres_checkpoint_auditor_password",
        },
    ]
    assert service["environment"]["DATABASE_PROVISION_ADMIN_URL_FILE"] == (
        "/run/secrets/database_provision_admin_url"
    )
    assert service["environment"]["POSTGRES_MIGRATION_PASSWORD_FILE"] == (
        "/run/secrets/postgres_migration_password"
    )
    assert service["environment"]["POSTGRES_RUNTIME_PASSWORD_FILE"] == (
        "/run/secrets/postgres_runtime_password"
    )
    assert service["environment"]["POSTGRES_MAINTENANCE_PASSWORD_FILE"] == (
        "/run/secrets/postgres_maintenance_password"
    )
    assert service["environment"][
        "POSTGRES_CHECKPOINT_AUDITOR_PASSWORD_FILE"
    ] == "/run/secrets/postgres_checkpoint_auditor_password"
    assert "env_file" not in service
    assert not {
        "POSTGRES_ADMIN_USER",
        "POSTGRES_ADMIN_PASSWORD",
        "POSTGRES_RUNTIME_PASSWORD",
        "MIGRATION_DATABASE_URL_FILE",
    }.intersection(service["environment"])


def test_ingestion_maintenance_is_manual_restricted_one_shot():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["ingestion-maintenance"]

    assert service["profiles"] == ["ingestion-maintenance"]
    assert service["restart"] == "no"
    assert service["entrypoint"] == ["python", "scripts/manage_ingestion.py"]
    assert service["command"] == ["--help"]
    assert "ports" not in service
    assert _network_names(service) == {"backend"}
    assert service["user"] == "0:0"
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["cap_add"] == ["DAC_READ_SEARCH"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["secrets"] == [
        {
            "source": "ingestion_maintenance_database_url",
            "target": "ingestion_maintenance_database_url",
        }
    ]
    assert service["environment"]["INGESTION_MAINTENANCE_DATABASE_URL_FILE"] == (
        "/run/secrets/ingestion_maintenance_database_url"
    )
    assert "MIGRATION_DATABASE_URL_FILE" not in service["environment"]
    assert "POSTGRES_PASSWORD" not in service["environment"]


def test_checkpoint_maintenance_plan_is_manual_and_cannot_sign_receipts():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["checkpoint-maintenance"]

    assert service["profiles"] == ["checkpoint-maintenance"]
    assert service["restart"] == "no"
    assert service["entrypoint"] == ["python", "scripts/checkpoint_cleanup.py"]
    assert service["command"] == ["--help"]
    assert "ports" not in service
    assert _network_names(service) == {"backend"}
    assert service["user"] == "0:0"
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["cap_add"] == ["DAC_READ_SEARCH"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["secrets"] == [
        {
            "source": "checkpoint_auditor_database_url",
            "target": "checkpoint_auditor_database_url",
        },
    ]
    assert service["environment"]["CHECKPOINT_AUDITOR_DATABASE_URL_FILE"] == (
        "/run/secrets/checkpoint_auditor_database_url"
    )
    assert "CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE" not in service["environment"]
    assert (
        "CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE"
        not in service["environment"]
    )
    assert "MIGRATION_DATABASE_URL_FILE" not in service["environment"]
    assert "POSTGRES_PASSWORD" not in service["environment"]


def test_checkpoint_maintenance_execute_isolated_from_plan_and_can_verify_receipts():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["checkpoint-maintenance-execute"]

    assert service["profiles"] == ["checkpoint-maintenance-execute"]
    assert service["restart"] == "no"
    assert service["entrypoint"] == ["python", "scripts/checkpoint_cleanup.py"]
    assert service["command"] == ["execute", "--help"]
    assert "ports" not in service
    assert _network_names(service) == {"backend"}
    assert service["user"] == "0:0"
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["cap_add"] == ["DAC_READ_SEARCH"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["secrets"] == [
        {
            "source": "checkpoint_maintenance_database_url",
            "target": "checkpoint_maintenance_database_url",
        },
        {
            "source": "checkpoint_maintenance_receipt_ed25519_public_key",
            "target": "checkpoint_maintenance_receipt_ed25519_public_key",
        },
    ]
    assert (
        service["environment"]["CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE"]
        == "/run/secrets/checkpoint_maintenance_receipt_ed25519_public_key"
    )
    assert "MIGRATION_DATABASE_URL_FILE" not in service["environment"]
    assert "POSTGRES_PASSWORD" not in service["environment"]


def test_build_and_vcs_ignore_all_environment_and_local_secret_files():
    dockerignore = DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
    gitignore = GITIGNORE.read_text(encoding="utf-8").splitlines()

    assert ".env*" in dockerignore
    assert "secrets/" in dockerignore
    assert ".env*" in gitignore
    assert "secrets/" in gitignore
    assert "!.env.example" in gitignore
    assert "!.env.runtime.example" in gitignore


def test_build_context_excludes_generated_test_and_lint_artifacts():
    dockerignore = set(DOCKERIGNORE.read_text(encoding="utf-8").splitlines())

    assert {".coverage*", ".pytest_cache/", "htmlcov/", ".ruff_cache/"} <= dockerignore


def test_build_context_excludes_non_runtime_and_message_artifacts():
    dockerignore = set(DOCKERIGNORE.read_text(encoding="utf-8").splitlines())

    assert {
        "tests/",
        "docs/",
        ".agent/",
        ".superpowers/",
        "*.eml",
        "*.pdf",
    } <= dockerignore


def test_dockerfile_copies_only_the_explicit_runtime_allowlist():
    dockerfile_lines = set(DOCKERFILE.read_text(encoding="utf-8").splitlines())

    assert "COPY . ." not in dockerfile_lines
    assert {
        "COPY src ./src",
        "COPY scripts ./scripts",
        "COPY alembic ./alembic",
        "COPY alembic.ini ./alembic.ini",
        "COPY skills_registry ./skills_registry",
    } <= dockerfile_lines


def test_vcs_ignores_coverage_artifacts_and_keeps_bootstrap_lockfile():
    gitignore = set(GITIGNORE.read_text(encoding="utf-8").splitlines())

    assert ".coverage*" in gitignore
    assert "!requirements.bootstrap.txt" in gitignore


def test_runtime_environment_template_excludes_control_plane_credentials():
    values: dict[str, str] = {}
    for raw_line in RUNTIME_ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value

    assert {"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"} <= values.keys()
    assert not {
        "MIGRATION_DATABASE_URL",
        "MIGRATION_DATABASE_URL_FILE",
        "CHECKPOINT_MAINTENANCE_DATABASE_URL",
        "CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE",
        "CHECKPOINT_AUDITOR_DATABASE_URL",
        "CHECKPOINT_AUDITOR_DATABASE_URL_FILE",
        "CHECKPOINT_MAINTENANCE_RECEIPT_ED25519_PUBLIC_KEY_FILE",
        "CHECKPOINT_MAINTENANCE_RECEIPT_HMAC_KEY_B64",
        "CHECKPOINT_MAINTENANCE_RECEIPT_HMAC_KEY_FILE",
        "CHECKPOINT_CLEANUP_RECEIPT_HMAC_KEY_B64",
        "POSTGRES_ADMIN_USER",
        "POSTGRES_ADMIN_PASSWORD",
        "POSTGRES_MIGRATION_USER",
        "POSTGRES_MIGRATION_PASSWORD",
    }.intersection(values)


@pytest.mark.parametrize(
    ("service_name", "expected_binding"),
    [
        ("postgres", "127.0.0.1:5432:5432"),
        ("qdrant", "127.0.0.1:6333:6333"),
    ],
)
def test_development_data_ports_are_bound_to_loopback_only(
    service_name: str,
    expected_binding: str,
):
    compose = _load_yaml(DEVELOPMENT_COMPOSE)

    ports = _ports(compose["services"][service_name])

    assert ports == (expected_binding,)


def test_development_application_port_is_explicitly_loopback_only():
    compose = _load_yaml(DEVELOPMENT_COMPOSE)
    ports = _ports(compose["services"]["ai-assistant-service"])

    assert ports == ("127.0.0.1:${APP_PORT:-8000}:8000",)


def test_env_example_declares_all_minimum_security_controls():
    values = _read_env_example()
    required = {
        "APP_ENV",
        "AI_EXCHANGE_IMAGE",
        "POSTGRES_PASSWORD",
        "EXCHANGE_CA_FILE",
        "EXCHANGE_TLS_HOSTNAME",
        "EXCHANGE_TLS_IP",
        "EXCHANGE_CA_FILE_HOST",
        "METRICS_TOKEN",
        "LARK_ALLOWED_OPEN_IDS",
    }

    assert required <= values.keys()
    assert values["APP_ENV"] == "development"


def test_env_example_declares_file_backed_greenfield_provisioning_secrets():
    values = _read_env_example()

    assert {
        "DATABASE_PROVISION_ADMIN_URL_FILE",
        "POSTGRES_MIGRATION_PASSWORD_FILE",
        "POSTGRES_RUNTIME_PASSWORD_FILE",
        "POSTGRES_MAINTENANCE_PASSWORD_FILE",
        "POSTGRES_CHECKPOINT_AUDITOR_PASSWORD_FILE",
    } <= values.keys()
    assert all(
        values[name].startswith("./secrets/")
        for name in (
            "DATABASE_PROVISION_ADMIN_URL_FILE",
            "POSTGRES_MIGRATION_PASSWORD_FILE",
            "POSTGRES_RUNTIME_PASSWORD_FILE",
            "POSTGRES_MAINTENANCE_PASSWORD_FILE",
            "POSTGRES_CHECKPOINT_AUDITOR_PASSWORD_FILE",
        )
    )


def test_env_example_enables_exchange_tls_verification_by_default():
    values = _read_env_example()

    assert values["EXCHANGE_SSL_VERIFY"].casefold() == "true"


def test_env_example_does_not_ship_environment_specific_lark_identifiers():
    values = _read_env_example()

    assert not values["LARK_APP_ID"].startswith("cli_")
    assert not values["LARK_CHAT_ID"].startswith("oc_")


def test_env_example_does_not_recommend_plain_http_exchange_endpoint():
    values = _read_env_example()
    exchange_url = values["EXCHANGE_API_URL"]

    assert not exchange_url or exchange_url.startswith("https://")
