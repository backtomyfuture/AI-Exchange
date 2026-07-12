"""Static RED tests for production and development deployment boundaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_COMPOSE = PROJECT_ROOT / "docker-compose.yml"
DEVELOPMENT_COMPOSE = PROJECT_ROOT / "docker-compose.dev.yml"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
RUNTIME_ENV_EXAMPLE = PROJECT_ROOT / ".env.runtime.example"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"
GITIGNORE = PROJECT_ROOT / ".gitignore"


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


def test_production_does_not_expose_host_gateway_to_application():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["ai-assistant-service"]

    assert "extra_hosts" not in service


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
    assert postgres_environment["POSTGRES_PASSWORD"] != runtime_environment["POSTGRES_PASSWORD"]


def test_production_application_uses_an_explicit_runtime_allowlist_only():
    compose = _load_yaml(PRODUCTION_COMPOSE)
    service = compose["services"]["ai-assistant-service"]

    assert "env_file" not in service
    assert "secrets" not in service
    forbidden_fragments = ("MIGRATION_DATABASE", "POSTGRES_ADMIN", "MIGRATION_PASSWORD")
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
    assert set(compose["secrets"]) == {"migration_database_url"}


def test_build_and_vcs_ignore_all_environment_and_local_secret_files():
    dockerignore = DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
    gitignore = GITIGNORE.read_text(encoding="utf-8").splitlines()

    assert ".env*" in dockerignore
    assert "secrets/" in dockerignore
    assert ".env*" in gitignore
    assert "secrets/" in gitignore
    assert "!.env.example" in gitignore
    assert "!.env.runtime.example" in gitignore


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
        "POSTGRES_PASSWORD",
        "EXCHANGE_CA_FILE",
        "METRICS_TOKEN",
        "LARK_ALLOWED_OPEN_IDS",
    }

    assert required <= values.keys()
    assert values["APP_ENV"] == "development"


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
