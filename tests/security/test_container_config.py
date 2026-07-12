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
    ("service_name", "field_name"),
    [
        ("postgres", "POSTGRES_USER"),
        ("postgres", "POSTGRES_PASSWORD"),
        ("postgres", "POSTGRES_DB"),
        ("ai-assistant-service", "POSTGRES_USER"),
        ("ai-assistant-service", "POSTGRES_PASSWORD"),
        ("ai-assistant-service", "POSTGRES_DB"),
        ("ai-assistant-service", "EXTERNAL_URL"),
    ],
)
def test_production_boundary_values_are_required_from_environment(
    service_name: str,
    field_name: str,
):
    compose = _load_yaml(PRODUCTION_COMPOSE)
    environment = compose["services"][service_name]["environment"]

    assert isinstance(environment, dict)
    configured = environment[field_name]
    assert isinstance(configured, str)
    assert f"${{{field_name}:?" in configured


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
