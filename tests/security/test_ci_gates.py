"""Static proof that release CI cannot silently skip the PostgreSQL contract."""

import tomllib
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLLING_WORKFLOW = PROJECT_ROOT / ".github" / "workflows" / "polling-postgres.yml"
PYPROJECT = PROJECT_ROOT / "pyproject.toml"

PINNED_POSTGRES_IMAGE = (
    "postgres:15@"
    "sha256:f30e3de0ac9cc938dac627ef2231099867c694b5f949fadb924c8c977428c399"
)
PINNED_CHECKOUT_ACTION = (
    "actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 # v4.3.1"
)
PINNED_SETUP_PYTHON_ACTION = (
    "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065 # v5.6.0"
)

FULL_SUITE_COMMAND = (
    "uv run --frozen python -m pytest -q --cov=src --cov-report=term-missing"
)
MANDATORY_COVERAGE_TARGETS = (
    "src/db/access_contract.py",
    "src/db/auditor.py",
    "src/db/checkpoint_saver.py",
    "src/db/runtime_boundary.py",
    "src/db/maintenance_fence.py",
    "src/db/maintenance_settings.py",
    "src/ingestion/email_events.py",
    "src/ingestion/recovery.py",
    "src/ingestion/runtime_capability.py",
)
COMPOSE_PLACEHOLDERS = {
    "AI_EXCHANGE_IMAGE": "ai-exchange:polling-ci",
    "EXTERNAL_URL": "https://polling-ci.invalid",
    "EXCHANGE_API_URL": ("https://exchange.polling-ci.invalid/api/v1/exchange/emails"),
    "EXCHANGE_API_KEY": "polling-ci-exchange-key",
    "EXCHANGE_ACCOUNT_ID": "8",
    "EXCHANGE_ACCOUNT_EMAIL": "polling-ci@example.invalid",
    "LARK_APP_ID": "polling-ci-lark-app",
    "LARK_APP_SECRET": "polling-ci-lark-secret",
    "LARK_ENCRYPT_KEY": "polling-ci-lark-encrypt",
    "LARK_CHAT_ID": "polling-ci-lark-chat",
    "LARK_ALLOWED_OPEN_IDS": "polling-ci-open-id",
    "OPENAI_API_KEY": "polling-ci-model-key",
    "OPENAI_API_BASE": "https://models.polling-ci.invalid/v1",
    "LLM_MODEL": "polling-ci-model",
    "EMBEDDING_API_KEY": "polling-ci-embedding-key",
    "EMBEDDING_BASE_URL": "https://embeddings.polling-ci.invalid/v1",
    "EMBEDDING_MODEL": "polling-ci-embedding-model",
    "TIER1_ARTIFACT_DIGEST": "polling-ci-tier1-digest",
}


def test_polling_ci_forces_real_postgres_role_ddl_suite() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert "image: postgres:15" in workflow
    assert "TEST_POSTGRES_ADMIN_URL:" in workflow
    assert 'TEST_POSTGRES_ROLE_DDL: "1"' in workflow
    assert "python -m pytest -q" in workflow
    assert "continue-on-error" not in workflow


def test_polling_ci_uses_digest_pinned_postgres_service_image() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert f"image: {PINNED_POSTGRES_IMAGE}" in workflow
    assert ":latest" not in workflow


def test_polling_ci_uses_commit_pinned_official_actions() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert f"uses: {PINNED_CHECKOUT_ACTION}" in workflow
    assert f"uses: {PINNED_SETUP_PYTHON_ACTION}" in workflow
    assert "@v4" not in workflow
    assert "@v5" not in workflow


def test_polling_ci_fails_before_pytest_when_mandatory_postgres_env_is_missing() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert "set -euo pipefail" in workflow
    assert "${TEST_POSTGRES_ADMIN_URL:?" in workflow
    assert "${TEST_POSTGRES_ROLE_DDL:?" in workflow
    assert 'test "$TEST_POSTGRES_ROLE_DDL" = "1"' in workflow


def test_polling_ci_uses_project_python_and_frozen_dependency_lock() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")
    normalized_workflow = " ".join(workflow.split())

    assert "runs-on: ubuntu-24.04" in workflow
    assert 'python-version: "3.12.13"' in workflow
    assert (
        "python -m pip install --no-cache-dir --index-url https://pypi.org/simple "
        "--only-binary=:all: --require-hashes --no-deps "
        "-r requirements.bootstrap.txt"
    ) in normalized_workflow
    assert 'python -m pip install "uv==0.11.28"' not in workflow
    assert "uv lock --check" in workflow
    assert "uv sync --frozen" in workflow
    assert "uv pip check --python .venv/bin/python" in workflow


def test_polling_ci_does_not_enable_an_empty_pip_cache() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert "cache: pip" not in workflow


def test_polling_ci_runs_pinned_ruff_without_mutating_the_lock() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert "uv run --frozen ruff check src tests" in workflow
    assert "uvx" not in workflow
    assert "uv add" not in workflow
    assert "pip install ruff" not in workflow


def test_ruff_is_an_exact_dev_dependency_excluded_from_production_export() -> None:
    pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert "ruff==0.15.13" in pyproject["dependency-groups"]["dev"]
    assert "uv export --frozen --no-dev --no-emit-project" in workflow


def test_test_frameworks_are_dev_dependencies_excluded_from_production() -> None:
    pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    production = set(pyproject["project"]["dependencies"])
    development = set(pyproject["dependency-groups"]["dev"])

    expected = {
        "pytest>=9.0.0",
        "pytest-asyncio>=0.25.0",
        "pytest-cov>=6.0.0",
        "pytest-mock>=3.14.0",
    }
    assert expected <= development
    assert production.isdisjoint(expected)


def test_polling_ci_proves_hashed_runtime_wheels_exist_for_linux_architectures() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")
    normalized_workflow = " ".join(workflow.split())

    assert (
        "uv export --frozen --no-dev --no-emit-project "
        "--format requirements-txt --output-file /tmp/requirements.lock"
    ) in normalized_workflow
    for architecture in ("x86_64", "aarch64"):
        assert (
            "uv pip install --dry-run --no-cache --python-version 3.12.13 "
            f"--python-platform {architecture}-unknown-linux-gnu "
            "--only-binary=:all: --require-hashes "
            f"--target /tmp/wheel-check-{architecture} "
            "-r /tmp/requirements.lock"
        ) in normalized_workflow


def test_polling_ci_validates_production_compose_with_only_placeholders() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")
    parsed_workflow = yaml.safe_load(workflow)
    compose_step = next(
        step
        for step in parsed_workflow["jobs"]["postgres-contract"]["steps"]
        if step.get("name") == "Validate production Compose"
    )

    assert compose_step["env"] == COMPOSE_PLACEHOLDERS
    assert "--profile migration" in workflow
    assert "--profile checkpoint-maintenance" in workflow
    assert "--profile checkpoint-maintenance-execute" in workflow
    assert "database-bootstrap" in workflow
    assert "checkpoint-maintenance" in workflow
    assert "checkpoint-maintenance-execute" in workflow
    assert "--env-file /dev/null config --quiet" in workflow


def test_polling_ci_runs_the_full_suite_without_skip_filters() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")
    pytest_lines = [
        line.strip().removeprefix("run: ")
        for line in workflow.splitlines()
        if "python -m pytest" in line
    ]

    assert pytest_lines == [FULL_SUITE_COMMAND]
    assert "--ignore" not in workflow
    assert all(" -k " not in line for line in pytest_lines)


def test_polling_ci_enforces_ninety_percent_for_each_critical_target() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    for target in MANDATORY_COVERAGE_TARGETS:
        expected = (
            "uv run --frozen coverage report --precision=2 "
            f"--fail-under=90 --include='{target}'"
        )
        assert expected in workflow

    assert (
        "uv run --frozen coverage report --precision=2 --fail-under=90 "
        "--include='src/db/roles.py,src/db/schema_contract.py,src/db/bootstrap.py'"
    ) in workflow


def test_polling_ci_has_no_failure_bypass_for_mandatory_gates() -> None:
    workflow = POLLING_WORKFLOW.read_text(encoding="utf-8")

    assert "continue-on-error" not in workflow
    assert "|| true" not in workflow
