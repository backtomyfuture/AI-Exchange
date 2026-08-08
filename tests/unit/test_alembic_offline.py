"""Tests for the one supported greenfield Alembic baseline."""

from __future__ import annotations

from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from io import StringIO
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASELINE = PROJECT_ROOT / "alembic" / "versions" / "20260808_0001_polling_baseline.py"
BASELINE_SQL = BASELINE.with_suffix(".sql")


def _revision_module():
    spec = spec_from_file_location("polling_baseline_revision", BASELINE)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_versions_directory_contains_only_the_greenfield_baseline() -> None:
    assert sorted(path.name for path in BASELINE.parent.glob("*.py")) == [
        BASELINE.name
    ]
    assert sorted(path.name for path in BASELINE.parent.glob("*.sql")) == [
        BASELINE_SQL.name
    ]


def test_baseline_is_root_revision_with_a_verified_snapshot() -> None:
    revision = _revision_module()

    assert revision.revision == "20260808_0001"
    assert revision.down_revision is None
    assert sha256(BASELINE_SQL.read_bytes()).hexdigest() == revision._BASELINE_SQL_SHA256
    assert revision._baseline_sql() == BASELINE_SQL.read_text(encoding="utf-8")


def test_baseline_is_explicitly_forward_only() -> None:
    revision = _revision_module()

    with pytest.raises(NotImplementedError, match="no_supported_downgrade"):
        revision.downgrade()


def test_offline_upgrade_emits_the_polling_only_catalog() -> None:
    output = StringIO()
    config = Config(str(PROJECT_ROOT / "alembic.ini"), output_buffer=output)
    config.set_main_option(
        "sqlalchemy.url",
        "postgresql://offline:offline@localhost/offline",
    )

    command.upgrade(config, "head", sql=True)

    rendered = output.getvalue()
    normalized = " ".join(rendered.split())
    assert "INSERT INTO alembic_version (version_num) VALUES ('20260808_0001')" in rendered
    assert "CREATE TABLE public.event_inbox" in rendered
    assert "CREATE FUNCTION public.greenfield_commit_sync_page" in rendered
    assert "CREATE TABLE public.daily_digest_executions" in rendered
    assert "greenfield_insert_webhook_event" not in rendered
    assert "cold_start" not in rendered.casefold()
    assert "%%" not in rendered
    assert "SET LOCAL check_function_bodies = false" in normalized
