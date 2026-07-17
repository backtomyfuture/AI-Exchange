from __future__ import annotations

import pytest


@pytest.fixture
def db(empty_schema, alembic_runner):
    """Upgrade one disposable role-separated database to the Alembic head."""

    alembic_runner.upgrade(empty_schema, "head")
    return empty_schema
