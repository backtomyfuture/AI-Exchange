from __future__ import annotations

from io import StringIO
from pathlib import Path

from alembic import command
from alembic.config import Config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_offline_upgrade_emits_fail_closed_0004_policy_migration_sql() -> None:
    output = StringIO()
    config = Config(str(PROJECT_ROOT / "alembic.ini"), output_buffer=output)
    config.set_main_option(
        "sqlalchemy.url",
        "postgresql://offline:offline@localhost/offline",
    )

    command.upgrade(config, "head", sql=True)

    rendered = output.getvalue()
    assert "LOCK TABLE event_inbox IN ACCESS EXCLUSIVE MODE" in rendered
    assert "event_inbox_not_empty_for_0004_migration" in rendered
    assert "DROP CONSTRAINT ck_event_inbox_processing_policy" in rendered
