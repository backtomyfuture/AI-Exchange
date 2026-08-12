"""Create the one supported empty-database polling baseline.

The accompanying SQL snapshot is deliberately schema-only and is derived from
the reviewed greenfield polling catalog.  There is no downgrade or upgrade
path from an earlier AI-Exchange application database: deployments start from
an empty PostgreSQL volume.
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from alembic import context, op


revision = "20260808_0001"
down_revision = None
branch_labels = None
depends_on = None

_BASELINE_SQL_PATH = Path(__file__).with_suffix(".sql")
_BASELINE_SQL_SHA256 = "34e6de16adef8fab6915e8f71662ab7929315e435f8e20c3547244c034626d5d"


def _baseline_sql() -> str:
    source = _BASELINE_SQL_PATH.read_text(encoding="utf-8")
    if sha256(source.encode("utf-8")).hexdigest() != _BASELINE_SQL_SHA256:
        raise RuntimeError("polling_baseline_sql_digest_invalid")
    return source


def upgrade() -> None:
    # PostgreSQL executes this reviewed schema snapshot as one simple query.
    # Literal percent characters are doubled in the source because psycopg
    # uses pyformat placeholders; the driver sends the canonical single-
    # percent SQL to PostgreSQL.
    # ``SET LOCAL check_function_bodies`` in the snapshot permits the canonical
    # function-before-table order produced by pg_dump while preserving the
    # migration transaction's normal session state after commit.
    source = _baseline_sql()
    if context.is_offline_mode():
        # Alembic's offline ``MockConnection`` has no driver-level execution
        # API.  The snapshot is emitted as canonical SQL for review; the
        # doubled percent signs are solely the online psycopg escaping.
        op.execute(source.replace("%%", "%"))
        return
    op.get_bind().exec_driver_sql(source)


def downgrade() -> None:
    raise NotImplementedError("greenfield_baseline_has_no_supported_downgrade")
