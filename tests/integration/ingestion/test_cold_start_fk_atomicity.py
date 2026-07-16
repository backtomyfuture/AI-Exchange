"""Real-PostgreSQL coverage for the cold-start cursor/plan FK pair.

The two deferred foreign keys only protect an approved plan with a non-null
apply binding.  First-page direct terminal transitions and pre-binding blocks
therefore remain service-level atomicity invariants covered by cold-start
service tests; this module deliberately makes no broader schema claim.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import psycopg
import pytest

from src.db.bootstrap import bootstrap_database
from src.db.schema import get_current_database_revision


_PLAN_TO_CURSOR_FK = "fk_sync_cold_start_plan_active_cursor"
_CURSOR_TO_PLAN_FK = "fk_sync_cursors_cold_start_plan"


async def _bootstrap(schema) -> None:
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    assert await get_current_database_revision(schema.dsn) == "20260713_0005"


def _insert_owner(schema, *, account_id: int) -> None:
    schema.execute(
        "INSERT INTO pipeline_ownership ("
        "account_id, generation, pipeline_name, state, fencing_token, "
        "created_by, reason"
        ") VALUES (%s, 1, 'durable', 'current_ingress', 1, "
        "'fk-atomicity-test', 'cold-start-fk-matrix')",
        (account_id,),
    )


def _insert_approved_plan_and_cursor(
    schema,
    *,
    account_id: int,
    bound: bool,
) -> UUID:
    now = datetime.now(UTC).replace(microsecond=0)
    plan_id = uuid4()
    apply_cursor = "apply-1" if bound else None
    apply_version = 0 if bound else None
    _insert_owner(schema, account_id=account_id)
    with psycopg.connect(schema.maintenance_dsn) as connection:
        with connection.transaction():
            connection.execute(
                "INSERT INTO sync_cold_start_plans ("
                "plan_id, account_id, folder_key, expected_cursor_status, "
                "expected_cursor_version, pipeline_name, generation, "
                "fencing_token, state, preview_cursor, preview_cursor_version, "
                "boundary_cursor, boundary_cursor_version, apply_cursor, "
                "apply_cursor_version, rolling_hash, page_count, item_count, "
                "contract_fingerprint, folder_scope_config_hash, plan_hash, "
                "actor, reason, expires_at, ready_at, approved_at"
                ") VALUES ("
                "%s, %s, 'INBOX', 'cold_start_pending', 0, 'durable', 1, 1, "
                "'approved', 'preview-1', 1, 'preview-1', 1, %s, %s, %s, "
                "1, 1, %s, %s, %s, 'fk-atomicity-test', "
                "'approved plan', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (
                    plan_id,
                    account_id,
                    apply_cursor,
                    apply_version,
                    "1" * 64,
                    "2" * 64,
                    "3" * 64,
                    "4" * 64,
                    now + timedelta(hours=1),
                ),
            )
            if bound:
                connection.execute(
                    "INSERT INTO sync_cursors ("
                    "account_id, folder_key, cursor, status, "
                    "last_success_at, last_attempt_at, cold_start_plan_id, "
                    "cold_start_plan_state"
                    ") VALUES (%s, 'INBOX', 'apply-1', "
                    "'cold_start_applying', %s, %s, %s, 'approved')",
                    (account_id, now, now, plan_id),
                )
            else:
                connection.execute(
                    "INSERT INTO sync_cursors ("
                    "account_id, folder_key, status, blocked_reason_code"
                    ") VALUES (%s, 'INBOX', 'cold_start_pending', "
                    "'awaiting_apply')",
                    (account_id,),
                )
    return plan_id


def _pair_snapshot(connection, *, plan_id: UUID, account_id: int) -> tuple[dict, dict]:
    plan = connection.execute(
        "SELECT pg_catalog.to_jsonb(plan_record) "
        "FROM sync_cold_start_plans AS plan_record WHERE plan_id = %s",
        (plan_id,),
    ).fetchone()[0]
    cursor = connection.execute(
        "SELECT pg_catalog.to_jsonb(cursor_record) "
        "FROM sync_cursors AS cursor_record "
        "WHERE account_id = %s AND folder_key = 'INBOX'",
        (account_id,),
    ).fetchone()[0]
    return plan, cursor


def _assert_fk_pair_is_initially_deferred(connection) -> None:
    rows = connection.execute(
        "SELECT conname, condeferrable, condeferred "
        "FROM pg_catalog.pg_constraint WHERE conname IN (%s, %s)",
        (_PLAN_TO_CURSOR_FK, _CURSOR_TO_PLAN_FK),
    ).fetchall()
    assert {row[0]: row[1:] for row in rows} == {
        _PLAN_TO_CURSOR_FK: (True, True),
        _CURSOR_TO_PLAN_FK: (True, True),
    }


def _bind_plan(connection, *, plan_id: UUID) -> None:
    result = connection.execute(
        "UPDATE sync_cold_start_plans "
        "SET apply_cursor = 'apply-1', apply_cursor_version = 1 "
        "WHERE plan_id = %s",
        (plan_id,),
    )
    assert result.rowcount == 1


def _bind_cursor(connection, *, plan_id: UUID, account_id: int) -> None:
    result = connection.execute(
        "UPDATE sync_cursors "
        "SET cursor = 'apply-1', version = 1, "
        "status = 'cold_start_applying', blocked_reason_code = NULL, "
        "last_success_at = CURRENT_TIMESTAMP, "
        "last_attempt_at = CURRENT_TIMESTAMP, cold_start_plan_id = %s, "
        "cold_start_plan_state = 'approved' "
        "WHERE account_id = %s AND folder_key = 'INBOX'",
        (plan_id, account_id),
    )
    assert result.rowcount == 1


def _advance_plan(connection, *, plan_id: UUID) -> None:
    result = connection.execute(
        "UPDATE sync_cold_start_plans "
        "SET apply_cursor = 'apply-2', apply_cursor_version = 1 "
        "WHERE plan_id = %s",
        (plan_id,),
    )
    assert result.rowcount == 1


def _advance_cursor(connection, *, account_id: int) -> None:
    result = connection.execute(
        "UPDATE sync_cursors "
        "SET cursor = 'apply-2', version = 1, "
        "last_success_at = CURRENT_TIMESTAMP, "
        "last_attempt_at = CURRENT_TIMESTAMP "
        "WHERE account_id = %s AND folder_key = 'INBOX'",
        (account_id,),
    )
    assert result.rowcount == 1


def _complete_plan(connection, *, plan_id: UUID) -> None:
    result = connection.execute(
        "UPDATE sync_cold_start_plans "
        "SET state = 'completed', completed_at = CURRENT_TIMESTAMP "
        "WHERE plan_id = %s",
        (plan_id,),
    )
    assert result.rowcount == 1


def _activate_cursor(connection, *, account_id: int) -> None:
    result = connection.execute(
        "UPDATE sync_cursors "
        "SET status = 'active', blocked_reason_code = NULL, "
        "contract_fingerprint = NULL, blocked_at = NULL, "
        "cold_start_plan_id = NULL, cold_start_plan_state = NULL "
        "WHERE account_id = %s AND folder_key = 'INBOX'",
        (account_id,),
    )
    assert result.rowcount == 1


def _block_plan(connection, *, plan_id: UUID) -> None:
    result = connection.execute(
        "UPDATE sync_cold_start_plans "
        "SET state = 'blocked', blocked_reason_code = 'apply_failed', "
        "blocked_fingerprint = %s, blocked_at = CURRENT_TIMESTAMP "
        "WHERE plan_id = %s",
        ("5" * 64, plan_id),
    )
    assert result.rowcount == 1


def _block_cursor(connection, *, account_id: int) -> None:
    result = connection.execute(
        "UPDATE sync_cursors "
        "SET status = 'blocked_contract', "
        "blocked_reason_code = 'apply_failed', contract_fingerprint = %s, "
        "blocked_at = CURRENT_TIMESTAMP, cold_start_plan_id = NULL, "
        "cold_start_plan_state = NULL "
        "WHERE account_id = %s AND folder_key = 'INBOX'",
        ("6" * 64, account_id),
    )
    assert result.rowcount == 1


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "expected_constraint"),
    (("plan", _PLAN_TO_CURSOR_FK), ("cursor", _CURSOR_TO_PLAN_FK)),
    ids=("plan-only", "cursor-only"),
)
async def test_first_binding_single_side_fails_at_xid_end_and_rolls_back(
    postgres_database_factory,
    side: str,
    expected_constraint: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 101
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=False,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        before = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )
        statement_completed = False
        with pytest.raises(psycopg.errors.ForeignKeyViolation) as exc_info:
            with connection.transaction():
                if side == "plan":
                    _bind_plan(connection, plan_id=plan_id)
                else:
                    _bind_cursor(
                        connection,
                        plan_id=plan_id,
                        account_id=account_id,
                    )
                statement_completed = True
                assert (
                    _pair_snapshot(
                        connection,
                        plan_id=plan_id,
                        account_id=account_id,
                    )
                    != before
                )

        assert statement_completed is True
        assert exc_info.value.diag.constraint_name == expected_constraint
        assert (
            _pair_snapshot(
                connection,
                plan_id=plan_id,
                account_id=account_id,
            )
            == before
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("order", ("plan-first", "cursor-first"))
async def test_first_binding_commits_both_sides_in_either_statement_order(
    postgres_database_factory,
    order: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 102
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=False,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        _assert_fk_pair_is_initially_deferred(connection)
        with connection.transaction():
            if order == "plan-first":
                _bind_plan(connection, plan_id=plan_id)
                _bind_cursor(connection, plan_id=plan_id, account_id=account_id)
            else:
                _bind_cursor(connection, plan_id=plan_id, account_id=account_id)
                _bind_plan(connection, plan_id=plan_id)

        plan, cursor = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )

    assert (
        plan["state"],
        plan["apply_cursor"],
        plan["apply_cursor_version"],
        plan["cursor_binding_plan_id"],
    ) == ("approved", "apply-1", 1, str(plan_id))
    assert (
        cursor["status"],
        cursor["cursor"],
        cursor["version"],
        cursor["cold_start_plan_id"],
        cursor["cold_start_plan_state"],
    ) == ("cold_start_applying", "apply-1", 1, str(plan_id), "approved")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "side",
    ("plan", "cursor"),
    ids=("plan-only", "cursor-only"),
)
async def test_existing_binding_single_side_advance_fails_at_xid_end_and_rolls_back(
    postgres_database_factory,
    side: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 103
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=True,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        before = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )
        statement_completed = False
        with pytest.raises(psycopg.errors.ForeignKeyViolation) as exc_info:
            with connection.transaction():
                if side == "plan":
                    _advance_plan(connection, plan_id=plan_id)
                else:
                    _advance_cursor(connection, account_id=account_id)
                statement_completed = True
                assert (
                    _pair_snapshot(
                        connection,
                        plan_id=plan_id,
                        account_id=account_id,
                    )
                    != before
                )

        assert statement_completed is True
        # This advance invalidates both mirrored FKs, so deferred RI trigger order
        # is not contractual. First binding, completion and block isolate directions.
        assert exc_info.value.diag.constraint_name in {
            _PLAN_TO_CURSOR_FK,
            _CURSOR_TO_PLAN_FK,
        }
        assert (
            _pair_snapshot(
                connection,
                plan_id=plan_id,
                account_id=account_id,
            )
            == before
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("order", ("plan-first", "cursor-first"))
async def test_existing_binding_advance_commits_in_either_statement_order(
    postgres_database_factory,
    order: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 104
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=True,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        with connection.transaction():
            if order == "plan-first":
                _advance_plan(connection, plan_id=plan_id)
                _advance_cursor(connection, account_id=account_id)
            else:
                _advance_cursor(connection, account_id=account_id)
                _advance_plan(connection, plan_id=plan_id)

        plan, cursor = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )

    assert (
        plan["state"],
        plan["apply_cursor"],
        plan["apply_cursor_version"],
        plan["cursor_binding_plan_id"],
    ) == ("approved", "apply-2", 1, str(plan_id))
    assert (
        cursor["status"],
        cursor["cursor"],
        cursor["version"],
        cursor["cold_start_plan_id"],
        cursor["cold_start_plan_state"],
    ) == ("cold_start_applying", "apply-2", 1, str(plan_id), "approved")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "expected_constraint"),
    (("plan", _CURSOR_TO_PLAN_FK), ("cursor", _PLAN_TO_CURSOR_FK)),
    ids=("plan-only", "active-cursor-only"),
)
async def test_completion_single_side_fails_at_xid_end_and_rolls_back(
    postgres_database_factory,
    side: str,
    expected_constraint: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 105
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=True,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        before = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )
        statement_completed = False
        with pytest.raises(psycopg.errors.ForeignKeyViolation) as exc_info:
            with connection.transaction():
                if side == "plan":
                    _complete_plan(connection, plan_id=plan_id)
                else:
                    _activate_cursor(connection, account_id=account_id)
                statement_completed = True
                assert (
                    _pair_snapshot(
                        connection,
                        plan_id=plan_id,
                        account_id=account_id,
                    )
                    != before
                )

        assert statement_completed is True
        assert exc_info.value.diag.constraint_name == expected_constraint
        assert (
            _pair_snapshot(
                connection,
                plan_id=plan_id,
                account_id=account_id,
            )
            == before
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("order", ("plan-first", "cursor-first"))
async def test_completion_commits_both_sides_in_either_statement_order(
    postgres_database_factory,
    order: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 106
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=True,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        with connection.transaction():
            if order == "plan-first":
                _complete_plan(connection, plan_id=plan_id)
                _activate_cursor(connection, account_id=account_id)
            else:
                _activate_cursor(connection, account_id=account_id)
                _complete_plan(connection, plan_id=plan_id)

        plan, cursor = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )

    assert (
        plan["state"],
        plan["apply_cursor"],
        plan["apply_cursor_version"],
        plan["cursor_binding_plan_id"],
    ) == ("completed", "apply-1", 0, None)
    assert plan["completed_at"] is not None
    assert (
        cursor["status"],
        cursor["cursor"],
        cursor["version"],
        cursor["cold_start_plan_id"],
        cursor["cold_start_plan_state"],
    ) == ("active", "apply-1", 0, None, None)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "expected_constraint"),
    (("plan", _CURSOR_TO_PLAN_FK), ("cursor", _PLAN_TO_CURSOR_FK)),
    ids=("plan-only", "blocked-cursor-only"),
)
async def test_block_single_side_fails_at_xid_end_and_rolls_back(
    postgres_database_factory,
    side: str,
    expected_constraint: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 107
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=True,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        before = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )
        statement_completed = False
        with pytest.raises(psycopg.errors.ForeignKeyViolation) as exc_info:
            with connection.transaction():
                if side == "plan":
                    _block_plan(connection, plan_id=plan_id)
                else:
                    _block_cursor(connection, account_id=account_id)
                statement_completed = True
                assert (
                    _pair_snapshot(
                        connection,
                        plan_id=plan_id,
                        account_id=account_id,
                    )
                    != before
                )

        assert statement_completed is True
        assert exc_info.value.diag.constraint_name == expected_constraint
        assert (
            _pair_snapshot(
                connection,
                plan_id=plan_id,
                account_id=account_id,
            )
            == before
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("order", ("plan-first", "cursor-first"))
async def test_block_commits_both_sides_in_either_statement_order(
    postgres_database_factory,
    order: str,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    account_id = 108
    plan_id = _insert_approved_plan_and_cursor(
        schema,
        account_id=account_id,
        bound=True,
    )

    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        with connection.transaction():
            if order == "plan-first":
                _block_plan(connection, plan_id=plan_id)
                _block_cursor(connection, account_id=account_id)
            else:
                _block_cursor(connection, account_id=account_id)
                _block_plan(connection, plan_id=plan_id)

        plan, cursor = _pair_snapshot(
            connection,
            plan_id=plan_id,
            account_id=account_id,
        )

    assert (
        plan["state"],
        plan["apply_cursor"],
        plan["apply_cursor_version"],
        plan["cursor_binding_plan_id"],
        plan["completed_at"],
        plan["blocked_reason_code"],
        plan["blocked_fingerprint"],
    ) == ("blocked", "apply-1", 0, None, None, "apply_failed", "5" * 64)
    assert plan["blocked_at"] is not None
    assert (
        cursor["status"],
        cursor["cursor"],
        cursor["version"],
        cursor["cold_start_plan_id"],
        cursor["cold_start_plan_state"],
        cursor["blocked_reason_code"],
        cursor["contract_fingerprint"],
    ) == ("blocked_contract", "apply-1", 0, None, None, "apply_failed", "6" * 64)
    assert cursor["blocked_at"] is not None
