from __future__ import annotations

import asyncio
import hashlib
import importlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from src.db.bootstrap import bootstrap_database
from src.db.schema import get_current_database_revision


_COMMANDS = (
    "cold_start.preview",
    "cold_start.approve",
    "cold_start.apply_page",
)


async def _bootstrap(schema) -> None:
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    assert await get_current_database_revision(schema.dsn) == "20260713_0005"


def _receipt_values(
    *,
    canonical_payload_hash: str = "a" * 64,
    result_hash: str = "b" * 64,
) -> dict[str, object]:
    return {
        "account_id": 8,
        "command_name": "cold_start.preview",
        "idempotency_key": "preview-command-1",
        "canonical_payload_hash": canonical_payload_hash,
        "outcome": "succeeded",
        "result_type": "sync_cold_start_plan",
        "result_id": str(uuid4()),
        "result_hash": result_hash,
        "authority_epoch": 0,
    }


def _insert_owner(schema) -> None:
    schema.execute(
        "INSERT INTO pipeline_ownership ("
        "account_id, generation, pipeline_name, state, fencing_token, "
        "created_by, reason"
        ") VALUES (8, 1, 'durable', 'current_ingress', 1, "
        "'integration-test', 'cold-start-test')"
    )


def _insert_blocked_plan(schema, **overrides) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    values = {
        "plan_id": uuid4(),
        "account_id": 8,
        "folder_key": "INBOX",
        "expected_cursor_status": "cold_start_pending",
        "expected_cursor": None,
        "expected_cursor_version": 0,
        "pipeline_name": "durable",
        "generation": 1,
        "fencing_token": 1,
        "state": "blocked",
        "preview_cursor": None,
        "preview_cursor_version": 0,
        "boundary_cursor": None,
        "boundary_cursor_version": None,
        "apply_cursor": None,
        "apply_cursor_version": None,
        "rolling_hash": None,
        "page_count": 0,
        "item_count": 0,
        "contract_fingerprint": "1" * 64,
        "folder_scope_config_hash": "2" * 64,
        "plan_hash": None,
        "actor": "integration-test",
        "reason": "adversarial blocked state",
        "blocked_reason_code": "test_blocked",
        "blocked_fingerprint": "3" * 64,
        "expires_at": now + timedelta(hours=1),
        "ready_at": None,
        "approved_at": None,
        "completed_at": None,
        "blocked_at": now,
        "created_at": now - timedelta(minutes=1),
    }
    values.update(overrides)
    schema.execute(
        "INSERT INTO sync_cold_start_plans ("
        "plan_id, account_id, folder_key, expected_cursor_status, "
        "expected_cursor, expected_cursor_version, pipeline_name, generation, "
        "fencing_token, state, preview_cursor, preview_cursor_version, "
        "boundary_cursor, boundary_cursor_version, apply_cursor, "
        "apply_cursor_version, rolling_hash, page_count, item_count, "
        "contract_fingerprint, folder_scope_config_hash, plan_hash, actor, "
        "reason, blocked_reason_code, blocked_fingerprint, expires_at, "
        "ready_at, approved_at, completed_at, blocked_at, created_at"
        ") VALUES ("
        "%(plan_id)s, %(account_id)s, %(folder_key)s, "
        "%(expected_cursor_status)s, %(expected_cursor)s, "
        "%(expected_cursor_version)s, %(pipeline_name)s, %(generation)s, "
        "%(fencing_token)s, %(state)s, %(preview_cursor)s, "
        "%(preview_cursor_version)s, %(boundary_cursor)s, "
        "%(boundary_cursor_version)s, %(apply_cursor)s, "
        "%(apply_cursor_version)s, %(rolling_hash)s, %(page_count)s, "
        "%(item_count)s, %(contract_fingerprint)s, "
        "%(folder_scope_config_hash)s, %(plan_hash)s, %(actor)s, %(reason)s, "
        "%(blocked_reason_code)s, %(blocked_fingerprint)s, %(expires_at)s, "
        "%(ready_at)s, %(approved_at)s, %(completed_at)s, %(blocked_at)s, "
        "%(created_at)s) ",
        values,
    )


def _insert_approved_plan_and_applying_cursor(schema):
    now = datetime.now(UTC).replace(microsecond=0)
    plan_id = uuid4()
    _insert_owner(schema)
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
                "%s, 8, 'INBOX', 'cold_start_pending', 0, 'durable', 1, 1, "
                "'approved', 'preview-1', 1, 'preview-1', 1, 'apply-1', 0, "
                "%s, 1, 1, %s, %s, %s, 'integration-test', "
                "'approved plan', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (
                    plan_id,
                    "4" * 64,
                    "5" * 64,
                    "6" * 64,
                    "7" * 64,
                    now + timedelta(hours=1),
                ),
            )
            connection.execute(
                "INSERT INTO sync_cursors ("
                "account_id, folder_key, cursor, status, "
                "last_success_at, last_attempt_at, cold_start_plan_id, "
                "cold_start_plan_state"
                ") VALUES (8, 'INBOX', 'apply-1', 'cold_start_applying', "
                "%s, %s, %s, 'approved')",
                (now - timedelta(seconds=10), now, plan_id),
            )
    return plan_id


def _insert_approved_plan_and_pending_cursor(schema):
    now = datetime.now(UTC).replace(microsecond=0)
    plan_id = uuid4()
    _insert_owner(schema)
    with psycopg.connect(schema.maintenance_dsn) as connection:
        with connection.transaction():
            connection.execute(
                "INSERT INTO sync_cold_start_plans ("
                "plan_id, account_id, folder_key, expected_cursor_status, "
                "expected_cursor_version, pipeline_name, generation, "
                "fencing_token, state, preview_cursor, preview_cursor_version, "
                "boundary_cursor, boundary_cursor_version, rolling_hash, "
                "page_count, item_count, contract_fingerprint, "
                "folder_scope_config_hash, plan_hash, actor, reason, "
                "expires_at, ready_at, approved_at"
                ") VALUES ("
                "%s, 8, 'INBOX', 'cold_start_pending', 0, 'durable', 1, 1, "
                "'approved', 'preview-1', 1, 'preview-1', 1, %s, 1, 1, %s, "
                "%s, %s, 'integration-test', 'approved plan', %s, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (
                    plan_id,
                    "4" * 64,
                    "5" * 64,
                    "6" * 64,
                    "7" * 64,
                    now + timedelta(hours=1),
                ),
            )
            connection.execute(
                "INSERT INTO sync_cursors ("
                "account_id, folder_key, cursor, status, "
                "blocked_reason_code"
                ") VALUES (8, 'INBOX', NULL, 'cold_start_pending', "
                "'awaiting_apply')"
            )
    return plan_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0005_installs_exact_receipt_tables_and_filtered_view(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)

    with psycopg.connect(schema.dsn, autocommit=True) as connection:
        relation_rows = connection.execute(
            "SELECT relation.relname::pg_catalog.text, "
            "relation.relkind::pg_catalog.text, owner.rolname::pg_catalog.text "
            "FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "ON namespace.oid = relation.relnamespace "
            "JOIN pg_catalog.pg_roles AS owner ON owner.oid = relation.relowner "
            "WHERE namespace.nspname = 'public' "
            "AND relation.relname IN ("
            "'pipeline_command_receipts', 'sync_cold_start_plans', "
            "'cold_start_command_receipts')"
        ).fetchall()
        assert set(relation_rows) == {
            ("pipeline_command_receipts", "r", schema.migration_role),
            ("sync_cold_start_plans", "r", schema.migration_role),
            ("cold_start_command_receipts", "v", schema.migration_role),
        }

        view_definition, check_option, relation_options = connection.execute(
            "SELECT pg_catalog.pg_get_viewdef(view_relation.oid, false), "
            "information_schema.views.check_option, "
            "COALESCE(view_relation.reloptions, ARRAY[]::pg_catalog.text[]) "
            "FROM pg_catalog.pg_class AS view_relation "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "ON namespace.oid = view_relation.relnamespace "
            "JOIN information_schema.views "
            "ON information_schema.views.table_schema = namespace.nspname "
            "AND information_schema.views.table_name = view_relation.relname "
            "WHERE namespace.nspname = 'public' "
            "AND view_relation.relname = 'cold_start_command_receipts'"
        ).fetchone()
        assert check_option == "CASCADED"
        assert "security_invoker" not in " ".join(relation_options)
        for command in _COMMANDS:
            assert view_definition.count(command) == 1
        assert "cold_start.cancel" not in view_definition

        command_check = connection.execute(
            "SELECT pg_catalog.pg_get_constraintdef(constraint_record.oid, false) "
            "FROM pg_catalog.pg_constraint AS constraint_record "
            "JOIN pg_catalog.pg_class AS relation "
            "ON relation.oid = constraint_record.conrelid "
            "WHERE relation.relname = 'pipeline_command_receipts' "
            "AND constraint_record.conname = "
            "'ck_pipeline_command_receipts_command_name'"
        ).fetchone()[0]
        for command in _COMMANDS:
            assert command_check.count(command) == 1
        assert "cold_start.cancel" not in command_check

        trigger_names = {
            row[0]
            for row in connection.execute(
                "SELECT trigger.tgname::pg_catalog.text "
                "FROM pg_catalog.pg_trigger AS trigger "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = trigger.tgrelid "
                "WHERE relation.relname = 'pipeline_command_receipts' "
                "AND NOT trigger.tgisinternal"
            ).fetchall()
        }
        assert trigger_names == {
            "trg_pipeline_command_receipts_guard_row",
            "trg_pipeline_command_receipts_guard_truncate",
        }

        plan_columns = {
            row[0]
            for row in connection.execute(
                "SELECT attribute.attname::pg_catalog.text "
                "FROM pg_catalog.pg_attribute AS attribute "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = attribute.attrelid "
                "WHERE relation.relname = 'sync_cold_start_plans' "
                "AND attribute.attnum > 0 AND NOT attribute.attisdropped"
            ).fetchall()
        }
        assert {
            "boundary_cursor_version",
            "ready_at",
            "blocked_fingerprint",
            "blocked_at",
        } <= plan_columns

        index_names = {
            row[0]
            for row in connection.execute(
                "SELECT index_relation.relname::pg_catalog.text "
                "FROM pg_catalog.pg_index AS index "
                "JOIN pg_catalog.pg_class AS index_relation "
                "ON index_relation.oid = index.indexrelid "
                "JOIN pg_catalog.pg_class AS table_relation "
                "ON table_relation.oid = index.indrelid "
                "WHERE table_relation.relname IN "
                "('sync_cold_start_plans', 'sync_cursors')"
            ).fetchall()
        }
        assert {
            "ix_sync_cold_start_plans_state_expiry",
            "uq_sync_cursors_cold_start_plan",
        } <= index_names


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "illegal_state",
    [
        "preseal-approved",
        "preseal-apply-pair",
        "sealed-approval-before-ready",
        "sealed-apply-without-approval",
        "sealed-blocked-before-ready",
        "sealed-blocked-before-approval",
    ],
    ids=(
        "preseal-approved",
        "preseal-apply-pair",
        "sealed-approval-before-ready",
        "sealed-apply-without-approval",
        "sealed-blocked-before-ready",
        "sealed-blocked-before-approval",
    ),
)
async def test_blocked_plan_rejects_impossible_approval_and_apply_states(
    postgres_database_factory,
    illegal_state,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    _insert_owner(schema)
    now = datetime.now(UTC).replace(microsecond=0)
    overrides: dict[str, object]
    if illegal_state == "preseal-approved":
        overrides = {"approved_at": now}
    elif illegal_state == "preseal-apply-pair":
        overrides = {"apply_cursor": "impossible", "apply_cursor_version": 1}
    else:
        overrides = {
            "preview_cursor": "preview-1",
            "preview_cursor_version": 1,
            "boundary_cursor": "preview-1",
            "boundary_cursor_version": 1,
            "rolling_hash": "4" * 64,
            "page_count": 1,
            "item_count": 1,
            "plan_hash": "5" * 64,
            "ready_at": now - timedelta(seconds=20),
        }
        if illegal_state == "sealed-approval-before-ready":
            overrides["approved_at"] = now - timedelta(seconds=30)
        elif illegal_state == "sealed-apply-without-approval":
            overrides.update(
                apply_cursor="apply-1",
                apply_cursor_version=1,
            )
        elif illegal_state == "sealed-blocked-before-ready":
            overrides["blocked_at"] = now - timedelta(seconds=30)
        else:
            overrides.update(
                approved_at=now - timedelta(seconds=10),
                blocked_at=now - timedelta(seconds=15),
            )

    with pytest.raises(psycopg.errors.CheckViolation):
        _insert_blocked_plan(schema, **overrides)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_applying_cursor_binding_blocks_runtime_drift_but_allows_atomic_maintenance(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    plan_id = _insert_approved_plan_and_applying_cursor(schema)

    with psycopg.connect(schema.runtime_dsn, autocommit=True) as connection:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute(
                "UPDATE sync_cursors SET cold_start_plan_state = 'approved' "
                "WHERE cold_start_plan_id = %s",
                (plan_id,),
            )

    with psycopg.connect(schema.runtime_dsn) as connection:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with connection.transaction():
                connection.execute(
                    "UPDATE sync_cursors "
                    "SET cursor = 'runtime-drift', version = 2 "
                    "WHERE account_id = 8 AND folder_key = 'INBOX'"
                )

    with psycopg.connect(schema.maintenance_dsn) as connection:
        with connection.transaction():
            connection.execute(
                "UPDATE sync_cold_start_plans "
                "SET apply_cursor = 'apply-2', apply_cursor_version = 2 "
                "WHERE plan_id = %s",
                (plan_id,),
            )
            connection.execute(
                "UPDATE sync_cursors SET cursor = 'apply-2', version = 2 "
                "WHERE cold_start_plan_id = %s",
                (plan_id,),
            )

    assert (
        schema.scalar(
            "SELECT cursor FROM sync_cursors WHERE cold_start_plan_id = %s",
            (plan_id,),
        )
        == "apply-2"
    )
    assert (
        schema.scalar(
            "SELECT apply_cursor FROM sync_cold_start_plans WHERE plan_id = %s",
            (plan_id,),
        )
        == "apply-2"
    )

    with psycopg.connect(schema.maintenance_dsn) as connection:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with connection.transaction():
                connection.execute(
                    "UPDATE sync_cold_start_plans SET state = 'blocked', "
                    "blocked_reason_code = 'apply_failed', "
                    "blocked_fingerprint = %s, blocked_at = CURRENT_TIMESTAMP "
                    "WHERE plan_id = %s",
                    ("8" * 64, plan_id),
                )

        with connection.transaction():
            connection.execute(
                "UPDATE sync_cold_start_plans SET state = 'blocked', "
                "blocked_reason_code = 'apply_failed', "
                "blocked_fingerprint = %s, blocked_at = CURRENT_TIMESTAMP "
                "WHERE plan_id = %s",
                ("8" * 64, plan_id),
            )
            connection.execute(
                "UPDATE sync_cursors SET status = 'blocked_contract', "
                "blocked_reason_code = 'apply_failed', "
                "contract_fingerprint = %s, blocked_at = CURRENT_TIMESTAMP, "
                "cold_start_plan_id = NULL, cold_start_plan_state = NULL "
                "WHERE cold_start_plan_id = %s",
                ("9" * 64, plan_id),
            )

    assert (
        schema.scalar(
            "SELECT state FROM sync_cold_start_plans WHERE plan_id = %s",
            (plan_id,),
        )
        == "blocked"
    )
    assert (
        schema.scalar(
            "SELECT status FROM sync_cursors WHERE account_id = 8 "
            "AND folder_key = 'INBOX'"
        )
        == "blocked_contract"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_approved_plan_apply_progress_requires_same_xid_cursor_binding(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    plan_id = _insert_approved_plan_and_pending_cursor(schema)

    with psycopg.connect(schema.maintenance_dsn) as connection:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with connection.transaction():
                connection.execute(
                    "UPDATE sync_cold_start_plans "
                    "SET apply_cursor = 'apply-1', apply_cursor_version = 1 "
                    "WHERE plan_id = %s",
                    (plan_id,),
                )

        with connection.transaction():
            connection.execute(
                "UPDATE sync_cold_start_plans "
                "SET apply_cursor = 'apply-1', apply_cursor_version = 1 "
                "WHERE plan_id = %s",
                (plan_id,),
            )
            connection.execute(
                "UPDATE sync_cursors SET cursor = 'apply-1', version = 1, "
                "status = 'cold_start_applying', blocked_reason_code = NULL, "
                "last_success_at = CURRENT_TIMESTAMP, "
                "last_attempt_at = CURRENT_TIMESTAMP, "
                "cold_start_plan_id = %s, cold_start_plan_state = 'approved' "
                "WHERE account_id = 8 AND folder_key = 'INBOX'",
                (plan_id,),
            )

    assert (
        schema.scalar(
            "SELECT cursor_binding_plan_id FROM sync_cold_start_plans "
            "WHERE plan_id = %s",
            (plan_id,),
        )
        == plan_id
    )
    assert (
        schema.scalar(
            "SELECT cold_start_plan_id FROM sync_cursors "
            "WHERE account_id = 8 AND folder_key = 'INBOX'"
        )
        == plan_id
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_command_receipt_view_enforces_namespace_and_role_acl(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)

    receipt_id = uuid4()
    params = (
        receipt_id,
        8,
        "cold_start.preview",
        "1" * 64,
        "2" * 64,
        "succeeded",
        "sync_cold_start_plan",
        str(uuid4()),
        "3" * 64,
        0,
    )
    insert_sql = (
        "INSERT INTO cold_start_command_receipts ("
        "id, account_id, command_name, idempotency_key_hash, "
        "canonical_payload_hash, outcome, result_type, result_id, "
        "result_hash, authority_epoch"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
    )
    with psycopg.connect(schema.maintenance_dsn, autocommit=True) as connection:
        assert connection.execute(insert_sql, params).fetchone() == (receipt_id,)
        assert connection.execute(
            "SELECT command_name FROM cold_start_command_receipts WHERE id = %s",
            (receipt_id,),
        ).fetchone() == ("cold_start.preview",)
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                insert_sql,
                (str(uuid4()), 8, "cold_start.cancel", *params[3:]),
            )
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                insert_sql,
                (*params[:6], "actor-raw-text", *params[7:]),
            )
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                insert_sql,
                (*params[:7], "payload-raw-text", *params[8:]),
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute("SELECT id FROM pipeline_command_receipts")

    with psycopg.connect(schema.runtime_dsn, autocommit=True) as connection:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute("SELECT id FROM cold_start_command_receipts")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute("SELECT id FROM pipeline_command_receipts")

    with psycopg.connect(schema.auditor_dsn, autocommit=True) as connection:
        assert connection.execute(
            "SELECT command_name FROM pipeline_command_receipts WHERE id = %s",
            (receipt_id,),
        ).fetchone() == ("cold_start.preview",)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            connection.execute(
                "INSERT INTO pipeline_command_receipts ("
                "id, account_id, command_name, idempotency_key_hash, "
                "canonical_payload_hash, outcome, result_type, result_id, "
                "result_hash, authority_epoch"
                ") VALUES (%s, 8, 'cold_start.preview', %s, %s, "
                "'succeeded', 'sync_cold_start_plan', 'result-2', %s, 0)",
                (str(uuid4()), "4" * 64, "5" * 64, "6" * 64),
            )


def _receipt_module():
    return importlib.import_module("src.ingestion.command_receipts")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_receipt_replay_conflict_and_same_xid_rollback(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    module = _receipt_module()
    repository = module.CommandReceiptRepository(target_schema="public")
    values = _receipt_values()

    async with await psycopg.AsyncConnection.connect(
        schema.maintenance_dsn,
    ) as connection:
        async with connection.transaction():
            first = await repository.transaction(connection).insert(**values)
        replay_values = {
            **values,
            "result_id": str(uuid4()),
            "result_hash": "d" * 64,
            "authority_epoch": 7,
        }
        async with connection.transaction():
            replay = await repository.transaction(connection).insert(**replay_values)
        assert replay == first

        conflicting = {**values, "canonical_payload_hash": "c" * 64}
        async with connection.transaction():
            with pytest.raises(module.IdempotencyConflict):
                await repository.transaction(connection).insert(**conflicting)

        async with connection.transaction():
            await connection.execute(
                "INSERT INTO sync_cursors ("
                "account_id, folder_key, cursor, status, blocked_reason_code, "
                "contract_fingerprint, blocked_at, last_success_at, "
                "last_attempt_at"
                ") VALUES (8, 'rollback-folder', NULL, 'cold_start_pending', "
                "'before-fault', NULL, NULL, NULL, NULL)"
            )

        rolled_back = {**values, "idempotency_key": "rollback-command"}
        with pytest.raises(RuntimeError, match="fault_after_receipt"):
            async with connection.transaction():
                await connection.execute(
                    "UPDATE sync_cursors SET blocked_reason_code = "
                    "'rollback-marker', updated_at = CURRENT_TIMESTAMP "
                    "WHERE account_id = 8 AND folder_key = 'rollback-folder'"
                )
                await repository.transaction(connection).insert(**rolled_back)
                raise RuntimeError("fault_after_receipt")

    with psycopg.connect(schema.dsn, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT idempotency_key_hash FROM pipeline_command_receipts "
            "WHERE account_id = 8 ORDER BY created_at, id"
        ).fetchall()
        cursor_reason = connection.execute(
            "SELECT blocked_reason_code FROM sync_cursors "
            "WHERE account_id = 8 AND folder_key = 'rollback-folder'"
        ).fetchone()
    assert len(rows) == 1
    assert cursor_reason == ("before-fault",)
    expected_key_hash = hashlib.sha256(
        b"pipeline-command-idempotency-v1\x00"
        b"8\x00cold_start.preview\x00preview-command-1"
    ).hexdigest()
    assert rows == [(expected_key_hash,)]
    assert "preview-command-1" not in str(rows)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_receipt_public_lookup_miss_hit_conflict_and_no_write(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    module = _receipt_module()
    repository = module.CommandReceiptRepository(target_schema="public")
    values = _receipt_values()

    async with await psycopg.AsyncConnection.connect(
        schema.maintenance_dsn,
    ) as connection:
        async with connection.transaction():
            transaction = repository.transaction(connection)
            assert (
                await transaction.lookup(
                    account_id=values["account_id"],
                    command_name=values["command_name"],
                    idempotency_key=values["idempotency_key"],
                    canonical_payload_hash=values["canonical_payload_hash"],
                )
                is None
            )
            count_row = await (
                await connection.execute(
                    "SELECT pg_catalog.count(*) FROM cold_start_command_receipts"
                )
            ).fetchone()
            assert count_row == (0,)
            inserted = await transaction.insert(**values)

        async with connection.transaction():
            transaction = repository.transaction(connection)
            replay = await transaction.lookup(
                account_id=values["account_id"],
                command_name=values["command_name"],
                idempotency_key=values["idempotency_key"],
                canonical_payload_hash=values["canonical_payload_hash"],
            )
            assert replay == inserted
            with pytest.raises(module.IdempotencyConflict):
                await transaction.lookup(
                    account_id=values["account_id"],
                    command_name=values["command_name"],
                    idempotency_key=values["idempotency_key"],
                    canonical_payload_hash="c" * 64,
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_receipt_transaction_object_rejects_cross_xid_reuse(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    module = _receipt_module()
    repository = module.CommandReceiptRepository(target_schema="public")

    async with await psycopg.AsyncConnection.connect(
        schema.maintenance_dsn,
    ) as connection:
        async with connection.transaction():
            transaction = repository.transaction(connection)
            await transaction.insert(**_receipt_values())
        async with connection.transaction():
            with pytest.raises(RuntimeError, match="transaction_changed"):
                await transaction.lookup(
                    account_id=8,
                    command_name="cold_start.preview",
                    idempotency_key="preview-command-1",
                    canonical_payload_hash="a" * 64,
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_receipt_repository_executes_actual_sql_with_dict_rows(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    module = _receipt_module()
    repository = module.CommandReceiptRepository(target_schema="public")

    async with await psycopg.AsyncConnection.connect(
        schema.maintenance_dsn,
        row_factory=dict_row,
    ) as connection:
        async with connection.transaction():
            receipt = await repository.transaction(connection).insert(
                **_receipt_values()
            )

    assert receipt.account_id == 8
    assert receipt.command_name == "cold_start.preview"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_receipt_conflict_reads_committed_winner(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    module = _receipt_module()
    repository = module.CommandReceiptRepository(target_schema="public")
    start = asyncio.Event()

    async def write(payload_hash: str):
        async with await psycopg.AsyncConnection.connect(
            schema.maintenance_dsn,
        ) as connection:
            async with connection.transaction():
                await start.wait()
                return await repository.transaction(connection).insert(
                    **_receipt_values(canonical_payload_hash=payload_hash)
                )

    first = asyncio.create_task(write("a" * 64))
    second = asyncio.create_task(write("c" * 64))
    start.set()
    outcomes = await asyncio.gather(first, second, return_exceptions=True)

    assert sum(isinstance(item, module.CommandReceipt) for item in outcomes) == 1
    assert sum(isinstance(item, module.IdempotencyConflict) for item in outcomes) == 1
    with psycopg.connect(schema.dsn, autocommit=True) as connection:
        assert connection.execute(
            "SELECT pg_catalog.count(*) FROM pipeline_command_receipts"
        ).fetchone() == (1,)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_pre_call_lookup_waits_and_replays_committed_winner(
    postgres_database_factory,
) -> None:
    schema = postgres_database_factory()
    await _bootstrap(schema)
    module = _receipt_module()
    repository = module.CommandReceiptRepository(target_schema="public")
    lock_held = asyncio.Event()
    release_first = asyncio.Event()
    values = _receipt_values()

    async def first_call():
        async with await psycopg.AsyncConnection.connect(
            schema.maintenance_dsn,
        ) as connection:
            async with connection.transaction():
                transaction = repository.transaction(connection)
                assert (
                    await transaction.lookup(
                        account_id=values["account_id"],
                        command_name=values["command_name"],
                        idempotency_key=values["idempotency_key"],
                        canonical_payload_hash=values["canonical_payload_hash"],
                    )
                    is None
                )
                lock_held.set()
                await release_first.wait()
                return await transaction.insert(**values)

    async def second_call():
        await lock_held.wait()
        async with await psycopg.AsyncConnection.connect(
            schema.maintenance_dsn,
        ) as connection:
            async with connection.transaction():
                return await repository.transaction(connection).lookup(
                    account_id=values["account_id"],
                    command_name=values["command_name"],
                    idempotency_key=values["idempotency_key"],
                    canonical_payload_hash=values["canonical_payload_hash"],
                )

    first = asyncio.create_task(first_call())
    await lock_held.wait()
    second = asyncio.create_task(second_call())
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(asyncio.shield(second), timeout=0.1)
    release_first.set()
    winner, replay = await asyncio.gather(first, second)

    assert replay == winner
