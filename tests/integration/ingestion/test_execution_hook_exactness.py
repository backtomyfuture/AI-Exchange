from __future__ import annotations

import psycopg
import pytest

from src.db.bootstrap import bootstrap_database
from src.db.roles import (
    DatabaseRoleError,
    require_maintenance_database_role,
    require_migration_database_role,
    require_runtime_database_role,
)


# This manifest is deliberately test-owned.  Importing the production trigger
# manifest here would allow the role gate and its regression test to drift in
# lockstep.
EXPECTED_TRIGGER_DEFINITIONS = (
    (
        "trg_audit_events_guard_row",
        "CREATE TRIGGER trg_audit_events_guard_row BEFORE DELETE OR UPDATE "
        "ON public.audit_events FOR EACH ROW EXECUTE FUNCTION "
        "reject_audit_events_mutation()",
    ),
    (
        "trg_audit_events_guard_truncate",
        "CREATE TRIGGER trg_audit_events_guard_truncate BEFORE TRUNCATE "
        "ON public.audit_events FOR EACH STATEMENT EXECUTE FUNCTION "
        "reject_audit_events_mutation()",
    ),
    (
        "trg_emails_processing_owner",
        "CREATE CONSTRAINT TRIGGER trg_emails_processing_owner AFTER INSERT OR "
        "UPDATE ON public.emails NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW "
        "EXECUTE FUNCTION enforce_email_processing_owner()",
    ),
    (
        "trg_event_inbox_guard_update",
        "CREATE TRIGGER trg_event_inbox_guard_update BEFORE UPDATE ON "
        "public.event_inbox FOR EACH ROW EXECUTE FUNCTION "
        "guard_event_inbox_update()",
    ),
    (
        "trg_pipeline_ownership_guard_row",
        "CREATE TRIGGER trg_pipeline_ownership_guard_row BEFORE INSERT OR DELETE "
        "OR UPDATE ON public.pipeline_ownership FOR EACH ROW EXECUTE FUNCTION "
        "guard_pipeline_ownership()",
    ),
    (
        "trg_pipeline_ownership_guard_truncate",
        "CREATE TRIGGER trg_pipeline_ownership_guard_truncate BEFORE TRUNCATE "
        "ON public.pipeline_ownership FOR EACH STATEMENT EXECUTE FUNCTION "
        "guard_pipeline_ownership()",
    ),
    (
        "trg_pipeline_shadow_guard_row",
        "CREATE TRIGGER trg_pipeline_shadow_guard_row BEFORE DELETE OR UPDATE "
        "ON public.pipeline_shadow_comparisons FOR EACH ROW EXECUTE FUNCTION "
        "guard_pipeline_shadow_comparison()",
    ),
    (
        "trg_pipeline_shadow_guard_truncate",
        "CREATE TRIGGER trg_pipeline_shadow_guard_truncate BEFORE TRUNCATE "
        "ON public.pipeline_shadow_comparisons FOR EACH STATEMENT EXECUTE FUNCTION "
        "guard_pipeline_shadow_comparison()",
    ),
)


# Namespace OIDs are intentionally represented by their names in this
# independent manifest.  Every approved FK must remain wholly inside public.
EXPECTED_FOREIGN_KEYS = (
    (
        "fk_audit_events_email",
        "public",
        "audit_events",
        ("account_id", "email_id"),
        "public",
        "emails",
        ("account_id", "id"),
        "s",
        "r",
        "r",
        False,
        False,
        True,
        0,
    ),
    (
        "fk_emails_pipeline_ownership",
        "public",
        "emails",
        ("account_id", "owner_generation", "owner_fencing_token"),
        "public",
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token"),
        "f",
        "r",
        "r",
        False,
        False,
        True,
        0,
    ),
    (
        "fk_emails_processing_inbox",
        "public",
        "emails",
        (
            "processing_inbox_id",
            "account_id",
            "external_email_id",
            "owner_generation",
            "owner_fencing_token",
        ),
        "public",
        "event_inbox",
        ("id", "account_id", "external_email_id", "generation", "fencing_token"),
        "s",
        "r",
        "r",
        False,
        False,
        True,
        0,
    ),
    (
        "fk_event_inbox_pipeline_ownership",
        "public",
        "event_inbox",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "public",
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
        "r",
        "r",
        False,
        False,
        True,
        0,
    ),
    (
        "fk_pipeline_shadow_ownership",
        "public",
        "pipeline_shadow_comparisons",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "public",
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
        "r",
        "r",
        False,
        False,
        True,
        0,
    ),
)


TRIGGER_DRIFT_DDL = {
    "when_false": """
        DROP TRIGGER trg_event_inbox_guard_update ON public.event_inbox;
        CREATE TRIGGER trg_event_inbox_guard_update
        BEFORE UPDATE ON public.event_inbox
        FOR EACH ROW WHEN (false)
        EXECUTE FUNCTION public.guard_event_inbox_update()
    """,
    "arguments": """
        DROP TRIGGER trg_event_inbox_guard_update ON public.event_inbox;
        CREATE TRIGGER trg_event_inbox_guard_update
        BEFORE UPDATE ON public.event_inbox
        FOR EACH ROW
        EXECUTE FUNCTION public.guard_event_inbox_update('unexpected')
    """,
    "constraint_deferrability": """
        DROP TRIGGER trg_emails_processing_owner ON public.emails;
        CREATE CONSTRAINT TRIGGER trg_emails_processing_owner
        AFTER INSERT OR UPDATE ON public.emails
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION public.enforce_email_processing_owner()
    """,
}


def _trigger_definitions(dsn: str) -> tuple[tuple[str, str], ...]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return tuple(
            conn.execute(
                """
                SELECT trigger.tgname::pg_catalog.text,
                       pg_catalog.pg_get_triggerdef(trigger.oid, false)
                FROM pg_catalog.pg_trigger AS trigger
                JOIN pg_catalog.pg_class AS relation
                  ON relation.oid = trigger.tgrelid
                WHERE relation.relnamespace = 'public'::pg_catalog.regnamespace
                  AND NOT trigger.tgisinternal
                ORDER BY trigger.tgname
                """
            ).fetchall()
        )


def _foreign_keys(dsn: str) -> tuple[tuple[object, ...], ...]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT foreign_key.conname::pg_catalog.text,
                   child_schema.nspname::pg_catalog.text,
                   child.relname::pg_catalog.text,
                   ARRAY(
                       SELECT attribute.attname::pg_catalog.text
                       FROM pg_catalog.unnest(foreign_key.conkey)
                            WITH ORDINALITY AS key_column(attnum, position)
                       JOIN pg_catalog.pg_attribute AS attribute
                         ON attribute.attrelid = child.oid
                        AND attribute.attnum = key_column.attnum
                       ORDER BY key_column.position
                   ),
                   parent_schema.nspname::pg_catalog.text,
                   parent.relname::pg_catalog.text,
                   ARRAY(
                       SELECT attribute.attname::pg_catalog.text
                       FROM pg_catalog.unnest(foreign_key.confkey)
                            WITH ORDINALITY AS key_column(attnum, position)
                       JOIN pg_catalog.pg_attribute AS attribute
                         ON attribute.attrelid = parent.oid
                        AND attribute.attnum = key_column.attnum
                       ORDER BY key_column.position
                   ),
                   foreign_key.confmatchtype::pg_catalog.text,
                   foreign_key.confupdtype::pg_catalog.text,
                   foreign_key.confdeltype::pg_catalog.text,
                   foreign_key.condeferrable,
                   foreign_key.condeferred,
                   foreign_key.convalidated,
                   foreign_key.conparentid
            FROM pg_catalog.pg_constraint AS foreign_key
            JOIN pg_catalog.pg_class AS child
              ON child.oid = foreign_key.conrelid
            JOIN pg_catalog.pg_namespace AS child_schema
              ON child_schema.oid = child.relnamespace
            JOIN pg_catalog.pg_class AS parent
              ON parent.oid = foreign_key.confrelid
            JOIN pg_catalog.pg_namespace AS parent_schema
              ON parent_schema.oid = parent.relnamespace
            WHERE foreign_key.contype = 'f'
              AND (
                  child_schema.nspname = 'public'
                  OR parent_schema.nspname = 'public'
              )
            ORDER BY foreign_key.conname
            """
        ).fetchall()
    return tuple(
        (
            *row[:3],
            tuple(row[3]),
            *row[4:6],
            tuple(row[6]),
            *row[7:],
        )
        for row in rows
    )


async def _accepted_role_gates(schema) -> list[str]:
    accepted: list[str] = []
    for name, gate, dsn, identity in (
        (
            "runtime",
            require_runtime_database_role,
            schema.runtime_dsn,
            schema.runtime_identity,
        ),
        (
            "migration",
            require_migration_database_role,
            schema.dsn,
            schema.bootstrap_identity,
        ),
        (
            "maintenance",
            require_maintenance_database_role,
            schema.maintenance_dsn,
            schema.maintenance_identity,
        ),
    ):
        try:
            await gate(dsn, **identity)
        except DatabaseRoleError:
            continue
        accepted.append(name)
    return accepted


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fresh_0004_matches_test_owned_trigger_and_fk_manifests(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)

    assert _trigger_definitions(schema.dsn) == EXPECTED_TRIGGER_DEFINITIONS
    assert _foreign_keys(schema.dsn) == EXPECTED_FOREIGN_KEYS


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("drift", sorted(TRIGGER_DRIFT_DDL))
async def test_all_role_gates_reject_trigger_semantic_drift(
    postgres_database_factory,
    drift,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    assert _trigger_definitions(schema.dsn) == EXPECTED_TRIGGER_DEFINITIONS

    schema.execute(TRIGGER_DRIFT_DDL[drift])

    assert _trigger_definitions(schema.dsn) != EXPECTED_TRIGGER_DEFINITIONS
    assert await _accepted_role_gates(schema) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_all_role_gates_reject_cross_schema_foreign_key_substitution(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    assert _foreign_keys(schema.dsn) == EXPECTED_FOREIGN_KEYS

    schema.admin_execute(
        """
        CREATE SCHEMA fk_contract_peer;
        CREATE TABLE fk_contract_peer.pipeline_ownership (
            account_id pg_catalog.int8 NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            pipeline_name pg_catalog.text NOT NULL,
            UNIQUE (account_id, generation, fencing_token, pipeline_name)
        );
        ALTER TABLE public.event_inbox
            DROP CONSTRAINT fk_event_inbox_pipeline_ownership;
        ALTER TABLE public.event_inbox
            ADD CONSTRAINT fk_event_inbox_pipeline_ownership
            FOREIGN KEY (account_id, generation, fencing_token, pipeline_name)
            REFERENCES fk_contract_peer.pipeline_ownership
                (account_id, generation, fencing_token, pipeline_name)
            MATCH FULL
            ON UPDATE RESTRICT
            ON DELETE RESTRICT
        """
    )

    assert _foreign_keys(schema.dsn) != EXPECTED_FOREIGN_KEYS
    assert await _accepted_role_gates(schema) == []
