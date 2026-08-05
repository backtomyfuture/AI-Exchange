from __future__ import annotations

import psycopg
import pytest

from src.db.schema_contract import (
    DatabaseSchemaContractError,
    _GREENFIELD_ROUTINE_PENDING_SOURCE_IDENTITIES,
    _GREENFIELD_ROUTINE_SOURCE_SHA256 as PRODUCTION_ROUTINE_SOURCE_SHA256,
    _POLLING_ONLY_ROUTINE_SOURCE_SHA256,
    require_database_schema_contract,
)


_GREENFIELD_ROUTINE_SOURCE_SHA256 = {
    (
        "greenfield_apply_email_event",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_expected_email_version bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_begin_inbox_effect",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_attempts bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_claim_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_lease_owner text, "
        "p_limit bigint, p_lease_seconds bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_drain_web_instance",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_expected_authority_epoch bigint, "
        "p_expected_capability_hash text",
    ): "f251ab3b82db21ff53e71b8520a444c54a4948e3139d2b497744f4993727c07a",
    (
        "greenfield_fail_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_attempts bigint, "
        "p_safe_error_code text, p_safe_error_summary text",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_finish_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_attempts bigint, p_completion jsonb",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_get_runtime_authority",
        "p_account_id bigint",
    ): "83c7710802ebe87c789541d335ae00b3b443099326721ae5c276b81289aa1e14",
    (
        "greenfield_heartbeat_web_instance",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_expected_authority_epoch bigint, "
        "p_expected_capability_hash text, p_accepted_count bigint, "
        "p_rejected_count bigint, p_lease_seconds bigint",
    ): "b8b514a4eefdaea8cc0b04d8e91f4abbd1c10a3e909fd22bf8b1006f598b6f6a",
    (
        "greenfield_initialize_runtime",
        "p_account_id bigint, p_capability_hash text, p_predecessor_hash text, "
        "p_capability_stage text, p_schema_revision text, "
        "p_schema_digest text, p_protocol_version bigint, "
        "p_minimum_build_id text, p_config_hash text, p_adapter_hash text, "
        "p_policy_manifest_hash text, p_evidence_manifest_hash text, "
        "p_policy_manifest_json text, p_policy_scope_count bigint, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "629db55e4a4a727c87f29ede0aa905e0852840ce330779080fa02e4316c12b7a",
    (
        "greenfield_insert_webhook_event",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_external_email_id text, "
        "p_folder_key text, p_raw_event_type text, p_change_kind text, "
        "p_dedupe_key text, p_source_version text, "
        "p_source_event_at timestamp with time zone, p_payload jsonb, "
        "p_processing_policy text",
    ): "90d1856349da36b686fbbbf9b9064b795dd819dcb2946a2731e7c7566f59f48b",
    (
        "greenfield_pause_runtime",
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "859dd32a2d4588383c0d6cb8e8be9448d9b11ced2cdf719bdc57a6bf8a6a1702",
    (
        "greenfield_reap_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_limit bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_register_web_instance",
        "p_account_id bigint, p_instance_id text, p_session_id uuid, "
        "p_expected_authority_epoch bigint, p_expected_authority_version "
        "bigint, p_schema_revision text, p_protocol_version bigint, "
        "p_build_id text, p_config_hash text, p_capability_hash text, "
        "p_lease_seconds bigint",
    ): "9ca45fe19a5fc3f071aa3ef9e4b015bfff6a5195b5c172ddb3b0a4434627a005",
    (
        "greenfield_renew_inbox",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_inbox_id uuid, "
        "p_execution_epoch bigint, p_lease_owner text, p_attempts bigint, "
        "p_lease_seconds bigint",
    ): "543100eb8abecbc7ef49f121b4b8dff28d15e13bac1ba98e6c32b10ad5bcf7a2",
    (
        "greenfield_requeue_inbox",
        "p_account_id bigint, p_inbox_id uuid, "
        "p_expected_execution_epoch bigint, p_expected_email_version bigint, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "c2212b40235a5ec862c4f775256b899b220c5ea60b1b7537cd2ad861e440fe8d",
    (
        "greenfield_resume_ingress",
        "p_account_id bigint, p_expected_authority_epoch bigint, "
        "p_expected_version bigint, p_expected_capability_hash text, "
        "p_actor text, p_reason text, p_idempotency_key text, "
        "p_canonical_payload_hash text",
    ): "9bc48d2dc92a03bd2919c7d68a134ef68c002a80a38a2d80212a1fde9af2511c",
}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fresh_0006_passes_exact_greenfield_schema_contract(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")

    await require_database_schema_contract(
        empty_schema.dsn,
        target_schema="public",
        require_complete=False,
        require_business_complete=True,
        expected_revision="20260716_0006",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fresh_0007_passes_the_extended_polling_schema_contract(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260728_0007")

    await require_database_schema_contract(
        empty_schema.dsn,
        target_schema="public",
        require_complete=False,
        require_business_complete=True,
        expected_revision="20260728_0007",
    )
    polling_identity = (
        "greenfield_commit_sync_page",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_folder_key text, "
        "p_expected_cursor text, p_expected_cursor_version bigint, "
        "p_next_cursor text, p_events jsonb, p_activation boolean",
    )
    assert set(_POLLING_ONLY_ROUTINE_SOURCE_SHA256) == {
        *PRODUCTION_ROUTINE_SOURCE_SHA256,
        polling_identity,
    }
    assert (
        _POLLING_ONLY_ROUTINE_SOURCE_SHA256[polling_identity]
        == "acbc4d4e474cb38f2f929a9327c58a8928db4ebdf8709679b42d6a34bdab292a"
    )


@pytest.mark.integration
def test_fresh_0006_pins_all_data_plane_routine_source_digests(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    assert len(_GREENFIELD_ROUTINE_SOURCE_SHA256) == 16
    assert _GREENFIELD_ROUTINE_PENDING_SOURCE_IDENTITIES == frozenset()
    assert {
        identity: PRODUCTION_ROUTINE_SOURCE_SHA256[identity]
        for identity in _GREENFIELD_ROUTINE_SOURCE_SHA256
    } == _GREENFIELD_ROUTINE_SOURCE_SHA256

    with psycopg.connect(empty_schema.dsn, autocommit=True) as connection:
        rows = connection.execute(
            "SELECT routine.proname::text, "
            "pg_catalog.pg_get_function_identity_arguments(routine.oid)::text, "
            "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to("
            "routine.prosrc, 'UTF8')), 'hex')::text "
            "FROM pg_catalog.pg_proc AS routine "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "  ON namespace.oid = routine.pronamespace "
            "WHERE namespace.nspname = current_schema() "
            "  AND routine.proname = ANY(%s::pg_catalog.text[]) "
            "ORDER BY routine.proname, "
            "pg_catalog.pg_get_function_identity_arguments(routine.oid)",
            (sorted({identity[0] for identity in _GREENFIELD_ROUTINE_SOURCE_SHA256}),),
        ).fetchall()

    assert {
        (name, identity_arguments): source_sha256
        for name, identity_arguments, source_sha256 in rows
    } == _GREENFIELD_ROUTINE_SOURCE_SHA256


@pytest.mark.integration
@pytest.mark.asyncio
async def test_0006_schema_contract_rejects_data_plane_body_drift(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    empty_schema.execute(
        """
        CREATE OR REPLACE FUNCTION greenfield_get_runtime_authority(
            p_account_id pg_catalog.int8
        )
        RETURNS TABLE (
            account_id pg_catalog.int8,
            state pg_catalog.text,
            generation pg_catalog.int8,
            fencing_token pg_catalog.int8,
            pipeline_name pg_catalog.text,
            authority_epoch pg_catalog.int8,
            version pg_catalog.int8,
            schema_revision pg_catalog.text,
            protocol_version pg_catalog.int8,
            build_id pg_catalog.text,
            config_hash pg_catalog.text,
            capability_hash pg_catalog.text,
            policy_manifest_hash pg_catalog.text,
            initialization_id pg_catalog.uuid,
            updated_at pg_catalog.timestamptz
        )
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $drifted_data_plane_body$
            SELECT
                NULL::pg_catalog.int8,
                NULL::pg_catalog.text,
                NULL::pg_catalog.int8,
                NULL::pg_catalog.int8,
                NULL::pg_catalog.text,
                NULL::pg_catalog.int8,
                NULL::pg_catalog.int8,
                NULL::pg_catalog.text,
                NULL::pg_catalog.int8,
                NULL::pg_catalog.text,
                NULL::pg_catalog.text,
                NULL::pg_catalog.text,
                NULL::pg_catalog.text,
                NULL::pg_catalog.uuid,
                NULL::pg_catalog.timestamptz
            WHERE false
        $drifted_data_plane_body$
        """
    )

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            empty_schema.dsn,
            target_schema="public",
            require_complete=False,
            require_business_complete=True,
            expected_revision="20260716_0006",
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift_sql",
    [
        "ALTER TABLE pipeline_runtime_authority ADD COLUMN hidden_state text",
        "DROP INDEX ix_pipeline_runtime_authority_state",
        "ALTER TABLE emails DROP CONSTRAINT fk_emails_processing_inbox",
        "DROP TRIGGER trg_emails_runtime_identity ON emails",
        "DROP FUNCTION greenfield_get_runtime_authority(bigint)",
        "CREATE OR REPLACE FUNCTION guard_pipeline_runtime_authority() "
        "RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog "
        "AS 'BEGIN RETURN NEW; END'",
    ],
    ids=(
        "column",
        "index",
        "foreign-key",
        "trigger",
        "routine",
        "guard-source",
    ),
)
async def test_0006_schema_contract_rejects_catalog_inventory_drift(
    alembic_runner,
    empty_schema,
    drift_sql: str,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    empty_schema.execute(drift_sql)

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            empty_schema.dsn,
            target_schema="public",
            require_complete=False,
            require_business_complete=True,
            expected_revision="20260716_0006",
        )
