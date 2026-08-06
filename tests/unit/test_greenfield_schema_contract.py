from __future__ import annotations

from src.db import schema_contract


_REVISION = "20260716_0006"
_POLLING_REVISION = "20260728_0007"
_DAILY_DIGEST_REVISION = "20260805_0008"


def test_0006_manifest_covers_greenfield_relations_columns_and_expressions() -> None:
    assert set(schema_contract._PHASE2_RELATION_KINDS_BY_REVISION[_REVISION]) == {
        "audit_events",
        "cold_start_command_receipts",
        "emails",
        "event_inbox",
        "pipeline_command_receipts",
        "pipeline_folder_scopes",
        "pipeline_initializations",
        "pipeline_ownership",
        "pipeline_runtime_authority",
        "pipeline_runtime_capabilities",
        "pipeline_runtime_instances",
        "sync_cold_start_plans",
        "sync_cursors",
    }
    columns = schema_contract._PHASE2_COLUMN_TYPES_BY_REVISION[_REVISION]
    assert columns[("event_inbox", "execution_epoch")] == "int8"
    assert columns[("emails", "owner_capability_hash")] == "bpchar"
    assert columns[("pipeline_runtime_capabilities", "stage_ordinal")] == "int2"
    assert columns[("pipeline_runtime_instances", "session_id")] == "uuid"
    assert all(relation != "pipeline_shadow_comparisons" for relation, _ in columns)

    assert (
        schema_contract._GREENFIELD_DEFAULT_EXPRESSIONS[
            ("event_inbox", "execution_epoch")
        ]
        == "0"
    )
    assert schema_contract._GREENFIELD_GENERATED_EXPRESSION_SHA256 == {
        **schema_contract.PHASE2_GENERATED_EXPRESSION_SHA256_BY_REVISION[
            "20260713_0005"
        ],
        ("pipeline_initializations", "receipt_command_name"): (
            "0440b0ae6fb921bb9d055d8f1afcfed9cb26ae4f128adec283ecf17420ad5df5"
        ),
        ("pipeline_runtime_capabilities", "predecessor_stage_ordinal"): (
            "ba2ac13d05d8de3a592d785d7154d56a8f1972fb0296e015c7f1884a705c3443"
        ),
        ("pipeline_runtime_capabilities", "stage_ordinal"): (
            "3eddd7c1d28b0ed2305ac6657ecae9f665138fcabb088fa39444d977f37910c6"
        ),
    }


def test_0006_manifest_pins_constraints_indexes_and_deferred_execution_hooks() -> None:
    assert (
        schema_contract._GREENFIELD_CHECK_CONSTRAINT_SHA256[
            ("pipeline_initializations", "ck_pipeline_initializations_transaction")
        ]
        == "b450a7bdfc435c095f0effdd04fdc27e3f0d9314283ee9724b33c9ba7511c1de"
    )

    processing_fk = next(
        row
        for row in schema_contract._GREENFIELD_FOREIGN_KEYS
        if row[0] == "fk_emails_processing_inbox"
    )
    assert processing_fk[1:6] == (
        "emails",
        (
            "processing_inbox_id",
            "account_id",
            "external_email_id",
            "owner_generation",
            "owner_fencing_token",
            "processing_execution_epoch",
            "owner_authority_epoch",
            "owner_capability_hash",
        ),
        "event_inbox",
        (
            "id",
            "account_id",
            "external_email_id",
            "generation",
            "fencing_token",
            "execution_epoch",
            "authority_epoch",
            "capability_hash",
        ),
        "s",
    )
    assert processing_fk[6:] == ("a", "r", True, True, True)

    email_trigger = next(
        row
        for row in schema_contract._GREENFIELD_TRIGGERS
        if row[0] == "trg_emails_runtime_identity"
    )
    assert email_trigger[:6] == (
        "trg_emails_runtime_identity",
        "emails",
        "guard_emails_runtime_identity",
        21,
        True,
        "O",
    )
    assert email_trigger[11:13] == (True, True)

    live_index = next(
        row
        for row in schema_contract._GREENFIELD_INDEXES
        if row[1] == "uq_pipeline_runtime_instances_live_identity"
    )
    assert live_index[2] is True
    assert live_index[9] == ("account_id", "workload", "instance_id")
    assert live_index[-1] == (
        "2f952cb9388375627f98891cc0eca31021b80e760cab62e84730a898221049f2"
    )


def test_0006_manifest_pins_guard_and_data_routine_structural_identity() -> None:
    expected_routines = {
        "greenfield_apply_email_event",
        "greenfield_begin_inbox_effect",
        "greenfield_claim_inbox",
        "greenfield_drain_web_instance",
        "greenfield_fail_inbox",
        "greenfield_finish_inbox",
        "greenfield_get_runtime_authority",
        "greenfield_heartbeat_web_instance",
        "greenfield_initialize_runtime",
        "greenfield_insert_webhook_event",
        "greenfield_pause_runtime",
        "greenfield_reap_inbox",
        "greenfield_register_web_instance",
        "greenfield_renew_inbox",
        "greenfield_requeue_inbox",
        "greenfield_resume_ingress",
        "guard_emails_runtime_identity",
        "guard_event_inbox_runtime_identity",
        "guard_pipeline_ownership",
        "guard_pipeline_runtime_authority",
        "guard_pipeline_runtime_instances",
        "reject_audit_events_mutation",
        "reject_pipeline_command_receipts_mutation",
        "reject_pipeline_folder_scopes_mutation",
        "reject_pipeline_initializations_mutation",
        "reject_pipeline_runtime_capabilities_mutation",
    }
    assert {row[0] for row in schema_contract._GREENFIELD_ROUTINES} == (
        expected_routines
    )
    assert schema_contract._GREENFIELD_ROUTINE_DIGEST_IDENTITIES == {
        (row[0], row[1]) for row in schema_contract._GREENFIELD_ROUTINES
    }
    assert set(schema_contract._GREENFIELD_ROUTINE_SOURCE_SHA256) <= (
        schema_contract._GREENFIELD_ROUTINE_DIGEST_IDENTITIES
    )
    assert all(
        name.startswith("greenfield_")
        for name, _identity_arguments in (
            schema_contract._GREENFIELD_ROUTINE_PENDING_SOURCE_IDENTITIES
        )
    )

    for routine in schema_contract._GREENFIELD_ROUTINES:
        assert routine[4] == "f"
        assert routine[12] in {
            ("search_path=pg_catalog",),
            ("search_path=public",),
        }
        if routine[0].startswith("greenfield_"):
            assert routine[5] is True
        else:
            assert routine[2] == "trigger"
            assert routine[5] is False


def test_0007_manifest_adds_only_the_fenced_sync_page_routine() -> None:
    assert schema_contract._PHASE2_RELATION_KINDS_BY_REVISION[_POLLING_REVISION] == (
        schema_contract._PHASE2_RELATION_KINDS_BY_REVISION[_REVISION]
    )
    assert schema_contract._PHASE2_COLUMN_TYPES_BY_REVISION[_POLLING_REVISION] == (
        schema_contract._PHASE2_COLUMN_TYPES_BY_REVISION[_REVISION]
    )
    added = schema_contract._POLLING_ONLY_ROUTINES - schema_contract._GREENFIELD_ROUTINES
    assert len(added) == 1
    routine = next(iter(added))
    assert routine[:3] == (
        "greenfield_commit_sync_page",
        "p_account_id bigint, p_session_id uuid, "
        "p_expected_lease_version bigint, p_folder_key text, "
        "p_expected_cursor text, p_expected_cursor_version bigint, "
        "p_next_cursor text, p_events jsonb, p_activation boolean",
        "TABLE(committed_cursor text, committed_version bigint, "
        "inserted_count bigint, duplicate_count bigint)",
    )
    assert schema_contract._POLLING_ONLY_ROUTINE_SOURCE_SHA256[
        (routine[0], routine[1])
    ] == "acbc4d4e474cb38f2f929a9327c58a8928db4ebdf8709679b42d6a34bdab292a"


def test_0008_manifest_adds_only_the_durable_daily_digest_execution_record() -> None:
    relations = schema_contract._PHASE2_RELATION_KINDS_BY_REVISION[
        _DAILY_DIGEST_REVISION
    ]
    columns = schema_contract._PHASE2_COLUMN_TYPES_BY_REVISION[
        _DAILY_DIGEST_REVISION
    ]

    assert relations == {
        **schema_contract._PHASE2_RELATION_KINDS_BY_REVISION[_POLLING_REVISION],
        "daily_digest_executions": "r",
    }
    assert columns[("daily_digest_executions", "delivery_parts")] == "jsonb"
    assert columns[("daily_digest_executions", "delivery_scope_hash")] == "bpchar"
    assert (
        "daily_digest_executions",
        "missed_reported_at",
    ) in schema_contract._PHASE2_NULLABLE_COLUMNS_BY_REVISION[
        _DAILY_DIGEST_REVISION
    ]
    assert schema_contract._DAILY_DIGEST_DEFAULT_EXPRESSIONS[
        ("daily_digest_executions", "attempt_count")
    ] == "0"
    assert any(
        row[0:4] == (
            "daily_digest_executions",
            "pk_daily_digest_executions",
            "pk_daily_digest_executions",
            "p",
        )
        for row in schema_contract._DAILY_DIGEST_UNIQUE_CONSTRAINTS
    )
