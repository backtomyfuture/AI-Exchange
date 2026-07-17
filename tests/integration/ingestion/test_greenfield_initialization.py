from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy.exc import DBAPIError

from src.ingestion.models import (
    ChangeKind,
    IngressSource,
    ProcessingPolicy,
)
from src.ingestion.policy import FolderScope, PolicySnapshot
from src.ingestion.runtime_authority import (
    RuntimeAuthority,
    RuntimeAuthorityState,
    RuntimeContract,
    canonical_authority_transition_payload,
    canonical_initialization_payload,
    canonical_policy_manifest,
)
from src.ingestion.recovery import (
    POSTGRES_BIGINT_MAX,
    RequeueCommand,
    canonical_requeue_payload_hash,
)
from src.ingestion.runtime_capability import (
    CAPABILITY_CHAIN_ROOT_HASH,
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
)


_NEW_TABLES = {
    "pipeline_folder_scopes",
    "pipeline_initializations",
    "pipeline_runtime_authority",
    "pipeline_runtime_capabilities",
    "pipeline_runtime_instances",
}
_NEW_GUARD_FUNCTIONS = {
    "guard_emails_runtime_identity",
    "guard_event_inbox_runtime_identity",
    "guard_pipeline_runtime_authority",
    "guard_pipeline_runtime_instances",
    "reject_pipeline_folder_scopes_mutation",
    "reject_pipeline_initializations_mutation",
    "reject_pipeline_runtime_capabilities_mutation",
}
_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_HASH_D = "d" * 64
_HASH_E = "e" * 64


def _policy_snapshot() -> PolicySnapshot:
    matrix = {
        (IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE): (
            ProcessingPolicy.FULL
        ),
        (IngressSource.WEBHOOK, "CreatedEvent", ChangeKind.CREATE): (
            ProcessingPolicy.IGNORED
        ),
        (IngressSource.WEBHOOK, "ModifiedEvent", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.WEBHOOK, "DeletedEvent", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "create", ChangeKind.CREATE): ProcessingPolicy.FULL,
        (IngressSource.SYNC, "update", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "delete", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
    }
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("inbox-alias", "inbox-id"),
                sync_folder="Inbox",
                event_policy_matrix=matrix,
            ),
        )
    )


def _greenfield_initialization_args(
    *,
    account_id: int = 8,
    actor: str = "测试操作员",
    reason: str = "全新系统初始化",
    idempotency_key: str = "initialize-8",
    policy_snapshot: PolicySnapshot | None = None,
) -> tuple[object, ...]:
    policy = canonical_policy_manifest(policy_snapshot or _policy_snapshot())
    capability = _phase2_capability(policy.hash)
    contract = RuntimeContract(
        schema_revision=capability.schema_revision,
        schema_digest=capability.schema_digest,
        protocol_version=capability.protocol_version,
        build_id=capability.minimum_build_id,
        config_hash=capability.config_hash,
        capability_manifest=capability,
    )
    _, payload_hash = canonical_initialization_payload(
        account_id=account_id,
        runtime_contract=contract,
        policy_manifest=policy,
        actor=actor,
        reason=reason,
    )
    return (
        account_id,
        capability.capability_hash,
        capability.predecessor_hash,
        capability.stage.value,
        capability.schema_revision,
        capability.schema_digest,
        capability.protocol_version,
        capability.minimum_build_id,
        capability.config_hash,
        capability.adapter_hash,
        policy.hash,
        capability.evidence_manifest_hash,
        policy.canonical_json,
        policy.scope_count,
        actor,
        reason,
        idempotency_key,
        payload_hash,
    )


def _remint_initialization_args_for_raw_policy(
    policy_json: str,
) -> tuple[object, ...]:
    policy_hash = hashlib.sha256(
        b"ai-exchange-folder-policy-manifest-v1\x00" + policy_json.encode("utf-8")
    ).hexdigest()
    capability = _phase2_capability(policy_hash)
    actor = "攻击模拟员"
    reason = "尝试铸造非规范策略"
    canonical_payload = (
        '{"account_id":8,"actor":'
        + json.dumps(actor, ensure_ascii=False, separators=(",", ":"))
        + ',"capability_hash":"'
        + capability.capability_hash
        + '","pipeline_name":"durable_v1","policy_manifest":'
        + policy_json
        + ',"policy_manifest_hash":"'
        + policy_hash
        + '","reason":'
        + json.dumps(reason, ensure_ascii=False, separators=(",", ":"))
        + ',"runtime_contract":{"build_id":"build-1","config_hash":"'
        + _HASH_B
        + '","protocol_version":1,"schema_digest":"'
        + _HASH_A
        + '","schema_revision":"20260716_0006"},"schema_version":1}'
    )
    payload_hash = hashlib.sha256(
        b"ai-exchange-greenfield-initialize-v1\x00" + canonical_payload.encode("utf-8")
    ).hexdigest()
    return (
        8,
        capability.capability_hash,
        capability.predecessor_hash,
        capability.stage.value,
        capability.schema_revision,
        capability.schema_digest,
        capability.protocol_version,
        capability.minimum_build_id,
        capability.config_hash,
        capability.adapter_hash,
        policy_hash,
        capability.evidence_manifest_hash,
        policy_json,
        len(json.loads(policy_json)["scopes"]),
        actor,
        reason,
        "forged-policy",
        payload_hash,
    )


def _call_greenfield_function(
    schema,
    function_name: str,
    arguments: tuple[object, ...],
) -> tuple[object, ...]:
    placeholders = ", ".join(["%s"] * len(arguments))
    with psycopg.connect(schema.dsn, autocommit=True) as connection:
        row = connection.execute(
            f"SELECT * FROM public.{function_name}({placeholders})",
            arguments,
        ).fetchone()
    assert row is not None
    return tuple(row)


def _authority_from_row(row: tuple[object, ...]) -> RuntimeAuthority:
    return RuntimeAuthority(
        account_id=row[0],
        state=row[1],
        generation=row[2],
        fencing_token=row[3],
        pipeline_name=row[4],
        authority_epoch=row[5],
        version=row[6],
        schema_revision=row[7],
        protocol_version=row[8],
        build_id=row[9],
        config_hash=row[10],
        capability_hash=row[11],
        policy_manifest_hash=row[12],
        initialization_id=str(row[13]),
        updated_at=row[14],
    )


def _authority_transition_args(
    authority: RuntimeAuthority,
    *,
    target_state: RuntimeAuthorityState,
    actor: str,
    reason: str,
    idempotency_key: str,
) -> tuple[object, ...]:
    _, payload_hash = canonical_authority_transition_payload(
        authority=authority,
        target_state=target_state,
        actor=actor,
        reason=reason,
    )
    return (
        authority.account_id,
        authority.authority_epoch,
        authority.version,
        authority.capability_hash,
        actor,
        reason,
        idempotency_key,
        payload_hash,
    )


def _routine_exists(schema, routine_name: str) -> bool:
    return bool(
        schema.scalar(
            "SELECT EXISTS ("
            "SELECT 1 FROM pg_catalog.pg_proc AS routine "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "  ON namespace.oid = routine.pronamespace "
            "WHERE namespace.nspname = current_schema() "
            "  AND routine.proname = %s"
            ")",
            (routine_name,),
        )
    )


def _constraint_definition(schema, constraint_name: str) -> str:
    return schema.scalar(
        "SELECT pg_catalog.pg_get_constraintdef(con.oid, true) "
        "FROM pg_catalog.pg_constraint AS con "
        "JOIN pg_catalog.pg_namespace AS namespace "
        "  ON namespace.oid = con.connamespace "
        "WHERE namespace.nspname = current_schema() "
        "  AND con.conname = %s",
        (constraint_name,),
    )


def _phase2_capability(policy_manifest_hash: str) -> RuntimeCapabilityManifest:
    return RuntimeCapabilityManifest(
        stage=RuntimeCapabilityStage.PHASE2_INGESTION,
        schema_revision="20260716_0006",
        schema_digest=_HASH_A,
        protocol_version=1,
        minimum_build_id="build-1",
        config_hash=_HASH_B,
        adapter_hash=_HASH_C,
        policy_manifest_hash=policy_manifest_hash,
        evidence_manifest_hash=_HASH_D,
        predecessor_hash=CAPABILITY_CHAIN_ROOT_HASH,
    )


def _insert_phase2_capability(schema, *, policy_manifest_hash: str) -> str:
    capability = _phase2_capability(policy_manifest_hash)
    schema.execute(
        "INSERT INTO pipeline_runtime_capabilities ("
        "capability_hash, predecessor_hash, stage, schema_revision, "
        "schema_digest, protocol_version, minimum_build_id, config_hash, "
        "adapter_hash, policy_manifest_hash, evidence_manifest_hash"
        ") VALUES ("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        ")",
        (
            capability.capability_hash,
            capability.predecessor_hash,
            capability.stage.value,
            capability.schema_revision,
            capability.schema_digest,
            capability.protocol_version,
            capability.minimum_build_id,
            capability.config_hash,
            capability.adapter_hash,
            capability.policy_manifest_hash,
            capability.evidence_manifest_hash,
        ),
    )
    return capability.capability_hash


def _policy_matrix() -> dict[str, str]:
    return {
        "sync:create:create": "full",
        "sync:delete:delete": "metadata_only",
        "sync:update:update": "metadata_only",
        "webhook:CreatedEvent:create": "ignored",
        "webhook:DeletedEvent:delete": "metadata_only",
        "webhook:ModifiedEvent:update": "metadata_only",
        "webhook:NewMailEvent:create": "full",
    }


def _insert_manual_review_pair(
    schema,
    *,
    capability_hash: str,
    event_id: str,
    email_id: str,
    external_email_id: str,
    dedupe_key: str,
    inbox_processing_started: bool = True,
    email_create_seen: bool = True,
    email_processing_started: bool = True,
) -> None:
    schema.execute(
        "INSERT INTO event_inbox ("
        "id, account_id, external_email_id, folder_key, source, "
        "raw_event_type, change_kind, dedupe_key, payload, processing_policy, "
        "pipeline_name, generation, fencing_token, authority_epoch, "
        "capability_hash, status, attempts, processing_started_at, "
        "safe_error_code"
        ") VALUES (%s, 8, %s, 'INBOX', 'webhook', 'NewMailEvent', 'create', "
        "%s, '{}'::jsonb, 'full', 'durable_v1', 1, 1, 1, %s, "
        "'manual_review', 1, CASE WHEN %s THEN CURRENT_TIMESTAMP END, "
        "'manual.review')",
        (
            event_id,
            external_email_id,
            dedupe_key,
            capability_hash,
            inbox_processing_started,
        ),
    )
    schema.execute(
        "INSERT INTO emails ("
        "id, account_id, external_email_id, source_folder_key, status, version, "
        "owner_generation, owner_fencing_token, owner_authority_epoch, "
        "owner_capability_hash, processing_inbox_id, "
        "processing_execution_epoch, create_seen_at, processing_started_at, "
        "safe_error_code, is_read"
        ") VALUES (%s, 8, %s, 'INBOX', 'manual_review', 1, 1, 1, 1, %s, "
        "%s, 0, CASE WHEN %s THEN CURRENT_TIMESTAMP END, "
        "CASE WHEN %s THEN CURRENT_TIMESTAMP END, 'manual.review', false)",
        (
            email_id,
            external_email_id,
            capability_hash,
            event_id,
            email_create_seen,
            email_processing_started,
        ),
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "populate",
    [
        ("INSERT INTO emails_log (id) VALUES ('legacy-sentinel')",),
        ("INSERT INTO app_kv_store (key, value) VALUES ('sentinel', 'value')",),
        (
            "INSERT INTO pipeline_ownership ("
            "account_id, generation, pipeline_name, state, fencing_token, "
            "created_by, reason"
            ") VALUES (8, 1, 'durable_v1', 'current_ingress', 1, "
            "'test-operator', 'nonempty sentinel')",
        ),
        (
            "INSERT INTO pipeline_ownership ("
            "account_id, generation, pipeline_name, state, fencing_token, "
            "created_by, reason"
            ") VALUES (8, 1, 'durable_v1', 'current_ingress', 1, "
            "'test-operator', 'shadow sentinel')",
            "INSERT INTO pipeline_shadow_comparisons ("
            "id, account_id, generation, fencing_token, pipeline_name, "
            "candidate_pipeline_name, candidate_build_id, "
            "candidate_config_hash, event_key, input_hash, legacy_status, "
            "shadow_status, comparison_status"
            ") VALUES ("
            "'00000000-0000-4000-8000-000000000001', 8, 1, 1, "
            "'durable_v1', 'candidate_v1', 'build-1', "
            f"'{_HASH_A}', '{_HASH_B}', '{_HASH_C}', "
            "'pending', 'pending', 'pending'"
            ")",
        ),
    ],
    ids=["legacy-email", "legacy-kv", "governed-ownership", "shadow-history"],
)
def test_0006_refuses_nonempty_greenfield_sources_atomically(
    alembic_runner,
    empty_schema,
    populate: tuple[str, ...],
) -> None:
    alembic_runner.upgrade(empty_schema, "20260713_0005")
    for statement in populate:
        empty_schema.execute(statement)

    with pytest.raises(DBAPIError, match="greenfield_reinitialize_required"):
        alembic_runner.upgrade(empty_schema, "20260716_0006")

    assert empty_schema.scalar("SELECT version_num FROM alembic_version") == (
        "20260713_0005"
    )
    assert empty_schema.table_exists("pipeline_shadow_comparisons")
    assert _routine_exists(empty_schema, "guard_pipeline_shadow_comparison")
    assert not empty_schema.column_exists("event_inbox", "execution_epoch")
    assert not empty_schema.column_exists("emails", "owner_authority_epoch")
    assert all(not empty_schema.table_exists(name) for name in _NEW_TABLES)


@pytest.mark.integration
def test_0006_late_ddl_failure_rolls_back_to_exact_0005_shape(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260713_0005")
    empty_schema.execute(
        "CREATE INDEX ix_pipeline_runtime_instances_lease ON emails(account_id)"
    )

    with pytest.raises(DBAPIError):
        alembic_runner.upgrade(empty_schema, "20260716_0006")

    assert empty_schema.scalar("SELECT version_num FROM alembic_version") == (
        "20260713_0005"
    )
    assert empty_schema.table_exists("pipeline_shadow_comparisons")
    assert _routine_exists(empty_schema, "guard_pipeline_shadow_comparison")
    assert not empty_schema.column_exists("event_inbox", "execution_epoch")
    assert not empty_schema.column_exists("emails", "owner_authority_epoch")
    assert all(not empty_schema.table_exists(name) for name in _NEW_TABLES)
    assert empty_schema.scalar(
        "SELECT to_regclass('ix_pipeline_runtime_instances_lease') IS NOT NULL"
    )


@pytest.mark.integration
def test_0006_access_exclusive_lock_serializes_a_concurrent_legacy_writer(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260713_0005")

    with psycopg.connect(empty_schema.dsn) as writer:
        writer.execute(
            "INSERT INTO app_kv_store (key, value) "
            "VALUES ('concurrent-sentinel', 'value')"
        )
        writer_pid = writer.info.backend_pid

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                alembic_runner.upgrade,
                empty_schema,
                "20260716_0006",
            )
            deadline = time.monotonic() + 5
            observed_lock_wait = False
            while time.monotonic() < deadline:
                with psycopg.connect(
                    empty_schema.admin_dsn,
                    autocommit=True,
                ) as observer:
                    observed_lock_wait = bool(
                        observer.execute(
                            "SELECT EXISTS ("
                            "SELECT 1 FROM pg_catalog.pg_stat_activity "
                            "WHERE datname = %s "
                            "  AND pid <> %s "
                            "  AND wait_event_type = 'Lock' "
                            "  AND query LIKE '%%LOCK TABLE%%'"
                            ")",
                            (empty_schema.database_name, writer_pid),
                        ).fetchone()[0]
                    )
                if observed_lock_wait:
                    break
                if future.done():
                    break
                time.sleep(0.02)

            completed_before_release = future.done()
            writer.commit()
            assert observed_lock_wait
            assert not completed_before_release
            with pytest.raises(DBAPIError, match="greenfield_reinitialize_required"):
                future.result(timeout=5)

    assert empty_schema.scalar("SELECT version_num FROM alembic_version") == (
        "20260713_0005"
    )
    assert not empty_schema.column_exists("event_inbox", "execution_epoch")
    assert all(not empty_schema.table_exists(name) for name in _NEW_TABLES)


@pytest.mark.integration
def test_0006_creates_exact_greenfield_authority_shape(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")

    assert empty_schema.scalar("SELECT version_num FROM alembic_version") == (
        "20260716_0006"
    )
    assert all(empty_schema.table_exists(name) for name in _NEW_TABLES)
    assert not empty_schema.table_exists("pipeline_shadow_comparisons")
    assert not _routine_exists(empty_schema, "guard_pipeline_shadow_comparison")

    required_columns = {
        "event_inbox": {
            "execution_epoch",
            "authority_epoch",
            "capability_hash",
            "lease_session_id",
        },
        "emails": {
            "owner_authority_epoch",
            "owner_capability_hash",
            "processing_execution_epoch",
        },
        "pipeline_runtime_capabilities": {
            "capability_hash",
            "predecessor_hash",
            "stage",
            "stage_ordinal",
            "predecessor_stage_ordinal",
            "schema_revision",
            "schema_digest",
            "protocol_version",
            "minimum_build_id",
            "config_hash",
            "adapter_hash",
            "policy_manifest_hash",
            "evidence_manifest_hash",
        },
        "pipeline_initializations": {
            "initialization_id",
            "command_receipt_id",
            "account_id",
            "generation",
            "fencing_token",
            "pipeline_name",
            "authority_epoch",
            "authority_version",
            "capability_hash",
            "policy_manifest_hash",
            "transaction_id",
            "actor",
            "reason",
        },
        "pipeline_folder_scopes": {
            "initialization_id",
            "account_id",
            "canonical_key",
            "webhook_ids",
            "sync_folder",
            "event_policy_matrix",
            "scope_hash",
            "policy_manifest_hash",
        },
        "pipeline_runtime_authority": {
            "account_id",
            "state",
            "generation",
            "fencing_token",
            "pipeline_name",
            "authority_epoch",
            "version",
            "schema_revision",
            "protocol_version",
            "build_id",
            "config_hash",
            "capability_hash",
            "policy_manifest_hash",
            "initialization_id",
        },
        "pipeline_runtime_instances": {
            "account_id",
            "workload",
            "instance_id",
            "session_id",
            "generation",
            "fencing_token",
            "authority_epoch",
            "capability_hash",
            "schema_revision",
            "protocol_version",
            "build_id",
            "config_hash",
            "lifecycle",
            "lease_version",
            "accepted_count",
            "rejected_count",
            "heartbeat_at",
            "lease_until",
        },
    }
    for relation_name, expected in required_columns.items():
        actual = set(
            empty_schema.scalar(
                "SELECT array_agg(column_name::text ORDER BY ordinal_position) "
                "FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = %s",
                (relation_name,),
            )
        )
        assert expected <= actual

    processing_identity = _constraint_definition(
        empty_schema,
        "uq_event_inbox_processing_identity",
    )
    assert all(
        name in processing_identity
        for name in ("execution_epoch", "authority_epoch", "capability_hash")
    )
    inbox_fk = _constraint_definition(
        empty_schema,
        "fk_emails_processing_inbox",
    )
    assert all(
        name in inbox_fk
        for name in ("processing_execution_epoch", "owner_authority_epoch")
    )
    assert "owner_capability_hash" in inbox_fk
    assert "ON UPDATE RESTRICT" not in inbox_fk
    assert "DEFERRABLE INITIALLY DEFERRED" in inbox_fk
    assert (
        empty_schema.scalar(
            "SELECT con.confupdtype::text "
            "FROM pg_catalog.pg_constraint AS con "
            "JOIN pg_catalog.pg_namespace AS namespace "
            "  ON namespace.oid = con.connamespace "
            "WHERE namespace.nspname = current_schema() "
            "  AND con.conname = 'fk_emails_processing_inbox'"
        )
        == "a"
    )
    for intentionally_absent in (
        "fk_event_inbox_runtime_authority",
        "fk_emails_runtime_authority",
        "fk_pipeline_runtime_instances_authority",
    ):
        assert _constraint_definition(empty_schema, intentionally_absent) is None
    lease_session_fk = _constraint_definition(
        empty_schema,
        "fk_event_inbox_lease_session",
    )
    assert all(
        column in lease_session_fk
        for column in (
            "lease_session_id",
            "authority_epoch",
            "capability_hash",
        )
    )
    email_trigger_timing = empty_schema.scalar(
        "SELECT jsonb_build_object("
        "  'deferrable', trigger.tgdeferrable, "
        "  'initially_deferred', trigger.tginitdeferred"
        ") "
        "FROM pg_catalog.pg_trigger AS trigger "
        "JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid "
        "JOIN pg_catalog.pg_namespace AS namespace "
        "  ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = current_schema() "
        "  AND trigger.tgname = 'trg_emails_runtime_identity'"
    )
    assert email_trigger_timing == {
        "deferrable": True,
        "initially_deferred": True,
    }

    event_defaults = empty_schema.scalar(
        "SELECT jsonb_object_agg(column_name, column_default) "
        "FROM information_schema.columns "
        "WHERE table_schema = current_schema() "
        "  AND table_name = 'event_inbox' "
        "  AND column_name IN ("
        "    'execution_epoch', 'authority_epoch', 'capability_hash', "
        "    'lease_session_id'"
        "  )"
    )
    assert event_defaults == {
        "execution_epoch": "0",
        "authority_epoch": None,
        "capability_hash": None,
        "lease_session_id": None,
    }

    root_constraint = _constraint_definition(
        empty_schema,
        "ck_pipeline_runtime_capabilities_predecessor",
    )
    assert CAPABILITY_CHAIN_ROOT_HASH in root_constraint
    assert CAPABILITY_CHAIN_ROOT_HASH == (
        "95771c6d473119376654d5530f7fe189c77d83e56fe08e91179f48b1040df86f"
    )

    authority_state = _constraint_definition(
        empty_schema,
        "ck_pipeline_runtime_authority_state",
    )
    assert all(
        state in authority_state for state in ("ingest_only", "paused", "active")
    )
    assert "phase4_graph_projection" not in authority_state

    routines = empty_schema.scalar(
        "SELECT jsonb_object_agg(routine.proname, jsonb_build_object("
        "  'security_definer', routine.prosecdef, "
        "  'config', COALESCE(to_jsonb(routine.proconfig), '[]'::jsonb), "
        "  'public_execute', has_function_privilege('public', routine.oid, 'EXECUTE')"
        ")) "
        "FROM pg_catalog.pg_proc AS routine "
        "JOIN pg_catalog.pg_namespace AS namespace "
        "  ON namespace.oid = routine.pronamespace "
        "WHERE namespace.nspname = current_schema() "
        "  AND routine.proname = ANY(%s)",
        (list(sorted(_NEW_GUARD_FUNCTIONS)),),
    )
    assert set(routines) == _NEW_GUARD_FUNCTIONS
    for contract in routines.values():
        assert contract["security_definer"] is False
        assert contract["public_execute"] is False
        assert contract["config"] == ["search_path=pg_catalog"]

    forbidden_transition_routines = empty_schema.scalar(
        "SELECT count(*) FROM pg_catalog.pg_proc AS routine "
        "JOIN pg_catalog.pg_namespace AS namespace "
        "  ON namespace.oid = routine.pronamespace "
        "WHERE namespace.nspname = current_schema() "
        "  AND routine.proname IN ("
        "    'activate_runtime_authority', 'pause_runtime_authority', "
        "    'resume_runtime_ingress', 'initialize_greenfield_runtime'"
        "  )"
    )
    assert forbidden_transition_routines == 0

    transaction_constraint = _constraint_definition(
        empty_schema,
        "ck_pipeline_initializations_transaction",
    )
    assert "^[1-9][0-9]{0,19}$" in transaction_constraint


@pytest.mark.integration
def test_0006_accepts_one_exact_phase2_manifest_and_rejects_drift(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    policy_manifest_hash = _HASH_E
    capability_hash = _insert_phase2_capability(
        empty_schema,
        policy_manifest_hash=policy_manifest_hash,
    )
    initialization_id = str(uuid4())
    command_receipt_id = str(uuid4())
    session_id = str(uuid4())
    inbox_id = str(uuid4())
    email_id = str(uuid4())

    empty_schema.execute(
        "INSERT INTO pipeline_ownership ("
        "account_id, generation, pipeline_name, state, fencing_token, "
        "created_by, reason"
        ") VALUES (8, 1, 'durable_v1', 'current_ingress', 1, "
        "'operator-1', 'greenfield initialization')"
    )
    empty_schema.execute(
        "INSERT INTO pipeline_command_receipts ("
        "id, account_id, command_name, idempotency_key_hash, "
        "canonical_payload_hash, outcome, result_type, result_id, "
        "result_hash, authority_epoch"
        ") VALUES (%s, 8, 'runtime.initialize', %s, %s, 'succeeded', "
        "'runtime_initialization', %s, %s, 1)",
        (
            command_receipt_id,
            _HASH_A,
            _HASH_B,
            initialization_id,
            _HASH_C,
        ),
    )
    empty_schema.execute(
        "INSERT INTO pipeline_initializations ("
        "initialization_id, command_receipt_id, account_id, generation, "
        "fencing_token, pipeline_name, authority_epoch, authority_version, "
        "capability_hash, policy_manifest_hash, transaction_id, actor, reason"
        ") VALUES (%s, %s, 8, 1, 1, 'durable_v1', 1, 1, %s, %s, "
        "'12345', 'operator-1', 'greenfield initialization')",
        (
            initialization_id,
            command_receipt_id,
            capability_hash,
            policy_manifest_hash,
        ),
    )
    empty_schema.execute(
        "INSERT INTO pipeline_folder_scopes ("
        "initialization_id, account_id, canonical_key, webhook_ids, "
        "sync_folder, event_policy_matrix, scope_hash, policy_manifest_hash"
        ") VALUES (%s, 8, 'INBOX', %s::jsonb, 'Inbox', %s::jsonb, %s, %s)",
        (
            initialization_id,
            '["inbox-id"]',
            psycopg.types.json.Jsonb(_policy_matrix()),
            _HASH_D,
            policy_manifest_hash,
        ),
    )
    empty_schema.execute(
        "INSERT INTO pipeline_runtime_authority ("
        "account_id, state, generation, fencing_token, pipeline_name, "
        "authority_epoch, version, schema_revision, protocol_version, "
        "build_id, config_hash, capability_hash, policy_manifest_hash, "
        "initialization_id"
        ") VALUES (8, 'ingest_only', 1, 1, 'durable_v1', 1, 1, "
        "'20260716_0006', 1, 'build-1', %s, %s, %s, %s)",
        (
            _HASH_B,
            capability_hash,
            policy_manifest_hash,
            initialization_id,
        ),
    )

    with pytest.raises(psycopg.errors.CheckViolation):
        empty_schema.execute(
            "UPDATE pipeline_runtime_authority "
            "SET state = 'active', authority_epoch = 2, version = 2"
        )

    empty_schema.execute(
        "INSERT INTO pipeline_runtime_instances ("
        "account_id, workload, instance_id, session_id, generation, "
        "fencing_token, authority_epoch, capability_hash, schema_revision, "
        "protocol_version, build_id, config_hash, lifecycle, lease_version, "
        "accepted_count, rejected_count, heartbeat_at, lease_until"
        ") VALUES (8, 'web', 'web-1', %s, 1, 1, 1, %s, "
        "'20260716_0006', 1, 'build-1', %s, 'active', 1, 0, 0, "
        "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '30 seconds')",
        (session_id, capability_hash, _HASH_B),
    )
    empty_schema.execute(
        "INSERT INTO event_inbox ("
        "id, account_id, external_email_id, folder_key, source, "
        "raw_event_type, change_kind, dedupe_key, payload, processing_policy, "
        "pipeline_name, generation, fencing_token, authority_epoch, "
        "capability_hash, status"
        ") VALUES (%s, 8, 'mail-1', 'INBOX', 'webhook', 'NewMailEvent', "
        "'create', %s, '{}'::jsonb, 'full', 'durable_v1', 1, 1, 1, %s, "
        "'pending')",
        (inbox_id, _HASH_A, capability_hash),
    )
    empty_schema.execute(
        "INSERT INTO emails ("
        "id, account_id, external_email_id, source_folder_key, status, "
        "owner_generation, owner_fencing_token, owner_authority_epoch, "
        "owner_capability_hash, is_read"
        ") VALUES (%s, 8, 'mail-1', 'INBOX', 'ingested', 1, 1, 1, %s, false)",
        (email_id, capability_hash),
    )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        empty_schema.execute(
            "UPDATE event_inbox SET status = 'leased', attempts = 1, "
            "lease_owner = 'worker-1', lease_session_id = %s, "
            "lease_until = CURRENT_TIMESTAMP + INTERVAL '30 seconds', "
            "processing_started_at = CURRENT_TIMESTAMP WHERE id = %s",
            (str(uuid4()), inbox_id),
        )

    empty_schema.execute(
        "UPDATE event_inbox SET status = 'leased', attempts = 1, "
        "lease_owner = 'worker-1', lease_session_id = %s, "
        "lease_until = CURRENT_TIMESTAMP + INTERVAL '30 seconds', "
        "processing_started_at = CURRENT_TIMESTAMP WHERE id = %s",
        (session_id, inbox_id),
    )
    empty_schema.execute(
        "UPDATE emails SET status = 'processing', processing_inbox_id = %s, "
        "processing_execution_epoch = 0, processing_started_at = "
        "CURRENT_TIMESTAMP WHERE id = %s",
        (inbox_id, email_id),
    )
    empty_schema.execute(
        "UPDATE emails SET status = 'no_action', processing_inbox_id = NULL, "
        "processing_execution_epoch = NULL WHERE id = %s",
        (email_id,),
    )
    assert empty_schema.scalar(
        "SELECT processing_inbox_id IS NULL "
        "AND processing_execution_epoch IS NULL FROM emails WHERE id = %s",
        (email_id,),
    )

    empty_schema.execute(
        "UPDATE pipeline_runtime_authority "
        "SET state = 'paused', authority_epoch = 2, version = 2, "
        "updated_at = clock_timestamp() WHERE account_id = 8"
    )
    assert empty_schema.scalar(
        "SELECT state = 'paused' AND authority_epoch = 2 "
        "FROM pipeline_runtime_authority WHERE account_id = 8"
    )
    assert empty_schema.scalar(
        "SELECT authority_epoch = 1 AND capability_hash = %s "
        "FROM pipeline_runtime_instances WHERE session_id = %s",
        (capability_hash, session_id),
    )
    assert empty_schema.scalar(
        "SELECT authority_epoch = 1 AND capability_hash = %s "
        "FROM event_inbox WHERE id = %s",
        (capability_hash, inbox_id),
    )
    assert empty_schema.scalar(
        "SELECT owner_authority_epoch = 1 AND owner_capability_hash = %s "
        "FROM emails WHERE id = %s",
        (capability_hash, email_id),
    )

    assert empty_schema.scalar("SELECT execution_epoch FROM event_inbox") == 0
    assert (
        empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_capabilities") == 1
    )
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_authority") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_instances") == 1

    with pytest.raises(psycopg.errors.CheckViolation):
        empty_schema.execute(
            "INSERT INTO pipeline_runtime_capabilities ("
            "capability_hash, predecessor_hash, stage, schema_revision, "
            "schema_digest, protocol_version, minimum_build_id, config_hash, "
            "adapter_hash, policy_manifest_hash, evidence_manifest_hash"
            ") VALUES (%s, %s, 'phase2_ingestion', '20260716_0006', %s, 1, "
            "'build-1', %s, %s, %s, %s)",
            (_HASH_E, _HASH_A, _HASH_A, _HASH_B, _HASH_C, _HASH_E, _HASH_D),
        )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        empty_schema.execute(
            "INSERT INTO pipeline_runtime_capabilities ("
            "capability_hash, predecessor_hash, stage, schema_revision, "
            "schema_digest, protocol_version, minimum_build_id, config_hash, "
            "adapter_hash, policy_manifest_hash, evidence_manifest_hash"
            ") VALUES (%s, %s, 'phase3_approval_send', '20260716_0007', %s, 2, "
            "'build-2', %s, %s, %s, %s)",
            (_HASH_E, _HASH_A, _HASH_A, _HASH_B, _HASH_C, _HASH_E, _HASH_D),
        )

    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        empty_schema.execute(
            "UPDATE pipeline_runtime_capabilities "
            "SET minimum_build_id = 'build-2' WHERE capability_hash = %s",
            (capability_hash,),
        )
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        empty_schema.execute("DELETE FROM pipeline_folder_scopes")
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        empty_schema.execute(
            "UPDATE pipeline_initializations SET reason = 'changed reason'"
        )
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        empty_schema.execute("TRUNCATE pipeline_folder_scopes")

    with pytest.raises(psycopg.errors.RaiseException, match="webhook_ids"):
        empty_schema.execute(
            "INSERT INTO pipeline_folder_scopes ("
            "initialization_id, account_id, canonical_key, webhook_ids, "
            "sync_folder, event_policy_matrix, scope_hash, policy_manifest_hash"
            ") VALUES (%s, 8, 'CUSTOM', %s::jsonb, 'Custom', %s::jsonb, %s, %s)",
            (
                initialization_id,
                '["webhook-z", "webhook-a"]',
                psycopg.types.json.Jsonb(_policy_matrix()),
                _HASH_A,
                policy_manifest_hash,
            ),
        )

    wrong_initialization_id = str(uuid4())
    wrong_receipt_id = str(uuid4())
    empty_schema.execute(
        "INSERT INTO pipeline_ownership ("
        "account_id, generation, pipeline_name, state, fencing_token, "
        "created_by, reason"
        ") VALUES (9, 1, 'durable_v1', 'current_ingress', 1, "
        "'operator-1', 'receipt mismatch test')"
    )
    empty_schema.execute(
        "INSERT INTO pipeline_command_receipts ("
        "id, account_id, command_name, idempotency_key_hash, "
        "canonical_payload_hash, outcome, result_type, result_id, "
        "result_hash, authority_epoch"
        ") VALUES (%s, 9, 'runtime.initialize', %s, %s, 'succeeded', "
        "'runtime_initialization', %s, %s, 1)",
        (wrong_receipt_id, _HASH_A, _HASH_B, str(uuid4()), _HASH_C),
    )
    with pytest.raises(psycopg.errors.RaiseException, match="receipt identity"):
        empty_schema.execute(
            "INSERT INTO pipeline_initializations ("
            "initialization_id, command_receipt_id, account_id, generation, "
            "fencing_token, pipeline_name, authority_epoch, authority_version, "
            "capability_hash, policy_manifest_hash, transaction_id, actor, reason"
            ") VALUES (%s, %s, 9, 1, 1, 'durable_v1', 1, 1, %s, %s, "
            "'12346', 'operator-1', 'receipt mismatch test')",
            (
                wrong_initialization_id,
                wrong_receipt_id,
                capability_hash,
                policy_manifest_hash,
            ),
        )


@pytest.mark.integration
def test_0006_requeue_moves_inbox_and_email_epoch_in_one_deferred_transaction(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    policy_manifest_hash = _HASH_E
    capability_hash = _insert_phase2_capability(
        empty_schema,
        policy_manifest_hash=policy_manifest_hash,
    )
    initialization_id = str(uuid4())
    command_receipt_id = str(uuid4())
    empty_schema.execute(
        "INSERT INTO pipeline_ownership ("
        "account_id, generation, pipeline_name, state, fencing_token, "
        "created_by, reason"
        ") VALUES (8, 1, 'durable_v1', 'current_ingress', 1, "
        "'operator-1', 'greenfield initialization')"
    )
    empty_schema.execute(
        "INSERT INTO pipeline_command_receipts ("
        "id, account_id, command_name, idempotency_key_hash, "
        "canonical_payload_hash, outcome, result_type, result_id, "
        "result_hash, authority_epoch"
        ") VALUES (%s, 8, 'runtime.initialize', %s, %s, 'succeeded', "
        "'runtime_initialization', %s, %s, 1)",
        (
            command_receipt_id,
            _HASH_A,
            _HASH_B,
            initialization_id,
            _HASH_C,
        ),
    )
    empty_schema.execute(
        "INSERT INTO pipeline_initializations ("
        "initialization_id, command_receipt_id, account_id, generation, "
        "fencing_token, pipeline_name, authority_epoch, authority_version, "
        "capability_hash, policy_manifest_hash, transaction_id, actor, reason"
        ") VALUES (%s, %s, 8, 1, 1, 'durable_v1', 1, 1, %s, %s, "
        "'12345', 'operator-1', 'greenfield initialization')",
        (
            initialization_id,
            command_receipt_id,
            capability_hash,
            policy_manifest_hash,
        ),
    )
    empty_schema.execute(
        "INSERT INTO pipeline_runtime_authority ("
        "account_id, state, generation, fencing_token, pipeline_name, "
        "authority_epoch, version, schema_revision, protocol_version, "
        "build_id, config_hash, capability_hash, policy_manifest_hash, "
        "initialization_id"
        ") VALUES (8, 'ingest_only', 1, 1, 'durable_v1', 1, 1, "
        "'20260716_0006', 1, 'build-1', %s, %s, %s, %s)",
        (
            _HASH_B,
            capability_hash,
            policy_manifest_hash,
            initialization_id,
        ),
    )

    event_id = str(uuid4())
    email_id = str(uuid4())
    _insert_manual_review_pair(
        empty_schema,
        capability_hash=capability_hash,
        event_id=event_id,
        email_id=email_id,
        external_email_id="mail-requeue-both",
        dedupe_key="1" * 64,
    )
    with psycopg.connect(empty_schema.dsn) as connection:
        connection.execute(
            "UPDATE event_inbox SET execution_epoch = 1, status = 'pending', "
            "attempts = 0, lease_owner = NULL, lease_until = NULL, "
            "lease_session_id = NULL, processing_started_at = NULL, "
            "effect_started_at = NULL, safe_error_code = NULL, "
            "safe_error_summary = NULL WHERE id = %s",
            (event_id,),
        )
        connection.execute(
            "UPDATE emails SET processing_execution_epoch = 1, "
            "status = 'retry_wait', version = 2, "
            "safe_error_code = 'inbox.requeued', safe_error_summary = NULL "
            "WHERE id = %s",
            (email_id,),
        )

    assert empty_schema.scalar(
        "SELECT execution_epoch = 1 AND status = 'pending' "
        "FROM event_inbox WHERE id = %s",
        (event_id,),
    )
    assert empty_schema.scalar(
        "SELECT processing_execution_epoch = 1 "
        "AND status = 'retry_wait' AND version = 2 "
        "FROM emails WHERE id = %s",
        (email_id,),
    )

    incomplete_event_id = str(uuid4())
    incomplete_email_id = str(uuid4())
    _insert_manual_review_pair(
        empty_schema,
        capability_hash=capability_hash,
        event_id=incomplete_event_id,
        email_id=incomplete_email_id,
        external_email_id="mail-requeue-incomplete",
        dedupe_key="2" * 64,
    )
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with psycopg.connect(empty_schema.dsn) as connection:
            connection.execute(
                "UPDATE event_inbox SET execution_epoch = 1, "
                "status = 'pending', attempts = 0, lease_owner = NULL, "
                "lease_until = NULL, lease_session_id = NULL, "
                "processing_started_at = NULL, effect_started_at = NULL, "
                "safe_error_code = NULL, safe_error_summary = NULL "
                "WHERE id = %s",
                (incomplete_event_id,),
            )

    assert empty_schema.scalar(
        "SELECT execution_epoch = 0 AND status = 'manual_review' "
        "FROM event_inbox WHERE id = %s",
        (incomplete_event_id,),
    )


@pytest.mark.integration
def test_0006_lease_and_processing_owner_require_exact_runtime_stamp(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")

    lease_definition = _constraint_definition(
        empty_schema,
        "ck_event_inbox_lease",
    )
    assert "lease_session_id" in lease_definition
    epoch_definition = _constraint_definition(
        empty_schema,
        "ck_event_inbox_execution_epoch",
    )
    assert "9223372036854775807" in epoch_definition

    email_binding = _constraint_definition(
        empty_schema,
        "ck_emails_processing_runtime_identity",
    )
    assert "processing_execution_epoch" in email_binding
    assert "processing_inbox_id" in email_binding


_GREENFIELD_SECURITY_DEFINER_ROUTINES = {
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
}


@pytest.mark.integration
def test_0006_creates_only_fixed_greenfield_security_definer_boundaries(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")

    contracts = empty_schema.scalar(
        "SELECT jsonb_object_agg(routine.proname, jsonb_build_object("
        "  'security_definer', routine.prosecdef, "
        "  'volatile', routine.provolatile, "
        "  'config', COALESCE(to_jsonb(routine.proconfig), '[]'::jsonb), "
        "  'public_execute', "
        "    has_function_privilege('public', routine.oid, 'EXECUTE'), "
        "  'identity_arguments', "
        "    pg_get_function_identity_arguments(routine.oid)"
        ")) "
        "FROM pg_catalog.pg_proc AS routine "
        "JOIN pg_catalog.pg_namespace AS namespace "
        "  ON namespace.oid = routine.pronamespace "
        "WHERE namespace.nspname = current_schema() "
        "  AND routine.proname = ANY(%s)",
        (list(sorted(_GREENFIELD_SECURITY_DEFINER_ROUTINES)),),
    )

    assert set(contracts) == _GREENFIELD_SECURITY_DEFINER_ROUTINES
    for routine_name, contract in contracts.items():
        assert contract["security_definer"] is True, routine_name
        assert contract["public_execute"] is False, routine_name
        assert contract["config"] == ["search_path=pg_catalog"], routine_name
        assert contract["identity_arguments"], routine_name
    assert contracts["greenfield_get_runtime_authority"]["volatile"] == "s"
    for routine_name in _GREENFIELD_SECURITY_DEFINER_ROUTINES - {
        "greenfield_get_runtime_authority"
    }:
        assert contracts[routine_name]["volatile"] == "v", routine_name


@pytest.mark.integration
def test_initialize_runtime_commits_exact_unicode_contract_and_replays(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    arguments = _greenfield_initialization_args()

    first = _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        arguments,
    )
    replay = _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        arguments,
    )

    assert first[0:11] == replay[0:11]
    assert first[11] is False
    assert replay[11] is True
    assert first[12] == replay[12]
    assert first[2:10] == (
        8,
        1,
        1,
        "durable_v1",
        1,
        1,
        arguments[1],
        arguments[10],
    )
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_ownership") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_initializations") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_folder_scopes") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_authority") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 1
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 1
    assert (
        empty_schema.scalar(
            "SELECT state FROM pipeline_runtime_authority WHERE account_id = 8"
        )
        == "ingest_only"
    )
    assert (
        empty_schema.scalar(
            "SELECT actor || ':' || reason FROM pipeline_initializations"
        )
        == "测试操作员:全新系统初始化"
    )

    conflicting_arguments = _greenfield_initialization_args(
        reason="不同的初始化语义",
    )
    with pytest.raises(psycopg.errors.RaiseException, match="idempotency_conflict"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_initialize_runtime",
            conflicting_arguments,
        )

    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 1
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 1


@pytest.mark.integration
def test_initialize_runtime_twenty_way_concurrency_mints_one_identity(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    arguments = _greenfield_initialization_args()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(
                _call_greenfield_function,
                empty_schema,
                "greenfield_initialize_runtime",
                arguments,
            )
            for _ in range(20)
        ]
        receipts = [future.result(timeout=20) for future in futures]

    assert len({(str(row[0]), str(row[1])) for row in receipts}) == 1
    assert sum(row[11] is False for row in receipts) == 1
    assert sum(row[11] is True for row in receipts) == 19
    assert len({row[10] for row in receipts}) == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_ownership") == 1
    assert (
        empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_capabilities") == 1
    )
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_initializations") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_authority") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 1
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 1


@pytest.mark.integration
def test_concurrent_different_account_initialization_has_one_global_winner(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    base_scope = _policy_snapshot().scopes[0]
    alternate_policy = PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="PROJECTS",
                webhook_ids=("projects-id",),
                sync_folder="Projects",
                event_policy_matrix=dict(base_scope.event_policy_matrix),
            ),
        )
    )
    account_8 = _greenfield_initialization_args(
        account_id=8,
        idempotency_key="initialize-global-race-8",
    )
    account_9 = _greenfield_initialization_args(
        account_id=9,
        idempotency_key="initialize-global-race-9",
        policy_snapshot=alternate_policy,
    )
    assert account_8[1] != account_9[1]
    empty_schema.execute(
        "CREATE FUNCTION public.delay_test_capability_insert() "
        "RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$ "
        "BEGIN PERFORM pg_catalog.pg_sleep(0.2); RETURN NEW; END $$"
    )
    empty_schema.execute(
        "CREATE TRIGGER trg_delay_test_capability_insert "
        "BEFORE INSERT ON pipeline_runtime_capabilities FOR EACH ROW "
        "EXECUTE FUNCTION public.delay_test_capability_insert()"
    )
    barrier = Barrier(2)

    def initialize(arguments: tuple[object, ...]) -> tuple[str, str]:
        barrier.wait(timeout=10)
        try:
            receipt = _call_greenfield_function(
                empty_schema,
                "greenfield_initialize_runtime",
                arguments,
            )
        except psycopg.errors.RaiseException as exc:
            return "rejected", str(exc).splitlines()[0]
        return "committed", str(receipt[2])

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(initialize, arguments)
            for arguments in (account_8, account_9)
        ]
        outcomes = [future.result(timeout=20) for future in futures]

    assert sorted(outcome[0] for outcome in outcomes) == ["committed", "rejected"]
    assert [outcome[1] for outcome in outcomes if outcome[0] == "rejected"] == [
        "greenfield_reinitialize_required"
    ]
    for relation in (
        "pipeline_ownership",
        "pipeline_runtime_capabilities",
        "pipeline_initializations",
        "pipeline_folder_scopes",
        "pipeline_runtime_authority",
        "pipeline_command_receipts",
        "audit_events",
    ):
        assert empty_schema.scalar(f"SELECT count(*) FROM {relation}") == 1


@pytest.mark.integration
@pytest.mark.parametrize("race_round", range(20))
def test_initialize_register_and_intake_account_lock_race_has_one_legal_order(
    alembic_runner,
    empty_schema,
    race_round: int,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    initialization = _greenfield_initialization_args(
        idempotency_key=f"initialize-race-{race_round}"
    )
    session_id = str(uuid4())
    registration = (
        8,
        f"web-race-{race_round}",
        session_id,
        1,
        1,
        initialization[4],
        initialization[6],
        initialization[7],
        initialization[8],
        initialization[1],
        30,
    )
    webhook = (
        8,
        session_id,
        1,
        f"mail-race-{race_round}",
        "inbox-id",
        "NewMailEvent",
        "create",
        f"{race_round + 100:064x}",
        None,
        None,
        psycopg.types.json.Jsonb({"race_round": race_round}),
        "full",
    )
    barrier = Barrier(3)

    def invoke(function_name: str, arguments: tuple[object, ...]) -> tuple[str, str]:
        barrier.wait(timeout=10)
        try:
            row = _call_greenfield_function(
                empty_schema,
                function_name,
                arguments,
            )
        except psycopg.errors.RaiseException as exc:
            return "rejected", str(exc).splitlines()[0]
        return "committed", str(row[0])

    operations = (
        ("greenfield_initialize_runtime", initialization),
        ("greenfield_register_web_instance", registration),
        ("greenfield_insert_webhook_event", webhook),
    )
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(invoke, function_name, arguments)
            for function_name, arguments in operations
        ]
        initialization_result, registration_result, webhook_result = (
            future.result(timeout=20) for future in futures
        )

    assert initialization_result[0] == "committed"
    if registration_result[0] == "rejected":
        assert registration_result[1] == "runtime_instance_authority_unavailable"
    if webhook_result[0] == "rejected":
        assert webhook_result[1] in {
            "greenfield_webhook_authority_unavailable",
            "greenfield_webhook_session_unavailable",
        }
    if webhook_result[0] == "committed":
        assert registration_result[0] == "committed"

    expected_instances = int(registration_result[0] == "committed")
    expected_inbox = int(webhook_result[0] == "committed")
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_initializations") == 1
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_authority") == 1
    assert (
        empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_instances")
        == expected_instances
    )
    assert empty_schema.scalar("SELECT count(*) FROM event_inbox") == expected_inbox
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 1
    assert (
        empty_schema.scalar("SELECT count(*) FROM audit_events") == 1 + expected_inbox
    )


@pytest.mark.integration
def test_initialize_runtime_late_audit_failure_rolls_back_every_fact(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    empty_schema.execute(
        "CREATE FUNCTION public.reject_test_initialization_audit() "
        "RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$ "
        "BEGIN IF NEW.action = 'runtime.initialize' THEN "
        "RAISE EXCEPTION 'injected_initialization_audit_failure'; "
        "END IF; RETURN NEW; END $$"
    )
    empty_schema.execute(
        "CREATE TRIGGER trg_reject_test_initialization_audit "
        "BEFORE INSERT ON audit_events FOR EACH ROW "
        "EXECUTE FUNCTION public.reject_test_initialization_audit()"
    )

    with pytest.raises(
        psycopg.errors.RaiseException,
        match="injected_initialization_audit_failure",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_initialize_runtime",
            _greenfield_initialization_args(),
        )

    for relation in (
        "pipeline_ownership",
        "pipeline_runtime_capabilities",
        "pipeline_initializations",
        "pipeline_folder_scopes",
        "pipeline_runtime_authority",
        "pipeline_command_receipts",
        "audit_events",
    ):
        assert empty_schema.scalar(f"SELECT count(*) FROM {relation}") == 0


@pytest.mark.integration
def test_initialize_runtime_accepts_python_canonical_escaped_unicode_policy(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    base_scope = _policy_snapshot().scopes[0]
    unicode_snapshot = PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="客户📧邮件",
                webhook_ids=("客户-📥",),
                sync_folder="客户文件夹",
                event_policy_matrix=dict(base_scope.event_policy_matrix),
            ),
        )
    )
    arguments = _greenfield_initialization_args(
        actor="中文操作员",
        reason="验证转义后的中文策略",
        policy_snapshot=unicode_snapshot,
    )
    assert "\\u" in str(arguments[12])

    receipt = _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        arguments,
    )

    assert receipt[2] == 8
    assert (
        empty_schema.scalar("SELECT canonical_key FROM pipeline_folder_scopes")
        == "客户📧邮件"
    )
    assert (
        empty_schema.scalar("SELECT webhook_ids ->> 0 FROM pipeline_folder_scopes")
        == "客户-📥"
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "mutation",
    ["scope_hash", "extra_scope_key", "duplicate_matrix", "pretty_json"],
)
def test_initialize_runtime_rejects_self_consistent_noncanonical_policy(
    alembic_runner,
    empty_schema,
    mutation: str,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    valid_policy = str(_greenfield_initialization_args()[12])
    parsed = json.loads(valid_policy)
    if mutation == "scope_hash":
        parsed["scopes"][0]["scope_hash"] = "f" * 64
        forged_policy = json.dumps(
            parsed,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    elif mutation == "extra_scope_key":
        parsed["scopes"][0]["unexpected"] = True
        forged_policy = json.dumps(
            parsed,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    elif mutation == "duplicate_matrix":
        parsed["scopes"][0]["event_policy_matrix"].append(
            dict(parsed["scopes"][0]["event_policy_matrix"][0])
        )
        forged_policy = json.dumps(
            parsed,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    else:
        forged_policy = json.dumps(
            parsed,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )

    with pytest.raises(
        psycopg.errors.RaiseException,
        match="greenfield_initialization_policy_invalid",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_initialize_runtime",
            _remint_initialization_args_for_raw_policy(forged_policy),
        )

    assert empty_schema.scalar("SELECT count(*) FROM pipeline_ownership") == 0
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_runtime_authority") == 0
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 0
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 0


@pytest.mark.integration
def test_initialize_runtime_rejects_noncanonical_idempotency_text(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    arguments = list(_greenfield_initialization_args())
    arguments[16] = " initialize-8 "

    with pytest.raises(
        psycopg.errors.RaiseException,
        match="greenfield_initialization_input_invalid",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_initialize_runtime",
            tuple(arguments),
        )

    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 0


@pytest.mark.integration
def test_security_definer_inputs_reject_python_stripped_unicode_edge_space(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    unicode_edge_spaces = tuple(
        chr(codepoint)
        for codepoint in (
            133,
            160,
            5760,
            *range(8192, 8203),
            8232,
            8233,
            8239,
            8287,
            12288,
        )
    )
    nonbreaking_space = unicode_edge_spaces[1]

    for edge_space in unicode_edge_spaces:
        invalid_initialization = list(_greenfield_initialization_args())
        invalid_initialization[14] = f"{edge_space}测试操作员"
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="greenfield_initialization_input_invalid",
        ):
            _call_greenfield_function(
                empty_schema,
                "greenfield_initialize_runtime",
                tuple(invalid_initialization),
            )

    policy = json.loads(str(_greenfield_initialization_args()[12]))
    policy["scopes"][0]["canonical_key"] = f"{nonbreaking_space}INBOX"
    invalid_policy = json.dumps(
        policy,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="greenfield_initialization_policy_invalid",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_initialize_runtime",
            _remint_initialization_args_for_raw_policy(invalid_policy),
        )

    initialization = _greenfield_initialization_args()
    _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        initialization,
    )
    authority = _authority_from_row(
        _call_greenfield_function(
            empty_schema,
            "greenfield_get_runtime_authority",
            (8,),
        )
    )
    transition = list(
        _authority_transition_args(
            authority,
            target_state=RuntimeAuthorityState.PAUSED,
            actor="测试操作员",
            reason="验证 Unicode 边界空白",
            idempotency_key="unicode-edge-transition",
        )
    )
    transition[4] = f"{nonbreaking_space}测试操作员"
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="runtime_authority_transition_input_invalid",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_pause_runtime",
            tuple(transition),
        )

    session_id = str(uuid4())
    _call_greenfield_function(
        empty_schema,
        "greenfield_register_web_instance",
        (
            8,
            "web-unicode-edge",
            session_id,
            authority.authority_epoch,
            authority.version,
            authority.schema_revision,
            authority.protocol_version,
            authority.build_id,
            authority.config_hash,
            authority.capability_hash,
            30,
        ),
    )
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="greenfield_webhook_input_invalid",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_insert_webhook_event",
            (
                8,
                session_id,
                1,
                f"{nonbreaking_space}mail-unicode-edge",
                "inbox-id",
                "NewMailEvent",
                "create",
                _HASH_A,
                None,
                None,
                psycopg.types.json.Jsonb({"safe": "payload"}),
                "full",
            ),
        )
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="greenfield_requeue_input_invalid",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_requeue_inbox",
            (
                8,
                str(uuid4()),
                0,
                1,
                f"{nonbreaking_space}测试操作员",
                "验证 Unicode 边界空白",
                "unicode-edge-requeue",
                _HASH_B,
            ),
        )

    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 1
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 1
    assert empty_schema.scalar("SELECT count(*) FROM event_inbox") == 0


@pytest.mark.integration
def test_runtime_pause_resume_are_cas_fenced_audited_and_replayable(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        _greenfield_initialization_args(),
    )
    initial = _authority_from_row(
        _call_greenfield_function(
            empty_schema,
            "greenfield_get_runtime_authority",
            (8,),
        )
    )
    pause_arguments = _authority_transition_args(
        initial,
        target_state=RuntimeAuthorityState.PAUSED,
        actor="值班操作员",
        reason="暂停入口进行检查",
        idempotency_key="pause-8-1",
    )

    paused = _call_greenfield_function(
        empty_schema,
        "greenfield_pause_runtime",
        pause_arguments,
    )
    pause_replay = _call_greenfield_function(
        empty_schema,
        "greenfield_pause_runtime",
        pause_arguments,
    )
    assert paused[0:6] == pause_replay[0:6]
    assert paused[6] is False
    assert pause_replay[6] is True
    assert paused[7:] == pause_replay[7:]
    assert paused[1:5] == ("runtime.pause", "ingest_only", 1, 1)
    paused_authority = _authority_from_row(paused[8:])
    assert paused_authority.state is RuntimeAuthorityState.PAUSED
    assert paused_authority.authority_epoch == 2
    assert paused_authority.version == 2

    forged_replay = list(pause_arguments)
    forged_replay[1] = 99
    forged_replay[5] = "篡改但沿用旧哈希"
    with pytest.raises(psycopg.errors.RaiseException, match="idempotency_conflict"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_pause_runtime",
            tuple(forged_replay),
        )

    stale_pause = _authority_transition_args(
        initial,
        target_state=RuntimeAuthorityState.PAUSED,
        actor="值班操作员",
        reason="第二个陈旧命令",
        idempotency_key="pause-8-stale",
    )
    with pytest.raises(psycopg.errors.RaiseException, match="authority_cas_conflict"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_pause_runtime",
            stale_pause,
        )

    resume_arguments = _authority_transition_args(
        paused_authority,
        target_state=RuntimeAuthorityState.INGEST_ONLY,
        actor="值班操作员",
        reason="检查完成恢复入口",
        idempotency_key="resume-8-2",
    )
    resumed = _call_greenfield_function(
        empty_schema,
        "greenfield_resume_ingress",
        resume_arguments,
    )
    resumed_authority = _authority_from_row(resumed[8:])
    assert resumed[1:5] == ("runtime.resume_ingress", "paused", 2, 2)
    assert resumed_authority.state is RuntimeAuthorityState.INGEST_ONLY
    assert resumed_authority.authority_epoch == 3
    assert resumed_authority.version == 3

    old_pause_replay = _call_greenfield_function(
        empty_schema,
        "greenfield_pause_runtime",
        pause_arguments,
    )
    assert old_pause_replay[6] is True
    assert _authority_from_row(old_pause_replay[8:]).state is (
        RuntimeAuthorityState.PAUSED
    )
    assert _authority_from_row(old_pause_replay[8:]).authority_epoch == 2
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 3
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 3


@pytest.mark.integration
def test_web_instance_lease_is_authority_fenced_monotonic_and_drainable(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    initialization = _greenfield_initialization_args()
    _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        initialization,
    )
    initial = _authority_from_row(
        _call_greenfield_function(
            empty_schema,
            "greenfield_get_runtime_authority",
            (8,),
        )
    )
    session_id = str(uuid4())
    registration_arguments = (
        8,
        "web-1",
        session_id,
        initial.authority_epoch,
        initial.version,
        initial.schema_revision,
        initial.protocol_version,
        initial.build_id,
        initial.config_hash,
        initial.capability_hash,
        30,
    )
    registered = _call_greenfield_function(
        empty_schema,
        "greenfield_register_web_instance",
        registration_arguments,
    )
    assert registered[0:13] == (
        8,
        "web",
        "web-1",
        registered[3],
        1,
        1,
        1,
        initial.capability_hash,
        "20260716_0006",
        1,
        "build-1",
        _HASH_B,
        "active",
    )
    assert str(registered[3]) == session_id
    assert registered[13:16] == (1, 0, 0)
    assert registered[17] > registered[16]
    assert (
        _call_greenfield_function(
            empty_schema,
            "greenfield_register_web_instance",
            registration_arguments,
        )
        == registered
    )
    changed_ttl = (*registration_arguments[:-1], 31)
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="registration_conflict",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_register_web_instance",
            changed_ttl,
        )

    heartbeated = _call_greenfield_function(
        empty_schema,
        "greenfield_heartbeat_web_instance",
        (8, session_id, 1, 1, initial.capability_hash, 1, 0, 30),
    )
    assert heartbeated[12:16] == ("active", 2, 1, 0)
    assert heartbeated[16] >= registered[16]
    assert heartbeated[17] > heartbeated[16]
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="registration_conflict",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_register_web_instance",
            registration_arguments,
        )

    with pytest.raises(psycopg.errors.RaiseException, match="instance_lease_cas"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_heartbeat_web_instance",
            (8, session_id, 1, 1, initial.capability_hash, 2, 0, 30),
        )

    paused = _call_greenfield_function(
        empty_schema,
        "greenfield_pause_runtime",
        _authority_transition_args(
            initial,
            target_state=RuntimeAuthorityState.PAUSED,
            actor="值班操作员",
            reason="暂停验证租约围栏",
            idempotency_key="pause-for-lease",
        ),
    )
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="runtime_instance_authority_unavailable",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_heartbeat_web_instance",
            (8, session_id, 2, 1, initial.capability_hash, 2, 0, 30),
        )

    drained = _call_greenfield_function(
        empty_schema,
        "greenfield_drain_web_instance",
        (8, session_id, 2, 1, initial.capability_hash),
    )
    assert drained[12:16] == ("draining", 3, 1, 0)
    paused_authority = _authority_from_row(paused[8:])
    resumed = _call_greenfield_function(
        empty_schema,
        "greenfield_resume_ingress",
        _authority_transition_args(
            paused_authority,
            target_state=RuntimeAuthorityState.INGEST_ONLY,
            actor="值班操作员",
            reason="恢复验证新租约",
            idempotency_key="resume-for-lease",
        ),
    )
    resumed_authority = _authority_from_row(resumed[8:])
    replacement_session = str(uuid4())
    replacement = _call_greenfield_function(
        empty_schema,
        "greenfield_register_web_instance",
        (
            8,
            "web-1",
            replacement_session,
            resumed_authority.authority_epoch,
            resumed_authority.version,
            resumed_authority.schema_revision,
            resumed_authority.protocol_version,
            resumed_authority.build_id,
            resumed_authority.config_hash,
            resumed_authority.capability_hash,
            30,
        ),
    )
    assert str(replacement[3]) == replacement_session
    assert replacement[6] == 3
    assert replacement[12:16] == ("active", 1, 0, 0)


@pytest.mark.integration
def test_webhook_insert_uses_database_policy_session_fence_and_exact_dedupe(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        _greenfield_initialization_args(),
    )
    authority = _authority_from_row(
        _call_greenfield_function(
            empty_schema,
            "greenfield_get_runtime_authority",
            (8,),
        )
    )
    session_id = str(uuid4())
    _call_greenfield_function(
        empty_schema,
        "greenfield_register_web_instance",
        (
            8,
            "web-hook",
            session_id,
            authority.authority_epoch,
            authority.version,
            authority.schema_revision,
            authority.protocol_version,
            authority.build_id,
            authority.config_hash,
            authority.capability_hash,
            30,
        ),
    )
    event_arguments = (
        8,
        session_id,
        1,
        "mail-1",
        "inbox-id",
        "NewMailEvent",
        "create",
        _HASH_A,
        "version-1",
        None,
        psycopg.types.json.Jsonb({"signed": "payload"}),
        "full",
    )

    for index, invalid_source_event_at in enumerate(
        (
            "infinity",
            "10000-01-01 00:00:00+00",
            "0001-01-01 00:00:00+00 BC",
            "0001-12-31 23:59:59.999999+00",
            "9999-01-01 00:00:00+00",
        )
    ):
        invalid_time = list(event_arguments)
        invalid_time[3] = f"mail-invalid-time-{index}"
        invalid_time[9] = invalid_source_event_at
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="greenfield_webhook_input_invalid",
        ):
            _call_greenfield_function(
                empty_schema,
                "greenfield_insert_webhook_event",
                tuple(invalid_time),
            )

    null_change_kind = list(event_arguments)
    null_change_kind[3] = "mail-null-change-kind"
    null_change_kind[6] = None
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="greenfield_webhook_input_invalid",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_insert_webhook_event",
            tuple(null_change_kind),
        )

    for index, (safe_source_event_at, session_timezone, expected_year) in enumerate(
        (
            ("0002-01-01 00:00:00+00", "America/New_York", 1),
            (
                "9998-12-31 23:59:59.999999+00",
                "Pacific/Kiritimati",
                9999,
            ),
        )
    ):
        safe_time = list(event_arguments)
        safe_time[3] = f"mail-safe-time-{index}"
        safe_time[7] = f"{index + 1000:064x}"
        safe_time[9] = safe_source_event_at
        safe_insert = _call_greenfield_function(
            empty_schema,
            "greenfield_insert_webhook_event",
            tuple(safe_time),
        )
        with psycopg.connect(empty_schema.dsn, autocommit=True) as connection:
            connection.execute(
                "SELECT pg_catalog.set_config('TimeZone', %s, false)",
                (session_timezone,),
            )
            decoded = connection.execute(
                "SELECT source_event_at FROM event_inbox WHERE id = %s",
                (str(safe_insert[0]),),
            ).fetchone()
        assert decoded is not None
        assert decoded[0].year == expected_year

    assert empty_schema.scalar("SELECT count(*) FROM event_inbox") == 2
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 3

    inserted = _call_greenfield_function(
        empty_schema,
        "greenfield_insert_webhook_event",
        event_arguments,
    )
    replay = _call_greenfield_function(
        empty_schema,
        "greenfield_insert_webhook_event",
        event_arguments,
    )
    assert str(inserted[0]) == str(replay[0])
    assert inserted[1] is False
    assert replay[1] is True
    assert (
        empty_schema.scalar(
            "SELECT status FROM event_inbox WHERE id = %s",
            (str(inserted[0]),),
        )
        == "pending"
    )
    assert empty_schema.scalar(
        "SELECT authority_epoch = 1 AND capability_hash = %s "
        "FROM event_inbox WHERE id = %s",
        (authority.capability_hash, str(inserted[0])),
    )

    forged_duplicate = list(event_arguments)
    forged_duplicate[10] = psycopg.types.json.Jsonb({"signed": "tampered"})
    with pytest.raises(psycopg.errors.RaiseException, match="dedupe_identity"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_insert_webhook_event",
            tuple(forged_duplicate),
        )

    wrong_policy = list(event_arguments)
    wrong_policy[7] = _HASH_B
    wrong_policy[11] = "ignored"
    with pytest.raises(psycopg.errors.RaiseException, match="policy_mismatch"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_insert_webhook_event",
            tuple(wrong_policy),
        )

    ignored = _call_greenfield_function(
        empty_schema,
        "greenfield_insert_webhook_event",
        (
            8,
            session_id,
            1,
            "mail-unknown",
            "unconfigured-folder",
            "NewMailEvent",
            "create",
            _HASH_C,
            None,
            None,
            psycopg.types.json.Jsonb({"signed": "ignored"}),
            "ignored",
        ),
    )
    assert (
        empty_schema.scalar(
            "SELECT status FROM event_inbox WHERE id = %s",
            (str(ignored[0]),),
        )
        == "completed"
    )

    _call_greenfield_function(
        empty_schema,
        "greenfield_pause_runtime",
        _authority_transition_args(
            authority,
            target_state=RuntimeAuthorityState.PAUSED,
            actor="值班操作员",
            reason="暂停验证 Webhook 围栏",
            idempotency_key="pause-for-webhook",
        ),
    )
    blocked = list(event_arguments)
    blocked[3] = "mail-blocked"
    blocked[7] = _HASH_D
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="webhook_authority_unavailable",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_insert_webhook_event",
            tuple(blocked),
        )

    assert empty_schema.scalar("SELECT count(*) FROM event_inbox") == 4
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 6


@pytest.mark.integration
def test_requeue_function_atomically_advances_inbox_email_and_receipt(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    initialization = _greenfield_initialization_args()
    _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        initialization,
    )
    inbox_id = str(uuid4())
    email_id = str(uuid4())
    _insert_manual_review_pair(
        empty_schema,
        capability_hash=str(initialization[1]),
        event_id=inbox_id,
        email_id=email_id,
        external_email_id="mail-requeue-function",
        dedupe_key=_HASH_A,
    )
    command = RequeueCommand(
        account_id=8,
        inbox_id=inbox_id,
        expected_execution_epoch=0,
        expected_email_version=1,
        actor="人工恢复员",
        reason="确认无外部副作用后重新排队",
        idempotency_key="requeue-function-1",
    )
    arguments = (
        command.account_id,
        command.inbox_id,
        command.expected_execution_epoch,
        command.expected_email_version,
        command.actor,
        command.reason,
        command.idempotency_key,
        canonical_requeue_payload_hash(command),
    )

    receipt = _call_greenfield_function(
        empty_schema,
        "greenfield_requeue_inbox",
        arguments,
    )
    replay = _call_greenfield_function(
        empty_schema,
        "greenfield_requeue_inbox",
        arguments,
    )
    assert receipt[0:8] == replay[0:8]
    assert receipt[8] is False
    assert replay[8] is True
    assert receipt[9] == replay[9]
    assert str(receipt[1]) == inbox_id
    assert str(receipt[2]) == email_id
    assert receipt[3:7] == (0, 1, 2, "retry_wait")
    assert empty_schema.scalar(
        "SELECT execution_epoch = 1 AND status = 'pending' "
        "AND attempts = 0 AND processing_started_at IS NULL "
        "AND safe_error_code IS NULL FROM event_inbox WHERE id = %s",
        (inbox_id,),
    )
    assert empty_schema.scalar(
        "SELECT processing_execution_epoch = 1 AND status = 'retry_wait' "
        "AND version = 2 AND safe_error_code = 'inbox.requeued' "
        "FROM emails WHERE id = %s",
        (email_id,),
    )

    forged_replay = list(arguments)
    forged_replay[3] = 99
    forged_replay[5] = "篡改旧回执参数"
    with pytest.raises(psycopg.errors.RaiseException, match="requeue_payload_invalid"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_requeue_inbox",
            tuple(forged_replay),
        )

    blocked_inbox = str(uuid4())
    blocked_email = str(uuid4())
    _insert_manual_review_pair(
        empty_schema,
        capability_hash=str(initialization[1]),
        event_id=blocked_inbox,
        email_id=blocked_email,
        external_email_id="mail-requeue-effect",
        dedupe_key=_HASH_B,
    )
    empty_schema.execute(
        "UPDATE event_inbox SET effect_started_at = CURRENT_TIMESTAMP WHERE id = %s",
        (blocked_inbox,),
    )
    blocked_command = RequeueCommand(
        account_id=8,
        inbox_id=blocked_inbox,
        expected_execution_epoch=0,
        expected_email_version=1,
        actor="人工恢复员",
        reason="有副作用的记录不得重试",
        idempotency_key="requeue-effect-blocked",
    )
    with pytest.raises(psycopg.errors.RaiseException, match="requeue_not_safe"):
        _call_greenfield_function(
            empty_schema,
            "greenfield_requeue_inbox",
            (
                blocked_command.account_id,
                blocked_command.inbox_id,
                blocked_command.expected_execution_epoch,
                blocked_command.expected_email_version,
                blocked_command.actor,
                blocked_command.reason,
                blocked_command.idempotency_key,
                canonical_requeue_payload_hash(blocked_command),
            ),
        )

    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 2
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 2


@pytest.mark.integration
def test_requeue_requires_one_complete_recoverable_aggregate_and_version_budget(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    initialization = _greenfield_initialization_args()
    _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        initialization,
    )
    capability_hash = str(initialization[1])

    edge_inbox_id = str(uuid4())
    edge_email_id = str(uuid4())
    _insert_manual_review_pair(
        empty_schema,
        capability_hash=capability_hash,
        event_id=edge_inbox_id,
        email_id=edge_email_id,
        external_email_id="mail-requeue-last-safe-version",
        dedupe_key=_HASH_A,
    )
    empty_schema.execute(
        "UPDATE event_inbox SET status = 'dead_letter', attempts = %s, "
        "safe_error_code = 'terminal.failure', "
        "safe_error_summary = 'safe summary' WHERE id = %s",
        (POSTGRES_BIGINT_MAX, edge_inbox_id),
    )
    empty_schema.execute(
        "UPDATE emails SET status = 'dead_letter', version = %s, "
        "safe_error_code = 'terminal.failure', "
        "safe_error_summary = 'safe summary' WHERE id = %s",
        (POSTGRES_BIGINT_MAX - 3, edge_email_id),
    )
    edge_command = RequeueCommand(
        account_id=8,
        inbox_id=edge_inbox_id,
        expected_execution_epoch=0,
        expected_email_version=POSTGRES_BIGINT_MAX - 3,
        actor='恢复员😀"\\',
        reason='确认“无副作用”后重试 😀 "\\',
        idempotency_key="requeue-last-safe-version",
    )
    edge_arguments = (
        edge_command.account_id,
        edge_command.inbox_id,
        edge_command.expected_execution_epoch,
        edge_command.expected_email_version,
        edge_command.actor,
        edge_command.reason,
        edge_command.idempotency_key,
        canonical_requeue_payload_hash(edge_command),
    )

    edge_receipt = _call_greenfield_function(
        empty_schema,
        "greenfield_requeue_inbox",
        edge_arguments,
    )
    edge_replay = _call_greenfield_function(
        empty_schema,
        "greenfield_requeue_inbox",
        edge_arguments,
    )
    assert edge_receipt[0:8] == edge_replay[0:8]
    assert edge_receipt[3:7] == (
        0,
        1,
        POSTGRES_BIGINT_MAX - 2,
        "retry_wait",
    )
    assert edge_receipt[8] is False
    assert edge_replay[8] is True
    assert empty_schema.scalar(
        "SELECT attempts = 0 AND execution_epoch = 1 AND status = 'pending' "
        "FROM event_inbox WHERE id = %s",
        (edge_inbox_id,),
    )

    unsafe_cases = (
        (
            "source-deleted",
            {},
            "UPDATE emails SET source_deleted_at = CURRENT_TIMESTAMP WHERE id = %s",
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "email-status-split",
            {},
            "UPDATE emails SET status = 'dead_letter' WHERE id = %s",
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "inbox-status-split",
            {},
            "UPDATE event_inbox SET status = 'dead_letter' WHERE id = %s",
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "safe-code-split",
            {},
            "UPDATE emails SET safe_error_code = 'different.code' WHERE id = %s",
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "safe-summary-split",
            {},
            "UPDATE emails SET safe_error_summary = 'different summary' WHERE id = %s",
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "missing-email-create-marker",
            {"email_create_seen": False},
            None,
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "missing-email-processing-marker",
            {"email_processing_started": False},
            None,
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "missing-inbox-processing-marker",
            {"inbox_processing_started": False},
            None,
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "email-effect-started",
            {},
            "UPDATE emails SET external_effects_started_at = CURRENT_TIMESTAMP "
            "WHERE id = %s",
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "inbox-effect-started",
            {},
            "UPDATE event_inbox SET effect_started_at = CURRENT_TIMESTAMP "
            "WHERE id = %s",
            "greenfield_requeue_not_safe",
            1,
        ),
        (
            "email-version-exhausted",
            {},
            "UPDATE emails SET version = 9223372036854775805 WHERE id = %s",
            "greenfield_requeue_input_invalid",
            POSTGRES_BIGINT_MAX - 2,
        ),
    )

    for index, (
        case_name,
        fixture_options,
        mutation_sql,
        expected_error,
        expected_email_version,
    ) in enumerate(unsafe_cases):
        inbox_id = str(uuid4())
        email_id = str(uuid4())
        _insert_manual_review_pair(
            empty_schema,
            capability_hash=capability_hash,
            event_id=inbox_id,
            email_id=email_id,
            external_email_id=f"mail-requeue-unsafe-{index}",
            dedupe_key=f"{index + 10:064x}",
            **fixture_options,
        )
        if mutation_sql is not None:
            target_id = inbox_id if "event_inbox" in mutation_sql else email_id
            empty_schema.execute(mutation_sql, (target_id,))

        inbox_before = empty_schema.scalar(
            "SELECT md5(row_to_json(inbox_row)::text) "
            "FROM event_inbox AS inbox_row WHERE id = %s",
            (inbox_id,),
        )
        email_before = empty_schema.scalar(
            "SELECT md5(row_to_json(email_row)::text) "
            "FROM emails AS email_row WHERE id = %s",
            (email_id,),
        )
        receipt_count = empty_schema.scalar(
            "SELECT count(*) FROM pipeline_command_receipts"
        )
        audit_count = empty_schema.scalar("SELECT count(*) FROM audit_events")
        command = RequeueCommand(
            account_id=8,
            inbox_id=inbox_id,
            expected_execution_epoch=0,
            expected_email_version=expected_email_version,
            actor="恢复安全审查员",
            reason=f"验证不安全恢复被拒绝 {case_name}",
            idempotency_key=f"requeue-unsafe-{case_name}",
        )
        arguments = (
            command.account_id,
            command.inbox_id,
            command.expected_execution_epoch,
            command.expected_email_version,
            command.actor,
            command.reason,
            command.idempotency_key,
            canonical_requeue_payload_hash(command),
        )

        with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
            _call_greenfield_function(
                empty_schema,
                "greenfield_requeue_inbox",
                arguments,
            )

        assert (
            empty_schema.scalar(
                "SELECT md5(row_to_json(inbox_row)::text) "
                "FROM event_inbox AS inbox_row WHERE id = %s",
                (inbox_id,),
            )
            == inbox_before
        )
        assert (
            empty_schema.scalar(
                "SELECT md5(row_to_json(email_row)::text) "
                "FROM emails AS email_row WHERE id = %s",
                (email_id,),
            )
            == email_before
        )
        assert (
            empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts")
            == receipt_count
        )
        assert empty_schema.scalar("SELECT count(*) FROM audit_events") == audit_count


@pytest.mark.integration
def test_requeue_rejects_work_stamped_by_a_previous_runtime_authority(
    alembic_runner,
    empty_schema,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")
    initialization = _greenfield_initialization_args()
    _call_greenfield_function(
        empty_schema,
        "greenfield_initialize_runtime",
        initialization,
    )
    authority = _authority_from_row(
        _call_greenfield_function(
            empty_schema,
            "greenfield_get_runtime_authority",
            (8,),
        )
    )
    inbox_id = str(uuid4())
    email_id = str(uuid4())
    _insert_manual_review_pair(
        empty_schema,
        capability_hash=authority.capability_hash,
        event_id=inbox_id,
        email_id=email_id,
        external_email_id="mail-stale-authority-requeue",
        dedupe_key=_HASH_A,
    )
    _call_greenfield_function(
        empty_schema,
        "greenfield_pause_runtime",
        _authority_transition_args(
            authority,
            target_state=RuntimeAuthorityState.PAUSED,
            actor="恢复安全审查员",
            reason="推进 authority epoch 后验证旧工作围栏",
            idempotency_key="pause-before-stale-requeue",
        ),
    )
    command = RequeueCommand(
        account_id=8,
        inbox_id=inbox_id,
        expected_execution_epoch=0,
        expected_email_version=1,
        actor="恢复安全审查员",
        reason="旧 authority 工作不得进入当前队列",
        idempotency_key="stale-authority-requeue",
    )
    inbox_before = empty_schema.scalar(
        "SELECT md5(row_to_json(inbox_row)::text) "
        "FROM event_inbox AS inbox_row WHERE id = %s",
        (inbox_id,),
    )
    email_before = empty_schema.scalar(
        "SELECT md5(row_to_json(email_row)::text) "
        "FROM emails AS email_row WHERE id = %s",
        (email_id,),
    )

    with pytest.raises(
        psycopg.errors.RaiseException,
        match="greenfield_requeue_not_safe",
    ):
        _call_greenfield_function(
            empty_schema,
            "greenfield_requeue_inbox",
            (
                command.account_id,
                command.inbox_id,
                command.expected_execution_epoch,
                command.expected_email_version,
                command.actor,
                command.reason,
                command.idempotency_key,
                canonical_requeue_payload_hash(command),
            ),
        )

    assert empty_schema.scalar(
        "SELECT md5(row_to_json(inbox_row)::text) "
        "FROM event_inbox AS inbox_row WHERE id = %s",
        (inbox_id,),
    ) == inbox_before
    assert empty_schema.scalar(
        "SELECT md5(row_to_json(email_row)::text) "
        "FROM emails AS email_row WHERE id = %s",
        (email_id,),
    ) == email_before
    assert empty_schema.scalar("SELECT count(*) FROM pipeline_command_receipts") == 2
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 2


@pytest.mark.integration
@pytest.mark.parametrize(
    ("routine_name", "arguments"),
    [
        (
            "greenfield_claim_inbox",
            "8, '00000000-0000-4000-8000-000000000001'::uuid, 1, 'worker-1', 1, 30",
        ),
        (
            "greenfield_renew_inbox",
            "8, '00000000-0000-4000-8000-000000000001'::uuid, 1, "
            "'00000000-0000-4000-8000-000000000002'::uuid, 0, "
            "'worker-1', 1, 30",
        ),
        (
            "greenfield_apply_email_event",
            "8, '00000000-0000-4000-8000-000000000001'::uuid, 1, "
            "'00000000-0000-4000-8000-000000000002'::uuid, 0, 0",
        ),
        (
            "greenfield_begin_inbox_effect",
            "8, '00000000-0000-4000-8000-000000000001'::uuid, 1, "
            "'00000000-0000-4000-8000-000000000002'::uuid, 0, 1",
        ),
        (
            "greenfield_finish_inbox",
            "8, '00000000-0000-4000-8000-000000000001'::uuid, 1, "
            "'00000000-0000-4000-8000-000000000002'::uuid, 0, 1, "
            "'{}'::jsonb",
        ),
        (
            "greenfield_fail_inbox",
            "8, '00000000-0000-4000-8000-000000000001'::uuid, 1, "
            "'00000000-0000-4000-8000-000000000002'::uuid, 0, 1, "
            "'inbox.failed', 'Inbox processing failed'",
        ),
        (
            "greenfield_reap_inbox",
            "8, '00000000-0000-4000-8000-000000000001'::uuid, 1, 1",
        ),
    ],
)
def test_phase2_worker_functions_are_present_but_fail_before_mutation(
    alembic_runner,
    empty_schema,
    routine_name: str,
    arguments: str,
) -> None:
    alembic_runner.upgrade(empty_schema, "20260716_0006")

    with pytest.raises(
        psycopg.errors.RaiseException,
        match="phase2_worker_authority_unavailable",
    ):
        empty_schema.execute(f"SELECT public.{routine_name}({arguments})")

    assert empty_schema.scalar("SELECT count(*) FROM event_inbox") == 0
    assert empty_schema.scalar("SELECT count(*) FROM emails") == 0
    assert empty_schema.scalar("SELECT count(*) FROM audit_events") == 0
