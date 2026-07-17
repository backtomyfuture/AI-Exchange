from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.domain.email_state import ProcessingOutcome
from src.ingestion.legacy_adapter import LegacyProcessingAdapter
from src.ingestion.models import ChangeKind, IngressSource, ProcessingPolicy
from src.ingestion.policy import FolderScope, PolicySnapshot
from src.ingestion.repository import InboxRepository
from src.ingestion.runtime import build_ingestion_runtime
from src.ingestion.runtime_authority import (
    GREENFIELD_PIPELINE_NAME,
    GreenfieldInitializer,
    RuntimeAuthorityRepository,
    RuntimeContract,
    RuntimeInstanceRepository,
)
from src.ingestion.runtime_capability import (
    CAPABILITY_CHAIN_ROOT_HASH,
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
)


_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_HASH_D = "d" * 64


def _snapshot() -> PolicySnapshot:
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
                webhook_ids=("inbox-id",),
                sync_folder="Inbox",
                event_policy_matrix=matrix,
            ),
        )
    )


def _contract(snapshot: PolicySnapshot) -> RuntimeContract:
    from src.ingestion.runtime_authority import canonical_policy_manifest

    capability = RuntimeCapabilityManifest(
        stage=RuntimeCapabilityStage.PHASE2_INGESTION,
        schema_revision="20260716_0006",
        schema_digest=_HASH_A,
        protocol_version=1,
        minimum_build_id="build-1",
        config_hash=_HASH_B,
        adapter_hash=_HASH_C,
        policy_manifest_hash=canonical_policy_manifest(snapshot).hash,
        evidence_manifest_hash=_HASH_D,
        predecessor_hash=CAPABILITY_CHAIN_ROOT_HASH,
    )
    return RuntimeContract(
        schema_revision=capability.schema_revision,
        schema_digest=capability.schema_digest,
        protocol_version=capability.protocol_version,
        build_id=capability.minimum_build_id,
        config_hash=capability.config_hash,
        capability_manifest=capability,
    )


async def _initialize(schema) -> None:
    snapshot = _snapshot()
    pool = AsyncConnectionPool(
        conninfo=schema.maintenance_dsn,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    try:
        await GreenfieldInitializer(pool).initialize(
            8,
            _contract(snapshot),
            snapshot,
            "integration-operator",
            "fresh runtime lifecycle",
            "runtime-lifecycle-initialize",
        )
    finally:
        await pool.close()


def _runtime_settings(schema) -> SimpleNamespace:
    return SimpleNamespace(
        database_url=schema.runtime_dsn,
        EXCHANGE_ACCOUNT_ID=8,
        INGESTION_INSTANCE_ID="integration-web",
        INGESTION_LEASE_SECONDS=30,
        INGESTION_HEARTBEAT_SECONDS=10,
        INGESTION_SHUTDOWN_SECONDS=5,
    )


def _webhook_payload() -> dict[str, object]:
    return {
        "account_id": 8,
        "event": "NewMailEvent",
        "timestamp": 1_752_384_245,
        "item_id": {"id": "runtime-message-1", "changekey": "version-1"},
        "parent_folder_id": {"id": "inbox-id"},
        "message": "new mail",
    }


async def _wait_for_inbox_status(
    schema,
    inbox_id: str,
    expected_status: str,
    *,
    timeout_seconds: float = 10.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    current_status = None
    while loop.time() < deadline:
        current_status = schema.scalar(
            "SELECT status FROM event_inbox WHERE id = %s",
            (inbox_id,),
        )
        if current_status == expected_status:
            return
        await asyncio.sleep(0.05)
    safe_error_code = schema.scalar(
        "SELECT safe_error_code FROM event_inbox WHERE id = %s",
        (inbox_id,),
    )
    safe_error_summary = schema.scalar(
        "SELECT safe_error_summary FROM event_inbox WHERE id = %s",
        (inbox_id,),
    )
    pytest.fail(
        f"event_inbox {inbox_id} did not reach {expected_status!r}; "
        f"last status was {current_status!r}; "
        f"safe error was {safe_error_code!r}: {safe_error_summary!r}"
    )


@pytest.mark.asyncio
async def test_greenfield_runtime_persists_webhook_and_clean_restart_deduplicates(
    empty_schema,
) -> None:
    await bootstrap_database(empty_schema.dsn, **empty_schema.bootstrap_identity)
    await _initialize(empty_schema)
    raw_payload = _webhook_payload()
    raw_body = json.dumps(
        raw_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    first = build_ingestion_runtime(_runtime_settings(empty_schema))
    await first.start()
    try:
        assert await first.check_ready() is True
        service = first.webhook_ingress_service
        assert service is not None
        receipt = await service.accept(
            raw_body=raw_body,
            payload=raw_payload,
            header_event="NewMailEvent",
        )
        assert receipt.duplicate is False
        assert (await first.queue_stats()).pending == 1
    finally:
        await first.stop()

    assert (
        empty_schema.scalar(
            "SELECT pg_catalog.count(*) FROM event_inbox "
            "WHERE account_id = 8 AND status = 'pending'"
        )
        == 1
    )
    assert (
        empty_schema.scalar(
            "SELECT pg_catalog.count(*) FROM pipeline_runtime_instances "
            "WHERE account_id = 8 AND lifecycle = 'active'"
        )
        == 0
    )

    second = build_ingestion_runtime(_runtime_settings(empty_schema))
    await second.start()
    try:
        service = second.webhook_ingress_service
        assert service is not None
        duplicate = await service.accept(
            raw_body=raw_body,
            payload=raw_payload,
            header_event="NewMailEvent",
        )
        assert duplicate.duplicate is True
        assert (await second.queue_stats()).pending == 1
    finally:
        await second.stop()

    assert (
        empty_schema.scalar(
            "SELECT pg_catalog.count(*) FROM event_inbox WHERE account_id = 8"
        )
        == 1
    )


@pytest.mark.asyncio
async def test_phase4_runtime_processes_webhook_to_completion_with_real_postgres(
    empty_schema,
    monkeypatch,
) -> None:
    await bootstrap_database(empty_schema.dsn, **empty_schema.bootstrap_identity)
    await _initialize(empty_schema)

    exchange_get_email = AsyncMock(
        return_value={
            "id": "runtime-message-1",
            "subject": "Phase 4 integration",
            "sender": "sender@example.test",
            "body": "integration body",
            "received_at": "2026-07-17T08:00:00+00:00",
        }
    )
    get_email_status = AsyncMock(return_value="waiting_approval")
    graph_invoke = AsyncMock(return_value={"draft": "integration draft"})
    lark_send = AsyncMock(return_value=None)
    processing_context = SimpleNamespace(
        exchange_client=SimpleNamespace(get_email=exchange_get_email),
        db_manager=SimpleNamespace(get_email_status=get_email_status),
        graph=SimpleNamespace(ainvoke=graph_invoke),
        lark_app=SimpleNamespace(send_approval_card=lark_send),
    )

    async def fake_business_processor(email_data, context, **kwargs):
        before_external_effect = kwargs["before_external_effect"]
        await before_external_effect("model", 0, _HASH_A)
        await context.graph.ainvoke({"email": email_data})
        await before_external_effect("feishu", 0, _HASH_B)
        await context.lark_app.send_approval_card(email_data["id"])
        return ProcessingOutcome.PROCESSED

    guarded_processor = AsyncMock(side_effect=fake_business_processor)

    class _TestLegacyProcessingAdapter(LegacyProcessingAdapter):
        __slots__ = ()

        def __init__(self, context, *, legacy_account_id: int):
            super().__init__(
                context,
                legacy_account_id=legacy_account_id,
                guarded_processor=guarded_processor,
            )

    monkeypatch.setattr(
        "src.ingestion.legacy_adapter.LegacyProcessingAdapter",
        _TestLegacyProcessingAdapter,
    )
    settings = _runtime_settings(empty_schema)
    settings.DURABLE_INBOX_ENABLED = True
    runtime = build_ingestion_runtime(
        settings,
        processing_context=processing_context,
    )
    raw_payload = _webhook_payload()
    raw_body = json.dumps(
        raw_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    await runtime.start()
    try:
        assert runtime.processing_ready is True
        assert await runtime.check_ready() is True
        service = runtime.webhook_ingress_service
        assert service is not None
        receipt = await service.accept(
            raw_body=raw_body,
            payload=raw_payload,
            header_event="NewMailEvent",
        )
        assert receipt.duplicate is False
        await _wait_for_inbox_status(empty_schema, receipt.inbox_id, "completed")
    finally:
        await runtime.stop()

    assert (
        empty_schema.scalar(
            "SELECT status FROM event_inbox WHERE id = %s",
            (receipt.inbox_id,),
        )
        == "completed"
    )
    assert empty_schema.scalar(
        "SELECT processing_started_at IS NOT NULL FROM event_inbox WHERE id = %s",
        (receipt.inbox_id,),
    ) is True
    assert empty_schema.scalar(
        "SELECT effect_started_at IS NOT NULL FROM event_inbox WHERE id = %s",
        (receipt.inbox_id,),
    ) is True
    assert empty_schema.scalar(
        "SELECT lease_owner IS NULL AND lease_until IS NULL "
        "AND lease_session_id IS NULL FROM event_inbox WHERE id = %s",
        (receipt.inbox_id,),
    ) is True
    assert (
        empty_schema.scalar(
            "SELECT status FROM emails "
            "WHERE account_id = 8 AND external_email_id = %s",
            ("runtime-message-1",),
        )
        == "waiting_approval"
    )
    assert empty_schema.scalar(
        "SELECT external_effects_started_at IS NOT NULL FROM emails "
        "WHERE account_id = 8 AND external_email_id = %s",
        ("runtime-message-1",),
    ) is True
    assert (
        empty_schema.scalar(
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE account_id = 8 AND action = 'email.processing_attempt'"
        )
        == 1
    )
    assert (
        empty_schema.scalar(
            "SELECT pg_catalog.count(*) FROM audit_events "
            "WHERE account_id = 8 AND action = 'email.processing_completed'"
        )
        == 1
    )
    assert (
        empty_schema.scalar(
            "SELECT pg_catalog.count(*) FROM pipeline_runtime_instances "
            "WHERE account_id = 8 AND lifecycle = 'active'"
        )
        == 0
    )
    exchange_get_email.assert_awaited_once_with("runtime-message-1")
    guarded_processor.assert_awaited_once()
    graph_invoke.assert_awaited_once()
    lark_send.assert_awaited_once_with("runtime-message-1")
    get_email_status.assert_awaited_once_with("runtime-message-1")


@pytest.mark.asyncio
async def test_expired_runtime_session_cannot_begin_processing_effect(
    empty_schema,
) -> None:
    await bootstrap_database(empty_schema.dsn, **empty_schema.bootstrap_identity)
    await _initialize(empty_schema)
    raw_payload = _webhook_payload()
    raw_body = json.dumps(
        raw_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    ingress_runtime = build_ingestion_runtime(_runtime_settings(empty_schema))
    await ingress_runtime.start()
    try:
        service = ingress_runtime.webhook_ingress_service
        assert service is not None
        receipt = await service.accept(
            raw_body=raw_body,
            payload=raw_payload,
            header_event="NewMailEvent",
        )
    finally:
        await ingress_runtime.stop()

    pool = AsyncConnectionPool(
        conninfo=empty_schema.runtime_dsn,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    try:
        authority = await RuntimeAuthorityRepository(pool).get(8)
        assert authority is not None
        worker_session_id = str(uuid4())
        await RuntimeInstanceRepository(pool).register(
            authority,
            _contract(_snapshot()),
            "effect-worker",
            worker_session_id,
            30,
        )
        repository = InboxRepository(pool)
        leases = await repository.claim_batch(
            "effect-worker",
            worker_session_id,
            (GREENFIELD_PIPELINE_NAME,),
            1,
            30,
        )
        assert len(leases) == 1
        lease = leases[0]
        assert lease.id == receipt.inbox_id
        application = await repository.apply_email_event(lease)
        assert application.should_process is True
        assert empty_schema.scalar(
            "SELECT effect_started_at IS NULL FROM event_inbox WHERE id = %s",
            (receipt.inbox_id,),
        ) is True
        assert empty_schema.scalar(
            "SELECT external_effects_started_at IS NULL FROM emails "
            "WHERE id = %s",
            (application.email_id,),
        ) is True

        empty_schema.execute(
            "UPDATE pipeline_runtime_instances SET "
            "lease_version = lease_version + 1, "
            "lease_until = heartbeat_at + INTERVAL '1 microsecond', "
            "updated_at = pg_catalog.clock_timestamp() "
            "WHERE session_id = %s",
            (worker_session_id,),
        )
        assert empty_schema.scalar(
            "SELECT lifecycle = 'active' "
            "AND lease_until <= pg_catalog.clock_timestamp() "
            "FROM pipeline_runtime_instances WHERE session_id = %s",
            (worker_session_id,),
        ) is True
        assert empty_schema.scalar(
            "SELECT lease_until > pg_catalog.clock_timestamp() "
            "FROM event_inbox WHERE id = %s",
            (receipt.inbox_id,),
        ) is True

        assert (
            await repository.begin_processing_effect(
                lease,
                application.email_id,
                application.version,
            )
            is False
        )
        assert empty_schema.scalar(
            "SELECT effect_started_at IS NULL FROM event_inbox WHERE id = %s",
            (receipt.inbox_id,),
        ) is True
        assert empty_schema.scalar(
            "SELECT external_effects_started_at IS NULL FROM emails "
            "WHERE id = %s",
            (application.email_id,),
        ) is True
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_expired_unlinked_lease_with_existing_email_is_recovered(
    empty_schema,
) -> None:
    await bootstrap_database(empty_schema.dsn, **empty_schema.bootstrap_identity)
    await _initialize(empty_schema)
    raw_payload = _webhook_payload()
    raw_body = json.dumps(
        raw_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    ingress_runtime = build_ingestion_runtime(_runtime_settings(empty_schema))
    await ingress_runtime.start()
    try:
        service = ingress_runtime.webhook_ingress_service
        assert service is not None
        receipt = await service.accept(
            raw_body=raw_body,
            payload=raw_payload,
            header_event="NewMailEvent",
        )
    finally:
        await ingress_runtime.stop()

    pool = AsyncConnectionPool(
        conninfo=empty_schema.runtime_dsn,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    try:
        authority = await RuntimeAuthorityRepository(pool).get(8)
        assert authority is not None
        worker_session_id = str(uuid4())
        await RuntimeInstanceRepository(pool).register(
            authority,
            _contract(_snapshot()),
            "renewal-worker",
            worker_session_id,
            30,
        )
        empty_schema.execute(
            "INSERT INTO emails ("
            "id, account_id, external_email_id, source_folder_key, status, "
            "owner_generation, owner_fencing_token, owner_authority_epoch, "
            "owner_capability_hash, is_read"
            ") VALUES (%s, 8, 'runtime-message-1', 'INBOX', 'ingested', "
            "1, 1, 1, %s, false)",
            (str(uuid4()), authority.capability_hash),
        )
        assert empty_schema.scalar(
            "SELECT processing_inbox_id IS NULL FROM emails "
            "WHERE account_id = 8 AND external_email_id = 'runtime-message-1'"
        ) is True

        repository = InboxRepository(pool)
        assert (
            await repository.claim_batch(
                "renewal-worker",
                str(uuid4()),
                (GREENFIELD_PIPELINE_NAME,),
                1,
                3,
            )
            == []
        )
        leases = await repository.claim_batch(
            "renewal-worker",
            worker_session_id,
            (GREENFIELD_PIPELINE_NAME,),
            1,
            3,
        )
        assert len(leases) == 1
        lease = leases[0]
        assert lease.id == receipt.inbox_id
        assert lease.lease_session_id == worker_session_id

        forged_session_lease = replace(lease, lease_session_id=str(uuid4()))
        assert await repository.renew(forged_session_lease, 3) is None
        empty_schema.execute(
            "UPDATE pipeline_runtime_instances SET "
            "lease_version = lease_version + 1, "
            "lease_until = heartbeat_at + INTERVAL '1 microsecond', "
            "updated_at = pg_catalog.clock_timestamp() "
            "WHERE session_id = %s",
            (worker_session_id,),
        )
        assert empty_schema.scalar(
            "SELECT lifecycle = 'active' "
            "AND lease_until <= pg_catalog.clock_timestamp() "
            "FROM pipeline_runtime_instances WHERE session_id = %s",
            (worker_session_id,),
        ) is True
        assert empty_schema.scalar(
            "SELECT lease_until > pg_catalog.clock_timestamp() "
            "FROM event_inbox WHERE id = %s",
            (receipt.inbox_id,),
        ) is True
        assert await repository.renew(lease, 3) is None

        empty_schema.execute(
            "UPDATE event_inbox SET "
            "lease_until = processing_started_at + INTERVAL '1 microsecond', "
            "updated_at = pg_catalog.clock_timestamp() WHERE id = %s",
            (receipt.inbox_id,),
        )
        assert empty_schema.scalar(
            "SELECT lease_until > received_at "
            "AND lease_until <= pg_catalog.clock_timestamp() "
            "FROM event_inbox WHERE id = %s",
            (receipt.inbox_id,),
        ) is True

        assert await repository.recover_expired_leases(1) == 1
    finally:
        await pool.close()

    assert (
        empty_schema.scalar(
            "SELECT status FROM event_inbox WHERE id = %s",
            (receipt.inbox_id,),
        )
        != "leased"
    )
    assert empty_schema.scalar(
        "SELECT lease_owner IS NULL AND lease_until IS NULL "
        "AND lease_session_id IS NULL "
        "FROM event_inbox WHERE id = %s",
        (receipt.inbox_id,),
    ) is True
