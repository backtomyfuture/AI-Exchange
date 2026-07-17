from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.ingestion.models import ChangeKind, IngressSource, ProcessingPolicy
from src.ingestion.policy import FolderScope, PolicySnapshot
from src.ingestion.runtime import build_ingestion_runtime
from src.ingestion.runtime_authority import (
    GreenfieldInitializer,
    RuntimeContract,
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
