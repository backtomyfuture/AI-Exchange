from __future__ import annotations

from uuid import uuid4

import pytest
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.ingestion.models import ChangeKind, IngressSource, ProcessingPolicy, SyncChange
from src.ingestion.normalization import normalize_sync_change
from src.ingestion.polling import (
    GreenfieldSyncPageWriter,
    PollingCursorCheckpoint,
    PollingCursorUnavailable,
    PollingPageCommitResult,
    PostgresPollingCursorStore,
)
from src.ingestion.policy import FolderScope, PolicySnapshot
from src.ingestion.runtime_authority import (
    GreenfieldInitializer,
    RuntimeAuthorityRepository,
    RuntimeContract,
    RuntimeInstanceLease,
    RuntimeInstanceRepository,
    canonical_policy_manifest,
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
        (IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE): ProcessingPolicy.FULL,
        (IngressSource.WEBHOOK, "CreatedEvent", ChangeKind.CREATE): ProcessingPolicy.IGNORED,
        (IngressSource.WEBHOOK, "ModifiedEvent", ChangeKind.UPDATE): ProcessingPolicy.METADATA_ONLY,
        (IngressSource.WEBHOOK, "DeletedEvent", ChangeKind.DELETE): ProcessingPolicy.METADATA_ONLY,
        (IngressSource.SYNC, "create", ChangeKind.CREATE): ProcessingPolicy.FULL,
        (IngressSource.SYNC, "update", ChangeKind.UPDATE): ProcessingPolicy.METADATA_ONLY,
        (IngressSource.SYNC, "delete", ChangeKind.DELETE): ProcessingPolicy.METADATA_ONLY,
    }
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("legacy-inbox-id",),
                sync_folder="INBOX",
                event_policy_matrix=matrix,
            ),
        )
    )


def _contract(snapshot: PolicySnapshot) -> RuntimeContract:
    capability = RuntimeCapabilityManifest(
        stage=RuntimeCapabilityStage.PHASE2_INGESTION,
        schema_revision="20260716_0006",
        schema_digest=_HASH_A,
        protocol_version=1,
        minimum_build_id="polling-store-test",
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


async def _initialize(schema) -> PolicySnapshot:
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
            "polling cursor store",
            "polling-store-initialize",
        )
    finally:
        await pool.close()
    return snapshot


def _event(
    external_email_id: str,
    cursor: str,
    *,
    source_version: str | None = None,
):
    return normalize_sync_change(
        8,
        "INBOX",
        cursor,
        SyncChange(
            kind=ChangeKind.CREATE,
            external_email_id=external_email_id,
            item={},
            source_version=source_version,
        ),
        processing_policy=ProcessingPolicy.FULL,
    )


class _LeasePageCommitter:
    """Integration adapter: production binds this to the session renewer."""

    def __init__(
        self,
        writer: GreenfieldSyncPageWriter,
        lease: RuntimeInstanceLease,
    ) -> None:
        self._writer = writer
        self._lease = lease

    async def commit_page(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
        events: object,
        *,
        activation: bool,
    ) -> PollingPageCommitResult:
        return await self._writer.commit_page(
            self._lease,
            checkpoint,
            next_cursor,
            tuple(events),
            activation=activation,
        )


async def _register_runtime_lease(
    pool: AsyncConnectionPool,
    snapshot: PolicySnapshot,
) -> RuntimeInstanceLease:
    authority = await RuntimeAuthorityRepository(pool).get(8)
    assert authority is not None
    return await RuntimeInstanceRepository(pool).register(
        authority,
        _contract(snapshot),
        "polling-store-test",
        str(uuid4()),
        30,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_polling_cursor_store_uses_the_runtime_role_and_commits_cursor_with_inbox(
    empty_schema,
) -> None:
    """Real PostgreSQL proves activation, atomic inbox commit, and rollback."""

    await bootstrap_database(empty_schema.dsn, **empty_schema.bootstrap_identity)
    snapshot = await _initialize(empty_schema)
    pool = AsyncConnectionPool(
        conninfo=empty_schema.runtime_dsn,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"row_factory": dict_row},
    )
    await pool.open()
    try:
        lease = await _register_runtime_lease(pool, snapshot)
        writer = GreenfieldSyncPageWriter(pool)
        store = PostgresPollingCursorStore(
            pool,
            _LeasePageCommitter(writer, lease),
            account_id=8,
            folder="INBOX",
        )

        initial = await store.load(8, "INBOX")
        assert initial == PollingCursorCheckpoint(cursor=None, version=0)
        await store.commit_activation_boundary(initial, "activation-cursor")
        assert empty_schema.scalar("SELECT count(*) FROM event_inbox") == 0
        assert empty_schema.scalar(
            "SELECT jsonb_build_array(cursor, status, version) "
            "FROM sync_cursors WHERE account_id = 8 AND folder_key = 'INBOX'"
        ) == ["activation-cursor", "active", 1]

        active = await store.load(8, "INBOX")
        event = _event("polling-message-1", "delta-cursor-1")
        await store.commit_delta(active, "delta-cursor-1", (event,))
        assert empty_schema.scalar(
            "SELECT jsonb_build_array(cursor, status, version) "
            "FROM sync_cursors WHERE account_id = 8 AND folder_key = 'INBOX'"
        ) == ["delta-cursor-1", "active", 2]
        assert empty_schema.scalar(
            "SELECT count(*) FROM event_inbox WHERE source = 'sync' "
            "AND external_email_id = 'polling-message-1'"
        ) == 1

        checkpoint = await store.load(8, "INBOX")
        with pytest.raises(PollingCursorUnavailable):
            await store.commit_delta(
                checkpoint,
                "delta-cursor-2",
                (
                    _event(
                        "polling-message-2",
                        "delta-cursor-2",
                        source_version="one",
                    ),
                    _event(
                        "polling-message-2",
                        "delta-cursor-2",
                        source_version="two",
                    ),
                ),
            )

        assert empty_schema.scalar(
            "SELECT cursor FROM sync_cursors "
            "WHERE account_id = 8 AND folder_key = 'INBOX'"
        ) == "delta-cursor-1"
        assert empty_schema.scalar(
            "SELECT count(*) FROM event_inbox "
            "WHERE external_email_id = 'polling-message-2'"
        ) == 0

        async with pool.connection() as connection:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                await connection.execute(
                    "UPDATE public.sync_cursors SET cursor = 'forbidden' "
                    "WHERE account_id = 8 AND folder_key = 'INBOX'"
                )
            await connection.rollback()
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                await connection.execute(
                    "INSERT INTO public.event_inbox (id, authority_epoch) "
                    "VALUES (pg_catalog.gen_random_uuid(), 1)"
                )
            await connection.rollback()
    finally:
        await pool.close()
