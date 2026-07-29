from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.ingestion import runtime as runtime_module
from src.domain.email_state import PipelineGenerationState
from src.ingestion.models import (
    ChangeKind,
    IngressReceipt,
    IngressSource,
    NormalizedIngressEvent,
    PipelineGeneration,
    ProcessingPolicy,
)
from src.ingestion.policy import FolderScope, PolicySnapshot
from src.ingestion.runtime import (
    GreenfieldWebhookWriter,
    IngestionRuntime,
    RuntimeManifestRepository,
    RuntimeShutdownError,
    RuntimeUnavailableError,
    build_ingestion_runtime,
)
from src.ingestion.runtime_authority import (
    RuntimeAuthority,
    RuntimeAuthorityState,
    RuntimeContract,
    RuntimeInstanceLease,
    RuntimeInstanceLifecycle,
    RuntimeWorkload,
    canonical_policy_manifest,
)
from src.ingestion.runtime_capability import (
    CAPABILITY_CHAIN_ROOT_HASH,
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
)
from src.ingestion.webhook import WebhookIngressService, WebhookIngressUnavailable
from src.ingestion.worker import DurableInboxWorker


_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_HASH_D = "d" * 64
_INITIALIZATION_ID = "00000000-0000-4000-8000-000000000001"
_INBOX_ID = "00000000-0000-4000-8000-000000000002"


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
                webhook_ids=("INBOX",),
                sync_folder="Inbox",
                event_policy_matrix=matrix,
            ),
        )
    )


def _contract(snapshot: PolicySnapshot | None = None) -> RuntimeContract:
    policy_hash = canonical_policy_manifest(snapshot or _snapshot()).hash
    capability = RuntimeCapabilityManifest(
        stage=RuntimeCapabilityStage.PHASE2_INGESTION,
        schema_revision="20260716_0006",
        schema_digest=_HASH_A,
        protocol_version=1,
        minimum_build_id="build-1",
        config_hash=_HASH_B,
        adapter_hash=_HASH_C,
        policy_manifest_hash=policy_hash,
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


def _authority(
    state: RuntimeAuthorityState = RuntimeAuthorityState.INGEST_ONLY,
) -> RuntimeAuthority:
    contract = _contract()
    return RuntimeAuthority(
        account_id=8,
        state=state,
        generation=1,
        fencing_token=1,
        pipeline_name="durable_v1",
        authority_epoch=1,
        version=1,
        schema_revision=contract.schema_revision,
        protocol_version=contract.protocol_version,
        build_id=contract.build_id,
        config_hash=contract.config_hash,
        capability_hash=contract.capability_manifest.capability_hash,
        policy_manifest_hash=contract.capability_manifest.policy_manifest_hash,
        initialization_id=_INITIALIZATION_ID,
        updated_at=datetime.now(UTC),
    )


def _lease(
    *,
    session_id: str,
    lease_version: int = 1,
    accepted_count: int = 0,
    rejected_count: int = 0,
    lifecycle: RuntimeInstanceLifecycle = RuntimeInstanceLifecycle.ACTIVE,
) -> RuntimeInstanceLease:
    authority = _authority()
    now = datetime.now(UTC)
    return RuntimeInstanceLease(
        account_id=authority.account_id,
        workload=RuntimeWorkload.WEB,
        instance_id="ai-exchange-web",
        session_id=session_id,
        generation=authority.generation,
        fencing_token=authority.fencing_token,
        authority_epoch=authority.authority_epoch,
        capability_hash=authority.capability_hash,
        schema_revision=authority.schema_revision,
        protocol_version=authority.protocol_version,
        build_id=authority.build_id,
        config_hash=authority.config_hash,
        lifecycle=lifecycle,
        lease_version=lease_version,
        accepted_count=accepted_count,
        rejected_count=rejected_count,
        heartbeat_at=now,
        lease_until=now + timedelta(seconds=30),
    )


def _event() -> NormalizedIngressEvent:
    return NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id="message-1",
        folder="INBOX",
        source_version="version-1",
        dedupe_key=_HASH_A,
        payload=MappingProxyType({"event": "NewMailEvent"}),
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=datetime.now(UTC),
    )


class _Pool:
    def __init__(
        self,
        events: list[str],
        *,
        open_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.events = events
        self.open_error = open_error
        self.close_error = close_error

    async def open(self) -> None:
        self.events.append("pool.open")
        if self.open_error is not None:
            raise self.open_error

    async def close(self) -> None:
        self.events.append("pool.close")
        if self.close_error is not None:
            raise self.close_error


class _AuthorityRepository:
    def __init__(
        self,
        events: list[str],
        authority: RuntimeAuthority | BaseException | None = None,
    ) -> None:
        self.events = events
        self.authority = authority if authority is not None else _authority()

    async def get(self, account_id: int) -> RuntimeAuthority:
        self.events.append(f"authority.get:{account_id}")
        if isinstance(self.authority, BaseException):
            raise self.authority
        return self.authority


class _ManifestRepository:
    def __init__(
        self,
        events: list[str],
        result: tuple[RuntimeContract, PolicySnapshot] | BaseException | None = None,
    ) -> None:
        self.events = events
        self.result = result if result is not None else (_contract(), _snapshot())

    async def load(
        self,
        authority: RuntimeAuthority,
    ) -> tuple[RuntimeContract, PolicySnapshot]:
        self.events.append("manifest.load")
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class _InstanceRepository:
    def __init__(
        self,
        events: list[str],
        *,
        heartbeat_error: Exception | None = None,
        drain_error: Exception | None = None,
    ):
        self.events = events
        self.heartbeat_error = heartbeat_error
        self.drain_error = drain_error
        self.heartbeat_calls: list[tuple[int, int, int]] = []

    async def register(
        self,
        authority: RuntimeAuthority,
        runtime_contract: RuntimeContract,
        instance_id: str,
        session_id: str,
        lease_seconds: int,
    ) -> RuntimeInstanceLease:
        self.events.append("instance.register")
        return _lease(session_id=session_id)

    async def heartbeat(
        self,
        lease: RuntimeInstanceLease,
        accepted_count: int,
        rejected_count: int,
        lease_seconds: int,
    ) -> RuntimeInstanceLease:
        self.events.append("instance.heartbeat")
        self.heartbeat_calls.append(
            (lease.lease_version, accepted_count, rejected_count)
        )
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        return _lease(
            session_id=lease.session_id,
            lease_version=lease.lease_version + 1,
            accepted_count=accepted_count,
            rejected_count=rejected_count,
        )

    async def drain(self, lease: RuntimeInstanceLease) -> RuntimeInstanceLease:
        self.events.append("instance.drain")
        if self.drain_error is not None:
            raise self.drain_error
        return _lease(
            session_id=lease.session_id,
            lease_version=lease.lease_version + 1,
            accepted_count=lease.accepted_count,
            rejected_count=lease.rejected_count,
            lifecycle=RuntimeInstanceLifecycle.DRAINING,
        )


class _Writer:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.block = False
        self.calls: list[tuple[RuntimeInstanceLease, NormalizedIngressEvent]] = []

    async def insert(
        self,
        lease: RuntimeInstanceLease,
        event: NormalizedIngressEvent,
    ) -> IngressReceipt:
        self.events.append("writer.insert")
        self.calls.append((lease, event))
        self.entered.set()
        if self.block:
            await self.release.wait()
        return IngressReceipt(inbox_id=_INBOX_ID, duplicate=False)


class _RecoveryRepository:
    def __init__(
        self,
        events: list[str],
        *,
        error: BaseException | None = None,
        block_first: bool = False,
    ) -> None:
        self.events = events
        self.error = error
        self.block_first = block_first
        self.calls: list[int] = []
        self.recovered_twice = asyncio.Event()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def recover_expired_leases(self, limit: int) -> int:
        self.events.append(f"recovery.recover:{limit}")
        self.calls.append(limit)
        self.entered.set()
        if self.block_first and len(self.calls) == 1:
            await self.release.wait()
        if len(self.calls) >= 2:
            self.recovered_twice.set()
        if self.error is not None:
            raise self.error
        return 0


class _ProcessingWorker:
    def __init__(
        self,
        events: list[str],
        *,
        start_error: BaseException | None = None,
        stop_error: BaseException | None = None,
        block_stop: bool = False,
    ) -> None:
        self.events = events
        self.start_error = start_error
        self.stop_error = stop_error
        self.block_stop = block_stop
        self.stop_entered = asyncio.Event()
        self.stop_release = asyncio.Event()
        self.ready = False

    async def start(self) -> None:
        self.events.append("worker.start")
        if self.start_error is not None:
            raise self.start_error
        self.ready = True

    async def stop(self, grace_seconds: float = 30.0) -> None:
        self.events.append(f"worker.stop:{grace_seconds}")
        self.ready = False
        self.stop_entered.set()
        if self.block_stop:
            await self.stop_release.wait()
        if self.stop_error is not None:
            raise self.stop_error


class _PollingRuntime:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.live = False
        self.ready = False

    async def start(self) -> None:
        self.events.append("polling.start")
        self.live = True
        self.ready = True

    async def stop(self) -> None:
        self.events.append("polling.stop")
        self.live = False
        self.ready = False


class _ActivatingPollingRuntime(_PollingRuntime):
    async def start(self) -> None:
        self.events.append("polling.start")
        self.live = True


class _AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args) -> bool:
        return False


class _Cursor:
    def __init__(self, *, one: object = None, many: list[object] | None = None):
        self.one = one
        self.many = many or []

    async def fetchone(self):
        return self.one

    async def fetchall(self):
        return self.many


class _Connection:
    def __init__(self, cursors: list[_Cursor]) -> None:
        self.cursors = cursors
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _AsyncContext:
        return _AsyncContext(self)

    async def execute(self, query: str, params: tuple[object, ...]) -> _Cursor:
        self.calls.append((query, params))
        return self.cursors.pop(0)


class _DatabasePool:
    def __init__(self, connection: _Connection) -> None:
        self.connection_value = connection

    def connection(self) -> _AsyncContext:
        return _AsyncContext(self.connection_value)


def _runtime(
    events: list[str],
    *,
    authority_repository: _AuthorityRepository | None = None,
    manifest_repository: _ManifestRepository | None = None,
    instance_repository: _InstanceRepository | None = None,
    writer: _Writer | None = None,
    pool: _Pool | None = None,
    processing_worker: _ProcessingWorker | None = None,
    recovery_repository: _RecoveryRepository | None = None,
    polling_runtime_factory=None,
    fail_stop=None,
) -> tuple[IngestionRuntime, _InstanceRepository, _Writer]:
    instances = instance_repository or _InstanceRepository(events)
    sink = writer or _Writer(events)
    runtime = IngestionRuntime(
        account_id=8,
        pool=pool or _Pool(events),
        authority_repository=authority_repository or _AuthorityRepository(events),
        manifest_repository=manifest_repository or _ManifestRepository(events),
        instance_repository=instances,
        webhook_writer=sink,
        instance_id="ai-exchange-web",
        session_id=str(uuid4()),
        lease_seconds=30,
        heartbeat_seconds=10,
        shutdown_seconds=1,
        processing_worker=processing_worker,
        inbox_recovery_repository=recovery_repository,
        polling_runtime_factory=polling_runtime_factory,
        fail_stop=fail_stop,
    )
    return runtime, instances, sink


@pytest.mark.asyncio
async def test_start_publishes_only_the_session_bound_webhook_runtime() -> None:
    events: list[str] = []
    runtime, _instances, _writer = _runtime(events)

    await runtime.start()

    assert runtime.ready is True
    assert isinstance(runtime.webhook_ingress_service, WebhookIngressService)
    assert events == [
        "pool.open",
        "authority.get:8",
        "manifest.load",
        "instance.register",
    ]

    await runtime.stop()
    assert events[-2:] == ["instance.drain", "pool.close"]


@pytest.mark.asyncio
async def test_start_recovers_then_starts_one_processing_runtime_before_ready() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
    )

    await runtime.start()

    assert runtime.processing_ready is True
    assert runtime.ready is True
    assert recovery.calls == [runtime_module._RECOVERY_BATCH_LIMIT]
    assert events[:6] == [
        "pool.open",
        "authority.get:8",
        "manifest.load",
        "instance.register",
        f"recovery.recover:{runtime_module._RECOVERY_BATCH_LIMIT}",
        "worker.start",
    ]
    recovery_tasks = [
        task
        for task in asyncio.all_tasks()
        if task.get_name() == "durable-inbox-expired-lease-recovery" and not task.done()
    ]
    assert len(recovery_tasks) == 1

    cancel_recovery = runtime._cancel_recovery

    async def tracked_cancel_recovery() -> None:
        events.append("recovery.stop")
        await cancel_recovery()

    runtime._cancel_recovery = tracked_cancel_recovery  # type: ignore[method-assign]
    await runtime.stop()

    assert runtime.processing_ready is False
    assert events.index("recovery.stop") < events.index("worker.stop:1.0")
    assert events.index("worker.start") < events.index("worker.stop:1.0")
    assert events.index("worker.stop:1.0") < events.index("instance.drain")
    assert events.index("instance.drain") < events.index("pool.close")


@pytest.mark.asyncio
async def test_start_and_stop_own_polling_before_runtime_readiness() -> None:
    events: list[str] = []
    poller = _PollingRuntime(events)
    snapshots: list[PolicySnapshot] = []
    runtime, _instances, _writer = _runtime(
        events,
        polling_runtime_factory=lambda snapshot: (
            snapshots.append(snapshot) or poller
        ),
    )

    await runtime.start()

    assert snapshots == [_snapshot()]
    assert runtime.polling_ready is True
    assert runtime.ready is True
    assert events.index("instance.register") < events.index("polling.start")

    await runtime.stop()

    assert runtime.polling_ready is False
    assert events.index("polling.stop") < events.index("instance.drain")


@pytest.mark.asyncio
async def test_live_poller_can_finish_initial_activation_without_fail_stopping_runtime() -> None:
    """Initial transient polling failure is not a lost scheduler."""

    events: list[str] = []
    poller = _ActivatingPollingRuntime(events)
    runtime, _instances, _writer = _runtime(
        events,
        polling_runtime_factory=lambda _snapshot: poller,
    )

    await runtime.start()

    assert runtime.polling_live is True
    assert runtime.polling_ready is False
    assert runtime.ready is False
    await runtime.heartbeat_once()

    poller.ready = True
    assert runtime.ready is True

    await runtime.stop()


@pytest.mark.asyncio
async def test_web_session_heartbeat_exists_while_startup_recovery_is_blocked() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events, block_first=True)
    worker = _ProcessingWorker(events)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
    )

    start_task = asyncio.create_task(runtime.start())
    await recovery.entered.wait()

    assert runtime._heartbeat_task is not None
    assert runtime._heartbeat_task.done() is False
    assert runtime.ready is False
    assert runtime.processing_ready is False
    assert runtime._state.accepting is False

    recovery.release.set()
    await start_task
    await runtime.stop()


@pytest.mark.asyncio
async def test_start_fails_closed_if_heartbeat_dies_during_recovery() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events, block_first=True)
    worker = _ProcessingWorker(events)
    instances = _InstanceRepository(
        events,
        heartbeat_error=RuntimeError("stale_session"),
    )
    runtime, _instances, _writer = _runtime(
        events,
        instance_repository=instances,
        processing_worker=worker,
        recovery_repository=recovery,
    )
    runtime._heartbeat_seconds = 0.001

    start_task = asyncio.create_task(runtime.start())
    await recovery.entered.wait()
    heartbeat = runtime._heartbeat_task
    assert heartbeat is not None
    await asyncio.wait_for(asyncio.shield(heartbeat), timeout=1)
    recovery.release.set()

    with pytest.raises(RuntimeUnavailableError, match="startup_failed"):
        await start_task

    assert runtime.ready is False
    assert runtime.processing_ready is False
    assert events.index("worker.start") < events.index("worker.stop:1.0")
    assert events.index("worker.stop:1.0") < events.index("instance.drain")
    assert events.index("instance.drain") < events.index("pool.close")


@pytest.mark.asyncio
async def test_background_heartbeat_loss_invokes_process_fail_stop() -> None:
    events: list[str] = []
    reasons: list[str] = []
    instances = _InstanceRepository(
        events,
        heartbeat_error=RuntimeError("stale_session"),
    )
    runtime, _instances, _writer = _runtime(
        events,
        instance_repository=instances,
        fail_stop=reasons.append,
    )
    runtime._heartbeat_seconds = 0.001
    await runtime.start()

    heartbeat = runtime._heartbeat_task
    assert heartbeat is not None
    await asyncio.wait_for(asyncio.shield(heartbeat), timeout=1)

    assert reasons == ["ingestion_runtime_heartbeat_lost"]
    assert runtime.ready is False
    await runtime.stop()


@pytest.mark.asyncio
async def test_worker_loss_is_escalated_by_web_session_heartbeat() -> None:
    events: list[str] = []
    reasons: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
        fail_stop=reasons.append,
    )
    runtime._heartbeat_seconds = 0.001
    await runtime.start()

    worker.ready = False
    heartbeat = runtime._heartbeat_task
    assert heartbeat is not None
    await asyncio.wait_for(asyncio.shield(heartbeat), timeout=1)

    assert reasons == ["ingestion_runtime_processing_lost"]
    assert runtime.processing_ready is False
    assert runtime.ready is False
    await runtime.stop()


@pytest.mark.asyncio
async def test_recovery_failure_is_escalated_by_web_session_heartbeat() -> None:
    events: list[str] = []
    reasons: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
        fail_stop=reasons.append,
    )
    runtime._recovery_interval_seconds = 0.001
    runtime._heartbeat_seconds = 0.001
    await runtime.start()

    recovery.error = RuntimeError("database_unavailable")
    recovery_task = runtime._recovery_task
    assert recovery_task is not None
    with pytest.raises(RuntimeError, match="database_unavailable"):
        await asyncio.wait_for(asyncio.shield(recovery_task), timeout=1)
    assert runtime.processing_ready is False
    assert runtime.ready is False

    heartbeat = runtime._heartbeat_task
    assert heartbeat is not None
    await asyncio.wait_for(asyncio.shield(heartbeat), timeout=1)
    assert reasons == ["ingestion_runtime_processing_lost"]

    recovery.error = None
    await runtime.stop()


@pytest.mark.asyncio
async def test_normal_shutdown_does_not_report_processing_loss() -> None:
    events: list[str] = []
    reasons: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
        fail_stop=reasons.append,
    )
    runtime._heartbeat_seconds = 0.001
    await runtime.start()

    await runtime.stop()

    assert reasons == []
    assert runtime.ready is False


@pytest.mark.asyncio
async def test_start_rollback_preserves_owners_when_worker_stop_is_unproved() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events, block_first=True)
    worker = _ProcessingWorker(
        events,
        stop_error=RuntimeError("worker_shutdown_unproved"),
    )
    instances = _InstanceRepository(
        events,
        heartbeat_error=RuntimeError("stale_session"),
    )
    runtime, _instances, _writer = _runtime(
        events,
        instance_repository=instances,
        processing_worker=worker,
        recovery_repository=recovery,
    )
    runtime._heartbeat_seconds = 0.001

    start_task = asyncio.create_task(runtime.start())
    await recovery.entered.wait()
    heartbeat = runtime._heartbeat_task
    assert heartbeat is not None
    await asyncio.wait_for(asyncio.shield(heartbeat), timeout=1)
    recovery.release.set()

    with pytest.raises(RuntimeShutdownError, match="startup_cleanup_incomplete"):
        await start_task

    assert runtime.ready is False
    assert runtime._stopped is False
    assert runtime._pool_open is True
    assert runtime._processing_started is True
    assert "instance.drain" not in events
    assert "pool.close" not in events

    worker.stop_error = None
    await runtime.stop()
    assert events.count("worker.stop:1.0") == 2
    assert events.index("instance.drain") < events.index("pool.close")


@pytest.mark.asyncio
async def test_recovery_loop_is_low_frequency_single_and_bounded() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
    )

    runtime._recovery_interval_seconds = 0.001
    await runtime.start()
    await asyncio.wait_for(recovery.recovered_twice.wait(), timeout=1)

    assert runtime.processing_ready is True
    assert len(recovery.calls) >= 2
    assert set(recovery.calls) == {runtime_module._RECOVERY_BATCH_LIMIT}
    assert (
        len(
            [
                task
                for task in asyncio.all_tasks()
                if task.get_name() == "durable-inbox-expired-lease-recovery"
                and not task.done()
            ]
        )
        == 1
    )

    await runtime.stop()


@pytest.mark.asyncio
async def test_processing_readiness_fails_closed_when_worker_is_not_live() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
    )

    await runtime.start()
    assert runtime.ready is True
    worker.ready = False

    assert runtime.processing_ready is False
    assert runtime.ready is False

    await runtime.stop()


@pytest.mark.asyncio
async def test_processing_start_failure_strictly_unwinds_every_started_owner() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events, start_error=RuntimeError("worker_failed"))
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
    )

    with pytest.raises(RuntimeUnavailableError, match="startup_failed"):
        await runtime.start()

    assert runtime.ready is False
    assert runtime.processing_ready is False
    assert runtime.webhook_ingress_service is None
    assert events == [
        "pool.open",
        "authority.get:8",
        "manifest.load",
        "instance.register",
        f"recovery.recover:{runtime_module._RECOVERY_BATCH_LIMIT}",
        "worker.start",
        "worker.stop:1.0",
        "instance.drain",
        "pool.close",
    ]


@pytest.mark.asyncio
async def test_webhook_commit_and_heartbeat_share_one_session_cas_gate() -> None:
    events: list[str] = []
    writer = _Writer(events)
    writer.block = True
    runtime, instances, _writer = _runtime(events, writer=writer)
    await runtime.start()

    insert_task = asyncio.create_task(runtime.webhook_inbox.insert(_event(), 1, 1))
    await writer.entered.wait()
    heartbeat_task = asyncio.create_task(runtime.heartbeat_once())
    await asyncio.sleep(0)

    assert instances.heartbeat_calls == [(1, 0, 0)]

    writer.release.set()
    receipt = await insert_task
    await heartbeat_task

    assert receipt == IngressReceipt(inbox_id=_INBOX_ID, duplicate=False)
    assert instances.heartbeat_calls == [(1, 0, 0), (2, 1, 0)]
    assert writer.calls[0][0].lease_version == 2
    assert runtime.lease is not None and runtime.lease.lease_version == 3
    await runtime.stop()


@pytest.mark.asyncio
async def test_heartbeat_authority_loss_disables_readiness_and_new_intake() -> None:
    events: list[str] = []
    instances = _InstanceRepository(
        events,
        heartbeat_error=RuntimeError("stale_session"),
    )
    runtime, _instances, _writer = _runtime(
        events,
        instance_repository=instances,
    )
    await runtime.start()

    with pytest.raises(RuntimeUnavailableError, match="heartbeat_failed"):
        await runtime.heartbeat_once()

    assert runtime.ready is False
    with pytest.raises(WebhookIngressUnavailable):
        await runtime.webhook_inbox.insert(_event(), 1, 1)
    await runtime.stop()


@pytest.mark.asyncio
async def test_start_failure_unwinds_the_owned_business_pool() -> None:
    events: list[str] = []
    runtime, _instances, _writer = _runtime(
        events,
        manifest_repository=_ManifestRepository(
            events,
            result=RuntimeError("invalid_manifest"),
        ),
    )

    with pytest.raises(RuntimeUnavailableError, match="startup_failed"):
        await runtime.start()

    assert runtime.ready is False
    assert runtime.webhook_ingress_service is None
    assert events == [
        "pool.open",
        "authority.get:8",
        "manifest.load",
        "pool.close",
    ]
    with pytest.raises(RuntimeUnavailableError, match="runtime_not_startable"):
        await runtime.start()


@pytest.mark.asyncio
async def test_pool_open_failure_still_attempts_strict_pool_rollback() -> None:
    events: list[str] = []
    runtime, _instances, _writer = _runtime(
        events,
        pool=_Pool(events, open_error=RuntimeError("open_failed")),
    )

    with pytest.raises(RuntimeUnavailableError, match="startup_failed"):
        await runtime.start()

    assert events == ["pool.open", "pool.close"]
    assert runtime.ready is False


@pytest.mark.asyncio
async def test_partial_pool_open_cleanup_failure_remains_retryable() -> None:
    events: list[str] = []
    pool = _Pool(
        events,
        open_error=RuntimeError("open_failed"),
        close_error=RuntimeError("close_unproved"),
    )
    runtime, _instances, _writer = _runtime(events, pool=pool)

    with pytest.raises(RuntimeShutdownError, match="startup_cleanup_incomplete"):
        await runtime.start()

    assert runtime._pool_open_attempted is True
    assert runtime._stopped is False
    assert events == ["pool.open", "pool.close"]

    pool.close_error = None
    await runtime.stop()

    assert events == ["pool.open", "pool.close", "pool.close"]
    assert runtime._pool_open_attempted is False
    assert runtime._stopped is True


@pytest.mark.asyncio
async def test_invalid_registered_lease_cleanup_failure_retains_retry_authority() -> (
    None
):
    events: list[str] = []

    class InvalidRegistrationRepository(_InstanceRepository):
        async def register(
            self,
            authority: RuntimeAuthority,
            runtime_contract: RuntimeContract,
            instance_id: str,
            session_id: str,
            lease_seconds: int,
        ) -> RuntimeInstanceLease:
            del authority, runtime_contract, instance_id, lease_seconds
            self.events.append("instance.register")
            return replace(_lease(session_id=session_id), account_id=9)

    instances = InvalidRegistrationRepository(
        events,
        drain_error=RuntimeError("drain_unproved"),
    )
    runtime, _instances, _writer = _runtime(
        events,
        instance_repository=instances,
    )

    with pytest.raises(RuntimeShutdownError, match="startup_cleanup_incomplete"):
        await runtime.start()

    assert runtime._state.lease is None
    assert runtime._registered_lease is not None
    assert runtime._registered_lease.account_id == 9
    assert runtime._pool_open_attempted is True
    assert runtime._stopped is False
    assert "pool.close" not in events

    instances.drain_error = None
    await runtime.stop()

    assert events[-2:] == ["instance.drain", "pool.close"]
    assert runtime._registered_lease is None
    assert runtime._pool_open_attempted is False
    assert runtime._stopped is True


@pytest.mark.asyncio
async def test_stop_rejects_new_intake_before_waiting_for_entered_commit() -> None:
    events: list[str] = []
    writer = _Writer(events)
    writer.block = True
    runtime, instances, _writer = _runtime(events, writer=writer)
    await runtime.start()
    insert_task = asyncio.create_task(runtime.webhook_inbox.insert(_event(), 1, 1))
    await writer.entered.wait()
    heartbeat = runtime._heartbeat_task
    assert heartbeat is not None
    assert instances.heartbeat_calls == [(1, 0, 0)]

    stop_task = asyncio.create_task(runtime.stop())
    await asyncio.sleep(0)

    assert runtime.ready is False
    assert heartbeat.done() is False
    with pytest.raises(WebhookIngressUnavailable):
        await runtime.webhook_inbox.insert(_event(), 1, 1)
    assert "instance.drain" not in events
    assert "pool.close" not in events

    writer.release.set()
    await insert_task
    await stop_task

    assert events.index("writer.insert") < events.index("instance.drain")
    assert events.index("instance.drain") < events.index("pool.close")


@pytest.mark.asyncio
async def test_stop_timeout_preserves_pool_until_entered_commit_finishes() -> None:
    events: list[str] = []
    writer = _Writer(events)
    writer.block = True
    runtime, _instances, _writer = _runtime(events, writer=writer)
    runtime._shutdown_seconds = 0.01
    await runtime.start()
    insert_task = asyncio.create_task(runtime.webhook_inbox.insert(_event(), 1, 1))
    await writer.entered.wait()

    with pytest.raises(
        runtime_module.RuntimeShutdownError, match="intake_drain_timeout"
    ):
        await runtime.stop()

    assert "pool.close" not in events
    assert "instance.drain" not in events
    assert runtime.webhook_ingress_service is not None
    assert runtime._pool_open is True
    assert runtime._stopped is False
    assert not any(
        task.get_name() == "greenfield-webhook-intake-drain" and not task.done()
        for task in asyncio.all_tasks()
    )

    writer.release.set()
    assert await insert_task == IngressReceipt(inbox_id=_INBOX_ID, duplicate=False)
    await runtime.stop()

    assert events.index("instance.drain") < events.index("pool.close")
    assert runtime.webhook_ingress_service is None


@pytest.mark.asyncio
async def test_stop_retries_each_unproved_owner_before_releasing_resources() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events, stop_error=RuntimeError("worker_stop"))
    instances = _InstanceRepository(
        events,
        heartbeat_error=RuntimeError("heartbeat"),
        drain_error=RuntimeError("drain"),
    )
    pool = _Pool(events, close_error=RuntimeError("pool_close"))
    runtime, _instances, _writer = _runtime(
        events,
        instance_repository=instances,
        pool=pool,
        processing_worker=worker,
        recovery_repository=recovery,
    )
    await runtime.start()
    runtime._state.accepted_count = 1

    with pytest.raises(RuntimeShutdownError, match="shutdown_incomplete"):
        await runtime.stop()

    assert runtime.ready is False
    assert runtime.processing_ready is False
    assert runtime.webhook_ingress_service is not None
    assert runtime._processing_started is True
    assert "instance.heartbeat" not in events
    assert "instance.drain" not in events
    assert "pool.close" not in events

    worker.stop_error = None
    with pytest.raises(RuntimeShutdownError, match="shutdown_incomplete"):
        await runtime.stop()

    assert runtime._processing_started is False
    assert "instance.heartbeat" in events
    assert "instance.drain" in events
    assert "pool.close" not in events
    assert runtime._pool_open is True

    instances.heartbeat_error = None
    instances.drain_error = None
    pool.close_error = None
    await runtime.stop()

    assert events.count("worker.stop:1.0") == 2
    assert events.index("instance.drain") < events.index("pool.close")
    assert runtime.webhook_ingress_service is None
    assert runtime._stopped is True


@pytest.mark.asyncio
async def test_real_worker_failed_stop_retains_tasks_and_runtime_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = DurableInboxWorker(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        worker_id="ai-exchange-web",
        lease_session_id=str(uuid4()),
        concurrency=1,
        lease_seconds=30,
        heartbeat_interval_seconds=10,
    )
    release = asyncio.Event()

    async def stubborn_consumer() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue

    consumer = asyncio.create_task(stubborn_consumer())
    worker._tasks = [consumer]
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,  # type: ignore[arg-type]
        recovery_repository=recovery,
    )
    monkeypatch.setattr(
        "src.ingestion.worker._WORKER_CANCELLATION_SECONDS",
        0.01,
    )
    runtime._shutdown_seconds = 0.01
    await runtime.start()

    with pytest.raises(RuntimeShutdownError, match="shutdown_incomplete"):
        await runtime.stop()

    assert worker.tasks == (consumer,)
    assert runtime._processing_started is True
    assert runtime._pool_open is True
    assert runtime._stopped is False
    assert "instance.drain" not in events
    assert "pool.close" not in events

    release.set()
    await asyncio.wait_for(consumer, timeout=0.1)
    await runtime.stop()

    assert worker.tasks == ()
    assert runtime._processing_started is False
    assert events.index("instance.drain") < events.index("pool.close")


@pytest.mark.asyncio
async def test_stop_cancellation_returns_without_releasing_owned_resources() -> None:
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events, block_stop=True)
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
    )
    await runtime.start()

    stop_task = asyncio.create_task(runtime.stop())
    await worker.stop_entered.wait()
    stop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(stop_task, timeout=0.1)

    assert "instance.drain" not in events
    assert "pool.close" not in events
    assert runtime.ready is False
    worker.stop_release.set()


@pytest.mark.asyncio
async def test_stop_preserves_dependency_cancellation_without_releasing_owners() -> (
    None
):
    events: list[str] = []
    recovery = _RecoveryRepository(events)
    worker = _ProcessingWorker(events, stop_error=asyncio.CancelledError())
    runtime, _instances, _writer = _runtime(
        events,
        processing_worker=worker,
        recovery_repository=recovery,
    )
    await runtime.start()

    with pytest.raises(asyncio.CancelledError):
        await runtime.stop()

    assert "instance.drain" not in events
    assert "pool.close" not in events
    assert runtime._processing_started is True
    assert runtime._pool_open is True
    assert runtime.webhook_ingress_service is not None

    worker.stop_error = None
    await runtime.stop()
    assert events.index("instance.drain") < events.index("pool.close")
    assert runtime.webhook_ingress_service is None


@pytest.mark.asyncio
async def test_check_ready_is_read_only_and_rejects_paused_authority() -> None:
    events: list[str] = []
    authorities = _AuthorityRepository(events)
    runtime, instances, _writer = _runtime(
        events,
        authority_repository=authorities,
    )
    await runtime.start()

    assert await runtime.check_ready() is True
    authorities.authority = _authority(RuntimeAuthorityState.PAUSED)
    assert await runtime.check_ready() is False
    assert instances.heartbeat_calls == []

    await runtime.stop()


def test_runtime_exposes_no_worker_or_sync_activation_surface() -> None:
    public = set(dir(IngestionRuntime))

    assert {"start", "stop", "check_ready", "heartbeat_once"} <= public
    assert not public.intersection(
        {
            "activate",
            "start_worker",
            "claim_batch",
            "start_sync",
            "run_polling",
        }
    )
    generation = PipelineGeneration(
        account_id=8,
        generation=1,
        pipeline_name="durable_v1",
        state=PipelineGenerationState.CURRENT_INGRESS,
        fencing_token=1,
    )
    assert generation.pipeline_name == "durable_v1"


@pytest.mark.asyncio
async def test_greenfield_writer_binds_the_current_session_and_lease_version() -> None:
    session_id = str(uuid4())
    connection = _Connection([_Cursor(one={"inbox_id": _INBOX_ID, "duplicate": False})])
    writer = GreenfieldWebhookWriter(_DatabasePool(connection))

    receipt = await writer.insert(_lease(session_id=session_id), _event())

    assert receipt == IngressReceipt(inbox_id=_INBOX_ID, duplicate=False)
    query, params = connection.calls[0]
    assert "greenfield_insert_webhook_event" in query
    assert len(params) == 12
    assert params[:3] == (8, session_id, 1)
    assert params[3:8] == (
        "message-1",
        "INBOX",
        "NewMailEvent",
        "create",
        _HASH_A,
    )
    assert params[-1] == "full"


@pytest.mark.asyncio
async def test_greenfield_writer_rejects_a_lease_without_a_safe_commit_window() -> None:
    session_id = str(uuid4())
    lease = replace(
        _lease(session_id=session_id),
        lease_until=datetime.now(UTC) + timedelta(milliseconds=500),
    )
    connection = _Connection([])
    writer = GreenfieldWebhookWriter(_DatabasePool(connection))

    with pytest.raises(WebhookIngressUnavailable):
        await writer.insert(lease, _event())

    assert connection.calls == []


def _database_manifest_rows(
    *,
    scope_hash: str | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    snapshot = _snapshot()
    contract = _contract(snapshot)
    capability = contract.capability_manifest
    scope = tuple(snapshot.scopes)[0]
    capability_row = {
        "stage": capability.stage.value,
        "schema_revision": capability.schema_revision,
        "schema_digest": capability.schema_digest,
        "protocol_version": capability.protocol_version,
        "minimum_build_id": capability.minimum_build_id,
        "config_hash": capability.config_hash,
        "adapter_hash": capability.adapter_hash,
        "policy_manifest_hash": capability.policy_manifest_hash,
        "evidence_manifest_hash": capability.evidence_manifest_hash,
        "predecessor_hash": capability.predecessor_hash,
    }
    matrix = {
        f"{source.value}:{raw_type}:{kind.value}": policy.value
        for (source, raw_type, kind), policy in scope.event_policy_matrix.items()
    }
    scope_row = {
        "canonical_key": scope.canonical_key,
        "webhook_ids": sorted(scope.webhook_ids),
        "sync_folder": scope.sync_folder,
        "event_policy_matrix": matrix,
        "scope_hash": scope_hash or scope.config_hash,
        "policy_manifest_hash": capability.policy_manifest_hash,
    }
    return capability_row, scope_row


@pytest.mark.asyncio
async def test_manifest_repository_reconstructs_and_revalidates_immutable_db_facts() -> (
    None
):
    capability_row, scope_row = _database_manifest_rows()
    connection = _Connection([_Cursor(one=capability_row), _Cursor(many=[scope_row])])

    contract, snapshot = await RuntimeManifestRepository(
        _DatabasePool(connection)
    ).load(_authority())

    assert contract == _contract(snapshot)
    assert canonical_policy_manifest(snapshot).hash == _authority().policy_manifest_hash
    assert len(connection.calls) == 2
    assert "pipeline_runtime_capabilities" in connection.calls[0][0]
    assert "pipeline_folder_scopes" in connection.calls[1][0]


@pytest.mark.asyncio
async def test_manifest_repository_fails_closed_on_scope_hash_drift() -> None:
    capability_row, scope_row = _database_manifest_rows(scope_hash=_HASH_D)
    connection = _Connection([_Cursor(one=capability_row), _Cursor(many=[scope_row])])

    with pytest.raises(RuntimeUnavailableError, match="manifest_unavailable"):
        await RuntimeManifestRepository(_DatabasePool(connection)).load(_authority())


def test_runtime_parsers_reject_noncanonical_database_shapes() -> None:
    assert runtime_module._row_mapping(
        ("value-a", "value-b"),
        ("a", "b"),
        error="invalid",
    ) == {"a": "value-a", "b": "value-b"}
    for row in ({"unexpected": "value"}, ("too-short",), object()):
        with pytest.raises(RuntimeUnavailableError, match="invalid"):
            runtime_module._row_mapping(row, ("a", "b"), error="invalid")

    for matrix in ([], {1: "full"}, {"not-a-policy-key": "full"}):
        with pytest.raises(RuntimeUnavailableError, match="manifest_unavailable"):
            runtime_module._policy_matrix_from_database(matrix)


@pytest.mark.asyncio
async def test_manifest_repository_fails_closed_on_missing_or_inconsistent_rows() -> (
    None
):
    missing = _Connection([_Cursor(one=None)])
    with pytest.raises(RuntimeUnavailableError, match="manifest_unavailable"):
        await RuntimeManifestRepository(_DatabasePool(missing)).load(_authority())

    capability_row, scope_row = _database_manifest_rows()
    malformed_capability = dict(capability_row, protocol_version="invalid")
    malformed = _Connection(
        [_Cursor(one=malformed_capability), _Cursor(many=[scope_row])]
    )
    with pytest.raises(RuntimeUnavailableError, match="manifest_unavailable"):
        await RuntimeManifestRepository(_DatabasePool(malformed)).load(_authority())

    wrong_stage = dict(
        capability_row,
        stage=RuntimeCapabilityStage.PHASE3_APPROVAL_SEND.value,
    )
    inconsistent_stage = _Connection(
        [_Cursor(one=wrong_stage), _Cursor(many=[scope_row])]
    )
    with pytest.raises(RuntimeUnavailableError, match="manifest_unavailable"):
        await RuntimeManifestRepository(_DatabasePool(inconsistent_stage)).load(
            _authority()
        )

    wrong_policy_scope = dict(scope_row, policy_manifest_hash=_HASH_A)
    inconsistent_scope = _Connection(
        [_Cursor(one=capability_row), _Cursor(many=[wrong_policy_scope])]
    )
    with pytest.raises(RuntimeUnavailableError, match="manifest_unavailable"):
        await RuntimeManifestRepository(_DatabasePool(inconsistent_scope)).load(
            _authority()
        )

    inconsistent_authority = replace(_authority(), config_hash=_HASH_C)
    contract_mismatch = _Connection(
        [_Cursor(one=capability_row), _Cursor(many=[scope_row])]
    )
    with pytest.raises(RuntimeUnavailableError, match="manifest_unavailable"):
        await RuntimeManifestRepository(_DatabasePool(contract_mismatch)).load(
            inconsistent_authority
        )


@pytest.mark.asyncio
async def test_runtime_views_fail_closed_outside_the_owned_account_and_authority() -> (
    None
):
    snapshot = _snapshot()
    frozen = runtime_module._FrozenSnapshotProvider(8, snapshot)
    assert await frozen.get_ready_snapshot(8) is snapshot
    with pytest.raises(WebhookIngressUnavailable):
        await frozen.get_ready_snapshot(9)

    events: list[str] = []
    authorities = _AuthorityRepository(events)
    ownership = runtime_module._RuntimeOwnershipView(8, authorities)
    assert await ownership.current_ingress(9) is None
    authorities.authority = _authority(RuntimeAuthorityState.PAUSED)
    assert await ownership.current_ingress(8) is None
    authorities.authority = _authority()
    generation = await ownership.current_ingress(8)
    assert generation == PipelineGeneration(
        account_id=8,
        generation=1,
        pipeline_name="durable_v1",
        state=PipelineGenerationState.CURRENT_INGRESS,
        fencing_token=1,
    )


@pytest.mark.asyncio
async def test_session_bound_inbox_counts_rejected_writes_and_invalid_receipts() -> (
    None
):
    state = runtime_module._SessionState(
        lease=_lease(session_id=str(uuid4())),
        accepting=True,
    )
    writer = SimpleNamespace(insert=AsyncMock())
    assert state.lease is not None
    renewer = AsyncMock(
        side_effect=(
            _lease(session_id=state.lease.session_id, lease_version=2),
            _lease(
                session_id=state.lease.session_id,
                lease_version=3,
                rejected_count=1,
            ),
        )
    )
    inbox = runtime_module._SessionBoundWebhookInbox(state, writer, renewer)

    with pytest.raises(WebhookIngressUnavailable):
        await inbox.insert(_event(), generation=2, fencing_token=1)
    renewer.assert_not_awaited()

    writer.insert.side_effect = RuntimeError("write_failed")
    with pytest.raises(RuntimeError, match="write_failed"):
        await inbox.insert(_event(), generation=1, fencing_token=1)
    assert state.rejected_count == 1

    writer.insert.side_effect = None
    writer.insert.return_value = object()
    with pytest.raises(WebhookIngressUnavailable):
        await inbox.insert(_event(), generation=1, fencing_token=1)
    assert state.rejected_count == 2


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"account_id": 0}, "account_id"),
        ({"instance_id": "contains space"}, "instance_id"),
        ({"heartbeat_seconds": 30}, "runtime timing"),
        ({"session_id": "not-a-uuid"}, "session_id"),
        ({"session_id": "00000000-0000-0000-0000-000000000000"}, "session_id"),
        ({"session_id": "00000000-0000-4000-8000-00000000000A"}, "session_id"),
        ({"pool": object()}, "dependency"),
        ({"fail_stop": object()}, "fail_stop"),
    ],
)
def test_runtime_constructor_rejects_invalid_configuration(
    override: dict[str, object],
    message: str,
) -> None:
    events: list[str] = []
    values: dict[str, object] = {
        "account_id": 8,
        "pool": _Pool(events),
        "authority_repository": _AuthorityRepository(events),
        "manifest_repository": _ManifestRepository(events),
        "instance_repository": _InstanceRepository(events),
        "webhook_writer": _Writer(events),
        "instance_id": "ai-exchange-web",
        "session_id": str(uuid4()),
        "lease_seconds": 30,
        "heartbeat_seconds": 10,
        "shutdown_seconds": 1,
    }
    values.update(override)

    with pytest.raises(ValueError, match=message):
        IngestionRuntime(**values)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_unstarted_runtime_is_idempotently_stopped_and_not_restartable() -> None:
    runtime, _instances, _writer = _runtime([])

    with pytest.raises(RuntimeUnavailableError, match="runtime_not_started"):
        await runtime.queue_stats()
    await runtime.stop()
    await runtime.stop()
    with pytest.raises(RuntimeUnavailableError, match="runtime_not_startable"):
        await runtime.start()


def test_runtime_factory_wires_one_dedicated_business_pool() -> None:
    settings = SimpleNamespace(
        database_url="postgresql://runtime@example.invalid/database",
        EXCHANGE_ACCOUNT_ID=8,
        INGESTION_INSTANCE_ID="runtime-web",
        INGESTION_LEASE_SECONDS=40,
        INGESTION_HEARTBEAT_SECONDS=10,
        INGESTION_SHUTDOWN_SECONDS=20,
    )
    pool = MagicMock()
    authority_repository = MagicMock()
    instance_repository = MagicMock()

    with (
        patch.object(
            runtime_module, "AsyncConnectionPool", return_value=pool
        ) as create,
        patch.object(
            runtime_module,
            "RuntimeAuthorityRepository",
            return_value=authority_repository,
        ),
        patch.object(
            runtime_module,
            "RuntimeInstanceRepository",
            return_value=instance_repository,
        ),
    ):
        runtime = build_ingestion_runtime(settings)

    assert isinstance(runtime, IngestionRuntime)
    assert runtime.ready is False
    assert runtime.processing_ready is False
    create.assert_called_once_with(
        conninfo=settings.database_url,
        min_size=1,
        max_size=4,
        open=False,
        kwargs={"autocommit": True, "row_factory": runtime_module.dict_row},
    )


def test_runtime_factory_requires_context_before_allocating_processing_pool() -> None:
    settings = SimpleNamespace(
        database_url="postgresql://runtime@example.invalid/database",
        EXCHANGE_ACCOUNT_ID=8,
        DURABLE_INBOX_ENABLED=True,
    )

    with patch.object(runtime_module, "AsyncConnectionPool") as create:
        with pytest.raises(ValueError, match="processing_context is required"):
            build_ingestion_runtime(settings)

    create.assert_not_called()


def test_runtime_factory_requires_durable_inbox_when_polling_is_enabled() -> None:
    settings = SimpleNamespace(
        database_url="postgresql://runtime@example.invalid/database",
        EXCHANGE_ACCOUNT_ID=8,
        POLLING_ENABLED=True,
    )

    with patch.object(runtime_module, "AsyncConnectionPool") as create:
        with pytest.raises(ValueError, match="polling requires durable Inbox"):
            build_ingestion_runtime(settings)

    create.assert_not_called()


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("INGESTION_SHADOW_ENABLED", "does not permit ingestion Shadow"),
        ("SYNC_RECONCILIATION_ENABLED", "does not permit Sync reconciliation"),
    ],
)
def test_runtime_factory_rejects_features_outside_phase4_lite_before_pool_creation(
    field: str,
    message: str,
) -> None:
    settings = SimpleNamespace(
        database_url="postgresql://runtime@example.invalid/database",
        EXCHANGE_ACCOUNT_ID=8,
        **{field: True},
    )

    with patch.object(runtime_module, "AsyncConnectionPool") as create:
        with pytest.raises(ValueError, match=message):
            build_ingestion_runtime(settings)

    create.assert_not_called()


def test_runtime_factory_wires_one_worker_on_the_same_business_pool() -> None:
    settings = SimpleNamespace(
        database_url="postgresql://runtime@example.invalid/database",
        EXCHANGE_ACCOUNT_ID=8,
        DURABLE_INBOX_ENABLED=True,
        INGESTION_INSTANCE_ID="runtime-web",
        INGESTION_LEASE_SECONDS=40,
        INGESTION_HEARTBEAT_SECONDS=10,
        INGESTION_SHUTDOWN_SECONDS=20,
    )
    processing_context = object()
    pool = MagicMock()
    authority_repository = MagicMock()
    instance_repository = MagicMock()
    generated_session_id = uuid4()

    with (
        patch.object(
            runtime_module,
            "uuid4",
            return_value=generated_session_id,
        ) as create_session_id,
        patch.object(
            runtime_module, "AsyncConnectionPool", return_value=pool
        ) as create,
        patch.object(
            runtime_module,
            "RuntimeAuthorityRepository",
            return_value=authority_repository,
        ),
        patch.object(
            runtime_module,
            "RuntimeInstanceRepository",
            return_value=instance_repository,
        ),
    ):
        runtime = build_ingestion_runtime(
            settings,
            processing_context=processing_context,
        )

    worker = runtime._processing_worker
    recovery = runtime._inbox_recovery_repository
    assert worker is not None
    assert recovery is not None
    assert worker.concurrency == 1
    assert worker._pipeline_names == ("durable_v1",)
    assert worker._worker_id == "runtime-web"
    assert worker._lease_session_id == runtime._session_id == str(generated_session_id)
    assert worker._lease_seconds == 40
    assert worker._heartbeat_interval_seconds == 10.0
    assert worker._inbox is recovery
    assert recovery._pool is pool
    assert worker._ownership._pool is pool
    adapter = worker._router.registry["durable_v1"]
    assert adapter.pipeline_name == "durable_v1"
    assert adapter.legacy_account_id == 8
    assert adapter._ctx is processing_context
    assert runtime._recovery_interval_seconds == 80.0
    create_session_id.assert_called_once_with()
    create.assert_called_once()
