"""Single-process durable-ingestion runtime.

The runtime owns the existing Durable Inbox worker and its bounded recovery
loop. When enabled, its polling child is the only external mail ingress and
commits ``sync_state`` deltas into that same Inbox.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import RFC_4122, UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from src.domain.email_state import PipelineGenerationState
from src.ingestion.models import (
    ChangeKind,
    InboxStats,
    IngressReceipt,
    IngressSource,
    NormalizedIngressEvent,
    PipelineGeneration,
    ProcessingPolicy,
)
from src.ingestion.policy import FolderScope, PolicySnapshot, ProcessingPolicyResolver
from src.ingestion.runtime_authority import (
    GREENFIELD_PIPELINE_NAME,
    RuntimeAuthority,
    RuntimeAuthorityRepository,
    RuntimeAuthorityState,
    RuntimeContract,
    RuntimeInstanceLease,
    RuntimeInstanceLifecycle,
    RuntimeInstanceRepository,
    canonical_policy_manifest,
    require_phase2_ingress_authority,
)
from src.ingestion.runtime_capability import (
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
)
from src.ingestion.repository import GreenfieldWebhookWriter, InboxRepository
from src.ingestion.webhook import WebhookIngressService, WebhookIngressUnavailable


logger = logging.getLogger(__name__)

_INSTANCE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z", re.ASCII)
_RECOVERY_BATCH_LIMIT = 500
_MIN_RECOVERY_INTERVAL_SECONDS = 30.0
_RECOVERY_LEASE_MULTIPLIER = 2.0
_PROCESS_CONTROL_EXCEPTIONS = (asyncio.CancelledError, KeyboardInterrupt, SystemExit)
_CAPABILITY_SQL = (
    "SELECT stage, schema_revision, schema_digest::text AS schema_digest, "
    "protocol_version, minimum_build_id, config_hash::text AS config_hash, "
    "adapter_hash::text AS adapter_hash, "
    "policy_manifest_hash::text AS policy_manifest_hash, "
    "evidence_manifest_hash::text AS evidence_manifest_hash, "
    "predecessor_hash::text AS predecessor_hash "
    "FROM public.pipeline_runtime_capabilities "
    "WHERE capability_hash = %s AND policy_manifest_hash = %s"
)
_SCOPES_SQL = (
    "SELECT canonical_key, webhook_ids, sync_folder, event_policy_matrix, "
    "scope_hash::text AS scope_hash, "
    "policy_manifest_hash::text AS policy_manifest_hash "
    "FROM public.pipeline_folder_scopes "
    "WHERE account_id = %s AND initialization_id = %s "
    "ORDER BY canonical_key"
)


def _raise_if_current_task_cancelled(error: BaseException) -> None:
    if not isinstance(error, asyncio.CancelledError):
        return
    current = asyncio.current_task()
    if current is not None and current.cancelling():
        raise error


class RuntimeUnavailableError(RuntimeError):
    """Safe fixed-token failure for an unavailable greenfield runtime."""


class RuntimeShutdownError(RuntimeError):
    """The runtime could not prove a complete bounded shutdown."""


@dataclass(frozen=True, slots=True)
class RuntimeHealthSnapshot:
    """One local, identifier-free view of the polling runtime's liveness.

    This small read-only Interface lets HTTP endpoints expose the state of the
    actual single polling worker without coupling to its internal tasks, lease
    or cursor implementation.
    """

    ready: bool
    processing_active: bool
    polling_active: bool
    polling_cursor_ready: bool


class _PoolPort(Protocol):
    async def open(self) -> None: ...

    async def close(self) -> None: ...


class _AuthorityPort(Protocol):
    async def get(self, account_id: int) -> RuntimeAuthority | None: ...


class _ManifestPort(Protocol):
    async def load(
        self,
        authority: RuntimeAuthority,
    ) -> tuple[RuntimeContract, PolicySnapshot]: ...


class _InstancePort(Protocol):
    async def register(
        self,
        authority: RuntimeAuthority,
        runtime_contract: RuntimeContract,
        instance_id: str,
        session_id: str,
        lease_seconds: int,
    ) -> RuntimeInstanceLease: ...

    async def heartbeat(
        self,
        lease: RuntimeInstanceLease,
        accepted_count: int,
        rejected_count: int,
        lease_seconds: int,
    ) -> RuntimeInstanceLease: ...

    async def drain(self, lease: RuntimeInstanceLease) -> RuntimeInstanceLease: ...


class _WebhookWriterPort(Protocol):
    async def insert(
        self,
        lease: RuntimeInstanceLease,
        event: NormalizedIngressEvent,
    ) -> IngressReceipt: ...


class _InboxRecoveryPort(Protocol):
    async def recover_expired_leases(self, limit: int) -> int: ...


class _ProcessingWorkerPort(Protocol):
    @property
    def ready(self) -> bool: ...

    async def start(self) -> None: ...

    async def stop(self, grace_seconds: float = 30.0) -> None: ...


class _PollingRuntimePort(Protocol):
    @property
    def live(self) -> bool: ...

    @property
    def ready(self) -> bool: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class _LeaseRenewerPort(Protocol):
    async def __call__(
        self,
        lease: RuntimeInstanceLease,
        accepted_count: int,
        rejected_count: int,
    ) -> RuntimeInstanceLease: ...


def _require_uuid4(value: object) -> str:
    if type(value) is not str:
        raise ValueError("session_id must be a canonical UUID4")
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError):
        raise ValueError("session_id must be a canonical UUID4") from None
    if parsed.version != 4 or parsed.variant != RFC_4122 or str(parsed) != value:
        raise ValueError("session_id must be a canonical UUID4")
    return value


def _row_mapping(
    row: object,
    columns: tuple[str, ...],
    *,
    error: str,
) -> dict[str, object]:
    if isinstance(row, Mapping):
        if set(row) != set(columns):
            raise RuntimeUnavailableError(error)
        return {column: row[column] for column in columns}
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        if len(row) != len(columns):
            raise RuntimeUnavailableError(error)
        return dict(zip(columns, row, strict=True))
    raise RuntimeUnavailableError(error)


def _policy_matrix_from_database(
    value: object,
) -> dict[tuple[IngressSource, str, ChangeKind], ProcessingPolicy]:
    if not isinstance(value, Mapping):
        raise RuntimeUnavailableError("manifest_unavailable")
    matrix: dict[tuple[IngressSource, str, ChangeKind], ProcessingPolicy] = {}
    try:
        for key, raw_policy in value.items():
            if type(key) is not str:
                raise ValueError
            source_text, raw_event_type, kind_text = key.split(":", 2)
            matrix[
                (IngressSource(source_text), raw_event_type, ChangeKind(kind_text))
            ] = ProcessingPolicy(raw_policy)
    except (TypeError, ValueError):
        raise RuntimeUnavailableError("manifest_unavailable") from None
    return matrix


class RuntimeManifestRepository:
    """Load and revalidate the immutable capability and policy DB facts."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def load(
        self,
        authority: RuntimeAuthority,
    ) -> tuple[RuntimeContract, PolicySnapshot]:
        exact_authority = require_phase2_ingress_authority(authority)
        capability_columns = (
            "stage",
            "schema_revision",
            "schema_digest",
            "protocol_version",
            "minimum_build_id",
            "config_hash",
            "adapter_hash",
            "policy_manifest_hash",
            "evidence_manifest_hash",
            "predecessor_hash",
        )
        scope_columns = (
            "canonical_key",
            "webhook_ids",
            "sync_folder",
            "event_policy_matrix",
            "scope_hash",
            "policy_manifest_hash",
        )
        try:
            async with self._pool.connection() as connection:
                capability_cursor = await connection.execute(
                    _CAPABILITY_SQL,
                    (
                        exact_authority.capability_hash,
                        exact_authority.policy_manifest_hash,
                    ),
                )
                capability_row = await capability_cursor.fetchone()
                if capability_row is None:
                    raise RuntimeUnavailableError("manifest_unavailable")
                material = _row_mapping(
                    capability_row,
                    capability_columns,
                    error="manifest_unavailable",
                )
                scopes_cursor = await connection.execute(
                    _SCOPES_SQL,
                    (
                        exact_authority.account_id,
                        exact_authority.initialization_id,
                    ),
                )
                scope_rows = await scopes_cursor.fetchall()

            capability = RuntimeCapabilityManifest(
                stage=material["stage"],
                schema_revision=material["schema_revision"],
                schema_digest=material["schema_digest"],
                protocol_version=material["protocol_version"],
                minimum_build_id=material["minimum_build_id"],
                config_hash=material["config_hash"],
                adapter_hash=material["adapter_hash"],
                policy_manifest_hash=material["policy_manifest_hash"],
                evidence_manifest_hash=material["evidence_manifest_hash"],
                predecessor_hash=material["predecessor_hash"],
            )
            if (
                capability.stage is not RuntimeCapabilityStage.PHASE2_INGESTION
                or capability.capability_hash != exact_authority.capability_hash
            ):
                raise RuntimeUnavailableError("manifest_unavailable")

            scopes: list[FolderScope] = []
            for row in scope_rows:
                scope = _row_mapping(row, scope_columns, error="manifest_unavailable")
                if (
                    scope["policy_manifest_hash"]
                    != exact_authority.policy_manifest_hash
                ):
                    raise RuntimeUnavailableError("manifest_unavailable")
                configured = FolderScope.configured(
                    canonical_key=scope["canonical_key"],
                    webhook_ids=scope["webhook_ids"],
                    sync_folder=scope["sync_folder"],
                    event_policy_matrix=_policy_matrix_from_database(
                        scope["event_policy_matrix"]
                    ),
                )
                if configured.config_hash != scope["scope_hash"]:
                    raise RuntimeUnavailableError("manifest_unavailable")
                scopes.append(configured)
            snapshot = PolicySnapshot(scopes=tuple(scopes))
            manifest = canonical_policy_manifest(snapshot)
            if (
                manifest.hash != exact_authority.policy_manifest_hash
                or capability.policy_manifest_hash != manifest.hash
            ):
                raise RuntimeUnavailableError("manifest_unavailable")
            contract = RuntimeContract(
                schema_revision=capability.schema_revision,
                schema_digest=capability.schema_digest,
                protocol_version=capability.protocol_version,
                build_id=capability.minimum_build_id,
                config_hash=capability.config_hash,
                capability_manifest=capability,
            )
            if (
                contract.schema_revision != exact_authority.schema_revision
                or contract.protocol_version != exact_authority.protocol_version
                or contract.build_id != exact_authority.build_id
                or contract.config_hash != exact_authority.config_hash
            ):
                raise RuntimeUnavailableError("manifest_unavailable")
            return contract, snapshot
        except RuntimeUnavailableError:
            raise
        except (KeyError, TypeError, ValueError, psycopg.Error):
            raise RuntimeUnavailableError("manifest_unavailable") from None


@dataclass(slots=True)
class _SessionState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    lease: RuntimeInstanceLease | None = None
    accepting: bool = False
    accepted_count: int = 0
    rejected_count: int = 0


class _FrozenSnapshotProvider:
    def __init__(self, account_id: int, snapshot: PolicySnapshot) -> None:
        self._account_id = account_id
        self._snapshot = snapshot

    async def get_ready_snapshot(self, account_id: int) -> PolicySnapshot:
        if account_id != self._account_id:
            raise WebhookIngressUnavailable()
        return self._snapshot


class _RuntimeOwnershipView:
    def __init__(self, account_id: int, repository: _AuthorityPort) -> None:
        self._account_id = account_id
        self._repository = repository

    async def current_ingress(self, account_id: int) -> PipelineGeneration | None:
        if account_id != self._account_id:
            return None
        authority = await self._repository.get(account_id)
        if (
            type(authority) is not RuntimeAuthority
            or authority.state is not RuntimeAuthorityState.INGEST_ONLY
        ):
            return None
        return PipelineGeneration(
            account_id=authority.account_id,
            generation=authority.generation,
            pipeline_name=authority.pipeline_name,
            state=PipelineGenerationState.CURRENT_INGRESS,
            fencing_token=authority.fencing_token,
        )


class _SessionBoundWebhookInbox:
    def __init__(
        self,
        state: _SessionState,
        writer: _WebhookWriterPort,
        renewer: _LeaseRenewerPort,
    ) -> None:
        self._state = state
        self._writer = writer
        self._renewer = renewer

    def disable(self) -> None:
        self._state.accepting = False

    async def wait_idle(self) -> None:
        async with self._state.lock:
            return

    async def insert(
        self,
        event: NormalizedIngressEvent,
        generation: int,
        fencing_token: int,
    ) -> IngressReceipt:
        if not self._state.accepting:
            raise WebhookIngressUnavailable()
        async with self._state.lock:
            lease = self._state.lease
            if (
                not self._state.accepting
                or type(lease) is not RuntimeInstanceLease
                or lease.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
                or generation != lease.generation
                or fencing_token != lease.fencing_token
            ):
                raise WebhookIngressUnavailable()
            try:
                current = await self._renewer(
                    lease,
                    self._state.accepted_count,
                    self._state.rejected_count,
                )
                if (
                    not self._state.accepting
                    or type(current) is not RuntimeInstanceLease
                    or current.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
                    or current.session_id != lease.session_id
                    or current.generation != lease.generation
                    or current.fencing_token != lease.fencing_token
                    or current.authority_epoch != lease.authority_epoch
                    or current.capability_hash != lease.capability_hash
                    or current.lease_version != lease.lease_version + 1
                    or current.accepted_count != self._state.accepted_count
                    or current.rejected_count != self._state.rejected_count
                ):
                    raise WebhookIngressUnavailable()
                self._state.lease = current
                receipt = await self._writer.insert(current, event)
            except BaseException:
                self._state.rejected_count += 1
                raise
            if type(receipt) is not IngressReceipt:
                self._state.rejected_count += 1
                raise WebhookIngressUnavailable()
            self._state.accepted_count += 1
            return receipt


class _SessionBoundPollingCommitter:
    """Serialize polling-page commits with the Web session heartbeat.

    The Gateway request happens before this object is entered.  Once a page is
    ready to persist, it renews the same ``web`` lease and holds the shared
    session lock until the database's fenced, one-page transaction returns.
    """

    def __init__(
        self,
        state: _SessionState,
        renewer: _LeaseRenewerPort,
        writer: Any,
    ) -> None:
        if type(state) is not _SessionState:
            raise ValueError("session state is invalid")
        if not callable(renewer) or not callable(getattr(writer, "commit_page", None)):
            raise ValueError("polling commit dependency is invalid")
        self._state = state
        self._renewer = renewer
        self._writer = writer

    async def commit_page(
        self,
        checkpoint: Any,
        next_cursor: str,
        events: Sequence[NormalizedIngressEvent],
        *,
        activation: bool,
    ) -> Any:
        from src.ingestion.polling import PollingCursorUnavailable

        exact_events = tuple(events)
        async with self._state.lock:
            lease = self._state.lease
            if (
                not self._state.accepting
                or type(lease) is not RuntimeInstanceLease
                or lease.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
            ):
                raise PollingCursorUnavailable()
            try:
                current = await self._renewer(
                    lease,
                    self._state.accepted_count,
                    self._state.rejected_count,
                )
                if (
                    type(current) is not RuntimeInstanceLease
                    or current.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
                    or current.session_id != lease.session_id
                    or current.generation != lease.generation
                    or current.fencing_token != lease.fencing_token
                    or current.authority_epoch != lease.authority_epoch
                    or current.capability_hash != lease.capability_hash
                    or current.lease_version != lease.lease_version + 1
                    or current.accepted_count != self._state.accepted_count
                    or current.rejected_count != self._state.rejected_count
                ):
                    raise PollingCursorUnavailable()
                self._state.lease = current
                result = await self._writer.commit_page(
                    current,
                    checkpoint,
                    next_cursor,
                    exact_events,
                    activation=activation,
                )
            except BaseException:
                self._state.rejected_count += len(exact_events)
                raise
            inserted = getattr(result, "inserted_count", None)
            duplicates = getattr(result, "duplicate_count", None)
            if (
                type(inserted) is not int
                or type(duplicates) is not int
                or inserted < 0
                or duplicates < 0
                or inserted + duplicates != len(exact_events)
            ):
                self._state.rejected_count += len(exact_events)
                raise PollingCursorUnavailable()
            self._state.accepted_count += len(exact_events)
            return result


class IngestionRuntime:
    """Own one session, its durable worker, and optional polling ingress."""

    def __init__(
        self,
        *,
        account_id: int,
        pool: _PoolPort,
        authority_repository: _AuthorityPort,
        manifest_repository: _ManifestPort,
        instance_repository: _InstancePort,
        webhook_writer: _WebhookWriterPort,
        instance_id: str,
        session_id: str,
        lease_seconds: int,
        heartbeat_seconds: int,
        shutdown_seconds: int,
        processing_worker: _ProcessingWorkerPort | None = None,
        inbox_recovery_repository: _InboxRecoveryPort | None = None,
        polling_runtime_factory: Callable[[PolicySnapshot], _PollingRuntimePort]
        | None = None,
        session_state: _SessionState | None = None,
        fail_stop: Callable[[str], None] | None = None,
    ) -> None:
        if type(account_id) is not int or not 1 <= account_id < 2**63:
            raise ValueError("account_id must be a positive BIGINT")
        if (
            type(instance_id) is not str
            or _INSTANCE_ID_PATTERN.fullmatch(instance_id) is None
        ):
            raise ValueError("instance_id is invalid")
        if (
            type(lease_seconds) is not int
            or not 2 <= lease_seconds <= 3600
            or type(heartbeat_seconds) is not int
            or not 1 <= heartbeat_seconds < lease_seconds
            or type(shutdown_seconds) is not int
            or not 1 <= shutdown_seconds <= 30
        ):
            raise ValueError("runtime timing is invalid")
        for dependency, method in (
            (pool, "open"),
            (pool, "close"),
            (authority_repository, "get"),
            (manifest_repository, "load"),
            (instance_repository, "register"),
            (instance_repository, "heartbeat"),
            (instance_repository, "drain"),
            (webhook_writer, "insert"),
        ):
            if not callable(getattr(dependency, method, None)):
                raise ValueError("runtime dependency is invalid")
        if (processing_worker is None) != (inbox_recovery_repository is None):
            raise ValueError("processing dependencies must be configured together")
        if processing_worker is not None:
            for dependency, method in (
                (processing_worker, "start"),
                (processing_worker, "stop"),
                (inbox_recovery_repository, "recover_expired_leases"),
            ):
                if not callable(getattr(dependency, method, None)):
                    raise ValueError("processing dependency is invalid")
        if polling_runtime_factory is not None and not callable(polling_runtime_factory):
            raise ValueError("polling_runtime_factory must be callable")
        if session_state is not None and type(session_state) is not _SessionState:
            raise ValueError("session_state is invalid")
        if fail_stop is not None and not callable(fail_stop):
            raise ValueError("fail_stop must be callable")
        self._account_id = account_id
        self._pool = pool
        self._authority_repository = authority_repository
        self._manifest_repository = manifest_repository
        self._instance_repository = instance_repository
        self._instance_id = instance_id
        self._session_id = _require_uuid4(session_id)
        self._lease_seconds = lease_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._shutdown_seconds = shutdown_seconds
        self._recovery_interval_seconds = max(
            _MIN_RECOVERY_INTERVAL_SECONDS,
            float(lease_seconds) * _RECOVERY_LEASE_MULTIPLIER,
        )
        self._processing_worker = processing_worker
        self._inbox_recovery_repository = inbox_recovery_repository
        self._polling_runtime_factory = polling_runtime_factory
        self._polling_runtime: _PollingRuntimePort | None = None
        self._fail_stop = fail_stop
        self._state = _SessionState() if session_state is None else session_state
        self._webhook_inbox = _SessionBoundWebhookInbox(
            self._state,
            webhook_writer,
            self._renew_webhook_lease,
        )
        self._service: WebhookIngressService | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._pool_open = False
        self._pool_open_attempted = False
        self._registered_lease: RuntimeInstanceLease | None = None
        self._started = False
        self._stopped = False
        self._ready = False
        self._processing_started = False
        self._processing_ready = False

    @property
    def ready(self) -> bool:
        lease = self._state.lease
        heartbeat = self._heartbeat_task
        processing_ready = self._processing_worker is None or self.processing_ready
        polling_ready = self._polling_runtime is None or self.polling_ready
        return bool(
            self._ready
            and processing_ready
            and polling_ready
            and self._state.accepting
            and heartbeat is not None
            and not heartbeat.done()
            and type(lease) is RuntimeInstanceLease
            and lease.lifecycle is RuntimeInstanceLifecycle.ACTIVE
            and lease.lease_until > datetime.now(UTC)
        )

    @property
    def processing_ready(self) -> bool:
        """Whether the configured worker and its sole recovery loop are live."""

        recovery = self._recovery_task
        worker = self._processing_worker
        return bool(
            worker is not None
            and self._processing_ready
            and worker.ready is True
            and recovery is not None
            and not recovery.done()
        )

    @property
    def polling_ready(self) -> bool:
        """Whether polling has established its activation cursor."""

        polling = self._polling_runtime
        return bool(polling is not None and polling.ready is True)

    @property
    def polling_live(self) -> bool:
        """Whether the configured polling scheduler is still running."""

        polling = self._polling_runtime
        return bool(polling is not None and polling.live is True)

    def health_snapshot(self) -> RuntimeHealthSnapshot:
        """Return the bounded liveness fields exposed to operators.

        It intentionally does not perform a database round trip.  Callers
        requiring authority validation must still use :meth:`check_ready`.
        """

        return RuntimeHealthSnapshot(
            ready=self.ready,
            processing_active=self.processing_ready,
            polling_active=self.polling_live,
            polling_cursor_ready=self.polling_ready,
        )

    @property
    def lease(self) -> RuntimeInstanceLease | None:
        return self._state.lease

    @property
    def webhook_inbox(self) -> _SessionBoundWebhookInbox:
        return self._webhook_inbox

    @property
    def webhook_ingress_service(self) -> WebhookIngressService | None:
        return self._service

    async def start(self) -> None:
        if self._started or self._stopped:
            raise RuntimeUnavailableError("runtime_not_startable")
        try:
            self._pool_open_attempted = True
            await self._pool.open()
            self._pool_open = True
            authority = require_phase2_ingress_authority(
                await self._authority_repository.get(self._account_id)
            )
            contract, snapshot = await self._manifest_repository.load(authority)
            lease = await self._instance_repository.register(
                authority,
                contract,
                self._instance_id,
                self._session_id,
                self._lease_seconds,
            )
            if type(lease) is RuntimeInstanceLease:
                self._registered_lease = lease
            if (
                type(lease) is not RuntimeInstanceLease
                or lease.account_id != self._account_id
                or lease.session_id != self._session_id
                or lease.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
            ):
                raise RuntimeUnavailableError("startup_failed")
            self._state.lease = lease
            self._state.accepted_count = lease.accepted_count
            self._state.rejected_count = lease.rejected_count
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(),
                name="greenfield-web-session-heartbeat",
            )
            self._service = WebhookIngressService(
                expected_account_id=self._account_id,
                snapshot_provider=_FrozenSnapshotProvider(self._account_id, snapshot),
                policy_resolver=ProcessingPolicyResolver(),
                ownership_repository=_RuntimeOwnershipView(
                    self._account_id,
                    self._authority_repository,
                ),
                inbox_repository=self._webhook_inbox,
            )
            await self._start_processing()
            # Polling uses the same session-bound intake gate as the retired
            # Webhook adapter.  It must be open during the baseline cycle,
            # while public readiness remains false until that cycle completes.
            self._state.accepting = True
            await self._start_polling(snapshot)
            if self._heartbeat_task is None or self._heartbeat_task.done():
                raise RuntimeUnavailableError("startup_failed")
            self._started = True
            self._state.accepting = True
            self._ready = True
        except BaseException as exc:
            self._ready = False
            self._state.accepting = False
            cleanup = asyncio.create_task(
                self._rollback_start(),
                name="greenfield-runtime-startup-rollback",
            )
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as cancellation:
                cleanup_result = await asyncio.gather(
                    cleanup,
                    return_exceptions=True,
                )
                if cleanup_result and isinstance(cleanup_result[0], BaseException):
                    raise cleanup_result[0]
                self._stopped = True
                raise cancellation
            self._stopped = True
            if isinstance(exc, asyncio.CancelledError):
                raise exc
            raise RuntimeUnavailableError("startup_failed") from None

    async def _start_processing(self) -> None:
        worker = self._processing_worker
        repository = self._inbox_recovery_repository
        if worker is None or repository is None:
            return
        recovered = await repository.recover_expired_leases(_RECOVERY_BATCH_LIMIT)
        if (
            type(recovered) is not int
            or recovered < 0
            or recovered > _RECOVERY_BATCH_LIMIT
        ):
            raise RuntimeError("expired lease recovery returned an invalid result")
        self._processing_started = True
        await worker.start()
        self._recovery_task = asyncio.create_task(
            self._recovery_loop(),
            name="durable-inbox-expired-lease-recovery",
        )
        self._processing_ready = True

    async def _start_polling(self, snapshot: PolicySnapshot) -> None:
        factory = self._polling_runtime_factory
        if factory is None:
            return
        polling = factory(snapshot)
        if (
            polling is None
            or not callable(getattr(polling, "start", None))
            or not callable(getattr(polling, "stop", None))
            or not isinstance(getattr(polling, "live", None), bool)
            or not isinstance(getattr(polling, "ready", None), bool)
        ):
            raise RuntimeUnavailableError("startup_failed")
        self._polling_runtime = polling
        await polling.start()
        if polling.live is not True:
            raise RuntimeUnavailableError("startup_failed")

    async def _recovery_loop(self) -> None:
        repository = self._inbox_recovery_repository
        if repository is None:
            return
        try:
            while True:
                await asyncio.sleep(self._recovery_interval_seconds)
                recovered = await repository.recover_expired_leases(
                    _RECOVERY_BATCH_LIMIT
                )
                if (
                    type(recovered) is not int
                    or recovered < 0
                    or recovered > _RECOVERY_BATCH_LIMIT
                ):
                    raise RuntimeError(
                        "expired lease recovery returned an invalid result"
                    )
        except asyncio.CancelledError:
            raise
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            logger.critical("Durable Inbox expired-lease recovery stopped")
            raise

    async def _cancel_recovery(self) -> None:
        self._processing_ready = False
        recovery = self._recovery_task
        self._recovery_task = None
        if recovery is None:
            return
        recovery.cancel()
        try:
            await recovery
        except asyncio.CancelledError as error:
            _raise_if_current_task_cancelled(error)
            pass
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            # A completed failed task is still a proven-stopped owner.  Its
            # failure already removed processing readiness and is escalated by
            # the Web-session heartbeat.
            pass

    async def _rollback_start(
        self,
    ) -> None:
        cleanup_failures: list[BaseException] = []
        try:
            await self._stop_polling()
        except BaseException as error:
            cleanup_failures.append(error)
        try:
            await self._cancel_heartbeat()
        except BaseException as error:
            cleanup_failures.append(error)
        try:
            await self._cancel_recovery()
        except BaseException as error:
            cleanup_failures.append(error)
        worker = self._processing_worker
        if worker is not None and self._processing_started:
            try:
                await worker.stop(grace_seconds=float(self._shutdown_seconds))
            except BaseException as error:
                cleanup_failures.append(error)
            else:
                self._processing_started = False
        if cleanup_failures:
            for error in cleanup_failures:
                if isinstance(error, _PROCESS_CONTROL_EXCEPTIONS):
                    raise error
            raise RuntimeShutdownError("startup_cleanup_incomplete")
        lease = self._state.lease or self._registered_lease
        if type(lease) is RuntimeInstanceLease:
            if lease.lifecycle is RuntimeInstanceLifecycle.ACTIVE:
                try:
                    drained = await self._instance_repository.drain(lease)
                except BaseException as error:
                    if isinstance(error, _PROCESS_CONTROL_EXCEPTIONS):
                        raise
                    raise RuntimeShutdownError("startup_cleanup_incomplete") from None
                self._state.lease = drained
                self._registered_lease = drained
            elif lease.lifecycle is not RuntimeInstanceLifecycle.DRAINING:
                raise RuntimeShutdownError("startup_cleanup_incomplete")
        if self._pool_open_attempted:
            try:
                await self._pool.close()
            except BaseException as error:
                if isinstance(error, _PROCESS_CONTROL_EXCEPTIONS):
                    raise
                raise RuntimeShutdownError("startup_cleanup_incomplete") from None
            else:
                self._pool_open = False
                self._pool_open_attempted = False
                self._registered_lease = None
        self._service = None

    async def _stop_polling(self) -> None:
        polling = self._polling_runtime
        if polling is None:
            return
        await polling.stop()

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._heartbeat_seconds)
                await self.heartbeat_once()
        except asyncio.CancelledError:
            raise
        except RuntimeUnavailableError as error:
            logger.critical("Greenfield Web session heartbeat failed closed")
            if self._fail_stop is not None:
                reason = (
                    "ingestion_runtime_processing_lost"
                    if str(error) == "processing_lost"
                    else (
                        "ingestion_runtime_polling_lost"
                        if str(error) == "polling_lost"
                        else "ingestion_runtime_heartbeat_lost"
                    )
                )
                self._fail_stop(reason)

    async def _renew_webhook_lease(
        self,
        lease: RuntimeInstanceLease,
        accepted_count: int,
        rejected_count: int,
    ) -> RuntimeInstanceLease:
        try:
            return await self._instance_repository.heartbeat(
                lease,
                accepted_count,
                rejected_count,
                self._lease_seconds,
            )
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._ready = False
            self._state.accepting = False
            raise WebhookIngressUnavailable() from None

    async def heartbeat_once(self) -> None:
        try:
            if (
                self._started
                and self._ready
                and self._state.accepting
                and self._processing_worker is not None
                and not self.processing_ready
            ):
                raise RuntimeUnavailableError("processing_lost")
            if (
                self._started
                and self._ready
                and self._polling_runtime is not None
                and not self.polling_live
            ):
                raise RuntimeUnavailableError("polling_lost")
            async with self._state.lock:
                lease = self._state.lease
                if (
                    type(lease) is not RuntimeInstanceLease
                    or lease.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
                ):
                    raise RuntimeError("lease_unavailable")
                current = await self._instance_repository.heartbeat(
                    lease,
                    self._state.accepted_count,
                    self._state.rejected_count,
                    self._lease_seconds,
                )
                if type(current) is not RuntimeInstanceLease:
                    raise RuntimeError("lease_invalid")
                self._state.lease = current
        except asyncio.CancelledError:
            raise
        except RuntimeUnavailableError:
            self._ready = False
            self._state.accepting = False
            raise
        except BaseException:
            self._ready = False
            self._state.accepting = False
            raise RuntimeUnavailableError("heartbeat_failed") from None

    async def check_ready(self) -> bool:
        if not self.ready:
            return False
        lease = self._state.lease
        try:
            authority = await self._authority_repository.get(self._account_id)
        except BaseException:
            return False
        return bool(
            type(authority) is RuntimeAuthority
            and type(lease) is RuntimeInstanceLease
            and authority.state is RuntimeAuthorityState.INGEST_ONLY
            and authority.account_id == lease.account_id
            and authority.generation == lease.generation
            and authority.fencing_token == lease.fencing_token
            and authority.authority_epoch == lease.authority_epoch
            and authority.capability_hash == lease.capability_hash
        )

    async def queue_stats(self) -> InboxStats:
        """Read one bounded aggregate from the owned business pool."""

        if not self._pool_open:
            raise RuntimeUnavailableError("runtime_not_started")
        return await InboxRepository(self._pool).stats()

    async def _cancel_heartbeat(self) -> None:
        heartbeat = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat is None:
            return
        heartbeat.cancel()
        try:
            await heartbeat
        except asyncio.CancelledError as error:
            _raise_if_current_task_cancelled(error)
            pass

    async def stop(self) -> None:
        if self._stopped:
            self._ready = False
            self._state.accepting = False
            return
        if not self._started:
            self._ready = False
            self._state.accepting = False
            if not self._pool_open_attempted:
                self._stopped = True
                return
            await self._rollback_start()
            self._stopped = True
            return
        if not self._pool_open:
            raise RuntimeShutdownError("shutdown_incomplete")
        self._ready = False
        self._webhook_inbox.disable()
        cleanup = asyncio.create_task(
            self._finish_stop(),
            name="greenfield-runtime-stop",
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            cleanup.cancel()
            cleanup.add_done_callback(self._consume_stop_result)
            raise

    @staticmethod
    def _consume_stop_result(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            task.exception()
        except BaseException:
            return

    async def _finish_stop(self) -> None:
        failures: list[str] = []
        process_control: BaseException | None = None

        def record_failure(code: str, error: BaseException) -> None:
            nonlocal process_control
            failures.append(code)
            if process_control is None and isinstance(
                error, _PROCESS_CONTROL_EXCEPTIONS
            ):
                process_control = error

        try:
            await self._stop_polling()
        except BaseException as exc:
            _raise_if_current_task_cancelled(exc)
            record_failure("polling_stop", exc)

        idle_waiter = asyncio.create_task(
            self._webhook_inbox.wait_idle(),
            name="greenfield-webhook-intake-drain",
        )
        try:
            await asyncio.wait_for(
                asyncio.shield(idle_waiter),
                timeout=self._shutdown_seconds,
            )
        except TimeoutError:
            failures.append("intake_drain_timeout")
            idle_waiter.cancel()
            await asyncio.gather(idle_waiter, return_exceptions=True)
        except BaseException as exc:
            _raise_if_current_task_cancelled(exc)
            record_failure("intake_drain", exc)

        try:
            await self._cancel_heartbeat()
        except BaseException as exc:
            _raise_if_current_task_cancelled(exc)
            record_failure("heartbeat_stop", exc)
        try:
            await self._cancel_recovery()
        except BaseException as exc:
            _raise_if_current_task_cancelled(exc)
            record_failure("recovery_stop", exc)

        worker = self._processing_worker
        if worker is not None and self._processing_started:
            try:
                await worker.stop(grace_seconds=float(self._shutdown_seconds))
            except BaseException as exc:
                _raise_if_current_task_cancelled(exc)
                record_failure("worker_stop", exc)
            else:
                self._processing_started = False

        self._processing_ready = False
        if failures:
            if process_control is not None:
                raise process_control
            code = (
                "intake_drain_timeout"
                if failures == ["intake_drain_timeout"]
                else "shutdown_incomplete"
            )
            raise RuntimeShutdownError(code)

        lease = self._state.lease
        if type(lease) is RuntimeInstanceLease:
            if lease.lifecycle is RuntimeInstanceLifecycle.ACTIVE:
                if (
                    lease.accepted_count != self._state.accepted_count
                    or lease.rejected_count != self._state.rejected_count
                ):
                    try:
                        lease = await self._instance_repository.heartbeat(
                            lease,
                            self._state.accepted_count,
                            self._state.rejected_count,
                            self._lease_seconds,
                        )
                        self._state.lease = lease
                    except BaseException as exc:
                        _raise_if_current_task_cancelled(exc)
                        record_failure("final_heartbeat", exc)
                try:
                    drained = await self._instance_repository.drain(lease)
                    self._state.lease = drained
                    lease = drained
                except BaseException as exc:
                    _raise_if_current_task_cancelled(exc)
                    record_failure("session_drain", exc)
            elif lease.lifecycle is not RuntimeInstanceLifecycle.DRAINING:
                failures.append("session_state")
        else:
            failures.append("session_missing")

        if (
            type(lease) is not RuntimeInstanceLease
            or lease.lifecycle is not RuntimeInstanceLifecycle.DRAINING
        ):
            if process_control is not None:
                raise process_control
            raise RuntimeShutdownError("shutdown_incomplete")

        try:
            await self._pool.close()
        except BaseException as exc:
            _raise_if_current_task_cancelled(exc)
            record_failure("pool_close", exc)
        else:
            self._pool_open = False
            self._pool_open_attempted = False
            self._registered_lease = None
            self._stopped = True
            self._service = None
        if process_control is not None:
            raise process_control
        if failures:
            raise RuntimeShutdownError("shutdown_incomplete")


def build_ingestion_runtime(
    settings: Any,
    *,
    processing_context: Any | None = None,
    fail_stop: Callable[[str], None] | None = None,
) -> IngestionRuntime:
    """Create the one production runtime around one dedicated business pool."""

    if bool(getattr(settings, "INGESTION_SHADOW_ENABLED", False)):
        raise ValueError("Phase4-Lite does not permit ingestion Shadow")
    if bool(getattr(settings, "SYNC_RECONCILIATION_ENABLED", False)):
        raise ValueError("Phase4-Lite does not permit Sync reconciliation")
    processing_enabled = bool(getattr(settings, "DURABLE_INBOX_ENABLED", False))
    polling_enabled = bool(getattr(settings, "POLLING_ENABLED", False))
    if polling_enabled and not processing_enabled:
        raise ValueError("polling requires durable Inbox processing")
    if processing_enabled and processing_context is None:
        raise ValueError(
            "processing_context is required when durable Inbox processing is enabled"
        )
    pool = AsyncConnectionPool(
        conninfo=settings.database_url,
        min_size=1,
        max_size=4,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    authority_repository = RuntimeAuthorityRepository(pool)
    instance_repository = RuntimeInstanceRepository(pool)
    session_id = str(uuid4())
    session_state = _SessionState()
    processing_worker: _ProcessingWorkerPort | None = None
    inbox_recovery_repository: _InboxRecoveryPort | None = None
    polling_runtime_factory: Callable[[PolicySnapshot], _PollingRuntimePort] | None = (
        None
    )
    if processing_enabled:
        # Local imports are required: ``init_app`` imports this runtime while
        # the compatibility adapter reaches ``exchange_service`` and then
        # ``init_app`` again.
        from src.ingestion.legacy_adapter import LegacyProcessingAdapter
        from src.ingestion.ownership import PipelineOwnershipRepository
        from src.ingestion.processing import ProcessingAdapterRouter
        from src.ingestion.worker import DurableInboxWorker

        inbox_repository = InboxRepository(pool)
        adapter = LegacyProcessingAdapter(
            processing_context,
            legacy_account_id=settings.EXCHANGE_ACCOUNT_ID,
        )
        processing_worker = DurableInboxWorker(
            inbox_repository,
            PipelineOwnershipRepository(pool),
            ProcessingAdapterRouter({GREENFIELD_PIPELINE_NAME: adapter}),
            worker_id=str(
                getattr(settings, "INGESTION_INSTANCE_ID", "ai-exchange-web")
            ),
            lease_session_id=session_id,
            pipeline_names=(GREENFIELD_PIPELINE_NAME,),
            concurrency=1,
            lease_seconds=int(getattr(settings, "INGESTION_LEASE_SECONDS", 30)),
            heartbeat_interval_seconds=float(
                getattr(settings, "INGESTION_HEARTBEAT_SECONDS", 10)
            ),
        )
        inbox_recovery_repository = inbox_repository
    if polling_enabled:
        if processing_context is None:
            raise ValueError("polling requires an application processing context")

        def build_polling_runtime(snapshot: PolicySnapshot) -> _PollingRuntimePort:
            # Keep imports local: the polling adapter uses ExchangeClient while
            # this module is imported during application-context construction.
            from src.ingestion.polling import (
                GreenfieldSyncPageWriter,
                PollingIngress,
                PollingRuntime,
                PostgresPollingCursorStore,
            )

            page_client = getattr(processing_context, "exchange_client", None)
            if not callable(getattr(page_client, "sync_polling", None)):
                raise RuntimeUnavailableError("polling_client_unavailable")
            scopes = ProcessingPolicyResolver().configured_scopes(snapshot)
            inbox_scopes = tuple(
                scope
                for scope in scopes
                if scope.canonical_key == "INBOX" and scope.sync_folder == "INBOX"
            )
            if len(inbox_scopes) != 1:
                raise RuntimeUnavailableError("polling_scope_unavailable")
            scope = inbox_scopes[0]

            async def renew_polling_lease(
                lease: RuntimeInstanceLease,
                accepted_count: int,
                rejected_count: int,
            ) -> RuntimeInstanceLease:
                from src.ingestion.polling import PollingCursorUnavailable

                try:
                    return await instance_repository.heartbeat(
                        lease,
                        accepted_count,
                        rejected_count,
                        int(getattr(settings, "INGESTION_LEASE_SECONDS", 30)),
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    session_state.accepting = False
                    raise PollingCursorUnavailable() from None

            page_committer = _SessionBoundPollingCommitter(
                session_state,
                renew_polling_lease,
                GreenfieldSyncPageWriter(pool),
            )

            cursor_store = PostgresPollingCursorStore(
                pool,
                page_committer,
                account_id=settings.EXCHANGE_ACCOUNT_ID,
                folder=scope.canonical_key,
            )
            return PollingRuntime(
                (
                    PollingIngress(
                        account_id=settings.EXCHANGE_ACCOUNT_ID,
                        scope=scope,
                        snapshot=snapshot,
                        page_client=page_client,
                        cursor_store=cursor_store,
                    ),
                ),
                interval_seconds=float(
                    getattr(settings, "POLLING_INTERVAL_SECONDS", 60)
                ),
            )

        polling_runtime_factory = build_polling_runtime
    return IngestionRuntime(
        account_id=settings.EXCHANGE_ACCOUNT_ID,
        pool=pool,
        authority_repository=authority_repository,
        manifest_repository=RuntimeManifestRepository(pool),
        instance_repository=instance_repository,
        webhook_writer=GreenfieldWebhookWriter(pool),
        instance_id=str(getattr(settings, "INGESTION_INSTANCE_ID", "ai-exchange-web")),
        session_id=session_id,
        lease_seconds=int(getattr(settings, "INGESTION_LEASE_SECONDS", 30)),
        heartbeat_seconds=int(getattr(settings, "INGESTION_HEARTBEAT_SECONDS", 10)),
        shutdown_seconds=int(getattr(settings, "INGESTION_SHUTDOWN_SECONDS", 30)),
        processing_worker=processing_worker,
        inbox_recovery_repository=inbox_recovery_repository,
        polling_runtime_factory=polling_runtime_factory,
        session_state=session_state,
        fail_stop=fail_stop,
    )


__all__ = [
    "GreenfieldWebhookWriter",
    "IngestionRuntime",
    "RuntimeHealthSnapshot",
    "RuntimeManifestRepository",
    "RuntimeShutdownError",
    "RuntimeUnavailableError",
    "build_ingestion_runtime",
]
