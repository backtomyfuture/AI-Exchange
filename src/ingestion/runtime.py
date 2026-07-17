"""Single-process greenfield durable-ingestion runtime.

Phase 2 owns only a Web process session and the verified Webhook-to-Inbox
commit path.  Worker, Sync, Graph, Lark and model effects remain deliberately
absent until their later capability stages are installed.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

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


class RuntimeUnavailableError(RuntimeError):
    """Safe fixed-token failure for an unavailable greenfield runtime."""


class RuntimeShutdownError(RuntimeError):
    """The runtime could not prove a complete bounded shutdown."""


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


class _LeaseRenewerPort(Protocol):
    async def __call__(
        self,
        lease: RuntimeInstanceLease,
        accepted_count: int,
        rejected_count: int,
    ) -> RuntimeInstanceLease: ...


def _require_uuid4(value: object) -> str:
    try:
        parsed = UUID(str(value))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("session_id must be a UUID4") from None
    if parsed.version != 4 or parsed.variant != UUID(str(uuid4())).variant:
        raise ValueError("session_id must be a UUID4")
    return str(parsed)


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


class IngestionRuntime:
    """Own exactly one Web session and one durable Webhook intake service."""

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
            or not 1 <= shutdown_seconds <= 3600
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
        self._state = _SessionState()
        self._webhook_inbox = _SessionBoundWebhookInbox(
            self._state,
            webhook_writer,
            self._renew_webhook_lease,
        )
        self._service: WebhookIngressService | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pool_open = False
        self._started = False
        self._stopped = False
        self._ready = False

    @property
    def ready(self) -> bool:
        lease = self._state.lease
        return bool(
            self._ready
            and self._state.accepting
            and type(lease) is RuntimeInstanceLease
            and lease.lifecycle is RuntimeInstanceLifecycle.ACTIVE
            and lease.lease_until > datetime.now(UTC)
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
        registered = False
        try:
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
            registered = True
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
            self._state.accepting = True
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
            self._started = True
            self._ready = True
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(),
                name="greenfield-web-session-heartbeat",
            )
        except BaseException as exc:
            self._ready = False
            self._state.accepting = False
            if registered and self._state.lease is not None:
                try:
                    await self._instance_repository.drain(self._state.lease)
                except BaseException:
                    pass
            if self._pool_open:
                try:
                    await self._pool.close()
                finally:
                    self._pool_open = False
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise RuntimeUnavailableError("startup_failed") from None

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._heartbeat_seconds)
                await self.heartbeat_once()
        except asyncio.CancelledError:
            raise
        except RuntimeUnavailableError:
            logger.critical("Greenfield Web session heartbeat failed closed")

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
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        if self._stopped or not self._pool_open:
            self._ready = False
            self._state.accepting = False
            self._stopped = True
            return
        self._ready = False
        self._webhook_inbox.disable()

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
            idle_waiter.cancel()
            try:
                await idle_waiter
            except asyncio.CancelledError:
                pass
            await self._cancel_heartbeat()
            try:
                await self._pool.close()
            except BaseException:
                logger.critical("Business pool close failed after intake drain timeout")
            finally:
                self._pool_open = False
                self._stopped = True
                self._service = None
            raise RuntimeShutdownError("intake_drain_timeout") from None

        await self._cancel_heartbeat()

        failures: list[str] = []
        async with self._state.lock:
            lease = self._state.lease
            if type(lease) is RuntimeInstanceLease:
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
                    except BaseException:
                        failures.append("final_heartbeat")
                try:
                    drained = await self._instance_repository.drain(lease)
                    self._state.lease = drained
                except BaseException:
                    failures.append("session_drain")
        try:
            await self._pool.close()
        except BaseException:
            failures.append("pool_close")
        finally:
            self._pool_open = False
            self._stopped = True
            self._service = None
        if failures:
            raise RuntimeShutdownError("shutdown_incomplete")


def build_ingestion_runtime(settings: Any) -> IngestionRuntime:
    """Create the one production runtime around one dedicated business pool."""

    pool = AsyncConnectionPool(
        conninfo=settings.database_url,
        min_size=1,
        max_size=4,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    authority_repository = RuntimeAuthorityRepository(pool)
    return IngestionRuntime(
        account_id=settings.EXCHANGE_ACCOUNT_ID,
        pool=pool,
        authority_repository=authority_repository,
        manifest_repository=RuntimeManifestRepository(pool),
        instance_repository=RuntimeInstanceRepository(pool),
        webhook_writer=GreenfieldWebhookWriter(pool),
        instance_id=str(getattr(settings, "INGESTION_INSTANCE_ID", "ai-exchange-web")),
        session_id=str(uuid4()),
        lease_seconds=int(getattr(settings, "INGESTION_LEASE_SECONDS", 30)),
        heartbeat_seconds=int(getattr(settings, "INGESTION_HEARTBEAT_SECONDS", 10)),
        shutdown_seconds=int(getattr(settings, "INGESTION_SHUTDOWN_SECONDS", 30)),
    )


__all__ = [
    "GreenfieldWebhookWriter",
    "IngestionRuntime",
    "RuntimeManifestRepository",
    "RuntimeShutdownError",
    "RuntimeUnavailableError",
    "build_ingestion_runtime",
]
