"""Dormant fixed-count durable Inbox worker and invocation lease authority.

This module deliberately owns no startup wiring or concrete processing adapter.
Calling :meth:`DurableInboxWorker.start` is the only way to create consumers.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from collections.abc import Awaitable, Callable, Iterable, Sequence
from enum import StrEnum
from typing import Protocol, TypeVar

from src.ingestion.email_events import (
    EmailEventApplication,
    EmailEventDisposition,
    EmailStatus,
)
from src.ingestion.models import (
    InboxLease,
    POSTGRES_BIGINT_MAX,
    PipelineGeneration,
    ProcessingPolicy,
)
from src.ingestion.processing import (
    BeforeExternalEffect,
    ExternalEffectAuthorizationError,
    ProcessingAdapter,
    ProcessingAdapterRouter,
    ProcessingCompletion,
    ProcessingFinishResult,
    ProcessingPolicyRejected,
)


logger = logging.getLogger(__name__)

_TARGET_HASH = re.compile(r"[0-9a-f]{64}\Z")
_PROCESS_CONTROL_EXCEPTIONS = (asyncio.CancelledError, KeyboardInterrupt, SystemExit)
_LOCAL_COMPLETION_POLICIES = frozenset(
    {
        ProcessingPolicy.METADATA_ONLY,
        ProcessingPolicy.HISTORICAL_SUPPRESSED,
        ProcessingPolicy.IGNORED,
    }
)
_ADAPTER_POLICIES = frozenset({ProcessingPolicy.FULL, ProcessingPolicy.ARCHIVE})


class LeaseAuthorityLost(ExternalEffectAuthorizationError):
    """The invocation no longer owns an exact usable lease token."""


class _LeaseAuthorityState(StrEnum):
    ACTIVE = "active"
    LOST = "lost"
    FROZEN = "frozen"


_T = TypeVar("_T")


def _same_lease_except_deadline(current: InboxLease, successor: object) -> bool:
    if type(successor) is not InboxLease:
        return False
    return (
        successor.id == current.id
        and successor.account_id == current.account_id
        and successor.pipeline_name == current.pipeline_name
        and successor.generation == current.generation
        and successor.fencing_token == current.fencing_token
        and successor.lease_owner == current.lease_owner
        and successor.attempts == current.attempts
        and successor.event == current.event
        and successor.received_at == current.received_at
        and successor.lease_until > current.lease_until
    )


class LeaseAuthority:
    """Serialize all uses of, and rotations to, one invocation's latest token.

    No token-reading accessor is exposed. Callers either run the entire fenced
    CAS while holding this authority, or permanently freeze and consume the
    latest token for one terminal transaction.
    """

    __slots__ = ("_lease", "_lost_event", "_mutex", "_state")

    def __init__(self, lease: InboxLease) -> None:
        if type(lease) is not InboxLease:
            raise ValueError("lease must be an exact InboxLease")
        self._lease: InboxLease | None = lease
        self._mutex = asyncio.Lock()
        self._state = _LeaseAuthorityState.ACTIVE
        self._lost_event = asyncio.Event()

    @property
    def is_lost(self) -> bool:
        return self._state is _LeaseAuthorityState.LOST

    @property
    def lost_event(self) -> asyncio.Event:
        return self._lost_event

    def _lose_unlocked(self) -> None:
        self._lease = None
        self._state = _LeaseAuthorityState.LOST
        self._lost_event.set()

    def _require_active_unlocked(self) -> InboxLease:
        if self._state is not _LeaseAuthorityState.ACTIVE or self._lease is None:
            raise LeaseAuthorityLost()
        return self._lease

    async def renew_with_current(
        self,
        renew: Callable[[InboxLease], Awaitable[InboxLease | None]],
    ) -> bool:
        """Renew internally without exposing the rotated token to callers."""

        if not callable(renew):
            raise ValueError("renew must be callable")
        async with self._mutex:
            current = self._require_active_unlocked()
            try:
                successor = await renew(current)
            except BaseException:
                self._lose_unlocked()
                raise
            if successor is None:
                self._lose_unlocked()
                return False
            if not _same_lease_except_deadline(current, successor):
                self._lose_unlocked()
                raise LeaseAuthorityLost()
            self._lease = successor
            return True

    async def run_with_current(
        self,
        operation: Callable[[InboxLease], Awaitable[_T]],
    ) -> _T:
        """Hold authority from latest-token read through the full database CAS."""

        if not callable(operation):
            raise ValueError("operation must be callable")
        async with self._mutex:
            current = self._require_active_unlocked()
            try:
                result = await operation(current)
            except BaseException:
                self._lose_unlocked()
                raise
            if result is not True:
                self._lose_unlocked()
                raise LeaseAuthorityLost()
            return result

    async def stop_and_freeze(self) -> InboxLease:
        """Permanently detach and return the only token allowed to finalize."""

        async with self._mutex:
            lease = self._require_active_unlocked()
            self._lease = None
            self._state = _LeaseAuthorityState.FROZEN
            return lease


class _InboxRepository(Protocol):
    async def claim_batch(
        self,
        worker_id: str,
        pipeline_names: Iterable[str],
        limit: int,
        lease_seconds: int,
    ) -> list[InboxLease]: ...

    async def apply_email_event(self, lease: InboxLease) -> EmailEventApplication: ...

    async def renew(
        self,
        lease: InboxLease,
        lease_seconds: int,
    ) -> InboxLease | None: ...

    async def complete(self, lease: InboxLease) -> bool: ...

    async def begin_processing_effect(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
    ) -> bool: ...

    async def finish_email_processing(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
        completion: ProcessingCompletion,
    ) -> ProcessingFinishResult: ...

    async def finish_email_processing_failure(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
        error: BaseException,
    ) -> ProcessingFinishResult: ...


class _OwnershipRepository(Protocol):
    async def get(
        self,
        account_id: int,
        generation: int,
    ) -> PipelineGeneration | None: ...


def _positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_seconds(name: str, value: object, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    normalized = float(value)
    minimum_ok = normalized >= 0 if allow_zero else normalized > 0
    if not math.isfinite(normalized) or not minimum_ok:
        raise ValueError(f"{name} must be a finite positive number")
    return normalized


def _pipeline_names(value: Sequence[str]) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError("pipeline_names must be a non-empty sequence")
    names = tuple(value)
    if (
        not names
        or any(
            type(name) is not str or not name or name != name.strip() or len(name) > 64
            for name in names
        )
        or len(names) != len(set(names))
    ):
        raise ValueError("pipeline_names must contain unique exact bounded names")
    if names != ("legacy_compat",):
        raise ValueError("Phase-2 Worker may claim only legacy_compat")
    return names


def _worker_id(value: object) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 128
    ):
        raise ValueError("worker_id must be exact bounded text")
    return value


def _validate_effect_request(
    kind: object, ordinal: object, target_hash: object
) -> None:
    if type(kind) is not str or kind not in {
        "detail",
        "content",
        "model",
        "feishu",
        "exchange_mutation",
        "qdrant",
    }:
        raise ValueError("effect kind is invalid")
    if (
        isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or ordinal < 0
        or ordinal > 2**63 - 1
    ):
        raise ValueError("effect ordinal must be a bounded non-negative integer")
    if type(target_hash) is not str or _TARGET_HASH.fullmatch(target_hash) is None:
        raise ValueError("effect target_hash must be a lowercase SHA-256 digest")


class DurableInboxWorker:
    """Dormant fixed-consumer Worker for fenced durable Inbox processing."""

    def __init__(
        self,
        inbox_repository: _InboxRepository,
        ownership_repository: _OwnershipRepository,
        router: ProcessingAdapterRouter,
        *,
        worker_id: str,
        pipeline_names: Sequence[str] = ("legacy_compat",),
        concurrency: int = 4,
        lease_seconds: int = 60,
        heartbeat_interval_seconds: float = 20.0,
        idle_seconds: float = 0.25,
    ) -> None:
        self._inbox = inbox_repository
        self._ownership = ownership_repository
        self._router = router
        self._worker_id = _worker_id(worker_id)
        self._pipeline_names = _pipeline_names(pipeline_names)
        self.concurrency = _positive_int("concurrency", concurrency)
        self._lease_seconds = _positive_int("lease_seconds", lease_seconds)
        self._heartbeat_interval_seconds = _positive_seconds(
            "heartbeat_interval_seconds",
            heartbeat_interval_seconds,
        )
        if self._heartbeat_interval_seconds >= self._lease_seconds:
            raise ValueError("heartbeat interval must be shorter than the lease")
        self._idle_seconds = _positive_seconds("idle_seconds", idle_seconds)
        self._tasks: list[asyncio.Task[None]] = []
        self._stop_requested = asyncio.Event()
        self._closed = False

    @property
    def tasks(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(self._tasks)

    async def start(self) -> None:
        """Create exactly ``concurrency`` consumers once; no runtime wiring."""

        if self._tasks or self._closed:
            return
        self._tasks = [
            asyncio.create_task(
                self._consume(),
                name=f"durable-inbox-consumer-{index}",
            )
            for index in range(self.concurrency)
        ]

    async def run_once(self, worker_id: str | None = None) -> int:
        """Claim at most one lease and process it inline in the caller task."""

        if self._stop_requested.is_set():
            return 0
        resolved_worker_id = (
            self._worker_id if worker_id is None else _worker_id(worker_id)
        )
        leases = await self._inbox.claim_batch(
            resolved_worker_id,
            self._pipeline_names,
            1,
            self._lease_seconds,
        )
        if not isinstance(leases, list) or len(leases) > 1:
            raise RuntimeError("claim_batch violated the one-lease Worker contract")
        if not leases:
            return 0
        lease = leases[0]
        if type(lease) is not InboxLease:
            raise RuntimeError("claim_batch returned an invalid lease")
        await self.process_lease(lease)
        return 1

    async def heartbeat_once(self, authority: LeaseAuthority) -> bool:
        if type(authority) is not LeaseAuthority:
            raise ValueError("authority must be an exact LeaseAuthority")
        return await authority.renew_with_current(
            lambda lease: self._inbox.renew(lease, self._lease_seconds)
        )

    async def process_lease(
        self,
        lease: InboxLease,
    ) -> ProcessingFinishResult | bool | None:
        """Apply one event and execute only the policy-authorized fixed branch."""

        if type(lease) is not InboxLease:
            raise ValueError("lease must be an exact InboxLease")
        application = await self._inbox.apply_email_event(lease)
        if type(application) is not EmailEventApplication:
            raise RuntimeError("apply_email_event returned an invalid application")

        if application.disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED:
            return None

        authority = LeaseAuthority(lease)
        if application.may_complete_without_processing:
            if application.should_process:
                raise RuntimeError("application has conflicting processing authority")
            frozen = await authority.stop_and_freeze()
            completed = await self._inbox.complete(frozen)
            if completed is not True:
                raise LeaseAuthorityLost()
            return True

        if not application.should_process:
            raise RuntimeError("application has no legal Worker lifecycle owner")

        return await self._process_elected(lease, application, authority)

    async def _process_elected(
        self,
        stamped_lease: InboxLease,
        application: EmailEventApplication,
        authority: LeaseAuthority,
    ) -> ProcessingFinishResult | None:
        heartbeat_stop = asyncio.Event()
        heartbeat_cancelled_invocation = asyncio.Event()
        heartbeat_cancel_baseline: list[int | None] = [None]
        invocation = asyncio.current_task()
        if invocation is None:  # pragma: no cover - asyncio always provides this.
            raise RuntimeError("durable Worker requires an asyncio task")
        heartbeat = asyncio.create_task(
            self._heartbeat_loop(
                authority,
                heartbeat_stop,
                invocation,
                heartbeat_cancelled_invocation,
                heartbeat_cancel_baseline,
            ),
            name=f"durable-inbox-heartbeat-{stamped_lease.id}",
        )

        try:
            completion = await self._select_and_process(
                stamped_lease,
                application,
                authority,
            )
        except _PROCESS_CONTROL_EXCEPTIONS:
            await self._stop_heartbeat(heartbeat_stop, heartbeat)
            if self._consume_internal_heartbeat_cancellation(
                authority,
                heartbeat_cancelled_invocation,
                heartbeat_cancel_baseline,
            ):
                return None
            raise
        except Exception as error:
            await self._stop_heartbeat(heartbeat_stop, heartbeat)
            if authority.is_lost:
                return None
            frozen = await authority.stop_and_freeze()
            result = await self._inbox.finish_email_processing_failure(
                frozen,
                application.email_id,
                application.version,
                error,
            )
            if type(result) is not ProcessingFinishResult:
                raise RuntimeError("processing failure returned an invalid result")
            return result
        except BaseException:
            await self._stop_heartbeat(heartbeat_stop, heartbeat)
            raise

        try:
            await self._stop_heartbeat(heartbeat_stop, heartbeat)
        except asyncio.CancelledError:
            # The adapter can finish while a renewal is still in flight.  If
            # that renewal then loses authority, the heartbeat cancellation is
            # delivered here rather than inside the adapter await above.
            if self._consume_internal_heartbeat_cancellation(
                authority,
                heartbeat_cancelled_invocation,
                heartbeat_cancel_baseline,
            ):
                return None
            raise
        if authority.is_lost:
            return None
        frozen = await authority.stop_and_freeze()
        result = await self._inbox.finish_email_processing(
            frozen,
            application.email_id,
            application.version,
            completion,
        )
        if type(result) is not ProcessingFinishResult:
            raise RuntimeError("processing finish returned an invalid result")
        return result

    async def _select_and_process(
        self,
        stamped_lease: InboxLease,
        application: EmailEventApplication,
        authority: LeaseAuthority,
    ) -> ProcessingCompletion:
        policy = stamped_lease.event.processing_policy
        if policy in _LOCAL_COMPLETION_POLICIES:
            return ProcessingCompletion.no_action()
        if policy not in _ADAPTER_POLICIES:
            raise ProcessingPolicyRejected()
        if (
            policy is ProcessingPolicy.FULL
            and application.version >= POSTGRES_BIGINT_MAX - 1
        ):
            # The repository owns the atomic dead-letter transition.  Returning
            # the non-terminal completion sentinel here reaches that transaction
            # without obtaining an adapter or authorizing any external effect.
            return ProcessingCompletion.waiting_approval()

        fresh_generation = await self._ownership.get(
            stamped_lease.account_id,
            stamped_lease.generation,
        )
        adapter: ProcessingAdapter = self._router.select(
            stamped_lease,
            fresh_generation,
        )
        before_external_effect: BeforeExternalEffect = self._effect_authorizer(
            authority,
            application,
        )
        completion = await adapter.process(
            stamped_lease,
            application,
            before_external_effect=before_external_effect,
        )
        if type(completion) is not ProcessingCompletion:
            raise RuntimeError("processing adapter returned an invalid completion")
        if (
            policy is ProcessingPolicy.ARCHIVE
            and completion.target_status is not EmailStatus.ARCHIVED
        ) or (
            policy is ProcessingPolicy.FULL
            and completion.target_status is EmailStatus.ARCHIVED
        ):
            raise ProcessingPolicyRejected()
        return completion

    def _effect_authorizer(
        self,
        authority: LeaseAuthority,
        application: EmailEventApplication,
    ) -> BeforeExternalEffect:
        async def before_external_effect(
            kind: str,
            ordinal: int,
            target_hash: str,
        ) -> None:
            _validate_effect_request(kind, ordinal, target_hash)
            await authority.run_with_current(
                lambda current: self._inbox.begin_processing_effect(
                    current,
                    application.email_id,
                    application.version,
                )
            )

        return before_external_effect

    @staticmethod
    def _consume_internal_heartbeat_cancellation(
        authority: LeaseAuthority,
        cancelled_invocation: asyncio.Event,
        cancel_baseline: list[int | None],
    ) -> bool:
        """Remove only the heartbeat's own cancellation request.

        A non-zero baseline, or a remaining cancellation request after one
        ``uncancel()``, proves that an external cancellation also exists and
        must continue to propagate.
        """

        if not cancelled_invocation.is_set() or not authority.is_lost:
            return False
        current = asyncio.current_task()
        baseline = cancel_baseline[0]
        if current is None or baseline is None or current.cancelling() == 0:
            return False
        current.uncancel()
        return baseline == 0 and current.cancelling() == 0

    async def _heartbeat_loop(
        self,
        authority: LeaseAuthority,
        stop: asyncio.Event,
        invocation: asyncio.Task[object],
        cancelled_invocation: asyncio.Event,
        cancel_baseline: list[int | None],
    ) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self._heartbeat_interval_seconds,
                )
                return
            except TimeoutError:
                pass
            try:
                renewal_succeeded = await self.heartbeat_once(authority)
            except _PROCESS_CONTROL_EXCEPTIONS:
                raise
            except Exception:
                cancel_baseline[0] = invocation.cancelling()
                cancelled_invocation.set()
                invocation.cancel()
                return
            if renewal_succeeded is not True:
                cancel_baseline[0] = invocation.cancelling()
                cancelled_invocation.set()
                invocation.cancel()
                return

    @staticmethod
    async def _stop_heartbeat(
        stop: asyncio.Event,
        heartbeat: asyncio.Task[None],
    ) -> None:
        stop.set()
        try:
            await asyncio.shield(heartbeat)
        except asyncio.CancelledError:
            await asyncio.gather(heartbeat, return_exceptions=True)
            raise
        except BaseException:
            # Renewal already marked authority lost; the invocation checks it
            # before any freeze/finalize path.
            return

    async def _consume(self) -> None:
        while not self._stop_requested.is_set():
            try:
                processed = await self.run_once()
            except asyncio.CancelledError:
                raise
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                logger.warning("Durable Inbox consumer iteration failed")
                processed = 0
            if processed:
                continue
            try:
                await asyncio.wait_for(
                    self._stop_requested.wait(),
                    timeout=self._idle_seconds,
                )
            except TimeoutError:
                pass

    async def stop(self, grace_seconds: float = 30.0) -> None:
        """Stop claiming, drain inline invocations, then cancel after the grace."""

        grace = _positive_seconds("grace_seconds", grace_seconds, allow_zero=True)
        self._closed = True
        self._stop_requested.set()
        if not self._tasks:
            return
        tasks = tuple(self._tasks)
        cleanup = asyncio.create_task(
            self._finish_stop(tasks, grace),
            name="durable-inbox-worker-stop",
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise
        finally:
            self._tasks.clear()

    @staticmethod
    async def _finish_stop(
        tasks: tuple[asyncio.Task[None], ...],
        grace_seconds: float,
    ) -> None:
        _done, pending = await asyncio.wait(tasks, timeout=grace_seconds)
        for task in pending:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


__all__ = ["DurableInboxWorker", "LeaseAuthority", "LeaseAuthorityLost"]
