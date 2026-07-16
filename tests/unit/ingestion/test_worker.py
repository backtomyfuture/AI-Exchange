from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from src.domain.email_state import PipelineGenerationState
from src.ingestion.email_events import (
    EmailEventApplication,
    EmailEventDecision,
    EmailEventDisposition,
    EmailEventReason,
    EmailStatus,
)
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    InboxStatus,
    IngressSource,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    PipelineGeneration,
    ProcessingPolicy,
)
from src.ingestion.processing import (
    ProcessingAdapterUnavailable,
    ProcessingCompletion,
    ProcessingFinishResult,
)
from src.ingestion.worker import (
    DurableInboxWorker,
    LeaseAuthority,
    LeaseAuthorityLost,
)


def _lease(
    *,
    policy: ProcessingPolicy = ProcessingPolicy.FULL,
    pipeline_name: str = "legacy_compat",
    lease_until: datetime | None = None,
) -> InboxLease:
    now = datetime.now(UTC)
    event = NormalizedIngressEvent(
        account_id=8,
        source=IngressSource.WEBHOOK,
        raw_event_type="NewMailEvent",
        kind=ChangeKind.CREATE,
        external_email_id="message-1",
        folder="INBOX",
        source_version="version-1",
        dedupe_key="a" * 64,
        payload={"id": "message-1"},
        processing_policy=policy,
        source_event_at=now,
    )
    return InboxLease(
        id=str(uuid4()),
        account_id=8,
        pipeline_name=pipeline_name,
        generation=3,
        fencing_token=7,
        lease_owner="worker-1",
        attempts=1,
        event=event,
        received_at=now,
        lease_until=lease_until or now + timedelta(minutes=5),
    )


def _generation(lease: InboxLease) -> PipelineGeneration:
    return PipelineGeneration(
        account_id=lease.account_id,
        generation=lease.generation,
        pipeline_name=lease.pipeline_name,
        state=PipelineGenerationState.CURRENT_INGRESS,
        fencing_token=lease.fencing_token,
    )


def _application(
    *,
    should_process: bool = True,
    may_complete_without_processing: bool = False,
    disposition: EmailEventDisposition = EmailEventDisposition.CREATOR_ELECTED,
) -> EmailEventApplication:
    if disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED:
        reason = EmailEventReason.PROCESSING_ATTEMPT_ALREADY_ELECTED
    elif should_process:
        reason = EmailEventReason.FIRST_CREATE
    else:
        reason = EmailEventReason.METADATA_EVENT
    status = (
        EmailStatus.PROCESSING
        if should_process
        or disposition is EmailEventDisposition.PROCESSING_ALREADY_ELECTED
        else EmailStatus.INGESTED
    )
    return EmailEventApplication(
        decision=EmailEventDecision(
            should_process=should_process,
            should_cancel=False,
            new_status=status,
            cancel_pending_side_effects=False,
            create_seen=(status is EmailStatus.PROCESSING),
            reason=reason,
        ),
        email_id=str(uuid4()),
        persisted_status=status,
        version=4,
        disposition=disposition,
        may_complete_without_processing=may_complete_without_processing,
    )


class _InboxRepository:
    def __init__(self, application: EmailEventApplication) -> None:
        self.application = application
        self.application_result: object = application
        self.claimed: list[InboxLease] = []
        self.claim_result: object | None = None
        self.renew_results: list[InboxLease | None | BaseException] = []
        self.apply_calls: list[InboxLease] = []
        self.renew_calls: list[InboxLease] = []
        self.complete_calls: list[InboxLease] = []
        self.effect_calls: list[tuple[InboxLease, str, int]] = []
        self.finish_calls: list[tuple[InboxLease, str, int, ProcessingCompletion]] = []
        self.failure_calls: list[tuple[InboxLease, str, int, BaseException]] = []
        self.renewed = asyncio.Event()
        self.renew_entered = asyncio.Event()
        self.renew_release = asyncio.Event()
        self.block_renew = False
        self.effect_result: object = True
        self.complete_result: object = True
        self.finish_result: object | None = None
        self.failure_result: object | None = None

    async def claim_batch(self, worker_id, pipeline_names, limit, lease_seconds):
        assert worker_id
        assert tuple(pipeline_names)
        assert limit == 1
        assert lease_seconds > 0
        if self.claim_result is not None:
            return self.claim_result
        if not self.claimed:
            return []
        return [self.claimed.pop(0)]

    async def apply_email_event(self, lease: InboxLease) -> EmailEventApplication:
        self.apply_calls.append(lease)
        return self.application_result  # type: ignore[return-value]

    async def renew(self, lease: InboxLease, lease_seconds: int):
        self.renew_calls.append(lease)
        if self.block_renew:
            self.renew_entered.set()
            await self.renew_release.wait()
        if self.renew_results:
            result = self.renew_results.pop(0)
        else:
            result = replace(
                lease,
                lease_until=lease.lease_until + timedelta(seconds=lease_seconds),
            )
        if isinstance(result, BaseException):
            raise result
        self.renewed.set()
        return result

    async def complete(self, lease: InboxLease) -> bool:
        self.complete_calls.append(lease)
        return self.complete_result  # type: ignore[return-value]

    async def begin_processing_effect(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
    ) -> bool:
        self.effect_calls.append((lease, email_id, expected_email_version))
        return self.effect_result  # type: ignore[return-value]

    async def finish_email_processing(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
        completion: ProcessingCompletion,
    ):
        self.finish_calls.append((lease, email_id, expected_email_version, completion))
        if self.finish_result is not None:
            return self.finish_result
        return ProcessingFinishResult(
            email_status=completion.target_status,
            inbox_status=InboxStatus.COMPLETED,
            replayed=False,
        )

    async def finish_email_processing_failure(
        self,
        lease: InboxLease,
        email_id: str,
        expected_email_version: int,
        error: BaseException,
    ):
        self.failure_calls.append((lease, email_id, expected_email_version, error))
        if self.failure_result is not None:
            return self.failure_result
        return ProcessingFinishResult(
            email_status=EmailStatus.RETRY_WAIT,
            inbox_status=InboxStatus.RETRY_WAIT,
            replayed=False,
        )


class _OwnershipRepository:
    def __init__(self, generation: PipelineGeneration) -> None:
        self.generation = generation
        self.calls: list[tuple[int, int]] = []

    async def get(self, account_id: int, generation: int):
        self.calls.append((account_id, generation))
        return self.generation


class _Adapter:
    pipeline_name = "legacy_compat"

    def __init__(self) -> None:
        self.calls = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.wait_for_release = False
        self.use_effect = False
        self.swallow_effect_authorization_error = False
        self.effect_args: tuple[object, object, object] = ("detail", 0, "b" * 64)
        self.error: BaseException | None = None
        self.completion: object = ProcessingCompletion.waiting_approval()
        self.cancelled = asyncio.Event()

    async def process(
        self,
        lease: InboxLease,
        application: EmailEventApplication,
        *,
        before_external_effect,
    ) -> ProcessingCompletion:
        self.calls.append((lease, application, before_external_effect))
        self.entered.set()
        try:
            if self.wait_for_release:
                await self.release.wait()
            if self.use_effect:
                try:
                    await before_external_effect(*self.effect_args)
                except LeaseAuthorityLost:
                    if not self.swallow_effect_authorization_error:
                        raise
            if self.error is not None:
                raise self.error
            return self.completion  # type: ignore[return-value]
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class _Router:
    def __init__(self, adapter: _Adapter) -> None:
        self.adapter = adapter
        self.calls: list[tuple[InboxLease, PipelineGeneration | None]] = []
        self.error: Exception | None = None

    def select(self, lease: InboxLease, fresh_generation: PipelineGeneration | None):
        self.calls.append((lease, fresh_generation))
        if self.error is not None:
            raise self.error
        return self.adapter


def _worker(
    lease: InboxLease,
    application: EmailEventApplication,
    *,
    heartbeat_interval_seconds: float = 10.0,
    concurrency: int = 2,
):
    inbox = _InboxRepository(application)
    ownership = _OwnershipRepository(_generation(lease))
    adapter = _Adapter()
    router = _Router(adapter)
    worker = DurableInboxWorker(
        inbox,
        ownership,
        router,
        worker_id="worker-1",
        pipeline_names=("legacy_compat",),
        concurrency=concurrency,
        lease_seconds=30,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        idle_seconds=0.01,
    )
    return worker, inbox, ownership, router, adapter


async def test_lease_authority_rotates_token_and_freezes_only_latest() -> None:
    original = _lease()
    renewed = replace(
        original,
        lease_until=original.lease_until + timedelta(seconds=30),
    )
    authority = LeaseAuthority(original)

    renewal_result = await authority.renew_with_current(lambda lease: _return(renewed))

    assert renewal_result is True
    assert type(renewal_result) is bool
    assert all(
        not hasattr(authority, name)
        for name in ("lease", "current", "current_lease", "latest_lease")
    )
    frozen = await authority.stop_and_freeze()

    assert frozen == renewed
    with pytest.raises(LeaseAuthorityLost):
        await authority.run_with_current(lambda _lease: _return(True))
    with pytest.raises(LeaseAuthorityLost):
        await authority.stop_and_freeze()


def test_lease_authority_rejects_nonlease_input() -> None:
    with pytest.raises(ValueError, match="InboxLease"):
        LeaseAuthority(object())  # type: ignore[arg-type]


async def _return(value):
    return value


async def test_lease_authority_holds_mutex_across_complete_effect_cas() -> None:
    original = _lease()
    renewed = replace(
        original,
        lease_until=original.lease_until + timedelta(seconds=30),
    )
    authority = LeaseAuthority(original)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def effect(lease: InboxLease) -> bool:
        assert lease == original
        entered.set()
        await release.wait()
        return True

    effect_task = asyncio.create_task(authority.run_with_current(effect))
    await entered.wait()
    renew_task = asyncio.create_task(
        authority.renew_with_current(lambda lease: _return(renewed))
    )
    await asyncio.sleep(0)

    assert not renew_task.done()
    release.set()
    assert await effect_task is True
    assert await renew_task is True
    assert await authority.stop_and_freeze() == renewed


async def test_freeze_waits_for_inflight_renewal_and_returns_its_successor() -> None:
    original = _lease()
    renewed = replace(
        original,
        lease_until=original.lease_until + timedelta(seconds=30),
    )
    authority = LeaseAuthority(original)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def renew(lease: InboxLease) -> InboxLease:
        assert lease == original
        entered.set()
        await release.wait()
        return renewed

    renew_task = asyncio.create_task(authority.renew_with_current(renew))
    await entered.wait()
    freeze_task = asyncio.create_task(authority.stop_and_freeze())
    await asyncio.sleep(0)

    assert not freeze_task.done()
    release.set()
    assert await renew_task is True
    assert await freeze_task == renewed


@pytest.mark.parametrize("non_true_result", [False, None, 0, 1, "true"])
async def test_lease_authority_fails_closed_on_non_exact_true_cas_result(
    non_true_result: object,
) -> None:
    authority = LeaseAuthority(_lease())

    with pytest.raises(LeaseAuthorityLost):
        await authority.run_with_current(
            lambda _lease: _return(non_true_result)  # type: ignore[arg-type]
        )

    assert authority.is_lost


async def test_lease_authority_loses_on_renew_or_effect_exception() -> None:
    renew_authority = LeaseAuthority(_lease())
    effect_authority = LeaseAuthority(_lease())

    async def fail(_lease: InboxLease):
        raise RuntimeError("commit outcome unknown")

    with pytest.raises(RuntimeError, match="unknown"):
        await renew_authority.renew_with_current(fail)
    with pytest.raises(RuntimeError, match="unknown"):
        await effect_authority.run_with_current(fail)

    assert renew_authority.is_lost
    assert renew_authority.lost_event.is_set()
    assert effect_authority.is_lost
    assert effect_authority.lost_event.is_set()


async def test_lease_authority_rejects_noncallable_operations() -> None:
    authority = LeaseAuthority(_lease())

    with pytest.raises(ValueError, match="renew"):
        await authority.renew_with_current(None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="operation"):
        await authority.run_with_current(None)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_successor", [None, "forged"])
async def test_lease_authority_loses_on_missing_or_forged_renewal(
    bad_successor: object,
) -> None:
    original = _lease()
    authority = LeaseAuthority(original)
    successor = (
        None
        if bad_successor is None
        else replace(
            original,
            lease_owner="other-worker",
            lease_until=original.lease_until + timedelta(seconds=30),
        )
    )

    if successor is None:
        assert await authority.renew_with_current(lambda _lease: _return(None)) is False
    else:
        with pytest.raises(LeaseAuthorityLost):
            await authority.renew_with_current(lambda _lease: _return(successor))

    assert authority.is_lost
    with pytest.raises(LeaseAuthorityLost):
        await authority.stop_and_freeze()


async def test_lease_authority_loses_on_nonlease_renewal_result() -> None:
    authority = LeaseAuthority(_lease())

    with pytest.raises(LeaseAuthorityLost):
        await authority.renew_with_current(lambda _lease: _return(object()))  # type: ignore[arg-type]

    assert authority.is_lost


async def test_processing_already_elected_exits_without_any_lease_lifecycle_call() -> (
    None
):
    lease = _lease()
    application = _application(
        should_process=False,
        disposition=EmailEventDisposition.PROCESSING_ALREADY_ELECTED,
    )
    worker, inbox, ownership, router, adapter = _worker(lease, application)

    assert await worker.process_lease(lease) is None

    assert inbox.apply_calls == [lease]
    assert not inbox.renew_calls
    assert not inbox.complete_calls
    assert not inbox.effect_calls
    assert not inbox.finish_calls
    assert not inbox.failure_calls
    assert not ownership.calls
    assert not router.calls
    assert not adapter.calls


async def test_may_complete_without_processing_uses_initial_token_without_heartbeat() -> (
    None
):
    lease = _lease(policy=ProcessingPolicy.METADATA_ONLY)
    application = _application(
        should_process=False,
        may_complete_without_processing=True,
        disposition=EmailEventDisposition.METADATA_SHELL_CREATED,
    )
    worker, inbox, ownership, router, adapter = _worker(lease, application)

    assert await worker.process_lease(lease) is True

    assert inbox.complete_calls == [lease]
    assert not inbox.renew_calls
    assert not ownership.calls
    assert not router.calls
    assert not adapter.calls


async def test_inbox_only_completion_fails_closed_on_non_exact_true_result() -> None:
    lease = _lease(policy=ProcessingPolicy.METADATA_ONLY)
    application = _application(
        should_process=False,
        may_complete_without_processing=True,
        disposition=EmailEventDisposition.METADATA_SHELL_CREATED,
    )
    worker, inbox, _ownership, _router, _adapter = _worker(lease, application)
    inbox.complete_result = 1

    with pytest.raises(LeaseAuthorityLost):
        await worker.process_lease(lease)


@pytest.mark.parametrize(
    "policy",
    [
        ProcessingPolicy.METADATA_ONLY,
        ProcessingPolicy.IGNORED,
        ProcessingPolicy.HISTORICAL_SUPPRESSED,
    ],
)
async def test_non_effect_policy_finishes_elected_aggregate_locally(
    policy: ProcessingPolicy,
) -> None:
    lease = _lease(policy=policy)
    application = _application()
    worker, inbox, ownership, router, adapter = _worker(lease, application)

    await worker.process_lease(lease)

    assert len(inbox.finish_calls) == 1
    assert inbox.finish_calls[0][3] == ProcessingCompletion.no_action()
    assert not inbox.effect_calls
    assert not ownership.calls
    assert not router.calls
    assert not adapter.calls


async def test_full_policy_version_exhaustion_finishes_before_adapter_or_effect() -> (
    None
):
    lease = _lease(policy=ProcessingPolicy.FULL)
    application = replace(_application(), version=POSTGRES_BIGINT_MAX - 1)
    worker, inbox, ownership, router, adapter = _worker(lease, application)

    await worker.process_lease(lease)

    assert len(inbox.finish_calls) == 1
    assert inbox.finish_calls[0][3] == ProcessingCompletion.waiting_approval()
    assert not inbox.effect_calls
    assert not ownership.calls
    assert not router.calls
    assert not adapter.calls
    assert not inbox.failure_calls


async def test_archive_policy_at_full_preflight_boundary_still_uses_adapter() -> None:
    lease = _lease(policy=ProcessingPolicy.ARCHIVE)
    application = replace(_application(), version=POSTGRES_BIGINT_MAX - 1)
    worker, inbox, ownership, router, adapter = _worker(lease, application)
    adapter.completion = ProcessingCompletion.archived()

    await worker.process_lease(lease)

    assert ownership.calls == [(lease.account_id, lease.generation)]
    assert len(router.calls) == 1
    assert len(adapter.calls) == 1
    assert inbox.finish_calls[0][3] == ProcessingCompletion.archived()
    assert not inbox.failure_calls


async def test_heartbeat_rotation_authorizes_effect_and_finish_with_latest_token() -> (
    None
):
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        heartbeat_interval_seconds=0.01,
    )
    renewed = replace(
        lease,
        lease_until=lease.lease_until + timedelta(seconds=30),
    )
    inbox.renew_results.append(renewed)
    adapter.wait_for_release = True
    adapter.use_effect = True

    task = asyncio.create_task(worker.process_lease(lease))
    await adapter.entered.wait()
    await asyncio.wait_for(inbox.renewed.wait(), timeout=1)
    adapter.release.set()
    await task

    assert inbox.effect_calls == [(renewed, application.email_id, application.version)]
    assert inbox.finish_calls[0][:3] == (
        renewed,
        application.email_id,
        application.version,
    )
    assert not inbox.failure_calls


async def test_worker_awaits_inflight_renewal_before_freeze_and_finish() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        heartbeat_interval_seconds=0.01,
    )
    renewed = replace(
        lease,
        lease_until=lease.lease_until + timedelta(seconds=30),
    )
    inbox.renew_results.append(renewed)
    inbox.block_renew = True
    adapter.wait_for_release = True

    task = asyncio.create_task(worker.process_lease(lease))
    await adapter.entered.wait()
    await asyncio.wait_for(inbox.renew_entered.wait(), timeout=1)
    adapter.release.set()
    await asyncio.sleep(0)

    assert not task.done()
    assert not inbox.finish_calls
    inbox.renew_release.set()
    await task

    assert inbox.finish_calls[0][0] == renewed


@pytest.mark.parametrize(
    "renewal_result",
    [None, RuntimeError("database unavailable")],
    ids=["lease-lost", "renewal-error"],
)
async def test_inflight_heartbeat_loss_after_adapter_returns_is_not_external_cancel(
    renewal_result: InboxLease | None | BaseException,
) -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        heartbeat_interval_seconds=0.01,
    )
    inbox.renew_results.append(renewal_result)
    inbox.block_renew = True
    adapter.wait_for_release = True

    task = asyncio.create_task(worker.process_lease(lease))
    await adapter.entered.wait()
    await asyncio.wait_for(inbox.renew_entered.wait(), timeout=1)
    adapter.release.set()
    await asyncio.sleep(0)
    inbox.renew_release.set()

    assert await asyncio.wait_for(task, timeout=1) is None
    assert not task.cancelled()
    assert not inbox.finish_calls
    assert not inbox.failure_calls


async def test_inflight_heartbeat_loss_does_not_stop_fixed_consumer() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        heartbeat_interval_seconds=0.01,
        concurrency=1,
    )
    inbox.claimed.append(lease)
    inbox.renew_results.append(None)
    inbox.block_renew = True
    adapter.wait_for_release = True
    claimed_again = asyncio.Event()
    claim_count = 0
    original_claim_batch = inbox.claim_batch

    async def tracked_claim_batch(*args, **kwargs):
        nonlocal claim_count
        claim_count += 1
        result = await original_claim_batch(*args, **kwargs)
        if claim_count >= 2:
            claimed_again.set()
        return result

    inbox.claim_batch = tracked_claim_batch  # type: ignore[method-assign]

    await worker.start()
    await adapter.entered.wait()
    await asyncio.wait_for(inbox.renew_entered.wait(), timeout=1)
    adapter.release.set()
    await asyncio.sleep(0)
    inbox.renew_release.set()

    await asyncio.wait_for(claimed_again.wait(), timeout=1)
    assert len(worker.tasks) == 1
    assert not worker.tasks[0].done()
    assert not inbox.finish_calls
    assert not inbox.failure_calls
    await worker.stop(grace_seconds=1)


async def test_external_cancel_during_normal_heartbeat_stop_still_propagates() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        heartbeat_interval_seconds=0.01,
    )
    inbox.block_renew = True
    adapter.wait_for_release = True

    task = asyncio.create_task(worker.process_lease(lease))
    await adapter.entered.wait()
    await asyncio.wait_for(inbox.renew_entered.wait(), timeout=1)
    adapter.release.set()
    await asyncio.sleep(0)
    task.cancel()
    inbox.renew_release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert not inbox.finish_calls
    assert not inbox.failure_calls


async def test_internal_cancellation_without_baseline_is_not_consumed() -> None:
    lease = _lease()
    authority = LeaseAuthority(lease)
    assert await authority.renew_with_current(lambda _lease: _return(None)) is False
    cancelled_invocation = asyncio.Event()
    cancelled_invocation.set()

    assert (
        DurableInboxWorker._consume_internal_heartbeat_cancellation(
            authority,
            cancelled_invocation,
            [None],
        )
        is False
    )


async def test_lost_heartbeat_cancels_adapter_without_failure_or_receipt() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        heartbeat_interval_seconds=0.01,
    )
    inbox.renew_results.append(None)
    adapter.wait_for_release = True

    result = await asyncio.wait_for(worker.process_lease(lease), timeout=1)

    assert result is None
    assert adapter.cancelled.is_set()
    assert not inbox.effect_calls
    assert not inbox.finish_calls
    assert not inbox.failure_calls


@pytest.mark.parametrize("effect_result", [False, None, 0, 1])
async def test_failed_effect_cas_loses_authority_and_never_finalizes(
    effect_result: object,
) -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    inbox.effect_result = effect_result
    adapter.use_effect = True

    assert await worker.process_lease(lease) is None

    assert len(inbox.effect_calls) == 1
    assert not inbox.finish_calls
    assert not inbox.failure_calls


async def test_lost_authority_blocks_finish_even_if_adapter_swallows_guard_error() -> (
    None
):
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    inbox.effect_result = False
    adapter.use_effect = True
    adapter.swallow_effect_authorization_error = True

    assert await worker.process_lease(lease) is None

    assert len(inbox.effect_calls) == 1
    assert not inbox.finish_calls
    assert not inbox.failure_calls


async def test_adapter_failure_is_atomically_finished_as_failure() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.error = RuntimeError("private details")

    await worker.process_lease(lease)

    assert not inbox.finish_calls
    assert len(inbox.failure_calls) == 1
    assert inbox.failure_calls[0][:3] == (
        lease,
        application.email_id,
        application.version,
    )


async def test_heartbeat_exception_cancels_adapter_without_finalization() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        heartbeat_interval_seconds=0.01,
    )
    inbox.renew_results.append(RuntimeError("database unavailable"))
    adapter.wait_for_release = True

    assert await asyncio.wait_for(worker.process_lease(lease), timeout=1) is None

    assert adapter.cancelled.is_set()
    assert not inbox.finish_calls
    assert not inbox.failure_calls


@pytest.mark.parametrize(
    "effect_args",
    [
        ("unknown", 0, "b" * 64),
        ("detail", True, "b" * 64),
        ("detail", -1, "b" * 64),
        ("detail", 0, "B" * 64),
    ],
)
async def test_invalid_effect_request_is_failed_before_database_cas(
    effect_args: tuple[object, object, object],
) -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.use_effect = True
    adapter.effect_args = effect_args

    await worker.process_lease(lease)

    assert not inbox.effect_calls
    assert len(inbox.failure_calls) == 1


@pytest.mark.parametrize(
    ("policy", "completion", "should_fail"),
    [
        (ProcessingPolicy.ARCHIVE, ProcessingCompletion.archived(), False),
        (ProcessingPolicy.ARCHIVE, ProcessingCompletion.waiting_approval(), True),
        (ProcessingPolicy.FULL, ProcessingCompletion.archived(), True),
    ],
)
async def test_adapter_completion_must_match_the_policy_lane(
    policy: ProcessingPolicy,
    completion: ProcessingCompletion,
    should_fail: bool,
) -> None:
    lease = _lease(policy=policy)
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.completion = completion

    await worker.process_lease(lease)

    assert bool(inbox.failure_calls) is should_fail
    assert bool(inbox.finish_calls) is not should_fail


async def test_invalid_adapter_or_repository_finish_result_fails_closed() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.completion = object()

    await worker.process_lease(lease)
    assert len(inbox.failure_calls) == 1

    worker, inbox, _ownership, _router, _adapter = _worker(lease, application)
    inbox.finish_result = object()
    with pytest.raises(RuntimeError, match="finish returned"):
        await worker.process_lease(lease)

    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.error = RuntimeError("adapter failed")
    inbox.failure_result = object()
    with pytest.raises(RuntimeError, match="failure returned"):
        await worker.process_lease(lease)


async def test_unavailable_router_never_falls_back_and_uses_aggregate_failure() -> None:
    lease = _lease(pipeline_name="durable_candidate")
    application = _application()
    worker, inbox, ownership, router, adapter = _worker(lease, application)
    ownership.generation = _generation(lease)
    router.error = ProcessingAdapterUnavailable()

    await worker.process_lease(lease)

    assert not adapter.calls
    assert not inbox.effect_calls
    assert len(inbox.failure_calls) == 1


async def test_external_cancellation_is_never_classified_or_finalized() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.wait_for_release = True

    task = asyncio.create_task(worker.process_lease(lease))
    await adapter.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not inbox.finish_calls
    assert not inbox.failure_calls


async def test_system_exit_is_never_classified_or_finalized() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.error = SystemExit(17)

    with pytest.raises(SystemExit) as raised:
        await worker.process_lease(lease)

    assert raised.value.code == 17
    assert not inbox.finish_calls
    assert not inbox.failure_calls


async def test_unknown_base_exception_still_stops_heartbeat_without_receipt() -> None:
    class ProcessControlProbe(BaseException):
        pass

    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(lease, application)
    adapter.error = ProcessControlProbe()

    with pytest.raises(ProcessControlProbe):
        await worker.process_lease(lease)

    assert not inbox.finish_calls
    assert not inbox.failure_calls


async def test_worker_starts_only_fixed_consumers_and_stops_idempotently() -> None:
    lease = _lease()
    worker, _inbox, _ownership, _router, _adapter = _worker(
        lease,
        _application(),
        concurrency=3,
    )

    await worker.start()
    assert len(worker.tasks) == worker.concurrency == 3
    original_tasks = worker.tasks
    await worker.start()
    assert worker.tasks == original_tasks
    await asyncio.sleep(0.02)
    await worker.stop(grace_seconds=1)
    await worker.stop(grace_seconds=1)

    assert worker.tasks == ()
    assert await worker.run_once() == 0


async def test_zero_grace_stop_cancels_inline_processing_without_receipt() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, adapter = _worker(
        lease,
        application,
        concurrency=1,
    )
    inbox.claimed.append(lease)
    adapter.wait_for_release = True

    await worker.start()
    await adapter.entered.wait()
    await worker.stop(grace_seconds=0)

    assert adapter.cancelled.is_set()
    assert worker.tasks == ()
    assert not inbox.finish_calls
    assert not inbox.failure_calls


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"worker_id": ""}, "worker_id"),
        ({"pipeline_names": "legacy_compat"}, "pipeline_names"),
        ({"pipeline_names": ()}, "pipeline_names"),
        ({"pipeline_names": ("legacy_compat", "legacy_compat")}, "pipeline_names"),
        ({"pipeline_names": ("durable_candidate",)}, "legacy_compat"),
        ({"concurrency": 0}, "concurrency"),
        ({"lease_seconds": True}, "lease_seconds"),
        ({"heartbeat_interval_seconds": True}, "heartbeat_interval_seconds"),
        ({"heartbeat_interval_seconds": 0}, "heartbeat_interval_seconds"),
        ({"heartbeat_interval_seconds": float("nan")}, "heartbeat_interval_seconds"),
        ({"heartbeat_interval_seconds": 30}, "heartbeat interval"),
        ({"idle_seconds": 0}, "idle_seconds"),
    ],
)
def test_worker_rejects_invalid_bounded_runtime_inputs(
    overrides: dict[str, object],
    message: str,
) -> None:
    lease = _lease()
    application = _application()
    inbox = _InboxRepository(application)
    ownership = _OwnershipRepository(_generation(lease))
    router = _Router(_Adapter())
    values: dict[str, object] = {
        "worker_id": "worker-1",
        "pipeline_names": ("legacy_compat",),
        "concurrency": 1,
        "lease_seconds": 30,
        "heartbeat_interval_seconds": 10,
        "idle_seconds": 0.01,
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        DurableInboxWorker(inbox, ownership, router, **values)  # type: ignore[arg-type]


async def test_run_once_claims_one_lease_and_processes_inline() -> None:
    lease = _lease(policy=ProcessingPolicy.METADATA_ONLY)
    application = _application(
        should_process=False,
        may_complete_without_processing=True,
        disposition=EmailEventDisposition.METADATA_SHELL_CREATED,
    )
    worker, inbox, _ownership, _router, _adapter = _worker(lease, application)
    inbox.claimed.append(lease)

    assert await worker.run_once() == 1
    assert inbox.complete_calls == [lease]
    assert await worker.run_once() == 0


async def test_worker_rejects_invalid_claim_and_application_shapes() -> None:
    lease = _lease()
    application = _application()
    worker, inbox, _ownership, _router, _adapter = _worker(lease, application)

    inbox.claim_result = (lease,)
    with pytest.raises(RuntimeError, match="one-lease"):
        await worker.run_once()

    inbox.claim_result = [object()]
    with pytest.raises(RuntimeError, match="invalid lease"):
        await worker.run_once()

    inbox.claim_result = None
    inbox.claimed.append(lease)
    inbox.application_result = object()
    with pytest.raises(RuntimeError, match="invalid application"):
        await worker.run_once()

    with pytest.raises(ValueError, match="InboxLease"):
        await worker.process_lease(object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="LeaseAuthority"):
        await worker.heartbeat_once(object())  # type: ignore[arg-type]
