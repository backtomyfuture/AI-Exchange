from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.domain.email_state import PipelineGenerationState
from src.domain.errors import DatabaseOperationError
from src.ingestion.email_events import EmailStatus
from src.ingestion.models import (
    InboxLease,
    InboxStatus,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
)
from src.ingestion.processing import (
    GuardedExternalEffectFailed,
    ProcessingCompletion,
    ProcessingCompletionRejected,
    ProcessingReceiptConflict,
    ReplaySafeExternalEffectFailed,
)
from src.ingestion.repository import (
    InboxRepository,
    _EmailRow,
    _ExpiredLeaseCandidate,
    _ProcessingInboxRow,
    _ProcessingResolution,
    _expired_candidate_from_row,
    _leases_equal,
    _processing_attempt_event_key,
    _processing_result_event_key,
    _require_email_id,
    _require_email_version,
)


class _AsyncContext:
    def __init__(self, value: object) -> None:
        self._value = value

    async def __aenter__(self) -> object:
        return self._value

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _Cursor:
    def __init__(
        self,
        *,
        one: object | None = None,
        many: list[object] | None = None,
    ) -> None:
        self._one = one
        self._many = many if many is not None else []

    async def fetchone(self) -> object | None:
        return self._one

    async def fetchall(self) -> list[object]:
        return self._many


class _QueuedConnection:
    def __init__(self, *cursors: _Cursor) -> None:
        self._cursors = list(cursors)

    def transaction(self) -> _AsyncContext:
        return _AsyncContext(self)

    async def execute(self, _statement: object, _params: object = None) -> _Cursor:
        if not self._cursors:
            raise AssertionError("unexpected database statement")
        return self._cursors.pop(0)


class _Pool:
    def __init__(self, connection: _QueuedConnection) -> None:
        self._connection = connection

    def connection(self) -> _AsyncContext:
        return _AsyncContext(self._connection)


class _MissingAttemptTransaction:
    async def _load_processing_attempt(
        self,
        _email: _EmailRow,
        _lease: InboxLease,
    ) -> bool:
        return False


class _NonExceptionFailure(BaseException):
    pass


def _lease(event: NormalizedIngressEvent) -> InboxLease:
    now = datetime.now(UTC)
    return InboxLease(
        id=str(uuid4()),
        account_id=event.account_id,
        pipeline_name="legacy_compat",
        generation=3,
        fencing_token=7,
        execution_epoch=0,
        authority_epoch=1,
        capability_hash="a" * 64,
        lease_session_id="00000000-0000-4000-8000-000000000002",
        lease_owner="worker-task8",
        attempts=1,
        event=event,
        received_at=now,
        lease_until=now + timedelta(minutes=5),
    )


def _expired_candidate(lease: InboxLease) -> _ExpiredLeaseCandidate:
    return _ExpiredLeaseCandidate(
        id=lease.id,
        account_id=lease.account_id,
        pipeline_name=lease.pipeline_name,
        generation=lease.generation,
        fencing_token=lease.fencing_token,
        execution_epoch=lease.execution_epoch,
        authority_epoch=lease.authority_epoch,
        capability_hash=lease.capability_hash,
        lease_session_id=lease.lease_session_id,
        lease_owner=lease.lease_owner,
        attempts=lease.attempts,
        event=lease.event,
        received_at=lease.received_at,
        lease_until=lease.lease_until,
    )


def _lease_database_row(
    lease: InboxLease,
    **overrides: object,
) -> dict[str, object]:
    event = lease.event
    row: dict[str, object] = {
        "id": lease.id,
        "account_id": lease.account_id,
        "external_email_id": event.external_email_id,
        "folder_key": event.folder,
        "source": event.source.value,
        "raw_event_type": event.raw_event_type,
        "change_kind": event.kind.value,
        "dedupe_key": event.dedupe_key,
        "source_version": event.source_version,
        "source_event_at": event.source_event_at,
        "payload": event.payload_for_storage(),
        "processing_policy": event.processing_policy.value,
        "pipeline_name": lease.pipeline_name,
        "generation": lease.generation,
        "fencing_token": lease.fencing_token,
        "execution_epoch": lease.execution_epoch,
        "authority_epoch": lease.authority_epoch,
        "capability_hash": lease.capability_hash,
        "lease_session_id": lease.lease_session_id,
        "lease_owner": lease.lease_owner,
        "lease_until": lease.lease_until,
        "attempts": lease.attempts,
        "received_at": lease.received_at,
    }
    row.update(overrides)
    return row


def _email_database_row(
    lease: InboxLease,
    **overrides: object,
) -> dict[str, object]:
    now = datetime.now(UTC)
    row: dict[str, object] = {
        "id": str(uuid4()),
        "account_id": lease.account_id,
        "external_email_id": lease.event.external_email_id,
        "source_folder_key": lease.event.folder,
        "status": EmailStatus.PROCESSING.value,
        "version": 4,
        "owner_generation": lease.generation,
        "owner_fencing_token": lease.fencing_token,
        "owner_authority_epoch": lease.authority_epoch,
        "owner_capability_hash": lease.capability_hash,
        "processing_inbox_id": lease.id,
        "processing_execution_epoch": lease.execution_epoch,
        "create_seen_at": now,
        "processing_started_at": now,
        "source_deleted_at": None,
        "external_effects_started_at": None,
        "safe_error_code": None,
        "safe_error_summary": None,
        "is_read": None,
        "is_read_refresh_required": False,
        "updated_at": now,
    }
    row.update(overrides)
    return row


def _email(
    lease: InboxLease,
    **overrides: object,
) -> _EmailRow:
    values: dict[str, object] = {
        "id": str(uuid4()),
        "account_id": lease.account_id,
        "external_email_id": lease.event.external_email_id,
        "source_folder_key": lease.event.folder,
        "status": EmailStatus.PROCESSING,
        "version": 4,
        "owner_generation": lease.generation,
        "owner_fencing_token": lease.fencing_token,
        "owner_authority_epoch": lease.authority_epoch,
        "owner_capability_hash": lease.capability_hash,
        "processing_inbox_id": lease.id,
        "processing_execution_epoch": lease.execution_epoch,
        "create_seen_at": datetime.now(UTC),
        "processing_started_at": datetime.now(UTC),
        "source_deleted_at": None,
        "external_effects_started_at": None,
        "safe_error_code": None,
        "safe_error_summary": None,
        "is_read": None,
        "is_read_refresh_required": False,
        "updated_at": datetime.now(UTC),
    }
    values.update(overrides)
    return _EmailRow(**values)  # type: ignore[arg-type]


def _inbox(
    token: InboxLease,
    **overrides: object,
) -> _ProcessingInboxRow:
    values: dict[str, object] = {
        "status": InboxStatus.LEASED,
        "lease": token,
        "effect_started_at": None,
        "attempts": token.attempts,
        "account_id": token.account_id,
        "external_email_id": token.event.external_email_id,
        "generation": token.generation,
        "fencing_token": token.fencing_token,
        "lease_active": True,
        "safe_error_code": None,
        "safe_error_summary": None,
    }
    values.update(overrides)
    return _ProcessingInboxRow(**values)  # type: ignore[arg-type]


def _resolution(lease: InboxLease) -> _ProcessingResolution:
    return _ProcessingResolution(
        email_status=EmailStatus.NO_ACTION,
        inbox_status=InboxStatus.COMPLETED,
        safe_code=None,
        safe_summary=None,
        attempts=lease.attempts,
        available_in_seconds=0,
    )


@pytest.mark.parametrize(
    ("effect_started", "expected_status", "expected_code"),
    [
        (
            False,
            InboxStatus.RETRY_WAIT,
            "inbox.transient_dependency",
        ),
        (
            True,
            InboxStatus.MANUAL_REVIEW,
            "inbox.effect_outcome_unknown",
        ),
    ],
    ids=["no-effect-marker", "effect-marker-remains-conservative"],
)
def test_failure_resolution_retries_safe_detail_only_without_effect_marker(
    normalized_event: NormalizedIngressEvent,
    effect_started: bool,
    expected_status: InboxStatus,
    expected_code: str,
) -> None:
    lease = _lease(normalized_event)
    effect_started_at = datetime.now(UTC) if effect_started else None

    resolution = InboxRepository._failure_resolution(
        error=ReplaySafeExternalEffectFailed(),
        lease=lease,
        email=_email(
            lease,
            external_effects_started_at=effect_started_at,
        ),
        inbox=_inbox(
            lease,
            effect_started_at=effect_started_at,
        ),
        expected_email_version=4,
    )

    assert resolution.inbox_status is expected_status
    assert resolution.email_status.value == expected_status.value
    assert resolution.safe_code == expected_code


def test_unknown_external_outcome_with_effect_marker_still_requires_review(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    effect_started_at = datetime.now(UTC)

    resolution = InboxRepository._failure_resolution(
        error=GuardedExternalEffectFailed(),
        lease=lease,
        email=_email(
            lease,
            external_effects_started_at=effect_started_at,
        ),
        inbox=_inbox(
            lease,
            effect_started_at=effect_started_at,
        ),
        expected_email_version=4,
    )

    assert resolution.inbox_status is InboxStatus.MANUAL_REVIEW
    assert resolution.safe_code == "inbox.effect_outcome_unknown"


def _repository_with_connection(
    *cursors: _Cursor,
) -> tuple[InboxRepository, _QueuedConnection]:
    connection = _QueuedConnection(*cursors)
    return InboxRepository(_Pool(connection)), connection


def _patch_transaction_setup(
    repository: InboxRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repository, "_configure_transaction", AsyncMock())
    monkeypatch.setattr(repository, "_acquire_account_lock", AsyncMock())


@pytest.mark.parametrize(
    ("field", "current_value", "stale_value"),
    (
        ("execution_epoch", 2, 1),
        ("authority_epoch", 2, 1),
        ("capability_hash", "b" * 64, "a" * 64),
        (
            "lease_session_id",
            "00000000-0000-4000-8000-000000000003",
            "00000000-0000-4000-8000-000000000002",
        ),
    ),
)
def test_lease_equality_includes_every_runtime_authorization_stamp(
    normalized_event: NormalizedIngressEvent,
    field: str,
    current_value: object,
    stale_value: object,
) -> None:
    current = replace(_lease(normalized_event), **{field: current_value})
    stale = replace(current, **{field: stale_value})

    assert _leases_equal(current, current) is True
    assert _leases_equal(stale, current) is False
    assert _leases_equal(current, stale) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "current_value", "stale_value"),
    (
        ("execution_epoch", 2, 1),
        ("authority_epoch", 2, 1),
        ("capability_hash", "b" * 64, "a" * 64),
        (
            "lease_session_id",
            "00000000-0000-4000-8000-000000000003",
            "00000000-0000-4000-8000-000000000002",
        ),
    ),
)
async def test_processing_authorization_rejects_each_stale_runtime_stamp_before_io(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    current_value: object,
    stale_value: object,
) -> None:
    current = replace(_lease(normalized_event), **{field: current_value})
    stale = replace(current, **{field: stale_value})
    repository = InboxRepository(object())
    transaction_calls = 0

    def forbidden_transaction(_connection: object) -> object:
        nonlocal transaction_calls
        transaction_calls += 1
        raise AssertionError("stale lease must fail before receipt lookup")

    monkeypatch.setattr(repository, "transaction", forbidden_transaction)

    with pytest.raises(ProcessingCompletionRejected):
        await repository._require_authorized_processing_attempt(
            object(),  # type: ignore[arg-type]
            lease=current,
            email=_email(current),
            inbox=_inbox(current, lease=stale),
            expected_email_version=4,
            ownership_state=PipelineGenerationState.CURRENT_INGRESS,
            require_unexpired=True,
        )

    assert transaction_calls == 0


def test_processing_receipt_event_keys_bind_execution_epoch_exactly(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    attempt_epoch_0 = _processing_attempt_event_key(lease.id, 0, lease.attempts)
    attempt_epoch_1 = _processing_attempt_event_key(lease.id, 1, lease.attempts)
    result_epoch_0 = _processing_result_event_key(
        lease.id,
        0,
        lease.attempts,
        "success",
    )
    result_epoch_1 = _processing_result_event_key(
        lease.id,
        1,
        lease.attempts,
        "success",
    )

    assert (
        attempt_epoch_0
        == hashlib.sha256(
            b"email-processing-attempt-v1\x00"
            + lease.id.encode("ascii")
            + b"\x000\x00"
            + str(lease.attempts).encode("ascii")
        ).hexdigest()
    )
    assert (
        attempt_epoch_1
        == hashlib.sha256(
            b"email-processing-attempt-v1\x00"
            + lease.id.encode("ascii")
            + b"\x001\x00"
            + str(lease.attempts).encode("ascii")
        ).hexdigest()
    )
    assert (
        result_epoch_0
        == hashlib.sha256(
            b"email-processing-result-v1\x00"
            + lease.id.encode("ascii")
            + b"\x000\x00"
            + str(lease.attempts).encode("ascii")
            + b"\x00success"
        ).hexdigest()
    )
    assert (
        result_epoch_1
        == hashlib.sha256(
            b"email-processing-result-v1\x00"
            + lease.id.encode("ascii")
            + b"\x001\x00"
            + str(lease.attempts).encode("ascii")
            + b"\x00success"
        ).hexdigest()
    )
    assert attempt_epoch_0 != attempt_epoch_1
    assert result_epoch_0 != result_epoch_1


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"payload": []}, "expired inbox candidate row is invalid"),
        ({"attempts": True}, "expired inbox candidate row is invalid"),
    ],
)
def test_expired_candidate_decoder_fails_closed_on_invalid_database_shape(
    normalized_event: NormalizedIngressEvent,
    overrides: dict[str, object],
    message: str,
) -> None:
    lease = _lease(normalized_event)

    with pytest.raises(DatabaseOperationError, match=message) as caught:
        _expired_candidate_from_row(_lease_database_row(lease, **overrides))

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False


@pytest.mark.parametrize(
    ("validator", "value", "message"),
    [
        (_require_email_id, object(), "UUID string"),
        (_require_email_id, "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", "canonical"),
        (_require_email_version, True, "PostgreSQL BIGINT"),
    ],
)
def test_processing_identifiers_are_exact_before_database_access(
    validator: object,
    value: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validator(value)  # type: ignore[operator]


@pytest.mark.asyncio
async def test_processing_email_lock_reports_missing_row_without_synthesizing_state(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    repository, connection = _repository_with_connection(_Cursor(one=None))

    assert (
        await repository._lock_processing_email(
            connection,
            lease,
            str(uuid4()),  # type: ignore[arg-type]
        )
        is None
    )


@pytest.mark.asyncio
async def test_processing_email_lock_rejects_cross_message_relation(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    row = _email_database_row(lease, external_email_id="different-message")
    repository, connection = _repository_with_connection(_Cursor(one=row))

    with pytest.raises(ProcessingCompletionRejected):
        await repository._lock_processing_email(
            connection,
            lease,
            str(row["id"]),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_processing_ownership_lock_rejects_noninteger_generation(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    row = {
        "account_id": lease.account_id,
        "generation": True,
        "pipeline_name": lease.pipeline_name,
        "state": PipelineGenerationState.CURRENT_INGRESS.value,
        "fencing_token": lease.fencing_token,
    }
    repository, connection = _repository_with_connection(_Cursor(many=[row]))

    with pytest.raises(DatabaseOperationError, match="ownership row is invalid"):
        await repository._lock_processing_ownership(
            connection,
            None,
            lease,
            require_executable=True,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_processing_ownership_lock_rejects_missing_generation(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    repository, connection = _repository_with_connection(_Cursor(many=[]))

    with pytest.raises(ProcessingCompletionRejected):
        await repository._lock_processing_ownership(
            connection,
            None,
            lease,
            require_executable=True,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_processing_ownership_lock_rejects_changed_incoming_stamp(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    row = {
        "account_id": lease.account_id,
        "generation": lease.generation,
        "pipeline_name": "replacement-pipeline",
        "state": PipelineGenerationState.CURRENT_INGRESS.value,
        "fencing_token": lease.fencing_token,
    }
    repository, connection = _repository_with_connection(_Cursor(many=[row]))

    with pytest.raises(ProcessingCompletionRejected):
        await repository._lock_processing_ownership(
            connection,
            None,
            lease,
            require_executable=True,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_processing_ownership_lock_rejects_changed_sticky_stamp(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    email = _email(lease, owner_generation=lease.generation + 1)
    incoming = {
        "account_id": lease.account_id,
        "generation": lease.generation,
        "pipeline_name": lease.pipeline_name,
        "state": PipelineGenerationState.CURRENT_INGRESS.value,
        "fencing_token": lease.fencing_token,
    }
    sticky = {
        "account_id": lease.account_id,
        "generation": email.owner_generation,
        "pipeline_name": lease.pipeline_name,
        "state": PipelineGenerationState.QUIESCING.value,
        "fencing_token": email.owner_fencing_token + 1,
    }
    repository, connection = _repository_with_connection(
        _Cursor(many=[incoming, sticky])
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository._lock_processing_ownership(
            connection,
            email,
            lease,
            require_executable=False,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_processing_inbox_lock_reports_missing_row_without_guessing_state(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    repository, connection = _repository_with_connection(_Cursor(one=None))

    assert (
        await repository._lock_processing_inbox(
            connection,
            lease,  # type: ignore[arg-type]
        )
        is None
    )


@pytest.mark.asyncio
async def test_processing_inbox_lock_rejects_invalid_scalar_projection(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    row = _lease_database_row(
        lease,
        attempts=True,
        inbox_status=InboxStatus.COMPLETED.value,
        effect_started_at=None,
        lease_active=False,
        safe_error_code=None,
        safe_error_summary=None,
    )
    repository, connection = _repository_with_connection(_Cursor(one=row))

    with pytest.raises(DatabaseOperationError, match="processing row is invalid"):
        await repository._lock_processing_inbox(
            connection,
            lease,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_expired_inbox_lock_rejects_invalid_scalar_projection(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    candidate = _expired_candidate(lease)
    row = _lease_database_row(
        lease,
        account_id=True,
        inbox_status=InboxStatus.COMPLETED.value,
        effect_started_at=None,
        lease_active=False,
        safe_error_code=None,
        safe_error_summary=None,
    )
    repository, connection = _repository_with_connection(_Cursor(one=row))

    with pytest.raises(
        DatabaseOperationError, match="expired inbox lock row is invalid"
    ):
        await repository._lock_expired_inbox(
            connection,
            candidate,
            skip_locked=True,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_unlinked_expired_lease_with_existing_email_is_requeued(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _lease(normalized_event)
    candidate = _expired_candidate(lease)
    repository, connection = _repository_with_connection(
        _Cursor(one=(str(uuid4()), None)),
        _Cursor(one=(candidate.id,)),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_expired_inbox",
        AsyncMock(return_value=_inbox(candidate, lease_active=False)),
    )
    append_audit = AsyncMock()
    monkeypatch.setattr(repository, "_append_audit", append_audit)

    recovered = await repository._recover_unlinked_expired_lease(
        connection,  # type: ignore[arg-type]
        candidate,
    )

    assert recovered is True
    append_audit.assert_not_awaited()


@pytest.mark.asyncio
async def test_unlinked_recovery_defers_if_email_becomes_linked_to_same_inbox(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _lease(normalized_event)
    candidate = _expired_candidate(lease)
    repository, connection = _repository_with_connection(
        _Cursor(one=(str(uuid4()), candidate.id)),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_expired_inbox",
        AsyncMock(return_value=_inbox(candidate, lease_active=False)),
    )

    recovered = await repository._recover_unlinked_expired_lease(
        connection,  # type: ignore[arg-type]
        candidate,
    )

    assert recovered is False


@pytest.mark.asyncio
async def test_processing_receipt_append_rejects_unreadable_insert_result(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _lease(normalized_event)
    repository, connection = _repository_with_connection(_Cursor())
    monkeypatch.setattr(
        repository,
        "_load_processing_result_receipt",
        AsyncMock(return_value=False),
    )

    with pytest.raises(ProcessingReceiptConflict):
        await repository._append_processing_result_receipt(
            connection,  # type: ignore[arg-type]
            lease=lease,
            email_id=str(uuid4()),
            expected_email_version=4,
            operation="success",
            resolution=_resolution(lease),
            completion=ProcessingCompletion.no_action(),
        )


def test_processing_replay_rejects_split_email_and_inbox_state(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    resolution = _resolution(lease)
    email = _email(
        lease,
        status=resolution.email_status,
        version=5,
        processing_inbox_id=lease.id,
    )
    inbox = _inbox(
        lease,
        status=resolution.inbox_status,
        lease=None,
    )

    with pytest.raises(ProcessingReceiptConflict):
        InboxRepository._assert_processing_replay_state(
            lease=lease,
            email=email,
            inbox=inbox,
            expected_email_version=4,
            operation="success",
            resolution=resolution,
        )


@pytest.mark.asyncio
async def test_effect_authorization_rejects_missing_attempt_receipt(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _lease(normalized_event)
    repository = InboxRepository(object())
    monkeypatch.setattr(
        repository,
        "transaction",
        lambda _connection: _MissingAttemptTransaction(),
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository._require_authorized_processing_attempt(
            object(),  # type: ignore[arg-type]
            lease=lease,
            email=_email(lease),
            inbox=_inbox(lease),
            expected_email_version=4,
            ownership_state=PipelineGenerationState.CURRENT_INGRESS,
            require_unexpired=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["email", "inbox"])
async def test_effect_start_fails_closed_when_locked_relation_disappears(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    lease = _lease(normalized_event)
    repository, _connection = _repository_with_connection()
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease)
    monkeypatch.setattr(
        repository,
        "_lock_processing_email",
        AsyncMock(return_value=None if missing == "email" else email),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=None),
    )

    assert (
        await repository.begin_processing_effect(lease, email.id, 4, "feishu")
        is False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_active", [False, True])
async def test_effect_start_idempotency_still_requires_live_runtime_session(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    runtime_active: bool,
) -> None:
    lease = _lease(normalized_event)
    now = datetime.now(UTC)
    runtime_row = {"session_id": lease.lease_session_id} if runtime_active else None
    repository, _connection = _repository_with_connection(_Cursor(one=runtime_row))
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease, external_effects_started_at=now)
    monkeypatch.setattr(
        repository,
        "_lock_processing_email",
        AsyncMock(return_value=email),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=_inbox(lease, effect_started_at=now)),
    )
    monkeypatch.setattr(
        repository,
        "_require_authorized_processing_attempt",
        AsyncMock(),
    )

    assert (
        await repository.begin_processing_effect(lease, email.id, 4, "feishu")
        is runtime_active
    )


@pytest.mark.asyncio
async def test_detail_authorization_checks_live_attempt_without_writing_effect_marker(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _lease(normalized_event)
    runtime_row = {"session_id": lease.lease_session_id}
    repository, _connection = _repository_with_connection(
        _Cursor(one=runtime_row)
    )
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease)
    inbox = _inbox(lease)
    monkeypatch.setattr(
        repository,
        "_lock_processing_email",
        AsyncMock(return_value=email),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=inbox),
    )
    monkeypatch.setattr(
        repository,
        "_require_authorized_processing_attempt",
        AsyncMock(),
    )

    assert (
        await repository.begin_processing_effect(
            lease,
            email.id,
            4,
            "detail",
        )
        is True
    )
    assert email.external_effects_started_at is None
    assert inbox.effect_started_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("effect_kind", [None, "unknown", "DETAIL"])
async def test_processing_effect_rejects_invalid_kind_before_database_access(
    normalized_event: NormalizedIngressEvent,
    effect_kind: object,
) -> None:
    lease = _lease(normalized_event)
    email_id = str(uuid4())
    repository = InboxRepository(object())

    with pytest.raises(ValueError, match="effect_kind"):
        await repository.begin_processing_effect(
            lease,
            email_id,
            4,
            effect_kind,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("write_failure", ["timestamp", "email", "inbox"])
async def test_effect_start_rejects_when_atomic_marker_write_is_lost(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    write_failure: str,
) -> None:
    lease = _lease(normalized_event)
    now = datetime.now(UTC)
    runtime_cursor = _Cursor(one={"session_id": lease.lease_session_id})
    cursors = [runtime_cursor, _Cursor(one=None)]
    if write_failure == "email":
        cursors = [
            runtime_cursor,
            _Cursor(one={"effect_started_at": now}),
            _Cursor(one=None),
        ]
    elif write_failure == "inbox":
        cursors = [
            runtime_cursor,
            _Cursor(one={"effect_started_at": now}),
            _Cursor(one=(str(uuid4()),)),
            _Cursor(one=None),
        ]
    repository, _connection = _repository_with_connection(*cursors)
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease)
    monkeypatch.setattr(
        repository,
        "_lock_processing_email",
        AsyncMock(return_value=email),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=_inbox(lease)),
    )
    monkeypatch.setattr(
        repository,
        "_require_authorized_processing_attempt",
        AsyncMock(),
    )

    if write_failure == "timestamp":
        with pytest.raises(DatabaseOperationError, match="timestamp is unavailable"):
            await repository.begin_processing_effect(lease, email.id, 4, "feishu")
    else:
        assert (
            await repository.begin_processing_effect(
                lease,
                email.id,
                4,
                "feishu",
            )
            is False
        )


@pytest.mark.asyncio
async def test_finish_rejects_nonexact_completion_before_database_access(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    repository = InboxRepository(object())

    with pytest.raises(ValueError, match="exact ProcessingCompletion"):
        await repository.finish_email_processing(
            lease,
            str(uuid4()),
            4,
            SimpleNamespace(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["email", "inbox"])
async def test_finish_fails_closed_when_locked_relation_disappears(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    lease = _lease(normalized_event)
    repository, _connection = _repository_with_connection()
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease)
    monkeypatch.setattr(
        repository,
        "_lock_processing_email",
        AsyncMock(return_value=None if missing == "email" else email),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=None),
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository.finish_email_processing(
            lease,
            email.id,
            4,
            ProcessingCompletion.no_action(),
        )


@pytest.mark.asyncio
async def test_finish_rejects_exhausted_version_before_any_write(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _lease(normalized_event)
    repository, _connection = _repository_with_connection()
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease, version=POSTGRES_BIGINT_MAX)
    monkeypatch.setattr(
        repository, "_lock_processing_email", AsyncMock(return_value=email)
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=_inbox(lease)),
    )
    monkeypatch.setattr(
        repository,
        "_load_processing_result_receipt",
        AsyncMock(return_value=False),
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository.finish_email_processing(
            lease,
            email.id,
            POSTGRES_BIGINT_MAX,
            ProcessingCompletion.no_action(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("write_failure", ["email", "inbox"])
async def test_finish_rejects_when_aggregate_write_is_lost(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    write_failure: str,
) -> None:
    lease = _lease(normalized_event)
    cursors = [_Cursor(one=None)]
    if write_failure == "inbox":
        cursors = [_Cursor(one=(str(uuid4()),)), _Cursor(one=None)]
    repository, _connection = _repository_with_connection(*cursors)
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease)
    monkeypatch.setattr(
        repository, "_lock_processing_email", AsyncMock(return_value=email)
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=_inbox(lease)),
    )
    monkeypatch.setattr(
        repository,
        "_load_processing_result_receipt",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        repository,
        "_require_authorized_processing_attempt",
        AsyncMock(),
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository.finish_email_processing(
            lease,
            email.id,
            4,
            ProcessingCompletion.no_action(),
        )


@pytest.mark.asyncio
async def test_failure_finish_rejects_invalid_and_nonexception_control_values(
    normalized_event: NormalizedIngressEvent,
) -> None:
    lease = _lease(normalized_event)
    repository = InboxRepository(object())

    with pytest.raises(ValueError, match="error must be an exception"):
        await repository.finish_email_processing_failure(
            lease,
            str(uuid4()),
            4,
            object(),  # type: ignore[arg-type]
        )
    control = _NonExceptionFailure("stop")
    with pytest.raises(_NonExceptionFailure) as caught:
        await repository.finish_email_processing_failure(
            lease,
            str(uuid4()),
            4,
            control,
        )
    assert caught.value is control


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["email", "inbox"])
async def test_failure_finish_fails_closed_when_locked_relation_disappears(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    lease = _lease(normalized_event)
    repository, _connection = _repository_with_connection()
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease)
    monkeypatch.setattr(
        repository,
        "_lock_processing_email",
        AsyncMock(return_value=None if missing == "email" else email),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=None),
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository.finish_email_processing_failure(
            lease,
            email.id,
            4,
            RuntimeError("dependency failed"),
        )


@pytest.mark.asyncio
async def test_failure_finish_rejects_exhausted_version_before_any_write(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _lease(normalized_event)
    repository, _connection = _repository_with_connection()
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease, version=POSTGRES_BIGINT_MAX)
    monkeypatch.setattr(
        repository, "_lock_processing_email", AsyncMock(return_value=email)
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=_inbox(lease)),
    )
    monkeypatch.setattr(
        repository,
        "_load_processing_result_receipt",
        AsyncMock(return_value=False),
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository.finish_email_processing_failure(
            lease,
            email.id,
            POSTGRES_BIGINT_MAX,
            RuntimeError("dependency failed"),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("write_failure", ["email", "inbox"])
async def test_failure_finish_rejects_when_aggregate_write_is_lost(
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
    write_failure: str,
) -> None:
    lease = _lease(normalized_event)
    cursors = [_Cursor(one=None)]
    if write_failure == "inbox":
        cursors = [_Cursor(one=(str(uuid4()),)), _Cursor(one=None)]
    repository, _connection = _repository_with_connection(*cursors)
    _patch_transaction_setup(repository, monkeypatch)
    email = _email(lease)
    monkeypatch.setattr(
        repository, "_lock_processing_email", AsyncMock(return_value=email)
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_ownership",
        AsyncMock(return_value=PipelineGenerationState.CURRENT_INGRESS),
    )
    monkeypatch.setattr(
        repository,
        "_lock_processing_inbox",
        AsyncMock(return_value=_inbox(lease)),
    )
    monkeypatch.setattr(
        repository,
        "_load_processing_result_receipt",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        repository,
        "_require_authorized_processing_attempt",
        AsyncMock(),
    )

    with pytest.raises(ProcessingCompletionRejected):
        await repository.finish_email_processing_failure(
            lease,
            email.id,
            4,
            RuntimeError("dependency failed"),
        )
