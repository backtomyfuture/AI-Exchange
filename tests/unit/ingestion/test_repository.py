from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg_pool import PoolTimeout

from src.domain.errors import DatabaseOperationError
from src.ingestion.models import InboxLease
from src.ingestion.repository import (
    InboxRepository,
    _failure_decision,
    _lease_from_row,
)


class _NeverPool:
    def connection(self):
        raise AssertionError("database access must not occur")


class _FailingConnectionContext:
    async def __aenter__(self):
        raise PoolTimeout("postgresql://secret-user:secret-password@db/internal")

    async def __aexit__(self, *_args):
        return False


class _FailingPool:
    def connection(self):
        return _FailingConnectionContext()


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, statement, _params=None):
        self.statements.append(str(statement))


class _ExplodingKindFailure(RuntimeError):
    @property
    def kind(self):
        raise RuntimeError("private kind getter content")


class _ControlFlowKindFailure(RuntimeError):
    @property
    def kind(self):
        raise KeyboardInterrupt("must propagate")


def _lease(normalized_event) -> InboxLease:
    now = datetime.now(UTC)
    return InboxLease(
        id=str(uuid4()),
        account_id=normalized_event.account_id,
        pipeline_name="durable_v1",
        generation=1,
        fencing_token=1,
        lease_owner="worker-1",
        attempts=0,
        event=normalized_event,
        received_at=now,
        lease_until=now + timedelta(minutes=1),
    )


def test_ingestion_boundary_exports_only_the_production_repository() -> None:
    import src.ingestion as ingestion

    assert ingestion.InboxRepository is InboxRepository
    public_methods = {
        name for name in InboxRepository.__dict__ if not name.startswith("_")
    }
    assert {
        "insert",
        "claim_batch",
        "renew",
        "begin_effect",
        "recover_expired_leases",
        "complete",
        "fail",
        "stats",
    }.issubset(public_methods)
    assert {
        "get",
        "status",
        "audit_count",
        "seed_lease",
        "expire_for_test",
        "seed_pending_policy",
    }.isdisjoint(public_methods)


def test_invalid_schema_text_is_rejected_without_database_access() -> None:
    with pytest.raises(ValueError, match="valid UTF-8"):
        InboxRepository(_NeverPool(), target_schema="\ud800")


@pytest.mark.asyncio
async def test_transaction_configuration_sets_read_committed_before_timeouts() -> None:
    connection = _RecordingConnection()

    await InboxRepository(_NeverPool())._configure_transaction(connection)

    assert connection.statements[0] == (
        "SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED"
    )
    assert "set_config('lock_timeout'" in connection.statements[1]


def test_kind_getter_failure_is_fixed_unknown_without_leaking_exception() -> None:
    decision = _failure_decision(_ExplodingKindFailure("private outer content"))

    assert decision.status.value == "dead_letter"
    assert decision.safe_code == "inbox.internal_invariant"
    assert decision.safe_summary == "Inbox processing invariant failed"


def test_kind_getter_does_not_swallow_base_exception_control_flow() -> None:
    with pytest.raises(KeyboardInterrupt, match="must propagate"):
        _failure_decision(_ControlFlowKindFailure("private outer content"))


@pytest.mark.asyncio
async def test_insert_inputs_are_validated_before_database_access(
    normalized_event,
) -> None:
    repository = InboxRepository(_NeverPool())

    invalid_calls = (
        lambda: repository.insert(object(), 1, 1),
        lambda: repository.insert(normalized_event, True, 1),
        lambda: repository.insert(normalized_event, 0, 1),
        lambda: repository.insert(normalized_event, 2**63, 1),
        lambda: repository.insert(normalized_event, 1, False),
        lambda: repository.insert(normalized_event, 1, 0),
        lambda: repository.insert(normalized_event, 1, 2**63),
    )

    for call in invalid_calls:
        with pytest.raises(ValueError):
            await call()


@pytest.mark.asyncio
async def test_claim_inputs_are_bounded_before_database_access() -> None:
    repository = InboxRepository(_NeverPool())
    invalid_calls = (
        lambda: repository.claim_batch("", {"durable_v1"}, 1, 60),
        lambda: repository.claim_batch(" worker", {"durable_v1"}, 1, 60),
        lambda: repository.claim_batch("x" * 129, {"durable_v1"}, 1, 60),
        lambda: repository.claim_batch("worker", set(), 1, 60),
        lambda: repository.claim_batch("worker", {""}, 1, 60),
        lambda: repository.claim_batch("worker", {"x" * 65}, 1, 60),
        lambda: repository.claim_batch(
            "worker", {f"pipeline-{index}" for index in range(65)}, 1, 60
        ),
        lambda: repository.claim_batch("worker", {"durable_v1"}, 0, 60),
        lambda: repository.claim_batch("worker", {"durable_v1"}, 501, 60),
        lambda: repository.claim_batch("worker", {"durable_v1"}, 1, 0),
        lambda: repository.claim_batch("worker", {"durable_v1"}, 1, 3601),
    )

    for call in invalid_calls:
        with pytest.raises(ValueError):
            await call()


@pytest.mark.asyncio
async def test_lease_and_reaper_inputs_are_bounded_before_database_access(
    normalized_event,
) -> None:
    repository = InboxRepository(_NeverPool())
    lease = _lease(normalized_event)

    invalid_calls = (
        lambda: repository.renew(object(), 60),
        lambda: repository.renew(lease, 0),
        lambda: repository.renew(lease, 3601),
        lambda: repository.begin_effect(object()),
        lambda: repository.complete(object()),
        lambda: repository.fail(object(), RuntimeError("safe")),
        lambda: repository.recover_expired_leases(0),
        lambda: repository.recover_expired_leases(501),
    )

    for call in invalid_calls:
        with pytest.raises(ValueError):
            await call()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "args", "operation"),
    [
        ("insert", (None, 1, 1), "insert_event_inbox"),
        (
            "claim_batch",
            ("worker", frozenset({"durable_v1"}), 1, 60),
            "claim_event_inbox",
        ),
        ("recover_expired_leases", (1,), "recover_event_inbox_leases"),
        ("stats", (), "read_event_inbox_stats"),
    ],
)
async def test_database_failures_are_privacy_safe_and_retryable(
    method: str,
    args: tuple[object, ...],
    operation: str,
    normalized_event,
) -> None:
    repository = InboxRepository(_FailingPool())
    if method == "insert":
        args = (normalized_event, 1, 1)

    with pytest.raises(DatabaseOperationError) as caught:
        await getattr(repository, method)(*args)

    assert caught.value.operation == operation
    assert caught.value.retryable is True
    assert "secret" not in str(caught.value)
    assert caught.value.__cause__ is None


def test_tuple_and_dict_rows_decode_to_the_same_lease(normalized_event) -> None:
    now = datetime.now(UTC)
    columns = (
        "id",
        "account_id",
        "external_email_id",
        "folder_key",
        "source",
        "raw_event_type",
        "change_kind",
        "dedupe_key",
        "source_version",
        "source_event_at",
        "payload",
        "processing_policy",
        "pipeline_name",
        "generation",
        "fencing_token",
        "lease_owner",
        "lease_until",
        "attempts",
        "received_at",
    )
    values = (
        str(uuid4()),
        normalized_event.account_id,
        normalized_event.external_email_id,
        normalized_event.folder,
        normalized_event.source.value,
        normalized_event.raw_event_type,
        normalized_event.kind.value,
        normalized_event.dedupe_key,
        normalized_event.source_version,
        normalized_event.source_event_at,
        normalized_event.payload_for_storage(),
        normalized_event.processing_policy.value,
        "durable_v1",
        1,
        1,
        "worker",
        now + timedelta(seconds=60),
        0,
        now,
    )

    assert _lease_from_row(values) == _lease_from_row(dict(zip(columns, values)))


def test_invalid_database_row_is_a_fixed_nonretryable_error() -> None:
    with pytest.raises(DatabaseOperationError) as caught:
        _lease_from_row(("invalid",))

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox database row is invalid"
