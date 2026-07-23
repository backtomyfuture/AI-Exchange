from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg.pq import TransactionStatus
from psycopg_pool import PoolTimeout

import src.ingestion.ownership as ownership_module
from src.domain.errors import DatabaseOperationError, ErrorKind, StaleFence
from src.domain.email_state import PipelineGenerationState
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    IngressSource,
    NormalizedIngressEvent,
    PipelineGeneration,
    ProcessingPolicy,
)
from src.ingestion.ownership import (
    PipelineOwnershipRepository,
    PipelineOwnershipTransaction,
    PipelineRetirementBlocked,
    RetirementBlockCode,
    _generation_from_row,
    ownership_advisory_lock_key,
)


class _NeverPool:
    def connection(self):
        raise AssertionError("database access must not occur")


class _FailingConnectionContext:
    async def __aenter__(self):
        raise PoolTimeout("database-secret-must-not-escape")

    async def __aexit__(self, *_args):
        return False


class _FailingPool:
    def connection(self):
        return _FailingConnectionContext()


class _FailingTransactionConnection:
    def __init__(self) -> None:
        self.info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)

    async def execute(self, *_args, **_kwargs):
        raise PoolTimeout("database-secret-must-not-escape")


def _lease() -> InboxLease:
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
        processing_policy=ProcessingPolicy.FULL,
        source_event_at=now,
    )
    return InboxLease(
        id=str(uuid4()),
        account_id=8,
        pipeline_name="legacy_compat",
        generation=1,
        fencing_token=1,
        execution_epoch=0,
        authority_epoch=1,
        capability_hash="a" * 64,
        lease_session_id="00000000-0000-4000-8000-000000000002",
        lease_owner="worker-1",
        attempts=1,
        event=event,
        received_at=now,
        lease_until=now + timedelta(minutes=5),
    )


def test_ownership_advisory_lock_key_is_stable() -> None:
    assert ownership_advisory_lock_key(8) == -2_138_553_817_419_182_844
    assert ownership_advisory_lock_key(8) == ownership_advisory_lock_key(8)


@pytest.mark.parametrize("account_id", [1, 8, 2**63 - 1])
def test_ownership_advisory_lock_key_fits_postgres_bigint(account_id: int) -> None:
    lock_key = ownership_advisory_lock_key(account_id)

    assert -(2**63) <= lock_key <= 2**63 - 1


@pytest.mark.parametrize("account_id", [0, -1, True, False, 1.0, "8", None, 2**63])
def test_ownership_advisory_lock_key_rejects_invalid_account_ids(
    account_id: object,
) -> None:
    with pytest.raises(ValueError, match="positive PostgreSQL BIGINT"):
        ownership_advisory_lock_key(account_id)  # type: ignore[arg-type]


def test_stale_fence_is_a_fixed_safe_internal_invariant() -> None:
    error = StaleFence()

    assert error.kind is ErrorKind.INTERNAL_INVARIANT
    assert error.safe_code == "pipeline.stale_fence"
    assert error.safe_summary == "Pipeline fence is stale"
    assert str(error) == error.safe_summary
    assert repr(error) == "StaleFence(safe_code='pipeline.stale_fence')"
    assert error.__cause__ is None


def test_retirement_block_is_fixed_and_type_checked() -> None:
    error = PipelineRetirementBlocked(RetirementBlockCode.EVIDENCE_UNAVAILABLE)

    assert error.safe_code is RetirementBlockCode.EVIDENCE_UNAVAILABLE
    assert error.safe_summary == "Pipeline retirement evidence is unavailable"
    assert str(error) == error.safe_summary
    assert repr(error) == (
        "PipelineRetirementBlocked("
        "safe_code='pipeline.retirement_evidence_unavailable')"
    )
    with pytest.raises(TypeError, match="RetirementBlockCode"):
        PipelineRetirementBlocked("pipeline.retirement_evidence_unavailable")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "row",
    [
        None,
        (8, 1),
        (8, 1, "legacy_compat", "not-a-state", 1),
    ],
)
def test_invalid_database_rows_fail_with_a_fixed_error(row: object) -> None:
    with pytest.raises(DatabaseOperationError) as caught:
        _generation_from_row(row)

    assert caught.value.operation == "read_pipeline_ownership"
    assert caught.value.retryable is False
    assert str(caught.value) == "pipeline ownership row is invalid"


def test_invalid_schema_text_is_rejected_without_database_access() -> None:
    with pytest.raises(ValueError, match="valid UTF-8"):
        PipelineOwnershipRepository(_NeverPool(), target_schema="\ud800")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("bootstrap", (True, "legacy_compat")),
        ("bootstrap", (0, "legacy_compat")),
        ("bootstrap", (2**63, "legacy_compat")),
        ("bootstrap", (8, " legacy_compat")),
        ("bootstrap", (8, "x" * 65)),
        ("get", (8, False)),
        ("get", (8, 0)),
        ("assert_fence", (8, 1, 0)),
        ("assert_fence", (8, 1, 2**63)),
        ("quiesce", (8, 1, 1, " bad", "safe reason")),
        ("quiesce", (8, 1, 1, "actor", "bad\nreason")),
    ],
)
async def test_public_inputs_are_rejected_before_database_access(
    method_name: str,
    args: tuple[object, ...],
) -> None:
    ownership = PipelineOwnershipRepository(_NeverPool())

    with pytest.raises(ValueError):
        await getattr(ownership, method_name)(*args)


def test_phase2_exposes_no_public_generation_switch() -> None:
    public_repository_methods = {
        name
        for name in PipelineOwnershipRepository.__dict__
        if not name.startswith("_")
    }
    transaction_methods = set(PipelineOwnershipTransaction.__dict__)

    assert {"switch", "promote", "create_next"}.isdisjoint(public_repository_methods)
    assert "transaction" in public_repository_methods
    assert {"_lock_quiesced", "_mark_draining", "_insert_current"}.issubset(
        transaction_methods
    )
    assert not {
        "lock_quiesced",
        "mark_draining",
        "insert_current",
    }.intersection(transaction_methods)


@pytest.mark.asyncio
async def test_can_execute_returns_false_only_for_a_stale_fence(monkeypatch) -> None:
    ownership = PipelineOwnershipRepository(_NeverPool())
    lease = _lease()

    async def stale(*_args, **_kwargs):
        raise StaleFence()

    monkeypatch.setattr(ownership, "assert_fence", stale)
    assert await ownership.can_execute(lease) is False

    outage = DatabaseOperationError(
        operation="assert_pipeline_fence",
        retryable=True,
        message="pipeline ownership read failed",
    )

    async def unavailable(*_args, **_kwargs):
        raise outage

    monkeypatch.setattr(ownership, "assert_fence", unavailable)
    with pytest.raises(DatabaseOperationError) as caught:
        await ownership.can_execute(lease)

    assert caught.value is outage


@pytest.mark.asyncio
async def test_can_execute_rejects_an_untyped_lease() -> None:
    ownership = PipelineOwnershipRepository(_NeverPool())

    with pytest.raises(ValueError, match="InboxLease"):
        await ownership.can_execute(object())  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "args", "operation"),
    [
        (
            "bootstrap",
            (8, "legacy_compat"),
            "bootstrap_pipeline_ownership",
        ),
        ("get", (8, 1), "get_pipeline_ownership"),
        ("current_ingress", (8,), "get_current_pipeline_ownership"),
        ("next_generation", (8,), "next_pipeline_generation"),
        ("assert_fence", (8, 1, 1), "assert_pipeline_fence"),
        (
            "quiesce",
            (8, 1, 1, "operator", "prepare cutover"),
            "quiesce_pipeline_ownership",
        ),
        (
            "retire",
            (8, 1, 1, "operator", "retire generation"),
            "retire_pipeline_ownership",
        ),
    ],
)
async def test_public_database_failures_are_fixed_and_retryable(
    method_name: str,
    args: tuple[object, ...],
    operation: str,
) -> None:
    ownership = PipelineOwnershipRepository(_FailingPool())

    with pytest.raises(DatabaseOperationError) as caught:
        await getattr(ownership, method_name)(*args)

    assert caught.value.operation == operation
    assert caught.value.retryable is True
    assert str(caught.value) == "pipeline ownership database operation failed"
    assert "database-secret" not in str(caught.value)


def _generation(state: PipelineGenerationState) -> PipelineGeneration:
    return PipelineGeneration(
        account_id=8,
        generation=1,
        pipeline_name="legacy_compat",
        state=state,
        fencing_token=1,
    )


@pytest.mark.asyncio
async def test_transaction_lock_database_failure_is_safely_wrapped(monkeypatch) -> None:
    connection = _FailingTransactionConnection()
    ownership = PipelineOwnershipRepository(_NeverPool())
    transaction = ownership.transaction(connection)  # type: ignore[arg-type]

    async def fail_configuration(_connection) -> None:
        raise PoolTimeout("database-secret-must-not-escape")

    monkeypatch.setattr(ownership, "_configure_transaction", fail_configuration)
    with pytest.raises(DatabaseOperationError) as caught:
        await transaction._lock_quiesced(8, 1, 1)

    assert caught.value.operation == "lock_pipeline_handoff"
    assert caught.value.retryable is True
    assert "database-secret" not in str(caught.value)


@pytest.mark.asyncio
async def test_transaction_mark_database_failure_is_safely_wrapped() -> None:
    connection = _FailingTransactionConnection()
    ownership = PipelineOwnershipRepository(_NeverPool())
    transaction = ownership.transaction(connection)  # type: ignore[arg-type]
    transaction._locked = _generation(PipelineGenerationState.QUIESCING)

    with pytest.raises(DatabaseOperationError) as caught:
        await transaction._mark_draining(
            transaction._locked,
            actor="operator",
            reason="handoff",
        )

    assert caught.value.operation == "mark_pipeline_draining"
    assert caught.value.retryable is True
    assert "database-secret" not in str(caught.value)


@pytest.mark.asyncio
async def test_transaction_insert_database_failure_is_safely_wrapped() -> None:
    connection = _FailingTransactionConnection()
    ownership = PipelineOwnershipRepository(_NeverPool())
    transaction = ownership.transaction(connection)  # type: ignore[arg-type]
    transaction._draining = _generation(PipelineGenerationState.DRAINING)

    with pytest.raises(DatabaseOperationError) as caught:
        await transaction._insert_current(
            account_id=8,
            pipeline_name="durable_v1",
            generation=2,
            fencing_token=2,
            actor="operator",
            reason="handoff",
        )

    assert caught.value.operation == "insert_current_pipeline"
    assert caught.value.retryable is True
    assert "database-secret" not in str(caught.value)


class _OwnershipCursor:
    def __init__(self, row: object) -> None:
        self._row = row

    async def fetchone(self) -> object:
        return self._row


class _RecordingOwnershipConnection:
    def __init__(self, *rows: object) -> None:
        self._rows = list(rows)
        self.statements: list[tuple[object, object]] = []

    async def execute(
        self,
        statement: object,
        params: object = None,
    ) -> _OwnershipCursor:
        self.statements.append((statement, params))
        row = self._rows.pop(0) if self._rows else None
        return _OwnershipCursor(row)


@pytest.mark.asyncio
async def test_ownership_sql_helpers_use_bounded_settings_and_exact_fence_params() -> (
    None
):
    ownership = PipelineOwnershipRepository(_NeverPool())
    configuration = _RecordingOwnershipConnection()

    await ownership._configure_transaction(configuration)  # type: ignore[arg-type]
    await ownership._acquire_account_lock(configuration, 8)  # type: ignore[arg-type]

    assert len(configuration.statements) == 2
    assert configuration.statements[0][1] == ("5000ms", "15000ms", "15000ms")
    assert configuration.statements[1][1] == (ownership_advisory_lock_key(8),)

    row = {
        "account_id": 8,
        "generation": 3,
        "pipeline_name": "pipeline-v2",
        "state": "current_ingress",
        "fencing_token": 9,
    }
    fenced = _RecordingOwnershipConnection(row)
    generation = await ownership._fetch_exact(  # type: ignore[arg-type]
        fenced,
        account_id=8,
        generation=3,
        fencing_token=9,
        for_update=True,
    )

    assert generation == PipelineGeneration(
        account_id=8,
        generation=3,
        pipeline_name="pipeline-v2",
        state=PipelineGenerationState.CURRENT_INGRESS,
        fencing_token=9,
    )
    assert fenced.statements[0][1] == (8, 3, 9)

    missing = _RecordingOwnershipConnection(None)
    assert (
        await ownership._fetch_exact(  # type: ignore[arg-type]
            missing,
            account_id=8,
            generation=4,
        )
        is None
    )
    assert missing.statements[0][1] == (8, 4)


@pytest.mark.asyncio
async def test_ownership_audit_hashes_safe_fields_and_default_guard_denies_retirement() -> (
    None
):
    ownership = PipelineOwnershipRepository(_NeverPool())
    generation = _generation(PipelineGenerationState.QUIESCING)
    connection = _RecordingOwnershipConnection()

    await ownership._audit(  # type: ignore[arg-type]
        connection,
        generation,
        action="pipeline.quiesce",
        actor="operator",
        reason="prepare cutover",
    )

    assert len(connection.statements) == 1
    audit_params = connection.statements[0][1]
    assert len(audit_params) == 8
    assert audit_params[2] == 8
    assert audit_params[4] == "pipeline.quiesce"
    assert audit_params[5:7] == ("operator", "prepare cutover")
    assert len(audit_params[1]) == 64
    assert len(audit_params[3]) == 64

    with pytest.raises(PipelineRetirementBlocked) as caught:
        await ownership._retirement_guard.assert_ready(  # type: ignore[arg-type]
            connection,
            generation,
        )
    assert caught.value.safe_code is RetirementBlockCode.EVIDENCE_UNAVAILABLE


def test_ownership_row_value_accepts_mapping_or_tuple_and_rejects_missing_data() -> (
    None
):
    row_value = getattr(ownership_module, "_row_value")

    assert row_value({"maximum_generation": 3}, 0, "maximum_generation") == 3
    assert row_value((4,), 0, "maximum_generation") == 4
    with pytest.raises(ValueError, match="missing"):
        row_value({}, 0, "maximum_generation")
    with pytest.raises(ValueError, match="invalid shape"):
        row_value((), 0, "maximum_generation")
