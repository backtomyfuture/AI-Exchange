from __future__ import annotations

import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg.pq import TransactionStatus
from psycopg_pool import PoolTimeout

from src.domain.errors import DatabaseOperationError
from src.ingestion.models import (
    ChangeKind,
    InboxLease,
    IngressReceipt,
    NormalizedIngressEvent,
)
from src.ingestion.repository import (
    EmailEventTransaction,
    InboxRepository,
    _email_from_row,
    _failure_decision,
    _json_values_equal,
    _lease_from_row,
    _require_pipeline_names,
    _row_values,
    _source_is_read,
)


_LEASE_SESSION_ID = "00000000-0000-4000-8000-000000000002"


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


class _AsyncContext:
    def __init__(self, value) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class _DelegatingConnection(_RecordingConnection):
    def transaction(self):
        return _AsyncContext(self)


class _DelegatingPool:
    def __init__(self, connection: _DelegatingConnection) -> None:
        self.connection_value = connection
        self.checkouts = 0

    def connection(self):
        self.checkouts += 1
        return _AsyncContext(self.connection_value)


class _StaticRowCursor:
    def __init__(self, row=None) -> None:
        self._row = row

    async def fetchone(self):
        return self._row


class _QuarantineReleaseConnection(_DelegatingConnection):
    def __init__(self, external_email_id: str) -> None:
        super().__init__()
        self.external_email_id = external_email_id
        self.executions: list[tuple[str, object]] = []

    async def execute(self, statement, params=None):
        rendered = (
            statement.as_string() if hasattr(statement, "as_string") else str(statement)
        )
        self.statements.append(rendered)
        self.executions.append((rendered, params))
        if "SELECT i.external_email_id" in rendered:
            return _StaticRowCursor((self.external_email_id,))
        if 'UPDATE "public"."emails"' in rendered:
            return _StaticRowCursor((str(uuid4()),))
        if 'UPDATE "public"."event_inbox"' in rendered:
            return _StaticRowCursor((1,))
        return _StaticRowCursor()


class _InsertSpy:
    def __init__(self, receipt: IngressReceipt) -> None:
        self.receipt = receipt
        self.calls: list[tuple[object, int, int]] = []

    async def insert(self, event, generation: int, fencing_token: int):
        self.calls.append((event, generation, fencing_token))
        return self.receipt


class _IdentityCursor:
    async def fetchone(self):
        return ("123", "read committed")


class _CallerConnection:
    def __init__(self) -> None:
        self.info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)

    async def execute(self, statement, _params=None):
        assert "pg_current_xact_id" in str(statement)
        return _IdentityCursor()


class _PayloadProbeConnection(_DelegatingConnection):
    def __init__(self) -> None:
        super().__init__()
        self.info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)

    async def execute(self, statement, _params=None):
        self.statements.append(str(statement))
        return _IdentityCursor()


class _OwnershipModeCursor:
    def __init__(self, row) -> None:
        self._row = row

    async def fetchone(self):
        return self._row


class _OwnershipModeConnection:
    def __init__(self) -> None:
        self.info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)
        self.events: list[str] = []
        self.statements: list[str] = []

    async def execute(self, statement, params=None):
        rendered = (
            statement.as_string() if hasattr(statement, "as_string") else str(statement)
        )
        self.statements.append(rendered)
        if "pg_current_xact_id" in rendered:
            self.events.append("xid.identity")
            return _OwnershipModeCursor(("123", "read committed"))
        if "pg_advisory_xact_lock_shared" in rendered:
            self.events.append("ownership.shared_lock")
            return _OwnershipModeCursor(None)
        if 'FROM "public"."pipeline_ownership"' in rendered:
            self.events.append("ownership.read")
            return _OwnershipModeCursor(("durable_v1",))
        if 'INSERT INTO "public"."event_inbox"' in rendered:
            self.events.append("inbox.insert")
            assert type(params) is tuple
            return _OwnershipModeCursor((params[0],))
        raise AssertionError(f"unexpected SQL: {rendered}")


class _BarrierOwnershipModeConnection(_OwnershipModeConnection):
    def __init__(self) -> None:
        super().__init__()
        self.identity_entered = asyncio.Event()
        self.identity_release = asyncio.Event()

    async def execute(self, statement, params=None):
        rendered = (
            statement.as_string() if hasattr(statement, "as_string") else str(statement)
        )
        if "pg_current_xact_id" in rendered:
            self.statements.append(rendered)
            self.events.append("xid.identity")
            self.identity_entered.set()
            await self.identity_release.wait()
            return _OwnershipModeCursor(("123", "read committed"))
        return await super().execute(statement, params)


class _HostileIngressEvent(NormalizedIngressEvent):
    def payload_for_storage(self):
        raise AssertionError("hostile event method must not execute")


class _ExplodingKindFailure(RuntimeError):
    @property
    def kind(self):
        raise RuntimeError("private kind getter content")


class _ControlFlowKindFailure(RuntimeError):
    @property
    def kind(self):
        raise KeyboardInterrupt("must propagate")


class _HostileBool:
    def __init__(self) -> None:
        self.truthiness_calls = 0

    def __bool__(self) -> bool:
        self.truthiness_calls += 1
        raise AssertionError("hostile truthiness must not execute")


def _lease(normalized_event) -> InboxLease:
    now = datetime.now(UTC)
    return InboxLease(
        id=str(uuid4()),
        account_id=normalized_event.account_id,
        pipeline_name="durable_v1",
        generation=1,
        fencing_token=1,
        execution_epoch=0,
        authority_epoch=1,
        capability_hash="a" * 64,
        lease_session_id="00000000-0000-4000-8000-000000000002",
        lease_owner="worker-1",
        attempts=0,
        event=normalized_event,
        received_at=now,
        lease_until=now + timedelta(minutes=1),
    )


def _lease_database_row(
    normalized_event: NormalizedIngressEvent,
    **overrides: object,
) -> dict[str, object]:
    now = datetime.now(UTC)
    values: dict[str, object] = {
        "id": str(uuid4()),
        "account_id": normalized_event.account_id,
        "external_email_id": normalized_event.external_email_id,
        "folder_key": normalized_event.folder,
        "source": normalized_event.source.value,
        "raw_event_type": normalized_event.raw_event_type,
        "change_kind": normalized_event.kind.value,
        "dedupe_key": normalized_event.dedupe_key,
        "source_version": normalized_event.source_version,
        "source_event_at": normalized_event.source_event_at,
        "payload": normalized_event.payload_for_storage(),
        "processing_policy": normalized_event.processing_policy.value,
        "pipeline_name": "durable_v1",
        "generation": 1,
        "fencing_token": 1,
        "execution_epoch": 0,
        "authority_epoch": 1,
        "capability_hash": "a" * 64,
        "lease_session_id": "00000000-0000-4000-8000-000000000002",
        "lease_owner": "worker",
        "lease_until": now + timedelta(seconds=60),
        "attempts": 0,
        "received_at": now,
    }
    values.update(overrides)
    return values


def _email_database_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "id": str(uuid4()),
        "account_id": 8,
        "external_email_id": "message-1",
        "source_folder_key": "INBOX",
        "status": "ingested",
        "version": 0,
        "owner_generation": 1,
        "owner_fencing_token": 1,
        "owner_authority_epoch": 1,
        "owner_capability_hash": "a" * 64,
        "processing_inbox_id": None,
        "processing_execution_epoch": None,
        "create_seen_at": datetime.now(UTC),
        "processing_started_at": None,
        "source_deleted_at": None,
        "external_effects_started_at": None,
        "safe_error_code": None,
        "safe_error_summary": None,
        "is_read": None,
        "is_read_refresh_required": False,
        "updated_at": datetime.now(UTC),
    }
    values.update(overrides)
    return values


def test_invalid_schema_text_is_rejected_without_database_access() -> None:
    with pytest.raises(ValueError, match="valid UTF-8"):
        InboxRepository(_NeverPool(), target_schema="\ud800")


@pytest.mark.asyncio
async def test_transaction_ownership_mode_keeps_default_lock_and_plain_shared_xid(
    normalized_event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = InboxRepository(_NeverPool())

    async def no_duplicate(_connection, _event):
        return None

    monkeypatch.setattr(repository, "_duplicate_receipt", no_duplicate)
    default_connection = _OwnershipModeConnection()
    plain_connection = _OwnershipModeConnection()

    await repository.transaction(default_connection).insert(normalized_event, 1, 1)
    await repository.transaction(
        plain_connection,
        for_key_share=False,
    ).insert(normalized_event, 1, 1)

    default_ownership_sql = next(
        statement
        for statement in default_connection.statements
        if "pipeline_ownership" in statement
    )
    plain_ownership_sql = next(
        statement
        for statement in plain_connection.statements
        if "pipeline_ownership" in statement
    )
    assert default_ownership_sql.endswith("FOR KEY SHARE")
    assert "FOR KEY SHARE" not in plain_ownership_sql
    assert (
        default_connection.events
        == plain_connection.events
        == [
            "xid.identity",
            "ownership.shared_lock",
            "ownership.read",
            "inbox.insert",
        ]
    )


@pytest.mark.parametrize("invalid", [None, 0, 1, "false", object()])
def test_transaction_rejects_non_exact_ownership_mode_without_query(
    invalid: object,
) -> None:
    repository = InboxRepository(_NeverPool())
    connection = _OwnershipModeConnection()

    with pytest.raises(ValueError, match="for_key_share"):
        repository.transaction(connection, for_key_share=invalid)

    assert connection.statements == []
    assert connection.events == []


def test_transaction_ownership_mode_is_keyword_only_and_defaults_locked() -> None:
    parameter = inspect.signature(InboxRepository.transaction).parameters[
        "for_key_share"
    ]

    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is True


@pytest.mark.parametrize(
    "invalid",
    [None, 0, 1, "false", object(), _HostileBool()],
)
def test_direct_transaction_constructor_rejects_non_exact_mode_without_query(
    invalid: object,
) -> None:
    repository = InboxRepository(_NeverPool())
    connection = _OwnershipModeConnection()

    with pytest.raises(ValueError, match="for_key_share"):
        EmailEventTransaction(
            repository,
            connection,
            for_key_share=invalid,
        )

    assert connection.statements == []
    assert connection.events == []


@pytest.mark.parametrize("mode", [False, True])
def test_direct_transaction_constructor_accepts_only_exact_boolean_modes(
    mode: bool,
) -> None:
    transaction = EmailEventTransaction(
        InboxRepository(_NeverPool()),
        _OwnershipModeConnection(),
        for_key_share=mode,
    )

    assert transaction._for_key_share is mode


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [None, 0, 1, "false", object(), _HostileBool()],
)
async def test_transaction_insert_revalidates_mutated_mode_before_payload_or_query(
    invalid: object,
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = InboxRepository(_NeverPool())
    connection = _OwnershipModeConnection()
    transaction = EmailEventTransaction(
        repository,
        connection,
        for_key_share=True,
    )
    transaction._for_key_share = invalid  # type: ignore[assignment]
    original = NormalizedIngressEvent.payload_for_storage
    payload_calls = 0

    def count_payload(event: NormalizedIngressEvent):
        nonlocal payload_calls
        payload_calls += 1
        return original(event)

    monkeypatch.setattr(NormalizedIngressEvent, "payload_for_storage", count_payload)

    with pytest.raises(ValueError, match="for_key_share"):
        await transaction.insert(normalized_event, 1, 1)

    assert payload_calls == 0
    assert connection.statements == []
    assert connection.events == []
    if isinstance(invalid, _HostileBool):
        assert invalid.truthiness_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [False, True])
async def test_transaction_insert_uses_one_pre_await_lock_mode_snapshot(
    mode: bool,
    normalized_event: NormalizedIngressEvent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = InboxRepository(_NeverPool())
    connection = _BarrierOwnershipModeConnection()

    async def no_duplicate(_connection, _event):
        return None

    monkeypatch.setattr(repository, "_duplicate_receipt", no_duplicate)
    transaction = EmailEventTransaction(
        repository,
        connection,
        for_key_share=mode,
    )

    pending = asyncio.create_task(transaction.insert(normalized_event, 1, 1))
    await connection.identity_entered.wait()
    transaction._for_key_share = not mode
    connection.identity_release.set()
    receipt = await pending

    ownership_sql = next(
        statement
        for statement in connection.statements
        if "pipeline_ownership" in statement
    )
    assert ownership_sql.endswith("FOR KEY SHARE") is mode
    assert receipt.duplicate is False
    assert transaction._for_key_share is not mode


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


def _hostile_event(event: NormalizedIngressEvent) -> _HostileIngressEvent:
    return _HostileIngressEvent(
        account_id=event.account_id,
        source=event.source,
        raw_event_type=event.raw_event_type,
        kind=event.kind,
        external_email_id=event.external_email_id,
        folder=event.folder,
        source_version=event.source_version,
        dedupe_key=event.dedupe_key,
        payload=event.payload,
        processing_policy=event.processing_policy,
        source_event_at=event.source_event_at,
    )


@pytest.mark.asyncio
async def test_insert_rejects_hostile_event_subclass_before_pool_or_xid(
    normalized_event,
) -> None:
    repository = InboxRepository(_NeverPool())
    hostile = _hostile_event(normalized_event)

    with pytest.raises(ValueError, match="exact NormalizedIngressEvent"):
        await repository.insert(hostile, 1, 1)
    with pytest.raises(ValueError, match="exact NormalizedIngressEvent"):
        await repository.transaction(_CallerConnection()).insert(hostile, 1, 1)


@pytest.mark.asyncio
async def test_pool_owned_insert_materializes_payload_once(
    normalized_event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _PayloadProbeConnection()
    repository = InboxRepository(_DelegatingPool(connection))
    original = NormalizedIngressEvent.payload_for_storage
    calls = 0

    def count_payload(event: NormalizedIngressEvent):
        nonlocal calls
        calls += 1
        return original(event)

    async def cancel_after_identity(_connection, _account_id: int) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(NormalizedIngressEvent, "payload_for_storage", count_payload)
    monkeypatch.setattr(repository, "_acquire_account_lock", cancel_after_identity)

    with pytest.raises(asyncio.CancelledError):
        await repository.insert(normalized_event, 1, 1)

    assert calls == 1


@pytest.mark.asyncio
async def test_transaction_insert_propagates_cancellation_without_pool_checkout(
    normalized_event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = InboxRepository(_NeverPool())

    async def cancel_at_account_lock(_connection, _account_id: int) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(repository, "_acquire_account_lock", cancel_at_account_lock)

    with pytest.raises(asyncio.CancelledError):
        await repository.transaction(_CallerConnection()).insert(
            normalized_event,
            generation=1,
            fencing_token=1,
        )


@pytest.mark.asyncio
async def test_pool_owned_insert_only_delegates_after_configuring_transaction(
    normalized_event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _DelegatingConnection()
    pool = _DelegatingPool(connection)
    repository = InboxRepository(pool)
    expected = IngressReceipt(inbox_id=str(uuid4()), duplicate=False)
    transaction = _InsertSpy(expected)
    monkeypatch.setattr(repository, "transaction", lambda value: transaction)

    assert await repository.insert(normalized_event, 1, 1) == expected
    assert pool.checkouts == 1
    assert transaction.calls == [(normalized_event, 1, 1)]
    assert connection.statements == [
        "SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SELECT pg_catalog.set_config('lock_timeout', %s, true), "
        "pg_catalog.set_config('statement_timeout', %s, true), "
        "pg_catalog.set_config('idle_in_transaction_session_timeout', %s, true)",
    ]


@pytest.mark.asyncio
async def test_initial_quarantine_release_advances_epoch_zero_without_replacing_owner():
    inbox_id = "00000000-0000-4000-8000-000000000123"
    connection = _QuarantineReleaseConnection("external-mail-id")
    repository = InboxRepository(_DelegatingPool(connection))

    released_epoch = await repository.release_intake_quarantine(
        inbox_id=inbox_id,
        expected_execution_epoch=0,
        actor="operator",
        reason="reviewed safe for processing",
    )

    assert released_epoch == 1
    email_update = next(
        statement
        for statement, _params in connection.executions
        if 'UPDATE "public"."emails"' in statement
    )
    inbox_update = next(
        statement
        for statement, _params in connection.executions
        if 'UPDATE "public"."event_inbox"' in statement
    )
    assert "processing_execution_epoch=%s" in email_update
    assert "owner_generation" not in email_update
    assert "owner_fencing_token" not in email_update
    assert "execution_epoch=%s, attempts=0" in inbox_update


@pytest.mark.asyncio
async def test_claim_inputs_are_bounded_before_database_access() -> None:
    repository = InboxRepository(_NeverPool())
    invalid_calls = (
        lambda: repository.claim_batch("", _LEASE_SESSION_ID, {"durable_v1"}, 1, 60),
        lambda: repository.claim_batch(
            " worker", _LEASE_SESSION_ID, {"durable_v1"}, 1, 60
        ),
        lambda: repository.claim_batch(
            "x" * 129, _LEASE_SESSION_ID, {"durable_v1"}, 1, 60
        ),
        lambda: repository.claim_batch("worker", "", {"durable_v1"}, 1, 60),
        lambda: repository.claim_batch(
            "worker",
            "00000000-0000-0000-8000-000000000002",
            {"durable_v1"},
            1,
            60,
        ),
        lambda: repository.claim_batch(
            "worker",
            "00000000-0000-4000-8000-00000000000A",
            {"durable_v1"},
            1,
            60,
        ),
        lambda: repository.claim_batch("worker", _LEASE_SESSION_ID, set(), 1, 60),
        lambda: repository.claim_batch("worker", _LEASE_SESSION_ID, {""}, 1, 60),
        lambda: repository.claim_batch(
            "worker", _LEASE_SESSION_ID, {"x" * 65}, 1, 60
        ),
        lambda: repository.claim_batch(
            "worker",
            _LEASE_SESSION_ID,
            {f"pipeline-{index}" for index in range(65)},
            1,
            60,
        ),
        lambda: repository.claim_batch(
            "worker", _LEASE_SESSION_ID, {"durable_v1"}, 0, 60
        ),
        lambda: repository.claim_batch(
            "worker", _LEASE_SESSION_ID, {"durable_v1"}, 501, 60
        ),
        lambda: repository.claim_batch(
            "worker", _LEASE_SESSION_ID, {"durable_v1"}, 1, 0
        ),
        lambda: repository.claim_batch(
            "worker", _LEASE_SESSION_ID, {"durable_v1"}, 1, 3601
        ),
    )

    for call in invalid_calls:
        with pytest.raises(ValueError):
            await call()


@pytest.mark.parametrize(
    "method_name",
    (
        "finish_email_processing",
        "finish_email_processing_failure",
        "complete",
        "fail",
        "_recover_linked_expired_lease",
        "_recover_unlinked_expired_lease",
    ),
)
def test_every_terminal_lease_mutation_clears_its_runtime_session(
    method_name: str,
) -> None:
    source = inspect.getsource(getattr(InboxRepository, method_name))

    assert "lease_session_id = NULL" in source


@pytest.mark.parametrize(
    "method_name",
    (
        "renew",
        "begin_effect",
        "begin_processing_effect",
        "finish_email_processing",
        "finish_email_processing_failure",
        "complete",
        "fail",
        "_recover_linked_expired_lease",
        "_recover_unlinked_expired_lease",
    ),
)
def test_every_lease_cas_matches_the_exact_runtime_session(method_name: str) -> None:
    source = inspect.getsource(getattr(InboxRepository, method_name))

    assert "lease_session_id = %s" in source


@pytest.mark.parametrize("method_name", ("renew", "begin_processing_effect"))
def test_session_sensitive_authorization_requires_exact_live_web_runtime_authority(
    method_name: str,
) -> None:
    source = inspect.getsource(getattr(InboxRepository, method_name))

    for predicate in (
        "runtime.session_id = e.lease_session_id",
        "runtime.account_id = e.account_id",
        "runtime.generation = e.generation",
        "runtime.fencing_token = e.fencing_token",
        "runtime.authority_epoch = e.authority_epoch",
        "runtime.capability_hash = e.capability_hash",
        "runtime.instance_id = e.lease_owner",
        "runtime.workload = 'web'",
        "runtime.lifecycle = 'active'",
        "runtime.lease_until > ",
    ):
        assert predicate in source
    if method_name == "begin_processing_effect":
        assert source.index("runtime.session_id = %s") < source.index(
            "if email_marked:"
        )
        assert source.index("runtime.session_id = e.lease_session_id") > source.index(
            "inbox_update ="
        )


@pytest.mark.parametrize(
    ("owner", "method_name"),
    (
        (InboxRepository, "_lock_processing_ownership"),
        (EmailEventTransaction, "_lock_ownership"),
    ),
)
def test_runtime_ownership_reads_rely_on_account_advisory_lock_without_row_lock(
    owner: type[object],
    method_name: str,
) -> None:
    source = inspect.getsource(getattr(owner, method_name))

    assert "pipeline_ownership" in source
    assert "FOR SHARE" not in source
    assert "FOR KEY SHARE" not in source


@pytest.mark.parametrize("pipeline_names", ("durable_v1", object()))
def test_pipeline_collection_rejects_scalar_and_noniterable_inputs_without_io(
    pipeline_names: object,
) -> None:
    with pytest.raises(ValueError, match="bounded collection"):
        _require_pipeline_names(pipeline_names)


def test_pipeline_collection_deduplicates_then_sorts_exact_names() -> None:
    assert _require_pipeline_names(["zeta", "alpha", "zeta"]) == (
        "alpha",
        "zeta",
    )


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
async def test_fail_rejects_nonexception_before_validating_lease_or_using_pool() -> (
    None
):
    repository = InboxRepository(_NeverPool())

    with pytest.raises(ValueError, match="error must be an exception"):
        await repository.fail(object(), object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_fail_propagates_baseexception_control_flow_before_lease_or_pool() -> (
    None
):
    repository = InboxRepository(_NeverPool())
    control_flow = KeyboardInterrupt("stop")

    with pytest.raises(KeyboardInterrupt, match="stop") as caught:
        await repository.fail(object(), control_flow)  # type: ignore[arg-type]

    assert caught.value is control_flow


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "args", "operation"),
    [
        ("insert", (None, 1, 1), "insert_event_inbox"),
        (
            "claim_batch",
            (
                "worker",
                _LEASE_SESSION_ID,
                frozenset({"durable_v1"}),
                1,
                60,
            ),
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
        "execution_epoch",
        "authority_epoch",
        "capability_hash",
        "lease_session_id",
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
        7,
        9,
        "b" * 64,
        "00000000-0000-4000-8000-000000000003",
        "worker",
        now + timedelta(seconds=60),
        0,
        now,
    )

    tuple_lease = _lease_from_row(values)
    dict_lease = _lease_from_row(dict(zip(columns, values)))

    assert tuple_lease == dict_lease
    assert (
        tuple_lease.execution_epoch,
        tuple_lease.authority_epoch,
        tuple_lease.capability_hash,
        tuple_lease.lease_session_id,
    ) == (
        7,
        9,
        "b" * 64,
        "00000000-0000-4000-8000-000000000003",
    )


def test_tuple_and_dict_rows_decode_email_owner_and_execution_stamps() -> None:
    processing_inbox_id = str(uuid4())
    row = _email_database_row(
        owner_authority_epoch=11,
        owner_capability_hash="c" * 64,
        processing_inbox_id=processing_inbox_id,
        processing_execution_epoch=13,
    )
    columns = tuple(row)
    values = tuple(row[column] for column in columns)

    tuple_email = _email_from_row(values)
    dict_email = _email_from_row(dict(zip(columns, values)))

    assert tuple_email == dict_email
    assert (
        tuple_email.owner_authority_epoch,
        tuple_email.owner_capability_hash,
        tuple_email.processing_inbox_id,
        tuple_email.processing_execution_epoch,
    ) == (11, "c" * 64, processing_inbox_id, 13)


def test_row_values_rejects_mapping_with_missing_columns_as_fixed_invariant() -> None:
    with pytest.raises(DatabaseOperationError) as caught:
        _row_values({"id": "only"}, ("id", "missing"))

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox database row is invalid"


def test_lease_decoder_rejects_nonmapping_payload_as_fixed_invariant(
    normalized_event: NormalizedIngressEvent,
) -> None:
    row = _lease_database_row(normalized_event, payload=[])

    with pytest.raises(DatabaseOperationError) as caught:
        _lease_from_row(row)

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox database row is invalid"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("version", True),
        ("owner_authority_epoch", True),
        ("owner_capability_hash", "A" * 64),
        ("processing_execution_epoch", True),
        ("processing_execution_epoch", -1),
        ("is_read", 1),
        ("is_read_refresh_required", 1),
    ),
)
def test_email_decoder_rejects_nonexact_scalar_types_as_fixed_invariant(
    field: str,
    value: object,
) -> None:
    row = _email_database_row(**{field: value})

    with pytest.raises(DatabaseOperationError) as caught:
        _email_from_row(row)

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert str(caught.value) == "email aggregate database row is invalid"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("execution_epoch", True),
        ("execution_epoch", -1),
        ("authority_epoch", 0),
        ("capability_hash", "A" * 64),
        ("lease_session_id", "not-a-session"),
    ),
)
def test_lease_decoder_rejects_invalid_runtime_stamps_as_fixed_invariant(
    normalized_event: NormalizedIngressEvent,
    field: str,
    value: object,
) -> None:
    row = _lease_database_row(normalized_event, **{field: value})

    with pytest.raises(DatabaseOperationError) as caught:
        _lease_from_row(row)

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox database row is invalid"


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    (
        (None, None, True),
        (None, 0, False),
        (True, 1, False),
        ({"key": 1}, [], False),
        ({"key": 1}, {"other": 1}, False),
        ({"key": [1, True]}, {"key": [1.0, True]}, True),
        ([], object(), False),
        ([1], [1, 2], False),
        ([{"key": 1}], [{"key": 1.0}], True),
        (object(), object(), False),
    ),
)
def test_json_equality_is_recursive_and_preserves_json_type_boundaries(
    left: object,
    right: object,
    expected: bool,
) -> None:
    assert _json_values_equal(left, right) is expected


def test_read_projection_fails_closed_for_unknown_ingress_source() -> None:
    event = SimpleNamespace(
        kind=ChangeKind.CREATE,
        source=object(),
        payload={"is_read": True},
    )

    assert _source_is_read(event) is None  # type: ignore[arg-type]


def test_invalid_database_row_is_a_fixed_nonretryable_error() -> None:
    with pytest.raises(DatabaseOperationError) as caught:
        _lease_from_row(("invalid",))

    assert caught.value.operation == "event_inbox_invariant"
    assert caught.value.retryable is False
    assert str(caught.value) == "event inbox database row is invalid"
