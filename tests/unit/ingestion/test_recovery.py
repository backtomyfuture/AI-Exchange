from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
from datetime import UTC, datetime, timedelta, timezone
from types import MappingProxyType
from uuid import UUID

import pytest

from src.ingestion.recovery import (
    POSTGRES_BIGINT_MAX,
    InboxRecoveryService,
    RequeueCommand,
    RequeueReceipt,
    canonical_requeue_payload_hash,
)


_COMMAND_RECEIPT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_INBOX_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
_EMAIL_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
_CREATED_AT = datetime(2026, 7, 16, 8, 30, tzinfo=UTC)
_RECEIPT_COLUMNS = (
    "command_receipt_id",
    "inbox_id",
    "email_id",
    "previous_execution_epoch",
    "execution_epoch",
    "email_version",
    "status",
    "transaction_id",
    "replayed",
    "created_at",
)
_EXPECTED_SQL = (
    "SELECT command_receipt_id, inbox_id, email_id, "
    "previous_execution_epoch, execution_epoch, email_version, status, "
    "transaction_id, replayed, created_at "
    "FROM public.greenfield_requeue_inbox(%s, %s, %s, %s, %s, %s, %s, %s)"
)


class _HostileText(str):
    __hash__ = str.__hash__

    def strip(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("hostile text normalization must not run")


class _DatetimeSubclass(datetime):
    pass


def _command(**overrides: object) -> RequeueCommand:
    values: dict[str, object] = {
        "account_id": 8,
        "inbox_id": _INBOX_ID,
        "expected_execution_epoch": 4,
        "expected_email_version": 11,
        "actor": "operator@example.com",
        "reason": "Retry after operator review",
        "idempotency_key": "requeue-command-1",
    }
    values.update(overrides)
    return RequeueCommand(**values)  # type: ignore[arg-type]


def _receipt_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "command_receipt_id": _COMMAND_RECEIPT_ID,
        "inbox_id": _INBOX_ID,
        "email_id": _EMAIL_ID,
        "previous_execution_epoch": 4,
        "execution_epoch": 5,
        "email_version": 12,
        "status": "retry_wait",
        "transaction_id": "123456",
        "replayed": False,
        "created_at": _CREATED_AT,
    }
    values.update(overrides)
    return values


def _receipt_row(*, mapping: bool = False, **overrides: object) -> object:
    values = _receipt_values(**overrides)
    if mapping:
        return dict(values)
    return tuple(values[column] for column in _RECEIPT_COLUMNS)


class _Cursor:
    def __init__(
        self,
        events: list[str],
        row: object,
        *,
        failure: BaseException | None = None,
    ) -> None:
        self._events = events
        self._row = row
        self._failure = failure

    async def fetchone(self) -> object:
        self._events.append("cursor.fetchone")
        if self._failure is not None:
            raise self._failure
        return self._row


class _TransactionContext:
    def __init__(
        self,
        events: list[str],
        *,
        exit_failure: BaseException | None = None,
    ) -> None:
        self._events = events
        self._exit_failure = exit_failure

    async def __aenter__(self) -> object:
        self._events.append("transaction.enter")
        return self

    async def __aexit__(self, *args: object) -> bool:
        self._events.append("transaction.exit")
        if self._exit_failure is not None:
            raise self._exit_failure
        return False


class _Connection:
    def __init__(
        self,
        events: list[str],
        row: object,
        *,
        execute_failure: BaseException | None = None,
        fetch_failure: BaseException | None = None,
        commit_failure: BaseException | None = None,
    ) -> None:
        self._events = events
        self._row = row
        self._execute_failure = execute_failure
        self._fetch_failure = fetch_failure
        self._commit_failure = commit_failure
        self.calls: list[tuple[object, object]] = []

    def transaction(self) -> _TransactionContext:
        self._events.append("transaction.create")
        return _TransactionContext(
            self._events,
            exit_failure=self._commit_failure,
        )

    async def execute(self, statement: object, params: object) -> _Cursor:
        self._events.append("connection.execute")
        self.calls.append((statement, params))
        if self._execute_failure is not None:
            raise self._execute_failure
        return _Cursor(
            self._events,
            self._row,
            failure=self._fetch_failure,
        )


class _ConnectionContext:
    def __init__(self, events: list[str], connection: _Connection) -> None:
        self._events = events
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        self._events.append("connection.enter")
        return self._connection

    async def __aexit__(self, *_args: object) -> bool:
        self._events.append("connection.exit")
        return False


class _Pool:
    def __init__(
        self,
        row: object,
        *,
        execute_failure: BaseException | None = None,
        fetch_failure: BaseException | None = None,
        commit_failure: BaseException | None = None,
    ) -> None:
        self.events: list[str] = []
        self.connection_value = _Connection(
            self.events,
            row,
            execute_failure=execute_failure,
            fetch_failure=fetch_failure,
            commit_failure=commit_failure,
        )
        self.connection_calls = 0

    def connection(self) -> _ConnectionContext:
        self.connection_calls += 1
        self.events.append("connection.create")
        return _ConnectionContext(self.events, self.connection_value)


class _NeverPool:
    def __init__(self) -> None:
        self.connection_calls = 0

    def connection(self) -> object:
        self.connection_calls += 1
        raise AssertionError("invalid input must fail before pool access")


def test_requeue_command_is_frozen_slotted_keyword_only_and_exact() -> None:
    command = _command()

    assert [field.name for field in dataclasses.fields(command)] == [
        "account_id",
        "inbox_id",
        "expected_execution_epoch",
        "expected_email_version",
        "actor",
        "reason",
        "idempotency_key",
    ]
    assert not hasattr(command, "__dict__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        command.expected_execution_epoch = 5  # type: ignore[misc]
    with pytest.raises(TypeError):
        RequeueCommand(  # type: ignore[misc]
            8,
            _INBOX_ID,
            4,
            11,
            "operator@example.com",
            "Retry after review",
            "command-1",
        )


def test_requeue_command_accepts_bounded_utf8_and_bigint_edges() -> None:
    exact_reason_limit = "界" * 170 + "ab"
    command = _command(
        expected_execution_epoch=POSTGRES_BIGINT_MAX,
        expected_email_version=POSTGRES_BIGINT_MAX,
        actor="操作员",
        reason=exact_reason_limit,
        idempotency_key="重试-命令-1",
    )

    assert command.expected_execution_epoch == POSTGRES_BIGINT_MAX
    assert command.expected_email_version == POSTGRES_BIGINT_MAX
    assert command.actor == "操作员"
    assert command.reason == exact_reason_limit
    assert len(command.reason.encode("utf-8")) == 512


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("account_id", True),
        ("account_id", 0),
        ("account_id", POSTGRES_BIGINT_MAX + 1),
        ("inbox_id", object()),
        ("inbox_id", ""),
        ("inbox_id", _INBOX_ID.upper()),
        ("inbox_id", "00000000-0000-1000-8000-000000000001"),
        ("expected_execution_epoch", True),
        ("expected_execution_epoch", -1),
        ("expected_execution_epoch", POSTGRES_BIGINT_MAX + 1),
        ("expected_email_version", False),
        ("expected_email_version", -1),
        ("expected_email_version", POSTGRES_BIGINT_MAX + 1),
        ("actor", ""),
        ("actor", " operator"),
        ("actor", "operator\n"),
        ("actor", "界" * 43),
        ("actor", "\ud800"),
        ("actor", _HostileText("operator")),
        ("reason", ""),
        ("reason", "reason\x00private"),
        ("reason", "界" * 171),
        ("reason", "\ud800"),
        ("idempotency_key", ""),
        ("idempotency_key", " key"),
        ("idempotency_key", "key\r"),
        ("idempotency_key", "界" * 1366),
        ("idempotency_key", "\ud800"),
        ("idempotency_key", _HostileText("key")),
    ),
)
def test_requeue_command_rejects_nonexact_unbounded_or_noncanonical_fields(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=field):
        _command(**{field: value})


def test_requeue_receipt_is_frozen_slotted_exact_and_defaults_retry_wait() -> None:
    values = _receipt_values()
    values.pop("status")
    receipt = RequeueReceipt(**values)  # type: ignore[arg-type]

    assert [field.name for field in dataclasses.fields(receipt)] == [
        "command_receipt_id",
        "inbox_id",
        "email_id",
        "previous_execution_epoch",
        "execution_epoch",
        "email_version",
        "status",
        "transaction_id",
        "replayed",
        "created_at",
    ]
    assert receipt.status == "retry_wait"
    assert not hasattr(receipt, "__dict__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        receipt.replayed = True  # type: ignore[misc]


def test_requeue_receipt_normalizes_aware_datetime_to_utc() -> None:
    created_at = datetime(
        2026,
        7,
        16,
        16,
        30,
        tzinfo=timezone(timedelta(hours=8)),
    )
    receipt = RequeueReceipt(
        **_receipt_values(created_at=created_at),  # type: ignore[arg-type]
    )

    assert receipt.created_at == datetime(2026, 7, 16, 8, 30, tzinfo=UTC)
    assert receipt.created_at.tzinfo is UTC


def test_requeue_receipt_accepts_epoch_version_and_transaction_id_edges() -> None:
    receipt = RequeueReceipt(
        **_receipt_values(  # type: ignore[arg-type]
            previous_execution_epoch=POSTGRES_BIGINT_MAX - 2,
            execution_epoch=POSTGRES_BIGINT_MAX - 1,
            email_version=POSTGRES_BIGINT_MAX - 2,
            transaction_id="9" * 20,
        )
    )

    assert receipt.previous_execution_epoch == POSTGRES_BIGINT_MAX - 2
    assert receipt.execution_epoch == POSTGRES_BIGINT_MAX - 1
    assert receipt.email_version == POSTGRES_BIGINT_MAX - 2
    assert receipt.transaction_id == "9" * 20


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"command_receipt_id": ""}, "command_receipt_id"),
        ({"command_receipt_id": _COMMAND_RECEIPT_ID.upper()}, "command_receipt_id"),
        (
            {"command_receipt_id": "00000000-0000-1000-8000-000000000001"},
            "command_receipt_id",
        ),
        ({"inbox_id": "not-a-uuid"}, "inbox_id"),
        ({"email_id": "not-a-uuid"}, "email_id"),
        ({"previous_execution_epoch": True}, "previous_execution_epoch"),
        ({"previous_execution_epoch": -1}, "previous_execution_epoch"),
        (
            {
                "previous_execution_epoch": POSTGRES_BIGINT_MAX - 1,
                "execution_epoch": POSTGRES_BIGINT_MAX,
            },
            "execution_epoch",
        ),
        ({"previous_execution_epoch": POSTGRES_BIGINT_MAX}, "execution_epoch"),
        ({"execution_epoch": True}, "execution_epoch"),
        ({"execution_epoch": 4}, "execution_epoch"),
        ({"execution_epoch": 6}, "execution_epoch"),
        ({"email_version": True}, "email_version"),
        ({"email_version": -1}, "email_version"),
        ({"email_version": POSTGRES_BIGINT_MAX - 1}, "email_version"),
        ({"email_version": POSTGRES_BIGINT_MAX}, "email_version"),
        ({"email_version": POSTGRES_BIGINT_MAX + 1}, "email_version"),
        ({"status": "active"}, "status"),
        ({"status": _HostileText("retry_wait")}, "status"),
        ({"transaction_id": 123}, "transaction_id"),
        ({"transaction_id": ""}, "transaction_id"),
        ({"transaction_id": "0"}, "transaction_id"),
        ({"transaction_id": "0123"}, "transaction_id"),
        ({"transaction_id": "12a"}, "transaction_id"),
        ({"transaction_id": "1" * 21}, "transaction_id"),
        ({"replayed": 1}, "replayed"),
        ({"created_at": datetime(2026, 7, 16)}, "created_at"),
        (
            {"created_at": _DatetimeSubclass(2026, 7, 16, tzinfo=UTC)},
            "created_at",
        ),
    ),
)
def test_requeue_receipt_rejects_invalid_identity_epoch_or_result(
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        RequeueReceipt(**_receipt_values(**overrides))  # type: ignore[arg-type]


def test_canonical_payload_hash_is_domain_separated_stable_and_exact() -> None:
    command = _command()
    canonical = {
        "account_id": command.account_id,
        "actor": command.actor,
        "command_name": "inbox.requeue",
        "expected_email_version": command.expected_email_version,
        "expected_execution_epoch": command.expected_execution_epoch,
        "inbox_id": command.inbox_id,
        "reason": command.reason,
        "schema_version": 1,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    plain_hash = hashlib.sha256(encoded).hexdigest()
    expected = hashlib.sha256(
        b"ai-exchange-inbox-requeue-command-v1\x00" + encoded
    ).hexdigest()

    assert canonical_requeue_payload_hash(command) == expected
    assert command.canonical_payload_hash == expected
    assert expected != plain_hash
    assert canonical_requeue_payload_hash(_command()) == expected
    assert (
        canonical_requeue_payload_hash(_command(idempotency_key="another-key"))
        == expected
    )


@pytest.mark.parametrize(
    "changed",
    (
        {"account_id": 9},
        {"inbox_id": "44444444-4444-4444-8444-444444444444"},
        {"expected_execution_epoch": 5},
        {"expected_email_version": 12},
        {"actor": "another-operator"},
        {"reason": "Another safe reason"},
    ),
)
def test_every_semantic_command_field_changes_canonical_payload_hash(
    changed: dict[str, object],
) -> None:
    assert canonical_requeue_payload_hash(_command(**changed)) != (
        canonical_requeue_payload_hash(_command())
    )


@pytest.mark.asyncio
async def test_requeue_calls_only_the_fixed_security_definer_function_and_commits() -> (
    None
):
    command = _command()
    pool = _Pool(_receipt_row(mapping=True))

    receipt = await InboxRecoveryService(pool).requeue(command)

    assert receipt == RequeueReceipt(**_receipt_values())  # type: ignore[arg-type]
    assert pool.events == [
        "connection.create",
        "connection.enter",
        "transaction.create",
        "transaction.enter",
        "connection.execute",
        "cursor.fetchone",
        "transaction.exit",
        "connection.exit",
    ]
    assert pool.connection_value.calls == [
        (
            _EXPECTED_SQL,
            (
                command.account_id,
                command.inbox_id,
                command.expected_execution_epoch,
                command.expected_email_version,
                command.actor,
                command.reason,
                command.idempotency_key,
                command.canonical_payload_hash,
            ),
        )
    ]
    lowered = _EXPECTED_SQL.lower()
    assert "public.greenfield_requeue_inbox" in lowered
    assert "inbox.requeue" not in lowered
    assert all(
        token not in lowered
        for token in (
            " insert ",
            " update ",
            " delete ",
            " merge ",
            "truncate",
            "copy ",
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mapping", [False, True])
async def test_requeue_decodes_exact_tuple_and_mapping_rows(mapping: bool) -> None:
    row = _receipt_row(mapping=mapping, replayed=True)

    receipt = await InboxRecoveryService(_Pool(row)).requeue(_command())

    assert receipt.replayed is True
    assert receipt.previous_execution_epoch == 4
    assert receipt.execution_epoch == 5
    assert receipt.email_version == 12


@pytest.mark.asyncio
async def test_requeue_accepts_the_last_safe_command_capacity() -> None:
    command = _command(
        expected_execution_epoch=POSTGRES_BIGINT_MAX - 2,
        expected_email_version=POSTGRES_BIGINT_MAX - 3,
    )
    row = _receipt_row(
        previous_execution_epoch=POSTGRES_BIGINT_MAX - 2,
        execution_epoch=POSTGRES_BIGINT_MAX - 1,
        email_version=POSTGRES_BIGINT_MAX - 2,
        transaction_id="9" * 20,
    )

    receipt = await InboxRecoveryService(_Pool(row)).requeue(command)

    assert receipt.execution_epoch == POSTGRES_BIGINT_MAX - 1
    assert receipt.email_version == POSTGRES_BIGINT_MAX - 2


@pytest.mark.asyncio
async def test_requeue_accepts_an_exact_read_only_mapping_row() -> None:
    row = MappingProxyType(_receipt_values())

    receipt = await InboxRecoveryService(_Pool(row)).requeue(_command())

    assert receipt.command_receipt_id == _COMMAND_RECEIPT_ID


@pytest.mark.asyncio
async def test_requeue_decodes_exact_database_uuid_values() -> None:
    row = _receipt_values(
        command_receipt_id=UUID(_COMMAND_RECEIPT_ID),
        inbox_id=UUID(_INBOX_ID),
        email_id=UUID(_EMAIL_ID),
    )

    receipt = await InboxRecoveryService(_Pool(row)).requeue(_command())

    assert (
        receipt.command_receipt_id,
        receipt.inbox_id,
        receipt.email_id,
    ) == (_COMMAND_RECEIPT_ID, _INBOX_ID, _EMAIL_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    (
        None,
        (),
        _receipt_row()[:-1],  # type: ignore[index]
        {"inbox_id": _INBOX_ID},
        {**_receipt_values(), "unexpected": "field"},
        _receipt_row(mapping=True, command_receipt_id="not-a-uuid"),
        _receipt_row(mapping=True, command_receipt_id=object()),
        _receipt_row(
            mapping=True,
            command_receipt_id="00000000-0000-1000-8000-000000000001",
        ),
        _receipt_row(mapping=True, transaction_id="1" * 21),
        _receipt_row(mapping=True, email_version=POSTGRES_BIGINT_MAX - 1),
        object(),
    ),
)
async def test_requeue_rejects_nonexact_database_row_shapes(row: object) -> None:
    with pytest.raises(RuntimeError, match="requeue receipt row is invalid"):
        await InboxRecoveryService(_Pool(row)).requeue(_command())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    (
        {
            "inbox_id": "44444444-4444-4444-8444-444444444444",
        },
        {
            "previous_execution_epoch": 5,
            "execution_epoch": 6,
        },
        {
            "email_version": 13,
        },
    ),
)
async def test_requeue_rejects_receipt_that_does_not_match_the_command(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(RuntimeError, match="requeue receipt does not match command"):
        await InboxRecoveryService(_Pool(_receipt_row(**overrides))).requeue(_command())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_command",
    (object(), None),
)
async def test_requeue_rejects_noncommand_before_pool_access(
    invalid_command: object,
) -> None:
    pool = _NeverPool()

    with pytest.raises(ValueError, match="exact RequeueCommand"):
        await InboxRecoveryService(pool).requeue(invalid_command)  # type: ignore[arg-type]

    assert pool.connection_calls == 0


@pytest.mark.asyncio
async def test_requeue_rejects_command_subclass_before_pool_access() -> None:
    class _CommandSubclass(RequeueCommand):
        pass

    command = _command()
    subclass = _CommandSubclass(
        **{
            field.name: getattr(command, field.name)
            for field in dataclasses.fields(command)
        }
    )
    pool = _NeverPool()

    with pytest.raises(ValueError, match="exact RequeueCommand"):
        await InboxRecoveryService(pool).requeue(subclass)

    assert pool.connection_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("expected_execution_epoch", POSTGRES_BIGINT_MAX - 1),
        ("expected_execution_epoch", POSTGRES_BIGINT_MAX),
        ("expected_email_version", POSTGRES_BIGINT_MAX - 2),
        ("expected_email_version", POSTGRES_BIGINT_MAX - 1),
        ("expected_email_version", POSTGRES_BIGINT_MAX),
    ),
)
async def test_requeue_rejects_exhausted_constructed_command_before_pool_access(
    field: str,
    value: int,
) -> None:
    command = _command(**{field: value})
    pool = _NeverPool()

    with pytest.raises(ValueError, match=field):
        await InboxRecoveryService(pool).requeue(command)

    assert pool.connection_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("account_id", True),
        ("inbox_id", "not-a-uuid"),
        ("expected_execution_epoch", POSTGRES_BIGINT_MAX - 1),
        ("expected_email_version", POSTGRES_BIGINT_MAX - 2),
        ("actor", _HostileText("operator")),
        ("reason", "界" * 171),
    ),
)
async def test_requeue_revalidates_mutation_and_increment_capacity_before_io(
    field: str,
    value: object,
) -> None:
    command = _command()
    object.__setattr__(command, field, value)
    pool = _NeverPool()

    with pytest.raises(ValueError, match=field):
        await InboxRecoveryService(pool).requeue(command)

    assert pool.connection_calls == 0


@pytest.mark.asyncio
async def test_commit_acknowledgement_failure_is_propagated_without_a_receipt() -> None:
    lost_ack = ConnectionError("commit acknowledgement lost")
    pool = _Pool(_receipt_row(), commit_failure=lost_ack)

    with pytest.raises(ConnectionError, match="commit acknowledgement lost") as caught:
        await InboxRecoveryService(pool).requeue(_command())

    assert caught.value is lost_ack
    assert pool.events[-2:] == ["transaction.exit", "connection.exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["execute", "fetch", "commit"])
@pytest.mark.parametrize(
    "failure_factory",
    (
        lambda: asyncio.CancelledError("cancelled"),
        lambda: KeyboardInterrupt("interrupt"),
        lambda: SystemExit("exit"),
    ),
    ids=("cancelled", "keyboard-interrupt", "system-exit"),
)
async def test_process_control_exceptions_propagate_unchanged(
    stage: str,
    failure_factory,
) -> None:
    failure = failure_factory()
    failures = {
        "execute_failure": failure if stage == "execute" else None,
        "fetch_failure": failure if stage == "fetch" else None,
        "commit_failure": failure if stage == "commit" else None,
    }
    pool = _Pool(_receipt_row(), **failures)

    with pytest.raises(type(failure)) as caught:
        await InboxRecoveryService(pool).requeue(_command())

    assert caught.value is failure
