from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone, tzinfo
import re
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg.pq import TransactionStatus

from src.ingestion import command_receipts


_LEADING_SQL_TRIVIA = re.compile(
    r"\A(?:\s+|--[^\r\n]*(?:\r?\n|\Z)|/\*.*?\*/)*",
    re.DOTALL,
)
_TRANSACTION_LIFECYCLE_SQL = re.compile(
    r"\A(?:BEGIN\b|START\s+TRANSACTION\b|COMMIT\b|ROLLBACK\b)",
    re.IGNORECASE,
)


def _normalized_sql(query: object) -> str:
    rendered = query if isinstance(query, str) else query.as_string()  # type: ignore[attr-defined]
    without_leading_trivia = rendered[_LEADING_SQL_TRIVIA.match(rendered).end() :]
    return " ".join(without_leading_trivia.split())


class _HostileString(str):
    def encode(self, *_args, **_kwargs):
        raise AssertionError("hostile encode executed")

    def strip(self, *_args, **_kwargs):
        raise AssertionError("hostile strip executed")


class _NoneOffsetTimezone(tzinfo):
    def utcoffset(self, _value: datetime | None) -> None:
        return None

    def dst(self, _value: datetime | None) -> None:
        return None

    def tzname(self, _value: datetime | None) -> str:
        return "invalid-none-offset"


class _RaisingTimezone(tzinfo):
    def utcoffset(self, _value: datetime | None) -> timedelta:
        raise RuntimeError("invalid timezone offset")

    def dst(self, _value: datetime | None) -> None:
        return None

    def tzname(self, _value: datetime | None) -> str:
        return "invalid-raising-offset"


class _HostileNormalizedDatetime(datetime):
    def astimezone(self, *_args, **_kwargs):
        return self


class _NoSqlConnection:
    info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)

    async def execute(self, *_args, **_kwargs):
        raise AssertionError("invalid input reached SQL")


class _NoTransactionConnection:
    def __init__(self) -> None:
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.execute_calls = 0

    async def execute(self, *_args, **_kwargs):
        self.execute_calls += 1
        raise AssertionError("missing transaction reached SQL")


class _StaticCursor:
    def __init__(self, row):
        self._row = row

    async def fetchone(self):
        return self._row


class _XidConnection:
    info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)

    def __init__(self, row):
        self._row = row

    async def execute(self, *_args, **_kwargs):
        return _StaticCursor(self._row)


class _ScriptedConnection:
    info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)

    def __init__(self, rows):
        self._rows = iter(rows)
        self.queries = []
        self.params = []

    async def execute(self, query, *args, **_kwargs):
        self.queries.append(query)
        self.params.append(args[0] if args else None)
        return _StaticCursor(next(self._rows))


class _LifecycleHostileConnection(_ScriptedConnection):
    def __init__(self, rows) -> None:
        super().__init__(rows)
        self.lifecycle_calls: list[str] = []
        self.lifecycle_sql: list[str] = []

    async def execute(self, query, *args, **kwargs):
        normalized = _normalized_sql(query)
        if _TRANSACTION_LIFECYCLE_SQL.match(normalized) is not None:
            self.lifecycle_sql.append(normalized)
            raise AssertionError("repository executed transaction lifecycle SQL")
        return await super().execute(query, *args, **kwargs)

    def transaction(self):
        self.lifecycle_calls.append("transaction")
        raise AssertionError("repository entered a caller-owned transaction")

    async def commit(self):
        self.lifecycle_calls.append("commit")
        raise AssertionError("repository committed a caller-owned transaction")

    async def rollback(self):
        self.lifecycle_calls.append("rollback")
        raise AssertionError("repository rolled back a caller-owned transaction")

    async def close(self):
        self.lifecycle_calls.append("close")
        raise AssertionError("repository closed a caller-owned connection")

    async def __aenter__(self):
        self.lifecycle_calls.append("enter")
        raise AssertionError("repository entered a caller-owned connection")

    async def __aexit__(self, *_args):
        self.lifecycle_calls.append("exit")
        raise AssertionError("repository exited a caller-owned connection")


def _values(**overrides):
    values = {
        "account_id": 8,
        "command_name": "cold_start.preview",
        "idempotency_key": "command-1",
        "canonical_payload_hash": "a" * 64,
        "outcome": "succeeded",
        "result_type": "sync_cold_start_plan",
        "result_id": str(uuid4()),
        "result_hash": "b" * 64,
        "authority_epoch": 0,
    }
    values.update(overrides)
    return values


def _receipt_row(**overrides):
    values = {
        "id": uuid4(),
        "account_id": 8,
        "command_name": "cold_start.preview",
        "idempotency_key_hash": command_receipts._hash_idempotency_key(
            8,
            "cold_start.preview",
            "command-1",
        ),
        "canonical_payload_hash": "a" * 64,
        "outcome": "succeeded",
        "result_type": "sync_cold_start_plan",
        "result_id": str(uuid4()),
        "result_hash": "b" * 64,
        "authority_epoch": 0,
        "created_at": datetime.now(UTC),
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize("target_schema", (None, "", "bad\x00schema"))
def test_repository_rejects_invalid_schema_before_constructing_sql(
    target_schema: object,
) -> None:
    with pytest.raises(ValueError, match="command_receipt_schema_invalid"):
        command_receipts.CommandReceiptRepository(target_schema=target_schema)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("lookup", "insert"))
async def test_receipt_operations_require_active_transaction_before_validation_or_sql(
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _NoTransactionConnection()
    transaction = command_receipts.CommandReceiptRepository().transaction(connection)
    validation_calls = 0
    hash_calls = 0

    def forbidden_validation(**_kwargs) -> None:
        nonlocal validation_calls
        validation_calls += 1
        raise AssertionError("input validation ran without an active transaction")

    def forbidden_hash(*_args) -> str:
        nonlocal hash_calls
        hash_calls += 1
        raise AssertionError("identity hashing ran without an active transaction")

    validation_name = (
        "_validate_lookup_input" if operation == "lookup" else "_validate_input"
    )
    monkeypatch.setattr(transaction, validation_name, forbidden_validation)
    monkeypatch.setattr(command_receipts, "_hash_idempotency_key", forbidden_hash)
    kwargs = {
        "account_id": True,
        "command_name": _HostileString("invalid.command"),
        "idempotency_key": _HostileString(""),
        "canonical_payload_hash": _HostileString("not-a-hash"),
    }
    if operation == "insert":
        kwargs.update(
            outcome="failed",
            result_type="invalid",
            result_id="invalid",
            result_hash="invalid",
            authority_epoch=False,
        )

    with pytest.raises(RuntimeError, match="command_receipt_transaction_required"):
        await getattr(transaction, operation)(**kwargs)

    assert validation_calls == 0
    assert hash_calls == 0
    assert connection.execute_calls == 0


def test_persisted_receipt_normalizes_aware_database_timestamp_to_utc() -> None:
    offset_stamp = datetime.now(UTC).astimezone(timezone(timedelta(hours=8)))

    receipt = command_receipts._row_to_receipt(_receipt_row(created_at=offset_stamp))

    assert receipt.created_at == offset_stamp.astimezone(UTC)
    assert receipt.created_at.tzinfo is UTC


@pytest.mark.parametrize(
    "created_at",
    (
        datetime(2026, 7, 15, 1, 2, 3),
        type("HostileDatetime", (datetime,), {})(2026, 7, 15, tzinfo=UTC),
        datetime(2026, 7, 15, 1, 2, 3, tzinfo=_NoneOffsetTimezone()),
        datetime(2026, 7, 15, 1, 2, 3, tzinfo=_RaisingTimezone()),
    ),
)
def test_persisted_receipt_rejects_naive_and_datetime_subclass_timestamps(
    created_at: object,
) -> None:
    with pytest.raises(RuntimeError, match="command_receipt_persisted_invalid"):
        command_receipts._row_to_receipt(_receipt_row(created_at=created_at))


def test_datetime_normalizer_rejects_hostile_nonexact_normalized_result() -> None:
    value = _HostileNormalizedDatetime(2026, 7, 15, 1, 2, 3, tzinfo=UTC)

    with pytest.raises(ValueError, match="persisted datetime is invalid"):
        command_receipts._normalize_database_datetime(value)


def test_public_ingestion_api_exports_receipt_contract() -> None:
    from src import ingestion

    assert ingestion.CommandReceipt is command_receipts.CommandReceipt
    assert (
        ingestion.CommandReceiptRepository is command_receipts.CommandReceiptRepository
    )
    assert ingestion.IdempotencyConflict is command_receipts.IdempotencyConflict


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("account_id", True),
        ("account_id", 2**63),
        ("authority_epoch", False),
        ("authority_epoch", 2**63),
        ("command_name", _HostileString("cold_start.preview")),
        ("idempotency_key", _HostileString("command-1")),
        ("canonical_payload_hash", _HostileString("a" * 64)),
        ("outcome", _HostileString("succeeded")),
        ("result_type", _HostileString("sync_cold_start_plan")),
        ("result_id", _HostileString(str(uuid4()))),
        ("result_hash", _HostileString("b" * 64)),
        ("result_type", "actor-raw-text"),
        ("result_id", "payload-raw-text"),
    ],
    ids=(
        "bool-account",
        "account-overflow",
        "bool-epoch",
        "epoch-overflow",
        "hostile-command",
        "hostile-key",
        "hostile-payload-hash",
        "hostile-outcome",
        "hostile-result-type",
        "hostile-result-id",
        "hostile-result-hash",
        "raw-result-type",
        "raw-result-id",
    ),
)
async def test_receipt_input_rejects_non_exact_or_out_of_range_types(
    field,
    value,
) -> None:
    repository = command_receipts.CommandReceiptRepository()

    with pytest.raises(ValueError, match="command_receipt_input_invalid"):
        await repository.transaction(_NoSqlConnection()).insert(
            **_values(**{field: value})
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("account_id", True),
        ("command_name", _HostileString("cold_start.preview")),
        ("account_id", 0),
        ("command_name", "unknown.command"),
        ("idempotency_key", ""),
        ("canonical_payload_hash", "A" * 64),
    ),
    ids=(
        "bool-account",
        "hostile-command",
        "zero-account",
        "unknown-command",
        "empty-key",
        "uppercase-hash",
    ),
)
async def test_lookup_input_fails_closed_before_identity_lock_or_sql(
    field: str,
    value: object,
) -> None:
    repository = command_receipts.CommandReceiptRepository()
    kwargs = {
        "account_id": 8,
        "command_name": "cold_start.preview",
        "idempotency_key": "command-1",
        "canonical_payload_hash": "a" * 64,
    }
    kwargs[field] = value

    with pytest.raises(ValueError, match="command_receipt_input_invalid"):
        await repository.transaction(_NoSqlConnection()).lookup(**kwargs)  # type: ignore[arg-type]


def test_row_decoder_rejects_invalid_persisted_shape() -> None:
    row = (
        uuid4(),
        8,
        "cold_start.preview",
        "not-a-hash",
        "a" * 64,
        "succeeded",
        "sync_cold_start_plan",
        str(uuid4()),
        "b" * 64,
        0,
        datetime.now(UTC),
    )

    with pytest.raises(RuntimeError, match="command_receipt_persisted_invalid"):
        command_receipts._row_to_receipt(row)


def test_row_decoder_accepts_exact_dict_row_shape() -> None:
    receipt_id = uuid4()
    created_at = datetime.now(UTC)
    row = {
        "id": receipt_id,
        "account_id": 8,
        "command_name": "cold_start.preview",
        "idempotency_key_hash": "c" * 64,
        "canonical_payload_hash": "a" * 64,
        "outcome": "succeeded",
        "result_type": "sync_cold_start_plan",
        "result_id": str(uuid4()),
        "result_hash": "b" * 64,
        "authority_epoch": 0,
        "created_at": created_at,
    }

    receipt = command_receipts._row_to_receipt(row)

    assert receipt.id == receipt_id
    assert receipt.created_at == created_at


@pytest.mark.parametrize("row", ({"id": uuid4()}, object()))
def test_row_decoder_rejects_mapping_key_drift_and_unsupported_row_types(
    row: object,
) -> None:
    with pytest.raises(RuntimeError, match="command_receipt_persisted_invalid"):
        command_receipts._row_to_receipt(row)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("result_type", "actor-raw-text"),
        ("result_id", "payload-raw-text"),
    ],
)
def test_row_decoder_rejects_nonminimal_persisted_result(field, value) -> None:
    with pytest.raises(RuntimeError, match="command_receipt_persisted_invalid"):
        command_receipts._row_to_receipt(_receipt_row(**{field: value}))


def test_idempotency_hash_is_domain_separated_by_account_and_command() -> None:
    hashes = {
        command_receipts._hash_idempotency_key(8, "cold_start.preview", "same"),
        command_receipts._hash_idempotency_key(9, "cold_start.preview", "same"),
        command_receipts._hash_idempotency_key(8, "cold_start.approve", "same"),
    }

    assert len(hashes) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction_id", ["", "abc", "１", "1" * 33])
async def test_transaction_pin_rejects_noncanonical_xid(transaction_id) -> None:
    repository = command_receipts.CommandReceiptRepository()
    connection = _XidConnection(
        {
            "transaction_id": transaction_id,
            "isolation_level": "read committed",
        }
    )

    with pytest.raises(RuntimeError, match="command_receipt_transaction_invalid"):
        await repository.transaction(connection).insert(**_values())


@pytest.mark.asyncio
async def test_public_lookup_miss_holds_identity_lock_without_writing() -> None:
    repository = command_receipts.CommandReceiptRepository()
    connection = _ScriptedConnection(
        [
            {"transaction_id": "91", "isolation_level": "read committed"},
            None,
            None,
        ]
    )

    receipt = await repository.transaction(connection).lookup(
        account_id=8,
        command_name="cold_start.preview",
        idempotency_key="command-1",
        canonical_payload_hash="a" * 64,
    )

    assert receipt is None
    assert len(connection.queries) == 3
    assert all("INSERT" not in str(query) for query in connection.queries)
    assert "pg_advisory_xact_lock" in str(connection.queries[1])
    identity_hash = command_receipts._hash_idempotency_key(
        8,
        "cold_start.preview",
        "command-1",
    )
    expected_lock_key = int.from_bytes(
        bytes.fromhex(identity_hash)[:8],
        byteorder="big",
        signed=True,
    )
    assert connection.params[1] == (expected_lock_key,)
    assert connection.params[2] == (8, "cold_start.preview", identity_hash)


@pytest.mark.asyncio
async def test_public_lookup_returns_matching_persisted_receipt() -> None:
    repository = command_receipts.CommandReceiptRepository()
    receipt_id = uuid4()
    result_id = str(uuid4())
    created_at = datetime(2026, 7, 15, 1, 2, 3, tzinfo=UTC)
    idempotency_key_hash = command_receipts._hash_idempotency_key(
        8,
        "cold_start.preview",
        "command-1",
    )
    row = {
        "id": receipt_id,
        "account_id": 8,
        "command_name": "cold_start.preview",
        "idempotency_key_hash": idempotency_key_hash,
        "canonical_payload_hash": "a" * 64,
        "outcome": "succeeded",
        "result_type": "sync_cold_start_plan",
        "result_id": result_id,
        "result_hash": "b" * 64,
        "authority_epoch": 0,
        "created_at": created_at,
    }
    expected = command_receipts.CommandReceipt(
        id=receipt_id,
        account_id=8,
        command_name="cold_start.preview",
        idempotency_key_hash=idempotency_key_hash,
        canonical_payload_hash="a" * 64,
        outcome="succeeded",
        result_type="sync_cold_start_plan",
        result_id=result_id,
        result_hash="b" * 64,
        authority_epoch=0,
        created_at=created_at,
    )
    connection = _ScriptedConnection(
        [
            {"transaction_id": "92", "isolation_level": "read committed"},
            None,
            row,
        ]
    )

    receipt = await repository.transaction(connection).lookup(
        account_id=8,
        command_name="cold_start.preview",
        idempotency_key="command-1",
        canonical_payload_hash="a" * 64,
    )

    assert receipt == expected


@pytest.mark.asyncio
async def test_public_lookup_rejects_payload_mismatch() -> None:
    repository = command_receipts.CommandReceiptRepository()
    connection = _ScriptedConnection(
        [
            {"transaction_id": "93", "isolation_level": "read committed"},
            None,
            _receipt_row(canonical_payload_hash="c" * 64),
        ]
    )

    with pytest.raises(command_receipts.IdempotencyConflict):
        await repository.transaction(connection).lookup(
            account_id=8,
            command_name="cold_start.preview",
            idempotency_key="command-1",
            canonical_payload_hash="a" * 64,
        )


@pytest.mark.asyncio
async def test_insert_rejects_missing_returning_row_after_single_insert_attempt() -> (
    None
):
    repository = command_receipts.CommandReceiptRepository()
    connection = _ScriptedConnection(
        [
            {"transaction_id": "94", "isolation_level": "read committed"},
            None,
            None,
            None,
        ]
    )

    with pytest.raises(RuntimeError, match="command_receipt_insert_failed"):
        await repository.transaction(connection).insert(**_values())

    assert len(connection.queries) == 4
    assert sum("INSERT INTO" in str(query) for query in connection.queries) == 1


@pytest.mark.asyncio
async def test_repository_never_manages_caller_owned_connection_lifecycle() -> None:
    connection = _LifecycleHostileConnection(
        [
            {"transaction_id": "95", "isolation_level": "read committed"},
            None,
            None,
        ]
    )

    receipt = (
        await command_receipts.CommandReceiptRepository()
        .transaction(connection)
        .lookup(
            account_id=8,
            command_name="cold_start.preview",
            idempotency_key="command-1",
            canonical_payload_hash="a" * 64,
        )
    )

    assert receipt is None
    assert connection.lifecycle_calls == []
    assert connection.lifecycle_sql == []
    assert len(connection.queries) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    (
        "BEGIN",
        " -- caller-owned\n START TRANSACTION READ WRITE",
        "/* caller-owned */ COMMIT AND CHAIN",
        "\nROLLBACK TO SAVEPOINT caller_owned",
    ),
)
async def test_lifecycle_hostile_connection_rejects_transaction_control_sql(
    statement: str,
) -> None:
    connection = _LifecycleHostileConnection([])

    with pytest.raises(
        AssertionError,
        match="repository executed transaction lifecycle SQL",
    ):
        await connection.execute(statement)

    assert connection.lifecycle_sql == [_normalized_sql(statement)]
    assert connection.queries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    (
        "SELECT 'BEGIN'",
        "SELECT 'START TRANSACTION'",
        "SELECT 'COMMIT'",
        "SELECT 'ROLLBACK'",
    ),
)
async def test_lifecycle_hostile_connection_allows_noncontrol_selects(
    statement: str,
) -> None:
    connection = _LifecycleHostileConnection([None])

    cursor = await connection.execute(statement)

    assert await cursor.fetchone() is None
    assert connection.lifecycle_sql == []
    assert connection.queries == [statement]
