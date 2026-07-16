"""Caller-owned transaction boundary for durable command idempotency receipts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus


_COMMAND_NAMES = frozenset(
    {
        "cold_start.preview",
        "cold_start.approve",
        "cold_start.apply_page",
    }
)
_LOWER_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CANONICAL_UUID_TEXT = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
_RESULT_TYPE = "sync_cold_start_plan"
_IDEMPOTENCY_DOMAIN = b"pipeline-command-idempotency-v1\x00"
_TRANSACTION_ROW_NAMES = ("transaction_id", "isolation_level")
_RECEIPT_ROW_NAMES = (
    "id",
    "account_id",
    "command_name",
    "idempotency_key_hash",
    "canonical_payload_hash",
    "outcome",
    "result_type",
    "result_id",
    "result_hash",
    "authority_epoch",
    "created_at",
)


class IdempotencyConflict(RuntimeError):
    """Raised when one idempotency key is replayed with different semantics."""

    def __init__(self) -> None:
        super().__init__("command_idempotency_conflict")


@dataclass(frozen=True, slots=True)
class CommandReceipt:
    id: UUID
    account_id: int
    command_name: str
    idempotency_key_hash: str
    canonical_payload_hash: str
    outcome: str
    result_type: str
    result_id: str
    result_hash: str
    authority_epoch: int
    created_at: datetime


class CommandReceiptRepository:
    """Bind receipt operations to a transaction owned by the caller."""

    def __init__(self, *, target_schema: str = "public") -> None:
        if (
            type(target_schema) is not str
            or not target_schema
            or "\x00" in target_schema
        ):
            raise ValueError("command_receipt_schema_invalid")
        self._receipt_view = sql.Identifier(
            target_schema,
            "cold_start_command_receipts",
        )

    def transaction(
        self,
        connection: psycopg.AsyncConnection,
    ) -> _CommandReceiptTransaction:
        return _CommandReceiptTransaction(connection, self._receipt_view)


class _CommandReceiptTransaction:
    def __init__(
        self,
        connection: psycopg.AsyncConnection,
        receipt_view: sql.Identifier,
    ) -> None:
        self._connection = connection
        self._receipt_view = receipt_view
        self._transaction_id: str | None = None

    async def lookup(
        self,
        *,
        account_id: int,
        command_name: str,
        idempotency_key: str,
        canonical_payload_hash: str,
    ) -> CommandReceipt | None:
        """Return a committed replay while holding its same-XID identity lock."""
        if self._connection.info.transaction_status is not TransactionStatus.INTRANS:
            raise RuntimeError("command_receipt_transaction_required")
        self._validate_lookup_input(
            account_id=account_id,
            command_name=command_name,
            idempotency_key=idempotency_key,
            canonical_payload_hash=canonical_payload_hash,
        )
        await self._pin_transaction()
        idempotency_key_hash = _hash_idempotency_key(
            account_id,
            command_name,
            idempotency_key,
        )
        await self._lock_identity(idempotency_key_hash)
        existing = await self._find(
            account_id=account_id,
            command_name=command_name,
            idempotency_key_hash=idempotency_key_hash,
        )
        if existing is None:
            return None
        return _require_matching_receipt(existing, canonical_payload_hash)

    async def insert(
        self,
        *,
        account_id: int,
        command_name: str,
        idempotency_key: str,
        canonical_payload_hash: str,
        outcome: str,
        result_type: str,
        result_id: str,
        result_hash: str,
        authority_epoch: int,
    ) -> CommandReceipt:
        if self._connection.info.transaction_status is not TransactionStatus.INTRANS:
            raise RuntimeError("command_receipt_transaction_required")
        self._validate_input(
            account_id=account_id,
            command_name=command_name,
            idempotency_key=idempotency_key,
            canonical_payload_hash=canonical_payload_hash,
            outcome=outcome,
            result_type=result_type,
            result_id=result_id,
            result_hash=result_hash,
            authority_epoch=authority_epoch,
        )
        await self._pin_transaction()
        idempotency_key_hash = _hash_idempotency_key(
            account_id,
            command_name,
            idempotency_key,
        )
        await self._lock_identity(idempotency_key_hash)

        existing = await self._find(
            account_id=account_id,
            command_name=command_name,
            idempotency_key_hash=idempotency_key_hash,
        )
        if existing is not None:
            return _require_matching_receipt(existing, canonical_payload_hash)

        cursor = await self._connection.execute(
            sql.SQL(
                "INSERT INTO {} ("
                "id, account_id, command_name, idempotency_key_hash, "
                "canonical_payload_hash, outcome, result_type, result_id, "
                "result_hash, authority_epoch"
                ") VALUES ("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
                ") RETURNING "
                "id, account_id, command_name, idempotency_key_hash, "
                "canonical_payload_hash, outcome, result_type, result_id, "
                "result_hash, authority_epoch, created_at"
            ).format(self._receipt_view),
            (
                uuid4(),
                account_id,
                command_name,
                idempotency_key_hash,
                canonical_payload_hash,
                outcome,
                result_type,
                result_id,
                result_hash,
                authority_epoch,
            ),
        )
        row = await cursor.fetchone()

        if row is None:
            raise RuntimeError("command_receipt_insert_failed")
        return _row_to_receipt(row)

    async def _lock_identity(self, idempotency_key_hash: str) -> None:
        advisory_lock_key = int.from_bytes(
            bytes.fromhex(idempotency_key_hash)[:8],
            byteorder="big",
            signed=True,
        )
        await self._connection.execute(
            "SELECT pg_catalog.pg_advisory_xact_lock(%s)",
            (advisory_lock_key,),
        )

    async def _pin_transaction(self) -> None:
        cursor = await self._connection.execute(
            "SELECT "
            "pg_catalog.pg_current_xact_id()::pg_catalog.text "
            "AS transaction_id, "
            "pg_catalog.current_setting('transaction_isolation') "
            "AS isolation_level",
        )
        row = await cursor.fetchone()
        values = _row_values(row, _TRANSACTION_ROW_NAMES)
        if (
            values is None
            or type(values[0]) is not str
            or type(values[1]) is not str
            or not 0 < len(values[0]) <= 32
            or not values[0].isascii()
            or not values[0].isdigit()
            or values[1] != "read committed"
        ):
            raise RuntimeError("command_receipt_transaction_invalid")
        transaction_id = values[0]
        if self._transaction_id is None:
            self._transaction_id = transaction_id
        elif self._transaction_id != transaction_id:
            raise RuntimeError("command_receipt_transaction_changed")

    async def _find(
        self,
        *,
        account_id: int,
        command_name: str,
        idempotency_key_hash: str,
    ) -> CommandReceipt | None:
        cursor = await self._connection.execute(
            sql.SQL(
                "SELECT "
                "id, account_id, command_name, idempotency_key_hash, "
                "canonical_payload_hash, outcome, result_type, result_id, "
                "result_hash, authority_epoch, created_at "
                "FROM {} "
                "WHERE account_id = %s "
                "AND command_name = %s "
                "AND idempotency_key_hash = %s"
            ).format(self._receipt_view),
            (account_id, command_name, idempotency_key_hash),
        )
        row = await cursor.fetchone()
        return None if row is None else _row_to_receipt(row)

    @staticmethod
    def _validate_input(
        *,
        account_id: int,
        command_name: str,
        idempotency_key: str,
        canonical_payload_hash: str,
        outcome: str,
        result_type: str,
        result_id: str,
        result_hash: str,
        authority_epoch: int,
    ) -> None:
        exact_types = (
            type(account_id) is int
            and type(command_name) is str
            and type(idempotency_key) is str
            and type(canonical_payload_hash) is str
            and type(outcome) is str
            and type(result_type) is str
            and type(result_id) is str
            and type(result_hash) is str
            and type(authority_epoch) is int
        )
        if not exact_types:
            raise ValueError("command_receipt_input_invalid")
        valid = (
            0 < account_id <= 2**63 - 1
            and command_name in _COMMAND_NAMES
            and 0 < len(idempotency_key.encode("utf-8")) <= 4096
            and _LOWER_HEX_SHA256.fullmatch(canonical_payload_hash) is not None
            and outcome == "succeeded"
            and result_type == _RESULT_TYPE
            and _CANONICAL_UUID_TEXT.fullmatch(result_id) is not None
            and _LOWER_HEX_SHA256.fullmatch(result_hash) is not None
            and 0 <= authority_epoch <= 2**63 - 1
        )
        if not valid:
            raise ValueError("command_receipt_input_invalid")

    @staticmethod
    def _validate_lookup_input(
        *,
        account_id: int,
        command_name: str,
        idempotency_key: str,
        canonical_payload_hash: str,
    ) -> None:
        exact_types = (
            type(account_id) is int
            and type(command_name) is str
            and type(idempotency_key) is str
            and type(canonical_payload_hash) is str
        )
        if not exact_types:
            raise ValueError("command_receipt_input_invalid")
        valid = (
            0 < account_id <= 2**63 - 1
            and command_name in _COMMAND_NAMES
            and 0 < len(idempotency_key.encode("utf-8")) <= 4096
            and _LOWER_HEX_SHA256.fullmatch(canonical_payload_hash) is not None
        )
        if not valid:
            raise ValueError("command_receipt_input_invalid")


def _hash_idempotency_key(
    account_id: int,
    command_name: str,
    idempotency_key: str,
) -> str:
    material = f"{account_id}\x00{command_name}\x00{idempotency_key}".encode("utf-8")
    return hashlib.sha256(_IDEMPOTENCY_DOMAIN + material).hexdigest()


def _row_values(
    row: object,
    names: tuple[str, ...],
) -> tuple[object, ...] | None:
    if isinstance(row, tuple):
        return row if len(row) == len(names) else None
    if isinstance(row, Mapping):
        if set(row.keys()) != set(names):
            return None
        return tuple(row[name] for name in names)
    return None


def _row_to_receipt(row: object) -> CommandReceipt:
    values = _row_values(row, _RECEIPT_ROW_NAMES)
    if values is None:
        raise RuntimeError("command_receipt_persisted_invalid")
    (
        receipt_id,
        account_id,
        command_name,
        idempotency_key_hash,
        canonical_payload_hash,
        outcome,
        result_type,
        result_id,
        result_hash,
        authority_epoch,
        created_at,
    ) = values
    exact_types = (
        type(receipt_id) is UUID
        and type(account_id) is int
        and type(command_name) is str
        and type(idempotency_key_hash) is str
        and type(canonical_payload_hash) is str
        and type(outcome) is str
        and type(result_type) is str
        and type(result_id) is str
        and type(result_hash) is str
        and type(authority_epoch) is int
        and type(created_at) is datetime
    )
    if not exact_types:
        raise RuntimeError("command_receipt_persisted_invalid")
    try:
        normalized_created_at = _normalize_database_datetime(created_at)
    except ValueError:
        raise RuntimeError("command_receipt_persisted_invalid") from None
    valid = (
        0 < account_id <= 2**63 - 1
        and command_name in _COMMAND_NAMES
        and _LOWER_HEX_SHA256.fullmatch(idempotency_key_hash) is not None
        and _LOWER_HEX_SHA256.fullmatch(canonical_payload_hash) is not None
        and outcome == "succeeded"
        and result_type == _RESULT_TYPE
        and _CANONICAL_UUID_TEXT.fullmatch(result_id) is not None
        and _LOWER_HEX_SHA256.fullmatch(result_hash) is not None
        and 0 <= authority_epoch <= 2**63 - 1
    )
    if not valid:
        raise RuntimeError("command_receipt_persisted_invalid")
    return CommandReceipt(
        id=receipt_id,
        account_id=account_id,
        command_name=command_name,
        idempotency_key_hash=idempotency_key_hash,
        canonical_payload_hash=canonical_payload_hash,
        outcome=outcome,
        result_type=result_type,
        result_id=result_id,
        result_hash=result_hash,
        authority_epoch=authority_epoch,
        created_at=normalized_created_at,
    )


def _normalize_database_datetime(value: datetime) -> datetime:
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError
        normalized = value.astimezone(UTC)
    except Exception:
        raise ValueError("persisted datetime is invalid") from None
    if type(normalized) is not datetime or normalized.tzinfo is not UTC:
        raise ValueError("persisted datetime is invalid")
    return normalized


def _require_matching_receipt(
    receipt: CommandReceipt,
    canonical_payload_hash: str,
) -> CommandReceipt:
    if receipt.canonical_payload_hash != canonical_payload_hash:
        raise IdempotencyConflict()
    return receipt
