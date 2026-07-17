"""Strongly typed greenfield Inbox recovery boundary.

Only the migration-owned ``public.greenfield_requeue_inbox`` function may
mutate recovery state.  This module validates a command, calls that one fixed
function, validates its immutable receipt, and returns only after the database
transaction context has acknowledged commit.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final
from uuid import UUID


POSTGRES_BIGINT_MAX: Final = 2**63 - 1

_ACTOR_MAX_BYTES: Final = 128
_REASON_MAX_BYTES: Final = 512
_IDEMPOTENCY_KEY_MAX_BYTES: Final = 4096
_CANONICAL_SCHEMA_VERSION: Final = 1
_COMMAND_NAME: Final = "inbox.requeue"
_CANONICAL_PAYLOAD_DOMAIN: Final = b"ai-exchange-inbox-requeue-command-v1\x00"
_TRANSACTION_ID_PATTERN: Final = re.compile(r"[1-9][0-9]{0,19}\Z", re.ASCII)
_RECEIPT_COLUMNS: Final = (
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
_REQUEUE_SQL: Final = (
    "SELECT command_receipt_id, inbox_id, email_id, "
    "previous_execution_epoch, execution_epoch, email_version, status, "
    "transaction_id, replayed, created_at "
    "FROM public.greenfield_requeue_inbox(%s, %s, %s, %s, %s, %s, %s, %s)"
)


def _require_bigint(
    name: str,
    value: object,
    *,
    minimum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= POSTGRES_BIGINT_MAX:
        raise ValueError(
            f"{name} must be an exact PostgreSQL BIGINT between "
            f"{minimum} and {POSTGRES_BIGINT_MAX}"
        )
    return value


def _require_uuid4(name: str, value: object) -> str:
    if type(value) is not str:
        raise ValueError(f"{name} must be a canonical UUIDv4 string")
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError):
        raise ValueError(f"{name} must be a canonical UUIDv4 string") from None
    if str(parsed) != value or parsed.version != 4:
        raise ValueError(f"{name} must be a canonical UUIDv4 string")
    return value


def _database_uuid4(name: str, value: object) -> str:
    if type(value) is UUID:
        parsed = value
    elif type(value) is str:
        try:
            parsed = UUID(value)
        except (AttributeError, ValueError):
            raise ValueError(f"{name} must be a canonical UUIDv4") from None
    else:
        raise ValueError(f"{name} must be a canonical UUIDv4")
    canonical = str(parsed)
    if parsed.version != 4 or (type(value) is str and canonical != value):
        raise ValueError(f"{name} must be a canonical UUIDv4")
    return canonical


def _require_text(
    name: str,
    value: object,
    *,
    maximum_bytes: int,
) -> str:
    if type(value) is not str:
        raise ValueError(f"{name} must be exact bounded UTF-8 text")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain valid UTF-8 text") from None
    if (
        not 1 <= len(encoded) <= maximum_bytes
        or value != value.strip()
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F
            for character in value
        )
    ):
        raise ValueError(f"{name} must be exact bounded UTF-8 text")
    return value


def _require_transaction_id(value: object) -> str:
    if type(value) is not str or _TRANSACTION_ID_PATTERN.fullmatch(value) is None:
        raise ValueError("transaction_id must be canonical bounded decimal text")
    return value


def _normalize_datetime(name: str, value: object) -> datetime:
    if type(value) is not datetime:
        raise ValueError(f"{name} must be an exact timezone-aware datetime")
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError
        return value.astimezone(UTC)
    except (OverflowError, TypeError, ValueError):
        raise ValueError(f"{name} must be an exact timezone-aware datetime") from None


def _validate_command_fields(
    command: RequeueCommand,
    *,
    require_increment_capacity: bool,
) -> None:
    _require_bigint("account_id", command.account_id, minimum=1)
    _require_uuid4("inbox_id", command.inbox_id)
    execution_epoch = _require_bigint(
        "expected_execution_epoch",
        command.expected_execution_epoch,
        minimum=0,
    )
    email_version = _require_bigint(
        "expected_email_version",
        command.expected_email_version,
        minimum=0,
    )
    _require_text("actor", command.actor, maximum_bytes=_ACTOR_MAX_BYTES)
    _require_text("reason", command.reason, maximum_bytes=_REASON_MAX_BYTES)
    _require_text(
        "idempotency_key",
        command.idempotency_key,
        maximum_bytes=_IDEMPOTENCY_KEY_MAX_BYTES,
    )
    if require_increment_capacity and execution_epoch > POSTGRES_BIGINT_MAX - 2:
        raise ValueError("expected_execution_epoch has no increment capacity")
    if require_increment_capacity and email_version > POSTGRES_BIGINT_MAX - 3:
        raise ValueError(
            "expected_email_version has insufficient processing increment capacity"
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class RequeueCommand:
    """One operator-authorized, idempotent requeue request."""

    account_id: int
    inbox_id: str
    expected_execution_epoch: int
    expected_email_version: int
    actor: str
    reason: str
    idempotency_key: str

    def __post_init__(self) -> None:
        _validate_command_fields(self, require_increment_capacity=False)

    @property
    def canonical_payload_hash(self) -> str:
        return canonical_requeue_payload_hash(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class RequeueReceipt:
    """The immutable result committed by ``greenfield_requeue_inbox``."""

    command_receipt_id: str
    inbox_id: str
    email_id: str
    previous_execution_epoch: int
    execution_epoch: int
    email_version: int
    status: str = "retry_wait"
    transaction_id: str
    replayed: bool
    created_at: datetime

    def __post_init__(self) -> None:
        _require_uuid4("command_receipt_id", self.command_receipt_id)
        _require_uuid4("inbox_id", self.inbox_id)
        _require_uuid4("email_id", self.email_id)
        previous_epoch = _require_bigint(
            "previous_execution_epoch",
            self.previous_execution_epoch,
            minimum=0,
        )
        execution_epoch = _require_bigint(
            "execution_epoch",
            self.execution_epoch,
            minimum=0,
        )
        if (
            previous_epoch > POSTGRES_BIGINT_MAX - 2
            or execution_epoch != previous_epoch + 1
        ):
            raise ValueError(
                "execution_epoch must be exactly previous_execution_epoch plus one"
            )
        email_version = _require_bigint(
            "email_version",
            self.email_version,
            minimum=0,
        )
        if email_version > POSTGRES_BIGINT_MAX - 2:
            raise ValueError(
                "email_version must preserve one processing increment capacity"
            )
        if type(self.status) is not str or self.status != "retry_wait":
            raise ValueError("status must be exactly retry_wait")
        _require_transaction_id(self.transaction_id)
        if type(self.replayed) is not bool:
            raise ValueError("replayed must be an exact boolean")
        object.__setattr__(
            self,
            "created_at",
            _normalize_datetime("created_at", self.created_at),
        )


def _require_exact_command(
    value: object,
    *,
    require_increment_capacity: bool,
) -> RequeueCommand:
    if type(value) is not RequeueCommand:
        raise ValueError("command must be an exact RequeueCommand")
    _validate_command_fields(
        value,
        require_increment_capacity=require_increment_capacity,
    )
    return value


def canonical_requeue_payload_hash(command: RequeueCommand) -> str:
    """Hash the fixed requeue semantics independently from its replay key."""

    exact = _require_exact_command(command, require_increment_capacity=False)
    canonical = {
        "account_id": exact.account_id,
        "actor": exact.actor,
        "command_name": _COMMAND_NAME,
        "expected_email_version": exact.expected_email_version,
        "expected_execution_epoch": exact.expected_execution_epoch,
        "inbox_id": exact.inbox_id,
        "reason": exact.reason,
        "schema_version": _CANONICAL_SCHEMA_VERSION,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(_CANONICAL_PAYLOAD_DOMAIN + encoded).hexdigest()


def _receipt_from_row(row: object) -> RequeueReceipt:
    try:
        if type(row) is tuple:
            if len(row) != len(_RECEIPT_COLUMNS):
                raise ValueError
            values = row
        elif isinstance(row, Mapping):
            if len(row) != len(_RECEIPT_COLUMNS) or set(row) != set(_RECEIPT_COLUMNS):
                raise ValueError
            values = tuple(row[column] for column in _RECEIPT_COLUMNS)
        else:
            raise ValueError
        material = dict(zip(_RECEIPT_COLUMNS, values, strict=True))
        return RequeueReceipt(
            command_receipt_id=_database_uuid4(
                "command_receipt_id",
                material["command_receipt_id"],
            ),
            inbox_id=_database_uuid4("inbox_id", material["inbox_id"]),
            email_id=_database_uuid4("email_id", material["email_id"]),
            previous_execution_epoch=material["previous_execution_epoch"],
            execution_epoch=material["execution_epoch"],
            email_version=material["email_version"],
            status=material["status"],
            transaction_id=material["transaction_id"],
            replayed=material["replayed"],
            created_at=material["created_at"],
        )
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("requeue receipt row is invalid") from None


def _require_receipt_matches_command(
    receipt: RequeueReceipt,
    command: RequeueCommand,
) -> None:
    if (
        receipt.inbox_id != command.inbox_id
        or receipt.previous_execution_epoch != command.expected_execution_epoch
        or receipt.execution_epoch != command.expected_execution_epoch + 1
        or receipt.email_version != command.expected_email_version + 1
    ):
        raise RuntimeError("requeue receipt does not match command")


class InboxRecoveryService:
    """Call the one fixed greenfield recovery function."""

    __slots__ = ("_pool",)

    def __init__(self, pool: object) -> None:
        self._pool = pool

    async def requeue(self, command: RequeueCommand) -> RequeueReceipt:
        exact = _require_exact_command(command, require_increment_capacity=True)
        payload_hash = canonical_requeue_payload_hash(exact)
        params = (
            exact.account_id,
            exact.inbox_id,
            exact.expected_execution_epoch,
            exact.expected_email_version,
            exact.actor,
            exact.reason,
            exact.idempotency_key,
            payload_hash,
        )
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(_REQUEUE_SQL, params)
                receipt = _receipt_from_row(await cursor.fetchone())
                _require_receipt_matches_command(receipt, exact)
        return receipt
