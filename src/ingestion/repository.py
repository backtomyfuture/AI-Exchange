"""Durable Inbox repository with fenced leases and bounded recovery."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from itertools import islice
from typing import Any, Final
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

from src.domain.email_state import PipelineGenerationState
from src.domain.errors import (
    DatabaseOperationError,
    ErrorKind,
    ManualReviewRequired,
    StaleFence,
)
from src.ingestion.models import (
    InboxDisposition,
    InboxDispositionStatus,
    InboxLease,
    InboxStats,
    IngressReceipt,
    NormalizedIngressEvent,
    POSTGRES_BIGINT_MAX,
    ProcessingPolicy,
)
from src.ingestion.ownership import ownership_advisory_lock_key


_DATABASE_EXCEPTIONS = (psycopg.Error, PoolTimeout)
_LOCK_TIMEOUT: Final = "5000ms"
_STATEMENT_TIMEOUT: Final = "15000ms"
_IDLE_TRANSACTION_TIMEOUT: Final = "15000ms"
_MAX_BATCH: Final = 500
_MAX_PIPELINES: Final = 64
_MAX_LEASE_SECONDS: Final = 3600
_MAX_RETRIES: Final = 5
_MAX_BACKOFF_SECONDS: Final = 900
_AUDIT_ACTOR: Final = "inbox_repository"

_CLAIMABLE_POLICIES = (
    ProcessingPolicy.FULL.value,
    ProcessingPolicy.ARCHIVE.value,
    ProcessingPolicy.METADATA_ONLY.value,
)
_SUPPRESSED_POLICIES = frozenset(
    {
        ProcessingPolicy.IGNORED,
        ProcessingPolicy.HISTORICAL_SUPPRESSED,
    }
)
_LEASE_OWNERSHIP_STATES = (
    PipelineGenerationState.CURRENT_INGRESS.value,
    PipelineGenerationState.QUIESCING.value,
    PipelineGenerationState.DRAINING.value,
)
_CLAIM_OWNERSHIP_STATES = (
    PipelineGenerationState.CURRENT_INGRESS.value,
    PipelineGenerationState.DRAINING.value,
)

_LEASE_COLUMNS = (
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
_LEASE_RETURNING = sql.SQL(", ").join(
    sql.SQL("e.{} ").format(sql.Identifier(column)) for column in _LEASE_COLUMNS
)


@dataclass(frozen=True, slots=True)
class _FailureDecision:
    status: InboxDispositionStatus
    safe_code: str
    safe_summary: str


class _AuditInvariantError(DatabaseOperationError):
    """Private marker for an append-and-compare audit collision."""

    def __init__(self) -> None:
        super().__init__(
            operation="event_inbox_invariant",
            retryable=False,
            message="event inbox audit invariant failed",
        )


_FAILURE_DECISIONS: Final[dict[ErrorKind, _FailureDecision]] = {
    ErrorKind.TRANSIENT_DEPENDENCY: _FailureDecision(
        InboxDispositionStatus.RETRY_WAIT,
        "inbox.transient_dependency",
        "Transient dependency failure",
    ),
    ErrorKind.RATE_LIMITED: _FailureDecision(
        InboxDispositionStatus.RETRY_WAIT,
        "inbox.rate_limited",
        "Dependency rate limit reached",
    ),
    ErrorKind.AUTHENTICATION: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.authentication_failure",
        "Dependency authentication failed",
    ),
    ErrorKind.VALIDATION: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.validation_failure",
        "Inbox event validation failed",
    ),
    ErrorKind.PERMANENT_DEPENDENCY: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.permanent_dependency_failure",
        "Permanent dependency failure",
    ),
    ErrorKind.POLICY_REJECTED: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.policy_rejected",
        "Inbox processing policy rejected",
    ),
    ErrorKind.INTERNAL_INVARIANT: _FailureDecision(
        InboxDispositionStatus.DEAD_LETTER,
        "inbox.internal_invariant",
        "Inbox processing invariant failed",
    ),
    ErrorKind.SEND_UNKNOWN: _FailureDecision(
        InboxDispositionStatus.MANUAL_REVIEW,
        "inbox.send_unknown",
        "Remote send outcome requires review",
    ),
}
_EFFECT_UNKNOWN = _FailureDecision(
    InboxDispositionStatus.MANUAL_REVIEW,
    "inbox.effect_outcome_unknown",
    "Remote effect outcome requires review",
)
_MANUAL_REVIEW = _FailureDecision(
    InboxDispositionStatus.MANUAL_REVIEW,
    "inbox.manual_review_required",
    "Inbox event requires manual review",
)
_DATABASE_TRANSIENT = _FailureDecision(
    InboxDispositionStatus.RETRY_WAIT,
    "inbox.database_transient",
    "Transient database operation failure",
)
_DATABASE_FAILURE = _FailureDecision(
    InboxDispositionStatus.DEAD_LETTER,
    "inbox.database_failure",
    "Database operation failed",
)
_UNKNOWN_FAILURE = _FAILURE_DECISIONS[ErrorKind.INTERNAL_INVARIANT]
_LEASE_EXPIRED = _FailureDecision(
    InboxDispositionStatus.RETRY_WAIT,
    "inbox.lease_expired",
    "Inbox worker lease expired",
)
_STALE_OWNERSHIP = _FailureDecision(
    InboxDispositionStatus.MANUAL_REVIEW,
    "inbox.stale_ownership",
    "Inbox ownership requires review",
)


def _require_bigint(name: str, value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > POSTGRES_BIGINT_MAX
    ):
        raise ValueError(f"{name} must be a positive PostgreSQL BIGINT")
    return value


def _require_bounded_int(name: str, value: object, *, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > maximum
    ):
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def _require_exact_text(name: str, value: object, *, max_length: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > max_length
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise ValueError(f"{name} must be an exact bounded string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain valid UTF-8 text") from None
    return value


def _require_pipeline_names(values: object) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise ValueError("pipeline_names must be a bounded collection")
    try:
        candidates = tuple(islice(iter(values), _MAX_PIPELINES + 1))  # type: ignore[arg-type]
    except TypeError:
        raise ValueError("pipeline_names must be a bounded collection") from None
    if not candidates or len(candidates) > _MAX_PIPELINES:
        raise ValueError("pipeline_names must contain between 1 and 64 names")
    normalized = tuple(
        _require_exact_text("pipeline_name", value, max_length=64)
        for value in candidates
    )
    if len(set(normalized)) != len(normalized):
        normalized = tuple(dict.fromkeys(normalized))
    return tuple(sorted(normalized))


def _database_error(operation: str, error: Exception) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation=operation,
        retryable=isinstance(error, (psycopg.OperationalError, PoolTimeout)),
        message="event inbox database operation failed",
    )


def _invariant_error(message: str) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation="event_inbox_invariant",
        retryable=False,
        message=message,
    )


def _row_values(row: object, columns: tuple[str, ...]) -> tuple[object, ...]:
    try:
        if isinstance(row, Mapping):
            return tuple(row[column] for column in columns)
        if isinstance(row, (tuple, list)) and len(row) >= len(columns):
            return tuple(row[index] for index in range(len(columns)))
    except (KeyError, IndexError, TypeError):
        pass
    raise _invariant_error("event inbox database row is invalid")


def _lease_from_row(row: object) -> InboxLease:
    try:
        values = dict(
            zip(_LEASE_COLUMNS, _row_values(row, _LEASE_COLUMNS), strict=True)
        )
        payload = values["payload"]
        if not isinstance(payload, Mapping):
            raise ValueError
        event = NormalizedIngressEvent(
            account_id=values["account_id"],
            source=values["source"],
            raw_event_type=values["raw_event_type"],
            kind=values["change_kind"],
            external_email_id=values["external_email_id"],
            folder=values["folder_key"],
            source_version=values["source_version"],
            dedupe_key=str(values["dedupe_key"]),
            payload=payload,
            processing_policy=values["processing_policy"],
            source_event_at=values["source_event_at"],
        )
        return InboxLease(
            id=str(values["id"]),
            account_id=values["account_id"],
            pipeline_name=values["pipeline_name"],
            generation=values["generation"],
            fencing_token=values["fencing_token"],
            lease_owner=values["lease_owner"],
            attempts=values["attempts"],
            event=event,
            received_at=values["received_at"],
            lease_until=values["lease_until"],
        )
    except DatabaseOperationError:
        raise
    except (KeyError, TypeError, ValueError):
        raise _invariant_error("event inbox database row is invalid") from None


def _audit_event_key(inbox_id: str, action: str, attempts: int) -> str:
    payload = f"event_inbox\x00{inbox_id}\x00{action}\x00{attempts}".encode()
    return hashlib.sha256(payload).hexdigest()


def _object_fingerprint(inbox_id: str) -> str:
    return hashlib.sha256(inbox_id.encode()).hexdigest()


def _next_attempts(attempts: int) -> int:
    return min(attempts + 1, POSTGRES_BIGINT_MAX)


def _failure_decision(error: BaseException) -> _FailureDecision:
    if isinstance(error, ManualReviewRequired):
        return _MANUAL_REVIEW
    if isinstance(error, DatabaseOperationError):
        return _DATABASE_TRANSIENT if error.retryable else _DATABASE_FAILURE
    try:
        kind = getattr(error, "kind", None)
    except Exception:
        return _UNKNOWN_FAILURE
    if isinstance(kind, ErrorKind):
        return _FAILURE_DECISIONS[kind]
    return _UNKNOWN_FAILURE


def _terminal_action(decision: _FailureDecision) -> str:
    if decision.safe_code == _EFFECT_UNKNOWN.safe_code:
        return "ingress.effect_unknown"
    if decision.status is InboxDispositionStatus.MANUAL_REVIEW:
        return "ingress.manual_review"
    return "ingress.dead_letter"


class InboxRepository:
    """The sole Phase-2 production mutation boundary for ``event_inbox``."""

    def __init__(self, pool: Any, *, target_schema: str = "public") -> None:
        self._pool = pool
        self._schema = _require_exact_text(
            "target_schema",
            target_schema,
            max_length=63,
        )

    def _table(self, name: str) -> sql.Identifier:
        return sql.Identifier(self._schema, name)

    async def _configure_transaction(
        self,
        connection: psycopg.AsyncConnection[Any],
    ) -> None:
        await connection.execute("SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
        await connection.execute(
            "SELECT "
            "pg_catalog.set_config('lock_timeout', %s, true), "
            "pg_catalog.set_config('statement_timeout', %s, true), "
            "pg_catalog.set_config("
            "'idle_in_transaction_session_timeout', %s, true)",
            (_LOCK_TIMEOUT, _STATEMENT_TIMEOUT, _IDLE_TRANSACTION_TIMEOUT),
        )

    async def _acquire_account_lock(
        self,
        connection: psycopg.AsyncConnection[Any],
        account_id: int,
    ) -> None:
        await connection.execute(
            "SELECT pg_catalog.pg_advisory_xact_lock_shared(%s)",
            (ownership_advisory_lock_key(account_id),),
        )

    async def _append_audit(
        self,
        connection: psycopg.AsyncConnection[Any],
        *,
        inbox_id: str,
        account_id: int,
        action: str,
        result: str,
        reason: str,
        attempts: int,
        safe_metadata: Mapping[str, object],
    ) -> None:
        event_key = _audit_event_key(inbox_id, action, attempts)
        fingerprint = _object_fingerprint(inbox_id)
        metadata = dict(safe_metadata)
        insert = sql.SQL(
            "INSERT INTO {} ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata"
            ") VALUES (%s, %s, %s, NULL, 'event_inbox', %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (event_key) DO NOTHING"
        ).format(self._table("audit_events"))
        await connection.execute(
            insert,
            (
                str(uuid4()),
                event_key,
                account_id,
                fingerprint,
                action,
                result,
                _AUDIT_ACTOR,
                reason,
                Jsonb(metadata),
            ),
        )
        select = sql.SQL(
            "SELECT account_id, email_id, object_type, object_fingerprint, "
            "action, result, actor, reason, safe_metadata "
            "FROM {} WHERE event_key = %s"
        ).format(self._table("audit_events"))
        cursor = await connection.execute(select, (event_key,))
        row = await cursor.fetchone()
        expected = (
            account_id,
            None,
            "event_inbox",
            fingerprint,
            action,
            result,
            _AUDIT_ACTOR,
            reason,
            metadata,
        )
        if (
            row is None
            or _row_values(
                row,
                (
                    "account_id",
                    "email_id",
                    "object_type",
                    "object_fingerprint",
                    "action",
                    "result",
                    "actor",
                    "reason",
                    "safe_metadata",
                ),
            )
            != expected
        ):
            raise _AuditInvariantError()

    async def _duplicate_receipt(
        self,
        connection: psycopg.AsyncConnection[Any],
        event: NormalizedIngressEvent,
    ) -> IngressReceipt | None:
        query = sql.SQL(
            "SELECT id, account_id, source, raw_event_type, change_kind, "
            "external_email_id, folder_key, source_version, source_event_at "
            "FROM {} WHERE dedupe_key = %s"
        ).format(self._table("event_inbox"))
        cursor = await connection.execute(query, (event.dedupe_key,))
        row = await cursor.fetchone()
        if row is None:
            return None
        values = _row_values(
            row,
            (
                "id",
                "account_id",
                "source",
                "raw_event_type",
                "change_kind",
                "external_email_id",
                "folder_key",
                "source_version",
                "source_event_at",
            ),
        )
        persisted_identity = values[1:8]
        expected_identity = (
            event.account_id,
            event.source.value,
            event.raw_event_type,
            event.kind.value,
            event.external_email_id,
            event.folder,
            event.source_version,
        )
        if persisted_identity != expected_identity:
            raise DatabaseOperationError(
                operation="insert_event_inbox",
                retryable=False,
                message="event inbox dedupe identity conflict",
            )
        if event.source_version is None and values[8] != event.source_event_at:
            raise DatabaseOperationError(
                operation="insert_event_inbox",
                retryable=False,
                message="event inbox dedupe identity conflict",
            )
        return IngressReceipt(inbox_id=str(values[0]), duplicate=True)

    async def insert(
        self,
        event: NormalizedIngressEvent,
        generation: int,
        fencing_token: int,
    ) -> IngressReceipt:
        if not isinstance(event, NormalizedIngressEvent):
            raise ValueError("event must be a NormalizedIngressEvent")
        generation = _require_bigint("generation", generation)
        fencing_token = _require_bigint("fencing_token", fencing_token)
        payload = event.payload_for_storage()
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, event.account_id)
                    duplicate = await self._duplicate_receipt(connection, event)
                    if duplicate is not None:
                        return duplicate

                    ownership_query = sql.SQL(
                        "SELECT pipeline_name FROM {} "
                        "WHERE account_id = %s AND generation = %s "
                        "AND fencing_token = %s AND state = 'current_ingress' "
                        "FOR KEY SHARE"
                    ).format(self._table("pipeline_ownership"))
                    ownership_cursor = await connection.execute(
                        ownership_query,
                        (event.account_id, generation, fencing_token),
                    )
                    ownership_row = await ownership_cursor.fetchone()
                    if ownership_row is None:
                        raise StaleFence()
                    pipeline_name = _row_values(
                        ownership_row,
                        ("pipeline_name",),
                    )[0]
                    if not isinstance(pipeline_name, str):
                        raise _invariant_error("event inbox ownership row is invalid")

                    suppressed = event.processing_policy in _SUPPRESSED_POLICIES
                    status = "completed" if suppressed else "pending"
                    inbox_id = str(uuid4())
                    insert = sql.SQL(
                        "INSERT INTO {} ("
                        "id, account_id, external_email_id, folder_key, source, "
                        "raw_event_type, change_kind, dedupe_key, source_version, "
                        "source_event_at, payload, processing_policy, pipeline_name, "
                        "generation, fencing_token, status, available_at"
                        ") VALUES ("
                        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                        "%s, %s, %s, pg_catalog.clock_timestamp()"
                        ") ON CONFLICT (dedupe_key) DO NOTHING RETURNING id"
                    ).format(self._table("event_inbox"))
                    inserted_cursor = await connection.execute(
                        insert,
                        (
                            inbox_id,
                            event.account_id,
                            event.external_email_id,
                            event.folder,
                            event.source.value,
                            event.raw_event_type,
                            event.kind.value,
                            event.dedupe_key,
                            event.source_version,
                            event.source_event_at,
                            Jsonb(payload),
                            event.processing_policy.value,
                            pipeline_name,
                            generation,
                            fencing_token,
                            status,
                        ),
                    )
                    inserted_row = await inserted_cursor.fetchone()
                    if inserted_row is None:
                        winner = await self._duplicate_receipt(connection, event)
                        if winner is None:
                            raise _invariant_error(
                                "event inbox dedupe winner is unavailable"
                            )
                        return winner
                    inserted_id = str(_row_values(inserted_row, ("id",))[0])
                    if inserted_id != inbox_id:
                        raise _invariant_error("event inbox insert row is invalid")
                    if suppressed:
                        await self._append_audit(
                            connection,
                            inbox_id=inbox_id,
                            account_id=event.account_id,
                            action="ingress.policy_suppressed",
                            result=status,
                            reason="inbox.policy_suppressed",
                            attempts=0,
                            safe_metadata={
                                "attempts": 0,
                                "processing_policy": event.processing_policy.value,
                                "status": status,
                            },
                        )
                    return IngressReceipt(inbox_id=inbox_id, duplicate=False)
        except (StaleFence, DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_event_inbox", error) from None

    async def claim_batch(
        self,
        worker_id: str,
        pipeline_names: Iterable[str],
        limit: int,
        lease_seconds: int,
    ) -> list[InboxLease]:
        worker_id = _require_exact_text("worker_id", worker_id, max_length=128)
        pipelines = _require_pipeline_names(pipeline_names)
        limit = _require_bounded_int("limit", limit, maximum=_MAX_BATCH)
        lease_seconds = _require_bounded_int(
            "lease_seconds",
            lease_seconds,
            maximum=_MAX_LEASE_SECONDS,
        )
        candidate_window = min(_MAX_BATCH, max(limit, limit * 4))
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    candidates = sql.SQL(
                        "SELECT candidate.account_id FROM ("
                        "SELECT e.account_id, e.received_at, e.id "
                        "FROM {} AS e JOIN {} AS p "
                        "ON p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "WHERE e.status IN ('pending', 'retry_wait') "
                        "AND e.available_at <= pg_catalog.statement_timestamp() "
                        "AND e.processing_policy = ANY(%s::pg_catalog.text[]) "
                        "AND e.pipeline_name = ANY(%s::pg_catalog.text[]) "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "ORDER BY e.received_at, e.id LIMIT %s"
                        ") AS candidate GROUP BY candidate.account_id "
                        "ORDER BY candidate.account_id"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    candidate_cursor = await connection.execute(
                        candidates,
                        (
                            list(_CLAIMABLE_POLICIES),
                            list(pipelines),
                            list(_CLAIM_OWNERSHIP_STATES),
                            candidate_window,
                        ),
                    )
                    candidate_rows = await candidate_cursor.fetchall()
                    account_ids: list[int] = []
                    for row in candidate_rows:
                        value = _row_values(row, ("account_id",))[0]
                        account_ids.append(_require_bigint("account_id", value))
                    for account_id in sorted(set(account_ids)):
                        await self._acquire_account_lock(connection, account_id)
                    if not account_ids:
                        return []

                    claim = sql.SQL(
                        "WITH claimable AS ("
                        "SELECT e.id FROM {} AS e JOIN {} AS p "
                        "ON p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "WHERE e.status IN ('pending', 'retry_wait') "
                        "AND e.available_at <= pg_catalog.statement_timestamp() "
                        "AND e.processing_policy = ANY(%s::pg_catalog.text[]) "
                        "AND e.pipeline_name = ANY(%s::pg_catalog.text[]) "
                        "AND e.account_id = ANY(%s::pg_catalog.int8[]) "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "ORDER BY e.received_at, e.id "
                        "FOR UPDATE OF e SKIP LOCKED LIMIT %s"
                        ") UPDATE {} AS e SET "
                        "status = 'leased', lease_owner = %s, "
                        "lease_until = pg_catalog.statement_timestamp() "
                        "+ pg_catalog.make_interval(secs => %s), "
                        "processing_started_at = COALESCE("
                        "e.processing_started_at, pg_catalog.statement_timestamp()), "
                        "safe_error_code = NULL, safe_error_summary = NULL, "
                        "updated_at = pg_catalog.statement_timestamp() "
                        "FROM claimable AS c WHERE e.id = c.id RETURNING {}"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                        self._table("event_inbox"),
                        _LEASE_RETURNING,
                    )
                    claimed_cursor = await connection.execute(
                        claim,
                        (
                            list(_CLAIMABLE_POLICIES),
                            list(pipelines),
                            sorted(set(account_ids)),
                            list(_CLAIM_OWNERSHIP_STATES),
                            limit,
                            worker_id,
                            lease_seconds,
                        ),
                    )
                    leases = [
                        _lease_from_row(row) for row in await claimed_cursor.fetchall()
                    ]
                    leases.sort(key=lambda item: (item.received_at, item.id))
                    return leases
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("claim_event_inbox", error) from None

    async def renew(
        self,
        lease: InboxLease,
        lease_seconds: int,
    ) -> InboxLease | None:
        lease = self._require_lease(lease)
        lease_seconds = _require_bounded_int(
            "lease_seconds",
            lease_seconds,
            maximum=_MAX_LEASE_SECONDS,
        )
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    query = sql.SQL(
                        "UPDATE {} AS e SET lease_until = GREATEST("
                        "e.lease_until + INTERVAL '1 microsecond', "
                        "pg_catalog.clock_timestamp() "
                        "+ pg_catalog.make_interval(secs => %s)), "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) RETURNING {}"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                        _LEASE_RETURNING,
                    )
                    cursor = await connection.execute(
                        query,
                        (
                            lease_seconds,
                            lease.id,
                            lease.account_id,
                            lease.pipeline_name,
                            lease.generation,
                            lease.fencing_token,
                            lease.lease_owner,
                            lease.attempts,
                            lease.lease_until,
                            list(_LEASE_OWNERSHIP_STATES),
                        ),
                    )
                    row = await cursor.fetchone()
                    return _lease_from_row(row) if row is not None else None
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("renew_event_inbox_lease", error) from None

    async def begin_effect(self, lease: InboxLease) -> bool:
        lease = self._require_lease(lease)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    query = sql.SQL(
                        "UPDATE {} AS e SET effect_started_at = COALESCE("
                        "e.effect_started_at, pg_catalog.clock_timestamp()), "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) RETURNING e.id"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    cursor = await connection.execute(
                        query,
                        (*self._lease_params(lease), list(_LEASE_OWNERSHIP_STATES)),
                    )
                    return await cursor.fetchone() is not None
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("begin_event_inbox_effect", error) from None

    async def complete(self, lease: InboxLease) -> bool:
        lease = self._require_lease(lease)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    query = sql.SQL(
                        "UPDATE {} AS e SET status = 'completed', "
                        "lease_owner = NULL, lease_until = NULL, "
                        "safe_error_code = NULL, safe_error_summary = NULL, "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "RETURNING e.account_id, e.attempts"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    cursor = await connection.execute(
                        query,
                        (*self._lease_params(lease), list(_LEASE_OWNERSHIP_STATES)),
                    )
                    row = await cursor.fetchone()
                    if row is None:
                        return False
                    account_id, attempts = _row_values(
                        row,
                        ("account_id", "attempts"),
                    )
                    await self._append_audit(
                        connection,
                        inbox_id=lease.id,
                        account_id=account_id,  # type: ignore[arg-type]
                        action="ingress.completed",
                        result="completed",
                        reason="inbox.completed",
                        attempts=attempts,  # type: ignore[arg-type]
                        safe_metadata={
                            "attempts": attempts,
                            "status": "completed",
                        },
                    )
                    return True
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("complete_event_inbox", error) from None

    async def fail(
        self,
        lease: InboxLease,
        error: BaseException,
    ) -> InboxDisposition:
        if isinstance(error, asyncio.CancelledError):
            raise error
        if not isinstance(error, BaseException):
            raise ValueError("error must be an exception")
        if not isinstance(error, Exception):
            raise error
        lease = self._require_lease(lease)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, lease.account_id)
                    select = sql.SQL(
                        "SELECT e.effect_started_at, e.attempts "
                        "FROM {} AS e JOIN {} AS p "
                        "ON p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "FOR UPDATE OF e"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    selected_cursor = await connection.execute(
                        select,
                        (*self._lease_params(lease), list(_LEASE_OWNERSHIP_STATES)),
                    )
                    selected = await selected_cursor.fetchone()
                    if selected is None:
                        raise StaleFence()
                    effect_started_at, attempts = _row_values(
                        selected,
                        ("effect_started_at", "attempts"),
                    )
                    if not isinstance(attempts, int) or isinstance(attempts, bool):
                        raise _invariant_error("event inbox attempts are invalid")
                    new_attempts = _next_attempts(attempts)
                    decision = (
                        _EFFECT_UNKNOWN
                        if effect_started_at is not None
                        else _failure_decision(error)
                    )
                    if (
                        decision.status is InboxDispositionStatus.RETRY_WAIT
                        and new_attempts > _MAX_RETRIES
                    ):
                        decision = _FailureDecision(
                            InboxDispositionStatus.DEAD_LETTER,
                            decision.safe_code,
                            decision.safe_summary,
                        )
                    backoff = (
                        min(5 * (2**attempts), _MAX_BACKOFF_SECONDS)
                        if decision.status is InboxDispositionStatus.RETRY_WAIT
                        else 0
                    )
                    update = sql.SQL(
                        "UPDATE {} AS e SET status = %s, lease_owner = NULL, "
                        "lease_until = NULL, attempts = %s, "
                        "available_at = CASE WHEN %s = 'retry_wait' "
                        "THEN pg_catalog.clock_timestamp() "
                        "+ pg_catalog.make_interval(secs => %s) "
                        "ELSE e.available_at END, safe_error_code = %s, "
                        "safe_error_summary = %s, "
                        "updated_at = pg_catalog.clock_timestamp() "
                        "FROM {} AS p WHERE e.id = %s AND e.account_id = %s "
                        "AND e.pipeline_name = %s AND e.generation = %s "
                        "AND e.fencing_token = %s AND e.lease_owner = %s "
                        "AND e.attempts = %s AND e.lease_until = %s "
                        "AND e.effect_started_at IS NOT DISTINCT FROM %s "
                        "AND e.status = 'leased' "
                        "AND e.lease_until > pg_catalog.clock_timestamp() "
                        "AND p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "AND p.state = ANY(%s::pg_catalog.text[]) "
                        "RETURNING e.attempts, e.available_at"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    updated_cursor = await connection.execute(
                        update,
                        (
                            decision.status.value,
                            new_attempts,
                            decision.status.value,
                            backoff,
                            decision.safe_code,
                            decision.safe_summary,
                            *self._lease_params(lease),
                            effect_started_at,
                            list(_LEASE_OWNERSHIP_STATES),
                        ),
                    )
                    updated = await updated_cursor.fetchone()
                    if updated is None:
                        raise StaleFence()
                    persisted_attempts, available_at = _row_values(
                        updated,
                        ("attempts", "available_at"),
                    )
                    if decision.status is not InboxDispositionStatus.RETRY_WAIT:
                        action = _terminal_action(decision)
                        await self._append_audit(
                            connection,
                            inbox_id=lease.id,
                            account_id=lease.account_id,
                            action=action,
                            result=decision.status.value,
                            reason=decision.safe_code,
                            attempts=new_attempts,
                            safe_metadata={
                                "attempts": new_attempts,
                                "safe_error_code": decision.safe_code,
                                "status": decision.status.value,
                            },
                        )
                    return InboxDisposition(
                        status=decision.status,
                        attempts=persisted_attempts,  # type: ignore[arg-type]
                        available_at=(
                            available_at
                            if decision.status is InboxDispositionStatus.RETRY_WAIT
                            else None
                        ),
                        safe_error_code=decision.safe_code,
                    )
        except (StaleFence, DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as database_error:
            raise _database_error("fail_event_inbox", database_error) from None

    async def recover_expired_leases(self, limit: int) -> int:
        limit = _require_bounded_int("limit", limit, maximum=_MAX_BATCH)
        audit_error: _AuditInvariantError | None = None
        recovered = 0
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    candidate_accounts = sql.SQL(
                        "SELECT candidate.account_id FROM ("
                        "SELECT e.account_id, e.lease_until, e.id FROM {} AS e "
                        "WHERE e.status = 'leased' "
                        "AND e.lease_until <= pg_catalog.clock_timestamp() "
                        "ORDER BY e.lease_until, e.id LIMIT %s"
                        ") AS candidate GROUP BY candidate.account_id "
                        "ORDER BY candidate.account_id"
                    ).format(self._table("event_inbox"))
                    candidate_cursor = await connection.execute(
                        candidate_accounts,
                        (_MAX_BATCH,),
                    )
                    account_ids: list[int] = []
                    for row in await candidate_cursor.fetchall():
                        value = _row_values(row, ("account_id",))[0]
                        account_ids.append(_require_bigint("account_id", value))
                    locked_accounts = sorted(set(account_ids))
                    for account_id in locked_accounts:
                        await self._acquire_account_lock(connection, account_id)
                    if not locked_accounts:
                        return 0

                    select = sql.SQL(
                        "SELECT e.id, e.account_id, e.lease_owner, e.lease_until, "
                        "e.attempts, e.effect_started_at, p.state "
                        "FROM {} AS e JOIN {} AS p "
                        "ON p.account_id = e.account_id "
                        "AND p.generation = e.generation "
                        "AND p.fencing_token = e.fencing_token "
                        "AND p.pipeline_name = e.pipeline_name "
                        "WHERE e.status = 'leased' "
                        "AND e.lease_until <= pg_catalog.clock_timestamp() "
                        "AND e.account_id = ANY(%s::pg_catalog.int8[]) "
                        "ORDER BY e.lease_until, e.id "
                        "FOR UPDATE OF e SKIP LOCKED LIMIT %s"
                    ).format(
                        self._table("event_inbox"),
                        self._table("pipeline_ownership"),
                    )
                    cursor = await connection.execute(
                        select,
                        (locked_accounts, _MAX_BATCH),
                    )
                    rows = await cursor.fetchall()
                    for row in rows:
                        if recovered >= limit:
                            break
                        (
                            inbox_id,
                            account_id,
                            lease_owner,
                            lease_until,
                            attempts,
                            effect_started_at,
                            ownership_state,
                        ) = _row_values(
                            row,
                            (
                                "id",
                                "account_id",
                                "lease_owner",
                                "lease_until",
                                "attempts",
                                "effect_started_at",
                                "state",
                            ),
                        )
                        if not isinstance(attempts, int) or isinstance(attempts, bool):
                            raise _invariant_error("event inbox attempts are invalid")
                        new_attempts = _next_attempts(attempts)
                        if ownership_state not in _LEASE_OWNERSHIP_STATES:
                            decision = _STALE_OWNERSHIP
                        elif effect_started_at is not None:
                            decision = _EFFECT_UNKNOWN
                        elif new_attempts <= _MAX_RETRIES:
                            decision = _LEASE_EXPIRED
                        else:
                            decision = _FailureDecision(
                                InboxDispositionStatus.DEAD_LETTER,
                                _LEASE_EXPIRED.safe_code,
                                _LEASE_EXPIRED.safe_summary,
                            )
                        backoff = (
                            min(5 * (2**attempts), _MAX_BACKOFF_SECONDS)
                            if decision.status is InboxDispositionStatus.RETRY_WAIT
                            else 0
                        )
                        updated = False
                        try:
                            async with connection.transaction():
                                update = sql.SQL(
                                    "UPDATE {} AS e SET status = %s, "
                                    "lease_owner = NULL, lease_until = NULL, "
                                    "attempts = %s, available_at = CASE "
                                    "WHEN %s = 'retry_wait' "
                                    "THEN pg_catalog.clock_timestamp() "
                                    "+ pg_catalog.make_interval(secs => %s) "
                                    "ELSE e.available_at END, safe_error_code = %s, "
                                    "safe_error_summary = %s, "
                                    "updated_at = pg_catalog.clock_timestamp() "
                                    "WHERE e.id = %s AND e.status = 'leased' "
                                    "AND e.lease_owner = %s AND e.lease_until = %s "
                                    "AND e.attempts = %s "
                                    "AND e.effect_started_at IS NOT DISTINCT FROM %s "
                                    "AND e.lease_until <= "
                                    "pg_catalog.clock_timestamp() RETURNING e.id"
                                ).format(self._table("event_inbox"))
                                updated_cursor = await connection.execute(
                                    update,
                                    (
                                        decision.status.value,
                                        new_attempts,
                                        decision.status.value,
                                        backoff,
                                        decision.safe_code,
                                        decision.safe_summary,
                                        inbox_id,
                                        lease_owner,
                                        lease_until,
                                        attempts,
                                        effect_started_at,
                                    ),
                                )
                                updated = await updated_cursor.fetchone() is not None
                                if (
                                    updated
                                    and decision.status
                                    is not InboxDispositionStatus.RETRY_WAIT
                                ):
                                    action = _terminal_action(decision)
                                    await self._append_audit(
                                        connection,
                                        inbox_id=str(inbox_id),
                                        account_id=account_id,  # type: ignore[arg-type]
                                        action=action,
                                        result=decision.status.value,
                                        reason=decision.safe_code,
                                        attempts=new_attempts,
                                        safe_metadata={
                                            "attempts": new_attempts,
                                            "safe_error_code": decision.safe_code,
                                            "status": decision.status.value,
                                        },
                                    )
                        except _AuditInvariantError as error:
                            if audit_error is None:
                                audit_error = error
                            continue
                        if updated:
                            recovered += 1
            if audit_error is not None:
                raise audit_error
            return recovered
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("recover_event_inbox_leases", error) from None

    async def stats(self) -> InboxStats:
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    query = sql.SQL(
                        "SELECT "
                        "pg_catalog.count(*) FILTER (WHERE status = 'pending') "
                        "AS pending, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'retry_wait') "
                        "AS retry_wait, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'leased') "
                        "AS leased, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'dead_letter') "
                        "AS dead_letter, "
                        "pg_catalog.count(*) FILTER (WHERE status = 'manual_review') "
                        "AS manual_review, "
                        "COALESCE(GREATEST("
                        "EXTRACT(epoch FROM ("
                        "pg_catalog.clock_timestamp() - ("
                        "pg_catalog.min(received_at) FILTER ("
                        "WHERE status IN ('pending', 'retry_wait'))))), 0), 0) "
                        "AS oldest_pending_seconds FROM {}"
                    ).format(self._table("event_inbox"))
                    cursor = await connection.execute(query)
                    row = await cursor.fetchone()
                    values = _row_values(
                        row,
                        (
                            "pending",
                            "retry_wait",
                            "leased",
                            "dead_letter",
                            "manual_review",
                            "oldest_pending_seconds",
                        ),
                    )
                    return InboxStats(
                        pending=values[0],  # type: ignore[arg-type]
                        retry_wait=values[1],  # type: ignore[arg-type]
                        leased=values[2],  # type: ignore[arg-type]
                        dead_letter=values[3],  # type: ignore[arg-type]
                        manual_review=values[4],  # type: ignore[arg-type]
                        oldest_pending_seconds=float(values[5]),
                    )
        except (DatabaseOperationError, ValueError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("read_event_inbox_stats", error) from None

    @staticmethod
    def _require_lease(value: object) -> InboxLease:
        if not isinstance(value, InboxLease):
            raise ValueError("lease must be an InboxLease")
        return value

    @staticmethod
    def _lease_params(lease: InboxLease) -> tuple[object, ...]:
        return (
            lease.id,
            lease.account_id,
            lease.pipeline_name,
            lease.generation,
            lease.fencing_token,
            lease.lease_owner,
            lease.attempts,
            lease.lease_until,
        )


__all__ = ["InboxRepository"]
