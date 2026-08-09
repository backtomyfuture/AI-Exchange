"""Transactional pipeline ownership and fencing boundary.

Polling may bootstrap the first generation and quiesce it, but it deliberately
has no public generation-switch operation.  The private transaction-bound
primitives are reserved for the later ActivationService, which must compose
them with authority, barrier, receipt, and audit facts in one transaction.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

from src.domain.email_state import PipelineGenerationState
from src.domain.errors import DatabaseOperationError, StaleFence
from src.ingestion.models import (
    InboxLease,
    POSTGRES_BIGINT_MAX,
    PipelineGeneration,
)


_EXECUTABLE_STATES = frozenset(
    {
        PipelineGenerationState.CURRENT_INGRESS,
        PipelineGenerationState.QUIESCING,
        PipelineGenerationState.DRAINING,
    }
)
_BLOCKING_INBOX_STATES = (
    "pending",
    "retry_wait",
    "leased",
    "manual_review",
    # There is no accounted-dead-letter proof in the current schema.  Treat
    # every dead letter as blocking until a later guard can prove otherwise.
    "dead_letter",
)
_BLOCKING_EMAIL_STATES = (
    "ingested",
    "processing",
    "retry_wait",
    "manual_review",
    "waiting_approval",
    "send_queued",
    "sending",
    "accepted",
    "send_unknown",
    # Task 7 makes recovery from dead_letter an authenticated administrator
    # action.  Until a later reconciliation guard proves it accounted, it is
    # unresolved ownership work and must block retirement.
    "dead_letter",
)
# Outcome-known terminal projections such as send_failed and delivery_failed
# intentionally remain outside this set.  The later guard accounts their
# Outbox/high-water evidence without making terminal history undeletable.
_DATABASE_EXCEPTIONS = (psycopg.Error, PoolTimeout)
_LOCK_TIMEOUT = "5000ms"
_STATEMENT_TIMEOUT = "15000ms"
_IDLE_TRANSACTION_TIMEOUT = "15000ms"


class RetirementBlockCode(StrEnum):
    EVIDENCE_UNAVAILABLE = "pipeline.retirement_evidence_unavailable"
    UNRESOLVED_WORK = "pipeline.retirement_unresolved_work"


_RETIREMENT_SUMMARIES = {
    RetirementBlockCode.EVIDENCE_UNAVAILABLE: (
        "Pipeline retirement evidence is unavailable"
    ),
    RetirementBlockCode.UNRESOLVED_WORK: (
        "Pipeline retirement blocked by unresolved work"
    ),
}


class PipelineRetirementBlocked(RuntimeError):
    """Fixed safe failure when a generation cannot yet be retired."""

    def __init__(self, safe_code: RetirementBlockCode) -> None:
        if not isinstance(safe_code, RetirementBlockCode):
            raise TypeError("safe_code must be a RetirementBlockCode")
        self.safe_code = safe_code
        self.safe_summary = _RETIREMENT_SUMMARIES[safe_code]
        super().__init__(self.safe_summary)

    def __repr__(self) -> str:
        return f"PipelineRetirementBlocked(safe_code={self.safe_code.value!r})"


class RetirementGuard(Protocol):
    """Later phases supply Outbox and reconciliation evidence through this seam."""

    async def assert_ready(
        self,
        connection: psycopg.AsyncConnection[Any],
        generation: PipelineGeneration,
    ) -> None: ...


class _DenyRetirementGuard:
    async def assert_ready(
        self,
        _connection: psycopg.AsyncConnection[Any],
        _generation: PipelineGeneration,
    ) -> None:
        raise PipelineRetirementBlocked(RetirementBlockCode.EVIDENCE_UNAVAILABLE)


def _require_bigint(name: str, value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or value > POSTGRES_BIGINT_MAX
    ):
        raise ValueError(f"{name} must be a positive PostgreSQL BIGINT")
    return value


def _require_exact_text(
    name: str,
    value: object,
    *,
    max_length: int,
) -> str:
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


def _hash_parts(*parts: object) -> str:
    encoded = json.dumps(
        list(parts),
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def ownership_advisory_lock_key(account_id: int) -> int:
    """Return the stable shared/exclusive lock key for one account."""

    _require_bigint("account_id", account_id)
    digest = hashlib.sha256(
        b"pipeline-ownership\x00" + str(account_id).encode("ascii")
    ).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _generation_from_row(row: object) -> PipelineGeneration:
    try:
        if isinstance(row, Mapping):
            values = (
                row["account_id"],
                row["generation"],
                row["pipeline_name"],
                row["state"],
                row["fencing_token"],
            )
        elif isinstance(row, (tuple, list)) and len(row) >= 5:
            values = row
        else:
            raise ValueError
        return PipelineGeneration(
            account_id=values[0],
            generation=values[1],
            pipeline_name=values[2],
            state=values[3],
            fencing_token=values[4],
        )
    except (KeyError, TypeError, ValueError, IndexError):
        raise DatabaseOperationError(
            operation="read_pipeline_ownership",
            retryable=False,
            message="pipeline ownership row is invalid",
        ) from None


def _database_error(operation: str, error: Exception) -> DatabaseOperationError:
    return DatabaseOperationError(
        operation=operation,
        retryable=isinstance(error, (psycopg.OperationalError, PoolTimeout)),
        message="pipeline ownership database operation failed",
    )


def _row_value(row: object, index: int, column: str) -> object:
    if isinstance(row, Mapping):
        try:
            return row[column]
        except KeyError:
            raise ValueError("database row is missing a required column") from None
    if isinstance(row, (tuple, list)) and len(row) > index:
        return row[index]
    raise ValueError("database row has an invalid shape")


class PipelineOwnershipRepository:
    """Read/control boundary for the current ownership schema."""

    def __init__(
        self,
        pool: Any,
        *,
        retirement_guard: RetirementGuard | None = None,
        target_schema: str = "public",
    ) -> None:
        self._pool = pool
        self._retirement_guard: RetirementGuard = (
            retirement_guard or _DenyRetirementGuard()
        )
        self._schema = _require_exact_text(
            "target_schema",
            target_schema,
            max_length=63,
        )

    def _table(self, name: str) -> sql.Identifier:
        return sql.Identifier(self._schema, name)

    async def _acquire_account_lock(
        self,
        connection: psycopg.AsyncConnection[Any],
        account_id: int,
    ) -> None:
        await connection.execute(
            "SELECT pg_catalog.pg_advisory_xact_lock(%s)",
            (ownership_advisory_lock_key(account_id),),
        )

    async def _configure_transaction(
        self,
        connection: psycopg.AsyncConnection[Any],
    ) -> None:
        await connection.execute(
            "SELECT "
            "pg_catalog.set_config('lock_timeout', %s, true), "
            "pg_catalog.set_config('statement_timeout', %s, true), "
            "pg_catalog.set_config("
            "'idle_in_transaction_session_timeout', %s, true)",
            (
                _LOCK_TIMEOUT,
                _STATEMENT_TIMEOUT,
                _IDLE_TRANSACTION_TIMEOUT,
            ),
        )

    async def _fetch_exact(
        self,
        connection: psycopg.AsyncConnection[Any],
        *,
        account_id: int,
        generation: int,
        fencing_token: int | None = None,
        for_update: bool = False,
    ) -> PipelineGeneration | None:
        token_predicate = (
            sql.SQL(" AND fencing_token = %s")
            if fencing_token is not None
            else sql.SQL("")
        )
        lock_clause = sql.SQL(" FOR UPDATE") if for_update else sql.SQL("")
        query = sql.SQL(
            "SELECT account_id, generation, pipeline_name, state, fencing_token "
            "FROM {} WHERE account_id = %s AND generation = %s{}{}"
        ).format(
            self._table("pipeline_ownership"),
            token_predicate,
            lock_clause,
        )
        params: tuple[object, ...]
        if fencing_token is None:
            params = (account_id, generation)
        else:
            params = (account_id, generation, fencing_token)
        cursor = await connection.execute(query, params)
        row = await cursor.fetchone()
        return _generation_from_row(row) if row is not None else None

    async def _audit(
        self,
        connection: psycopg.AsyncConnection[Any],
        generation: PipelineGeneration,
        *,
        action: str,
        actor: str,
        reason: str,
    ) -> None:
        event_key = _hash_parts(
            "pipeline-ownership-audit-v1",
            generation.account_id,
            generation.generation,
            generation.fencing_token,
            action,
            actor,
            reason,
        )
        object_fingerprint = _hash_parts(
            "pipeline-ownership-object-v1",
            generation.account_id,
            generation.generation,
            generation.fencing_token,
            generation.pipeline_name,
        )
        query = sql.SQL(
            "INSERT INTO {} ("
            "id, event_key, account_id, email_id, object_type, "
            "object_fingerprint, action, result, actor, reason, safe_metadata"
            ") VALUES (%s, %s, %s, NULL, 'pipeline_ownership', %s, %s, "
            "'succeeded', %s, %s, %s) "
            "ON CONFLICT (event_key) DO NOTHING"
        ).format(self._table("audit_events"))
        await connection.execute(
            query,
            (
                str(uuid4()),
                event_key,
                generation.account_id,
                object_fingerprint,
                action,
                actor,
                reason,
                Jsonb(
                    {
                        "generation": generation.generation,
                        "fencing_token": generation.fencing_token,
                        "state": generation.state.value,
                    }
                ),
            ),
        )

    async def bootstrap(
        self,
        account_id: int,
        pipeline_name: str,
        *,
        actor: str = "system",
        reason: str = "initial bootstrap",
    ) -> PipelineGeneration:
        account_id = _require_bigint("account_id", account_id)
        pipeline_name = _require_exact_text(
            "pipeline_name",
            pipeline_name,
            max_length=64,
        )
        actor = _require_exact_text("actor", actor, max_length=128)
        reason = _require_exact_text("reason", reason, max_length=512)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, account_id)
                    query = sql.SQL(
                        "SELECT account_id, generation, pipeline_name, state, "
                        "fencing_token FROM {} WHERE account_id = %s "
                        "ORDER BY generation FOR UPDATE"
                    ).format(self._table("pipeline_ownership"))
                    cursor = await connection.execute(query, (account_id,))
                    rows = await cursor.fetchall()
                    if rows:
                        generations = tuple(_generation_from_row(row) for row in rows)
                        current = next(
                            (
                                generation
                                for generation in generations
                                if generation.state
                                is PipelineGenerationState.CURRENT_INGRESS
                            ),
                            None,
                        )
                        if (
                            current is not None
                            and current.pipeline_name == pipeline_name
                        ):
                            return current
                        raise StaleFence()

                    insert = sql.SQL(
                        "INSERT INTO {} ("
                        "account_id, generation, pipeline_name, state, "
                        "fencing_token, created_by, reason"
                        ") VALUES (%s, 1, %s, 'current_ingress', 1, %s, %s) "
                        "RETURNING account_id, generation, pipeline_name, state, "
                        "fencing_token"
                    ).format(self._table("pipeline_ownership"))
                    inserted = await connection.execute(
                        insert,
                        (account_id, pipeline_name, actor, reason),
                    )
                    row = await inserted.fetchone()
                    generation = _generation_from_row(row)
                    await self._audit(
                        connection,
                        generation,
                        action="pipeline.bootstrap",
                        actor=actor,
                        reason=reason,
                    )
                    return generation
        except (StaleFence, ValueError, DatabaseOperationError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("bootstrap_pipeline_ownership", error) from None

    async def get(
        self,
        account_id: int,
        generation: int,
    ) -> PipelineGeneration | None:
        account_id = _require_bigint("account_id", account_id)
        generation = _require_bigint("generation", generation)
        try:
            async with self._pool.connection() as connection:
                return await self._fetch_exact(
                    connection,
                    account_id=account_id,
                    generation=generation,
                )
        except DatabaseOperationError:
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("get_pipeline_ownership", error) from None

    async def current_ingress(
        self,
        account_id: int,
    ) -> PipelineGeneration | None:
        account_id = _require_bigint("account_id", account_id)
        try:
            async with self._pool.connection() as connection:
                query = sql.SQL(
                    "SELECT account_id, generation, pipeline_name, state, "
                    "fencing_token FROM {} WHERE account_id = %s "
                    "AND state = 'current_ingress'"
                ).format(self._table("pipeline_ownership"))
                cursor = await connection.execute(query, (account_id,))
                row = await cursor.fetchone()
                return _generation_from_row(row) if row is not None else None
        except DatabaseOperationError:
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("get_current_pipeline_ownership", error) from None

    async def next_generation(self, account_id: int) -> int:
        account_id = _require_bigint("account_id", account_id)
        try:
            async with self._pool.connection() as connection:
                query = sql.SQL(
                    "SELECT COALESCE(pg_catalog.max(generation), 0) "
                    "AS maximum_generation FROM {} WHERE account_id = %s"
                ).format(self._table("pipeline_ownership"))
                cursor = await connection.execute(query, (account_id,))
                row = await cursor.fetchone()
                try:
                    maximum = (
                        _row_value(row, 0, "maximum_generation")
                        if row is not None
                        else None
                    )
                except ValueError:
                    maximum = None
                if (
                    isinstance(maximum, bool)
                    or not isinstance(maximum, int)
                    or maximum < 0
                    or maximum >= POSTGRES_BIGINT_MAX
                ):
                    raise DatabaseOperationError(
                        operation="next_pipeline_generation",
                        retryable=False,
                        message="pipeline generation is unavailable",
                    )
                return maximum + 1
        except DatabaseOperationError:
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("next_pipeline_generation", error) from None

    async def assert_fence(
        self,
        account_id: int,
        generation: int,
        fencing_token: int,
    ) -> PipelineGeneration:
        """Check a stamp for diagnostics or continuation of an existing lease.

        Claim, effect-start, and completion code must still repeat this fence
        predicate in its own locked transaction/CAS; this standalone read is
        not authorization for a new claim or a remote side effect.
        """

        account_id = _require_bigint("account_id", account_id)
        generation = _require_bigint("generation", generation)
        fencing_token = _require_bigint("fencing_token", fencing_token)
        try:
            async with self._pool.connection() as connection:
                current = await self._fetch_exact(
                    connection,
                    account_id=account_id,
                    generation=generation,
                    fencing_token=fencing_token,
                )
                if current is None or current.state not in _EXECUTABLE_STATES:
                    raise StaleFence()
                return current
        except (StaleFence, DatabaseOperationError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("assert_pipeline_fence", error) from None

    async def can_execute(self, lease: InboxLease) -> bool:
        if not isinstance(lease, InboxLease):
            raise ValueError("lease must be an InboxLease")
        try:
            await self.assert_fence(
                lease.account_id,
                lease.generation,
                lease.fencing_token,
            )
        except StaleFence:
            return False
        return True

    async def quiesce(
        self,
        account_id: int,
        expected_generation: int,
        expected_fencing_token: int,
        actor: str,
        reason: str,
    ) -> PipelineGeneration:
        account_id = _require_bigint("account_id", account_id)
        expected_generation = _require_bigint(
            "expected_generation",
            expected_generation,
        )
        expected_fencing_token = _require_bigint(
            "expected_fencing_token",
            expected_fencing_token,
        )
        actor = _require_exact_text("actor", actor, max_length=128)
        reason = _require_exact_text("reason", reason, max_length=512)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, account_id)
                    current_query = sql.SQL(
                        "SELECT account_id, generation, pipeline_name, state, "
                        "fencing_token FROM {} WHERE account_id = %s "
                        "AND state = 'current_ingress' FOR UPDATE"
                    ).format(self._table("pipeline_ownership"))
                    current_cursor = await connection.execute(
                        current_query,
                        (account_id,),
                    )
                    current_row = await current_cursor.fetchone()
                    if current_row is not None:
                        transitioned = True
                        current = _generation_from_row(current_row)
                        if (
                            current.generation != expected_generation
                            or current.fencing_token != expected_fencing_token
                        ):
                            raise StaleFence()
                        update = sql.SQL(
                            "UPDATE {} SET state = 'quiescing', reason = %s, "
                            "updated_at = pg_catalog.statement_timestamp() "
                            "WHERE account_id = %s AND generation = %s "
                            "AND fencing_token = %s AND state = 'current_ingress' "
                            "RETURNING account_id, generation, pipeline_name, "
                            "state, fencing_token"
                        ).format(self._table("pipeline_ownership"))
                        updated_cursor = await connection.execute(
                            update,
                            (
                                reason,
                                account_id,
                                expected_generation,
                                expected_fencing_token,
                            ),
                        )
                        updated_row = await updated_cursor.fetchone()
                        if updated_row is None:
                            raise StaleFence()
                        quiesced = _generation_from_row(updated_row)
                    else:
                        transitioned = False
                        quiesced = await self._fetch_exact(
                            connection,
                            account_id=account_id,
                            generation=expected_generation,
                            fencing_token=expected_fencing_token,
                            for_update=True,
                        )
                        if (
                            quiesced is None
                            or quiesced.state is not PipelineGenerationState.QUIESCING
                        ):
                            raise StaleFence()
                    if transitioned:
                        await self._audit(
                            connection,
                            quiesced,
                            action="pipeline.quiesce",
                            actor=actor,
                            reason=reason,
                        )
                    return quiesced
        except (StaleFence, ValueError, DatabaseOperationError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("quiesce_pipeline_ownership", error) from None

    async def retire(
        self,
        account_id: int,
        expected_generation: int,
        expected_fencing_token: int,
        actor: str,
        reason: str,
    ) -> PipelineGeneration:
        account_id = _require_bigint("account_id", account_id)
        expected_generation = _require_bigint(
            "expected_generation",
            expected_generation,
        )
        expected_fencing_token = _require_bigint(
            "expected_fencing_token",
            expected_fencing_token,
        )
        actor = _require_exact_text("actor", actor, max_length=128)
        reason = _require_exact_text("reason", reason, max_length=512)
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await self._configure_transaction(connection)
                    await self._acquire_account_lock(connection, account_id)
                    generation = await self._fetch_exact(
                        connection,
                        account_id=account_id,
                        generation=expected_generation,
                        fencing_token=expected_fencing_token,
                        for_update=True,
                    )
                    if (
                        generation is None
                        or generation.state is not PipelineGenerationState.DRAINING
                    ):
                        raise StaleFence()

                    inbox_query = sql.SQL(
                        "SELECT id FROM {} WHERE account_id = %s "
                        "AND generation = %s AND fencing_token = %s "
                        "AND status = ANY(%s) LIMIT 1 FOR UPDATE"
                    ).format(self._table("event_inbox"))
                    inbox_cursor = await connection.execute(
                        inbox_query,
                        (
                            account_id,
                            expected_generation,
                            expected_fencing_token,
                            list(_BLOCKING_INBOX_STATES),
                        ),
                    )
                    if await inbox_cursor.fetchone() is not None:
                        raise PipelineRetirementBlocked(
                            RetirementBlockCode.UNRESOLVED_WORK
                        )

                    email_query = sql.SQL(
                        "SELECT id FROM {} WHERE account_id = %s "
                        "AND owner_generation = %s "
                        "AND owner_fencing_token = %s "
                        "AND status = ANY(%s) LIMIT 1 FOR UPDATE"
                    ).format(self._table("emails"))
                    email_cursor = await connection.execute(
                        email_query,
                        (
                            account_id,
                            expected_generation,
                            expected_fencing_token,
                            list(_BLOCKING_EMAIL_STATES),
                        ),
                    )
                    if await email_cursor.fetchone() is not None:
                        raise PipelineRetirementBlocked(
                            RetirementBlockCode.UNRESOLVED_WORK
                        )

                    await self._retirement_guard.assert_ready(
                        connection,
                        generation,
                    )
                    update = sql.SQL(
                        "UPDATE {} SET state = 'retired', reason = %s, "
                        "updated_at = pg_catalog.statement_timestamp() "
                        "WHERE account_id = %s AND generation = %s "
                        "AND fencing_token = %s AND state = 'draining' "
                        "RETURNING account_id, generation, pipeline_name, "
                        "state, fencing_token"
                    ).format(self._table("pipeline_ownership"))
                    updated_cursor = await connection.execute(
                        update,
                        (
                            reason,
                            account_id,
                            expected_generation,
                            expected_fencing_token,
                        ),
                    )
                    updated_row = await updated_cursor.fetchone()
                    if updated_row is None:
                        raise StaleFence()
                    retired = _generation_from_row(updated_row)
                    await self._audit(
                        connection,
                        retired,
                        action="pipeline.retire",
                        actor=actor,
                        reason=reason,
                    )
                    return retired
        except (
            StaleFence,
            ValueError,
            DatabaseOperationError,
            PipelineRetirementBlocked,
        ):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("retire_pipeline_ownership", error) from None

    def transaction(
        self,
        connection: psycopg.AsyncConnection[Any],
    ) -> PipelineOwnershipTransaction:
        return PipelineOwnershipTransaction(self, connection)


class PipelineOwnershipTransaction:
    """Private handoff primitives that never acquire or commit a connection."""

    def __init__(
        self,
        repository: PipelineOwnershipRepository,
        connection: psycopg.AsyncConnection[Any],
    ) -> None:
        self._repository = repository
        self._connection = connection
        self._transaction_id: str | None = None
        self._locked: PipelineGeneration | None = None
        self._draining: PipelineGeneration | None = None
        self._inserted = False

    def _require_transaction(self) -> None:
        if self._connection.info.transaction_status is not TransactionStatus.INTRANS:
            raise RuntimeError("pipeline ownership transaction is required")

    async def _assert_transaction_identity(self) -> None:
        self._require_transaction()
        cursor = await self._connection.execute(
            "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id"
        )
        row = await cursor.fetchone()
        try:
            transaction_id = (
                _row_value(row, 0, "transaction_id") if row is not None else None
            )
        except ValueError:
            transaction_id = None
        if (
            not isinstance(transaction_id, str)
            or not transaction_id.isascii()
            or not transaction_id.isdigit()
            or len(transaction_id) > 32
        ):
            raise DatabaseOperationError(
                operation="bind_pipeline_handoff_transaction",
                retryable=False,
                message="pipeline ownership transaction identity is invalid",
            )
        if self._transaction_id is None:
            self._transaction_id = transaction_id
        elif self._transaction_id != transaction_id:
            raise StaleFence()

    async def _lock_quiesced(
        self,
        account_id: int,
        expected_generation: int,
        expected_fencing_token: int,
    ) -> PipelineGeneration:
        self._require_transaction()
        account_id = _require_bigint("account_id", account_id)
        expected_generation = _require_bigint(
            "expected_generation",
            expected_generation,
        )
        expected_fencing_token = _require_bigint(
            "expected_fencing_token",
            expected_fencing_token,
        )
        if self._locked is not None:
            raise StaleFence()
        try:
            await self._assert_transaction_identity()
            await self._repository._configure_transaction(self._connection)
            await self._repository._acquire_account_lock(
                self._connection,
                account_id,
            )
            locked = await self._repository._fetch_exact(
                self._connection,
                account_id=account_id,
                generation=expected_generation,
                fencing_token=expected_fencing_token,
                for_update=True,
            )
            if locked is None or locked.state is not PipelineGenerationState.QUIESCING:
                raise StaleFence()
            self._locked = locked
            return locked
        except (StaleFence, DatabaseOperationError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("lock_pipeline_handoff", error) from None

    async def _mark_draining(
        self,
        locked: PipelineGeneration,
        *,
        actor: str,
        reason: str,
    ) -> PipelineGeneration:
        self._require_transaction()
        actor = _require_exact_text("actor", actor, max_length=128)
        reason = _require_exact_text("reason", reason, max_length=512)
        if self._locked is None or locked != self._locked or self._draining is not None:
            raise StaleFence()
        try:
            await self._assert_transaction_identity()
            await self._repository._configure_transaction(self._connection)
            await self._repository._acquire_account_lock(
                self._connection,
                locked.account_id,
            )
            update = sql.SQL(
                "UPDATE {} SET state = 'draining', reason = %s, "
                "updated_at = pg_catalog.statement_timestamp() "
                "WHERE account_id = %s AND generation = %s "
                "AND fencing_token = %s AND state = 'quiescing' "
                "RETURNING account_id, generation, pipeline_name, state, "
                "fencing_token"
            ).format(self._repository._table("pipeline_ownership"))
            cursor = await self._connection.execute(
                update,
                (
                    reason,
                    locked.account_id,
                    locked.generation,
                    locked.fencing_token,
                ),
            )
            row = await cursor.fetchone()
            if row is None:
                raise StaleFence()
            draining = _generation_from_row(row)
            await self._repository._audit(
                self._connection,
                draining,
                action="pipeline.mark_draining",
                actor=actor,
                reason=reason,
            )
            self._draining = draining
            return draining
        except (StaleFence, DatabaseOperationError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("mark_pipeline_draining", error) from None

    async def _insert_current(
        self,
        *,
        account_id: int,
        pipeline_name: str,
        generation: int,
        fencing_token: int,
        actor: str,
        reason: str,
    ) -> PipelineGeneration:
        self._require_transaction()
        account_id = _require_bigint("account_id", account_id)
        pipeline_name = _require_exact_text(
            "pipeline_name",
            pipeline_name,
            max_length=64,
        )
        generation = _require_bigint("generation", generation)
        fencing_token = _require_bigint("fencing_token", fencing_token)
        actor = _require_exact_text("actor", actor, max_length=128)
        reason = _require_exact_text("reason", reason, max_length=512)
        if (
            self._draining is None
            or self._draining.account_id != account_id
            or self._inserted
        ):
            raise StaleFence()
        try:
            await self._assert_transaction_identity()
            await self._repository._configure_transaction(self._connection)
            await self._repository._acquire_account_lock(
                self._connection,
                account_id,
            )
            persisted_draining = await self._repository._fetch_exact(
                self._connection,
                account_id=self._draining.account_id,
                generation=self._draining.generation,
                fencing_token=self._draining.fencing_token,
                for_update=True,
            )
            if (
                persisted_draining != self._draining
                or persisted_draining is None
                or persisted_draining.state is not PipelineGenerationState.DRAINING
            ):
                raise StaleFence()
            maxima_query = sql.SQL(
                "SELECT pg_catalog.max(generation) AS maximum_generation, "
                "pg_catalog.max(fencing_token) AS maximum_fencing_token "
                "FROM {} WHERE account_id = %s"
            ).format(self._repository._table("pipeline_ownership"))
            maxima_cursor = await self._connection.execute(
                maxima_query,
                (account_id,),
            )
            maxima = await maxima_cursor.fetchone()
            try:
                maximum_generation = (
                    _row_value(maxima, 0, "maximum_generation")
                    if maxima is not None
                    else None
                )
                maximum_fencing_token = (
                    _row_value(maxima, 1, "maximum_fencing_token")
                    if maxima is not None
                    else None
                )
            except ValueError:
                maximum_generation = None
                maximum_fencing_token = None
            if (
                maxima is None
                or not isinstance(maximum_generation, int)
                or not isinstance(maximum_fencing_token, int)
                or maximum_generation >= POSTGRES_BIGINT_MAX
                or maximum_fencing_token >= POSTGRES_BIGINT_MAX
                or generation != maximum_generation + 1
                or fencing_token != maximum_fencing_token + 1
            ):
                raise StaleFence()
            insert = sql.SQL(
                "INSERT INTO {} ("
                "account_id, generation, pipeline_name, state, fencing_token, "
                "created_by, reason"
                ") VALUES (%s, %s, %s, 'current_ingress', %s, %s, %s) "
                "RETURNING account_id, generation, pipeline_name, state, "
                "fencing_token"
            ).format(self._repository._table("pipeline_ownership"))
            cursor = await self._connection.execute(
                insert,
                (
                    account_id,
                    generation,
                    pipeline_name,
                    fencing_token,
                    actor,
                    reason,
                ),
            )
            row = await cursor.fetchone()
            inserted = _generation_from_row(row)
            await self._repository._audit(
                self._connection,
                inserted,
                action="pipeline.insert_current",
                actor=actor,
                reason=reason,
            )
            self._inserted = True
            return inserted
        except (StaleFence, DatabaseOperationError):
            raise
        except _DATABASE_EXCEPTIONS as error:
            raise _database_error("insert_current_pipeline", error) from None


__all__ = [
    "PipelineOwnershipRepository",
    "PipelineRetirementBlocked",
    "RetirementBlockCode",
    "RetirementGuard",
    "ownership_advisory_lock_key",
]
