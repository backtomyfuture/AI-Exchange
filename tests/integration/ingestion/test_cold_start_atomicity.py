from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg.conninfo import make_conninfo
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from src.db.bootstrap import bootstrap_database
from src.domain.errors import SyncContractError, SyncTransientError
from src.ingestion.cold_start import (
    ColdStartPlanView,
    ColdStartPlanState,
    ColdStartRunStatus,
    ColdStartService,
)
from src.ingestion.command_receipts import CommandReceiptRepository
from src.ingestion.models import ChangeKind, SyncBatch, SyncChange
from src.ingestion.ownership import PipelineOwnershipRepository
from src.ingestion.policy import ProcessingPolicyResolver
from src.ingestion.repository import InboxRepository
from src.ingestion.sync import sync_advisory_lock_keys
from tests.integration.ingestion.test_sync_atomicity import (
    _NeverPool,
    _PermitProvider,
    _SnapshotProvider,
    _batch,
    _create_change,
    _multi_folder_snapshot,
    _snapshot,
)


class _ColdStartOrigin:
    def __init__(self, outcomes: list[SyncBatch]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[int, str, str | None, int]] = []

    async def fetch_cold_start_page(
        self,
        account_id: int,
        sync_folder: str,
        cursor: str | None,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, sync_folder, cursor, limit))
        if not self.outcomes:
            raise AssertionError("unexpected cold-start Origin request")
        return self.outcomes.pop(0)


class _OrdinaryPageClient:
    def __init__(self, outcomes: list[SyncBatch]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[int, str, str, int]] = []

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, cursor, limit))
        if not self.outcomes:
            raise AssertionError("unexpected ordinary Exchange request")
        return self.outcomes.pop(0)


class _ContractFailingOrdinaryClient:
    def __init__(self) -> None:
        self.error = SyncContractError()
        self.calls: list[tuple[int, str, str, int]] = []
        self.raised: list[SyncContractError] = []

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, cursor, limit))
        self.raised.append(self.error)
        raise self.error


class _TransientFailingOrdinaryClient:
    def __init__(self, before_raise: Callable[[], None]) -> None:
        self._before_raise = before_raise
        self.error = SyncTransientError(retry_after_seconds=17)
        self.calls: list[tuple[int, str, str, int]] = []

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, cursor, limit))
        self._before_raise()
        raise self.error


class _InjectedReceiptFailure(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _ReceiptFailureObservation:
    plan_count: int
    lookup_transaction_id: str | None
    insert_transaction_id: str
    transaction_status: object


class _PlanVisibleThenFailReceiptRepository:
    def __init__(self) -> None:
        self._delegate = CommandReceiptRepository()
        self.observations: list[_ReceiptFailureObservation] = []

    def transaction(self, connection: Any) -> _PlanVisibleThenFailReceiptTransaction:
        return _PlanVisibleThenFailReceiptTransaction(
            connection,
            self._delegate.transaction(connection),
            self.observations,
        )


class _PlanVisibleThenFailReceiptTransaction:
    def __init__(
        self,
        connection: Any,
        delegate: Any,
        observations: list[_ReceiptFailureObservation],
    ) -> None:
        self._connection = connection
        self._delegate = delegate
        self._observations = observations

    async def lookup(self, **kwargs: object) -> object:
        return await self._delegate.lookup(**kwargs)

    async def insert(self, **_kwargs: object) -> object:
        cursor = await self._connection.execute(
            "SELECT pg_catalog.count(*) AS plan_count, "
            "pg_catalog.pg_current_xact_id()::pg_catalog.text "
            "AS transaction_id FROM sync_cold_start_plans"
        )
        row = await cursor.fetchone()
        assert row is not None
        self._observations.append(
            _ReceiptFailureObservation(
                plan_count=int(row["plan_count"]),
                lookup_transaction_id=self._delegate._transaction_id,
                insert_transaction_id=str(row["transaction_id"]),
                transaction_status=self._connection.info.transaction_status,
            )
        )
        raise _InjectedReceiptFailure("receipt insert failure after plan visibility")


_APPLY_CONTROL_SQL = frozenset(
    {
        "set local transaction isolation level read committed",
        "select pg_catalog.pg_try_advisory_lock(%s, %s) as acquired",
        "select pg_catalog.pg_advisory_unlock(%s, %s) as released",
        "select pg_catalog.pg_advisory_xact_lock_shared(%s)",
        "select pg_catalog.pg_advisory_xact_lock(%s)",
        "select pg_catalog.clock_timestamp() as database_now",
        (
            "select pg_catalog.pg_current_xact_id()::pg_catalog.text "
            "as transaction_id, "
            "pg_catalog.current_setting('transaction_isolation') "
            "as isolation_level"
        ),
        (
            "select pg_catalog.pg_current_xact_id()::pg_catalog.text "
            "as transaction_id, "
            "pg_catalog.current_setting('transaction_isolation') "
            "as transaction_isolation"
        ),
        (
            "select pg_catalog.set_config('lock_timeout', %s, true), "
            "pg_catalog.set_config('statement_timeout', %s, true), "
            "pg_catalog.set_config('idle_in_transaction_session_timeout', "
            "%s, true)"
        ),
    }
)
_APPLY_READ_RELATIONS = frozenset(
    {
        "public.cold_start_command_receipts",
        "public.event_inbox",
        "public.pipeline_ownership",
        "public.sync_cold_start_plans",
        "public.sync_cursors",
    }
)
_SQL_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_$]*\Z")
_SQL_NON_FUNCTION_PAREN_KEYWORDS = frozenset(
    {
        "and",
        "as",
        "case",
        "cross",
        "else",
        "end",
        "except",
        "exists",
        "filter",
        "from",
        "full",
        "group",
        "having",
        "in",
        "inner",
        "intersect",
        "join",
        "left",
        "limit",
        "not",
        "offset",
        "on",
        "or",
        "order",
        "over",
        "partition",
        "right",
        "select",
        "then",
        "union",
        "values",
        "when",
        "where",
        "within",
    }
)


def _sql_tokens(statement: str) -> tuple[str, ...] | None:
    """Lex the guarded SQL subset while discarding whitespace and comments."""

    if type(statement) is not str:
        return None
    tokens: list[str] = []
    index = 0
    length = len(statement)
    while index < length:
        character = statement[index]
        following = statement[index + 1] if index + 1 < length else ""
        if character.isspace():
            index += 1
            continue
        if character == "-" and following == "-":
            carriage_return = statement.find("\r", index + 2)
            line_feed = statement.find("\n", index + 2)
            endings = tuple(
                ending for ending in (carriage_return, line_feed) if ending >= 0
            )
            line_end = min(endings) if endings else -1
            index = length if line_end < 0 else line_end + 1
            continue
        if character == "/" and following == "*":
            depth = 1
            index += 2
            while index < length and depth:
                pair = statement[index : index + 2]
                if pair == "/*":
                    depth += 1
                    index += 2
                elif pair == "*/":
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                return None
            continue
        if character == "'":
            value: list[str] = []
            index += 1
            while index < length:
                if statement[index] == "'":
                    if index + 1 < length and statement[index + 1] == "'":
                        value.append("'")
                        index += 2
                        continue
                    index += 1
                    break
                value.append(statement[index])
                index += 1
            else:
                return None
            tokens.append("string:" + "".join(value))
            continue
        if character == '"':
            value = []
            index += 1
            while index < length:
                if statement[index] == '"':
                    if index + 1 < length and statement[index + 1] == '"':
                        value.append('"')
                        index += 2
                        continue
                    index += 1
                    break
                value.append(statement[index])
                index += 1
            else:
                return None
            identifier = "".join(value)
            tokens.append("quoted:" + identifier)
            continue
        if character == "$":
            delimiter_match = re.match(
                r"\$(?:[a-zA-Z_][a-zA-Z0-9_]*)?\$",
                statement[index:],
            )
            if delimiter_match is None:
                return None
            delimiter = delimiter_match.group(0)
            body_start = index + len(delimiter)
            closing = statement.find(delimiter, body_start)
            if closing < 0:
                return None
            tokens.append("string:" + statement[body_start:closing])
            index = closing + len(delimiter)
            continue
        if character == "%":
            if statement.startswith("%s", index):
                tokens.append("%s")
                index += 2
                continue
            named = re.match(
                r"%\([a-zA-Z_][a-zA-Z0-9_]*\)s",
                statement[index:],
            )
            if named is None:
                return None
            token = named.group(0)
            tokens.append(token.lower())
            index += len(token)
            continue
        if character.isalpha() or character == "_":
            end = index + 1
            while end < length and (
                statement[end].isalnum() or statement[end] in {"_", "$"}
            ):
                end += 1
            identifier = statement[index:end].lower()
            tokens.append(
                identifier
                if identifier.isascii()
                and _SQL_IDENTIFIER.fullmatch(identifier) is not None
                else "unicode:" + identifier
            )
            index = end
            continue
        if character.isdigit():
            end = index + 1
            while end < length and (statement[end].isdigit() or statement[end] == "."):
                end += 1
            tokens.append(statement[index:end])
            index = end
            continue
        operator = next(
            (
                candidate
                for candidate in (
                    "#>>",
                    "->>",
                    "::",
                    "<=",
                    ">=",
                    "<>",
                    "!=",
                    "||",
                    "->",
                    "#>",
                )
                if statement.startswith(candidate, index)
            ),
            None,
        )
        if operator is not None:
            tokens.append(operator)
            index += len(operator)
            continue
        if character in "(),.;=*+-/<>[]":
            tokens.append(character)
            index += 1
            continue
        return None
    return tuple(tokens)


def _has_sql_separator(statement: str) -> bool:
    tokens = _sql_tokens(statement)
    return tokens is None or ";" in tokens


_APPLY_CONTROL_TOKENS = frozenset(
    tokens
    for statement in _APPLY_CONTROL_SQL
    if (tokens := _sql_tokens(statement)) is not None
)
_APPLY_READ_RELATION_TOKENS = frozenset(
    tuple(relation.split(".", maxsplit=1)) for relation in _APPLY_READ_RELATIONS
)


def _contains_noncontrol_function_call(tokens: tuple[str, ...]) -> bool:
    for index, token in enumerate(tokens):
        if (
            _SQL_IDENTIFIER.fullmatch(token) is None
            or token in _SQL_NON_FUNCTION_PAREN_KEYWORDS
        ):
            continue
        end = index
        while (
            end + 2 < len(tokens)
            and tokens[end + 1] == "."
            and _SQL_IDENTIFIER.fullmatch(tokens[end + 2]) is not None
        ):
            end += 2
        if end + 1 < len(tokens) and tokens[end + 1] == "(":
            return True
    return False


def _read_relations_are_allowed(tokens: tuple[str, ...]) -> bool:
    relation_count = 0
    first_from: int | None = None
    quoted_relation_positions: set[int] = set()
    for index, token in enumerate(tokens):
        if token not in {"from", "join"}:
            continue
        if first_from is None:
            first_from = index
        if index + 3 >= len(tokens) or tokens[index + 2] != ".":
            return False
        if index + 4 < len(tokens) and tokens[index + 4] == "(":
            return False
        raw_schema, raw_table = tokens[index + 1], tokens[index + 3]
        schema = raw_schema.removeprefix("quoted:")
        table = raw_table.removeprefix("quoted:")
        if (
            not schema.isascii()
            or not table.isascii()
            or _SQL_IDENTIFIER.fullmatch(schema) is None
            or _SQL_IDENTIFIER.fullmatch(table) is None
        ):
            return False
        relation = (schema, table)
        if relation not in _APPLY_READ_RELATION_TOKENS:
            return False
        if raw_schema.startswith("quoted:"):
            quoted_relation_positions.add(index + 1)
        if raw_table.startswith("quoted:"):
            quoted_relation_positions.add(index + 3)
        relation_count += 1
    if first_from is not None:
        depth = 0
        for token in tokens[first_from + 1 :]:
            if token == "(":
                depth += 1
            elif token == ")":
                depth -= 1
                if depth < 0:
                    return False
            elif token == "," and depth == 0:
                return False
        if depth:
            return False
    if any(
        token.startswith(("quoted:", "unicode:"))
        and index not in quoted_relation_positions
        for index, token in enumerate(tokens)
    ):
        return False
    return relation_count > 0


def _is_allowed_apply_read_sql(statement: str) -> bool:
    tokens = _sql_tokens(statement)
    if tokens is None or ";" in tokens:
        return False
    if tokens in _APPLY_CONTROL_TOKENS:
        return True
    if not tokens or tokens[0] != "select" or tokens.count("select") != 1:
        return False
    if (
        "into" in tokens
        or "table" in tokens
        or "::" in tokens
        or _contains_noncontrol_function_call(tokens)
    ):
        return False
    return _read_relations_are_allowed(tokens)


_COMMAND_REPLAY_MARKER_ORDER = ("receipt_lookup", "plan_lookup")
_COMMAND_REPLAY_SQL_DIGEST_MARKERS: dict[str, str] = {}


@dataclass(frozen=True, slots=True)
class _CommandReplayExpectation:
    account_id: int
    command_name: str
    idempotency_key_hash: str
    plan_id: UUID


def _command_replay_marker(
    statement: str,
    params: object,
    expected: _CommandReplayExpectation,
) -> str | None:
    tokens = _sql_tokens(statement)
    if tokens is None or ";" in tokens:
        return None
    if tokens in _APPLY_CONTROL_TOKENS:
        return "control"
    if any(comment in statement for comment in ("--", "/*", "*/")):
        return None
    digest = hashlib.sha256("\x00".join(tokens).encode("utf-8")).hexdigest()
    marker = _COMMAND_REPLAY_SQL_DIGEST_MARKERS.get(digest)
    if marker == "receipt_lookup":
        return (
            marker
            if (
                type(params) is tuple
                and len(params) == 3
                and type(params[0]) is int
                and params[0] == expected.account_id
                and type(params[1]) is str
                and params[1] == expected.command_name
                and type(params[2]) is str
                and params[2] == expected.idempotency_key_hash
            )
            else None
        )
    if marker == "plan_lookup":
        return (
            marker
            if (
                type(params) is tuple
                and len(params) == 1
                and type(params[0]) is UUID
                and params[0] == expected.plan_id
            )
            else None
        )
    return None


def _command_replay_marker_is_next(trace: list[str], marker: str) -> bool:
    index = len(trace)
    return (
        index < len(_COMMAND_REPLAY_MARKER_ORDER)
        and marker == _COMMAND_REPLAY_MARKER_ORDER[index]
    )


def _is_replay_forbidden_sql(statement: str) -> bool:
    return not _is_allowed_apply_read_sql(statement)


class _ReplayGuardConnection:
    def __init__(self, connection: Any, statements: list[str]) -> None:
        self._connection = connection
        self._statements = statements

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        self._statements.append(rendered)
        if _is_replay_forbidden_sql(rendered):
            raise AssertionError("completed replay attempted non-read-only SQL")
        return await self._connection.execute(statement, params)


class _ReplayGuardPool:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool
        self.statements: list[str] = []

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _ReplayGuardConnection:
        return _ReplayGuardConnection(await self._pool.getconn(), self.statements)

    async def putconn(self, connection: _ReplayGuardConnection) -> None:
        await self._pool.putconn(connection._connection)


class _InjectedApplyStatementFailure(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _ExecutedApplyDml:
    marker: str
    rowcount: int
    transaction_id: str
    transaction_status: object


@dataclass(frozen=True, slots=True)
class _ApplyCommitBoundary:
    phase: str
    markers: tuple[str, ...]
    transaction_status: object


_APPLY_DML_ORDER = (
    "event_inbox.insert",
    "sync_cursors.update",
    "sync_cold_start_plans.update",
    "cold_start_command_receipts.insert",
)
_BLOCK_DML_ORDER = (
    "sync_cold_start_plans.blocked_update",
    "sync_cursors.blocked_update",
    "audit_events.block_insert",
)


_DML_TOKEN_DIGEST_MARKERS = {
    "5b0c03fa6dcd69bb23410a6e98e8e796889781e652fe60d7d0c8335b1c681411": (
        "event_inbox.insert"
    ),
    "6dcbecbb1fcc35d386a08c2a4f95c96efe54518186537dc4102f1e8981568ec2": (
        "sync_cursors.update"
    ),
    "7fba9cda0e5f8c8371ee0f581d762524198da98a568ca4b6162b1fe3a45c26f1": (
        "sync_cold_start_plans.update"
    ),
    "d0c5337483e3ec81ffd307409b43ba3beaac5bdb6513845395a626a150bb2d04": (
        "cold_start_command_receipts.insert"
    ),
    "0b96f44734de4543396f8b8bf569642fe2a42785d69f2b4fe97a3620d4f014e6": (
        "sync_cold_start_plans.blocked_update"
    ),
    "4831379d7d53d009d59cbb3a76bc3566262455784360d3dfa756b8af7bcc1101": (
        "sync_cursors.blocked_update"
    ),
    "e8785b15c95a33fb7e41dc8f50a7f83e535575eacf7b3ae672a69aa624a51343": (
        "audit_events.block_insert"
    ),
}
_APPLY_CURSOR_PARAM_KEYS = frozenset(
    {
        "account_id",
        "database_stamp",
        "expected_blocked_at",
        "expected_blocked_reason_code",
        "expected_contract_fingerprint",
        "expected_cursor",
        "expected_plan_id",
        "expected_plan_state",
        "expected_retry_after_at",
        "expected_status",
        "expected_transient_failures",
        "expected_version",
        "folder_key",
        "next_cursor",
        "target_plan_id",
        "target_plan_state",
        "target_status",
    }
)
_APPLY_PLAN_PARAM_KEYS = frozenset(
    {
        "account_id",
        "database_stamp",
        "expected_apply_cursor",
        "expected_apply_cursor_version",
        "expected_boundary_cursor",
        "expected_boundary_cursor_version",
        "expected_config_hash",
        "expected_contract_fingerprint",
        "expected_plan_hash",
        "expected_version",
        "folder_key",
        "next_cursor",
        "next_cursor_version",
        "plan_id",
        "target_state",
        "terminal",
    }
)
_BLOCK_PLAN_PARAM_KEYS = frozenset(
    {
        "account_id",
        "blocked_at",
        "blocked_fingerprint",
        "expected_apply_cursor",
        "expected_apply_cursor_version",
        "expected_boundary_cursor",
        "expected_boundary_cursor_version",
        "expected_item_count",
        "expected_page_count",
        "expected_plan_hash",
        "expected_preview_cursor",
        "expected_preview_cursor_version",
        "expected_rolling_hash",
        "expected_state",
        "expected_version",
        "folder_key",
        "plan_id",
        "safe_code",
    }
)
_BLOCK_CURSOR_PARAM_KEYS = frozenset(
    {
        "account_id",
        "blocked_at",
        "blocked_fingerprint",
        "expected_blocked_at",
        "expected_blocked_reason_code",
        "expected_contract_fingerprint",
        "expected_cursor",
        "expected_last_attempt_at",
        "expected_last_success_at",
        "expected_plan_id",
        "expected_plan_state",
        "expected_retry_after_at",
        "expected_status",
        "expected_transient_failures",
        "expected_updated_at",
        "expected_version",
        "folder_key",
        "safe_code",
    }
)


def _exact_parameter_dict(
    params: object,
    expected_keys: frozenset[str],
) -> dict[str, Any] | None:
    if (
        type(params) is not dict
        or any(type(key) is not str for key in params)
        or set(params) != expected_keys
    ):
        return None
    return params


def _dml_params_are_allowed(marker: str, params: object) -> bool:
    if marker == "event_inbox.insert":
        return type(params) is tuple and len(params) == 16
    if marker == "cold_start_command_receipts.insert":
        return (
            type(params) is tuple
            and len(params) == 10
            and params[2] == "cold_start.apply_page"
            and params[5] == "succeeded"
            and params[6] == "sync_cold_start_plan"
        )
    if marker == "audit_events.block_insert":
        return (
            type(params) is tuple
            and len(params) == 8
            and params[4] == "blocked"
            and params[5] == "cold_start_service"
            and params[6] == "exchange.sync.contract_invalid"
        )
    if marker == "sync_cursors.update":
        values = _exact_parameter_dict(params, _APPLY_CURSOR_PARAM_KEYS)
        if values is None:
            return False
        if values["target_status"] == "active":
            return (
                values["target_plan_id"] is None and values["target_plan_state"] is None
            )
        return (
            values["target_status"] == "cold_start_applying"
            and values["target_plan_id"] is not None
            and values["target_plan_state"] == "approved"
        )
    if marker == "sync_cold_start_plans.update":
        values = _exact_parameter_dict(params, _APPLY_PLAN_PARAM_KEYS)
        return values is not None and (
            (values["terminal"] is True and values["target_state"] == "completed")
            or (values["terminal"] is False and values["target_state"] == "approved")
        )
    if marker == "sync_cold_start_plans.blocked_update":
        values = _exact_parameter_dict(params, _BLOCK_PLAN_PARAM_KEYS)
        return (
            values is not None
            and values["safe_code"] == "exchange.sync.contract_invalid"
            and values["expected_state"] == "approved"
        )
    if marker == "sync_cursors.blocked_update":
        values = _exact_parameter_dict(params, _BLOCK_CURSOR_PARAM_KEYS)
        return (
            values is not None
            and values["safe_code"] == "exchange.sync.contract_invalid"
            and values["expected_status"] == "cold_start_pending"
        )
    return False


def _apply_dml_marker(statement: str, params: object) -> str | None:
    tokens = _sql_tokens(statement)
    if tokens is None or ";" in tokens:
        return None
    digest = hashlib.sha256("\x00".join(tokens).encode("utf-8")).hexdigest()
    marker = _DML_TOKEN_DIGEST_MARKERS.get(digest)
    if marker is None or not _dml_params_are_allowed(marker, params):
        return None
    return marker


class _ApplyStatementTransaction:
    def __init__(
        self,
        transaction: Any,
        connection: Any,
        observations: list[_ExecutedApplyDml],
        commit_boundaries: list[_ApplyCommitBoundary],
        required_commit_order: tuple[str, ...] | None,
    ) -> None:
        self._transaction = transaction
        self._connection = connection
        self._observations = observations
        self._commit_boundaries = commit_boundaries
        self._required_commit_order = required_commit_order
        self._start = len(observations)

    async def __aenter__(self) -> Any:
        return await self._transaction.__aenter__()

    async def __aexit__(self, *args: object) -> object:
        error_type = args[0] if args else None
        markers = tuple(item.marker for item in self._observations[self._start :])
        trace_commit = error_type is None and bool(markers)
        if trace_commit:
            assert self._required_commit_order is not None
            assert markers == self._required_commit_order
            assert self._connection.info.transaction_status is TransactionStatus.INTRANS
            self._commit_boundaries.append(
                _ApplyCommitBoundary(
                    phase="before_commit",
                    markers=markers,
                    transaction_status=self._connection.info.transaction_status,
                )
            )
        outcome = await self._transaction.__aexit__(*args)
        if trace_commit:
            assert self._connection.info.transaction_status is TransactionStatus.IDLE
            self._commit_boundaries.append(
                _ApplyCommitBoundary(
                    phase="after_commit",
                    markers=markers,
                    transaction_status=self._connection.info.transaction_status,
                )
            )
        return outcome


class _ApplyStatementFaultConnection:
    def __init__(
        self,
        connection: Any,
        *,
        target: str | None,
        observations: list[_ExecutedApplyDml],
        error: _InjectedApplyStatementFailure | None,
        commit_boundaries: list[_ApplyCommitBoundary],
        required_commit_order: tuple[str, ...] | None,
    ) -> None:
        self._connection = connection
        self._target = target
        self._observations = observations
        self._error = error
        self._commit_boundaries = commit_boundaries
        self._required_commit_order = required_commit_order

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def transaction(self, *args: object, **kwargs: object) -> Any:
        return _ApplyStatementTransaction(
            self._connection.transaction(*args, **kwargs),
            self._connection,
            self._observations,
            self._commit_boundaries,
            self._required_commit_order,
        )

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        marker = _apply_dml_marker(rendered, params)
        if _has_sql_separator(rendered):
            raise AssertionError("unexpected apply SQL statement boundary")
        if marker is None and not _is_allowed_apply_read_sql(rendered):
            raise AssertionError("unexpected apply SQL relation, verb, or command")

        cursor = await self._connection.execute(statement, params)
        if marker is None:
            return cursor

        rowcount = cursor.rowcount
        assert rowcount == 1
        status = self._connection.info.transaction_status
        assert status is TransactionStatus.INTRANS
        xid_cursor = await self._connection.execute(
            "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id"
        )
        xid_row = await xid_cursor.fetchone()
        assert (
            type(xid_row) is dict
            and set(xid_row) == {"transaction_id"}
            and type(xid_row["transaction_id"]) is str
            and xid_row["transaction_id"].isascii()
            and xid_row["transaction_id"].isdigit()
        )
        self._observations.append(
            _ExecutedApplyDml(
                marker=marker,
                rowcount=rowcount,
                transaction_id=xid_row["transaction_id"],
                transaction_status=status,
            )
        )
        if marker == self._target:
            assert self._error is not None
            raise self._error
        return cursor


class _ApplyStatementFaultPool:
    def __init__(self, pool: AsyncConnectionPool, target: str | None) -> None:
        if target is not None and target not in (*_APPLY_DML_ORDER, *_BLOCK_DML_ORDER):
            raise ValueError("unknown apply fault target")
        self._pool = pool
        self._target = target
        self.observations: list[_ExecutedApplyDml] = []
        self.commit_boundaries: list[_ApplyCommitBoundary] = []
        self.error = (
            None
            if target is None
            else _InjectedApplyStatementFailure(f"injected failure after {target}")
        )
        self._required_commit_order = _APPLY_DML_ORDER if target is None else None

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _ApplyStatementFaultConnection:
        return _ApplyStatementFaultConnection(
            await self._pool.getconn(),
            target=self._target,
            observations=self.observations,
            error=self.error,
            commit_boundaries=self.commit_boundaries,
            required_commit_order=self._required_commit_order,
        )

    async def putconn(self, connection: _ApplyStatementFaultConnection) -> None:
        await self._pool.putconn(connection._connection)


_PREVIEW_ACCEPTANCE_DML_ORDER = (
    "sync_cold_start_plans.insert",
    "cold_start_command_receipts.insert",
)
_PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_ORDER = (
    "6557bf090d094b39d367714496b6926f4b3a1114d20e325883bfa177cacbc01c",
    "d0c5337483e3ec81ffd307409b43ba3beaac5bdb6513845395a626a150bb2d04",
)
_PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_MARKERS = dict(
    zip(
        _PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_ORDER,
        _PREVIEW_ACCEPTANCE_DML_ORDER,
        strict=True,
    )
)

_APPROVE_COMMAND_DML_ORDER = (
    "sync_cold_start_plans.approve_update",
    "audit_events.approve_insert",
    "approve_command_receipts.insert",
)
_APPROVE_COMMAND_DML_TOKEN_DIGEST_MARKERS = {
    "c0d53673215ae656dbd3d2a48981bb1ceaf356c8d8fa026febf4e9a186a55d54": (
        "sync_cold_start_plans.approve_update"
    ),
    "30271b5d184964254cffc27f81639e2a6bdb8fadbdd85f09b0cf456c43da1edd": (
        "audit_events.approve_insert"
    ),
    "d0c5337483e3ec81ffd307409b43ba3beaac5bdb6513845395a626a150bb2d04": (
        "approve_command_receipts.insert"
    ),
}
_APPROVE_PLAN_PARAM_KEYS = frozenset(
    {
        "plan_id",
        "account_id",
        "folder_key",
        "expected_version",
        "expected_plan_hash",
        "expected_boundary_cursor",
        "expected_boundary_cursor_version",
    }
)


def _approve_plan_params_are_exact(params: object) -> bool:
    values = _exact_parameter_dict(params, _APPROVE_PLAN_PARAM_KEYS)
    return (
        values is not None
        and type(values["plan_id"]) is UUID
        and type(values["account_id"]) is int
        and values["account_id"] >= 1
        and _is_exact_bounded_sql_param_text(values["folder_key"], 512)
        and type(values["expected_version"]) is int
        and values["expected_version"] >= 1
        and _is_sha256_sql_param(values["expected_plan_hash"])
        and _is_exact_bounded_sql_param_text(
            values["expected_boundary_cursor"],
            8192,
        )
        and type(values["expected_boundary_cursor_version"]) is int
        and values["expected_boundary_cursor_version"] >= 1
    )


def _approve_receipt_params_are_exact(params: object) -> bool:
    if type(params) is not tuple or len(params) != 10:
        return False
    return (
        type(params[0]) is UUID
        and type(params[1]) is int
        and params[1] >= 1
        and type(params[2]) is str
        and params[2] == "cold_start.approve"
        and _is_sha256_sql_param(params[3])
        and _is_sha256_sql_param(params[4])
        and type(params[5]) is str
        and params[5] == "succeeded"
        and type(params[6]) is str
        and params[6] == "sync_cold_start_plan"
        and _is_canonical_uuid_text(params[7])
        and _is_sha256_sql_param(params[8])
        and type(params[9]) is int
        and params[9] >= 1
    )


def _approve_audit_params_are_exact(params: object) -> bool:
    if type(params) is not tuple or len(params) != 8:
        return False
    metadata = params[7]
    if type(metadata) is not Jsonb or type(metadata.obj) is not dict:
        return False
    values = metadata.obj
    samples = values.get("redacted_samples")
    return (
        type(params[0]) is UUID
        and _is_sha256_sql_param(params[1])
        and type(params[2]) is int
        and params[2] >= 1
        and _is_sha256_sql_param(params[3])
        and type(params[4]) is str
        and params[4] == "approved"
        and _is_exact_bounded_sql_param_text(params[5], 128)
        and _is_exact_bounded_sql_param_text(params[6], 512)
        and set(values)
        == {"plan_id", "plan_hash", "page_count", "item_count", "redacted_samples"}
        and _is_canonical_uuid_text(values["plan_id"])
        and _is_sha256_sql_param(values["plan_hash"])
        and type(values["page_count"]) is int
        and values["page_count"] >= 1
        and type(values["item_count"]) is int
        and values["item_count"] >= 0
        and type(samples) is list
        and all(
            type(sample) is dict
            and set(sample) == {"kind", "external_email_id_hash"}
            and sample["kind"] in {"create", "update", "delete"}
            and _is_sha256_sql_param(sample["external_email_id_hash"])
            for sample in samples
        )
    )


def _approve_ack_loss_dml_marker(statement: str, params: object) -> str | None:
    if type(statement) is not str or any(
        comment in statement for comment in ("--", "/*", "*/")
    ):
        return None
    tokens = _sql_tokens(statement)
    if tokens is None or ";" in tokens:
        return None
    digest = hashlib.sha256("\x00".join(tokens).encode("utf-8")).hexdigest()
    marker = _APPROVE_COMMAND_DML_TOKEN_DIGEST_MARKERS.get(digest)
    if marker == "sync_cold_start_plans.approve_update":
        return marker if _approve_plan_params_are_exact(params) else None
    if marker == "audit_events.approve_insert":
        return marker if _approve_audit_params_are_exact(params) else None
    if marker == "approve_command_receipts.insert":
        return marker if _approve_receipt_params_are_exact(params) else None
    return None


def _is_exact_bounded_sql_param_text(value: object, maximum: int) -> bool:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F
            for character in value
        )
    ):
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _is_sha256_sql_param(value: object) -> bool:
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _preview_acceptance_plan_params_are_exact(params: object) -> bool:
    if type(params) is not tuple or len(params) != 14:
        return False
    (
        plan_id,
        account_id,
        folder_key,
        expected_status,
        expected_cursor,
        expected_cursor_version,
        pipeline_name,
        generation,
        fencing_token,
        contract_fingerprint,
        scope_config_hash,
        actor,
        reason,
        plan_ttl_seconds,
    ) = params
    eligible_cursor = (
        type(expected_status) is str
        and expected_status == "cold_start_pending"
        and expected_cursor is None
    ) or (
        type(expected_status) is str
        and expected_status == "reset_required"
        and _is_exact_bounded_sql_param_text(expected_cursor, 8192)
    )
    return (
        type(plan_id) is UUID
        and type(account_id) is int
        and account_id >= 1
        and _is_exact_bounded_sql_param_text(folder_key, 512)
        and eligible_cursor
        and type(expected_cursor_version) is int
        and expected_cursor_version >= 0
        and _is_exact_bounded_sql_param_text(pipeline_name, 64)
        and type(generation) is int
        and generation >= 1
        and type(fencing_token) is int
        and fencing_token >= 1
        and _is_sha256_sql_param(contract_fingerprint)
        and _is_sha256_sql_param(scope_config_hash)
        and _is_exact_bounded_sql_param_text(actor, 128)
        and _is_exact_bounded_sql_param_text(reason, 512)
        and type(plan_ttl_seconds) is int
        and 1 <= plan_ttl_seconds <= 7 * 24 * 60 * 60
    )


def _preview_acceptance_receipt_params_are_exact(params: object) -> bool:
    if type(params) is not tuple or len(params) != 10:
        return False
    try:
        result_plan_id = UUID(params[7]) if type(params[7]) is str else None
    except ValueError:
        return False
    return (
        type(params[0]) is UUID
        and type(params[1]) is int
        and params[1] >= 1
        and type(params[2]) is str
        and params[2] == "cold_start.preview"
        and _is_sha256_sql_param(params[3])
        and _is_sha256_sql_param(params[4])
        and type(params[5]) is str
        and params[5] == "succeeded"
        and type(params[6]) is str
        and params[6] == "sync_cold_start_plan"
        and result_plan_id is not None
        and str(result_plan_id) == params[7]
        and _is_sha256_sql_param(params[8])
        and type(params[9]) is int
        and params[9] >= 1
    )


def _preview_acceptance_dml_marker(statement: str, params: object) -> str | None:
    if type(statement) is not str or any(
        comment in statement for comment in ("--", "/*", "*/")
    ):
        return None
    tokens = _sql_tokens(statement)
    if tokens is None or ";" in tokens:
        return None
    digest = hashlib.sha256("\x00".join(tokens).encode("utf-8")).hexdigest()
    marker = _PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_MARKERS.get(digest)
    if marker == "sync_cold_start_plans.insert":
        return marker if _preview_acceptance_plan_params_are_exact(params) else None
    if marker == "cold_start_command_receipts.insert":
        return marker if _preview_acceptance_receipt_params_are_exact(params) else None
    return None


_PREVIEW_PAGE_UPDATE_TOKEN_DIGEST = (
    "ebd2b3a5f255bb83268f5b03e90fb1712de85baaac884b10bb248c1301719586"
)
_PREVIEW_PAGE_UPDATE_PARAM_KEYS = frozenset(
    {
        "account_id",
        "boundary_cursor",
        "boundary_cursor_version",
        "expected_item_count",
        "expected_page_count",
        "expected_preview_cursor",
        "expected_preview_cursor_version",
        "expected_rolling_hash",
        "expected_version",
        "folder_key",
        "item_count",
        "next_cursor",
        "plan_hash",
        "plan_id",
        "redacted_samples",
        "rolling_hash",
        "target_state",
        "terminal",
    }
)


def _preview_page_update_params_are_exact(params: object) -> bool:
    values = _exact_parameter_dict(params, _PREVIEW_PAGE_UPDATE_PARAM_KEYS)
    if values is None:
        return False
    expected_page_count = values["expected_page_count"]
    expected_item_count = values["expected_item_count"]
    expected_preview_version = values["expected_preview_cursor_version"]
    expected_version = values["expected_version"]
    boundary_version = values["boundary_cursor_version"]
    item_count = values["item_count"]
    if (
        type(expected_page_count) is not int
        or type(expected_item_count) is not int
        or type(expected_preview_version) is not int
        or type(expected_version) is not int
        or type(item_count) is not int
        or expected_page_count < 0
        or expected_item_count < 0
        or item_count < expected_item_count
        or expected_preview_version != expected_page_count
        or expected_version != expected_page_count
        or type(values["next_cursor"]) is not str
        or not values["next_cursor"]
        or type(values["rolling_hash"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", values["rolling_hash"]) is None
    ):
        return False
    if expected_page_count == 0:
        if (
            values["expected_preview_cursor"] is not None
            or values["expected_rolling_hash"] is not None
        ):
            return False
    elif (
        type(values["expected_preview_cursor"]) is not str
        or type(values["expected_rolling_hash"]) is not str
    ):
        return False
    if values["terminal"] is True:
        return (
            values["target_state"] == "ready"
            and values["boundary_cursor"] == values["next_cursor"]
            and type(boundary_version) is int
            and boundary_version == expected_page_count + 1
            and type(values["plan_hash"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", values["plan_hash"]) is not None
        )
    return (
        values["terminal"] is False
        and values["target_state"] == "previewing"
        and values["boundary_cursor"] is None
        and boundary_version is None
        and values["plan_hash"] is None
    )


def _is_preview_page_plan_update(statement: str, params: object) -> bool:
    tokens = _sql_tokens(statement)
    if tokens is None or ";" in tokens:
        return False
    digest = hashlib.sha256("\x00".join(tokens).encode("utf-8")).hexdigest()
    return (
        digest == _PREVIEW_PAGE_UPDATE_TOKEN_DIGEST
        and _preview_page_update_params_are_exact(params)
    )


@dataclass(frozen=True, slots=True)
class _PreviewPageUpdateObservation:
    marker: str
    rowcount: int
    assigned_transaction_id: str
    transaction_id: str
    backend_pid: int
    transaction_status: object
    http_phase: str
    armed: bool


@dataclass(frozen=True, slots=True)
class _PreviewAcceptanceDmlObservation:
    marker: str
    statement_digest: str
    rowcount: int
    transaction_id: str
    backend_pid: int
    transaction_status: object
    http_phase: str


_COMMAND_RECEIPT_ROW_KEYS = frozenset(
    {
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
    }
)


class _CapturedRowCursor:
    def __init__(
        self,
        cursor: Any,
        capture: Callable[[object], None],
    ) -> None:
        self._cursor = cursor
        self._capture = capture

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    async def fetchone(self) -> object:
        row = await self._cursor.fetchone()
        self._capture(row)
        return row


class _PreviewAckLossFaultState:
    def __init__(
        self,
        mode: str,
        *,
        fault_target: str = "preview_page",
        expected_replay_idempotency_key: str | None = None,
    ) -> None:
        if mode not in {"commit_then_raise", "rollback_then_raise"}:
            raise ValueError("unknown preview ACK-loss mode")
        if fault_target not in {"preview_page", "acceptance"}:
            raise ValueError("unknown preview ACK-loss target")
        if fault_target == "acceptance" and mode != "commit_then_raise":
            raise ValueError("preview acceptance ACK-loss requires a real commit")
        self.mode = mode
        self.fault_target = fault_target
        self.error = RuntimeError(
            "preview acceptance transaction acknowledgement lost"
            if fault_target == "acceptance"
            else "preview page transaction acknowledgement lost"
        )
        self.observations: list[_PreviewPageUpdateObservation] = []
        self.acceptance_observations: list[_PreviewAcceptanceDmlObservation] = []
        self.acceptance_params: list[tuple[str, tuple[object, ...]]] = []
        self.acceptance_receipt_row: dict[str, object] | None = None
        self.acceptance_fault_armed = False
        self._http_phase = "pre_http"
        self._http_transition_count = 0
        self.post_http_entered = asyncio.Event()
        self.outcome_reached = asyncio.Event()
        self.outcome_release = asyncio.Event()
        self.exit_phases: list[tuple[str, int, object]] = []
        self.raised_errors: list[RuntimeError] = []
        self.recovery_sql: list[tuple[str, str]] = []
        self.recovery_business_marker_trace: list[str] = []
        self.recovery_control_sql: list[str] = []
        self.expected_replay_idempotency_key_hash = (
            _independent_command_idempotency_hash(
                8,
                "cold_start.preview",
                expected_replay_idempotency_key,
            )
            if expected_replay_idempotency_key is not None
            else None
        )
        self.accepted_plan_id: UUID | None = None
        self._fault_budget = 1

    @property
    def fault_budget(self) -> int:
        return self._fault_budget

    @property
    def http_phase(self) -> str:
        return self._http_phase

    @property
    def http_transition_count(self) -> int:
        return self._http_transition_count

    def transition_to_post_http(self) -> None:
        assert self._http_phase == "pre_http"
        assert self._http_transition_count == 0
        self._http_phase = "post_http"
        self._http_transition_count = 1
        self.post_http_entered.set()

    def claim_fault(self) -> bool:
        if self._fault_budget == 0:
            return False
        assert self._fault_budget == 1
        self._fault_budget = 0
        return True

    def capture_acceptance_receipt_row(self, row: object) -> None:
        assert type(row) is dict and set(row) == _COMMAND_RECEIPT_ROW_KEYS
        assert self.acceptance_receipt_row is None
        self.acceptance_receipt_row = dict(row)

    def bind_accepted_plan_id(self, plan_id: UUID) -> None:
        assert type(plan_id) is UUID
        if self.accepted_plan_id is None:
            self.accepted_plan_id = plan_id
        else:
            assert self.accepted_plan_id == plan_id

    def replay_expectation(self) -> _CommandReplayExpectation:
        assert self.expected_replay_idempotency_key_hash is not None
        assert self.accepted_plan_id is not None
        return _CommandReplayExpectation(
            account_id=8,
            command_name="cold_start.preview",
            idempotency_key_hash=self.expected_replay_idempotency_key_hash,
            plan_id=self.accepted_plan_id,
        )


class _PreviewAckLossRollbackSentinel(RuntimeError):
    pass


class _PreviewAckLossTransaction:
    def __init__(
        self,
        connection: _PreviewAckLossConnection,
        transaction: Any,
    ) -> None:
        self._connection = connection
        self._transaction = transaction
        self._armed: (
            _PreviewPageUpdateObservation | _PreviewAcceptanceDmlObservation | None
        ) = None

    async def __aenter__(self) -> object:
        value = await self._transaction.__aenter__()
        assert self._connection._active_transaction is None
        self._connection._active_transaction = self
        return value

    def arm(self, observation: _PreviewPageUpdateObservation) -> None:
        assert self._connection._active_transaction is self
        assert self._armed is None
        assert self._connection._state.fault_target == "preview_page"
        assert observation.armed is True
        self._armed = observation

    def arm_acceptance(
        self,
        observation: _PreviewAcceptanceDmlObservation,
    ) -> None:
        assert self._connection._active_transaction is self
        assert self._armed is None
        assert self._connection._state.fault_target == "acceptance"
        assert observation.marker == "cold_start_command_receipts.insert"
        assert observation.http_phase == "pre_http"
        self._armed = observation

    @property
    def armed(self) -> bool:
        return self._armed is not None

    async def __aexit__(
        self,
        error_type: object,
        error: object,
        traceback: object,
    ) -> object:
        should_fault = error_type is None and self._armed is not None
        try:
            if not should_fault:
                return await self._transaction.__aexit__(
                    error_type,
                    error,
                    traceback,
                )
            assert self._armed is not None
            if self._connection._state.mode == "commit_then_raise":
                await self._transaction.__aexit__(None, None, None)
            else:
                sentinel = _PreviewAckLossRollbackSentinel(
                    "force a real rollback before acknowledgement loss"
                )
                await self._transaction.__aexit__(
                    type(sentinel),
                    sentinel,
                    sentinel.__traceback__,
                )
            assert (
                self._connection._connection.info.transaction_status
                is TransactionStatus.IDLE
            )
            self._connection._state.exit_phases.append(
                (
                    self._connection._state.mode,
                    self._armed.backend_pid,
                    self._connection._connection.info.transaction_status,
                )
            )
            self._connection._active_transaction = None
            self._connection._state.outcome_reached.set()
            await self._connection._state.outcome_release.wait()
            self._connection._state.raised_errors.append(self._connection._state.error)
            raise self._connection._state.error
        finally:
            if self._connection._active_transaction is self:
                self._connection._active_transaction = None


class _PreviewAckLossConnection:
    def __init__(
        self,
        connection: Any,
        state: _PreviewAckLossFaultState,
        *,
        phase: str,
    ) -> None:
        if phase not in {"origin", "recovery"}:
            raise ValueError("unknown preview ACK-loss connection phase")
        self._connection = connection
        self._state = state
        self._phase = phase
        self._backend_pid = connection.info.backend_pid
        assert type(self._backend_pid) is int
        self._active_transaction: _PreviewAckLossTransaction | None = None
        self._initial_target_armed = False
        self._recovery_update_seen = False
        self._acceptance_plan_identity: tuple[UUID, int, int] | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def transaction(self, *args: object, **kwargs: object) -> Any:
        return _PreviewAckLossTransaction(
            self,
            self._connection.transaction(*args, **kwargs),
        )

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        target = _is_preview_page_plan_update(rendered, params)
        acceptance_marker: str | None = None
        acceptance_digest: str | None = None
        if self._phase == "origin":
            if self._state.http_phase == "pre_http":
                if target:
                    raise AssertionError(
                        "preview update attempted before HTTP completion"
                    )
                if not _is_allowed_apply_read_sql(rendered):
                    acceptance_marker = _preview_acceptance_dml_marker(
                        rendered,
                        params,
                    )
                    acceptance_index = len(self._state.acceptance_observations)
                    if (
                        acceptance_marker is None
                        or acceptance_index >= len(_PREVIEW_ACCEPTANCE_DML_ORDER)
                        or acceptance_marker
                        != _PREVIEW_ACCEPTANCE_DML_ORDER[acceptance_index]
                    ):
                        raise AssertionError("unexpected origin pre-HTTP SQL")
                    if (
                        self._active_transaction is None
                        or self._connection.info.transaction_status
                        is not TransactionStatus.INTRANS
                    ):
                        raise AssertionError(
                            "preview acceptance DML attempted outside active transaction"
                        )
                    assert type(params) is tuple
                    if acceptance_marker == "sync_cold_start_plans.insert":
                        if self._acceptance_plan_identity is not None:
                            raise AssertionError("unexpected origin pre-HTTP SQL")
                    else:
                        if self._acceptance_plan_identity is None or (
                            params[1],
                            params[7],
                            params[9],
                        ) != (
                            self._acceptance_plan_identity[1],
                            str(self._acceptance_plan_identity[0]),
                            self._acceptance_plan_identity[2],
                        ):
                            raise AssertionError("unexpected origin pre-HTTP SQL")
                    tokens = _sql_tokens(rendered)
                    assert tokens is not None and ";" not in tokens
                    acceptance_digest = hashlib.sha256(
                        "\x00".join(tokens).encode("utf-8")
                    ).hexdigest()
            else:
                assert self._state.http_phase == "post_http"
                if self._initial_target_armed:
                    if target or not _is_allowed_apply_read_sql(rendered):
                        raise AssertionError(
                            "unexpected SQL after armed preview update"
                        )
                elif not target and not _is_allowed_apply_read_sql(rendered):
                    raise AssertionError("unexpected origin post-HTTP SQL")
        if target and (
            self._active_transaction is None
            or self._connection.info.transaction_status is not TransactionStatus.INTRANS
        ):
            raise AssertionError("preview update attempted outside active transaction")
        if self._phase == "recovery":
            if target:
                if (
                    self._state.mode != "rollback_then_raise"
                    or self._recovery_update_seen
                ):
                    raise AssertionError("unexpected preview recovery mutation")
                recovery_marker = "preview_page_update"
                self._recovery_update_seen = True
            else:
                if not _is_allowed_apply_read_sql(rendered):
                    raise AssertionError("unexpected preview recovery SQL")
                if self._state.fault_target == "acceptance":
                    marker = _command_replay_marker(
                        rendered,
                        params,
                        self._state.replay_expectation(),
                    )
                    if marker is None:
                        raise AssertionError(
                            "unexpected preview recovery business relation"
                        )
                    if marker == "control":
                        self._state.recovery_control_sql.append(rendered)
                    else:
                        if not _command_replay_marker_is_next(
                            self._state.recovery_business_marker_trace,
                            marker,
                        ):
                            raise AssertionError(
                                "unexpected preview recovery business relation"
                            )
                        self._state.recovery_business_marker_trace.append(marker)
                recovery_marker = "read_control"
            self._state.recovery_sql.append((recovery_marker, rendered))
        cursor = await self._connection.execute(statement, params)
        if acceptance_marker is not None:
            assert acceptance_digest is not None
            assert type(params) is tuple
            assert cursor.rowcount == 1
            assert self._connection.info.transaction_status is TransactionStatus.INTRANS
            assert self._active_transaction is not None
            identity_cursor = await self._connection.execute(
                "SELECT "
                "pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text "
                "AS assigned_transaction_id, "
                "pg_catalog.pg_current_xact_id()::pg_catalog.text "
                "AS transaction_id, "
                "pg_catalog.pg_backend_pid() AS backend_pid"
            )
            identity_row = await identity_cursor.fetchone()
            assert type(identity_row) is dict and set(identity_row) == {
                "assigned_transaction_id",
                "transaction_id",
                "backend_pid",
            }
            assigned_transaction_id = identity_row["assigned_transaction_id"]
            transaction_id = identity_row["transaction_id"]
            backend_pid = identity_row["backend_pid"]
            assert (
                type(assigned_transaction_id) is str
                and assigned_transaction_id.isascii()
                and assigned_transaction_id.isdigit()
                and assigned_transaction_id == transaction_id
                and type(backend_pid) is int
                and backend_pid == self._backend_pid
            )
            if acceptance_marker == "sync_cold_start_plans.insert":
                self._acceptance_plan_identity = (params[0], params[1], params[8])
                self._state.bind_accepted_plan_id(params[0])
            acceptance_observation = _PreviewAcceptanceDmlObservation(
                marker=acceptance_marker,
                statement_digest=acceptance_digest,
                rowcount=cursor.rowcount,
                transaction_id=transaction_id,
                backend_pid=backend_pid,
                transaction_status=self._connection.info.transaction_status,
                http_phase=self._state.http_phase,
            )
            assert type(params) is tuple
            self._state.acceptance_params.append((acceptance_marker, params))
            self._state.acceptance_observations.append(acceptance_observation)
            if (
                acceptance_marker == "cold_start_command_receipts.insert"
                and self._state.fault_target == "acceptance"
                and self._state.claim_fault()
            ):
                self._state.acceptance_fault_armed = True
                self._active_transaction.arm_acceptance(acceptance_observation)
            if acceptance_marker == "cold_start_command_receipts.insert":
                return _CapturedRowCursor(
                    cursor,
                    self._state.capture_acceptance_receipt_row,
                )
            return cursor
        if not target:
            return cursor
        assert self._state.http_phase == "post_http"
        assert cursor.rowcount == 1
        assert self._connection.info.transaction_status is TransactionStatus.INTRANS
        assert self._active_transaction is not None
        identity_cursor = await self._connection.execute(
            "SELECT "
            "pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text "
            "AS assigned_transaction_id, "
            "pg_catalog.pg_current_xact_id()::pg_catalog.text "
            "AS transaction_id, "
            "pg_catalog.pg_backend_pid() AS backend_pid"
        )
        row = await identity_cursor.fetchone()
        assert type(row) is dict and set(row) == {
            "assigned_transaction_id",
            "transaction_id",
            "backend_pid",
        }
        assigned_transaction_id = row["assigned_transaction_id"]
        transaction_id = row["transaction_id"]
        backend_pid = row["backend_pid"]
        assert (
            type(assigned_transaction_id) is str
            and assigned_transaction_id.isascii()
            and assigned_transaction_id.isdigit()
            and assigned_transaction_id == transaction_id
            and type(backend_pid) is int
            and backend_pid == self._connection.info.backend_pid
        )
        armed = self._state.fault_target == "preview_page" and self._state.claim_fault()
        observation = _PreviewPageUpdateObservation(
            marker="sync_cold_start_plans.preview_page_update",
            rowcount=cursor.rowcount,
            assigned_transaction_id=assigned_transaction_id,
            transaction_id=transaction_id,
            backend_pid=backend_pid,
            transaction_status=self._connection.info.transaction_status,
            http_phase=self._state.http_phase,
            armed=armed,
        )
        self._state.observations.append(observation)
        if armed:
            self._active_transaction.arm(observation)
            self._initial_target_armed = True
        return cursor


class _PreviewAckLossPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        state: _PreviewAckLossFaultState,
    ) -> None:
        self._pool = pool
        self._state = state
        self.checked_out_pids: list[int] = []
        self.checked_out_connections: list[_PreviewAckLossConnection] = []
        self.returned_pids: list[int] = []
        self.returned_closed: list[bool] = []

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _PreviewAckLossConnection:
        if len(self.checked_out_pids) >= 2:
            raise AssertionError("unexpected third preview recovery checkout")
        connection = await self._pool.getconn()
        backend_pid = connection.info.backend_pid
        assert type(backend_pid) is int
        phase = "origin" if not self.checked_out_pids else "recovery"
        self.checked_out_pids.append(backend_pid)
        guarded_connection = _PreviewAckLossConnection(
            connection,
            self._state,
            phase=phase,
        )
        self.checked_out_connections.append(guarded_connection)
        return guarded_connection

    async def putconn(self, connection: _PreviewAckLossConnection) -> None:
        if (
            connection._phase == "recovery"
            and self._state.fault_target == "acceptance"
            and tuple(self._state.recovery_business_marker_trace)
            != _COMMAND_REPLAY_MARKER_ORDER
        ):
            raise AssertionError("incomplete preview recovery business relation trace")
        self.returned_pids.append(connection._backend_pid)
        self.returned_closed.append(connection.closed is True)
        await self._pool.putconn(connection._connection)


@dataclass(frozen=True, slots=True)
class _ApproveAckLossDmlObservation:
    marker: str
    statement_digest: str
    rowcount: int
    transaction_id: str
    backend_pid: int
    transaction_status: object
    params: object


class _ApproveAckLossFaultState:
    def __init__(
        self,
        *,
        plan_id: UUID,
        ready_plan: dict[str, object],
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> None:
        assert type(plan_id) is UUID
        assert ready_plan["plan_id"] == str(plan_id)
        assert ready_plan["state"] == "ready"
        self.plan_id = plan_id
        self.ready_plan = dict(ready_plan)
        self.actor = actor
        self.reason = reason
        self.idempotency_key = idempotency_key
        self.error = RuntimeError("approve command transaction acknowledgement lost")
        self.observations: list[_ApproveAckLossDmlObservation] = []
        self.origin_sql: list[str] = []
        self.replay_sql: list[str] = []
        self.replay_business_marker_trace: list[str] = []
        self.replay_control_sql: list[str] = []
        self.locator_calls: list[str] = []
        self.approved_row: dict[str, Any] | None = None
        self.receipt_row: dict[str, object] | None = None
        self.outcome_reached = asyncio.Event()
        self.outcome_release = asyncio.Event()
        self.exit_phases: list[tuple[int, object]] = []
        self.raised_errors: list[RuntimeError] = []
        self._fault_budget = 1

    @property
    def fault_budget(self) -> int:
        return self._fault_budget

    def claim_fault(self) -> bool:
        if self._fault_budget == 0:
            return False
        assert self._fault_budget == 1
        self._fault_budget = 0
        return True

    def capture_approved_row(self, row: object) -> None:
        assert type(row) is dict
        assert row["plan_id"] == self.plan_id
        assert row["state"] == "approved"
        assert self.approved_row is None
        self.approved_row = dict(row)

    def capture_receipt_row(self, row: object) -> None:
        assert type(row) is dict and set(row) == _COMMAND_RECEIPT_ROW_KEYS
        assert self.receipt_row is None
        self.receipt_row = dict(row)

    def replay_expectation(self) -> _CommandReplayExpectation:
        return _CommandReplayExpectation(
            account_id=int(self.ready_plan["account_id"]),
            command_name="cold_start.approve",
            idempotency_key_hash=_independent_command_idempotency_hash(
                int(self.ready_plan["account_id"]),
                "cold_start.approve",
                self.idempotency_key,
            ),
            plan_id=self.plan_id,
        )


class _ApproveObservedCursor:
    def __init__(self, cursor: Any, state: _ApproveAckLossFaultState) -> None:
        self._cursor = cursor
        self._state = state

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    async def fetchone(self) -> object:
        row = await self._cursor.fetchone()
        self._state.capture_approved_row(row)
        return row


class _ApproveAckLossTransaction:
    def __init__(self, connection: _ApproveAckLossConnection, transaction: Any) -> None:
        self._connection = connection
        self._transaction = transaction
        self._armed: _ApproveAckLossDmlObservation | None = None

    async def __aenter__(self) -> object:
        value = await self._transaction.__aenter__()
        assert self._connection._active_transaction is None
        self._connection._active_transaction = self
        return value

    def arm(self, observation: _ApproveAckLossDmlObservation) -> None:
        assert self._connection._active_transaction is self
        assert self._armed is None
        assert observation.marker == "approve_command_receipts.insert"
        self._armed = observation

    async def __aexit__(
        self,
        error_type: object,
        error: object,
        traceback: object,
    ) -> object:
        should_fault = error_type is None and self._armed is not None
        try:
            if not should_fault:
                return await self._transaction.__aexit__(
                    error_type,
                    error,
                    traceback,
                )
            assert self._armed is not None
            await self._transaction.__aexit__(None, None, None)
            assert (
                self._connection._connection.info.transaction_status
                is TransactionStatus.IDLE
            )
            self._connection._active_transaction = None
            self._connection._state.exit_phases.append(
                (self._armed.backend_pid, TransactionStatus.IDLE)
            )
            self._connection._state.outcome_reached.set()
            await self._connection._state.outcome_release.wait()
            self._connection._state.raised_errors.append(self._connection._state.error)
            raise self._connection._state.error
        finally:
            if self._connection._active_transaction is self:
                self._connection._active_transaction = None


def _approve_ack_loss_params_match_state(
    marker: str,
    params: object,
    state: _ApproveAckLossFaultState,
) -> bool:
    ready = state.ready_plan
    if marker == "sync_cold_start_plans.approve_update":
        return params == {
            "plan_id": state.plan_id,
            "account_id": ready["account_id"],
            "folder_key": ready["folder_key"],
            "expected_version": ready["version"],
            "expected_plan_hash": ready["plan_hash"],
            "expected_boundary_cursor": ready["boundary_cursor"],
            "expected_boundary_cursor_version": ready["boundary_cursor_version"],
        }
    approved = state.approved_row
    if approved is None:
        return False
    if marker == "audit_events.approve_insert":
        expected_metadata = {
            "plan_id": str(state.plan_id),
            "plan_hash": approved["plan_hash"],
            "page_count": approved["page_count"],
            "item_count": approved["item_count"],
            "redacted_samples": approved["redacted_samples"],
        }
        return (
            type(params) is tuple
            and len(params) == 8
            and type(params[0]) is UUID
            and params[1]
            == _independent_canonical_digest(
                "cold-start.audit-event.v1",
                {
                    "v": 1,
                    "action": "cold_start.approve",
                    "plan_id": str(state.plan_id),
                    "plan_version": ready["version"],
                },
            )
            and params[2] == ready["account_id"]
            and params[3]
            == _independent_canonical_digest(
                "cold-start.audit-object.v1",
                {"v": 1, "plan_id": str(state.plan_id)},
            )
            and params[4] == "approved"
            and params[5] == state.actor
            and params[6] == state.reason
            and type(params[7]) is Jsonb
            and params[7].obj == expected_metadata
        )
    if marker == "approve_command_receipts.insert":
        payload_hash, idempotency_hash, result_hash = (
            _independent_approve_command_hashes(
                approved=approved,
                actor=state.actor,
                reason=state.reason,
                idempotency_key=state.idempotency_key,
            )
        )
        return (
            type(params) is tuple
            and len(params) == 10
            and type(params[0]) is UUID
            and params[1:]
            == (
                ready["account_id"],
                "cold_start.approve",
                idempotency_hash,
                payload_hash,
                "succeeded",
                "sync_cold_start_plan",
                str(state.plan_id),
                result_hash,
                ready["fencing_token"],
            )
        )
    return False


class _ApproveAckLossConnection:
    def __init__(
        self,
        connection: Any,
        state: _ApproveAckLossFaultState,
        *,
        phase: str,
    ) -> None:
        if phase not in {"locator_origin", "origin", "locator_replay", "replay"}:
            raise ValueError("unknown approve ACK-loss phase")
        self._connection = connection
        self._state = state
        self._phase = phase
        self._backend_pid = connection.info.backend_pid
        assert type(self._backend_pid) is int
        self._active_transaction: _ApproveAckLossTransaction | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def transaction(self, *args: object, **kwargs: object) -> Any:
        return _ApproveAckLossTransaction(
            self,
            self._connection.transaction(*args, **kwargs),
        )

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        if self._phase.startswith("locator_"):
            if (
                _sql_tokens(rendered) != _APPLY_ACK_LOSS_LOCATOR_TOKENS
                or params != (self._state.plan_id,)
                or self._active_transaction is not None
                or self._connection.info.transaction_status
                is not TransactionStatus.IDLE
                or self._phase in self._state.locator_calls
            ):
                raise AssertionError("unexpected approve ACK-loss locator SQL")
            self._state.locator_calls.append(self._phase)
            cursor = await self._connection.execute(statement, params)
            assert self._connection.info.transaction_status is TransactionStatus.IDLE
            return cursor

        marker = _approve_ack_loss_dml_marker(rendered, params)
        allowed_read = _is_allowed_apply_read_sql(rendered)
        if _has_sql_separator(rendered):
            raise AssertionError("unexpected approve ACK-loss SQL boundary")
        if self._phase == "replay":
            if marker is not None or not allowed_read:
                raise AssertionError("unexpected approve replay mutation")
            replay_marker = _command_replay_marker(
                rendered,
                params,
                self._state.replay_expectation(),
            )
            if replay_marker is None:
                raise AssertionError("unexpected approve replay business relation")
            if replay_marker == "control":
                self._state.replay_control_sql.append(rendered)
            else:
                if not _command_replay_marker_is_next(
                    self._state.replay_business_marker_trace,
                    replay_marker,
                ):
                    raise AssertionError("unexpected approve replay business relation")
                self._state.replay_business_marker_trace.append(replay_marker)
            self._state.replay_sql.append(rendered)
            return await self._connection.execute(statement, params)

        assert self._phase == "origin"
        if marker is None:
            if not allowed_read:
                raise AssertionError("unexpected approve origin SQL")
            self._state.origin_sql.append(rendered)
            return await self._connection.execute(statement, params)
        marker_index = len(self._state.observations)
        if (
            marker_index >= len(_APPROVE_COMMAND_DML_ORDER)
            or marker != _APPROVE_COMMAND_DML_ORDER[marker_index]
            or not _approve_ack_loss_params_match_state(marker, params, self._state)
        ):
            raise AssertionError("unexpected approve ACK-loss DML")
        if (
            self._active_transaction is None
            or self._connection.info.transaction_status is not TransactionStatus.INTRANS
        ):
            raise AssertionError("approve DML attempted outside active transaction")

        cursor = await self._connection.execute(statement, params)
        assert cursor.rowcount == 1
        identity_cursor = await self._connection.execute(
            "SELECT "
            "pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text "
            "AS assigned_transaction_id, "
            "pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id, "
            "pg_catalog.pg_backend_pid() AS backend_pid"
        )
        identity_row = await identity_cursor.fetchone()
        assert type(identity_row) is dict
        transaction_id = identity_row["transaction_id"]
        assert (
            type(transaction_id) is str
            and transaction_id == identity_row["assigned_transaction_id"]
            and identity_row["backend_pid"] == self._backend_pid
        )
        tokens = _sql_tokens(rendered)
        assert tokens is not None
        observation = _ApproveAckLossDmlObservation(
            marker=marker,
            statement_digest=hashlib.sha256(
                "\x00".join(tokens).encode("utf-8")
            ).hexdigest(),
            rowcount=cursor.rowcount,
            transaction_id=transaction_id,
            backend_pid=self._backend_pid,
            transaction_status=self._connection.info.transaction_status,
            params=params,
        )
        self._state.observations.append(observation)
        if marker == "sync_cold_start_plans.approve_update":
            return _ApproveObservedCursor(cursor, self._state)
        if marker == "approve_command_receipts.insert" and self._state.claim_fault():
            self._active_transaction.arm(observation)
        if marker == "approve_command_receipts.insert":
            return _CapturedRowCursor(cursor, self._state.capture_receipt_row)
        return cursor


class _ApproveAckLossPool:
    _PHASES = ("locator_origin", "origin", "locator_replay", "replay")

    def __init__(
        self, pool: AsyncConnectionPool, state: _ApproveAckLossFaultState
    ) -> None:
        self._pool = pool
        self._state = state
        self.checked_out_pids: list[int] = []
        self.checked_out_connections: list[_ApproveAckLossConnection] = []
        self.returned_pids: list[int] = []
        self.returned_closed: list[bool] = []

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _ApproveAckLossConnection:
        index = len(self.checked_out_pids)
        if index >= len(self._PHASES):
            raise AssertionError("unexpected fifth approve ACK-loss checkout")
        connection = await self._pool.getconn()
        guarded = _ApproveAckLossConnection(
            connection,
            self._state,
            phase=self._PHASES[index],
        )
        self.checked_out_pids.append(guarded._backend_pid)
        self.checked_out_connections.append(guarded)
        return guarded

    async def putconn(self, connection: _ApproveAckLossConnection) -> None:
        if (
            connection._phase == "replay"
            and tuple(self._state.replay_business_marker_trace)
            != _COMMAND_REPLAY_MARKER_ORDER
        ):
            raise AssertionError("incomplete approve replay business relation trace")
        self.returned_pids.append(connection._backend_pid)
        self.returned_closed.append(connection.closed is True)
        await self._pool.putconn(connection._connection)


class _BlockingColdStartOrigin:
    def __init__(
        self,
        outcome: SyncBatch,
        state: _PreviewAckLossFaultState,
    ) -> None:
        self._outcome: SyncBatch | None = outcome
        self._state = state
        self.calls: list[tuple[int, str, str | None, int]] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    @property
    def exhausted(self) -> bool:
        return self._outcome is None

    async def fetch_cold_start_page(
        self,
        account_id: int,
        sync_folder: str,
        cursor: str | None,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, sync_folder, cursor, limit))
        if self._outcome is None or len(self.calls) != 1 or cursor is not None:
            raise AssertionError("unexpected blocking cold-start Origin request")
        assert self._state.http_phase == "pre_http"
        assert self._state.http_transition_count == 0
        self.entered.set()
        await self.release.wait()
        assert self._state.http_phase == "pre_http"
        self._state.transition_to_post_http()
        outcome = self._outcome
        self._outcome = None
        return outcome


@dataclass(frozen=True, slots=True)
class _ApplyAckLossDmlObservation:
    phase: str
    marker: str
    rowcount: int
    assigned_transaction_id: str
    transaction_id: str
    backend_pid: int
    transaction_status: object
    http_phase: str
    armed: bool
    stable_parameter_projection: object


@dataclass(frozen=True, slots=True)
class _ApplyAckLossExpectedContract:
    account_id: int
    folder_key: str
    plan_id: UUID
    boundary_cursor: str
    terminal_cursor: str
    external_email_id: str
    source_version: str
    pipeline_name: str
    generation: int
    fencing_token: int
    plan_hash: str
    contract_fingerprint: str
    config_hash: str
    dedupe_key: str
    apply_payload_hash: str
    receipt_idempotency_hash: str
    batch_result_hash: str


class _ApplyAckLossFaultState:
    def __init__(self, mode: str) -> None:
        if mode not in {"commit_then_raise", "rollback_then_raise"}:
            raise ValueError("unknown apply ACK-loss mode")
        self.mode = mode
        self.error = RuntimeError("terminal apply transaction acknowledgement lost")
        self.observations: list[_ApplyAckLossDmlObservation] = []
        self._http_phase = "pre_http"
        self._http_transition_count = 0
        self.post_http_entered = asyncio.Event()
        self.outcome_reached = asyncio.Event()
        self.outcome_release = asyncio.Event()
        self.exit_phases: list[tuple[str, int, object]] = []
        self.raised_errors: list[RuntimeError] = []
        self.recovery_sql: list[tuple[str, str]] = []
        self.executed_params: dict[str, list[tuple[str, object]]] = {
            "origin": [],
            "recovery": [],
        }
        self.expected_locator_plan_id: UUID | None = None
        self.locator_calls = 0
        self.expected_contract: _ApplyAckLossExpectedContract | None = None
        self._fault_budget = 1

    @property
    def http_phase(self) -> str:
        return self._http_phase

    @property
    def http_transition_count(self) -> int:
        return self._http_transition_count

    @property
    def fault_budget(self) -> int:
        return self._fault_budget

    def transition_to_post_http(self) -> None:
        assert self._http_phase == "pre_http"
        assert self._http_transition_count == 0
        self._http_phase = "post_http"
        self._http_transition_count = 1
        self.post_http_entered.set()

    def claim_fault(self) -> bool:
        if self._fault_budget == 0:
            return False
        assert self._fault_budget == 1
        self._fault_budget = 0
        return True

    def bind_locator_plan_id(self, plan_id: UUID) -> None:
        assert self._http_phase == "pre_http"
        assert self._http_transition_count == 0
        assert self.locator_calls == 0
        assert self.expected_locator_plan_id is None
        assert self.expected_contract is None
        assert not self.observations
        assert type(plan_id) is UUID
        self.expected_locator_plan_id = plan_id

    def bind_expected_contract(
        self,
        expected: _ApplyAckLossExpectedContract,
    ) -> None:
        assert self._http_phase == "pre_http"
        assert self._http_transition_count == 0
        assert not self.observations
        assert self.expected_contract is None
        assert type(expected) is _ApplyAckLossExpectedContract
        self.expected_contract = expected


class _ApplyAckLossRollbackSentinel(RuntimeError):
    pass


class _ApplyAckLossTransaction:
    def __init__(
        self,
        connection: _ApplyAckLossConnection,
        transaction: Any,
    ) -> None:
        self._connection = connection
        self._transaction = transaction
        self._armed: _ApplyAckLossDmlObservation | None = None

    async def __aenter__(self) -> object:
        value = await self._transaction.__aenter__()
        assert self._connection._active_transaction is None
        self._connection._active_transaction = self
        return value

    def arm(self, observation: _ApplyAckLossDmlObservation) -> None:
        assert self._connection._active_transaction is self
        assert self._armed is None
        assert observation.phase == "origin"
        assert observation.marker == "cold_start_command_receipts.insert"
        assert observation.armed is True
        self._armed = observation

    @property
    def armed(self) -> bool:
        return self._armed is not None

    async def __aexit__(
        self,
        error_type: object,
        error: object,
        traceback: object,
    ) -> object:
        should_fault = error_type is None and self._armed is not None
        try:
            if not should_fault:
                return await self._transaction.__aexit__(
                    error_type,
                    error,
                    traceback,
                )
            assert self._armed is not None
            if self._connection._state.mode == "commit_then_raise":
                await self._transaction.__aexit__(None, None, None)
            else:
                sentinel = _ApplyAckLossRollbackSentinel(
                    "force a real terminal apply rollback before acknowledgement loss"
                )
                await self._transaction.__aexit__(
                    type(sentinel),
                    sentinel,
                    sentinel.__traceback__,
                )
            assert (
                self._connection._connection.info.transaction_status
                is TransactionStatus.IDLE
            )
            self._connection._state.exit_phases.append(
                (
                    self._connection._state.mode,
                    self._armed.backend_pid,
                    self._connection._connection.info.transaction_status,
                )
            )
            self._connection._active_transaction = None
            self._connection._state.outcome_reached.set()
            await self._connection._state.outcome_release.wait()
            self._connection._state.raised_errors.append(self._connection._state.error)
            raise self._connection._state.error
        finally:
            if self._connection._active_transaction is self:
                self._connection._active_transaction = None


_APPLY_ACK_LOSS_LOCATOR_SQL = (
    "SELECT plan_id, account_id, folder_key "
    "FROM public.sync_cold_start_plans WHERE plan_id = %s"
)
_APPLY_ACK_LOSS_LOCATOR_TOKENS = _sql_tokens(_APPLY_ACK_LOSS_LOCATOR_SQL)


def _is_canonical_uuid_text(value: object) -> bool:
    if type(value) is not str:
        return False
    try:
        parsed = UUID(value)
    except ValueError:
        return False
    return str(parsed) == value


def _independent_canonical_digest(domain: str, projection: dict[str, object]) -> str:
    encoded = json.dumps(
        projection,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(domain.encode("ascii") + b"\x00" + encoded).hexdigest()


def _independent_command_idempotency_hash(
    account_id: int,
    command_name: str,
    idempotency_key: str,
) -> str:
    return hashlib.sha256(
        b"pipeline-command-idempotency-v1\x00"
        + f"{account_id}\x00{command_name}\x00{idempotency_key}".encode("utf-8")
    ).hexdigest()


def _independent_digest_timestamp(value: object) -> str:
    if type(value) is str:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        assert type(value) is datetime
        parsed = value
    assert parsed.tzinfo is not None
    return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _command_receipt_projection(row: object) -> dict[str, object]:
    assert type(row) is dict and _COMMAND_RECEIPT_ROW_KEYS <= set(row)
    receipt_id = row["id"]
    if type(receipt_id) is UUID:
        normalized_id = str(receipt_id)
    else:
        assert _is_canonical_uuid_text(receipt_id)
        normalized_id = receipt_id
    created_at = row["created_at"]
    if type(created_at) is str:
        parsed_created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    else:
        assert type(created_at) is datetime
        parsed_created_at = created_at
    assert (
        parsed_created_at.tzinfo is not None
        and parsed_created_at.utcoffset() == timedelta(0)
    )
    projection = {
        "id": normalized_id,
        "account_id": row["account_id"],
        "command_name": row["command_name"],
        "idempotency_key_hash": row["idempotency_key_hash"],
        "canonical_payload_hash": row["canonical_payload_hash"],
        "outcome": row["outcome"],
        "result_type": row["result_type"],
        "result_id": row["result_id"],
        "result_hash": row["result_hash"],
        "authority_epoch": row["authority_epoch"],
        "created_at": _independent_digest_timestamp(parsed_created_at),
    }
    assert set(projection) == _COMMAND_RECEIPT_ROW_KEYS
    return projection


def _independent_preview_command_hashes(
    *,
    plan: dict[str, object],
    actor: str,
    reason: str,
    idempotency_key: str,
) -> tuple[str, str, str]:
    payload_hash = _independent_canonical_digest(
        "cold-start.preview-payload.v1",
        {
            "v": 1,
            "account_id": plan["account_id"],
            "canonical_folder": plan["folder_key"],
            "actor": actor,
            "reason": reason,
        },
    )
    idempotency_hash = _independent_command_idempotency_hash(
        int(plan["account_id"]),
        "cold_start.preview",
        idempotency_key,
    )
    result_hash = _independent_canonical_digest(
        "cold-start.preview-result.v1",
        {
            "v": 1,
            "plan_id": plan["plan_id"],
            "account_id": plan["account_id"],
            "canonical_folder": plan["folder_key"],
            "expected_cursor_status": plan["expected_cursor_status"],
            "expected_cursor_version": plan["expected_cursor_version"],
            "expected_cursor_hash": None,
            "pipeline_name": plan["pipeline_name"],
            "generation": plan["generation"],
            "fencing_token": plan["fencing_token"],
            "contract_fingerprint": plan["contract_fingerprint"],
            "folder_scope_config_hash": plan["folder_scope_config_hash"],
            "created_at": _independent_digest_timestamp(plan["created_at"]),
            "expires_at": _independent_digest_timestamp(plan["expires_at"]),
        },
    )
    return payload_hash, idempotency_hash, result_hash


def _independent_first_preview_page_hashes(
    *,
    account_id: int,
    cursor: str,
    external_email_id: str,
) -> tuple[str, str]:
    sample_hash = _independent_canonical_digest(
        "cold-start.sample-external-id.v1",
        {
            "v": 1,
            "account_id": account_id,
            "external_email_id": external_email_id,
        },
    )
    batch_hash = _independent_canonical_digest(
        "cold-start.batch.v1",
        {
            "v": 1,
            "contract_version": "exchange_sync_contract_v2",
            "cursor": cursor,
            "includes_last": False,
            "changes": [
                {
                    "kind": "create",
                    "external_email_id": external_email_id,
                    "source_version": "version-1",
                    "item": {
                        "id": external_email_id,
                        "subject": "safe subject",
                    },
                }
            ],
        },
    )
    rolling_hash = hashlib.sha256(
        b"cold-start.preview-rolling.v1\x00"
        + b"0" * 64
        + b"\x00"
        + batch_hash.encode("ascii")
    ).hexdigest()
    return sample_hash, rolling_hash


def _independent_approve_command_hashes(
    *,
    approved: dict[str, object],
    actor: str,
    reason: str,
    idempotency_key: str,
) -> tuple[str, str, str]:
    plan_id = approved["plan_id"]
    assert type(plan_id) is UUID
    account_id = approved["account_id"]
    assert type(account_id) is int
    payload_hash = _independent_canonical_digest(
        "cold-start.approve-payload.v1",
        {
            "v": 1,
            "plan_id": str(plan_id),
            "actor": actor,
            "reason": reason,
        },
    )
    idempotency_hash = _independent_command_idempotency_hash(
        account_id,
        "cold_start.approve",
        idempotency_key,
    )
    result_hash = _independent_canonical_digest(
        "cold-start.approve-result.v1",
        {
            "v": 1,
            "plan_id": str(plan_id),
            "plan_hash": approved["plan_hash"],
            "pipeline_name": approved["pipeline_name"],
            "generation": approved["generation"],
            "fencing_token": approved["fencing_token"],
            "folder_scope_config_hash": approved["folder_scope_config_hash"],
            "approved_at": _independent_digest_timestamp(approved["approved_at"]),
        },
    )
    return payload_hash, idempotency_hash, result_hash


def _independent_apply_ack_loss_hashes(
    *,
    account_id: int,
    folder_key: str,
    plan_id: UUID,
    boundary_cursor: str,
    terminal_cursor: str,
    external_email_id: str,
    source_version: str,
) -> tuple[str, str, str, str]:
    dedupe_projection = {
        "schema_version": 1,
        "account_id": account_id,
        "source": "sync",
        "raw_event_type": "create",
        "kind": "create",
        "external_email_id": external_email_id,
        "folder": folder_key,
        "source_version": source_version,
        "cursor": terminal_cursor,
        "source_event_at": None,
        "raw_body_sha256": None,
    }
    dedupe_key = hashlib.sha256(
        json.dumps(
            dedupe_projection,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    cursor_hash = _independent_canonical_digest(
        "cold-start.cursor.v1",
        {"v": 1, "cursor": boundary_cursor},
    )
    apply_payload_hash = _independent_canonical_digest(
        "cold-start.apply-page-payload.v1",
        {
            "v": 1,
            "command_name": "cold_start.apply_page",
            "account_id": account_id,
            "canonical_folder": folder_key,
            "plan_id": str(plan_id),
            "plan_version": 2,
            "cursor_status": "cold_start_pending",
            "cursor_version": 0,
            "request_cursor_hash": cursor_hash,
        },
    )
    receipt_idempotency_hash = hashlib.sha256(
        b"pipeline-command-idempotency-v1\x00"
        + f"{account_id}\x00cold_start.apply_page\x00{apply_payload_hash}".encode(
            "utf-8"
        )
    ).hexdigest()
    batch_hash = _independent_canonical_digest(
        "cold-start.batch.v1",
        {
            "v": 1,
            "contract_version": "exchange_sync_contract_v2",
            "cursor": terminal_cursor,
            "includes_last": True,
            "changes": [
                {
                    "kind": "create",
                    "external_email_id": external_email_id,
                    "source_version": source_version,
                    "item": {
                        "id": external_email_id,
                        "subject": "safe subject",
                    },
                }
            ],
        },
    )
    batch_result_hash = _independent_canonical_digest(
        "cold-start.apply-page-result.v1",
        {"v": 1, "batch_hash": batch_hash},
    )
    return (
        dedupe_key,
        apply_payload_hash,
        receipt_idempotency_hash,
        batch_result_hash,
    )


def _apply_ack_loss_params_are_exact(
    marker: str,
    params: object,
    *,
    expected: _ApplyAckLossExpectedContract,
    prior: list[tuple[str, object]],
) -> bool:
    if marker == "event_inbox.insert":
        if type(params) is not tuple or len(params) != 16 or prior:
            return False
        payload = params[10]
        expected_payload = {
            "cursor": expected.terminal_cursor,
            "change_type": "create",
            "id": expected.external_email_id,
            "item": {
                "id": expected.external_email_id,
                "subject": "safe subject",
            },
            "source_version": expected.source_version,
        }
        return (
            _is_canonical_uuid_text(params[0])
            and type(params[1]) is int
            and params[1] == expected.account_id
            and type(params[2]) is str
            and params[2] == expected.external_email_id
            and type(params[3]) is str
            and params[3] == expected.folder_key
            and type(params[4]) is str
            and params[4] == "sync"
            and type(params[5]) is str
            and params[5] == "create"
            and type(params[6]) is str
            and params[6] == "create"
            and type(params[7]) is str
            and params[7] == expected.dedupe_key
            and type(params[8]) is str
            and params[8] == expected.source_version
            and params[9] is None
            and type(payload) is Jsonb
            and type(payload.obj) is dict
            and payload.obj == expected_payload
            and type(params[11]) is str
            and params[11] == "full"
            and type(params[12]) is str
            and params[12] == expected.pipeline_name
            and type(params[13]) is int
            and params[13] == expected.generation
            and type(params[14]) is int
            and params[14] == expected.fencing_token
            and type(params[15]) is str
            and params[15] == "pending"
        )

    by_marker = dict(prior)
    event_params = by_marker.get("event_inbox.insert")
    if type(event_params) is not tuple or len(event_params) != 16:
        return False
    if marker == "sync_cursors.update":
        values = _exact_parameter_dict(params, _APPLY_CURSOR_PARAM_KEYS)
        return (
            len(prior) == 1
            and values is not None
            and type(values["account_id"]) is int
            and values["account_id"] == expected.account_id
            and type(values["folder_key"]) is str
            and values["folder_key"] == expected.folder_key
            and type(values["next_cursor"]) is str
            and values["next_cursor"] == expected.terminal_cursor
            and type(values["target_status"]) is str
            and values["target_status"] == "active"
            and values["target_plan_id"] is None
            and values["target_plan_state"] is None
            and type(values["database_stamp"]) is datetime
            and values["database_stamp"].tzinfo is not None
            and type(values["expected_status"]) is str
            and values["expected_status"] == "cold_start_pending"
            and values["expected_cursor"] is None
            and type(values["expected_version"]) is int
            and values["expected_version"] == 0
            and type(values["expected_blocked_reason_code"]) is str
            and values["expected_blocked_reason_code"] == "sync.cold_start_required"
            and values["expected_contract_fingerprint"] is None
            and values["expected_blocked_at"] is None
            and type(values["expected_transient_failures"]) is int
            and values["expected_transient_failures"] == 0
            and values["expected_retry_after_at"] is None
            and values["expected_plan_id"] is None
            and values["expected_plan_state"] is None
        )
    cursor_params = by_marker.get("sync_cursors.update")
    if type(cursor_params) is not dict:
        return False
    if marker == "sync_cold_start_plans.update":
        values = _exact_parameter_dict(params, _APPLY_PLAN_PARAM_KEYS)
        return (
            len(prior) == 2
            and values is not None
            and type(values["account_id"]) is int
            and values["account_id"] == expected.account_id
            and type(values["folder_key"]) is str
            and values["folder_key"] == expected.folder_key
            and type(values["plan_id"]) is UUID
            and values["plan_id"] == expected.plan_id
            and type(values["target_state"]) is str
            and values["target_state"] == "completed"
            and values["terminal"] is True
            and type(values["next_cursor"]) is str
            and values["next_cursor"] == expected.terminal_cursor
            and type(values["next_cursor_version"]) is int
            and values["next_cursor_version"] == 1
            and type(values["database_stamp"]) is datetime
            and values["database_stamp"] == cursor_params["database_stamp"]
            and type(values["expected_version"]) is int
            and values["expected_version"] == 2
            and type(values["expected_boundary_cursor"]) is str
            and values["expected_boundary_cursor"] == expected.boundary_cursor
            and type(values["expected_boundary_cursor_version"]) is int
            and values["expected_boundary_cursor_version"] == 1
            and values["expected_apply_cursor"] is None
            and values["expected_apply_cursor_version"] is None
            and type(values["expected_plan_hash"]) is str
            and values["expected_plan_hash"] == expected.plan_hash
            and type(values["expected_contract_fingerprint"]) is str
            and values["expected_contract_fingerprint"] == expected.contract_fingerprint
            and type(values["expected_config_hash"]) is str
            and values["expected_config_hash"] == expected.config_hash
        )
    plan_params = by_marker.get("sync_cold_start_plans.update")
    if type(plan_params) is not dict:
        return False
    if marker == "cold_start_command_receipts.insert":
        return (
            len(prior) == 3
            and type(params) is tuple
            and len(params) == 10
            and type(params[0]) is UUID
            and type(params[1]) is int
            and params[1] == expected.account_id
            and type(params[2]) is str
            and params[2] == "cold_start.apply_page"
            and type(params[3]) is str
            and params[3] == expected.receipt_idempotency_hash
            and type(params[4]) is str
            and params[4] == expected.apply_payload_hash
            and type(params[5]) is str
            and params[5] == "succeeded"
            and type(params[6]) is str
            and params[6] == "sync_cold_start_plan"
            and type(params[7]) is str
            and params[7] == str(expected.plan_id)
            and plan_params["plan_id"] == expected.plan_id
            and type(params[8]) is str
            and params[8] == expected.batch_result_hash
            and type(params[9]) is int
            and params[9] == expected.fencing_token
            and event_params[14] == params[9]
        )
    return False


def _stable_apply_ack_loss_value(value: object) -> object:
    if type(value) is Jsonb:
        return ("jsonb", _stable_apply_ack_loss_value(value.obj))
    if type(value) is dict:
        return tuple(
            (key, _stable_apply_ack_loss_value(item))
            for key, item in sorted(value.items())
        )
    if type(value) in (tuple, list):
        return tuple(_stable_apply_ack_loss_value(item) for item in value)
    return value


def _apply_ack_loss_stable_parameter_projection(
    marker: str,
    params: object,
) -> object:
    if marker in {"event_inbox.insert", "cold_start_command_receipts.insert"}:
        assert type(params) is tuple
        projected = params[1:]
    else:
        assert type(params) is dict
        projected = {
            key: value for key, value in params.items() if key != "database_stamp"
        }
    return _stable_apply_ack_loss_value(projected)


class _ApplyAckLossConnection:
    def __init__(
        self,
        connection: Any,
        state: _ApplyAckLossFaultState,
        *,
        phase: str,
    ) -> None:
        if phase not in {"locator", "origin", "recovery"}:
            raise ValueError("unknown apply ACK-loss connection phase")
        self._connection = connection
        self._state = state
        self._phase = phase
        self._backend_pid = connection.info.backend_pid
        assert type(self._backend_pid) is int
        self._active_transaction: _ApplyAckLossTransaction | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def transaction(self, *args: object, **kwargs: object) -> Any:
        return _ApplyAckLossTransaction(
            self,
            self._connection.transaction(*args, **kwargs),
        )

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        if self._phase == "locator":
            if (
                _sql_tokens(rendered) != _APPLY_ACK_LOSS_LOCATOR_TOKENS
                or type(params) is not tuple
                or len(params) != 1
                or type(params[0]) is not UUID
                or self._state.expected_locator_plan_id is None
                or params[0] != self._state.expected_locator_plan_id
                or self._state.locator_calls != 0
                or self._active_transaction is not None
                or self._connection.info.transaction_status
                is not TransactionStatus.IDLE
            ):
                raise AssertionError("unexpected apply ACK-loss locator SQL")
            self._state.locator_calls += 1
            cursor = await self._connection.execute(statement, params)
            assert self._connection.info.transaction_status is TransactionStatus.IDLE
            return cursor

        marker = _apply_dml_marker(rendered, params)
        allowed_read = _is_allowed_apply_read_sql(rendered)
        if _has_sql_separator(rendered):
            raise AssertionError("unexpected apply ACK-loss SQL boundary")
        if self._phase == "origin":
            if self._state.http_phase == "pre_http":
                if marker is not None or not allowed_read:
                    raise AssertionError("unexpected apply ACK-loss SQL")
            else:
                assert self._state.http_phase == "post_http"
                if marker is None and not allowed_read:
                    raise AssertionError("unexpected apply ACK-loss SQL")
                if marker is not None:
                    origin_markers = tuple(
                        item.marker
                        for item in self._state.observations
                        if item.phase == "origin"
                    )
                    marker_index = len(origin_markers)
                    if (
                        marker_index >= len(_APPLY_DML_ORDER)
                        or marker != _APPLY_DML_ORDER[marker_index]
                    ):
                        raise AssertionError("unexpected apply ACK-loss SQL")
        else:
            assert self._phase == "recovery"
            assert self._state.http_phase == "post_http"
            if marker is None:
                if not allowed_read:
                    raise AssertionError("unexpected apply ACK-loss recovery SQL")
                self._state.recovery_sql.append(("read_control", rendered))
            else:
                if self._state.mode != "rollback_then_raise":
                    raise AssertionError("unexpected apply ACK-loss recovery mutation")
                recovery_markers = tuple(
                    item.marker
                    for item in self._state.observations
                    if item.phase == "recovery"
                )
                marker_index = len(recovery_markers)
                if (
                    marker_index >= len(_APPLY_DML_ORDER)
                    or marker != _APPLY_DML_ORDER[marker_index]
                ):
                    raise AssertionError("unexpected apply ACK-loss recovery mutation")

        phase_params = self._state.executed_params[self._phase]
        if marker is not None and (
            self._state.expected_contract is None
            or not _apply_ack_loss_params_are_exact(
                marker,
                params,
                expected=self._state.expected_contract,
                prior=phase_params,
            )
        ):
            raise AssertionError("unexpected apply ACK-loss DML parameters")
        if marker is not None and (
            self._active_transaction is None
            or self._connection.info.transaction_status is not TransactionStatus.INTRANS
        ):
            raise AssertionError(
                "apply ACK-loss DML attempted outside active transaction"
            )

        cursor = await self._connection.execute(statement, params)
        if marker is None:
            return cursor
        assert cursor.rowcount == 1
        assert self._connection.info.transaction_status is TransactionStatus.INTRANS
        assert self._active_transaction is not None
        identity_cursor = await self._connection.execute(
            "SELECT "
            "pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text "
            "AS assigned_transaction_id, "
            "pg_catalog.pg_current_xact_id()::pg_catalog.text "
            "AS transaction_id, "
            "pg_catalog.pg_backend_pid() AS backend_pid"
        )
        row = await identity_cursor.fetchone()
        assert type(row) is dict and set(row) == {
            "assigned_transaction_id",
            "transaction_id",
            "backend_pid",
        }
        assigned_transaction_id = row["assigned_transaction_id"]
        transaction_id = row["transaction_id"]
        backend_pid = row["backend_pid"]
        assert (
            type(assigned_transaction_id) is str
            and assigned_transaction_id.isascii()
            and assigned_transaction_id.isdigit()
            and assigned_transaction_id == transaction_id
            and type(backend_pid) is int
            and backend_pid == self._backend_pid
        )
        armed = (
            self._phase == "origin"
            and marker == "cold_start_command_receipts.insert"
            and self._state.claim_fault()
        )
        stable_parameter_projection = _apply_ack_loss_stable_parameter_projection(
            marker,
            params,
        )
        observation = _ApplyAckLossDmlObservation(
            phase=self._phase,
            marker=marker,
            rowcount=cursor.rowcount,
            assigned_transaction_id=assigned_transaction_id,
            transaction_id=transaction_id,
            backend_pid=backend_pid,
            transaction_status=self._connection.info.transaction_status,
            http_phase=self._state.http_phase,
            armed=armed,
            stable_parameter_projection=stable_parameter_projection,
        )
        phase_params.append((marker, params))
        self._state.observations.append(observation)
        if self._phase == "recovery":
            self._state.recovery_sql.append((marker, rendered))
        if armed:
            self._active_transaction.arm(observation)
        return cursor


class _ApplyAckLossPool:
    _PHASES = ("locator", "origin", "recovery")

    def __init__(
        self,
        pool: AsyncConnectionPool,
        state: _ApplyAckLossFaultState,
    ) -> None:
        self._pool = pool
        self._state = state
        self.checked_out_pids: list[int] = []
        self.checked_out_connections: list[_ApplyAckLossConnection] = []
        self.returned_pids: list[int] = []
        self.returned_closed: list[bool] = []

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _ApplyAckLossConnection:
        checkout_index = len(self.checked_out_pids)
        if checkout_index >= len(self._PHASES):
            raise AssertionError("unexpected fourth apply ACK-loss checkout")
        connection = await self._pool.getconn()
        backend_pid = connection.info.backend_pid
        assert type(backend_pid) is int
        guarded_connection = _ApplyAckLossConnection(
            connection,
            self._state,
            phase=self._PHASES[checkout_index],
        )
        self.checked_out_pids.append(backend_pid)
        self.checked_out_connections.append(guarded_connection)
        return guarded_connection

    async def putconn(self, connection: _ApplyAckLossConnection) -> None:
        self.returned_pids.append(connection._backend_pid)
        self.returned_closed.append(connection.closed is True)
        await self._pool.putconn(connection._connection)


class _BlockingOrdinaryPageClient:
    def __init__(
        self,
        outcome: SyncBatch,
        state: _ApplyAckLossFaultState,
    ) -> None:
        self._outcome: SyncBatch | None = outcome
        self._state = state
        self.calls: list[tuple[int, str, str, int]] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    @property
    def exhausted(self) -> bool:
        return self._outcome is None

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, cursor, limit))
        if self._outcome is None or len(self.calls) != 1:
            raise AssertionError("unexpected blocking ordinary Sync request")
        assert self._state.http_phase == "pre_http"
        assert self._state.http_transition_count == 0
        self.entered.set()
        await self.release.wait()
        assert self._state.http_phase == "pre_http"
        self._state.transition_to_post_http()
        outcome = self._outcome
        self._outcome = None
        return outcome


_G9_UNLOCK_TOKENS = _sql_tokens(
    "SELECT pg_catalog.pg_advisory_unlock(%s, %s) AS released"
)


class _G9CancellationState:
    def __init__(self, flow: str) -> None:
        if flow not in {"preview", "apply"}:
            raise ValueError("unknown G9 cancellation flow")
        self.flow = flow
        self.phase = "setup"
        self.retained_pid: int | None = None
        self.checked_out_pids: list[int] = []
        self.checked_out_roles: list[str] = []
        self.returned_pids: list[int] = []
        self.returned_closed: list[bool] = []
        self.target_dml_attempts: list[str] = []
        self.cleanup_events: list[tuple[object, ...]] = []
        self.ordering_events: list[tuple[object, ...]] = []
        self.cancelled_error: asyncio.CancelledError | None = None


class _G9BlockingColdStartOrigin:
    def __init__(self, state: _G9CancellationState) -> None:
        assert state.flow == "preview"
        self._state = state
        self.calls: list[tuple[int, str, str | None, int]] = []
        self.entered = asyncio.Event()
        self._never_release = asyncio.Event()

    async def fetch_cold_start_page(
        self,
        account_id: int,
        sync_folder: str,
        cursor: str | None,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, sync_folder, cursor, limit))
        if len(self.calls) != 1 or cursor is not None:
            raise AssertionError("unexpected G9 preview HTTP request")
        self._state.phase = "http_in_flight"
        self.entered.set()
        try:
            await self._never_release.wait()
        except asyncio.CancelledError as error:
            assert self._state.cancelled_error is None
            self._state.cancelled_error = error
            self._state.phase = "cancelled"
            raise
        raise AssertionError("G9 preview HTTP barrier unexpectedly released")


class _G9BlockingOrdinaryPageClient:
    def __init__(self, state: _G9CancellationState) -> None:
        assert state.flow == "apply"
        self._state = state
        self.calls: list[tuple[int, str, str, int]] = []
        self.entered = asyncio.Event()
        self._never_release = asyncio.Event()

    async def sync_emails(
        self,
        account_id: int,
        folder: str,
        cursor: str,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, folder, cursor, limit))
        if len(self.calls) != 1:
            raise AssertionError("unexpected G9 apply HTTP request")
        self._state.phase = "http_in_flight"
        self.entered.set()
        try:
            await self._never_release.wait()
        except asyncio.CancelledError as error:
            assert self._state.cancelled_error is None
            self._state.cancelled_error = error
            self._state.phase = "cancelled"
            raise
        raise AssertionError("G9 apply HTTP barrier unexpectedly released")


class _G9UnlockObservedCursor:
    def __init__(
        self,
        cursor: Any,
        state: _G9CancellationState,
        backend_pid: int,
    ) -> None:
        self._cursor = cursor
        self._state = state
        self._backend_pid = backend_pid

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    async def fetchone(self) -> object:
        row = await self._cursor.fetchone()
        assert type(row) is dict and row == {"released": True}
        self._state.cleanup_events.append(("unlock", self._backend_pid))
        return row


class _G9CancellationConnection:
    def __init__(
        self,
        connection: Any,
        state: _G9CancellationState,
        *,
        role: str,
    ) -> None:
        self._connection = connection
        self._state = state
        self._role = role
        self._backend_pid = connection.info.backend_pid
        assert type(self._backend_pid) is int

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        if self._state.flow == "preview":
            target_marker = (
                "sync_cold_start_plans.preview_page_update"
                if _is_preview_page_plan_update(rendered, params)
                else None
            )
        else:
            target_marker = _apply_dml_marker(rendered, params)
        if target_marker is not None and self._state.phase in {
            "http_in_flight",
            "cancelled",
        }:
            self._state.target_dml_attempts.append(target_marker)
            raise AssertionError("G9 target DML attempted during HTTP cancellation")
        if _sql_tokens(rendered) == _G9_UNLOCK_TOKENS:
            if (
                self._role != "retained"
                or self._state.phase != "cancelled"
                or self._state.retained_pid != self._backend_pid
                or self._connection.info.transaction_status
                is not TransactionStatus.IDLE
            ):
                raise AssertionError("unexpected G9 advisory unlock")
            cursor = await self._connection.execute(statement, params)
            return _G9UnlockObservedCursor(
                cursor,
                self._state,
                self._backend_pid,
            )
        return await self._connection.execute(statement, params)


class _G9CancellationPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        state: _G9CancellationState,
    ) -> None:
        self._pool = pool
        self._state = state
        self._roles = (
            ("retained",) if state.flow == "preview" else ("locator", "retained")
        )
        self.checked_out_connections: list[_G9CancellationConnection] = []

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _G9CancellationConnection:
        index = len(self._state.checked_out_pids)
        if index >= len(self._roles):
            raise AssertionError("unexpected G9 pool checkout")
        raw = await self._pool.getconn()
        role = self._roles[index]
        connection = _G9CancellationConnection(raw, self._state, role=role)
        self._state.checked_out_pids.append(connection._backend_pid)
        self._state.checked_out_roles.append(role)
        self._state.ordering_events.append((f"{role}.getconn", connection._backend_pid))
        if role == "retained":
            assert self._state.retained_pid is None
            self._state.retained_pid = connection._backend_pid
        self.checked_out_connections.append(connection)
        return connection

    async def putconn(self, connection: _G9CancellationConnection) -> None:
        closed = connection.closed is True
        self._state.returned_pids.append(connection._backend_pid)
        self._state.returned_closed.append(closed)
        if connection._role == "retained":
            if (
                closed
                or connection.info.transaction_status is not TransactionStatus.IDLE
                or self._state.cleanup_events != [("unlock", connection._backend_pid)]
            ):
                raise AssertionError("unexpected G9 retained putconn state")
        await self._pool.putconn(connection._connection)
        if connection._role == "locator":
            self._state.ordering_events.append(
                ("locator.putconn", connection._backend_pid)
            )
        else:
            self._state.cleanup_events.append(
                ("putconn", connection._backend_pid, False, "idle")
            )


class _G9TrackingPermitProvider(_PermitProvider):
    def __init__(self, state: _G9CancellationState) -> None:
        super().__init__()
        self._state = state

    async def try_acquire(
        self,
        account_id: int,
        canonical_folder: str,
    ) -> Any:
        if self._state.flow == "apply":
            if not (
                self._state.checked_out_roles == ["locator"]
                and len(self._state.checked_out_pids) == 1
                and self._state.returned_pids == self._state.checked_out_pids
                and self._state.returned_closed == [False]
                and self._state.ordering_events
                == [
                    ("locator.getconn", self._state.checked_out_pids[0]),
                    ("locator.putconn", self._state.checked_out_pids[0]),
                ]
            ):
                raise AssertionError("G9 apply permit acquired before locator return")
        elif not (
            self._state.checked_out_roles == []
            and self._state.returned_pids == []
            and self._state.ordering_events == []
        ):
            raise AssertionError("G9 preview permit acquired after pool activity")
        lease = await super().try_acquire(account_id, canonical_folder)
        assert lease is not None
        self._state.ordering_events.append(("permit.acquire",))
        return lease

    def _release(self) -> None:
        assert self._state.retained_pid is not None
        assert self._state.cleanup_events == [
            ("unlock", self._state.retained_pid),
            ("putconn", self._state.retained_pid, False, "idle"),
        ]
        super()._release()
        self._state.cleanup_events.append(("permit.release",))


_G10_LOCATOR_TOKENS = _sql_tokens(
    "SELECT plan_id, account_id, folder_key "
    "FROM public.sync_cold_start_plans WHERE plan_id = %s"
)


class _G10LocatorCancellationState:
    def __init__(self, mode: str, plan_id: UUID) -> None:
        if mode not in {"fetch_cancel", "return_cancel"}:
            raise ValueError("unknown G10 locator cancellation mode")
        self.mode = mode
        self.plan_id = plan_id
        self.phase = "setup"
        self.backend_pid: int | None = None
        self.statements: list[tuple[str, object]] = []
        self.forbidden_statements: list[str] = []
        self.putconn_intents: list[tuple[int, bool, object]] = []
        self.putconn_completions: list[tuple[int, bool]] = []
        self.close_events: list[tuple[str, int, bool]] = []
        self.cancelled_error: asyncio.CancelledError | None = None
        self.fetch_entered = asyncio.Event()
        self.row_fetched = asyncio.Event()
        self.allow_row_return = asyncio.Event()
        self.return_delegate_entered = asyncio.Event()
        self.second_return_entered = asyncio.Event()
        self.pool_lock_held = asyncio.Event()
        self.release_pool_lock = asyncio.Event()
        self.borrowed_pids: list[int] = []


class _G10LocatorCursor:
    def __init__(
        self,
        cursor: Any,
        state: _G10LocatorCancellationState,
    ) -> None:
        self._cursor = cursor
        self._state = state

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    async def fetchone(self) -> object:
        if self._state.mode == "fetch_cancel":
            self._state.phase = "fetch_in_flight"
            self._state.fetch_entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError as error:
                assert self._state.cancelled_error is None
                self._state.cancelled_error = error
                self._state.phase = "cancelled"
                raise
            raise AssertionError("G10 fetch barrier unexpectedly released")

        row = await self._cursor.fetchone()
        self._state.phase = "row_fetched"
        self._state.row_fetched.set()
        await self._state.allow_row_return.wait()
        self._state.phase = "normal_return"
        return row


class _G10LocatorConnection:
    def __init__(
        self,
        connection: Any,
        state: _G10LocatorCancellationState,
    ) -> None:
        self._connection = connection
        self._state = state
        backend_pid = connection.info.backend_pid
        assert type(backend_pid) is int
        self._backend_pid = backend_pid

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        if not (
            _sql_tokens(rendered) == _G10_LOCATOR_TOKENS
            and type(params) is tuple
            and params == (self._state.plan_id,)
            and len(self._state.statements) == 0
        ):
            self._state.forbidden_statements.append(rendered)
            raise AssertionError("G10 locator attempted forbidden SQL")
        self._state.statements.append((rendered, params))
        cursor = await self._connection.execute(statement, params)
        return _G10LocatorCursor(cursor, self._state)

    async def close(self) -> None:
        self._state.close_events.append(
            ("close.start", self._backend_pid, self.closed is True)
        )
        await self._connection.close()
        self._state.close_events.append(
            ("close.done", self._backend_pid, self.closed is True)
        )


class _G10LocatorPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        state: _G10LocatorCancellationState,
    ) -> None:
        self._pool = pool
        self._state = state
        self.connection: _G10LocatorConnection | None = None

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _G10LocatorConnection:
        if self.connection is not None:
            raise AssertionError("unexpected second G10 locator checkout")
        raw = await self._pool.getconn()
        connection = _G10LocatorConnection(raw, self._state)
        self.connection = connection
        self._state.backend_pid = connection._backend_pid
        self._state.phase = "checked_out"
        return connection

    async def putconn(self, connection: _G10LocatorConnection) -> None:
        if connection is not self.connection:
            raise AssertionError("G10 returned an unknown locator handle")
        closed = connection.closed is True
        status = connection.info.transaction_status
        self._state.putconn_intents.append((connection._backend_pid, closed, status))
        intent = len(self._state.putconn_intents)
        if self._state.mode == "return_cancel" and intent == 1:
            if closed or status is not TransactionStatus.IDLE:
                raise AssertionError("G10 first return must carry an open IDLE backend")
            self._state.return_delegate_entered.set()
            try:
                await self._pool.putconn(connection._connection)
            except asyncio.CancelledError as error:
                assert self._state.cancelled_error is None
                self._state.cancelled_error = error
                self._state.phase = "cancelled"
                raise
            self._state.putconn_completions.append(
                (connection._backend_pid, connection.closed is True)
            )
            return

        if not closed:
            raise AssertionError("G10 cleanup putconn requires a closed backend")
        if self._state.mode == "return_cancel":
            if intent != 2:
                raise AssertionError("G10 return cancellation requires two intents")
            self._state.second_return_entered.set()
        elif intent != 1:
            raise AssertionError("G10 fetch cancellation requires one return intent")
        await self._pool.putconn(connection._connection)
        self._state.putconn_completions.append(
            (connection._backend_pid, connection.closed is True)
        )


class _G10NeverSnapshotProvider:
    def __init__(self) -> None:
        self.calls: list[int] = []

    async def get_ready_snapshot(self, account_id: int) -> object:
        self.calls.append(account_id)
        raise AssertionError("G10 cancellation crossed into snapshot lookup")


class _G10NeverPolicyResolver:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def configured_scopes(self, snapshot: object) -> object:
        self.calls.append(snapshot)
        raise AssertionError("G10 cancellation crossed into policy resolution")


_G11_TRY_LOCK_TOKENS = _sql_tokens(
    "SELECT pg_catalog.pg_try_advisory_lock(%s, %s) AS acquired"
)
_G11_UNLOCK_TOKENS = _sql_tokens(
    "SELECT pg_catalog.pg_advisory_unlock(%s, %s) AS released"
)


class _G11BlockingOrigin:
    def __init__(self, outcome: SyncBatch) -> None:
        self._outcome: SyncBatch | None = outcome
        self.calls: list[tuple[int, str, str | None, int]] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch_cold_start_page(
        self,
        account_id: int,
        sync_folder: str,
        cursor: str | None,
        limit: int,
    ) -> SyncBatch:
        self.calls.append((account_id, sync_folder, cursor, limit))
        if self._outcome is None or len(self.calls) != 1:
            raise AssertionError("unexpected G11 cold-start Origin request")
        self.entered.set()
        await self.release.wait()
        outcome = self._outcome
        self._outcome = None
        return outcome


class _G11ObservedLockCursor:
    def __init__(
        self,
        cursor: Any,
        events: list[tuple[object, ...]],
        *,
        role: str,
        checkout_id: int,
        backend_pid: int,
        field: str,
        connection: Any,
    ) -> None:
        self._cursor = cursor
        self._events = events
        self._role = role
        self._checkout_id = checkout_id
        self._backend_pid = backend_pid
        self._field = field
        self._connection = connection

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    async def fetchone(self) -> object:
        row = await self._cursor.fetchone()
        assert type(row) is dict
        value = row.get(self._field)
        assert type(value) is bool
        self._events.append(
            (
                "lock.result",
                self._role,
                self._checkout_id,
                self._backend_pid,
                self._field,
                value,
                self._connection.info.transaction_status,
            )
        )
        return row


class _G11TrackingConnection:
    def __init__(
        self,
        connection: Any,
        events: list[tuple[object, ...]],
        *,
        role: str,
        checkout_id: int,
    ) -> None:
        self._connection = connection
        self._events = events
        self._role = role
        self._checkout_id = checkout_id
        backend_pid = connection.info.backend_pid
        assert type(backend_pid) is int
        self._backend_pid = backend_pid

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    async def execute(self, statement: Any, params: object = None) -> Any:
        rendered = (
            statement
            if isinstance(statement, str)
            else statement.as_string()
            if hasattr(statement, "as_string")
            else str(statement)
        )
        tokens = _sql_tokens(rendered)
        self._events.append(
            (
                "execute",
                self._role,
                self._checkout_id,
                self._backend_pid,
                tokens,
                params,
                self._connection.info.transaction_status,
            )
        )
        cursor = await self._connection.execute(statement, params)
        field = (
            "acquired"
            if tokens == _G11_TRY_LOCK_TOKENS
            else "released"
            if tokens == _G11_UNLOCK_TOKENS
            else None
        )
        if field is None:
            return cursor
        return _G11ObservedLockCursor(
            cursor,
            self._events,
            role=self._role,
            checkout_id=self._checkout_id,
            backend_pid=self._backend_pid,
            field=field,
            connection=self._connection,
        )


class _G11TrackingPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        roles: tuple[str, ...],
    ) -> None:
        if not roles:
            raise ValueError("G11 pool roles must be non-empty")
        self._pool = pool
        self._roles = roles
        self.events: list[tuple[object, ...]] = []
        self.connections: list[_G11TrackingConnection] = []

    @property
    def kwargs(self) -> object:
        return self._pool.kwargs

    @property
    def close_returns(self) -> object:
        return self._pool.close_returns

    async def getconn(self) -> _G11TrackingConnection:
        raw = await self._pool.getconn()
        checkout_id = len(self.connections)
        role = self._roles[checkout_id % len(self._roles)]
        connection = _G11TrackingConnection(
            raw,
            self.events,
            role=role,
            checkout_id=checkout_id,
        )
        self.connections.append(connection)
        self.events.append(
            (
                "getconn",
                role,
                checkout_id,
                connection._backend_pid,
                connection.closed is True,
                connection.info.transaction_status,
            )
        )
        return connection

    async def putconn(self, connection: _G11TrackingConnection) -> None:
        self.events.append(
            (
                "putconn.start",
                connection._role,
                connection._checkout_id,
                connection._backend_pid,
                connection.closed is True,
                connection.info.transaction_status,
            )
        )
        await self._pool.putconn(connection._connection)
        self.events.append(
            (
                "putconn.done",
                connection._role,
                connection._checkout_id,
                connection._backend_pid,
                connection.closed is True,
                connection.info.transaction_status,
            )
        )


def _assert_g11_busy_trace(
    events: list[tuple[object, ...]],
    *,
    plan_id: UUID,
    lock_keys: tuple[int, int],
) -> int:
    assert [event[0] for event in events] == [
        "getconn",
        "execute",
        "putconn.start",
        "putconn.done",
        "getconn",
        "execute",
        "lock.result",
        "putconn.start",
        "putconn.done",
    ]
    locator_get, locator_execute, locator_start, locator_done = events[:4]
    retained_get, retained_execute, lock_result, retained_start, retained_done = events[
        4:
    ]
    assert locator_get[1] == locator_execute[1] == "locator"
    assert locator_get[2:4] == locator_execute[2:4]
    assert locator_execute[4] == _G10_LOCATOR_TOKENS
    assert locator_execute[5] == (plan_id,)
    assert locator_start[1:4] == locator_done[1:4] == locator_get[1:4]
    assert (
        locator_start[4:]
        == locator_done[4:]
        == (
            False,
            TransactionStatus.IDLE,
        )
    )

    assert retained_get[1] == retained_execute[1] == "retained"
    assert retained_get[2:4] == retained_execute[2:4]
    assert retained_execute[4] == _G11_TRY_LOCK_TOKENS
    assert retained_execute[5] == lock_keys
    assert lock_result[1:4] == retained_get[1:4]
    assert lock_result[4:] == (
        "acquired",
        False,
        TransactionStatus.IDLE,
    )
    assert retained_start[1:4] == retained_done[1:4] == retained_get[1:4]
    assert (
        retained_start[4:]
        == retained_done[4:]
        == (
            False,
            TransactionStatus.IDLE,
        )
    )
    assert locator_done[2] != retained_get[2]
    assert locator_get[3] == retained_get[3]
    return int(retained_get[3])


async def _wait_for_g11_pool_waiter(
    pool: AsyncConnectionPool,
    borrower: asyncio.Task[Any],
) -> None:
    deadline = asyncio.get_running_loop().time() + 3.0
    while pool.get_stats()["requests_waiting"] != 1:
        if borrower.done():
            raise AssertionError("G11 competing borrower did not wait")
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("G11 competing borrower was not queued")
        await asyncio.sleep(0)
    stats = pool.get_stats()
    assert stats["requests_waiting"] == 1
    assert stats["pool_size"] == 1
    assert stats["pool_available"] == 0
    assert borrower.done() is False


async def _cleanup_g11_borrower(
    pool: AsyncConnectionPool,
    borrower: asyncio.Task[Any] | None,
    connection: Any | None,
    *,
    returned: bool,
) -> None:
    if borrower is None:
        return
    if not borrower.done():
        borrower.cancel()
        try:
            await borrower
        except BaseException:
            pass
    if connection is None and not borrower.cancelled():
        try:
            connection = borrower.result()
        except BaseException:
            connection = None
    if connection is not None and not returned:
        await pool.putconn(connection)


def _backend_pid_exists(runtime: _ColdStartRuntime, backend_pid: int) -> bool:
    return bool(
        _scalar(
            runtime,
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity "
            "WHERE datname = pg_catalog.current_database() AND pid = %s",
            (backend_pid,),
        )
    )


def _backend_pid_has_no_open_transaction(
    runtime: _ColdStartRuntime,
    backend_pid: int,
) -> bool:
    return bool(
        _scalar(
            runtime,
            "SELECT xact_start IS NULL FROM pg_catalog.pg_stat_activity "
            "WHERE datname = pg_catalog.current_database() AND pid = %s",
            (backend_pid,),
        )
    )


def _backend_pid_advisory_lock_count(
    runtime: _ColdStartRuntime,
    backend_pid: int,
) -> int:
    return int(
        _scalar(
            runtime,
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_locks "
            "WHERE pid = %s AND locktype = 'advisory'",
            (backend_pid,),
        )
    )


def _probe_session_advisory_lock(
    runtime: _ColdStartRuntime,
    keys: tuple[int, int],
) -> tuple[bool, bool | None]:
    with psycopg.connect(
        runtime.schema.admin_dsn,
        autocommit=True,
        row_factory=dict_row,
    ) as connection:
        acquired_row = connection.execute(
            "SELECT pg_catalog.pg_try_advisory_lock(%s, %s) AS acquired",
            keys,
        ).fetchone()
        assert acquired_row is not None
        acquired = acquired_row["acquired"]
        assert type(acquired) is bool
        if not acquired:
            return False, None
        released_row = connection.execute(
            "SELECT pg_catalog.pg_advisory_unlock(%s, %s) AS released",
            keys,
        ).fetchone()
        assert released_row is not None
        released = released_row["released"]
        assert type(released) is bool
        return True, released


async def _wait_until_backend_pid_disappears(
    runtime: _ColdStartRuntime,
    backend_pid: int,
) -> None:
    deadline = asyncio.get_running_loop().time() + 3.0
    while _backend_pid_exists(runtime, backend_pid):
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("retired preview backend PID is still visible")
        await asyncio.sleep(0.01)


@dataclass(slots=True)
class _ColdStartRuntime:
    schema: Any
    pool: AsyncConnectionPool
    application_name: str


@pytest_asyncio.fixture
async def cold_start_runtime(postgres_database_factory) -> _ColdStartRuntime:
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)

    ownership_pool = AsyncConnectionPool(
        conninfo=schema.runtime_dsn,
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await ownership_pool.open()
    try:
        await PipelineOwnershipRepository(ownership_pool).bootstrap(8, "durable_v1")
    finally:
        await ownership_pool.close()

    application_name = f"cold-start-test-{schema.database_name[-12:]}"
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            schema.maintenance_dsn,
            application_name=application_name,
        ),
        min_size=1,
        max_size=3,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    try:
        yield _ColdStartRuntime(schema, pool, application_name)
    finally:
        await pool.close()


async def _open_maintenance_pool(
    runtime: _ColdStartRuntime,
    process_name: str,
) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            runtime.schema.maintenance_dsn,
            application_name=_process_application_name(runtime, process_name),
        ),
        min_size=1,
        max_size=3,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    return pool


def _process_application_name(
    runtime: _ColdStartRuntime,
    process_name: str,
) -> str:
    return f"cold-start-{process_name}-{runtime.schema.database_name[-8:]}"


def _seed_cold_start_cursor(
    runtime: _ColdStartRuntime,
    state: str,
) -> tuple[str | None, int]:
    if state == "cold_start_pending":
        runtime.schema.maintenance_execute(
            "INSERT INTO sync_cursors ("
            "account_id, folder_key, cursor, status, blocked_reason_code"
            ") VALUES (8, 'INBOX', NULL, 'cold_start_pending', "
            "'sync.cold_start_required')"
        )
        return None, 0
    if state != "reset_required":
        raise AssertionError("unknown cold-start cursor fixture")
    runtime.schema.maintenance_execute(
        "INSERT INTO sync_cursors ("
        "account_id, folder_key, cursor, status, blocked_reason_code, "
        "last_attempt_at) VALUES (8, 'INBOX', 'opaque+Stale/%3D', "
        "'reset_required', 'exchange.sync.cursor_invalid', CURRENT_TIMESTAMP)"
    )
    return "opaque+Stale/%3D", 0


def _service(
    runtime: _ColdStartRuntime,
    *,
    origin: _ColdStartOrigin,
    ordinary: _OrdinaryPageClient,
    preview_max_pages: int = 4,
    apply_max_pages: int = 4,
    pool: AsyncConnectionPool | None = None,
    receipt_repository: Any | None = None,
    permit: Any | None = None,
    snapshot_provider: Any | None = None,
    policy_resolver: Any | None = None,
) -> ColdStartService:
    return ColdStartService(
        cold_start_origin=origin,
        ordinary_page_client=ordinary,
        snapshot_provider=(
            _SnapshotProvider(_snapshot())
            if snapshot_provider is None
            else snapshot_provider
        ),
        policy_resolver=(
            ProcessingPolicyResolver() if policy_resolver is None else policy_resolver
        ),
        folder_permit=_PermitProvider() if permit is None else permit,
        maintenance_pool=runtime.pool if pool is None else pool,
        inbox_repository=InboxRepository(_NeverPool()),
        receipt_repository=(
            CommandReceiptRepository()
            if receipt_repository is None
            else receipt_repository
        ),
        page_limit=100,
        preview_max_pages=preview_max_pages,
        preview_max_run_seconds=30.0,
        apply_max_pages=apply_max_pages,
        apply_max_run_seconds=30.0,
        plan_ttl_seconds=3600,
        locator_timeout=3.0,
        cleanup_timeout=1.0,
        contract_fingerprint="e" * 64,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_non_utc_database_session_normalizes_plan_clock_and_receipt_replay(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Non-UTC/%3D",
        includes_last=True,
        changes=(),
    )
    origin = _ColdStartOrigin([boundary])
    process_name = "non-utc-normalization"
    application_name = _process_application_name(cold_start_runtime, process_name)
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    try:
        async with pool.connection() as connection:
            await connection.execute("SET TIME ZONE 'Asia/Shanghai'")
            setting_cursor = await connection.execute(
                "SELECT pg_catalog.current_setting('TimeZone') AS timezone"
            )
            assert await setting_cursor.fetchone() == {"timezone": "Asia/Shanghai"}

        service = _service(
            cold_start_runtime,
            origin=origin,
            ordinary=_OrdinaryPageClient([]),
            pool=pool,
        )
        call = {
            "actor": "non-utc-operator",
            "reason": "prove database timestamp normalization",
            "idempotency_key": "non-utc-preview-key",
        }

        first = await service.preview(8, "INBOX", **call)
        replay = await service.preview(8, "INBOX", **call)

        assert first.status is ColdStartRunStatus.READY
        assert first.plan is not None
        assert replay.status is ColdStartRunStatus.READY
        assert replay.plan == first.plan
        assert replay.pages_committed == 0
        assert replay.changes_observed == 0
        assert origin.calls == [(8, "Inbox", None, 100)]
        timestamps = (
            first.plan.created_at,
            first.plan.updated_at,
            first.plan.expires_at,
            first.plan.ready_at,
        )
        assert all(
            type(timestamp) is datetime and timestamp.tzinfo is UTC
            for timestamp in timestamps
        )
    finally:
        await pool.close()
    assert _application_session_count(cold_start_runtime, application_name) == 0


def _rows(
    runtime: _ColdStartRuntime,
    statement: str,
    params: tuple[object, ...] = (),
) -> list[dict[str, Any]]:
    with psycopg.connect(
        runtime.schema.maintenance_dsn,
        autocommit=True,
        row_factory=dict_row,
    ) as connection:
        return list(connection.execute(statement, params).fetchall())


def _plan_view_projection(plan: ColdStartPlanView) -> dict[str, object]:
    projection = {
        "plan_id": plan.plan_id,
        "account_id": plan.account_id,
        "canonical_folder": plan.canonical_folder,
        "state": plan.state.value,
        "boundary_cursor": plan.boundary_cursor,
        "page_count": plan.page_count,
        "item_count": plan.item_count,
        "redacted_samples": [
            {
                "kind": sample.kind.value,
                "external_email_id_hash": sample.external_email_id_hash,
            }
            for sample in plan.redacted_samples
        ],
        "contract_fingerprint": plan.contract_fingerprint,
        "folder_scope_config_hash": plan.folder_scope_config_hash,
        "plan_hash": plan.plan_hash,
        "blocked_reason_code": plan.blocked_reason_code,
        "blocked_fingerprint": plan.blocked_fingerprint,
        "expires_at": plan.expires_at,
        "ready_at": plan.ready_at,
        "approved_at": plan.approved_at,
        "completed_at": plan.completed_at,
        "blocked_at": plan.blocked_at,
        "created_at": plan.created_at,
        "updated_at": plan.updated_at,
    }
    assert len(projection) == 20
    return projection


def _durable_plan_view_projection(
    runtime: _ColdStartRuntime,
    plan_id: UUID,
) -> dict[str, object]:
    rows = _rows(
        runtime,
        "SELECT plan_id, account_id, folder_key AS canonical_folder, state, "
        "boundary_cursor, page_count, item_count, redacted_samples, "
        "contract_fingerprint, folder_scope_config_hash, plan_hash, "
        "blocked_reason_code, blocked_fingerprint, expires_at, ready_at, "
        "approved_at, completed_at, blocked_at, created_at, updated_at "
        "FROM sync_cold_start_plans WHERE plan_id = %s",
        (plan_id,),
    )
    assert len(rows) == 1 and len(rows[0]) == 20
    return rows[0]


def _admin_rows(
    runtime: _ColdStartRuntime,
    statement: str,
    params: tuple[object, ...] = (),
) -> list[dict[str, Any]]:
    with psycopg.connect(
        runtime.schema.admin_dsn,
        autocommit=True,
        row_factory=dict_row,
    ) as connection:
        return list(connection.execute(statement, params).fetchall())


def _scalar(
    runtime: _ColdStartRuntime,
    statement: str,
    params: tuple[object, ...] = (),
) -> object:
    rows = _rows(runtime, statement, params)
    assert len(rows) == 1 and len(rows[0]) == 1
    return next(iter(rows[0].values()))


def _durable_physical_snapshot(
    runtime: _ColdStartRuntime,
) -> dict[str, list[dict[str, Any]]]:
    return {
        "plan": _rows(
            runtime,
            "SELECT pg_catalog.to_jsonb(row_data) AS row_data, "
            "row_data.xmin::pg_catalog.text AS xmin, "
            "row_data.ctid::pg_catalog.text AS ctid "
            "FROM sync_cold_start_plans AS row_data ORDER BY row_data.plan_id",
        ),
        "cursor": _rows(
            runtime,
            "SELECT pg_catalog.to_jsonb(row_data) AS row_data, "
            "row_data.xmin::pg_catalog.text AS xmin, "
            "row_data.ctid::pg_catalog.text AS ctid "
            "FROM sync_cursors AS row_data "
            "ORDER BY row_data.account_id, row_data.folder_key",
        ),
        "inbox": _rows(
            runtime,
            "SELECT pg_catalog.to_jsonb(row_data) AS row_data, "
            "row_data.xmin::pg_catalog.text AS xmin, "
            "row_data.ctid::pg_catalog.text AS ctid "
            "FROM event_inbox AS row_data ORDER BY row_data.id",
        ),
        "receipts": _admin_rows(
            runtime,
            "SELECT pg_catalog.to_jsonb(row_data) AS row_data, "
            "row_data.xmin::pg_catalog.text AS xmin, "
            "row_data.ctid::pg_catalog.text AS ctid "
            "FROM pipeline_command_receipts AS row_data "
            "WHERE row_data.command_name IN ("
            "'cold_start.preview', 'cold_start.approve', "
            "'cold_start.apply_page') ORDER BY row_data.id",
        ),
        "audits": _rows(
            runtime,
            "SELECT pg_catalog.to_jsonb(row_data) AS row_data, "
            "row_data.xmin::pg_catalog.text AS xmin, "
            "row_data.ctid::pg_catalog.text AS ctid "
            "FROM audit_events AS row_data ORDER BY row_data.id",
        ),
    }


def _application_session_count(
    runtime: _ColdStartRuntime,
    application_name: str,
) -> int:
    with psycopg.connect(runtime.schema.admin_dsn, autocommit=True) as connection:
        row = connection.execute(
            "SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity "
            "WHERE datname = pg_catalog.current_database() "
            "AND application_name = %s",
            (application_name,),
        ).fetchone()
    assert row is not None
    return int(row[0])


class _SqlDelegateProbe:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object]] = []
        self.result = object()

    async def execute(self, statement: object, params: object = None) -> object:
        self.calls.append((statement, params))
        return self.result


def _apply_sql_guard_connection(
    delegate: _SqlDelegateProbe,
) -> _ApplyStatementFaultConnection:
    return _ApplyStatementFaultConnection(
        delegate,
        target=_APPLY_DML_ORDER[0],
        observations=[],
        error=_InjectedApplyStatementFailure("unused"),
        commit_boundaries=[],
        required_commit_order=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TEMP TABLE forged AS SELECT 1",
        "CREATE VIEW forged AS SELECT 1",
        "CALL public.side_effect()",
        "DO $$ BEGIN PERFORM 1; END $$",
        "VACUUM durable_table",
        "MYSTERY durable_table",
        "SELECT 1; SELECT 2",
        (
            "WITH changed AS (DELETE FROM public.sync_cursors RETURNING *) "
            "SELECT * FROM changed"
        ),
        (
            "WITH changed AS (INSERT INTO public.event_inbox DEFAULT VALUES "
            "RETURNING *) SELECT * FROM changed"
        ),
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT forged",
        "SELECT 1 INTO TEMP TABLE forged",
        "SELECT 1 INTO/*gap*/ TEMP TABLE forged FROM public.sync_cursors",
        "SELECT public.side_effect()",
        "SELECT public.side_effect () FROM public.sync_cursors",
        "SELECT public.side_effect/*gap*/() FROM public.sync_cursors",
        (
            "SELECT pg_catalog.set_config "
            "('application_name', 'forged', false) FROM public.sync_cursors"
        ),
        "SELECT * FROM public.sync_cursors_forged",
        "SELECT * FROM public.sync_cursors_backup",
        "SELECT 1 /* FROM public.sync_cursors */",
        "SELECT * FROM public.sync_cursors, public.emails",
        'SELECT public."SideEffect" () FROM public.sync_cursors',
        'SELECT "select"() FROM public.sync_cursors',
        "SELECT Δfunc () FROM public.sync_cursors",
        "SELECT 'unterminated FROM public.sync_cursors",
        "SELECT $tag$unterminated FROM public.sync_cursors",
        "SELECT cursor FROM public.sync_cursors UNION TABLE public.emails",
        'SELECT * FROM "public"."sync_cursors"()',
        "SELECT cursor::public.evil FROM public.sync_cursors",
        "SELECT cursor FROM public.sync_cursors -- comment\r; SELECT 2",
        "SELECT cursor FROM public.sync_cursors -- comment\r\n; SELECT 2",
        "SELECT cursor FROM public.sync_cursors -- comment\n; SELECT 2",
        (
            "SELECT cursor.cursor FROM public.sync_cursors AS cursor "
            "JOIN public.emails AS email ON true"
        ),
        "UPDATE public.emails SET status = 'forged'",
        "UPDATE public.sync_cursors AS cursor SET forged = true",
        ("INSERT INTO public.audit_events (action) VALUES ('not-cold-start-block')"),
        "/* unterminated",
    ],
)
async def test_apply_sql_guard_rejects_unknown_or_mutating_sql_before_delegate(
    statement: str,
) -> None:
    delegate = _SqlDelegateProbe()
    connection = _apply_sql_guard_connection(delegate)

    with pytest.raises(AssertionError, match="unexpected apply SQL"):
        await connection.execute(statement)

    assert delegate.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        (
            "SELECT cursor, status, version FROM public.sync_cursors "
            "WHERE account_id = %s FOR UPDATE"
        ),
        "SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SELECT pg_catalog.pg_try_advisory_lock(%s, %s) AS acquired",
        (
            "/* retained apply read */ SELECT plan_id FROM "
            "public.sync_cold_start_plans WHERE plan_id = %s FOR UPDATE"
        ),
    ],
)
async def test_apply_sql_guard_delegates_only_known_read_and_control_sql(
    statement: str,
) -> None:
    delegate = _SqlDelegateProbe()
    connection = _apply_sql_guard_connection(delegate)

    result = await connection.execute(statement, (1,))

    assert result is delegate.result
    assert delegate.calls == [(statement, (1,))]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO durable_table VALUES (1)",
        "WITH candidate AS (SELECT 1) UPDATE durable_table SET value = 1",
        "DELETE FROM durable_table",
        "MERGE INTO durable_table USING source ON false WHEN NOT MATCHED THEN INSERT DEFAULT VALUES",
        "COPY durable_table FROM STDIN",
        "TRUNCATE TABLE durable_table",
        "WITH candidate AS (SELECT 1) SELECT 1",
        (
            "WITH changed AS (UPDATE durable_table SET value = 1 RETURNING *) "
            "SELECT * FROM changed"
        ),
        (
            "WITH changed AS (DELETE FROM durable_table RETURNING *) "
            "SELECT * FROM changed"
        ),
        (
            "WITH changed AS (INSERT INTO durable_table DEFAULT VALUES RETURNING *) "
            "SELECT * FROM changed"
        ),
        "VACUUM durable_table",
        "SELECT 1; SELECT 2",
        "SELECT 1 INTO/*gap*/ TEMP TABLE forged FROM public.sync_cursors",
        "SELECT public.side_effect () FROM public.sync_cursors",
        "SELECT public.side_effect/*gap*/() FROM public.sync_cursors",
        (
            "SELECT pg_catalog.set_config "
            "('application_name', 'forged', false) FROM public.sync_cursors"
        ),
        "SELECT * FROM public.sync_cursors_forged",
        "SELECT * FROM public.sync_cursors_backup",
        "SELECT 1 /* FROM public.sync_cursors */",
        "SELECT * FROM public.sync_cursors, public.emails",
        'SELECT public."SideEffect" () FROM public.sync_cursors',
        'SELECT "select"() FROM public.sync_cursors',
        "SELECT Δfunc () FROM public.sync_cursors",
        "SELECT 'unterminated FROM public.sync_cursors",
        "SELECT $tag$unterminated FROM public.sync_cursors",
        "SELECT cursor FROM public.sync_cursors UNION TABLE public.emails",
        'SELECT * FROM "public"."sync_cursors"()',
        "SELECT cursor::public.evil FROM public.sync_cursors",
        "SELECT cursor FROM public.sync_cursors -- comment\r; SELECT 2",
        "SELECT cursor FROM public.sync_cursors -- comment\r\n; SELECT 2",
        "SELECT cursor FROM public.sync_cursors -- comment\n; SELECT 2",
        (
            "SELECT cursor.cursor FROM public.sync_cursors AS cursor "
            "JOIN public.emails AS email ON true"
        ),
    ],
)
async def test_completed_replay_guard_rejects_non_read_only_sql_before_forwarding(
    statement: str,
) -> None:
    class NeverForward:
        async def execute(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("mutation reached the database connection")

    statements: list[str] = []
    connection = _ReplayGuardConnection(NeverForward(), statements)

    with pytest.raises(
        AssertionError,
        match="completed replay attempted non-read-only SQL",
    ):
        await connection.execute(statement)

    assert statements == [statement]


@pytest.mark.asyncio
async def test_completed_replay_guard_allows_select_for_update_locking_read() -> None:
    class Forward:
        def __init__(self) -> None:
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            return "forwarded"

    raw = Forward()
    statements: list[str] = []
    connection = _ReplayGuardConnection(raw, statements)
    statement = "SELECT cursor FROM public.sync_cursors FOR UPDATE"

    result = await connection.execute(statement, (1,))

    assert result == "forwarded"
    assert statements == [statement]
    assert raw.calls == [(statement, (1,))]


@pytest.mark.parametrize(
    "statement",
    [
        (
            "UPDATE public.sync_cursors AS cursor SET "
            "status = 'blocked_contract', version = cursor.version + 1"
        ),
        (
            "UPDATE public.sync_cursors AS cursor SET "
            "status = 'blocked_contract', version = cursor.version + 2 "
            "WHERE account_id = %(account_id)s"
        ),
        (
            "UPDATE public.sync_cold_start_plans AS plan SET "
            "state = 'blocked', version = plan.version + 1"
        ),
        (
            "UPDATE public.sync_cold_start_plans AS plan SET "
            "state = %(target_state)s, version = plan.version + 2 "
            "WHERE plan_id = %(plan_id)s"
        ),
        "INSERT INTO public.event_inbox (forged) VALUES (1)",
        ("INSERT INTO public.cold_start_command_receipts (forged) VALUES (1)"),
        (
            "INSERT INTO public.audit_events (action, detail) "
            "VALUES ('forged', 'cold_start.block')"
        ),
        "INSERT INTO public.event_inbox_backup (id) VALUES (1)",
        "UPDATE private.sync_cursors SET cursor = %(next_cursor)s",
        "/* UPDATE public.sync_cursors SET cursor = %(next_cursor)s */ SELECT 1",
    ],
)
def test_apply_dml_marker_rejects_forged_or_incomplete_statement_shapes(
    statement: str,
) -> None:
    assert _apply_dml_marker(statement, None) is None


def _parameter_dict(
    keys: frozenset[str],
    **overrides: object,
) -> dict[str, object]:
    values: dict[str, object] = dict.fromkeys(keys)
    values.update(overrides)
    return values


def _apply_ack_loss_guard_fixture() -> tuple[
    _ApplyAckLossExpectedContract,
    tuple[tuple[str, object], ...],
]:
    plan_id = UUID("f7d96e11-a38e-45d4-a51c-87c00f35d93a")
    boundary_cursor = "opaque+Boundary-G7-guard/%3D"
    terminal_cursor = "opaque+Terminal-G7-guard/%3D"
    external_email_id = "ordinary-g7-guard"
    source_version = "version-1"
    (
        dedupe_key,
        apply_payload_hash,
        receipt_idempotency_hash,
        batch_result_hash,
    ) = _independent_apply_ack_loss_hashes(
        account_id=8,
        folder_key="INBOX",
        plan_id=plan_id,
        boundary_cursor=boundary_cursor,
        terminal_cursor=terminal_cursor,
        external_email_id=external_email_id,
        source_version=source_version,
    )
    expected = _ApplyAckLossExpectedContract(
        account_id=8,
        folder_key="INBOX",
        plan_id=plan_id,
        boundary_cursor=boundary_cursor,
        terminal_cursor=terminal_cursor,
        external_email_id=external_email_id,
        source_version=source_version,
        pipeline_name="durable_v1",
        generation=1,
        fencing_token=17,
        plan_hash="a" * 64,
        contract_fingerprint="b" * 64,
        config_hash="c" * 64,
        dedupe_key=dedupe_key,
        apply_payload_hash=apply_payload_hash,
        receipt_idempotency_hash=receipt_idempotency_hash,
        batch_result_hash=batch_result_hash,
    )
    database_stamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    event_params = (
        "0f1570b2-3bfe-4a2d-bc58-5a1cbbeefad4",
        expected.account_id,
        expected.external_email_id,
        expected.folder_key,
        "sync",
        "create",
        "create",
        expected.dedupe_key,
        expected.source_version,
        None,
        Jsonb(
            {
                "cursor": expected.terminal_cursor,
                "change_type": "create",
                "id": expected.external_email_id,
                "item": {
                    "id": expected.external_email_id,
                    "subject": "safe subject",
                },
                "source_version": expected.source_version,
            }
        ),
        "full",
        expected.pipeline_name,
        expected.generation,
        expected.fencing_token,
        "pending",
    )
    cursor_params = _parameter_dict(
        _APPLY_CURSOR_PARAM_KEYS,
        account_id=expected.account_id,
        folder_key=expected.folder_key,
        next_cursor=expected.terminal_cursor,
        target_status="active",
        target_plan_id=None,
        target_plan_state=None,
        database_stamp=database_stamp,
        expected_status="cold_start_pending",
        expected_cursor=None,
        expected_version=0,
        expected_blocked_reason_code="sync.cold_start_required",
        expected_contract_fingerprint=None,
        expected_blocked_at=None,
        expected_transient_failures=0,
        expected_retry_after_at=None,
        expected_plan_id=None,
        expected_plan_state=None,
    )
    plan_params = _parameter_dict(
        _APPLY_PLAN_PARAM_KEYS,
        account_id=expected.account_id,
        folder_key=expected.folder_key,
        plan_id=expected.plan_id,
        target_state="completed",
        terminal=True,
        next_cursor=expected.terminal_cursor,
        next_cursor_version=1,
        database_stamp=database_stamp,
        expected_version=2,
        expected_boundary_cursor=expected.boundary_cursor,
        expected_boundary_cursor_version=1,
        expected_apply_cursor=None,
        expected_apply_cursor_version=None,
        expected_plan_hash=expected.plan_hash,
        expected_contract_fingerprint=expected.contract_fingerprint,
        expected_config_hash=expected.config_hash,
    )
    receipt_params = (
        UUID("6f06c556-b1a4-4cba-a41a-56ec7aa1bfeb"),
        expected.account_id,
        "cold_start.apply_page",
        expected.receipt_idempotency_hash,
        expected.apply_payload_hash,
        "succeeded",
        "sync_cold_start_plan",
        str(expected.plan_id),
        expected.batch_result_hash,
        expected.fencing_token,
    )
    return expected, (
        ("event_inbox.insert", event_params),
        ("sync_cursors.update", cursor_params),
        ("sync_cold_start_plans.update", plan_params),
        ("cold_start_command_receipts.insert", receipt_params),
    )


def test_apply_ack_loss_guard_accepts_exact_terminal_contract_and_digest_targets() -> (
    None
):
    expected, sequence = _apply_ack_loss_guard_fixture()
    apply_digest_targets = tuple(_DML_TOKEN_DIGEST_MARKERS.items())[:4]

    assert tuple(marker for _, marker in apply_digest_targets) == _APPLY_DML_ORDER
    assert tuple(digest for digest, _ in apply_digest_targets) == (
        "5b0c03fa6dcd69bb23410a6e98e8e796889781e652fe60d7d0c8335b1c681411",
        "6dcbecbb1fcc35d386a08c2a4f95c96efe54518186537dc4102f1e8981568ec2",
        "7fba9cda0e5f8c8371ee0f581d762524198da98a568ca4b6162b1fe3a45c26f1",
        "d0c5337483e3ec81ffd307409b43ba3beaac5bdb6513845395a626a150bb2d04",
    )
    assert all(
        len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
        for value in (
            expected.dedupe_key,
            expected.apply_payload_hash,
            expected.receipt_idempotency_hash,
            expected.batch_result_hash,
        )
    )
    prior: list[tuple[str, object]] = []
    for marker, params in sequence:
        assert _dml_params_are_allowed(marker, params)
        assert _apply_ack_loss_params_are_exact(
            marker,
            params,
            expected=expected,
            prior=prior,
        )
        prior.append((marker, params))


def test_apply_ack_loss_guard_rejects_forged_values_and_cross_dml_contracts() -> None:
    expected, sequence = _apply_ack_loss_guard_fixture()
    event_params = sequence[0][1]
    cursor_params = sequence[1][1]
    plan_params = sequence[2][1]
    receipt_params = sequence[3][1]
    assert type(event_params) is tuple
    assert type(cursor_params) is dict
    assert type(plan_params) is dict
    assert type(receipt_params) is tuple

    def changed_tuple(
        values: tuple[object, ...],
        index: int,
        forged: object,
    ) -> tuple[object, ...]:
        changed = list(values)
        changed[index] = forged
        return tuple(changed)

    for forged_event in (
        changed_tuple(event_params, 1, True),
        changed_tuple(event_params, 7, "f" * 64),
        changed_tuple(event_params, 10, Jsonb({"forged": True})),
        (*event_params, "extra"),
    ):
        assert not _apply_ack_loss_params_are_exact(
            "event_inbox.insert",
            forged_event,
            expected=expected,
            prior=[],
        )

    event_prior = [sequence[0]]
    for forged_cursor in (
        {**cursor_params, "account_id": True},
        {**cursor_params, "next_cursor": "forged-cursor"},
        {**cursor_params, "expected_version": True},
        {**cursor_params, "forged": None},
    ):
        assert not _apply_ack_loss_params_are_exact(
            "sync_cursors.update",
            forged_cursor,
            expected=expected,
            prior=event_prior,
        )

    cursor_prior = list(sequence[:2])
    for forged_plan in (
        {**plan_params, "plan_id": str(expected.plan_id)},
        {**plan_params, "expected_version": True},
        {**plan_params, "expected_plan_hash": "A" * 64},
        {
            **plan_params,
            "database_stamp": datetime(2026, 7, 16, 12, 0, 1, tzinfo=UTC),
        },
    ):
        assert not _apply_ack_loss_params_are_exact(
            "sync_cold_start_plans.update",
            forged_plan,
            expected=expected,
            prior=cursor_prior,
        )

    plan_prior = list(sequence[:3])
    for index, forged in (
        (3, "f" * 64),
        (4, "e" * 64),
        (8, "d" * 64),
        (9, True),
    ):
        assert not _apply_ack_loss_params_are_exact(
            "cold_start_command_receipts.insert",
            changed_tuple(receipt_params, index, forged),
            expected=expected,
            prior=plan_prior,
        )


def test_apply_ack_loss_fault_budget_is_pool_shared_and_one_shot() -> None:
    state = _ApplyAckLossFaultState("commit_then_raise")
    error = state.error

    assert state.claim_fault() is True
    assert state.claim_fault() is False
    assert state.fault_budget == 0
    assert state.error is error
    assert str(state.error) == "terminal apply transaction acknowledgement lost"


def test_apply_dml_parameter_contracts_reject_wrong_tuple_shapes_and_values() -> None:
    event = tuple(range(16))
    assert _dml_params_are_allowed("event_inbox.insert", event)
    assert not _dml_params_are_allowed("event_inbox.insert", event[:-1])
    assert not _dml_params_are_allowed("event_inbox.insert", list(event))

    receipt = (
        None,
        None,
        "cold_start.apply_page",
        None,
        None,
        "succeeded",
        "sync_cold_start_plan",
        None,
        None,
        None,
    )
    assert _dml_params_are_allowed("cold_start_command_receipts.insert", receipt)
    for index, forged in (
        (2, "cold_start.preview"),
        (5, "failed"),
        (6, "forged_result"),
    ):
        changed = list(receipt)
        changed[index] = forged
        assert not _dml_params_are_allowed(
            "cold_start_command_receipts.insert",
            tuple(changed),
        )
    assert not _dml_params_are_allowed(
        "cold_start_command_receipts.insert",
        receipt[:-1],
    )

    audit = (
        None,
        None,
        None,
        None,
        "blocked",
        "cold_start_service",
        "exchange.sync.contract_invalid",
        None,
    )
    assert _dml_params_are_allowed("audit_events.block_insert", audit)
    for index, forged in (
        (4, "forged"),
        (5, "forged_actor"),
        (6, "forged.safe_code"),
    ):
        changed = list(audit)
        changed[index] = forged
        assert not _dml_params_are_allowed(
            "audit_events.block_insert",
            tuple(changed),
        )
    assert not _dml_params_are_allowed("audit_events.block_insert", audit[:-1])


def test_apply_dml_parameter_contracts_reject_wrong_cas_keys_and_state_pairs() -> None:
    cursor = _parameter_dict(
        _APPLY_CURSOR_PARAM_KEYS,
        target_status="active",
        target_plan_id=None,
        target_plan_state=None,
    )
    assert _dml_params_are_allowed("sync_cursors.update", cursor)
    assert not _dml_params_are_allowed(
        "sync_cursors.update",
        {**cursor, "target_plan_id": "forged"},
    )
    assert not _dml_params_are_allowed(
        "sync_cursors.update",
        {**cursor, "forged": None},
    )
    missing_cursor_key = dict(cursor)
    missing_cursor_key.pop("expected_version")
    assert not _dml_params_are_allowed("sync_cursors.update", missing_cursor_key)

    plan = _parameter_dict(
        _APPLY_PLAN_PARAM_KEYS,
        terminal=True,
        target_state="completed",
    )
    assert _dml_params_are_allowed("sync_cold_start_plans.update", plan)
    assert not _dml_params_are_allowed(
        "sync_cold_start_plans.update",
        {**plan, "target_state": "approved"},
    )
    assert not _dml_params_are_allowed(
        "sync_cold_start_plans.update",
        {**plan, "forged": None},
    )
    missing_plan_key = dict(plan)
    missing_plan_key.pop("expected_version")
    assert not _dml_params_are_allowed(
        "sync_cold_start_plans.update",
        missing_plan_key,
    )

    blocked_plan = _parameter_dict(
        _BLOCK_PLAN_PARAM_KEYS,
        safe_code="exchange.sync.contract_invalid",
        expected_state="approved",
    )
    assert _dml_params_are_allowed(
        "sync_cold_start_plans.blocked_update",
        blocked_plan,
    )
    assert not _dml_params_are_allowed(
        "sync_cold_start_plans.blocked_update",
        {**blocked_plan, "safe_code": "forged.safe_code"},
    )
    assert not _dml_params_are_allowed(
        "sync_cold_start_plans.blocked_update",
        {**blocked_plan, "expected_state": "ready"},
    )

    blocked_cursor = _parameter_dict(
        _BLOCK_CURSOR_PARAM_KEYS,
        safe_code="exchange.sync.contract_invalid",
        expected_status="cold_start_pending",
    )
    assert _dml_params_are_allowed(
        "sync_cursors.blocked_update",
        blocked_cursor,
    )
    assert not _dml_params_are_allowed(
        "sync_cursors.blocked_update",
        {**blocked_cursor, "safe_code": "forged.safe_code"},
    )
    assert not _dml_params_are_allowed(
        "sync_cursors.blocked_update",
        {**blocked_cursor, "expected_status": "active"},
    )


def _preview_acceptance_plan_guard_params(
    plan_id: UUID | None = None,
) -> tuple[object, ...]:
    return (
        plan_id or uuid4(),
        8,
        "INBOX",
        "cold_start_pending",
        None,
        0,
        "durable_v1",
        1,
        17,
        "a" * 64,
        "b" * 64,
        "g6-actor",
        "prove exact preview acceptance",
        3600,
    )


def _preview_acceptance_receipt_guard_params(
    plan_id: UUID,
) -> tuple[object, ...]:
    return (
        uuid4(),
        8,
        "cold_start.preview",
        "c" * 64,
        "d" * 64,
        "succeeded",
        "sync_cold_start_plan",
        str(plan_id),
        "e" * 64,
        17,
    )


def _preview_acceptance_receipt_guard_sql() -> str:
    return (
        'INSERT INTO "public"."cold_start_command_receipts" ('
        "id, account_id, command_name, idempotency_key_hash, "
        "canonical_payload_hash, outcome, result_type, result_id, "
        "result_hash, authority_epoch"
        ") VALUES ("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        ") RETURNING "
        "id, account_id, command_name, idempotency_key_hash, "
        "canonical_payload_hash, outcome, result_type, result_id, "
        "result_hash, authority_epoch, created_at"
    )


def _command_receipt_lookup_guard_sql() -> str:
    return (
        "SELECT id, account_id, command_name, idempotency_key_hash, "
        "canonical_payload_hash, outcome, result_type, result_id, "
        "result_hash, authority_epoch, created_at "
        'FROM "public"."cold_start_command_receipts" '
        "WHERE account_id = %s AND command_name = %s "
        "AND idempotency_key_hash = %s"
    )


def _approve_audit_guard_sql() -> str:
    return (
        "INSERT INTO public.audit_events ("
        "id, event_key, account_id, email_id, object_type, "
        "object_fingerprint, action, result, actor, reason, safe_metadata"
        ") VALUES (%s, %s, %s, NULL, 'sync_cold_start_plan', %s, "
        "'cold_start.approve', %s, %s, %s, %s)"
    )


_APPROVE_PLAN_RETURNING_KEYS = frozenset(
    {
        "plan_id",
        "account_id",
        "folder_key",
        "expected_cursor_status",
        "expected_cursor",
        "expected_cursor_version",
        "pipeline_name",
        "generation",
        "fencing_token",
        "state",
        "version",
        "preview_cursor",
        "preview_cursor_version",
        "boundary_cursor",
        "boundary_cursor_version",
        "apply_cursor",
        "apply_cursor_version",
        "rolling_hash",
        "page_count",
        "item_count",
        "redacted_samples",
        "contract_fingerprint",
        "folder_scope_config_hash",
        "plan_hash",
        "actor",
        "reason",
        "blocked_reason_code",
        "blocked_fingerprint",
        "expires_at",
        "ready_at",
        "approved_at",
        "completed_at",
        "blocked_at",
        "created_at",
        "updated_at",
    }
)


def _approve_plan_guard_sql() -> str:
    return (
        "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
        "UPDATE public.sync_cold_start_plans AS plan SET state = 'approved', "
        "version = plan.version + 1, approved_at = stamp.at, "
        "updated_at = stamp.at FROM stamp "
        "WHERE plan.plan_id = %(plan_id)s "
        "AND plan.account_id = %(account_id)s "
        "AND plan.folder_key = %(folder_key)s AND plan.state = 'ready' "
        "AND plan.version = %(expected_version)s "
        "AND plan.plan_hash = %(expected_plan_hash)s "
        "AND plan.boundary_cursor = %(expected_boundary_cursor)s "
        "AND plan.boundary_cursor_version = "
        "%(expected_boundary_cursor_version)s "
        "AND stamp.at < plan.expires_at RETURNING "
        + ", ".join(sorted(_APPROVE_PLAN_RETURNING_KEYS))
    )


def _command_plan_lookup_guard_sql() -> str:
    return (
        "SELECT "
        + ", ".join(sorted(_APPROVE_PLAN_RETURNING_KEYS))
        + " FROM public.sync_cold_start_plans "
        "WHERE plan_id = %s FOR UPDATE"
    )


for _command_replay_statement, _command_replay_marker_name in (
    (_command_receipt_lookup_guard_sql(), "receipt_lookup"),
    (_command_plan_lookup_guard_sql(), "plan_lookup"),
):
    _command_replay_tokens = _sql_tokens(_command_replay_statement)
    assert _command_replay_tokens is not None
    _COMMAND_REPLAY_SQL_DIGEST_MARKERS[
        hashlib.sha256("\x00".join(_command_replay_tokens).encode("utf-8")).hexdigest()
    ] = _command_replay_marker_name
assert set(_COMMAND_REPLAY_SQL_DIGEST_MARKERS.values()) == set(
    _COMMAND_REPLAY_MARKER_ORDER
)


def test_approve_ack_loss_guard_recognizes_only_exact_audit_dml() -> None:
    params = (
        uuid4(),
        "a" * 64,
        8,
        "b" * 64,
        "approved",
        "g8-approver",
        "approve exact durable plan",
        Jsonb(
            {
                "plan_id": "2238c88c-d065-44ca-8fc7-6df1818b547e",
                "plan_hash": "c" * 64,
                "page_count": 1,
                "item_count": 1,
                "redacted_samples": [
                    {"kind": "create", "external_email_id_hash": "d" * 64}
                ],
            }
        ),
    )

    assert _approve_ack_loss_dml_marker(_approve_audit_guard_sql(), params) == (
        "audit_events.approve_insert"
    )
    assert (
        _approve_ack_loss_dml_marker(
            _approve_audit_guard_sql() + "; DELETE FROM public.audit_events",
            params,
        )
        is None
    )
    forged = list(params)
    forged[4] = "failed"
    assert (
        _approve_ack_loss_dml_marker(
            _approve_audit_guard_sql(),
            tuple(forged),
        )
        is None
    )


def _approve_ack_loss_direct_state() -> _ApproveAckLossFaultState:
    plan_id = UUID("2238c88c-d065-44ca-8fc7-6df1818b547e")
    return _ApproveAckLossFaultState(
        plan_id=plan_id,
        ready_plan={
            "plan_id": str(plan_id),
            "state": "ready",
            "account_id": 8,
            "folder_key": "INBOX",
            "version": 1,
            "plan_hash": "c" * 64,
            "boundary_cursor": "opaque+G8-direct/%3D",
            "boundary_cursor_version": 1,
            "fencing_token": 1,
        },
        actor="g8-approver",
        reason="approve exact durable plan",
        idempotency_key="g8-approve-direct-key",
    )


def _approve_direct_plan_params(
    state: _ApproveAckLossFaultState,
) -> dict[str, object]:
    ready = state.ready_plan
    return {
        "plan_id": state.plan_id,
        "account_id": ready["account_id"],
        "folder_key": ready["folder_key"],
        "expected_version": ready["version"],
        "expected_plan_hash": ready["plan_hash"],
        "expected_boundary_cursor": ready["boundary_cursor"],
        "expected_boundary_cursor_version": ready["boundary_cursor_version"],
    }


def _bind_approve_direct_approved_row(
    state: _ApproveAckLossFaultState,
) -> dict[str, Any]:
    state.capture_approved_row(
        {
            "plan_id": state.plan_id,
            "account_id": state.ready_plan["account_id"],
            "state": "approved",
            "plan_hash": "c" * 64,
            "page_count": 1,
            "item_count": 1,
            "redacted_samples": [
                {"kind": "create", "external_email_id_hash": "d" * 64}
            ],
            "pipeline_name": "durable_v1",
            "generation": 1,
            "fencing_token": 1,
            "folder_scope_config_hash": "e" * 64,
            "approved_at": datetime(2026, 7, 16, 8, 30, tzinfo=UTC),
        }
    )
    assert state.approved_row is not None
    return state.approved_row


def _approve_direct_audit_params(
    state: _ApproveAckLossFaultState,
) -> tuple[object, ...]:
    approved = state.approved_row
    assert approved is not None
    return (
        UUID("6f06c556-b1a4-4cba-a41a-56ec7aa1bfeb"),
        _independent_canonical_digest(
            "cold-start.audit-event.v1",
            {
                "v": 1,
                "action": "cold_start.approve",
                "plan_id": str(state.plan_id),
                "plan_version": state.ready_plan["version"],
            },
        ),
        state.ready_plan["account_id"],
        _independent_canonical_digest(
            "cold-start.audit-object.v1",
            {"v": 1, "plan_id": str(state.plan_id)},
        ),
        "approved",
        state.actor,
        state.reason,
        Jsonb(
            {
                "plan_id": str(state.plan_id),
                "plan_hash": approved["plan_hash"],
                "page_count": approved["page_count"],
                "item_count": approved["item_count"],
                "redacted_samples": approved["redacted_samples"],
            }
        ),
    )


def _approve_direct_receipt_params(
    state: _ApproveAckLossFaultState,
) -> tuple[object, ...]:
    approved = state.approved_row
    assert approved is not None
    payload_hash, idempotency_hash, result_hash = _independent_approve_command_hashes(
        approved=approved,
        actor=state.actor,
        reason=state.reason,
        idempotency_key=state.idempotency_key,
    )
    return (
        UUID("24ef6735-d7a7-44df-bbc4-0f19a2de8c43"),
        state.ready_plan["account_id"],
        "cold_start.approve",
        idempotency_hash,
        payload_hash,
        "succeeded",
        "sync_cold_start_plan",
        str(state.plan_id),
        result_hash,
        state.ready_plan["fencing_token"],
    )


def _approve_direct_dml(
    state: _ApproveAckLossFaultState,
    marker: str,
) -> tuple[str, object]:
    if marker == "sync_cold_start_plans.approve_update":
        return _approve_plan_guard_sql(), _approve_direct_plan_params(state)
    if marker == "audit_events.approve_insert":
        return _approve_audit_guard_sql(), _approve_direct_audit_params(state)
    if marker == "approve_command_receipts.insert":
        return (
            _preview_acceptance_receipt_guard_sql(),
            _approve_direct_receipt_params(state),
        )
    raise AssertionError("unknown direct approve DML marker")


def _approve_direct_observation(
    state: _ApproveAckLossFaultState,
    marker: str,
) -> _ApproveAckLossDmlObservation:
    statement, params = _approve_direct_dml(state, marker)
    digest = hashlib.sha256(
        "\x00".join(_sql_tokens(statement) or ()).encode("utf-8")
    ).hexdigest()
    assert _APPROVE_COMMAND_DML_TOKEN_DIGEST_MARKERS[digest] == marker
    return _ApproveAckLossDmlObservation(
        marker=marker,
        statement_digest=digest,
        rowcount=1,
        transaction_id="123",
        backend_pid=12345,
        transaction_status=TransactionStatus.INTRANS,
        params=params,
    )


class _ApproveDmlNeverDelegate:
    class Info:
        backend_pid = 12345

        def __init__(self, status: object) -> None:
            self.transaction_status = status

    def __init__(self, status: object = TransactionStatus.INTRANS) -> None:
        self.info = self.Info(status)
        self.calls: list[tuple[object, object]] = []

    async def execute(self, statement: object, params: object = None) -> object:
        self.calls.append((statement, params))
        raise AssertionError("invalid approve DML reached delegate")


async def _assert_direct_approve_dml_rejected(
    *,
    state: _ApproveAckLossFaultState,
    statement: str,
    params: object,
    active_transaction: bool = True,
    error: str = "unexpected approve ACK-loss DML",
) -> None:
    delegate = _ApproveDmlNeverDelegate(
        TransactionStatus.INTRANS if active_transaction else TransactionStatus.IDLE
    )
    connection = _ApproveAckLossConnection(delegate, state, phase="origin")
    if active_transaction:
        connection._active_transaction = object()  # type: ignore[assignment]
    before = list(state.observations)

    with pytest.raises(AssertionError, match=error):
        await connection.execute(statement, params)

    assert delegate.calls == []
    assert state.observations == before


class _ReplayRelationNeverDelegate:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    def __init__(self) -> None:
        self.info = self.Info()
        self.calls: list[tuple[object, object]] = []

    async def execute(self, statement: object, params: object = None) -> object:
        self.calls.append((statement, params))
        raise AssertionError("forbidden replay relation reached delegate")


@pytest.mark.asyncio
async def test_preview_acceptance_recovery_rejects_event_inbox_before_delegate() -> (
    None
):
    delegate = _ReplayRelationNeverDelegate()
    state = _PreviewAckLossFaultState(
        "commit_then_raise",
        fault_target="acceptance",
        expected_replay_idempotency_key="g8-preview-direct-key",
    )
    state.bind_accepted_plan_id(UUID("2d9f38fd-fb9e-44a7-a457-37f641680ebb"))
    connection = _PreviewAckLossConnection(delegate, state, phase="recovery")

    with pytest.raises(
        AssertionError,
        match="unexpected preview recovery business relation",
    ):
        await connection.execute(
            "SELECT id FROM public.event_inbox WHERE id = %s",
            (uuid4(),),
        )

    assert delegate.calls == []


@pytest.mark.asyncio
async def test_approve_replay_rejects_event_inbox_before_delegate() -> None:
    delegate = _ReplayRelationNeverDelegate()
    state = _approve_ack_loss_direct_state()
    connection = _ApproveAckLossConnection(delegate, state, phase="replay")

    with pytest.raises(
        AssertionError,
        match="unexpected approve replay business relation",
    ):
        await connection.execute(
            "SELECT id FROM public.event_inbox WHERE id = %s",
            (uuid4(),),
        )

    assert delegate.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("flow", "attack"),
    [
        ("preview", "wrong-account"),
        ("preview", "receipt-plan-join"),
        ("preview", "wrong-hash"),
        ("preview", "receipt-extra-column"),
        ("preview", "receipt-where-deformation"),
        ("preview", "wrong-plan-id"),
        ("preview", "plan-extra-column"),
        ("preview", "plan-where-deformation"),
        ("preview", "repeated-receipt"),
        ("preview", "plan-before-receipt"),
        ("approve", "wrong-account"),
        ("approve", "receipt-plan-join"),
        ("approve", "wrong-hash"),
        ("approve", "receipt-extra-column"),
        ("approve", "receipt-where-deformation"),
        ("approve", "wrong-plan-id"),
        ("approve", "plan-extra-column"),
        ("approve", "plan-where-deformation"),
        ("approve", "repeated-receipt"),
        ("approve", "plan-before-receipt"),
    ],
)
async def test_command_replay_dynamic_counterexample_rejected_before_delegate(
    flow: str,
    attack: str,
) -> None:
    delegate = _ReplayRelationNeverDelegate()
    if flow == "preview":
        idempotency_key = "g8-preview-dynamic-key"
        state = _PreviewAckLossFaultState(
            "commit_then_raise",
            fault_target="acceptance",
            expected_replay_idempotency_key=idempotency_key,
        )
        state.bind_accepted_plan_id(UUID("2d9f38fd-fb9e-44a7-a457-37f641680ebb"))
        connection: Any = _PreviewAckLossConnection(
            delegate,
            state,
            phase="recovery",
        )
        error = "unexpected preview recovery business relation"
        command_name = "cold_start.preview"
    else:
        state = _approve_ack_loss_direct_state()
        idempotency_key = state.idempotency_key
        connection = _ApproveAckLossConnection(delegate, state, phase="replay")
        error = "unexpected approve replay business relation"
        command_name = "cold_start.approve"
    expected = state.replay_expectation()
    if attack == "wrong-account":
        statement = _command_receipt_lookup_guard_sql()
        params: object = (
            999,
            command_name,
            _independent_command_idempotency_hash(
                8,
                command_name,
                idempotency_key,
            ),
        )
    elif attack == "receipt-plan-join":
        statement = (
            "SELECT receipt.id FROM public.cold_start_command_receipts AS receipt "
            "JOIN public.sync_cold_start_plans AS plan "
            "ON receipt.result_id = plan.plan_id"
        )
        params = None
    elif attack == "wrong-hash":
        statement = _command_receipt_lookup_guard_sql()
        params = (expected.account_id, expected.command_name, "f" * 64)
    elif attack == "receipt-extra-column":
        statement = _command_receipt_lookup_guard_sql().replace(
            "SELECT id,",
            "SELECT forged, id,",
        )
        params = (
            expected.account_id,
            expected.command_name,
            expected.idempotency_key_hash,
        )
    elif attack == "receipt-where-deformation":
        statement = _command_receipt_lookup_guard_sql() + " AND result_id = %s"
        params = (
            expected.account_id,
            expected.command_name,
            expected.idempotency_key_hash,
            str(expected.plan_id),
        )
    elif attack == "wrong-plan-id":
        statement = _command_plan_lookup_guard_sql()
        params = (UUID("d4ef0da1-f7cc-48cf-9440-597682bda5ef"),)
    elif attack == "plan-extra-column":
        statement = _command_plan_lookup_guard_sql().replace(
            "SELECT ",
            "SELECT forged, ",
            1,
        )
        params = (expected.plan_id,)
    elif attack == "plan-where-deformation":
        statement = _command_plan_lookup_guard_sql().replace(
            "WHERE plan_id = %s",
            "WHERE plan_id = %s AND account_id = %s",
        )
        params = (expected.plan_id, expected.account_id)
    elif attack == "repeated-receipt":
        if flow == "preview":
            state.recovery_business_marker_trace.append("receipt_lookup")
        else:
            state.replay_business_marker_trace.append("receipt_lookup")
        statement = _command_receipt_lookup_guard_sql()
        params = (
            expected.account_id,
            expected.command_name,
            expected.idempotency_key_hash,
        )
    else:
        assert attack == "plan-before-receipt"
        statement = _command_plan_lookup_guard_sql()
        params = (expected.plan_id,)

    with pytest.raises(AssertionError, match=error):
        await connection.execute(statement, params)

    assert delegate.calls == []


@pytest.mark.asyncio
async def test_approve_ack_loss_rejects_shape_valid_state_wrong_plan_before_delegate() -> (
    None
):
    state = _approve_ack_loss_direct_state()
    statement, baseline = _approve_direct_dml(
        state,
        "sync_cold_start_plans.approve_update",
    )
    assert type(baseline) is dict
    mutated = {**baseline, "expected_version": baseline["expected_version"] + 1}
    assert _approve_ack_loss_dml_marker(statement, mutated) == (
        "sync_cold_start_plans.approve_update"
    )

    await _assert_direct_approve_dml_rejected(
        state=state,
        statement=statement,
        params=mutated,
    )


@pytest.mark.asyncio
async def test_approve_ack_loss_rejects_audit_cross_field_before_delegate() -> None:
    state = _approve_ack_loss_direct_state()
    _bind_approve_direct_approved_row(state)
    state.observations.append(
        _approve_direct_observation(
            state,
            "sync_cold_start_plans.approve_update",
        )
    )
    statement, baseline = _approve_direct_dml(state, "audit_events.approve_insert")
    assert type(baseline) is tuple
    metadata = baseline[7]
    assert type(metadata) is Jsonb and type(metadata.obj) is dict
    mutated_metadata = dict(metadata.obj)
    mutated_metadata["plan_hash"] = "f" * 64
    mutated = (*baseline[:7], Jsonb(mutated_metadata))
    assert _approve_ack_loss_dml_marker(statement, mutated) == (
        "audit_events.approve_insert"
    )

    await _assert_direct_approve_dml_rejected(
        state=state,
        statement=statement,
        params=mutated,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_index", "replacement"),
    [
        (3, "0" * 64),
        (4, "1" * 64),
        (8, "2" * 64),
        (9, 2),
        (7, "f448b9ac-9672-489d-a735-e38c640ad0c4"),
    ],
    ids=("key", "payload", "result", "fence", "plan-id"),
)
async def test_approve_ack_loss_rejects_receipt_cross_field_before_delegate(
    field_index: int,
    replacement: object,
) -> None:
    state = _approve_ack_loss_direct_state()
    _bind_approve_direct_approved_row(state)
    state.observations.extend(
        _approve_direct_observation(state, marker)
        for marker in _APPROVE_COMMAND_DML_ORDER[:2]
    )
    statement, baseline = _approve_direct_dml(
        state,
        "approve_command_receipts.insert",
    )
    assert type(baseline) is tuple
    mutated = list(baseline)
    mutated[field_index] = replacement
    params = tuple(mutated)
    assert _approve_ack_loss_dml_marker(statement, params) == (
        "approve_command_receipts.insert"
    )

    await _assert_direct_approve_dml_rejected(
        state=state,
        statement=statement,
        params=params,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("marker", "prior_markers"),
    [
        ("audit_events.approve_insert", ()),
        (
            "approve_command_receipts.insert",
            ("sync_cold_start_plans.approve_update",),
        ),
        ("approve_command_receipts.insert", _APPROVE_COMMAND_DML_ORDER),
    ],
    ids=("audit-before-plan", "receipt-before-audit", "fourth-dml"),
)
async def test_approve_ack_loss_rejects_wrong_order_or_fourth_dml_before_delegate(
    marker: str,
    prior_markers: tuple[str, ...],
) -> None:
    state = _approve_ack_loss_direct_state()
    _bind_approve_direct_approved_row(state)
    state.observations.extend(
        _approve_direct_observation(state, prior) for prior in prior_markers
    )
    statement, params = _approve_direct_dml(state, marker)

    await _assert_direct_approve_dml_rejected(
        state=state,
        statement=statement,
        params=params,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", _APPROVE_COMMAND_DML_ORDER)
async def test_approve_ack_loss_rejects_exact_dml_outside_xid_before_delegate(
    marker: str,
) -> None:
    state = _approve_ack_loss_direct_state()
    _bind_approve_direct_approved_row(state)
    index = _APPROVE_COMMAND_DML_ORDER.index(marker)
    state.observations.extend(
        _approve_direct_observation(state, prior)
        for prior in _APPROVE_COMMAND_DML_ORDER[:index]
    )
    statement, params = _approve_direct_dml(state, marker)

    await _assert_direct_approve_dml_rejected(
        state=state,
        statement=statement,
        params=params,
        active_transaction=False,
        error="approve DML attempted outside active transaction",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("marker", "approved_field", "replacement"),
    [
        ("audit_events.approve_insert", "page_count", 2),
        ("approve_command_receipts.insert", "pipeline_name", "forged_v2"),
    ],
)
async def test_approve_ack_loss_cross_dml_binding_mutation_rejected_before_delegate(
    marker: str,
    approved_field: str,
    replacement: object,
) -> None:
    state = _approve_ack_loss_direct_state()
    approved = _bind_approve_direct_approved_row(state)
    index = _APPROVE_COMMAND_DML_ORDER.index(marker)
    state.observations.extend(
        _approve_direct_observation(state, prior)
        for prior in _APPROVE_COMMAND_DML_ORDER[:index]
    )
    statement, params = _approve_direct_dml(state, marker)
    approved[approved_field] = replacement

    await _assert_direct_approve_dml_rejected(
        state=state,
        statement=statement,
        params=params,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("statement", "params"),
    [
        ("DELETE FROM public.sync_cold_start_plans", None),
        (
            _approve_audit_guard_sql(),
            (
                UUID("6f06c556-b1a4-4cba-a41a-56ec7aa1bfeb"),
                "a" * 64,
                8,
                "b" * 64,
                "failed",
                "g8-approver",
                "approve exact durable plan",
                Jsonb({"forged": True}),
            ),
        ),
    ],
)
async def test_approve_ack_loss_guard_rejects_unknown_or_forged_before_delegate(
    statement: str,
    params: object,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, sql: object, values: object = None) -> object:
            self.calls.append((sql, values))
            raise AssertionError("forged approve DML reached delegate")

    delegate = NeverDelegate()
    state = _approve_ack_loss_direct_state()
    connection = _ApproveAckLossConnection(delegate, state, phase="origin")
    connection._active_transaction = object()  # type: ignore[assignment]

    with pytest.raises(AssertionError, match="unexpected approve origin SQL"):
        await connection.execute(statement, params)

    assert delegate.calls == []
    assert state.observations == []


def test_approve_ack_loss_fault_budget_is_pool_shared_and_one_shot() -> None:
    state = _approve_ack_loss_direct_state()
    error = state.error

    assert state.claim_fault() is True
    assert state.claim_fault() is False
    assert state.fault_budget == 0
    assert state.error is error
    assert str(error) == "approve command transaction acknowledgement lost"


@pytest.mark.asyncio
async def test_approve_ack_loss_pool_rejects_fifth_checkout_before_delegate() -> None:
    class Info:
        def __init__(self, backend_pid: int) -> None:
            self.backend_pid = backend_pid
            self.transaction_status = TransactionStatus.IDLE

    class RawConnection:
        def __init__(self, backend_pid: int) -> None:
            self.info = Info(backend_pid)
            self.closed = False

    class ProbePool:
        def __init__(self) -> None:
            self.kwargs: dict[str, object] = {}
            self.close_returns = False
            self.getconn_calls = 0
            self.connections = [
                RawConnection(41001),
                RawConnection(41001),
                RawConnection(41002),
                RawConnection(41002),
            ]

        async def getconn(self) -> RawConnection:
            self.getconn_calls += 1
            if not self.connections:
                raise AssertionError("fifth checkout reached underlying pool")
            return self.connections.pop(0)

    raw_pool = ProbePool()
    guarded_pool = _ApproveAckLossPool(  # type: ignore[arg-type]
        raw_pool,
        _approve_ack_loss_direct_state(),
    )

    connections = [await guarded_pool.getconn() for _ in range(4)]

    assert [connection._phase for connection in connections] == list(
        _ApproveAckLossPool._PHASES
    )
    assert raw_pool.getconn_calls == 4
    raw_pool.getconn_calls = 0
    with pytest.raises(
        AssertionError,
        match="unexpected fifth approve ACK-loss checkout",
    ):
        await guarded_pool.getconn()
    assert raw_pool.getconn_calls == 0


def _preview_page_guard_params() -> dict[str, object]:
    return _parameter_dict(
        _PREVIEW_PAGE_UPDATE_PARAM_KEYS,
        target_state="ready",
        terminal=True,
        next_cursor="opaque+G6/%3D",
        boundary_cursor="opaque+G6/%3D",
        boundary_cursor_version=1,
        rolling_hash="a" * 64,
        plan_hash="b" * 64,
        expected_version=0,
        expected_preview_cursor=None,
        expected_preview_cursor_version=0,
        expected_rolling_hash=None,
        expected_page_count=0,
        expected_item_count=0,
        item_count=1,
    )


def _preview_page_guard_sql() -> str:
    return (
        "WITH stamp AS (SELECT pg_catalog.clock_timestamp() AS at) "
        "UPDATE public.sync_cold_start_plans AS plan SET "
        "state = %(target_state)s, version = plan.version + 1, "
        "preview_cursor = %(next_cursor)s, "
        "preview_cursor_version = plan.preview_cursor_version + 1, "
        "boundary_cursor = %(boundary_cursor)s, "
        "boundary_cursor_version = %(boundary_cursor_version)s, "
        "rolling_hash = %(rolling_hash)s, "
        "page_count = plan.page_count + 1, item_count = %(item_count)s, "
        "redacted_samples = %(redacted_samples)s, "
        "plan_hash = %(plan_hash)s, "
        "ready_at = CASE WHEN %(terminal)s THEN stamp.at ELSE NULL END, "
        "updated_at = stamp.at FROM stamp "
        "WHERE plan.plan_id = %(plan_id)s "
        "AND plan.account_id = %(account_id)s "
        "AND plan.folder_key = %(folder_key)s "
        "AND plan.state = 'previewing' "
        "AND plan.version = %(expected_version)s "
        "AND plan.preview_cursor IS NOT DISTINCT FROM "
        "%(expected_preview_cursor)s "
        "AND plan.preview_cursor_version = "
        "%(expected_preview_cursor_version)s "
        "AND plan.rolling_hash IS NOT DISTINCT FROM "
        "%(expected_rolling_hash)s "
        "AND plan.page_count = %(expected_page_count)s "
        "AND plan.item_count = %(expected_item_count)s "
        "AND stamp.at < plan.expires_at RETURNING "
        + ", ".join(sorted(_APPROVE_PLAN_RETURNING_KEYS))
    )


def test_preview_ack_loss_guard_rejects_wrong_sql_and_non_page_update() -> None:
    params = _preview_page_guard_params()
    assert _preview_page_update_params_are_exact(params)
    assert not _is_preview_page_plan_update("SELECT 1", params)
    assert not _is_preview_page_plan_update(
        "UPDATE public.sync_cold_start_plans SET state = 'ready'",
        params,
    )
    assert not _preview_page_update_params_are_exact({**params, "forged": None})
    assert not _preview_page_update_params_are_exact(
        {**params, "target_state": "completed"}
    )
    missing_cas = dict(params)
    missing_cas.pop("expected_page_count")
    assert not _preview_page_update_params_are_exact(missing_cas)


@pytest.mark.asyncio
@pytest.mark.parametrize("flow", ["preview", "apply"])
async def test_g9_http_cancellation_target_dml_rejected_before_delegate(
    flow: str,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("G9 target DML reached delegate")

    state = _G9CancellationState(flow)
    state.phase = "http_in_flight"
    state.retained_pid = 12345
    delegate = NeverDelegate()
    connection = _G9CancellationConnection(
        delegate,
        state,
        role="retained",
    )
    if flow == "preview":
        statement = _preview_page_guard_sql()
        params: object = _preview_page_guard_params()
        expected_marker = "sync_cold_start_plans.preview_page_update"
    else:
        _expected, sequence = _apply_ack_loss_guard_fixture()
        statement = _preview_acceptance_receipt_guard_sql()
        params = sequence[-1][1]
        expected_marker = "cold_start_command_receipts.insert"

    with pytest.raises(
        AssertionError,
        match="G9 target DML attempted during HTTP cancellation",
    ):
        await connection.execute(statement, params)

    assert delegate.calls == []
    assert state.target_dml_attempts == [expected_marker]


@pytest.mark.asyncio
@pytest.mark.parametrize("flow", ["preview", "apply"])
async def test_g9_blocking_http_client_preserves_cancelled_error_identity(
    flow: str,
) -> None:
    state = _G9CancellationState(flow)
    if flow == "preview":
        client: Any = _G9BlockingColdStartOrigin(state)
        coroutine = client.fetch_cold_start_page(8, "Inbox", None, 100)
    else:
        client = _G9BlockingOrdinaryPageClient(state)
        coroutine = client.sync_emails(8, "Inbox", "opaque+G9/%3D", 100)
    task = asyncio.create_task(coroutine)
    await asyncio.wait_for(client.entered.wait(), timeout=1.0)

    assert task.cancel() is True
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert caught.value is state.cancelled_error
    assert task.cancelled() is True
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_g9_cleanup_order_guards_reject_putconn_or_permit_early() -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    class RawConnection:
        info = Info()
        closed = False

    class NeverPool:
        kwargs = {"autocommit": True}
        close_returns = False

        def __init__(self) -> None:
            self.putconn_calls: list[object] = []

        async def putconn(self, connection: object) -> None:
            self.putconn_calls.append(connection)
            raise AssertionError("out-of-order G9 putconn reached delegate")

    state = _G9CancellationState("preview")
    state.phase = "cancelled"
    state.retained_pid = 12345
    raw_pool = NeverPool()
    guarded_pool = _G9CancellationPool(raw_pool, state)  # type: ignore[arg-type]
    connection = _G9CancellationConnection(
        RawConnection(),
        state,
        role="retained",
    )

    with pytest.raises(AssertionError, match="unexpected G9 retained putconn state"):
        await guarded_pool.putconn(connection)
    assert raw_pool.putconn_calls == []

    permit_state = _G9CancellationState("preview")
    permit_state.retained_pid = 12345
    permit_state.cleanup_events = [("unlock", 12345)]
    permit = _G9TrackingPermitProvider(permit_state)
    lease = await permit.try_acquire(8, "INBOX")
    assert lease is not None
    with pytest.raises(AssertionError):
        lease.release()
    assert permit.acquire_count == 1
    assert permit.release_count == 0
    assert permit.active is True

    premature_state = _G9CancellationState("apply")
    premature_permit = _G9TrackingPermitProvider(premature_state)
    with pytest.raises(
        AssertionError,
        match="G9 apply permit acquired before locator return",
    ):
        await premature_permit.try_acquire(8, "INBOX")
    assert premature_permit.acquire_count == 0
    assert premature_permit.release_count == 0
    assert premature_permit.active is False


@pytest.mark.asyncio
async def test_g10_fetch_barrier_preserves_cancelled_error_identity() -> None:
    state = _G10LocatorCancellationState("fetch_cancel", uuid4())
    cursor = _G10LocatorCursor(object(), state)
    task = asyncio.create_task(cursor.fetchone())
    await asyncio.wait_for(state.fetch_entered.wait(), timeout=1.0)

    assert task.cancel() is True
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert caught.value is state.cancelled_error
    assert task.cancelled() is True


@pytest.mark.asyncio
async def test_g10_locator_sql_guard_rejects_lock_or_dml_before_delegate() -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    class NeverDelegate:
        info = Info()
        closed = False
        autocommit = True

        def __init__(self) -> None:
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("forbidden G10 SQL reached delegate")

    state = _G10LocatorCancellationState("fetch_cancel", uuid4())
    delegate = NeverDelegate()
    connection = _G10LocatorConnection(delegate, state)

    for statement in (
        "SELECT pg_catalog.pg_try_advisory_lock(%s, %s)",
        "UPDATE public.sync_cold_start_plans SET state = 'blocked'",
    ):
        with pytest.raises(AssertionError, match="attempted forbidden SQL"):
            await connection.execute(statement, (1, 2))

    assert delegate.calls == []
    assert len(state.forbidden_statements) == 2
    assert state.statements == []


@pytest.mark.asyncio
async def test_g10_cleanup_guard_never_repooled_an_open_backend() -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    class RawConnection:
        info = Info()
        closed = False
        autocommit = True

    class NeverPool:
        kwargs = {"autocommit": True}
        close_returns = False

        def __init__(self) -> None:
            self.calls: list[object] = []

        async def putconn(self, connection: object) -> None:
            self.calls.append(connection)
            raise AssertionError("open G10 backend reached pool delegate")

    state = _G10LocatorCancellationState("fetch_cancel", uuid4())
    raw_pool = NeverPool()
    guarded_pool = _G10LocatorPool(raw_pool, state)  # type: ignore[arg-type]
    connection = _G10LocatorConnection(RawConnection(), state)
    guarded_pool.connection = connection

    with pytest.raises(AssertionError, match="requires a closed backend"):
        await guarded_pool.putconn(connection)

    assert raw_pool.calls == []
    assert state.putconn_intents == [(12345, False, TransactionStatus.IDLE)]


def test_preview_acceptance_guard_accepts_exact_production_receipt_contract() -> None:
    plan_id = uuid4()
    plan_params = _preview_acceptance_plan_guard_params(plan_id)
    receipt_params = _preview_acceptance_receipt_guard_params(plan_id)
    receipt_sql = _preview_acceptance_receipt_guard_sql()
    tokens = _sql_tokens(receipt_sql)

    assert _preview_acceptance_plan_params_are_exact(plan_params)
    assert _preview_acceptance_receipt_params_are_exact(receipt_params)
    assert tokens is not None
    assert (
        hashlib.sha256("\x00".join(tokens).encode("utf-8")).hexdigest()
        == (_PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_ORDER[1])
    )
    assert _preview_acceptance_dml_marker(receipt_sql, receipt_params) == (
        "cold_start_command_receipts.insert"
    )


def test_preview_acceptance_guard_rejects_forged_parameter_contracts() -> None:
    plan_id = uuid4()
    valid_plan = _preview_acceptance_plan_guard_params(plan_id)
    valid_receipt = _preview_acceptance_receipt_guard_params(plan_id)
    receipt_sql = _preview_acceptance_receipt_guard_sql()

    plan_variants: list[tuple[object, ...]] = []
    for index, forged in (
        (0, str(plan_id)),
        (1, True),
        (3, "active"),
        (4, "forged-cursor"),
        (7, 0),
        (9, "A" * 64),
        (11, " leading-space"),
        (13, 0),
    ):
        variant = list(valid_plan)
        variant[index] = forged
        plan_variants.append(tuple(variant))
    plan_variants.extend((valid_plan[:-1], (*valid_plan, "extra")))
    assert all(
        not _preview_acceptance_plan_params_are_exact(variant)
        for variant in plan_variants
    )

    receipt_variants: list[object] = [list(valid_receipt), valid_receipt[:-1]]
    for index, forged in (
        (0, str(valid_receipt[0])),
        (1, True),
        (2, "cold_start.apply_page"),
        (3, "A" * 64),
        (5, "failed"),
        (6, "forged_result"),
        (7, "not-a-uuid"),
        (9, 0),
    ):
        variant = list(valid_receipt)
        variant[index] = forged
        receipt_variants.append(tuple(variant))
    assert all(
        _preview_acceptance_dml_marker(receipt_sql, variant) is None
        for variant in receipt_variants
    )


@pytest.mark.parametrize(
    "forged_sql",
    [
        'INSERT INTO "public"."audit_events" DEFAULT VALUES',
        _preview_acceptance_receipt_guard_sql() + " /* forged */",
        "-- forged\n" + _preview_acceptance_receipt_guard_sql(),
        _preview_acceptance_receipt_guard_sql()
        + "; DELETE FROM public.sync_cold_start_plans",
        _preview_acceptance_receipt_guard_sql().replace(
            '"cold_start_command_receipts"',
            '"cold_start_command_receipt"',
        ),
    ],
)
def test_preview_acceptance_guard_rejects_forged_sql_comment_or_batch(
    forged_sql: str,
) -> None:
    params = _preview_acceptance_receipt_guard_params(uuid4())

    assert _preview_acceptance_dml_marker(forged_sql, params) is None


@pytest.mark.parametrize(
    ("fault_target", "message"),
    [
        ("preview_page", "preview page transaction acknowledgement lost"),
        (
            "acceptance",
            "preview acceptance transaction acknowledgement lost",
        ),
    ],
)
def test_preview_ack_loss_fault_budget_is_pool_shared_and_one_shot(
    fault_target: str,
    message: str,
) -> None:
    state = _PreviewAckLossFaultState(
        "commit_then_raise",
        fault_target=fault_target,
    )

    assert state.claim_fault() is True
    assert state.claim_fault() is False
    assert state.fault_budget == 0
    assert type(state.error) is RuntimeError
    assert str(state.error) == message


@pytest.mark.asyncio
async def test_apply_ack_loss_guard_rejects_unknown_dml_before_delegate() -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("unknown apply DML reached delegate")

    delegate = NeverDelegate()
    state = _ApplyAckLossFaultState("commit_then_raise")
    state.transition_to_post_http()
    connection = _ApplyAckLossConnection(delegate, state, phase="origin")

    with pytest.raises(AssertionError, match="unexpected apply ACK-loss SQL"):
        await connection.execute("UPDATE public.sync_cursors SET version = version + 1")

    assert delegate.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "mode", "marker_index"),
    [
        ("origin", "commit_then_raise", 0),
        ("recovery", "rollback_then_raise", 2),
    ],
)
async def test_apply_ack_loss_connection_wires_exact_parameter_guard_before_delegate(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    mode: str,
    marker_index: int,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("forged exact parameters reached delegate")

    expected, sequence = _apply_ack_loss_guard_fixture()
    marker, valid_params = sequence[marker_index]
    if marker_index == 0:
        assert type(valid_params) is tuple
        forged_values = list(valid_params)
        forged_values[7] = "f" * 64
        forged_params: object = tuple(forged_values)
    else:
        assert type(valid_params) is dict
        forged_params = {
            **valid_params,
            "database_stamp": datetime(2026, 7, 16, 12, 0, 1, tzinfo=UTC),
        }
    prior = list(sequence[:marker_index])
    assert _dml_params_are_allowed(marker, forged_params)
    assert not _apply_ack_loss_params_are_exact(
        marker,
        forged_params,
        expected=expected,
        prior=prior,
    )

    delegate = NeverDelegate()
    state = _ApplyAckLossFaultState(mode)
    state.bind_expected_contract(expected)
    state.transition_to_post_http()
    state.executed_params[phase].extend(prior)
    state.observations.extend(
        _ApplyAckLossDmlObservation(
            phase=phase,
            marker=prior_marker,
            rowcount=1,
            assigned_transaction_id="91",
            transaction_id="91",
            backend_pid=12345,
            transaction_status=TransactionStatus.INTRANS,
            http_phase="post_http",
            armed=False,
            stable_parameter_projection=None,
        )
        for prior_marker, _ in prior
    )
    observations_before = list(state.observations)
    params_before = list(state.executed_params[phase])
    connection = _ApplyAckLossConnection(delegate, state, phase=phase)
    connection._active_transaction = object()  # type: ignore[assignment]
    monkeypatch.setitem(
        globals(),
        "_apply_dml_marker",
        lambda _statement, _params: marker,
    )

    with pytest.raises(
        AssertionError,
        match="unexpected apply ACK-loss DML parameters",
    ):
        await connection.execute("exact-marker-with-forged-parameters", forged_params)

    assert state.observations == observations_before
    assert state.executed_params[phase] == params_before
    assert delegate.calls == []


@pytest.mark.asyncio
async def test_apply_ack_loss_locator_binds_plan_id_and_is_one_shot_before_delegate() -> (
    None
):
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    class Delegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []
            self.result = object()

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            return self.result

    expected_plan_id = UUID("2238c88c-d065-44ca-8fc7-6df1818b547e")
    wrong_delegate = Delegate()
    wrong_state = _ApplyAckLossFaultState("commit_then_raise")
    wrong_state.bind_locator_plan_id(expected_plan_id)
    wrong_connection = _ApplyAckLossConnection(
        wrong_delegate,
        wrong_state,
        phase="locator",
    )

    with pytest.raises(AssertionError, match="unexpected apply ACK-loss locator SQL"):
        await wrong_connection.execute(
            _APPLY_ACK_LOSS_LOCATOR_SQL,
            (UUID("ad70c206-e932-443d-b558-461f39010c3e"),),
        )
    assert wrong_delegate.calls == []
    assert wrong_state.locator_calls == 0

    delegate = Delegate()
    state = _ApplyAckLossFaultState("commit_then_raise")
    state.bind_locator_plan_id(expected_plan_id)
    connection = _ApplyAckLossConnection(delegate, state, phase="locator")

    result = await connection.execute(
        _APPLY_ACK_LOSS_LOCATOR_SQL,
        (expected_plan_id,),
    )

    assert result is delegate.result
    assert delegate.calls == [(_APPLY_ACK_LOSS_LOCATOR_SQL, (expected_plan_id,))]
    assert state.locator_calls == 1
    with pytest.raises(AssertionError, match="unexpected apply ACK-loss locator SQL"):
        await connection.execute(
            _APPLY_ACK_LOSS_LOCATOR_SQL,
            (expected_plan_id,),
        )
    assert delegate.calls == [(_APPLY_ACK_LOSS_LOCATOR_SQL, (expected_plan_id,))]
    assert state.locator_calls == 1


@pytest.mark.asyncio
async def test_apply_ack_loss_guard_rejects_target_dml_before_http_before_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("pre-HTTP apply DML reached delegate")

    expected, sequence = _apply_ack_loss_guard_fixture()
    delegate = NeverDelegate()
    state = _ApplyAckLossFaultState("commit_then_raise")
    state.bind_expected_contract(expected)
    connection = _ApplyAckLossConnection(delegate, state, phase="origin")
    monkeypatch.setitem(
        globals(),
        "_apply_dml_marker",
        lambda _statement, _params: "event_inbox.insert",
    )

    with pytest.raises(AssertionError, match="unexpected apply ACK-loss SQL"):
        await connection.execute("target-event-inbox-insert", sequence[0][1])

    assert state.http_phase == "pre_http"
    assert state.observations == []
    assert delegate.calls == []


@pytest.mark.asyncio
async def test_apply_ack_loss_guard_rejects_target_dml_outside_transaction_before_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("out-of-transaction apply DML reached delegate")

    expected, sequence = _apply_ack_loss_guard_fixture()
    delegate = NeverDelegate()
    state = _ApplyAckLossFaultState("commit_then_raise")
    state.bind_expected_contract(expected)
    state.transition_to_post_http()
    connection = _ApplyAckLossConnection(delegate, state, phase="origin")
    monkeypatch.setitem(
        globals(),
        "_apply_dml_marker",
        lambda _statement, _params: "event_inbox.insert",
    )

    with pytest.raises(
        AssertionError,
        match="apply ACK-loss DML attempted outside active transaction",
    ):
        await connection.execute("target-event-inbox-insert", sequence[0][1])

    assert state.observations == []
    assert state.executed_params["origin"] == []
    assert delegate.calls == []


@pytest.mark.asyncio
async def test_apply_ack_loss_guard_rejects_second_dml_after_terminal_receipt_before_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("post-receipt apply DML reached delegate")

    expected, sequence = _apply_ack_loss_guard_fixture()
    delegate = NeverDelegate()
    state = _ApplyAckLossFaultState("commit_then_raise")
    state.bind_expected_contract(expected)
    state.transition_to_post_http()
    assert state.claim_fault() is True
    state.observations.extend(
        _ApplyAckLossDmlObservation(
            phase="origin",
            marker=marker,
            rowcount=1,
            assigned_transaction_id="91",
            transaction_id="91",
            backend_pid=12345,
            transaction_status=TransactionStatus.INTRANS,
            http_phase="post_http",
            armed=marker == "cold_start_command_receipts.insert",
            stable_parameter_projection=None,
        )
        for marker in _APPLY_DML_ORDER
    )
    connection = _ApplyAckLossConnection(delegate, state, phase="origin")
    monkeypatch.setitem(
        globals(),
        "_apply_dml_marker",
        lambda _statement, _params: "cold_start_command_receipts.insert",
    )

    with pytest.raises(AssertionError, match="unexpected apply ACK-loss SQL"):
        await connection.execute("second-terminal-receipt", sequence[3][1])

    assert state.fault_budget == 0
    assert len(state.observations) == 4
    assert delegate.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "marker_index", "message"),
    [
        (
            "commit_then_raise",
            0,
            "unexpected apply ACK-loss recovery mutation",
        ),
        (
            "rollback_then_raise",
            1,
            "unexpected apply ACK-loss recovery mutation",
        ),
    ],
)
async def test_apply_ack_loss_recovery_guard_rejects_mutation_or_wrong_order_before_delegate(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    marker_index: int,
    message: str,
) -> None:
    class Info:
        backend_pid = 22345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("forbidden recovery DML reached delegate")

    expected, sequence = _apply_ack_loss_guard_fixture()
    marker, params = sequence[marker_index]
    delegate = NeverDelegate()
    state = _ApplyAckLossFaultState(mode)
    state.bind_expected_contract(expected)
    state.transition_to_post_http()
    connection = _ApplyAckLossConnection(delegate, state, phase="recovery")
    monkeypatch.setitem(
        globals(),
        "_apply_dml_marker",
        lambda _statement, _params: marker,
    )

    with pytest.raises(AssertionError, match=message):
        await connection.execute("forbidden-recovery-mutation", params)

    assert state.observations == []
    assert state.executed_params["recovery"] == []
    assert delegate.calls == []


@pytest.mark.asyncio
async def test_apply_ack_loss_pool_allows_three_phases_and_rejects_fourth_before_getconn() -> (
    None
):
    class Info:
        def __init__(self, backend_pid: int) -> None:
            self.backend_pid = backend_pid
            self.transaction_status = TransactionStatus.IDLE

    class RawConnection:
        def __init__(self, backend_pid: int) -> None:
            self.info = Info(backend_pid)
            self.closed = False

    class ProbePool:
        def __init__(self) -> None:
            self.kwargs: dict[str, object] = {}
            self.close_returns = False
            self.getconn_calls = 0
            self.connections = [
                RawConnection(31001),
                RawConnection(31001),
                RawConnection(31002),
            ]

        async def getconn(self) -> RawConnection:
            self.getconn_calls += 1
            if not self.connections:
                raise AssertionError("fourth checkout reached underlying pool")
            return self.connections.pop(0)

    raw_pool = ProbePool()
    guarded_pool = _ApplyAckLossPool(  # type: ignore[arg-type]
        raw_pool,
        _ApplyAckLossFaultState("commit_then_raise"),
    )

    connections = [await guarded_pool.getconn() for _ in range(3)]

    assert [connection._phase for connection in connections] == [
        "locator",
        "origin",
        "recovery",
    ]
    assert guarded_pool.checked_out_pids == [31001, 31001, 31002]
    assert raw_pool.getconn_calls == 3
    raw_pool.getconn_calls = 0
    with pytest.raises(
        AssertionError, match="unexpected fourth apply ACK-loss checkout"
    ):
        await guarded_pool.getconn()
    assert raw_pool.getconn_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    ["recovery", "origin_post_http_before_target", "origin_after_target"],
)
async def test_preview_ack_loss_guard_rejects_unknown_dml_before_delegate(
    phase: str,
) -> None:
    class Info:
        backend_pid = 12345

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("unknown DML reached delegate")

    delegate = NeverDelegate()
    state = _PreviewAckLossFaultState("commit_then_raise")
    state.transition_to_post_http()
    connection = _PreviewAckLossConnection(
        delegate,
        state,
        phase="recovery" if phase == "recovery" else "origin",
    )
    if phase == "origin_after_target":
        connection._initial_target_armed = True
    statement = "UPDATE public.sync_cursors SET version = version + 1"

    with pytest.raises(AssertionError, match="unexpected .*SQL"):
        await connection.execute(statement)

    assert delegate.calls == []


@pytest.mark.asyncio
async def test_preview_ack_loss_guard_rejects_unknown_pre_http_dml_before_delegate() -> (
    None
):
    class Info:
        backend_pid = 12345

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("unknown pre-HTTP DML reached delegate")

    delegate = NeverDelegate()
    state = _PreviewAckLossFaultState("commit_then_raise")
    connection = _PreviewAckLossConnection(delegate, state, phase="origin")
    statement = "INSERT INTO public.cold_start_command_receipts DEFAULT VALUES"

    with pytest.raises(AssertionError, match="unexpected origin pre-HTTP SQL"):
        await connection.execute(statement)

    assert state.http_phase == "pre_http"
    assert delegate.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forgery",
    ["reversed_order", "account", "plan_id", "fencing_token", "third_dml"],
)
async def test_preview_acceptance_state_machine_rejects_forgery_before_delegate(
    forgery: str,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("forged preview acceptance reached delegate")

    plan_id = uuid4()
    params = list(_preview_acceptance_receipt_guard_params(plan_id))
    state = _PreviewAckLossFaultState("commit_then_raise")
    if forgery != "reversed_order":
        state.acceptance_observations.append(
            _PreviewAcceptanceDmlObservation(
                marker="sync_cold_start_plans.insert",
                statement_digest=_PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_ORDER[0],
                rowcount=1,
                transaction_id="101",
                backend_pid=12345,
                transaction_status=TransactionStatus.INTRANS,
                http_phase="pre_http",
            )
        )
    if forgery == "account":
        params[1] = 9
    elif forgery == "plan_id":
        params[7] = str(uuid4())
    elif forgery == "fencing_token":
        params[9] = 18
    elif forgery == "third_dml":
        state.acceptance_observations.append(
            _PreviewAcceptanceDmlObservation(
                marker="cold_start_command_receipts.insert",
                statement_digest=_PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_ORDER[1],
                rowcount=1,
                transaction_id="101",
                backend_pid=12345,
                transaction_status=TransactionStatus.INTRANS,
                http_phase="pre_http",
            )
        )

    delegate = NeverDelegate()
    connection = _PreviewAckLossConnection(delegate, state, phase="origin")
    connection._active_transaction = object()  # type: ignore[assignment]
    connection._acceptance_plan_identity = (plan_id, 8, 17)

    with pytest.raises(AssertionError, match="unexpected origin pre-HTTP SQL"):
        await connection.execute(
            _preview_acceptance_receipt_guard_sql(),
            tuple(params),
        )

    assert delegate.calls == []


@pytest.mark.asyncio
async def test_preview_ack_loss_guard_rejects_second_exact_target_before_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.INTRANS

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("second exact target reached delegate")

    monkeypatch.setitem(
        globals(),
        "_is_preview_page_plan_update",
        lambda _statement, _params: True,
    )
    delegate = NeverDelegate()
    state = _PreviewAckLossFaultState("commit_then_raise")
    state.transition_to_post_http()
    connection = _PreviewAckLossConnection(delegate, state, phase="origin")
    connection._initial_target_armed = True
    connection._active_transaction = object()  # type: ignore[assignment]

    with pytest.raises(AssertionError, match="after armed preview update"):
        await connection.execute("second exact preview target", {})

    assert delegate.calls == []


@pytest.mark.asyncio
async def test_preview_ack_loss_guard_rejects_exact_target_outside_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Info:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    class NeverDelegate:
        def __init__(self) -> None:
            self.info = Info()
            self.calls: list[tuple[object, object]] = []

        async def execute(self, statement: object, params: object = None) -> object:
            self.calls.append((statement, params))
            raise AssertionError("out-of-XID target reached delegate")

    monkeypatch.setitem(
        globals(),
        "_is_preview_page_plan_update",
        lambda _statement, _params: True,
    )
    delegate = NeverDelegate()
    connection = _PreviewAckLossConnection(
        delegate,
        _PreviewAckLossFaultState("rollback_then_raise"),
        phase="recovery",
    )

    with pytest.raises(AssertionError, match="outside active transaction"):
        await connection.execute("exact preview target", {})

    assert delegate.calls == []


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("initial_state", ["cold_start_pending", "reset_required"])
async def test_cold_start_preview_approve_and_two_page_apply_are_atomic(
    cold_start_runtime: _ColdStartRuntime,
    initial_state: str,
) -> None:
    expected_cursor, expected_version = _seed_cold_start_cursor(
        cold_start_runtime,
        initial_state,
    )
    preview_first = _batch(
        "opaque+Preview-1/%3D",
        includes_last=False,
        changes=(_create_change("historical-create"),),
    )
    preview_terminal = _batch(
        "opaque+Boundary/%3D",
        includes_last=True,
        changes=(
            SyncChange(
                kind=ChangeKind.DELETE,
                external_email_id="historical-delete",
                source_version="historical-v2",
                item=None,
            ),
        ),
    )
    apply_first = _batch(
        "opaque+Apply-1/%3D",
        includes_last=False,
        changes=(_create_change("ordinary-create"),),
    )
    apply_terminal = _batch(
        "opaque+Apply-2/%3D",
        includes_last=True,
        changes=(
            SyncChange(
                kind=ChangeKind.UPDATE,
                external_email_id="ordinary-create",
                source_version="ordinary-v2",
                item={"id": "ordinary-create", "is_read": True},
            ),
        ),
    )
    origin = _ColdStartOrigin([preview_first, preview_terminal])
    ordinary = _OrdinaryPageClient([apply_first, apply_terminal])
    service = _service(cold_start_runtime, origin=origin, ordinary=ordinary)

    ready = await service.preview(
        8,
        "INBOX",
        actor="integration-preview",
        reason="review historical messages",
        idempotency_key=f"preview-{initial_state}",
    )

    assert ready.status is ColdStartRunStatus.READY
    assert ready.pages_committed == 2
    assert ready.changes_observed == 2
    assert ready.plan is not None
    assert ready.plan.state is ColdStartPlanState.READY
    assert ready.plan.boundary_cursor == preview_terminal.cursor
    assert ready.plan.page_count == 2
    assert ready.plan.item_count == 2
    assert origin.calls == [
        (8, "Inbox", None, 100),
        (8, "Inbox", preview_first.cursor, 100),
    ]
    assert (
        _scalar(
            cold_start_runtime,
            "SELECT pg_catalog.count(*) FROM event_inbox",
        )
        == 0
    )
    plan_before_approval = _rows(
        cold_start_runtime,
        "SELECT expected_cursor_status, expected_cursor, "
        "expected_cursor_version, state, version, preview_cursor, "
        "preview_cursor_version, boundary_cursor, boundary_cursor_version, "
        "apply_cursor, apply_cursor_version, page_count, item_count "
        "FROM sync_cold_start_plans WHERE plan_id = %s",
        (ready.plan.plan_id,),
    )
    assert plan_before_approval == [
        {
            "expected_cursor_status": initial_state,
            "expected_cursor": expected_cursor,
            "expected_cursor_version": expected_version,
            "state": "ready",
            "version": 2,
            "preview_cursor": preview_terminal.cursor,
            "preview_cursor_version": 2,
            "boundary_cursor": preview_terminal.cursor,
            "boundary_cursor_version": 2,
            "apply_cursor": None,
            "apply_cursor_version": None,
            "page_count": 2,
            "item_count": 2,
        }
    ]

    approved = await service.approve(
        ready.plan.plan_id,
        actor="integration-approver",
        reason="historical suppression approved",
        idempotency_key=f"approve-{initial_state}",
    )
    assert approved.status is ColdStartRunStatus.APPROVED
    assert approved.pages_committed == approved.changes_observed == 0

    completed = await service.apply(ready.plan.plan_id)

    assert completed.status is ColdStartRunStatus.COMPLETED
    assert completed.pages_committed == 2
    assert completed.changes_observed == 2
    assert completed.plan is not None
    assert completed.plan.state is ColdStartPlanState.COMPLETED
    assert ordinary.calls == [
        (8, "Inbox", preview_terminal.cursor, 100),
        (8, "Inbox", apply_first.cursor, 100),
    ]
    assert _rows(
        cold_start_runtime,
        "SELECT external_email_id, change_kind, source_version, "
        "processing_policy, pipeline_name, generation, fencing_token "
        "FROM event_inbox ORDER BY received_at, external_email_id",
    ) == [
        {
            "external_email_id": "ordinary-create",
            "change_kind": "create",
            "source_version": "version-1",
            "processing_policy": "full",
            "pipeline_name": "durable_v1",
            "generation": 1,
            "fencing_token": 1,
        },
        {
            "external_email_id": "ordinary-create",
            "change_kind": "update",
            "source_version": "ordinary-v2",
            "processing_policy": "metadata_only",
            "pipeline_name": "durable_v1",
            "generation": 1,
            "fencing_token": 1,
        },
    ]
    assert _rows(
        cold_start_runtime,
        "SELECT cursor, status, version, blocked_reason_code, "
        "cold_start_plan_id, cold_start_plan_state, transient_failures, "
        "retry_after_at FROM sync_cursors "
        "WHERE account_id = 8 AND folder_key = 'INBOX'",
    ) == [
        {
            "cursor": apply_terminal.cursor,
            "status": "active",
            "version": 2,
            "blocked_reason_code": None,
            "cold_start_plan_id": None,
            "cold_start_plan_state": None,
            "transient_failures": 0,
            "retry_after_at": None,
        }
    ]
    assert _rows(
        cold_start_runtime,
        "SELECT state, version, apply_cursor, apply_cursor_version, "
        "completed_at IS NOT NULL AS has_completed_at "
        "FROM sync_cold_start_plans WHERE plan_id = %s",
        (ready.plan.plan_id,),
    ) == [
        {
            "state": "completed",
            "version": 5,
            "apply_cursor": apply_terminal.cursor,
            "apply_cursor_version": 2,
            "has_completed_at": True,
        }
    ]
    receipt_rows = _rows(
        cold_start_runtime,
        "SELECT command_name, outcome, result_type, result_id, "
        "authority_epoch FROM cold_start_command_receipts "
        "ORDER BY created_at, command_name",
    )
    assert len(receipt_rows) == 4
    assert [row["command_name"] for row in receipt_rows].count(
        "cold_start.preview"
    ) == 1
    assert [row["command_name"] for row in receipt_rows].count(
        "cold_start.approve"
    ) == 1
    assert [row["command_name"] for row in receipt_rows].count(
        "cold_start.apply_page"
    ) == 2
    assert all(
        row["outcome"] == "succeeded"
        and row["result_type"] == "sync_cold_start_plan"
        and row["result_id"] == str(ready.plan.plan_id)
        and row["authority_epoch"] == 1
        for row in receipt_rows
    )
    assert _rows(
        cold_start_runtime,
        "SELECT action, result, actor, reason FROM audit_events "
        "ORDER BY created_at, id",
    ) == [
        {
            "action": "pipeline.bootstrap",
            "result": "succeeded",
            "actor": "system",
            "reason": "initial bootstrap",
        },
        {
            "action": "cold_start.approve",
            "result": "approved",
            "actor": "integration-approver",
            "reason": "historical suppression approved",
        },
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_transient_post_http_expiry_blocks_both_in_one_xid(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Boundary-Transient-Drift/%3D",
        includes_last=True,
        changes=(_create_change("historical-transient-drift"),),
    )
    first = _batch(
        "opaque+Apply-Transient-Drift/%3D",
        includes_last=False,
        changes=(_create_change("ordinary-before-transient-drift"),),
    )
    origin = _ColdStartOrigin([boundary])
    first_ordinary = _OrdinaryPageClient([first])
    service = _service(
        cold_start_runtime,
        origin=origin,
        ordinary=first_ordinary,
        apply_max_pages=1,
    )
    ready = await service.preview(
        8,
        "INBOX",
        actor="transient-drift-preview",
        reason="seal transient drift boundary",
        idempotency_key="transient-drift-preview-key",
    )
    assert ready.plan is not None
    await service.approve(
        ready.plan.plan_id,
        actor="transient-drift-approver",
        reason="approve transient drift boundary",
        idempotency_key="transient-drift-approve-key",
    )
    applying = await service.apply(ready.plan.plan_id)
    assert applying.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert applying.plan is not None
    assert applying.plan.state is ColdStartPlanState.APPROVED

    def expire_after_http_preflight() -> None:
        updated = _admin_rows(
            cold_start_runtime,
            "UPDATE sync_cold_start_plans SET expires_at = "
            "pg_catalog.clock_timestamp() WHERE plan_id = %s "
            "RETURNING plan_id",
            (ready.plan.plan_id,),
        )
        assert updated == [{"plan_id": ready.plan.plan_id}]

    transient_ordinary = _TransientFailingOrdinaryClient(expire_after_http_preflight)
    retry_service = _service(
        cold_start_runtime,
        origin=_ColdStartOrigin([]),
        ordinary=transient_ordinary,  # type: ignore[arg-type]
    )

    result = await retry_service.apply(ready.plan.plan_id)

    assert result.status is ColdStartRunStatus.BLOCKED
    assert result.safe_code == "cold_start.expired"
    assert result.pages_committed == result.changes_observed == 0
    assert result.plan is not None
    assert result.plan.state is ColdStartPlanState.BLOCKED
    assert transient_ordinary.calls == [(8, "Inbox", first.cursor, 100)]
    assert _rows(
        cold_start_runtime,
        "SELECT state, blocked_reason_code, apply_cursor, "
        "apply_cursor_version FROM sync_cold_start_plans WHERE plan_id = %s",
        (ready.plan.plan_id,),
    ) == [
        {
            "state": "blocked",
            "blocked_reason_code": "cold_start.expired",
            "apply_cursor": first.cursor,
            "apply_cursor_version": 1,
        }
    ]
    assert _rows(
        cold_start_runtime,
        "SELECT status, cursor, version, blocked_reason_code, "
        "transient_failures, retry_after_at, cold_start_plan_id, "
        "cold_start_plan_state FROM sync_cursors "
        "WHERE account_id = 8 AND folder_key = 'INBOX'",
    ) == [
        {
            "status": "blocked_contract",
            "cursor": first.cursor,
            "version": 2,
            "blocked_reason_code": "cold_start.expired",
            "transient_failures": 0,
            "retry_after_at": None,
            "cold_start_plan_id": None,
            "cold_start_plan_state": None,
        }
    ]
    assert _rows(
        cold_start_runtime,
        "SELECT action, reason FROM audit_events WHERE action = 'cold_start.block'",
    ) == [{"action": "cold_start.block", "reason": "cold_start.expired"}]
    block_xids = _rows(
        cold_start_runtime,
        "SELECT 'plan' AS object_type, xmin::pg_catalog.text AS xid "
        "FROM sync_cold_start_plans WHERE plan_id = %s UNION ALL "
        "SELECT 'cursor', xmin::pg_catalog.text FROM sync_cursors "
        "WHERE account_id = 8 AND folder_key = 'INBOX' UNION ALL "
        "SELECT 'audit', xmin::pg_catalog.text FROM audit_events "
        "WHERE action = 'cold_start.block' ORDER BY object_type",
        (ready.plan.plan_id,),
    )
    assert [row["object_type"] for row in block_xids] == ["audit", "cursor", "plan"]
    assert len({row["xid"] for row in block_xids}) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_resumes_across_fresh_pools_and_completed_replay_is_read_only(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Boundary-G2/%3D",
        includes_last=True,
        changes=(_create_change("historical-g2"),),
    )
    first = _batch(
        "opaque+Apply-G2-1/%3D",
        includes_last=False,
        changes=(_create_change("ordinary-g2-first"),),
    )
    terminal = _batch(
        "opaque+Apply-G2-2/%3D",
        includes_last=True,
        changes=(_create_change("ordinary-g2-terminal"),),
    )
    origin_a = _ColdStartOrigin([boundary])
    ordinary_a = _OrdinaryPageClient([first])
    service_a = _service(
        cold_start_runtime,
        origin=origin_a,
        ordinary=ordinary_a,
        apply_max_pages=1,
    )
    ready = await service_a.preview(
        8,
        "INBOX",
        actor="g2-preview",
        reason="seal cross process boundary",
        idempotency_key="g2-preview-key",
    )
    assert ready.plan is not None
    await service_a.approve(
        ready.plan.plan_id,
        actor="g2-approver",
        reason="approve cross process resume",
        idempotency_key="g2-approve-key",
    )

    first_result = await service_a.apply(ready.plan.plan_id)

    assert first_result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
    assert first_result.pages_committed == first_result.changes_observed == 1
    assert ordinary_a.calls == [(8, "Inbox", boundary.cursor, 100)]
    assert _rows(
        cold_start_runtime,
        "SELECT cursor, status, version, cold_start_plan_id, "
        "cold_start_plan_state FROM sync_cursors "
        "WHERE account_id = 8 AND folder_key = 'INBOX'",
    ) == [
        {
            "cursor": first.cursor,
            "status": "cold_start_applying",
            "version": 1,
            "cold_start_plan_id": ready.plan.plan_id,
            "cold_start_plan_state": "approved",
        }
    ]
    old_application_name = cold_start_runtime.application_name
    await cold_start_runtime.pool.close()
    assert _application_session_count(cold_start_runtime, old_application_name) == 0

    origin_b = _ColdStartOrigin([])
    ordinary_b = _OrdinaryPageClient([terminal])
    pool_b_application_name = _process_application_name(cold_start_runtime, "g2-b")
    pool_b = await _open_maintenance_pool(cold_start_runtime, "g2-b")
    try:
        service_b = _service(
            cold_start_runtime,
            origin=origin_b,
            ordinary=ordinary_b,
            pool=pool_b,
        )
        completed = await service_b.apply(ready.plan.plan_id)
    finally:
        await pool_b.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_b_application_name,
            )
            == 0
        )

    assert service_b is not service_a
    assert completed.status is ColdStartRunStatus.COMPLETED
    assert completed.pages_committed == completed.changes_observed == 1
    assert origin_b.calls == []
    assert ordinary_b.calls == [(8, "Inbox", first.cursor, 100)]
    assert (
        _scalar(
            cold_start_runtime,
            "SELECT pg_catalog.count(*) FROM event_inbox",
        )
        == 2
    )
    assert (
        _scalar(
            cold_start_runtime,
            "SELECT pg_catalog.count(*) FROM cold_start_command_receipts",
        )
        == 4
    )

    durable_before_replay = _durable_physical_snapshot(cold_start_runtime)
    origin_c = _ColdStartOrigin([])
    ordinary_c = _OrdinaryPageClient([])
    pool_c_application_name = _process_application_name(cold_start_runtime, "g2-c")
    pool_c = await _open_maintenance_pool(cold_start_runtime, "g2-c")
    replay_guard = _ReplayGuardPool(pool_c)
    try:
        service_c = _service(
            cold_start_runtime,
            origin=origin_c,
            ordinary=ordinary_c,
            pool=replay_guard,
        )
        replay = await service_c.apply(ready.plan.plan_id)
    finally:
        await pool_c.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_c_application_name,
            )
            == 0
        )

    assert service_c is not service_b and service_c is not service_a
    assert replay.status is ColdStartRunStatus.COMPLETED
    assert replay.pages_committed == replay.changes_observed == 0
    assert origin_c.calls == ordinary_c.calls == []
    assert replay_guard.statements
    assert all(
        not _is_replay_forbidden_sql(statement) for statement in replay_guard.statements
    )
    durable_after_replay = _durable_physical_snapshot(cold_start_runtime)
    assert durable_after_replay == durable_before_replay


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preview_plan_insert_rolls_back_when_receipt_insert_fails(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    origin = _ColdStartOrigin([])
    ordinary = _OrdinaryPageClient([])
    receipts = _PlanVisibleThenFailReceiptRepository()
    durable_before = _durable_physical_snapshot(cold_start_runtime)
    pool_application_name = _process_application_name(cold_start_runtime, "g3")
    pool = await _open_maintenance_pool(cold_start_runtime, "g3")
    try:
        service = _service(
            cold_start_runtime,
            origin=origin,
            ordinary=ordinary,
            pool=pool,
            receipt_repository=receipts,
        )

        with pytest.raises(
            _InjectedReceiptFailure,
            match="receipt insert failure after plan visibility",
        ):
            await service.preview(
                8,
                "INBOX",
                actor="g3-preview",
                reason="prove plan and receipt rollback together",
                idempotency_key="g3-preview-key",
            )

        assert len(receipts.observations) == 1
        observation = receipts.observations[0]
        assert observation.plan_count == 1
        assert observation.lookup_transaction_id == observation.insert_transaction_id
        assert observation.transaction_status is TransactionStatus.INTRANS
        assert origin.calls == ordinary.calls == []
        assert _durable_physical_snapshot(cold_start_runtime) == durable_before
        assert (
            _scalar(
                cold_start_runtime,
                "SELECT pg_catalog.count(*) FROM sync_cold_start_plans",
            )
            == 0
        )
        assert (
            _scalar(
                cold_start_runtime,
                "SELECT pg_catalog.count(*) FROM cold_start_command_receipts",
            )
            == 0
        )

        async with pool.connection() as connection:
            assert connection.info.transaction_status is TransactionStatus.IDLE
            cursor = await connection.execute(
                "SELECT pg_catalog.count(*) AS plan_count FROM sync_cold_start_plans"
            )
            row = await cursor.fetchone()
            assert row is not None and int(row["plan_count"]) == 0
            assert connection.info.transaction_status is TransactionStatus.IDLE

        pool_stats = pool.get_stats()
        assert pool_stats["requests_waiting"] == 0
        assert pool_stats["pool_size"] == pool_stats["pool_available"]
    finally:
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault_target", _BLOCK_DML_ORDER)
async def test_contract_block_rolls_back_after_each_executed_dml_statement(
    cold_start_runtime: _ColdStartRuntime,
    fault_target: str,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Boundary-G5/%3D",
        includes_last=True,
        changes=(_create_change("historical-g5"),),
    )
    setup_origin = _ColdStartOrigin([boundary])
    setup_service = _service(
        cold_start_runtime,
        origin=setup_origin,
        ordinary=_OrdinaryPageClient([]),
    )
    ready = await setup_service.preview(
        8,
        "INBOX",
        actor="g5-preview",
        reason="prepare block rollback matrix",
        idempotency_key=f"g5-preview-{fault_target}",
    )
    assert ready.plan is not None
    approved = await setup_service.approve(
        ready.plan.plan_id,
        actor="g5-approver",
        reason="approve block rollback matrix",
        idempotency_key=f"g5-approve-{fault_target}",
    )
    assert approved.status is ColdStartRunStatus.APPROVED
    assert setup_origin.calls == [(8, "Inbox", None, 100)]
    durable_before = _durable_physical_snapshot(cold_start_runtime)

    apply_origin = _ColdStartOrigin([])
    ordinary = _ContractFailingOrdinaryClient()
    process_name = f"g5-{_BLOCK_DML_ORDER.index(fault_target)}"
    pool_application_name = _process_application_name(
        cold_start_runtime,
        process_name,
    )
    pool = await _open_maintenance_pool(cold_start_runtime, process_name)
    fault_pool = _ApplyStatementFaultPool(pool, fault_target)
    try:
        apply_service = _service(
            cold_start_runtime,
            origin=apply_origin,
            ordinary=ordinary,
            pool=fault_pool,
        )
        with pytest.raises(_InjectedApplyStatementFailure) as captured:
            await apply_service.apply(ready.plan.plan_id)

        assert captured.value is fault_pool.error
        assert apply_origin.calls == []
        assert ordinary.calls == [(8, "Inbox", boundary.cursor, 100)]
        assert ordinary.raised == [ordinary.error]
        expected_dml = _BLOCK_DML_ORDER[: _BLOCK_DML_ORDER.index(fault_target) + 1]
        assert tuple(item.marker for item in fault_pool.observations) == expected_dml
        assert all(
            item.rowcount == 1 and item.transaction_status is TransactionStatus.INTRANS
            for item in fault_pool.observations
        )
        assert len({item.transaction_id for item in fault_pool.observations}) == 1
        assert _durable_physical_snapshot(cold_start_runtime) == durable_before
        assert _rows(
            cold_start_runtime,
            "SELECT state, version, apply_cursor, apply_cursor_version, "
            "blocked_reason_code, blocked_at FROM sync_cold_start_plans "
            "WHERE plan_id = %s",
            (ready.plan.plan_id,),
        ) == [
            {
                "state": "approved",
                "version": 2,
                "apply_cursor": None,
                "apply_cursor_version": None,
                "blocked_reason_code": None,
                "blocked_at": None,
            }
        ]
        assert _rows(
            cold_start_runtime,
            "SELECT cursor, status, version, blocked_reason_code, blocked_at "
            "FROM sync_cursors "
            "WHERE account_id = 8 AND folder_key = 'INBOX'",
        ) == [
            {
                "cursor": None,
                "status": "cold_start_pending",
                "version": 0,
                "blocked_reason_code": "sync.cold_start_required",
                "blocked_at": None,
            }
        ]
        assert (
            _scalar(
                cold_start_runtime,
                "SELECT pg_catalog.count(*) FROM event_inbox",
            )
            == 0
        )
        assert _rows(
            cold_start_runtime,
            "SELECT command_name FROM cold_start_command_receipts "
            "ORDER BY command_name",
        ) == [
            {"command_name": "cold_start.approve"},
            {"command_name": "cold_start.preview"},
        ]
        assert _rows(
            cold_start_runtime,
            "SELECT action, result FROM audit_events ORDER BY created_at, id",
        ) == [
            {"action": "pipeline.bootstrap", "result": "succeeded"},
            {"action": "cold_start.approve", "result": "approved"},
        ]

        async with pool.connection() as connection:
            assert connection.info.transaction_status is TransactionStatus.IDLE
            cursor = await connection.execute(
                "SELECT pg_catalog.count(*) AS inbox_count FROM event_inbox"
            )
            row = await cursor.fetchone()
            assert row is not None and int(row["inbox_count"]) == 0
            assert connection.info.transaction_status is TransactionStatus.IDLE

        pool_stats = pool.get_stats()
        assert pool_stats["requests_waiting"] == 0
        assert pool_stats["pool_size"] == pool_stats["pool_available"]
    finally:
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_terminal_apply_commits_exact_dml_trace_before_return(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Boundary-G4-Success/%3D",
        includes_last=True,
        changes=(_create_change("historical-g4-success"),),
    )
    setup_origin = _ColdStartOrigin([boundary])
    setup_service = _service(
        cold_start_runtime,
        origin=setup_origin,
        ordinary=_OrdinaryPageClient([]),
    )
    ready = await setup_service.preview(
        8,
        "INBOX",
        actor="g4-success-preview",
        reason="prepare successful terminal apply trace",
        idempotency_key="g4-success-preview",
    )
    assert ready.plan is not None
    approved = await setup_service.approve(
        ready.plan.plan_id,
        actor="g4-success-approver",
        reason="approve successful terminal apply trace",
        idempotency_key="g4-success-approve",
    )
    assert approved.status is ColdStartRunStatus.APPROVED

    terminal = _batch(
        "opaque+Terminal-G4-Success/%3D",
        includes_last=True,
        changes=(_create_change("ordinary-g4-success"),),
    )
    apply_origin = _ColdStartOrigin([])
    ordinary = _OrdinaryPageClient([terminal])
    process_name = "g4-success"
    pool_application_name = _process_application_name(
        cold_start_runtime,
        process_name,
    )
    pool = await _open_maintenance_pool(cold_start_runtime, process_name)
    trace_pool = _ApplyStatementFaultPool(pool, None)
    try:
        apply_service = _service(
            cold_start_runtime,
            origin=apply_origin,
            ordinary=ordinary,
            pool=trace_pool,
        )
        completed = await apply_service.apply(ready.plan.plan_id)

        assert completed.status is ColdStartRunStatus.COMPLETED
        assert completed.pages_committed == completed.changes_observed == 1
        assert apply_origin.calls == []
        assert ordinary.calls == [(8, "Inbox", boundary.cursor, 100)]
        assert tuple(item.marker for item in trace_pool.observations) == (
            _APPLY_DML_ORDER
        )
        assert len({item.transaction_id for item in trace_pool.observations}) == 1
        assert all(
            item.rowcount == 1 and item.transaction_status is TransactionStatus.INTRANS
            for item in trace_pool.observations
        )
        assert trace_pool.commit_boundaries == [
            _ApplyCommitBoundary(
                phase="before_commit",
                markers=_APPLY_DML_ORDER,
                transaction_status=TransactionStatus.INTRANS,
            ),
            _ApplyCommitBoundary(
                phase="after_commit",
                markers=_APPLY_DML_ORDER,
                transaction_status=TransactionStatus.IDLE,
            ),
        ]
        assert _rows(
            cold_start_runtime,
            "SELECT state, version, apply_cursor, apply_cursor_version "
            "FROM sync_cold_start_plans WHERE plan_id = %s",
            (ready.plan.plan_id,),
        ) == [
            {
                "state": "completed",
                "version": 3,
                "apply_cursor": terminal.cursor,
                "apply_cursor_version": 1,
            }
        ]
        assert _rows(
            cold_start_runtime,
            "SELECT cursor, status, version FROM sync_cursors "
            "WHERE account_id = 8 AND folder_key = 'INBOX'",
        ) == [{"cursor": terminal.cursor, "status": "active", "version": 1}]
        assert _rows(
            cold_start_runtime,
            "SELECT external_email_id, processing_policy FROM event_inbox",
        ) == [
            {
                "external_email_id": "ordinary-g4-success",
                "processing_policy": "full",
            }
        ]
        assert _rows(
            cold_start_runtime,
            "SELECT command_name, pg_catalog.count(*) AS receipt_count "
            "FROM cold_start_command_receipts GROUP BY command_name "
            "ORDER BY command_name",
        ) == [
            {"command_name": "cold_start.apply_page", "receipt_count": 1},
            {"command_name": "cold_start.approve", "receipt_count": 1},
            {"command_name": "cold_start.preview", "receipt_count": 1},
        ]

        async with pool.connection() as connection:
            assert connection.info.transaction_status is TransactionStatus.IDLE
        pool_stats = pool.get_stats()
        assert pool_stats["requests_waiting"] == 0
        assert pool_stats["pool_size"] == pool_stats["pool_available"]
    finally:
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault_target", _APPLY_DML_ORDER)
async def test_terminal_apply_rolls_back_after_each_executed_dml_statement(
    cold_start_runtime: _ColdStartRuntime,
    fault_target: str,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Boundary-G4/%3D",
        includes_last=True,
        changes=(_create_change("historical-g4"),),
    )
    setup_origin = _ColdStartOrigin([boundary])
    setup_service = _service(
        cold_start_runtime,
        origin=setup_origin,
        ordinary=_OrdinaryPageClient([]),
    )
    ready = await setup_service.preview(
        8,
        "INBOX",
        actor="g4-preview",
        reason="prepare terminal apply rollback matrix",
        idempotency_key=f"g4-preview-{fault_target}",
    )
    assert ready.plan is not None
    approved = await setup_service.approve(
        ready.plan.plan_id,
        actor="g4-approver",
        reason="approve terminal apply rollback matrix",
        idempotency_key=f"g4-approve-{fault_target}",
    )
    assert approved.status is ColdStartRunStatus.APPROVED
    assert setup_origin.calls == [(8, "Inbox", None, 100)]
    durable_before = _durable_physical_snapshot(cold_start_runtime)

    terminal = _batch(
        "opaque+Terminal-G4/%3D",
        includes_last=True,
        changes=(_create_change("ordinary-g4"),),
    )
    apply_origin = _ColdStartOrigin([])
    ordinary = _OrdinaryPageClient([terminal])
    process_name = f"g4-{_APPLY_DML_ORDER.index(fault_target)}"
    pool_application_name = _process_application_name(
        cold_start_runtime,
        process_name,
    )
    pool = await _open_maintenance_pool(cold_start_runtime, process_name)
    fault_pool = _ApplyStatementFaultPool(pool, fault_target)
    try:
        apply_service = _service(
            cold_start_runtime,
            origin=apply_origin,
            ordinary=ordinary,
            pool=fault_pool,
        )
        with pytest.raises(_InjectedApplyStatementFailure) as captured:
            await apply_service.apply(ready.plan.plan_id)

        assert captured.value is fault_pool.error
        assert apply_origin.calls == []
        assert ordinary.calls == [(8, "Inbox", boundary.cursor, 100)]
        expected_dml = _APPLY_DML_ORDER[: _APPLY_DML_ORDER.index(fault_target) + 1]
        assert tuple(item.marker for item in fault_pool.observations) == expected_dml
        assert all(
            item.rowcount == 1 and item.transaction_status is TransactionStatus.INTRANS
            for item in fault_pool.observations
        )
        assert len({item.transaction_id for item in fault_pool.observations}) == 1
        assert _durable_physical_snapshot(cold_start_runtime) == durable_before
        assert _rows(
            cold_start_runtime,
            "SELECT state, version, apply_cursor, apply_cursor_version, "
            "completed_at FROM sync_cold_start_plans WHERE plan_id = %s",
            (ready.plan.plan_id,),
        ) == [
            {
                "state": "approved",
                "version": 2,
                "apply_cursor": None,
                "apply_cursor_version": None,
                "completed_at": None,
            }
        ]
        assert _rows(
            cold_start_runtime,
            "SELECT cursor, status, version FROM sync_cursors "
            "WHERE account_id = 8 AND folder_key = 'INBOX'",
        ) == [
            {
                "cursor": None,
                "status": "cold_start_pending",
                "version": 0,
            }
        ]
        assert (
            _scalar(
                cold_start_runtime,
                "SELECT pg_catalog.count(*) FROM event_inbox",
            )
            == 0
        )
        assert _rows(
            cold_start_runtime,
            "SELECT command_name FROM cold_start_command_receipts "
            "ORDER BY command_name",
        ) == [
            {"command_name": "cold_start.approve"},
            {"command_name": "cold_start.preview"},
        ]

        async with pool.connection() as connection:
            assert connection.info.transaction_status is TransactionStatus.IDLE
            cursor = await connection.execute(
                "SELECT pg_catalog.count(*) AS inbox_count FROM event_inbox"
            )
            row = await cursor.fetchone()
            assert row is not None and int(row["inbox_count"]) == 0
            assert connection.info.transaction_status is TransactionStatus.IDLE

        pool_stats = pool.get_stats()
        assert pool_stats["requests_waiting"] == 0
        assert pool_stats["pool_size"] == pool_stats["pool_available"]
    finally:
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_mode",
    ["commit_then_raise", "rollback_then_raise"],
)
async def test_preview_page_ack_loss_recovers_exact_database_outcome(
    cold_start_runtime: _ColdStartRuntime,
    fault_mode: str,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    audits_before_preview = _durable_physical_snapshot(cold_start_runtime)["audits"]
    boundary = _batch(
        f"opaque+Boundary-G6-{fault_mode}/%3D",
        includes_last=True,
        changes=(_create_change(f"historical-g6-{fault_mode}"),),
    )
    ordinary = _OrdinaryPageClient([])
    permit = _PermitProvider()
    state = _PreviewAckLossFaultState(fault_mode)
    origin = _BlockingColdStartOrigin(boundary, state)
    process_name = f"g6-{fault_mode.replace('_then_raise', '')}"
    pool_application_name = _process_application_name(
        cold_start_runtime,
        process_name,
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    fault_pool = _PreviewAckLossPool(pool, state)
    task: asyncio.Task[Any] | None = None
    result: Any = None
    pre: dict[str, list[dict[str, Any]]] | None = None
    visible: dict[str, list[dict[str, Any]]] | None = None
    try:
        service = _service(
            cold_start_runtime,
            origin=origin,  # type: ignore[arg-type]
            ordinary=ordinary,
            pool=fault_pool,  # type: ignore[arg-type]
            permit=permit,
            preview_max_pages=1,
        )
        task = asyncio.create_task(
            service.preview(
                8,
                "INBOX",
                actor=f"g6-{fault_mode}",
                reason="prove exact preview ACK-loss recovery",
                idempotency_key=f"g6-preview-{fault_mode}",
            )
        )
        await asyncio.wait_for(origin.entered.wait(), timeout=5.0)
        assert origin.calls == [(8, "Inbox", None, 100)]
        assert len(fault_pool.checked_out_pids) == 1
        old_pid = fault_pool.checked_out_pids[0]
        assert state.http_phase == "pre_http"
        assert state.http_transition_count == 0
        assert not state.post_http_entered.is_set()
        assert len(fault_pool.checked_out_connections) == 1
        origin_connection = fault_pool.checked_out_connections[0]
        assert origin_connection._phase == "origin"
        assert origin_connection._backend_pid == old_pid
        assert origin_connection._active_transaction is None
        assert (
            origin_connection._connection.info.transaction_status
            is TransactionStatus.IDLE
        )
        acceptance = state.acceptance_observations
        assert tuple(item.marker for item in acceptance) == (
            _PREVIEW_ACCEPTANCE_DML_ORDER
        )
        assert tuple(item.statement_digest for item in acceptance) == (
            _PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_ORDER
        )
        assert len({item.transaction_id for item in acceptance}) == 1
        assert all(
            item.rowcount == 1
            and item.backend_pid == old_pid
            and item.transaction_status is TransactionStatus.INTRANS
            and item.http_phase == "pre_http"
            for item in acceptance
        )
        assert _backend_pid_exists(cold_start_runtime, old_pid)

        pre = _durable_physical_snapshot(cold_start_runtime)
        assert len(pre["plan"]) == 1
        assert pre["plan"][0]["row_data"]["state"] == "previewing"
        assert pre["plan"][0]["row_data"]["version"] == 0
        assert pre["plan"][0]["row_data"]["page_count"] == 0
        assert pre["audits"] == audits_before_preview
        assert [receipt["row_data"]["command_name"] for receipt in pre["receipts"]] == [
            "cold_start.preview"
        ]

        origin.release.set()
        await asyncio.wait_for(state.post_http_entered.wait(), timeout=5.0)
        assert state.http_phase == "post_http"
        assert state.http_transition_count == 1
        await asyncio.wait_for(state.outcome_reached.wait(), timeout=5.0)
        assert len(state.observations) == 1
        first_observation = state.observations[0]
        assert first_observation.armed is True
        assert first_observation.backend_pid == old_pid
        assert first_observation.rowcount == 1
        assert first_observation.transaction_status is TransactionStatus.INTRANS
        assert first_observation.http_phase == "post_http"
        assert (
            first_observation.assigned_transaction_id
            == first_observation.transaction_id
        )
        assert state.exit_phases == [(fault_mode, old_pid, TransactionStatus.IDLE)]
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, old_pid)

        visible = _durable_physical_snapshot(cold_start_runtime)
        if fault_mode == "commit_then_raise":
            assert visible["plan"] != pre["plan"]
            assert visible["plan"][0]["row_data"]["state"] == "ready"
            assert visible["plan"][0]["row_data"]["version"] == 1
            assert visible["plan"][0]["row_data"]["page_count"] == 1
        else:
            assert visible == pre
        assert _backend_pid_exists(cold_start_runtime, old_pid)

        state.outcome_release.set()
        result = await asyncio.wait_for(task, timeout=10.0)

        assert result.status is ColdStartRunStatus.READY
        assert result.plan is not None
        assert result.pages_committed == 1
        assert result.changes_observed == 1
        assert result.safe_code is None
        assert origin.calls == [(8, "Inbox", None, 100)]
        assert origin.exhausted is True
        assert ordinary.calls == []
        assert permit.acquire_count == 2
        assert permit.release_count == 2
        assert permit.active is False

        assert len(fault_pool.checked_out_pids) == 2
        recovery_pid = fault_pool.checked_out_pids[1]
        assert recovery_pid != old_pid
        assert fault_pool.returned_pids == [old_pid, recovery_pid]
        assert fault_pool.returned_closed == [True, False]
        await _wait_until_backend_pid_disappears(cold_start_runtime, old_pid)
        assert not _backend_pid_exists(cold_start_runtime, old_pid)

        assert state.fault_budget == 0
        assert state.http_phase == "post_http"
        assert state.http_transition_count == 1
        assert state.raised_errors == [state.error]
        assert type(state.error) is RuntimeError
        observations = state.observations
        assert state.recovery_sql
        assert all(
            marker in {"read_control", "preview_page_update"}
            for marker, _statement in state.recovery_sql
        )
        if fault_mode == "commit_then_raise":
            assert len(observations) == 1
            assert observations[0].backend_pid == old_pid
            assert all(
                marker == "read_control" for marker, _statement in state.recovery_sql
            )
        else:
            assert len(observations) == 2
            assert observations[1].armed is False
            assert observations[1].backend_pid == recovery_pid
            assert observations[1].transaction_status is TransactionStatus.INTRANS
            assert (
                observations[1].assigned_transaction_id
                == observations[1].transaction_id
            )
            assert observations[0].transaction_id != observations[1].transaction_id
            assert [
                marker
                for marker, _statement in state.recovery_sql
                if marker == "preview_page_update"
            ] == ["preview_page_update"]

        final = _durable_physical_snapshot(cold_start_runtime)
        assert pre is not None and visible is not None
        if fault_mode == "commit_then_raise":
            assert final == visible
        else:
            assert visible == pre
            assert final["plan"] != pre["plan"]
        for domain in ("cursor", "inbox", "receipts", "audits"):
            assert final[domain] == pre[domain]

        assert len(final["plan"]) == 1
        plan = final["plan"][0]["row_data"]
        assert plan["state"] == "ready"
        assert plan["version"] == 1
        assert plan["preview_cursor"] == boundary.cursor
        assert plan["boundary_cursor"] == boundary.cursor
        assert plan["preview_cursor_version"] == 1
        assert plan["boundary_cursor_version"] == 1
        assert plan["page_count"] == 1
        assert plan["item_count"] == 1
        assert (
            type(plan["plan_hash"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", plan["plan_hash"]) is not None
        )
        assert plan["ready_at"] is not None
        assert final["cursor"][0]["row_data"]["status"] == "cold_start_pending"
        assert final["cursor"][0]["row_data"]["version"] == 0
        assert final["cursor"][0]["row_data"]["cursor"] is None
        assert final["inbox"] == []
        assert [
            receipt["row_data"]["command_name"] for receipt in final["receipts"]
        ] == ["cold_start.preview"]

        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"]
    finally:
        origin.release.set()
        state.outcome_release.set()
        if task is not None:
            if not task.done():
                task.cancel()
            try:
                await task
            except BaseException:
                pass
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preview_acceptance_commit_ack_loss_exact_key_retry_replays_receipt(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    state = _PreviewAckLossFaultState(
        "commit_then_raise",
        fault_target="acceptance",
        expected_replay_idempotency_key="g8-preview-acceptance-key",
    )
    state.outcome_release.set()
    origin = _ColdStartOrigin([])
    ordinary = _OrdinaryPageClient([])
    permit = _PermitProvider()
    pool_application_name = _process_application_name(
        cold_start_runtime,
        "g8-preview-acceptance",
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    fault_pool = _PreviewAckLossPool(pool, state)
    try:
        service = _service(
            cold_start_runtime,
            origin=origin,
            ordinary=ordinary,
            pool=fault_pool,  # type: ignore[arg-type]
            permit=permit,
            preview_max_pages=1,
        )
        call = {
            "actor": "g8-preview-actor",
            "reason": "prove preview acceptance exact-key replay",
            "idempotency_key": "g8-preview-acceptance-key",
        }

        with pytest.raises(RuntimeError) as raised:
            await service.preview(8, "INBOX", **call)

        assert raised.value is state.error
        assert state.outcome_reached.is_set()
        assert state.fault_budget == 0
        assert state.http_phase == "pre_http"
        assert state.http_transition_count == 0
        assert origin.calls == []
        assert ordinary.calls == []
        assert (
            tuple(item.marker for item in state.acceptance_observations)
            == _PREVIEW_ACCEPTANCE_DML_ORDER
        )
        assert (
            tuple(item.statement_digest for item in state.acceptance_observations)
            == _PREVIEW_ACCEPTANCE_DML_TOKEN_DIGEST_ORDER
        )
        assert len({item.transaction_id for item in state.acceptance_observations}) == 1
        old_transaction_id = state.acceptance_observations[0].transaction_id
        old_pid = fault_pool.checked_out_pids[0]
        assert all(
            item.backend_pid == old_pid
            and item.transaction_status is TransactionStatus.INTRANS
            and item.http_phase == "pre_http"
            for item in state.acceptance_observations
        )
        assert fault_pool.returned_pids == [old_pid]
        assert fault_pool.returned_closed == [True]
        await _wait_until_backend_pid_disappears(cold_start_runtime, old_pid)

        committed = _durable_physical_snapshot(cold_start_runtime)
        assert len(committed["plan"]) == 1
        plan_row = committed["plan"][0]["row_data"]
        assert plan_row["state"] == "previewing"
        assert plan_row["version"] == 0
        assert plan_row["account_id"] == 8
        assert plan_row["folder_key"] == "INBOX"
        assert plan_row["expected_cursor_status"] == "cold_start_pending"
        assert plan_row["expected_cursor"] is None
        assert plan_row["expected_cursor_version"] == 0
        assert plan_row["pipeline_name"] == "durable_v1"
        assert plan_row["generation"] == 1
        assert plan_row["fencing_token"] == 1
        assert plan_row["contract_fingerprint"] == "e" * 64
        assert plan_row["folder_scope_config_hash"] == _snapshot().scopes[0].config_hash
        assert plan_row["actor"] == call["actor"]
        assert plan_row["reason"] == call["reason"]
        assert committed["plan"][0]["xmin"] == old_transaction_id
        assert len(committed["receipts"]) == 1
        receipt_row = committed["receipts"][0]["row_data"]
        assert receipt_row["command_name"] == "cold_start.preview"
        assert committed["receipts"][0]["xmin"] == old_transaction_id
        assert tuple(marker for marker, _ in state.acceptance_params) == (
            _PREVIEW_ACCEPTANCE_DML_ORDER
        )
        plan_params = state.acceptance_params[0][1]
        receipt_params = state.acceptance_params[1][1]
        assert plan_params == (
            UUID(plan_row["plan_id"]),
            8,
            "INBOX",
            "cold_start_pending",
            None,
            0,
            "durable_v1",
            1,
            1,
            "e" * 64,
            _snapshot().scopes[0].config_hash,
            call["actor"],
            call["reason"],
            3600,
        )
        payload_hash, idempotency_hash, result_hash = (
            _independent_preview_command_hashes(
                plan=plan_row,
                actor=call["actor"],
                reason=call["reason"],
                idempotency_key=call["idempotency_key"],
            )
        )
        assert receipt_params == (
            UUID(receipt_row["id"]),
            8,
            "cold_start.preview",
            idempotency_hash,
            payload_hash,
            "succeeded",
            "sync_cold_start_plan",
            plan_row["plan_id"],
            result_hash,
            1,
        )
        assert receipt_row["account_id"] == 8
        assert receipt_row["idempotency_key_hash"] == idempotency_hash
        assert receipt_row["canonical_payload_hash"] == payload_hash
        assert receipt_row["outcome"] == "succeeded"
        assert receipt_row["result_type"] == "sync_cold_start_plan"
        assert receipt_row["result_id"] == plan_row["plan_id"]
        assert receipt_row["result_hash"] == result_hash
        assert receipt_row["authority_epoch"] == 1
        returned_receipt_row = state.acceptance_receipt_row
        assert returned_receipt_row is not None
        durable_receipt_projection = _command_receipt_projection(receipt_row)
        returned_receipt_projection = _command_receipt_projection(returned_receipt_row)
        assert returned_receipt_projection == durable_receipt_projection
        assert receipt_params == (
            UUID(returned_receipt_projection["id"]),
            returned_receipt_projection["account_id"],
            returned_receipt_projection["command_name"],
            returned_receipt_projection["idempotency_key_hash"],
            returned_receipt_projection["canonical_payload_hash"],
            returned_receipt_projection["outcome"],
            returned_receipt_projection["result_type"],
            returned_receipt_projection["result_id"],
            returned_receipt_projection["result_hash"],
            returned_receipt_projection["authority_epoch"],
        )

        result = await service.preview(8, "INBOX", **call)

        assert result.status is ColdStartRunStatus.PREVIEWING
        assert result.plan is not None
        assert str(result.plan.plan_id) == committed["plan"][0]["row_data"]["plan_id"]
        assert _plan_view_projection(result.plan) == _durable_plan_view_projection(
            cold_start_runtime,
            result.plan.plan_id,
        )
        assert result.pages_committed == 0
        assert result.changes_observed == 0
        assert result.safe_code is None
        assert origin.calls == []
        assert ordinary.calls == []
        assert _durable_physical_snapshot(cold_start_runtime) == committed
        assert len(state.acceptance_observations) == 2
        assert state.recovery_sql
        assert all(marker == "read_control" for marker, _ in state.recovery_sql)
        assert tuple(state.recovery_business_marker_trace) == (
            _COMMAND_REPLAY_MARKER_ORDER
        )
        assert state.recovery_control_sql
        assert all(
            _command_replay_marker(
                statement,
                None,
                state.replay_expectation(),
            )
            == "control"
            for statement in state.recovery_control_sql
        )
        recovery_sql = [statement.lower() for _, statement in state.recovery_sql]
        assert not any(
            forbidden in statement
            for statement in recovery_sql
            for forbidden in (
                "pipeline_ownership",
                "sync_cursors",
                "clock_timestamp",
                "state in ('previewing', 'ready', 'approved')",
            )
        )
        receipt_read = next(
            index
            for index, statement in enumerate(recovery_sql)
            if "cold_start_command_receipts" in statement
            and statement.lstrip().startswith("select")
        )
        plan_read = next(
            index
            for index, statement in enumerate(recovery_sql)
            if "from public.sync_cold_start_plans" in statement
        )
        assert receipt_read < plan_read
        assert permit.acquire_count == 2
        assert permit.release_count == 2
        assert permit.active is False
        assert len(fault_pool.checked_out_pids) == 2
        retry_pid = fault_pool.checked_out_pids[1]
        assert retry_pid != old_pid
        assert _backend_pid_exists(cold_start_runtime, retry_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retry_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retry_pid) == 0
        assert fault_pool.returned_pids == [old_pid, retry_pid]
        assert fault_pool.returned_closed == [True, False]
        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"]
    finally:
        state.outcome_release.set()
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_approve_commit_ack_loss_exact_key_retry_replays_receipt(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Boundary-G8-approve/%3D",
        includes_last=True,
        changes=(_create_change("historical-g8-approve"),),
    )
    setup_origin = _ColdStartOrigin([boundary])
    setup_service = _service(
        cold_start_runtime,
        origin=setup_origin,
        ordinary=_OrdinaryPageClient([]),
        preview_max_pages=1,
    )
    ready = await setup_service.preview(
        8,
        "INBOX",
        actor="g8-preview-setup",
        reason="prepare exact approve ACK-loss replay",
        idempotency_key="g8-preview-setup-key",
    )
    assert ready.status is ColdStartRunStatus.READY
    assert ready.plan is not None
    pre = _durable_physical_snapshot(cold_start_runtime)
    assert len(pre["plan"]) == 1
    ready_row = pre["plan"][0]["row_data"]
    assert ready_row["state"] == "ready"
    assert ready_row["version"] == 1

    actor = "g8-approve-actor"
    reason = "prove approve exact-key durable replay"
    idempotency_key = "g8-approve-command-key"
    state = _ApproveAckLossFaultState(
        plan_id=ready.plan.plan_id,
        ready_plan=ready_row,
        actor=actor,
        reason=reason,
        idempotency_key=idempotency_key,
    )
    state.outcome_release.set()
    origin = _ColdStartOrigin([])
    ordinary = _OrdinaryPageClient([])
    permit = _PermitProvider()
    pool_application_name = _process_application_name(
        cold_start_runtime,
        "g8-approve",
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    fault_pool = _ApproveAckLossPool(pool, state)
    try:
        service = _service(
            cold_start_runtime,
            origin=origin,
            ordinary=ordinary,
            pool=fault_pool,  # type: ignore[arg-type]
            permit=permit,
        )

        with pytest.raises(RuntimeError) as raised:
            await service.approve(
                ready.plan.plan_id,
                actor=actor,
                reason=reason,
                idempotency_key=idempotency_key,
            )

        assert raised.value is state.error
        assert state.outcome_reached.is_set()
        assert state.fault_budget == 0
        assert state.raised_errors == [state.error]
        assert tuple(item.marker for item in state.observations) == (
            _APPROVE_COMMAND_DML_ORDER
        )
        assert tuple(item.statement_digest for item in state.observations) == tuple(
            _APPROVE_COMMAND_DML_TOKEN_DIGEST_MARKERS
        )
        assert len({item.transaction_id for item in state.observations}) == 1
        old_transaction_id = state.observations[0].transaction_id
        old_pid = fault_pool.checked_out_pids[0]
        assert fault_pool.checked_out_pids == [old_pid, old_pid]
        assert all(
            item.rowcount == 1
            and item.backend_pid == old_pid
            and item.transaction_status is TransactionStatus.INTRANS
            for item in state.observations
        )
        assert state.exit_phases == [(old_pid, TransactionStatus.IDLE)]
        assert state.locator_calls == ["locator_origin"]
        assert fault_pool.checked_out_connections[0]._connection is (
            fault_pool.checked_out_connections[1]._connection
        )
        assert fault_pool.returned_pids == [old_pid, old_pid]
        assert fault_pool.returned_closed == [False, True]
        await _wait_until_backend_pid_disappears(cold_start_runtime, old_pid)

        committed = _durable_physical_snapshot(cold_start_runtime)
        plan_row = committed["plan"][0]["row_data"]
        assert plan_row["state"] == "approved"
        assert plan_row["version"] == 2
        assert committed["plan"][0]["xmin"] == old_transaction_id
        approve_audits = [
            item
            for item in committed["audits"]
            if item["row_data"]["action"] == "cold_start.approve"
        ]
        approve_receipts = [
            item
            for item in committed["receipts"]
            if item["row_data"]["command_name"] == "cold_start.approve"
        ]
        assert len(approve_audits) == len(approve_receipts) == 1
        assert approve_audits[0]["xmin"] == old_transaction_id
        assert approve_receipts[0]["xmin"] == old_transaction_id
        approved = state.approved_row
        assert approved is not None
        payload_hash, idempotency_hash, result_hash = (
            _independent_approve_command_hashes(
                approved=approved,
                actor=actor,
                reason=reason,
                idempotency_key=idempotency_key,
            )
        )
        receipt_row = approve_receipts[0]["row_data"]
        receipt_params = state.observations[2].params
        assert type(receipt_params) is tuple
        assert receipt_params == (
            UUID(receipt_row["id"]),
            8,
            "cold_start.approve",
            idempotency_hash,
            payload_hash,
            "succeeded",
            "sync_cold_start_plan",
            str(ready.plan.plan_id),
            result_hash,
            1,
        )
        assert receipt_row["idempotency_key_hash"] == idempotency_hash
        assert receipt_row["canonical_payload_hash"] == payload_hash
        assert receipt_row["result_hash"] == result_hash
        assert receipt_row["result_id"] == str(ready.plan.plan_id)
        assert receipt_row["authority_epoch"] == 1
        returned_receipt_row = state.receipt_row
        assert returned_receipt_row is not None
        durable_receipt_projection = _command_receipt_projection(receipt_row)
        returned_receipt_projection = _command_receipt_projection(returned_receipt_row)
        assert returned_receipt_projection == durable_receipt_projection
        assert receipt_params == (
            UUID(returned_receipt_projection["id"]),
            returned_receipt_projection["account_id"],
            returned_receipt_projection["command_name"],
            returned_receipt_projection["idempotency_key_hash"],
            returned_receipt_projection["canonical_payload_hash"],
            returned_receipt_projection["outcome"],
            returned_receipt_projection["result_type"],
            returned_receipt_projection["result_id"],
            returned_receipt_projection["result_hash"],
            returned_receipt_projection["authority_epoch"],
        )

        result = await service.approve(
            ready.plan.plan_id,
            actor=actor,
            reason=reason,
            idempotency_key=idempotency_key,
        )

        assert result.status is ColdStartRunStatus.APPROVED
        assert result.plan is not None
        assert _plan_view_projection(result.plan) == _durable_plan_view_projection(
            cold_start_runtime,
            result.plan.plan_id,
        )
        assert result.pages_committed == 0
        assert result.changes_observed == 0
        assert result.safe_code is None
        assert origin.calls == []
        assert ordinary.calls == []
        assert _durable_physical_snapshot(cold_start_runtime) == committed
        assert len(state.observations) == 3
        assert state.locator_calls == ["locator_origin", "locator_replay"]
        assert len(fault_pool.checked_out_pids) == 4
        retry_pid = fault_pool.checked_out_pids[2]
        assert retry_pid != old_pid
        assert _backend_pid_exists(cold_start_runtime, retry_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retry_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retry_pid) == 0
        assert fault_pool.checked_out_pids == [old_pid, old_pid, retry_pid, retry_pid]
        assert fault_pool.checked_out_connections[2]._connection is (
            fault_pool.checked_out_connections[3]._connection
        )
        assert fault_pool.returned_pids == [old_pid, old_pid, retry_pid, retry_pid]
        assert fault_pool.returned_closed == [False, True, False, False]
        assert permit.acquire_count == 2
        assert permit.release_count == 2
        assert permit.active is False
        assert state.replay_sql
        assert tuple(state.replay_business_marker_trace) == (
            _COMMAND_REPLAY_MARKER_ORDER
        )
        assert state.replay_control_sql
        assert all(
            _command_replay_marker(
                statement,
                None,
                state.replay_expectation(),
            )
            == "control"
            for statement in state.replay_control_sql
        )
        replay_sql = [statement.lower() for statement in state.replay_sql]
        assert not any(
            forbidden in statement
            for statement in replay_sql
            for forbidden in (
                "pipeline_ownership",
                "sync_cursors",
                "clock_timestamp",
                "state in ('previewing', 'ready', 'approved')",
            )
        )
        receipt_read = next(
            index
            for index, statement in enumerate(replay_sql)
            if "cold_start_command_receipts" in statement
            and statement.lstrip().startswith("select")
        )
        plan_read = next(
            index
            for index, statement in enumerate(replay_sql)
            if "from public.sync_cold_start_plans" in statement
        )
        assert receipt_read < plan_read
        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"]
    finally:
        state.outcome_release.set()
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


async def _prepare_g10_approved_plan(
    runtime: _ColdStartRuntime,
    *,
    suffix: str,
) -> UUID:
    _seed_cold_start_cursor(runtime, "cold_start_pending")
    boundary = _batch(
        f"opaque+Boundary-G10-{suffix}/%3D",
        includes_last=True,
        changes=(_create_change(f"historical-g10-{suffix}"),),
    )
    origin = _ColdStartOrigin([boundary])
    service = _service(
        runtime,
        origin=origin,
        ordinary=_OrdinaryPageClient([]),
        preview_max_pages=1,
    )
    ready = await service.preview(
        8,
        "INBOX",
        actor=f"g10-preview-{suffix}",
        reason="prepare locator cancellation proof",
        idempotency_key=f"g10-preview-{suffix}",
    )
    assert ready.status is ColdStartRunStatus.READY
    assert ready.plan is not None
    approved = await service.approve(
        ready.plan.plan_id,
        actor=f"g10-approve-{suffix}",
        reason="approve locator cancellation proof",
        idempotency_key=f"g10-approve-{suffix}",
    )
    assert approved.status is ColdStartRunStatus.APPROVED
    assert approved.plan is not None
    assert origin.calls == [(8, "Inbox", None, 100)]
    return approved.plan.plan_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_g10_locator_fetch_external_cancel_retires_real_backend(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    plan_id = await _prepare_g10_approved_plan(
        cold_start_runtime,
        suffix="fetch",
    )
    durable_before = _durable_physical_snapshot(cold_start_runtime)
    assert durable_before["plan"]
    assert durable_before["cursor"]
    assert durable_before["receipts"]
    assert durable_before["audits"]

    state = _G10LocatorCancellationState("fetch_cancel", plan_id)
    snapshot_provider = _G10NeverSnapshotProvider()
    policy_resolver = _G10NeverPolicyResolver()
    permit = _PermitProvider()
    origin = _ColdStartOrigin([])
    ordinary = _OrdinaryPageClient([])
    pool_application_name = _process_application_name(
        cold_start_runtime,
        "g10-fetch-cancel",
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    guarded_pool = _G10LocatorPool(pool, state)
    task: asyncio.Task[Any] | None = None
    observed_cancellations: list[asyncio.CancelledError] = []

    async def observed_apply(service: ColdStartService) -> object:
        try:
            return await service.apply(plan_id)
        except asyncio.CancelledError as error:
            observed_cancellations.append(error)
            raise

    try:
        service = _service(
            cold_start_runtime,
            origin=origin,
            ordinary=ordinary,
            pool=guarded_pool,  # type: ignore[arg-type]
            permit=permit,
            snapshot_provider=snapshot_provider,
            policy_resolver=policy_resolver,
        )
        task = asyncio.create_task(observed_apply(service))
        await asyncio.wait_for(state.fetch_entered.wait(), timeout=5.0)

        assert state.backend_pid is not None
        old_pid = state.backend_pid
        assert len(state.statements) == 1
        assert _sql_tokens(state.statements[0][0]) == _G10_LOCATOR_TOKENS
        assert state.statements[0][1] == (plan_id,)
        assert state.forbidden_statements == []
        assert state.putconn_intents == []
        assert state.close_events == []
        assert snapshot_provider.calls == []
        assert policy_resolver.calls == []
        assert permit.acquire_count == permit.release_count == 0
        assert permit.active is False
        assert origin.calls == []
        assert ordinary.calls == []
        assert _backend_pid_exists(cold_start_runtime, old_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, old_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, old_pid) == 0
        assert _durable_physical_snapshot(cold_start_runtime) == durable_before

        assert task.cancel() is True
        with pytest.raises(asyncio.CancelledError) as caught:
            await asyncio.wait_for(task, timeout=5.0)

        assert caught.value is state.cancelled_error
        assert observed_cancellations == [caught.value]
        assert type(caught.value) is asyncio.CancelledError
        assert task.cancelled() is True
        assert state.close_events == [
            ("close.start", old_pid, False),
            ("close.done", old_pid, True),
        ]
        assert len(state.putconn_intents) == 1
        returned_pid, returned_closed, returned_status = state.putconn_intents[0]
        assert returned_pid == old_pid
        assert returned_closed is True
        assert returned_status is TransactionStatus.UNKNOWN
        assert state.putconn_completions == [(old_pid, True)]
        assert state.forbidden_statements == []
        assert snapshot_provider.calls == []
        assert policy_resolver.calls == []
        assert permit.acquire_count == permit.release_count == 0
        assert permit.active is False
        assert origin.calls == []
        assert ordinary.calls == []
        assert _durable_physical_snapshot(cold_start_runtime) == durable_before

        await _wait_until_backend_pid_disappears(cold_start_runtime, old_pid)
        assert not _backend_pid_exists(cold_start_runtime, old_pid)
        async with pool.connection(timeout=5.0) as connection:
            new_pid = connection.info.backend_pid
            assert type(new_pid) is int and new_pid != old_pid
            cursor = await connection.execute("SELECT 1 AS healthy")
            assert await cursor.fetchone() == {"healthy": 1}
            assert connection.info.transaction_status is TransactionStatus.IDLE
        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"] == 1
    finally:
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        await pool.close()
        assert (
            _application_session_count(cold_start_runtime, pool_application_name) == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_g10_locator_return_external_cancel_finishes_real_pool_handoff(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    plan_id = await _prepare_g10_approved_plan(
        cold_start_runtime,
        suffix="return",
    )
    durable_before = _durable_physical_snapshot(cold_start_runtime)
    assert all(durable_before[key] for key in ("plan", "cursor", "receipts", "audits"))

    state = _G10LocatorCancellationState("return_cancel", plan_id)
    snapshot_provider = _G10NeverSnapshotProvider()
    policy_resolver = _G10NeverPolicyResolver()
    permit = _PermitProvider()
    origin = _ColdStartOrigin([])
    ordinary = _OrdinaryPageClient([])
    pool_application_name = _process_application_name(
        cold_start_runtime,
        "g10-return-cancel",
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    guarded_pool = _G10LocatorPool(pool, state)
    task: asyncio.Task[Any] | None = None
    borrower: asyncio.Task[int] | None = None
    lock_holder: asyncio.Task[None] | None = None
    observed_cancellations: list[asyncio.CancelledError] = []

    async def observed_apply(service: ColdStartService) -> object:
        try:
            return await service.apply(plan_id)
        except asyncio.CancelledError as error:
            observed_cancellations.append(error)
            raise

    async def competing_borrower() -> int:
        connection = await pool.getconn(timeout=3.0)
        backend_pid = connection.info.backend_pid
        assert type(backend_pid) is int
        state.borrowed_pids.append(backend_pid)
        try:
            cursor = await connection.execute("SELECT 1 AS healthy")
            assert await cursor.fetchone() == {"healthy": 1}
            assert connection.info.transaction_status is TransactionStatus.IDLE
        finally:
            await pool.putconn(connection)
        return backend_pid

    async def hold_real_pool_lock() -> None:
        async with pool._lock:
            state.pool_lock_held.set()
            await state.release_pool_lock.wait()

    try:
        service = _service(
            cold_start_runtime,
            origin=origin,
            ordinary=ordinary,
            pool=guarded_pool,  # type: ignore[arg-type]
            permit=permit,
            snapshot_provider=snapshot_provider,
            policy_resolver=policy_resolver,
        )
        task = asyncio.create_task(observed_apply(service))
        await asyncio.wait_for(state.row_fetched.wait(), timeout=5.0)

        assert state.backend_pid is not None
        old_pid = state.backend_pid
        assert len(state.statements) == 1
        assert _sql_tokens(state.statements[0][0]) == _G10_LOCATOR_TOKENS
        assert state.statements[0][1] == (plan_id,)
        assert state.putconn_intents == []
        assert state.close_events == []
        assert _backend_pid_exists(cold_start_runtime, old_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, old_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, old_pid) == 0

        borrower = asyncio.create_task(competing_borrower())
        for _ in range(100):
            if pool.get_stats()["requests_waiting"] == 1:
                break
            await asyncio.sleep(0)
        assert pool.get_stats()["requests_waiting"] == 1
        assert state.borrowed_pids == []

        lock_holder = asyncio.create_task(hold_real_pool_lock())
        await asyncio.wait_for(state.pool_lock_held.wait(), timeout=2.0)
        state.allow_row_return.set()
        await asyncio.wait_for(state.return_delegate_entered.wait(), timeout=2.0)
        assert guarded_pool.connection is not None
        raw_connection = guarded_pool.connection._connection
        for _ in range(100):
            if raw_connection._pool is None:
                break
            await asyncio.sleep(0)
        assert raw_connection._pool is None
        assert raw_connection.closed is False
        assert raw_connection.info.transaction_status is TransactionStatus.IDLE
        assert state.putconn_intents == [(old_pid, False, TransactionStatus.IDLE)]
        assert state.putconn_completions == []
        assert state.borrowed_pids == []
        assert snapshot_provider.calls == []
        assert policy_resolver.calls == []
        assert permit.acquire_count == permit.release_count == 0
        assert permit.active is False
        assert origin.calls == []
        assert ordinary.calls == []
        assert state.forbidden_statements == []
        assert _durable_physical_snapshot(cold_start_runtime) == durable_before

        assert task.cancel() is True
        for _ in range(100):
            await asyncio.sleep(0)
            if (
                state.cancelled_error is not None
                or state.second_return_entered.is_set()
            ):
                break
        assert state.cancelled_error is None, (
            "the real psycopg_pool.putconn child was cancelled after detaching "
            "the connection from pool accounting"
        )
        assert not state.second_return_entered.is_set(), (
            "locator attempted ambiguous close/re-return instead of finishing "
            "the shielded real pool handoff"
        )
        assert task.done() is False

        state.release_pool_lock.set()
        assert lock_holder is not None
        await asyncio.wait_for(lock_holder, timeout=2.0)
        with pytest.raises(asyncio.CancelledError) as caught:
            await asyncio.wait_for(task, timeout=5.0)
        assert observed_cancellations == [caught.value]
        assert type(caught.value) is asyncio.CancelledError
        assert task.cancelled() is True

        assert borrower is not None
        borrowed_pid = await asyncio.wait_for(borrower, timeout=5.0)
        assert borrowed_pid == old_pid
        assert state.borrowed_pids == [old_pid]
        assert state.putconn_intents == [(old_pid, False, TransactionStatus.IDLE)]
        assert state.putconn_completions == [(old_pid, False)]
        assert state.close_events == []
        assert snapshot_provider.calls == []
        assert policy_resolver.calls == []
        assert permit.acquire_count == permit.release_count == 0
        assert permit.active is False
        assert origin.calls == []
        assert ordinary.calls == []
        assert state.forbidden_statements == []
        assert _durable_physical_snapshot(cold_start_runtime) == durable_before
        assert _backend_pid_exists(cold_start_runtime, old_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, old_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, old_pid) == 0
        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"] == 1
    finally:
        state.allow_row_return.set()
        state.release_pool_lock.set()
        if lock_holder is not None and not lock_holder.done():
            try:
                await lock_holder
            except BaseException:
                pass
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        if borrower is not None and not borrower.done():
            borrower.cancel()
            try:
                await borrower
            except BaseException:
                pass
        await pool.close()
        assert (
            _application_session_count(cold_start_runtime, pool_application_name) == 0
        )


@pytest.mark.asyncio
async def test_g11_tracking_pool_done_observes_post_delegate_state() -> None:
    class _MutableInfo:
        backend_pid = 12345
        transaction_status = TransactionStatus.IDLE

    class _MutableConnection:
        def __init__(self) -> None:
            self.closed = False
            self.info = _MutableInfo()

    class _MutatingReturnPool:
        async def putconn(self, connection: _MutableConnection) -> None:
            connection.closed = True
            connection.info.transaction_status = TransactionStatus.UNKNOWN

    raw_connection = _MutableConnection()
    delegate = _MutatingReturnPool()
    tracking_pool = _G11TrackingPool(
        delegate,  # type: ignore[arg-type]
        roles=("retained",),
    )
    connection = _G11TrackingConnection(
        raw_connection,
        tracking_pool.events,
        role="retained",
        checkout_id=0,
    )

    await tracking_pool.putconn(connection)

    assert tracking_pool.events == [
        (
            "putconn.start",
            "retained",
            0,
            12345,
            False,
            TransactionStatus.IDLE,
        ),
        (
            "putconn.done",
            "retained",
            0,
            12345,
            True,
            TransactionStatus.UNKNOWN,
        ),
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_g11_cold_start_session_lock_serializes_independent_services(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    first_application_name = _process_application_name(
        cold_start_runtime,
        "g11-first-preview",
    )
    second_application_name = _process_application_name(
        cold_start_runtime,
        "g11-plan-entry",
    )
    first_pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=first_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    second_pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=second_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await first_pool.open()
    try:
        await second_pool.open()
    except BaseException:
        await first_pool.close()
        raise

    first_tracking_pool = _G11TrackingPool(first_pool, roles=("retained",))
    second_tracking_pool = _G11TrackingPool(
        second_pool,
        roles=("locator", "retained"),
    )
    first_permit = _PermitProvider()
    second_permit = _PermitProvider()
    first_origin = _G11BlockingOrigin(
        _batch(
            "opaque+G11-preview-1/%3D",
            includes_last=False,
            changes=(_create_change("historical-g11-first"),),
        )
    )
    second_origin = _G11BlockingOrigin(
        _batch(
            "opaque+G11-preview-2/%3D",
            includes_last=True,
            changes=(_create_change("historical-g11-second"),),
        )
    )
    first_ordinary = _OrdinaryPageClient([])
    second_ordinary = _OrdinaryPageClient([])
    first_service = _service(
        cold_start_runtime,
        origin=first_origin,  # type: ignore[arg-type]
        ordinary=first_ordinary,
        pool=first_tracking_pool,  # type: ignore[arg-type]
        permit=first_permit,
        preview_max_pages=1,
    )
    second_service = _service(
        cold_start_runtime,
        origin=second_origin,  # type: ignore[arg-type]
        ordinary=second_ordinary,
        pool=second_tracking_pool,  # type: ignore[arg-type]
        permit=second_permit,
        preview_max_pages=1,
    )
    first_task: asyncio.Task[Any] | None = None
    second_task: asyncio.Task[Any] | None = None
    first_borrower: asyncio.Task[Any] | None = None
    second_borrower: asyncio.Task[Any] | None = None
    first_borrowed_connection: Any | None = None
    second_borrowed_connection: Any | None = None
    first_borrowed_returned = False
    second_borrowed_returned = False
    try:
        first_task = asyncio.create_task(
            first_service.preview(
                8,
                "INBOX",
                actor="g11-first-preview",
                reason="hold the retained session lock across Origin HTTP",
                idempotency_key="g11-first-preview-key",
            )
        )
        await asyncio.wait_for(first_origin.entered.wait(), timeout=5.0)

        plan_rows = _rows(
            cold_start_runtime,
            "SELECT plan_id FROM sync_cold_start_plans",
        )
        assert len(plan_rows) == 1
        plan_id = plan_rows[0]["plan_id"]
        assert type(plan_id) is UUID
        lock_keys = sync_advisory_lock_keys(8, "INBOX")
        assert len(first_tracking_pool.connections) == 1
        first_pid = first_tracking_pool.connections[0]._backend_pid
        assert type(first_pid) is int
        assert first_task.done() is False
        assert first_origin.calls == [(8, "Inbox", None, 100)]
        assert first_ordinary.calls == []
        assert first_permit.acquire_count == 1
        assert first_permit.release_count == 0
        assert first_permit.active is True
        assert (
            _application_session_count(
                cold_start_runtime,
                first_application_name,
            )
            == 1
        )
        assert _backend_pid_exists(cold_start_runtime, first_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, first_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, first_pid) == 1
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            False,
            None,
        )
        assert [
            event[4:6]
            for event in first_tracking_pool.events
            if event[0] == "lock.result"
        ] == [("acquired", True)]
        assert not any(
            event[4] == "released"
            for event in first_tracking_pool.events
            if event[0] == "lock.result"
        )
        first_lock_result_index = next(
            index
            for index, event in enumerate(first_tracking_pool.events)
            if event[0] == "lock.result"
            and event[1] == "retained"
            and event[4:6] == ("acquired", True)
        )
        assert not any(
            event[0] in {"putconn.start", "putconn.done"}
            and event[1:3] == ("retained", 0)
            for event in first_tracking_pool.events[first_lock_result_index + 1 :]
        )
        first_borrower = asyncio.create_task(first_pool.getconn(timeout=30.0))
        await _wait_for_g11_pool_waiter(first_pool, first_borrower)

        held_snapshot = _durable_physical_snapshot(cold_start_runtime)
        assert len(held_snapshot["plan"]) == 1
        held_plan = held_snapshot["plan"][0]["row_data"]
        assert held_plan["plan_id"] == str(plan_id)
        assert held_plan["state"] == "previewing"
        assert held_plan["version"] == 0
        assert held_plan["page_count"] == held_plan["item_count"] == 0
        assert held_plan["preview_cursor"] is None
        assert held_plan["boundary_cursor"] is None
        assert len(held_snapshot["cursor"]) == 1
        assert held_snapshot["cursor"][0]["row_data"]["status"] == (
            "cold_start_pending"
        )
        assert held_snapshot["cursor"][0]["row_data"]["version"] == 0
        assert held_snapshot["cursor"][0]["row_data"]["cursor"] is None
        assert held_snapshot["inbox"] == []
        assert [
            item["row_data"]["command_name"] for item in held_snapshot["receipts"]
        ] == ["cold_start.preview"]
        assert [
            (item["row_data"]["action"], item["row_data"]["result"])
            for item in held_snapshot["audits"]
        ] == [("pipeline.bootstrap", "succeeded")]

        busy_pids: list[int] = []
        for call_number, entrypoint in enumerate(
            ("resume", "approve", "apply"),
            start=1,
        ):
            before_busy = _durable_physical_snapshot(cold_start_runtime)
            assert before_busy == held_snapshot
            event_offset = len(second_tracking_pool.events)
            if entrypoint == "resume":
                busy = await second_service.resume(plan_id)
            elif entrypoint == "approve":
                busy = await second_service.approve(
                    plan_id,
                    actor="g11-second-approve",
                    reason="prove approve cannot cross the retained lock",
                    idempotency_key="g11-second-approve-key",
                )
            else:
                busy = await second_service.apply(plan_id)

            assert busy.status is ColdStartRunStatus.BUSY_SKIP
            assert busy.plan is None
            assert busy.pages_committed == busy.changes_observed == 0
            assert busy.safe_code == "cold_start.busy"
            assert _durable_physical_snapshot(cold_start_runtime) == before_busy
            assert first_origin.calls == [(8, "Inbox", None, 100)]
            assert second_origin.calls == []
            assert first_ordinary.calls == second_ordinary.calls == []
            assert second_permit.acquire_count == call_number
            assert second_permit.release_count == call_number
            assert second_permit.active is False

            busy_pid = _assert_g11_busy_trace(
                second_tracking_pool.events[event_offset:],
                plan_id=plan_id,
                lock_keys=lock_keys,
            )
            busy_pids.append(busy_pid)
            assert busy_pid != first_pid
            assert _backend_pid_exists(cold_start_runtime, busy_pid)
            assert _backend_pid_has_no_open_transaction(
                cold_start_runtime,
                busy_pid,
            )
            assert (
                _backend_pid_advisory_lock_count(
                    cold_start_runtime,
                    busy_pid,
                )
                == 0
            )
            assert (
                _backend_pid_advisory_lock_count(
                    cold_start_runtime,
                    first_pid,
                )
                == 1
            )
            assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
                False,
                None,
            )

        assert busy_pids == [busy_pids[0]] * 3
        assert (
            _application_session_count(
                cold_start_runtime,
                second_application_name,
            )
            == 1
        )
        assert first_application_name != second_application_name
        assert first_borrower.done() is False
        assert first_pool.get_stats()["requests_waiting"] == 1
        assert first_pool.get_stats()["pool_available"] == 0

        first_origin.release.set()
        first_result = await asyncio.wait_for(first_task, timeout=5.0)
        assert first_result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
        assert first_result.plan is not None
        assert first_result.plan.state is ColdStartPlanState.PREVIEWING
        assert first_result.plan.page_count == 1
        assert first_result.plan.item_count == 1
        assert first_result.pages_committed == first_result.changes_observed == 1
        assert first_result.safe_code == "cold_start.budget_exhausted"
        assert first_permit.acquire_count == first_permit.release_count == 1
        assert first_permit.active is False
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, first_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, first_pid) == 0
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            True,
            True,
        )
        first_borrowed_connection = await asyncio.wait_for(
            first_borrower,
            timeout=5.0,
        )
        assert first_borrowed_connection.info.backend_pid == first_pid
        assert first_borrowed_connection.info.transaction_status is (
            TransactionStatus.IDLE
        )
        await first_pool.putconn(first_borrowed_connection)
        first_borrowed_returned = True
        assert first_pool.get_stats()["requests_waiting"] == 0
        assert first_pool.get_stats()["pool_available"] == 1

        after_first = _durable_physical_snapshot(cold_start_runtime)
        assert after_first != held_snapshot
        after_first_plan = after_first["plan"][0]["row_data"]
        assert after_first_plan["state"] == "previewing"
        assert after_first_plan["version"] == 1
        assert after_first_plan["page_count"] == 1
        assert after_first_plan["item_count"] == 1
        assert after_first_plan["preview_cursor"] == "opaque+G11-preview-1/%3D"
        assert after_first_plan["boundary_cursor"] is None
        assert after_first["cursor"] == held_snapshot["cursor"]
        assert after_first["inbox"] == []
        assert after_first["receipts"] == held_snapshot["receipts"]
        assert after_first["audits"] == held_snapshot["audits"]

        retry_event_offset = len(second_tracking_pool.events)
        second_task = asyncio.create_task(second_service.resume(plan_id))
        await asyncio.wait_for(second_origin.entered.wait(), timeout=5.0)
        retry_events = second_tracking_pool.events[retry_event_offset:]
        assert second_task.done() is False
        assert second_origin.calls == [(8, "Inbox", "opaque+G11-preview-1/%3D", 100)]
        assert second_ordinary.calls == []
        assert second_permit.acquire_count == 4
        assert second_permit.release_count == 3
        assert second_permit.active is True
        assert [event[0] for event in retry_events[:7]] == [
            "getconn",
            "execute",
            "putconn.start",
            "putconn.done",
            "getconn",
            "execute",
            "lock.result",
        ]
        retry_locator_get = retry_events[0]
        retry_locator_execute = retry_events[1]
        retry_locator_done = retry_events[3]
        retry_retained_get = retry_events[4]
        retry_retained_execute = retry_events[5]
        retry_lock_result = retry_events[6]
        assert retry_locator_get[1] == retry_locator_execute[1] == "locator"
        assert retry_locator_execute[4] == _G10_LOCATOR_TOKENS
        assert retry_locator_execute[5] == (plan_id,)
        assert retry_locator_done[0] == "putconn.done"
        assert retry_locator_done[1:4] == retry_locator_get[1:4]
        assert retry_retained_get[1] == retry_retained_execute[1] == "retained"
        assert retry_retained_execute[4] == _G11_TRY_LOCK_TOKENS
        assert retry_retained_execute[5] == lock_keys
        assert retry_lock_result[1:4] == retry_retained_get[1:4]
        assert retry_lock_result[4:] == (
            "acquired",
            True,
            TransactionStatus.IDLE,
        )
        retry_lock_result_index = retry_events.index(retry_lock_result)
        assert not any(
            event[0] in {"putconn.start", "putconn.done"}
            and event[1:3] == retry_retained_get[1:3]
            for event in retry_events[retry_lock_result_index + 1 :]
        )
        second_pid = int(retry_retained_get[3])
        assert second_pid == busy_pids[0]
        assert second_pid != first_pid
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, second_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, first_pid) == 0
        assert _backend_pid_advisory_lock_count(cold_start_runtime, second_pid) == 1
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            False,
            None,
        )
        assert _durable_physical_snapshot(cold_start_runtime) == after_first
        assert all(
            not (
                type(event[4]) is tuple
                and event[4]
                and event[4][0] in {"insert", "update", "delete"}
            )
            for event in retry_events
            if event[0] == "execute"
        )
        second_borrower = asyncio.create_task(second_pool.getconn(timeout=30.0))
        await _wait_for_g11_pool_waiter(second_pool, second_borrower)

        second_origin.release.set()
        ready = await asyncio.wait_for(second_task, timeout=5.0)
        assert ready.status is ColdStartRunStatus.READY
        assert ready.plan is not None
        assert ready.plan.state is ColdStartPlanState.READY
        assert ready.plan.page_count == 2
        assert ready.plan.item_count == 2
        assert ready.plan.boundary_cursor == "opaque+G11-preview-2/%3D"
        assert ready.pages_committed == ready.changes_observed == 1
        assert ready.safe_code is None
        assert first_origin.calls == [(8, "Inbox", None, 100)]
        assert second_origin.calls == [(8, "Inbox", "opaque+G11-preview-1/%3D", 100)]
        assert first_ordinary.calls == second_ordinary.calls == []
        assert second_permit.acquire_count == second_permit.release_count == 4
        assert second_permit.active is False
        second_borrowed_connection = await asyncio.wait_for(
            second_borrower,
            timeout=5.0,
        )
        assert second_borrowed_connection.info.backend_pid == second_pid
        assert second_borrowed_connection.info.transaction_status is (
            TransactionStatus.IDLE
        )
        await second_pool.putconn(second_borrowed_connection)
        second_borrowed_returned = True
        assert second_pool.get_stats()["requests_waiting"] == 0
        assert second_pool.get_stats()["pool_available"] == 1

        final_snapshot = _durable_physical_snapshot(cold_start_runtime)
        assert len(final_snapshot["plan"]) == 1
        final_plan = final_snapshot["plan"][0]["row_data"]
        assert final_plan["state"] == "ready"
        assert final_plan["version"] == 2
        assert final_plan["page_count"] == final_plan["preview_cursor_version"] == 2
        assert final_plan["item_count"] == 2
        assert final_plan["preview_cursor"] == "opaque+G11-preview-2/%3D"
        assert final_plan["boundary_cursor"] == "opaque+G11-preview-2/%3D"
        assert final_plan["boundary_cursor_version"] == 2
        assert type(final_plan["rolling_hash"]) is str
        assert type(final_plan["plan_hash"]) is str
        assert final_snapshot["cursor"] == held_snapshot["cursor"]
        assert final_snapshot["inbox"] == []
        assert final_snapshot["receipts"] == held_snapshot["receipts"]
        assert final_snapshot["audits"] == held_snapshot["audits"]

        assert _backend_pid_has_no_open_transaction(cold_start_runtime, first_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, second_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, first_pid) == 0
        assert _backend_pid_advisory_lock_count(cold_start_runtime, second_pid) == 0
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            True,
            True,
        )
        assert first_pool.get_stats()["requests_waiting"] == 0
        assert (
            first_pool.get_stats()["pool_size"]
            == (first_pool.get_stats()["pool_available"])
            == 1
        )
        assert second_pool.get_stats()["requests_waiting"] == 0
        assert (
            second_pool.get_stats()["pool_size"]
            == (second_pool.get_stats()["pool_available"])
            == 1
        )
        assert (
            _application_session_count(
                cold_start_runtime,
                first_application_name,
            )
            == 1
        )
        assert (
            _application_session_count(
                cold_start_runtime,
                second_application_name,
            )
            == 1
        )
    finally:
        first_origin.release.set()
        second_origin.release.set()
        for task in (first_task, second_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
        await _cleanup_g11_borrower(
            first_pool,
            first_borrower,
            first_borrowed_connection,
            returned=first_borrowed_returned,
        )
        await _cleanup_g11_borrower(
            second_pool,
            second_borrower,
            second_borrowed_connection,
            returned=second_borrowed_returned,
        )
        await second_pool.close()
        await first_pool.close()
        assert (
            _application_session_count(cold_start_runtime, first_application_name) == 0
        )
        assert (
            _application_session_count(cold_start_runtime, second_application_name) == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_g12_cold_start_different_folders_run_http_concurrently_without_cross_folder_mutation(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    cold_start_runtime.schema.maintenance_execute(
        "INSERT INTO sync_cursors ("
        "account_id, folder_key, cursor, status, blocked_reason_code"
        ") VALUES "
        "(8, 'INBOX', NULL, 'cold_start_pending', "
        "'sync.cold_start_required'), "
        "(8, 'PROJECTS', NULL, 'cold_start_pending', "
        "'sync.cold_start_required')"
    )
    inbox_application_name = _process_application_name(
        cold_start_runtime,
        "g12-inbox-preview",
    )
    projects_application_name = _process_application_name(
        cold_start_runtime,
        "g12-projects-preview",
    )
    inbox_pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=inbox_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    projects_pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=projects_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    inbox_tracking_pool = _G11TrackingPool(inbox_pool, roles=("retained",))
    projects_tracking_pool = _G11TrackingPool(
        projects_pool,
        roles=("retained",),
    )
    inbox_permit = _PermitProvider()
    projects_permit = _PermitProvider()
    inbox_preview_cursor = "opaque+G12-inbox-preview-1/%3D"
    projects_preview_cursor = "opaque+G12-projects-preview-1/%3D"
    inbox_origin = _G11BlockingOrigin(
        _batch(
            inbox_preview_cursor,
            includes_last=False,
            changes=(_create_change("historical-g12-inbox"),),
        )
    )
    projects_origin = _G11BlockingOrigin(
        _batch(
            projects_preview_cursor,
            includes_last=False,
            changes=(_create_change("historical-g12-projects"),),
        )
    )
    policy_snapshot = _SnapshotProvider(_multi_folder_snapshot())
    inbox_ordinary = _OrdinaryPageClient([])
    projects_ordinary = _OrdinaryPageClient([])
    inbox_service = _service(
        cold_start_runtime,
        origin=inbox_origin,  # type: ignore[arg-type]
        ordinary=inbox_ordinary,
        pool=inbox_tracking_pool,  # type: ignore[arg-type]
        permit=inbox_permit,
        snapshot_provider=policy_snapshot,
        preview_max_pages=1,
    )
    projects_service = _service(
        cold_start_runtime,
        origin=projects_origin,  # type: ignore[arg-type]
        ordinary=projects_ordinary,
        pool=projects_tracking_pool,  # type: ignore[arg-type]
        permit=projects_permit,
        snapshot_provider=policy_snapshot,
        preview_max_pages=1,
    )
    inbox_task: asyncio.Task[Any] | None = None
    projects_task: asyncio.Task[Any] | None = None
    await inbox_pool.open()
    try:
        await projects_pool.open()
    except BaseException:
        await inbox_pool.close()
        raise
    try:
        inbox_task = asyncio.create_task(
            inbox_service.preview(
                8,
                "INBOX",
                actor="g12-inbox-preview",
                reason="prove different folders do not serialize cold-start HTTP",
                idempotency_key="g12-inbox-preview-key",
            )
        )
        projects_task = asyncio.create_task(
            projects_service.preview(
                8,
                "PROJECTS",
                actor="g12-projects-preview",
                reason="prove different folders do not serialize cold-start HTTP",
                idempotency_key="g12-projects-preview-key",
            )
        )
        await asyncio.wait_for(
            asyncio.gather(
                inbox_origin.entered.wait(),
                projects_origin.entered.wait(),
            ),
            timeout=5.0,
        )

        assert inbox_task.done() is False
        assert projects_task.done() is False
        assert inbox_origin.calls == [(8, "Inbox", None, 100)]
        assert projects_origin.calls == [(8, "Projects", None, 100)]
        assert inbox_ordinary.calls == projects_ordinary.calls == []
        assert inbox_permit.acquire_count == projects_permit.acquire_count == 1
        assert inbox_permit.release_count == projects_permit.release_count == 0
        assert inbox_permit.active is projects_permit.active is True

        inbox_lock_keys = sync_advisory_lock_keys(8, "INBOX")
        projects_lock_keys = sync_advisory_lock_keys(8, "PROJECTS")
        assert inbox_lock_keys != projects_lock_keys
        held_pids: list[int] = []
        for tracking_pool, expected_keys in (
            (inbox_tracking_pool, inbox_lock_keys),
            (projects_tracking_pool, projects_lock_keys),
        ):
            assert len(tracking_pool.connections) == 1
            tracked_connection = tracking_pool.connections[0]
            backend_pid = tracked_connection._backend_pid
            held_pids.append(backend_pid)
            lock_executes = [
                event
                for event in tracking_pool.events
                if event[0] == "execute" and event[4] == _G11_TRY_LOCK_TOKENS
            ]
            assert len(lock_executes) == 1
            assert lock_executes[0][1:4] == (
                "retained",
                0,
                backend_pid,
            )
            assert lock_executes[0][5:] == (
                expected_keys,
                TransactionStatus.IDLE,
            )
            lock_results = [
                event
                for event in tracking_pool.events
                if event[0] == "lock.result" and event[4] == "acquired"
            ]
            assert lock_results == [
                (
                    "lock.result",
                    "retained",
                    0,
                    backend_pid,
                    "acquired",
                    True,
                    TransactionStatus.IDLE,
                )
            ]
            assert not any(
                event[0] in {"putconn.start", "putconn.done"}
                for event in tracking_pool.events
            )
            assert _backend_pid_has_no_open_transaction(
                cold_start_runtime,
                backend_pid,
            )
            assert (
                _backend_pid_advisory_lock_count(
                    cold_start_runtime,
                    backend_pid,
                )
                == 1
            )
        assert len(set(held_pids)) == 2

        with psycopg.connect(
            cold_start_runtime.schema.admin_dsn,
            autocommit=True,
            row_factory=dict_row,
        ) as probe_connection:
            probe_results = []
            for lock_keys in (inbox_lock_keys, projects_lock_keys):
                row = probe_connection.execute(
                    "SELECT pg_catalog.pg_try_advisory_lock(%s, %s) AS acquired",
                    lock_keys,
                ).fetchone()
                assert row is not None
                probe_results.append(row["acquired"])
        assert probe_results == [False, False]

        assert (
            _application_session_count(
                cold_start_runtime,
                inbox_application_name,
            )
            == 1
        )
        assert (
            _application_session_count(
                cold_start_runtime,
                projects_application_name,
            )
            == 1
        )
        for pool in (inbox_pool, projects_pool):
            stats = pool.get_stats()
            assert stats["requests_waiting"] == 0
            assert stats["pool_size"] == 1
            assert stats["pool_available"] == 0

        held_snapshot = _durable_physical_snapshot(cold_start_runtime)
        held_plans = {
            row["row_data"]["folder_key"]: row["row_data"]
            for row in held_snapshot["plan"]
        }
        assert set(held_plans) == {"INBOX", "PROJECTS"}
        for folder in ("INBOX", "PROJECTS"):
            plan = held_plans[folder]
            assert plan["state"] == "previewing"
            assert plan["version"] == 0
            assert plan["page_count"] == plan["item_count"] == 0
            assert plan["preview_cursor"] is None
            assert plan["preview_cursor_version"] == 0
            assert plan["boundary_cursor"] is None
            assert plan["boundary_cursor_version"] is None
        held_cursors = {
            row["row_data"]["folder_key"]: row["row_data"]
            for row in held_snapshot["cursor"]
        }
        assert set(held_cursors) == {"INBOX", "PROJECTS"}
        for folder in ("INBOX", "PROJECTS"):
            cursor = held_cursors[folder]
            assert cursor["cursor"] is None
            assert cursor["status"] == "cold_start_pending"
            assert cursor["version"] == 0
            assert cursor["cold_start_plan_id"] is None
            assert cursor["cold_start_plan_state"] is None
        assert held_snapshot["inbox"] == []
        assert len(held_snapshot["receipts"]) == 2
        receipt_inputs = {
            "INBOX": (
                "g12-inbox-preview",
                "g12-inbox-preview-key",
            ),
            "PROJECTS": (
                "g12-projects-preview",
                "g12-projects-preview-key",
            ),
        }
        expected_receipts = {}
        for folder, (actor, idempotency_key) in receipt_inputs.items():
            payload_hash, idempotency_hash, result_hash = (
                _independent_preview_command_hashes(
                    plan=held_plans[folder],
                    actor=actor,
                    reason=("prove different folders do not serialize cold-start HTTP"),
                    idempotency_key=idempotency_key,
                )
            )
            expected_receipts[idempotency_hash] = {
                "idempotency_key_hash": idempotency_hash,
                "account_id": 8,
                "command_name": "cold_start.preview",
                "canonical_payload_hash": payload_hash,
                "outcome": "succeeded",
                "result_type": "sync_cold_start_plan",
                "result_id": held_plans[folder]["plan_id"],
                "result_hash": result_hash,
                "authority_epoch": held_plans[folder]["fencing_token"],
            }
        actual_receipts = {}
        for item in held_snapshot["receipts"]:
            row = item["row_data"]
            identity = row["idempotency_key_hash"]
            assert identity not in actual_receipts
            actual_receipts[identity] = {
                "idempotency_key_hash": identity,
                "account_id": row["account_id"],
                "command_name": row["command_name"],
                "canonical_payload_hash": row["canonical_payload_hash"],
                "outcome": row["outcome"],
                "result_type": row["result_type"],
                "result_id": row["result_id"],
                "result_hash": row["result_hash"],
                "authority_epoch": row["authority_epoch"],
            }
        assert actual_receipts == expected_receipts
        assert [
            (row["row_data"]["action"], row["row_data"]["result"])
            for row in held_snapshot["audits"]
        ] == [("pipeline.bootstrap", "succeeded")]

        inbox_origin.release.set()
        projects_origin.release.set()
        inbox_result, projects_result = await asyncio.wait_for(
            asyncio.gather(inbox_task, projects_task),
            timeout=5.0,
        )

        results = {
            result.plan.canonical_folder: result
            for result in (inbox_result, projects_result)
            if result.plan is not None
        }
        assert set(results) == {"INBOX", "PROJECTS"}
        for folder in ("INBOX", "PROJECTS"):
            result = results[folder]
            assert result.status is ColdStartRunStatus.BUDGET_EXHAUSTED
            assert result.plan is not None
            assert result.plan.state is ColdStartPlanState.PREVIEWING
            assert result.plan.page_count == result.plan.item_count == 1
            assert result.plan.boundary_cursor is None
            assert result.pages_committed == result.changes_observed == 1
            assert result.safe_code == "cold_start.budget_exhausted"
        assert inbox_permit.acquire_count == inbox_permit.release_count == 1
        assert projects_permit.acquire_count == projects_permit.release_count == 1
        assert inbox_permit.active is projects_permit.active is False

        final_snapshot = _durable_physical_snapshot(cold_start_runtime)
        final_plans = {
            row["row_data"]["folder_key"]: row["row_data"]
            for row in final_snapshot["plan"]
        }
        assert set(final_plans) == {"INBOX", "PROJECTS"}
        expected_preview_pages = {
            "INBOX": (
                inbox_preview_cursor,
                "historical-g12-inbox",
            ),
            "PROJECTS": (
                projects_preview_cursor,
                "historical-g12-projects",
            ),
        }
        for folder, (
            expected_cursor,
            external_email_id,
        ) in expected_preview_pages.items():
            plan = final_plans[folder]
            sample_hash, rolling_hash = _independent_first_preview_page_hashes(
                account_id=8,
                cursor=expected_cursor,
                external_email_id=external_email_id,
            )
            assert plan["state"] == "previewing"
            assert plan["version"] == 1
            assert plan["page_count"] == plan["item_count"] == 1
            assert plan["preview_cursor"] == expected_cursor
            assert plan["preview_cursor_version"] == 1
            assert plan["boundary_cursor"] is None
            assert plan["boundary_cursor_version"] is None
            assert plan["rolling_hash"] == rolling_hash
            assert plan["redacted_samples"] == [
                {
                    "kind": "create",
                    "external_email_id_hash": sample_hash,
                }
            ]
            assert plan["plan_hash"] is None
        assert final_snapshot["cursor"] == held_snapshot["cursor"]
        assert final_snapshot["inbox"] == held_snapshot["inbox"] == []
        assert final_snapshot["receipts"] == held_snapshot["receipts"]
        assert final_snapshot["audits"] == held_snapshot["audits"]

        for tracking_pool, backend_pid, lock_keys in (
            (inbox_tracking_pool, held_pids[0], inbox_lock_keys),
            (projects_tracking_pool, held_pids[1], projects_lock_keys),
        ):
            unlock_executes = [
                event
                for event in tracking_pool.events
                if event[0] == "execute" and event[4] == _G11_UNLOCK_TOKENS
            ]
            assert len(unlock_executes) == 1
            assert unlock_executes[0][1:4] == ("retained", 0, backend_pid)
            assert unlock_executes[0][5:] == (
                lock_keys,
                TransactionStatus.IDLE,
            )
            released_results = [
                event
                for event in tracking_pool.events
                if event[0] == "lock.result" and event[4] == "released"
            ]
            assert released_results == [
                (
                    "lock.result",
                    "retained",
                    0,
                    backend_pid,
                    "released",
                    True,
                    TransactionStatus.IDLE,
                )
            ]
            putconn_done = [
                event for event in tracking_pool.events if event[0] == "putconn.done"
            ]
            assert putconn_done == [
                (
                    "putconn.done",
                    "retained",
                    0,
                    backend_pid,
                    False,
                    TransactionStatus.IDLE,
                )
            ]
            assert _backend_pid_has_no_open_transaction(
                cold_start_runtime,
                backend_pid,
            )
            assert (
                _backend_pid_advisory_lock_count(
                    cold_start_runtime,
                    backend_pid,
                )
                == 0
            )
            assert _probe_session_advisory_lock(
                cold_start_runtime,
                lock_keys,
            ) == (True, True)
        for pool in (inbox_pool, projects_pool):
            stats = pool.get_stats()
            assert stats["requests_waiting"] == 0
            assert stats["pool_size"] == stats["pool_available"] == 1
    finally:
        inbox_origin.release.set()
        projects_origin.release.set()
        for task in (inbox_task, projects_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
        try:
            await projects_pool.close()
        finally:
            await inbox_pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                inbox_application_name,
            )
            == 0
        )
        assert (
            _application_session_count(
                cold_start_runtime,
                projects_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preview_http_in_flight_external_cancel_cleans_session_exactly(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    state = _G9CancellationState("preview")
    origin = _G9BlockingColdStartOrigin(state)
    ordinary = _OrdinaryPageClient([])
    permit = _G9TrackingPermitProvider(state)
    pool_application_name = _process_application_name(
        cold_start_runtime,
        "g9-preview-cancel",
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    guarded_pool = _G9CancellationPool(pool, state)
    task: asyncio.Task[Any] | None = None
    try:
        service = _service(
            cold_start_runtime,
            origin=origin,  # type: ignore[arg-type]
            ordinary=ordinary,
            pool=guarded_pool,  # type: ignore[arg-type]
            permit=permit,
            preview_max_pages=1,
        )
        task = asyncio.create_task(
            service.preview(
                8,
                "INBOX",
                actor="g9-preview-cancel",
                reason="prove HTTP cancellation cleanup",
                idempotency_key="g9-preview-cancel-key",
            )
        )
        await asyncio.wait_for(origin.entered.wait(), timeout=5.0)

        assert origin.calls == [(8, "Inbox", None, 100)]
        assert ordinary.calls == []
        assert state.phase == "http_in_flight"
        assert state.checked_out_roles == ["retained"]
        assert len(state.checked_out_pids) == 1
        retained_pid = state.checked_out_pids[0]
        assert state.ordering_events == [
            ("permit.acquire",),
            ("retained.getconn", retained_pid),
        ]
        assert state.retained_pid == retained_pid
        retained = guarded_pool.checked_out_connections[0]
        assert retained._backend_pid == retained_pid
        assert retained._connection.info.transaction_status is TransactionStatus.IDLE
        assert _backend_pid_exists(cold_start_runtime, retained_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retained_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retained_pid) == 1
        lock_keys = sync_advisory_lock_keys(8, "INBOX")
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            False,
            None,
        )
        assert permit.acquire_count == 1
        assert permit.release_count == 0
        assert permit.active is True
        assert state.target_dml_attempts == []
        assert state.cleanup_events == []
        assert state.returned_pids == []

        barrier_snapshot = _durable_physical_snapshot(cold_start_runtime)
        assert len(barrier_snapshot["plan"]) == 1
        barrier_plan = barrier_snapshot["plan"][0]["row_data"]
        assert barrier_plan["state"] == "previewing"
        assert barrier_plan["version"] == 0
        assert barrier_plan["page_count"] == 0
        assert len(barrier_snapshot["cursor"]) == 1
        barrier_cursor = barrier_snapshot["cursor"][0]["row_data"]
        assert barrier_cursor["status"] == "cold_start_pending"
        assert barrier_cursor["version"] == 0
        assert barrier_cursor["cursor"] is None
        assert barrier_snapshot["inbox"] == []
        assert [
            (item["row_data"]["action"], item["row_data"]["result"])
            for item in barrier_snapshot["audits"]
        ] == [("pipeline.bootstrap", "succeeded")]
        assert [
            item["row_data"]["command_name"] for item in barrier_snapshot["receipts"]
        ] == ["cold_start.preview"]

        assert task.cancel() is True
        with pytest.raises(asyncio.CancelledError) as caught:
            await asyncio.wait_for(task, timeout=5.0)

        assert caught.value is state.cancelled_error
        assert type(caught.value) is asyncio.CancelledError
        assert task.cancelled() is True
        assert state.phase == "cancelled"
        assert state.cleanup_events == [
            ("unlock", retained_pid),
            ("putconn", retained_pid, False, "idle"),
            ("permit.release",),
        ]
        assert state.checked_out_pids == [retained_pid]
        assert state.returned_pids == [retained_pid]
        assert state.returned_closed == [False]
        assert permit.acquire_count == permit.release_count == 1
        assert permit.active is False
        assert state.target_dml_attempts == []
        assert origin.calls == [(8, "Inbox", None, 100)]
        assert ordinary.calls == []
        assert _durable_physical_snapshot(cold_start_runtime) == barrier_snapshot
        assert _backend_pid_exists(cold_start_runtime, retained_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retained_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retained_pid) == 0
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            True,
            True,
        )

        async with pool.connection() as connection:
            assert connection.info.backend_pid == retained_pid
            assert connection.info.transaction_status is TransactionStatus.IDLE
            cursor = await connection.execute("SELECT 1 AS healthy")
            assert await cursor.fetchone() == {"healthy": 1}
            assert connection.info.transaction_status is TransactionStatus.IDLE
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retained_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retained_pid) == 0
        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"]
    finally:
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_http_in_flight_external_cancel_cleans_session_exactly(
    cold_start_runtime: _ColdStartRuntime,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        "opaque+Boundary-G9-apply/%3D",
        includes_last=True,
        changes=(_create_change("historical-g9-apply"),),
    )
    setup_origin = _ColdStartOrigin([boundary])
    setup_service = _service(
        cold_start_runtime,
        origin=setup_origin,
        ordinary=_OrdinaryPageClient([]),
        preview_max_pages=1,
    )
    ready = await setup_service.preview(
        8,
        "INBOX",
        actor="g9-apply-preview",
        reason="prepare apply HTTP cancellation",
        idempotency_key="g9-apply-preview-key",
    )
    assert ready.status is ColdStartRunStatus.READY
    assert ready.plan is not None
    approved = await setup_service.approve(
        ready.plan.plan_id,
        actor="g9-apply-approve",
        reason="approve apply HTTP cancellation",
        idempotency_key="g9-apply-approve-key",
    )
    assert approved.status is ColdStartRunStatus.APPROVED
    assert setup_origin.calls == [(8, "Inbox", None, 100)]

    state = _G9CancellationState("apply")
    ordinary = _G9BlockingOrdinaryPageClient(state)
    apply_origin = _ColdStartOrigin([])
    permit = _G9TrackingPermitProvider(state)
    pool_application_name = _process_application_name(
        cold_start_runtime,
        "g9-apply-cancel",
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    guarded_pool = _G9CancellationPool(pool, state)
    task: asyncio.Task[Any] | None = None
    try:
        service = _service(
            cold_start_runtime,
            origin=apply_origin,
            ordinary=ordinary,  # type: ignore[arg-type]
            pool=guarded_pool,  # type: ignore[arg-type]
            permit=permit,
            apply_max_pages=1,
        )
        task = asyncio.create_task(service.apply(ready.plan.plan_id))
        await asyncio.wait_for(ordinary.entered.wait(), timeout=5.0)

        assert apply_origin.calls == []
        assert ordinary.calls == [(8, "Inbox", boundary.cursor, 100)]
        assert state.phase == "http_in_flight"
        assert state.checked_out_roles == ["locator", "retained"]
        assert len(state.checked_out_pids) == 2
        retained_pid = state.checked_out_pids[0]
        assert state.checked_out_pids == [retained_pid, retained_pid]
        assert state.ordering_events == [
            ("locator.getconn", retained_pid),
            ("locator.putconn", retained_pid),
            ("permit.acquire",),
            ("retained.getconn", retained_pid),
        ]
        assert state.retained_pid == retained_pid
        locator, retained = guarded_pool.checked_out_connections
        assert locator._connection is retained._connection
        assert retained._connection.info.transaction_status is TransactionStatus.IDLE
        assert state.returned_pids == [retained_pid]
        assert state.returned_closed == [False]
        assert _backend_pid_exists(cold_start_runtime, retained_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retained_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retained_pid) == 1
        lock_keys = sync_advisory_lock_keys(8, "INBOX")
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            False,
            None,
        )
        assert permit.acquire_count == 1
        assert permit.release_count == 0
        assert permit.active is True
        assert state.target_dml_attempts == []
        assert state.cleanup_events == []

        barrier_snapshot = _durable_physical_snapshot(cold_start_runtime)
        assert len(barrier_snapshot["plan"]) == 1
        barrier_plan = barrier_snapshot["plan"][0]["row_data"]
        assert barrier_plan["state"] == "approved"
        assert barrier_plan["version"] == 2
        assert barrier_plan["apply_cursor"] is None
        assert len(barrier_snapshot["cursor"]) == 1
        barrier_cursor = barrier_snapshot["cursor"][0]["row_data"]
        assert barrier_cursor["status"] == "cold_start_pending"
        assert barrier_cursor["version"] == 0
        assert barrier_cursor["cursor"] is None
        assert barrier_snapshot["inbox"] == []
        assert sorted(
            item["row_data"]["command_name"] for item in barrier_snapshot["receipts"]
        ) == ["cold_start.approve", "cold_start.preview"]
        assert sorted(
            (item["row_data"]["action"], item["row_data"]["result"])
            for item in barrier_snapshot["audits"]
        ) == [
            ("cold_start.approve", "approved"),
            ("pipeline.bootstrap", "succeeded"),
        ]

        assert task.cancel() is True
        with pytest.raises(asyncio.CancelledError) as caught:
            await asyncio.wait_for(task, timeout=5.0)

        assert caught.value is state.cancelled_error
        assert type(caught.value) is asyncio.CancelledError
        assert task.cancelled() is True
        assert state.phase == "cancelled"
        assert state.cleanup_events == [
            ("unlock", retained_pid),
            ("putconn", retained_pid, False, "idle"),
            ("permit.release",),
        ]
        assert state.checked_out_pids == [retained_pid, retained_pid]
        assert state.returned_pids == [retained_pid, retained_pid]
        assert state.returned_closed == [False, False]
        assert permit.acquire_count == permit.release_count == 1
        assert permit.active is False
        assert state.target_dml_attempts == []
        assert apply_origin.calls == []
        assert ordinary.calls == [(8, "Inbox", boundary.cursor, 100)]
        assert _durable_physical_snapshot(cold_start_runtime) == barrier_snapshot
        assert _backend_pid_exists(cold_start_runtime, retained_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retained_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retained_pid) == 0
        assert _probe_session_advisory_lock(cold_start_runtime, lock_keys) == (
            True,
            True,
        )

        async with pool.connection() as connection:
            assert connection.info.backend_pid == retained_pid
            assert connection.info.transaction_status is TransactionStatus.IDLE
            cursor = await connection.execute("SELECT 1 AS healthy")
            assert await cursor.fetchone() == {"healthy": 1}
            assert connection.info.transaction_status is TransactionStatus.IDLE
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, retained_pid)
        assert _backend_pid_advisory_lock_count(cold_start_runtime, retained_pid) == 0
        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"]
    finally:
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_mode",
    ["commit_then_raise", "rollback_then_raise"],
)
async def test_terminal_apply_ack_loss_recovers_exact_database_outcome(
    cold_start_runtime: _ColdStartRuntime,
    fault_mode: str,
) -> None:
    _seed_cold_start_cursor(cold_start_runtime, "cold_start_pending")
    boundary = _batch(
        f"opaque+Boundary-G7-{fault_mode}/%3D",
        includes_last=True,
        changes=(_create_change(f"historical-g7-{fault_mode}"),),
    )
    setup_origin = _ColdStartOrigin([boundary])
    setup_service = _service(
        cold_start_runtime,
        origin=setup_origin,
        ordinary=_OrdinaryPageClient([]),
        preview_max_pages=1,
    )
    ready = await setup_service.preview(
        8,
        "INBOX",
        actor=f"g7-preview-{fault_mode}",
        reason="prepare terminal apply ACK-loss recovery",
        idempotency_key=f"g7-preview-{fault_mode}",
    )
    assert ready.status is ColdStartRunStatus.READY
    assert ready.plan is not None
    approved = await setup_service.approve(
        ready.plan.plan_id,
        actor=f"g7-approve-{fault_mode}",
        reason="approve terminal apply ACK-loss recovery",
        idempotency_key=f"g7-approve-{fault_mode}",
    )
    assert approved.status is ColdStartRunStatus.APPROVED
    assert setup_origin.calls == [(8, "Inbox", None, 100)]

    terminal = _batch(
        f"opaque+Terminal-G7-{fault_mode}/%3D",
        includes_last=True,
        changes=(_create_change(f"ordinary-g7-{fault_mode}"),),
    )
    state = _ApplyAckLossFaultState(fault_mode)
    state.bind_locator_plan_id(ready.plan.plan_id)
    ordinary = _BlockingOrdinaryPageClient(terminal, state)
    apply_origin = _ColdStartOrigin([])
    permit = _PermitProvider()
    process_name = f"g7-{fault_mode.replace('_then_raise', '')}"
    pool_application_name = _process_application_name(
        cold_start_runtime,
        process_name,
    )
    pool = AsyncConnectionPool(
        conninfo=make_conninfo(
            cold_start_runtime.schema.maintenance_dsn,
            application_name=pool_application_name,
        ),
        min_size=1,
        max_size=1,
        open=False,
        close_returns=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    fault_pool = _ApplyAckLossPool(pool, state)
    task: asyncio.Task[Any] | None = None
    result: Any = None
    pre: dict[str, list[dict[str, Any]]] | None = None
    visible: dict[str, list[dict[str, Any]]] | None = None
    try:
        apply_service = _service(
            cold_start_runtime,
            origin=apply_origin,
            ordinary=ordinary,  # type: ignore[arg-type]
            pool=fault_pool,  # type: ignore[arg-type]
            permit=permit,
            apply_max_pages=1,
        )
        task = asyncio.create_task(apply_service.apply(ready.plan.plan_id))
        await asyncio.wait_for(ordinary.entered.wait(), timeout=5.0)

        assert apply_origin.calls == []
        assert ordinary.calls == [(8, "Inbox", boundary.cursor, 100)]
        assert state.http_phase == "pre_http"
        assert state.http_transition_count == 0
        assert state.locator_calls == 1
        assert not state.post_http_entered.is_set()
        assert fault_pool.checked_out_pids == [
            fault_pool.checked_out_pids[0],
            fault_pool.checked_out_pids[0],
        ]
        old_pid = fault_pool.checked_out_pids[0]
        assert len(fault_pool.checked_out_connections) == 2
        locator_connection, origin_connection = fault_pool.checked_out_connections
        assert locator_connection._phase == "locator"
        assert origin_connection._phase == "origin"
        assert (
            locator_connection._backend_pid == origin_connection._backend_pid == old_pid
        )
        assert locator_connection._connection is origin_connection._connection
        assert locator_connection._active_transaction is None
        assert origin_connection._active_transaction is None
        assert (
            origin_connection._connection.info.transaction_status
            is TransactionStatus.IDLE
        )
        assert fault_pool.returned_pids == [old_pid]
        assert fault_pool.returned_closed == [False]
        assert _backend_pid_exists(cold_start_runtime, old_pid)
        assert _backend_pid_has_no_open_transaction(cold_start_runtime, old_pid)
        assert permit.acquire_count == 1
        assert permit.release_count == 0
        assert permit.active is True
        assert state.observations == []

        pre = _durable_physical_snapshot(cold_start_runtime)
        assert len(pre["plan"]) == 1
        pre_plan = pre["plan"][0]["row_data"]
        assert pre_plan["state"] == "approved"
        assert pre_plan["version"] == 2
        assert pre_plan["preview_cursor"] == boundary.cursor
        assert pre_plan["boundary_cursor"] == boundary.cursor
        assert pre_plan["apply_cursor"] is None
        assert pre_plan["apply_cursor_version"] is None
        assert len(pre["cursor"]) == 1
        assert pre["cursor"][0]["row_data"]["status"] == "cold_start_pending"
        assert pre["cursor"][0]["row_data"]["version"] == 0
        assert pre["cursor"][0]["row_data"]["cursor"] is None
        assert pre["inbox"] == []
        assert sorted(item["row_data"]["command_name"] for item in pre["receipts"]) == [
            "cold_start.approve",
            "cold_start.preview",
        ]
        audits_before_apply = pre["audits"]
        assert sorted(
            (item["row_data"]["action"], item["row_data"]["result"])
            for item in audits_before_apply
        ) == [
            ("cold_start.approve", "approved"),
            ("pipeline.bootstrap", "succeeded"),
        ]
        assert type(pre_plan["plan_hash"]) is str
        (
            dedupe_key,
            apply_payload_hash,
            receipt_idempotency_hash,
            batch_result_hash,
        ) = _independent_apply_ack_loss_hashes(
            account_id=8,
            folder_key="INBOX",
            plan_id=ready.plan.plan_id,
            boundary_cursor=boundary.cursor,
            terminal_cursor=terminal.cursor,
            external_email_id=f"ordinary-g7-{fault_mode}",
            source_version="version-1",
        )
        state.bind_expected_contract(
            _ApplyAckLossExpectedContract(
                account_id=8,
                folder_key="INBOX",
                plan_id=ready.plan.plan_id,
                boundary_cursor=boundary.cursor,
                terminal_cursor=terminal.cursor,
                external_email_id=f"ordinary-g7-{fault_mode}",
                source_version="version-1",
                pipeline_name=pre_plan["pipeline_name"],
                generation=pre_plan["generation"],
                fencing_token=pre_plan["fencing_token"],
                plan_hash=pre_plan["plan_hash"],
                contract_fingerprint=pre_plan["contract_fingerprint"],
                config_hash=pre_plan["folder_scope_config_hash"],
                dedupe_key=dedupe_key,
                apply_payload_hash=apply_payload_hash,
                receipt_idempotency_hash=receipt_idempotency_hash,
                batch_result_hash=batch_result_hash,
            )
        )

        ordinary.release.set()
        await asyncio.wait_for(state.post_http_entered.wait(), timeout=5.0)
        assert state.http_phase == "post_http"
        assert state.http_transition_count == 1
        await asyncio.wait_for(state.outcome_reached.wait(), timeout=5.0)

        origin_observations = [
            item for item in state.observations if item.phase == "origin"
        ]
        assert tuple(item.marker for item in origin_observations) == _APPLY_DML_ORDER
        assert len({item.transaction_id for item in origin_observations}) == 1
        assert all(
            item.assigned_transaction_id == item.transaction_id
            and item.rowcount == 1
            and item.backend_pid == old_pid
            and item.transaction_status is TransactionStatus.INTRANS
            and item.http_phase == "post_http"
            for item in origin_observations
        )
        assert [item.armed for item in origin_observations] == [
            False,
            False,
            False,
            True,
        ]
        old_transaction_id = origin_observations[0].transaction_id
        assert state.exit_phases == [(fault_mode, old_pid, TransactionStatus.IDLE)]

        visible = _durable_physical_snapshot(cold_start_runtime)
        if fault_mode == "commit_then_raise":
            assert visible["plan"] != pre["plan"]
            assert visible["plan"][0]["row_data"]["state"] == "completed"
            assert visible["cursor"] != pre["cursor"]
            assert visible["cursor"][0]["row_data"]["status"] == "active"
            assert len(visible["inbox"]) == 1
            assert len(visible["receipts"]) == 3
        else:
            assert visible == pre
        assert _backend_pid_exists(cold_start_runtime, old_pid)

        state.outcome_release.set()
        result = await asyncio.wait_for(task, timeout=10.0)

        assert result.status is ColdStartRunStatus.COMPLETED
        assert result.plan is not None
        assert result.pages_committed == 1
        assert result.changes_observed == 1
        assert result.safe_code is None
        assert apply_origin.calls == []
        assert ordinary.calls == [(8, "Inbox", boundary.cursor, 100)]
        assert ordinary.exhausted is True
        assert permit.acquire_count == 2
        assert permit.release_count == 2
        assert permit.active is False

        assert len(fault_pool.checked_out_pids) == 3
        assert fault_pool.checked_out_pids[:2] == [old_pid, old_pid]
        recovery_pid = fault_pool.checked_out_pids[2]
        assert recovery_pid != old_pid
        assert fault_pool.returned_pids == [old_pid, old_pid, recovery_pid]
        assert fault_pool.returned_closed == [False, True, False]
        await _wait_until_backend_pid_disappears(cold_start_runtime, old_pid)
        assert not _backend_pid_exists(cold_start_runtime, old_pid)
        assert _backend_pid_exists(cold_start_runtime, recovery_pid)

        assert state.fault_budget == 0
        assert state.http_phase == "post_http"
        assert state.http_transition_count == 1
        assert state.raised_errors == [state.error]
        assert type(state.error) is RuntimeError
        recovery_observations = [
            item for item in state.observations if item.phase == "recovery"
        ]
        assert state.recovery_sql
        if fault_mode == "commit_then_raise":
            assert recovery_observations == []
            assert all(marker == "read_control" for marker, _ in state.recovery_sql)
        else:
            assert tuple(item.marker for item in recovery_observations) == (
                _APPLY_DML_ORDER
            )
            assert len({item.transaction_id for item in recovery_observations}) == 1
            assert recovery_observations[0].transaction_id != old_transaction_id
            assert all(
                item.assigned_transaction_id == item.transaction_id
                and item.rowcount == 1
                and item.backend_pid == recovery_pid
                and item.transaction_status is TransactionStatus.INTRANS
                and item.http_phase == "post_http"
                and item.armed is False
                for item in recovery_observations
            )
            assert [
                marker for marker, _ in state.recovery_sql if marker != "read_control"
            ] == list(_APPLY_DML_ORDER)
            assert [
                item.stable_parameter_projection for item in recovery_observations
            ] == [item.stable_parameter_projection for item in origin_observations]

        final = _durable_physical_snapshot(cold_start_runtime)
        assert pre is not None and visible is not None
        if fault_mode == "commit_then_raise":
            assert final == visible
        else:
            assert visible == pre
            assert final != pre
        assert final["audits"] == audits_before_apply
        assert len(final["plan"]) == 1
        plan = final["plan"][0]["row_data"]
        assert plan["state"] == "completed"
        assert plan["version"] == 3
        assert plan["preview_cursor"] == boundary.cursor
        assert plan["boundary_cursor"] == boundary.cursor
        assert plan["apply_cursor"] == terminal.cursor
        assert plan["apply_cursor_version"] == 1
        assert plan["completed_at"] is not None
        durable_plans = _rows(
            cold_start_runtime,
            "SELECT plan_id, account_id, folder_key, state, boundary_cursor, "
            "page_count, item_count, redacted_samples, contract_fingerprint, "
            "folder_scope_config_hash, plan_hash, blocked_reason_code, "
            "blocked_fingerprint, expires_at, ready_at, approved_at, "
            "completed_at, blocked_at, created_at, updated_at "
            "FROM sync_cold_start_plans WHERE plan_id = %s",
            (ready.plan.plan_id,),
        )
        assert len(durable_plans) == 1
        assert {
            "plan_id": result.plan.plan_id,
            "account_id": result.plan.account_id,
            "folder_key": result.plan.canonical_folder,
            "state": result.plan.state.value,
            "boundary_cursor": result.plan.boundary_cursor,
            "page_count": result.plan.page_count,
            "item_count": result.plan.item_count,
            "redacted_samples": [
                {
                    "kind": sample.kind.value,
                    "external_email_id_hash": sample.external_email_id_hash,
                }
                for sample in result.plan.redacted_samples
            ],
            "contract_fingerprint": result.plan.contract_fingerprint,
            "folder_scope_config_hash": result.plan.folder_scope_config_hash,
            "plan_hash": result.plan.plan_hash,
            "blocked_reason_code": result.plan.blocked_reason_code,
            "blocked_fingerprint": result.plan.blocked_fingerprint,
            "expires_at": result.plan.expires_at,
            "ready_at": result.plan.ready_at,
            "approved_at": result.plan.approved_at,
            "completed_at": result.plan.completed_at,
            "blocked_at": result.plan.blocked_at,
            "created_at": result.plan.created_at,
            "updated_at": result.plan.updated_at,
        } == durable_plans[0]
        assert len(final["cursor"]) == 1
        cursor = final["cursor"][0]["row_data"]
        assert cursor["status"] == "active"
        assert cursor["version"] == 1
        assert cursor["cursor"] == terminal.cursor
        assert cursor["cold_start_plan_id"] is None
        assert cursor["cold_start_plan_state"] is None
        assert len(final["inbox"]) == 1
        assert final["inbox"][0]["row_data"]["external_email_id"] == (
            f"ordinary-g7-{fault_mode}"
        )
        assert sorted(
            item["row_data"]["command_name"] for item in final["receipts"]
        ) == [
            "cold_start.apply_page",
            "cold_start.approve",
            "cold_start.preview",
        ]
        assert (
            sum(
                item["row_data"]["command_name"] == "cold_start.apply_page"
                for item in final["receipts"]
            )
            == 1
        )
        winning_transaction_id = (
            old_transaction_id
            if fault_mode == "commit_then_raise"
            else recovery_observations[0].transaction_id
        )
        apply_receipt = next(
            item
            for item in final["receipts"]
            if item["row_data"]["command_name"] == "cold_start.apply_page"
        )
        assert {
            final["plan"][0]["xmin"],
            final["cursor"][0]["xmin"],
            final["inbox"][0]["xmin"],
            apply_receipt["xmin"],
        } == {winning_transaction_id}

        stats = pool.get_stats()
        assert stats["requests_waiting"] == 0
        assert stats["pool_size"] == stats["pool_available"]
    finally:
        ordinary.release.set()
        state.outcome_release.set()
        if task is not None:
            if not task.done():
                task.cancel()
            try:
                await task
            except BaseException:
                pass
        await pool.close()
        assert (
            _application_session_count(
                cold_start_runtime,
                pool_application_name,
            )
            == 0
        )
