from __future__ import annotations

import asyncio
import importlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Protocol

import pytest

from src.domain.errors import DatabaseOperationError
from src.utils.db_async import AsyncDatabaseManager


MAX_ACTION_ID_BYTES = 512


class StatusCAS(Protocol):
    async def compare_and_set_status(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        target: str,
    ) -> bool: ...


class AtomicStatusDB:
    """Small stateful CAS fake; the lock models the database winner boundary."""

    def __init__(self, initial_status: str) -> None:
        self.status = initial_status
        self.calls: list[tuple[str, frozenset[str], str]] = []
        self._lock = asyncio.Lock()

    async def compare_and_set_status(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        target: str,
    ) -> bool:
        async with self._lock:
            self.calls.append((email_id, expected, target))
            if self.status not in expected:
                return False
            self.status = target
            return True


class FailingStatusDB:
    def __init__(self, error: DatabaseOperationError) -> None:
        self.error = error
        self.calls = 0

    async def compare_and_set_status(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        target: str,
    ) -> bool:
        del email_id, expected, target
        self.calls += 1
        raise self.error


class SpyStatusDB:
    def __init__(self) -> None:
        self.calls = 0

    async def compare_and_set_status(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        target: str,
    ) -> bool:
        del email_id, expected, target
        self.calls += 1
        return True


class FakeCursor:
    def __init__(self, *, rowcount: int) -> None:
        self.rowcount = rowcount
        self.executions: list[tuple[str, object]] = []

    async def __aenter__(self) -> FakeCursor:
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        del exc_type, exc, traceback
        return False

    async def execute(self, query: str, params: object = None) -> None:
        self.executions.append((query, params))


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor


@asynccontextmanager
async def _connection(cursor: FakeCursor) -> AsyncIterator[FakeConnection]:
    yield FakeConnection(cursor)


def _manager(cursor: FakeCursor) -> AsyncDatabaseManager:
    manager = AsyncDatabaseManager(
        SimpleNamespace(database_url="postgresql://test/test")
    )
    manager.get_connection = lambda: _connection(cursor)  # type: ignore[method-assign]
    return manager


def _claim_api():
    return importlib.import_module("src.safety.approval_claim")


@pytest.mark.asyncio
async def test_concurrent_approval_claims_have_exactly_one_winner() -> None:
    api = _claim_api()
    db: StatusCAS = AtomicStatusDB("waiting_approval")

    outcomes = await asyncio.gather(
        api.claim_approval("mail-approval", "user-a", db),
        api.claim_approval("mail-approval", "user-b", db),
    )

    assert sorted(outcomes) == [False, True]
    assert isinstance(db, AtomicStatusDB)
    assert db.status == "approved"
    assert all(
        call == ("mail-approval", frozenset({"waiting_approval"}), "approved")
        for call in db.calls
    )


@pytest.mark.asyncio
async def test_approval_and_rejection_race_have_one_terminal_winner() -> None:
    api = _claim_api()
    db: StatusCAS = AtomicStatusDB("waiting_approval")

    outcomes = await asyncio.gather(
        api.claim_approval("mail-action", "approver", db),
        api.claim_rejection("mail-action", "rejector", db),
    )

    assert sorted(outcomes) == [False, True]
    assert isinstance(db, AtomicStatusDB)
    assert db.status in {"approved", "rejected"}
    assert {call[1] for call in db.calls} == {
        frozenset({"waiting_approval"})
    }
    assert {call[2] for call in db.calls} == {"approved", "rejected"}


@pytest.mark.asyncio
async def test_concurrent_send_claims_have_exactly_one_winner() -> None:
    api = _claim_api()
    db: StatusCAS = AtomicStatusDB("approved")

    outcomes = await asyncio.gather(
        api.claim_send("mail-send", db),
        api.claim_send("mail-send", db),
    )

    assert sorted(outcomes) == [False, True]
    assert isinstance(db, AtomicStatusDB)
    assert db.status == "sending"
    assert all(
        call == ("mail-send", frozenset({"approved"}), "sending")
        for call in db.calls
    )


@pytest.mark.asyncio
async def test_draft_save_and_approval_race_have_one_winner() -> None:
    api = _claim_api()
    db: StatusCAS = AtomicStatusDB("waiting_approval")

    outcomes = await asyncio.gather(
        api.claim_draft_save("mail-draft", db),
        api.claim_approval("mail-draft", "approver", db),
    )

    assert sorted(outcomes) == [False, True]
    assert isinstance(db, AtomicStatusDB)
    assert db.status in {"saving_draft", "approved"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_name", "args"),
    [
        ("claim_approval", ("", "user-1")),
        ("claim_approval", ("   ", "user-1")),
        ("claim_approval", ("x" * (MAX_ACTION_ID_BYTES + 1), "user-1")),
        ("claim_approval", ("mail-1", "")),
        ("claim_approval", ("mail-1", "u" * (MAX_ACTION_ID_BYTES + 1))),
        ("claim_rejection", (None, "user-1")),
        ("claim_rejection", ("mail-1", None)),
        ("claim_send", ("",)),
        ("claim_send", (b"mail-1",)),
    ],
)
async def test_invalid_action_identifiers_never_reach_database(
    operation_name: str,
    args: tuple[object, ...],
) -> None:
    api = _claim_api()
    db = SpyStatusDB()

    with pytest.raises(ValueError):
        await getattr(api, operation_name)(*args, db)

    assert db.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_name", "args"),
    [
        ("claim_approval", ("mail-db-error", "user-1")),
        ("claim_rejection", ("mail-db-error", "user-1")),
        ("claim_send", ("mail-db-error",)),
    ],
)
async def test_claim_database_errors_propagate_instead_of_becoming_cas_losses(
    operation_name: str,
    args: tuple[str, ...],
) -> None:
    api = _claim_api()
    error = DatabaseOperationError(
        operation="compare_and_set_status",
        retryable=True,
        message="bounded database failure",
    )
    db = FailingStatusDB(error)

    with pytest.raises(DatabaseOperationError) as caught:
        await getattr(api, operation_name)(*args, db)

    assert caught.value is error
    assert db.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("rowcount", "expected"), [(1, True), (0, False)])
async def test_save_draft_if_status_only_writes_waiting_approval_rows(
    rowcount: int,
    expected: bool,
) -> None:
    cursor = FakeCursor(rowcount=rowcount)
    manager = _manager(cursor)

    result = await manager.save_draft_if_status("mail-edit", "updated body")

    assert result is expected
    query, params = cursor.executions[-1]
    normalized = " ".join(query.lower().split())
    set_clause, where_clause = normalized.split(" where ", 1)
    assert "draft_content = %s" in set_clause
    assert "status =" not in set_clause
    assert "id = %s" in where_clause
    assert "status = %s" in where_clause
    assert params == ("updated body", "mail-edit", "waiting_approval")


@pytest.mark.asyncio
async def test_save_draft_if_status_rejects_invalid_id_before_connection() -> None:
    manager = AsyncDatabaseManager(
        SimpleNamespace(database_url="postgresql://test/test")
    )
    connection_calls = 0

    def forbidden_connection():
        nonlocal connection_calls
        connection_calls += 1
        raise AssertionError("database must not be touched")

    manager.get_connection = forbidden_connection  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        await manager.save_draft_if_status("", "updated body")

    assert connection_calls == 0


@pytest.mark.asyncio
async def test_startup_recovery_is_atomic_bounded_and_reports_affected_rows() -> None:
    cursor = FakeCursor(rowcount=2)
    manager = _manager(cursor)

    affected = await manager.recover_incomplete_approval_states()

    assert affected == 2
    query, params = cursor.executions[-1]
    normalized = " ".join(query.lower().split())
    assert normalized.startswith("update emails_log set")
    assert "status = case status" in normalized
    assert "error_message = case status" in normalized
    assert "where status = any(%s)" in normalized
    assert params == (
        "sending",
        "send_unknown",
        "manual_review",
        "approved",
        "approval_handoff_incomplete",
        "sending",
        "send_outcome_unknown",
        "saving_draft",
        "draft_save_outcome_unknown",
        "recovering",
        "self_healing_interrupted",
        ["approved", "sending", "saving_draft", "recovering"],
    )


@pytest.mark.asyncio
async def test_startup_recovery_is_idempotent_and_filters_other_states() -> None:
    first_cursor = FakeCursor(rowcount=2)
    manager = _manager(first_cursor)

    first = await manager.recover_incomplete_approval_states()

    second_cursor = FakeCursor(rowcount=0)
    manager.get_connection = (  # type: ignore[method-assign]
        lambda: _connection(second_cursor)
    )
    second = await manager.recover_incomplete_approval_states()

    assert (first, second) == (2, 0)
    for cursor in (first_cursor, second_cursor):
        query, params = cursor.executions[-1]
        normalized = " ".join(query.lower().split())
        assert "where status = any(%s)" in normalized
        assert params[-1] == [
            "approved",
            "sending",
            "saving_draft",
            "recovering",
        ]
        for untouched in (
            "waiting_approval",
            "rejected",
            "sent",
            "manual_review",
            "draft_saved",
            "send_unknown",
        ):
            assert untouched not in params[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize(("rowcount", "expected_result"), [(1, True), (0, False)])
async def test_manual_review_transition_persists_safe_code_atomically(
    rowcount: int,
    expected_result: bool,
) -> None:
    cursor = FakeCursor(rowcount=rowcount)
    manager = _manager(cursor)

    result = await manager.compare_and_set_manual_review(
        "mail-unknown-send",
        expected=frozenset({"sending"}),
        error_code="send_outcome_unknown",
    )

    assert result is expected_result
    query, params = cursor.executions[-1]
    normalized = " ".join(query.lower().split())
    assert "set status = %s" in normalized
    assert "error_message = %s" in normalized
    assert "where id = %s and status = any(%s)" in normalized
    assert params == (
        "manual_review",
        "send_outcome_unknown",
        "mail-unknown-send",
        ["sending"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(("rowcount", "expected_result"), [(1, True), (0, False)])
async def test_send_unknown_transition_is_one_way_and_atomic(
    rowcount: int,
    expected_result: bool,
) -> None:
    cursor = FakeCursor(rowcount=rowcount)
    manager = _manager(cursor)

    result = await manager.compare_and_set_send_unknown(
        "mail-unknown-send",
        error_code="send_outcome_unknown",
    )

    assert result is expected_result
    query, params = cursor.executions[-1]
    normalized = " ".join(query.lower().split())
    assert "set status = %s" in normalized
    assert "error_message = %s" in normalized
    assert "where id = %s and status = %s" in normalized
    assert params == (
        "send_unknown",
        "send_outcome_unknown",
        "mail-unknown-send",
        "sending",
    )
