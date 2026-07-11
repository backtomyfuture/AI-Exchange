from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import psycopg
import pytest

from src.domain.email_state import (
    SAFE_DUPLICATE_READ_STATUSES,
    InitialEmailWriteResult,
    PipelineGenerationState,
    ProcessingOutcome,
)
from src.domain.errors import DatabaseOperationError, ErrorKind, ManualReviewRequired
from src.utils.db_async import AsyncDatabaseManager


class FakeCursor:
    def __init__(self, *, rowcount: int = 1, fetchone_result=None):
        self.rowcount = rowcount
        self.fetchone_result = fetchone_result
        self.executions = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def execute(self, query, params=None):
        self.executions.append((query, params))

    async def fetchone(self):
        return self.fetchone_result


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class FailingConnection:
    def __init__(self):
        self.side_effect = None

    def __call__(self):
        return self

    async def __aenter__(self):
        raise self.side_effect

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@asynccontextmanager
async def fake_connection(connection: FakeConnection):
    yield connection


def connection_factory(cursor: FakeCursor):
    return lambda: fake_connection(FakeConnection(cursor))


@pytest.fixture
def db_manager():
    settings = MagicMock(database_url="postgresql://test/test")
    return AsyncDatabaseManager(settings)


@pytest.fixture
def duplicate_cursor():
    return connection_factory(FakeCursor(rowcount=0))


@pytest.fixture
def failing_connection():
    return FailingConnection()


def test_master_domain_values_are_stable():
    assert {kind.value for kind in ErrorKind} == {
        "validation_error",
        "authentication_error",
        "rate_limited",
        "transient_dependency_error",
        "permanent_dependency_error",
        "policy_rejected",
        "send_unknown",
        "internal_invariant_error",
    }
    assert {state.value for state in PipelineGenerationState} == {
        "current_ingress",
        "quiescing",
        "draining",
        "retired",
    }
    assert {outcome.value for outcome in ProcessingOutcome} == {
        "processed",
        "failed",
        "duplicate",
        "archived",
        "manual_review",
    }
    assert SAFE_DUPLICATE_READ_STATUSES == frozenset(
        {"waiting_approval", "notified_readonly", "skipped", "sent"}
    )


def test_manual_review_error_keeps_safe_fields():
    error = ManualReviewRequired(
        reason="ambiguous send result",
        safe_summary="Delivery must be verified manually",
    )

    assert error.reason == "ambiguous send result"
    assert error.safe_summary == "Delivery must be verified manually"
    assert str(error) == "Delivery must be verified manually"


@pytest.mark.asyncio
async def test_created_is_typed(db_manager):
    db_manager.get_connection = connection_factory(FakeCursor(rowcount=1))

    result = await db_manager.log_initial_email({"id": "mail-created"})

    assert result is InitialEmailWriteResult.CREATED


@pytest.mark.asyncio
async def test_duplicate_is_typed(db_manager, duplicate_cursor):
    db_manager.get_connection = duplicate_cursor

    result = await db_manager.log_initial_email({"id": "mail-1"})

    assert result is InitialEmailWriteResult.DUPLICATE


@pytest.mark.asyncio
async def test_database_failure_is_not_duplicate(db_manager, failing_connection):
    failing_connection.side_effect = psycopg.OperationalError("database unavailable")
    db_manager.get_connection = failing_connection

    with pytest.raises(DatabaseOperationError) as caught:
        await db_manager.log_initial_email({"id": "mail-2"})

    assert caught.value.operation == "log_initial_email"
    assert caught.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stored_row", "expected"),
    [({"status": "waiting_approval"}, "waiting_approval"), (None, None)],
)
async def test_get_email_status_returns_stored_value_or_none(
    db_manager, stored_row, expected
):
    cursor = FakeCursor(fetchone_result=stored_row)
    db_manager.get_connection = connection_factory(cursor)

    assert await db_manager.get_email_status("mail-status") == expected
    assert cursor.executions[-1][1] == ("mail-status",)


@pytest.mark.asyncio
async def test_get_email_status_wraps_database_failure(db_manager, failing_connection):
    failing_connection.side_effect = psycopg.OperationalError("database unavailable")
    db_manager.get_connection = failing_connection

    with pytest.raises(DatabaseOperationError) as caught:
        await db_manager.get_email_status("mail-status")

    assert caught.value.operation == "get_email_status"
    assert caught.value.retryable is True


@pytest.mark.asyncio
async def test_update_status_raises_when_email_is_missing(db_manager):
    db_manager.get_connection = connection_factory(FakeCursor(rowcount=0))

    with pytest.raises(DatabaseOperationError) as caught:
        await db_manager.update_status("missing-mail", "ingested")

    assert caught.value.operation == "update_status"
    assert caught.value.retryable is False


@pytest.mark.asyncio
async def test_update_status_wraps_database_failure(db_manager, failing_connection):
    failing_connection.side_effect = psycopg.OperationalError("database unavailable")
    db_manager.get_connection = failing_connection

    with pytest.raises(DatabaseOperationError) as caught:
        await db_manager.update_status("mail-status", "ingested")

    assert caught.value.operation == "update_status"
    assert caught.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("rowcount", "expected"), [(1, True), (0, False)])
async def test_compare_and_set_status_reports_whether_transition_won(
    db_manager, rowcount, expected
):
    cursor = FakeCursor(rowcount=rowcount)
    db_manager.get_connection = connection_factory(cursor)

    result = await db_manager.compare_and_set_status(
        "mail-cas",
        expected=frozenset({"waiting_approval"}),
        target="approved",
    )

    assert result is expected
    query, params = cursor.executions[-1]
    assert "WHERE id=%s AND status=ANY(%s)" in query
    assert params == ("approved", "mail-cas", ["waiting_approval"])


@pytest.mark.asyncio
async def test_compare_and_set_status_wraps_database_failure(
    db_manager, failing_connection
):
    failing_connection.side_effect = psycopg.OperationalError("database unavailable")
    db_manager.get_connection = failing_connection

    with pytest.raises(DatabaseOperationError) as caught:
        await db_manager.compare_and_set_status(
            "mail-cas",
            expected=frozenset({"waiting_approval"}),
            target="approved",
        )

    assert caught.value.operation == "compare_and_set_status"
    assert caught.value.retryable is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("rowcount", "expected_result"), [(1, True), (0, False)])
async def test_claim_self_healing_is_atomic_and_reclaims_only_stale_claims(
    db_manager, rowcount, expected_result
):
    cursor = FakeCursor(rowcount=rowcount)
    db_manager.get_connection = connection_factory(cursor)

    result = await db_manager.claim_self_healing(
        "mail-heal",
        immediate=frozenset({"error", "delivery_failed"}),
        stale=frozenset({"analyzed", "ingested", "pending"}),
        stale_after_seconds=1800,
    )

    assert result is expected_result
    query, params = cursor.executions[-1]
    normalized_query = " ".join(query.split())
    assert "SET status = %s" in normalized_query
    assert normalized_query.count("status = ANY(%s)") == 2
    assert "status = %s AND updated_at <" not in normalized_query
    assert params == (
        "recovering",
        "mail-heal",
        ["delivery_failed", "error"],
        ["analyzed", "ingested", "pending"],
        1800,
    )


@pytest.mark.asyncio
async def test_claim_self_healing_wraps_database_failure(
    db_manager, failing_connection
):
    failing_connection.side_effect = psycopg.OperationalError("database unavailable")
    db_manager.get_connection = failing_connection

    with pytest.raises(DatabaseOperationError) as caught:
        await db_manager.claim_self_healing(
            "mail-heal",
            immediate=frozenset({"error"}),
            stale=frozenset({"ingested"}),
            stale_after_seconds=1800,
        )

    assert caught.value.operation == "claim_self_healing"
    assert caught.value.retryable is True
