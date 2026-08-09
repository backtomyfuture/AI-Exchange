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
from src.router.decision import RouteDecision


class FakeCursor:
    def __init__(
        self,
        *,
        rowcount: int = 1,
        fetchone_result=None,
        fetchone_results=None,
    ):
        self.rowcount = rowcount
        self.fetchone_result = fetchone_result
        self.fetchone_results = list(fetchone_results or [])
        self.executions = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def execute(self, query, params=None):
        self.executions.append((query, params))

    async def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return self.fetchone_result


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def transaction(self):
        return FakeTransaction()


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


def _canonical_decision() -> dict:
    return {
        "outcome": "matched",
        "route": "read_only",
        "params": {},
        "provenance": {
            "tier": "tier2",
            "source_version": "routing-label-v1",
            "evidence_ids": ["history-1", "history-2"],
            "confidence": 1.0,
        },
        "reason_code": "historical_consensus",
        "selected_action_fingerprint": "sha256:" + "a" * 64,
        "candidate_actions": [],
    }


@pytest.mark.asyncio
async def test_route_decision_is_inserted_once_with_exact_readback(db_manager):
    decision = RouteDecision.model_validate(_canonical_decision())
    cursor = FakeCursor(
        fetchone_results=[
            {
                "decision_digest": decision.canonical_digest(),
                "decision_json": decision.model_dump(mode="json"),
            },
            {"decision_digest": decision.canonical_digest()},
        ]
    )
    db_manager.get_connection = connection_factory(cursor)

    persisted = await db_manager.persist_route_decision(
        inbox_id="00000000-0000-4000-8000-000000000001",
        account_id=8,
        external_email_id="mail-1",
        decision_raw=decision,
    )

    assert persisted == decision
    statements = [" ".join(query.split()) for query, _ in cursor.executions]
    assert any("ON CONFLICT (inbox_id) DO NOTHING" in query for query in statements)
    assert any("INSERT INTO handoff_executions" in query for query in statements)


@pytest.mark.asyncio
async def test_route_decision_conflict_fails_closed(db_manager):
    cursor = FakeCursor(
        fetchone_results=[
            {"decision_digest": "0" * 64, "decision_json": {}},
        ]
    )
    db_manager.get_connection = connection_factory(cursor)

    with pytest.raises(DatabaseOperationError, match="immutable route decision conflict"):
        await db_manager.persist_route_decision(
            inbox_id="00000000-0000-4000-8000-000000000001",
            account_id=8,
            external_email_id="mail-1",
            decision_raw=_canonical_decision(),
        )


@pytest.mark.asyncio
async def test_handoff_transition_is_exact_compare_and_set(db_manager):
    cursor = FakeCursor(rowcount=1)
    db_manager.get_connection = connection_factory(cursor)

    await db_manager.advance_handoff_execution(
        inbox_id="00000000-0000-4000-8000-000000000001",
        expected_state="planned",
        next_state="effect_committed",
    )

    query, params = cursor.executions[-1]
    assert "WHERE inbox_id = %s AND state = %s" in query
    assert params[-1] == "planned"


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
@pytest.mark.parametrize(
    "protected_status",
    ["approved", "sending", "send_unknown", "sent"],
)
async def test_update_status_rejects_cas_only_targets_before_database_io(
    db_manager,
    protected_status: str,
):
    db_manager.get_connection = lambda: pytest.fail("database must not be touched")

    with pytest.raises(ValueError, match="status_requires_compare_and_set"):
        await db_manager.update_status("mail-status", protected_status)


@pytest.mark.asyncio
async def test_update_status_cannot_overwrite_started_or_terminal_send_states(
    db_manager,
):
    cursor = FakeCursor(rowcount=1)
    db_manager.get_connection = connection_factory(cursor)

    await db_manager.update_status("mail-status", "ingested")

    query, params = cursor.executions[-1]
    assert "WHERE id = %s AND status <> ALL(%s)" in query
    assert params == (
        "ingested",
        "mail-status",
        ["approved", "send_unknown", "sending", "sent"],
    )


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
@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("send_unknown", "approved"),
        ("sent", "approved"),
        ("sending", "approved"),
        ("approved", "sent"),
    ],
)
async def test_compare_and_set_status_rejects_send_state_bypasses_before_io(
    db_manager,
    source: str,
    target: str,
):
    db_manager.get_connection = lambda: pytest.fail("database must not be touched")

    with pytest.raises(ValueError, match="email_status_transition_not_allowed"):
        await db_manager.compare_and_set_status(
            "mail-cas",
            expected=frozenset({source}),
            target=target,
        )


@pytest.mark.asyncio
async def test_compare_and_set_status_rejects_ambiguous_source_set_before_io(
    db_manager,
):
    db_manager.get_connection = lambda: pytest.fail("database must not be touched")

    with pytest.raises(ValueError, match="invalid_email_status_transition"):
        await db_manager.compare_and_set_status(
            "mail-cas",
            expected=frozenset({"waiting_approval", "approved"}),
            target="sending",
        )


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
