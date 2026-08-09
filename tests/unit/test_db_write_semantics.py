from contextlib import asynccontextmanager
from datetime import UTC, datetime
from hashlib import sha256
import json
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
from src.ingestion.processing import ProcessingEffectScope
from src.utils.db_async import (
    ApprovalCommitStatus,
    AsyncDatabaseManager,
)
from src.router.decision import (
    DecisionOutcome,
    RouteDecision,
    RouteProvenance,
    RouteTier,
)
from src.safety.execution_gate import ApprovedExecutionEnvelope


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


def _route_scope() -> ProcessingEffectScope:
    return ProcessingEffectScope(
        account_id=8,
        inbox_id="00000000-0000-4000-8000-000000000001",
        generation=1,
        fencing_token=1,
        attempts=1,
        email_id="00000000-0000-4000-8000-000000000002",
        expected_email_version=2,
        event_dedupe_key="d" * 64,
        external_email_id="mail-1",
    )


@pytest.mark.asyncio
async def test_route_decision_is_inserted_once_with_exact_readback(db_manager):
    decision = RouteDecision.model_validate(_canonical_decision())
    cursor = FakeCursor(
        fetchone_results=[
            {"decision_json": None},
            {
                "decision_digest": decision.canonical_digest(),
                "decision_json": decision.model_dump(mode="json"),
            },
            {"decision_digest": decision.canonical_digest()},
        ]
    )
    db_manager.get_connection = connection_factory(cursor)

    persisted = await db_manager.persist_route_decision(
        scope=_route_scope(),
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
            {"decision_json": None},
            {"decision_digest": "0" * 64, "decision_json": {}},
        ]
    )
    db_manager.get_connection = connection_factory(cursor)

    with pytest.raises(DatabaseOperationError, match="immutable route decision conflict"):
        await db_manager.persist_route_decision(
            scope=_route_scope(),
            decision_raw=_canonical_decision(),
        )


@pytest.mark.asyncio
async def test_route_persistence_requires_exact_processing_scope(db_manager):
    with pytest.raises(ValueError, match="ProcessingEffectScope"):
        await db_manager.persist_route_decision(
            scope=None,
            decision_raw=_canonical_decision(),
        )


@pytest.mark.asyncio
async def test_route_recovery_requires_live_runtime_and_exact_scope(db_manager):
    decision = RouteDecision.model_validate(_canonical_decision())
    cursor = FakeCursor(
        fetchone_result={"decision_json": decision.model_dump(mode="json")}
    )
    db_manager.get_connection = connection_factory(cursor)

    recovered = await db_manager.get_route_decision_for_attempt(
        scope=_route_scope()
    )

    assert recovered == decision
    query, params = cursor.executions[0]
    rendered = str(query)
    assert "pipeline_runtime_instances" in rendered
    assert "pipeline_ownership" in rendered
    assert "runtime.lifecycle = 'active'" in rendered
    assert "runtime.lease_until > statement_timestamp()" in rendered
    assert params == (
        _route_scope().inbox_id,
        _route_scope().account_id,
        _route_scope().external_email_id,
        _route_scope().generation,
        _route_scope().fencing_token,
        _route_scope().execution_epoch,
        _route_scope().authority_epoch,
        _route_scope().capability_hash,
        _route_scope().lease_session_id,
        _route_scope().lease_owner,
        _route_scope().email_id,
        _route_scope().expected_email_version,
    )


@pytest.mark.asyncio
async def test_stale_route_recovery_fails_closed(db_manager):
    cursor = FakeCursor(fetchone_result=None)
    db_manager.get_connection = connection_factory(cursor)

    with pytest.raises(DatabaseOperationError, match="route authority is stale"):
        await db_manager.get_route_decision_for_attempt(scope=_route_scope())


@pytest.mark.asyncio
async def test_stale_payload_edit_is_rejected_before_revision_insert(db_manager):
    cursor = FakeCursor(
        fetchone_results=[
            {
                "payload_revision": 2,
                "decision_digest": "1" * 64,
                "plan_digest": "2" * 64,
                "evidence_digest": "3" * 64,
                "state": "approval_pending",
            },
            None,
        ]
    )
    db_manager.get_connection = connection_factory(cursor)

    with pytest.raises(DatabaseOperationError, match="stale payload edit"):
        await db_manager.create_payload_revision(
            inbox_id="00000000-0000-4000-8000-000000000001",
            expected_version=4,
            expected_payload_revision=2,
            expected_payload_digest="4" * 64,
            payload={
                "decision_digest": "1" * 64,
                "plan_digest": "2" * 64,
                "evidence_digest": "3" * 64,
                "draft_digest": "5" * 64,
                "editor": "operator",
                "edited_at": "2026-08-09T00:00:00+00:00",
            },
        )

    assert len(cursor.executions) == 2
    assert not any(
        "INSERT INTO execution_payload_revisions" in str(query)
        for query, _params in cursor.executions
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("exact_replay", [True, False])
async def test_payload_edit_replay_recovers_only_the_exact_committed_successor(
    db_manager,
    exact_replay,
):
    inbox_id = "00000000-0000-4000-8000-000000000001"
    original_draft = "committed successor"
    requested_draft = original_draft if exact_replay else "different stale edit"

    def payload_for(draft):
        return {
            "decision_digest": "1" * 64,
            "plan_digest": "2" * 64,
            "evidence_digest": "3" * 64,
            "draft_digest": sha256(draft.encode()).hexdigest(),
            "draft_content": draft,
            "draft_ref": {"draft_id": "mail-1"},
            "to": ["recipient@example.com"],
            "cc": [],
            "attachment_refs": [],
            "attachment_digests": [],
            "external_recipient_acknowledged": True,
            "editor": "operator",
            "edited_at": "2026-08-09T00:00:00+00:00",
        }

    committed = payload_for(original_draft)
    canonical_committed = {
        key: committed[key]
        for key in (
            "decision_digest",
            "plan_digest",
            "evidence_digest",
            "draft_digest",
            "draft_content",
            "draft_ref",
            "to",
            "cc",
            "attachment_refs",
            "attachment_digests",
            "external_recipient_acknowledged",
        )
    }
    committed_digest = sha256(
        json.dumps(
            canonical_committed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
    ).hexdigest()
    successor = {
        "payload_digest": committed_digest,
        "decision_digest": committed["decision_digest"],
        "plan_digest": committed["plan_digest"],
        "evidence_digest": committed["evidence_digest"],
        "draft_digest": committed["draft_digest"],
        "draft_content": committed["draft_content"],
        "draft_ref": committed["draft_ref"],
        "to_recipients": committed["to"],
        "cc_recipients": committed["cc"],
        "attachment_refs": committed["attachment_refs"],
        "attachment_digests": committed["attachment_digests"],
        "external_recipient_acknowledged": committed[
            "external_recipient_acknowledged"
        ],
    }
    cursor = FakeCursor(
        fetchone_results=[
            {
                "payload_revision": 3,
                "decision_digest": "1" * 64,
                "plan_digest": "2" * 64,
                "evidence_digest": "3" * 64,
                "state": "approval_pending",
            },
            {"exists": 1},
            successor,
        ]
    )
    db_manager.get_connection = connection_factory(cursor)

    operation = db_manager.create_payload_revision(
        inbox_id=inbox_id,
        expected_version=5,
        expected_payload_revision=2,
        expected_payload_digest="a" * 64,
        payload=payload_for(requested_draft),
    )
    if exact_replay:
        assert await operation == 3
    else:
        with pytest.raises(DatabaseOperationError, match="stale payload edit"):
            await operation

    assert len(cursor.executions) == 3
    assert not any(
        "INSERT INTO execution_payload_revisions" in str(query)
        for query, _params in cursor.executions
    )
    assert not any(
        "UPDATE handoff_runs" in str(query)
        for query, _params in cursor.executions
    )


@pytest.mark.asyncio
async def test_exact_approved_envelope_replay_is_idempotent(db_manager):
    inbox_id = "00000000-0000-4000-8000-000000000001"
    email_id = "mail-approved-1"
    draft = "approved draft"
    decision = RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route="reply",
        params={},
        provenance=RouteProvenance(
            tier=RouteTier.TIER3,
            source_version="test-v1",
            confidence=1.0,
        ),
        reason_code="test_reply",
        handoff_profile_id="generic_reply_v1",
    )
    payload = {
        "decision_digest": decision.canonical_digest(),
        "plan_digest": "1" * 64,
        "evidence_digest": "2" * 64,
        "draft_digest": sha256(draft.encode()).hexdigest(),
        "draft_content": draft,
        "draft_ref": {"draft_id": email_id},
        "to": ["recipient@example.com"],
        "cc": [],
        "attachment_refs": [],
        "attachment_digests": [],
        "external_recipient_acknowledged": True,
    }
    payload_digest = sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
    ).hexdigest()
    envelope = ApprovedExecutionEnvelope(
        inbox_id=inbox_id,
        account_id=8,
        email_id=email_id,
        payload_revision=3,
        payload_digest=payload_digest,
        route_decision=decision,
        decision_digest=decision.canonical_digest(),
        plan_digest="1" * 64,
        evidence_digest="2" * 64,
        draft_digest=sha256(draft.encode()).hexdigest(),
        draft_content=draft,
        draft_ref={"draft_id": email_id},
        to=("recipient@example.com",),
        external_recipient_acknowledged=True,
        approver="operator",
        approved_at=datetime.now(UTC),
    )
    envelope_json = envelope.model_dump(mode="json")
    cursor = FakeCursor(
        fetchone_result={
            "envelope_json": envelope_json,
            "envelope_digest": envelope.canonical_digest(),
            "inbox_id": inbox_id,
            "payload_revision": 3,
            "envelope_payload_digest": payload_digest,
            "payload_payload_digest": payload_digest,
            "approver": "operator",
            "decision_digest": decision.canonical_digest(),
            "plan_digest": "1" * 64,
            "evidence_digest": "2" * 64,
            "draft_digest": sha256(draft.encode()).hexdigest(),
            "payload_decision_digest": decision.canonical_digest(),
            "payload_plan_digest": "1" * 64,
            "payload_evidence_digest": "2" * 64,
            "payload_draft_digest": sha256(draft.encode()).hexdigest(),
            "decision_json": decision.model_dump(mode="json"),
            "external_email_id": email_id,
            "account_id": 8,
            "envelope_count": 1,
        }
    )
    db_manager.get_connection = connection_factory(cursor)

    result = await db_manager.approve_payload_revision(
        inbox_id=inbox_id,
        revision=3,
        expected_version=99,
        payload_digest=payload_digest,
        approver="operator",
        approved_at="2026-08-09T00:00:00+00:00",
    )

    assert result.status is ApprovalCommitStatus.ALREADY_APPROVED_EXACT
    assert result.envelope == envelope_json
    assert result.envelope_digest == envelope.canonical_digest()
    assert len(cursor.executions) == 1
    assert "approved_execution_envelopes" in str(cursor.executions[0][0])

    with pytest.raises(DatabaseOperationError, match="replay conflict"):
        await db_manager.approve_payload_revision(
            inbox_id=inbox_id,
            revision=3,
            expected_version=99,
            payload_digest=payload_digest,
            approver="different-operator",
            approved_at="2026-08-09T00:00:00+00:00",
        )
    with pytest.raises(DatabaseOperationError, match="replay conflict"):
        await db_manager.approve_payload_revision(
            inbox_id=inbox_id,
            revision=3,
            expected_version=99,
            payload_digest="f" * 64,
            approver="operator",
            approved_at="2026-08-09T00:00:00+00:00",
        )
    assert len(cursor.executions) == 3
    assert not any(
        "INSERT INTO approved_execution_envelopes" in str(query)
        for query, _params in cursor.executions
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
