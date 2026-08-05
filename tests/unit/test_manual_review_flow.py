import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from src.domain.email_state import InitialEmailWriteResult, ProcessingOutcome
from src.domain.errors import DatabaseOperationError
from src.exchange_service import (
    AttachmentUploadProjection,
    CleanupHandleSnapshot,
    _run_ai_path,
    process_and_archive_email,
)
from src.graph.builder import build_graph
from src.graph.state_factory import sanitize_graph_delta
from src.storage import ContentRef


def _manual_delta(state, code="categorizer_model_failed"):
    return sanitize_graph_delta(
        state,
        {
            "classification": {
                "priority": "P1",
                "intent": "审批",
                "summary": "需要人工审核",
                "reasoning": code,
                "confidence": 0.0,
            },
            "approval_status": "manual_review",
            "next_step": "manual_review",
            "safe_error_summary": code,
        },
    )


@pytest.mark.asyncio
async def test_graph_routes_categorizer_manual_review_without_downstream_nodes(
    graph_node_harness,
    monkeypatch,
):
    downstream_called = False

    async def categorizer(state, dependencies):
        assert dependencies is graph_node_harness.dependencies
        return _manual_delta(state)

    async def retriever(state, dependencies):
        nonlocal downstream_called
        downstream_called = True
        return sanitize_graph_delta(state, {"next_step": "drafter"})

    monkeypatch.setattr("src.graph.builder.categorize_email", categorizer)
    monkeypatch.setattr("src.graph.builder.retrieve_context", retriever)
    graph = build_graph(
        checkpointer=InMemorySaver(),
        dependencies=graph_node_harness.dependencies,
    )
    state = graph_node_harness.state(
        {"id": "manual-category", "subject": "subject", "body": "body"}
    )

    result = await graph.ainvoke(
        state,
        config={"configurable": {"thread_id": "manual-category"}},
    )

    assert result["next_step"] == "manual_review"
    assert result["approval_status"] == "manual_review"
    assert result["classification"].get("need_reply") is not False
    assert downstream_called is False


@pytest.mark.asyncio
async def test_graph_routes_drafter_manual_review_without_reviewer(
    graph_node_harness,
    monkeypatch,
):
    reviewer_called = False

    async def categorizer(state, dependencies):
        return sanitize_graph_delta(
            state,
            {
                "classification": {
                    "priority": "P1",
                    "need_reply": True,
                    "intent": "咨询",
                    "summary": "reply",
                    "reasoning": "test",
                    "confidence": 1.0,
                },
                "next_step": "rag_search",
            },
        )

    async def retriever(state, dependencies):
        return sanitize_graph_delta(state, {"next_step": "drafter"})

    async def drafter(state, dependencies):
        return _manual_delta(state, "drafter_model_failed")

    async def reviewer(state, dependencies):
        nonlocal reviewer_called
        reviewer_called = True
        return sanitize_graph_delta(state, {"next_step": "approval"})

    monkeypatch.setattr("src.graph.builder.categorize_email", categorizer)
    monkeypatch.setattr("src.graph.builder.retrieve_context", retriever)
    monkeypatch.setattr("src.graph.builder.generate_draft", drafter)
    monkeypatch.setattr("src.graph.builder.review_draft", reviewer)
    graph = build_graph(
        checkpointer=InMemorySaver(),
        dependencies=graph_node_harness.dependencies,
    )
    state = graph_node_harness.state(
        {"id": "manual-drafter", "subject": "subject", "body": "body"}
    )

    result = await graph.ainvoke(
        state,
        config={"configurable": {"thread_id": "manual-drafter"}},
    )

    assert result["next_step"] == "manual_review"
    assert reviewer_called is False


@pytest.mark.asyncio
async def test_compiled_reviewer_manual_route_never_reaches_sender(
    graph_node_harness,
    monkeypatch,
):
    sender_called = False

    async def categorizer(state, dependencies):
        return sanitize_graph_delta(
            state,
            {
                "classification": {
                    "priority": "P1",
                    "need_reply": True,
                    "intent": "咨询",
                    "summary": "reply",
                    "reasoning": "test",
                    "confidence": 1.0,
                },
                "next_step": "rag_search",
            },
        )

    async def retriever(state, dependencies):
        return sanitize_graph_delta(state, {"next_step": "drafter"})

    async def drafter(state, dependencies):
        return sanitize_graph_delta(
            state,
            {"draft_id": state["email_id"], "next_step": "reviewer"},
        )

    async def reviewer(state, dependencies):
        return _manual_delta(state, "reviewer_model_failed")

    async def sender(state, dependencies, config=None):
        nonlocal sender_called
        sender_called = True
        return state

    monkeypatch.setattr("src.graph.builder.categorize_email", categorizer)
    monkeypatch.setattr("src.graph.builder.retrieve_context", retriever)
    monkeypatch.setattr("src.graph.builder.generate_draft", drafter)
    monkeypatch.setattr("src.graph.builder.review_draft", reviewer)
    monkeypatch.setattr("src.graph.builder.send_final_email", sender)
    graph = build_graph(
        checkpointer=InMemorySaver(),
        dependencies=graph_node_harness.dependencies,
    )
    email_id = "manual-reviewer"
    config = {"configurable": {"thread_id": email_id}}
    state = graph_node_harness.state(
        {"id": email_id, "subject": "subject", "body": "body"}
    )

    interrupted = await graph.ainvoke(state, config=config)
    snapshot = await graph.aget_state(config)

    assert interrupted["next_step"] == "manual_review"
    assert snapshot.next == ("manual_review",)

    completed = await graph.ainvoke(None, config=config)

    assert completed["approval_status"] == "manual_review"
    assert completed["safe_error_summary"] == "reviewer_model_failed"
    assert sender_called is False


@pytest.mark.asyncio
async def test_run_ai_path_sends_manual_review_card_without_mark_read():
    email_id = "manual-service"
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000061",
        key_version="v1",
        sha256="8" * 64,
    )
    ctx = MagicMock()
    ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
    call_order = []

    async def update_status(*args, **kwargs):
        raise AssertionError("manual review must use CAS")

    async def compare_and_set_manual_review(*args, **kwargs):
        call_order.append("manual_review_persisted")
        return True

    async def cleanup_resources(*args, **kwargs):
        call_order.append("resources_cleaned")

    ctx.db_manager.update_status = AsyncMock(side_effect=update_status)
    ctx.db_manager.compare_and_set_manual_review = AsyncMock(
        side_effect=compare_and_set_manual_review
    )
    ctx.exchange_client.mark_as_read = AsyncMock()
    projection = {
        "classification": {},
        "draft": "",
        "context": [],
        "email": {"id": email_id},
        "routing_log": [],
        "active_skills": [],
        "next_step": "manual_review",
        "safe_error_summary": "categorizer_model_failed",
    }
    manual_card = AsyncMock(
        side_effect=lambda *_args, **_kwargs: call_order.append(
            "manual_card_delivered"
        )
        or True
    )

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ), patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=AttachmentUploadProjection((), ())),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new=AsyncMock(),
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=projection),
    ), patch(
        "src.exchange_service._cleanup_graph_drive_files",
        new=AsyncMock(side_effect=cleanup_resources),
    ) as cleanup, patch(
        "src.exchange_service._dispatch_manual_review_notification",
        new=manual_card,
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(return_value={"delivered": True, "kind": "skipped"}),
    ) as dispatch, patch(
        "src.exchange_service._mark_email_read",
        new=AsyncMock(),
    ) as mark_read:
        outcome = await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )

    assert outcome is ProcessingOutcome.MANUAL_REVIEW
    assert call_order == [
        "manual_card_delivered",
        "manual_review_persisted",
        "resources_cleaned",
    ]
    manual_card.assert_awaited_once_with(
        email_id,
        projection,
        _effect_boundary=None,
    )
    ctx.db_manager.compare_and_set_manual_review.assert_awaited_with(
        email_id,
        expected=frozenset(
            {
                "pending",
                "recovering",
                "ingested",
                "analyzed",
                "drafted",
                "error",
                "delivery_failed",
            }
        ),
        error_code="categorizer_model_failed",
    )
    ctx.db_manager.update_status.assert_not_awaited()
    cleanup.assert_awaited_once()
    dispatch.assert_not_awaited()
    mark_read.assert_not_awaited()
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_email_returns_typed_manual_review_outcome(mock_env):
    ctx = MagicMock()
    ctx.db_manager.log_initial_email = AsyncMock(
        return_value=InitialEmailWriteResult.CREATED
    )
    manual = AsyncMock(return_value=ProcessingOutcome.MANUAL_REVIEW)

    with patch(
        "src.exchange_service._ensure_durable_content_ref",
        new=AsyncMock(),
    ), patch("src.exchange_service._run_ai_path", new=manual):
        outcome = await process_and_archive_email(
            {"id": "manual-outcome", "attachments": []},
            ctx,
        )

    assert outcome is ProcessingOutcome.MANUAL_REVIEW


async def test_run_ai_path_normalizes_untrusted_manual_review_code():
    email_id = "manual-code-boundary"
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000062",
        key_version="v1",
        sha256="9" * 64,
    )
    ctx = MagicMock()
    ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
    ctx.db_manager.update_status = AsyncMock()
    ctx.db_manager.compare_and_set_manual_review = AsyncMock(return_value=True)
    projection = {
        "classification": {},
        "draft": "",
        "context": [],
        "email": {"id": email_id},
        "routing_log": [],
        "active_skills": [],
        "approval_status": "manual_review",
        "next_step": "approval",
        "safe_error_summary": "private-content-should-not-be-persisted",
    }
    manual_card = AsyncMock(return_value=True)

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ), patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=AttachmentUploadProjection((), ())),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new=AsyncMock(),
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=projection),
    ), patch(
        "src.exchange_service._cleanup_graph_drive_files",
        new=AsyncMock(),
    ), patch(
        "src.exchange_service._dispatch_manual_review_notification",
        new=manual_card,
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(),
    ) as dispatch:
        outcome = await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )

    assert outcome is ProcessingOutcome.MANUAL_REVIEW
    manual_card.assert_awaited_once_with(
        email_id,
        projection,
        _effect_boundary=None,
    )
    ctx.db_manager.compare_and_set_manual_review.assert_awaited_with(
        email_id,
        expected=frozenset(
            {
                "pending",
                "recovering",
                "ingested",
                "analyzed",
                "drafted",
                "error",
                "delivery_failed",
            }
        ),
        error_code="invalid_classification",
    )
    ctx.db_manager.update_status.assert_not_awaited()
    dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_ai_path_escalates_card_delivery_when_manual_review_cas_loses():
    email_id = "manual-cas-loser"
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000063",
        key_version="v1",
        sha256="a" * 64,
    )
    ctx = MagicMock()
    ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
    ctx.db_manager.compare_and_set_manual_review = AsyncMock(return_value=False)
    cleanup = AsyncMock()
    projection = {
        "approval_status": "manual_review",
        "next_step": "manual_review",
        "safe_error_summary": "reviewer_model_failed",
    }
    manual_card = AsyncMock(return_value=True)

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ), patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=AttachmentUploadProjection((), ())),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new=AsyncMock(),
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=projection),
    ), patch(
        "src.exchange_service._cleanup_graph_drive_files",
        new=cleanup,
    ), patch(
        "src.exchange_service._dispatch_manual_review_notification",
        new=manual_card,
    ), patch(
        "src.exchange_service._dispatch_notification",
        new=AsyncMock(),
    ) as dispatch, pytest.raises(DatabaseOperationError) as caught:
        await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )

    assert caught.value.operation == "compare_and_set_manual_review"
    manual_card.assert_awaited_once_with(
        email_id,
        projection,
        _effect_boundary=None,
    )
    cleanup.assert_not_awaited()
    dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_manual_review_commit_then_raise_is_confirmed_before_cleanup(caplog):
    email_id = "manual-commit-readback"
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000064",
        key_version="v1",
        sha256="b" * 64,
    )
    ctx = MagicMock()
    ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
    ctx.db_manager.compare_and_set_manual_review = AsyncMock(
        side_effect=DatabaseOperationError(
            operation="compare_and_set_manual_review",
            retryable=True,
            message="PRIVATE-MANUAL-COMMIT-DETAIL",
        )
    )
    ctx.db_manager.get_email_status = AsyncMock(return_value="manual_review")
    cleanup = AsyncMock()
    caplog.set_level("ERROR", logger="ExchangeService")

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ), patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=AttachmentUploadProjection((), ())),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new=AsyncMock(),
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(
            return_value={
                "next_step": "manual_review",
                "safe_error_summary": "reviewer_model_failed",
            }
        ),
    ), patch(
        "src.exchange_service._cleanup_graph_drive_files",
        new=cleanup,
    ), patch(
        "src.exchange_service._dispatch_manual_review_notification",
        new=AsyncMock(return_value=True),
    ):
        outcome = await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )

    assert outcome is ProcessingOutcome.MANUAL_REVIEW
    cleanup.assert_awaited_once()
    assert "PRIVATE-MANUAL-COMMIT-DETAIL" not in caplog.text


@pytest.mark.asyncio
async def test_manual_cleanup_cancellation_preserves_manual_and_cancellation_identity():
    email_id = "manual-cleanup-cancel"
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000065",
        key_version="v1",
        sha256="c" * 64,
    )
    cancellation = asyncio.CancelledError()
    ctx = MagicMock()
    ctx.db_manager.get_content_ref = AsyncMock(return_value=ref)
    ctx.db_manager.compare_and_set_manual_review = AsyncMock(return_value=True)
    cleanup = AsyncMock(side_effect=cancellation)

    with patch(
        "src.exchange_service.get_settings",
        return_value=SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    ), patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._upload_attachments_to_lark",
        new=AsyncMock(return_value=AttachmentUploadProjection((), ())),
    ), patch(
        "src.exchange_service._ingest_to_qdrant",
        new=AsyncMock(),
    ), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(
            return_value={
                "next_step": "manual_review",
                "safe_error_summary": "reviewer_model_failed",
            }
        ),
    ), patch(
        "src.exchange_service._cleanup_graph_drive_files",
        new=cleanup,
    ), patch(
        "src.exchange_service._dispatch_manual_review_notification",
        new=AsyncMock(return_value=True),
    ), pytest.raises(asyncio.CancelledError) as caught:
        await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )

    assert caught.value is cancellation
    ctx.db_manager.compare_and_set_manual_review.assert_awaited_once()
    assert cleanup.await_count == 1
