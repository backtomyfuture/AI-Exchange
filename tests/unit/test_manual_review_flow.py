from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from src.domain.email_state import InitialEmailWriteResult, ProcessingOutcome
from src.email_feishu_delivery import (
    EmailDeliveryDisposition,
    EmailDeliveryKind,
    EmailDeliveryOutcome,
    ManualReviewNotificationRequest,
)
from src.exchange_service import CleanupHandleSnapshot, _run_ai_path, process_and_archive_email
from src.graph.builder import build_graph
from src.graph.state_factory import sanitize_graph_delta
from src.router.decision import RouteDecision
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
    route_decision_factory,
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
    state["route_decision"] = route_decision_factory("reply")

    interrupted = await graph.ainvoke(state, config=config)
    snapshot = await graph.aget_state(config)

    assert interrupted["next_step"] == "manual_review"
    assert snapshot.next == ("manual_review",)

    completed = await graph.ainvoke(None, config=config)

    assert completed["approval_status"] == "manual_review"
    assert completed["safe_error_summary"] == "reviewer_model_failed"
    assert sender_called is False


def _run_path_context(email_id: str) -> SimpleNamespace:
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000061",
        key_version="v1",
        sha256="8" * 64,
    )
    return SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=ref),
            update_status=AsyncMock(),
        ),
        email_processor=SimpleNamespace(update_email_labels=MagicMock(return_value=True)),
        exchange_client=SimpleNamespace(mark_as_read=AsyncMock(return_value=True)),
        email_feishu_delivery=SimpleNamespace(deliver=AsyncMock()),
    )


@pytest.mark.asyncio
async def test_run_ai_path_projects_manual_review_to_delivery_without_mark_read(
    route_decision_factory,
):
    email_id = "manual-service"
    ctx = _run_path_context(email_id)
    ctx.email_feishu_delivery.deliver.return_value = EmailDeliveryOutcome(
        EmailDeliveryKind.MANUAL_REVIEW,
        EmailDeliveryDisposition.CONFIRMED,
        pdf_token="review-pdf",
    )
    route_decision = route_decision_factory("reply")
    projection = {
        "classification": {},
        "draft": "",
        "context": [],
        "email": {"id": email_id},
        "routing_log": [],
        "next_step": "manual_review",
        "safe_error_summary": "categorizer_model_failed",
        "route_decision": route_decision,
    }
    routing_engine = SimpleNamespace(
        resolve_route=AsyncMock(
            return_value=RouteDecision.model_validate(route_decision)
        ),
    )

    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch("src.exchange_service._ingest_to_qdrant", new=AsyncMock()), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=projection),
    ), patch(
        "src.exchange_service.get_routing_engine", return_value=routing_engine
    ), patch(
        "src.exchange_service._routing_evidence_hits", new=AsyncMock(return_value=[])
    ), patch("src.exchange_service._mark_email_read", new=AsyncMock()) as mark_read:
        outcome = await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )

    assert outcome is ProcessingOutcome.MANUAL_REVIEW
    request = ctx.email_feishu_delivery.deliver.await_args.args[0]
    assert isinstance(request, ManualReviewNotificationRequest)
    assert request.reason == "categorizer_model_failed"
    ctx.email_processor.update_email_labels.assert_called()
    mark_read.assert_not_awaited()
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_delivery_outcome_quarantines_outer_path_without_mark_read():
    email_id = "unknown-card-outcome"
    ctx = _run_path_context(email_id)
    ctx.email_feishu_delivery.deliver.return_value = EmailDeliveryOutcome(
        EmailDeliveryKind.APPROVAL,
        EmailDeliveryDisposition.UNKNOWN,
        pdf_token="review-pdf",
    )
    projection = {
        "classification": {"need_reply": True},
        "draft": "请审批",
        "context": [],
        "email": {"id": email_id},
        "routing_log": [],
    }

    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch("src.exchange_service._ingest_to_qdrant", new=AsyncMock()), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=projection),
    ), patch("src.exchange_service._mark_email_read", new=AsyncMock()) as mark_read:
        outcome = await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )

    assert outcome is ProcessingOutcome.MANUAL_REVIEW
    mark_read.assert_not_awaited()


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
