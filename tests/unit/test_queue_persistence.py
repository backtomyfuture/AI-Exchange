from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.domain.email_state import ProcessingOutcome
from src.email_feishu_delivery import (
    EmailDeliveryDisposition,
    EmailDeliveryKind,
    EmailDeliveryOutcome,
)
from src.exchange_service import CleanupHandleSnapshot, _run_ai_path
from src.router.decision import RouteDecision
from src.storage import ContentRef


def _context() -> SimpleNamespace:
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000047",
        key_version="v1",
        sha256="4" * 64,
    )
    return SimpleNamespace(
        db_manager=SimpleNamespace(
            get_content_ref=AsyncMock(return_value=ref),
            update_status=AsyncMock(),
        ),
        email_processor=SimpleNamespace(
            process_email=MagicMock(return_value=True),
            update_email_labels=MagicMock(return_value=True),
        ),
        exchange_client=SimpleNamespace(mark_as_read=AsyncMock(return_value=True)),
        email_feishu_delivery=SimpleNamespace(deliver=AsyncMock()),
    )


@pytest.mark.asyncio
async def test_failed_ai_path_is_not_marked_as_read(route_decision_factory):
    ctx = _context()
    routing_engine = SimpleNamespace(
        resolve_route=AsyncMock(
            return_value=RouteDecision.model_validate(route_decision_factory("reply"))
        ),
    )
    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch("src.exchange_service._ingest_to_qdrant", new=AsyncMock()), patch(
        "src.exchange_service._run_ai_pipeline", new=AsyncMock(return_value=None)
    ), patch(
        "src.exchange_service.get_routing_engine", return_value=routing_engine
    ), patch(
        "src.exchange_service._routing_evidence_hits", new=AsyncMock(return_value=[])
    ):
        outcome = await _run_ai_path(
            "fail-test",
            {"id": "fail-test"},
            ctx,
            {"configurable": {"thread_id": "fail-test"}},
        )

    assert outcome is ProcessingOutcome.FAILED
    ctx.email_feishu_delivery.deliver.assert_not_awaited()
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_confirmed_delivery_is_marked_read_once(route_decision_factory):
    ctx = _context()
    ctx.email_feishu_delivery.deliver.return_value = EmailDeliveryOutcome(
        EmailDeliveryKind.APPROVAL,
        EmailDeliveryDisposition.CONFIRMED,
    )
    route_decision = route_decision_factory("reply")
    pipeline_result = {
        "classification": {"need_reply": True, "priority": "P1", "intent": "审批"},
        "draft": "draft",
        "context": [],
        "email": {"id": "success-test", "attachments": []},
        "routing_log": [],
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
        "src.exchange_service._run_ai_pipeline", new=AsyncMock(return_value=pipeline_result)
    ), patch(
        "src.exchange_service.get_routing_engine", return_value=routing_engine
    ), patch(
        "src.exchange_service._routing_evidence_hits", new=AsyncMock(return_value=[])
    ):
        outcome = await _run_ai_path(
            "success-test",
            {"id": "success-test"},
            ctx,
            {"configurable": {"thread_id": "success-test"}},
        )

    assert outcome is ProcessingOutcome.PROCESSED
    ctx.exchange_client.mark_as_read.assert_awaited_once_with("success-test", is_read=True)
