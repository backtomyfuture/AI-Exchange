from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.domain.email_state import InitialEmailWriteResult, ProcessingOutcome
from src.email_feishu_delivery import (
    EmailDeliveryDisposition,
    EmailDeliveryKind,
    EmailDeliveryOutcome,
)
from src.storage import ContentRef
from src.router.decision import RouteDecision


def _context() -> SimpleNamespace:
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000037",
        key_version="v1",
        sha256="3" * 64,
    )
    return SimpleNamespace(
        db_manager=SimpleNamespace(
            log_initial_email=AsyncMock(return_value=InitialEmailWriteResult.CREATED),
            set_content_ref_if_absent=AsyncMock(return_value=True),
            get_content_ref=AsyncMock(return_value=ref),
            update_status=AsyncMock(),
        ),
        content_store=SimpleNamespace(put_email=AsyncMock(return_value=ref)),
        email_processor=SimpleNamespace(process_email=MagicMock(return_value=True)),
        exchange_client=SimpleNamespace(mark_as_read=AsyncMock(return_value=True)),
        email_feishu_delivery=SimpleNamespace(deliver=AsyncMock()),
        graph=MagicMock(),
    )


@pytest.mark.asyncio
async def test_archive_route_never_constructs_email_feishu_delivery():
    from src.exchange_service import process_and_archive_email

    ctx = _context()
    archive = AsyncMock()
    email = {"id": "SENT_001", "subject": "sent", "sender": "me@example.com"}

    with patch("src.exchange_service._archive_only", new=archive):
        outcome = await process_and_archive_email(email, ctx, skip_analysis=True)

    assert outcome is ProcessingOutcome.ARCHIVED
    archive.assert_awaited_once()
    ctx.email_feishu_delivery.deliver.assert_not_awaited()
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_skipped_email_never_constructs_email_feishu_delivery(
    route_decision_factory,
):
    from src.exchange_service import _run_ai_path
    from src.exchange_service import CleanupHandleSnapshot

    ctx = _context()
    pipeline_result = {
        "classification": {"need_reply": False, "priority": "P3", "intent": "垃圾邮件"},
        "draft": "",
        "context": [],
        "email": {"id": "INBOX_001", "attachments": []},
        "routing_log": [],
    }
    routing_engine = SimpleNamespace(
        resolve_route=AsyncMock(
            return_value=RouteDecision.model_validate(
                route_decision_factory("no_action")
            )
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
            "INBOX_001",
            {"id": "INBOX_001", "attachments": []},
            ctx,
            {"configurable": {"thread_id": "INBOX_001"}},
        )

    assert outcome is ProcessingOutcome.PROCESSED
    ctx.email_feishu_delivery.deliver.assert_not_awaited()
    ctx.exchange_client.mark_as_read.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_notification_delivery_is_the_only_owner_of_business_attachments(
    route_decision_factory,
):
    from src.exchange_service import _run_ai_path
    from src.exchange_service import CleanupHandleSnapshot

    ctx = _context()
    ctx.email_feishu_delivery.deliver.return_value = EmailDeliveryOutcome(
        EmailDeliveryKind.READ_NOTIFICATION,
        EmailDeliveryDisposition.CONFIRMED,
    )
    pipeline_result = {
        "classification": {"need_reply": False, "priority": "P1", "intent": "通知"},
        "draft": "",
        "context": [],
        "email": {
            "id": "INBOX_READ_ONLY",
            "attachments": [{"name": "notice.pdf", "content": "UERG"}],
        },
        "routing_log": [],
    }
    route_decision = route_decision_factory("read_only")
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
        await _run_ai_path(
            "INBOX_READ_ONLY",
            {
                "id": "INBOX_READ_ONLY",
                "attachments": [{"name": "notice.pdf", "content": "UERG"}],
            },
            ctx,
            {"configurable": {"thread_id": "INBOX_READ_ONLY"}},
        )

    request = ctx.email_feishu_delivery.deliver.await_args.args[0]
    assert request.email_data["attachments"][0]["name"] == "notice.pdf"
