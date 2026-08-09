"""The Exchange read state follows only a resolved Email Feishu Delivery."""

import asyncio
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
from src.storage import ContentRef


def _context(email_id: str) -> SimpleNamespace:
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000057",
        key_version="v1",
        sha256="5" * 64,
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


def _projection(email_id: str, *, need_reply: bool = True) -> dict[str, object]:
    return {
        "classification": {
            "need_reply": need_reply,
            "priority": "P1",
            "intent": "审批",
        },
        "draft": "draft body",
        "context": [],
        "email": {"id": email_id, "subject": "s"},
        "routing_log": [],
    }


async def _run_with_projection(ctx, email_id: str, projection: dict[str, object]):
    with patch(
        "src.exchange_service._snapshot_cleanup_handles",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch(
        "src.exchange_service._checkpoint_ai_path_resources",
        new=AsyncMock(return_value=CleanupHandleSnapshot()),
    ), patch("src.exchange_service._ingest_to_qdrant", new=AsyncMock()), patch(
        "src.exchange_service._run_ai_pipeline",
        new=AsyncMock(return_value=projection),
    ):
        return await _run_ai_path(
            email_id,
            {"id": email_id, "attachments": []},
            ctx,
            {"configurable": {"thread_id": email_id}},
        )


@pytest.mark.asyncio
async def test_exchange_marks_read_after_confirmed_approval_delivery():
    email_id = "mark-read-confirmed"
    ctx = _context(email_id)
    order: list[str] = []

    async def deliver(*_args):
        order.append("delivery")
        return EmailDeliveryOutcome(
            EmailDeliveryKind.APPROVAL,
            EmailDeliveryDisposition.CONFIRMED,
            pdf_token="review-pdf",
        )

    async def mark_read(*_args, **_kwargs):
        order.append("mark-read")
        return True

    ctx.email_feishu_delivery.deliver.side_effect = deliver
    ctx.exchange_client.mark_as_read.side_effect = mark_read

    outcome = await _run_with_projection(ctx, email_id, _projection(email_id))

    assert outcome is ProcessingOutcome.PROCESSED
    assert order == ["delivery", "mark-read"]
    ctx.exchange_client.mark_as_read.assert_awaited_once_with(email_id, is_read=True)


@pytest.mark.asyncio
async def test_known_delivery_failure_leaves_exchange_unread():
    email_id = "mark-read-known-failure"
    ctx = _context(email_id)
    ctx.email_feishu_delivery.deliver.return_value = EmailDeliveryOutcome(
        EmailDeliveryKind.READ_NOTIFICATION,
        EmailDeliveryDisposition.KNOWN_FAILURE,
    )

    outcome = await _run_with_projection(
        ctx,
        email_id,
        _projection(email_id, need_reply=False),
    )

    assert outcome is ProcessingOutcome.FAILED
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_delivery_outcome_leaves_exchange_unread_and_is_manual():
    email_id = "mark-read-unknown"
    ctx = _context(email_id)
    ctx.email_feishu_delivery.deliver.return_value = EmailDeliveryOutcome(
        EmailDeliveryKind.APPROVAL,
        EmailDeliveryDisposition.UNKNOWN,
        pdf_token="possibly-linked-pdf",
    )

    outcome = await _run_with_projection(ctx, email_id, _projection(email_id))

    assert outcome is ProcessingOutcome.MANUAL_REVIEW
    ctx.exchange_client.mark_as_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_intentional_skip_marks_exchange_read_without_constructing_delivery():
    email_id = "mark-read-skipped"
    ctx = _context(email_id)
    projection = _projection(email_id, need_reply=False)
    projection["classification"] = {
        "need_reply": False,
        "priority": "P3",
        "intent": "垃圾邮件",
    }

    outcome = await _run_with_projection(ctx, email_id, projection)

    assert outcome is ProcessingOutcome.PROCESSED
    ctx.email_feishu_delivery.deliver.assert_not_awaited()
    ctx.exchange_client.mark_as_read.assert_awaited_once_with(email_id, is_read=True)


@pytest.mark.asyncio
async def test_mark_read_cancellation_does_not_replay_confirmed_delivery():
    email_id = "mark-read-cancelled"
    ctx = _context(email_id)
    cancellation = asyncio.CancelledError()
    ctx.email_feishu_delivery.deliver.return_value = EmailDeliveryOutcome(
        EmailDeliveryKind.APPROVAL,
        EmailDeliveryDisposition.CONFIRMED,
        pdf_token="review-pdf",
    )
    ctx.exchange_client.mark_as_read.side_effect = cancellation

    with pytest.raises(asyncio.CancelledError) as caught:
        await _run_with_projection(ctx, email_id, _projection(email_id))

    assert caught.value is cancellation
    ctx.email_feishu_delivery.deliver.assert_awaited_once()
