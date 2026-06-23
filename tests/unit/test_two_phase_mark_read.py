"""
C1: Two-phase mark_as_read tests.

Verify that an email is only marked as read on Exchange AFTER the user-facing
delivery (Lark card / explicit skip) succeeds. Card delivery failure must
leave the email unread on the server so SelfHealer / human can retry.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.exchange_service import _dispatch_notification, process_and_archive_email


@pytest.fixture
def ctx():
    c = MagicMock()
    c.db_manager = AsyncMock()
    c.email_processor = MagicMock()
    c.graph = AsyncMock()
    c.exchange_client = AsyncMock()
    return c


def _pipeline_result(need_reply=True, priority="P1", intent="审批"):
    return {
        "classification": {"need_reply": need_reply, "priority": priority, "intent": intent},
        "draft": "draft body",
        "context": [],
        "email": {"id": "msg-c1", "subject": "s"},
        "routing_log": [],
        "active_skills": [],
    }


@pytest.mark.asyncio
async def test_dispatch_returns_delivered_true_when_card_succeeds(ctx):
    with patch("src.exchange_service.lark_app.send_approval_card", return_value=True), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )
    assert result == {"delivered": True, "kind": "approval"}
    ctx.db_manager.update_status.assert_any_call("msg-c1", "waiting_approval")


@pytest.mark.asyncio
async def test_dispatch_returns_delivered_false_on_card_failure(ctx):
    with patch("src.exchange_service.lark_app.send_approval_card", return_value=False), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        result = await _dispatch_notification(
            "msg-c1", _pipeline_result(need_reply=True), ctx, {}
        )
    assert result == {"delivered": False, "kind": "approval"}
    # status should be 'delivery_failed' instead of 'waiting_approval'
    failed_calls = [
        c for c in ctx.db_manager.update_status.call_args_list
        if "delivery_failed" in c.args
    ]
    assert failed_calls, "Expected status update to delivery_failed"


@pytest.mark.asyncio
async def test_dispatch_read_only_failure_marks_delivery_failed(ctx):
    with patch("src.exchange_service.lark_app.send_read_only_card", return_value=False), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        result = await _dispatch_notification(
            "msg-c1",
            _pipeline_result(need_reply=False, priority="P1", intent="通知"),
            ctx,
            {},
        )
    assert result == {"delivered": False, "kind": "read_only"}


@pytest.mark.asyncio
async def test_dispatch_skipped_path_treats_as_delivered(ctx):
    """P3/non-notification mails are intentionally not surfaced - count as delivered."""
    result = await _dispatch_notification(
        "msg-c1",
        _pipeline_result(need_reply=False, priority="P3", intent="垃圾邮件"),
        ctx,
        {},
    )
    assert result == {"delivered": True, "kind": "skipped"}
    ctx.db_manager.update_status.assert_any_call("msg-c1", "skipped")


@pytest.mark.asyncio
async def test_process_email_skips_mark_read_when_dispatch_fails(ctx):
    """Card delivery failure must leave the email unread on Exchange."""
    ctx.db_manager.log_initial_email.return_value = True

    async def mock_astream(*a, **k):
        yield {"categorizer": {"classification": {"need_reply": True, "priority": "P1"}}}
        yield {"drafter": {"draft": "d"}}
    ctx.graph.astream = mock_astream

    final_state = MagicMock()
    final_state.values = {
        "classification": {"need_reply": True, "priority": "P1", "intent": "审批"},
        "draft": "d",
        "context": [],
        "email": {"id": "msg-fail"},
        "routing_log": [],
        "active_skills": [],
    }
    ctx.graph.aget_state.return_value = final_state

    with patch("src.exchange_service.lark_app.send_approval_card", return_value=False), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        await process_and_archive_email({"id": "msg-fail"}, ctx)

    ctx.exchange_client.mark_as_read.assert_not_called()


@pytest.mark.asyncio
async def test_process_email_marks_read_only_after_successful_dispatch(ctx):
    ctx.db_manager.log_initial_email.return_value = True

    async def mock_astream(*a, **k):
        yield {"categorizer": {"classification": {"need_reply": True, "priority": "P1"}}}
        yield {"drafter": {"draft": "d"}}
    ctx.graph.astream = mock_astream

    final_state = MagicMock()
    final_state.values = {
        "classification": {"need_reply": True, "priority": "P1", "intent": "审批"},
        "draft": "d",
        "context": [],
        "email": {"id": "msg-ok"},
        "routing_log": [],
        "active_skills": [],
    }
    ctx.graph.aget_state.return_value = final_state
    ctx.exchange_client.mark_as_read.return_value = True

    with patch("src.exchange_service.lark_app.send_approval_card", return_value=True), \
         patch("src.exchange_service.lark_app.generate_and_upload_pdf",
               new=AsyncMock(return_value=None)):
        await process_and_archive_email({"id": "msg-ok"}, ctx)

    ctx.exchange_client.mark_as_read.assert_called_once_with("msg-ok", is_read=True)


@pytest.mark.asyncio
async def test_self_healer_picks_up_delivery_failed():
    """SelfHealer's stuck list query must include delivery_failed status."""
    from src.utils import self_healing
    assert "delivery_failed" in self_healing.STUCK_STATUSES
