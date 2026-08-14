from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.daily_digest import (
    ClaimedDigestPart,
    DailyDigestExecution,
    DailyDigestScheduler,
    DailyDigestSnapshot,
    DailyDigestWindow,
    DigestEmailItem,
    _email_item_from_row,
    delivery_scope_hash,
    render_daily_digest,
)
from src.utils.lark_messaging import (
    LarkTextDelivery,
    LarkTextReconciliationUnavailable,
    find_daily_digest_headers,
    send_daily_digest_text,
)


def _window() -> DailyDigestWindow:
    return DailyDigestWindow(
        datetime(2026, 8, 4, 10, tzinfo=UTC),
        datetime(2026, 8, 5, 10, tzinfo=UTC),
    )


def _snapshot(*, emails=(), backlog=(), missed=()) -> DailyDigestSnapshot:
    return DailyDigestSnapshot(
        window=_window(),
        emails=tuple(emails),
        historical_backlog=tuple(backlog),
        missed_windows=tuple(missed),
        ready=True,
        processing_active=True,
        polling_active=True,
        polling_cursor_ready=True,
    )


def test_reporting_window_uses_completed_shanghai_18_to_18_boundary() -> None:
    before_boundary = DailyDigestWindow.latest_completed(
        datetime(2026, 8, 5, 9, 59, tzinfo=UTC)
    )
    at_boundary = DailyDigestWindow.latest_completed(
        datetime(2026, 8, 5, 10, 0, tzinfo=UTC)
    )

    assert before_boundary.label == "2026-08-03 18:00~2026-08-04 18:00"
    assert at_boundary.label == "2026-08-04 18:00~2026-08-05 18:00"


def test_silent_share_alert_marks_digest_header() -> None:
    snapshot = _snapshot()
    object.__setattr__(snapshot, "silent_share_alert", True)
    object.__setattr__(snapshot, "silent_share", 0.8)
    object.__setattr__(snapshot, "silent_baseline_share", 0.1)
    object.__setattr__(snapshot, "silent_count", 8)
    header, text = render_daily_digest(snapshot, is_backfill=False, max_bytes=12_000)[0]
    assert header.startswith("【需关注】")
    assert "静默路由占比偏离基线" in text


def test_zero_volume_digest_is_plain_text_and_has_all_three_sections() -> None:
    header, text = render_daily_digest(
        _snapshot(),
        is_backfill=False,
        max_bytes=12_000,
    )[0]

    assert text.startswith(f"{header}\n")
    assert "今日概况" in text
    assert "需关注事项" in text
    assert "邮件清单" in text
    assert "收到邮件：0" in text
    assert "【需关注】" not in header
    assert "今日无新邮件" in text
    assert "interactive" not in text
    assert "card" not in text.lower()


def test_digest_lists_metadata_and_next_action_but_not_an_email_body() -> None:
    item = DigestEmailItem(
        received_at=datetime(2026, 8, 5, 2, 30, tzinfo=UTC),
        sender="领导 <leader@example.test>",
        subject="请确认航班收益方案",
        status="waiting_approval",
    )
    text = render_daily_digest(
        _snapshot(emails=(item,)),
        is_backfill=False,
        max_bytes=12_000,
    )[0][1]

    assert "领导 <leader@example.test>" in text
    assert "请确认航班收益方案" in text
    assert "待审批" in text
    assert "请在飞书完成审批" in text
    assert "邮件正文不应出现" not in text


def _item_from_row(*, sender: str, subject: str, status: str) -> DigestEmailItem:
    return _email_item_from_row(
        {
            "received_at": datetime(2026, 8, 5, 4, 23, tzinfo=UTC),
            "sender": sender,
            "subject": subject,
            "status": status,
        }
    )


def test_serialized_mailbox_sender_is_rendered_as_a_readable_name() -> None:
    item = _item_from_row(
        sender=(
            "Mailbox(name='武珉（Annie）', email_address='m.wu@tianjin-air.com', "
            "routing_type='SMTP', mailbox_type='Mailbox')"
        ),
        subject="FW: 请阅处：关于组织开展中秋、国庆双节福利实物礼包论坛票选活动的通知",
        status="manual_review",
    )
    text = render_daily_digest(
        _snapshot(emails=(item,)),
        is_backfill=False,
        max_bytes=12_000,
    )[0][1]

    assert "武珉（Annie）" in text
    assert "Mailbox(" not in text
    assert "email_address" not in text
    assert "routing_type" not in text


def test_long_mailbox_name_is_parsed_before_sender_truncation() -> None:
    long_name = "天津航空有限责任公司市场营销委员会收益管理中心党总支书记"
    item = _item_from_row(
        sender=(
            f"Mailbox(name='{long_name}', email_address='m.wu@tianjin-air.com', "
            "routing_type='SMTP', mailbox_type='Mailbox')"
        ),
        subject="长名字发件人",
        status="manual_review",
    )
    text = render_daily_digest(
        _snapshot(emails=(item,)),
        is_backfill=False,
        max_bytes=12_000,
    )[0][1]

    assert long_name in text
    assert "Mailbox(" not in text


def test_serialized_mailbox_sender_falls_back_to_address_when_name_is_empty() -> None:
    item = _item_from_row(
        sender=(
            "Mailbox(name='', email_address='zhang-xia@tianjin-air.com', "
            "routing_type='SMTP', mailbox_type='Mailbox')"
        ),
        subject="转发: 呈阅知：关于基地园区西侧车位封控施工的通知",
        status="no_action",
    )
    text = render_daily_digest(
        _snapshot(emails=(item,)),
        is_backfill=False,
        max_bytes=12_000,
    )[0][1]

    assert "zhang-xia@tianjin-air.com" in text
    assert "Mailbox(" not in text


def test_plain_sender_text_is_rendered_unchanged() -> None:
    item = _item_from_row(
        sender="sender@example.test",
        subject="普通发件人",
        status="no_action",
    )
    text = render_daily_digest(
        _snapshot(emails=(item,)),
        is_backfill=False,
        max_bytes=12_000,
    )[0][1]

    assert "sender@example.test" in text


def test_draft_saved_is_not_an_attention_item_and_backlog_is_labelled() -> None:
    draft = DigestEmailItem(
        received_at=datetime(2026, 8, 5, 2, tzinfo=UTC),
        sender="sender@example.test",
        subject="已保存草稿",
        status="draft_saved",
    )
    backlog = DigestEmailItem(
        received_at=datetime(2026, 8, 3, 2, tzinfo=UTC),
        sender="old@example.test",
        subject="历史待审批邮件",
        status="waiting_approval",
    )
    text = render_daily_digest(
        _snapshot(emails=(draft,), backlog=(backlog,)),
        is_backfill=True,
        max_bytes=12_000,
    )[0][1]
    attention, email_list = text.split("邮件清单", 1)

    assert "补发：原报告窗口" in text
    assert "历史积压 |" in attention
    assert "已保存草稿" not in attention
    assert "已保存草稿" in email_list


def test_unconfirmed_or_failed_send_states_are_attention_items() -> None:
    items = (
        DigestEmailItem(
            received_at=datetime(2026, 8, 5, 2, tzinfo=UTC),
            sender="queue@example.test",
            subject="等待发送",
            status="send_queued",
        ),
        DigestEmailItem(
            received_at=datetime(2026, 8, 5, 3, tzinfo=UTC),
            sender="failure@example.test",
            subject="投递失败",
            status="delivery_failed",
        ),
    )

    text = render_daily_digest(
        _snapshot(emails=items),
        is_backfill=False,
        max_bytes=12_000,
    )[0][1]
    attention, _email_list = text.split("邮件清单", 1)

    assert "等待发送" in attention
    assert "投递失败" in attention
    assert "失败或积压：2" in text


def test_large_digest_is_split_into_bounded_numbered_text_parts() -> None:
    emails = tuple(
        DigestEmailItem(
            received_at=datetime(2026, 8, 5, 2, minute % 60, tzinfo=UTC),
            sender=f"sender-{minute}@example.test",
            subject="需要跟进的邮件" * 24,
            status="manual_review",
        )
        for minute in range(40)
    )

    parts = render_daily_digest(_snapshot(emails=emails), is_backfill=False, max_bytes=1024)

    assert len(parts) > 1
    for index, (header, text) in enumerate(parts, start=1):
        assert header.startswith("【邮件日报 ")
        assert f"第 {index}/{len(parts)} 部分" in header
        assert text.startswith(f"{header}\n")
        assert len(text.encode("utf-8")) <= 1024


def test_scope_hash_is_stable_and_does_not_reveal_chat_identifier() -> None:
    chat_id = "oc_private_daily_digest_chat"

    hashed = delivery_scope_hash(chat_id)

    assert hashed == delivery_scope_hash(chat_id)
    assert len(hashed) == 64
    assert chat_id not in hashed


def test_daily_digest_sender_uses_a_lark_text_message_with_stable_uuid() -> None:
    response = MagicMock()
    response.success.return_value = True
    response.data.message_id = "message-1"
    create = MagicMock(return_value=response)
    client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(create=create)))
    )

    outcome = send_daily_digest_text(
        "【邮件日报 2026-08-04 18:00~2026-08-05 18:00】\n今日概况",
        request_uuid="6dfb0f00-58be-5100-b4ad-5f39d4e317bd",
        chat_id="oc_daily",
        lark_api_client=client,
    )

    request = create.call_args.args[0]
    payload = json.loads(request.request_body.content)
    assert outcome == LarkTextDelivery(True, True, "message-1")
    assert request.request_body.msg_type == "text"
    assert request.request_body.uuid == "6dfb0f00-58be-5100-b4ad-5f39d4e317bd"
    assert payload == {
        "text": "【邮件日报 2026-08-04 18:00~2026-08-05 18:00】\n今日概况"
    }


def test_reconciliation_returns_only_matching_bot_digest_headers() -> None:
    header = "【邮件日报 2026-08-04 18:00~2026-08-05 18:00】"
    response = SimpleNamespace(
        success=lambda: True,
        data=SimpleNamespace(
            items=(
                SimpleNamespace(
                    msg_type="text",
                    sender=SimpleNamespace(sender_type="app"),
                    body=SimpleNamespace(content=json.dumps({"text": f"{header}\n概况"})),
                ),
                SimpleNamespace(
                    msg_type="text",
                    sender=SimpleNamespace(sender_type="user"),
                    body=SimpleNamespace(content=json.dumps({"text": f"{header}\n伪造"})),
                ),
                SimpleNamespace(
                    msg_type="text",
                    sender=SimpleNamespace(sender_type="app"),
                    body=SimpleNamespace(content=json.dumps({"text": "无关聊天内容"})),
                ),
            ),
            has_more=False,
            page_token=None,
        ),
    )
    list_messages = MagicMock(return_value=response)
    client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(list=list_messages)))
    )

    found = find_daily_digest_headers(
        {header},
        not_before=datetime(2026, 8, 5, 10, tzinfo=UTC),
        not_after=datetime(2026, 8, 5, 11, tzinfo=UTC),
        chat_id="oc_daily",
        lark_api_client=client,
    )

    assert found == {header}
    request = list_messages.call_args.args[0]
    assert request.container_id == "oc_daily"


def test_reconciliation_fails_closed_when_the_bounded_chat_scan_is_incomplete() -> None:
    response = SimpleNamespace(
        success=lambda: True,
        data=SimpleNamespace(items=(), has_more=True, page_token="next-page"),
    )
    client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(message=SimpleNamespace(list=MagicMock(return_value=response)))
        )
    )

    with pytest.raises(LarkTextReconciliationUnavailable, match="incomplete"):
        find_daily_digest_headers(
            {"【邮件日报 2026-08-04 18:00~2026-08-05 18:00】"},
            not_before=datetime(2026, 8, 5, 10, tzinfo=UTC),
            not_after=datetime(2026, 8, 5, 11, tzinfo=UTC),
            chat_id="oc_daily",
            lark_api_client=client,
            max_pages=1,
        )


@pytest.mark.asyncio
async def test_scheduler_marks_a_late_new_execution_as_backfill_without_sending_twice() -> None:
    class Repository:
        def __init__(self) -> None:
            self.expired_cutoffs = []
            self.ensure_kwargs = None

        async def mark_expired_executions_missed(self, cutoff):
            self.expired_cutoffs.append(cutoff)

        async def get_execution(self, _window):
            return None

        async def build_snapshot(self, window, *, health):
            assert health.ready is True
            return _snapshot()

        async def ensure_execution(self, snapshot, **kwargs):
            self.ensure_kwargs = kwargs
            return DailyDigestExecution(
                account_id=8,
                delivery_scope_hash="a" * 64,
                window=snapshot.window,
                state="confirmed",
                is_backfill=kwargs["is_backfill"],
                parts=(),
            )

    scheduler = DailyDigestScheduler(
        database=object(),
        account_id=8,
        chat_id="oc_daily",
        health_snapshot=lambda: SimpleNamespace(
            ready=True,
            processing_active=True,
            polling_active=True,
            polling_cursor_ready=True,
        ),
    )
    repository = Repository()
    scheduler._repository = repository

    await scheduler.run_due(now=datetime(2026, 8, 5, 13, tzinfo=UTC))

    assert repository.ensure_kwargs == {
        "is_backfill": True,
        "max_bytes": 12_000,
    }
    assert repository.expired_cutoffs == [
        datetime(2026, 8, 4, 10, tzinfo=UTC)
    ]


@pytest.mark.asyncio
async def test_scheduler_marks_transport_uncertainty_for_reconciliation() -> None:
    class Repository:
        def __init__(self) -> None:
            self.unknown = []

        async def reconciliation_candidates(self, *_args, **_kwargs):
            return None

        async def claim_next_part(self, *_args, **_kwargs):
            if self.unknown:
                return None
            return ClaimedDigestPart(
                index=0,
                header="【邮件日报 2026-08-04 18:00~2026-08-05 18:00】",
                text="日报",
                request_uuid="6dfb0f00-58be-5100-b4ad-5f39d4e317bd",
            )

        async def mark_delivery_unknown(self, _window, part, **_kwargs):
            self.unknown.append(part)

        async def mark_delivery_rejected(self, *_args, **_kwargs):
            raise AssertionError("transport uncertainty must not be a direct retry")

        async def mark_delivery_confirmed(self, *_args, **_kwargs):
            raise AssertionError("transport uncertainty must not be confirmed")

    scheduler = DailyDigestScheduler(
        database=object(),
        account_id=8,
        chat_id="oc_daily",
        health_snapshot=lambda: SimpleNamespace(),
        sender=lambda *_args, **_kwargs: LarkTextDelivery(False, False),
    )
    repository = Repository()
    scheduler._repository = repository

    await scheduler._deliver_pending_execution(_window())

    assert len(repository.unknown) == 1
