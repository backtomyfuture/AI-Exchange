from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.commands import handlers
from src.commands.router import CommandRouter


@pytest.mark.asyncio
async def test_health_command_reports_the_durable_runtime_queue(monkeypatch) -> None:
    runtime = SimpleNamespace(
        check_ready=AsyncMock(return_value=True),
        queue_stats=AsyncMock(
            return_value=SimpleNamespace(
                pending=2,
                retry_wait=3,
                leased=4,
                manual_review=5,
                dead_letter=6,
            )
        ),
    )
    monkeypatch.setattr(
        "src.init_app.get_runtime_app_context",
        lambda: SimpleNamespace(ingestion_runtime=runtime),
    )
    monkeypatch.setattr(handlers, "_db_manager", object())
    router = CommandRouter()
    router.register("/health", handlers.handle_health)

    reply = await router.dispatch("/health")

    assert reply == (
        "🏥 系统健康状态:\n"
        "  数据库: 🟢\n"
        "  运行时: 🟢 READY\n"
        "  队列: 待处理 2，重试等待 3，处理中 4，人工复核 5，死信 6"
    )


@pytest.mark.asyncio
async def test_queue_command_reports_durable_inbox_counts(monkeypatch) -> None:
    runtime = SimpleNamespace(
        check_ready=AsyncMock(return_value=True),
        queue_stats=AsyncMock(
            return_value=SimpleNamespace(
                pending=2,
                retry_wait=3,
                leased=4,
                manual_review=5,
                dead_letter=6,
            )
        ),
    )
    monkeypatch.setattr(
        "src.init_app.get_runtime_app_context",
        lambda: SimpleNamespace(ingestion_runtime=runtime),
    )
    router = CommandRouter()
    router.register("/queue", handlers.handle_queue)

    reply = await router.dispatch("/queue")

    assert reply == (
        "📦 队列状态:\n"
        "  待处理: 2\n"
        "  重试等待: 3\n"
        "  处理中: 4\n"
        "  人工复核: 5\n"
        "  死信: 6"
    )


@pytest.mark.asyncio
async def test_search_command_deduplicates_chunks_and_formats_sender(monkeypatch) -> None:
    raw_sender = (
        "Mailbox(name='武珉（Annie）', email_address='m.wu@tianjin-air.com', "
        "routing_type='SMTP', mailbox_type='Mailbox')"
    )
    candidates = [
        {
            "id": "mail-one",
            "subject": "中秋、国庆双节福利实物礼包论坛票选活动",
            "sender": raw_sender,
        }
        for _ in range(5)
    ] + [
        {
            "id": "mail-two",
            "subject": "第二封不同的礼包邮件",
            "sender": "第二位发件人 <second@example.com>",
        }
    ]

    class Retriever:
        def search(self, *, query_text: str, limit: int):
            assert query_text == "礼包"
            return candidates[:limit]

    monkeypatch.setattr("src.utils.retriever.get_retriever", lambda: Retriever())
    router = CommandRouter()
    router.register("/search", handlers.handle_search)

    reply = await router.dispatch("/search 礼包")

    assert reply == (
        "🔍 搜索结果 (2 条):\n\n"
        "  · [中秋、国庆双节福利实物礼包论坛票选活动] from 武珉（Annie） <m.wu@tianjin-air.com>\n"
        "  · [第二封不同的礼包邮件] from 第二位发件人 <second@example.com>"
    )
