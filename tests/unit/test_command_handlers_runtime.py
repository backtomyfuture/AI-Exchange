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
