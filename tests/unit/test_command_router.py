import pytest
import logging

from src.commands.router import CommandRouter


@pytest.mark.asyncio
async def test_dispatch_known_command():
    router = CommandRouter()

    async def mock_handler(args):
        return f"got: {args}"

    router.register("/test", mock_handler)
    result = await router.dispatch("/test hello")
    assert result == "got: hello"


@pytest.mark.asyncio
async def test_dispatch_unknown_command():
    router = CommandRouter()
    result = await router.dispatch("/unknown")
    assert "未知指令" in result


@pytest.mark.asyncio
async def test_dispatch_non_command():
    router = CommandRouter()
    result = await router.dispatch("hello world")
    assert result is None


@pytest.mark.asyncio
async def test_dispatch_failure_never_logs_or_replies_with_exception_text(caplog):
    router = CommandRouter()
    secret = "private-command-exception-sentinel"

    async def failing_handler(_args):
        raise RuntimeError(secret)

    router.register("/fail", failing_handler)
    with caplog.at_level(logging.ERROR, logger="src.commands.router"):
        result = await router.dispatch("/fail")

    assert result == "指令执行失败，请稍后重试"
    assert secret not in caplog.text
    assert "RuntimeError" in caplog.text
