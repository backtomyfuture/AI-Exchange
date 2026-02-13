import pytest

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
