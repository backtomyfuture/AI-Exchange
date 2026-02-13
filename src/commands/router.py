import logging
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

CommandReply = str | dict[str, Any] | None
CommandHandler = Callable[[str], Awaitable[CommandReply]]


class CommandRouter:
    def __init__(self):
        self._commands: dict[str, CommandHandler] = {}

    def register(self, name: str, handler: CommandHandler):
        self._commands[name.lower()] = handler

    async def dispatch(self, text: str) -> Optional[CommandReply]:
        text = (text or "").strip()
        if not text.startswith("/"):
            return None

        parts = text.split(maxsplit=1)
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        handler = self._commands.get(cmd)
        if handler is None:
            return f"未知指令: {cmd}\n发送 /help 查看可用指令"

        try:
            return await handler(args)
        except Exception as e:
            logger.error("Command %s failed: %s", cmd, e)
            return f"指令执行失败: {e}"
