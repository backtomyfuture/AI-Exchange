"""OAuth-based LangChain chat model base class.

子类实现 _get_token() 和 _call_api()，支持 OAuth 认证的 LLM provider
（OpenAI Codex、Gemini CLI）作为 ChatOpenAI 的直接替换，LangGraph 节点
代码零改动。
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from typing import Any

from langchain_core.callbacks import CallbackManagerForLLMRun, AsyncCallbackManagerForLLMRun
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage, AIMessage
from langchain_core.outputs import ChatResult, ChatGeneration

logger = logging.getLogger(__name__)


class OAuthChatModel(BaseChatModel):
    """OAuth 认证 LLM provider 的基类。

    继承 LangChain 的 BaseChatModel，与 ChatOpenAI 无缝替换。
    子类只需实现：
      - _get_token(): 获取/刷新 OAuth 凭据
      - _call_api(): 使用 token 调用模型 API
    """

    model_name: str = ""
    temperature: float = 0.7

    @abstractmethod
    async def _get_token(self) -> dict:
        """返回 OAuth 凭据，如已过期则刷新。"""
        ...

    @abstractmethod
    async def _call_api(
        self, messages: list[BaseMessage], token: dict, **kwargs: Any
    ) -> str:
        """使用 OAuth token 调用 LLM API，返回响应文本。"""
        ...

    @property
    def _llm_type(self) -> str:
        return "oauth"

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        raise NotImplementedError(
            "OAuthChatModel 仅支持异步调用，请使用 ainvoke() 而非 invoke()。"
        )

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        token = await self._get_token()
        content = await self._call_api(messages, token, **kwargs)
        message = AIMessage(content=content)
        return ChatResult(generations=[ChatGeneration(message=message)])
