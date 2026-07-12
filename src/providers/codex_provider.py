"""OpenAI Codex provider — OAuth 认证，兼容 LangChain。

改编自 nanobot 的 openai_codex_provider.py，重写为继承 OAuthChatModel
而非 nanobot 的 LLMProvider。使用 oauth_cli_kit 管理 token，
通过 Codex Responses API 的 SSE 流式响应获取内容。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, AsyncGenerator

import httpx
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage, AIMessage

from src.providers.oauth_base import OAuthChatModel

logger = logging.getLogger(__name__)

DEFAULT_CODEX_URL = "https://chatgpt.com/backend-api/codex/responses"


def _get_codex_token():
    """导入并调用 oauth_cli_kit，独立函数便于测试。"""
    from oauth_cli_kit import get_token
    return get_token()


class CodexChatModel(OAuthChatModel):
    """通过 OAuth（oauth_cli_kit）接入 OpenAI Codex。"""

    model_name: str = "openai-codex/gpt-5.1-codex"

    @property
    def _llm_type(self) -> str:
        return "openai-codex"

    async def _get_token(self) -> dict:
        token = await asyncio.to_thread(_get_codex_token)
        return {"access_token": token.access, "account_id": token.account_id}

    async def _call_api(
        self, messages: list[BaseMessage], token: dict, **kwargs: Any
    ) -> str:
        system_prompt, input_items = _convert_lc_messages(messages)
        headers = _build_headers(token["account_id"], token["access_token"])

        body: dict[str, Any] = {
            "model": _strip_model_prefix(self.model_name),
            "store": False,
            "stream": True,
            "instructions": system_prompt,
            "input": input_items,
            "text": {"verbosity": "medium"},
            "include": ["reasoning.encrypted_content"],
            "prompt_cache_key": _prompt_cache_key(input_items),
        }

        try:
            return await _request_codex(
                DEFAULT_CODEX_URL,
                headers,
                body,
                verify=True,
            )
        except Exception as exc:
            logger.error("Codex API failed: error_type=%s", type(exc).__name__)
            raise


def _strip_model_prefix(model: str) -> str:
    if model.startswith("openai-codex/") or model.startswith("openai_codex/"):
        return model.split("/", 1)[1]
    return model


def _build_headers(account_id: str, access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "chatgpt-account-id": account_id,
        "OpenAI-Beta": "responses=experimental",
        "originator": "ai-exchange",
        "User-Agent": "ai-exchange (python)",
        "accept": "text/event-stream",
        "content-type": "application/json",
    }


def _convert_lc_messages(
    messages: list[BaseMessage],
) -> tuple[str, list[dict[str, Any]]]:
    """将 LangChain 消息转换为 Codex Responses API 格式。"""
    system_prompt = ""
    input_items: list[dict[str, Any]] = []

    for idx, msg in enumerate(messages):
        if isinstance(msg, SystemMessage):
            system_prompt = msg.content if isinstance(msg.content, str) else ""
            continue

        if isinstance(msg, HumanMessage):
            text = msg.content if isinstance(msg.content, str) else ""
            input_items.append({
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            })
            continue

        if isinstance(msg, AIMessage):
            text = msg.content if isinstance(msg.content, str) else ""
            if text:
                input_items.append({
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": text}],
                    "status": "completed",
                    "id": f"msg_{idx}",
                })
            continue

    return system_prompt, input_items


def _prompt_cache_key(input_items: list[dict[str, Any]]) -> str:
    raw = json.dumps(input_items, ensure_ascii=True, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def _request_codex(
    url: str, headers: dict, body: dict, verify: bool
) -> str:
    if verify is not True:
        raise ValueError("Codex TLS verification must remain enabled")
    async with httpx.AsyncClient(timeout=120.0, verify=verify) as client:
        async with client.stream("POST", url, headers=headers, json=body) as resp:
            if resp.status_code != 200:
                raise RuntimeError(_friendly_error(resp.status_code))
            return await _consume_sse(resp)


async def _iter_sse(response: httpx.Response) -> AsyncGenerator[dict[str, Any], None]:
    buffer: list[str] = []
    async for line in response.aiter_lines():
        if line == "":
            if buffer:
                data_lines = [
                    data_line[5:].strip()
                    for data_line in buffer
                    if data_line.startswith("data:")
                ]
                buffer = []
                if not data_lines:
                    continue
                data = "\n".join(data_lines).strip()
                if not data or data == "[DONE]":
                    continue
                try:
                    yield json.loads(data)
                except Exception:
                    continue
            continue
        buffer.append(line)


async def _consume_sse(response: httpx.Response) -> str:
    content = ""
    async for event in _iter_sse(response):
        event_type = event.get("type")
        if event_type == "response.output_text.delta":
            content += event.get("delta") or ""
        elif event_type in {"error", "response.failed"}:
            raise RuntimeError("Codex 响应失败")
    return content


def _friendly_error(status_code: int) -> str:
    if status_code == 429:
        return "ChatGPT 使用配额超限或触发速率限制。"
    return f"Codex API request failed (HTTP {status_code})"
