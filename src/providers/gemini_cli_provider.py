"""Gemini CLI provider — OAuth 认证，兼容 LangChain。

从 ~/.gemini/oauth_creds.json 读取 OAuth 凭据（由 Gemini CLI
`gemini auth login` 写入）。通过 Google OAuth2 端点刷新 access token，
调用 Gemini OpenAI 兼容 API。
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage

from src.providers.oauth_base import OAuthChatModel

logger = logging.getLogger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/openai"

# token 缓存，避免每次调用都刷新
_token_cache: dict[str, Any] = {}


def _find_credentials_path() -> Path:
    """定位 Gemini CLI OAuth 凭据文件路径。"""
    return Path.home() / ".gemini" / "oauth_creds.json"


def _strip_model_prefix(model: str) -> str:
    if model.startswith("gemini-cli/") or model.startswith("gemini_cli/"):
        return model.split("/", 1)[1]
    return model


class GeminiCliChatModel(OAuthChatModel):
    """通过 Gemini CLI 本地 OAuth 凭据接入 Gemini。"""

    model_name: str = "gemini-cli/gemini-2.5-flash"

    @property
    def _llm_type(self) -> str:
        return "gemini-cli"

    async def _get_token(self) -> dict:
        # 如果缓存 token 仍在有效期内（保留 60 秒余量），直接返回
        if _token_cache.get("access_token") and _token_cache.get("expires_at", 0) > time.time() + 60:
            return _token_cache

        creds_path = _find_credentials_path()
        with open(creds_path) as f:
            creds = json.load(f)

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "client_id": creds["client_id"],
                    "client_secret": creds["client_secret"],
                    "refresh_token": creds["refresh_token"],
                    "grant_type": "refresh_token",
                },
            )

        if resp.status_code != 200:
            raise RuntimeError(f"Google OAuth token 刷新失败: {resp.status_code} {resp.text}")

        token_data = resp.json()
        _token_cache["access_token"] = token_data["access_token"]
        _token_cache["expires_at"] = time.time() + token_data.get("expires_in", 3600)

        return _token_cache

    async def _call_api(
        self, messages: list[BaseMessage], token: dict, **kwargs: Any
    ) -> str:
        model = _strip_model_prefix(self.model_name)
        openai_messages = _convert_to_openai_format(messages)

        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"{GEMINI_API_BASE}/chat/completions",
                headers={
                    "Authorization": f"Bearer {token['access_token']}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": openai_messages,
                    "temperature": self.temperature,
                },
            )

        if resp.status_code != 200:
            raise RuntimeError(f"Gemini API 错误: {resp.status_code} {resp.text}")

        data = resp.json()
        return data["choices"][0]["message"]["content"]


def _convert_to_openai_format(messages: list[BaseMessage]) -> list[dict[str, str]]:
    """将 LangChain 消息转换为 OpenAI chat 格式。"""
    result = []
    for msg in messages:
        content = msg.content if isinstance(msg.content, str) else ""
        if isinstance(msg, SystemMessage):
            result.append({"role": "system", "content": content})
        elif isinstance(msg, HumanMessage):
            result.append({"role": "user", "content": content})
        else:
            result.append({"role": "assistant", "content": content})
    return result
