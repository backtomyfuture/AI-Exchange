# OAuth Provider Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add OAuth-based LLM provider support (OpenAI Codex, Gemini CLI) to ai-exchange, enabling per-role model switching to OAuth models with zero changes to graph nodes.

**Architecture:** Extend existing `ProviderSpec` with `is_oauth`/`is_direct` flags. Create `OAuthChatModel` base class inheriting LangChain's `BaseChatModel` so OAuth providers are seamless drop-in replacements for `ChatOpenAI`. Factory gains an OAuth branch that lazy-imports provider classes.

**Tech Stack:** LangChain `BaseChatModel`, `oauth_cli_kit` (for Codex), `httpx` (for Gemini CLI token refresh + API calls), SSE streaming.

---

### Task 1: Extend ProviderSpec with OAuth fields

**Files:**
- Modify: `src/providers/registry.py:18-49`
- Test: `tests/unit/test_provider_registry.py`

**Step 1: Write the failing test**

Create `tests/unit/test_provider_registry.py`:

```python
"""Provider registry tests — OAuth extension."""

from src.providers.registry import ProviderSpec, match_provider, PROVIDERS


class TestProviderSpecOAuthFields:
    """Test that ProviderSpec has is_oauth and is_direct fields."""

    def test_default_is_oauth_false(self):
        spec = ProviderSpec(name="test")
        assert spec.is_oauth is False

    def test_default_is_direct_false(self):
        spec = ProviderSpec(name="test")
        assert spec.is_direct is False

    def test_oauth_provider_spec(self):
        spec = ProviderSpec(name="test_oauth", is_oauth=True, is_direct=True)
        assert spec.is_oauth is True
        assert spec.is_direct is True


class TestOAuthProviderRegistration:
    """Test that OAuth providers are registered and matchable."""

    def test_codex_in_registry(self):
        names = [p.name for p in PROVIDERS]
        assert "openai_codex" in names

    def test_gemini_cli_in_registry(self):
        names = [p.name for p in PROVIDERS]
        assert "gemini_cli" in names

    def test_codex_match_by_keyword(self):
        spec = match_provider("openai-codex/gpt-5.1-codex")
        assert spec is not None
        assert spec.name == "openai_codex"
        assert spec.is_oauth is True

    def test_gemini_cli_match_by_keyword(self):
        spec = match_provider("gemini-cli/gemini-2.5-flash")
        assert spec is not None
        assert spec.name == "gemini_cli"
        assert spec.is_oauth is True

    def test_existing_providers_unchanged(self):
        spec = match_provider("gpt-4o")
        assert spec is not None
        assert spec.name == "openai"
        assert spec.is_oauth is False
```

**Step 2: Run test to verify it fails**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_provider_registry.py -v`
Expected: FAIL — `is_oauth` field doesn't exist on ProviderSpec

**Step 3: Implement — add fields and registry entries**

In `src/providers/registry.py`, add two fields to `ProviderSpec` after `model_overrides`:

```python
    is_oauth: bool = False
    is_direct: bool = False
```

Add two entries to `PROVIDERS` tuple, between Chinese Cloud Providers and International Cloud Providers sections:

```python
    # === OAuth Providers (use OAuth flow, bypass ChatOpenAI) ================

    ProviderSpec(
        name="openai_codex",
        display_name="OpenAI Codex",
        keywords=("openai-codex",),
        env_key="",
        default_base_url="https://chatgpt.com/backend-api/codex/responses",
        is_oauth=True,
        is_direct=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="gemini_cli",
        display_name="Gemini CLI",
        keywords=("gemini-cli",),
        env_key="",
        default_base_url="https://generativelanguage.googleapis.com/v1beta/openai",
        is_oauth=True,
        is_direct=True,
        supports_json_mode=True,
        supports_vision=True,
    ),
```

**Step 4: Run test to verify it passes**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_provider_registry.py -v`
Expected: All 7 tests PASS

**Step 5: Commit**

```bash
git add src/providers/registry.py tests/unit/test_provider_registry.py
git commit -m "feat(providers): add is_oauth/is_direct fields and OAuth provider entries"
```

---

### Task 2: Create OAuthChatModel base class

**Files:**
- Create: `src/providers/oauth_base.py`
- Test: `tests/unit/test_oauth_base.py`

**Step 1: Write the failing test**

Create `tests/unit/test_oauth_base.py`:

```python
"""OAuth base chat model tests."""

import pytest
from unittest.mock import AsyncMock, patch
from langchain_core.messages import HumanMessage, AIMessage

from src.providers.oauth_base import OAuthChatModel


class ConcreteOAuthModel(OAuthChatModel):
    """Test implementation of OAuthChatModel."""

    model_name: str = "test-model"

    async def _get_token(self) -> dict:
        return {"access_token": "test-token"}

    async def _call_api(
        self, messages: list, token: dict, **kwargs
    ) -> str:
        return "test response"

    @property
    def _llm_type(self) -> str:
        return "test-oauth"


class TestOAuthChatModel:

    def test_concrete_class_instantiates(self):
        model = ConcreteOAuthModel()
        assert model._llm_type == "test-oauth"

    @pytest.mark.asyncio
    async def test_ainvoke_returns_ai_message(self):
        model = ConcreteOAuthModel()
        result = await model.ainvoke([HumanMessage(content="hello")])
        assert isinstance(result, AIMessage)
        assert result.content == "test response"

    @pytest.mark.asyncio
    async def test_invoke_calls_get_token_then_api(self):
        model = ConcreteOAuthModel()
        model._get_token = AsyncMock(return_value={"access_token": "tok"})
        model._call_api = AsyncMock(return_value="reply")

        result = await model.ainvoke([HumanMessage(content="hi")])

        model._get_token.assert_awaited_once()
        model._call_api.assert_awaited_once()
        assert result.content == "reply"
```

**Step 2: Run test to verify it fails**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_oauth_base.py -v`
Expected: FAIL — `src.providers.oauth_base` not found

**Step 3: Implement OAuthChatModel**

Create `src/providers/oauth_base.py`:

```python
"""OAuth-based LangChain chat model base class.

Subclasses implement _get_token() and _call_api() to support
OAuth providers (OpenAI Codex, Gemini CLI) as drop-in replacements
for ChatOpenAI in LangGraph nodes.
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
    """Base class for OAuth-authenticated LLM providers.

    Integrates with LangChain by implementing BaseChatModel.
    Subclasses only need to provide:
      - _get_token(): obtain/refresh OAuth credentials
      - _call_api(): make the actual API call with the token
    """

    model_name: str = ""
    temperature: float = 0.7

    @abstractmethod
    async def _get_token(self) -> dict:
        """Return OAuth credentials. Refresh if expired."""
        ...

    @abstractmethod
    async def _call_api(
        self, messages: list[BaseMessage], token: dict, **kwargs: Any
    ) -> str:
        """Call the LLM API with OAuth token. Return response text."""
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
            "OAuthChatModel is async-only. Use ainvoke() instead of invoke()."
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
```

**Step 4: Run test to verify it passes**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_oauth_base.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add src/providers/oauth_base.py tests/unit/test_oauth_base.py
git commit -m "feat(providers): add OAuthChatModel base class for LangChain integration"
```

---

### Task 3: Implement Codex provider

**Files:**
- Create: `src/providers/codex_provider.py`
- Test: `tests/unit/test_codex_provider.py`
- Reference: `C:\Users\f1480\Documents\nanobot\nanobot\providers\openai_codex_provider.py`

**Step 1: Write the failing test**

Create `tests/unit/test_codex_provider.py`:

```python
"""OpenAI Codex provider tests.

Tests use mocking — actual OAuth + API calls are not made.
"""

import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage

from src.providers.codex_provider import (
    CodexChatModel,
    _convert_lc_messages,
    _strip_model_prefix,
)


class TestStripModelPrefix:

    def test_strips_openai_codex_prefix(self):
        assert _strip_model_prefix("openai-codex/gpt-5.1-codex") == "gpt-5.1-codex"

    def test_strips_openai_codex_underscore_prefix(self):
        assert _strip_model_prefix("openai_codex/gpt-5.1-codex") == "gpt-5.1-codex"

    def test_no_prefix_unchanged(self):
        assert _strip_model_prefix("gpt-5.1-codex") == "gpt-5.1-codex"


class TestConvertLcMessages:

    def test_system_message_extracted(self):
        msgs = [SystemMessage(content="Be helpful"), HumanMessage(content="Hi")]
        system, items = _convert_lc_messages(msgs)
        assert system == "Be helpful"
        assert len(items) == 1

    def test_human_message_converted(self):
        msgs = [HumanMessage(content="Hello")]
        _, items = _convert_lc_messages(msgs)
        assert items[0]["role"] == "user"
        assert items[0]["content"][0]["type"] == "input_text"

    def test_ai_message_converted(self):
        msgs = [AIMessage(content="Sure")]
        _, items = _convert_lc_messages(msgs)
        assert items[0]["type"] == "message"
        assert items[0]["role"] == "assistant"


class TestCodexChatModel:

    def test_llm_type(self):
        model = CodexChatModel(model_name="openai-codex/gpt-5.1-codex")
        assert model._llm_type == "openai-codex"

    @pytest.mark.asyncio
    async def test_get_token_calls_oauth_cli_kit(self):
        model = CodexChatModel(model_name="openai-codex/gpt-5.1-codex")
        mock_token = MagicMock()
        mock_token.access = "test-access-token"
        mock_token.account_id = "test-account-id"

        with patch(
            "src.providers.codex_provider._get_codex_token",
            return_value=mock_token,
        ):
            token = await model._get_token()
            assert token["access_token"] == "test-access-token"
            assert token["account_id"] == "test-account-id"

    @pytest.mark.asyncio
    async def test_ainvoke_returns_ai_message(self):
        model = CodexChatModel(model_name="openai-codex/gpt-5.1-codex")

        with patch.object(model, "_get_token", new_callable=AsyncMock) as mock_tok, \
             patch.object(model, "_call_api", new_callable=AsyncMock) as mock_api:
            mock_tok.return_value = {"access_token": "t", "account_id": "a"}
            mock_api.return_value = "Codex reply"

            result = await model.ainvoke([HumanMessage(content="test")])
            assert isinstance(result, AIMessage)
            assert result.content == "Codex reply"
```

**Step 2: Run test to verify it fails**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_codex_provider.py -v`
Expected: FAIL — `src.providers.codex_provider` not found

**Step 3: Implement CodexChatModel**

Create `src/providers/codex_provider.py`. This is adapted from nanobot's `openai_codex_provider.py`, rewritten to extend `OAuthChatModel` instead of nanobot's `LLMProvider`:

```python
"""OpenAI Codex provider — OAuth-based, LangChain-compatible.

Adapted from nanobot's openai_codex_provider.py. Uses oauth_cli_kit
for token management and the Codex Responses API with SSE streaming.
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
    """Import and call oauth_cli_kit. Separated for testability."""
    from oauth_cli_kit import get_token
    return get_token()


class CodexChatModel(OAuthChatModel):
    """OpenAI Codex via OAuth (oauth_cli_kit)."""

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
            try:
                content = await _request_codex(DEFAULT_CODEX_URL, headers, body, verify=True)
            except Exception as e:
                if "CERTIFICATE_VERIFY_FAILED" not in str(e):
                    raise
                logger.warning("SSL verify failed for Codex API; retrying with verify=False")
                content = await _request_codex(DEFAULT_CODEX_URL, headers, body, verify=False)
            return content
        except Exception as e:
            logger.error("Codex API error: %s", e)
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
    """Convert LangChain messages to Codex Responses API format."""
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
    async with httpx.AsyncClient(timeout=120.0, verify=verify) as client:
        async with client.stream("POST", url, headers=headers, json=body) as resp:
            if resp.status_code != 200:
                text = await resp.aread()
                raise RuntimeError(_friendly_error(resp.status_code, text.decode("utf-8", "ignore")))
            return await _consume_sse(resp)


async def _iter_sse(response: httpx.Response) -> AsyncGenerator[dict[str, Any], None]:
    buffer: list[str] = []
    async for line in response.aiter_lines():
        if line == "":
            if buffer:
                data_lines = [l[5:].strip() for l in buffer if l.startswith("data:")]
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
            raise RuntimeError("Codex response failed")
    return content


def _friendly_error(status_code: int, raw: str) -> str:
    if status_code == 429:
        return "ChatGPT usage quota exceeded or rate limit triggered."
    return f"HTTP {status_code}: {raw}"
```

**Step 4: Run test to verify it passes**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_codex_provider.py -v`
Expected: All 6 tests PASS

**Step 5: Commit**

```bash
git add src/providers/codex_provider.py tests/unit/test_codex_provider.py
git commit -m "feat(providers): add OpenAI Codex OAuth provider (adapted from nanobot)"
```

---

### Task 4: Implement Gemini CLI provider

**Files:**
- Create: `src/providers/gemini_cli_provider.py`
- Test: `tests/unit/test_gemini_cli_provider.py`

**Step 1: Write the failing test**

Create `tests/unit/test_gemini_cli_provider.py`:

```python
"""Gemini CLI OAuth provider tests."""

import json
import pytest
from unittest.mock import AsyncMock, patch, mock_open, MagicMock
from pathlib import Path
from langchain_core.messages import HumanMessage, AIMessage

from src.providers.gemini_cli_provider import (
    GeminiCliChatModel,
    _find_credentials_path,
    _strip_model_prefix,
)


class TestStripModelPrefix:

    def test_strips_gemini_cli_prefix(self):
        assert _strip_model_prefix("gemini-cli/gemini-2.5-flash") == "gemini-2.5-flash"

    def test_no_prefix_unchanged(self):
        assert _strip_model_prefix("gemini-2.5-flash") == "gemini-2.5-flash"


class TestFindCredentialsPath:

    def test_returns_path_object(self):
        path = _find_credentials_path()
        assert isinstance(path, Path)

    def test_path_contains_gemini(self):
        path = _find_credentials_path()
        assert "gemini" in str(path).lower() or ".gemini" in str(path)


class TestGeminiCliChatModel:

    def test_llm_type(self):
        model = GeminiCliChatModel(model_name="gemini-cli/gemini-2.5-flash")
        assert model._llm_type == "gemini-cli"

    @pytest.mark.asyncio
    async def test_get_token_reads_creds_file(self):
        model = GeminiCliChatModel(model_name="gemini-cli/gemini-2.5-flash")
        fake_creds = {
            "client_id": "cid",
            "client_secret": "csecret",
            "refresh_token": "rtoken",
        }
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new-access-token",
            "expires_in": 3600,
        }

        with patch("builtins.open", mock_open(read_data=json.dumps(fake_creds))), \
             patch("src.providers.gemini_cli_provider._find_credentials_path"), \
             patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response):
            token = await model._get_token()
            assert token["access_token"] == "new-access-token"

    @pytest.mark.asyncio
    async def test_ainvoke_returns_ai_message(self):
        model = GeminiCliChatModel(model_name="gemini-cli/gemini-2.5-flash")

        with patch.object(model, "_get_token", new_callable=AsyncMock) as mock_tok, \
             patch.object(model, "_call_api", new_callable=AsyncMock) as mock_api:
            mock_tok.return_value = {"access_token": "t"}
            mock_api.return_value = "Gemini reply"

            result = await model.ainvoke([HumanMessage(content="test")])
            assert isinstance(result, AIMessage)
            assert result.content == "Gemini reply"
```

**Step 2: Run test to verify it fails**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_gemini_cli_provider.py -v`
Expected: FAIL — `src.providers.gemini_cli_provider` not found

**Step 3: Implement GeminiCliChatModel**

Create `src/providers/gemini_cli_provider.py`:

```python
"""Gemini CLI provider — OAuth-based, LangChain-compatible.

Reads OAuth credentials from ~/.gemini/oauth_creds.json (written by
Gemini CLI during `gemini auth login`). Refreshes access tokens via
Google's OAuth2 endpoint. Calls the Gemini OpenAI-compatible API.
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

# Token cache to avoid refreshing on every call
_token_cache: dict[str, Any] = {}


def _find_credentials_path() -> Path:
    """Locate Gemini CLI OAuth credentials file."""
    return Path.home() / ".gemini" / "oauth_creds.json"


def _strip_model_prefix(model: str) -> str:
    if model.startswith("gemini-cli/") or model.startswith("gemini_cli/"):
        return model.split("/", 1)[1]
    return model


class GeminiCliChatModel(OAuthChatModel):
    """Gemini via local OAuth credentials from Gemini CLI."""

    model_name: str = "gemini-cli/gemini-2.5-flash"

    @property
    def _llm_type(self) -> str:
        return "gemini-cli"

    async def _get_token(self) -> dict:
        # Return cached token if still valid (with 60s margin)
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
            raise RuntimeError(f"Google OAuth token refresh failed: {resp.status_code} {resp.text}")

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
            raise RuntimeError(f"Gemini API error: {resp.status_code} {resp.text}")

        data = resp.json()
        return data["choices"][0]["message"]["content"]


def _convert_to_openai_format(messages: list[BaseMessage]) -> list[dict[str, str]]:
    """Convert LangChain messages to OpenAI chat format."""
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
```

**Step 4: Run test to verify it passes**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_gemini_cli_provider.py -v`
Expected: All 5 tests PASS

**Step 5: Commit**

```bash
git add src/providers/gemini_cli_provider.py tests/unit/test_gemini_cli_provider.py
git commit -m "feat(providers): add Gemini CLI OAuth provider"
```

---

### Task 5: Wire OAuth providers into factory

**Files:**
- Modify: `src/providers/factory.py:1-143`
- Modify: `src/providers/__init__.py`
- Test: `tests/unit/test_factory_oauth.py`

**Step 1: Write the failing test**

Create `tests/unit/test_factory_oauth.py`:

```python
"""Factory OAuth integration tests."""

import pytest
from unittest.mock import patch, MagicMock

from src.providers.factory import get_llm


class TestFactoryOAuthBranch:

    @patch("src.providers.factory._create_oauth_model")
    def test_codex_model_routes_to_oauth(self, mock_create):
        mock_create.return_value = MagicMock()
        result = get_llm(model="openai-codex/gpt-5.1-codex")
        mock_create.assert_called_once()
        args = mock_create.call_args
        assert args[0][0].name == "openai_codex"

    @patch("src.providers.factory._create_oauth_model")
    def test_gemini_cli_model_routes_to_oauth(self, mock_create):
        mock_create.return_value = MagicMock()
        result = get_llm(model="gemini-cli/gemini-2.5-flash")
        mock_create.assert_called_once()
        args = mock_create.call_args
        assert args[0][0].name == "gemini_cli"

    @patch("src.providers.factory.ChatOpenAI")
    def test_normal_model_still_uses_chatopenai(self, mock_chat):
        get_llm(model="gpt-4o")
        mock_chat.assert_called_once()

    @patch("src.providers.factory.ChatOpenAI")
    def test_no_api_key_warning_for_oauth(self, mock_chat):
        """OAuth models should not warn about missing API key."""
        # This just ensures no exception is thrown
        with patch("src.providers.factory._create_oauth_model") as mock_create:
            mock_create.return_value = MagicMock()
            result = get_llm(model="openai-codex/gpt-5.1-codex")
            # Should not have called ChatOpenAI
            mock_chat.assert_not_called()
```

**Step 2: Run test to verify it fails**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_factory_oauth.py -v`
Expected: FAIL — `_create_oauth_model` not found in factory

**Step 3: Modify factory.py**

Update `src/providers/factory.py`:

```python
"""
LLM Factory — creates LangChain chat models with provider auto-detection.

Supports both API Key providers (via ChatOpenAI) and OAuth providers
(via OAuthChatModel subclasses) for per-role model selection.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_openai import ChatOpenAI

from src.config import get_settings, resolve_secret
from src.providers.registry import ProviderSpec, match_provider

logger = logging.getLogger(__name__)

# Role name → Settings field suffix mapping
_ROLE_MODEL_FIELDS = {
    "categorizer": "LLM_CATEGORIZER_MODEL",
    "drafter": "LLM_DRAFTER_MODEL",
    "reviewer": "LLM_REVIEWER_MODEL",
    "router": "LLM_ROUTER_MODEL",
    "summary": "LLM_SUMMARY_MODEL",
    "consolidator": "LLM_CONSOLIDATOR_MODEL",
}

# OAuth provider name → (module_path, class_name)
_OAUTH_PROVIDERS: dict[str, tuple[str, str]] = {
    "openai_codex": ("src.providers.codex_provider", "CodexChatModel"),
    "gemini_cli": ("src.providers.gemini_cli_provider", "GeminiCliChatModel"),
}


def _resolve_model_for_role(role: str) -> str:
    """Resolve the model name for a given role, falling back to default."""
    settings = get_settings()
    if role and role in _ROLE_MODEL_FIELDS:
        override = getattr(settings, _ROLE_MODEL_FIELDS[role], "")
        if override:
            return override
    return settings.LLM_MODEL


def _resolve_provider_credentials(
    spec: ProviderSpec | None,
    model: str,
) -> tuple[str, str]:
    """Resolve API key and base URL for a provider."""
    settings = get_settings()
    import os

    api_key = ""
    base_url = ""

    if spec:
        if spec.env_key:
            api_key = os.environ.get(spec.env_key, "")
        if not api_key and spec.name != "custom":
            provider_setting = f"{spec.name.upper()}_API_KEY"
            setting_val = getattr(settings, provider_setting, None)
            if setting_val:
                api_key = resolve_secret(setting_val)

        base_url = spec.default_base_url

    if not api_key:
        api_key = resolve_secret(settings.OPENAI_API_KEY)
    if settings.OPENAI_API_BASE:
        base_url = settings.OPENAI_API_BASE

    return api_key, base_url


def _create_oauth_model(
    spec: ProviderSpec,
    model: str,
    temperature: float,
    **kwargs: Any,
) -> BaseChatModel:
    """Create an OAuth-based chat model via lazy import."""
    if spec.name not in _OAUTH_PROVIDERS:
        raise ValueError(f"No OAuth provider class registered for '{spec.name}'")

    module_path, class_name = _OAUTH_PROVIDERS[spec.name]
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    logger.info("Creating OAuth LLM: model=%s, provider=%s", model, spec.label)
    return cls(model_name=model, temperature=temperature, **kwargs)


def get_llm(
    model: str | None = None,
    temperature: float = 0.7,
    role: str = "",
    **kwargs: Any,
) -> BaseChatModel:
    """Create a chat model with provider auto-detection.

    Returns ChatOpenAI for API Key providers, or an OAuthChatModel
    subclass for OAuth providers (Codex, Gemini CLI).
    """
    settings = get_settings()

    if model is None:
        model = _resolve_model_for_role(role)

    api_key = resolve_secret(settings.OPENAI_API_KEY)
    base_url = settings.OPENAI_API_BASE

    spec = match_provider(model, api_key=api_key, api_base=base_url)

    # OAuth providers bypass ChatOpenAI entirely
    if spec and spec.is_oauth:
        return _create_oauth_model(spec, model, temperature, **kwargs)

    resolved_key, resolved_base = _resolve_provider_credentials(spec, model)

    if not resolved_key:
        logger.warning(
            "No API key found for model '%s' (provider=%s). LLM calls will likely fail.",
            model,
            spec.name if spec else "unknown",
        )

    model_kwargs = {}
    if spec:
        model_kwargs = spec.get_model_kwargs(model)
        if model_kwargs.get("temperature") is not None:
            temperature = model_kwargs.pop("temperature")

    final_kwargs = {
        "model": model,
        "temperature": temperature,
        "base_url": resolved_base or None,
        "api_key": resolved_key or "not-set",
        "max_retries": 2,
        "timeout": 60,
        **model_kwargs,
        **kwargs,
    }

    if spec:
        logger.debug(
            "Creating LLM: model=%s, provider=%s, base_url=%s",
            model, spec.label, resolved_base,
        )

    return ChatOpenAI(**final_kwargs)


def get_llm_for_role(
    role: str,
    temperature: float = 0.7,
    **kwargs: Any,
) -> BaseChatModel:
    """Shorthand: create an LLM for a specific functional role."""
    return get_llm(model=None, temperature=temperature, role=role, **kwargs)
```

Update `src/providers/__init__.py` to export new symbols:

```python
from src.providers.registry import ProviderSpec, PROVIDERS, match_provider
from src.providers.factory import get_llm, get_llm_for_role

__all__ = [
    "ProviderSpec",
    "PROVIDERS",
    "match_provider",
    "get_llm",
    "get_llm_for_role",
]
```

**Step 4: Run all tests to verify nothing broke**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_factory_oauth.py tests/unit/test_llm_factory.py -v`
Expected: All tests PASS (new OAuth tests + existing factory tests)

**Step 5: Commit**

```bash
git add src/providers/factory.py src/providers/__init__.py tests/unit/test_factory_oauth.py
git commit -m "feat(providers): wire OAuth providers into factory with lazy import"
```

---

### Task 6: Add optional dependency and update .env.example

**Files:**
- Modify: `pyproject.toml`
- Modify: `.env.example` (if exists)

**Step 1: Add optional dependency to pyproject.toml**

Add after `[project]` dependencies section:

```toml
[project.optional-dependencies]
codex = ["oauth-cli-kit>=0.1.3"]
```

**Step 2: Add OAuth model examples to .env.example**

Append to the LLM configuration section:

```bash
# OAuth models (no API key needed — uses local OAuth credentials)
# LLM_DRAFTER_MODEL=openai-codex/gpt-5.1-codex     # Requires: pip install ai-exchange[codex]
# LLM_CATEGORIZER_MODEL=gemini-cli/gemini-2.5-flash # Requires: gemini auth login
```

**Step 3: Commit**

```bash
git add pyproject.toml .env.example
git commit -m "chore: add oauth-cli-kit optional dependency and .env.example docs"
```

---

### Task 7: Run full test suite and verify

**Step 1: Run all provider-related tests**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/unit/test_provider_registry.py tests/unit/test_oauth_base.py tests/unit/test_codex_provider.py tests/unit/test_gemini_cli_provider.py tests/unit/test_factory_oauth.py tests/unit/test_llm_factory.py -v`
Expected: All tests PASS

**Step 2: Run full test suite for regression**

Run: `cd C:\Users\f1480\Documents\ai-exchange && python -m pytest tests/ -v --timeout=30`
Expected: No regressions — all existing tests still pass

**Step 3: Final commit if any fixes needed**

Only if Step 2 reveals issues that need fixing.
