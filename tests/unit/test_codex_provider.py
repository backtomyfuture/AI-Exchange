"""OpenAI Codex provider tests.

Tests use mocking — actual OAuth + API calls are not made.
"""

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
