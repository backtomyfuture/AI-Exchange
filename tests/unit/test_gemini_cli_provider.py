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
             patch("src.providers.gemini_cli_provider._token_cache", {}), \
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
