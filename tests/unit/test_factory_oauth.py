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
        """OAuth 模型不应警告 API key 缺失。"""
        with patch("src.providers.factory._create_oauth_model") as mock_create:
            mock_create.return_value = MagicMock()
            result = get_llm(model="openai-codex/gpt-5.1-codex")
            # 不应调用 ChatOpenAI
            mock_chat.assert_not_called()
