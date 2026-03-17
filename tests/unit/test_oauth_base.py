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
