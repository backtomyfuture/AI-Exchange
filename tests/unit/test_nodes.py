import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.nodes import categorizer, drafter


@pytest.mark.asyncio
async def test_categorize_email_success(mock_env):
    """Test successful email categorization."""
    state = {"email": {"subject": "Q", "body": "Context"}, "classification": {}}

    def fake_retry_decorator(**_kwargs):
        def _decorator(_fn):
            async def _wrapped(_payload):
                return {
                    "priority": "P1",
                    "need_reply": True,
                    "intent": "咨询",
                    "summary": "Test Summary",
                    "reasoning": "Test Reason",
                    "confidence": 0.9,
                }

            return _wrapped

        return _decorator

    with patch("src.utils.llm_factory.LLMFactory.create_llm", return_value=MagicMock()), patch(
        "src.nodes.categorizer.with_llm_retry", side_effect=fake_retry_decorator
    ):
        result = await categorizer.categorize_email(state)

    assert result["classification"]["priority"] == "P1"
    assert result["classification"]["need_reply"] is True
    assert result["next_step"] == "rag_search"


@pytest.mark.asyncio
async def test_generate_draft_with_feedback(mock_env):
    """Test draft generation when user provides feedback (bypass LLM)."""
    state = {"email": {"subject": "Test"}, "feedback": "User rewrote this.", "draft": "Old draft"}

    with patch("src.utils.llm_factory.LLMFactory.create_llm"):
        result = await drafter.generate_draft(state)

    assert result["draft"] == "User rewrote this."
    assert result["feedback"] is None
    assert result["next_step"] == "approval"


@pytest.mark.asyncio
async def test_generate_draft_no_feedback(mock_env):
    """Test draft generation via LLM."""
    state = {
        "email": {"subject": "Subj", "body": "Body", "sender": "me"},
        "context": [{"subject": "Hist", "body": "Old"}],
        "feedback": None,
    }

    with patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create, patch(
        "src.nodes.drafter.with_llm_retry", side_effect=lambda **_: (lambda fn: fn)
    ):
        mock_create.return_value = MagicMock()
        mock_response = MagicMock(content="Generated Draft Content")
        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value=mock_response)
        mock_prompt = MagicMock()
        mock_prompt.__or__.return_value = mock_chain
        with patch("src.nodes.drafter.ChatPromptTemplate.from_messages", return_value=mock_prompt):
            result = await drafter.generate_draft(state)

    assert result["draft"] == "Generated Draft Content"
    assert result["next_step"] == "approval"
