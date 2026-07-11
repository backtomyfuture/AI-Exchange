import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from src.nodes.categorizer import categorize_email

@pytest.mark.asyncio
async def test_categorize_email_retry_logic(graph_node_harness):
    """
    Test that categorize_email retries on failure.
    """
    state = graph_node_harness.state(
        {
            "id": "retry-mail",
            "subject": "Test Retry",
            "body": "This is a test body."
        },
    )

    success_result = {
        "priority": "P2",
        "need_reply": True,
        "intent": "咨询",
        "summary": "Test retry email",
        "reasoning": "Retry success",
        "confidence": 0.8,
    }

    with patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create_llm, \
         patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_class:
        
        mock_llm = MagicMock()
        mock_create_llm.return_value = mock_llm
        
        mock_prompt = MagicMock()
        mock_prompt_class.from_messages.return_value = mock_prompt
        mock_prompt.partial.return_value = mock_prompt

        mock_chain = MagicMock()
        
        call_count = [0]
        async def side_effect_fn(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                raise Exception(f"Rate Limit {call_count[0]}")
            return success_result
        
        mock_chain.ainvoke = AsyncMock(side_effect=side_effect_fn)
        
        mock_intermediate = MagicMock()
        mock_prompt.__or__.return_value = mock_intermediate
        mock_intermediate.__or__.return_value = mock_chain

        result = await categorize_email(state, graph_node_harness.dependencies)
        
        assert result["classification"]["reasoning"] == "Retry success"
        assert call_count[0] == 3
