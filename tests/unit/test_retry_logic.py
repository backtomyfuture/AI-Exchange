
import pytest
from unittest.mock import MagicMock, patch
from src.nodes.categorizer import categorize_email

def test_categorize_email_retry_logic():
    """
    Test that categorize_email retries on failure.
    """
    state = {
        "email": {
            "subject": "Test Retry",
            "body": "This is a test body."
        },
        "classification": {},
        "context": [],
        "draft": "",
        "approval_status": "pending",
        "next_step": ""
    }

    # Mock success result
    success_result = {
        "priority": "P2",
        "need_reply": True,
        "intent": "咨询",
        "reasoning": "Retry success"
    }

    with patch("src.nodes.categorizer.ChatOpenAI"), \
         patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_class:
        
        # Setup chain mock
        mock_prompt = MagicMock()
        mock_prompt_class.from_messages.return_value = mock_prompt
        mock_prompt.partial.return_value = mock_prompt # vital because of .partial() call

        mock_chain = MagicMock()
        
        # Simulate 2 Failures then Success
        # Note: We use a generic Exception because tenacity is configured to retry on Exception (or we can use a specific one if needed)
        # In our code we used @retry(..., reraise=True) on generic invocation, usually catching all exceptions if not specified otherwise in our memory of the code (I checked, it was default retry which catches generic Exception? No, wait.
        # Let's check my edit: @retry(..., reraise=True). Default matches Exception.
        
        mock_chain.invoke.side_effect = [Exception("Rate Limit 1"), Exception("Rate Limit 2"), success_result]
        
        # Chain construction: prompt | llm | parser
        mock_intermediate = MagicMock()
        mock_prompt.__or__.return_value = mock_intermediate
        mock_intermediate.__or__.return_value = mock_chain

        # Execute
        result = categorize_email(state)
        
        # Verification
        # It should succeed properly
        assert result["classification"]["reasoning"] == "Retry success"
        
        # It should have called invoke 3 times
        assert mock_chain.invoke.call_count == 3
