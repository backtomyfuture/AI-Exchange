import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.nodes import categorizer, drafter
from src.graph.state import AgentState

@pytest.mark.asyncio
async def test_categorize_email_success(mock_env):
    """Test successful email categorization."""
    
    # Mock LLM and Chain
    with patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create_llm:
        mock_llm = MagicMock()
        mock_create_llm.return_value = mock_llm
        
        # We need to mock the entire chain: prompt | llm | parser
        # But categorize_email constructs the chain internally.
        # It calls chain.ainvoke(payload).
        # Easier to mock the chain construction or the `ainvoke` result if possible.
        # However, it builds chain = prompt | llm | parser.
        # It's cleaner to mock ChatPromptTemplate, LLM, JsonOutputParser if we want whitebox testing.
        # OR better: Mock the `chain.ainvoke` by patching loosely or using `with patch(...)` on the chain components.
        
        # Actually, let's patch the `chain.ainvoke` by patching the retry wrapper or just the `ainvoke` of the chain components?
        # The chain is created inside the function.
        
        # Strategy: Mock Chain creation in `src.nodes.categorizer`.
        # Since `prompt | llm | parser` creates a RunnableSequence.
        
        # Let's try mocking the `invoke_with_retry` decorator? No, that's hard.
        # Let's mock `ChatOpenAI`, `ChatPromptTemplate`, `JsonOutputParser` and their composition.
        
        # Simplified: Mock `chain.ainvoke` via patching where the chain is composed? 
        # Since it uses the `|` operator, it returns a RunnableSequence. 
        # Maybe we can just mock `chain.ainvoke` if we can access it? No it's local.
        
        # Alternative: Mock `chain.ainvoke` by mocking `LLMFactory` to return a mock LLM, 
        # and checking if we can control the output of the whole chain.
        # But `parser` is the last step.
        
        # Let's mock `invoke_with_retry` inner function? No.
        
        # Let's use `patch("src.nodes.categorizer.llm_rate_limiter")` to avoid waiting.
        pass

    with patch("src.nodes.categorizer.llm_rate_limiter.acquire", new_callable=AsyncMock), \
         patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create_llm:
         
        mock_chain_result = {
            "priority": "P1",
            "need_reply": True,
            "intent": "咨询",
            "summary": "Test Summary",
            "reasoning": "Test Reason"
        }
        
        # To make the chain.ainvoke work, we need to mock the pipeline.
        # If we mock the objects involved in the `|` operation...
        # A simpler way often used with LangChain is to mock the `ainvoke` method on the resulting chain object.
        # But we don't see the chain object.
        
        # Workaround: Mock `ChatOpenAI` behavior such that when it's invoked, it returns a message that `JsonOutputParser` likes?
        # `parser` will run on the output of LLM.
        # If we mock LLM to return an AIMessage with JSON string content.
        
        mock_llm_instance = AsyncMock()
        from langchain_core.messages import AIMessage
        mock_llm_instance.ainvoke.return_value = AIMessage(content='{"priority": "P1", "need_reply": true, "intent": "咨询", "summary": "Test Summary", "reasoning": "Test Reason"}')
        mock_create_llm.return_value = mock_llm_instance
        
        # Note: ChatPromptTemplate | LLM | Parser calls LLM.ainvoke with formatted prompt.
        # We assume the standard LangChain behavior holds.
        
        state = {
            "email": {"subject": "Q", "body": "Context"},
            "classification": {}
        }
        
        result = await categorizer.categorize_email(state)
        
        assert result["classification"]["priority"] == "P1"
        assert result["classification"]["need_reply"] is True
        assert result["next_step"] == "rag_search"


@pytest.mark.asyncio
async def test_generate_draft_with_feedback(mock_env):
    """Test draft generation when user provides feedback (bypass LLM)."""
    state = {
        "email": {"subject": "Test"},
        "feedback": "User rewrote this.",
        "draft": "Old draft"
    }
    
    # Needs LLM factory mock even if skipped? The code imports it inside. 
    # Actually the code checks feedback first, so it might skip `LLMFactory`.
    # Let's see `src/nodes/drafter.py`. 
    # Yes, lines 29 checks feedback. If present, returns immediately.
    # IMPT: It imports LLMFactory inside ? No, line 23 inside.
    # Ah, lines 23-24 init LLM. Then line 26 gets feedback.
    # So we MUST mock LLMFactory to avoid real calls or errors.

    with patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create:
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
        "feedback": None
    }
    
    with patch("src.nodes.drafter.llm_rate_limiter.acquire", new_callable=AsyncMock), \
         patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create:
            
        mock_llm = AsyncMock()
        from langchain_core.messages import AIMessage
        mock_llm.ainvoke.return_value = AIMessage(content="Generated Draft Content")
        mock_create.return_value = mock_llm
        
        result = await drafter.generate_draft(state)
        
        assert result["draft"] == "Generated Draft Content"
        assert result["next_step"] == "approval"
