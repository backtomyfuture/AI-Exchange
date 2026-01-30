import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from src.nodes.categorizer import categorize_email, EmailClassification
from src.graph.state import AgentState

@pytest.mark.asyncio
async def test_categorize_email_need_reply():
    """
    测试分类节点：需要回复的情况。
    """
    state: AgentState = {
        "email": {
            "subject": "关于下周会议的确认",
            "body": "你好，请问下周一的会议你是否参加？"
        },
        "classification": {},
        "context": [],
        "draft": "",
        "approval_status": "pending",
        "next_step": ""
    }

    with patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create_llm:
        mock_llm = MagicMock()
        mock_create_llm.return_value = mock_llm

        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value={
            "priority": "P1",
            "need_reply": True,
            "intent": "咨询",
            "summary": "询问下周一会议参加情况",
            "reasoning": "用户询问会议参加情况，需要明确答复。"
        })

        with patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt_class.from_messages.return_value = mock_prompt
            mock_prompt.partial.return_value = mock_prompt
            
            mock_intermediate_chain = MagicMock()
            mock_prompt.__or__.return_value = mock_intermediate_chain
            mock_intermediate_chain.__or__.return_value = mock_chain

            result = await categorize_email(state)

            assert result["classification"]["priority"] == "P1"
            assert result["classification"]["need_reply"] is True
            assert result["classification"]["intent"] == "咨询"
            assert result["next_step"] == "rag_search"

@pytest.mark.asyncio
async def test_categorize_email_no_reply():
    """
    测试分类节点：不需要回复的情况（如通知）。
    """
    state: AgentState = {
        "email": {
            "subject": "工资单已发放",
            "body": "您的本月工资单已发放，请查收附件。"
        },
        "classification": {},
        "context": [],
        "draft": "",
        "approval_status": "pending",
        "next_step": ""
    }

    with patch("src.utils.llm_factory.LLMFactory.create_llm") as mock_create_llm:
        mock_llm = MagicMock()
        mock_create_llm.return_value = mock_llm

        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value={
            "priority": "P3",
            "need_reply": False,
            "intent": "通知",
            "summary": "工资单发放通知",
            "reasoning": "这是一封自动发放的通知邮件，无需回复。"
        })

        with patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt_class.from_messages.return_value = mock_prompt
            mock_prompt.partial.return_value = mock_prompt
            
            mock_intermediate_chain = MagicMock()
            mock_prompt.__or__.return_value = mock_intermediate_chain
            mock_intermediate_chain.__or__.return_value = mock_chain

            result = await categorize_email(state)

            assert result["classification"]["need_reply"] is False
            assert result["classification"]["intent"] == "通知"
            assert result["next_step"] == "end"
