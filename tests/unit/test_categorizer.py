import pytest
from unittest.mock import MagicMock, patch
from src.nodes.categorizer import categorize_email, EmailClassification
from src.graph.state import AgentState

def test_categorize_email_need_reply():
    """
    测试分类节点：需要回复的情况。
    """
    # 模拟输入状态
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

    # 模拟 LLM 返回结果
    mock_classification = EmailClassification(
        priority="P1",
        need_reply=True,
        intent="咨询",
        reasoning="用户询问会议参加情况，需要明确答复。"
    )

    # 使用 patch 模拟 ChatOpenAI
    with patch("src.nodes.categorizer.ChatOpenAI") as mock_llm_class:
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        # 模拟 structured_llm.invoke(prompt) 的链式调用
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = {
            "priority": "P1",
            "need_reply": True,
            "intent": "咨询",
            "reasoning": "用户询问会议参加情况，需要明确答复。"
        }

        # 模拟 ChatPromptTemplate 的组合 (| 操作符)
        with patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt_class.from_messages.return_value = mock_prompt
            # 必须模拟 partial()，因为它在 categorize_email 中被调用
            mock_prompt.partial.return_value = mock_prompt
            
            # 模拟 prompt | llm | parser 的链式构建
            mock_intermediate_chain = MagicMock()
            mock_prompt.__or__.return_value = mock_intermediate_chain
            mock_intermediate_chain.__or__.return_value = mock_chain

            # 执行节点函数
            result = categorize_email(state)
            
            # 验证 invoke 调用时包含了 image_info
            mock_chain.invoke.assert_called_with({"subject": state["email"]["subject"], "body": state["email"]["body"], "image_info": ""})

            # 验证结果
            assert result["classification"]["priority"] == "P1"
            assert result["classification"]["need_reply"] is True
            assert result["classification"]["intent"] == "咨询"
            assert result["next_step"] == "rag_search"

def test_categorize_email_no_reply():
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

    mock_classification = EmailClassification(
        priority="P3",
        need_reply=False,
        intent="通知",
        reasoning="这是一封自动发放的通知邮件，无需回复。"
    )

    with patch("src.nodes.categorizer.ChatOpenAI") as mock_llm_class:
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        mock_chain = MagicMock()
        mock_chain.invoke.return_value = {
            "priority": "P3",
            "need_reply": False,
            "intent": "通知",
            "reasoning": "这是一封自动发放的通知邮件，无需回复。"
        }

        with patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_class:
            mock_prompt = MagicMock()
            mock_prompt_class.from_messages.return_value = mock_prompt
            # 必须模拟 partial()，因为它在 categorize_email 中被调用
            mock_prompt.partial.return_value = mock_prompt
            
            mock_intermediate_chain = MagicMock()
            mock_prompt.__or__.return_value = mock_intermediate_chain
            mock_intermediate_chain.__or__.return_value = mock_chain

            result = categorize_email(state)

            assert result["classification"]["need_reply"] is False
            assert result["classification"]["intent"] == "通知"
            assert result["next_step"] == "end"
