import pytest
from unittest.mock import MagicMock, patch
from src.graph.state import AgentState
from src.nodes.retriever_node import retrieve_context
from src.nodes.drafter import generate_draft

@pytest.fixture
def mock_state() -> AgentState:
    return {
        "email": {
            "subject": "关于下周会议的安排",
            "body": "请问下周一上午有空开会吗？讨论一下项目进度。",
            "sender": "boss@example.com",
            "date": "2026-01-26"
        },
        "classification": {
            "priority": "P1",
            "need_reply": True,
            "intent": "咨询",
            "reasoning": "询问会议安排"
        },
        "context": [],
        "draft": "",
        "approval_status": "pending",
        "next_step": ""
    }

@patch("src.nodes.retriever_node.EmailRetriever")
def test_retrieve_context(MockRetriever, mock_state):
    # 模拟检索器返回结果
    mock_instance = MockRetriever.return_value
    mock_instance.search.return_value = [
        {"sender": "boss@example.com", "subject": "上次会议纪要", "body": "这是上次会议的纪要..."}
    ]

    new_state = retrieve_context(mock_state)

    assert len(new_state["context"]) == 1
    assert new_state["context"][0]["subject"] == "上次会议纪要"
    assert new_state["next_step"] == "drafter"
    mock_instance.search.assert_called()

@patch("src.nodes.drafter.ChatOpenAI")
def test_generate_draft(MockChatOpenAI, mock_state):
    # 模拟 LLM 返回
    mock_llm_instance = MagicMock()
    MockChatOpenAI.return_value = mock_llm_instance

    mock_response = MagicMock()
    mock_response.content = "<thought>参考了上次会议纪要。</thought>\n<draft>下周一上午我有空。</draft>"
    mock_llm_instance.invoke.return_value = mock_response

    mock_state["context"] = [{"sender": "boss@example.com", "subject": "上次会议纪要", "body": "这是上次会议的纪要..."}]

    new_state = generate_draft(mock_state)

    assert "<thought>" in new_state["draft"]
    assert "<draft>" in new_state["draft"]
    assert new_state["next_step"] == "approval"
    mock_llm_instance.invoke.assert_called()
