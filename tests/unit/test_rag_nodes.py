import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, AsyncMock
from src.graph.state import AgentState
from src.nodes.retriever_node import retrieve_context
from src.nodes.drafter import generate_draft

@pytest.fixture
def mock_state(graph_node_harness) -> AgentState:
    return graph_node_harness.state(
        {
            "id": "rag-mail",
            "subject": "关于下周会议的安排",
            "body": "请问下周一上午有空开会吗？讨论一下项目进度。",
            "sender": "boss@example.com",
            "date": "2026-01-26"
        },
        classification={
            "priority": "P1",
            "need_reply": True,
            "intent": "咨询",
            "reasoning": "询问会议安排"
        },
        context=[],
    )

@pytest.mark.asyncio
@patch("src.nodes.retriever_node.get_retriever")
async def test_retrieve_context(
    mock_get_retriever,
    mock_state,
    graph_node_harness,
):
    mock_instance = mock_get_retriever.return_value
    mock_instance.search.return_value = [
        {"sender": "boss@example.com", "subject": "上次会议纪要", "body": "这是上次会议的纪要..."}
    ]
    router = SimpleNamespace(
        apply_tier2_hits=AsyncMock(return_value={}),
        apply_tier3_fallback=AsyncMock(return_value={"routing_stage": "none"}),
    )

    with patch("src.nodes.retriever_node.get_routing_engine", return_value=router):
        new_state = await retrieve_context(mock_state, graph_node_harness.dependencies)

    assert len(new_state["context_summaries"]) == 1
    assert new_state["context_summaries"][0]["subject"] == "上次会议纪要"
    assert new_state["next_step"] == "drafter"
    mock_instance.search.assert_called()

@pytest.mark.asyncio
@patch("src.providers.factory.get_llm_for_role")
@patch("src.nodes.drafter.ChatPromptTemplate")
async def test_generate_draft(
    mock_prompt_class,
    mock_create_llm,
    mock_state,
    graph_node_harness,
):
    mock_llm_instance = MagicMock()
    mock_create_llm.return_value = mock_llm_instance

    mock_response = MagicMock()
    mock_response.content = "下周一上午我有空，可以开会讨论项目进度。"
    
    mock_chain = MagicMock()
    mock_chain.ainvoke = AsyncMock(return_value=mock_response)
    
    mock_prompt = MagicMock()
    mock_prompt_class.from_messages.return_value = mock_prompt
    mock_prompt.__or__.return_value = mock_chain

    mock_state["context_summaries"] = [
        {
            "sender": "boss@example.com",
            "subject": "上次会议纪要",
            "snippet": "这是上次会议的纪要...",
        }
    ]

    new_state = await generate_draft(mock_state, graph_node_harness.dependencies)

    assert "下周一" in graph_node_harness.drafts[new_state["draft_id"]]
    assert new_state["next_step"] == "approval"
