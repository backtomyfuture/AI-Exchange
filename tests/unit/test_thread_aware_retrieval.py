import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.nodes.retriever_node import retrieve_context


@pytest.mark.asyncio
async def test_thread_context_fetched_first(
    graph_node_harness,
    route_decision_factory,
):
    """When thread_id is present, search_by_thread is called first."""
    mock_retriever = MagicMock()
    mock_retriever.search_by_thread.return_value = [
        {"id": "t1", "text": "thread msg 1"},
        {"id": "t2", "text": "thread msg 2"},
    ]
    mock_retriever.search.return_value = [
        {"id": "s1", "text": "semantic match"},
    ]

    state = graph_node_harness.state(
        {
            "id": "thread-one",
            "subject": "Re: meeting",
            "body": "body text",
            "sender": "a@b.com",
            "conversation_id": "conv-123",
        },
        context=[],
        route_decision=route_decision_factory("reply"),
    )

    with patch(
        "src.nodes.retriever_node.get_retriever",
        return_value=mock_retriever,
    ), patch(
        "src.nodes.retriever_node._generate_thread_summary",
        new=AsyncMock(return_value="deterministic thread summary"),
    ):
        result = await retrieve_context(state, graph_node_harness.dependencies)

    mock_retriever.search_by_thread.assert_called_once_with(
        thread_id="conv-123",
        limit=5,
        exclude_email_id="thread-one",
    )
    assert len(result["context_summaries"]) == 3
    assert result["context_summaries"][0]["id"] == "t1"


@pytest.mark.asyncio
async def test_no_thread_id_falls_back_to_semantic(
    graph_node_harness,
    route_decision_factory,
):
    """Without thread_id, only semantic search is used."""
    mock_retriever = MagicMock()
    mock_retriever.search.return_value = [{"id": "s1", "text": "semantic"}]

    state = graph_node_harness.state(
        {
            "id": "thread-two",
            "subject": "Hello",
            "body": "world",
            "sender": "x@y.com",
        },
        context=[],
        route_decision=route_decision_factory("reply"),
    )

    with patch(
        "src.nodes.retriever_node.get_retriever",
        return_value=mock_retriever,
    ):
        result = await retrieve_context(state, graph_node_harness.dependencies)

    mock_retriever.search_by_thread.assert_not_called()
    mock_retriever.search.assert_called_once()
    assert len(result["context_summaries"]) == 1
