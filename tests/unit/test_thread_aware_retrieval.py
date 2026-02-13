import pytest
from unittest.mock import patch, MagicMock
from src.nodes.retriever_node import retrieve_context


@pytest.mark.asyncio
async def test_thread_context_fetched_first():
    """When thread_id is present, search_by_thread is called first."""
    mock_retriever = MagicMock()
    mock_retriever.search_by_thread.return_value = [
        {"id": "t1", "text": "thread msg 1"},
        {"id": "t2", "text": "thread msg 2"},
    ]
    mock_retriever.search.return_value = [
        {"id": "s1", "text": "semantic match"},
    ]

    state = {
        "email": {
            "subject": "Re: meeting",
            "body": "body text",
            "sender": "a@b.com",
            "conversation_id": "conv-123",
        },
        "context": [],
    }

    with patch("src.nodes.retriever_node.get_retriever", return_value=mock_retriever):
        result = await retrieve_context(state)

    mock_retriever.search_by_thread.assert_called_once_with(thread_id="conv-123", limit=5)
    assert len(result["context"]) == 3
    assert result["context"][0]["id"] == "t1"


@pytest.mark.asyncio
async def test_no_thread_id_falls_back_to_semantic():
    """Without thread_id, only semantic search is used."""
    mock_retriever = MagicMock()
    mock_retriever.search.return_value = [{"id": "s1", "text": "semantic"}]

    state = {
        "email": {"subject": "Hello", "body": "world", "sender": "x@y.com"},
        "context": [],
    }

    with patch("src.nodes.retriever_node.get_retriever", return_value=mock_retriever):
        result = await retrieve_context(state)

    mock_retriever.search_by_thread.assert_not_called()
    mock_retriever.search.assert_called_once()
    assert len(result["context"]) == 1
