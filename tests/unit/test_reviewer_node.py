import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from src.nodes.reviewer import review_draft


def _passthrough_retry(**_kw):
    def decorator(fn):
        return fn
    return decorator


@pytest.mark.asyncio
async def test_review_pass(graph_node_harness):
    """Reviewer returns a small pass result when draft passes."""
    state = graph_node_harness.state(
        {"id": "review-pass", "subject": "Q", "body": "body"},
        draft="Good draft",
        metadata={},
    )

    mock_response = MagicMock()
    mock_response.content = '{"pass": true, "issues": ""}'

    mock_chain = MagicMock()
    mock_chain.ainvoke = AsyncMock(return_value=mock_response)

    mock_llm = MagicMock()
    mock_prompt = MagicMock()
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)

    with patch("src.utils.llm_factory.LLMFactory.create_llm", return_value=mock_llm), \
         patch("src.nodes.reviewer.with_llm_retry", side_effect=_passthrough_retry), \
         patch("src.nodes.reviewer.ChatPromptTemplate") as mock_ct:
        mock_ct.from_messages.return_value = mock_prompt
        result = await review_draft(state, graph_node_harness.dependencies)

    assert result.get("next_step") != "drafter"
    assert result["review_result"]["passed"] is True
    assert "draft" not in result


@pytest.mark.asyncio
async def test_review_fail_triggers_rewrite(graph_node_harness):
    """Reviewer sets next_step=drafter when draft fails review."""
    state = graph_node_harness.state(
        {"id": "review-fail", "subject": "Q", "body": "body"},
        draft="Bad draft",
        metadata={},
    )

    mock_response = MagicMock()
    mock_response.content = '{"pass": false, "issues": "Missing key point"}'

    mock_chain = MagicMock()
    mock_chain.ainvoke = AsyncMock(return_value=mock_response)

    mock_llm = MagicMock()
    mock_prompt = MagicMock()
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)

    with patch("src.utils.llm_factory.LLMFactory.create_llm", return_value=mock_llm), \
         patch("src.nodes.reviewer.with_llm_retry", side_effect=_passthrough_retry), \
         patch("src.nodes.reviewer.ChatPromptTemplate") as mock_ct:
        mock_ct.from_messages.return_value = mock_prompt
        result = await review_draft(state, graph_node_harness.dependencies)

    assert result["next_step"] == "drafter"
    assert result["metadata"]["review_count"] == 1


@pytest.mark.asyncio
async def test_review_skipped_on_second_attempt(graph_node_harness):
    """Reviewer skips if review_count >= 1."""
    state = graph_node_harness.state(
        {"id": "review-second", "subject": "Q", "body": "body"},
        draft="Some draft",
        metadata={"review_count": 1},
    )
    result = await review_draft(state, graph_node_harness.dependencies)
    assert result["review_result"]["passed"] is True
    assert set(result) == {"review_result", "next_step"}
