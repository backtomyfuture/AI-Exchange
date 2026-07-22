import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.nodes import categorizer, drafter


@pytest.mark.asyncio
async def test_categorize_email_success(mock_env, graph_node_harness):
    """Test successful email categorization."""
    state = graph_node_harness.state(
        {"id": "node-category", "subject": "Q", "body": "Context"},
        classification={},
    )

    def fake_retry_decorator(**_kwargs):
        def _decorator(_fn):
            async def _wrapped(_payload):
                return {
                    "priority": "P1",
                    "need_reply": True,
                    "intent": "咨询",
                    "summary": "Test Summary",
                    "reasoning": "Test Reason",
                    "confidence": 0.9,
                }

            return _wrapped

        return _decorator

    routing_engine = MagicMock()
    routing_engine.execute_router = AsyncMock(side_effect=lambda routed: routed)
    with (
        patch("src.nodes.categorizer.get_routing_engine", return_value=routing_engine),
        patch("src.providers.factory.get_llm_for_role", return_value=MagicMock()),
        patch(
            "src.nodes.categorizer.with_llm_retry",
            side_effect=fake_retry_decorator,
        ),
    ):
        result = await categorizer.categorize_email(
            state,
            graph_node_harness.dependencies,
        )

    assert result["classification"]["priority"] == "P1"
    assert result["classification"]["need_reply"] is True
    assert result["next_step"] == "rag_search"


@pytest.mark.asyncio
async def test_generate_forward_draft_uses_store_not_feedback_state(
    mock_env,
    graph_node_harness,
):
    """Forward draft text is durable and Graph receives only its identifier."""
    state = graph_node_harness.state(
        {"id": "node-forward", "subject": "Test"},
        classification={"action": "forward"},
    )

    with patch("src.providers.factory.get_llm_for_role"):
        result = await drafter.generate_draft(
            state,
            graph_node_harness.dependencies,
        )

    assert graph_node_harness.drafts[result["draft_id"]] == "呈阅"
    assert "draft" not in result
    assert "feedback" not in result
    assert result["next_step"] == "approval"


@pytest.mark.asyncio
async def test_generate_draft_no_feedback(mock_env, graph_node_harness):
    """Test draft generation via LLM."""
    state = graph_node_harness.state(
        {
            "id": "node-draft",
            "subject": "Subj",
            "body": "Body",
            "sender": "me",
        },
        context=[{"subject": "Hist", "body": "Old"}],
    )

    with (
        patch("src.providers.factory.get_llm_for_role") as mock_create,
        patch(
            "src.nodes.drafter.with_llm_retry", side_effect=lambda **_: lambda fn: fn
        ),
    ):
        mock_create.return_value = MagicMock()
        mock_response = MagicMock(content="Generated Draft Content")
        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value=mock_response)
        mock_prompt = MagicMock()
        mock_prompt.__or__.return_value = mock_chain
        with patch(
            "src.nodes.drafter.ChatPromptTemplate.from_messages",
            return_value=mock_prompt,
        ):
            result = await drafter.generate_draft(
                state,
                graph_node_harness.dependencies,
            )

    assert graph_node_harness.drafts[result["draft_id"]] == "Generated Draft Content"
    assert result["next_step"] == "approval"
