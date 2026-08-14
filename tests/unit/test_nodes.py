import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.nodes import categorizer, drafter


@pytest.mark.asyncio
async def test_categorize_email_success(
    mock_env,
    graph_node_harness,
    route_decision_factory,
):
    """Test successful email categorization."""
    state = graph_node_harness.state(
        {"id": "node-category", "subject": "Q", "body": "Context"},
        classification={},
        route_decision=route_decision_factory("reply"),
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

    with (
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
    assert "next_step" not in result


@pytest.mark.asyncio
async def test_categorize_email_reuses_complete_tier3_metadata(
    mock_env,
    graph_node_harness,
    route_decision_factory,
):
    state = graph_node_harness.state(
        {"id": "node-tier3", "subject": "FYI", "body": "notice"},
        classification={
            "action": "reply",
            "need_reply": True,
            "priority": "P2",
            "intent": "通知",
            "summary": "already classified",
            "tier3_metadata_complete": True,
        },
        route_decision=route_decision_factory("reply"),
    )
    with patch("src.providers.factory.get_llm_for_role") as mock_llm:
        result = await categorizer.categorize_email(
            state,
            graph_node_harness.dependencies,
        )
    mock_llm.assert_not_called()
    assert result["classification"]["summary"] == "already classified"


@pytest.mark.asyncio
async def test_generate_forward_draft_uses_store_not_feedback_state(
    mock_env,
    graph_node_harness,
    route_decision_factory,
):
    """Forward draft text is durable and Graph receives only its identifier."""
    from src.handoff.profiles import get_handoff_profile

    plan = get_handoff_profile("generic_forward_v1").build_plan()
    state = graph_node_harness.state(
        {"id": "node-forward", "subject": "Test"},
        classification={"action": "forward"},
        route_decision=route_decision_factory("forward"),
        handoff_plan=plan.model_dump(mode="json"),
    )

    with patch("src.providers.factory.get_llm_for_role") as mock_llm:
        result = await drafter.generate_draft(
            state,
            graph_node_harness.dependencies,
        )

    mock_llm.assert_not_called()
    assert graph_node_harness.drafts[result["draft_id"]] == "呈阅"
    assert "draft" not in result
    assert "feedback" not in result
    assert result["next_step"] == "approval"


@pytest.mark.asyncio
async def test_drafter_uses_handoff_plan_not_forward_route_special_case(
    mock_env,
    graph_node_harness,
    route_decision_factory,
):
    """A forward route still honors the persisted writing contract."""
    state = graph_node_harness.state(
        {"id": "node-forward-custom", "subject": "Test"},
        classification={"action": "forward"},
        route_decision=route_decision_factory("forward"),
        handoff_plan={
            "schema_version": 1,
            "profile_id": "generic_forward_v1",
            "required_sources": [],
            "optional_sources": [],
            "max_items_per_source": 5,
            "writer_mode": "fixed",
            "prompt_modifier": None,
            "fixed_draft": "请阅示",
        },
    )

    with patch("src.providers.factory.get_llm_for_role") as mock_llm:
        result = await drafter.generate_draft(
            state,
            graph_node_harness.dependencies,
        )

    mock_llm.assert_not_called()
    assert graph_node_harness.drafts[result["draft_id"]] == "请阅示"


@pytest.mark.asyncio
async def test_categorize_email_does_not_ask_llm_for_need_reply(
    mock_env,
    graph_node_harness,
    route_decision_factory,
):
    """need_reply is derived from the frozen route, not from the categorizer LLM."""
    state = graph_node_harness.state(
        {"id": "node-category-no-need-reply", "subject": "Q", "body": "Context"},
        classification={},
        route_decision=route_decision_factory("reply"),
    )

    def fake_retry_decorator(**_kwargs):
        def _decorator(_fn):
            async def _wrapped(_payload):
                return {
                    "priority": "P1",
                    "need_reply": False,
                    "intent": "咨询",
                    "summary": "Test Summary",
                    "reasoning": "Test Reason",
                    "confidence": 0.9,
                }

            return _wrapped

        return _decorator

    with (
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

    assert "need_reply" not in categorizer.EmailClassification.model_fields
    assert result["classification"]["need_reply"] is True
    assert result["classification"]["priority"] == "P1"


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
