import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from langchain_core.runnables import RunnableLambda


def _forward_skill():
    skill = MagicMock()
    skill.manifest.name = "Test forward"
    skill.manifest.depends_on = None

    async def execute(state):
        classification = dict(state.get("classification") or {})
        classification.update(
            {
                "priority": "P0",
                "need_reply": True,
                "intent": "转发",
                "action": "forward",
                "reasoning": "test forward skill",
            }
        )
        email = dict(state.get("email") or {})
        email["draft_to"] = ["forward-target@example.com"]
        email["draft_cc"] = []
        return {"classification": classification, "email": email, "draft": "test forward"}

    skill.execute = AsyncMock(side_effect=execute)
    return skill


def _fixed_classification(result):
    def retry_factory(**_kwargs):
        def decorator(_function):
            async def wrapped(_payload):
                return result

            return wrapped

        return decorator

    return retry_factory


@pytest.mark.asyncio
async def test_categorizer_invokes_routing_engine(graph_node_harness):
    """Verify that categorize_email calls RoutingEngine before LLM classification."""
    mock_engine = MagicMock()
    mock_engine.execute_router = AsyncMock(return_value={
        "email": {"subject": "Test", "body": "Hello", "sender": "vip@test.com"},
        "classification": {},
        "context": [],
        "active_skills": ["skill_vip_handling"],
        "routing_log": ["Tier 1 Match: ['skill_vip_handling']"],
        "system_prompt_modifier": None,
        "priority_level": 10,
    })

    state = graph_node_harness.state(
        {
            "id": "routing-one",
            "subject": "Test",
            "body": "Hello",
            "sender": "vip@test.com",
        },
    )

    classification = {
        "priority": "P0",
        "need_reply": True,
        "intent": "审批",
        "summary": "Test",
        "reasoning": "VIP",
        "confidence": 1.0,
    }
    with patch(
        "src.nodes.categorizer.get_routing_engine",
        return_value=mock_engine,
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_fixed_classification(classification),
    ):
        from src.nodes.categorizer import categorize_email

        result = await categorize_email(
            state,
            graph_node_harness.dependencies,
        )

    mock_engine.execute_router.assert_awaited_once()
    assert "skill_vip_handling" in result.get("active_skills", [])


@pytest.mark.asyncio
async def test_routing_log_preserved_through_categorizer(graph_node_harness):
    """Verify routing_log from engine is preserved in output state."""
    mock_engine = MagicMock()
    mock_engine.execute_router = AsyncMock(return_value={
        "email": {"subject": "Report", "body": "Q1", "sender": "test@test.com"},
        "classification": {},
        "context": [],
        "active_skills": [],
        "routing_log": ["Tier 1 No match", "Tier 3 Skipped"],
        "system_prompt_modifier": None,
    })

    state = graph_node_harness.state(
        {
            "id": "routing-two",
            "subject": "Report",
            "body": "Q1",
            "sender": "test@test.com",
        },
    )

    classification = {
        "priority": "P2",
        "need_reply": False,
        "intent": "通知",
        "summary": "Q1 Report",
        "reasoning": "Notification",
        "confidence": 1.0,
    }
    with patch(
        "src.nodes.categorizer.get_routing_engine",
        return_value=mock_engine,
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_fixed_classification(classification),
    ):
        from src.nodes.categorizer import categorize_email

        result = await categorize_email(
            state,
            graph_node_harness.dependencies,
        )

    assert len(result.get("routing_log", [])) >= 1


@pytest.mark.asyncio
@pytest.mark.parametrize("tier", ["tier1", "tier3"])
async def test_real_forward_skill_projects_safe_recipients_and_draft(
    graph_node_harness,
    tier,
):
    from src.nodes.categorizer import categorize_email
    from src.nodes.retriever_node import retrieve_context
    from src.router.engine import RoutingEngine

    forward_skill = _forward_skill()
    engine = RoutingEngine()
    engine.skill_manager.get_all_skills = MagicMock(
        return_value={"test_forward": forward_skill}
    )
    engine.skill_manager.get_skill = MagicMock(return_value=forward_skill)

    if tier == "tier1":
        engine.t1_router.route = MagicMock(return_value=["test_forward"])
    else:
        engine.t1_router.route = MagicMock(return_value=[])
        engine._tier3_llm_route = AsyncMock(return_value=["test_forward"])

    state = graph_node_harness.state(
        {
            "id": f"forward-{tier}",
            "subject": "Forward this",
            "body": "body",
            "sender": "sender@example.com",
            "draft_to": ["sender@example.com"],
            "draft_cc": ["copy@example.com"],
        }
    )

    if tier == "tier1":
        with patch("src.nodes.categorizer.get_routing_engine", return_value=engine):
            result = await categorize_email(state, graph_node_harness.dependencies)
    else:
        classification = {
            "priority": "P2",
            "need_reply": True,
            "intent": "咨询",
            "summary": "需要继续检索路由",
            "reasoning": "Tier 1 未命中",
            "confidence": 1.0,
        }
        with patch(
            "src.nodes.categorizer.get_routing_engine",
            return_value=engine,
        ), patch(
            "src.providers.factory.get_llm_for_role",
            return_value=RunnableLambda(lambda value: value),
        ), patch(
            "src.nodes.categorizer.with_llm_retry",
            side_effect=_fixed_classification(classification),
        ):
            categorized = await categorize_email(
                state,
                graph_node_harness.dependencies,
            )

        assert categorized["routing_stage"] == "pending"
        retriever = MagicMock()
        retriever.search.return_value = []
        with patch(
            "src.nodes.retriever_node.get_routing_engine",
            return_value=engine,
        ), patch(
            "src.nodes.retriever_node.get_retriever",
            return_value=retriever,
        ), patch(
            "src.nodes.retriever_node._retrieve_experience",
            new=AsyncMock(return_value=[]),
        ), patch(
            "src.nodes.retriever_node._retrieve_style_guidance",
            new=AsyncMock(return_value=""),
        ), patch(
            "src.nodes.retriever_node._retrieve_user_preferences",
            new=AsyncMock(return_value=[]),
        ):
            result = await retrieve_context(
                {**state, **categorized},
                graph_node_harness.dependencies,
            )

        engine._tier3_llm_route.assert_awaited_once()

    assert result["classification"]["action"] == "forward"
    assert result["draft_to"] == ["forward-target@example.com"]
    assert result["draft_cc"] == []
    assert result["draft_id"] == f"forward-{tier}"
    assert graph_node_harness.draft_saves == [(f"forward-{tier}", "test forward")]
    assert "email" not in result
    assert "draft" not in result
