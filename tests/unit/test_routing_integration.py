import pytest
from unittest.mock import patch, AsyncMock, MagicMock


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

    with patch("src.nodes.categorizer.get_routing_engine", return_value=mock_engine), \
         patch("src.utils.llm_factory.LLMFactory") as mock_llm_factory:
        mock_llm = MagicMock()
        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value={
            "priority": "P0", "need_reply": True,
            "intent": "审批", "summary": "Test", "reasoning": "VIP"
        })
        mock_llm_factory.create_llm.return_value = mock_llm

        with patch("src.nodes.categorizer.JsonOutputParser") as mock_parser_cls:
            mock_parser = MagicMock()
            mock_parser.get_format_instructions.return_value = "format"
            mock_parser_cls.return_value = mock_parser

            with patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_cls:
                mock_prompt_template = MagicMock()
                mock_prompt_template.partial.return_value = mock_prompt_template
                mock_prompt_template.__or__ = MagicMock(
                    return_value=MagicMock(__or__=MagicMock(return_value=mock_chain))
                )
                mock_prompt_cls.from_messages.return_value = mock_prompt_template

                from src.nodes.categorizer import categorize_email
                result = await categorize_email(
                    state,
                    graph_node_harness.dependencies,
                )

        mock_engine.execute_router.assert_called_once()
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

    with patch("src.nodes.categorizer.get_routing_engine", return_value=mock_engine), \
         patch("src.utils.llm_factory.LLMFactory") as mock_llm_factory:
        mock_llm = MagicMock()
        mock_chain = MagicMock()
        mock_chain.ainvoke = AsyncMock(return_value={
            "priority": "P2", "need_reply": False,
            "intent": "通知", "summary": "Q1 Report", "reasoning": "Notification"
        })
        mock_llm_factory.create_llm.return_value = mock_llm

        with patch("src.nodes.categorizer.JsonOutputParser") as mock_parser_cls:
            mock_parser = MagicMock()
            mock_parser.get_format_instructions.return_value = "format"
            mock_parser_cls.return_value = mock_parser
            with patch("src.nodes.categorizer.ChatPromptTemplate") as mock_prompt_cls:
                mock_prompt_template = MagicMock()
                mock_prompt_template.partial.return_value = mock_prompt_template
                mock_prompt_template.__or__ = MagicMock(
                    return_value=MagicMock(__or__=MagicMock(return_value=mock_chain))
                )
                mock_prompt_cls.from_messages.return_value = mock_prompt_template

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
    from skills_registry.skill_forward_boss.handler import Skill as ForwardBossSkill
    from src.nodes.categorizer import categorize_email
    from src.router.engine import RoutingEngine

    manifest = MagicMock()
    manifest.name = "Forward to Boss Verification"
    manifest.depends_on = None
    forward_skill = ForwardBossSkill(manifest)
    engine = RoutingEngine()
    engine.skill_manager.get_all_skills = MagicMock(
        return_value={"skill_forward_boss": forward_skill}
    )
    engine.skill_manager.get_skill = MagicMock(return_value=forward_skill)

    if tier == "tier1":
        engine.t1_router.route = MagicMock(return_value=["skill_forward_boss"])
    else:
        engine.t1_router.route = MagicMock(return_value=[])
        engine._tier3_llm_route = AsyncMock(return_value=["skill_forward_boss"])

    state = graph_node_harness.state(
        {
            "id": f"forward-{tier}",
            "subject": "Forward this",
            "body": "body",
            "sender": "sender@example.com",
            "draft_to": ["sender@example.com"],
        }
    )

    with patch("src.nodes.categorizer.get_routing_engine", return_value=engine):
        result = await categorize_email(state, graph_node_harness.dependencies)

    assert result["classification"]["action"] == "forward"
    assert result["draft_to"] == ["boss@company.com"]
    assert result["draft_cc"] == []
    assert result["draft_id"] == f"forward-{tier}"
    assert graph_node_harness.draft_saves == [(f"forward-{tier}", "呈阅")]
    assert "email" not in result
    assert "draft" not in result
