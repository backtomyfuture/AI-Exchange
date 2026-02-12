import pytest
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_categorizer_invokes_routing_engine():
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

    state = {
        "email": {"subject": "Test", "body": "Hello", "sender": "vip@test.com"},
        "classification": {},
        "context": [],
        "active_skills": [],
        "routing_log": [],
        "system_prompt_modifier": None,
        "draft": "",
        "approval_status": "pending",
        "next_step": "",
    }

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
                result = await categorize_email(state)

        mock_engine.execute_router.assert_called_once()
        assert "skill_vip_handling" in result.get("active_skills", [])


@pytest.mark.asyncio
async def test_routing_log_preserved_through_categorizer():
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

    state = {
        "email": {"subject": "Report", "body": "Q1", "sender": "test@test.com"},
        "classification": {},
        "context": [],
        "active_skills": [],
        "routing_log": [],
        "system_prompt_modifier": None,
        "draft": "",
        "approval_status": "pending",
        "next_step": "",
    }

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
                result = await categorize_email(state)

        assert len(result.get("routing_log", [])) >= 1
