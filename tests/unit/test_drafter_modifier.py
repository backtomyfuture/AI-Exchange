import pytest
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_drafter_uses_system_prompt_modifier(graph_node_harness):
    """Verify drafter appends system_prompt_modifier to LLM system prompt."""
    captured_messages = []

    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = "草稿内容"

    state = graph_node_harness.state(
        {
            "id": "modifier-one",
            "subject": "季报",
            "body": "Q1 数据",
            "sender": "boss@test.com",
        },
        context=[],
        system_prompt_modifier="【语气指令】使用 BLUF 原则，结论先行。",
    )

    with patch("src.utils.llm_factory.LLMFactory") as mock_factory:
        mock_factory.create_llm.return_value = mock_llm

        with patch("src.nodes.drafter.ChatPromptTemplate") as mock_prompt_cls:
            mock_chain = MagicMock()
            mock_chain.ainvoke = AsyncMock(return_value=mock_response)
            mock_template = MagicMock()
            mock_template.__or__ = MagicMock(return_value=mock_chain)

            def capture_from_messages(messages):
                captured_messages.extend(messages)
                return mock_template

            mock_prompt_cls.from_messages.side_effect = capture_from_messages

            from src.nodes.drafter import generate_draft
            await generate_draft(state, graph_node_harness.dependencies)

    # The system message (first tuple) should contain the modifier text
    assert len(captured_messages) >= 1
    system_msg = captured_messages[0][1]  # ("system", <content>)
    assert "BLUF" in system_msg


@pytest.mark.asyncio
async def test_drafter_works_without_modifier(graph_node_harness):
    """Verify drafter works normally when system_prompt_modifier is None."""
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = "正常草稿"
    mock_chain = MagicMock()
    mock_chain.ainvoke = AsyncMock(return_value=mock_response)

    state = graph_node_harness.state(
        {
            "id": "modifier-two",
            "subject": "普通邮件",
            "body": "内容",
            "sender": "user@test.com",
        },
        context=[],
        system_prompt_modifier=None,
    )

    with patch("src.utils.llm_factory.LLMFactory") as mock_factory:
        mock_factory.create_llm.return_value = mock_llm
        with patch("src.nodes.drafter.ChatPromptTemplate") as mock_prompt_cls:
            mock_template = MagicMock()
            mock_template.__or__ = MagicMock(return_value=mock_chain)
            mock_prompt_cls.from_messages.return_value = mock_template

            from src.nodes.drafter import generate_draft
            result = await generate_draft(state, graph_node_harness.dependencies)

    assert graph_node_harness.drafts[result["draft_id"]] == "正常草稿"
