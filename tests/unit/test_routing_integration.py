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


def _skill_routed_state(classification):
    """Engine output after a Tier 1 modifier skill decided classification fields."""
    return {
        "classification": classification,
        "active_skills": ["skill_auto_cc"],
        "routing_log": ["Tier 1 Match: ['skill_auto_cc']"],
        "routing_stage": "tier1",
    }


@pytest.mark.asyncio
async def test_tier1_skill_need_reply_false_is_not_overridden_by_llm(
    graph_node_harness,
):
    """CC 规则判定 need_reply=false 后，LLM 分类不得再覆盖为需要回复。

    回归：「呈兰总阅示」邮件（我在 CC）被 skill_auto_cc 命中后，LLM 以
    need_reply=true/P1 覆盖 Skill 结果，导致飞书收到草稿审批卡片。
    """
    mock_engine = MagicMock()
    mock_engine.execute_router = AsyncMock(
        return_value=_skill_routed_state(
            {
                "priority": "P3",
                "need_reply": False,
                "card_type": "none",
                "reasoning": "[Auto-Skill: 抄送通知] 回复率 0%。",
            }
        )
    )

    state = graph_node_harness.state(
        {
            "id": "cc-only-mail",
            "subject": "【呈兰总阅示】需求风险评估及排期请示",
            "body": "兰总：现将风险评估呈上，请批示。",
            "sender": "Mailbox(name='宗晓婷(Vicky)', email_address='xt_zong@tianjin-air.com')",
            "to": ["Mailbox(name='兰娟(juliet)', email_address='lanjuan@tianjin-air.com')"],
            "cc": ["Mailbox(name='傅强3', email_address='q-fu@tianjin-air.com')"],
        }
    )

    llm_classification = {
        "priority": "P1",
        "need_reply": True,
        "intent": "审批",
        "summary": "傅强呈报需求风险评估，请求领导审批。",
        "reasoning": "正式审批申请，需要领导决策。",
        "confidence": 0.95,
    }
    with patch(
        "src.nodes.categorizer.get_routing_engine",
        return_value=mock_engine,
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_fixed_classification(llm_classification),
    ):
        from src.nodes.categorizer import categorize_email

        result = await categorize_email(state, graph_node_harness.dependencies)

    # Skill 显式决定的字段保持权威；LLM 只补充未决定的字段。
    assert result["classification"]["need_reply"] is False
    assert result["classification"]["priority"] == "P3"
    assert result["classification"]["summary"] == "傅强呈报需求风险评估，请求领导审批。"
    assert result["classification"]["intent"] == "审批"
    assert result["next_step"] == "end"
    assert result["active_skills"] == ["skill_auto_cc"]


@pytest.mark.asyncio
async def test_tier1_skill_need_reply_true_is_not_overridden_by_llm(
    graph_node_harness,
):
    """Skill 判定需要回复时，LLM 也不得降级为不回复。"""
    mock_engine = MagicMock()
    mock_engine.execute_router = AsyncMock(
        return_value=_skill_routed_state(
            {
                "priority": "P1",
                "need_reply": True,
                "card_type": "approval",
                "reasoning": "[Skill Match: Direct Recipient] 确保回复。",
            }
        )
    )

    state = graph_node_harness.state(
        {
            "id": "direct-mail",
            "subject": "呈阅知：月度会会议纪要",
            "body": "请各位知悉。",
            "sender": "sender@example.com",
            "to": ["q-fu@tianjin-air.com"],
            "cc": [],
        }
    )

    llm_classification = {
        "priority": "P2",
        "need_reply": False,
        "intent": "通知",
        "summary": "会议纪要通报。",
        "reasoning": "仅需知悉。",
        "confidence": 0.9,
    }
    with patch(
        "src.nodes.categorizer.get_routing_engine",
        return_value=mock_engine,
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_fixed_classification(llm_classification),
    ):
        from src.nodes.categorizer import categorize_email

        result = await categorize_email(state, graph_node_harness.dependencies)

    assert result["classification"]["need_reply"] is True
    assert result["classification"]["priority"] == "P1"
    assert result["classification"]["summary"] == "会议纪要通报。"
    assert result["next_step"] == "rag_search"


@pytest.mark.asyncio
async def test_categorizer_prompt_includes_recipient_context_and_my_role(
    graph_node_harness,
    monkeypatch,
):
    """LLM 分类提示词必须包含发件人/收件人/抄送和系统判定的“我的角色”。"""
    from src.config import get_settings

    monkeypatch.setenv("EXCHANGE_ACCOUNT_EMAIL", "q-fu@tianjin-air.com")
    get_settings.cache_clear()

    mock_engine = MagicMock()
    mock_engine.execute_router = AsyncMock(side_effect=lambda local_state: local_state)

    state = graph_node_harness.state(
        {
            "id": "recipient-context-mail",
            "subject": "【呈兰总阅示】需求风险评估及排期请示",
            "body": "兰总：请批示。",
            "sender": "Mailbox(name='宗晓婷(Vicky)', email_address='xt_zong@tianjin-air.com')",
            "to": ["Mailbox(name='兰娟(juliet)', email_address='lanjuan@tianjin-air.com')"],
            "cc": ["Mailbox(name='傅强3', email_address='q-fu@tianjin-air.com')"],
        }
    )

    captured = {}

    def capture_budget(_role, value, *, budget):
        captured["prompt"] = value

    llm_classification = {
        "priority": "P3",
        "need_reply": False,
        "intent": "通知",
        "summary": "呈领导阅示，抄送我知悉。",
        "reasoning": "我仅在 CC 中。",
        "confidence": 0.9,
    }
    try:
        with patch(
            "src.nodes.categorizer.get_routing_engine",
            return_value=mock_engine,
        ), patch(
            "src.nodes.categorizer.enforce_model_input_budget",
            side_effect=capture_budget,
        ), patch(
            "src.providers.factory.get_llm_for_role",
            return_value=RunnableLambda(lambda value: value),
        ), patch(
            "src.nodes.categorizer.with_llm_retry",
            side_effect=_fixed_classification(llm_classification),
        ):
            from src.nodes.categorizer import categorize_email

            await categorize_email(state, graph_node_harness.dependencies)
    finally:
        get_settings.cache_clear()

    prompt = captured["prompt"]
    assert "发件人: 宗晓婷(Vicky) <xt_zong@tianjin-air.com>" in prompt
    assert "收件人(To): 兰娟(juliet) <lanjuan@tianjin-air.com>" in prompt
    assert "抄送(CC): 傅强3 <q-fu@tianjin-air.com>" in prompt
    # 系统判定的角色行必须位于不可信邮件内容之外。
    trusted_zone = prompt.split("</email_content>", 1)[1]
    assert "仅被抄送" in trusted_zone
    assert "q-fu@tianjin-air.com" in trusted_zone
