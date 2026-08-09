import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.runnables import RunnableLambda

from src.domain.send_result import ExchangeSendResult
from src.graph.dependencies import GraphDependencies
from src.graph.builder import build_graph
from src.graph.state_factory import build_initial_graph_state, hydrate_graph_content
from src.nodes.categorizer import categorize_email
from src.nodes.drafter import generate_draft
from src.nodes.retriever_node import retrieve_context
from src.nodes.reviewer import review_draft
from src.nodes.sender import send_final_email
from src.storage import ContentRef


@pytest.fixture(autouse=True)
def _configured_content_account(monkeypatch):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )


class FakeContentStore:
    def __init__(self, email):
        self.email = deepcopy(email)
        self.loads = []

    async def load_email(self, ref, *, include_attachments=False):
        self.loads.append((ref, include_attachments))
        return deepcopy(self.email)


class FakeDraftStore:
    def __init__(self):
        self.values = {}
        self.saves = []
        self.loads = []

    async def save_draft(self, email_id, content):
        self.saves.append((email_id, content))
        self.values[email_id] = content
        return email_id

    async def load_draft(self, draft_id):
        self.loads.append(draft_id)
        return self.values[draft_id]


def _ref():
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000007",
        key_version="v1",
        sha256="0" * 64,
    )


def _state(email=None):
    email = email or {
        "id": "mail-1",
        "subject": "subject",
        "sender": "sender@example.com",
        "body": "CURRENT-BODY-SENTINEL",
        "attachments": [{"name": "secret", "content": "BASE64-SENTINEL"}],
    }
    return build_initial_graph_state(email, _ref())


def _dependencies(email=None):
    content_store = FakeContentStore(email or {
        "id": "mail-1",
        "subject": "subject",
        "sender": "sender@example.com",
        "body": "CURRENT-BODY-SENTINEL",
        "attachments": [{"name": "secret"}],
    })
    drafts = FakeDraftStore()
    return GraphDependencies(content_store=content_store, drafts=drafts)


@pytest.mark.asyncio
async def test_hydration_rejects_a_draft_owned_by_another_email():
    dependencies = _dependencies()
    dependencies.drafts.values["mail-2"] = "OTHER-MAIL-DRAFT-SENTINEL"
    state = _state()
    state["draft_id"] = "mail-2"

    with pytest.raises(ValueError, match="draft_email_mismatch"):
        await hydrate_graph_content(state, dependencies)

    assert dependencies.drafts.loads == []


def _classification_retry(**_kwargs):
    def decorator(_function):
        async def wrapped(_payload):
            return {
                "priority": "P1",
                "need_reply": True,
                "intent": "咨询",
                "summary": "s" * 2_000,
                "reasoning": "r" * 2_000,
                "confidence": 0.9,
            }

        return wrapped

    return decorator


def _draft_retry(content):
    def factory(**_kwargs):
        def decorator(_function):
            async def wrapped(_payload):
                return SimpleNamespace(content=content)

            return wrapped

        return decorator

    return factory


def test_graph_builder_accepts_the_shared_dependencies():
    graph = build_graph(dependencies=_dependencies())

    assert {"categorizer", "retriever", "drafter", "reviewer", "sender"}.issubset(
        graph.get_graph().nodes
    )


@pytest.mark.asyncio
async def test_categorizer_hydrates_body_locally_and_returns_only_bounded_delta():
    dependencies = _dependencies()
    state = _state()
    captured = {}
    router = MagicMock()
    router.execute_router = AsyncMock(side_effect=lambda local_state: local_state)

    def capture_budget(_role, value, *, budget):
        captured["prompt"] = value

    with patch("src.nodes.categorizer.get_routing_engine", return_value=router), patch(
        "src.nodes.categorizer.with_llm_retry", side_effect=_classification_retry
    ), patch(
        "src.nodes.categorizer.enforce_model_input_budget", side_effect=capture_budget
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await categorize_email(state, dependencies)

    assert "CURRENT-BODY-SENTINEL" in captured["prompt"]
    assert dependencies.content_store.loads == [(_ref(), False)]
    assert set(result) == {"classification", "next_step"}
    assert len(result["classification"]["summary"].encode()) <= 512
    assert "email" not in result
    assert "content_ref" not in result


@pytest.mark.asyncio
async def test_categorizer_sends_latest_reply_separately_from_quoted_history():
    email = {
        "id": "mail-evolution",
        "subject": "答复: 外部信息单据待处理提醒",
        "sender": "sender@example.com",
        "to": [],
        "cc": [],
        "body": """
            <p>呈阅</p>
            <div style="border:none;border-top:solid #E1E1E1 1.0pt">
              <p><b>发件人:</b> 数字化安全管理平台信箱</p>
              <p><b>发送时间:</b> 2026年7月28日 23:12</p>
              <p><b>收件人:</b> 信息技术部</p>
              <p><b>主题:</b> 外部信息单据待处理提醒</p>
              <p>请及时填写信息评估结论。</p>
            </div>
        """,
    }
    dependencies = _dependencies(email)
    state = _state(email)
    captured = {}

    def fake_model(prompt_value):
        captured["prompt"] = prompt_value.to_string()
        return json.dumps(
            {
                "priority": "P3",
                "need_reply": False,
                "intent": "通知",
                "summary": "转呈历史通知供阅知。",
                "reasoning": "本轮新增内容只有呈阅，没有要求回复。",
                "confidence": 1.0,
            },
            ensure_ascii=False,
        )

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(fake_model),
    ):
        result = await categorize_email(state, dependencies)

    assert "<current_message>\n呈阅\n</current_message>" in captured["prompt"]
    assert "<quoted_history>" in captured["prompt"]
    assert "请及时填写信息评估结论。" in captured["prompt"]
    current_section = captured["prompt"].split("<current_message>", 1)[1].split(
        "</current_message>",
        1,
    )[0]
    assert "请及时填写信息评估结论。" not in current_section
    # need_reply here comes from the LLM's own classification, not a Tier 1
    # override: skill_auto_1446 (the body_match skill that used to fire on
    # "呈阅") was retired during the Tier 1 v1 migration -- it had no
    # sender/to/cc anchor, which the new schema requires and the old one
    # didn't. The current/quoted-history separation this test actually
    # exercises is asserted above via captured["prompt"].
    assert result["classification"]["need_reply"] is False


@pytest.mark.asyncio
async def test_categorizer_does_not_route_on_keyword_found_only_in_history():
    email = {
        "id": "mail-history-keyword",
        "subject": "答复: 项目材料",
        "sender": "sender@example.com",
        "to": [],
        "cc": [],
        "body": """
            <p>请继续修改本轮材料，完成后回复。</p>
            <div class="gmail_quote">
              <p>呈阅，请知悉。</p>
            </div>
        """,
    }
    dependencies = _dependencies(email)
    state = _state(email)

    def fake_model(_prompt_value):
        return json.dumps(
            {
                "priority": "P1",
                "need_reply": True,
                "intent": "审批",
                "summary": "要求继续修改并回复。",
                "reasoning": "本轮新增内容提出了明确任务。",
                "confidence": 1.0,
            },
            ensure_ascii=False,
        )

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(fake_model),
    ):
        result = await categorize_email(state, dependencies)

    # skill_auto_1446 (retired, see comment in the test above) used to be the
    # probe for "a keyword only in quoted history must not trigger Tier 1";
    # that guarantee now lives in the current/quoted-history split itself.
    assert result["classification"]["need_reply"] is True
    assert result["next_step"] == "rag_search"


@pytest.mark.asyncio
async def test_categorizer_persists_fixed_forward_draft_and_recipients():
    dependencies = _dependencies()
    state = _state()
    router = MagicMock()

    async def route(local_state):
        local_state = deepcopy(local_state)
        local_state["classification"] = {
            "priority": "P0",
            "need_reply": True,
            "intent": "转发",
            "action": "forward",
        }
        local_state["email"]["draft_to"] = ["boss@example.com"]
        local_state["email"]["draft_cc"] = ["observer@example.com"]
        local_state["draft"] = "呈阅"
        return local_state

    router.execute_router = AsyncMock(side_effect=route)

    with patch("src.nodes.categorizer.get_routing_engine", return_value=router):
        result = await categorize_email(state, dependencies)

    assert dependencies.drafts.saves == [("mail-1", "呈阅")]
    assert result["draft_id"] == "mail-1"
    assert result["draft_to"] == ["boss@example.com"]
    assert result["draft_cc"] == ["observer@example.com"]
    assert "draft" not in result
    assert "email" not in result


@pytest.mark.asyncio
async def test_retriever_keeps_complete_hits_local_and_returns_capped_summaries():
    email = {
        "id": "mail-1",
        "subject": "subject",
        "sender": "sender@example.com",
        "body": (
            "<p>CURRENT-BODY-SENTINEL</p>"
            '<img src="data:image/png;base64,UkFXLUlNQUdFLUJZVEVT">'
            '<div class="gmail_quote"><p>QUOTED-OLD-TASK-SENTINEL</p></div>'
        ),
    }
    dependencies = _dependencies(email)
    state = _state(email)
    state["classification"] = {"need_reply": False}
    complete_hit = "COMPLETE-HIT-SENTINEL-" + "x" * 5_000
    retriever = MagicMock()
    retriever.search.return_value = [
        {"id": "old-1", "sender": "one", "subject": "first", "body": complete_hit},
        {"id": "old-2", "sender": "two", "subject": "second", "body": complete_hit},
    ]
    router = MagicMock()
    router.apply_tier2_hits = AsyncMock(return_value={})
    router.apply_tier3_fallback = AsyncMock(
        return_value={"routing_stage": "none"}
    )

    with patch("src.nodes.retriever_node.get_retriever", return_value=retriever), patch(
        "src.nodes.retriever_node.get_routing_engine", return_value=router
    ), patch(
        "src.nodes.retriever_node._generate_thread_summary",
        new_callable=AsyncMock,
        return_value="small thread summary",
    ), patch(
        "src.nodes.retriever_node._retrieve_experience",
        new_callable=AsyncMock,
        return_value=[],
    ), patch(
        "src.nodes.retriever_node._retrieve_style_guidance",
        new_callable=AsyncMock,
        return_value="",
    ), patch(
        "src.nodes.retriever_node._retrieve_user_preferences",
        new_callable=AsyncMock,
        return_value=[],
    ):
        result = await retrieve_context(state, dependencies)

    assert "CURRENT-BODY-SENTINEL" in retriever.search.call_args.kwargs["query_text"]
    assert "[内嵌图片]" in retriever.search.call_args.kwargs["query_text"]
    assert "QUOTED-OLD-TASK-SENTINEL" not in retriever.search.call_args.kwargs[
        "query_text"
    ]
    assert "data:image" not in retriever.search.call_args.kwargs["query_text"]
    assert retriever.search.call_args.kwargs["exclude_email_id"] == "mail-1"
    routed_email = router.apply_tier2_hits.await_args.args[0]["email"]
    assert routed_email["body"] == "CURRENT-BODY-SENTINEL\n[内嵌图片]"
    encoded = json.dumps(result, ensure_ascii=False)
    assert "context" not in result
    assert len(result["context_summaries"]) == 2
    assert complete_hit not in encoded
    assert "COMPLETE-HIT-SENTINEL" in encoded
    assert result["metadata"]["thread_summary"] == "small thread summary"
    assert dependencies.content_store.loads == [(_ref(), False)]


@pytest.mark.asyncio
async def test_reply_required_retrieval_projects_visual_summary_without_image_bytes():
    email = {
        "id": "mail-vision",
        "subject": "请结合图片回复",
        "sender": "sender@example.com",
        "body": '<p>正文</p><img src="cid:chart.png">',
        "attachments": [
            {
                "name": "chart.png",
                "content_type": "image/png",
                "content_id": "chart.png",
                "is_inline": True,
                "content": "iVBORw0KGgo=",
            },
            {
                "name": "terms.pdf",
                "content_type": "application/pdf",
                "content": "JVBERi0xLjcK",
            },
        ],
    }
    dependencies = _dependencies(email)
    state = _state(email)
    state["classification"] = {
        "priority": "P1",
        "need_reply": True,
        "intent": "咨询",
    }
    retriever = MagicMock()
    retriever.search.return_value = []
    router = MagicMock()
    router.apply_tier2_hits = AsyncMock(return_value={})
    router.apply_tier3_fallback = AsyncMock(
        return_value={"routing_stage": "none"}
    )

    with patch("src.nodes.retriever_node.get_retriever", return_value=retriever), patch(
        "src.nodes.retriever_node.get_routing_engine", return_value=router
    ), patch(
        "src.nodes.retriever_node._retrieve_experience",
        new_callable=AsyncMock,
        return_value=[],
    ), patch(
        "src.nodes.retriever_node._retrieve_style_guidance",
        new_callable=AsyncMock,
        return_value="",
    ), patch(
        "src.nodes.retriever_node._retrieve_user_preferences",
        new_callable=AsyncMock,
        return_value=[],
    ), patch(
        "src.utils.image_analyzer.analyze_images",
        new_callable=AsyncMock,
        return_value="图表显示本月成本下降 12%。",
    ) as analyze:
        result = await retrieve_context(state, dependencies)

    analyze.assert_awaited_once_with(
        [
            {
                "name": "chart.png",
                "content": "iVBORw0KGgo=",
                "mime_type": "image/png",
            }
        ]
    )
    assert dependencies.content_store.loads == [(_ref(), False), (_ref(), True)]
    assert result["metadata"]["image_analysis"] == "图表显示本月成本下降 12%。"
    encoded = json.dumps(result, ensure_ascii=False)
    assert "iVBORw0KGgo=" not in encoded
    assert "attachments" not in result
    assert "email" not in result


@pytest.mark.asyncio
async def test_drafter_saves_complete_draft_and_returns_only_draft_id():
    dependencies = _dependencies()
    state = _state()
    state["context_summaries"] = [
        {"id": "old", "sender": "old-sender", "subject": "old-subject", "snippet": "old"}
    ]
    complete_draft = "COMPLETE-DRAFT-SENTINEL-" + "d" * 20_000
    captured = {}

    def capture_budget(_role, value, *, budget):
        captured["prompt"] = value

    with patch(
        "src.nodes.drafter.with_llm_retry", side_effect=_draft_retry(complete_draft)
    ), patch(
        "src.nodes.drafter.enforce_model_input_budget", side_effect=capture_budget
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await generate_draft(state, dependencies)

    assert "CURRENT-BODY-SENTINEL" in captured["prompt"]
    assert dependencies.drafts.saves == [("mail-1", complete_draft)]
    assert result == {
        "draft_id": "mail-1",
        "approval_status": "pending",
        "next_step": "approval",
    }
    assert complete_draft not in json.dumps(result)


@pytest.mark.asyncio
async def test_drafter_distinguishes_current_request_from_quoted_history():
    email = {
        "id": "mail-draft-evolution",
        "subject": "答复: 项目材料",
        "sender": "sender@example.com",
        "body": """
            <p>本轮请仅确认收到。</p>
            <div class="gmail_quote">
              <p>旧请求：请重新编制预算并提交说明。</p>
            </div>
        """,
    }
    dependencies = _dependencies(email)
    state = _state(email)
    captured = {}

    def capture_budget(_role, value, *, budget):
        captured["prompt"] = value

    with patch(
        "src.nodes.drafter.with_llm_retry",
        side_effect=_draft_retry("收到，谢谢。"),
    ), patch(
        "src.nodes.drafter.enforce_model_input_budget",
        side_effect=capture_budget,
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        await generate_draft(state, dependencies)

    assert "<current_message>\n本轮请仅确认收到。\n</current_message>" in captured[
        "prompt"
    ]
    assert "<quoted_history>" in captured["prompt"]
    current_section = captured["prompt"].split("<current_message>", 1)[1].split(
        "</current_message>",
        1,
    )[0]
    assert "旧请求：请重新编制预算并提交说明。" not in current_section


@pytest.mark.asyncio
async def test_drafter_rewrite_uses_bounded_review_issues_and_replaces_store_value():
    dependencies = _dependencies()
    dependencies.drafts.values["mail-1"] = "FIRST-DRAFT-SENTINEL"
    state = _state()
    state["draft_id"] = "mail-1"
    state["metadata"] = {
        "review_count": 1,
        "review_issues": "REVIEW-ISSUE-SENTINEL: 回答遗漏核心请求",
    }
    captured = {}

    def capture_budget(_role, value, *, budget):
        captured["prompt"] = value

    with patch(
        "src.nodes.drafter.with_llm_retry",
        side_effect=_draft_retry("SECOND-DRAFT-SENTINEL"),
    ), patch(
        "src.nodes.drafter.enforce_model_input_budget", side_effect=capture_budget
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await generate_draft(state, dependencies)

    assert "REVIEW-ISSUE-SENTINEL" in captured["prompt"]
    assert dependencies.drafts.saves[-1] == ("mail-1", "SECOND-DRAFT-SENTINEL")
    assert result["draft_id"] == "mail-1"
    assert "FIRST-DRAFT-SENTINEL" not in json.dumps(result)
    assert "SECOND-DRAFT-SENTINEL" not in json.dumps(result)


@pytest.mark.asyncio
async def test_reviewer_loads_draft_and_email_locally_without_returning_either():
    dependencies = _dependencies()
    dependencies.drafts.values["mail-1"] = "COMPLETE-DRAFT-SENTINEL"
    state = _state()
    state["draft_id"] = "mail-1"
    captured = {}

    def capture_budget(_role, value, *, budget):
        captured["prompt"] = value

    with patch(
        "src.nodes.reviewer.with_llm_retry",
        side_effect=_draft_retry('{"pass": true, "issues": ""}'),
    ), patch(
        "src.nodes.reviewer.enforce_model_input_budget", side_effect=capture_budget
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await review_draft(state, dependencies)

    assert "CURRENT-BODY-SENTINEL" in captured["prompt"]
    assert "COMPLETE-DRAFT-SENTINEL" in captured["prompt"]
    assert dependencies.drafts.loads == ["mail-1"]
    assert "email" not in result
    assert "draft" not in result
    assert result["review_result"]["passed"] is True


@pytest.mark.asyncio
async def test_reviewer_checks_current_request_separately_from_quoted_history():
    email = {
        "id": "mail-review-evolution",
        "subject": "答复: 项目材料",
        "sender": "sender@example.com",
        "body": """
            <p>本轮请仅确认收到。</p>
            <div class="gmail_quote">
              <p>旧请求：请重新编制预算并提交说明。</p>
            </div>
        """,
    }
    dependencies = _dependencies(email)
    dependencies.drafts.values[email["id"]] = "已收到，谢谢。"
    state = _state(email)
    state["draft_id"] = email["id"]
    captured = {}

    def capture_budget(_role, value, *, budget):
        captured["prompt"] = value

    with patch(
        "src.nodes.reviewer.with_llm_retry",
        side_effect=_draft_retry('{"pass": true, "issues": ""}'),
    ), patch(
        "src.nodes.reviewer.enforce_model_input_budget",
        side_effect=capture_budget,
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        await review_draft(state, dependencies)

    assert "<current_message>\n本轮请仅确认收到。\n</current_message>" in captured[
        "prompt"
    ]
    assert "<quoted_history>" in captured["prompt"]
    current_section = captured["prompt"].split("<current_message>", 1)[1].split(
        "</current_message>",
        1,
    )[0]
    assert "旧请求：请重新编制预算并提交说明。" not in current_section


@pytest.mark.asyncio
async def test_sender_hydrates_content_and_draft_then_returns_small_delta():
    dependencies = _dependencies()
    dependencies.drafts.values["mail-1"] = "COMPLETE-DRAFT-SENTINEL"
    state = _state()
    state.update(
        {
            "draft_id": "mail-1",
            "draft_to": ["to@example.com"],
            "draft_cc": ["cc@example.com"],
            "approval_status": "approved",
        }
    )
    persisted_status = {"value": "approved"}

    async def compare_and_set_status(_email_id, *, expected, target):
        if persisted_status["value"] not in expected:
            return False
        persisted_status["value"] = target
        return True

    ctx = SimpleNamespace(
        exchange_client=SimpleNamespace(
            reply_email_result=AsyncMock(
                return_value=ExchangeSendResult.sent()
            )
        ),
        db_manager=SimpleNamespace(
            compare_and_set_status=AsyncMock(side_effect=compare_and_set_status),
        ),
        email_processor=SimpleNamespace(process_sent_email=MagicMock()),
    )

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(state, dependencies)

    ctx.exchange_client.reply_email_result.assert_awaited_once_with(
        email_id="mail-1",
        body="COMPLETE-DRAFT-SENTINEL",
        to=["to@example.com"],
        cc=["cc@example.com"],
    )
    assert result == {"next_step": "end"}
    assert persisted_status["value"] == "sent"
    assert "email" not in result
    assert "draft" not in result
