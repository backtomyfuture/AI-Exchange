from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.runnables import RunnableLambda

from src.nodes.categorizer import categorize_email
from src.nodes.drafter import generate_draft
from src.nodes.retriever_node import _generate_thread_summary
from src.nodes.reviewer import review_draft
from src.router.engine import RoutingEngine
from src.safety.model_budget import (
    ModelInputTooLarge,
    TokenBudget,
    conservative_token_upper_bound,
    enforce_model_input_budget,
    token_budget_from_settings,
)
from src.utils.rate_limiter import llm_rate_limiter


def test_utf8_byte_upper_bound_blocks_oversized_prompt():
    budget = TokenBudget(
        max_input_tokens=8,
        max_output_tokens=2,
        max_total_tokens=10,
    )

    with pytest.raises(ModelInputTooLarge):
        enforce_model_input_budget(
            "categorizer",
            "这是一段超过预算的正文",
            budget=budget,
        )


def test_conservative_token_upper_bound_is_utf8_byte_length():
    value = "a汉🙂"

    assert conservative_token_upper_bound(value) == len(value.encode("utf-8"))


def test_model_input_budget_allows_input_and_total_equality():
    budget = TokenBudget(
        max_input_tokens=8,
        max_output_tokens=2,
        max_total_tokens=10,
    )

    enforce_model_input_budget("drafter", "12345678", budget=budget)


def test_model_input_budget_enforces_reserved_output_against_total():
    budget = TokenBudget(
        max_input_tokens=100,
        max_output_tokens=5,
        max_total_tokens=10,
    )

    with pytest.raises(ModelInputTooLarge) as caught:
        enforce_model_input_budget("reviewer", "123456", budget=budget)

    assert caught.value.role == "reviewer"


def test_token_budget_uses_defaults_for_missing_magicmock_settings():
    assert token_budget_from_settings(MagicMock()) == TokenBudget(
        max_input_tokens=122_880,
        max_output_tokens=8_192,
        max_total_tokens=131_072,
    )


def _reject_and_capture(captured: dict):
    def reject(role: str, value: str, *, budget: TokenBudget) -> None:
        captured.update(role=role, value=value, budget=budget)
        raise ModelInputTooLarge(role)

    return reject


def _passthrough_retry(**_kwargs):
    return lambda function: function


def _oversize_provider(role: str) -> RunnableLambda:
    async def raise_oversize(_value):
        raise ModelInputTooLarge(role)

    return RunnableLambda(raise_oversize)


@pytest.mark.asyncio
async def test_categorizer_budgets_complete_rendered_messages_before_retry(
    graph_node_harness,
):
    captured: dict = {}
    provider_calls = 0

    async def provider(_value):
        nonlocal provider_calls
        provider_calls += 1
        return {"never": "called"}

    state = graph_node_harness.state(
        {
            "id": "budget-category",
            "subject": "budget-subject",
            "body": "budget-body",
            "image_analysis": "budget-image-analysis",
        },
        classification={},
        metadata={
            "experience_hints": [
                {
                    "category": "budget-history-category",
                    "pattern": "budget-history-pattern",
                    "confidence": 0.9,
                }
            ]
        },
    )
    router = MagicMock()
    router.execute_router = AsyncMock(return_value=state)

    with patch("src.nodes.categorizer.get_routing_engine", return_value=router), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(provider),
    ), patch(
        "src.nodes.categorizer.enforce_model_input_budget",
        side_effect=_reject_and_capture(captured),
    ), patch.object(
        llm_rate_limiter,
        "acquire",
        new_callable=AsyncMock,
    ) as mock_acquire:
        with pytest.raises(ModelInputTooLarge):
            await categorize_email(state, graph_node_harness.dependencies)

    assert captured["role"] == "categorizer"
    assert "budget-subject" in captured["value"]
    assert "budget-body" in captured["value"]
    assert "budget-image-analysis" in captured["value"]
    assert "budget-history-pattern" in captured["value"]
    assert "priority" in captured["value"]
    assert provider_calls == 0
    mock_acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_drafter_budgets_modifiers_history_and_metadata_before_retry(
    graph_node_harness,
):
    captured: dict = {}
    provider_calls = 0

    async def provider(_value):
        nonlocal provider_calls
        provider_calls += 1
        return SimpleNamespace(content="never called")

    state = graph_node_harness.state(
        {
            "id": "budget-draft",
            "sender": "budget-sender",
            "subject": "budget-subject",
            "body": "budget-body",
        },
        context=[
            {
                "sender": "budget-history-sender",
                "subject": "budget-history-subject",
                "body": "budget-history-body",
            }
        ],
        classification={},
        system_prompt_modifier="budget-system-modifier",
        metadata={
            "style_guidance": "budget-style-guidance",
            "preference_hints": [{"pattern": "budget-preference-hint"}],
            "thread_summary": "budget-thread-summary",
        },
    )

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(provider),
    ), patch(
        "src.nodes.drafter.enforce_model_input_budget",
        side_effect=_reject_and_capture(captured),
    ), patch.object(
        llm_rate_limiter,
        "acquire",
        new_callable=AsyncMock,
    ) as mock_acquire:
        with pytest.raises(ModelInputTooLarge):
            await generate_draft(state, graph_node_harness.dependencies)

    assert captured["role"] == "drafter"
    for fragment in (
        "budget-system-modifier",
        "budget-style-guidance",
        "budget-preference-hint",
        "budget-thread-summary",
        "budget-history-body",
        "budget-sender",
        "budget-subject",
        "budget-body",
    ):
        assert fragment in captured["value"]
    assert provider_calls == 0
    mock_acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_reviewer_budgets_complete_rendered_messages_before_retry(
    graph_node_harness,
):
    captured: dict = {}
    provider_calls = 0

    async def provider(_value):
        nonlocal provider_calls
        provider_calls += 1
        return SimpleNamespace(content='{"pass": true}')

    state = graph_node_harness.state(
        {
            "id": "budget-review",
            "subject": "budget-review-subject",
            "body": "budget-review-body",
        },
        draft="budget-review-draft",
        metadata={},
    )

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(provider),
    ), patch(
        "src.nodes.reviewer.enforce_model_input_budget",
        side_effect=_reject_and_capture(captured),
    ), patch.object(
        llm_rate_limiter,
        "acquire",
        new_callable=AsyncMock,
    ) as mock_acquire:
        with pytest.raises(ModelInputTooLarge):
            await review_draft(state, graph_node_harness.dependencies)

    assert captured["role"] == "reviewer"
    assert "邮件质量审核员" in captured["value"]
    assert "budget-review-subject" in captured["value"]
    assert "budget-review-body" in captured["value"]
    assert "budget-review-draft" in captured["value"]
    assert provider_calls == 0
    mock_acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_thread_summary_budgets_full_prompt_before_model_factory():
    captured: dict = {}
    provider_calls = 0

    async def provider(_value):
        nonlocal provider_calls
        provider_calls += 1
        return SimpleNamespace(content="never called")

    contexts = [
        {
            "sender": "budget-sender-one",
            "subject": "budget-context-subject-one",
            "body": "budget-context-body-one",
        },
        {
            "sender": "budget-sender-two",
            "subject": "budget-context-subject-two",
            "body": "budget-context-body-two",
        },
    ]

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(provider),
    ) as mock_get_llm, patch(
        "src.nodes.retriever_node.enforce_model_input_budget",
        side_effect=_reject_and_capture(captured),
    ):
        with pytest.raises(ModelInputTooLarge):
            await _generate_thread_summary(contexts, "budget-thread-subject")

    mock_get_llm.assert_not_called()
    assert captured["role"] == "summary"
    assert "budget-thread-subject" in captured["value"]
    assert "budget-context-body-one" in captured["value"]
    assert "budget-context-body-two" in captured["value"]
    assert provider_calls == 0


@pytest.mark.asyncio
async def test_thread_summary_uses_summary_role_for_small_prompt():
    llm = MagicMock()
    llm.ainvoke = AsyncMock(
        return_value=SimpleNamespace(content="  concise summary  ")
    )
    contexts = [
        {"sender": "one", "subject": "first", "body": "body one"},
        {"sender": "two", "subject": "second", "body": "body two"},
    ]

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=llm,
    ) as mock_get_llm:
        result = await _generate_thread_summary(contexts, "small subject")

    assert result == "concise summary"
    mock_get_llm.assert_called_once_with("summary", temperature=0)
    llm.ainvoke.assert_awaited_once()


@pytest.mark.asyncio
async def test_tier3_router_budgets_prompt_with_all_skill_descriptions():
    captured: dict = {}
    provider_calls = 0

    async def provider(_value):
        nonlocal provider_calls
        provider_calls += 1
        return SimpleNamespace(content="NONE")

    engine = object.__new__(RoutingEngine)
    skills = {
        "budget-skill-one": SimpleNamespace(
            manifest=SimpleNamespace(description="budget-description-one")
        ),
        "budget-skill-two": SimpleNamespace(
            manifest=SimpleNamespace(description="budget-description-two")
        ),
    }
    state = {
        "email": {
            "subject": "budget-router-subject",
            "body": "budget-router-body",
        }
    }

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(provider),
    ), patch(
        "src.router.engine.enforce_model_input_budget",
        side_effect=_reject_and_capture(captured),
    ):
        with pytest.raises(ModelInputTooLarge):
            await engine._tier3_llm_route(state, skills)

    assert captured["role"] == "router"
    assert "budget-router-subject" in captured["value"]
    assert "budget-router-body" in captured["value"]
    assert "budget-skill-one: budget-description-one" in captured["value"]
    assert "budget-skill-two: budget-description-two" in captured["value"]
    assert provider_calls == 0


@pytest.mark.asyncio
async def test_categorizer_fallback_does_not_swallow_model_input_too_large(
    graph_node_harness,
):
    state = graph_node_harness.state(
        {"id": "oversize-category", "subject": "subject", "body": "body"},
    )
    router = MagicMock()
    router.execute_router = AsyncMock(return_value=state)

    with patch("src.nodes.categorizer.get_routing_engine", return_value=router), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=_oversize_provider("categorizer"),
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_passthrough_retry,
    ):
        with pytest.raises(ModelInputTooLarge):
            await categorize_email(state, graph_node_harness.dependencies)


@pytest.mark.asyncio
async def test_drafter_fallback_does_not_swallow_model_input_too_large(
    graph_node_harness,
):
    state = graph_node_harness.state(
        {
            "id": "oversize-draft",
            "sender": "sender",
            "subject": "subject",
            "body": "body",
        },
        context=[],
    )

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=_oversize_provider("drafter"),
    ), patch(
        "src.nodes.drafter.with_llm_retry",
        side_effect=_passthrough_retry,
    ):
        with pytest.raises(ModelInputTooLarge):
            await generate_draft(state, graph_node_harness.dependencies)


@pytest.mark.asyncio
async def test_reviewer_fallback_does_not_swallow_model_input_too_large(
    graph_node_harness,
):
    state = graph_node_harness.state(
        {"id": "oversize-review", "subject": "subject", "body": "body"},
        draft="draft",
        metadata={},
    )

    with patch(
        "src.providers.factory.get_llm_for_role",
        return_value=_oversize_provider("reviewer"),
    ), patch(
        "src.nodes.reviewer.with_llm_retry",
        side_effect=_passthrough_retry,
    ):
        with pytest.raises(ModelInputTooLarge):
            await review_draft(state, graph_node_harness.dependencies)
