"""Fail-closed contracts for model and safety-review decision paths."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.runnables import RunnableLambda

from src.nodes.categorizer import categorize_email
from src.nodes.drafter import generate_draft
from src.nodes.retriever_node import retrieve_context
from src.nodes.reviewer import review_draft
from src.router.engine import RoutingEngine
from src.safety.model_budget import ModelInputTooLarge


def _retry_outcome(outcome, *, calls: list[object] | None = None):
    """Replace retry decoration while preserving the node's invocation boundary."""

    def factory(**_kwargs):
        def decorator(_function):
            async def wrapped(payload):
                if calls is not None:
                    calls.append(payload)
                if isinstance(outcome, BaseException):
                    raise outcome
                return outcome

            return wrapped

        return decorator

    return factory


def _assert_manual_review(result: dict, *raw_failures: str) -> None:
    assert result["next_step"] == "manual_review"
    assert result["approval_status"] == "manual_review"
    safe_code = result["safe_error_summary"]
    assert isinstance(safe_code, str)
    assert 0 < len(safe_code.encode("utf-8")) <= 256

    encoded = json.dumps(result, ensure_ascii=False)
    for raw_failure in raw_failures:
        assert raw_failure not in encoded


def _assert_manual_classification(result: dict, *raw_failures: str) -> None:
    _assert_manual_review(result, *raw_failures)
    assert "need_reply" not in (result.get("classification") or {})


def _identity_router() -> SimpleNamespace:
    return SimpleNamespace(
        execute_router=AsyncMock(side_effect=lambda state: state),
    )


@pytest.mark.asyncio
async def test_categorizer_timeout_requires_manual_review(graph_node_harness):
    raw_error = "categorizer-timeout-private-detail"
    state = graph_node_harness.state(
        {"id": "categorizer-timeout", "subject": "Q", "body": "body"}
    )

    with patch(
        "src.nodes.categorizer.get_routing_engine", return_value=_identity_router()
    ), patch(
        "src.nodes.categorizer.enforce_model_input_budget"
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_retry_outcome(TimeoutError(raw_error)),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await categorize_email(state, graph_node_harness.dependencies)

    _assert_manual_classification(result, raw_error)


@pytest.mark.asyncio
async def test_categorizer_invalid_schema_requires_manual_review(graph_node_harness):
    state = graph_node_harness.state(
        {"id": "categorizer-schema", "subject": "Q", "body": "body"}
    )

    with patch(
        "src.nodes.categorizer.get_routing_engine", return_value=_identity_router()
    ), patch(
        "src.nodes.categorizer.enforce_model_input_budget"
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_retry_outcome({"priority": "P1"}),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await categorize_email(state, graph_node_harness.dependencies)

    _assert_manual_classification(result)


@pytest.mark.asyncio
async def test_categorizer_token_overflow_requires_manual_review(graph_node_harness):
    state = graph_node_harness.state(
        {"id": "categorizer-budget", "subject": "Q", "body": "body"}
    )

    with patch(
        "src.nodes.categorizer.get_routing_engine", return_value=_identity_router()
    ), patch(
        "src.nodes.categorizer.enforce_model_input_budget",
        side_effect=ModelInputTooLarge("categorizer"),
    ):
        result = await categorize_email(state, graph_node_harness.dependencies)

    _assert_manual_classification(result)


@pytest.mark.asyncio
async def test_tier3_router_failure_requires_manual_review():
    raw_error = "router-timeout-private-detail"
    engine = RoutingEngine()
    skill = SimpleNamespace(manifest=SimpleNamespace(description="route mail"))
    engine.t1_router.route = MagicMock(return_value=[])
    engine.skill_manager.get_all_skills = MagicMock(return_value={"skill_x": skill})
    model = SimpleNamespace(ainvoke=AsyncMock(side_effect=TimeoutError(raw_error)))
    state = {
        "email": {
            "id": "router-failure",
            "sender": "sender@example.com",
            "subject": "Q",
            "body": "body",
        },
        "classification": {},
        "active_skills": [],
        "routing_log": [],
        "metadata": {},
    }

    with patch("src.router.engine.enforce_model_input_budget"), patch(
        "src.providers.factory.get_llm_for_role", return_value=model
    ):
        result = await engine.apply_tier3_fallback(state)

    _assert_manual_review(result, raw_error)


@pytest.mark.asyncio
async def test_thread_summary_failure_requires_manual_review(graph_node_harness):
    raw_error = "summary-timeout-private-detail"
    state = graph_node_harness.state(
        {"id": "summary-failure", "subject": "Q", "body": "body"}
    )
    retriever = MagicMock()
    retriever.search.return_value = [
        {"id": "old-1", "sender": "one@example.com", "subject": "Q", "body": "a"},
        {"id": "old-2", "sender": "two@example.com", "subject": "Q", "body": "b"},
    ]
    router = SimpleNamespace(
        apply_tier2_hits=AsyncMock(return_value={}),
        apply_tier3_fallback=AsyncMock(return_value={"routing_stage": "none"}),
    )
    model = SimpleNamespace(ainvoke=AsyncMock(side_effect=TimeoutError(raw_error)))

    with patch("src.nodes.retriever_node.get_retriever", return_value=retriever), patch(
        "src.nodes.retriever_node.get_routing_engine", return_value=router
    ), patch(
        "src.nodes.retriever_node.enforce_model_input_budget"
    ), patch(
        "src.providers.factory.get_llm_for_role", return_value=model
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
        result = await retrieve_context(state, graph_node_harness.dependencies)

    _assert_manual_review(result, raw_error)


@pytest.mark.asyncio
async def test_drafter_failure_does_not_write_placeholder_draft(graph_node_harness):
    raw_error = "drafter-timeout-private-detail"
    state = graph_node_harness.state(
        {"id": "drafter-timeout", "subject": "Q", "body": "body"}
    )

    with patch("src.nodes.drafter.enforce_model_input_budget"), patch(
        "src.nodes.drafter.with_llm_retry",
        side_effect=_retry_outcome(TimeoutError(raw_error)),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await generate_draft(state, graph_node_harness.dependencies)

    assert graph_node_harness.draft_saves == []
    _assert_manual_review(result, raw_error)


@pytest.mark.asyncio
async def test_drafter_empty_response_does_not_write_empty_draft(graph_node_harness):
    state = graph_node_harness.state(
        {"id": "drafter-empty", "subject": "Q", "body": "body"}
    )

    with patch("src.nodes.drafter.enforce_model_input_budget"), patch(
        "src.nodes.drafter.with_llm_retry",
        side_effect=_retry_outcome(SimpleNamespace(content="  \n")),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await generate_draft(state, graph_node_harness.dependencies)

    assert graph_node_harness.draft_saves == []
    _assert_manual_review(result)


async def _review_outcome(graph_node_harness, response_or_error):
    state = graph_node_harness.state(
        {"id": "review-model", "subject": "Q", "body": "body"},
        draft="candidate draft",
        metadata={},
    )
    outcome = (
        response_or_error
        if isinstance(response_or_error, BaseException)
        else SimpleNamespace(content=response_or_error)
    )
    guard_result = {
        "passed": True,
        "summary": "ok",
        "sensitive_issues": [],
        "hallucination_issues": [],
    }

    with patch("src.nodes.reviewer.enforce_model_input_budget"), patch(
        "src.nodes.reviewer.with_llm_retry", side_effect=_retry_outcome(outcome)
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.utils.content_guard.ContentGuard.run_all_checks",
        new_callable=AsyncMock,
        return_value=guard_result,
    ):
        return await review_draft(state, graph_node_harness.dependencies)


@pytest.mark.asyncio
async def test_reviewer_invalid_json_requires_manual_review(graph_node_harness):
    result = await _review_outcome(graph_node_harness, "not-json")

    _assert_manual_review(result)


@pytest.mark.asyncio
async def test_reviewer_missing_pass_requires_manual_review(graph_node_harness):
    result = await _review_outcome(
        graph_node_harness,
        '{"issues": "model omitted its decision"}',
    )

    _assert_manual_review(result)


@pytest.mark.asyncio
async def test_reviewer_timeout_requires_manual_review(graph_node_harness):
    raw_error = "reviewer-timeout-private-detail"
    result = await _review_outcome(graph_node_harness, TimeoutError(raw_error))

    _assert_manual_review(result, raw_error)


@pytest.mark.asyncio
async def test_second_failed_review_is_rechecked_then_requires_manual_review(
    graph_node_harness,
):
    state = graph_node_harness.state(
        {"id": "review-second-failure", "subject": "Q", "body": "body"},
        draft="rewritten candidate",
        metadata={"review_count": 1, "review_issues": "first review failed"},
    )
    calls: list[object] = []

    with patch("src.nodes.reviewer.enforce_model_input_budget"), patch(
        "src.nodes.reviewer.with_llm_retry",
        side_effect=_retry_outcome(
            SimpleNamespace(content='{"pass": false, "issues": "still incomplete"}'),
            calls=calls,
        ),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await review_draft(state, graph_node_harness.dependencies)

    assert len(calls) == 1
    _assert_manual_review(result)


@pytest.mark.asyncio
async def test_content_guard_exception_requires_manual_review(graph_node_harness):
    raw_error = "content-guard-private-detail"
    state = graph_node_harness.state(
        {"id": "guard-failure", "subject": "Q", "body": "body"},
        draft="candidate draft",
        metadata={},
    )

    with patch("src.nodes.reviewer.enforce_model_input_budget"), patch(
        "src.nodes.reviewer.with_llm_retry",
        side_effect=_retry_outcome(
            SimpleNamespace(content='{"pass": true, "issues": ""}')
        ),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.utils.content_guard.ContentGuard.run_all_checks",
        new_callable=AsyncMock,
        side_effect=RuntimeError(raw_error),
    ):
        result = await review_draft(state, graph_node_harness.dependencies)

    _assert_manual_review(result, raw_error)


@pytest.mark.asyncio
async def test_empty_draft_requires_manual_review(graph_node_harness):
    state = graph_node_harness.state(
        {"id": "review-empty", "subject": "Q", "body": "body"},
        draft="",
        metadata={},
    )

    result = await review_draft(state, graph_node_harness.dependencies)

    _assert_manual_review(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("need_reply", "confidence"),
    [
        ("false", 0.8),
        (0, 0.8),
        ("yes", 0.8),
        (True, True),
    ],
)
async def test_categorizer_rejects_coerced_decision_types(
    graph_node_harness,
    need_reply,
    confidence,
):
    state = graph_node_harness.state(
        {"id": "categorizer-strict", "subject": "Q", "body": "body"}
    )
    classification = {
        "priority": "P1",
        "need_reply": need_reply,
        "intent": "咨询",
        "summary": "summary",
        "reasoning": "reason",
        "confidence": confidence,
    }

    with patch(
        "src.nodes.categorizer.get_routing_engine",
        return_value=_identity_router(),
    ), patch(
        "src.nodes.categorizer.with_llm_retry",
        side_effect=_retry_outcome(classification),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await categorize_email(state, graph_node_harness.dependencies)

    _assert_manual_classification(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ["", "unknown_skill", "NONE, skill_x"])
async def test_tier3_invalid_decision_requires_manual_review(content):
    engine = RoutingEngine()
    skill = SimpleNamespace(manifest=SimpleNamespace(description="route mail"))
    engine.t1_router.route = MagicMock(return_value=[])
    engine.skill_manager.get_all_skills = MagicMock(return_value={"skill_x": skill})
    model = SimpleNamespace(
        ainvoke=AsyncMock(return_value=SimpleNamespace(content=content))
    )
    state = {
        "email": {"id": "router-invalid", "subject": "Q", "body": "body"},
        "classification": {},
    }

    with patch("src.router.engine.enforce_model_input_budget"), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=model,
    ):
        result = await engine.apply_tier3_fallback(state)

    _assert_manual_review(result)


@pytest.mark.asyncio
async def test_large_router_failure_still_returns_bounded_manual_delta():
    engine = RoutingEngine()
    skill = SimpleNamespace(manifest=SimpleNamespace(description="route mail"))
    engine.t1_router.route = MagicMock(return_value=[])
    engine.skill_manager.get_all_skills = MagicMock(return_value={"skill_x": skill})
    model = SimpleNamespace(ainvoke=AsyncMock(side_effect=TimeoutError("private")))
    state = {
        "email": {"id": "router-large", "subject": "Q", "body": "b" * 20_000},
        "classification": {},
    }

    with patch("src.router.engine.enforce_model_input_budget"), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=model,
    ):
        result = await engine.apply_tier3_fallback(state)

    _assert_manual_review(result, "private")
    assert "email" not in result


@pytest.mark.asyncio
async def test_empty_thread_summary_requires_manual_review(graph_node_harness):
    state = graph_node_harness.state(
        {"id": "summary-empty", "subject": "Q", "body": "body"}
    )
    retriever = MagicMock()
    retriever.search.return_value = [
        {"id": "old-1", "sender": "one@example.com", "subject": "Q", "body": "a"},
        {"id": "old-2", "sender": "two@example.com", "subject": "Q", "body": "b"},
    ]
    router = SimpleNamespace(
        apply_tier2_hits=AsyncMock(return_value={}),
        apply_tier3_fallback=AsyncMock(return_value={"routing_stage": "none"}),
    )
    model = SimpleNamespace(
        ainvoke=AsyncMock(return_value=SimpleNamespace(content="  \n"))
    )

    with patch("src.nodes.retriever_node.get_retriever", return_value=retriever), patch(
        "src.nodes.retriever_node.get_routing_engine",
        return_value=router,
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=model,
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
        result = await retrieve_context(state, graph_node_harness.dependencies)

    _assert_manual_review(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [
        SimpleNamespace(content=["not", "text"]),
        SimpleNamespace(),
    ],
)
async def test_drafter_rejects_non_string_model_content(
    graph_node_harness,
    outcome,
):
    state = graph_node_harness.state(
        {"id": "drafter-invalid-content", "subject": "Q", "body": "body"}
    )

    with patch(
        "src.nodes.drafter.with_llm_retry",
        side_effect=_retry_outcome(outcome),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ):
        result = await generate_draft(state, graph_node_harness.dependencies)

    assert graph_node_harness.draft_saves == []
    _assert_manual_review(result)


@pytest.mark.asyncio
async def test_content_guard_rejection_requires_manual_review(graph_node_harness):
    state = graph_node_harness.state(
        {"id": "guard-rejected", "subject": "Q", "body": "body"},
        draft="candidate draft",
        metadata={},
    )
    guard_result = {
        "passed": False,
        "summary": "PRIVATE-GUARD-DETAIL",
        "sensitive_issues": [{"category": "secret"}],
        "hallucination_issues": [],
    }

    with patch(
        "src.nodes.reviewer.with_llm_retry",
        side_effect=_retry_outcome(
            SimpleNamespace(content='{"pass": true, "issues": ""}')
        ),
    ), patch(
        "src.providers.factory.get_llm_for_role",
        return_value=RunnableLambda(lambda value: value),
    ), patch(
        "src.utils.content_guard.ContentGuard.run_all_checks",
        new=AsyncMock(return_value=guard_result),
    ):
        result = await review_draft(state, graph_node_harness.dependencies)

    _assert_manual_review(result, "PRIVATE-GUARD-DETAIL")


@pytest.mark.asyncio
async def test_tier2_skill_failure_requires_manual_review(graph_node_harness):
    raw_error = "tier2-skill-private-detail"
    state = graph_node_harness.state(
        {"id": "tier2-failure", "subject": "Q", "body": "body"}
    )
    retriever = MagicMock()
    retriever.search.return_value = []
    router = SimpleNamespace(
        apply_tier2_hits=AsyncMock(side_effect=RuntimeError(raw_error))
    )

    with patch(
        "src.nodes.retriever_node.get_retriever", return_value=retriever
    ), patch(
        "src.nodes.retriever_node.get_routing_engine", return_value=router
    ):
        result = await retrieve_context(state, graph_node_harness.dependencies)

    _assert_manual_review(result, raw_error)


@pytest.mark.asyncio
async def test_missing_configured_skill_is_not_silently_ignored():
    engine = RoutingEngine()
    engine.skill_manager.get_skill = MagicMock(return_value=None)

    with pytest.raises(RuntimeError, match="router_skill_failed"):
        await engine._apply_skills(
            {"email": {"id": "missing-skill"}},
            ["missing_skill"],
        )


@pytest.mark.asyncio
async def test_later_skill_failure_does_not_leak_earlier_partial_update():
    engine = RoutingEngine()
    original = {
        "email": {"id": "partial-skill"},
        "metadata": {"original": True},
    }
    first = SimpleNamespace(
        manifest=SimpleNamespace(depends_on=[]),
        execute=AsyncMock(return_value={"metadata": {"partial": True}}),
    )
    second = SimpleNamespace(
        manifest=SimpleNamespace(depends_on=[]),
        execute=AsyncMock(side_effect=RuntimeError("PRIVATE-SKILL-FAILURE")),
    )
    engine.skill_manager.get_skill = MagicMock(
        side_effect=lambda skill_id: {"first": first, "second": second}.get(skill_id)
    )

    with pytest.raises(RuntimeError, match="router_skill_failed"):
        await engine._apply_skills(original, ["first", "second"])

    assert original == {
        "email": {"id": "partial-skill"},
        "metadata": {"original": True},
    }
