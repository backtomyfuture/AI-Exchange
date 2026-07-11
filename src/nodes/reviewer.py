import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from src.config import get_settings
from src.graph.dependencies import GraphDependencies
from src.graph.state import AgentState
from src.graph.state_factory import hydrate_graph_content, sanitize_graph_delta
from src.safety.model_budget import (
    ModelInputTooLarge,
    enforce_model_input_budget,
    rendered_messages_for_budget,
    token_budget_from_settings,
)
from src.utils.retry_decorator import with_llm_retry

logger = logging.getLogger(__name__)


async def review_draft(
    state: AgentState,
    dependencies: GraphDependencies,
) -> AgentState:
    """Review draft quality before human approval. Auto-rewrite once if poor.
    Also runs ContentGuard checks (hallucination + sensitive info)."""
    email, draft = await hydrate_graph_content(state, dependencies)
    review_count = (state.get("metadata") or {}).get("review_count", 0)

    if not draft or review_count >= 1:
        return await _run_content_guard(state, draft, email)

    prompt = ChatPromptTemplate.from_messages([
        ("system", """你是一个邮件质量审核员。请评估以下回复草稿的质量。

检查项：
1. 是否遗漏了原始邮件中的关键问题或请求
2. 语气是否专业得体
3. 信息是否准确（不编造事实）
4. 是否完整回应了邮件的核心诉求

请输出 JSON：{{"pass": true/false, "issues": "问题描述（如有）"}}
只输出 JSON，不要其他文字。"""),
        ("user", """原始邮件主题: {subject}
原始邮件正文: {body}

回复草稿:
{draft}""")
    ])

    payload = {
        "subject": email.get("subject", ""),
        "body": email.get("body", "")[:1000],
        "draft": draft,
    }
    rendered_prompt = rendered_messages_for_budget(
        prompt.format_messages(**payload)
    )
    enforce_model_input_budget(
        "reviewer",
        rendered_prompt,
        budget=token_budget_from_settings(get_settings()),
    )

    from src.providers.factory import get_llm_for_role
    llm = get_llm_for_role("reviewer", temperature=0)
    chain = prompt | llm

    @with_llm_retry(max_attempts=2)
    async def invoke_review(payload):
        return await chain.ainvoke(payload)

    try:
        response = await invoke_review(payload)

        result = json.loads(response.content.strip())

        if result.get("pass", True):
            logger.info("Draft review: PASS")
            return await _run_content_guard(state, draft, email)
        else:
            issues = result.get("issues", "")
            logger.info(
                "Draft review failed: issues_present=%s issues_bytes=%d",
                bool(issues),
                len(str(issues).encode("utf-8")),
            )
            metadata = dict(state.get("metadata") or {})
            metadata["review_count"] = review_count + 1
            metadata["review_issues"] = issues
            return {
                **sanitize_graph_delta(
                    state,
                    {
                        "metadata": metadata,
                        "review_result": {
                            "passed": False,
                            "issues": result.get("issues", ""),
                        },
                        "next_step": "drafter",
                    },
                )
            }
    except ModelInputTooLarge:
        raise
    except Exception as exc:
        logger.warning(
            "Draft review failed, passing through: error_type=%s",
            type(exc).__name__,
        )
        return await _run_content_guard(state, draft, email)


async def _run_content_guard(state: AgentState, draft: str, email: dict) -> AgentState:
    """Run ContentGuard checks and store warnings in metadata."""
    if not draft:
        return sanitize_graph_delta(
            state,
            {
                "review_result": {"passed": False, "issues": "empty_draft"},
                "next_step": "approval",
            },
        )
    try:
        from src.utils.content_guard import ContentGuard
        guard = ContentGuard()
        result = await guard.run_all_checks(draft, email)
        if not result["passed"]:
            metadata = dict(state.get("metadata") or {})
            metadata["content_guard"] = {
                "passed": False,
                "summary": result["summary"],
                "sensitive_issues": [
                    issue.get("category", "sensitive")
                    for issue in result["sensitive_issues"][:5]
                    if isinstance(issue, dict)
                ],
                "hallucination_issues": [
                    issue.get("type", "unverified")
                    for issue in result["hallucination_issues"][:5]
                    if isinstance(issue, dict)
                ],
            }
            logger.info(
                "ContentGuard found issues: sensitive_count=%d hallucination_count=%d",
                len(result["sensitive_issues"]),
                len(result["hallucination_issues"]),
            )
            return sanitize_graph_delta(
                state,
                {
                    "metadata": metadata,
                    "review_result": {
                        "passed": False,
                        "summary": result["summary"],
                    },
                    "next_step": "approval",
                },
            )
    except Exception as exc:
        logger.debug("ContentGuard skipped: error_type=%s", type(exc).__name__)
    return sanitize_graph_delta(
        state,
        {
            "review_result": {"passed": True, "summary": "检查通过"},
            "next_step": "approval",
        },
    )
