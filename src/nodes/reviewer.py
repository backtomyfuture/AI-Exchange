import json
import logging
from langchain_core.prompts import ChatPromptTemplate
from src.graph.state import AgentState
from src.utils.retry_decorator import with_llm_retry

logger = logging.getLogger(__name__)


async def review_draft(state: AgentState) -> AgentState:
    """Review draft quality before human approval. Auto-rewrite once if poor.
    Also runs ContentGuard checks (hallucination + sensitive info)."""
    draft = state.get("draft", "")
    email = state.get("email", {})
    review_count = (state.get("metadata") or {}).get("review_count", 0)

    if not draft or review_count >= 1:
        return await _run_content_guard(state, draft, email)

    from src.providers.factory import get_llm_for_role
    llm = get_llm_for_role("reviewer", temperature=0)

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

    chain = prompt | llm

    @with_llm_retry(max_attempts=2)
    async def invoke_review(payload):
        return await chain.ainvoke(payload)

    try:
        response = await invoke_review({
            "subject": email.get("subject", ""),
            "body": email.get("body", "")[:1000],
            "draft": draft,
        })

        result = json.loads(response.content.strip())

        if result.get("pass", True):
            logger.info("Draft review: PASS")
            return await _run_content_guard(state, draft, email)
        else:
            logger.info("Draft review: FAIL - %s. Requesting rewrite.", result.get("issues"))
            metadata = dict(state.get("metadata") or {})
            metadata["review_count"] = review_count + 1
            metadata["review_issues"] = result.get("issues", "")
            return {
                "metadata": metadata,
                "next_step": "drafter",
            }
    except Exception as e:
        logger.warning("Draft review failed, passing through: %s", e)
        return await _run_content_guard(state, draft, email)


async def _run_content_guard(state: AgentState, draft: str, email: dict) -> AgentState:
    """Run ContentGuard checks and store warnings in metadata."""
    if not draft:
        return state
    try:
        from src.utils.content_guard import ContentGuard
        guard = ContentGuard()
        result = await guard.run_all_checks(draft, email)
        if not result["passed"]:
            metadata = dict(state.get("metadata") or {})
            metadata["content_guard"] = {
                "passed": False,
                "summary": result["summary"],
                "sensitive_issues": result["sensitive_issues"][:5],
                "hallucination_issues": result["hallucination_issues"][:5],
            }
            logger.info("ContentGuard: %s", result["summary"])
            return {**state, "metadata": metadata}
    except Exception as e:
        logger.debug("ContentGuard skipped: %s", e)
    return state
