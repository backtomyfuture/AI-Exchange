import logging
from langchain_core.prompts import ChatPromptTemplate
from src.config import get_settings
from src.graph.state import AgentState
from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import hydrate_email_from_state, sanitize_graph_delta
from src.safety.model_budget import (
    ModelInputTooLarge,
    enforce_model_input_budget,
    rendered_messages_for_budget,
    token_budget_from_settings,
)
from src.utils.retry_decorator import with_llm_retry

logger = logging.getLogger(__name__)

async def generate_draft(
    state: AgentState,
    dependencies: GraphDependencies,
) -> AgentState:
    """
    拟稿节点：参考检索到的背景生成邮件回复草稿。
    """
    email = await hydrate_email_from_state(state, dependencies)
    context_list = state.get("context_summaries", [])

    # 格式化背景信息
    context_str = ""
    for i, ctx in enumerate(context_list):
        context_str += f"--- 历史邮件 {i+1} ---\n"
        context_str += f"发件人: {ctx.get('sender', '未知')}\n"
        context_str += f"主题: {ctx.get('subject', '无主题')}\n"
        context_str += f"内容摘要: {ctx.get('snippet', '')}\n\n"
    
    # --- 初始拟稿逻辑 (仅在没有 feedback 时执行) ---
    base_system_prompt = """你是一个专业的行政助手。
你的任务是根据提供的【历史背景】和【当前邮件】，代用户拟写一封回复邮件。

要求：
1. 参考历史背景中的信息，确保回复的一致性和准确性。
2. 模仿用户的稳重、专业且礼貌的写作风格。
3. 直接输出最终的邮件回复正文。
4. 不要输出 <thought> 或 <draft> 标签，也不要包含任何解释性文字。
5. 【重要】绝对不要包含原邮件内容、发件人信息或引用历史。系统会自动追加，如果你输出了会导致重复。只输出你的回复部分即可。

请使用中文回复。"""

    modifier = state.get("system_prompt_modifier")
    if modifier:
        base_system_prompt = base_system_prompt + "\n\n" + modifier.strip()

    metadata = state.get("metadata") or {}

    review_issues = metadata.get("review_issues", "")
    if review_issues:
        base_system_prompt += (
            "\n\n【上一轮审核修订要求】:\n"
            + str(review_issues)
            + "\n请在新草稿中逐项修正，仍只输出完整回复正文。"
        )

    style_guidance = metadata.get("style_guidance", "")
    if style_guidance:
        base_system_prompt += "\n\n【写作风格参考】:\n" + style_guidance

    pref_hints = metadata.get("preference_hints") or []
    if pref_hints:
        pref_lines = []
        for p in pref_hints[:5]:
            pattern = p.get("pattern", "")
            if pattern:
                pref_lines.append(f"- {pattern}")
        if pref_lines:
            base_system_prompt += "\n\n【用户偏好（基于历史修改学习）】:\n" + "\n".join(pref_lines)

    thread_summary = metadata.get("thread_summary", "")
    extra_context = ""
    if thread_summary:
        extra_context = f"\n\n【会话进展摘要】:\n{thread_summary}"

    prompt = ChatPromptTemplate.from_messages([
        ("system", base_system_prompt),
        ("user", """【历史背景】:
{context}{extra_context}

<email_content>
【当前待回复邮件】:
发件人: {sender}
主题: {subject}
正文:
{body}
</email_content>""")
    ])
    # Forwarding skills may have already persisted their fixed draft in categorizer.
    classification = state.get("classification", {})
    if classification.get("action") == "forward":
        draft_id = state.get("draft_id")
        if not draft_id:
            draft_id = await dependencies.drafts.save_draft(
                state["email_id"],
                "呈阅",
            )
        return sanitize_graph_delta(
            state,
            {
                "draft_id": draft_id,
                "approval_status": "pending",
                "next_step": "approval",
            },
        )

    payload = {
        "context": context_str if context_str else "无相关历史背景",
        "extra_context": extra_context,
        "sender": email.get("sender", ""),
        "subject": email.get("subject", ""),
        "body": email.get("body", ""),
    }
    rendered_prompt = rendered_messages_for_budget(
        prompt.format_messages(**payload)
    )
    enforce_model_input_budget(
        "drafter",
        rendered_prompt,
        budget=token_budget_from_settings(get_settings()),
    )

    from src.providers.factory import get_llm_for_role
    llm = get_llm_for_role("drafter", temperature=0.7)
    chain = prompt | llm

    @with_llm_retry(max_attempts=3)
    async def invoke_with_retry(payload):
        return await chain.ainvoke(payload)

    try:
        logger.info(
            "Generating draft: reviewer_rewrite=%s",
            bool(review_issues),
        )
        response = await invoke_with_retry(payload)
    except ModelInputTooLarge:
        raise
    except Exception as exc:
        logger.error(
            "Draft generation failed: error_type=%s",
            type(exc).__name__,
        )
        # Safe user-facing draft; never persist exception text.
        from langchain_core.messages import AIMessage
        response = AIMessage(content="草稿生成暂时失败，请稍后重试。")

    # 更新状态，清除反馈以防循环
    import re
    cleaned_draft = str(response.content or "")
    # 保险起见，清理可能存在的标签
    cleaned_draft = re.sub(r'<thought>.*?</thought>', '', cleaned_draft, flags=re.DOTALL)
    cleaned_draft = re.sub(r'<draft>', '', cleaned_draft)
    cleaned_draft = re.sub(r'</draft>', '', cleaned_draft)
    cleaned_draft = cleaned_draft.strip()

    draft_id = await dependencies.drafts.save_draft(state["email_id"], cleaned_draft)
    return sanitize_graph_delta(
        state,
        {
            "draft_id": draft_id,
            "approval_status": "pending",
            "next_step": "approval",
        },
    )
