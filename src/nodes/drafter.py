import logging
from langchain_core.prompts import ChatPromptTemplate
from src.graph.state import AgentState
from src.utils.retry_decorator import with_llm_retry

logger = logging.getLogger(__name__)

async def generate_draft(state: AgentState) -> AgentState:
    """
    拟稿节点：参考检索到的背景生成邮件回复草稿。
    """
    email = state.get("email", {})
    context_list = state.get("context", [])

    # 格式化背景信息
    context_str = ""
    for i, ctx in enumerate(context_list):
        context_str += f"--- 历史邮件 {i+1} ---\n"
        context_str += f"发件人: {ctx.get('sender', '未知')}\n"
        context_str += f"主题: {ctx.get('subject', '无主题')}\n"
        context_str += f"内容: {ctx.get('body', '')[:300]}...\n\n"

    from src.providers.factory import get_llm_for_role
    llm = get_llm_for_role("drafter", temperature=0.7)

    feedback = state.get("feedback")

    if feedback:
        logger.info("Applying user feedback as final draft content.")
        # 用户在弹窗中直接修改了草稿。逻辑：
        # 弹窗输入 (feedback) 就是拟稿的全文正文。
        # 直接使用用户的版本作为新草稿，跳过 LLM 处理以确保 100% 忠于用户的编辑并提高响应速度。
        return {
            "draft": feedback.strip(),
            "feedback": None,
            "approval_status": "pending",
            "next_step": "approval",
        }
    
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
    chain = prompt | llm
    
    @with_llm_retry(max_attempts=3)
    async def invoke_with_retry(payload):
        return await chain.ainvoke(payload)

    # Check for Forwarding action - Skip LLM
    classification = state.get("classification", {})
    if classification.get("action") == "forward":
        logger.info("Action is 'forward'. Skipping LLM draft generation. Using existing draft.")
        return state

    try:
        logger.info("Generating draft with LLM and retrieved context.")
        response = await invoke_with_retry({
            "context": context_str if context_str else "无相关历史背景",
            "extra_context": extra_context,
            "sender": email.get("sender", ""),
            "subject": email.get("subject", ""),
            "body": email.get("body", "")
        })
    except Exception as e:
        # 如果达到最大重试次数，返回错误提示
        from langchain_core.messages import AIMessage
        response = AIMessage(content=f"Error generating draft: {str(e)}")

    # 更新状态，清除反馈以防循环
    import re
    cleaned_draft = response.content
    # 保险起见，清理可能存在的标签
    cleaned_draft = re.sub(r'<thought>.*?</thought>', '', cleaned_draft, flags=re.DOTALL)
    cleaned_draft = re.sub(r'<draft>', '', cleaned_draft)
    cleaned_draft = re.sub(r'</draft>', '', cleaned_draft)
    cleaned_draft = cleaned_draft.strip()

    return {
        "draft": cleaned_draft,
        "feedback": None,
        "approval_status": "pending",
        "next_step": "approval",
    }
