import os
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from src.graph.state import AgentState
from src.utils.rate_limiter import llm_rate_limiter

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

    # 初始化 LLM
    from src.utils.llm_factory import LLMFactory
    llm = LLMFactory.create_llm(temperature=0.7)

    feedback = state.get("feedback")
    prev_draft = state.get("draft")

    if feedback:
        # 用户在弹窗中直接修改了草稿。逻辑：
        # 弹窗输入 (feedback) 就是拟稿的全文正文。
        # 直接使用用户的版本作为新草稿，跳过 LLM 处理以确保 100% 忠于用户的编辑并提高响应速度。
        return {
             **state,
             "draft": feedback.strip(),
             "feedback": None,
             "approval_status": "pending",
             "next_step": "approval"
        }
    
    # --- 初始拟稿逻辑 (仅在没有 feedback 时执行) ---
    prompt = ChatPromptTemplate.from_messages([
        ("system", """你是一个专业的行政助手。
你的任务是根据提供的【历史背景】和【当前邮件】，代用户拟写一封回复邮件。

要求：
1. 参考历史背景中的信息，确保回复的一致性和准确性。
2. 模仿用户的稳重、专业且礼貌的写作风格。
3. 直接输出最终的邮件回复正文。
4. 不要输出 <thought> 或 <draft> 标签，也不要包含任何解释性文字。

请使用中文回复。"""),
        ("user", """【历史背景】:
{context}

【当前待回复邮件】:
发件人: {sender}
主题: {subject}
正文:
{body}""")
    ])
    chain = prompt | llm
    
    from tenacity import retry, stop_after_attempt, wait_random_exponential
    
    @retry(
        wait=wait_random_exponential(multiplier=2, max=120),
        stop=stop_after_attempt(12),
        reraise=True
    )
    async def invoke_with_retry(payload):
        # 在调用前获取全局限流许可
        await llm_rate_limiter.acquire()
        return await chain.ainvoke(payload)

    try:
        response = await invoke_with_retry({
            "context": context_str if context_str else "无相关历史背景",
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
        **state,
        "draft": cleaned_draft,
        "feedback": None,
        "approval_status": "pending",
        "next_step": "approval"
    }
