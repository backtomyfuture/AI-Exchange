from src.graph.state import AgentState
from src.utils.retriever import EmailRetriever

def retrieve_context(state: AgentState) -> AgentState:
    """
    检索节点：根据当前邮件内容从向量数据库中检索相关背景。
    """
    email = state.get("email", {})
    subject = email.get("subject", "")
    body = email.get("body", "")
    sender = email.get("sender", "")

    # 组合查询文本：主题 + 正文的一部分
    query_text = f"Subject: {subject}\nBody: {body[:500]}"

    retriever = EmailRetriever()

    # 检索历史邮件，优先搜索同一发件人的记录
    results = retriever.search(query_text=query_text, sender=sender, limit=3)

    # 如果同一发件人没搜到，尝试全局搜索
    if not results:
        results = retriever.search(query_text=query_text, limit=3)

    # 更新状态中的 context
    return {
        **state,
        "context": results,
        "next_step": "drafter"
    }
