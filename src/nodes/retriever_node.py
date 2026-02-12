import asyncio
from src.graph.state import AgentState
from src.utils.retriever import get_retriever

async def retrieve_context(state: AgentState) -> AgentState:
    """
    检索节点：根据当前邮件内容从向量数据库中检索相关背景。
    使用 asyncio.to_thread 将同步的向量搜索操作放到线程池执行，避免阻塞事件循环。
    """
    email = state.get("email", {})
    subject = email.get("subject", "")
    body = email.get("body", "")
    sender = email.get("sender", "")

    query_text = f"Subject: {subject}\nBody: {body[:500]}"

    retriever = get_retriever()

    # 使用 to_thread 避免阻塞 asyncio 事件循环
    results = await asyncio.to_thread(
        retriever.search, query_text=query_text, sender=sender, limit=3
    )

    if not results:
        results = await asyncio.to_thread(
            retriever.search, query_text=query_text, limit=3
        )

    return {
        **state,
        "context": results,
        "next_step": "drafter"
    }
