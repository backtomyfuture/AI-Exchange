import asyncio
from src.graph.state import AgentState
from src.utils.retriever import get_retriever

async def retrieve_context(state: AgentState) -> AgentState:
    """
    检索节点：先按邮件线程检索同会话历史，再用语义搜索补充，避免阻塞事件循环。
    """
    email = state.get("email", {})
    subject = email.get("subject", "")
    body = email.get("body", "")
    sender = email.get("sender", "")
    thread_id = email.get("thread_id") or email.get("conversation_id")

    retriever = get_retriever()
    results = []

    # Priority 1: same-thread context
    if thread_id:
        thread_results = await asyncio.to_thread(
            retriever.search_by_thread, thread_id=thread_id, limit=5
        )
        results.extend(thread_results)

    # Priority 2: semantic search (fill remaining slots)
    remaining = max(0, 5 - len(results))
    if remaining > 0:
        query_text = f"Subject: {subject}\nBody: {body[:500]}"
        semantic_results = await asyncio.to_thread(
            retriever.search, query_text=query_text, sender=sender, limit=remaining
        )
        # Deduplicate by id
        seen_ids = {r.get("id") for r in results}
        for r in semantic_results:
            if r.get("id") not in seen_ids:
                results.append(r)

    return {
        "context": results,
        "next_step": "drafter",
    }
