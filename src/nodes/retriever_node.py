import asyncio
import logging

from src.graph.state import AgentState
from src.utils.retriever import get_retriever

logger = logging.getLogger(__name__)


async def retrieve_context(state: AgentState) -> AgentState:
    """
    检索节点：线程检索 → 语义检索 → 经验记忆检索，避免阻塞事件循环。
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
        seen_ids = {r.get("id") for r in results}
        for r in semantic_results:
            if r.get("id") not in seen_ids:
                results.append(r)

    # Priority 3: experience memory (Tier 2 enhancement)
    experience_hints = await _retrieve_experience(subject, body, sender)

    updates: dict = {
        "context": results,
        "next_step": "drafter",
    }
    if experience_hints:
        metadata = dict(state.get("metadata") or {})
        metadata["experience_hints"] = experience_hints
        updates["metadata"] = metadata

    return updates


async def _retrieve_experience(subject: str, body: str, sender: str) -> list[dict]:
    """Retrieve relevant processing experience insights from Qdrant."""
    try:
        from src.init_app import app_context

        if not app_context.email_processor:
            return []

        from src.memory.consolidator import search_experience

        query = f"sender: {sender}\nsubject: {subject}\nbody: {body[:300]}"
        return await search_experience(
            query_text=query,
            email_processor=app_context.email_processor,
            limit=3,
            min_confidence=0.6,
        )
    except Exception as e:
        logger.debug("Experience retrieval skipped: %s", e)
        return []
