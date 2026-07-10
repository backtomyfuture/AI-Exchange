import asyncio
import logging

from src.config import get_settings
from src.graph.state import AgentState
from src.router.engine import get_routing_engine
from src.safety.model_budget import (
    ModelInputTooLarge,
    enforce_model_input_budget,
    token_budget_from_settings,
)
from src.utils.retriever import get_retriever

logger = logging.getLogger(__name__)


async def retrieve_context(state: AgentState) -> AgentState:
    """
    检索节点：线程检索 → 语义检索 → Tier 2 标签投票 → 经验记忆 → 线程摘要 → 风格指导，
    全程使用 ``asyncio.to_thread`` 避免阻塞事件循环。
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

    # Priority 2b: Tier 2 semantic routing - vote on past similar emails' labels.
    tier2_delta: dict = {}
    try:
        engine = get_routing_engine()
        tier2_delta = await engine.apply_tier2_hits(state, results)
    except Exception as e:
        logger.debug("Tier 2 routing skipped: %s", e)

    # Priority 3: experience memory (Tier 2 enhancement)
    experience_hints = await _retrieve_experience(subject, body, sender)

    metadata = dict(state.get("metadata") or {})
    if experience_hints:
        metadata["experience_hints"] = experience_hints

    # Priority 4: thread summary (Phase 3)
    if results:
        thread_summary = await _generate_thread_summary(results, subject)
        if thread_summary:
            metadata["thread_summary"] = thread_summary

    # Priority 5: style guidance (Phase 5)
    style_guidance = await _retrieve_style_guidance(sender)
    if style_guidance:
        metadata["style_guidance"] = style_guidance

    # Priority 6: user preference hints (Phase 2)
    preference_hints = await _retrieve_user_preferences(subject, sender)
    if preference_hints:
        metadata["preference_hints"] = preference_hints

    updates: dict = {
        "context": results,
        "next_step": "drafter",
    }
    if metadata:
        updates["metadata"] = metadata

    # Merge Tier 2 delta last so it doesn't get clobbered by metadata expansion above.
    # Reducer-controlled fields (active_skills, routing_log, tool_calls) are already
    # delta-shaped by apply_tier2_hits.
    for k, v in tier2_delta.items():
        if k == "metadata" and isinstance(v, dict):
            merged = dict(updates.get("metadata") or {})
            merged.update(v)
            updates["metadata"] = merged
        else:
            updates[k] = v

    return updates


async def _generate_thread_summary(context_results: list[dict], subject: str) -> str:
    """Summarize thread history into a concise progress note for the drafter."""
    if len(context_results) < 2:
        return ""
    try:
        from src.providers.factory import get_llm_for_role

        thread_text_parts = []
        for i, ctx in enumerate(context_results[:5]):
            s = ctx.get("sender", "?")
            sub = ctx.get("subject", "?")
            body_snippet = (ctx.get("body") or ctx.get("chunk_text") or "")[:200]
            thread_text_parts.append(f"[{i+1}] {s} — {sub}\n{body_snippet}")

        prompt = (
            "请用2-3句话概括以下邮件往来的进展状态（这件事进展到哪了），帮助撰写回复。"
            "只输出摘要，不要解释。\n\n"
            f"主题: {subject}\n\n" + "\n---\n".join(thread_text_parts)
        )
        enforce_model_input_budget(
            "summary",
            prompt,
            budget=token_budget_from_settings(get_settings()),
        )
        llm = get_llm_for_role("summary", temperature=0)
        response = await llm.ainvoke(prompt)
        summary = response.content.strip()
        logger.info("Thread summary generated (%d chars)", len(summary))
        return summary
    except ModelInputTooLarge:
        raise
    except Exception as e:
        logger.debug("Thread summary generation skipped: %s", e)
        return ""


async def _retrieve_style_guidance(sender: str) -> str:
    """Retrieve writing style guidance from StyleProfiler."""
    try:
        from src.memory.style_profiler import StyleProfiler
        from src.init_app import app_context
        if not app_context.email_processor:
            return ""
        profiler = StyleProfiler(email_processor=app_context.email_processor)
        return await profiler.get_style_guidance(sender_email=sender)
    except Exception as e:
        logger.debug("Style guidance retrieval skipped: %s", e)
        return ""


async def _retrieve_user_preferences(subject: str, sender: str) -> list[dict]:
    """Retrieve relevant user preferences from PreferenceLearner."""
    try:
        from src.memory.preference_learner import UserPreferenceLearner
        from src.init_app import app_context
        if not app_context.email_processor:
            return []
        learner = UserPreferenceLearner(
            db_manager=app_context.db_manager,
            email_processor=app_context.email_processor,
        )
        context_query = f"sender: {sender}\nsubject: {subject}"
        return await learner.get_preferences(context=context_query, limit=5)
    except Exception as e:
        logger.debug("Preference retrieval skipped: %s", e)
        return []


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
