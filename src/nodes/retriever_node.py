import asyncio
import logging
from copy import deepcopy

from src.config import get_settings
from src.graph.dependencies import GraphDependencies
from src.graph.state import AgentState
from src.graph.state_factory import hydrate_email_from_state, sanitize_graph_delta
from src.router.engine import get_routing_engine
from src.safety.model_budget import (
    ModelInputTooLarge,
    enforce_model_input_budget,
    token_budget_from_settings,
)
from src.safety.manual_review import build_manual_review_delta
from src.utils.retriever import get_retriever

logger = logging.getLogger(__name__)


def _merge_unique(existing: object, incoming: object) -> list:
    result = list(existing) if isinstance(existing, list) else []
    if not isinstance(incoming, list):
        return result
    for item in incoming:
        if item not in result:
            result.append(item)
    return result


async def retrieve_context(
    state: AgentState,
    dependencies: GraphDependencies,
) -> AgentState:
    """
    检索节点：线程检索 → 语义检索 → Tier 2 标签投票 → 经验记忆 → 线程摘要 → 风格指导，
    全程使用 ``asyncio.to_thread`` 避免阻塞事件循环。
    """
    email = await hydrate_email_from_state(state, dependencies)
    local_state = deepcopy(dict(state))
    local_state["email"] = email
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
        tier2_delta = await engine.apply_tier2_hits(local_state, results)
    except Exception as exc:
        logger.error(
            "Tier 2 routing failed; manual review required: error_type=%s",
            type(exc).__name__,
        )
        return build_manual_review_delta(state, "router_skill_failed")

    # Priority 3: experience memory (Tier 2 enhancement)
    experience_hints = await _retrieve_experience(subject, body, sender)

    metadata = dict(state.get("metadata") or {})
    if experience_hints:
        metadata["experience_hints"] = experience_hints

    # Priority 4: thread summary (Phase 3)
    if results:
        try:
            thread_summary = await _generate_thread_summary(results, subject)
        except ModelInputTooLarge:
            return build_manual_review_delta(state, "summary_input_too_large")
        except Exception as exc:
            logger.error(
                "Thread summary unavailable; manual review required: error_type=%s",
                type(exc).__name__,
            )
            return build_manual_review_delta(state, "summary_model_failed")
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

    context_summaries = []
    for result in results[:5]:
        if not isinstance(result, dict):
            continue
        context_summaries.append(
            {
                "id": result.get("id") or result.get("email_id") or "",
                "sender": result.get("sender", ""),
                "subject": result.get("subject", ""),
                "snippet": result.get("body") or result.get("chunk_text") or "",
            }
        )

    updates: dict = {
        "context_summaries": context_summaries,
        "next_step": "drafter",
    }
    if metadata:
        updates["metadata"] = metadata

    fixed_draft = tier2_delta.pop("_draft_content", None)
    if isinstance(fixed_draft, str):
        updates["draft_id"] = await dependencies.drafts.save_draft(
            state["email_id"],
            fixed_draft,
        )

    # List fields have replacement semantics. Merge and de-duplicate explicitly
    # before the common byte/item caps are applied.
    for k, v in tier2_delta.items():
        if k == "metadata" and isinstance(v, dict):
            merged = dict(updates.get("metadata") or {})
            merged.update(v)
            updates["metadata"] = merged
        elif k in {"active_skills", "routing_log", "tool_calls"}:
            updates[k] = _merge_unique(state.get(k), v)
        else:
            updates[k] = v

    return sanitize_graph_delta(state, updates)


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
        content = getattr(response, "content", None)
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError("summary_empty_response")
        summary = content.strip()
        logger.info("Thread summary generated (%d chars)", len(summary))
        return summary
    except ModelInputTooLarge:
        raise
    except Exception as exc:
        logger.error(
            "Thread summary generation failed: error_type=%s",
            type(exc).__name__,
        )
        raise RuntimeError("summary_model_failed") from None


async def _retrieve_style_guidance(sender: str) -> str:
    """Retrieve writing style guidance from StyleProfiler."""
    try:
        from src.memory.style_profiler import StyleProfiler
        from src.init_app import app_context
        if not app_context.email_processor:
            return ""
        profiler = StyleProfiler(email_processor=app_context.email_processor)
        return await profiler.get_style_guidance(sender_email=sender)
    except Exception as exc:
        logger.debug(
            "Style guidance retrieval skipped: error_type=%s",
            type(exc).__name__,
        )
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
    except Exception as exc:
        logger.debug(
            "Preference retrieval skipped: error_type=%s",
            type(exc).__name__,
        )
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
    except Exception as exc:
        logger.debug(
            "Experience retrieval skipped: error_type=%s",
            type(exc).__name__,
        )
        return []
