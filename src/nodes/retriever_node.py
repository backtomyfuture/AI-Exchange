import asyncio
import logging
from src.config import get_settings
from src.graph.dependencies import GraphDependencies
from src.graph.state import AgentState
from src.graph.state_factory import (
    hydrate_email_for_image_analysis,
    hydrate_email_from_state,
    sanitize_graph_delta,
)
from src.safety.attachments import get_attachment_policy
from src.safety.model_budget import (
    ModelInputTooLarge,
    enforce_model_input_budget,
    token_budget_from_settings,
)
from src.safety.manual_review import build_manual_review_delta
from src.router.decision import RouteDecision
from src.router.tier1.schema import CanonicalRoute
from src.utils import image_analyzer
from src.utils.email_body_projection import project_email_body_for_model
from src.utils.retriever import get_retriever

logger = logging.getLogger(__name__)


def _visual_analysis_inputs(email: object) -> list[dict[str, str]]:
    if not isinstance(email, dict):
        return []
    attachments = email.get("attachments")
    if not isinstance(attachments, list):
        return []

    attachment_policy = get_attachment_policy()
    images: list[dict[str, str]] = []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        decision = attachment_policy.assess(attachment)
        if not decision.allowed or not decision.is_image or decision.content is None:
            continue
        suffix = decision.name.rsplit(".", 1)[-1].casefold()
        mime_type = {
            "gif": "image/gif",
            "jpeg": "image/jpeg",
            "jpg": "image/jpeg",
            "png": "image/png",
            "webp": "image/webp",
        }[suffix]
        images.append(
            {
                "name": decision.name,
                "content": attachment["content"],
                "mime_type": mime_type,
            }
        )
    return images


async def retrieve_context(
    state: AgentState,
    dependencies: GraphDependencies,
) -> AgentState:
    """
    检索节点：写作证据 → 经验记忆 → 线程摘要 → 风格指导。

    Production supplies a persisted final route before this node. This node
    can enrich writing evidence but cannot select or change that route.
    全程使用 ``asyncio.to_thread`` 避免阻塞事件循环。
    """
    email = await hydrate_email_from_state(state, dependencies)
    try:
        decision = RouteDecision.model_validate(state.get("route_decision"))
    except Exception:
        return build_manual_review_delta(state, "router_execution_failed")
    if decision.route not in {CanonicalRoute.REPLY, CanonicalRoute.FORWARD}:
        return {}
    body_projection = project_email_body_for_model(
        email.get("body", ""),
        unique_body=email.get("unique_body"),
    )
    # Semantic matching and fallback routing describe the current turn.  Using
    # recursively quoted history here would vote on an older task instead of
    # the sender's latest update.
    email["body"] = body_projection.current_text
    subject = email.get("subject", "")
    body = email.get("body", "")
    sender = email.get("sender", "")
    thread_id = email.get("thread_id") or email.get("conversation_id")
    email_id = state.get("email_id") or email.get("id")

    retriever = get_retriever()
    results = []

    precomputed_evidence = bool(state.get("evidence_pack_digest"))
    if precomputed_evidence:
        results = [
            {
                "id": item.get("id"),
                "sender": item.get("sender", ""),
                "subject": item.get("subject", ""),
                "body": item.get("snippet", ""),
            }
            for item in state.get("context_summaries", [])
            if isinstance(item, dict)
        ]

    # Priority 1: same-thread writing context
    if not precomputed_evidence and thread_id:
        thread_results = await asyncio.to_thread(
            retriever.search_by_thread,
            thread_id=thread_id,
            limit=5,
            exclude_email_id=email_id,
        )
        results.extend(thread_results)

    # Priority 2: semantic search (fill remaining slots)
    remaining = max(0, 5 - len(results))
    if not precomputed_evidence and remaining > 0:
        query_text = f"Subject: {subject}\nBody: {body[:500]}"
        semantic_results = await asyncio.to_thread(
            retriever.search,
            query_text=query_text,
            sender=sender,
            limit=remaining,
            exclude_email_id=email_id,
        )
        seen_ids = {r.get("id") for r in results}
        for r in semantic_results:
            if r.get("id") not in seen_ids:
                results.append(r)

    # Legacy enrichment remains available only when no durable profile has
    # already constrained the evidence sources for this handoff.
    experience_hints = (
        []
        if precomputed_evidence
        else await _retrieve_experience(subject, body, sender)
    )

    metadata = dict(state.get("metadata") or {})
    if experience_hints:
        metadata["experience_hints"] = experience_hints

    # Priority 4: thread summary (Approval)
    if results:
        try:
            thread_summary = await _generate_thread_summary(results, subject)
        except ModelInputTooLarge:
            logger.warning("Thread summary skipped: reason=input_too_large")
            thread_summary = ""
        except Exception as exc:
            logger.error(
                "Thread summary unavailable; continuing without it: error_type=%s",
                type(exc).__name__,
            )
            thread_summary = ""
        if thread_summary:
            metadata["thread_summary"] = thread_summary

    # Priority 5: style guidance (Phase 5)
    style_guidance = (
        ""
        if precomputed_evidence
        else await _retrieve_style_guidance(sender)
    )
    if style_guidance:
        metadata["style_guidance"] = style_guidance

    # Priority 6: user preference hints (Polling)
    preference_hints = (
        []
        if precomputed_evidence
        else await _retrieve_user_preferences(subject, sender)
    )
    if preference_hints:
        metadata["preference_hints"] = preference_hints

    image_email = await hydrate_email_for_image_analysis(state, dependencies)
    image_inputs = _visual_analysis_inputs(image_email)
    if image_inputs:
        try:
            image_analysis = await image_analyzer.analyze_images(image_inputs)
        except Exception as exc:
            logger.warning(
                "Visual summary unavailable: error_type=%s",
                type(exc).__name__,
            )
            image_analysis = "图片分析暂不可用，请查看原始邮件图片。"
        if image_analysis:
            metadata["image_analysis"] = image_analysis

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

    updates: dict = {"context_summaries": context_summaries}
    if metadata:
        updates["metadata"] = metadata

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
