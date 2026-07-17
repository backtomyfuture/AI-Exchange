from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Sequence

from langchain_core.runnables import RunnableConfig

from src.domain.send_result import ExchangeSendOutcome, ExchangeSendResult
from src.graph.dependencies import GraphDependencies
from src.graph.state import AgentState
from src.graph.state_factory import hydrate_graph_content, sanitize_graph_delta
from src.safety.approval_claim import (
    claim_send,
    complete_send,
    mark_send_unknown,
    move_to_manual_review,
)
from src.safety.manual_review import build_manual_review_delta
from src.safety.recipients import normalize_recipient_address

logger = logging.getLogger(__name__)


def _deduplicate(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


async def send_final_email(
    state: AgentState,
    dependencies: GraphDependencies,
    config: RunnableConfig | None = None,
) -> AgentState:
    """Send once behind the persisted ``approved -> sending`` claim."""
    del config
    if state.get("approval_status", "pending") != "approved":
        return sanitize_graph_delta(state, {"next_step": "approval"})

    from lark_oapi.api.contact.v3 import GetUserRequest

    from src.init_app import get_app_context
    import src.utils.lark_app as lark_app

    ctx = get_app_context()
    email_id = state["email_id"]

    async def fail_before_send(code: str) -> AgentState:
        moved = await move_to_manual_review(
            email_id,
            ctx.db_manager,
            expected=frozenset({"approved"}),
            code=code,
        )
        if not moved:
            return sanitize_graph_delta(state, {"next_step": "end"})
        return build_manual_review_delta(state, code)

    async def fail_after_send(code: str) -> AgentState:
        try:
            await mark_send_unknown(
                email_id,
                ctx.db_manager,
                code=code,
            )
        except Exception as exc:
            logger.error(
                "Send quarantine persistence failed: error_type=%s",
                type(exc).__name__,
            )
        return build_manual_review_delta(state, code)

    try:
        email_data, draft = await hydrate_graph_content(state, dependencies)
    except Exception as exc:
        logger.error(
            "Send payload hydration failed: error_type=%s",
            type(exc).__name__,
        )
        return await fail_before_send("approval_handoff_failed")

    if not isinstance(draft, str) or not draft.strip():
        return await fail_before_send("empty_draft")

    async def resolve_recipient(recipient: object) -> str | None:
        if recipient is None:
            return None
        try:
            value = str(recipient).strip()
        except Exception as exc:
            logger.error(
                "Recipient conversion failed: error_type=%s",
                type(exc).__name__,
            )
            return None
        if not value:
            return None
        if "open_id=" in value:
            open_id = value.replace("open_id=", "").strip()
            client = lark_app.lark_api_client
            if not open_id or not client:
                logger.warning("Lark recipient resolution unavailable")
                return None
            try:
                request = (
                    GetUserRequest.builder()
                    .user_id(open_id)
                    .user_id_type("open_id")
                    .build()
                )
                response = await asyncio.to_thread(
                    client.contact.v3.user.get,
                    request,
                )
                if response.success() and response.data and response.data.user:
                    resolved = (
                        response.data.user.enterprise_email
                        or response.data.user.email
                    )
                    return normalize_recipient_address(resolved)
                logger.warning("Lark recipient resolution returned no email")
                return None
            except Exception as exc:
                logger.error(
                    "Lark recipient resolution failed: error_type=%s",
                    type(exc).__name__,
                )
                return None

        if "email_address=" in value:
            match = re.search(r"email_address=['\"](.*?)['\"]", value)
            if not match or not match.group(1).strip():
                return None
            return normalize_recipient_address(match.group(1))
        return normalize_recipient_address(value)

    raw_to = state.get("draft_to") or []
    raw_cc = state.get("draft_cc") or []
    if not isinstance(raw_to, Sequence) or isinstance(raw_to, (str, bytes)):
        return await fail_before_send("recipient_resolution_failed")
    if not isinstance(raw_cc, Sequence) or isinstance(raw_cc, (str, bytes)):
        return await fail_before_send("recipient_resolution_failed")

    resolved_to = [await resolve_recipient(recipient) for recipient in raw_to]
    resolved_cc = [await resolve_recipient(recipient) for recipient in raw_cc]
    if (
        not resolved_to
        or any(recipient is None for recipient in resolved_to)
        or any(recipient is None for recipient in resolved_cc)
    ):
        return await fail_before_send("recipient_resolution_failed")

    final_to = _deduplicate([recipient for recipient in resolved_to if recipient])
    final_cc = _deduplicate([recipient for recipient in resolved_cc if recipient])
    if not final_to:
        return await fail_before_send("recipient_resolution_failed")

    action = (state.get("classification") or {}).get("action", "reply")
    if action not in {"reply", "forward"}:
        return await fail_before_send("approval_handoff_failed")

    try:
        claimed = await claim_send(email_id, ctx.db_manager)
    except Exception as exc:
        logger.error(
            "Send claim outcome is ambiguous: error_type=%s",
            type(exc).__name__,
        )
        return sanitize_graph_delta(state, {"next_step": "end"})

    if not claimed:
        return sanitize_graph_delta(state, {"next_step": "end"})

    try:
        if action == "forward":
            send_result = await ctx.exchange_client.forward_email_result(
                email_id=email_id,
                to=_deduplicate([*final_to, *final_cc]),
                body=draft,
            )
        else:
            send_result = await ctx.exchange_client.reply_email_result(
                email_id=email_id,
                body=draft,
                to=final_to,
                cc=final_cc,
            )
    except asyncio.CancelledError:
        await asyncio.shield(fail_after_send("send_outcome_unknown"))
        raise
    except Exception as exc:
        logger.error(
            "Exchange send outcome is unknown: error_type=%s",
            type(exc).__name__,
        )
        send_result = ExchangeSendResult.unknown()

    if (
        not isinstance(send_result, ExchangeSendResult)
        or send_result.outcome is not ExchangeSendOutcome.SENT
    ):
        return await fail_after_send("send_outcome_unknown")

    try:
        completed = await complete_send(email_id, ctx.db_manager)
    except Exception as exc:
        logger.error(
            "Send completion outcome is ambiguous: error_type=%s",
            type(exc).__name__,
        )
        try:
            completed = await ctx.db_manager.get_email_status(email_id) == "sent"
        except Exception as read_exc:
            logger.error(
                "Send completion readback failed: error_type=%s",
                type(read_exc).__name__,
            )
            completed = False
    if not completed:
        return await fail_after_send("send_outcome_unknown")

    try:
        ctx.email_processor.process_sent_email(
            original_email_data=email_data,
            reply_content=draft,
        )
    except Exception as exc:
        logger.error(
            "Post-send projection failed: error_type=%s",
            type(exc).__name__,
        )
    logger.info("Email send completed: action=%s", action)
    return sanitize_graph_delta(state, {"next_step": "end"})
