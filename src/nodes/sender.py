from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from uuid import uuid4

from langchain_core.runnables import RunnableConfig

from src.domain.send_result import ExchangeSendOutcome, ExchangeSendResult
from src.graph.dependencies import GraphDependencies
from src.graph.state import AgentState
from src.graph.state_factory import (
    hydrate_email_from_state,
    sanitize_graph_delta,
)
from src.safety.approval_claim import (
    mark_send_unknown,
    move_to_manual_review,
)
from src.safety.manual_review import build_manual_review_delta
from src.safety.execution_gate import ExecutionGate

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

    from src.init_app import get_app_context

    ctx = get_app_context()
    email_id = state["email_id"]
    inbox_id = state.get("inbox_id")
    durable_run = None
    durable_envelope = None

    async def fail_before_send(code: str) -> AgentState:
        if durable_run is not None:
            try:
                await ctx.db_manager.transition_handoff_manual_review(
                    inbox_id=inbox_id,
                    expected_version=int(durable_run["version"]),
                )
            except Exception as exc:
                logger.error(
                    "Handoff quarantine failed: error_type=%s",
                    type(exc).__name__,
                )
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
        if durable_run is not None:
            try:
                await ctx.db_manager.complete_execution(
                    inbox_id=inbox_id,
                    expected_version=int(durable_run["version"]) + 1,
                    sent=False,
                )
            except Exception as exc:
                logger.error(
                    "Handoff failure persistence failed: error_type=%s",
                    type(exc).__name__,
                )
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

    if inbox_id is None:
        return await fail_before_send("durable_approval_required")
    try:
        durable_run = await ctx.db_manager.get_handoff_run(inbox_id)
        if durable_run and durable_run.get("state") in {
            "executing",
            "completed",
            "failed",
            "manual_review",
            "rejected",
            "draft_saved",
        }:
            # A concurrent or replayed graph invocation does not own the
            # approved -> executing claim and must not quarantine the winner.
            return sanitize_graph_delta(state, {"next_step": "end"})
        if (
            not durable_run
            or durable_run.get("state") != "approved"
            or not durable_run.get("payload_revision")
        ):
            raise ValueError("approved_handoff_unavailable")
        raw_envelope = await ctx.db_manager.get_approved_execution_envelope(
            inbox_id=inbox_id,
            revision=int(durable_run["payload_revision"]),
        )
        if not isinstance(raw_envelope, dict):
            raise ValueError("approved_envelope_unavailable")
        durable_envelope = ExecutionGate().validate(
            raw_envelope.get("envelope"),
            expected_envelope_digest=raw_envelope.get("envelope_digest"),
        )
        if (
            durable_envelope.inbox_id != inbox_id
            or durable_envelope.email_id != email_id
            or durable_envelope.payload_revision != durable_run["payload_revision"]
            or durable_envelope.decision_digest != durable_run["decision_digest"]
            or durable_envelope.plan_digest != durable_run["plan_digest"]
            or durable_envelope.evidence_digest != durable_run["evidence_digest"]
        ):
            raise ValueError("approved_envelope_email_mismatch")
        draft = durable_envelope.draft_content
        final_to = list(durable_envelope.to)
        final_cc = list(durable_envelope.cc)
        action = durable_envelope.route_decision.route.value
        email_data = None
    except Exception as exc:
        logger.error(
            "Execution gate rejected approved payload: error_type=%s",
            type(exc).__name__,
        )
        return await fail_before_send("approval_handoff_failed")

    try:
        claimed = await ctx.db_manager.claim_execution(
            inbox_id=inbox_id,
            revision=int(durable_run["payload_revision"]),
            expected_version=int(durable_run["version"]),
            claim_id=str(uuid4()),
        )
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
                include_attachments=bool(
                    durable_envelope
                    and durable_envelope.route_decision.params.get(
                        "include_attachments", False
                    )
                ),
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
        completed = await ctx.db_manager.complete_execution(
            inbox_id=inbox_id,
            expected_version=int(durable_run["version"]) + 1,
            sent=True,
        )
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
        if email_data is None:
            email_data = await hydrate_email_from_state(state, dependencies)
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
