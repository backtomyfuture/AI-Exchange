import logging
import re

from langchain_core.runnables import RunnableConfig

from src.graph.dependencies import GraphDependencies
from src.graph.state import AgentState
from src.graph.state_factory import hydrate_graph_content, sanitize_graph_delta

logger = logging.getLogger(__name__)


async def send_final_email(
    state: AgentState,
    dependencies: GraphDependencies,
    config: RunnableConfig | None = None,
) -> AgentState:
    """Resolve the complete email and draft only at the final send boundary."""
    del config
    if state.get("approval_status", "pending") != "approved":
        return sanitize_graph_delta(state, {"next_step": "approval"})

    email_data, draft = await hydrate_graph_content(state, dependencies)

    from src.init_app import get_app_context
    import src.utils.lark_app as lark_app
    from lark_oapi.api.contact.v3 import GetUserRequest

    ctx = get_app_context()

    def resolve_recipient(recipient: object) -> str | None:
        value = str(recipient)
        if "open_id=" in value:
            open_id = value.replace("open_id=", "").strip()
            client = lark_app.lark_api_client
            if not client:
                logger.warning("Lark client missing; recipient resolution skipped")
                return None
            try:
                request = (
                    GetUserRequest.builder()
                    .user_id(open_id)
                    .user_id_type("open_id")
                    .build()
                )
                response = client.contact.v3.user.get(request)
                if response.success() and response.data and response.data.user:
                    return (
                        response.data.user.enterprise_email
                        or response.data.user.email
                        or None
                    )
                logger.warning("Lark recipient resolution returned no email")
                return None
            except Exception as exc:
                logger.error(
                    "Lark recipient resolution failed: error_type=%s",
                    type(exc).__name__,
                )
                return None

        if "email_address='" in value:
            match = re.search(r"email_address='(.*?)'", value)
            if match:
                return match.group(1)
        return value

    final_to = [
        resolved
        for recipient in (state.get("draft_to") or [])
        if (resolved := resolve_recipient(recipient))
    ]
    final_cc = [
        resolved
        for recipient in (state.get("draft_cc") or [])
        if (resolved := resolve_recipient(recipient))
    ]

    email_id = state["email_id"]
    if state.get("classification", {}).get("action") == "forward":
        success = await ctx.exchange_client.forward_email(
            email_id=email_id,
            to=list(dict.fromkeys([*final_to, *final_cc])),
            body=draft,
        )
        action_type = "forwarded"
    else:
        success = await ctx.exchange_client.reply_email(
            email_id=email_id,
            body=draft,
            to=final_to,
            cc=final_cc,
        )
        action_type = "sent"

    if not success:
        await ctx.db_manager.update_status(email_id, "failed_sending")
        logger.error("Email send failed")
        return sanitize_graph_delta(
            state,
            {"next_step": "end", "safe_error_summary": "send_failed"},
        )

    await ctx.db_manager.update_status(email_id, action_type)
    ctx.email_processor.process_sent_email(
        original_email_data=email_data,
        reply_content=draft,
    )
    logger.info("Email send completed: action=%s", action_type)
    return sanitize_graph_delta(state, {"next_step": "end"})
