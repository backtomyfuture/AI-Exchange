import json
import logging
import asyncio
import re
import html
import hashlib
import hmac
import threading
from datetime import UTC, datetime
from concurrent.futures import (
    CancelledError as FutureCancelledError,
    Future as ConcurrentFuture,
    TimeoutError as FutureTimeoutError,
)
from collections.abc import Callable
from typing import Any, List, Optional
import lark_oapi
from lark_oapi.api.im.v1.model.create_message_request import CreateMessageRequest
from lark_oapi.api.im.v1.model.create_message_request_body import CreateMessageRequestBody
from lark_oapi.api.im.v1.model.patch_message_request import PatchMessageRequest
from lark_oapi.api.im.v1.model.patch_message_request_body import PatchMessageRequestBody
from lark_oapi.api.im.v1.model.reply_message_request import ReplyMessageRequest
from lark_oapi.api.im.v1.model.reply_message_request_body import ReplyMessageRequestBody
from lark_oapi.ws import Client as WsClient
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse, CallBackToast

from src.utils.card_builder import LarkCardBuilder
from src.config import get_settings, resolve_secret
from src.graph.dependencies import GraphDependencies
from src.graph.resource_locks import get_graph_resource_lock
from src.graph.state_factory import (
    hydrate_draft_from_state,
    hydrate_email_from_state,
    hydrate_graph_content,
    sanitize_graph_delta,
    truncate_utf8,
)
from src.router.decision import RouteDecision
from src.router.tier1.schema import CanonicalRoute
from src.safety.approval_claim import (
    claim_approval,
    claim_draft_save,
    claim_rejection,
    complete_draft_save,
    get_approval_action_lock,
    mark_send_unknown,
    move_to_manual_review,
)
from src.safety.input_limits import input_limits_from_settings
from src.safety.manual_review import build_manual_review_delta
from src.safety.recipients import recipients_follow_route, resolve_recipients
from src.security.auth import is_lark_operator_allowed
from src.security.redaction import fingerprint_identifier
from src.utils.lark_recipient_editor import (
    build_recipient_field,
    clear_recipient_edit_temp,
    extract_external_emails_from_recipients,
    merge_keep_and_add,
    merge_unique,
    normalize_email_list,
    normalize_uid_list,
    read_selected_open_ids,
)
from src.utils.lark_pdf_flow import PdfFlowOutcome
from src.commands.router import CommandRouter
from src.commands.handlers import (
    init_commands,
    handle_help,
    handle_stats,
    handle_queue,
    handle_pending,
    handle_search,
    handle_health,
    handle_routing,
    handle_test_rule,
    handle_ai_report,
)

logger = logging.getLogger(__name__)
ACTION_WAIT_TIMEOUT_SECONDS = 30
LARK_IDENTIFIER_MAX_LENGTH = 512
ALLOWED_CARD_ACTIONS = frozenset(
    {
        "approve",
        "reject",
        "reject_with_reason",
        "save_draft_only",
        "view_original",
        "view_original_pdf",
        "mark_read",
        "edit_to",
        "edit_cc",
        "edit_draft",
        "select_to",
        "select_cc",
        "save_to",
        "search_to",
        "search_cc",
        "save_cc",
        "save_draft",
        "submit",
        "Button_submit",
        "form_submit_draft",
        "cancel_edit",
        "modify",
        "save_modification",
        "cancel_modification",
    }
)
DURABLE_BOUND_CARD_ACTIONS = frozenset(
    {
        "approve",
        "reject",
        "reject_with_reason",
        "save_draft_only",
        "edit_to",
        "edit_cc",
        "edit_draft",
        "select_to",
        "select_cc",
        "save_to",
        "search_to",
        "search_cc",
        "save_cc",
        "save_draft",
        "submit",
        "Button_submit",
        "form_submit_draft",
        "cancel_edit",
        "modify",
        "save_modification",
        "cancel_modification",
    }
)

# Global instances
lark_ws_client: Optional[WsClient] = None
_lark_ws_thread: threading.Thread | None = None
_lark_ws_stop_requested = threading.Event()
lark_api_client: Optional[lark_oapi.Client] = None
card_builder: Optional[LarkCardBuilder] = None
db_manager = None
graph = None
graph_dependencies: Optional[GraphDependencies] = None
exchange_client = None
worker_loop = None
_command_router: Optional[CommandRouter] = None
_lark_intake_enabled = True
_lark_intake_lock = threading.Lock()
_lark_background_futures: set[Any] = set()
_lark_background_completions: dict[Any, ConcurrentFuture] = {}
_LARK_CANCEL_FINALIZE_SECONDS = 1.0


def enable_lark_intake() -> None:
    global _lark_intake_enabled
    with _lark_intake_lock:
        _lark_intake_enabled = True


def disable_lark_intake() -> None:
    global _lark_intake_enabled
    with _lark_intake_lock:
        _lark_intake_enabled = False


def _register_builtin_commands():
    global _command_router
    _command_router = CommandRouter()
    _command_router.register("/help", handle_help)
    _command_router.register("/stats", handle_stats)
    _command_router.register("/queue", handle_queue)
    _command_router.register("/pending", handle_pending)
    _command_router.register("/search", handle_search)
    _command_router.register("/health", handle_health)
    _command_router.register("/routing", handle_routing)
    _command_router.register("/test_rule", handle_test_rule)
    _command_router.register("/ai_report", handle_ai_report)


def _read_nested(obj: Any, *path: str) -> Any:
    current = obj
    for part in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


def _valid_lark_identifier(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and len(value) <= LARK_IDENTIFIER_MAX_LENGTH
        and not any(character in value for character in "\r\n\0")
    )


def _rejected_card_action(content: str = "无权执行该操作") -> dict[str, Any]:
    return {"toast": {"type": "error", "content": content}}


def _valid_durable_card_binding(
    inbox_id: object,
    payload_revision: object,
    payload_digest: object,
) -> bool:
    return bool(
        isinstance(inbox_id, str)
        and re.fullmatch(r"[0-9a-f-]{36}", inbox_id)
        and isinstance(payload_revision, int)
        and payload_revision > 0
        and isinstance(payload_digest, str)
        and re.fullmatch(r"[0-9a-f]{64}", payload_digest)
    )


def verify_lark_signature(timestamp: str, nonce: str, body: str, signature: str) -> bool:
    """
    验证飞书事件签名，确保请求来源合法。
    
    Args:
        timestamp: 请求头中的 X-Lark-Request-Timestamp
        nonce: 请求头中的 X-Lark-Request-Nonce
        body: 请求体原始字符串
        signature: 请求头中的 X-Lark-Signature
    
    Returns:
        bool: 签名验证是否通过
    """
    settings = get_settings()
    encrypt_key = resolve_secret(settings.LARK_ENCRYPT_KEY)
    
    if not encrypt_key:
        logger.warning("LARK_ENCRYPT_KEY not configured; signature rejected.")
        return False
    
    content = f"{timestamp}{nonce}{encrypt_key}{body}"
    expected_signature = hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    # 使用恒时比较防止时序攻击
    return hmac.compare_digest(expected_signature, signature)

def _format_address_str(raw_str: str) -> str:
    """Format address string like 'name=..., email=...' to 'Name <email>'"""
    if not raw_str:
        return ""
    try:
        # Check for our project's specific string format
        m = re.search(r"name=['\"](.*?)['\"],?\s*email_address=['\"](.*?)['\"]", str(raw_str))
        if m:
            name, email = m.groups()
            return f"{html.escape(name)} &lt;{html.escape(email)}&gt;"
        
        # Check for standard "Name <email>" format
        m2 = re.search(r"(.*?) <(.*?)>", str(raw_str))
        if m2:
            return f"{html.escape(m2.group(1).strip())} &lt;{html.escape(m2.group(2).strip())}&gt;"

        return html.escape(str(raw_str))
    except Exception:
        return html.escape(str(raw_str))

def init_lark_app(
    db_mgr,
    graph_instance,
    ex_client,
    worker_loop_arg=None,
    *,
    dependencies: GraphDependencies | None = None,
):
    """
    Initialize global dependencies
    """
    global db_manager, graph, graph_dependencies, exchange_client, lark_api_client, card_builder, worker_loop
    disable_lark_intake()
    db_manager = db_mgr
    graph = graph_instance
    graph_dependencies = dependencies
    exchange_client = ex_client
    if worker_loop_arg:
        worker_loop = worker_loop_arg
    
    settings = get_settings()
    app_id = settings.LARK_APP_ID
    app_secret = resolve_secret(settings.LARK_APP_SECRET)
    
    if app_id and app_secret:
        lark_api_client = lark_oapi.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark_oapi.LogLevel.CRITICAL) \
            .build()
            
        if settings.LARK_CHAT_ID:
            _resolve_current_user_email(settings.LARK_CHAT_ID)
            
        card_builder = LarkCardBuilder(lark_api_client, exchange_client=exchange_client)
        logger.info("Lark API Client initialized.")
    else:
        card_builder = LarkCardBuilder(None, exchange_client=exchange_client)
        logger.warning("Lark App ID or Secret missing. Lark features disabled.")

    init_commands(db_mgr)
    _register_builtin_commands()
    enable_lark_intake()


def _require_graph_dependencies() -> GraphDependencies:
    if graph_dependencies is None:
        raise RuntimeError("lark_graph_dependencies_unavailable")
    return graph_dependencies


async def _hydrate_lark_projection(state) -> tuple[dict[str, Any], str]:
    values = state.values
    email_data, draft = await hydrate_graph_content(
        values,
        _require_graph_dependencies(),
        require_draft=False,
    )
    recipient_ui = values.get("recipient_ui") or {}
    for field in ("to", "cc"):
        ui = recipient_ui.get(field) if isinstance(recipient_ui, dict) else None
        if not isinstance(ui, dict):
            continue
        email_data[f"draft_{field}_options"] = list(ui.get("options") or [])
        email_data[f"draft_{field}_new_selected"] = list(ui.get("selected") or [])
        email_data[f"draft_{field}_external_input"] = ui.get("external_input", "")
        email_data[f"draft_{field}_search_hint"] = ui.get("search_hint", "")
    if values.get("inbox_id") is not None:
        email_data["_approval_inbox_id"] = values.get("inbox_id")
        email_data["_approval_payload_revision"] = values.get("payload_revision")
        email_data["_approval_payload_digest"] = values.get("payload_digest")
    return email_data, draft


async def _hydrate_current_durable_payload_projection(
    state: Any,
    *,
    reconcile_projection: bool = True,
) -> tuple[dict[str, Any], str]:
    """Render the current immutable payload even when graph projection lagged."""
    values = state.values
    inbox_id = values.get("inbox_id")
    if inbox_id is None:
        return await _hydrate_lark_projection(state)
    snapshot = await db_manager.get_current_payload_revision_snapshot(
        inbox_id=str(inbox_id)
    )
    if not snapshot:
        raise ValueError("current_payload_unavailable")
    revision = int(snapshot["revision"])
    payload_digest = str(snapshot["payload_digest"])
    draft = snapshot.get("draft_content")
    if not isinstance(draft, str) or not draft.strip():
        raise ValueError("current_payload_draft_unavailable")
    email_data = await hydrate_email_from_state(
        values,
        _require_graph_dependencies(),
    )
    email_data["draft_to"] = list(snapshot.get("to_recipients") or [])
    email_data["draft_cc"] = list(snapshot.get("cc_recipients") or [])
    email_data["_approval_inbox_id"] = str(inbox_id)
    email_data["_approval_payload_revision"] = revision
    email_data["_approval_payload_digest"] = payload_digest

    projection_is_current = (
        values.get("payload_revision") == revision
        and values.get("payload_digest") == payload_digest
    )
    if projection_is_current:
        recipient_ui = values.get("recipient_ui") or {}
        for field in ("to", "cc"):
            ui = recipient_ui.get(field) if isinstance(recipient_ui, dict) else None
            if not isinstance(ui, dict):
                continue
            email_data[f"draft_{field}_options"] = list(ui.get("options") or [])
            email_data[f"draft_{field}_new_selected"] = list(ui.get("selected") or [])
            email_data[f"draft_{field}_external_input"] = ui.get(
                "external_input", ""
            )
            email_data[f"draft_{field}_search_hint"] = ui.get("search_hint", "")
        return email_data, draft

    if not reconcile_projection:
        return email_data, draft

    # Reconcile only projections. The immutable payload remains authoritative
    # even if either best-effort write fails or this process is interrupted.
    try:
        await _require_graph_dependencies().drafts.save_draft_if_status(
            str(values["email_id"]), draft
        )
        config = {"configurable": {"thread_id": values["email_id"]}}
        await graph.aupdate_state(
            config,
            _bounded_human_update(
                state,
                {
                    "draft_id": values["email_id"],
                    "draft_to": email_data["draft_to"],
                    "draft_cc": email_data["draft_cc"],
                    "recipient_ui": {},
                    "payload_revision": revision,
                    "payload_digest": payload_digest,
                },
            ),
        )
    except Exception as exc:
        logger.warning(
            "Durable payload projection reconciliation failed: error_type=%s",
            type(exc).__name__,
        )
    return email_data, draft


def _bounded_human_update(state, delta: dict[str, Any]) -> dict[str, Any]:
    return sanitize_graph_delta(state.values, delta)

def _collect_cleanup_tokens(state) -> List[str]:
    """
    Collect all file tokens that need to be cleaned up from the state.
    Includes bounded top-level attachment tokens and the generated PDF token.
    """
    tokens: List[str] = []
    for token in state.values.get("attachment_tokens") or []:
        if isinstance(token, str) and token and token not in tokens:
            tokens.append(token)
    pdf_token = state.values.get("pdf_token")
    if isinstance(pdf_token, str) and pdf_token and pdf_token not in tokens:
        tokens.append(pdf_token)
    return tokens


async def _cleanup_action_drive_tokens(
    email_id: str,
    state: Any,
    *,
    _state_lock_held: bool = False,
    _targets: tuple[str, ...] | None = None,
) -> None:
    """Delete action-scoped Drive files and remove only confirmed successes."""
    if not _state_lock_held:
        async with get_graph_resource_lock(email_id):
            config = {"configurable": {"thread_id": email_id}}
            try:
                latest = await graph.aget_state(config)
                stale_attachment_tokens = set(
                    state.values.get("attachment_tokens") or []
                )
                latest_attachment_tokens = set(
                    latest.values.get("attachment_tokens") or []
                )
                stale_pdf_token = state.values.get("pdf_token")
                latest_pdf_token = latest.values.get("pdf_token")
            except Exception as exc:
                logger.warning(
                    "Action cleanup reconciliation failed: error_type=%s",
                    type(exc).__name__,
                )
                return

            if (
                stale_attachment_tokens == latest_attachment_tokens
                and stale_pdf_token == latest_pdf_token
            ):
                targets = tuple(_collect_cleanup_tokens(latest))
            else:
                stale_targets = set(_collect_cleanup_tokens(state))
                targets = tuple(
                    token
                    for token in (latest.values.get("attachment_tokens") or [])
                    if token in stale_targets and token != latest_pdf_token
                )
            await _cleanup_action_drive_tokens(
                email_id,
                latest,
                _state_lock_held=True,
                _targets=targets,
            )
        return
    targets = list(_targets) if _targets is not None else _collect_cleanup_tokens(state)
    if not targets:
        return

    deleted: set[str] = set()
    from src.utils.lark_file_ops import delete_file_from_drive

    for token in targets:
        try:
            if await asyncio.to_thread(
                delete_file_from_drive,
                token,
                lark_api_client=lark_api_client,
            ):
                deleted.add(token)
        except Exception as exc:
            logger.error(
                "Action Drive cleanup failed: error_type=%s",
                type(exc).__name__,
            )
    if not deleted:
        return

    config = {"configurable": {"thread_id": email_id}}
    try:
        latest = await graph.aget_state(config)
        values = latest.values
        update = _bounded_human_update(
            latest,
            {
                "attachment_tokens": [
                    token
                    for token in (values.get("attachment_tokens") or [])
                    if token not in deleted
                ],
                "pdf_token": (
                    None
                    if values.get("pdf_token") in deleted
                    else values.get("pdf_token")
                ),
            },
        )
        await graph.aupdate_state(config, update)
    except Exception as exc:
        # Stale handles are safe: a later retry can delete idempotently.
        logger.warning(
            "Action cleanup state update failed: error_type=%s",
            type(exc).__name__,
        )


async def _resume_graph_then_cleanup(
    email_id: str,
    state: Any,
    config: dict[str, Any],
    *,
    preserve_durable_approval: bool = False,
) -> None:
    """Resume the workflow first, then reconcile action cleanup handles."""
    async def quarantine_resume_failure() -> bool:
        if preserve_durable_approval:
            logger.warning(
                "Durable approval remains recoverable after graph resume failure"
            )
            return False
        try:
            persisted_status = await db_manager.get_email_status(email_id)
            recovery_codes = {
                "approved": "approval_handoff_failed",
                "rejected": "approval_handoff_failed",
            }
            if persisted_status == "sending":
                return await mark_send_unknown(
                    email_id,
                    db_manager,
                    code="send_outcome_unknown",
                )
            recovery_code = recovery_codes.get(persisted_status)
            if recovery_code:
                return await move_to_manual_review(
                    email_id,
                    db_manager,
                    expected=frozenset({persisted_status}),
                    code=recovery_code,
                )
            return persisted_status in {
                "sent",
                "draft_saved",
                "manual_review",
                "send_unknown",
            }
        except Exception as recovery_exc:
            logger.error(
                "Action resume recovery failed: error_type=%s",
                type(recovery_exc).__name__,
            )
            return False

    async with get_graph_resource_lock(email_id):
        cleanup_allowed = True
        try:
            await graph.ainvoke(None, config=config)
            try:
                persisted_status = await db_manager.get_email_status(email_id)
            except Exception as status_exc:
                logger.error(
                    "Post-action terminal readback failed: error_type=%s",
                    type(status_exc).__name__,
                )
                cleanup_allowed = False
            else:
                cleanup_allowed = persisted_status in {
                    "sent",
                    "rejected",
                    "draft_saved",
                    "manual_review",
                    "send_unknown",
                }
        except asyncio.CancelledError:
            logger.warning("Action graph resume was cancelled")
            cleanup_allowed = False
            cleanup_allowed = await quarantine_resume_failure()
            raise
        except Exception as exc:
            logger.error(
                "Action graph resume failed: error_type=%s",
                type(exc).__name__,
            )
            cleanup_allowed = False
            cleanup_allowed = await quarantine_resume_failure()
        finally:
            if not cleanup_allowed:
                logger.warning(
                    "Skipping action cleanup because terminal persistence is unconfirmed"
                )
            else:
                try:
                    latest = await graph.aget_state(config)
                except Exception as exc:
                    logger.warning(
                        "Post-action cleanup state lookup failed: error_type=%s",
                        type(exc).__name__,
                    )
                else:
                    await _cleanup_action_drive_tokens(
                        email_id,
                        latest,
                        _state_lock_held=True,
                    )

def _resolve_current_user_email(chat_id: str):

    """
    Dynamically resolve the current user's email based on LARK_CHAT_ID.
    Logic: GetChat -> OwnerID (OpenID) -> GetUser -> Email/Username
    """
    if not lark_api_client:
        return

    try:
        # Import models here to avoid circular/early import issues if sdk not ready
        from lark_oapi.api.im.v1.model.get_chat_request import GetChatRequest
        from lark_oapi.api.contact.v3.model.get_user_request import GetUserRequest
        
        logger.info(
            "Resolving Lark chat identity: chat=%s",
            fingerprint_identifier(chat_id, namespace="lark_chat"),
        )
        
        # 1. Get Chat Owner (P2P Owner = User)
        req = GetChatRequest.builder().chat_id(chat_id).build()
        resp = lark_api_client.im.v1.chat.get(req)
        
        if not resp.success():
            logger.warning("Failed to resolve chat identity: code=%s", resp.code)
            return
            
        owner_id = resp.data.owner_id
        if not owner_id:
            return

        # 2. Get User Profile
        user_req = GetUserRequest.builder().user_id(owner_id).user_id_type("open_id").build()
        user_resp = lark_api_client.contact.v3.user.get(user_req)
        
        if not user_resp.success():
             logger.warning(
                 "Failed to resolve user identity: code=%s",
                 user_resp.code,
             )
             return
             
        user = user_resp.data.user
        logger.info(
            "Lark identity resolved: actor=%s",
            fingerprint_identifier(owner_id, namespace="lark_actor"),
        )
        
        # 3. Derive Exchange Email
        # Strategy: Use local part of Lark email, or full email if matches user preference
        # User Instruction: "Append tianjin-air.com"
        
        effective_email = user.email
        if user.email:
            # Extract local part "q-fu" from "q-fu@hnair.com"
            local_part = user.email.split("@")[0]
            # We set this as EXCHANGE_ACCOUNT_EMAIL. 
            # The card builder logic checks `if my_email in recipient_email`. 
            # Setting it to the local part "q-fu" is the most robust way to match "q-fu@tianjin-air.com" AND "q-fu@hnair.com"
            effective_email = local_part
            
        if effective_email:
            settings = get_settings()
            settings.EXCHANGE_ACCOUNT_EMAIL = effective_email
            logger.info(
                "Exchange account identity configured: account=%s",
                fingerprint_identifier(effective_email, namespace="exchange_account"),
            )
            
    except Exception as exc:
        logger.error("Identity resolution failed: error_type=%s", type(exc).__name__)

def send_system_notification(title: str, content: str, template: str = "red"):
    """Send a system notification card. Delegates to lark_messaging."""
    from src.utils.lark_messaging import send_system_notification as _impl
    return _impl(title, content, template, lark_api_client=lark_api_client)


def update_card_ui(message_id, card_content):
    """
    Update the card UI via API (Patch Message).
    """
    if not lark_api_client:
        return
    try:
        # Patch Request
        req = PatchMessageRequest.builder() \
            .message_id(message_id) \
            .request_body(PatchMessageRequestBody.builder()
                .content(json.dumps(card_content))
                .build()) \
            .build()
        
        logger.info("Patching interactive card")
            
        resp = lark_api_client.im.v1.message.patch(req)
        
        logger.info(f"Patch Response Code: {resp.code}")
        if not resp.success():
             logger.error("Failed to patch card: code=%s", resp.code)
        else:
             logger.info(
                 "Interactive card patch succeeded: message=%s",
                 fingerprint_identifier(message_id, namespace="lark_message"),
             )
    except Exception as exc:
        logger.error("Card patch failed: error_type=%s", type(exc).__name__)



def handle_card_action(event):
    """
    Handle card interactions (WS) - Callback receives single 'event' object
    """
    try:
        if not hasattr(event, "event") or not event.event.action:
             logger.warning("Invalid event structure: missing event.action")
             return _rejected_card_action("无效的操作请求")

        user_id = _read_nested(event, "event", "operator", "open_id")
        if not is_lark_operator_allowed(user_id, get_settings()):
            logger.warning(
                "Rejected unauthorized Lark card action: actor=%s",
                fingerprint_identifier(user_id, namespace="lark_actor"),
            )
            return _rejected_card_action()

        action_value = event.event.action.value
        # Parse Value
        if isinstance(action_value, str):
            try:
                data = json.loads(action_value)
            except Exception:
                return _rejected_card_action("无效的操作请求")
        else:
            data = action_value

        if not isinstance(data, dict):
            return _rejected_card_action("无效的操作请求")

        action_type = data.get("action")
        email_id = data.get("id")
        inbox_id = data.get("inbox_id")
        payload_revision = data.get("payload_revision")
        payload_digest = data.get("payload_digest")
        callback_has_binding = any(
            value is not None
            for value in (inbox_id, payload_revision, payload_digest)
        )
        callback_binding_is_valid = _valid_durable_card_binding(
            inbox_id, payload_revision, payload_digest
        )
        # open_message_id is likely nested in context for card triggers, checking both
        if hasattr(event.event, "context") and hasattr(event.event.context, "open_message_id"):
             message_id = event.event.context.open_message_id
        else:
             message_id = getattr(event.event, "open_message_id", None)
        
        if (
            action_type not in ALLOWED_CARD_ACTIONS
            or not _valid_lark_identifier(email_id)
            or not _valid_lark_identifier(message_id)
        ):
            logger.warning(
                "Rejected malformed Lark card action: action=%s",
                action_type if action_type in ALLOWED_CARD_ACTIONS else "unknown",
            )
            return _rejected_card_action("无效的操作请求")

        if (
            action_type in DURABLE_BOUND_CARD_ACTIONS
            and callback_has_binding
            and not callback_binding_is_valid
        ):
            return _rejected_card_action("无效的审批版本")

        logger.info(
            "Accepted Lark card action: action=%s email=%s message=%s actor=%s",
            action_type,
            fingerprint_identifier(email_id, namespace="email"),
            fingerprint_identifier(message_id, namespace="lark_message"),
            fingerprint_identifier(user_id, namespace="lark_actor"),
        )
        
        def get_current_state(eid):
            config = {"configurable": {"thread_id": eid}}
            return safe_async_wait(graph.aget_state(config))

        state = get_current_state(email_id)

        if not state or not state.values:
             logger.warning(
                 "No state found for card action: email=%s action=%s",
                 fingerprint_identifier(email_id, namespace="email"),
                 action_type,
             )
             # Ensure we return a properly formatted error response
             return {"toast": {"type": "error", "content": "找不到任务状态或已失效"}}

        durable_inbox_id = state.values.get("inbox_id")
        if action_type in DURABLE_BOUND_CARD_ACTIONS:
            graph_is_durable = durable_inbox_id is not None
            if not (
                graph_is_durable == callback_binding_is_valid
                and (
                    not graph_is_durable
                    or inbox_id == durable_inbox_id
                )
            ):
                return _rejected_card_action("旧审批卡已失效，请刷新卡片")
            if graph_is_durable and action_type != "approve" and not safe_async_wait(
                db_manager.is_current_payload_revision(
                    inbox_id=inbox_id,
                    revision=payload_revision,
                    payload_digest=payload_digest,
                )
            ):
                return _rejected_card_action("审批内容已更新，请使用最新卡片")

        if action_type == "view_original":
            return {
                "toast": {
                    "type": "warning",
                    "content": "Web 原文预览暂未开放，请使用 PDF",
                }
            }

        slim_email = state.values.get("email") or {}
        slim_subject = (
            slim_email.get("subject", "Email")
            if isinstance(slim_email, dict)
            else "Email"
        )

        if action_type in {"approve", "reject", "reject_with_reason"}:
            if action_type == "approve":
                if state.values.get("inbox_id") is not None and inbox_id is None:
                    return _rejected_card_action("旧审批卡已失效，请刷新卡片")
                processed = process_approval(
                    email_id,
                    user_id,
                    inbox_id=inbox_id,
                    payload_revision=payload_revision,
                    payload_digest=payload_digest,
                )
                processed_text = "已批准"
                toast_type = "success"
                toast_content = "审批请求已提交"
            else:
                reason_text = ""
                if action_type == "reject_with_reason":
                    option = getattr(event.event.action, "option", None) or ""
                    reason_map = {
                        "tone_wrong": "语气不当",
                        "content_error": "内容有误",
                        "no_reply_needed": "无需回复",
                        "other": "其他原因",
                    }
                    reason_text = reason_map.get(option, option or "未指定")
                processed = process_rejection(
                    email_id,
                    user_id,
                    reason=reason_text,
                    inbox_id=inbox_id,
                    payload_revision=payload_revision,
                    payload_digest=payload_digest,
                )
                processed_text = (
                    f"已拒绝 ({reason_text})" if reason_text else "已拒绝"
                )
                toast_type = "info"
                toast_content = (
                    f"已拒绝: {reason_text}" if reason_text else "已拒绝该拟稿"
                )

            if not processed:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "该审批已处理或已转人工复核，请刷新卡片",
                    }
                }
            new_card = LarkCardBuilder.get_processed_card(
                processed_text,
                slim_subject,
            )
            return {
                "toast": {"type": toast_type, "content": toast_content},
                "card": {"type": "raw", "data": new_card},
            }

        if action_type == "save_draft_only":
            claimed = safe_async_wait(
                _claim_draft_save_action(
                    email_id,
                    inbox_id=inbox_id,
                    payload_revision=payload_revision,
                    payload_digest=payload_digest,
                    state=state,
                )
            )
            if not claimed:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "该审批已处理，不能重复保存 Exchange 草稿",
                    }
                }

            operation = _run_claimed_draft_save(email_id, state, claim=claimed)
            try:
                safe_async_run(operation)
            except Exception as exc:
                operation.close()
                logger.error(
                    "Exchange draft scheduling failed: error_type=%s",
                    type(exc).__name__,
                )
                if claimed.get("inbox_id") is not None:
                    safe_async_wait(
                        db_manager.fail_payload_draft_save(
                            inbox_id=str(claimed["inbox_id"]),
                            expected_version=int(claimed["handoff_version"]),
                            error_code="draft_save_outcome_unknown",
                        )
                    )
                else:
                    safe_async_wait(
                        move_to_manual_review(
                            email_id,
                            db_manager,
                            expected=frozenset({"saving_draft"}),
                            code="draft_save_outcome_unknown",
                        )
                    )
                return {
                    "toast": {
                        "type": "warning",
                        "content": "草稿保存未确认，已转人工复核",
                    }
                }
            return {
                "toast": {
                    "type": "info",
                    "content": "正在保存 Exchange 草稿，请稍候",
                }
            }

        guarded_edit_actions = {
            "edit_to",
            "edit_cc",
            "edit_draft",
            "select_to",
            "select_cc",
            "save_to",
            "save_cc",
            "search_to",
            "search_cc",
            "save_draft",
            "submit",
            "Button_submit",
            "form_submit_draft",
            "modify",
            "save_modification",
        }
        if action_type in guarded_edit_actions:
            persisted_status = safe_async_wait(
                db_manager.get_email_status(email_id)
            )
            if persisted_status != "waiting_approval":
                return {
                    "toast": {
                        "type": "warning",
                        "content": "审批状态已变化，当前内容不可再修改",
                    }
                }
             
        if durable_inbox_id is not None:
            email_data, draft = safe_async_wait(
                _hydrate_current_durable_payload_projection(
                    state,
                    reconcile_projection=action_type
                    not in {"search_to", "search_cc"},
                )
            )
        else:
            email_data, draft = safe_async_wait(_hydrate_lark_projection(state))
        context_summaries = state.values.get("context_summaries", [])
        classification = state.values.get("classification", {})
        subject = email_data.get("subject", "Email")
        action_card_builder = card_builder

        def refresh_editable_projection():
            latest_state = get_current_state(email_id)
            hydrate = (
                _hydrate_current_durable_payload_projection(latest_state)
                if durable_inbox_id is not None
                else _hydrate_lark_projection(latest_state)
            )
            latest_email, latest_draft = safe_async_wait(hydrate)
            return latest_state, latest_email, latest_draft

        logger.info(
            "State fetched for card action: email=%s action=%s",
            fingerprint_identifier(email_id, namespace="email"),
            action_type,
        )

        if action_type == "view_original":
            logger.info("Executing Request: View Original (H5 Strategy)")
            
            # Get External URL
            settings = get_settings()
            external_url = settings.EXTERNAL_URL
            h5_url = f"{external_url}/email/{email_id}"
            
            # Build Simple Card with Button
            card_content = {
                "header": {
                    "template": "blue",
                    "title": {"content": "📄 原始邮件 (Web版)", "tag": "plain_text"}
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": "点击下方按钮在浏览器中查看完整邮件内容："}
                    },
                    {
                        "tag": "action",
                        "actions": [{
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "🔗 点击查看完整原文"},
                            "type": "primary",
                            "url": h5_url
                        }]
                    }
                ]
            }

            try:
                # Use ReplyMessageRequest to reply in thread
                req_msg = ReplyMessageRequest.builder() \
                    .message_id(message_id) \
                    .request_body(ReplyMessageRequestBody.builder()
                        .msg_type("interactive")
                        .content(json.dumps(card_content))
                        .build()) \
                    .build()
                
                lark_api_client.im.v1.message.reply(req_msg)

                return {
                    "toast": {
                        "type": "success",
                        "content": "已发送链接至评论区"
                    }
                }
            except Exception as exc:
                logger.error(
                    "Original-view reply failed: error_type=%s",
                    type(exc).__name__,
                )
                raise

        elif action_type == "view_original_pdf":
            logger.info("Executing Request: View Original PDF")
            safe_async_run(
                process_pdf_generation_and_reply(email_id, state, message_id)
            )
            
            return {
                "toast": {
                    "type": "info",
                    "content": "正在生成PDF文件，请稍候..."
                }
            }


        # 只读卡片 - 标记已阅
        elif action_type == "mark_read":
            logger.info(
                "Mark-read card action: email=%s actor=%s",
                fingerprint_identifier(email_id, namespace="email"),
                fingerprint_identifier(user_id, namespace="lark_actor"),
            )
            safe_async_wait(db_manager.update_status(email_id, "read"))
            safe_async_run(_cleanup_action_drive_tokens(email_id, state))

            new_card = LarkCardBuilder.get_processed_card("已阅", subject)
            return {
                "toast": {
                    "type": "success",
                    "content": "已标记为已阅"
                },
                "card": {
                    "type": "raw",
                    "data": new_card
                }
            }
            
        # 编辑收件人
        elif action_type == "edit_to":
            logger.info("Editing recipient field: field=to")
            edit_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field="to")
            return {
                "toast": {"type": "info", "content": "编辑收件人"},
                "card": {"type": "raw", "data": edit_card}
            }
        
        # 编辑抄送人
        elif action_type == "edit_cc":
            edit_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field="cc")
            return {
                "toast": {"type": "info", "content": "编辑抄送人"},
                "card": {"type": "raw", "data": edit_card}
            }
        
        # 编辑正文
        elif action_type == "edit_draft":
            edit_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field="draft")
            return {
                "toast": {"type": "info", "content": "编辑正文"},
                "card": {"type": "raw", "data": edit_card}
            }
        
        # 选择人员时的回调 - 支持多选并直接更新卡片
        elif action_type == "select_to":
            action_data = event.event.action
            selected_uids = read_selected_open_ids(action_data)
            logger.info("Recipient selection changed: field=to selected_count=%d", len(selected_uids))
            if not selected_uids:
                view_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field=None)
                return {
                    "toast": {"type": "warning", "content": "收件人至少保留 1 人"},
                    "card": {"type": "raw", "data": view_card}
                }

            new_to = [f"open_id={uid}" for uid in selected_uids]

            saved = safe_async_wait(
                _update_recipient_field_if_waiting(
                    email_id,
                    {"draft_to": new_to},
                    inbox_id=inbox_id,
                    payload_revision=payload_revision,
                    payload_digest=payload_digest,
                )
            )
            if not saved:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "审批状态已变化，收件人未更新",
                    }
                }
            state, email_data, draft = refresh_editable_projection()

            view_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": f"收件人已更新（{len(new_to)}人）"},
                "card": {"type": "raw", "data": view_card}
            }
        
        elif action_type == "select_cc":
            action_data = event.event.action
            selected_uids = read_selected_open_ids(action_data)
            logger.info("Recipient selection changed: field=cc selected_count=%d", len(selected_uids))
            new_cc = [f"open_id={uid}" for uid in selected_uids]

            saved = safe_async_wait(
                _update_recipient_field_if_waiting(
                    email_id,
                    {"draft_cc": new_cc},
                    inbox_id=inbox_id,
                    payload_revision=payload_revision,
                    payload_digest=payload_digest,
                )
            )
            if not saved:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "审批状态已变化，抄送人未更新",
                    }
                }
            state, email_data, draft = refresh_editable_projection()

            view_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": f"抄送人已更新（{len(new_cc)}人）"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 保存收件人
        elif action_type == "save_to":
            action_data = event.event.action
            form_values = getattr(action_data, "form_value", None) or {}
            logger.info("Saving recipient field: field=to")
            keep_uids = normalize_uid_list(form_values.get("to_existing"))
            add_uids = normalize_uid_list(form_values.get("to_new"))
            external_raw = form_values.get("to_external_input", None)
            external_emails = normalize_email_list(external_raw)
            if external_raw is None:
                external_emails = extract_external_emails_from_recipients(email_data.get("draft_to"))

            new_to = build_recipient_field(
                merge_keep_and_add(keep_uids, add_uids), external_emails
            )

            if not new_to:
                edit_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field="to")
                return {
                    "toast": {"type": "warning", "content": "收件人至少保留 1 人（飞书人员或外部邮箱）"},
                    "card": {"type": "raw", "data": edit_card}
                }

            saved = safe_async_wait(
                _update_recipient_field_if_waiting(
                    email_id,
                    {"draft_to": new_to, "recipient_ui": {"to": {}}},
                    inbox_id=inbox_id,
                    payload_revision=payload_revision,
                    payload_digest=payload_digest,
                )
            )
            if not saved:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "审批状态已变化，收件人未保存",
                    }
                }
            state, email_data, draft = refresh_editable_projection()
            clear_recipient_edit_temp(email_data, "to")

            view_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": f"收件人已保存（{len(new_to)}人）"},
                "card": {"type": "raw", "data": view_card}
            }

        # 搜索收件人候选
        elif action_type in ("search_to", "search_cc"):
            field_type = "to" if action_type == "search_to" else "cc"
            action_data = event.event.action
            form_values = getattr(action_data, "form_value", None) or {}
            keyword = str(form_values.get(f"{field_type}_search_keyword", "")).strip()
            logger.info(
                "Searching recipient candidates: field=%s keyword_bytes=%d",
                field_type,
                len(keyword.encode("utf-8")),
            )

            if not keyword:
                edit_card = action_card_builder.build_approval_card(
                    email_id, draft, context_summaries, email_data, classification, edit_field=field_type
                )
                return {
                    "toast": {"type": "warning", "content": "请输入关键词后再搜索"},
                    "card": {"type": "raw", "data": edit_card}
                }

            if not message_id:
                return {"toast": {"type": "error", "content": "无法定位消息，请重新打开卡片后重试"}}

            async def _search_and_patch_recipients():
                try:
                    matched_uids = await asyncio.to_thread(
                        card_builder.search_person_picker_candidates,
                        keyword,
                    )
                    def _next_hint(total_candidates: int) -> str:
                        if matched_uids:
                            return f"本次命中 {len(matched_uids)} 人，累计候选 {total_candidates} 人。可继续搜索并勾选后保存。"
                        return f"未找到“{keyword}”匹配人员，累计候选 {total_candidates} 人。仅支持邮箱前缀精确搜索（@前部分）。"

                    config = {"configurable": {"thread_id": email_id}}
                    async with get_approval_action_lock(email_id):
                        if (
                            await db_manager.get_email_status(email_id)
                            != "waiting_approval"
                        ):
                            logger.info(
                                "Dropping stale recipient search result: field=%s",
                                field_type,
                            )
                            return
                        if durable_inbox_id is not None and not await (
                            db_manager.is_current_payload_revision(
                                inbox_id=inbox_id,
                                revision=payload_revision,
                                payload_digest=payload_digest,
                            )
                        ):
                            logger.info(
                                "Dropping result from a stale recipient search: field=%s",
                                field_type,
                            )
                            return
                        latest_state = await graph.aget_state(config)
                        current_ui = (
                            latest_state.values.get("recipient_ui") or {}
                        )
                        field_ui = current_ui.get(field_type) or {}
                        selected_raw = form_values.get(f"{field_type}_new", None)
                        if selected_raw is None:
                            selected_new = normalize_uid_list(
                                field_ui.get("selected")
                            )
                        else:
                            selected_new = normalize_uid_list(selected_raw)
                        current_options = normalize_uid_list(
                            field_ui.get("options")
                        )
                        merged_options = merge_unique(
                            current_options + matched_uids + selected_new
                        )
                        external_raw = form_values.get(
                            f"{field_type}_external_input",
                            None,
                        )
                        if external_raw is None:
                            external_input = str(
                                field_ui.get("external_input", "") or ""
                            )
                        else:
                            external_input = str(external_raw or "").strip()
                        ui_delta = {
                            field_type: {
                                "options": merged_options,
                                "selected": selected_new,
                                "external_input": external_input,
                                "search_hint": _next_hint(len(merged_options)),
                            }
                        }
                        if durable_inbox_id is not None:
                            # Search options are ephemeral UI, not execution
                            # payload. Never persist them over a newer durable
                            # revision from another process.
                            latest_email, latest_draft = (
                                await _hydrate_current_durable_payload_projection(
                                    latest_state,
                                    reconcile_projection=False,
                                )
                            )
                            field_projection = ui_delta[field_type]
                            latest_email[f"draft_{field_type}_options"] = list(
                                field_projection["options"]
                            )
                            latest_email[f"draft_{field_type}_new_selected"] = list(
                                field_projection["selected"]
                            )
                            latest_email[f"draft_{field_type}_external_input"] = (
                                field_projection["external_input"]
                            )
                            latest_email[f"draft_{field_type}_search_hint"] = (
                                field_projection["search_hint"]
                            )
                        else:
                            await graph.aupdate_state(
                                config,
                                _bounded_human_update(
                                    latest_state,
                                    {"recipient_ui": ui_delta},
                                ),
                            )
                            latest_state = await graph.aget_state(config)
                            latest_email, latest_draft = (
                                await _hydrate_lark_projection(latest_state)
                            )
                        latest_classification = latest_state.values.get(
                            "classification",
                            classification,
                        )
                        latest_context = latest_state.values.get(
                            "context_summaries",
                            [],
                        )
                        edit_card = action_card_builder.build_approval_card(
                            email_id,
                            latest_draft,
                            latest_context,
                            latest_email,
                            latest_classification,
                            edit_field=field_type,
                        )
                        if durable_inbox_id is not None and not await (
                            db_manager.is_current_payload_revision(
                                inbox_id=inbox_id,
                                revision=payload_revision,
                                payload_digest=payload_digest,
                            )
                        ):
                            logger.info(
                                "Dropping recipient search card for advanced revision: field=%s",
                                field_type,
                            )
                            return
                        update_card_ui(message_id, edit_card)
                    logger.info(
                        "Recipient search finished: field=%s keyword_bytes=%d matches=%d",
                        field_type,
                        len(keyword.encode("utf-8")),
                        len(matched_uids),
                    )
                except Exception as exc:
                    logger.error(
                        "Recipient search failed: field=%s error_type=%s",
                        field_type,
                        type(exc).__name__,
                    )

            safe_async_run(_search_and_patch_recipients())
            return {"toast": {"type": "info", "content": f"正在搜索“{keyword}”，稍后自动更新候选人..."}}
        
        # 保存抄送人
        elif action_type == "save_cc":
            action_data = event.event.action
            form_values = getattr(action_data, "form_value", None) or {}
            logger.info("Saving recipient field: field=cc")
            keep_uids = normalize_uid_list(form_values.get("cc_existing"))
            add_uids = normalize_uid_list(form_values.get("cc_new"))
            external_raw = form_values.get("cc_external_input", None)
            external_emails = normalize_email_list(external_raw)
            if external_raw is None:
                external_emails = extract_external_emails_from_recipients(email_data.get("draft_cc"))

            new_cc = build_recipient_field(
                merge_keep_and_add(keep_uids, add_uids), external_emails
            )
            saved = safe_async_wait(
                _update_recipient_field_if_waiting(
                    email_id,
                    {"draft_cc": new_cc, "recipient_ui": {"cc": {}}},
                    inbox_id=inbox_id,
                    payload_revision=payload_revision,
                    payload_digest=payload_digest,
                )
            )
            if not saved:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "审批状态已变化，抄送人未保存",
                    }
                }
            state, email_data, draft = refresh_editable_projection()
            clear_recipient_edit_temp(email_data, "cc")

            view_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": f"抄送人已保存（{len(new_cc)}人）"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 保存正文 (form submit)
        elif action_type in ("save_draft", "submit", "Button_submit", "form_submit_draft"):
            action_data = event.event.action
            form_values = getattr(action_data, 'form_value', None) or {}
            logger.info("Saving edited draft")
            new_draft = form_values.get("draft_input", "")
            if not new_draft:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "正文不能为空，未保存修改",
                    }
                }
            if not process_modification(
                email_id,
                new_draft,
                inbox_id=inbox_id,
                payload_revision=payload_revision,
                payload_digest=payload_digest,
            ):
                return {
                    "toast": {
                        "type": "warning",
                        "content": "审批状态已变化，正文未保存",
                    }
                }
            logger.info(
                "Draft updated: bytes=%d",
                len(new_draft.encode("utf-8")),
            )
            state, email_data, new_draft = refresh_editable_projection()
            view_card = action_card_builder.build_approval_card(email_id, new_draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": "正文已保存"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 取消编辑（通用）
        elif action_type == "cancel_edit":
            view_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "info", "content": "已取消"},
                "card": {"type": "raw", "data": view_card}
            }

        # 保留旧的modify处理，兼容可能的旧卡片
        elif action_type == "modify":
            edit_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field="draft")
            return {
                "toast": {"type": "info", "content": "编辑正文"},
                "card": {"type": "raw", "data": edit_card}
            }

        elif action_type == "save_modification":
            form_values = event.event.action.form_value or {}
            new_draft = form_values.get("draft_input", "")
            if not new_draft:
                return {
                    "toast": {
                        "type": "warning",
                        "content": "正文不能为空，未保存修改",
                    }
                }
            if not process_modification(
                email_id,
                new_draft,
                inbox_id=inbox_id,
                payload_revision=payload_revision,
                payload_digest=payload_digest,
            ):
                return {
                    "toast": {
                        "type": "warning",
                        "content": "审批状态已变化，正文未保存",
                    }
                }
            state, email_data, new_draft = refresh_editable_projection()
            view_card = action_card_builder.build_approval_card(email_id, new_draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": "修改已保存"},
                "card": {"type": "raw", "data": view_card}
            }

        elif action_type == "cancel_modification":
            view_card = action_card_builder.build_approval_card(email_id, draft, context_summaries, email_data, classification, edit_field=None)
            return {
                "toast": {"type": "info", "content": "已取消编辑"},
                "card": {"type": "raw", "data": view_card}
            }

    except Exception as exc:
        logger.error(
            "Card action failed: error_type=%s",
            type(exc).__name__,
        )
        err_resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "error"
        toast.content = "操作失败，请稍后重试"
        err_resp.toast = toast
        return err_resp


def _handle_p2_im_message_receive(event):
    """Handle incoming private text messages and dispatch slash commands."""
    try:
        message = _read_nested(event, "event", "message")
        if message is None:
            return

        message_type = _read_nested(message, "message_type")
        chat_type = _read_nested(message, "chat_type")
        sender_open_id = _read_nested(event, "event", "sender", "sender_id", "open_id")

        if message_type != "text" or chat_type != "p2p" or not sender_open_id:
            return

        if not is_lark_operator_allowed(sender_open_id, get_settings()):
            logger.warning(
                "Rejected unauthorized Lark command: actor=%s",
                fingerprint_identifier(sender_open_id, namespace="lark_actor"),
            )
            return

        raw_content = _read_nested(message, "content") or ""

        try:
            content = json.loads(raw_content).get("text", "")
        except Exception:
            content = str(raw_content)

        async def _dispatch():
            if _command_router is None:
                logger.warning("Command router not initialized.")
                return
            reply = await _command_router.dispatch(content)
            if reply is None:
                return
            if not lark_api_client:
                logger.warning("Lark API client not initialized for command response.")
                return

            if isinstance(reply, dict):
                msg_type = "interactive"
                content_str = json.dumps(reply, ensure_ascii=False)
            else:
                msg_type = "text"
                content_str = json.dumps({"text": str(reply)}, ensure_ascii=False)

            req = CreateMessageRequest.builder() \
                .receive_id_type("open_id") \
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(sender_open_id)
                    .msg_type(msg_type)
                    .content(content_str)
                    .build()
                ) \
                .build()
            resp = lark_api_client.im.v1.message.create(req)
            if not resp.success():
                logger.error("Command reply send failed: code=%s", resp.code)

        safe_async_run(_dispatch())
    except Exception as exc:
        logger.error("Message event failed: error_type=%s", type(exc).__name__)

def build_final_response(text):
    """
    Returns a simple card update to show final status
    """
    return {
        "header": {"title": {"content": "处理完成", "tag": "plain_text"}, "template": "grey"},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": text}}]
    }

def _observe_background_future(future):
    def completed(done) -> None:
        with _lark_intake_lock:
            completion = _lark_background_completions.get(done)
            if completion is None or completion.done():
                _lark_background_futures.discard(done)
                _lark_background_completions.pop(done, None)
        try:
            done.result()
        except (asyncio.CancelledError, FutureCancelledError):
            logger.warning("Background action was cancelled")
        except BaseException as exc:
            logger.error(
                "Background action failed: error_type=%s",
                type(exc).__name__,
            )

    future.add_done_callback(completed)
    return future


async def _run_lark_background_operation(coro, completion: ConcurrentFuture):
    try:
        return await coro
    finally:
        if not completion.done():
            completion.set_result(None)


def _track_foreign_background_completion(future, completion: ConcurrentFuture) -> None:
    def finalized(_done) -> None:
        with _lark_intake_lock:
            _lark_background_futures.discard(future)
            _lark_background_completions.pop(future, None)

    completion.add_done_callback(finalized)


def _run_no_loop_fallback(coro, future, completion: ConcurrentFuture):
    """Run an admitted fallback without holding the intake admission lock."""
    try:
        return asyncio.run(coro)
    finally:
        if not future.done():
            future.set_result(None)
        if not completion.done():
            completion.set_result(None)


async def _await_foreign_background_future(future: asyncio.Future) -> Any:
    return await future


def _background_waiter(future):
    if not isinstance(future, asyncio.Future):
        return asyncio.wrap_future(future)

    current_loop = asyncio.get_running_loop()
    owner_loop = future.get_loop()
    if owner_loop is current_loop:
        return future
    if owner_loop.is_running():
        bridge = asyncio.run_coroutine_threadsafe(
            _await_foreign_background_future(future),
            owner_loop,
        )
        return asyncio.wrap_future(bridge)

    if not future.done():
        future.cancel()

    async def collect_stopped_loop_future() -> Any:
        return future.result()

    return current_loop.create_task(collect_stopped_loop_future())


async def drain_lark_background_tasks(
    *,
    timeout_seconds: float = ACTION_WAIT_TIMEOUT_SECONDS,
) -> None:
    """Wait for accepted Lark actions, then cancel and collect on timeout."""
    with _lark_intake_lock:
        pending_futures = tuple(_lark_background_futures)
        completion_by_future = {
            future: _lark_background_completions.get(future)
            for future in pending_futures
        }
    if not pending_futures:
        return

    waiter_to_future = {}
    for future in pending_futures:
        completion = completion_by_future[future]
        waiter = (
            asyncio.wrap_future(completion)
            if completion is not None
            else _background_waiter(future)
        )
        waiter_to_future[waiter] = future

    done_waiters, pending_waiters = await asyncio.wait(
        waiter_to_future,
        timeout=max(0.0, timeout_seconds),
    )
    for waiter in pending_waiters:
        waiter_to_future[waiter].cancel()

    if pending_waiters:
        finalized, still_pending = await asyncio.wait(
            pending_waiters,
            timeout=_LARK_CANCEL_FINALIZE_SECONDS,
        )
        done_waiters.update(finalized)
        if still_pending:
            raise RuntimeError("lark_background_shutdown_timeout")

    await asyncio.gather(*done_waiters, return_exceptions=True)
    with _lark_intake_lock:
        for future in pending_futures:
            if future.done():
                _lark_background_futures.discard(future)


async def stop_lark_intake(
    *,
    timeout_seconds: float = ACTION_WAIT_TIMEOUT_SECONDS,
) -> None:
    """Close the Lark intake gate and collect every accepted background action."""
    disable_lark_intake()
    await drain_lark_background_tasks(timeout_seconds=timeout_seconds)


def safe_async_run(coro):
    """
    Safely run a coroutine in the background (non-blocking for the caller).
    This is thread-safe and MUST be used when calling async code from the WebSocket thread.
    """
    global worker_loop
    completion = None
    no_loop_fallback = False
    with _lark_intake_lock:
        if not _lark_intake_enabled:
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise RuntimeError("lark_intake_disabled")
        if worker_loop and worker_loop.is_running():
            completion = ConcurrentFuture()
            future = asyncio.run_coroutine_threadsafe(
                _run_lark_background_operation(coro, completion),
                worker_loop,
            )
            _lark_background_futures.add(future)
            _lark_background_completions[future] = completion
        else:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is None:
                future = ConcurrentFuture()
                completion = ConcurrentFuture()
                _lark_background_futures.add(future)
                _lark_background_completions[future] = completion
                no_loop_fallback = True
            else:
                future = loop.create_task(coro)
                _lark_background_futures.add(future)
    if completion is not None:
        _track_foreign_background_completion(future, completion)
    if no_loop_fallback:
        return _run_no_loop_fallback(coro, future, completion)
    return _observe_background_future(future)

def safe_async_wait(coro):
    """
    Wait for a coroutine to complete and return the result.
    This is thread-safe and MUST be used when calling async code from the WebSocket thread.
    """
    global worker_loop
    completion = None
    no_loop_fallback = False
    with _lark_intake_lock:
        if not _lark_intake_enabled:
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise RuntimeError("lark_intake_disabled")
        if worker_loop and worker_loop.is_running():
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if current_loop is worker_loop:
                close = getattr(coro, "close", None)
                if callable(close):
                    close()
                raise RuntimeError("safe_async_wait_same_loop")
            completion = ConcurrentFuture()
            future = asyncio.run_coroutine_threadsafe(
                _run_lark_background_operation(coro, completion),
                worker_loop,
            )
            _lark_background_futures.add(future)
            _lark_background_completions[future] = completion
        else:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                future = ConcurrentFuture()
                completion = ConcurrentFuture()
                _lark_background_futures.add(future)
                _lark_background_completions[future] = completion
                no_loop_fallback = True
            else:
                close = getattr(coro, "close", None)
                if callable(close):
                    close()
                raise RuntimeError("safe_async_wait_running_loop")

    if completion is not None:
        _track_foreign_background_completion(future, completion)
    if no_loop_fallback:
        return _run_no_loop_fallback(coro, future, completion)

    try:
        return future.result(timeout=ACTION_WAIT_TIMEOUT_SECONDS)
    except FutureTimeoutError:
        future.cancel()
        raise RuntimeError("safe_async_wait_timeout") from None

async def _move_claimed_action_to_manual(
    email_id: str,
    state: Any | None,
    *,
    expected: frozenset[str],
) -> None:
    try:
        moved = await move_to_manual_review(
            email_id,
            db_manager,
            expected=expected,
            code="approval_handoff_failed",
        )
    except Exception as exc:
        logger.error(
            "Claimed action recovery failed: error_type=%s",
            type(exc).__name__,
        )
        return
    if not moved or state is None:
        return
    inbox_id = state.values.get("inbox_id")
    if inbox_id is not None:
        try:
            run = await db_manager.get_handoff_run(inbox_id)
            if run and run.get("state") in {
                "planned",
                "evidence_ready",
                "approval_pending",
                "approved",
            }:
                await db_manager.transition_handoff_manual_review(
                    inbox_id=inbox_id,
                    expected_version=int(run["version"]),
                )
        except Exception as exc:
            logger.error(
                "Durable handoff recovery failed: error_type=%s",
                type(exc).__name__,
            )
    try:
        config = {"configurable": {"thread_id": email_id}}
        update = build_manual_review_delta(
            state.values,
            "approval_handoff_failed",
        )
        await graph.aupdate_state(config, update)
    except Exception as exc:
        logger.error(
            "Manual-review graph handoff failed: error_type=%s",
            type(exc).__name__,
        )


async def _claim_action_safely(
    email_id: str,
    *,
    claimed_status: str,
    operation: Any,
) -> bool:
    """Stop on an ambiguous claim without attributing another worker's win."""
    try:
        return await operation()
    except Exception as exc:
        logger.error(
            "Action claim outcome is ambiguous: status=%s error_type=%s",
            claimed_status,
            type(exc).__name__,
        )
    return False


async def _process_approval_action(
    email_id: str,
    user_id: str,
    *,
    inbox_id: str | None = None,
    payload_revision: int | None = None,
    payload_digest: str | None = None,
) -> tuple[Any, dict[str, Any]] | None:
    async with get_approval_action_lock(email_id):
        if inbox_id is not None:
            config = {"configurable": {"thread_id": email_id}}
            state = await graph.aget_state(config)
            try:
                if state.values.get("inbox_id") != inbox_id:
                    return None
                run = await db_manager.get_handoff_run(inbox_id)
                if not run:
                    return None
                await db_manager.approve_payload_revision(
                    inbox_id=inbox_id,
                    revision=int(payload_revision or 0),
                    payload_digest=str(payload_digest or ""),
                    expected_version=int(run["version"]),
                    approver=user_id,
                    approved_at=datetime.now(UTC),
                )
                await graph.aupdate_state(
                    config,
                    _bounded_human_update(state, {"approval_status": "approved"}),
                )
                return state, config
            except Exception as exc:
                logger.warning(
                    "Durable approval rejected: error_type=%s", type(exc).__name__
                )
                return None
        if not await _claim_action_safely(
            email_id,
            claimed_status="approved",
            operation=lambda: claim_approval(email_id, user_id, db_manager),
        ):
            return None

        config = {"configurable": {"thread_id": email_id}}
        state = None
        try:
            state = await graph.aget_state(config)
            # The UI normally rejects an unbound durable card before this
            # point. Direct/stale callers that bypass it are quarantined after
            # winning the legacy claim, never resumed as mutable sends.
            if state.values.get("inbox_id") is not None:
                raise RuntimeError("durable_approval_binding_required")
            final_draft = await hydrate_draft_from_state(
                state.values,
                _require_graph_dependencies(),
            )
            await db_manager.update_status(
                email_id,
                None,
                approver_user_id=user_id,
                final_draft=final_draft,
            )
            update = _bounded_human_update(
                state,
                {"approval_status": "approved"},
            )
            await graph.aupdate_state(config, update)
        except asyncio.CancelledError:
            logger.warning("Approval handoff was cancelled after claim")
            await _move_claimed_action_to_manual(
                email_id,
                state,
                expected=frozenset({"approved"}),
            )
            raise
        except Exception as exc:
            logger.error(
                "Approval handoff failed: error_type=%s",
                type(exc).__name__,
            )
            await _move_claimed_action_to_manual(
                email_id,
                state,
                expected=frozenset({"approved"}),
            )
            return None
        return state, config


async def _claim_draft_save_action(
    email_id: str,
    *,
    inbox_id: str | None = None,
    payload_revision: int | None = None,
    payload_digest: str | None = None,
    state: Any | None = None,
) -> dict[str, object] | None:
    async with get_approval_action_lock(email_id):
        if inbox_id is None:
            claimed = await _claim_action_safely(
                email_id,
                claimed_status="saving_draft",
                operation=lambda: claim_draft_save(email_id, db_manager),
            )
            return {"durable": False} if claimed else None
        if state is None:
            state = await graph.aget_state(
                {"configurable": {"thread_id": email_id}}
            )
        durable_inbox_id = state.values.get("inbox_id")
        if durable_inbox_id is not None:
            if not (
                inbox_id == durable_inbox_id
                and _valid_durable_card_binding(
                    inbox_id, payload_revision, payload_digest
                )
            ):
                return None
            try:
                run = await db_manager.get_handoff_run(inbox_id)
                if not run:
                    return None
                return await db_manager.claim_payload_draft_save(
                    inbox_id=inbox_id,
                    revision=payload_revision,
                    payload_digest=payload_digest,
                    expected_version=int(run["version"]),
                )
            except Exception as exc:
                logger.error(
                    "Durable draft-save claim is ambiguous: error_type=%s",
                    type(exc).__name__,
                )
                return None
        return None


async def _process_rejection_action(
    email_id: str,
    user_id: str,
    reason: str = "",
    *,
    inbox_id: str | None = None,
    payload_revision: int | None = None,
    payload_digest: str | None = None,
) -> tuple[Any, dict[str, Any]] | None:
    async with get_approval_action_lock(email_id):
        config = {"configurable": {"thread_id": email_id}}
        if inbox_id is not None:
            state = await graph.aget_state(config)
            durable_inbox_id = state.values.get("inbox_id")
            if not (
                inbox_id == durable_inbox_id
                and _valid_durable_card_binding(
                    inbox_id, payload_revision, payload_digest
                )
            ):
                return None
            try:
                run = await db_manager.get_handoff_run(inbox_id)
                if not run or not await db_manager.reject_payload_revision(
                    inbox_id=inbox_id,
                    revision=payload_revision,
                    payload_digest=payload_digest,
                    expected_version=int(run["version"]),
                    approver=user_id,
                    reason=truncate_utf8(reason, max_bytes=512) if reason else "",
                ):
                    return None
            except Exception as exc:
                logger.warning(
                    "Durable rejection rejected: error_type=%s", type(exc).__name__
                )
                return None
            try:
                await graph.aupdate_state(
                    config,
                    _bounded_human_update(
                        state,
                        {"approval_status": "rejected"},
                    ),
                )
            except Exception as exc:
                # The durable rejection is authoritative.  A graph projection
                # can be retried without changing that terminal outcome.
                logger.warning(
                    "Durable rejection projection failed: error_type=%s",
                    type(exc).__name__,
                )
            return state, config

        if not await _claim_action_safely(
            email_id,
            claimed_status="rejected",
            operation=lambda: claim_rejection(email_id, user_id, db_manager),
        ):
            return None

        state = None
        try:
            state = await graph.aget_state(config)
            kwargs = {"approver_user_id": user_id}
            if reason:
                kwargs["rejection_reason"] = truncate_utf8(
                    reason,
                    max_bytes=512,
                )
            await db_manager.update_status(email_id, None, **kwargs)
            update = _bounded_human_update(
                state,
                {"approval_status": "rejected"},
            )
            await graph.aupdate_state(config, update)
        except asyncio.CancelledError:
            logger.warning("Rejection handoff was cancelled after claim")
            await _move_claimed_action_to_manual(
                email_id,
                state,
                expected=frozenset({"rejected"}),
            )
            raise
        except Exception as exc:
            logger.error(
                "Rejection handoff failed: error_type=%s",
                type(exc).__name__,
            )
            await _move_claimed_action_to_manual(
                email_id,
                state,
                expected=frozenset({"rejected"}),
            )
            return None
        return state, config


async def _process_modification_action(
    email_id: str,
    new_draft: object,
    *,
    inbox_id: str | None = None,
    payload_revision: int | None = None,
    payload_digest: str | None = None,
) -> bool:
    if not isinstance(new_draft, str) or not new_draft.strip():
        return False
    if len(new_draft.encode("utf-8")) > input_limits_from_settings(
        get_settings()
    ).body_bytes:
        return False

    async with get_approval_action_lock(email_id):
        dependencies = _require_graph_dependencies()
        if await db_manager.get_email_status(email_id) != "waiting_approval":
            return False
        config = {"configurable": {"thread_id": email_id}}
        binding: dict[str, object] | None = None
        state = await graph.aget_state(config)
        state_inbox_id = state.values.get("inbox_id")
        callback_has_binding = any(
            value is not None
            for value in (inbox_id, payload_revision, payload_digest)
        )
        if callback_has_binding or state_inbox_id is not None:
            if not (
                inbox_id == state_inbox_id
                and _valid_durable_card_binding(
                    inbox_id, payload_revision, payload_digest
                )
            ):
                return False
            binding = await _append_current_payload_revision(
                state,
                user_id="lark-editor",
                expected_payload_revision=payload_revision,
                expected_payload_digest=payload_digest,
                draft_override=new_draft,
            )
        draft_projected = await dependencies.drafts.save_draft_if_status(
            email_id,
            new_draft,
        )
        delta: dict[str, object] = {"draft_id": email_id}
        if binding is not None:
            delta.update(
                payload_revision=binding["payload_revision"],
                payload_digest=binding["payload_digest"],
            )
        update = _bounded_human_update(state, delta)
        try:
            if not draft_projected:
                raise RuntimeError("draft_projection_rejected")
            await graph.aupdate_state(config, update)
        except Exception as exc:
            if binding is None:
                return False
            logger.warning(
                "Durable draft projection lagged payload revision: error_type=%s",
                type(exc).__name__,
            )
        logger.info(
            "Modification saved: bytes=%d",
            len(new_draft.encode("utf-8")),
        )
        return True


async def _update_recipient_field_if_waiting(
    email_id: str,
    delta: dict[str, Any],
    *,
    inbox_id: str | None = None,
    payload_revision: int | None = None,
    payload_digest: str | None = None,
) -> bool:
    allowed_fields = {"draft_to", "draft_cc", "recipient_ui"}
    if not delta or not set(delta).issubset(allowed_fields):
        raise ValueError("invalid_recipient_update")
    async with get_approval_action_lock(email_id):
        if await db_manager.get_email_status(email_id) != "waiting_approval":
            return False
        config = {"configurable": {"thread_id": email_id}}
        state = await graph.aget_state(config)
        try:
            decision = RouteDecision.model_validate(
                state.values.get("route_decision")
            )
        except Exception:
            return False
        if (
            decision.route is CanonicalRoute.FORWARD
            and not decision.params.get("allow_recipient_edit", True)
        ):
            return False
        binding: dict[str, object] | None = None
        state_inbox_id = state.values.get("inbox_id")
        callback_has_binding = any(
            value is not None
            for value in (inbox_id, payload_revision, payload_digest)
        )
        if callback_has_binding or state_inbox_id is not None:
            if not (
                inbox_id == state_inbox_id
                and _valid_durable_card_binding(
                    inbox_id, payload_revision, payload_digest
                )
            ):
                return False
            binding = await _append_current_payload_revision(
                state,
                user_id="lark-editor",
                expected_payload_revision=payload_revision,
                expected_payload_digest=payload_digest,
                recipient_delta=delta,
            )
        projection_delta = dict(delta)
        if binding is not None:
            projection_delta.update(
                payload_revision=binding["payload_revision"],
                payload_digest=binding["payload_digest"],
            )
        update = _bounded_human_update(state, projection_delta)
        try:
            await graph.aupdate_state(config, update)
        except Exception as exc:
            if binding is None:
                raise
            logger.warning(
                "Durable recipient projection lagged payload revision: error_type=%s",
                type(exc).__name__,
            )
        return True


async def _append_current_payload_revision(
    state: Any,
    *,
    user_id: str,
    expected_payload_revision: int,
    expected_payload_digest: str,
    draft_override: str | None = None,
    recipient_delta: dict[str, Any] | None = None,
) -> dict[str, object]:
    """Freeze the latest editable projection before any revised card is rendered."""
    values = dict(state.values)
    for key, value in (recipient_delta or {}).items():
        if key in {"draft_to", "draft_cc"}:
            values[key] = value
    inbox_id = str(values["inbox_id"])
    email_data, draft = await hydrate_graph_content(
        values, _require_graph_dependencies()
    )
    if draft_override is not None:
        draft = draft_override
    decision = RouteDecision.model_validate(values.get("route_decision"))
    if decision.params.get("include_attachments", False):
        raise ValueError("unbound_forward_attachments")
    resolved = await resolve_recipients(
        values.get("draft_to") or [],
        values.get("draft_cc") or [],
        lark_client=lark_api_client,
    )
    if resolved is None or not await recipients_follow_route(
        decision,
        email_data,
        resolved,
        lark_client=lark_api_client,
    ):
        raise ValueError("recipient_policy_mismatch")
    run = await db_manager.get_handoff_run(inbox_id)
    if not run:
        raise ValueError("durable_handoff_unavailable")
    revision = await db_manager.create_payload_revision(
        inbox_id=inbox_id,
        expected_version=int(run["version"]),
        expected_payload_revision=expected_payload_revision,
        expected_payload_digest=expected_payload_digest,
        payload={
            "decision_digest": decision.canonical_digest(),
            "plan_digest": values.get("handoff_plan_digest"),
            "evidence_digest": values.get("evidence_pack_digest"),
            "draft_digest": hashlib.sha256(draft.encode()).hexdigest(),
            "draft_content": draft,
            "draft_ref": {"draft_id": values.get("draft_id")},
            "to": list(resolved.to), "cc": list(resolved.cc),
            "attachment_refs": [], "attachment_digests": [],
            "external_recipient_acknowledged": True,
            "editor": user_id, "edited_at": datetime.now(UTC),
        },
    )
    binding = await db_manager.get_payload_revision_binding(
        inbox_id=inbox_id, revision=revision
    )
    if binding is None:
        raise ValueError("payload_binding_unavailable")
    binding.update(
        draft_content=draft,
        to_recipients=list(resolved.to),
        cc_recipients=list(resolved.cc),
    )
    return binding


def _schedule_claimed_action_resume(
    email_id: str,
    state: Any,
    config: dict[str, Any],
    *,
    expected: frozenset[str],
) -> bool:
    preserve_durable_approval = bool(
        expected in {frozenset({"approved"}), frozenset({"rejected"})}
        and state.values.get("inbox_id") is not None
    )
    resume = _resume_graph_then_cleanup(
        email_id,
        state,
        config,
        preserve_durable_approval=preserve_durable_approval,
    )
    try:
        safe_async_run(resume)
    except Exception as exc:
        resume.close()
        logger.error(
            "Action resume scheduling failed: error_type=%s",
            type(exc).__name__,
        )
        if not preserve_durable_approval:
            safe_async_wait(
                _move_claimed_action_to_manual(
                    email_id,
                    state,
                    expected=expected,
                )
            )
        return False
    return True


def process_approval(
    email_id,
    user_id,
    *,
    inbox_id=None,
    payload_revision=None,
    payload_digest=None,
):
    handoff = safe_async_wait(
        _process_approval_action(
            email_id,
            user_id,
            inbox_id=inbox_id,
            payload_revision=payload_revision,
            payload_digest=payload_digest,
        )
    )
    if handoff is None:
        return False
    state, config = handoff
    if not _schedule_claimed_action_resume(
        email_id,
        state,
        config,
        expected=frozenset({"approved"}),
    ):
        return False
    logger.info("Approval processed; resuming graph")
    return True

def process_rejection(
    email_id,
    user_id,
    reason: str = "",
    *,
    inbox_id=None,
    payload_revision=None,
    payload_digest=None,
):
    handoff = safe_async_wait(
        _process_rejection_action(
            email_id,
            user_id,
            reason,
            inbox_id=inbox_id,
            payload_revision=payload_revision,
            payload_digest=payload_digest,
        )
    )
    if handoff is None:
        return False
    state, config = handoff
    if not _schedule_claimed_action_resume(
        email_id,
        state,
        config,
        expected=frozenset({"rejected"}),
    ):
        return False
    logger.info("Rejection processed; resuming graph")
    return True
    
def process_modification(
    email_id,
    new_draft,
    *,
    inbox_id=None,
    payload_revision=None,
    payload_digest=None,
):
    return safe_async_wait(
        _process_modification_action(
            email_id,
            new_draft,
            inbox_id=inbox_id,
            payload_revision=payload_revision,
            payload_digest=payload_digest,
        )
    )

async def _process_claimed_draft_save(
    email_id: str,
    state: Any | None = None,
    *,
    claim: dict[str, object] | None = None,
) -> bool:
    durable_inbox_id = claim.get("inbox_id") if claim else None
    durable_version = claim.get("handoff_version") if claim else None

    async def fail_closed(code: str) -> bool:
        try:
            if durable_inbox_id is not None and isinstance(durable_version, int):
                await db_manager.fail_payload_draft_save(
                    inbox_id=str(durable_inbox_id),
                    expected_version=durable_version,
                    error_code=code,
                )
            else:
                await move_to_manual_review(
                    email_id,
                    db_manager,
                    expected=frozenset({"saving_draft"}),
                    code=code,
                )
        except Exception as exc:
            logger.error(
                "Draft-save quarantine failed: error_type=%s",
                type(exc).__name__,
            )
        return False

    config = {"configurable": {"thread_id": email_id}}
    if durable_inbox_id is not None:
        email_data = {"subject": claim.get("subject") or ""}
        draft = claim.get("draft_content")
        raw_to = claim.get("to_recipients") or []
        raw_cc = claim.get("cc_recipients") or []
    else:
        try:
            if state is None:
                state = await graph.aget_state(config)
            email_data, draft = await _hydrate_lark_projection(state)
        except Exception as exc:
            logger.error(
                "Exchange draft hydration failed: error_type=%s",
                type(exc).__name__,
            )
            return await fail_closed("approval_handoff_failed")
        raw_to = state.values.get("draft_to") or []
        raw_cc = state.values.get("draft_cc") or []

    if not isinstance(draft, str) or not draft.strip():
        return await fail_closed("empty_draft")

    resolved = await resolve_recipients(
        raw_to,
        raw_cc,
        lark_client=lark_api_client,
    )
    if resolved is None:
        return await fail_closed("recipient_resolution_failed")
    final_to = list(resolved.to)
    final_cc = list(resolved.cc)

    subject = "Re: " + str(email_data.get("subject", ""))
    body = draft + "<br><br>--<br>AI Generated Draft"
    logger.info(
        "Saving Exchange draft: to_count=%d cc_count=%d",
        len(final_to),
        len(final_cc),
    )
    try:
        success = bool(
            exchange_client
            and await exchange_client.create_draft(
                list(dict.fromkeys(final_to)),
                subject,
                body,
                cc=list(dict.fromkeys(final_cc)),
            )
        )
    except Exception as exc:
        logger.error(
            "Exchange draft outcome is unknown: error_type=%s",
            type(exc).__name__,
        )
        success = False
    if not success:
        return await fail_closed("draft_save_outcome_unknown")

    try:
        if durable_inbox_id is not None and isinstance(durable_version, int):
            completed = await db_manager.complete_payload_draft_save(
                inbox_id=str(durable_inbox_id),
                expected_version=durable_version,
            )
        else:
            completed = await complete_draft_save(email_id, db_manager)
    except Exception as exc:
        logger.error(
            "Draft-save completion outcome is ambiguous: error_type=%s",
            type(exc).__name__,
        )
        try:
            completed = await db_manager.get_email_status(email_id) == "draft_saved"
        except Exception as read_exc:
            logger.error(
                "Draft-save completion readback failed: error_type=%s",
                type(read_exc).__name__,
            )
            completed = False
    if not completed:
        return await fail_closed("draft_save_outcome_unknown")

    try:
        if state is None:
            state = await graph.aget_state(config)
        await _cleanup_action_drive_tokens(email_id, state)
    except Exception as exc:
        logger.error(
            "Draft-save cleanup failed: error_type=%s",
            type(exc).__name__,
        )
    return True


async def _run_claimed_draft_save(
    email_id: str,
    state: Any | None = None,
    *,
    claim: dict[str, object] | None = None,
) -> bool:
    try:
        return await _process_claimed_draft_save(email_id, state, claim=claim)
    except asyncio.CancelledError:
        logger.warning("Exchange draft save was cancelled")
        try:
            if claim and claim.get("inbox_id") is not None:
                await db_manager.fail_payload_draft_save(
                    inbox_id=str(claim["inbox_id"]),
                    expected_version=int(claim["handoff_version"]),
                    error_code="draft_save_outcome_unknown",
                )
            else:
                await move_to_manual_review(
                    email_id,
                    db_manager,
                    expected=frozenset({"saving_draft"}),
                    code="draft_save_outcome_unknown",
                )
        except Exception as exc:
            logger.error(
                "Cancelled draft-save quarantine failed: error_type=%s",
                type(exc).__name__,
            )
        raise


async def process_save_draft(email_id, state=None) -> bool:
    claim = await _claim_draft_save_action(email_id, state=state)
    if not claim:
        return False
    return await _run_claimed_draft_save(email_id, state, claim=claim)

async def process_pdf_generation_and_reply(
    email_id,
    state,
    message_id,
) -> PdfFlowOutcome | None:
    """Generate PDF and reply with file link. Delegates to lark_pdf_flow."""
    from src.utils.lark_file_ops import delete_file_from_drive, upload_file_to_drive
    from src.utils.lark_pdf_flow import process_pdf_generation_and_reply as _impl

    def upload_file(name: str, content: bytes, size: int):
        return upload_file_to_drive(
            name,
            content,
            size,
            lark_api_client=lark_api_client,
        )

    def delete_file(token: str) -> bool:
        return delete_file_from_drive(token, lark_api_client=lark_api_client)

    return await _impl(
        email_id,
        state,
        message_id,
        graph=graph,
        dependencies=_require_graph_dependencies(),
        lark_api_client=lark_api_client,
        upload_fn=upload_file,
        delete_fn=delete_file,
    )


def _lark_ws_connection_ready(client: object | None) -> bool:
    """Fail closed unless the pinned SDK exposes one open WebSocket."""

    if client is None:
        return False
    try:
        connection = getattr(client, "_conn", None)
        if connection is None:
            return False

        # websockets has represented connection state through an enum, ``open``
        # and ``closed`` across supported releases. Prefer the enum when present
        # because it distinguishes OPEN from CONNECTING/CLOSING precisely.
        state = getattr(connection, "state", None)
        state_name = getattr(state, "name", None)
        if isinstance(state_name, str):
            return state_name.casefold() == "open"

        open_state = getattr(connection, "open", None)
        if type(open_state) is bool:
            return open_state

        closed_state = getattr(connection, "closed", None)
        if type(closed_state) is bool:
            return not closed_state
    except Exception:
        return False

    return False


def lark_ws_ready() -> bool:
    """Return whether the callback thread owns a currently open connection."""

    client = lark_ws_client
    thread = _lark_ws_thread
    return bool(
        client is not None
        and thread is not None
        and thread.is_alive()
        and not _lark_ws_stop_requested.is_set()
        and _lark_ws_connection_ready(client)
    )


def begin_lark_ws_shutdown() -> None:
    """Disarm unexpected-exit handling before intentional process shutdown."""

    _lark_ws_stop_requested.set()


def start_lark_ws(
    *,
    fail_stop: Callable[[str], None] | None = None,
):
    """
    Start WebSocket Client in a background thread
    """
    settings = get_settings()
    app_id = settings.LARK_APP_ID
    app_secret = resolve_secret(settings.LARK_APP_SECRET)
    
    if not (app_id and app_secret):
        logger.warning("Lark App ID/Secret missing. WS Client not started.")
        return

    global lark_ws_client, _lark_ws_thread

    if _lark_ws_thread is not None and _lark_ws_thread.is_alive():
        logger.warning("Lark WebSocket Client is already running.")
        return

    builder = lark_oapi.EventDispatcherHandler.builder("", "") \
        .register_p2_card_action_trigger(handle_card_action)
    if hasattr(builder, "register_p2_im_message_receive_v1"):
        builder = builder.register_p2_im_message_receive_v1(_handle_p2_im_message_receive)
    else:
        logger.warning("Lark SDK does not support im.message.receive_v1 registration.")
    event_handler = builder.build()

    lark_ws_client = lark_oapi.ws.Client(
        app_id, 
        app_secret, 
        event_handler=event_handler,
        log_level=lark_oapi.LogLevel.CRITICAL
    )

    _lark_ws_stop_requested.clear()
    client = lark_ws_client

    def run_client() -> None:
        try:
            client.start()
        except BaseException as exc:
            logger.critical(
                "Lark WebSocket Client exited with error: error_type=%s",
                type(exc).__name__,
            )
        finally:
            if not _lark_ws_stop_requested.is_set():
                logger.critical("Lark WebSocket Client exited unexpectedly")
                if fail_stop is not None:
                    fail_stop("lark_ws_lost")

    _lark_ws_thread = threading.Thread(
        target=run_client,
        name="lark-websocket-client",
        daemon=True,
    )
    _lark_ws_thread.start()
    logger.info("Lark WebSocket Client started in background thread.")


def stop_lark_ws(*, timeout_seconds: float = 5.0) -> None:
    """Disconnect the SDK client and join its daemon thread with a hard bound."""

    global lark_ws_client, _lark_ws_thread
    disable_lark_intake()
    begin_lark_ws_shutdown()
    client = lark_ws_client
    thread = _lark_ws_thread
    if client is None or thread is None:
        lark_ws_client = None
        _lark_ws_thread = None
        return

    disconnect_error = False
    try:
        setattr(client, "_auto_reconnect", False)
        public_stop = getattr(client, "stop", None)
        if callable(public_stop):
            public_stop()
        else:
            from lark_oapi.ws import client as ws_client_module

            sdk_loop = ws_client_module.loop
            disconnect = getattr(client, "_disconnect", None)
            if callable(disconnect) and sdk_loop.is_running():
                future = asyncio.run_coroutine_threadsafe(disconnect(), sdk_loop)
                try:
                    future.result(timeout=max(0.0, timeout_seconds))
                except BaseException:
                    disconnect_error = True
            if sdk_loop.is_running():
                sdk_loop.call_soon_threadsafe(sdk_loop.stop)
    except BaseException:
        disconnect_error = True

    thread.join(timeout=max(0.0, timeout_seconds))
    if thread.is_alive() or disconnect_error:
        raise RuntimeError("lark_ws_shutdown_failed")
    lark_ws_client = None
    _lark_ws_thread = None
