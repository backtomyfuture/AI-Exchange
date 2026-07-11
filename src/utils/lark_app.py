import json
import logging
import asyncio
import re
import html
import hashlib
import hmac
from copy import deepcopy
from typing import Dict, Any, List, Optional
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
    hydrate_graph_content,
    sanitize_graph_delta,
)
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
TEST_CARD_ID_PREFIX = "test_push_"

# Global instances
lark_ws_client: Optional[WsClient] = None
lark_api_client: Optional[lark_oapi.Client] = None
card_builder: Optional[LarkCardBuilder] = None
db_manager = None
graph = None
graph_dependencies: Optional[GraphDependencies] = None
exchange_client = None
worker_loop = None
_mock_store = {} # Store for test card states
_command_router: Optional[CommandRouter] = None


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
        logger.warning("LARK_ENCRYPT_KEY not configured, skipping signature verification.")
        return True
    
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

def upload_file_to_drive(name: str, content: bytes, size: int) -> Optional[dict]:
    """Upload file to Lark Drive. Delegates to lark_file_ops."""
    from src.utils.lark_file_ops import upload_file_to_drive as _impl
    return _impl(name, content, size, lark_api_client=lark_api_client)

def delete_file_from_drive(file_token: str) -> bool:
    """Delete a file from Lark Drive. Delegates to lark_file_ops."""
    from src.utils.lark_file_ops import delete_file_from_drive as _impl
    return _impl(file_token, lark_api_client=lark_api_client)

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
            .log_level(lark_oapi.LogLevel.INFO) \
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


def _require_graph_dependencies() -> GraphDependencies:
    if graph_dependencies is None:
        raise RuntimeError("lark_graph_dependencies_unavailable")
    return graph_dependencies


def is_test_card_id(email_id: Any) -> bool:
    return isinstance(email_id, str) and email_id.startswith(TEST_CARD_ID_PREFIX)


def _is_explicit_test_card(email_id: Any) -> bool:
    """Only DEBUG-mode entries explicitly seeded in memory are test cards."""
    if not is_test_card_id(email_id):
        return False
    try:
        debug_enabled = bool(get_settings().DEBUG)
    except Exception:
        return False
    return debug_enabled and email_id in _mock_store


def _test_card_builder() -> LarkCardBuilder:
    """Return a pure card renderer with no Lark or Exchange lookup clients."""
    return LarkCardBuilder(None, exchange_client=None)


def _search_test_card_candidates(state: Any, field: str, keyword: str) -> List[str]:
    """Search only the candidate directory explicitly seeded in test memory."""
    directories = state.values.get("recipient_candidates") or {}
    candidates = directories.get(field) if isinstance(directories, dict) else []
    needle = keyword.strip().casefold()
    if not needle:
        return []

    matches: List[str] = []
    for candidate in candidates or []:
        if isinstance(candidate, str):
            open_id = candidate.strip()
            search_text = open_id
        elif isinstance(candidate, dict):
            open_id = str(candidate.get("open_id") or "").strip()
            search_text = " ".join(
                str(candidate.get(key) or "")
                for key in ("search_text", "name", "email", "open_id")
            )
        else:
            continue
        if open_id and needle in search_text.casefold() and open_id not in matches:
            matches.append(open_id)
        if len(matches) >= 10:
            break
    return matches


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
    for token in targets:
        try:
            if await asyncio.to_thread(delete_file_from_drive, token):
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
) -> None:
    """Resume the workflow first, then reconcile action cleanup handles."""
    async with get_graph_resource_lock(email_id):
        try:
            await graph.ainvoke(None, config=config)
        finally:
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
        
        logger.info(f"Resolving identity for Chat ID: {chat_id}")
        
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
        logger.info(f"Identity Resolved: {user.name} ({user.email})")
        
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
            logger.info(f"Global Identity Configured: EXCHANGE_ACCOUNT_EMAIL = {effective_email}")
            
    except Exception as exc:
        logger.error("Identity resolution failed: error_type=%s", type(exc).__name__)

def send_approval_card(email_id: str, draft: str, context: List[dict], email_data: dict,
                       classification: dict, pdf_url: str = None,
                       routing_log: List = None, active_skills: List = None):
    """Send an interactive approval card. Delegates to lark_messaging."""
    from src.utils.lark_messaging import send_approval_card as _impl
    return _impl(email_id, draft, context, email_data, classification, pdf_url=pdf_url,
                 routing_log=routing_log, active_skills=active_skills,
                 lark_api_client=lark_api_client, card_builder=card_builder)

def send_system_notification(title: str, content: str, template: str = "red"):
    """Send a system notification card. Delegates to lark_messaging."""
    from src.utils.lark_messaging import send_system_notification as _impl
    return _impl(title, content, template, lark_api_client=lark_api_client)




# Event Handlers

def send_read_only_card(email_id: str, context: List[dict], email_data: dict,
                        classification: dict, pdf_url: str = None,
                        routing_log: List = None, active_skills: List = None):
    """Send a read-only card. Delegates to lark_messaging."""
    from src.utils.lark_messaging import send_read_only_card as _impl
    return _impl(email_id, context, email_data, classification, pdf_url=pdf_url,
                 routing_log=routing_log, active_skills=active_skills,
                 lark_api_client=lark_api_client, card_builder=card_builder)


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
             logger.info(f"Patch Success for {message_id}")
    except Exception as exc:
        logger.error("Card patch failed: error_type=%s", type(exc).__name__)



def handle_card_action(event):
    """
    Handle card interactions (WS) - Callback receives single 'event' object
    """
    try:
        if not hasattr(event, "event") or not event.event.action:
             logger.warning("Invalid event structure: missing event.action")
             return

        action_value = event.event.action.value
        # Parse Value
        if isinstance(action_value, str):
            try:
                data = json.loads(action_value)
            except Exception:
                return
        else:
            data = action_value

        action_type = data.get("action")
        email_id = data.get("id")
        user_id = event.event.operator.open_id
        # open_message_id is likely nested in context for card triggers, checking both
        if hasattr(event.event, "context") and hasattr(event.event.context, "open_message_id"):
             message_id = event.event.context.open_message_id
        else:
             message_id = getattr(event.event, "open_message_id", None)
        
        if not message_id:
             logger.warning("Could not find open_message_id in event. Cannot patch.")
        
        logger.info(f">>> RAW ACTION DATA: {action_type}, id={email_id}, msg_id={message_id}")
        logger.info(f"User ID: {user_id}")
        
        def get_current_state(eid):
            config = {"configurable": {"thread_id": eid}}
            return safe_async_wait(graph.aget_state(config))

        is_test_card = _is_explicit_test_card(email_id)
        # Explicit test-card boundary: never touch production Graph or stores.
        if is_test_card:
             logger.info("Using explicitly seeded test-card state")
             state = _mock_store[email_id]
        else:
            state = get_current_state(email_id)

        if not state or not state.values:
             logger.warning(f"No state found for {email_id}. Action: {action_type}")
             # Ensure we return a properly formatted error response
             return {"toast": {"type": "error", "content": "找不到任务状态或已失效"}}
             
        if is_test_card:
            email_data = state.values.get("email", {})
            draft = state.values.get("draft", "")
            context_summaries = state.values.get("context", [])
        else:
            email_data, draft = safe_async_wait(_hydrate_lark_projection(state))
            context_summaries = state.values.get("context_summaries", [])
        classification = state.values.get("classification", {})
        subject = email_data.get("subject", "Email")
        action_card_builder = _test_card_builder() if is_test_card else card_builder

        logger.info(f"State fetched for {email_id}. Action: {action_type}")

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
            if is_test_card:
                safe_async_run(
                    _process_test_card_pdf_generation_and_reply(
                        email_id,
                        state,
                        message_id,
                    )
                )
            else:
                safe_async_run(
                    process_pdf_generation_and_reply(email_id, state, message_id)
                )
            
            return {
                "toast": {
                    "type": "info",
                    "content": "正在生成PDF文件，请稍候..."
                }
            }


        elif action_type == "approve":
            process_approval(email_id, user_id)
            new_card = LarkCardBuilder.get_processed_card("已批准", subject)
            
            # 关键：直接在响应中返回新卡片，实现实时更新
            return {
                "toast": {
                    "type": "success",
                    "content": "审批请求已提交"
                },
                "card": {
                    "type": "raw",
                    "data": new_card
                }
            }
            
        elif action_type == "reject":
            process_rejection(email_id, user_id)
            new_card = LarkCardBuilder.get_processed_card("已拒绝", subject)
            return {
                "toast": {"type": "info", "content": "已拒绝该拟稿"},
                "card": {"type": "raw", "data": new_card}
            }

        elif action_type == "reject_with_reason":
            option = getattr(event.event.action, "option", None) or ""
            reason_map = {
                "tone_wrong": "语气不当",
                "content_error": "内容有误",
                "no_reply_needed": "无需回复",
                "other": "其他原因",
            }
            reason_text = reason_map.get(option, option or "未指定")
            process_rejection(email_id, user_id, reason=reason_text)
            new_card = LarkCardBuilder.get_processed_card(f"已拒绝 ({reason_text})", subject)
            return {
                "toast": {"type": "info", "content": f"已拒绝: {reason_text}"},
                "card": {"type": "raw", "data": new_card}
            }
            
        # 只读卡片 - 标记已阅
        elif action_type == "mark_read":
            logger.info(f"Mark as read for email {email_id} by {user_id}")
            if is_test_card:
                state.values["status"] = "read"
            else:
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
            email_data["draft_to"] = new_to

            # PERSISTENCE
            if is_test_card:
                # Update Mock Store
                mock_email = _mock_store[email_id].values["email"]
                if "original_to" not in mock_email:
                    mock_email["original_to"] = list(mock_email.get("to", []))
                mock_email["draft_to"] = new_to
            else:
                config = {"configurable": {"thread_id": email_id}}
                update = _bounded_human_update(state, {"draft_to": new_to})
                safe_async_wait(graph.aupdate_state(config, update))

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
            email_data["draft_cc"] = new_cc

            # PERSISTENCE
            if is_test_card:
                # Update Mock Store
                mock_email = _mock_store[email_id].values["email"]
                if "original_cc" not in mock_email:
                    mock_email["original_cc"] = list(mock_email.get("cc", []))
                mock_email["draft_cc"] = new_cc
            else:
                config = {"configurable": {"thread_id": email_id}}
                update = _bounded_human_update(state, {"draft_cc": new_cc})
                safe_async_wait(graph.aupdate_state(config, update))

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

            email_data["draft_to"] = new_to
            clear_recipient_edit_temp(email_data, "to")
            if is_test_card:
                mock_email = _mock_store[email_id].values["email"]
                if "original_to" not in mock_email:
                    mock_email["original_to"] = list(mock_email.get("to", []))
                mock_email["draft_to"] = new_to
                clear_recipient_edit_temp(mock_email, "to")
            else:
                config = {"configurable": {"thread_id": email_id}}
                update = _bounded_human_update(
                    state,
                    {"draft_to": new_to, "recipient_ui": {"to": {}}},
                )
                safe_async_wait(graph.aupdate_state(config, update))

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
                    if is_test_card:
                        matched_uids = _search_test_card_candidates(
                            state,
                            field_type,
                            keyword,
                        )
                    else:
                        matched_uids = await asyncio.to_thread(
                            card_builder.search_person_picker_candidates,
                            keyword,
                        )
                    options_key = f"draft_{field_type}_options"
                    hint_key = f"draft_{field_type}_search_hint"
                    selected_key = f"draft_{field_type}_new_selected"
                    external_key = f"draft_{field_type}_external_input"

                    def _next_hint(total_candidates: int) -> str:
                        if matched_uids:
                            return f"本次命中 {len(matched_uids)} 人，累计候选 {total_candidates} 人。可继续搜索并勾选后保存。"
                        return f"未找到“{keyword}”匹配人员，累计候选 {total_candidates} 人。仅支持邮箱前缀精确搜索（@前部分）。"

                    if is_test_card:
                        mock_state = _mock_store.get(email_id)
                        if not mock_state:
                            return
                        current_email = mock_state.values.get("email", {}).copy()
                        selected_raw = form_values.get(f"{field_type}_new", None)
                        if selected_raw is None:
                            selected_new = normalize_uid_list(current_email.get(selected_key))
                        else:
                            selected_new = normalize_uid_list(selected_raw)
                        current_options = normalize_uid_list(current_email.get(options_key))
                        merged_options = merge_unique(current_options + matched_uids + selected_new)
                        external_raw = form_values.get(f"{field_type}_external_input", None)
                        if external_raw is None:
                            external_input = str(current_email.get(external_key, "") or "")
                        else:
                            external_input = str(external_raw or "").strip()
                        current_email[options_key] = merged_options
                        current_email[selected_key] = selected_new
                        current_email[external_key] = external_input
                        current_email[hint_key] = _next_hint(len(merged_options))
                        mock_state.values["email"] = current_email
                        latest_email = current_email
                        latest_classification = mock_state.values.get("classification", classification)
                        latest_draft = mock_state.values.get("draft", "")
                    else:
                        config = {"configurable": {"thread_id": email_id}}
                        latest_state = await graph.aget_state(config)
                        latest_email, latest_draft = await _hydrate_lark_projection(
                            latest_state
                        )
                        current_ui = latest_state.values.get("recipient_ui") or {}
                        field_ui = current_ui.get(field_type) or {}
                        selected_raw = form_values.get(f"{field_type}_new", None)
                        if selected_raw is None:
                            selected_new = normalize_uid_list(field_ui.get("selected"))
                        else:
                            selected_new = normalize_uid_list(selected_raw)
                        current_options = normalize_uid_list(field_ui.get("options"))
                        merged_options = merge_unique(current_options + matched_uids + selected_new)
                        external_raw = form_values.get(f"{field_type}_external_input", None)
                        if external_raw is None:
                            external_input = str(field_ui.get("external_input", "") or "")
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
                        await graph.aupdate_state(
                            config,
                            _bounded_human_update(
                                latest_state,
                                {"recipient_ui": ui_delta},
                            ),
                        )
                        latest_state = await graph.aget_state(config)
                        latest_email, latest_draft = await _hydrate_lark_projection(
                            latest_state
                        )
                        latest_classification = latest_state.values.get("classification", classification)
                        latest_context = latest_state.values.get("context_summaries", [])

                    edit_card = action_card_builder.build_approval_card(
                        email_id,
                        latest_draft,
                        latest_context if not is_test_card else context_summaries,
                        latest_email,
                        latest_classification,
                        edit_field=field_type,
                    )
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
            email_data["draft_cc"] = new_cc
            clear_recipient_edit_temp(email_data, "cc")
            if is_test_card:
                mock_email = _mock_store[email_id].values["email"]
                if "original_cc" not in mock_email:
                    mock_email["original_cc"] = list(mock_email.get("cc", []))
                mock_email["draft_cc"] = new_cc
                clear_recipient_edit_temp(mock_email, "cc")
            else:
                config = {"configurable": {"thread_id": email_id}}
                update = _bounded_human_update(
                    state,
                    {"draft_cc": new_cc, "recipient_ui": {"cc": {}}},
                )
                safe_async_wait(graph.aupdate_state(config, update))

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
            if new_draft:
                if is_test_card:
                     _mock_store[email_id].values["draft"] = new_draft
                process_modification(email_id, new_draft)
                logger.info("Draft updated: bytes=%d", len(new_draft.encode("utf-8")))
            else:
                new_draft = draft
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

        elif action_type == "save_draft_only":
            logger.info("Executing Request: Save Draft")
            safe_async_run(process_save_draft(email_id, state))
            new_card = LarkCardBuilder.get_processed_card("已存草稿", subject)
            return {
                "toast": {"type": "success", "content": "草稿已保存"},
                "card": {"type": "raw", "data": new_card}
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
            process_modification(email_id, new_draft)
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
        raw_content = _read_nested(message, "content") or ""

        if message_type != "text" or chat_type != "p2p" or not sender_open_id:
            return

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

def safe_async_run(coro):
    """
    Safely run a coroutine in the background (non-blocking for the caller).
    This is thread-safe and MUST be used when calling async code from the WebSocket thread.
    """
    global worker_loop
    if worker_loop and worker_loop.is_running():
        asyncio.run_coroutine_threadsafe(coro, worker_loop)
    else:
        # Fallback for when not running in the main application context (e.g. tests)
        logger.warning("Worker loop not available or not running. Falling back to unsafe async run.")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            asyncio.run(coro)

def safe_async_wait(coro):
    """
    Wait for a coroutine to complete and return the result.
    This is thread-safe and MUST be used when calling async code from the WebSocket thread.
    """
    global worker_loop
    if worker_loop and worker_loop.is_running():
        future = asyncio.run_coroutine_threadsafe(coro, worker_loop)
        return future.result()
    else:
         # Fallback for when not running in the main application context
        logger.warning("Worker loop not available or not running. Falling back to unsafe async wait.")
        try:
            return asyncio.run(coro)
        except RuntimeError:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(coro)

def process_approval(email_id, user_id):
    if _is_explicit_test_card(email_id):
        values = _mock_store[email_id].values
        values["approval_status"] = "approved"
        values["approver_user_id"] = user_id
        values["status"] = "approved"
        return

    config = {"configurable": {"thread_id": email_id}}
    state = safe_async_wait(graph.aget_state(config))
    final_draft = safe_async_wait(
        hydrate_draft_from_state(state.values, _require_graph_dependencies())
    )
    safe_async_wait(db_manager.update_status(
        email_id, "approved",
        approver_user_id=user_id,
        final_draft=final_draft,
    ))

    update = _bounded_human_update(state, {"approval_status": "approved"})
    safe_async_wait(graph.aupdate_state(config, update))
    logger.info("Approval processed; resuming graph")
    
    safe_async_run(_resume_graph_then_cleanup(email_id, state, config))

def process_rejection(email_id, user_id, reason: str = ""):
    if _is_explicit_test_card(email_id):
        values = _mock_store[email_id].values
        values["approval_status"] = "rejected"
        values["approver_user_id"] = user_id
        values["rejection_reason"] = reason
        values["status"] = "rejected"
        return

    config = {"configurable": {"thread_id": email_id}}
    state = safe_async_wait(graph.aget_state(config))
    update = _bounded_human_update(state, {"approval_status": "rejected"})
    safe_async_wait(graph.aupdate_state(config, update))
    kwargs = {"approver_user_id": user_id}
    if reason:
        kwargs["rejection_reason"] = reason
    safe_async_wait(db_manager.update_status(email_id, "rejected", **kwargs))
    logger.info("Rejection processed; resuming graph")
    safe_async_run(_resume_graph_then_cleanup(email_id, state, config))
    
def process_modification(email_id, new_draft):
    if _is_explicit_test_card(email_id):
        values = _mock_store[email_id].values
        values["draft"] = new_draft
        values["approval_status"] = "modify"
        values["status"] = "modified"
        return
    config = {"configurable": {"thread_id": email_id}}
    state = safe_async_wait(graph.aget_state(config))
    dependencies = _require_graph_dependencies()
    draft_id = safe_async_wait(dependencies.drafts.save_draft(email_id, new_draft))
    update = _bounded_human_update(
        state,
        {"draft_id": draft_id, "approval_status": "modify"},
    )
    safe_async_wait(graph.aupdate_state(config, update))
    safe_async_wait(db_manager.update_status(email_id, "modified"))
    logger.info("Modification saved: bytes=%d", len(new_draft.encode("utf-8")))

async def process_save_draft(email_id, state):
    try:
        if _is_explicit_test_card(email_id):
            state.values["status"] = "draft_saved"
            return

        email_data, draft = await _hydrate_lark_projection(state)
        
        # 1. Resolve Recipients (Copy logic from sender.py)
        from lark_oapi.api.contact.v3 import GetUserRequest
        
        def resolve_recipient(recipient_str):
            # Check for open_id=xxx
            if "open_id=" in str(recipient_str):
                open_id = str(recipient_str).replace("open_id=", "").strip()
                if not lark_api_client:
                    return None
                try:
                    req = GetUserRequest.builder().user_id(open_id).user_id_type("open_id").build()
                    resp = lark_api_client.contact.v3.user.get(req)
                    if resp.success() and resp.data and resp.data.user:
                            # Prioritize enterprise_email, then email
                            email = resp.data.user.enterprise_email or resp.data.user.email
                            if email:
                                return email
                    return None
                except Exception as exc:
                    logger.error(
                        "Recipient resolution failed: error_type=%s",
                        type(exc).__name__,
                    )
                    return None
            
            # Extract from legacy format "name='...', email_address='...'" or just return string
            if "email_address='" in str(recipient_str):
                m = re.search(r"email_address='(.*?)'", str(recipient_str))
                if m:
                    return m.group(1)
            
            return str(recipient_str)
    
        final_to = []
        raw_to = email_data.get("draft_to", [])
        if isinstance(raw_to, str):
            raw_to = [raw_to]
        for r in raw_to:
            resolved = resolve_recipient(r)
            if resolved:
                final_to.append(resolved)

        final_cc = []
        raw_cc = email_data.get("draft_cc", [])
        if isinstance(raw_cc, str):
            raw_cc = [raw_cc]
        for r in raw_cc:
            resolved = resolve_recipient(r)
            if resolved:
                final_cc.append(resolved)

        subject = "Re: " + email_data.get("subject", "")
        body = draft + "<br><br>--<br>AI Generated Draft"
        
        if exchange_client:
             logger.info(
                 "Saving Exchange draft: to_count=%d cc_count=%d",
                 len(final_to),
                 len(final_cc),
             )
             await exchange_client.create_draft(final_to, subject, body, cc=final_cc)
        
        await db_manager.update_status(email_id, "draft_saved")

        await _cleanup_action_drive_tokens(email_id, state)

    except Exception as exc:
        logger.error(
            "Exchange draft save failed: error_type=%s",
            type(exc).__name__,
        )


async def _render_test_card_pdf(email_data: Dict[str, Any]) -> Optional[bytes]:
    """Render explicitly seeded test data without Graph or persistence stores."""
    from src.utils.email_renderer import render_email_html
    from src.utils.pdf_generator import convert_html_to_pdf

    try:
        loop = asyncio.get_running_loop()
        isolated_email = deepcopy(email_data)
        html_content = await loop.run_in_executor(
            None,
            render_email_html,
            isolated_email,
        )
        if not html_content:
            return None
        return await loop.run_in_executor(
            None,
            convert_html_to_pdf,
            html_content,
        )
    except Exception as exc:
        logger.error(
            "Test-card PDF render failed: error_type=%s",
            type(exc).__name__,
        )
        return None


async def _process_test_card_pdf_generation_and_reply(
    email_id: str,
    state: Any,
    message_id: str,
) -> None:
    """Render/reply from explicit in-memory test state only."""
    if not _is_explicit_test_card(email_id):
        return
    if _mock_store.get(email_id) is not state or not lark_api_client:
        return

    pdf_bytes = await _render_test_card_pdf(state.values.get("email") or {})
    if not pdf_bytes:
        return

    filename = f"Email_Export_{email_id}.pdf"
    try:
        result = await asyncio.to_thread(
            upload_file_to_drive,
            filename,
            pdf_bytes,
            len(pdf_bytes),
        )
    except Exception as exc:
        logger.error(
            "Test-card PDF upload failed: error_type=%s",
            type(exc).__name__,
        )
        return
    if not isinstance(result, dict):
        return

    file_url = result.get("url")
    file_token = result.get("file_token")
    if not isinstance(file_url, str) or not file_url:
        return
    if not isinstance(file_token, str) or not file_token:
        return

    state.values["pdf_token"] = file_token
    card_content = {
        "header": {
            "template": "blue",
            "title": {"content": "📄 PDF 原文已生成", "tag": "plain_text"},
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"点击下方按钮查看测试 PDF：\nFilename: *{filename}*",
                },
            },
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "📂 打开 PDF"},
                        "type": "primary",
                        "url": file_url,
                    }
                ],
            },
        ],
    }
    request = (
        ReplyMessageRequest.builder()
        .message_id(message_id)
        .request_body(
            ReplyMessageRequestBody.builder()
            .msg_type("interactive")
            .content(json.dumps(card_content))
            .build()
        )
        .build()
    )
    try:
        lark_api_client.im.v1.message.reply(request)
    except Exception as exc:
        logger.error(
            "Test-card PDF reply failed: error_type=%s",
            type(exc).__name__,
        )

async def generate_and_upload_pdf(
    email_id: str,
) -> Optional[Dict[str, Any]] | PdfFlowOutcome:
    """Resolve strict Graph refs, render email -> PDF, then upload."""
    from src.utils.lark_pdf_flow import generate_and_upload_pdf as _impl
    config = {"configurable": {"thread_id": email_id}}
    state = await graph.aget_state(config)
    return await _impl(
        email_id,
        state,
        dependencies=_require_graph_dependencies(),
        upload_fn=upload_file_to_drive,
        delete_fn=delete_file_from_drive,
    )


async def process_pdf_generation_and_reply(
    email_id,
    state,
    message_id,
) -> PdfFlowOutcome | None:
    """Generate PDF and reply with file link. Delegates to lark_pdf_flow."""
    from src.utils.lark_pdf_flow import process_pdf_generation_and_reply as _impl
    return await _impl(
        email_id,
        state,
        message_id,
        graph=graph,
        dependencies=_require_graph_dependencies(),
        lark_api_client=lark_api_client,
        upload_fn=upload_file_to_drive,
        delete_fn=delete_file_from_drive,
    )


def start_lark_ws():
    """
    Start WebSocket Client in a background thread
    """
    settings = get_settings()
    app_id = settings.LARK_APP_ID
    app_secret = resolve_secret(settings.LARK_APP_SECRET)
    
    if not (app_id and app_secret):
        logger.warning("Lark App ID/Secret missing. WS Client not started.")
        return

    global lark_ws_client

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
        log_level=lark_oapi.LogLevel.INFO
    )

    import threading
    ws_thread = threading.Thread(target=lark_ws_client.start, daemon=True)
    ws_thread.start()
    logger.info("Lark WebSocket Client started in background thread.")
