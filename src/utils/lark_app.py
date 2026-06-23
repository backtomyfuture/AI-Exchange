import os
import json
import logging
import asyncio
import re
import html
import io
import hashlib
import hmac
from typing import Dict, Any, List, Optional
import lark_oapi
from lark_oapi.api.im.v1 import *
from lark_oapi.api.contact.v3 import *
from lark_oapi.ws import Client as WsClient
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse, CallBackCard, CallBackToast

from src.utils.card_builder import LarkCardBuilder, html_to_lark_md
from src.config import get_settings, resolve_secret
from src.utils.email_renderer import render_email_html
from src.utils.pdf_generator import convert_html_to_pdf
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

# Global instances
lark_ws_client: Optional[WsClient] = None
lark_api_client: Optional[lark_oapi.Client] = None
card_builder: Optional[LarkCardBuilder] = None
db_manager = None
graph = None
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

def init_lark_app(db_mgr, graph_instance, ex_client, worker_loop_arg=None):
    """
    Initialize global dependencies
    """
    global db_manager, graph, exchange_client, lark_api_client, card_builder, worker_loop
    db_manager = db_mgr
    graph = graph_instance
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

def _collect_cleanup_tokens(state) -> List[str]:
    """
    Collect all file tokens that need to be cleaned up from the state.
    Includes:
    1. 'attachment_tokens' (legacy/explicit list)
    2. 'pdf_token' (generated PDF)
    3. 'email.attachments[].lark_file_token' (original attachments)
    """
    tokens = set()
    
    # 1. Explicit tokens
    if state.values.get("attachment_tokens"):
        tokens.update(state.values["attachment_tokens"])
        
    # 2. PDF Token
    if state.values.get("pdf_token"):
        tokens.add(state.values["pdf_token"])
        
    # 3. Email Attachments
    email_data = state.values.get("email", {})
    attachments = email_data.get("attachments", [])
    for att in attachments:
        if att.get("lark_file_token"):
            tokens.add(att["lark_file_token"])
            
    return list(tokens)

def _resolve_current_user_email(chat_id: str):

    """
    Dynamically resolve the current user's email based on LARK_CHAT_ID.
    Logic: GetChat -> OwnerID (OpenID) -> GetUser -> Email/Username
    """
    if not lark_api_client: return

    try:
        # Import models here to avoid circular/early import issues if sdk not ready
        from lark_oapi.api.im.v1.model.get_chat_request import GetChatRequest
        from lark_oapi.api.contact.v3.model.get_user_request import GetUserRequest
        
        logger.info(f"Resolving identity for Chat ID: {chat_id}")
        
        # 1. Get Chat Owner (P2P Owner = User)
        req = GetChatRequest.builder().chat_id(chat_id).build()
        resp = lark_api_client.im.v1.chat.get(req)
        
        if not resp.success():
            logger.warning(f"Failed to resolve identity (GetChat): {resp.code} - {resp.msg}")
            return
            
        owner_id = resp.data.owner_id
        if not owner_id:
            return

        # 2. Get User Profile
        user_req = GetUserRequest.builder().user_id(owner_id).user_id_type("open_id").build()
        user_resp = lark_api_client.contact.v3.user.get(user_req)
        
        if not user_resp.success():
             logger.warning(f"Failed to resolve identity (GetUser): {user_resp.code} - {user_resp.msg}")
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
            
    except Exception as e:
        logger.error(f"Error resolving identity: {e}")

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
    if not lark_api_client: return
    try:
        # Patch Request
        req = PatchMessageRequest.builder() \
            .message_id(message_id) \
            .request_body(PatchMessageRequestBody.builder()
                .content(json.dumps(card_content))
                .build()) \
            .build()
        
        logger.info(f"Patching Card JSON: {json.dumps(card_content, ensure_ascii=False)}")
            
        resp = lark_api_client.im.v1.message.patch(req)
        
        logger.info(f"Patch Response Code: {resp.code}")
        if not resp.success():
             logger.error(f"Failed to patch card {message_id}: {resp.code} - {resp.msg} - {resp.error}")
        else:
             logger.info(f"Patch Success for {message_id}")
    except Exception as e:
        logger.error(f"Error patching card: {e}")



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
            except:
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

        # Common data for UI updates
        state = get_current_state(email_id) 
        
         # --- TEST CARD FALLBACK ---
        # 对测试卡片总是使用MOCK STATE
        if str(email_id).startswith("test_push_"):
             logger.info(f"Injecting MOCK STATE for test card: {email_id}")
             # Check if we have stored state
             if email_id in _mock_store:
                 state = _mock_store[email_id]
             else:
                 class MockState:
                     def __init__(self):
                         self.values = {
                             "draft": "Thank you, I have received the update.",
                             "email": {
                                 "subject": "TEST: Complex Email Rendering",
                                 "sender": "name='System', email_address='q-fu@tianjin-air.com'",
                                 "to": ["name='Jarod', email_address='q-fu@tianjin-air.com'", "name='Zhang', email_address='yy-zhang1@tianjin-air.com'"],
                                 "cc": ["name='Jarod-CC', email_address='q-fu@tianjin-air.com'", "name='Zhang-CC', email_address='yy-zhang1@tianjin-air.com'"],
                                 "attachments": [{"name": "itinerary.pdf"}, {"name": "invoice_scan.jpg"}]
                             },
                             "classification": {
                                "reasoning": "This is a test notification sent manually.",
                                "summary": "这是一封系统生成的测试邮件，包含两个模拟附件（行程单和发票扫描件）。请确认系统是否正确解析并显示了这些内容。"
                            }
                         }
                 state = MockState()
                 _mock_store[email_id] = state

        if not state or not state.values:
             logger.warning(f"No state found for {email_id}. Action: {action_type}")
             # Ensure we return a properly formatted error response
             return {"toast": {"type": "error", "content": "找不到任务状态或已失效"}}
             
        email_data = state.values.get("email", {})
        classification = state.values.get("classification", {})
        subject = email_data.get("subject", "Email")

        logger.info(f"State fetched for {email_id}. Action: {action_type}")

        # Prepare Base Response (ACK)
        response = P2CardActionTriggerResponse()
        
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
            except Exception as e:
                logger.error(f"Error sending reply: {e}")
                raise e

        elif action_type == "view_original_pdf":
            logger.info("Executing Request: View Original PDF")
            # Trigger background task
            safe_async_run(process_pdf_generation_and_reply(email_id, state, message_id))
            
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
            safe_async_wait(db_manager.update_status(email_id, "read"))
            
            # Remove attachments (PDF + Originals)
            att_tokens = _collect_cleanup_tokens(state)
            
            if att_tokens:
                logger.info(f"Cleaning up {len(att_tokens)} attachments (mark_read)...")
                for tok in att_tokens:
                     safe_async_run(asyncio.to_thread(delete_file_from_drive, tok))

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
            draft = state.values.get("draft", "")
            logger.info(f"edit_to: email_data={email_data}, draft={draft[:50] if draft else 'None'}")
            edit_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field="to")
            return {
                "toast": {"type": "info", "content": "编辑收件人"},
                "card": {"type": "raw", "data": edit_card}
            }
        
        # 编辑抄送人
        elif action_type == "edit_cc":
            draft = state.values.get("draft", "")
            edit_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field="cc")
            return {
                "toast": {"type": "info", "content": "编辑抄送人"},
                "card": {"type": "raw", "data": edit_card}
            }
        
        # 编辑正文
        elif action_type == "edit_draft":
            draft = state.values.get("draft", "")
            edit_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field="draft")
            return {
                "toast": {"type": "info", "content": "编辑正文"},
                "card": {"type": "raw", "data": edit_card}
            }
        
        # 选择人员时的回调 - 支持多选并直接更新卡片
        elif action_type == "select_to":
            action_data = event.event.action
            selected_uids = read_selected_open_ids(action_data)
            logger.info(f"select_to: selected={selected_uids}")
            if not selected_uids:
                draft = state.values.get("draft", "")
                view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
                return {
                    "toast": {"type": "warning", "content": "收件人至少保留 1 人"},
                    "card": {"type": "raw", "data": view_card}
                }

            new_to = [f"open_id={uid}" for uid in selected_uids]
            email_data["draft_to"] = new_to

            # PERSISTENCE
            if str(email_id).startswith("test_push_"):
                # Update Mock Store
                mock_email = _mock_store[email_id].values["email"]
                if "original_to" not in mock_email:
                    mock_email["original_to"] = list(mock_email.get("to", []))
                mock_email["draft_to"] = new_to
            else:
                # Update Graph State
                config = {"configurable": {"thread_id": email_id}}
                current_email = state.values.get("email", {}).copy()
                if "original_to" not in current_email:
                    current_email["original_to"] = list(current_email.get("to", []))
                current_email["draft_to"] = new_to
                safe_async_wait(graph.aupdate_state(config, {"email": current_email}))

            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": f"收件人已更新（{len(new_to)}人）"},
                "card": {"type": "raw", "data": view_card}
            }
        
        elif action_type == "select_cc":
            action_data = event.event.action
            selected_uids = read_selected_open_ids(action_data)
            logger.info(f"select_cc: selected={selected_uids}")
            new_cc = [f"open_id={uid}" for uid in selected_uids]
            email_data["draft_cc"] = new_cc

            # PERSISTENCE
            if str(email_id).startswith("test_push_"):
                # Update Mock Store
                mock_email = _mock_store[email_id].values["email"]
                if "original_cc" not in mock_email:
                    mock_email["original_cc"] = list(mock_email.get("cc", []))
                mock_email["draft_cc"] = new_cc
            else:
                # Update Graph State
                config = {"configurable": {"thread_id": email_id}}
                current_email = state.values.get("email", {}).copy()
                if "original_cc" not in current_email:
                    current_email["original_cc"] = list(current_email.get("cc", []))
                current_email["draft_cc"] = new_cc
                safe_async_wait(graph.aupdate_state(config, {"email": current_email}))

            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": f"抄送人已更新（{len(new_cc)}人）"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 保存收件人
        elif action_type == "save_to":
            action_data = event.event.action
            form_values = getattr(action_data, "form_value", None) or {}
            logger.info(f"save_to action data: value={action_data.value}, form_value={form_values}")
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
                draft = state.values.get("draft", "")
                edit_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field="to")
                return {
                    "toast": {"type": "warning", "content": "收件人至少保留 1 人（飞书人员或外部邮箱）"},
                    "card": {"type": "raw", "data": edit_card}
                }

            email_data["draft_to"] = new_to
            clear_recipient_edit_temp(email_data, "to")
            if str(email_id).startswith("test_push_"):
                mock_email = _mock_store[email_id].values["email"]
                if "original_to" not in mock_email:
                    mock_email["original_to"] = list(mock_email.get("to", []))
                mock_email["draft_to"] = new_to
                clear_recipient_edit_temp(mock_email, "to")
            else:
                config = {"configurable": {"thread_id": email_id}}
                current_email = state.values.get("email", {}).copy()
                if "original_to" not in current_email:
                    current_email["original_to"] = list(current_email.get("to", []))
                current_email["draft_to"] = new_to
                clear_recipient_edit_temp(current_email, "to")
                safe_async_wait(graph.aupdate_state(config, {"email": current_email}))

            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
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
            logger.info("search_%s form_value=%s", field_type, form_values)

            if not keyword:
                draft = state.values.get("draft", "")
                edit_card = card_builder.build_approval_card(
                    email_id, draft, [], email_data, classification, edit_field=field_type
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
                    options_key = f"draft_{field_type}_options"
                    hint_key = f"draft_{field_type}_search_hint"
                    selected_key = f"draft_{field_type}_new_selected"
                    external_key = f"draft_{field_type}_external_input"

                    def _next_hint(total_candidates: int) -> str:
                        if matched_uids:
                            return f"本次命中 {len(matched_uids)} 人，累计候选 {total_candidates} 人。可继续搜索并勾选后保存。"
                        return f"未找到“{keyword}”匹配人员，累计候选 {total_candidates} 人。仅支持邮箱前缀精确搜索（@前部分）。"

                    if str(email_id).startswith("test_push_"):
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
                        current_email = latest_state.values.get("email", {}).copy()
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
                        await graph.aupdate_state(config, {"email": current_email})
                        latest_state = await graph.aget_state(config)
                        latest_email = latest_state.values.get("email", current_email)
                        latest_classification = latest_state.values.get("classification", classification)
                        latest_draft = latest_state.values.get("draft", "")

                    edit_card = card_builder.build_approval_card(
                        email_id,
                        latest_draft,
                        [],
                        latest_email,
                        latest_classification,
                        edit_field=field_type,
                    )
                    update_card_ui(message_id, edit_card)
                    logger.info(
                        "Recipient search finished: field=%s, keyword=%s, matches=%s",
                        field_type,
                        keyword,
                        len(matched_uids),
                    )
                except Exception as e:
                    logger.error("Recipient search failed: field=%s, err=%s", field_type, e, exc_info=True)

            safe_async_run(_search_and_patch_recipients())
            return {"toast": {"type": "info", "content": f"正在搜索“{keyword}”，稍后自动更新候选人..."}}
        
        # 保存抄送人
        elif action_type == "save_cc":
            action_data = event.event.action
            form_values = getattr(action_data, "form_value", None) or {}
            logger.info(f"save_cc action data: value={action_data.value}, form_value={form_values}")
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
            if str(email_id).startswith("test_push_"):
                mock_email = _mock_store[email_id].values["email"]
                if "original_cc" not in mock_email:
                    mock_email["original_cc"] = list(mock_email.get("cc", []))
                mock_email["draft_cc"] = new_cc
                clear_recipient_edit_temp(mock_email, "cc")
            else:
                config = {"configurable": {"thread_id": email_id}}
                current_email = state.values.get("email", {}).copy()
                if "original_cc" not in current_email:
                    current_email["original_cc"] = list(current_email.get("cc", []))
                current_email["draft_cc"] = new_cc
                clear_recipient_edit_temp(current_email, "cc")
                safe_async_wait(graph.aupdate_state(config, {"email": current_email}))

            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": f"抄送人已保存（{len(new_cc)}人）"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 保存正文 (form submit)
        elif action_type in ("save_draft", "submit", "Button_submit", "form_submit_draft"):
            action_data = event.event.action
            form_values = getattr(action_data, 'form_value', None) or {}
            logger.info(f"Form submit: form_value={form_values}, value={action_data.value}")
            new_draft = form_values.get("draft_input", "")
            if new_draft:
                if str(email_id).startswith("test_push_"):
                     _mock_store[email_id].values["draft"] = new_draft
                process_modification(email_id, new_draft)
                logger.info(f"Draft updated to: {new_draft[:50]}...")
            else:
                new_draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, new_draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": "正文已保存"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 取消编辑（通用）
        elif action_type == "cancel_edit":
            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
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
            draft = state.values.get("draft", "")
            edit_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field="draft")
            return {
                "toast": {"type": "info", "content": "编辑正文"},
                "card": {"type": "raw", "data": edit_card}
            }

        elif action_type == "save_modification":
            form_values = event.event.action.form_value or {}
            new_draft = form_values.get("draft_input", "")
            process_modification(email_id, new_draft)
            view_card = card_builder.build_approval_card(email_id, new_draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": "修改已保存"},
                "card": {"type": "raw", "data": view_card}
            }

        elif action_type == "cancel_modification":
            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "info", "content": "已取消编辑"},
                "card": {"type": "raw", "data": view_card}
            }

    except Exception as e:
        logger.error(f"Error handling card action: {e}", exc_info=True)
        err_resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "error"
        toast.content = f"操作失败: {str(e)[:50]}"
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
                logger.error("Command reply send failed: %s - %s", resp.code, resp.msg)

        safe_async_run(_dispatch())
    except Exception as e:
        logger.error("Error handling message event: %s", e)

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
    config = {"configurable": {"thread_id": email_id}}
    state = safe_async_wait(graph.aget_state(config))

    final_draft = state.values.get("draft", "") if state and state.values else ""
    safe_async_wait(db_manager.update_status(
        email_id, "approved",
        approver_user_id=user_id,
        final_draft=final_draft,
    ))

    safe_async_wait(graph.aupdate_state(config, {"approval_status": "approved"}))
    logger.info(f"Approval processed for {email_id} by {user_id}. Executing graph...")
    
    att_tokens = _collect_cleanup_tokens(state)

    if att_tokens:
        logger.info(f"Cleaning up {len(att_tokens)} attachments...")
        for tok in att_tokens:
             safe_async_run(asyncio.to_thread(delete_file_from_drive, tok))

    safe_async_run(graph.ainvoke(None, config=config))

def process_rejection(email_id, user_id, reason: str = ""):
    config = {"configurable": {"thread_id": email_id}}
    safe_async_wait(graph.aupdate_state(config, {"approval_status": "rejected"}))
    kwargs = {"approver_user_id": user_id}
    if reason:
        kwargs["rejection_reason"] = reason
    safe_async_wait(db_manager.update_status(email_id, "rejected", **kwargs))
    logger.info(f"Rejection processed for {email_id} by {user_id} reason={reason}. Executing graph...")
    # Remove attachments (PDF + Originals)
    state = safe_async_wait(graph.aget_state(config))
    
    att_tokens = _collect_cleanup_tokens(state)

    if att_tokens:
        logger.info(f"Cleaning up {len(att_tokens)} attachments...")
        for tok in att_tokens:
             safe_async_run(asyncio.to_thread(delete_file_from_drive, tok))

    safe_async_run(graph.ainvoke(None, config=config))
    
def process_modification(email_id, new_draft):
    config = {"configurable": {"thread_id": email_id}}
    safe_async_wait(graph.aupdate_state(config, {
        "draft": new_draft, 
        "approval_status": "modify"
    }))
    safe_async_wait(db_manager.update_status(email_id, "modified"))
    logger.info(f"Modification saved for {email_id}. New draft length: {len(new_draft)}")

async def process_save_draft(email_id, state):
    try:
        draft = state.values.get("draft", "")
        email_data = state.values.get("email", {})
        
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
                except Exception as e:
                    logger.error(f"Error resolving open_id {open_id}: {e}")
                    return None
            
            # Extract from legacy format "name='...', email_address='...'" or just return string
            if "email_address='" in str(recipient_str):
                m = re.search(r"email_address='(.*?)'", str(recipient_str))
                if m: return m.group(1)
            
            return str(recipient_str)
    
        final_to = []
        raw_to = email_data.get("draft_to", [])
        if isinstance(raw_to, str): raw_to = [raw_to]
        for r in raw_to:
            resolved = resolve_recipient(r)
            if resolved: final_to.append(resolved)

        final_cc = []
        raw_cc = email_data.get("draft_cc", [])
        if isinstance(raw_cc, str): raw_cc = [raw_cc]
        for r in raw_cc:
            resolved = resolve_recipient(r)
            if resolved: final_cc.append(resolved)

        subject = "Re: " + email_data.get("subject", "")
        body = draft + "<br><br>--<br>AI Generated Draft"
        
        if exchange_client:
             logger.info(f"Saving draft for {email_id}. To: {final_to}, Cc: {final_cc}")
             await exchange_client.create_draft(final_to, subject, body, cc=final_cc)
        
        await db_manager.update_status(email_id, "draft_saved")

        # Remove attachments (PDF + Originals)
        att_tokens = _collect_cleanup_tokens(state)

        if att_tokens:
            logger.info(f"Cleaning up {len(att_tokens)} attachments (async)...")
            for tok in att_tokens:
                 await asyncio.to_thread(delete_file_from_drive, tok)

    except Exception as e:
        logger.error(f"Error in process_save_draft: {e}", exc_info=True)

async def generate_and_upload_pdf(email_id: str, email_data: dict) -> Optional[Dict[str, Any]]:
    """Render email -> PDF -> Lark Drive. Delegates to lark_pdf_flow."""
    from src.utils.lark_pdf_flow import generate_and_upload_pdf as _impl
    return await _impl(email_id, email_data, upload_fn=upload_file_to_drive)


async def process_pdf_generation_and_reply(email_id, state, message_id):
    """Generate PDF and reply with file link. Delegates to lark_pdf_flow."""
    from src.utils.lark_pdf_flow import process_pdf_generation_and_reply as _impl
    await _impl(
        email_id,
        state,
        message_id,
        graph=graph,
        lark_api_client=lark_api_client,
        upload_fn=upload_file_to_drive,
        safe_async_wait=safe_async_wait,
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
