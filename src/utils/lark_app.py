import os
import json
import logging
import asyncio
import re
import html
import io
from typing import Dict, Any, List, Optional
import lark_oapi
from lark_oapi.api.im.v1 import *
from lark_oapi.api.contact.v3 import *
from lark_oapi.ws import Client as WsClient
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse, CallBackCard, CallBackToast

from src.utils.card_builder import LarkCardBuilder, html_to_lark_md
from src.config import get_settings
from src.utils.email_renderer import render_email_html
from src.utils.pdf_generator import convert_html_to_pdf

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
    """
    Upload file to Lark Drive. Returns dict with 'file_token' and 'url'.
    Requires LARK_DRIVE_FOLDER_TOKEN in env.
    """
    if not lark_api_client:
        logger.warning("Lark Client not initialized.")
        return None
        
    settings = get_settings()
    folder_token = settings.LARK_DRIVE_FOLDER_TOKEN
    if not folder_token:
        logger.warning("Lark Drive Folder Token not configured (LARK_DRIVE_FOLDER_TOKEN). Skipping upload.")
        return None

    try:
        from lark_oapi.api.drive.v1 import UploadAllFileRequest, UploadAllFileRequestBody
        
        request = UploadAllFileRequest.builder() \
            .request_body(UploadAllFileRequestBody.builder()
                .file_name(name)
                .parent_type("explorer")
                .parent_node(folder_token)
                .size(size)
                .file(io.BytesIO(content))
                .build()) \
            .build()
            
        response = lark_api_client.drive.v1.file.upload_all(request)
        if not response.success():
            logger.error(f"Failed to upload file {name}: {response.code} - {response.msg}")
            return None
            
        data = response.data
        file_token = data.file_token
        # Construct URL manually as API doesn't return it
        # Format: https://www.feishu.cn/file/{file_token}
        url = getattr(data, "url", "")
        if not url and file_token:
            url = f"https://www.feishu.cn/file/{file_token}"
            
        logger.info(f"File uploaded. Token: {file_token}, Constructed URL: {url}")
            
        return {
            "file_token": file_token,
            "url": url
        }
    except Exception as e:
        logger.error(f"Exception uploading file {name}: {e}")
        return None

def delete_file_from_drive(file_token: str) -> bool:
    """
    Delete a file from Lark Drive (trash).
    """
    if not lark_api_client or not file_token:
        return False
        
    try:
        from lark_oapi.api.drive.v1 import DeleteFileRequest
        
        # Note: Drive v1 Delete puts file in trash
        request = DeleteFileRequest.builder() \
            .file_token(file_token) \
            .type("file") \
            .build()
            
        response = lark_api_client.drive.v1.file.delete(request)
        if not response.success():
            logger.warning(f"Failed to delete file {file_token}: {response.code} - {response.msg}")
            return False
            
        logger.info(f"File deleted (moved to trash): {file_token}")
        return True
    except Exception as e:
        logger.error(f"Exception deleting file {file_token}: {e}")
        return False

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
    app_secret = settings.LARK_APP_SECRET
    
    if app_id and app_secret:
        lark_api_client = lark_oapi.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark_oapi.LogLevel.INFO) \
            .build()
            
        # Optimize Identity Logic: Resolve "Me" from Chat ID
        if settings.LARK_CHAT_ID:
            _resolve_current_user_email(settings.LARK_CHAT_ID)
            
        card_builder = LarkCardBuilder(lark_api_client)
        logger.info("Lark API Client initialized.")
    else:
        card_builder = LarkCardBuilder(None)
        logger.warning("Lark App ID or Secret missing. Lark features disabled.")

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

def send_approval_card(email_id: str, draft: str, context: List[dict], email_data: dict, classification: dict, pdf_url: str = None):
    """
    Send an interactive card to the configured Lark group/user.
    """
    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send card.")
        return
    
    settings = get_settings()
    chat_id = settings.LARK_CHAT_ID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return

    card_content = card_builder.build_approval_card(email_id, draft, context, email_data, classification, pdf_url=pdf_url)
    
    request = CreateMessageRequest.builder() \
        .receive_id_type("chat_id") \
        .request_body(CreateMessageRequestBody.builder()
            .receive_id(chat_id)
            .msg_type("interactive")
            .content(json.dumps(card_content))
            .build()) \
        .build()

    response = lark_api_client.im.v1.message.create(request)
    if not response.success():
        logger.error(f"Failed to send Lark card: {response.code} - {response.msg}")
    else:
        logger.info(f"Lark card sent for email {email_id}. Msg ID: {response.data.message_id}")





# Event Handlers

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
            
            # 关键：直接在响应中返回新卡片，实现实时更新
            return {
                "toast": {
                    "type": "info",
                    "content": "已拒绝该拟稿"
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
        
        # 选择人员时的回调 - 直接更新卡片显示选中的人
        elif action_type == "select_to":
            action_data = event.event.action
            selected_uid = getattr(action_data, 'option', None)
            logger.info(f"select_to: selected={selected_uid}")
            if selected_uid:
                # 更新email_data中的to
                new_to = [f"open_id={selected_uid}"]
                email_data["to"] = new_to
                
                # PERSISTENCE
                if str(email_id).startswith("test_push_"):
                    # Update Mock Store
                    _mock_store[email_id].values["email"]["to"] = new_to
                else:
                    # Update Graph State
                    config = {"configurable": {"thread_id": email_id}}
                    # Merge deep update manually or just update whole email object
                    # Graph update usually merges top-level keys. We need to be careful.
                    # Assuming we can update 'email' key.
                    # Retrieve current email data first to avoid overwriting other fields (already done in 'state')
                    current_email = state.values.get("email", {}).copy()
                    current_email["to"] = new_to
                    safe_async_wait(graph.aupdate_state(config, {"email": current_email}))
                
            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": "收件人已更新"},
                "card": {"type": "raw", "data": view_card}
            }
        
        elif action_type == "select_cc":
            action_data = event.event.action
            selected_uid = getattr(action_data, 'option', None)
            logger.info(f"select_cc: selected={selected_uid}")
            if selected_uid:
                new_cc = [f"open_id={selected_uid}"]
                email_data["cc"] = new_cc
                
                 # PERSISTENCE
                if str(email_id).startswith("test_push_"):
                    # Update Mock Store
                    _mock_store[email_id].values["email"]["cc"] = new_cc
                else:
                    # Update Graph State
                    config = {"configurable": {"thread_id": email_id}}
                    current_email = state.values.get("email", {}).copy()
                    current_email["cc"] = new_cc
                    safe_async_wait(graph.aupdate_state(config, {"email": current_email}))

            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "success", "content": "抄送人已更新"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 保存收件人
        elif action_type == "save_to":
            action_data = event.event.action
            logger.info(f"save_to action data: value={action_data.value}, option={getattr(action_data, 'option', None)}, form_value={action_data.form_value}")
            # 目前无法持久化，仅返回原卡片
            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "info", "content": "暂不支持修改收件人（测试模式）"},
                "card": {"type": "raw", "data": view_card}
            }
        
        # 保存抄送人
        elif action_type == "save_cc":
            # select_person 的值可能在 option 或 form_value 中
            action_data = event.event.action
            logger.info(f"save_cc action data: value={action_data.value}, option={getattr(action_data, 'option', None)}, form_value={action_data.form_value}")
            # 目前无法持久化，仅返回原卡片
            draft = state.values.get("draft", "")
            view_card = card_builder.build_approval_card(email_id, draft, [], email_data, classification, edit_field=None)
            return {
                "toast": {"type": "info", "content": "暂不支持修改抄送人（测试模式）"},
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
    safe_async_wait(graph.aupdate_state(config, {"approval_status": "approved"}))
    safe_async_wait(db_manager.update_status(email_id, "approved"))
    logger.info(f"Approval processed for {email_id} by {user_id}. Executing graph...")
    # Execute graph
    # Remove attachment if exists
    state = safe_async_wait(graph.aget_state(config))
    pdf_token = state.values.get("pdf_token")
    if pdf_token:
        logger.info(f"Cleaning up PDF attachment: {pdf_token}")
        safe_async_run(asyncio.to_thread(delete_file_from_drive, pdf_token))

    safe_async_run(graph.ainvoke(None, config=config))

def process_rejection(email_id, user_id):
    config = {"configurable": {"thread_id": email_id}}
    safe_async_wait(graph.aupdate_state(config, {"approval_status": "rejected"}))
    safe_async_wait(db_manager.update_status(email_id, "rejected"))
    logger.info(f"Rejection processed for {email_id} by {user_id}. Executing graph...")
    # Remove attachment if exists
    state = safe_async_wait(graph.aget_state(config))
    pdf_token = state.values.get("pdf_token")
    if pdf_token:
        logger.info(f"Cleaning up PDF attachment: {pdf_token}")
        safe_async_run(asyncio.to_thread(delete_file_from_drive, pdf_token))

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
    draft = state.values.get("draft", "")
    email_data = state.values.get("email", {})
    to = email_data.get("sender")
    subject = "Re: " + email_data.get("subject", "")
    body = draft + "<br><br>--<br>AI Generated Draft"
    if exchange_client:
         await exchange_client.create_draft(str(to), subject, body)
    await db_manager.update_status(email_id, "draft_saved")

    # Remove attachment if exists
    pdf_token = state.values.get("pdf_token")
    if pdf_token:
        logger.info(f"Cleaning up PDF attachment: {pdf_token}")
        # Running in async context, but delete is sync (using client), wrapping in thread or simple call if safe
        # delete_file_from_drive uses lark_api_client which is thread safe for HTTP calls
        delete_file_from_drive(pdf_token)

async def generate_and_upload_pdf(email_id: str, email_data: dict) -> Optional[str]:
    """
    Generate PDF and upload to Lark Drive. Returns dict with url and token if successful.
    """
    try:
        logger.info(f"Starting PDF generation for {email_id}")
        
        # 1. Render HTML (CPU bound)
        loop = asyncio.get_running_loop()
        html_content = await loop.run_in_executor(None, render_email_html, email_data)
        
        # 2. Convert to PDF (CPU/IO bound)
        pdf_bytes = await loop.run_in_executor(None, convert_html_to_pdf, html_content)
        
        if not pdf_bytes:
            logger.error("PDF generation returned empty bytes.")
            return None

        # 3. Upload to Drive (Network bound)
        filename = f"Email_Export_{email_id}.pdf"
        upload_resp = await loop.run_in_executor(None, upload_file_to_drive, filename, pdf_bytes, len(pdf_bytes))
        
        if not upload_resp:
            logger.error("PDF Upload failed.")
            return None

        file_url = upload_resp["url"]
        file_token = upload_resp["file_token"]
        logger.info(f"PDF Uploaded: {file_url} (Token: {file_token})")
        return {"url": file_url, "file_token": file_token}
        
    except Exception as e:
        logger.error(f"Error in generate_and_upload_pdf: {e}", exc_info=True)
        return None

async def process_pdf_generation_and_reply(email_id, state, message_id):
    """
    Generate PDF and reply with file link. (Deprecated Action Handler)
    """
    try:
        email_data = state.values.get("email", {})
        result = await generate_and_upload_pdf(email_id, email_data)
        
        if not result:
            return

        file_url = result["url"]
        file_token = result["file_token"]

        # Store token in state for later cleanup
        config = {"configurable": {"thread_id": email_id}}
        safe_async_wait(graph.aupdate_state(config, {"pdf_token": file_token}))

        filename = f"Email_Export_{email_id}.pdf"

        # 4. Reply with Card
        card_content = {
            "header": {
                "template": "blue",
                "title": {"content": "📄 PDF 原文已生成", "tag": "plain_text"}
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"点击下方按钮查看 PDF 文件：\\nFilename: *{filename}*"}
                },
                {
                    "tag": "action",
                    "actions": [{
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "📂 打开 PDF"},
                        "type": "primary",
                        "url": file_url
                    }]
                }
            ]
        }
        
        req_msg = ReplyMessageRequest.builder() \
            .message_id(message_id) \
            .request_body(ReplyMessageRequestBody.builder() \
                .msg_type("interactive") \
                .content(json.dumps(card_content)) \
                .build()) \
            .build()
        
        lark_api_client.im.v1.message.reply(req_msg)
        logger.info("PDF Reply sent successfully.")

    except Exception as e:
        logger.error(f"Error in PDF generation process: {e}", exc_info=True)

        # 4. Reply with Card
        card_content = {
            "header": {
                "template": "blue",
                "title": {"content": "📄 PDF 原文已生成", "tag": "plain_text"}
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"点击下方按钮查看 PDF 文件：\nFilename: *{filename}*"}
                },
                {
                    "tag": "action",
                    "actions": [{
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "📂 打开 PDF"},
                        "type": "primary",
                        "url": file_url
                    }]
                }
            ]
        }
        
        req_msg = ReplyMessageRequest.builder() \
            .message_id(message_id) \
            .request_body(ReplyMessageRequestBody.builder() \
                .msg_type("interactive") \
                .content(json.dumps(card_content)) \
                .build()) \
            .build()
        
        lark_api_client.im.v1.message.reply(req_msg)
        logger.info("PDF Reply sent successfully.")

    except Exception as e:
        logger.error(f"Error in PDF generation process: {e}", exc_info=True)


def start_lark_ws():
    """
    Start WebSocket Client in a background thread
    """
    settings = get_settings()
    app_id = settings.LARK_APP_ID
    app_secret = settings.LARK_APP_SECRET
    
    if not (app_id and app_secret):
        logger.warning("Lark App ID/Secret missing. WS Client not started.")
        return

    global lark_ws_client

    event_handler = lark_oapi.EventDispatcherHandler.builder("", "") \
        .register_p2_card_action_trigger(handle_card_action) \
        .build()

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
