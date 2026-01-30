import os
import json
import logging
import asyncio
import re
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import lark_oapi
from lark_oapi.api.im.v1 import *
from lark_oapi.api.contact.v3 import * # Import Contact API
from lark_oapi.ws import Client as WsClient
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse, CallBackCard, CallBackToast

# Initialize Logger
logger = logging.getLogger(__name__)

# Global instances
lark_ws_client: Optional[WsClient] = None
lark_api_client: Optional[lark_oapi.Client] = None
db_manager = None
graph = None
exchange_client = None
worker_loop = None # Added for thread-safe async execution

def init_lark_app(db_mgr, graph_instance, ex_client, worker_loop_arg=None):
    """
    Initialize global dependencies
    """
    global db_manager, graph, exchange_client, lark_api_client, worker_loop
    db_manager = db_mgr
    graph = graph_instance
    exchange_client = ex_client
    if worker_loop_arg:
        worker_loop = worker_loop_arg
    db_manager = db_mgr
    graph = graph_instance
    exchange_client = ex_client
    
    app_id = os.environ.get("LARK_APP_ID")
    app_secret = os.environ.get("LARK_APP_SECRET")
    
    if app_id and app_secret:
        lark_api_client = lark_oapi.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark_oapi.LogLevel.DEBUG) \
            .build()
        logger.info("Lark API Client initialized with DEBUG level.")
    else:
        logger.warning("Lark App ID or Secret missing. Lark features disabled.")

def send_approval_card(email_id: str, draft: str, context: List[dict], email_data: dict, classification: dict):
    """
    Send an interactive card to the configured Lark group/user.
    """
    if not lark_api_client:
        logger.error("Lark Client not initialized. Cannot send card.")
        return

    chat_id = os.environ.get("LARK_CHAT_ID") # Target Group or User OpenID
    if not chat_id:
        logger.error("LARK_CHAT_ID not configured.")
        return

    card_content = build_approval_card(email_id, draft, context, email_data, classification)
    
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

def html_to_lark_md(html_str):
    """
    Convert HTML to Lark Markdown using BeautifulSoup.
    Matches the logic from Slack app but adapted for Lark.
    """
    if not html_str: return ""
    try:
        soup = BeautifulSoup(html_str, "html.parser")
        
        # 1. Handle Links [text](url)
        for a in soup.find_all("a"):
            href = a.get("href")
            text = a.get_text(strip=True)
            if href and not href.startswith("data:"):
                a.replace_with(f"[{text}]({href})")
            else:
                a.replace_with(text)
        
        # 2. Handle Formatting & Headers
        # Headers -> Bold
        for h in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
             text = h.get_text(strip=True)
             if text:
                 h.replace_with(f"\n**{text}**\n")

        # Basic Formatting
        for b in soup.find_all(["b", "strong"]):
            b.replace_with(f"**{b.get_text(strip=True)}**")
        for i in soup.find_all(["i", "em"]):
            i.replace_with(f"*{i.get_text(strip=True)}*")
        for s in soup.find_all(["s", "strike", "del"]):
            s.replace_with(f"~~{s.get_text(strip=True)}~~")
        for c in soup.find_all("code"):
            c.replace_with(f"`{c.get_text(strip=True)}`")
        for q in soup.find_all("blockquote"):
            q.replace_with(f"> {q.get_text(strip=True)}")
            
        # 3. Lists
        for ul in soup.find_all("ul"):
            for li in ul.find_all("li", recursive=False):
                li.prefix = "• "
                li.replace_with(f"• {li.get_text(strip=True)}\n")
            ul.unwrap() # Remove the ul tag but keep content
            
        for ol in soup.find_all("ol"):
            index = 1
            for li in ol.find_all("li", recursive=False):
                li.replace_with(f"{index}. {li.get_text(strip=True)}\n")
                index += 1
            ol.unwrap()

        # 4. Images
        for img in soup.find_all("img"):
            alt = img.get("alt", "图片")
            if len(alt) > 20 or "cid:" in alt: alt = "图片"
            img.replace_with(f" [🖼️ {alt}] ")

        # 5. Tables - Basic Representation
        for table in soup.find_all("table"):
            # A very simple text representation of table
            rows = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                rows.append(" | ".join(cells))
            table_text = "\n".join(rows)
            table.replace_with(f"\n{table_text}\n")

        # 6. Layout Cleanup
        for tag in soup.find_all(["p", "div", "br"]):
            tag.append("\n") 

        # 7. Extract Text
        text = soup.get_text()
        text = re.sub(r'\n{3,}', '\n\n', text) # Normalize duplicate newlines
        return text.strip()
    except Exception as e:
        return f"Markdown解析出错: {e}"

def build_approval_card(email_id: str, draft: str, context: List[dict], email_data: dict, classification: dict, is_edit_mode: bool = False, feedback_value: str = "") -> dict:
    """
    Constructs the Lark Card JSON.
    """
    subject = email_data.get("subject", "No Subject")
    # Clean subject
    subject = re.sub(r"^(Subject|主题)[:：]\s*", "", subject, flags=re.IGNORECASE).strip()

    raw_sender = email_data.get("sender", "Unknown")
    received_at = email_data.get("received_at", "Unknown Time")
    
    # Parse Sender
    sender = str(raw_sender)
    match = re.search(r"name='(.*?)', email_address='(.*?)'", sender)
    sender_display = sender
    if match:
        name, email = match.groups()
        sender_display = f"{name}"
    else:
        if 'email_address=' in sender:
             m2 = re.search(r"email_address='([^']*)'", sender)
             if m2: sender_display = m2.group(1)

    # Recipients (Mock or Parse)
    to_list = email_data.get("to", [])
    if isinstance(to_list, str): to_list = [to_list]
    to_str = ", ".join([str(t).split("'")[1] if "name='" in str(t) else str(t) for t in to_list[:2]])
    
    cc_list = email_data.get("cc", [])
    if isinstance(cc_list, str): cc_list = [cc_list]
    cc_str = ", ".join([str(c).split("'")[1] if "name='" in str(c) else str(c) for c in cc_list[:2]])
    if not cc_str: cc_str = "无"

    # Context (Original Email Snippet)
    original_snippet = "无内容摘要"
    if context:
        # We try to get raw text of the *first* chunk which usually is the latest email body
        text = context[0].get('chunk_text') or context[0].get('body') or ""
        text = text.strip()
        original_snippet = text[:150] + "..." if len(text) > 150 else text

    reason = classification.get("reasoning", "智能生成")
    
    # --- Card Structure (Premium UI) ---
    
    # --- Helper: User Lookup ---
    def lookup_lark_users(emails: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Lookup Lark User IDs (Open ID) by assuming Email Prefix = User ID.
        Returns map: {email -> {'open_id': xxx, 'name': xxx}}
        """
        if not emails or not lark_api_client: return {}
        
        email_map = {}
        logger.info(f"Looking up Lark users via UserID (Prefix) strategy for: {emails}")

        for email in emails:
            try:
                # Strategy: Extract "q-fu" from "q-fu@..."
                user_id_input = email.split("@")[0]
                
                # Fetch User by User ID
                req = GetUserRequest.builder() \
                    .user_id(user_id_input) \
                    .user_id_type("user_id") \
                    .build()

                resp = lark_api_client.contact.v3.user.get(req)
                
                if not resp.success():
                    # Common error: User not found (e.g. valid email but prefix != user_id, or permission)
                    logger.warning(f"Lookup failed for user_id='{user_id_input}' (email: {email}): {resp.code}")
                    continue

                if resp.data and resp.data.user:
                     found_open_id = resp.data.user.open_id
                     found_name = resp.data.user.name
                     if found_open_id:
                         logger.info(f"Resolved {email} -> {found_name} ({found_open_id})")
                         email_map[email] = {'open_id': found_open_id, 'name': found_name}
                
            except Exception as e:
                logger.error(f"Error resolving user {email}: {e}")
            
        return email_map

    # Collect all emails for lookup
    all_emails = []
    
    def extract_email(s):
        m = re.search(r"email_address='(.*?)'", str(s))
        if m: return m.group(1)
        # Fallback for "Name <email>" format if present
        m2 = re.search(r"<([^>]+)>", str(s))
        if m2: return m2.group(1)
        return None

    # Sender
    sender_email = extract_email(raw_sender)
    if sender_email: all_emails.append(sender_email)
    
    # Recipients
    for r in email_data.get("to", []):
        e = extract_email(r)
        if e: all_emails.append(e)
        
    for c in email_data.get("cc", []):
         e = extract_email(c)
         if e: all_emails.append(e)
         
    # Perform Lookup
    user_map = lookup_lark_users(list(set(all_emails)))
    
    # --- Helper: Name Formatting (Updated) ---
    def format_recipients(recipient_list, show_email=False, limit=3):
        """
        Format recipients list.
        If user is found -> [Name](lark_profile_link) (Clean Link)
        Else -> "Name (Email)"
        """
        if not recipient_list: return "无"
        
        formatted_items = []
        for r in recipient_list:
            r_str = str(r)
            # Parse "name='X', email_address='Y'"
            name = r_str
            email = ""
            
            m_name = re.search(r"name='(.*?)'", r_str)
            if m_name: name = m_name.group(1)
            
            m_email = re.search(r"email_address='(.*?)'", r_str)
            if m_email: email = m_email.group(1)
            
            # 1. Try Lark Profile Link
            if email and email in user_map:
                u_info = user_map[email]
                lark_id = u_info['open_id']
                real_name = u_info['name']
                lark_id = u_info['open_id']
                real_name = u_info['name']
                # Clean Markdown Link to Profile
                # Try 'feishu://' scheme which might be registered to the app on desktop
                formatted_items.append(f"[{real_name}](feishu://applink.feishu.cn/client/contact/open?openId={lark_id})")
            # 2. Fallback
            elif show_email and email:
                formatted_items.append(f"{name} ({email})")
            else:
                formatted_items.append(name)
                
        if len(formatted_items) > limit:
            remaining = len(formatted_items) - limit
            # Note: Mentions in "collapsed" text might count as characters, but for safety we just append
            return ", ".join(formatted_items[:limit]) + f" 等{len(formatted_items)}人"
        return ", ".join(formatted_items)

    # Prepare Display Strings
    # 1. Incoming Context (Name Only)
    from_display_simple = format_recipients([raw_sender], show_email=False, limit=1)
    to_display_simple = format_recipients(email_data.get("to", []), show_email=False, limit=5)
    cc_display_simple = format_recipients(email_data.get("cc", []), show_email=False, limit=5)
    
    # 2. Outgoing Context (Name + Email for safety)
    # Usually we reply to the Sender, but if Reply-To exists or standard reply logic...
    # For this display we assume Reply To -> Sender, and Reply All -> Sender + CC (excluding self)
    # To keep it simple for the user request: "Reply To" info
    reply_to_display = format_recipients([raw_sender], show_email=True, limit=3)
    
    # --- Card Structure (Refined Flow) ---
    
    elements = []
    
    # Header
    header = {
        "template": "blue",
        "title": {
            "content": f"� 拟稿审批: {subject}",
            "tag": "plain_text"
        }
    }
    
    # BLOCK 1: INCOMING CONTEXT (History)
    # 1.1 AI Reasoning (Top Note)
    elements.append({
        "tag": "note",
        "elements": [
             {"tag": "plain_text", "content": f"💡 AI 处理说明: {reason}"}
        ]
    })
    
    # --- Helper: User Element Builder ---
    def build_user_row(label, recipients_list, email_data_key):
        """
        Builds a row (ColumnSet) for a user field.
        Horizontal Layout: Label | User1 | User2 | User3 | More...
        """
        # 1. Extract and Lookup
        raw_list = recipients_list if isinstance(recipients_list, list) else [recipients_list]
        
        matched_ids = []
        leftover_text = [] # Names of people not found in Lark
        
        for r in raw_list:
            e = extract_email(r)
            
            # Name Fallback
            name = str(r)
            m_name = re.search(r"name='(.*?)'", str(r))
            if m_name: name = m_name.group(1)
            elif e: name = e.split("@")[0]
            
            if e and e in user_map:
                 # Found User
                 matched_ids.append(user_map[e]['open_id'])
            else:
                 # Not Found - Add to leftover
                 leftover_text.append(name)
        
        # 2. Build Columns
        columns = []
        
        # Col 1: Label
        columns.append({
            "tag": "column",
            "width": "auto", # Shrink to fit text
            "vertical_align": "center",
            "elements": [{
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**{label}**"}
            }]
        })
        
        # Col 2+: Users (Horizontal)
        if matched_ids:
            display_limit = 5
            for uid in matched_ids[:display_limit]:
                 columns.append({
                    "tag": "column",
                    "width": "auto", 
                    "vertical_align": "center",
                    "elements": [{
                        "tag": "person", 
                        "user_id": uid,
                        "style": "normal" 
                    }]
                 })
            
            # Overflow
            overflow_text_parts = []
            
            # Internal overflow
            if len(matched_ids) > display_limit:
                 overflow_text_parts.append(f"+{len(matched_ids) - display_limit}")
            
            # External overflow (Leftover names)
            if leftover_text:
                 # Just show count if too many, or first name?
                 # User wants to know who.
                 if len(leftover_text) == 1:
                     overflow_text_parts.append(leftover_text[0])
                 else:
                     overflow_text_parts.append(f"+{len(leftover_text)}外部")
            
            if overflow_text_parts:
                 final_overflow = " ".join(overflow_text_parts)
                 columns.append({
                    "tag": "column",
                    "width": "auto",
                    "vertical_align": "center",
                    "elements": [{
                        "tag": "div",
                        "text": {"tag": "plain_text", "content": final_overflow}
                    }]
                 })
                 
        else:
             # No internal users found, just show text 
             display_text = format_recipients(raw_list, show_email=False, limit=3)
             columns.append({
                "tag": "column",
                "width": "auto",
                "vertical_align": "center",
                "elements": [{
                     "tag": "div",
                     "text": {"tag": "lark_md", "content": display_text}
                }]
             })
             
        # SPACER COLUMN (Crucial for Left Alignment)
        # This eats up all remaining space, pushing previous columns to the left.
        columns.append({
            "tag": "column",
            "width": "weighted",
            "weight": 1,
            "elements": [] # Empty
        })

        return {
            "tag": "column_set",
            "flex_mode": "none",
            "background_style": "default",
            "horizontal_spacing": "small",
            "columns": columns
        }

    # 1.2 Incoming Metadata (Left-Aligned Compact Block)
    # Layout: [Sender] | [Recipient] ... [Spacer]
    
    compact_columns = []
    
    # --- LEFT GROUP (Sender) ---
    compact_columns.append({
        "tag": "column",
        "width": "auto",
        "vertical_align": "center",
        "elements": [{
            "tag": "div", 
            "text": {"tag": "lark_md", "content": "**👤 发件人:**"}
        }]
    })
    
    # Sender Avatar
    sender_uid = None
    sender_email = extract_email(raw_sender)
    if sender_email and sender_email in user_map:
        sender_uid = user_map[sender_email]['open_id']
        
    if sender_uid:
        compact_columns.append({
            "tag": "column",
            "width": "auto",
            "vertical_align": "center",
            "elements": [{"tag": "person", "user_id": sender_uid, "style": "normal"}]
        })
    else:
        s_name = sender_email.split("@")[0] if sender_email else "Unknown"
        compact_columns.append({
            "tag": "column",
            "width": "auto",
            "vertical_align": "center",
            "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": s_name}}]
        })

    # --- DIVIDER ---
    compact_columns.append({
        "tag": "column",
        "width": "auto",
        "vertical_align": "center",
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "<font color='lightgrey'>&nbsp;&nbsp;|&nbsp;&nbsp;</font>"}}]
    })

    # --- RIGHT GROUP (Recipient) ---
    # Recipient Label
    label_text = "**👥 收件人:**"
    target_list = email_data.get("to", [])
    if not target_list and email_data.get("cc"):
        label_text = "**👀 抄送人:**"
        target_list = email_data.get("cc", [])

    compact_columns.append({
        "tag": "column",
        "width": "auto",
        "vertical_align": "center",
        "elements": [{
            "tag": "div",
            "text": {"tag": "lark_md", "content": label_text}
        }]
    })

    # Recipient Avatar (Max 1)
    recip_matched = []
    recip_leftover = []
    
    for r in target_list:
        e = extract_email(r)
        name = str(r)
        if e: name = e.split("@")[0]
        if e and e in user_map: recip_matched.append(user_map[e]['open_id'])
        else: recip_leftover.append(name)
            
    display_limit = 1
    for uid in recip_matched[:display_limit]:
        compact_columns.append({
            "tag": "column",
            "width": "auto",
            "vertical_align": "center",
            "elements": [{"tag": "person", "user_id": uid, "style": "normal"}]
        })
        
    # Overflow
    overflow_txt = ""
    count_others = (len(recip_matched) - display_limit if len(recip_matched) > display_limit else 0) + len(recip_leftover)
    if count_others > 0: overflow_txt += f" +{count_others}"
         
    if overflow_txt:
        compact_columns.append({
            "tag": "column",
            "width": "auto",
            "vertical_align": "center",
            "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": overflow_txt}}]
        })

    # --- SPACER (Pushes everything upstream to the Left) ---
    compact_columns.append({
        "tag": "column",
        "width": "weighted",
        "weight": 1,
        "elements": []
    })

    elements.append({
        "tag": "column_set",
        "flex_mode": "none",
        "background_style": "default",
        "horizontal_spacing": "small", # Keep elements tight within groups
        "columns": compact_columns
    })

    elements.append({"tag": "hr"})

    # BLOCK 2: ORIGINAL EMAIL CONTENT
    elements.append({
         "tag": "markdown",
         "content": f"**📄 原始邮件摘要:**"
    })
    elements.append({
         "tag": "div",
         "text": {
             "tag": "lark_md", 
             "content": f"*{original_snippet}*"
         }
    })
    
    # Attachments
    attachments = email_data.get("attachments", [])
    if attachments:
        att_text = ""
        for att in attachments[:3]:
             fname = att.get("name", "Unknown File")
             att_text += f"📎 {fname}\n"
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": att_text.strip()}
        })

    # View Original Button
    elements.append({
        "tag": "action",
        "actions": [
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "👀 查看完整原文 (HTML)"},
                "type": "default",
                "value": {"action": "view_original", "id": email_id}
            }
        ]
    })
    
    elements.append({"tag": "hr"})

    # BLOCK 3: DRAFT CONTENT
    # 3.0 Draft Header
    elements.append({
         "tag": "markdown",
          "content": f"**✍️ 拟定回复:**"
    })
    
    # 3.1 Reply Metadata (Now using Person Elements!)
    # User Request: Remove "From" as it is redundant (always me)
    
    if is_edit_mode:
         # EDIT MODE: Show Person Select Inputs
         
         # Note: We need initial selected IDs. 
         # reusing 'user_map' from earlier scope to find open_ids for current recipients
         
         # Helper to get OpenIDs from email list
         def get_open_ids(email_list):
             ids = []
             for item in email_list:
                 e = extract_email(item)
                 if e and e in user_map:
                     ids.append(user_map[e]['open_id'])
             return ids

         current_to_ids = get_open_ids([raw_sender]) # Reply To = Sender of original
         # Wait, Reply To is usually the 'Reply-To' header or the Sender.
         
         current_cc_ids = get_open_ids(cc_list)
         
         # To Selector (Label + Input in Action Block)
         elements.append({
             "tag": "div",
             "text": {"tag": "lark_md", "content": "**📥 收件人 (To):**"}
         })
         # To Selector (Label + Input in Action Block)
         elements.append({
             "tag": "div",
             "text": {"tag": "lark_md", "content": "**📥 收件人 (To):**"}
         })
         elements.append({
             "tag": "action",
             "actions": [{
                 "tag": "select_person",
                 "placeholder": {"tag": "plain_text", "content": "选择收件人"},
                 "initial_value": current_to_ids[0] if current_to_ids else "",
                 # "multi_select": True # Try to enable multi if supported, otherwise single
                 "value": {"key": "reply_to_ids"} 
             }]
         })
         
         # Cc Selector
         elements.append({
             "tag": "div",
             "text": {"tag": "lark_md", "content": "**👀 抄送人 (Cc):**"}
         })
         elements.append({
             "tag": "action",
             "actions": [{
                 "tag": "select_person",
                 "placeholder": {"tag": "plain_text", "content": "选择抄送人"},
                 "initial_value": current_cc_ids[0] if current_cc_ids else "",
                 "value": {"key": "reply_cc_ids"}
             }]
         })
         
    else:
        # VIEW MODE: Text/Avatar Display
        
        # To
        elements.append(build_user_row("📥 收件人:", [raw_sender], "reply_to"))
        
        # Cc
        if cc_list:
              elements.append(build_user_row("👀 抄送人:", cc_list, "reply_cc"))
          
    elements.append({"tag": "hr"})

    # 3.2 Draft Body
    if is_edit_mode:
        elements.append({
             "tag": "input",
             "name": "draft_input",
             "placeholder": {"tag": "plain_text", "content": "可以直接在此处编辑回复内容..."},
             "default_value": feedback_value or draft,
             "multi_line": True
        })
        elements.append({
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "💾 保存并提交"},
                    "type": "primary",
                    "value": {"action": "save_modification", "id": email_id}
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "� 取消编辑"},
                    "type": "default",
                    "value": {"action": "cancel_modification", "id": email_id} 
                }
            ]
        })
    else:
        # View Mode
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"{draft}"
            }
        })
        
    elements.append({"tag": "hr"})

    # BLOCK 4: PRIMARY ACTIONS
    elements.append({
        "tag": "action",
        "actions": [
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "✅ 批准发送"},
                "type": "primary", 
                "value": {"action": "approve", "id": email_id}
            },
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "✏️ 编辑回复"},
                "type": "default",
                "value": {"action": "modify", "id": email_id}
            },
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "💾 存为草稿"},
                "type": "default",
                "value": {"action": "save_draft_only", "id": email_id}
            },
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "🛑 拒绝"},
                "type": "danger",
                "value": {"action": "reject", "id": email_id}
            }
        ]
    })

    card = {
        "header": header,
        "elements": elements
    }
    return card

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

def get_processed_card(status_text, original_subject=""):
    """
    Returns a collapsed card for processed state.
    """
    return {
        "header": {
            "title": {"content": f"{status_text} | {original_subject}", "tag": "plain_text"},
            "template": "grey"
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": "✅ 已处理完成，无需继续操作。"}
            }
        ]
    }

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
        if (not state or not state.values) and str(email_id).startswith("test_push_"):
             logger.info(f"Injecting MOCK STATE for test card: {email_id}")
             # Create a mock state object compatible with the code below
             class MockState:
                 def __init__(self):
                     self.values = {
                         "draft": "Thank you, I have received the update.",
                         "email": {
                             "subject": "TEST: Complex Email Rendering",
                             "to": ["name='Jarod', email_address='q-fu@tianjin-air.com'"],
                             "cc": ["name='Jarod-CC', email_address='q-fu@tianjin-air.com'"],
                             "attachments": [{"name": "itinerary.pdf"}, {"name": "invoice_scan.jpg"}]
                         },
                         "classification": {"reasoning": "Test Notification"}
                     }
             state = MockState()

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
            logger.info("Executing Request: View Original (File Strategy)")
            full_body_html = email_data.get('body', '')
            subject_safe = re.sub(r'[\\/*?:"<>|]', "", subject)
            filename = f"Original_{subject_safe[:30]}.html"
            
            # Create Temp File
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w+', suffix=".html", delete=False, encoding='utf-8') as tmp:
                tmp.write(full_body_html)
                tmp_path = tmp.name
            
            try:
                # 1. Upload File
                logger.info(f"Uploading file: {tmp_path}")
                file_key = None
                with open(tmp_path, "rb") as f:
                    # Note: Lark OAPI 'create' usually takes a tuple/file-like object
                    # We use the im.v1.file.create endpoint logic
                    req_file = lark_oapi.api.im.v1.model.CreateFileRequest.builder() \
                        .request_body(lark_oapi.api.im.v1.model.CreateFileRequestBody.builder()
                            .file_type("stream")
                            .file_name(filename)
                            .file(f)
                            .build()) \
                        .build()
                    
                    resp_file = lark_api_client.im.v1.file.create(req_file)
                    
                    if not resp_file.success():
                        logger.error(f"Failed to upload file: {resp_file.code} - {resp_file.msg}")
                        raise Exception(f"File upload failed: {resp_file.msg}")
                    
                    file_key = resp_file.data.file_key
                    logger.info(f"File uploaded. Key: {file_key}")

                # 2. Send File Message
                if file_key:
                    content = {"file_key": file_key}
                    req_msg = CreateMessageRequest.builder() \
                        .receive_id_type("open_id") \
                        .request_body(CreateMessageRequestBody.builder()
                            .receive_id(user_id)
                            .msg_type("file")
                            .content(json.dumps(content))
                            .build()) \
                        .build()
                    
                    lark_api_client.im.v1.message.create(req_msg)

                    toast = CallBackToast()
                    toast.type = "success"
                    toast.content = "原文已作为文件发送"
                    response.toast = toast
                    return response
            except Exception as e:
                logger.error(f"Error sending file: {e}")
                raise e
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        elif action_type == "approve":
            process_approval(email_id, user_id)
            new_card = get_processed_card(f"已批准", subject)
            # Strategy: Explicit Patch via API
            update_card_ui(message_id, new_card)
            
            toast = CallBackToast()
            toast.type = "success"
            toast.content = "审批请求已提交"
            response.toast = toast
            return response
            
        elif action_type == "reject":
            process_rejection(email_id, user_id)
            new_card = get_processed_card(f"已拒绝", subject)
            update_card_ui(message_id, new_card)
            
            toast = CallBackToast()
            toast.type = "info"
            toast.content = "已拒绝该拟稿"
            response.toast = toast
            return response
            
        elif action_type == "modify":
            draft = state.values.get("draft", "")
            edit_card = build_approval_card(email_id, draft, [], email_data, classification, is_edit_mode=True)
            
            # STRATEGY: Synchronous Update via Return
            # We return the card content directly. This prevents the "Flash Revert".
            # Now that the 'select_person' tag is fixed, this should render correctly.
            
            return {
                "toast": {
                    "type": "info",
                    "content": "已进入编辑模式"
                },
                "card": edit_card
            }

        elif action_type == "save_draft_only":
            logger.info("Executing Request: Save Draft")
            safe_async_run(process_save_draft(email_id, state))
            new_card = get_processed_card(f"已存草稿", subject)
            update_card_ui(message_id, new_card)
            
            toast = CallBackToast()
            toast.type = "success"
            toast.content = "草稿已保存"
            response.toast = toast
            return response

        elif action_type == "save_modification":
            form_values = event.event.action.form_value or {}
            new_draft = form_values.get("draft_input", "")
            process_modification(email_id, new_draft)
            view_card = build_approval_card(email_id, new_draft, [], email_data, classification, is_edit_mode=False)
            update_card_ui(message_id, view_card)
            
            toast = CallBackToast()
            toast.type = "success"
            toast.content = "修改已保存"
            response.toast = toast
            return response

        elif action_type == "cancel_modification":
             draft = state.values.get("draft", "")
             view_card = build_approval_card(email_id, draft, [], email_data, classification, is_edit_mode=False)
             update_card_ui(message_id, view_card)
             return response

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
    db_manager.update_status(email_id, "approved")
    logger.info(f"Approval processed for {email_id} by {user_id}. Executing graph...")
    # Execute graph
    safe_async_run(graph.ainvoke(None, config=config))

def process_rejection(email_id, user_id):
    config = {"configurable": {"thread_id": email_id}}
    safe_async_wait(graph.aupdate_state(config, {"approval_status": "rejected"}))
    db_manager.update_status(email_id, "rejected")
    logger.info(f"Rejection processed for {email_id} by {user_id}. Executing graph...")
    safe_async_run(graph.ainvoke(None, config=config))
    
def process_modification(email_id, new_draft):
    config = {"configurable": {"thread_id": email_id}}
    safe_async_wait(graph.aupdate_state(config, {
        "draft": new_draft, 
        "approval_status": "modify"
    }))
    db_manager.update_status(email_id, "modified")
    logger.info(f"Modification saved for {email_id}. New draft length: {len(new_draft)}")

async def process_save_draft(email_id, state):
    draft = state.values.get("draft", "")
    email_data = state.values.get("email", {})
    to = email_data.get("sender")
    subject = "Re: " + email_data.get("subject", "")
    body = draft + "<br><br>--<br>AI Generated Draft"
    if exchange_client:
         await exchange_client.create_draft(str(to), subject, body)
    db_manager.update_status(email_id, "draft_saved")

def start_lark_ws():
    """
    Start WebSocket Client in a background thread
    """
    app_id = os.environ.get("LARK_APP_ID")
    app_secret = os.environ.get("LARK_APP_SECRET")
    
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
