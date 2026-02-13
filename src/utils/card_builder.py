"""
Lark Card Builder - 飞书卡片构建模块
从 lark_app.py 拆分，负责卡片 JSON 结构的构建逻辑。
"""
import os
import re
import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote
from src.config import get_settings

logger = logging.getLogger(__name__)


def html_to_lark_md(html_str: str) -> str:
    """
    Convert HTML to Lark Markdown using BeautifulSoup.
    """
    if not html_str:
        return ""
    try:
        soup = BeautifulSoup(html_str, "html.parser")

        for a in soup.find_all("a"):
            href = a.get("href")
            text = a.get_text(strip=True)
            if href and not href.startswith("data:"):
                a.replace_with(f"[{text}]({href})")
            else:
                a.replace_with(text)

        for h in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            text = h.get_text(strip=True)
            if text:
                h.replace_with(f"\n**{text}**\n")

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

        for ul in soup.find_all("ul"):
            for li in ul.find_all("li", recursive=False):
                li.prefix = "• "
                li.replace_with(f"• {li.get_text(strip=True)}\n")
            ul.unwrap()

        for ol in soup.find_all("ol"):
            index = 1
            for li in ol.find_all("li", recursive=False):
                li.replace_with(f"{index}. {li.get_text(strip=True)}\n")
                index += 1
            ol.unwrap()

        for img in soup.find_all("img"):
            alt = img.get("alt", "图片")
            if len(alt) > 20 or "cid:" in alt:
                alt = "图片"
            img.replace_with(f" [🖼️ {alt}] ")

        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                rows.append(" | ".join(cells))
            table_text = "\n".join(rows)
            table.replace_with(f"\n{table_text}\n")

        for tag in soup.find_all(["p", "div", "br"]):
            tag.append("\n")

        text = soup.get_text()
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
    except Exception as e:
        return f"Markdown解析出错: {e}"


def extract_email_address(raw: str) -> Optional[str]:
    """从各种格式中提取邮箱地址"""
    raw_str = str(raw).strip()
    
    # Format 1: name='张霞', email_address='zhang-xia@tianjin-air.com'
    m = re.search(r"email_address='(.*?)'", raw_str)
    if m:
        return m.group(1)
    
    # Format 2: 张霞 <zhang-xia@tianjin-air.com>
    m2 = re.search(r"<([^>]+)>", raw_str)
    if m2:
        return m2.group(1)
    
    # Format 3: Pure email (zhang-xia@tianjin-air.com)
    if '@' in raw_str and ' ' not in raw_str:
        return raw_str
    
    return None


class LarkCardBuilder:
    """飞书卡片构建器"""

    def __init__(self, lark_api_client=None, exchange_client=None):
        self.lark_api_client = lark_api_client
        self.exchange_client = exchange_client
        self._user_cache: Dict[str, Dict[str, str]] = {}

    @staticmethod
    def _normalize_pdf_url(pdf_url: Any) -> Optional[str]:
        """Support both string URL and {'url': ..., 'file_token': ...} payload."""
        if not pdf_url:
            return None
        if isinstance(pdf_url, str):
            return pdf_url.strip() or None
        if isinstance(pdf_url, dict):
            url = pdf_url.get("url")
            if isinstance(url, str) and url.strip():
                return url.strip()
        return None

    def lookup_lark_users(self, emails: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Lookup Lark User IDs by email prefix strategy.
        Falls back to Exchange contact resolution for unresolved emails.
        Returns map: {email -> {'open_id': xxx, 'name': xxx}} or {email -> {'name': xxx}} for Exchange-only.
        """
        if not emails:
            return {}

        email_map = {}
        unresolved_emails = []
        logger.info(f"Looking up Lark users for: {emails}")

        # Phase 1: Lark lookup
        for email in emails:
            if email in self._user_cache:
                email_map[email] = self._user_cache[email]
                continue

            if not self.lark_api_client:
                unresolved_emails.append(email)
                continue

            try:
                from lark_oapi.api.contact.v3 import GetUserRequest
                user_id_input = email.split("@")[0]
                req = GetUserRequest.builder() \
                    .user_id(user_id_input) \
                    .user_id_type("user_id") \
                    .build()

                resp = self.lark_api_client.contact.v3.user.get(req)

                if not resp.success():
                    logger.warning(f"Lookup failed for user_id='{user_id_input}' (email={email}): code={resp.code}, msg={resp.msg}")
                    unresolved_emails.append(email)
                    continue

                if resp.data and resp.data.user:
                    found_open_id = resp.data.user.open_id
                    found_name = resp.data.user.name
                    if found_open_id:
                        logger.info(f"Resolved {email} -> {found_name} ({found_open_id})")
                        user_info = {'open_id': found_open_id, 'name': found_name}
                        email_map[email] = user_info
                        self._user_cache[email] = user_info
                    else:
                        unresolved_emails.append(email)
                else:
                    unresolved_emails.append(email)

            except Exception as e:
                logger.error(f"Error resolving user {email}: {e}")
                unresolved_emails.append(email)

        # Phase 2: Exchange contact resolution fallback
        if unresolved_emails and self.exchange_client:
            import asyncio
            logger.info(f"Falling back to Exchange contact resolve for: {unresolved_emails}")
            for email in unresolved_emails:
                try:
                    # Run async resolve_contact in sync context
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            name = pool.submit(
                                asyncio.run,
                                self.exchange_client.resolve_contact(email)
                            ).result(timeout=10)
                    else:
                        name = asyncio.run(self.exchange_client.resolve_contact(email))
                    
                    if name:
                        logger.info(f"Exchange resolved {email} -> {name}")
                        user_info = {'name': name}  # No open_id
                        email_map[email] = user_info
                        self._user_cache[email] = user_info
                    else:
                        logger.info(f"Exchange could not resolve {email}")
                except Exception as e:
                    logger.error(f"Exchange contact resolve error for {email}: {e}")

        return email_map

    def _format_recipients(
        self,
        recipient_list: List,
        user_map: Dict[str, Dict[str, str]],
        show_email: bool = False,
        limit: int = 3
    ) -> str:
        """Format recipients list with Lark profile links"""
        if not recipient_list:
            return "无"

        formatted_items = []
        for r in recipient_list:
            r_str = str(r)
            name = r_str
            email = ""

            m_name = re.search(r"name='(.*?)'", r_str)
            if m_name:
                name = m_name.group(1)

            m_email = re.search(r"email_address='(.*?)'", r_str)
            if m_email:
                email = m_email.group(1)

            if email and email in user_map:
                u_info = user_map[email]
                if u_info.get('open_id'):
                    lark_id = u_info['open_id']
                    real_name = u_info['name']
                    formatted_items.append(
                        f"[{real_name}](feishu://applink.feishu.cn/client/contact/open?openId={lark_id})"
                    )
                else:
                    # Exchange-resolved: show name only
                    formatted_items.append(u_info.get('name', name))
            elif show_email and email:
                formatted_items.append(f"{name} ({email})")
            else:
                formatted_items.append(name)

        if len(formatted_items) > limit:
            return ", ".join(formatted_items[:limit]) + f" 等{len(formatted_items)}人"
        return ", ".join(formatted_items)

    def _build_user_row(
        self,
        label: str,
        recipients_list: List,
        user_map: Dict[str, Dict[str, str]]
    ) -> dict:
        """Build a column_set row for user display"""
        raw_list = recipients_list if isinstance(recipients_list, list) else [recipients_list]

        matched_ids = []
        leftover_text = []

        for r in raw_list:
            e = extract_email_address(r)
            name = str(r)
            m_name = re.search(r"name='(.*?)'", str(r))
            if m_name:
                name = m_name.group(1)
            elif e:
                name = e.split("@")[0]

            if e and e in user_map:
                u_info = user_map[e]
                if u_info.get('open_id'):
                    matched_ids.append(u_info['open_id'])
                else:
                    # Exchange-resolved: use the resolved name
                    leftover_text.append(u_info.get('name', name))
            else:
                leftover_text.append(name)

        columns = [{
            "tag": "column",
            "width": "auto",
            "vertical_align": "center",
            "elements": [{
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**{label}**"}
            }]
        }]

        if matched_ids:
            display_limit = 5
            for uid in matched_ids[:display_limit]:
                columns.append({
                    "tag": "column",
                    "width": "auto",
                    "vertical_align": "center",
                    "elements": [{"tag": "person", "user_id": uid, "style": "normal"}]
                })

            overflow_text_parts = []
            if len(matched_ids) > display_limit:
                overflow_text_parts.append(f"+{len(matched_ids) - display_limit}")
            if leftover_text:
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
            display_text = self._format_recipients(raw_list, user_map, show_email=False, limit=3)
            columns.append({
                "tag": "column",
                "width": "auto",
                "vertical_align": "center",
                "elements": [{
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": display_text}
                }]
            })

        columns.append({
            "tag": "column",
            "width": "weighted",
            "weight": 1,
            "elements": []
        })

        return {
            "tag": "column_set",
            "flex_mode": "none",
            "background_style": "default",
            "horizontal_spacing": "small",
            "columns": columns
        }

    def build_approval_card(
        self,
        email_id: str,
        draft: str,
        context: List[dict],
        email_data: dict,
        classification: dict,
        edit_field: str = None,  # None=普通模式, "to"/"cc"/"draft"=编辑对应字段
        feedback_value: str = "",
        pdf_url: str = None
    ) -> dict:
        """
        Constructs the Lark Card JSON for approval workflow.
        edit_field: None=view mode, "to"/"cc"/"draft"=edit specific field
        """
        subject = email_data.get("subject", "No Subject")
        subject = re.sub(r"^(Subject|主题)[:：]\s*", "", subject, flags=re.IGNORECASE).strip()
        raw_sender = email_data.get("sender", "Unknown")

        original_snippet = "无内容摘要"
        if context:
            text = context[0].get('chunk_text') or context[0].get('body') or ""
            text = text.strip()
            original_snippet = text[:150] + "..." if len(text) > 150 else text

        reason = classification.get("reasoning", "智能生成")
        cc_list = email_data.get("cc", [])
        if isinstance(cc_list, str):
            cc_list = [cc_list]

        # Collect all emails for user lookup
        all_emails = []
        sender_email = extract_email_address(raw_sender)
        if sender_email:
            all_emails.append(sender_email)
        for r in email_data.get("to", []):
            e = extract_email_address(r)
            if e:
                all_emails.append(e)
        for c in cc_list:
            e = extract_email_address(c)
            if e:
                all_emails.append(e)

        user_map = self.lookup_lark_users(list(set(all_emails)))

        elements = []

        # Header
        header = {
            "template": "blue",
            "title": {
                "content": f"📬 拟稿审批: {subject}",
                "tag": "plain_text"
            }
        }

        # AI Note
        elements.append({
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": f"💡 AI 处理说明: {reason}"}]
        })

        # Sender/Recipient compact row
        compact_columns = self._build_compact_header_row(raw_sender, email_data, user_map)
        elements.append({
            "tag": "column_set",
            "flex_mode": "none",
            "background_style": "default",
            "horizontal_spacing": "small",
            "columns": compact_columns
        })
        elements.append({"tag": "hr"})

        logger.info(f"Build Card Debug: Sender={raw_sender}, To={email_data.get('to')}, Cc={cc_list}, PDF={pdf_url}")
        resolved_pdf_url = self._normalize_pdf_url(pdf_url)

        # Original email summary
        header_text = "**📄 原始邮件摘要:**"
        if resolved_pdf_url:
            header_text = f"**📄 原始邮件摘要:** ([📄 查看完整原文 (PDF)]({resolved_pdf_url}))"
        
        # Use div+lark_md instead of bare markdown tag for better link support
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": header_text
            }
        })
        
        # Optimize summary: Priority 1 - LLM Summary, Priority 2 - Body, Priority 3 - Context
        llm_summary = classification.get("summary")
        if llm_summary:
             original_snippet = llm_summary
        else:
             # Fallback: existing logic
             raw_body = email_data.get("body", "")
             if raw_body:
                 # Strip HTML tags
                 try:
                     soup = BeautifulSoup(raw_body, "html.parser")
                     text = soup.get_text(separator=" ", strip=True)
                     original_snippet = text[:200] + "..." if len(text) > 200 else text
                 except Exception:
                     original_snippet = raw_body[:200]
             elif context:
                 # Fallback to context if body is missing
                 text = context[0].get('chunk_text') or context[0].get('body') or ""
                 text = text.strip()
                 # Try to remove Subject/Attachment lines if present in chunk
                 clean_lines = [line for line in text.split('\n') if not line.lower().startswith(('subject:', '附件:', '【'))]
                 text = " ".join(clean_lines)
                 original_snippet = text[:200] + "..." if len(text) > 200 else text

        summary_content = f"*{original_snippet}*"
        
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": summary_content}
        })

        # Attachments - Filter out inline images (content_id indicates embedded image in body)
        attachments = email_data.get("attachments", [])
        real_attachments = [att for att in attachments if not att.get('content_id')]
        
        if real_attachments:
            att_lines = []
            for att in real_attachments[:5]:
                name = att.get('name', 'Unknown File')
                if att.get('lark_file_url'):
                    att_lines.append(f"📎 [{name}]({att['lark_file_url']})")
                else:
                    # Skip attachments without URL (not uploaded)
                    logger.debug(f"Skipping attachment without URL: {name}")
                    continue
            
            if att_lines:
                att_text = "\n".join(att_lines)
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": att_text}})

        # Web view link hidden by request
        # settings = get_settings()
        # external_url = settings.EXTERNAL_URL
        # h5_url = f"{external_url}/email/{email_id}"
        # encoded_url = quote(h5_url, safe="")
        # sidebar_url = f"https://applink.feishu.cn/client/web_url/open?url={encoded_url}&mode=sidebar-semi"
        # elements.append({
        #     "tag": "action",
        #     "actions": [{
        #         "tag": "button",
        #         "text": {"tag": "plain_text", "content": "👀 查看完整原文 (Web)"},
        #         "type": "default",
        #         "url": sidebar_url
        #     }]
        # })
        elements.append({"tag": "hr"})

        # Draft section
        elements.append({"tag": "markdown", "content": "**✍️ 拟定回复:**"})

        # 收件人部分: 默认回复给发件人，但如果用户修改了(含有open_id)，则使用修改后的列表
        draft_to_list = [raw_sender]
        current_to = email_data.get("to", [])
        if current_to and any(str(x).startswith("open_id=") for x in current_to):
            draft_to_list = current_to

        elements.extend(self._build_recipient_section(
            email_id, "to", "📥 收件人 (To):", draft_to_list, user_map, 
            is_editing=(edit_field == "to")
        ))

        # 抄送人部分
        elements.extend(self._build_recipient_section(
            email_id, "cc", "👀 抄送人 (Cc):", cc_list, user_map,
            is_editing=(edit_field == "cc")
        ))

        elements.append({"tag": "hr"})

        # 草稿正文部分
        elements.extend(self._build_draft_section(
            email_id, draft, feedback_value,
            is_editing=(edit_field == "draft")
        ))

        elements.append({"tag": "hr"})

        # Action buttons - 简化为3个主要按钮
        elements.append({
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "✅ 批准发送"},
                 "type": "primary", "value": {"action": "approve", "id": email_id}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "💾 存为草稿"},
                 "type": "default", "value": {"action": "save_draft_only", "id": email_id}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "🛑 拒绝"},
                 "type": "danger", "value": {"action": "reject", "id": email_id}}
            ]
        })

        return {"header": header, "elements": elements}

    def build_read_only_card(
        self,
        email_id: str,
        context: List[dict],
        email_data: dict,
        classification: dict,
        pdf_url: str = None
    ) -> dict:
        """
        构建只读卡片 - 用于重要但不需要回复的邮件。
        
        与审批卡片相比：
        - 保留: 邮件信息展示、附件链接、PDF原文链接
        - 移除: 回复草稿区域、收件人编辑、批准/拒绝按钮
        - 新增: 已阅按钮
        """
        subject = email_data.get("subject", "No Subject")
        subject = re.sub(r"^(Subject|主题)[:：]\s*", "", subject, flags=re.IGNORECASE).strip()
        raw_sender = email_data.get("sender", "Unknown")

        reason = classification.get("reasoning", "智能生成")
        cc_list = email_data.get("cc", [])
        if isinstance(cc_list, str):
            cc_list = [cc_list]

        # Collect all emails for user lookup
        all_emails = []
        sender_email = extract_email_address(raw_sender)
        if sender_email:
            all_emails.append(sender_email)
        for r in email_data.get("to", []):
            e = extract_email_address(r)
            if e:
                all_emails.append(e)
        for c in cc_list:
            e = extract_email_address(c)
            if e:
                all_emails.append(e)

        user_map = self.lookup_lark_users(list(set(all_emails)))

        elements = []

        # Header - 使用紫色区分只读卡片
        priority = classification.get("priority", "P1")
        priority_emoji = {"P0": "🔴", "P1": "🟠", "P2": "🟡", "P3": "⚪"}.get(priority, "📧")
        header = {
            "template": "purple",
            "title": {
                "content": f"{priority_emoji} 重要邮件: {subject}",
                "tag": "plain_text"
            }
        }

        # AI Note
        elements.append({
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": f"💡 AI 处理说明: {reason}（无需回复）"}]
        })

        # Sender/Recipient compact row
        compact_columns = self._build_compact_header_row(raw_sender, email_data, user_map)
        elements.append({
            "tag": "column_set",
            "flex_mode": "none",
            "background_style": "default",
            "horizontal_spacing": "small",
            "columns": compact_columns
        })
        elements.append({"tag": "hr"})

        # Debug logging for recipients
        logger.info(f"Build Read-Only Card: Sender={raw_sender}, To={email_data.get('to')}, Subject={subject}, PDF={pdf_url}")

        resolved_pdf_url = self._normalize_pdf_url(pdf_url)

        # Original email summary
        header_text = "**📄 邮件内容摘要:**"
        if resolved_pdf_url:
            header_text = f"**📄 邮件内容摘要:** ([📄 查看完整原文 (PDF)]({resolved_pdf_url}))"
        
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": header_text
            }
        })
        
        # Summary content
        llm_summary = classification.get("summary")
        if llm_summary:
            original_snippet = llm_summary
        else:
            raw_body = email_data.get("body", "")
            if raw_body:
                try:
                    soup = BeautifulSoup(raw_body, "html.parser")
                    text = soup.get_text(separator=" ", strip=True)
                    original_snippet = text[:300] + "..." if len(text) > 300 else text
                except Exception:
                    original_snippet = raw_body[:300]
            elif context:
                text = context[0].get('chunk_text') or context[0].get('body') or ""
                text = text.strip()
                clean_lines = [line for line in text.split('\n') if not line.lower().startswith(('subject:', '附件:', '【'))]
                text = " ".join(clean_lines)
                original_snippet = text[:300] + "..." if len(text) > 300 else text
            else:
                original_snippet = "无内容摘要"

        summary_content = f"*{original_snippet}*"
        
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": summary_content}
        })

        # Attachments - Filter out inline images (content_id indicates embedded image in body)
        attachments = email_data.get("attachments", [])
        real_attachments = [att for att in attachments if not att.get('content_id')]
        
        if real_attachments:
            att_lines = []
            for att in real_attachments[:5]:
                name = att.get('name', 'Unknown File')
                if att.get('lark_file_url'):
                    att_lines.append(f"📎 [{name}]({att['lark_file_url']})")
                else:
                    # Skip attachments without URL (not uploaded)
                    logger.debug(f"Skipping attachment without URL: {name}")
                    continue
            
            if att_lines:
                att_text = "\n".join(att_lines)
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": att_text}})
        
        # Log filtered attachments for debugging
        if len(attachments) != len(real_attachments):
            logger.info(f"Filtered {len(attachments) - len(real_attachments)} inline images from attachments")

        elements.append({"tag": "hr"})

        # Action button - 只有"已阅"按钮
        elements.append({
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "✅ 已阅"},
                 "type": "primary", "value": {"action": "mark_read", "id": email_id}}
            ]
        })

        return {"header": header, "elements": elements}

    def _build_compact_header_row(
        self,
        raw_sender: str,
        email_data: dict,
        user_map: Dict[str, Dict[str, str]]
    ) -> List[dict]:
        """
        Build compact header row: Sender | To | Cc
        Logic: Show only 1 person per field. Prioritize 'Me' in To/Cc.
        """
        settings = get_settings()
        my_email = settings.EXCHANGE_ACCOUNT_EMAIL or ""
        
        # Debug Logging for Missing Recipients
        to_list = email_data.get("to", [])
        cc_list = email_data.get("cc", [])
        logger.info(f"Compact Header Debug: Sender={raw_sender}, To={to_list} (len={len(to_list)}), Cc={cc_list} (len={len(cc_list)})")

        columns = []
        
        # Helper to get ONE representative
        def get_one_person_and_count(people_list):
            if not people_list:
                return None, 0
                
            total = len(people_list)
            selected = people_list[0]
            
            # Try to find 'Me'
            if my_email:
                for p in people_list:
                    e = extract_email_address(p)
                    if e and my_email.lower() in e.lower():
                        selected = p
                        break
            
            return selected, total - 1

        # Separator element
        def make_separator():
             return {
                "tag": "column", "width": "auto", "vertical_align": "center",
                "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "<font color='lightgrey'>&nbsp;|&nbsp;</font>"}}]
            }
            
        # --- 1. Sender Section (Always Present) ---
        sender_email = extract_email_address(raw_sender)
        sender_uid = user_map.get(sender_email, {}).get('open_id') if sender_email else None
        
        label_content = "**👤 发件人:**"
        # Check if sender is Me
        if my_email and sender_email and my_email.lower() in sender_email.lower():
             label_content = "**👤 发件人 (我):**"

        columns.append({
            "tag": "column", "width": "auto", "vertical_align": "center",
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": label_content}}]
        })
        
        if sender_uid:
            columns.append({
                "tag": "column", "width": "auto", "vertical_align": "center",
                "elements": [{"tag": "person", "user_id": sender_uid, "style": "normal"}]
            })
        else:
            # Try Exchange-resolved name, then fall back to email
            s_name = user_map.get(sender_email, {}).get('name') if sender_email else None
            if not s_name:
                s_name = sender_email if sender_email else "Unknown"
            columns.append({
                "tag": "column", "width": "auto", "vertical_align": "center",
                "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": s_name}}]
            })

        # --- 2. To Section ---
        to_person, to_count = get_one_person_and_count(to_list)
        
        if to_person:
            # Add separator only if we have a To section
            columns.append(make_separator())
            
            columns.append({
                "tag": "column", "width": "auto", "vertical_align": "center",
                "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "**👥 收件人:**"}}]
            })
            
            e = extract_email_address(to_person)
            uid = user_map.get(e, {}).get('open_id') if e else None
            
            if uid:
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "person", "user_id": uid, "style": "normal"}]
                })
            else:
                # Try Exchange-resolved name, then fall back to email
                name = user_map.get(e, {}).get('name') if e else None
                if not name:
                    name = e if e else "Unknown"
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": name}}]
                })
                
            if to_count > 0:
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": f" +{to_count}"}}]
                })

        # --- 3. Cc Section ---
        cc_person, cc_count = get_one_person_and_count(cc_list)
        
        if cc_person:
            # Add separator only if we have a Cc section (works even if To is missing, though unusual)
            columns.append(make_separator())
            
            columns.append({
                "tag": "column", "width": "auto", "vertical_align": "center",
                "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "**👀 抄送:**"}}]
            })
            
            e = extract_email_address(cc_person)
            uid = user_map.get(e, {}).get('open_id') if e else None
            
            if uid:
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "person", "user_id": uid, "style": "normal"}]
                })
            else:
                # Try Exchange-resolved name, then fall back to email
                name = user_map.get(e, {}).get('name') if e else None
                if not name:
                    name = e if e else "Unknown"
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": name}}]
                })
                
            if cc_count > 0:
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": f" +{cc_count}"}}]
                })

        # Filler to align left
        columns.append({"tag": "column", "width": "weighted", "weight": 1, "elements": []})

        return columns

    def _build_recipient_section(
        self,
        email_id: str,
        field_type: str,  # "to" or "cc"
        label: str,
        recipients: List,
        user_map: Dict[str, Dict[str, str]],
        is_editing: bool = False
    ) -> List[dict]:
        """Build recipient section with inline edit button"""
        elements = []
        
        if is_editing:
            person_picker = {
                "tag": "multi_select_person",
                "placeholder": {"tag": "plain_text", "content": "从可见成员中多选"},
                "value": {"action": f"select_{field_type}", "id": email_id}
            }

            # 编辑模式：显示人员选择器，选择后立即更新
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**{label}** *(选择后自动保存)*"}
            })
            elements.append({
                "tag": "action",
                "actions": [
                    person_picker,
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "✕ 取消"},
                        "type": "default",
                        "value": {"action": "cancel_edit", "id": email_id}
                    }
                ]
            })
        else:
            # 只读模式：显示人员 + 编辑按钮
            columns = [
                {"tag": "column", "width": "auto", "vertical_align": "center",
                 "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": f"**{label}**"}}]}
            ]
            
            # 显示收件人
            for r in recipients[:3]:
                # 支持 open_id=xxx 格式（选择后的格式）
                if str(r).startswith("open_id="):
                    uid = str(r).replace("open_id=", "")
                    columns.append({
                        "tag": "column", "width": "auto", "vertical_align": "center",
                        "elements": [{"tag": "person", "user_id": uid, "style": "normal"}]
                    })
                    continue
                
                email = extract_email_address(r)
                uid = user_map.get(email, {}).get('open_id') if email else None
                if uid:
                    columns.append({
                        "tag": "column", "width": "auto", "vertical_align": "center",
                        "elements": [{"tag": "person", "user_id": uid, "style": "normal"}]
                    })
                elif email:
                    name = email.split("@")[0]
                    columns.append({
                        "tag": "column", "width": "auto", "vertical_align": "center",
                        "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": name}}]
                    })
            
            # 显示更多数量
            if len(recipients) > 3:
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": f"+{len(recipients)-3}"}}]
                })
            
            # 占位
            columns.append({"tag": "column", "width": "weighted", "weight": 1, "elements": []})
            
            # 编辑按钮
            columns.append({
                "tag": "column", "width": "auto", "vertical_align": "center",
                "elements": [{
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "✏️"},
                    "type": "text",
                    "size": "small",
                    "value": {"action": f"edit_{field_type}", "id": email_id}
                }]
            })
            
            elements.append({
                "tag": "column_set",
                "flex_mode": "none",
                "background_style": "default",
                "horizontal_spacing": "small",
                "columns": columns
            })
        
        return elements

    def _build_draft_section(
        self,
        email_id: str,
        draft: str,
        feedback_value: str,
        is_editing: bool = False
    ) -> List[dict]:
        """Build draft content section with inline edit button"""
        elements = []
        
        if is_editing:
            # 编辑模式：使用 form + input（按官方文档格式）
            default_val = str(feedback_value or draft or "")
            elements.append({
                "tag": "form",
                "name": "Form_draft",
                "elements": [
                    {
                        "tag": "input",
                        "name": "draft_input",
                        "input_type": "multiline_text",
                        "rows": 4,
                        "placeholder": {
                            "tag": "plain_text",
                            "content": "请输入回复内容"
                        },
                        "default_value": default_val,
                        "width": "fill",
                        "label": {
                            "tag": "plain_text",
                            "content": "📝 正文:"
                        },
                        "label_position": "top"
                    },
                    {
                        "tag": "column_set",
                        "flex_mode": "none",
                        "background_style": "default",
                        "columns": [
                            {
                                "tag": "column",
                                "width": "auto",
                                "vertical_align": "top",
                                "elements": [
                                    {
                                        "tag": "button",
                                        "text": {"tag": "plain_text", "content": "✓ 保存"},
                                        "type": "primary",
                                        "action_type": "form_submit",
                                        "name": "Button_submit",
                                        "value": {"action": "form_submit_draft", "id": email_id}
                                    }
                                ]
                            },
                            {
                                "tag": "column",
                                "width": "auto",
                                "vertical_align": "top",
                                "elements": [
                                    {
                                        "tag": "button",
                                        "text": {"tag": "plain_text", "content": "✕ 取消"},
                                        "type": "default",
                                        "value": {"action": "cancel_edit", "id": email_id},
                                        "name": "Button_cancel"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            })
        else:
            # 只读模式：显示内容 + 编辑按钮
            elements.append({
                "tag": "column_set",
                "flex_mode": "none",
                "background_style": "default",
                "columns": [
                    {"tag": "column", "width": "auto", "vertical_align": "center",
                     "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "**📝 正文:**"}}]},
                    {"tag": "column", "width": "weighted", "weight": 1, "elements": []},
                    {"tag": "column", "width": "auto", "vertical_align": "center",
                     "elements": [{
                         "tag": "button",
                         "text": {"tag": "plain_text", "content": "✏️ 编辑"},
                         "type": "text",
                         "size": "small",
                         "value": {"action": "edit_draft", "id": email_id}
                     }]}
                ]
            })
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": draft or "*（空）*"}
            })
        
        return elements

    @staticmethod
    def get_processed_card(status_text: str, original_subject: str = "") -> dict:
        """Returns a collapsed card for processed state."""
        return {
            "header": {
                "title": {"content": f"{status_text} | {original_subject}", "tag": "plain_text"},
                "template": "grey"
            },
            "elements": [{
                "tag": "div",
                "text": {"tag": "lark_md", "content": "✅ 已处理完成，无需继续操作。"}
            }]
        }
