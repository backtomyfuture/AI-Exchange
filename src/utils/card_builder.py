"""
Lark Card Builder - 飞书卡片构建模块
从 lark_app.py 拆分，负责卡片 JSON 结构的构建逻辑。
"""
import asyncio
import re
import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from src.config import get_settings
from src.safety.manual_review import manual_review_reason_label, normalize_manual_review_code
from src.security.redaction import fingerprint_identifier
from src.utils.email_attachments import select_business_attachments

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
    except Exception:
        return "Markdown解析失败"


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

    def __init__(
        self,
        lark_api_client=None,
        exchange_client=None,
        exchange_loop: asyncio.AbstractEventLoop | None = None,
    ):
        self.lark_api_client = lark_api_client
        self.exchange_client = exchange_client
        self.exchange_loop = exchange_loop
        self._user_cache: Dict[str, Dict[str, str]] = {}

    def _resolve_exchange_contact(self, email: str) -> Optional[str]:
        """Resolve a contact on the Exchange client's owning event loop.

        Card construction runs in a worker thread, while ``ExchangeClient``
        owns one shared ``httpx.AsyncClient`` on the application loop. Creating
        a second event loop here can bind httpcore's async primitives to the
        wrong loop and poison the next request, including mark-as-read.
        """
        if self.exchange_loop is not None:
            if self.exchange_loop.is_closed():
                raise RuntimeError("exchange_contact_loop_closed")
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if current_loop is self.exchange_loop:
                raise RuntimeError("exchange_contact_called_on_owner_loop")
            future = asyncio.run_coroutine_threadsafe(
                self.exchange_client.resolve_contact(email),
                self.exchange_loop,
            )
            return future.result(timeout=10)

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.exchange_client.resolve_contact(email))
        raise RuntimeError("exchange_contact_loop_unavailable")

    @staticmethod
    def _approval_action_value(
        action: str,
        email_id: str,
        binding: dict[str, object],
    ) -> dict[str, object]:
        return {"action": action, "id": email_id, **binding}

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
        logger.info("Looking up Lark users: count=%d", len(emails))

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
                    logger.warning(
                        "Lark user lookup failed: user=%s code=%s",
                        fingerprint_identifier(email, namespace="email"),
                        resp.code,
                    )
                    unresolved_emails.append(email)
                    continue

                if resp.data and resp.data.user:
                    found_open_id = resp.data.user.open_id
                    found_name = resp.data.user.name
                    if found_open_id:
                        logger.info(
                            "Lark user resolved: email=%s open_id=%s",
                            fingerprint_identifier(email, namespace="email"),
                            fingerprint_identifier(found_open_id, namespace="lark_actor"),
                        )
                        user_info = {'open_id': found_open_id, 'name': found_name}
                        email_map[email] = user_info
                        self._user_cache[email] = user_info
                    else:
                        unresolved_emails.append(email)
                else:
                    unresolved_emails.append(email)

            except Exception as exc:
                logger.error(
                    "Lark user resolution failed: email=%s error_type=%s",
                    fingerprint_identifier(email, namespace="email"),
                    type(exc).__name__,
                )
                unresolved_emails.append(email)

        # Polling: Exchange contact resolution fallback
        if unresolved_emails and self.exchange_client:
            logger.info(
                "Falling back to Exchange contact resolution: count=%d",
                len(unresolved_emails),
            )
            for email in unresolved_emails:
                try:
                    name = self._resolve_exchange_contact(email)
                    
                    if name:
                        logger.info(
                            "Exchange contact resolved: email=%s",
                            fingerprint_identifier(email, namespace="email"),
                        )
                        user_info = {'name': name}  # No open_id
                        email_map[email] = user_info
                        self._user_cache[email] = user_info
                    else:
                        logger.info(
                            "Exchange contact unresolved: email=%s",
                            fingerprint_identifier(email, namespace="email"),
                        )
                except Exception as exc:
                    logger.error(
                        "Exchange contact resolution failed: email=%s error_type=%s",
                        fingerprint_identifier(email, namespace="email"),
                        type(exc).__name__,
                    )

        return email_map

    def search_person_picker_candidates(self, keyword: str) -> List[str]:
        """
        Search selectable open_ids by exact mailbox prefix (user_id) only.
        No org-wide name matching or cache in v1.
        """
        raw_keyword = (keyword or "").strip().lower()
        if not raw_keyword or not self.lark_api_client:
            return []

        from lark_oapi.api.contact.v3 import GetUserRequest

        # Accept both full email and direct prefix input.
        user_prefix = raw_keyword.split("@", 1)[0].strip()
        if not user_prefix:
            return []

        results: List[str] = []
        try:
            req = GetUserRequest.builder() \
                .user_id_type("user_id") \
                .department_id_type("department_id") \
                .user_id(user_prefix) \
                .build()
            resp = self.lark_api_client.contact.v3.user.get(req)
            if resp.success():
                body = getattr(resp, "data", None)
                user = getattr(body, "user", None) if body else None
                open_id = str(getattr(user, "open_id", "") or "").strip()
                if open_id:
                    results.append(open_id)
        except Exception as exc:
            logger.warning(
                "Prefix lookup failed: prefix=%s error_type=%s",
                fingerprint_identifier(user_prefix, namespace="user_prefix"),
                type(exc).__name__,
            )

        logger.info(
            "Recipient search stats: mode=prefix_only keyword_bytes=%d hits=%d",
            len(raw_keyword.encode("utf-8")),
            len(results),
        )
        return results

    @staticmethod
    def _collect_open_ids_from_recipients(
        recipients: List,
        user_map: Dict[str, Dict[str, str]]
    ) -> List[str]:
        """Extract open_id list from mixed recipient formats."""
        collected: List[str] = []
        seen = set()
        for r in recipients or []:
            uid = None
            r_str = str(r).strip()
            if r_str.startswith("open_id="):
                uid = r_str.split("=", 1)[1].strip()
            else:
                email = extract_email_address(r)
                if email:
                    uid = user_map.get(email, {}).get("open_id")
            if uid and uid not in seen:
                collected.append(uid)
                seen.add(uid)
        return collected

    @staticmethod
    def _collect_external_emails_from_recipients(recipients: List) -> List[str]:
        """Extract non-open_id emails from recipients for external-input defaults."""
        emails: List[str] = []
        seen = set()
        for r in recipients or []:
            r_str = str(r).strip()
            if not r_str or r_str.startswith("open_id="):
                continue
            email = extract_email_address(r_str)
            if email and email not in seen:
                emails.append(email)
                seen.add(email)
        return emails

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

    @staticmethod
    def _build_routing_note(
        routing_log: Optional[List[str]] = None,
        classification: Optional[dict] = None,
    ) -> List[dict]:
        """Build a compact routing-info note for observability."""
        parts = []
        if routing_log:
            parts.append(" → ".join(routing_log[:4]))
        conf = (classification or {}).get("confidence")
        if conf is not None:
            parts.append(f"Conf: {conf:.0%}")
        if not parts:
            return []
        return [{
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": f"🔀 路由: {' | '.join(parts)}"}]
        }]

    @staticmethod
    def _build_content_guard_warning(classification: Optional[dict] = None) -> List[dict]:
        """Build a warning note if ContentGuard found issues."""
        guard_data = (classification or {}).get("_content_guard")
        if not guard_data or guard_data.get("passed", True):
            return []
        summary = guard_data.get("summary", "")
        return [{
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": f"⚠️ 质量检查: {summary}"}]
        }]

    def _build_email_info_section(
        self,
        raw_sender: str,
        to_list: list,
        cc_list: list,
        user_map: Dict[str, Dict[str, str]]
    ) -> List[dict]:
        """
        Build email info rows: Sender, To, Cc displayed on separate lines
        using _build_user_row for full person component rendering.
        """
        elements = []
        settings = get_settings()
        my_email = (settings.EXCHANGE_ACCOUNT_EMAIL or "").strip().lower()

        sender_email = extract_email_address(raw_sender)
        sender_label = "👤 发件人:"
        if my_email and sender_email and my_email in sender_email.lower():
            sender_label = "👤 发件人 (我):"
        if raw_sender and raw_sender != "Unknown":
            elements.append(self._build_user_row(sender_label, [raw_sender], user_map))

        if to_list:
            elements.append(self._build_user_row("👥 收件人:", to_list, user_map))

        if cc_list:
            elements.append(self._build_user_row("👀 抄送:", cc_list, user_map))

        return elements

    def build_approval_card(
        self,
        email_id: str,
        draft: str,
        context: List[dict],
        email_data: dict,
        classification: dict,
        edit_field: str = None,  # None=普通模式, "to"/"cc"/"draft"=编辑对应字段
        feedback_value: str = "",
        pdf_url: str = None,
        routing_log: Optional[List[str]] = None,
        *,
        inbox_id: str | None = None,
        payload_revision: int | None = None,
        payload_digest: str | None = None,
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
            text = (
                context[0].get("snippet")
                or context[0].get("chunk_text")
                or context[0].get("body")
                or ""
            )
            text = text.strip()
            original_snippet = text[:150] + "..." if len(text) > 150 else text

        reason = classification.get("reasoning", "智能生成")

        original_to_list = email_data.get("original_to", email_data.get("to", []))
        if isinstance(original_to_list, str):
            original_to_list = [original_to_list]

        original_cc_list = email_data.get("original_cc", email_data.get("cc", []))
        if isinstance(original_cc_list, str):
            original_cc_list = [original_cc_list]

        draft_to_list = email_data.get("draft_to", [])
        if isinstance(draft_to_list, str):
            draft_to_list = [draft_to_list]

        draft_cc_list = email_data.get("draft_cc", [])
        if isinstance(draft_cc_list, str):
            draft_cc_list = [draft_cc_list]

        # Collect all emails for user lookup
        all_emails = []
        sender_email = extract_email_address(raw_sender)
        if sender_email:
            all_emails.append(sender_email)
        for r in original_to_list:
            e = extract_email_address(r)
            if e:
                all_emails.append(e)
        for c in original_cc_list:
            e = extract_email_address(c)
            if e:
                all_emails.append(e)
        for r in draft_to_list:
            e = extract_email_address(r)
            if e:
                all_emails.append(e)
        for c in draft_cc_list:
            e = extract_email_address(c)
            if e:
                all_emails.append(e)

        user_map = self.lookup_lark_users(list(set(all_emails)))
        to_picker_candidates = email_data.get("draft_to_options", []) or []
        cc_picker_candidates = email_data.get("draft_cc_options", []) or []
        to_search_hint = email_data.get("draft_to_search_hint", "")
        cc_search_hint = email_data.get("draft_cc_search_hint", "")
        to_new_selected = email_data.get("draft_to_new_selected", []) or []
        cc_new_selected = email_data.get("draft_cc_new_selected", []) or []
        to_external_input = str(email_data.get("draft_to_external_input", "") or "")
        cc_external_input = str(email_data.get("draft_cc_external_input", "") or "")
        if not to_external_input:
            to_external_input = "; ".join(self._collect_external_emails_from_recipients(draft_to_list))
        if not cc_external_input:
            cc_external_input = "; ".join(self._collect_external_emails_from_recipients(draft_cc_list))

        inbox_id = inbox_id or email_data.get("_approval_inbox_id")
        payload_revision = payload_revision or email_data.get("_approval_payload_revision")
        payload_digest = payload_digest or email_data.get("_approval_payload_digest")
        action_binding: dict[str, object] = {}
        if inbox_id is not None:
            if not (
                isinstance(payload_revision, int)
                and payload_revision > 0
                and isinstance(payload_digest, str)
                and re.fullmatch(r"[0-9a-f]{64}", payload_digest)
                and isinstance(inbox_id, str)
                and re.fullmatch(r"[0-9a-f-]{36}", inbox_id)
            ):
                raise ValueError("invalid_durable_card_binding")
            action_binding = {
                "inbox_id": inbox_id,
                "payload_revision": payload_revision,
                "payload_digest": payload_digest,
            }

        to_existing_candidates = self._collect_open_ids_from_recipients(draft_to_list, user_map)
        for uid in self._collect_open_ids_from_recipients(original_to_list, user_map):
            if uid not in to_existing_candidates:
                to_existing_candidates.append(uid)
            if len(to_existing_candidates) >= 5:
                break

        cc_existing_candidates = self._collect_open_ids_from_recipients(draft_cc_list, user_map)
        for uid in self._collect_open_ids_from_recipients(original_cc_list, user_map):
            if uid not in cc_existing_candidates:
                cc_existing_candidates.append(uid)
            if len(cc_existing_candidates) >= 5:
                break

        elements = []

        # Header
        is_forward = classification.get("action") == "forward"
        header_title = f"📬 拟定转发: {subject}" if is_forward else f"📬 拟稿审批: {subject}"
        
        header = {
            "template": "blue",
            "title": {
                "content": header_title,
                "tag": "plain_text"
            }
        }

        # AI Note
        elements.append({
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": f"💡 AI 处理说明: {reason}"}]
        })

        # Email info section: Sender / To / Cc on separate rows
        elements.extend(self._build_email_info_section(
            raw_sender, original_to_list, original_cc_list, user_map
        ))
        elements.append({"tag": "hr"})

        logger.info(
            "Building approval card: original_to=%d original_cc=%d draft_to=%d draft_cc=%d pdf=%s",
            len(original_to_list),
            len(original_cc_list),
            len(draft_to_list),
            len(draft_cc_list),
            bool(pdf_url),
        )
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
                 text = (
                     context[0].get("snippet")
                     or context[0].get("chunk_text")
                     or context[0].get("body")
                     or ""
                 )
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

        # Attachments - show only standalone business files.
        real_attachments = select_business_attachments(email_data)
        
        if real_attachments:
            att_lines = []
            for att in real_attachments[:5]:
                name = att.get('name', 'Unknown File')
                if att.get('lark_file_url'):
                    att_lines.append(f"📎 [{name}]({att['lark_file_url']})")
                else:
                    # Skip attachments without URL (not uploaded)
                    logger.debug("Skipping attachment without URL")
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
        # Draft section
        draft_section_title = "**✍️ 拟定转发语:**" if is_forward else "**✍️ 拟定回复:**"
        elements.append({"tag": "markdown", "content": draft_section_title})

        to_label = "📥 转发给 (To):" if is_forward else "📥 收件人 (To):"
        elements.extend(self._build_recipient_section(
            email_id, "to", to_label, draft_to_list, user_map,
            existing_candidates=to_existing_candidates,
            picker_candidates=to_picker_candidates,
            new_selected=to_new_selected,
            search_hint=to_search_hint,
            external_input=to_external_input,
            is_editing=(edit_field == "to"),
            action_binding=action_binding,
        ))

        # 抄送人部分
        elements.extend(self._build_recipient_section(
            email_id, "cc", "👀 抄送人 (Cc):", draft_cc_list, user_map,
            existing_candidates=cc_existing_candidates,
            picker_candidates=cc_picker_candidates,
            new_selected=cc_new_selected,
            search_hint=cc_search_hint,
            external_input=cc_external_input,
            is_editing=(edit_field == "cc"),
            action_binding=action_binding,
        ))

        elements.append({"tag": "hr"})

        # 草稿正文部分
        elements.extend(self._build_draft_section(
            email_id, draft, feedback_value,
            is_editing=(edit_field == "draft"),
            action_binding=action_binding,
        ))

        elements.append({"tag": "hr"})

        approval_value = self._approval_action_value(
            "approve", email_id, action_binding
        )

        # Action buttons
        elements.append({
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "✅ 批准转发" if is_forward else "✅ 批准发送"},
                 "type": "primary", "value": approval_value},
                {"tag": "button", "text": {"tag": "plain_text", "content": "💾 存为草稿"},
                 "type": "default", "value": self._approval_action_value(
                     "save_draft_only", email_id, action_binding
                 )},
                {
                    "tag": "select_static",
                    "placeholder": {"tag": "plain_text", "content": "🛑 拒绝..."},
                    "options": [
                        {"text": {"tag": "plain_text", "content": "语气不当"}, "value": "tone_wrong"},
                        {"text": {"tag": "plain_text", "content": "内容有误"}, "value": "content_error"},
                        {"text": {"tag": "plain_text", "content": "无需回复"}, "value": "no_reply_needed"},
                        {"text": {"tag": "plain_text", "content": "其他原因"}, "value": "other"},
                    ],
                    "value": self._approval_action_value(
                        "reject_with_reason", email_id, action_binding
                    ),
                },
            ]
        })

        elements.extend(self._build_routing_note(routing_log, classification))

        return {"header": header, "elements": elements}

    def build_read_only_card(
        self,
        email_id: str,
        context: List[dict],
        email_data: dict,
        classification: dict,
        pdf_url: str = None,
        routing_log: Optional[List[str]] = None,
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

        to_list = email_data.get("to", [])
        if isinstance(to_list, str):
            to_list = [to_list]

        cc_list = email_data.get("cc", [])
        if isinstance(cc_list, str):
            cc_list = [cc_list]

        # Collect all emails for user lookup
        all_emails = []
        sender_email = extract_email_address(raw_sender)
        if sender_email:
            all_emails.append(sender_email)
        for r in to_list:
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

        # Email info section: Sender / To / Cc on separate rows
        elements.extend(self._build_email_info_section(
            raw_sender, to_list, cc_list, user_map
        ))
        elements.append({"tag": "hr"})

        # Debug logging for recipients
        logger.info(
            "Building read-only card: to_count=%d cc_count=%d pdf=%s",
            len(to_list),
            len(cc_list),
            bool(pdf_url),
        )

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
                text = (
                    context[0].get("snippet")
                    or context[0].get("chunk_text")
                    or context[0].get("body")
                    or ""
                )
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

        # Attachments - show only standalone business files.
        attachments = email_data.get("attachments", [])
        real_attachments = select_business_attachments(email_data)
        
        if real_attachments:
            att_lines = []
            for att in real_attachments[:5]:
                name = att.get('name', 'Unknown File')
                if att.get('lark_file_url'):
                    att_lines.append(f"📎 [{name}]({att['lark_file_url']})")
                else:
                    # Skip attachments without URL (not uploaded)
                    logger.debug("Skipping attachment without URL")
                    continue
            
            if att_lines:
                att_text = "\n".join(att_lines)
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": att_text}})
        
        # Log filtered attachments for debugging
        if len(attachments) != len(real_attachments):
            logger.info(
                "Filtered inline images from attachments: count=%d",
                len(attachments) - len(real_attachments),
            )

        elements.append({"tag": "hr"})

        # Action button - 只有"已阅"按钮
        elements.append({
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "✅ 已阅"},
                 "type": "primary", "value": {"action": "mark_read", "id": email_id}}
            ]
        })

        elements.extend(self._build_routing_note(routing_log, classification))

        return {"header": header, "elements": elements}

    def build_manual_review_card(
        self,
        email_id: str,
        email_data: dict,
        reason: str,
        classification: Optional[dict] = None,
        pdf_url: Any = None,
        routing_log: Optional[List[str]] = None,
    ) -> dict:
        """
        构建人工复核卡片 - 用于系统安全兜底转人工的邮件。

        与只读卡片相比：
        - 保留: 邮件信息展示（发件人/收件人/抄送）、附件链接、PDF原文链接、内容摘要
        - 移除: 所有操作按钮（不可确认，只能去 Exchange 手工处理）
        - 新增: 人工复核原因（中文说明）
        """
        subject = email_data.get("subject", "No Subject")
        subject = re.sub(r"^(Subject|主题)[:：]\s*", "", subject, flags=re.IGNORECASE).strip()
        raw_sender = email_data.get("sender", "Unknown")

        to_list = email_data.get("to", [])
        if isinstance(to_list, str):
            to_list = [to_list]

        cc_list = email_data.get("cc", [])
        if isinstance(cc_list, str):
            cc_list = [cc_list]

        # Collect all emails for user lookup
        all_emails = []
        sender_email = extract_email_address(raw_sender)
        if sender_email:
            all_emails.append(sender_email)
        for r in to_list:
            e = extract_email_address(r)
            if e:
                all_emails.append(e)
        for c in cc_list:
            e = extract_email_address(c)
            if e:
                all_emails.append(e)

        user_map = self.lookup_lark_users(list(set(all_emails)))

        elements = []

        header = {
            "template": "red",
            "title": {
                "content": f"⚠️ 需要人工处理: {subject}",
                "tag": "plain_text"
            }
        }

        safe_code = normalize_manual_review_code(reason)
        reason_label = manual_review_reason_label(safe_code)
        elements.append({
            "tag": "note",
            "elements": [{"tag": "plain_text", "content": f"💡 复核原因: {reason_label}"}]
        })

        # Email info section: Sender / To / Cc on separate rows
        elements.extend(self._build_email_info_section(
            raw_sender, to_list, cc_list, user_map
        ))
        elements.append({"tag": "hr"})

        resolved_pdf_url = self._normalize_pdf_url(pdf_url)

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

        classification = classification or {}
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
            else:
                original_snippet = "无内容摘要"

        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"*{original_snippet}*"}
        })

        # Attachments - show only standalone business files.
        real_attachments = select_business_attachments(email_data)
        if real_attachments:
            att_lines = []
            for att in real_attachments[:5]:
                name = att.get('name', 'Unknown File')
                if att.get('lark_file_url'):
                    att_lines.append(f"📎 [{name}]({att['lark_file_url']})")
                else:
                    continue
            if att_lines:
                att_text = "\n".join(att_lines)
                elements.append({"tag": "div", "text": {"tag": "lark_md", "content": att_text}})

        elements.append({"tag": "hr"})
        elements.append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": "邮件保持未读，请在 Exchange 收件箱手工处理。",
                }
            ],
        })

        elements.extend(self._build_routing_note(routing_log, classification))

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
        logger.info(
            "Building compact header: to_count=%d cc_count=%d",
            len(to_list),
            len(cc_list),
        )

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
            
            to_person_str = str(to_person).strip()
            uid = None
            e = None
            if to_person_str.startswith("open_id="):
                uid = to_person_str.split("=", 1)[1].strip()
            else:
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
            
            cc_person_str = str(cc_person).strip()
            uid = None
            e = None
            if cc_person_str.startswith("open_id="):
                uid = cc_person_str.split("=", 1)[1].strip()
            else:
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
        existing_candidates: Optional[List[str]] = None,
        picker_candidates: Optional[List[str]] = None,
        new_selected: Optional[List[str]] = None,
        search_hint: str = "",
        external_input: str = "",
        is_editing: bool = False,
        action_binding: Optional[dict[str, object]] = None,
    ) -> List[dict]:
        """Build recipient section with inline edit button"""
        elements = []
        binding = action_binding or {}
        
        if is_editing:
            selected_open_ids = self._collect_open_ids_from_recipients(recipients, user_map)
            selected_set = set(selected_open_ids)
            existing_pool = []
            existing_seen = set()
            for uid in selected_open_ids + list(existing_candidates or []):
                if uid and uid not in existing_seen:
                    existing_pool.append(uid)
                    existing_seen.add(uid)
            max_existing = max(5, len(selected_open_ids))
            existing_options = existing_pool[:max_existing]

            # 编辑模式：使用 form + 显式保存
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**{label}**"}
            })

            form_elements = []
            if existing_options:
                existing_picker = {
                    "tag": "multi_select_person",
                    "name": f"{field_type}_existing",
                    "placeholder": {"tag": "plain_text", "content": "原有人员（可取消勾选移除）"},
                    "options": [{"value": uid} for uid in existing_options],
                }
                if selected_open_ids:
                    existing_picker["selected_values"] = selected_open_ids
                form_elements.append(existing_picker)

            form_elements.append({
                "tag": "input",
                "name": f"{field_type}_search_keyword",
                "input_type": "text",
                "placeholder": {"tag": "plain_text", "content": "输入邮箱前缀（@前部分）后点击搜索"},
                "label": {"tag": "plain_text", "content": "🔎 新增人员搜索"},
                "label_position": "top",
                "width": "fill"
            })

            # Keep button in column_set to avoid unsupported "action" container inside form.
            form_elements.append({
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
                                "text": {"tag": "plain_text", "content": "搜索匹配人员"},
                                "type": "default",
                                "action_type": "form_submit",
                                "name": f"Button_search_{field_type}",
                                "value": self._approval_action_value(
                                    f"search_{field_type}", email_id, binding
                                ),
                            }
                        ]
                    },
                    {"tag": "column", "width": "weighted", "weight": 1, "elements": []}
                ]
            })

            form_elements.append({
                "tag": "input",
                "name": f"{field_type}_external_input",
                "input_type": "text",
                "placeholder": {"tag": "plain_text", "content": "外部邮箱/群组，多个用逗号或分号分隔"},
                "label": {"tag": "plain_text", "content": "🌐 外部邮箱（非飞书用户）"},
                "label_position": "top",
                "width": "fill",
                "default_value": str(external_input or "")
            })

            new_picker = {
                "tag": "multi_select_person",
                "name": f"{field_type}_new",
                "placeholder": {"tag": "plain_text", "content": "新增人员（先搜索，再从结果中选择）"},
            }
            option_values = []
            option_seen = set()
            for uid in list(picker_candidates or []) + list(new_selected or []):
                if not uid or uid in selected_set or uid in option_seen:
                    continue
                option_values.append(uid)
                option_seen.add(uid)
            if option_values:
                new_picker["options"] = [{"value": uid} for uid in option_values]
                selected_values = [uid for uid in (new_selected or []) if uid in option_seen]
                if selected_values:
                    new_picker["selected_values"] = selected_values
            form_elements.append(new_picker)

            form_elements.append({
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
                                "name": f"Button_submit_{field_type}",
                                "value": self._approval_action_value(
                                    f"save_{field_type}", email_id, binding
                                ),
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
                                "value": self._approval_action_value(
                                    "cancel_edit", email_id, binding
                                ),
                                "name": f"Button_cancel_{field_type}"
                            }
                        ]
                    }
                ]
            })

            elements.append({
                "tag": "form",
                "name": f"Form_{field_type}",
                "elements": form_elements
            })

            elements.append({
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": search_hint or "提示：支持累计搜索结果；外部邮箱可直接填写，保存后生效。"
                    }
                ]
            })
        else:
            # 只读模式：显示人员 + 编辑按钮
            columns = [
                {"tag": "column", "width": "auto", "vertical_align": "center",
                 "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": f"**{label}**"}}]}
            ]
            rendered_count = 0
            display_limit = 5
            hidden_external_count = 0

            def _render_recipient_item(recipient: Any):
                """Return ('person', uid, is_external) or ('text', content, is_external)."""
                r_str = str(recipient).strip()
                if not r_str:
                    return ("text", "Unknown", False)

                if r_str.startswith("open_id="):
                    uid = r_str.replace("open_id=", "").strip()
                    if uid:
                        return ("person", uid, False)
                    return ("text", "Unknown", False)

                email = extract_email_address(recipient)
                if email:
                    uid = user_map.get(email, {}).get("open_id")
                    if uid:
                        return ("person", uid, False)
                    # Keep original display logic: prefer resolved Exchange name, else raw email text.
                    display_text = user_map.get(email, {}).get("name") or email
                    return ("text", display_text, True)

                m_name = re.search(r"name='(.*?)'", r_str)
                if m_name:
                    return ("text", m_name.group(1).strip()[:32], False)
                return ("text", r_str[:32], False)

            for idx, r in enumerate(recipients or []):
                item_type, value, is_external = _render_recipient_item(r)
                if idx >= display_limit:
                    if is_external:
                        hidden_external_count += 1
                    continue

                if item_type == "person":
                    columns.append({
                        "tag": "column", "width": "auto", "vertical_align": "center",
                        "elements": [{"tag": "person", "user_id": value, "style": "normal"}]
                    })
                else:
                    columns.append({
                        "tag": "column", "width": "auto", "vertical_align": "center",
                        "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": str(value)[:32]}}]
                    })
                rendered_count += 1

            hidden_count = max(0, len(recipients or []) - display_limit)
            if hidden_count > 0:
                overflow_text = f"+{hidden_count}"
                if hidden_external_count > 0:
                    overflow_text += f"（含外部{hidden_external_count}）"
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": overflow_text}}]
                })

            if rendered_count == 0:
                columns.append({
                    "tag": "column", "width": "auto", "vertical_align": "center",
                    "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": "（未设置）"}}]
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
                    "value": self._approval_action_value(
                        f"edit_{field_type}", email_id, binding
                    ),
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
        is_editing: bool = False,
        action_binding: Optional[dict[str, object]] = None,
    ) -> List[dict]:
        """Build draft content section with inline edit button"""
        elements = []
        binding = action_binding or {}
        
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
                                        "value": self._approval_action_value(
                                            "form_submit_draft", email_id, binding
                                        ),
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
                                        "value": self._approval_action_value(
                                            "cancel_edit", email_id, binding
                                        ),
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
                         "value": self._approval_action_value(
                             "edit_draft", email_id, binding
                         ),
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
