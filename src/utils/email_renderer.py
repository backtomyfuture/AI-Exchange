import html
import logging
from typing import Dict, Any, Optional
from bs4 import BeautifulSoup
import re
from datetime import datetime

logger = logging.getLogger(__name__)


def _format_datetime_cn(dt_str: str) -> str:
    """将时间格式化为中文易读格式，如 2026年2月4日 10:30"""
    if not dt_str or dt_str == "Unknown Date":
        return "未知时间"
    
    try:
        raw = str(dt_str).strip()
        # ISO格式: 2026-02-04T10:30:00 或 2026-02-04T10:30:00+08:00
        if 'T' in raw:
            dt = datetime.fromisoformat(raw.replace('Z', '+00:00'))
            return dt.strftime("%Y年%-m月%-d日 %H:%M")
    except:
        pass
    
    # 尝试解析其他格式
    try:
        # 格式: 2026-02-04 10:30:00
        dt = datetime.strptime(str(dt_str)[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y年%-m月%-d日 %H:%M")
    except:
        pass
    
    return str(dt_str)


def _format_address_str(raw_str: str) -> str:
    """Format address string like 'name=..., email=...' to 'Name <email>'"""
    if not raw_str:
        return ""
    try:
        raw = str(raw_str).strip()
        
        # Format: Mailbox(name='张霞', email_address='zhang-xia@tianjin-air.com', routing_type='...')
        # Also matches: name='张霞', email_address='zhang-xia@tianjin-air.com'
        m = re.search(r"name=['\"]([^'\"]*)['\"].*?email_address=['\"]([^'\"]+)['\"]", raw)
        if m:
            name, email = m.groups()
            if name:
                return f"{html.escape(name)} &lt;{html.escape(email)}&gt;"
            else:
                return html.escape(email)
        
        # Format: 张霞 <zhang-xia@tianjin-air.com>
        m2 = re.search(r"(.*?)\s*<(.+?)>", raw)
        if m2:
            name = m2.group(1).strip()
            email = m2.group(2).strip()
            if name:
                return f"{html.escape(name)} &lt;{html.escape(email)}&gt;"
            else:
                return html.escape(email)
        
        # Format: Pure email
        if '@' in raw and ' ' not in raw:
            return html.escape(raw)

        return html.escape(raw)
    except Exception:
        return html.escape(str(raw_str))


def render_email_html(email_data: Dict[str, Any]) -> str:
    """
    Render email data into a standalone HTML string (Outlook style).
    Handles CID image replacement. Optimized for PDF export with minimal whitespace.
    """
    subject = email_data.get("subject", "No Subject")
    subject = email_data.get("subject", "No Subject")
    raw_body = email_data.get("body", "<i>No Content</i>")
    
    # Safety check for huge bodies (e.g. > 10MB) to prevent PDF crash
    if raw_body and len(raw_body) > 10 * 1024 * 1024:
        logger.warning(f"Email body too large ({len(raw_body)} bytes). Truncating for PDF generation.")
        # Try to keep start and end? Or just start.
        full_body_html = raw_body[:200000] + "<br><br><b>[Content Truncated due to size limit]</b>"
    else:
        full_body_html = raw_body

    
    # Construct Email Headers for Outlook-like view
    sender_str = _format_address_str(email_data.get('sender', ''))
    
    to_list = email_data.get("to", [])
    if isinstance(to_list, str):
        to_list = [to_list]
    to_str = "; ".join([_format_address_str(x) for x in to_list if x])
    
    cc_list = email_data.get("cc", [])
    if isinstance(cc_list, str):
        cc_list = [cc_list]
    cc_str = "; ".join([_format_address_str(x) for x in cc_list if x])
    
    sent_at = email_data.get("received_at") or email_data.get("datetime_received") or "Unknown Date"
    sent_at = _format_datetime_cn(sent_at)
    
    subject_escaped = html.escape(subject)
    
    # Process CID Images and Strip Outer HTML/BODY
    # Always parse with BS4 to ensure we handle full HTML documents correctly
    try:
        if not full_body_html:
            full_body_html = ""
            
        soup = BeautifulSoup(full_body_html, 'html.parser')
        
        # 1. Extract body content if this is a full HTML doc
        if soup.body:
             # decode_contents() returns the inner HTML of the tag
            new_body = soup.body.decode_contents()
            # If body was found, we want to work with its content from now on
            # But we also need to keep any styles defined in <head><style>...
            # This is tricky strictly. For now, let's assume inline styles are safest.
            # Or content might be just the body.
            # Simplify: just take the body content.
            # Re-parse the isolated body content to handle CIDs inside it properly
            soup = BeautifulSoup(new_body, 'html.parser')
        
        # 2. Process CID Images
        attachments = email_data.get("attachments", [])
        if attachments and "cid:" in str(soup):
            cid_map = {}
            for att in attachments:
                if att.get("content_id") and att.get("content"):
                    cid_map[att["content_id"]] = att["content"]
            
            for img in soup.find_all('img'):
                src = img.get('src', '')
                if src.startswith('cid:'):
                    cid = src[4:].strip('<>')
                    if cid in cid_map:
                        mime_type = "image/png"
                        if cid.lower().endswith((".jpg", ".jpeg")):
                            mime_type = "image/jpeg"
                        elif cid.lower().endswith(".gif"):
                            mime_type = "image/gif"
                        img['src'] = f"data:{mime_type};base64,{cid_map[cid]}"
        
        full_body_html = str(soup)
            
    except Exception as e:
        logger.error(f"Error processing HTML body/images: {e}")

    # Build CC row if exists
    cc_row = f'<div style="margin-bottom:4px;"><b>抄送:</b> {cc_str}</div>' if cc_str else ''

    # Compact Outlook-style HTML (minimized whitespace for better PDF pagination)
    full_email_html = f'''<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{subject_escaped}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
/* Basic reset only - PDF Generator controls main font and page Layout via CSS file */
body{{line-height:1.5;color:#333;}}
.header div{{margin-bottom:4px;}}
.body p{{margin-bottom:0.8em;}}
.body table{{border-collapse:collapse;margin:0.5em 0;width:100%;table-layout:fixed;word-wrap:break-word;}}
.body td,.body th{{border:1px solid #ddd;padding:6px 10px;word-break:break-all;overflow-wrap:break-word;}}
</style>
</head><body>
<div class="header">
<div><b>发件人:</b> {sender_str}</div>
<div><b>发送时间:</b> {sent_at}</div>
<div><b>收件人:</b> {to_str}</div>
{cc_row}<div class="subject"><b>主题:</b> {subject_escaped}</div>
</div>
<div class="body">{full_body_html}</div>
</body></html>'''
    
    return full_email_html
