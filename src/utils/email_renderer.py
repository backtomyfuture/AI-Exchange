import html
import logging
from typing import Dict, Any, List
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)

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

def render_email_html(email_data: Dict[str, Any]) -> str:
    """
    Render email data into a standalone HTML string (Outlook style).
    Handles CID image replacement.
    """
    subject = email_data.get("subject", "No Subject")
    full_body_html = email_data.get("body", "<i>No Content</i>")
    
    # Construct Email Headers for Outlook-like view
    sender_str = _format_address_str(email_data.get('sender', ''))
    
    to_list = email_data.get("to", [])
    if isinstance(to_list, str): to_list = [to_list]
    to_str = "; ".join([_format_address_str(x) for x in to_list])
    
    cc_list = email_data.get("cc", [])
    if isinstance(cc_list, str): cc_list = [cc_list]
    cc_str = "; ".join([_format_address_str(x) for x in cc_list])
    
    sent_at = email_data.get("received_at") or email_data.get("datetime_received") or "Unknown Date"
    
    subject_escaped = html.escape(subject)
    
    # Process CID Images
    # If attachments have content, use it to replace cid: references
    attachments = email_data.get("attachments", [])
    if attachments and "cid:" in full_body_html:
        try:
            soup = BeautifulSoup(full_body_html, 'html.parser')
            
            # Map CID to content
            cid_map = {}
            for att in attachments:
                if att.get("content_id") and att.get("content"):
                    cid_map[att["content_id"]] = att["content"]
            
            for img in soup.find_all('img'):
                src = img.get('src', '')
                if src.startswith('cid:'):
                    cid = src[4:].strip('<>')
                    if cid in cid_map:
                        # Determine mime type (simple guess)
                        mime_type = "image/png" # Default
                        if cid.endswith(".jpg") or cid.endswith(".jpeg"): mime_type = "image/jpeg"
                        elif cid.endswith(".gif"): mime_type = "image/gif"
                        
                        img['src'] = f"data:{mime_type};base64,{cid_map[cid]}"
            
            full_body_html = str(soup)
        except Exception as e:
            logger.error(f"Error processing inline images: {e}")

    header_html = f"""
    <div style="font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; font-size: 14px; color: #333; margin-bottom: 20px;">
        <div style="border-bottom: 1px solid #e5e5e5; padding-bottom: 10px; background-color: #f9f9f9; padding: 15px;">
            <div style="margin-bottom: 5px;"><b>发件人:</b> {sender_str}</div>
            <div style="margin-bottom: 5px;"><b>发送时间:</b> {sent_at}</div>
            <div style="margin-bottom: 5px;"><b>收件人:</b> {to_str}</div>
            {f'<div style="margin-bottom: 5px;"><b>抄送:</b> {cc_str}</div>' if cc_str else ''}
            <div style="margin-top: 10px; font-size: 16px;"><b>主题:</b> {subject_escaped}</div>
        </div>
    </div>
    """
    
    # Combine
    full_email_html = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{subject_escaped}</title>
        <style>
            body {{ font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; font-size: 14px; line-height: 1.5; margin: 0; padding: 0; }}
            .container {{ padding: 15px; }}
            img {{ max-width: 100%; height: auto; }}
        </style>
    </head>
    <body>
        <div class="container">
            {header_html}
            <div>
                {full_body_html}
            </div>
        </div>
    </body>
    </html>
    """
    
    return full_email_html
