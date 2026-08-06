"""Render an Exchange email into sanitized, self-contained PDF HTML."""

from __future__ import annotations

import base64
import binascii
from datetime import datetime
import html
import logging
import re
from types import MappingProxyType
from typing import Any

from bs4 import BeautifulSoup

from src.security.html import bound_email_html, sanitize_email_html
from src.security.pdf import PdfAsset, pdf_asset_url, register_pdf_asset
from src.utils.mailbox_text import parse_serialized_mailbox


logger = logging.getLogger(__name__)
_MAX_ENCODED_INLINE_ASSET = 7_100_000
_INLINE_DATA_URL = re.compile(
    r"\Adata:(image/(?:gif|jpeg|png));base64,([A-Za-z0-9+/=\s]+)\Z",
    re.IGNORECASE,
)
_IMAGE_TYPES_BY_SUFFIX = {
    ".gif": "image/gif",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".png": "image/png",
}


class RenderedEmailHtml(str):
    """A string-compatible document carrying its closed PDF asset registry."""

    assets: MappingProxyType[str, PdfAsset]

    def __new__(
        cls,
        value: str,
        *,
        assets: dict[str, PdfAsset] | None = None,
    ) -> "RenderedEmailHtml":
        instance = super().__new__(cls, value)
        instance.assets = MappingProxyType(dict(assets or {}))
        return instance


def _format_datetime_cn(value: object) -> str:
    if not value or value == "Unknown Date":
        return "未知时间"

    raw = str(value).strip()
    try:
        if "T" in raw:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.strftime("%Y年%-m月%-d日 %H:%M")
    except (TypeError, ValueError):
        pass

    try:
        parsed = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        return parsed.strftime("%Y年%-m月%-d日 %H:%M")
    except (TypeError, ValueError):
        return raw


def _format_address_str(value: object) -> str:
    if not value:
        return ""
    raw = str(value).strip()

    parsed = parse_serialized_mailbox(raw)
    if parsed is not None and parsed.address:
        return (
            f"{html.escape(parsed.name)} &lt;{html.escape(parsed.address)}&gt;"
            if parsed.name
            else html.escape(parsed.address)
        )

    display = re.search(r"(.*?)\s*<(.+?)>", raw)
    if display:
        name, address = (part.strip() for part in display.groups())
        return (
            f"{html.escape(name)} &lt;{html.escape(address)}&gt;"
            if name
            else html.escape(address)
        )
    return html.escape(raw)


def _attachment_declared_type(attachment: dict[str, Any]) -> str | None:
    for key in ("mime_type", "content_type", "type"):
        value = attachment.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    name = attachment.get("name") or attachment.get("content_id") or ""
    lowered = str(name).casefold()
    for suffix, mime_type in _IMAGE_TYPES_BY_SUFFIX.items():
        if lowered.endswith(suffix):
            return mime_type
    return None


def _decode_inline_asset(attachment: dict[str, Any]) -> PdfAsset | None:
    encoded = attachment.get("content")
    if (
        not isinstance(encoded, str)
        or not encoded
        or len(encoded) > _MAX_ENCODED_INLINE_ASSET
    ):
        return None
    compact = "".join(encoded.split())
    try:
        decoded = base64.b64decode(compact, validate=True)
        return register_pdf_asset(decoded, _attachment_declared_type(attachment))
    except (binascii.Error, ValueError):
        return None


def _registered_data_url_asset(
    source: str,
    assets: dict[str, PdfAsset],
) -> PdfAsset | None:
    """Resolve only data URLs backed by this email's validated attachments."""

    match = _INLINE_DATA_URL.fullmatch(source)
    if match is None:
        return None
    encoded = match.group(2)
    if len(encoded) > _MAX_ENCODED_INLINE_ASSET:
        return None
    compact = "".join(encoded.split())
    try:
        candidate = register_pdf_asset(
            base64.b64decode(compact, validate=True),
            match.group(1).casefold(),
        )
    except (binascii.Error, ValueError):
        return None

    registered = assets.get(candidate.sha256)
    if (
        registered is None
        or registered.content != candidate.content
        or registered.mime_type != candidate.mime_type
    ):
        return None
    return registered


def _replace_inline_images(
    soup: BeautifulSoup,
    attachments: object,
) -> dict[str, PdfAsset]:
    by_content_id: dict[str, PdfAsset] = {}
    assets: dict[str, PdfAsset] = {}

    if isinstance(attachments, list):
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            content_id = attachment.get("content_id")
            if not isinstance(content_id, str) or not content_id.strip():
                continue
            asset = _decode_inline_asset(attachment)
            if asset is None:
                continue
            normalized = content_id.strip().strip("<>").casefold()
            by_content_id[normalized] = asset
            assets[asset.sha256] = asset

    for image in soup.find_all("img"):
        source = image.get("src")
        if not isinstance(source, str):
            continue
        if source.casefold().startswith("cid:"):
            content_id = source[4:].strip().strip("<>").casefold()
            asset = by_content_id.get(content_id)
        elif source.casefold().startswith("data:"):
            asset = _registered_data_url_asset(source, assets)
        else:
            continue
        if asset is None:
            image.decompose()
        else:
            image["src"] = pdf_asset_url(asset)

    return assets


def render_email_html(email_data: dict[str, Any]) -> RenderedEmailHtml:
    """Render safe HTML and retain only validated, in-memory CID image assets."""

    subject = str(email_data.get("subject") or "No Subject")
    raw_body = email_data.get("body") or "<i>No Content</i>"
    raw_body = raw_body if isinstance(raw_body, str) else str(raw_body)
    raw_body, source_truncated = bound_email_html(raw_body)

    soup = BeautifulSoup(raw_body, "html.parser")
    if soup.body is not None:
        soup = BeautifulSoup(soup.body.decode_contents(), "html.parser")
    assets = _replace_inline_images(soup, email_data.get("attachments", []))
    sanitized = sanitize_email_html(
        str(soup),
        allowed_asset_ids=assets,
        source_truncated=source_truncated,
    )
    body_html = sanitized.html or "<p><i>无可显示内容</i></p>"
    if sanitized.removed_elements or sanitized.removed_attributes or sanitized.truncated:
        logger.info(
            "Sanitized email HTML: removed_elements=%d removed_attributes=%d truncated=%s",
            sanitized.removed_elements,
            sanitized.removed_attributes,
            sanitized.truncated,
        )

    sender = _format_address_str(email_data.get("sender", ""))
    to_values = email_data.get("to", [])
    if isinstance(to_values, str):
        to_values = [to_values]
    cc_values = email_data.get("cc", [])
    if isinstance(cc_values, str):
        cc_values = [cc_values]
    to_rendered = "; ".join(_format_address_str(item) for item in to_values if item)
    cc_rendered = "; ".join(_format_address_str(item) for item in cc_values if item)
    sent_at = _format_datetime_cn(
        email_data.get("received_at")
        or email_data.get("datetime_received")
        or "Unknown Date"
    )
    subject_escaped = html.escape(subject)
    cc_row = (
        f'<div class="envelope-row"><b>抄送:</b> {cc_rendered}</div>'
        if cc_rendered
        else ""
    )

    document = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{subject_escaped}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{line-height:1.5;color:#333;}}
.envelope-row{{margin-bottom:4px;}}
.body p{{margin-bottom:0.8em;}}
.body table{{border-collapse:collapse;margin:0.5em 0;width:100%;table-layout:fixed;word-wrap:break-word;}}
.body td,.body th{{border:1px solid #ddd;padding:6px 10px;word-break:break-all;overflow-wrap:break-word;}}
</style>
</head><body>
<div class="header">
<div class="envelope-row"><b>发件人:</b> {sender}</div>
<div class="envelope-row"><b>发送时间:</b> {html.escape(sent_at)}</div>
<div class="envelope-row"><b>收件人:</b> {to_rendered}</div>
{cc_row}<div class="subject"><b>主题:</b> {subject_escaped}</div>
</div>
<div class="body">{body_html}</div>
</body></html>"""
    return RenderedEmailHtml(document, assets=assets)


__all__ = ["RenderedEmailHtml", "render_email_html"]
