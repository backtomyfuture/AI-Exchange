"""
PDF generation + Lark Drive upload flow.

Extracted from ``lark_app`` so the giant module stops growing and these CPU /
network heavy paths can be tested in isolation.

Both functions accept their Lark dependencies as keyword arguments rather than
reaching into module-level globals. ``lark_app`` keeps thin shim functions for
backwards compatibility with callers that have not yet been migrated.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional

from lark_oapi.api.im.v1 import (
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)

from src.utils.email_renderer import render_email_html
from src.utils.pdf_generator import convert_html_to_pdf

logger = logging.getLogger(__name__)


async def generate_and_upload_pdf(
    email_id: str,
    email_data: dict,
    *,
    upload_fn: Callable[[str, bytes, int], Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    """
    Render an email to PDF and upload it to Lark Drive.

    Args:
        email_id: Logical thread/email id (used in the file name only).
        email_data: Dict consumable by ``email_renderer.render_email_html``.
        upload_fn: Callable executing the Lark Drive upload. Signature must be
            ``(filename, content_bytes, size) -> {"url", "file_token"} | None``.
            Injected so tests can stub the Lark side effect.

    Returns:
        ``{"url": ..., "file_token": ...}`` on success, ``None`` on any failure.
    """
    try:
        logger.info("Starting PDF generation for %s", email_id)
        loop = asyncio.get_running_loop()

        html_content = await loop.run_in_executor(None, render_email_html, email_data)
        if html_content:
            logger.info("HTML content for PDF generated, size=%d bytes", len(html_content))
        else:
            logger.warning("HTML content for PDF is empty.")

        try:
            pdf_bytes = await loop.run_in_executor(None, convert_html_to_pdf, html_content)
        except Exception as pdf_err:
            logger.error("convert_html_to_pdf failed: %s", pdf_err, exc_info=True)
            return None

        if not pdf_bytes:
            logger.error("PDF generation returned empty bytes.")
            return None

        filename = f"Email_Export_{email_id}.pdf"
        logger.info("Uploading PDF: %s (size=%d)", filename, len(pdf_bytes))

        try:
            upload_resp = await loop.run_in_executor(
                None, upload_fn, filename, pdf_bytes, len(pdf_bytes)
            )
        except Exception as up_err:
            logger.error("Lark Drive upload failed: %s", up_err, exc_info=True)
            return None

        if not upload_resp:
            logger.error("PDF upload returned empty response.")
            return None

        return {
            "url": upload_resp.get("url"),
            "file_token": upload_resp.get("file_token"),
        }
    except Exception as exc:
        logger.error("Error in generate_and_upload_pdf: %s", exc, exc_info=True)
        return None


async def process_pdf_generation_and_reply(
    email_id: str,
    state: Any,
    message_id: str,
    *,
    graph: Any,
    lark_api_client: Any,
    upload_fn: Callable[[str, bytes, int], Optional[Dict[str, Any]]],
    safe_async_wait: Callable,
) -> None:
    """
    Generate the PDF, persist its file_token in graph state, and reply with a
    Lark card containing the open-PDF button.

    Dependencies are injected so we can mock graph / Lark / upload in tests.
    """
    try:
        email_data = state.values.get("email", {})
        result = await generate_and_upload_pdf(email_id, email_data, upload_fn=upload_fn)
        if not result:
            return

        file_url = result["url"]
        file_token = result["file_token"]

        config = {"configurable": {"thread_id": email_id}}
        safe_async_wait(graph.aupdate_state(config, {"pdf_token": file_token}))

        filename = f"Email_Export_{email_id}.pdf"
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
                        "content": f"点击下方按钮查看 PDF 文件：\nFilename: *{filename}*",
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

        if not lark_api_client:
            logger.warning("Lark API client not configured; skipping PDF reply.")
            return

        req_msg = (
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
        lark_api_client.im.v1.message.reply(req_msg)
        logger.info("PDF reply sent successfully for %s.", email_id)
    except Exception as exc:
        logger.error("Error in PDF generation process: %s", exc, exc_info=True)
