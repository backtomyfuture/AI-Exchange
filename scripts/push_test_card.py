#!/usr/bin/env python3
"""从 EML 发送显式隔离的 ``test_push_`` 飞书测试卡片。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import sys
from collections.abc import Mapping
from copy import deepcopy
from email import policy
from email.header import decode_header
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import quote
from uuid import uuid4

from dotenv import load_dotenv

# Allow direct execution from the repository root or scripts directory.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import get_settings
from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import build_initial_graph_state, sanitize_graph_delta
from src.storage import ContentRef
from src.utils import lark_app, lark_pdf_flow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestPush")


class TestCardPdfFlowError(RuntimeError):
    """Fail-closed signal retaining handles for explicit operator recovery."""

    def __init__(
        self,
        status: str,
        *,
        cleanup_tokens: tuple[str, ...] = (),
        protected_tokens: tuple[str, ...] = (),
    ) -> None:
        super().__init__(f"test_card_pdf_unresolved:{status}")
        self.status = status
        self.cleanup_tokens = cleanup_tokens
        self.protected_tokens = protected_tokens


class _TestCardContentStore:
    """仅供 ``test_push_`` PDF 渲染使用的进程内内容边界。"""

    def __init__(self, ref: ContentRef, email: dict[str, Any]):
        self._ref = ref
        self._email = deepcopy(email)

    async def load_email(
        self,
        ref: ContentRef,
        *,
        include_attachments: bool = False,
    ) -> dict[str, Any]:
        if ref != self._ref:
            raise KeyError("test_card_content_ref_not_found")
        email = deepcopy(self._email)
        if not include_attachments:
            for attachment in email.get("attachments", []):
                if isinstance(attachment, dict):
                    attachment.pop("content", None)
        return email


class _TestCardDraftStore:
    """测试卡片专用草稿边界，不接触生产数据库。"""

    def __init__(self, email_id: str, draft: str):
        self._drafts = {email_id: draft}

    async def save_draft(self, email_id: str, content: str) -> str:
        self._drafts[email_id] = content
        return email_id

    async def load_draft(self, draft_id: str) -> str:
        return self._drafts[draft_id]


def _require_test_email_id(email_data: dict[str, Any]) -> str:
    email_id = email_data.get("id")
    if not isinstance(email_id, str) or not email_id.startswith("test_push_"):
        raise ValueError("test_card_id_must_start_with_test_push")
    return email_id


def build_test_pdf_boundary(
    email_data: dict[str, Any],
    *,
    draft: str,
) -> tuple[SimpleNamespace, GraphDependencies]:
    """创建供当前 PDF API 使用的瘦 State 和显式进程内依赖。"""
    email = deepcopy(email_data)
    email_id = _require_test_email_id(email)
    canonical = json.dumps(
        email,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    ref = ContentRef(
        account_id=get_settings().EXCHANGE_ACCOUNT_ID,
        object_id=str(uuid4()),
        key_version="v1",
        sha256=hashlib.sha256(canonical).hexdigest(),
    )
    metadata = {
        **email,
        "draft_to": list(email.get("to") or []),
        "draft_cc": list(email.get("cc") or []),
    }
    values = build_initial_graph_state(metadata, ref)
    values.update(sanitize_graph_delta(values, {"draft_id": email_id}))
    dependencies = GraphDependencies(
        content_store=_TestCardContentStore(ref, email),
        drafts=_TestCardDraftStore(email_id, draft),
    )
    return SimpleNamespace(values=values), dependencies


async def generate_test_card_pdf(
    email_data: dict[str, Any],
    *,
    draft: str,
    upload_fn: Callable[[str, bytes, int], dict[str, Any] | None] | None = None,
    delete_fn: Callable[[str], bool] | None = None,
) -> dict[str, Any] | lark_pdf_flow.PdfFlowOutcome | None:
    """通过显式测试依赖调用当前 PDF 流程，不读取生产 Graph 或数据库。"""
    state, dependencies = build_test_pdf_boundary(email_data, draft=draft)
    return await lark_pdf_flow.generate_and_upload_pdf(
        email_data["id"],
        state,
        dependencies=dependencies,
        upload_fn=upload_fn or lark_app.upload_file_to_drive,
        delete_fn=delete_fn or lark_app.delete_file_from_drive,
    )


async def resolve_test_card_pdf_result(
    result: dict[str, Any] | lark_pdf_flow.PdfFlowOutcome | None,
    *,
    delete_fn: Callable[[str], bool],
) -> tuple[str | None, str | None]:
    """Resolve a PDF result without discarding cleanup/protected handles."""
    if result is None:
        return None, None
    if isinstance(result, Mapping):
        url = result.get("url")
        token = result.get("file_token")
        if (
            isinstance(url, str)
            and url
            and isinstance(token, str)
            and token
        ):
            return url, token
        protected_tokens = (token,) if isinstance(token, str) and token else ()
        raise TestCardPdfFlowError(
            "invalid_result",
            protected_tokens=protected_tokens,
        )
    if not isinstance(result, lark_pdf_flow.PdfFlowOutcome):
        raise TestCardPdfFlowError("unknown_result_type")

    if result.protected_tokens:
        raise TestCardPdfFlowError(
            result.status,
            cleanup_tokens=result.cleanup_tokens,
            protected_tokens=result.protected_tokens,
        )

    unresolved: list[str] = []
    for token in result.cleanup_tokens:
        try:
            deleted = await asyncio.to_thread(delete_fn, token)
        except Exception as exc:
            logger.error(
                "Test PDF cleanup retry failed: error_type=%s",
                type(exc).__name__,
            )
            deleted = False
        if not deleted:
            unresolved.append(token)

    if unresolved or not result.cleanup_tokens:
        raise TestCardPdfFlowError(
            result.status,
            cleanup_tokens=tuple(unresolved),
        )

    logger.warning(
        "Test PDF generation failed but cleanup was reconciled: status=%s count=%d",
        result.status,
        len(result.cleanup_tokens),
    )
    return None, None


def register_test_card_state(
    email_data: dict[str, Any],
    *,
    draft: str,
    context: list[dict[str, Any]],
    classification: dict[str, Any],
    attachment_tokens: list[str] | None = None,
    pdf_token: str | None = None,
) -> None:
    """把完整测试数据放入明确的 ``test_push_`` 内存区，而非 Graph State。"""
    email_id = _require_test_email_id(email_data)
    lark_app._mock_store[email_id] = SimpleNamespace(
        values={
            "email": deepcopy(email_data),
            "draft": draft,
            "context": deepcopy(context),
            "classification": deepcopy(classification),
            "attachment_tokens": list(attachment_tokens or []),
            "pdf_token": pdf_token,
        }
    )


def decode_str(header_value: Any) -> str:
    if not header_value:
        return ""
    result = ""
    for data, encoding in decode_header(header_value):
        if not isinstance(data, bytes):
            result += str(data)
            continue
        if encoding:
            try:
                result += data.decode(encoding)
                continue
            except (LookupError, UnicodeDecodeError):
                pass
        try:
            result += data.decode("utf-8")
        except UnicodeDecodeError:
            result += data.decode("gb18030", errors="replace")
    return result


def _extract_body(message: Any) -> str:
    for content_type in ("text/html", "text/plain"):
        for part in message.walk():
            if part.get_content_type() != content_type:
                continue
            try:
                return str(part.get_content())
            except (LookupError, UnicodeDecodeError):
                payload = part.get_payload(decode=True) or b""
                return payload.decode("gb18030", errors="replace")
    return ""


async def _inject_debug_original(
    email_data: dict[str, Any],
    *,
    draft: str,
    context: list[dict[str, Any]],
    classification: dict[str, Any],
    attachment_tokens: list[str],
    pdf_token: str | None,
) -> bool:
    """可选地把测试原文送入启用 DEBUG 的本地服务进程。"""
    try:
        import requests

        external_url = os.getenv("EXTERNAL_URL", "http://localhost:8000")
        debug_url = f"{external_url}/debug/inject_email"
        logger.info("Injecting mock email to debug endpoint")
        response = await asyncio.to_thread(
            requests.post,
            debug_url,
            json={
                **email_data,
                "draft": draft,
                "context": context,
                "classification": classification,
                "attachment_tokens": attachment_tokens,
                "pdf_token": pdf_token,
                "recipient_candidates": {"to": [], "cc": []},
            },
            timeout=5,
        )
        response.raise_for_status()
        logger.info("Mock email injected successfully")
        return True
    except Exception as exc:
        logger.warning(
            "Mock email injection failed: error_type=%s",
            type(exc).__name__,
        )
        return False


async def _cleanup_test_card_files(
    tokens: list[str],
    *,
    delete_fn: Callable[[str], bool],
) -> None:
    for token in dict.fromkeys(item for item in tokens if item):
        try:
            deleted = await asyncio.to_thread(delete_fn, token)
        except Exception as exc:
            logger.error(
                "Test card file cleanup failed: error_type=%s",
                type(exc).__name__,
            )
            deleted = False
        if not deleted:
            logger.error("Test card file cleanup remains unresolved")


async def _remove_debug_original(email_id: str) -> bool:
    """Best-effort removal of a test state after confirmed card-send failure."""
    try:
        import requests

        external_url = os.getenv("EXTERNAL_URL", "http://localhost:8000")
        debug_url = (
            f"{external_url}/debug/inject_email/{quote(email_id, safe='')}"
        )
        response = await asyncio.to_thread(
            requests.delete,
            debug_url,
            timeout=5,
        )
        response.raise_for_status()
        return True
    except Exception as exc:
        logger.warning(
            "Mock email cleanup failed: error_type=%s",
            type(exc).__name__,
        )
        return False


async def main() -> None:
    logger.info("Loading environment variables...")
    load_dotenv()
    get_settings.cache_clear()

    app_id = os.getenv("LARK_APP_ID")
    chat_id = os.getenv("LARK_CHAT_ID")
    if not app_id or not chat_id:
        logger.error("LARK_APP_ID or LARK_CHAT_ID is missing")
        return

    logger.info("Initializing isolated Lark test client")
    lark_app.init_lark_app(None, None, None, dependencies=None)
    if not lark_app.lark_api_client:
        logger.error("Failed to initialize Lark API Client")
        return

    eml_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "tests",
            "fixtures",
            "synthetic_notification.eml",
        )
    )
    if not os.path.exists(eml_path):
        logger.error("EML fixture was not found")
        return

    import email

    logger.info("Parsing EML fixture")
    with open(eml_path, "rb") as file_obj:
        message = email.message_from_binary_file(file_obj, policy=policy.default)

    subject = decode_str(message["subject"])
    sender = decode_str(message["from"])
    to_list = [decode_str(item).strip() for item in str(message["to"] or "").split(",") if item]
    cc_list = [decode_str(item).strip() for item in str(message["cc"] or "").split(",") if item]

    attachments: list[dict[str, Any]] = []
    attachment_tokens: list[str] = []
    for part in message.walk():
        if part.get_content_maintype() == "multipart":
            continue
        if part.get_content_disposition() is None and part.get("Content-ID") is None:
            continue
        content_bytes = part.get_payload(decode=True)
        if not content_bytes:
            continue

        filename = decode_str(part.get_filename() or "untitled")
        content_id = part.get("Content-ID", "").strip("<>")
        logger.info(
            "Found attachment: bytes=%d has_cid=%s",
            len(content_bytes),
            bool(content_id),
        )
        upload = lark_app.upload_file_to_drive(
            filename,
            content_bytes,
            len(content_bytes),
        )
        attachment = {
            "name": filename,
            "content_id": content_id,
            "content": base64.b64encode(content_bytes).decode("utf-8"),
            "size": len(content_bytes),
        }
        if upload:
            attachment["lark_file_url"] = upload["url"]
            attachment["lark_file_token"] = upload["file_token"]
            attachment_tokens.append(upload["file_token"])
        attachments.append(attachment)

    email_data = {
        "id": "test_push_REAL_EML",
        "subject": subject,
        "sender": sender,
        "to": to_list,
        "cc": cc_list,
        "received_at": str(message["date"]),
        "body": _extract_body(message) or "No body content found.",
        "attachments": attachments,
    }
    classification = {"reasoning": "This is a test notification sent manually."}
    context = [{"chunk_text": "Flight details preview..."}]
    draft = "Thank you, I have received the update."

    logger.info("Generating PDF through the isolated test boundary")
    pdf_result = await generate_test_card_pdf(email_data, draft=draft)
    pdf_url, pdf_token = await resolve_test_card_pdf_result(
        pdf_result,
        delete_fn=lark_app.delete_file_from_drive,
    )
    register_test_card_state(
        email_data,
        draft=draft,
        context=context,
        classification=classification,
        attachment_tokens=attachment_tokens,
        pdf_token=pdf_token,
    )
    injected = await _inject_debug_original(
        email_data,
        draft=draft,
        context=context,
        classification=classification,
        attachment_tokens=attachment_tokens,
        pdf_token=pdf_token,
    )
    if not injected:
        await _remove_debug_original(email_data["id"])
        await _cleanup_test_card_files(
            [*attachment_tokens, *([pdf_token] if pdf_token else [])],
            delete_fn=lark_app.delete_file_from_drive,
        )
        lark_app._mock_store.pop(email_data["id"], None)
        raise RuntimeError("test_card_debug_injection_failed")

    logger.info("Sending test card")
    sent = lark_app.send_approval_card(
        email_id=email_data["id"],
        draft=draft,
        context=context,
        email_data=email_data,
        classification=classification,
        pdf_url=pdf_url,
    )
    if sent:
        logger.info("Test card sent successfully")
    else:
        logger.error("Test card delivery failed")
        await _remove_debug_original(email_data["id"])
        await _cleanup_test_card_files(
            [*attachment_tokens, *([pdf_token] if pdf_token else [])],
            delete_fn=lark_app.delete_file_from_drive,
        )
        lark_app._mock_store.pop(email_data["id"], None)
        raise RuntimeError("test_card_delivery_failed")


if __name__ == "__main__":
    asyncio.run(main())
