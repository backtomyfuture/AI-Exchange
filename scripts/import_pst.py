#!/usr/bin/env python3
"""
邮件导入工具 —— 将 .pst / .mbox / .eml / Exchange 服务器邮件批量解析并存入 Qdrant。

快速开始 (使用 uv，无需手动建虚拟环境):
    uv run scripts/import_pst.py archive.pst --dry-run
    uv run scripts/import_pst.py archive.pst
    uv run scripts/import_pst.py --source exchange --dry-run

完整说明见 docs/history-import-and-skill-discovery.md
"""

from __future__ import annotations

import argparse
import asyncio
import email as email_lib
import email.policy
import email.utils
import hashlib
import logging
import mailbox
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pst_import")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ParsedEmail:
    """Intermediate representation of a parsed email."""

    id: str
    subject: str
    sender: str
    to: list[str]
    cc: list[str]
    body: str
    received_at: str
    source_folder: str = ""
    message_type: str = "received"
    in_reply_to: str = ""
    conversation_id: str = ""
    attachments_metadata: list[dict] = field(default_factory=list)
    import_source: str = "pst_import"

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "subject": self.subject,
            "sender": self.sender,
            "to": self.to,
            "cc": self.cc,
            "body": self.body,
            "received_at": self.received_at,
            "source_folder": self.source_folder,
            "type": self.message_type,
            "in_reply_to": self.in_reply_to,
            "thread_id": self.conversation_id,
            "attachments": [],
            "attachments_metadata": self.attachments_metadata,
            "_import_source": self.import_source,
        }


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _generate_email_id(raw: bytes | str) -> str:
    """Deterministic ID from content hash."""
    data = raw if isinstance(raw, bytes) else raw.encode("utf-8", errors="replace")
    return f"pst_{hashlib.sha256(data).hexdigest()[:20]}"


def _parse_address_list(raw: str | None) -> list[str]:
    """Split a comma-separated address header into a list."""
    if not raw:
        return []
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def _parse_date(raw: str | None) -> str:
    """Parse email date header to ISO format."""
    if not raw:
        return datetime.now().isoformat()
    try:
        dt = email_lib.utils.parsedate_to_datetime(raw)
        return dt.isoformat()
    except Exception:
        return datetime.now().isoformat()


def _extract_body(msg: email_lib.message.Message) -> str:
    """Extract the best available body (prefer HTML, fallback to plain)."""
    html_body = ""
    text_body = ""

    if not msg.is_multipart():
        ct = msg.get_content_type()
        try:
            content = msg.get_content()
        except Exception:
            content = ""
        if ct == "text/html":
            return content or ""
        return content or ""

    for part in msg.walk():
        ct = part.get_content_type()
        if ct == "text/html" and not html_body:
            try:
                html_body = part.get_content()
            except Exception:
                pass
        elif ct == "text/plain" and not text_body:
            try:
                text_body = part.get_content()
            except Exception:
                pass

    return html_body or text_body or ""


def _extract_attachments_metadata(msg: email_lib.message.Message) -> list[dict]:
    """Extract attachment metadata without content."""
    result = []
    if not msg.is_multipart():
        return result
    for part in msg.iter_attachments():
        filename = part.get_filename() or "unknown"
        ct = part.get_content_type()
        try:
            payload = part.get_payload(decode=True)
            size = len(payload) if payload else 0
        except Exception:
            size = 0
        result.append({"name": filename, "content_type": ct, "size": size})
    return result


def _infer_folder_type(folder_name: str) -> str:
    """Infer message type from folder name."""
    lower = folder_name.lower()
    sent_keywords = ["sent", "已发送", "发件箱", "outbox"]
    draft_keywords = ["draft", "草稿"]
    if any(kw in lower for kw in sent_keywords):
        return "sent"
    if any(kw in lower for kw in draft_keywords):
        return "draft"
    return "received"


def parse_email_message(
    msg: email_lib.message.Message,
    folder: str = "",
    raw_bytes: bytes | None = None,
) -> Optional[ParsedEmail]:
    """Parse a stdlib email.message.Message into ParsedEmail."""
    try:
        msg_id = msg.get("Message-ID", "")
        if raw_bytes:
            eid = _generate_email_id(raw_bytes)
        elif msg_id:
            eid = _generate_email_id(msg_id)
        else:
            eid = _generate_email_id(msg.as_string())

        message_type = _infer_folder_type(folder)
        in_reply_to = msg.get("In-Reply-To", "") or ""
        references = msg.get("References", "") or ""
        conversation_id = in_reply_to.split()[0] if in_reply_to else (
            references.split()[0] if references else ""
        )

        return ParsedEmail(
            id=eid,
            subject=msg.get("Subject", "(无主题)") or "(无主题)",
            sender=msg.get("From", "unknown") or "unknown",
            to=_parse_address_list(msg.get("To")),
            cc=_parse_address_list(msg.get("Cc")),
            body=_extract_body(msg),
            received_at=_parse_date(msg.get("Date")),
            source_folder=folder,
            message_type=message_type,
            in_reply_to=in_reply_to,
            conversation_id=conversation_id,
            attachments_metadata=_extract_attachments_metadata(msg),
        )
    except Exception as e:
        logger.warning("Failed to parse email: %s", e)
        return None


# ---------------------------------------------------------------------------
# Source iterators
# ---------------------------------------------------------------------------

def iter_from_mbox(mbox_path: Path, folder: str = "") -> Iterator[ParsedEmail]:
    """Yield ParsedEmails from an mbox file."""
    if not folder:
        folder = mbox_path.stem
    mbox = mailbox.mbox(str(mbox_path))
    for msg in mbox:
        raw = msg.as_bytes()
        parsed_msg = email_lib.message_from_bytes(raw, policy=email_lib.policy.default)
        result = parse_email_message(parsed_msg, folder=folder, raw_bytes=raw)
        if result:
            yield result


def iter_from_eml(eml_path: Path, folder: str = "") -> Iterator[ParsedEmail]:
    """Yield a single ParsedEmail from an .eml file."""
    raw = eml_path.read_bytes()
    msg = email_lib.message_from_bytes(raw, policy=email_lib.policy.default)
    result = parse_email_message(msg, folder=folder, raw_bytes=raw)
    if result:
        yield result


def iter_from_eml_dir(dir_path: Path) -> Iterator[ParsedEmail]:
    """Yield ParsedEmails from all .eml files in a directory (recursive)."""
    for eml_file in sorted(dir_path.rglob("*.eml")):
        folder = eml_file.parent.name if eml_file.parent != dir_path else ""
        yield from iter_from_eml(eml_file, folder=folder)


def _iter_from_pst_pypff(pst_path: Path) -> Iterator[ParsedEmail]:
    """Parse PST using pypff (libpff-python) — pure pip, no system deps."""
    import pypff

    pst = pypff.file()
    pst.open(str(pst_path))

    try:
        root = pst.get_root_folder()
        yield from _walk_pypff_folder(root, "")
    finally:
        pst.close()


def _walk_pypff_folder(folder, parent_path: str) -> Iterator[ParsedEmail]:
    """Recursively walk pypff folders and yield emails."""
    folder_name = folder.name or ""
    current_path = (
        f"{parent_path}/{folder_name}" if parent_path else folder_name
    )
    display_folder = folder_name or "Root"
    message_type = _infer_folder_type(current_path)

    for i in range(folder.number_of_sub_messages):
        try:
            msg = folder.get_sub_message(i)
            parsed = _pypff_message_to_email(msg, display_folder, message_type)
            if parsed:
                yield parsed
        except Exception as e:
            logger.debug("Skipping message %d in %s: %s", i, display_folder, e)

    for i in range(folder.number_of_sub_folders):
        try:
            sub = folder.get_sub_folder(i)
            yield from _walk_pypff_folder(sub, current_path)
        except Exception as e:
            logger.debug("Skipping subfolder %d in %s: %s", i, display_folder, e)


def _pypff_message_to_email(
    msg, folder: str, message_type: str,
) -> Optional[ParsedEmail]:
    """Convert a pypff message object to ParsedEmail."""
    try:
        subject = msg.subject or "(无主题)"
        sender = getattr(msg, "sender_name", "") or ""
        sender_email = getattr(msg, "sender_email_address", "")
        if not sender:
            sender = sender_email or "unknown"
        elif sender_email:
            sender = f"{sender} <{sender_email}>"

        body = msg.plain_text_body or ""
        if isinstance(body, bytes):
            body = body.decode("utf-8", errors="replace")
        html_body = msg.html_body or ""
        if isinstance(html_body, bytes):
            html_body = html_body.decode("utf-8", errors="replace")
        best_body = html_body or body

        received_at = ""
        if msg.delivery_time:
            received_at = msg.delivery_time.isoformat()
        elif msg.client_submit_time:
            received_at = msg.client_submit_time.isoformat()
        if not received_at:
            received_at = datetime.now().isoformat()

        # Build transport headers for reply detection
        headers = msg.transport_headers or ""
        if isinstance(headers, bytes):
            headers = headers.decode("utf-8", errors="replace")

        in_reply_to = ""
        conversation_id = ""
        to_list: list[str] = []
        cc_list: list[str] = []

        if headers:
            hdr_msg = email_lib.message_from_string(headers)
            in_reply_to = hdr_msg.get("In-Reply-To", "") or ""
            refs = hdr_msg.get("References", "") or ""
            conversation_id = (
                in_reply_to.split()[0] if in_reply_to
                else (refs.split()[0] if refs else "")
            )
            to_list = _parse_address_list(hdr_msg.get("To"))
            cc_list = _parse_address_list(hdr_msg.get("Cc"))

        # Attachment metadata
        attachments_meta: list[dict] = []
        try:
            for j in range(msg.number_of_attachments):
                att = msg.get_attachment(j)
                name = att.name or f"attachment_{j}"
                size = att.size if hasattr(att, "size") else 0
                attachments_meta.append({
                    "name": name,
                    "content_type": "application/octet-stream",
                    "size": size,
                })
        except Exception:
            pass

        eid = _generate_email_id(
            f"{subject}|{sender}|{received_at}|{folder}"
        )

        return ParsedEmail(
            id=eid,
            subject=subject,
            sender=sender,
            to=to_list,
            cc=cc_list,
            body=best_body,
            received_at=received_at,
            source_folder=folder,
            message_type=message_type,
            in_reply_to=in_reply_to,
            conversation_id=conversation_id,
            attachments_metadata=attachments_meta,
        )
    except Exception as e:
        logger.debug("Failed to convert pypff message: %s", e)
        return None


def _discover_mbox_files(directory: Path) -> list[Path]:
    """Find all mbox-like files in a readpst output directory."""
    results = []
    for f in sorted(directory.rglob("*")):
        if f.is_file() and not f.name.startswith("."):
            if f.suffix in (".mbox", "") and f.stat().st_size > 0:
                results.append(f)
    return results


def _iter_from_pst_readpst(pst_path: Path) -> Iterator[ParsedEmail]:
    """Fallback: extract PST via readpst CLI, then parse mbox/eml output."""
    if shutil.which("readpst") is None:
        if sys.platform == "darwin":
            msg = "readpst 未安装。Mac 平台请运行: brew install libpst"
        else:
            msg = "readpst 未安装。Linux 平台请运行: sudo apt install pst-utils"
        logger.error(msg)
        sys.exit(1)

    with tempfile.TemporaryDirectory(prefix="pst_import_") as tmpdir:
        tmpdir_path = Path(tmpdir)
        logger.info("readpst: 解压到 %s", tmpdir_path)

        result = subprocess.run(
            ["readpst", "-r", "-8", "-o", str(tmpdir_path), str(pst_path)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            logger.error("readpst 失败: %s", result.stderr.strip())
            sys.exit(1)

        mbox_files = _discover_mbox_files(tmpdir_path)
        if mbox_files:
            for mbox_file in mbox_files:
                folder = mbox_file.parent.name or mbox_file.stem
                yield from iter_from_mbox(mbox_file, folder=folder)
            return

        eml_files = list(tmpdir_path.rglob("*.eml"))
        if eml_files:
            for eml_file in sorted(eml_files):
                folder = eml_file.parent.name
                yield from iter_from_eml(eml_file, folder=folder)
            return

        logger.warning("readpst 解压后未找到邮件文件")


def iter_from_pst(pst_path: Path) -> Iterator[ParsedEmail]:
    """Parse a .pst file. Uses pypff (pip) first, readpst as fallback."""
    try:
        import pypff  # noqa: F401
        logger.info("使用 pypff 解析 PST 文件 (纯 Python)")
        yield from _iter_from_pst_pypff(pst_path)
    except ImportError:
        logger.info("pypff 未安装，尝试 readpst 后备方案")
        logger.info("提示: pip install libpff-python 可避免系统依赖")
        yield from _iter_from_pst_readpst(pst_path)


# ---------------------------------------------------------------------------
# Exchange server source
# ---------------------------------------------------------------------------

def _exchange_item_to_parsed_email(
    item: dict, folder: str = "",
) -> Optional[ParsedEmail]:
    """Convert an Exchange API email dict to ParsedEmail."""
    try:
        subject = item.get("subject", "(无主题)") or "(无主题)"
        sender = _parse_exchange_sender(
            item.get("sender") or item.get("from")
        )
        to_raw = item.get("to", [])
        cc_raw = item.get("cc", [])

        # Handle both list and string formats
        if isinstance(to_raw, str):
            to_list = _parse_address_list(to_raw)
        else:
            to_list = [str(a) for a in to_raw] if to_raw else []
        if isinstance(cc_raw, str):
            cc_list = _parse_address_list(cc_raw)
        else:
            cc_list = [str(a) for a in cc_raw] if cc_raw else []

        body = item.get("body", "") or ""
        received_at = (
            item.get("received_at")
            or item.get("received_time")
            or item.get("date", "")
            or ""
        )
        if not received_at:
            received_at = datetime.now().isoformat()

        message_type = _infer_folder_type(folder)
        in_reply_to = item.get("in_reply_to", "") or ""
        conversation_id = item.get("conversation_id") or item.get("thread_id", "") or ""
        if not conversation_id and in_reply_to:
            conversation_id = in_reply_to.split()[0]

        # Attachment metadata
        attachments_meta: list[dict] = []
        for att in item.get("attachments", []) or []:
            if isinstance(att, dict):
                attachments_meta.append({
                    "name": att.get("name", "unknown"),
                    "content_type": att.get("content_type", "application/octet-stream"),
                    "size": att.get("size", 0),
                })

        # Build deterministic ID from Exchange email ID or content
        exchange_id = item.get("id", "")
        if exchange_id:
            eid = f"exc_{hashlib.sha256(exchange_id.encode()).hexdigest()[:20]}"
        else:
            eid = f"exc_{hashlib.sha256(f'{subject}|{sender}|{received_at}'.encode()).hexdigest()[:20]}"

        return ParsedEmail(
            id=eid,
            subject=subject,
            sender=sender,
            to=to_list,
            cc=cc_list,
            body=body,
            received_at=received_at,
            source_folder=folder,
            message_type=message_type,
            in_reply_to=in_reply_to,
            conversation_id=conversation_id,
            attachments_metadata=attachments_meta,
            import_source="exchange_import",
        )
    except Exception as e:
        logger.warning("Failed to convert Exchange email: %s", e)
        return None


def _parse_exchange_sender(raw_sender: str | None) -> str:
    """Parse Exchange sender which may be Mailbox(...) format or plain string."""
    if not raw_sender:
        return "unknown"
    # Handle Mailbox(name='...', email_address='...', ...) format
    import re
    email_match = re.search(r"email_address='([^']*)'", str(raw_sender))
    name_match = re.search(r"name='([^']*)'", str(raw_sender))
    if email_match:
        email_addr = email_match.group(1)
        name = name_match.group(1) if name_match else ""
        if name:
            return f"{name} <{email_addr}>"
        return email_addr
    return str(raw_sender)


def _fetch_exchange_emails(
    folder: str = "INBOX",
    limit: int = 0,
    all_mail: bool = False,
) -> list[ParsedEmail]:
    """Fetch emails from Exchange server via synchronous HTTP calls.

    Uses offset-based pagination (small page size) to avoid Exchange gateway
    504 timeouts.  ``limit=0`` means fetch all available emails.
    """
    from src.config import get_settings, resolve_secret
    import httpx

    PAGE_SIZE = 5  # Exchange gateway times out on larger requests

    settings = get_settings()
    api_url = settings.EXCHANGE_API_URL.rstrip("/")
    api_key = resolve_secret(settings.EXCHANGE_API_KEY)
    account_id = settings.EXCHANGE_ACCOUNT_ID
    ssl_verify = settings.EXCHANGE_SSL_VERIFY

    headers = {"X-API-KEY": api_key} if api_key else {}

    base_params: dict = {
        "account_id": account_id,
        "folder": folder,
    }
    if not all_mail:
        base_params["unread_only"] = "True"

    results: list[ParsedEmail] = []
    fetch_all = (limit == 0)
    max_fetch = limit if limit > 0 else 999_999  # effectively unlimited

    with httpx.Client(
        verify=ssl_verify,
        headers=headers,
        timeout=httpx.Timeout(120.0, connect=10.0),
    ) as http:
        list_url = f"{api_url}/list"
        offset = 0
        page = 0
        total_on_server: int | None = None

        while len(results) < max_fetch:
            page_size = min(PAGE_SIZE, max_fetch - len(results))
            params = {**base_params, "limit": page_size, "offset": offset}

            page += 1
            target_desc = "全部" if fetch_all else str(limit)
            logger.info(
                "正在拉取 Exchange 邮件 (第 %d 页, offset=%d, 已获取 %d/%s)...",
                page, offset, len(results), target_desc,
            )

            try:
                response = http.get(list_url, params=params)
            except Exception as e:
                logger.warning("Exchange 列表请求失败 [%s]: %s", type(e).__name__, e)
                break

            if response.status_code != 200:
                logger.warning(
                    "列表获取失败: %s - %s", response.status_code, response.text[:200],
                )
                break

            data = response.json().get("data", {})
            items = data.get("items", [])

            if total_on_server is None:
                total_on_server = data.get("total", 0)
                logger.info("Exchange 服务器共有 %d 封邮件", total_on_server)

            if not items:
                logger.info("没有更多邮件了")
                break

            from urllib.parse import quote
            page_success = 0

            for item in items:
                email_id = item.get("id", "")
                if not email_id:
                    continue

                # Fetch full detail
                encoded_id = quote(email_id, safe="")
                detail_url = f"{api_url}/{encoded_id}"
                try:
                    detail_resp = http.get(
                        detail_url,
                        params={"account_id": account_id},
                        timeout=30.0,
                    )
                    if detail_resp.status_code == 200:
                        detail_data = detail_resp.json().get("data", {})
                        if detail_data:
                            if "id" not in detail_data:
                                detail_data["id"] = email_id
                            parsed = _exchange_item_to_parsed_email(
                                detail_data, folder=folder,
                            )
                            if parsed:
                                results.append(parsed)
                                page_success += 1
                    else:
                        logger.warning(
                            "详情获取失败 (ID: %s): %s",
                            email_id, detail_resp.status_code,
                        )
                except Exception as e:
                    logger.warning(
                        "获取详情异常 [%s] (ID: %s): %s",
                        type(e).__name__, email_id, e,
                    )

            offset += len(items)
            logger.info("本页获取 %d 封, 累计 %d/%s", page_success, len(results), target_desc)

            # Stop if server returned fewer items than requested (end of list)
            if len(items) < page_size:
                logger.info("已到达邮件列表末尾")
                break

    return results


# Well-known EWS folder name mapping: display name -> API name
# This is standard Exchange/EWS protocol mapping, not hardcoded business logic.
_EWS_FOLDER_MAP: dict[str, str] = {
    "收件箱": "inbox",
    "已发送邮件": "sent",
    "草稿": "drafts",
    "已删除邮件": "deleteditems",
    "垃圾邮件": "junkemail",
    "发件箱": "outbox",
    # English display names
    "Inbox": "inbox",
    "Sent Items": "sent",
    "Drafts": "drafts",
    "Deleted Items": "deleteditems",
    "Junk Email": "junkemail",
    "Outbox": "outbox",
}

# Non-mail folder classes to exclude (IPF.Note = mail; others are not mail)
_NON_MAIL_CLASSES = {"IPF.Contact", "IPF.Appointment", "IPF.Task",
                     "IPF.Journal", "IPF.StickyNote", "IPF.Activity"}


def _get_exchange_mail_folders() -> list[tuple[str, str]]:
    """Return list of (api_folder_name, display_name) for folders likely containing mail.

    Dynamically discovers folders via the folders/all API and filters by:
    1. folder_class (only IPF.Note or unset = mail folders)
    2. item count > 0
    3. Name not a GUID (system internal folders)
    """
    from src.config import get_settings, resolve_secret
    import httpx, re

    settings = get_settings()
    api_url = settings.EXCHANGE_API_URL.rstrip("/")
    api_key = resolve_secret(settings.EXCHANGE_API_KEY)
    account_id = settings.EXCHANGE_ACCOUNT_ID
    ssl_verify = settings.EXCHANGE_SSL_VERIFY
    headers = {"X-API-KEY": api_key} if api_key else {}

    folders_url = f"{api_url}/folders/all"
    try:
        r = httpx.get(
            folders_url,
            params={"account_id": account_id},
            headers=headers, verify=ssl_verify, timeout=30.0,
        )
        if r.status_code != 200:
            # Fallback endpoint
            base_url = re.sub(r"/emails/?$", "", api_url)
            r = httpx.get(
                f"{base_url}/folders/all",
                params={"account_id": account_id},
                headers=headers, verify=ssl_verify, timeout=30.0,
            )
    except Exception as e:
        logger.error("获取文件夹列表失败: %s", e)
        return [("inbox", "Inbox")]

    if r.status_code != 200:
        logger.warning("文件夹列表请求失败 (%s), 仅使用 Inbox", r.status_code)
        return [("inbox", "Inbox")]

    folders_data = r.json().get("data", {}).get("folders", [])
    result: list[tuple[str, str]] = []

    for f in folders_data:
        name = f.get("name", "")
        total = f.get("total_count", f.get("child_item_count", 0)) or 0
        folder_class = f.get("folder_class", "")

        # Skip empty folders
        if total == 0:
            continue

        # Skip GUID-named folders (internal system folders like {A9E2BC46-...})
        if re.match(r"^\{[0-9A-Fa-f-]+\}$", name):
            continue

        # Skip non-mail folders by class
        if folder_class:
            # Exact match or prefix match for non-mail classes
            if any(folder_class == cls or folder_class.startswith(cls + ".")
                   for cls in _NON_MAIL_CLASSES):
                continue

        # Map known system folder names to EWS API names
        if name in _EWS_FOLDER_MAP:
            api_name = _EWS_FOLDER_MAP[name]
        else:
            api_name = name  # custom folders use their display name directly

        # Avoid duplicates (e.g. Chinese and English names mapping to same API name)
        if not any(api == api_name for api, _ in result):
            result.append((api_name, name))
            logger.debug("发现邮件文件夹: %s -> %s (%d 封, class=%s)",
                         name, api_name, total, folder_class or "未知")

    if not result:
        result = [("inbox", "Inbox")]

    return result


def iter_from_exchange(
    folder: str = "ALL",
    limit: int = 0,
    all_mail: bool = False,
) -> Iterator[ParsedEmail]:
    """Fetch emails from Exchange server and yield ParsedEmail.

    When folder is "ALL", iterates over all mail folders.
    """
    if folder.upper() == "ALL":
        mail_folders = _get_exchange_mail_folders()
        logger.info("将遍历 %d 个邮件文件夹: %s",
                     len(mail_folders),
                     ", ".join(f"{d}({a})" for a, d in mail_folders))
        for api_name, display_name in mail_folders:
            logger.info("━━━ 开始拉取文件夹: %s ━━━", display_name)
            emails = _fetch_exchange_emails(
                folder=api_name, limit=limit, all_mail=all_mail,
            )
            for em in emails:
                em.source_folder = display_name
            yield from emails
    else:
        emails = _fetch_exchange_emails(
            folder=folder, limit=limit, all_mail=all_mail,
        )
        yield from emails


# ---------------------------------------------------------------------------
# Import engine
# ---------------------------------------------------------------------------

@dataclass
class ImportStats:
    total: int = 0
    imported: int = 0
    skipped: int = 0
    failed: int = 0
    points_created: int = 0

    def print_summary(self):
        print(f"\n{'━' * 50}")
        print("导入结果汇总:")
        print(f"  扫描邮件数:  {self.total}")
        print(f"  成功导入:    {self.imported}")
        print(f"  跳过 (空):   {self.skipped}")
        print(f"  失败:        {self.failed}")
        print(f"  Qdrant 点数: {self.points_created}")
        print(f"{'━' * 50}")


def run_import(
    source: Path | None = None,
    batch_size: int = 50,
    dry_run: bool = False,
    *,
    source_type: str = "file",
    exchange_folder: str = "INBOX",
    exchange_limit: int = 100,
    exchange_all_mail: bool = False,
) -> ImportStats:
    """Main import logic — detect source type, parse, and batch-ingest."""
    stats = ImportStats()

    if source_type == "exchange":
        email_iter = iter_from_exchange(
            folder=exchange_folder,
            limit=exchange_limit,
            all_mail=exchange_all_mail,
        )
        mail_scope = "全部邮件" if exchange_all_mail else "未读邮件"
        limit_desc = f"最多 {exchange_limit} 封" if exchange_limit > 0 else "全部"
        source_desc = f"Exchange 服务器 [{exchange_folder}] ({mail_scope}, {limit_desc})"
    else:
        assert source is not None
        suffix = source.suffix.lower()
        if source.is_dir():
            email_iter = iter_from_eml_dir(source)
            source_desc = f"EML 目录: {source}"
        elif suffix == ".pst":
            email_iter = iter_from_pst(source)
            source_desc = f"PST 文件: {source}"
        elif suffix == ".mbox":
            email_iter = iter_from_mbox(source)
            source_desc = f"Mbox 文件: {source}"
        elif suffix == ".eml":
            email_iter = iter_from_eml(source)
            source_desc = f"EML 文件: {source}"
        else:
            logger.error("不支持的文件格式: %s", suffix)
            sys.exit(1)

    print("\n📧 邮件导入工具")
    print(f"   来源: {source_desc}")
    print(f"   批次: {batch_size}")
    print(f"   模式: {'预览 (DRY RUN)' if dry_run else '正式导入'}")
    print()

    processor = None
    if not dry_run:
        try:
            from src.utils.email_processor import EmailProcessor

            processor = EmailProcessor()
            processor.init_collection()
            logger.info("Qdrant 连接成功，集合已就绪")
        except Exception as e:
            logger.error("无法连接到 Qdrant/Embedding 服务: %s", e)
            logger.error("请检查 .env 中的 QDRANT_URL, EMBEDDING_BASE_URL, EMBEDDING_API_KEY")
            sys.exit(1)

    batch: list[dict] = []

    try:
        for parsed in email_iter:
            stats.total += 1

            if not parsed.body.strip():
                stats.skipped += 1
                continue

            if dry_run:
                _type_icon = "📤" if parsed.message_type == "sent" else "📥"
                print(
                    f"  {_type_icon} [{parsed.source_folder}] "
                    f"{parsed.subject[:55]:<55} "
                    f"({parsed.sender[:30]})"
                )
                stats.imported += 1
                continue

            batch.append(parsed.to_dict())

            if len(batch) >= batch_size:
                points = processor.process_batch(batch)
                stats.points_created += points
                stats.imported += len(batch)
                logger.info(
                    "批次完成: %d 封邮件 → %d 个向量点 (累计: %d)",
                    len(batch), points, stats.imported,
                )
                batch = []

    except KeyboardInterrupt:
        logger.info("用户中断")

    if batch and not dry_run:
        try:
            points = processor.process_batch(batch)
            stats.points_created += points
            stats.imported += len(batch)
        except Exception as e:
            logger.error("最后批次失败: %s", e)
            stats.failed += len(batch)

    stats.print_summary()
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="PST/Mbox/EML/Exchange 邮件导入 Qdrant 工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 本地文件导入
  python scripts/import_pst.py archive.pst
  python scripts/import_pst.py archive.pst --dry-run
  python scripts/import_pst.py emails.mbox --batch-size 100
  python scripts/import_pst.py ./eml_folder/

  # 从 Exchange 服务器导入
  python scripts/import_pst.py --source exchange --dry-run
  python scripts/import_pst.py --source exchange --folder Inbox --limit 50
  python scripts/import_pst.py --source exchange --all-mail --limit 200
        """,
    )
    parser.add_argument(
        "source",
        nargs="?",
        default=None,
        help="PST/Mbox/EML 文件路径或 EML 目录路径 (--source file 时必填)",
    )
    parser.add_argument(
        "--source",
        dest="source_type",
        choices=["file", "exchange"],
        default="file",
        help="数据来源: file=本地文件 (默认), exchange=Exchange 服务器",
    )
    parser.add_argument(
        "--folder",
        default="ALL",
        help="Exchange 文件夹: ALL=全部文件夹 (默认), 或指定如 inbox/sent/drafts/文件夹名",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="从 Exchange 拉取的最大邮件数 (默认: 0=全部)，仅 --source exchange 时生效",
    )
    parser.add_argument(
        "--all-mail",
        action="store_true",
        help="拉取全部邮件（含已读），默认只拉未读。仅 --source exchange 时生效",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="每批次处理的邮件数 (默认: 50)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅预览解析结果，不实际写入 Qdrant",
    )

    args = parser.parse_args()

    if args.source_type == "exchange":
        run_import(
            source=None,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            source_type="exchange",
            exchange_folder=args.folder,
            exchange_limit=args.limit,
            exchange_all_mail=args.all_mail,
        )
    else:
        if not args.source:
            parser.error("本地文件模式需要指定 SOURCE 路径")
        source = Path(args.source)
        if not source.exists():
            logger.error("文件或目录不存在: %s", source)
            sys.exit(1)
        run_import(source, batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
