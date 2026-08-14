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
import email as email_lib
import email.policy
import email.utils
import hashlib
import html as html_lib
import logging
import mailbox
import re
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


class ExchangeImportIncomplete(RuntimeError):
    """The Exchange source could not prove a complete bounded read."""


_EXCHANGE_DETAIL_TIMEOUT_SECONDS = 55.0


def _exchange_ssl_verify(settings) -> bool | str:
    ssl_verify = bool(settings.EXCHANGE_SSL_VERIFY)
    ca_file = str(getattr(settings, "EXCHANGE_CA_FILE", "") or "").strip()
    return ca_file if ssl_verify and ca_file else ssl_verify


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
    internet_message_id: str = ""
    attachments_metadata: list[dict] = field(default_factory=list)
    import_source: str = "pst_import"
    route_decision: dict | None = None

    def to_dict(self) -> dict:
        payload = {
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
            "internet_message_id": self.internet_message_id,
            "attachments": [],
            "attachments_metadata": self.attachments_metadata,
            "_import_source": self.import_source,
        }
        payload["history_dedupe_key"] = _history_dedupe_key(self)
        if self.route_decision is not None:
            payload["route_decision"] = self.route_decision
        return payload


def _history_dedupe_key(message: ParsedEmail) -> str:
    """Return a source-independent identity for one Historical Email."""
    internet_message_id = " ".join(message.internet_message_id.split()).casefold()
    if internet_message_id:
        return f"message-id:{internet_message_id}"

    def normalized(value: str) -> str:
        return " ".join(value.split()).casefold()

    signature = "\0".join(
        (
            normalized(message.subject),
            normalized(message.sender),
            ",".join(sorted(normalized(value) for value in message.to)),
            ",".join(sorted(normalized(value) for value in message.cc)),
            normalized(message.received_at),
            normalized(message.body),
        )
    )
    return f"signature:{hashlib.sha256(signature.encode('utf-8')).hexdigest()}"


# ---------------------------------------------------------------------------
# Conservative historical route inference
# ---------------------------------------------------------------------------

_FORWARDED_MESSAGE_MARKER = re.compile(
    r"(?:"
    r"^-{2,}\s*(?:forwarded message|original message)\s*-{2,}"
    r"|^-{2,}\s*(?:转发邮件|原始邮件)\s*-{2,}"
    r"|^\s*(?:转发邮件|原始邮件)\s*[:：-]"
    r")",
    re.IGNORECASE | re.MULTILINE,
)
_FORWARDED_HEADER_PATTERNS = {
    "from": re.compile(
        r"^\s*(?:from|发件人)\s*[:：]\s*(.*?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "subject": re.compile(
        r"^\s*(?:subject|主题)\s*[:：]\s*(.*?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
}
_EMAIL_ADDRESS_PATTERN = re.compile(
    r"[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+",
    re.IGNORECASE,
)


def _normalize_message_id(value: str | None) -> str:
    """Normalize whitespace only for an exact Message-ID comparison."""
    return " ".join(str(value or "").split()).casefold()


def _plainish_body(value: object) -> str:
    """Make forwarded-header detection work for plain text and simple HTML."""
    text = html_lib.unescape(str(value or ""))
    return re.sub(r"<[^>]*>", "\n", text)


def _extract_address(value: object) -> str:
    """Extract one concrete address from RFC or Exchange mailbox text."""
    if isinstance(value, dict):
        for key in ("email", "email_address", "address", "value"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip().casefold()
        return ""

    text = str(value or "").strip()
    if not text:
        return ""
    mailbox_match = re.search(
        r"email_address=['\"]([^'\"]+)['\"]",
        text,
        flags=re.IGNORECASE,
    )
    if mailbox_match:
        return mailbox_match.group(1).strip().casefold()
    parsed = email_lib.utils.parseaddr(text)[1].strip()
    if parsed and "@" in parsed:
        return parsed.casefold()
    address_match = _EMAIL_ADDRESS_PATTERN.search(text)
    return address_match.group(0).casefold() if address_match else ""


def _concrete_addresses(values: list[str]) -> list[str]:
    """Return deduplicated concrete addresses in their first-seen order."""
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        address = _extract_address(value)
        if address and address not in seen:
            seen.add(address)
            result.append(address)
    return result


def _normalize_subject(value: str | None) -> str:
    """Remove common reply/forward prefixes before exact subject comparison."""
    subject = " ".join(str(value or "").split()).casefold()
    while True:
        stripped = re.sub(
            r"^(?:(?:re|fw|fwd)\s*[:：]\s*|(?:回复|答复|转发)\s*[:：]\s*)",
            "",
            subject,
        )
        if stripped == subject:
            return subject
        subject = stripped


def _forwarded_header(body: str, name: str) -> str:
    match = _FORWARDED_HEADER_PATTERNS[name].search(body)
    return " ".join(match.group(1).split()) if match else ""


def _body_contains_original(body: str, original: ParsedEmail) -> bool:
    """Require a bounded body fragment when no embedded Message-ID is present."""
    original_body = " ".join(_plainish_body(original.body).split())
    if len(original_body) < 8:
        return False
    fragment = original_body[:160].casefold()
    return fragment in " ".join(body.split()).casefold()


def _forwarded_original(
    sent: ParsedEmail,
    received: list[ParsedEmail],
) -> ParsedEmail | None:
    """Find one received message proven to be quoted by a sent forward."""
    body = _plainish_body(sent.body)
    if not _FORWARDED_MESSAGE_MARKER.search(body):
        return None

    forwarded_sender = _extract_address(_forwarded_header(body, "from"))
    forwarded_subject = _normalize_subject(_forwarded_header(body, "subject"))
    if not forwarded_sender or not forwarded_subject:
        return None

    candidates = [
        original
        for original in received
        if _extract_address(original.sender) == forwarded_sender
        and _normalize_subject(original.subject) == forwarded_subject
    ]
    if len(candidates) != 1:
        return None

    original = candidates[0]
    normalized_body = _normalize_message_id(body)
    message_id_proof = (
        bool(original.internet_message_id)
        and _normalize_message_id(original.internet_message_id) in normalized_body
    )
    if not message_id_proof and not _body_contains_original(body, original):
        return None
    return original


def _historical_route_decision(
    route: str,
    *,
    evidence_ids: list[str],
    params: dict | None = None,
    reason_code: str,
) -> dict:
    """Create a schema-compatible label for an observed historical action."""
    return {
        "outcome": "matched",
        "route": route,
        "params": params or {},
        "provenance": {
            "tier": "system",
            "source_version": "history-import-route-v1",
            "evidence_ids": evidence_ids[:16],
            "confidence": 1.0,
        },
        "reason_code": reason_code,
    }


def _infer_historical_route_decisions(
    messages: list[ParsedEmail],
) -> dict[str, dict]:
    """Infer only exact observed replies and proven forwards.

    The returned mapping targets received messages.  Sent messages are evidence
    and are never assigned a route decision themselves.  Any missing,
    ambiguous, or conflicting evidence is deliberately left unlabeled.
    """
    received = [message for message in messages if message.message_type == "received"]
    sent = [message for message in messages if message.message_type == "sent"]
    candidates: dict[str, list[tuple[str, dict, str]]] = {}

    def add_candidate(
        original: ParsedEmail,
        route: str,
        decision: dict,
        evidence_id: str,
    ) -> None:
        candidates.setdefault(original.id, []).append(
            (route, decision, evidence_id)
        )

    for sent_message in sent:
        exact_replies = [
            original
            for original in received
            if _normalize_message_id(sent_message.in_reply_to)
            and _normalize_message_id(sent_message.in_reply_to)
            == _normalize_message_id(original.internet_message_id)
        ]
        if exact_replies:
            # Message-IDs should be unique.  More than one match is not safe
            # evidence, so do not assign this sent message to any original.
            if len(exact_replies) == 1:
                original = exact_replies[0]
                add_candidate(
                    original,
                    "reply",
                    _historical_route_decision(
                        "reply",
                        evidence_ids=[sent_message.id],
                        reason_code="historical_sent_reply",
                    ),
                    sent_message.id,
                )
            continue

        original = _forwarded_original(sent_message, received)
        if original is None:
            continue
        recipients = _concrete_addresses(sent_message.to)
        if not recipients:
            continue
        cc = _concrete_addresses(sent_message.cc)
        params = {"fixed_recipients": recipients}
        if cc:
            params["cc"] = cc
        add_candidate(
            original,
            "forward",
            _historical_route_decision(
                "forward",
                evidence_ids=[sent_message.id],
                params=params,
                reason_code="historical_sent_forward",
            ),
            sent_message.id,
        )

    result: dict[str, dict] = {}
    for original_id, rows in candidates.items():
        signatures = {
            (
                route,
                str(decision.get("params") or {}),
            )
            for route, decision, _evidence_id in rows
        }
        if len(signatures) != 1:
            continue
        route, first_decision, _ = rows[0]
        evidence_ids = list(dict.fromkeys(row[2] for row in rows))
        result[original_id] = {
            **first_decision,
            "provenance": {
                **first_decision["provenance"],
                "evidence_ids": evidence_ids[:16],
            },
        }
    return result


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
            internet_message_id=msg_id.strip(),
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
        internet_message_id = ""
        to_list: list[str] = []
        cc_list: list[str] = []

        if headers:
            hdr_msg = email_lib.message_from_string(headers)
            internet_message_id = hdr_msg.get("Message-ID", "") or ""
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
            internet_message_id=internet_message_id,
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
        to_raw = item.get("to") or item.get("to_recipients") or []
        cc_raw = item.get("cc") or item.get("cc_recipients") or []

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
            internet_message_id=(
                item.get("internet_message_id") or item.get("message_id", "") or ""
            ),
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
    ssl_verify = _exchange_ssl_verify(settings)

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
        detail_failures = 0

        while offset < max_fetch:
            page_size = min(PAGE_SIZE, max_fetch - offset)
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
                logger.warning("Exchange 列表请求失败: error_type=%s", type(e).__name__)
                raise ExchangeImportIncomplete("exchange_import_list_failed") from e

            if response.status_code != 200:
                logger.warning("Exchange 列表获取失败: status=%s", response.status_code)
                raise ExchangeImportIncomplete("exchange_import_list_failed")

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
                    detail_failures += 1
                    continue

                # Fetch full detail
                encoded_id = quote(email_id, safe="")
                detail_url = f"{api_url}/{encoded_id}"
                detail_resp = None
                for detail_attempt in range(2):
                    try:
                        detail_resp = http.get(
                            detail_url,
                            params={"account_id": account_id, "folder": folder},
                            timeout=_EXCHANGE_DETAIL_TIMEOUT_SECONDS,
                        )
                    except (httpx.TimeoutException, httpx.TransportError) as e:
                        if detail_attempt == 0:
                            logger.warning(
                                "Exchange 详情请求瞬时失败，重试一次: error_type=%s",
                                type(e).__name__,
                            )
                            continue
                        logger.warning(
                            "Exchange 详情请求异常: error_type=%s",
                            type(e).__name__,
                        )
                        detail_failures += 1
                        break

                    if detail_resp.status_code >= 500 and detail_attempt == 0:
                        logger.warning(
                            "Exchange 详情返回瞬时错误，重试一次: status=%s",
                            detail_resp.status_code,
                        )
                        continue
                    break

                try:
                    if detail_resp is not None and detail_resp.status_code == 200:
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
                                detail_failures += 1
                        else:
                            detail_failures += 1
                    elif detail_resp is not None:
                        detail_failures += 1
                        logger.warning(
                            "Exchange 详情获取失败: status=%s",
                            detail_resp.status_code,
                        )
                except Exception as e:
                    detail_failures += 1
                    logger.warning(
                        "Exchange 详情解析异常: error_type=%s",
                        type(e).__name__,
                    )

            offset += len(items)
            logger.info("本页获取 %d 封, 累计 %d/%s", page_success, len(results), target_desc)

            # Stop if server returned fewer items than requested (end of list)
            if len(items) < page_size:
                logger.info("已到达邮件列表末尾")
                break

    expected_count = min(total_on_server or 0, max_fetch)
    if offset != expected_count:
        raise ExchangeImportIncomplete(
            f"exchange_import_count_mismatch:{offset}/{expected_count}"
        )
    if detail_failures:
        raise ExchangeImportIncomplete(
            f"exchange_import_detail_failed:{detail_failures}"
        )
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

_HISTORY_EXCLUDED_FOLDER_API_NAMES = frozenset(
    {"deleteditems", "drafts", "junkemail", "outbox"}
)

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
    import re

    import httpx

    settings = get_settings()
    api_url = settings.EXCHANGE_API_URL.rstrip("/")
    api_key = resolve_secret(settings.EXCHANGE_API_KEY)
    account_id = settings.EXCHANGE_ACCOUNT_ID
    ssl_verify = _exchange_ssl_verify(settings)
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
        logger.error("Exchange 文件夹列表请求失败: error_type=%s", type(e).__name__)
        raise ExchangeImportIncomplete(
            "exchange_import_folder_discovery_failed"
        ) from e

    if r.status_code != 200:
        logger.warning("文件夹列表请求失败 (%s)", r.status_code)
        raise ExchangeImportIncomplete("exchange_import_folder_discovery_failed")

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

        if api_name.casefold() in _HISTORY_EXCLUDED_FOLDER_API_NAMES:
            continue

        # Avoid duplicates (e.g. Chinese and English names mapping to same API name)
        if not any(api == api_name for api, _ in result):
            result.append((api_name, name))
            logger.debug("发现邮件文件夹: %s -> %s (%d 封, class=%s)",
                         name, api_name, total, folder_class or "未知")

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
# Historical route inference
# ---------------------------------------------------------------------------

_FORWARD_PREFIXES = ("fw:", "fwd:", "转发:")
_REPLY_PREFIXES = ("re:", "回复:", "答复:")


def _strip_subject_prefixes(subject: str) -> tuple[str, str | None]:
    remaining = " ".join(str(subject or "").split())
    action = None
    while True:
        folded = remaining.casefold()
        matched = None
        for prefix in (*_FORWARD_PREFIXES, *_REPLY_PREFIXES):
            if folded.startswith(prefix):
                matched = prefix
                break
        if matched is None:
            return remaining, action
        if action is None:
            action = "forward" if matched in _FORWARD_PREFIXES else "reply"
        remaining = remaining[len(matched):].lstrip()


def _normalized_message_id(value: str) -> str:
    return " ".join(str(value or "").split()).casefold()


def _historical_decision(*, route: str, params: dict, reason_code: str) -> dict:
    from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
    from src.router.tier1.schema import CanonicalRoute

    return RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route=CanonicalRoute(route),
        params=params,
        provenance=RouteProvenance(
            tier=RouteTier.HISTORICAL_INFERRED,
            source_version="historical-inferred-v1",
            confidence=0.4,
        ),
        reason_code=reason_code,
        handoff_profile_id=(
            "generic_reply_v1" if route == "reply" else "generic_forward_v1"
        ),
    ).model_dump(mode="json")


def infer_historical_route_decisions(emails: list[ParsedEmail]) -> dict[str, dict]:
    """Infer reply/forward labels from sent mail. Never guess read_only."""

    by_message_id: dict[str, ParsedEmail] = {}
    by_conversation: dict[str, list[ParsedEmail]] = {}
    received: list[ParsedEmail] = []
    sent: list[ParsedEmail] = []
    for parsed_email in emails:
        if parsed_email.message_type == "sent":
            sent.append(parsed_email)
            continue
        if parsed_email.message_type != "received":
            continue
        received.append(parsed_email)
        message_id = _normalized_message_id(parsed_email.internet_message_id)
        if message_id:
            by_message_id[message_id] = parsed_email
        conversation_id = _normalized_message_id(parsed_email.conversation_id)
        if conversation_id:
            by_conversation.setdefault(conversation_id, []).append(parsed_email)

    labels: dict[str, dict] = {}
    for sent_email in sent:
        target = None
        in_reply_to = _normalized_message_id(sent_email.in_reply_to)
        if in_reply_to:
            target = by_message_id.get(in_reply_to)
        if target is None:
            conversation_id = _normalized_message_id(sent_email.conversation_id)
            candidates = by_conversation.get(conversation_id, [])
            if len(candidates) == 1:
                target = candidates[0]
        if target is None or target.id in labels:
            continue
        _, action = _strip_subject_prefixes(sent_email.subject)
        if action == "forward" or (
            action is None and _strip_subject_prefixes(target.subject)[0]
            and sent_email.subject.casefold().startswith(("fw:", "fwd:", "转发:"))
        ):
            labels[target.id] = _historical_decision(
                route="forward",
                params={
                    "fixed_recipients": list(sent_email.to),
                    "cc": list(sent_email.cc),
                },
                reason_code="historical_forward",
            )
            continue
        if in_reply_to or action == "reply":
            labels[target.id] = _historical_decision(
                route="reply",
                params={"reply_mode": "sender_only"},
                reason_code="historical_reply",
            )
    return labels


# ---------------------------------------------------------------------------
# Import engine
# ---------------------------------------------------------------------------

@dataclass
class ImportStats:
    total: int = 0
    imported: int = 0
    skipped: int = 0
    duplicates: int = 0
    failed: int = 0
    points_created: int = 0
    route_reply: int = 0
    route_forward: int = 0
    route_unprocessed: int = 0

    def print_summary(self):
        print(f"\n{'━' * 50}")
        print("导入结果汇总:")
        print(f"  扫描邮件数:  {self.total}")
        print(f"  成功导入:    {self.imported}")
        print(f"  跳过 (空):   {self.skipped}")
        print(f"  跳过 (重复): {self.duplicates}")
        print(f"  失败:        {self.failed}")
        print(f"  Qdrant 点数: {self.points_created}")
        print(f"  route_decision=reply:   {self.route_reply}")
        print(f"  route_decision=forward: {self.route_forward}")
        print(f"  route_decision 未处理:   {self.route_unprocessed}")
        print(f"{'━' * 50}")


def _iter_from_file_source(source: Path) -> tuple[Iterator[ParsedEmail], str]:
    suffix = source.suffix.lower()
    if source.is_dir():
        return iter_from_eml_dir(source), f"EML 目录: {source}"
    if suffix == ".pst":
        return iter_from_pst(source), f"PST 文件: {source}"
    if suffix == ".mbox":
        return iter_from_mbox(source), f"Mbox 文件: {source}"
    if suffix == ".eml":
        return iter_from_eml(source), f"EML 文件: {source}"
    raise ValueError(f"unsupported_history_source:{suffix}")


def run_import(
    source: Path | None = None,
    batch_size: int = 50,
    dry_run: bool = False,
    *,
    source_type: str = "file",
    exchange_folder: str = "INBOX",
    exchange_limit: int = 100,
    exchange_all_mail: bool = False,
    supplement_source: Path | None = None,
    route_evidence_folder: str | None = None,
    route_evidence_limit: int = 0,
) -> ImportStats:
    """Main import logic — detect source type, parse, and batch-ingest."""
    stats = ImportStats()

    if source_type == "exchange":
        primary_iter = iter_from_exchange(
            folder=exchange_folder,
            limit=exchange_limit,
            all_mail=exchange_all_mail,
        )
        mail_scope = "全部邮件" if exchange_all_mail else "未读邮件"
        limit_desc = f"最多 {exchange_limit} 封" if exchange_limit > 0 else "全部"
        source_desc = f"Exchange 服务器 [{exchange_folder}] ({mail_scope}, {limit_desc})"
        primary_messages = list(primary_iter)
        relation_messages = list(primary_messages)

        if route_evidence_folder:
            evidence_limit = route_evidence_limit or exchange_limit
            if (
                route_evidence_folder.casefold()
                != exchange_folder.casefold()
            ):
                evidence_messages = list(
                    iter_from_exchange(
                        folder=route_evidence_folder,
                        limit=evidence_limit,
                        all_mail=True,
                    )
                )
                relation_messages.extend(evidence_messages)
                source_desc = (
                    f"{source_desc}；路由证据 "
                    f"[{route_evidence_folder}, 最多 {evidence_limit or '全部'} 封]"
                )

        if supplement_source is not None:
            supplement_iter, supplement_desc = _iter_from_file_source(
                supplement_source
            )
            supplement_messages = list(supplement_iter)
            primary_messages.extend(supplement_messages)
            relation_messages.extend(supplement_messages)
            source_desc = f"{source_desc} + 补充 {supplement_desc}"
        email_messages = primary_messages
    else:
        assert source is not None
        if supplement_source is not None or route_evidence_folder:
            raise ValueError("supplement_source_requires_exchange")
        email_iter, source_desc = _iter_from_file_source(source)
        email_messages = list(email_iter)
        relation_messages = list(email_messages)

    # A source can expose the same message through multiple folders or a local
    # supplement.  Deduplicate the relation graph before inferring labels so
    # duplicate copies cannot turn one proven match into an ambiguity.
    unique_relation_messages: list[ParsedEmail] = []
    seen_relation_keys: set[str] = set()
    for message in relation_messages:
        relation_key = _history_dedupe_key(message)
        if relation_key in seen_relation_keys:
            continue
        seen_relation_keys.add(relation_key)
        unique_relation_messages.append(message)
    route_decisions = _infer_historical_route_decisions(unique_relation_messages)

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
    seen_dedupe_keys: set[str] = set()
    parsed_emails: list[ParsedEmail] = []

    try:
        for parsed in email_messages:
            stats.total += 1

            if not parsed.body.strip():
                stats.skipped += 1
                continue

            dedupe_key = _history_dedupe_key(parsed)
            if dedupe_key in seen_dedupe_keys:
                stats.duplicates += 1
                continue
            seen_dedupe_keys.add(dedupe_key)

            parsed.route_decision = route_decisions.get(parsed.id)
            if parsed.route_decision is None:
                stats.route_unprocessed += 1
            elif parsed.route_decision.get("route") == "reply":
                stats.route_reply += 1
            elif parsed.route_decision.get("route") == "forward":
                stats.route_forward += 1
            parsed_emails.append(parsed)
            if dry_run:
                _type_icon = "📤" if parsed.message_type == "sent" else "📥"
                route = (
                    parsed.route_decision.get("route")
                    if parsed.route_decision
                    else "unprocessed"
                )
                print(
                    f"  {_type_icon} [{parsed.source_folder}] "
                    f"{parsed.subject[:55]:<55} "
                    f"({parsed.sender[:30]}) route={route}"
                )
                stats.imported += 1
                continue

            batch.append(parsed.to_dict())

            if len(batch) >= batch_size:
                points = processor.process_batch(batch, wait=True)
                stats.points_created += points
                if points > 0:
                    stats.imported += len(batch)
                else:
                    stats.failed += len(batch)
                logger.info(
                    "批次完成: %d 封邮件 → %d 个向量点 (累计: %d)",
                    len(batch), points, stats.imported,
                )
                batch = []

    except KeyboardInterrupt:
        logger.info("用户中断")

    if batch and not dry_run:
        try:
            points = processor.process_batch(batch, wait=True)
            stats.points_created += points
            if points > 0:
                stats.imported += len(batch)
            else:
                stats.failed += len(batch)
        except Exception as e:
            logger.error("最后批次失败: %s", e)
            stats.failed += len(batch)

    if processor is not None and parsed_emails:
        labeled = 0
        for parsed in parsed_emails:
            if not parsed.route_decision:
                continue
            processor.update_email_labels(
                parsed.id,
                route_decision=parsed.route_decision,
                human_verified=False,
                draft_edited=False,
                label_source="historical_inferred",
                eligible_for_tier2=False,
            )
            labeled += 1
        logger.info("Wrote historical route labels: count=%d", labeled)

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
  python scripts/import_pst.py --source exchange --folder ALL --limit 0 \
    --all-mail --supplement archive.pst --dry-run
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
        "--supplement",
        type=Path,
        default=None,
        help="Exchange 优先导入后，用 PST/Mbox/EML 补充缺失历史邮件",
    )
    parser.add_argument(
        "--route-evidence-folder",
        default=None,
        help=(
            "仅 Exchange 模式：额外读取指定文件夹作为历史 route_decision 证据，"
            "例如 sent；证据邮件本身不导入"
        ),
    )
    parser.add_argument(
        "--route-evidence-limit",
        type=int,
        default=0,
        help=(
            "route_decision 证据邮件上限；默认沿用 --limit，0 表示沿用 --limit"
        ),
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
        if args.supplement is not None and not args.supplement.exists():
            parser.error("--supplement 文件或目录不存在")
        stats = run_import(
            source=None,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            source_type="exchange",
            exchange_folder=args.folder,
            exchange_limit=args.limit,
            exchange_all_mail=args.all_mail,
            supplement_source=args.supplement,
            route_evidence_folder=args.route_evidence_folder,
            route_evidence_limit=args.route_evidence_limit,
        )
    else:
        if not args.source:
            parser.error("本地文件模式需要指定 SOURCE 路径")
        source = Path(args.source)
        if not source.exists():
            logger.error("文件或目录不存在: %s", source)
            sys.exit(1)
        stats = run_import(source, batch_size=args.batch_size, dry_run=args.dry_run)

    if stats.failed:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
