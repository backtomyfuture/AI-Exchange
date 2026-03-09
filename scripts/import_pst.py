#!/usr/bin/env python3
"""
PST 历史邮件导入工具 —— 将 .pst 文件中的邮件批量解析并存入 Qdrant 向量数据库。

支持格式:
  - .pst  (纯 Python 解析，pip install libpff-python)
  - .mbox (Python 标准库直接解析)
  - .eml  (单封邮件) 或包含 .eml 文件的目录

使用方法:
    # 导入 PST 文件
    python scripts/import_pst.py /path/to/archive.pst

    # 导入 mbox 文件
    python scripts/import_pst.py /path/to/mail.mbox

    # 导入 eml 文件目录
    python scripts/import_pst.py /path/to/eml_dir/

    # 先预览不实际导入
    python scripts/import_pst.py /path/to/archive.pst --dry-run

    # 指定批次大小
    python scripts/import_pst.py /path/to/archive.pst --batch-size 100

环境准备:
    # PST 文件需要 pypff:
    pip install libpff-python

    # 确保 .env 配置了 Qdrant 和 Embedding 服务
    # QDRANT_URL=http://localhost:6333
    # EMBEDDING_BASE_URL=...
    # EMBEDDING_API_KEY=...
"""

from __future__ import annotations

import argparse
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
            "_import_source": "pst_import",
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
        sender = msg.sender_name or ""
        if not sender:
            sender = msg.sender_email_address or "unknown"
        elif msg.sender_email_address:
            sender = f"{sender} <{msg.sender_email_address}>"

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
        logger.error(
            "readpst 未安装。请运行: sudo apt install pst-utils"
        )
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
    source: Path,
    batch_size: int = 50,
    dry_run: bool = False,
) -> ImportStats:
    """Main import logic — detect source type, parse, and batch-ingest."""

    from src.utils.email_processor import EmailProcessor

    stats = ImportStats()

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
        description="PST/Mbox/EML 历史邮件导入 Qdrant 工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python scripts/import_pst.py archive.pst
  python scripts/import_pst.py archive.pst --dry-run
  python scripts/import_pst.py emails.mbox --batch-size 100
  python scripts/import_pst.py ./eml_folder/
        """,
    )
    parser.add_argument(
        "source",
        help="PST/Mbox/EML 文件路径或 EML 目录路径",
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
    source = Path(args.source)

    if not source.exists():
        logger.error("文件或目录不存在: %s", source)
        sys.exit(1)

    run_import(source, batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
