import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_db_manager = None
_circuit_breaker = None
_SEARCH_DISPLAY_LIMIT = 5
_SEARCH_CANDIDATE_LIMIT = _SEARCH_DISPLAY_LIMIT * 5


def init_commands(db_manager):
    global _db_manager, _circuit_breaker
    _db_manager = db_manager
    from src.utils.circuit_breaker import circuit_breaker

    _circuit_breaker = circuit_breaker


def _ingestion_runtime() -> object | None:
    """Return the already-started durable ingestion runtime without creating one."""

    from src.init_app import get_runtime_app_context

    return get_runtime_app_context().ingestion_runtime


async def _runtime_queue_stats() -> tuple[bool, object | None]:
    """Read the bounded durable Inbox aggregate from the runtime owner."""

    runtime = _ingestion_runtime()
    check_ready = getattr(runtime, "check_ready", None)
    queue_stats = getattr(runtime, "queue_stats", None)
    if not callable(check_ready) or not callable(queue_stats):
        return False, None
    try:
        if not await check_ready():
            return False, None
        return True, await queue_stats()
    except Exception as exc:
        logger.error("runtime command status unavailable: error_type=%s", type(exc).__name__)
        return False, None


async def handle_help(args: str) -> str:
    _ = args
    return (
        "📋 可用指令：\n"
        "/stats [today|week] - 邮件统计\n"
        "/queue - 队列与系统状态\n"
        "/pending - 待审批邮件\n"
        "/search <关键词> - 搜索历史邮件\n"
        "/health - 系统健康状态\n"
        "/help - 显示本帮助"
    )


def _build_stats_card(rows: list[dict[str, Any]], period: str, total: int) -> dict[str, Any]:
    card = {
        "header": {
            "template": "blue",
            "title": {"content": f"📊 邮件统计 ({period})", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**共处理 {total} 封邮件**"}},
            {"tag": "hr"},
        ],
    }
    for row in rows:
        card["elements"].append(
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"· **{row['status']}**: {row['cnt']} 封"},
            }
        )
    return card


async def handle_stats(args: str) -> dict[str, Any] | str:
    if not _db_manager:
        return "数据库未初始化"

    period = (args or "").strip().lower() or "today"
    now = datetime.now()
    if period == "week":
        start = (now - timedelta(days=7)).date()
    else:
        period = "today"
        start = now.date()

    try:
        async with _db_manager.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT status, COUNT(*) as cnt FROM emails_log "
                    "WHERE DATE(processed_at) >= %s GROUP BY status ORDER BY status",
                    (start,),
                )
                rows = await cur.fetchall()

        if not rows:
            return f"📊 {period} 暂无邮件处理记录"

        total = sum(int(r["cnt"]) for r in rows)
        return _build_stats_card(rows, period, total)
    except Exception as exc:
        logger.error("handle_stats failed: error_type=%s", type(exc).__name__)
        return "查询失败，请稍后重试"


async def handle_queue(args: str) -> str:
    _ = args
    available, stats = await _runtime_queue_stats()
    if not available or stats is None:
        return "📦 运行时未就绪，暂无法读取队列状态"
    return (
        "📦 队列状态:\n"
        f"  待处理: {stats.pending}\n"
        f"  重试等待: {stats.retry_wait}\n"
        f"  处理中: {stats.leased}\n"
        f"  人工复核: {stats.manual_review}\n"
        f"  死信: {stats.dead_letter}"
    )


async def handle_pending(args: str) -> str:
    _ = args
    if not _db_manager:
        return "数据库未初始化"
    try:
        async with _db_manager.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, subject, sender, updated_at FROM emails_log "
                    "WHERE status = 'waiting_approval' ORDER BY updated_at DESC LIMIT 10"
                )
                rows = await cur.fetchall()
        if not rows:
            return "✅ 暂无待审批邮件"
        lines = [f"⏳ 待审批邮件 ({len(rows)} 封):\n"]
        for row in rows:
            lines.append(f"  · [{(row.get('subject') or '无主题')[:30]}] from {row.get('sender', '未知')}")
        return "\n".join(lines)
    except Exception as exc:
        logger.error("handle_pending failed: error_type=%s", type(exc).__name__)
        return "查询失败，请稍后重试"


def _deduplicate_search_results(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse Qdrant chunks into one command result per logical email."""

    unique_rows: list[dict[str, Any]] = []
    seen_email_ids: set[str] = set()
    for row in rows:
        email_id = row.get("id") or row.get("email_id") or row.get("internet_message_id")
        if email_id is not None:
            key = str(email_id).strip()
            if key:
                if key in seen_email_ids:
                    continue
                seen_email_ids.add(key)
        unique_rows.append(row)
        if len(unique_rows) >= _SEARCH_DISPLAY_LIMIT:
            break
    return unique_rows


def _format_command_sender(value: object) -> str:
    """Render Exchange's serialized Mailbox value as readable plain text."""

    raw = str(value or "").strip()
    if not raw:
        return "未知"
    mailbox = re.search(
        r"name=['\"]([^'\"]*)['\"].*?email_address=['\"]([^'\"]+)['\"]",
        raw,
    )
    if mailbox:
        name, address = (part.strip() for part in mailbox.groups())
        return f"{name} <{address}>" if name else address
    address = re.search(r"email_address=['\"]([^'\"]+)['\"]", raw)
    if address:
        return address.group(1).strip()
    return raw


async def handle_search(args: str) -> str:
    keyword = (args or "").strip()
    if not keyword:
        return "用法: /search <关键词>"
    from src.utils.retriever import get_retriever

    retriever = get_retriever()
    candidates = await asyncio.to_thread(
        retriever.search,
        query_text=keyword,
        limit=_SEARCH_CANDIDATE_LIMIT,
    )
    results = _deduplicate_search_results(candidates)
    if not results:
        return f"🔍 未找到与 '{keyword}' 相关的邮件"
    lines = [f"🔍 搜索结果 ({len(results)} 条):\n"]
    for row in results:
        lines.append(
            f"  · [{(row.get('subject') or '无主题')[:40]}] "
            f"from {_format_command_sender(row.get('sender'))}"
        )
    return "\n".join(lines)


async def handle_health(args: str) -> str:
    _ = args
    available, stats = await _runtime_queue_stats()
    db_ok = "🟢" if _db_manager else "🔴"
    runtime_status = "🟢 READY" if available else "🔴 NOT_READY"
    queue_line = "  队列: 不可用"
    if stats is not None:
        queue_line = (
            "  队列: "
            f"待处理 {stats.pending}，"
            f"重试等待 {stats.retry_wait}，"
            f"处理中 {stats.leased}，"
            f"人工复核 {stats.manual_review}，"
            f"死信 {stats.dead_letter}"
        )
    return (
        "🏥 系统健康状态:\n"
        f"  数据库: {db_ok}\n"
        f"  运行时: {runtime_status}\n"
        f"{queue_line}"
    )


async def handle_routing(args: str) -> str:
    """Query routing decision for an email by ID."""
    email_id = (args or "").strip()
    if not email_id:
        return "用法: /routing <email_id>"
    if not _db_manager:
        return "数据库未初始化"
    try:
        async with _db_manager.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT routing_log, active_skills, classification FROM emails_log WHERE id = %s",
                    (email_id,),
                )
                row = await cur.fetchone()
        if not row:
            return f"未找到邮件: {email_id}"
        import json as _json

        routing_log = row.get("routing_log") or []
        if isinstance(routing_log, str):
            routing_log = _json.loads(routing_log)
        active_skills = row.get("active_skills") or []
        if isinstance(active_skills, str):
            active_skills = _json.loads(active_skills)
        cls = row.get("classification") or {}
        if isinstance(cls, str):
            cls = _json.loads(cls)

        lines = [f"🔀 路由详情 [{email_id[:20]}...]:\n"]
        lines.append(f"  Skills: {', '.join(active_skills) if active_skills else '无'}")
        lines.append(f"  路由链: {' → '.join(routing_log) if routing_log else '无记录'}")
        lines.append(f"  Priority: {cls.get('priority', '?')}")
        lines.append(f"  Intent: {cls.get('intent', '?')}")
        lines.append(f"  Confidence: {cls.get('confidence', '?')}")
        lines.append(f"  Need Reply: {cls.get('need_reply', '?')}")
        return "\n".join(lines)
    except Exception as exc:
        logger.error("handle_routing failed: error_type=%s", type(exc).__name__)
        return "查询失败，请稍后重试"


async def handle_test_rule(args: str) -> str:
    """Dry-run routing rules against a given subject/sender."""
    parts = (args or "").strip()
    if not parts:
        return "用法: /test_rule 发件人邮箱 邮件主题\n示例: /test_rule ceo@corp.com 紧急会议通知"
    tokens = parts.split(maxsplit=1)
    sender = tokens[0]
    subject = tokens[1] if len(tokens) > 1 else ""

    try:
        from src.router.engine import get_routing_engine
        engine = get_routing_engine()
        report = engine.dry_run(subject=subject, sender=sender)
    except Exception as exc:
        logger.error("handle_test_rule failed: error_type=%s", type(exc).__name__)
        return "规则引擎错误，请稍后重试"

    lines = ["🧪 规则沙盒测试结果:\n"]
    lines.append(f"  发件人: {sender}")
    lines.append(f"  主题: {subject or '(空)'}")
    lines.append("")
    t1 = report.get("tier1", [])
    lines.append(f"  Tier 1 匹配: {', '.join(t1) if t1 else '无匹配 → 将进入 Tier 3 LLM'}")
    lines.append(f"  已注册 Skills ({len(report.get('skills_available', []))}):")
    for s in report.get("skills_available", [])[:10]:
        lines.append(f"    · {s}")
    return "\n".join(lines)


async def handle_ai_report(args: str) -> str:
    """Weekly AI performance report."""
    if not _db_manager:
        return "数据库未初始化"
    try:
        async with _db_manager.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE status IN ('approved','sent','forwarded')) AS approved,
                        COUNT(*) FILTER (WHERE status = 'rejected') AS rejected,
                        COUNT(*) FILTER (WHERE status = 'modified') AS modified,
                        COUNT(*) FILTER (WHERE original_draft IS NOT NULL AND final_draft IS NOT NULL
                                         AND original_draft = final_draft) AS no_edit,
                        COUNT(*) FILTER (WHERE original_draft IS NOT NULL AND final_draft IS NOT NULL
                                         AND original_draft != final_draft) AS edited,
                        COUNT(*) FILTER (WHERE rejection_reason IS NOT NULL) AS has_reason,
                        COUNT(*) AS total
                    FROM emails_log
                    WHERE processed_at >= CURRENT_DATE - INTERVAL '7 days'
                """)
                row = await cur.fetchone()

        total = row["total"] or 0
        approved = row["approved"] or 0
        rejected = row["rejected"] or 0
        no_edit = row["no_edit"] or 0
        edited = row["edited"] or 0
        drafts_total = no_edit + edited
        pass_rate = f"{no_edit / drafts_total * 100:.0f}%" if drafts_total > 0 else "N/A"

        lines = [
            "📊 本周 AI 表现报告:\n",
            f"  总处理: {total} 封",
            f"  已批准: {approved}  |  已拒绝: {rejected}",
            f"  草稿一次通过率: {pass_rate} ({no_edit}/{drafts_total})",
            f"  用户编辑后批准: {edited} 封",
        ]
        return "\n".join(lines)
    except Exception as exc:
        logger.error("handle_ai_report failed: error_type=%s", type(exc).__name__)
        return "查询失败，请稍后重试"
