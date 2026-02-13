import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_db_manager = None
_circuit_breaker = None


def init_commands(db_manager):
    global _db_manager, _circuit_breaker
    _db_manager = db_manager
    from src.utils.circuit_breaker import circuit_breaker

    _circuit_breaker = circuit_breaker


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
    except Exception as e:
        logger.error("handle_stats failed: %s", e)
        return f"查询失败: {e}"


async def handle_queue(args: str) -> str:
    _ = args
    from src.exchange_service import _webhook_queue

    queue_size = _webhook_queue.qsize() if _webhook_queue else 0
    cb_status = "🔴 熔断中" if _circuit_breaker and _circuit_breaker.is_open else "🟢 正常"
    return f"📦 队列深度: {queue_size}\n⚡ 熔断器: {cb_status}"


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
    except Exception as e:
        logger.error("handle_pending failed: %s", e)
        return f"查询失败: {e}"


async def handle_search(args: str) -> str:
    keyword = (args or "").strip()
    if not keyword:
        return "用法: /search <关键词>"
    from src.utils.retriever import get_retriever

    retriever = get_retriever()
    results = await asyncio.to_thread(retriever.search, query_text=keyword, limit=5)
    if not results:
        return f"🔍 未找到与 '{keyword}' 相关的邮件"
    lines = [f"🔍 搜索结果 ({len(results)} 条):\n"]
    for row in results:
        lines.append(
            f"  · [{(row.get('subject') or '无主题')[:40]}] from {row.get('sender', '未知')}"
        )
    return "\n".join(lines)


async def handle_health(args: str) -> str:
    _ = args
    from src.exchange_service import _webhook_queue

    cb_status = "🔴 OPEN" if _circuit_breaker and _circuit_breaker.is_open else "🟢 CLOSED"
    db_ok = "🟢" if _db_manager else "🔴"
    queue_size = _webhook_queue.qsize() if _webhook_queue else -1
    return (
        "🏥 系统健康状态:\n"
        f"  数据库: {db_ok}\n"
        f"  熔断器: {cb_status}\n"
        f"  队列深度: {queue_size}"
    )
