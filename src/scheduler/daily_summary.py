"""
每日邮件摘要定时任务模块

提供自动化的每日邮件处理摘要功能。
"""

import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Optional

from src.config import get_settings
from src.providers.factory import get_llm_for_role

logger = logging.getLogger(__name__)

# 全局引用（由 main.py 注入）
_db_manager = None
_lark_module = None


def init_scheduler(db_manager, lark_module):
    """
    初始化定时任务模块的依赖。
    
    Args:
        db_manager: 异步数据库管理器
        lark_module: lark_app 模块引用（用于发送消息）
    """
    global _db_manager, _lark_module
    _db_manager = db_manager
    _lark_module = lark_module
    logger.info("Daily summary scheduler initialized.")


async def generate_daily_summary() -> Optional[str]:
    """
    生成当日邮件处理摘要。
    
    Returns:
        摘要文本，如果没有处理邮件则返回 None
    """
    if not _db_manager:
        logger.warning("Scheduler DB manager not initialized.")
        return None
    
    try:
        # 查询今日处理的邮件记录
        today = datetime.now().date()
        
        # 假设 db_manager 有获取今日记录的方法
        # 如果没有，需要添加或使用原始 SQL
        today_records = await _db_manager.get_records_by_date(today)
        
        if not today_records:
            logger.info("No emails processed today, skipping summary.")
            return None
        
        # 统计各状态数量
        stats = {
            "approved": 0,
            "rejected": 0,
            "draft_saved": 0,
            "pending": 0
        }
        
        subjects = []
        for record in today_records:
            status = record.get("status", "pending")
            stats[status] = stats.get(status, 0) + 1
            subjects.append(record.get("subject", "无主题"))
        
        # 使用 LLM 生成摘要
        llm = get_llm_for_role("summary", temperature=0.5)
        
        prompt = f"""请用简洁的中文总结今日邮件处理情况：

处理统计:
- 已批准发送: {stats.get('approved', 0)} 封
- 已拒绝: {stats.get('rejected', 0)} 封
- 已存草稿: {stats.get('draft_saved', 0)} 封
- 待处理: {stats.get('pending', 0)} 封

邮件主题列表（前10封）:
{chr(10).join(['- ' + s for s in subjects[:10]])}

请生成一份简短的每日报告（不超过200字），包括：
1. 今日概况
2. 需关注事项（如待处理邮件）
"""
        
        response = await llm.ainvoke(prompt)
        summary = response.content.strip()
        
        # 添加标题
        final_summary = f"📧 **每日邮件摘要** ({today.strftime('%Y-%m-%d')})\n\n{summary}"
        
        logger.info(f"Daily summary generated: {len(summary)} chars")
        return final_summary
        
    except Exception as exc:
        logger.error("Daily summary generation failed: error_type=%s", type(exc).__name__)
        return None


async def send_daily_summary():
    """
    生成并发送每日摘要到飞书群。
    """
    summary = await generate_daily_summary()
    
    if not summary:
        return
    
    if not _lark_module or not _lark_module.lark_api_client:
        logger.warning("Lark client not available, cannot send summary.")
        return
    
    try:
        settings = get_settings()
        chat_id = settings.LARK_CHAT_ID
        
        if not chat_id:
            logger.warning("LARK_CHAT_ID not configured.")
            return
        
        # 发送文本消息
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
        import json
        
        request = CreateMessageRequest.builder() \
            .receive_id_type("chat_id") \
            .request_body(CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps({"text": summary}))
                .build()) \
            .build()
        
        response = _lark_module.lark_api_client.im.v1.message.create(request)
        
        if response.success():
            logger.info("Daily summary sent successfully.")
        else:
            logger.error("Daily summary send rejected: code=%s", response.code)
            
    except Exception as exc:
        logger.error("Daily summary send failed: error_type=%s", type(exc).__name__)


async def run_scheduler(send_time: time = None):
    """
    运行每日定时任务调度器。
    
    Args:
        send_time: 发送时间，默认 18:00
    """
    if send_time is None:
        send_time = time(hour=18, minute=0)
    
    logger.info(f"Daily summary scheduler started. Will run at {send_time}")
    
    while True:
        now = datetime.now()
        
        # 计算下次执行时间
        target = datetime.combine(now.date(), send_time)
        if now >= target:
            # 今天已过，设置为明天
            target += timedelta(days=1)
        
        wait_seconds = (target - now).total_seconds()
        logger.info(f"Next daily summary scheduled at {target} (in {wait_seconds:.0f} seconds)")
        
        await asyncio.sleep(wait_seconds)
        
        # 执行摘要任务
        try:
            await send_daily_summary()
        except Exception as exc:
            logger.error("Daily summary task failed: error_type=%s", type(exc).__name__)
        
        # 短暂休眠避免重复执行
        await asyncio.sleep(60)
