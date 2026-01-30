import os
import uuid
from qdrant_client import QdrantClient, models
from src.graph.state import AgentState
from src.utils.exchange_api import ExchangeClient
from src.utils.db import DatabaseManager
from src.utils.email_processor import EmailProcessor

async def send_final_email(state: AgentState) -> AgentState:
    """
    发送最终审批通过的邮件，并将发送记录存入 Qdrant。
    """
    approval_status = state.get("approval_status", "pending")
    email_data = state.get("email", {})
    draft = state.get("draft", "")

    if approval_status == "approved":
        client = ExchangeClient()
        db_manager = DatabaseManager()
        
        # Use server-side reply to ensure history is handled correctly
        success = await client.reply_email(
            email_id=email_data.get("id"),
            body=draft
        )

        if success:
            # 记录到数据库
            db_manager.update_status(email_data.get("id"), "sent")
            
            # 将发送记录存回 Qdrant 向量库，用于未来的 RAG
            # 使用封装好的 Processor 方法
            processor = EmailProcessor()
            processor.process_sent_email(
                original_email_data=email_data,
                reply_content=draft
            )
            print(f"邮件已成功发送并存入向量库。邮件 ID: {email_data.get('id')}")
        else:
            db_manager.update_status(email_data.get('id'), "failed_sending")
            print(f"邮件发送失败。邮件 ID: {email_data.get('id')}")

    return state
