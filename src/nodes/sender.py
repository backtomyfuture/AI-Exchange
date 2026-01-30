import logging
from src.graph.state import AgentState

logger = logging.getLogger(__name__)

async def send_final_email(state: AgentState, config: dict = None) -> AgentState:
    """
    发送最终审批通过的邮件，并将发送记录存入 Qdrant。
    通过 AppContext 单例获取共享资源，避免重复创建实例。
    """
    approval_status = state.get("approval_status", "pending")
    email_data = state.get("email", {})
    draft = state.get("draft", "")

    if approval_status == "approved":
        from src.init_app import get_app_context
        # Import internally to avoid circular dependency
        from src.utils.lark_app import lark_api_client
        from lark_oapi.api.contact.v3 import GetUserRequest
        import re

        ctx = get_app_context()
        
        # Helper to resolve open_id to email
        def resolve_recipient(recipient_str):
            # Check for open_id=xxx
            if "open_id=" in str(recipient_str):
                open_id = str(recipient_str).replace("open_id=", "").strip()
                if not lark_api_client:
                    logger.warning(f"Lark client missing, cannot resolve open_id: {open_id}")
                    return None
                try:
                    req = GetUserRequest.builder().user_id(open_id).user_id_type("open_id").build()
                    resp = lark_api_client.contact.v3.user.get(req)
                    if resp.success() and resp.data and resp.data.user:
                         # Prioritize enterprise_email, then email
                         email = resp.data.user.enterprise_email or resp.data.user.email
                         if email:
                             return email
                    logger.warning(f"Could not resolve open_id {open_id} to email.")
                    return None
                except Exception as e:
                    logger.error(f"Error resolving open_id {open_id}: {e}")
                    return None
            
            # Extract from legacy format "name='...', email_address='...'" or just return string
            if "email_address='" in str(recipient_str):
                m = re.search(r"email_address='(.*?)'", str(recipient_str))
                if m: return m.group(1)
            
            return str(recipient_str)

        # Resolve To/Cc lists
        final_to = []
        raw_to = email_data.get("to", [])
        if isinstance(raw_to, str): raw_to = [raw_to]
        for r in raw_to:
            resolved = resolve_recipient(r)
            if resolved: final_to.append(resolved)

        final_cc = []
        raw_cc = email_data.get("cc", [])
        if isinstance(raw_cc, str): raw_cc = [raw_cc]
        for r in raw_cc:
            resolved = resolve_recipient(r)
            if resolved: final_cc.append(resolved)

        logger.info(f"Sending reply. To: {final_to}, Cc: {final_cc}")

        success = await ctx.exchange_client.reply_email(
            email_id=email_data.get("id"),
            body=draft,
            to=final_to,
            cc=final_cc
        )

        if success:
            await ctx.db_manager.update_status(email_data.get("id"), "sent")
            ctx.email_processor.process_sent_email(
                original_email_data=email_data,
                reply_content=draft
            )
            logger.info(f"邮件已成功发送并存入向量库。邮件 ID: {email_data.get('id')}")
        else:
            await ctx.db_manager.update_status(email_data.get('id'), "failed_sending")
            logger.error(f"邮件发送失败。邮件 ID: {email_data.get('id')}")

    return state
