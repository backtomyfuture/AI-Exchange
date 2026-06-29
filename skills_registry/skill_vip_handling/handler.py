from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        VIP 处理逻辑：
        - 如果我在 TO 收件人中 → P0 + 需要回复（审批卡片）
        - 如果我不在 TO 中（仅 CC 或知会）→ P1 只读通知，不需要拟稿
        """
        from src.utils.notification_policy import is_direct_recipient

        email = state.get("email", {})
        classification = state.get("classification", {})

        if is_direct_recipient(email):
            classification.update({
                "priority": "P0",
                "need_reply": True,
                "intent": "审批",
                "reasoning": "VIP 发件人直接发送给我，提升至最高优先级。",
                "card_type": "approval",
            })
            return {"classification": classification, "priority_level": 10}
        else:
            classification.update({
                "priority": "P1",
                "need_reply": False,
                "intent": "通知",
                "reasoning": "VIP 发件人邮件，但我不在收件人(TO)中，仅需阅知。",
                "card_type": "read_only",
            })
            return {"classification": classification, "priority_level": 8}
