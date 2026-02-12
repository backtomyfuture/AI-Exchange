from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState

class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        处理直接发送给“我”的邮件。
        逻辑：确保 need_reply 为 True，并标记为高优先级（P1）。
        """
        classification = state.get("classification", {})
        
        # 只有在尚未分类或分类认为不需要回复时，才强制标记
        # 或者我们直接覆盖优先级以确保关注
        classification.update({
            "priority": "P1",
            "need_reply": True,
            "reasoning": (classification.get("reasoning", "") + 
                         " [Skill Match: Direct Recipient] 邮件直接发送给收件人，确保回复并提升优先级。").strip(),
            "card_type": "approval"  # 直接发给我的邮件需要审批卡片
        })
        
        return {
            "classification": classification,
            "priority_level": 8  # 略低于 VIP (10)
        }
