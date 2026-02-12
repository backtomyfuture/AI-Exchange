from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState

class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        VIP 处理逻辑：强制修改分类为 P0
        """
        classification = state.get("classification", {})
        classification.update({
            "priority": "P0",
            "need_reply": True,
            "intent": "审批",
            "reasoning": "匹配 Tier 1 VIP 发件人规则，自动提升至最高优先级。",
            "card_type": "approval"  # VIP 邮件始终需要审批卡片
        })
        
        return {
            "classification": classification,
            "priority_level": 10
        }
