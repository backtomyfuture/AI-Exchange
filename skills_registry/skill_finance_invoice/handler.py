from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState

class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        classification = state.get("classification", {})
        classification.update({
            "intent": "审批",
            "reasoning": "检测到财务相关关键词 (发票/支付)，自动归类为审批流。",
            "card_type": "approval"  # 财务邮件需要审批卡片
        })
        
        return {
            "classification": classification,
            "priority_level": 7
        }
