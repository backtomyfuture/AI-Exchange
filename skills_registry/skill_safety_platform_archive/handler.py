from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        classification = state.get("classification", {})
        classification.update({
            "priority": "P3",
            "need_reply": False,
            "intent": "通知",
            "reasoning": "数字化安全管理平台系统自动邮件，直接归档。",
            "card_type": "skip",
        })
        return {"classification": classification, "priority_level": 0}
