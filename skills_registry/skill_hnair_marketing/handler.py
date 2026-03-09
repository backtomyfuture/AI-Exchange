from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        classification = state.get("classification", {})
        classification.update({
            "priority": "P3",
            "need_reply": False,
            "intent": "垃圾邮件",
            "reasoning": "海航航空商城营销推广邮件，自动归档。",
            "card_type": "skip",
        })
        return {"classification": classification, "priority_level": 0}
