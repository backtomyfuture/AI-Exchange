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
            "reasoning": "张霞转发的知会类信息，仅需阅知。",
            "card_type": "read_only",
        })
        return {"classification": classification, "priority_level": 2}
