from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        来自 lanjuan@tianjin-air.com 直接发给我的邮件 (4 封, 回复率 75%)
        """
        classification = state.get("classification", {})
        classification.update({
            "priority": "P1",
            "need_reply": True,
            "reasoning": (
                classification.get("reasoning", "")
                + " [Auto-Skill: lanjuan 直接发给我] 匹配自动发现规则，回复率 75%。"
            ).strip(),
            "card_type": "approval",
        })

        updates: Dict[str, Any] = {
            "classification": classification,
            "priority_level": 8,
        }

        return updates
