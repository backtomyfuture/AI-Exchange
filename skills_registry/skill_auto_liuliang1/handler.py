from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        来自 liuliang1@tianjin-air.com 的邮件 (7 封, 回复率 14%)
        """
        classification = state.get("classification", {})
        classification.update({
            "priority": "P2",
            "need_reply": False,
            "reasoning": (
                classification.get("reasoning", "")
                + " [Auto-Skill: liuliang1 邮件处理] 匹配自动发现规则，回复率 14%。"
            ).strip(),
            "card_type": "none",
        })

        updates: Dict[str, Any] = {
            "classification": classification,
            "priority_level": 5,
        }

        return updates
