from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        发送到 m.wu@tianjin-air.com 的邮件，我通过群组成员身份收到 (8 封, 回复率 0%)
        """
        classification = state.get("classification", {})
        classification.update({
            "priority": "P3",
            "need_reply": False,
            "reasoning": (
                classification.get("reasoning", "")
                + " [Auto-Skill: m.wu 群组邮件] 匹配自动发现规则，回复率 0%。"
            ).strip(),
            "card_type": "none",
        })

        updates: Dict[str, Any] = {
            "classification": classification,
            "priority_level": 2,
        }

        return updates
