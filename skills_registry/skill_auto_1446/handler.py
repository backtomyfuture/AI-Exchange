from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState


class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        主题含转发标记或正文含呈阅/请知关键词的邮件 (110 封, 回复率 5%)，通常不需要回复
        """
        classification = state.get("classification", {})
        classification.update({
            "priority": "P3",
            "need_reply": False,
            "reasoning": (
                classification.get("reasoning", "")
                + " [Auto-Skill: 转发与呈阅邮件] 匹配自动发现规则，回复率 5%。"
            ).strip(),
            "card_type": "none",
        })

        updates: Dict[str, Any] = {
            "classification": classification,
            "priority_level": 2,
        }

        return updates
