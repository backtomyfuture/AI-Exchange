from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState

class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        不在办公室状态处理
        """
        modifier = (
            "【状态通知】发件人目前可能处于休假、出差或不在办公室状态。"
            "请在回复中礼貌地告知对方目前可能无法及时处理深度事务，"
            "并建议对方如有紧急事项请通过其他方式联系。"
        )
        
        return {
            "system_prompt_modifier": (state.get("system_prompt_modifier") or "") + "\n" + modifier,
            "priority_level": 5
        }
