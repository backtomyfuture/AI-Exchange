from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState

class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        修改 System Prompt 修饰符
        """
        modifier = (
            "【语气指令】你正在回复一封重要的汇报邮件。请务必遵守以下原则：\n"
            "1. 使用 BLUF (Bottom Line Up Front) 原则，结论先行。\n"
            "2. 保持极致简洁，避免寒暄和废话。\n"
            "3. 使用条目化列表 (Bullet points) 呈现核心数据或结论。\n"
            "4. 语气需极其专业、稳重。"
        )
        
        return {
            "system_prompt_modifier": (state.get("system_prompt_modifier") or "") + "\n" + modifier
        }
