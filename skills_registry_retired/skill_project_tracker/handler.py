import re
from typing import Dict, Any
from src.router.base import BaseSkill
from src.graph.state import AgentState

class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        提取项目 ID 并标记
        """
        email = state.get("email", {})
        text = email.get("subject", "") + " " + email.get("body", "")
        
        # 搜索 P-XXXX 模式
        project_ids = re.findall(r"P-\d{4}", text)
        
        metadata = state.get("metadata", {})
        if project_ids:
            metadata["detected_projects"] = list(set(project_ids))
            
        return {
            "metadata": metadata,
            "routing_log": state.get("routing_log", []) + [f"Detected projects: {project_ids}"]
        }
