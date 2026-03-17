from typing import Any, Dict, Optional, List
from pydantic import BaseModel, Field
from src.graph.state import AgentState

class SkillTrigger(BaseModel):
    priority: int = 50
    conditions: Optional[List[Dict[str, Any]]] = None
    condition_logic: str = "and"

class SkillManifest(BaseModel):
    id: str
    name: str
    description: str
    version: str = "1.0.0"
    triggers: Optional[SkillTrigger] = None
    execution_mode: str = "modifier" # "modifier", "action", "chain"
    depends_on: Optional[List[str]] = None  # Skill 依赖列表

class BaseSkill:
    """
    所有 Skill 的基类。
    """
    def __init__(self, manifest: SkillManifest, config: Optional[Dict[str, Any]] = None):
        self.manifest = manifest
        self.config = config or {}

    async def execute(self, state: AgentState) -> Dict[str, Any]:
        """
        执行 Skill 逻辑。
        返回的数据将用于合并更新 AgentState。
        """
        raise NotImplementedError("Skill must implement execute method")

    def __repr__(self):
        return f"<Skill {self.manifest.name} (v{self.manifest.version})>"
