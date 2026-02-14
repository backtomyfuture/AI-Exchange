import logging
from src.router.base import BaseSkill
from src.graph.state import AgentState

logger = logging.getLogger(__name__)

class Skill(BaseSkill):
    async def execute(self, state: AgentState) -> dict:
        classification = state.get("classification", {})
        email = state.get("email", {})
        
        # 1. Force P0/Need Reply to trigger HITL
        classification["priority"] = "P0"
        classification["need_reply"] = True
        
        # 2. Set Forwarding Metadata
        classification["intent"] = "转发"
        classification["action"] = "forward"  # Key flag for downstream nodes
        classification["reasoning"] = f"Triggered by skill {self.manifest.name}"
        
        # 3. Pre-fill Data
        # Targets for forwarding
        email["draft_to"] = ["boss@company.com"] 
        email["draft_cc"] = []
        
        # Fixed content
        draft_content = "呈阅" 
        
        logger.info(f"Skill '{self.manifest.name}' activated. Action set to 'forward'.")
        
        return {
            "classification": classification, 
            "email": email, 
            "draft": draft_content
        }
