import logging
from typing import List, Dict, Any, Tuple
from src.graph.state import AgentState
from src.router.tier1_reflex import Tier1ReflexRouter
from src.router.manager import get_skill_manager

logger = logging.getLogger(__name__)

class RoutingEngine:
    """
    分层路由引擎：协调 T1, T2, T3 决策过程。
    """
    def __init__(self):
        self.t1_router = Tier1ReflexRouter()
        self.skill_manager = get_skill_manager()

    async def execute_router(self, state: AgentState) -> AgentState:
        """
        执行路由生命周期
        """
        email = state.get("email", {})
        routing_log = state.get("routing_log", []) or []
        active_skills = state.get("active_skills", []) or []
        
        # --- Stage 1: Tier 1 (Reflex Layer) ---
        t1_matches = self.t1_router.route(email)
        if t1_matches:
            routing_log.append(f"Tier 1 Match: {t1_matches}")
            active_skills.extend(t1_matches)
            
            # 立即执行匹配的 Skill 逻辑
            state = await self._apply_skills(state, t1_matches)
            
            # 如果匹配度极高或标记为 skip_llm，可以提前返回
            # 这里我们保守一点，继续流转状态
            state["routing_log"] = routing_log
            state["active_skills"] = list(set(active_skills))
            return state

        # --- Stage 2: Tier 2 (Semantic Layer) ---
        # 注意：Tier 2 通常集成在 retriever_node 中，
        # 因为它需要等待向量数据库的检索结果。
        routing_log.append("Tier 1 No match, moving to Tier 2/3")
        
        # --- Stage 3: Tier 3 (LLM Reasoning Layer) ---
        # 当 Tier 1/2 没有明确匹配时，调用 LLM 选择合适的 Skill
        # 优化：仅在有可用 Skill 时才调用 LLM，避免无意义的 LLM 开销
        skills = self.skill_manager.get_all_skills()
        if skills:
            t3_matches = await self._tier3_llm_route(state, skills)
            if t3_matches:
                routing_log.append(f"Tier 3 LLM Match: {t3_matches}")
                active_skills.extend(t3_matches)
                state = await self._apply_skills(state, t3_matches)
        else:
            routing_log.append("Tier 3 Skipped: No skills registered")
        
        state["routing_log"] = routing_log
        state["active_skills"] = list(set(active_skills))
        return state

    async def _tier3_llm_route(self, state: AgentState, skills: Dict[str, Any] = None) -> List[str]:
        """
        Tier 3: 基于 Skill 描述的 LLM 路由。
        根据邮件内容，让 LLM 从可用 Skill 中选择最合适的技能。
        """
        try:
            from src.providers.factory import get_llm_for_role
            
            if skills is None:
                skills = self.skill_manager.get_all_skills()
            if not skills:
                return []
            
            skill_descriptions = []
            for skill_id, skill in skills.items():
                desc = getattr(skill.manifest, 'description', skill_id)
                skill_descriptions.append(f"- {skill_id}: {desc}")
            
            email = state.get("email", {})
            subject = email.get('subject', '')
            body = email.get('body', '')[:500] if email.get('body') else ''
            
            prompt = f"""根据以下邮件内容，从可用技能中选择最合适的（可多选，用逗号分隔，无匹配返回 NONE）:

邮件主题: {subject}
邮件正文: {body}

可用技能:
{chr(10).join(skill_descriptions)}

请只输出技能 ID，例如: skill_vip_handling, skill_project_tracker
如果没有合适的技能，请返回: NONE"""

            llm = get_llm_for_role("router", temperature=0)
            response = await llm.ainvoke(prompt)
            
            content = response.content.strip()
            if "NONE" in content.upper():
                logger.info("Tier 3 LLM: No matching skill found")
                return []
            
            # 解析 LLM 返回的技能 ID 列表
            matched_ids = [s.strip() for s in content.split(",") if s.strip()]
            
            # 验证返回的 ID 是否真实存在
            valid_ids = [sid for sid in matched_ids if sid in skills]
            logger.info(f"Tier 3 LLM matched skills: {valid_ids}")
            return valid_ids
            
        except Exception as e:
            logger.error(f"Tier 3 LLM routing error: {e}")
            return []

    async def _apply_skills(self, state: AgentState, skill_ids: List[str]) -> AgentState:
        """
        执行 Skill 处理器并合并状态（使用不可变方式）
        """
        # 解析依赖顺序
        from src.router.dependency import resolve_skill_order
        
        dependency_graph = {}
        for sid in skill_ids:
            skill = self.skill_manager.get_skill(sid)
            if skill and hasattr(skill.manifest, 'depends_on') and skill.manifest.depends_on:
                dependency_graph[sid] = skill.manifest.depends_on
        
        ordered_skills = resolve_skill_order(skill_ids, dependency_graph)
        
        # 创建新的状态副本
        new_state = dict(state)
        
        for sid in ordered_skills:
            skill = self.skill_manager.get_skill(sid)
            if skill:
                try:
                    update = await skill.execute(new_state)
                    # 不可变合并：创建新字典而非修改原字典
                    for key, val in update.items():
                        if isinstance(val, dict) and key in new_state and isinstance(new_state[key], dict):
                            new_state[key] = {**new_state[key], **val}
                        else:
                            new_state[key] = val
                    logger.info(f"Applied Skill Logic: {sid}")
                except Exception as e:
                    logger.error(f"Error executing skill {sid}: {e}")
        return new_state


    def dry_run(self, subject: str, sender: str, body: str = "") -> Dict[str, Any]:
        """Simulate routing without executing skills. Returns the decision report."""
        email = {"subject": subject, "sender": sender, "body": body}
        report: Dict[str, Any] = {"tier1": [], "tier3_candidates": [], "skills_available": []}

        t1_matches = self.t1_router.route(email)
        report["tier1"] = t1_matches or []

        skills = self.skill_manager.get_all_skills()
        for sid, skill in skills.items():
            desc = getattr(skill.manifest, "description", sid)
            report["skills_available"].append(f"{sid}: {desc}")

        return report


# 全局单例 - 避免每封邮件都重新加载 Skills 和初始化路由器
_routing_engine = None

def get_routing_engine() -> RoutingEngine:
    """获取 RoutingEngine 全局单例"""
    global _routing_engine
    if _routing_engine is None:
        _routing_engine = RoutingEngine()
    return _routing_engine


