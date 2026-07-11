import logging
from collections import Counter
from copy import deepcopy
from typing import List, Dict, Any, Iterable, Tuple
from src.config import get_settings
from src.graph.state import AgentState
from src.router.tier1_reflex import Tier1ReflexRouter
from src.router.manager import get_skill_manager
from src.safety.model_budget import (
    ModelInputTooLarge,
    enforce_model_input_budget,
    token_budget_from_settings,
)
from src.safety.manual_review import (
    build_manual_review_delta,
    manual_review_classification,
)

logger = logging.getLogger(__name__)

# Tier 2 voting thresholds - tuned conservatively to avoid mis-activations.
TIER2_MIN_HITS = 2          # Skill must appear in at least N similar past emails.
TIER2_MIN_RATIO = 0.5       # And in at least 50% of the inspected hits.

class RoutingEngine:
    """
    分层路由引擎：协调 T1, T2, T3 决策过程。
    """
    def __init__(self):
        self.t1_router = Tier1ReflexRouter()
        self.skill_manager = get_skill_manager()

    async def execute_router(self, state: AgentState) -> AgentState:
        """
        执行路由生命周期。

        返回值由调用节点投影为有界 Graph delta；列表字段不依赖 reducer。
        """
        email = state.get("email", {})
        routing_log: List[str] = list(state.get("routing_log") or [])
        active_skills: List[str] = list(state.get("active_skills") or [])
        existing_skills = set(active_skills)

        # --- Stage 1: Tier 1 (Reflex Layer) ---
        t1_matches = self.t1_router.route(email)
        if t1_matches:
            routing_log.append(f"Tier 1 Match: {t1_matches}")
            for sid in t1_matches:
                if sid not in existing_skills and sid not in active_skills:
                    active_skills.append(sid)

            # 立即执行匹配的 Skill 逻辑
            state = await self._apply_skills(state, t1_matches)
            state["routing_log"] = routing_log
            state["active_skills"] = active_skills
            return state

        # --- Stage 2: Tier 2 (Semantic Layer) ---
        # Tier 2 通常集成在 retriever_node 中，因为它需要等待向量数据库的检索结果。
        routing_log.append("Tier 1 No match, moving to Tier 2/3")

        # --- Stage 3: Tier 3 (LLM Reasoning Layer) ---
        # 仅在有可用 Skill 时才调用 LLM，避免无意义的 LLM 开销。
        skills = self.skill_manager.get_all_skills()
        if skills:
            try:
                t3_matches = await self._tier3_llm_route(state, skills)
            except ModelInputTooLarge:
                delta = build_manual_review_delta(
                    {},
                    "router_input_too_large",
                    classification=manual_review_classification(
                        "router_input_too_large"
                    ),
                )
                return delta
            except Exception as exc:
                logger.error(
                    "Tier 3 LLM routing unavailable: error_type=%s",
                    type(exc).__name__,
                )
                delta = build_manual_review_delta(
                    {},
                    "router_model_failed",
                    classification=manual_review_classification(
                        "router_model_failed"
                    ),
                )
                return delta
            if t3_matches:
                routing_log.append(f"Tier 3 LLM Match: {t3_matches}")
                for sid in t3_matches:
                    if sid not in existing_skills and sid not in active_skills:
                        active_skills.append(sid)
                state = await self._apply_skills(state, t3_matches)
        else:
            routing_log.append("Tier 3 Skipped: No skills registered")

        state["routing_log"] = routing_log
        state["active_skills"] = active_skills
        return state

    def _tier2_route(
        self,
        hits: Iterable[Dict[str, Any]],
        existing_skills: Iterable[str],
        skills: Dict[str, Any] | None = None,
        min_hits: int = TIER2_MIN_HITS,
        min_ratio: float = TIER2_MIN_RATIO,
    ) -> List[str]:
        """
        Tier 2: 基于历史 RAG hit 的标签做投票激活。

        - 每条 hit 的 ``payload['active_skills']`` 视为一票。
        - 同一邮件 (chunk 多条) 的同一标签按 ``hit['id']`` 去重，避免重复加权。
        - 仅在某个 skill 的命中次数 >= ``min_hits`` 且占比 >= ``min_ratio`` 时激活。
        - 已激活的 skill 不再回投。
        - 若 ``skills`` 注册表给出，则过滤未知 ID。
        """
        existing = set(existing_skills or [])
        valid_pool = set(skills.keys()) if skills else None

        seen_pairs: set[Tuple[str, str]] = set()
        skill_counter: Counter = Counter()
        valid_emails: set[str] = set()

        for hit in hits or []:
            if not isinstance(hit, dict):
                continue
            email_id = str(hit.get("id") or hit.get("email_id") or "")
            past_skills = hit.get("active_skills") or []
            if not past_skills:
                continue
            valid_emails.add(email_id or id(hit))
            for sid in past_skills:
                if not isinstance(sid, str) or not sid:
                    continue
                if sid in existing:
                    continue
                if valid_pool is not None and sid not in valid_pool:
                    continue
                key = (email_id or str(id(hit)), sid)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                skill_counter[sid] += 1

        total_emails = max(1, len(valid_emails))
        chosen: List[str] = []
        for sid, count in skill_counter.most_common():
            if count < min_hits:
                continue
            if (count / total_emails) < min_ratio:
                continue
            chosen.append(sid)
        if chosen:
            logger.info(
                "Tier 2 activated %s from %d labelled hits (counter=%s)",
                chosen,
                total_emails,
                dict(skill_counter),
            )
        return chosen

    async def apply_tier2_hits(
        self,
        state: AgentState,
        hits: Iterable[Dict[str, Any]],
    ) -> AgentState:
        """
        Public Tier 2 entry called by retriever_node after Qdrant search.

        Activates voted skills and runs their handlers. Returns a transient,
        local-only outcome which retriever_node projects into bounded State.
        """
        skills = self.skill_manager.get_all_skills()
        existing = state.get("active_skills") or []
        chosen = self._tier2_route(hits, existing, skills)
        if not chosen:
            return {}
        before = deepcopy(dict(state))
        new_state = await self._apply_skills(before, chosen)
        delta: Dict[str, Any] = {
            "active_skills": chosen,
            "routing_log": [f"Tier 2 Match: {chosen}"],
        }
        for key in (
            "classification",
            "metadata",
            "system_prompt_modifier",
            "priority_level",
        ):
            if key in new_state and new_state.get(key) != before.get(key):
                delta[key] = deepcopy(new_state[key])

        new_tool_calls = new_state.get("tool_calls") or []
        old_tool_calls = before.get("tool_calls") or []
        if len(new_tool_calls) > len(old_tool_calls):
            delta["tool_calls"] = deepcopy(new_tool_calls[len(old_tool_calls):])

        routed_email = new_state.get("email")
        previous_email = before.get("email")
        if isinstance(routed_email, dict):
            previous_email = previous_email if isinstance(previous_email, dict) else {}
            for field in ("draft_to", "draft_cc"):
                if (
                    field in routed_email
                    and routed_email.get(field) != previous_email.get(field)
                ):
                    delta[field] = deepcopy(routed_email[field])
        fixed_draft = new_state.get("draft")
        if isinstance(fixed_draft, str) and fixed_draft != before.get("draft"):
            delta["_draft_content"] = fixed_draft
        return delta

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

            enforce_model_input_budget(
                "router",
                prompt,
                budget=token_budget_from_settings(get_settings()),
            )
            llm = get_llm_for_role("router", temperature=0)
            response = await llm.ainvoke(prompt)
            
            raw_content = getattr(response, "content", None)
            if not isinstance(raw_content, str):
                raise RuntimeError("router_schema_invalid")
            content = raw_content.strip()
            if content.upper() == "NONE":
                logger.info("Tier 3 LLM: No matching skill found")
                return []
            if not content:
                raise RuntimeError("router_schema_invalid")
            
            # 解析 LLM 返回的技能 ID 列表
            matched_ids = [s.strip() for s in content.split(",") if s.strip()]
            if (
                not matched_ids
                or any(skill_id.upper() == "NONE" for skill_id in matched_ids)
                or any(skill_id not in skills for skill_id in matched_ids)
            ):
                raise RuntimeError("router_schema_invalid")
            
            valid_ids = list(dict.fromkeys(matched_ids))
            logger.info("Tier 3 LLM matched skill count=%d", len(valid_ids))
            return valid_ids
            
        except ModelInputTooLarge:
            raise
        except Exception as exc:
            logger.error(
                "Tier 3 LLM routing failed: error_type=%s",
                type(exc).__name__,
            )
            raise RuntimeError("router_model_failed") from None

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
        
        # Skill handlers historically mutate nested classification/email values.
        # A deep copy keeps those mutations local and makes before/after projection
        # deterministic.
        new_state = deepcopy(dict(state))
        
        for sid in ordered_skills:
            skill = self.skill_manager.get_skill(sid)
            if skill is None:
                logger.error("Configured skill is unavailable: skill=%s", sid)
                raise RuntimeError("router_skill_failed")
            try:
                candidate_state = deepcopy(new_state)
                update = await skill.execute(candidate_state)
                if not isinstance(update, dict):
                    raise TypeError("invalid_skill_update")
                # 不可变合并：创建新字典而非修改原字典
                for key, val in update.items():
                    if (
                        isinstance(val, dict)
                        and key in candidate_state
                        and isinstance(candidate_state[key], dict)
                    ):
                        candidate_state[key] = {
                            **candidate_state[key],
                            **val,
                        }
                    else:
                        candidate_state[key] = val
                new_state = candidate_state
                logger.info("Applied skill logic: skill=%s", sid)
            except Exception as exc:
                logger.error(
                    "Skill execution failed: skill=%s error_type=%s",
                    sid,
                    type(exc).__name__,
                )
                raise RuntimeError("router_skill_failed") from None
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
