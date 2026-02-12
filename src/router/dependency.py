"""
Skill 依赖管理模块

提供技能执行顺序解析功能，支持依赖排序。
"""

from typing import List, Dict
from collections import deque
import logging

logger = logging.getLogger(__name__)


def resolve_skill_order(skill_ids: List[str], dependency_graph: Dict[str, List[str]]) -> List[str]:
    """
    使用拓扑排序解析 Skill 执行顺序。
    
    Args:
        skill_ids: 需要执行的技能 ID 列表
        dependency_graph: 依赖图，key 为技能 ID，value 为其依赖的技能列表
    
    Returns:
        排序后的技能 ID 列表（依赖项在前）
    
    Example:
        >>> resolve_skill_order(
        ...     ["skill_b", "skill_a", "skill_c"],
        ...     {"skill_c": ["skill_a"], "skill_b": ["skill_a"]}
        ... )
        ['skill_a', 'skill_b', 'skill_c']  # skill_a 无依赖，最先执行
    """
    if not skill_ids:
        return []
    
    # 过滤只关注当前需要执行的技能
    skill_set = set(skill_ids)
    
    # 构建入度表（只统计集合内的依赖关系）
    in_degree = {sid: 0 for sid in skill_ids}
    
    # 构建有效的依赖图（只保留集合内的依赖）
    filtered_graph = {}
    for sid in skill_ids:
        deps = dependency_graph.get(sid, [])
        # 只保留在当前集合中的依赖
        valid_deps = [d for d in deps if d in skill_set]
        filtered_graph[sid] = valid_deps
        in_degree[sid] = len(valid_deps)
    
    # 初始化队列：所有入度为 0 的技能
    queue = deque([sid for sid, deg in in_degree.items() if deg == 0])
    result = []
    
    while queue:
        current = queue.popleft()
        result.append(current)
        
        # 更新依赖于当前技能的节点的入度
        for sid in skill_ids:
            if current in filtered_graph.get(sid, []):
                in_degree[sid] -= 1
                if in_degree[sid] == 0:
                    queue.append(sid)
    
    # 如果结果少于输入，说明存在循环依赖
    if len(result) < len(skill_ids):
        missing = set(skill_ids) - set(result)
        logger.warning(f"Circular dependency detected for skills: {missing}. Using original order for these.")
        # 将循环依赖的技能追加到末尾
        result.extend(list(missing))
    
    return result
