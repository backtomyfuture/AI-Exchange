import os
import yaml
import importlib.util
import logging
from typing import Dict, List, Optional, Type
from src.router.base import BaseSkill, SkillManifest

logger = logging.getLogger(__name__)

class SkillManager:
    """
    Skill 注册中心。
    负责从 skills_registry 目录加载所有 Skill。
    """
    def __init__(self, registry_path: str = "skills_registry"):
        self.registry_path = registry_path
        self.skills: Dict[str, BaseSkill] = {}
        self._load_all_skills()

    def _load_all_skills(self):
        """
        扫描目录并加载 Skill。
        """
        if not os.path.exists(self.registry_path):
            os.makedirs(self.registry_path)
            return

        for skill_id in os.listdir(self.registry_path):
            skill_dir = os.path.join(self.registry_path, skill_id)
            if not os.path.isdir(skill_dir) or skill_id.startswith("__"):
                continue

            try:
                # 1. 加载 manifest.yaml
                manifest_path = os.path.join(skill_dir, "manifest.yaml")
                if not os.path.exists(manifest_path):
                    logger.warning(f"Skill {skill_id} missing manifest.yaml, skipping.")
                    continue

                with open(manifest_path, 'r', encoding='utf-8') as f:
                    manifest_data = yaml.safe_load(f)
                    manifest = SkillManifest(**manifest_data)

                # 2. 动态加载 handler.py
                handler_path = os.path.join(skill_dir, "handler.py")
                if not os.path.exists(handler_path):
                    logger.warning(f"Skill {skill_id} missing handler.py, skipping.")
                    continue

                # 动态导入模块
                spec = importlib.util.spec_from_file_location(f"skills.{skill_id}.handler", handler_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # 查找继承自 BaseSkill 的类或名为 Skill 的类
                skill_class = getattr(module, "Skill", None)
                if not skill_class:
                    logger.error(f"Skill {skill_id} handler.py must define a 'Skill' class.")
                    continue

                # 实例化并注册
                self.skills[skill_id] = skill_class(manifest=manifest)
                logger.info(f"Loaded Skill: {manifest.name} ({skill_id})")

            except Exception as e:
                logger.error(f"Failed to load skill {skill_id}: {e}")

    def get_skill(self, skill_id: str) -> Optional[BaseSkill]:
        return self.skills.get(skill_id)

    def get_all_skills(self) -> Dict[str, BaseSkill]:
        return self.skills

    def get_tier1_triggers(self) -> List[Dict]:
        """
        返回所有定义了确定性触发条件的 Skill 及其规则。
        """
        triggers = []
        for sid, skill in self.skills.items():
            if skill.manifest.triggers and skill.manifest.triggers.conditions:
                triggers.append({
                    "skill_id": sid,
                    "priority": skill.manifest.triggers.priority,
                    "conditions": skill.manifest.triggers.conditions
                })
        # 按优先级排序
        return sorted(triggers, key=lambda x: x['priority'], reverse=True)

# 全局单例
_manager = None

def get_skill_manager() -> SkillManager:
    global _manager
    if _manager is None:
        # 尝试从项目根目录查找
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        registry = os.path.join(root, "skills_registry")
        _manager = SkillManager(registry_path=registry)
    return _manager
