import re
import logging
from typing import List, Dict, Any
from src.router.manager import get_skill_manager

logger = logging.getLogger(__name__)

class Tier1ReflexRouter:
    """
    Tier 1 反射路由器：执行硬编码在 Skill Manifest 中的规则。
    """
    def __init__(self):
        self.manager = get_skill_manager()

    def route(self, email: Dict[str, Any]) -> List[str]:
        """
        根据邮件内容，返回匹配的 Skill ID 列表。
        """
        matched_skills = []
        triggers = self.manager.get_tier1_triggers()
        
        to_list = email.get("to") or []
        subject = email.get("subject") or ""
        body = email.get("body") or ""
        sender = email.get("sender") or ""
        if isinstance(to_list, str):
            to_list = [to_list]

        for trigger in triggers:
            skill_id = trigger["skill_id"]
            conditions = trigger["conditions"]
            
            is_match = True
            for cond in conditions:
                if not self._check_condition(cond, subject, body, sender, to_list):
                    is_match = False
                    break
            
            if is_match:
                logger.info(f"Tier 1 Match found: {skill_id}")
                matched_skills.append(skill_id)
        
        return matched_skills

    def _check_condition(self, cond: Dict, subject: str, body: str, sender: str, to_list: List[str]) -> bool:
        """
        检查单个条件是否匹配。
        支持类型: sender_match, subject_match, body_match, header_match, to_match
        支持操作符: eq, contains, regex, in
        """
        c_type = cond.get("type")
        operator = cond.get("operator", "contains")
        value = cond.get("value")
        
        # 处理 $ME 占位符
        if isinstance(value, str) and "$ME" in value:
            from src.config import get_settings
            me_email = get_settings().EXCHANGE_ACCOUNT_EMAIL
            value = value.replace("$ME", me_email)
        elif isinstance(value, list):
            from src.config import get_settings
            me_email = get_settings().EXCHANGE_ACCOUNT_EMAIL
            value = [v.replace("$ME", me_email) if isinstance(v, str) else v for v in value]
        
        # 获取要检查的目标文本或列表
        if c_type == "sender_match":
            target = sender
        elif c_type == "subject_match":
            target = subject
        elif c_type == "body_match":
            target = body
        elif c_type == "to_match":
            # 对收件人列表进行匹配
            if operator == "contains":
                return any(value.lower() in t.lower() for t in to_list)
            elif operator == "eq":
                return any(value.lower() == t.lower() for t in to_list)
            elif operator == "in":
                # 指 value (list) 中是否有任何一个在 to_list 中，或者 value (str) 是否在 to_list 中
                check_values = value if isinstance(value, list) else [value]
                return any(t.lower() in [v.lower() for v in check_values] for t in to_list)
            return False
        else:
            return False

        # 执行标准匹配 (针对字符串 target)
        if operator == "eq":
            return target == value
        elif operator == "contains":
            return value.lower() in target.lower()
        elif operator == "regex":
            return bool(re.search(value, target, re.IGNORECASE))
        elif operator == "in":
            return target.lower() in [v.lower() for v in value]
        
        return False
