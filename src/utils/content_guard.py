"""
Content Guard — 草稿质量与安全检查。
检测幻觉（事实核验）和敏感信息（regex 拦截），纯规则驱动，无 LLM 调用。
"""

import re
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

_CHINESE_NAME_RE = re.compile(r"[\u4e00-\u9fff]{2,4}")
_DATE_RE = re.compile(
    r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日号]?"
    r"|\d{1,2}[-/月]\d{1,2}[日号]?"
    r"|(?:周|星期)[一二三四五六日天]"
    r"|(?:今|明|后|昨|前)天"
    r"|(?:上|下|这|本)(?:周|个?月)"
)
_NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?%?\b")


class ContentGuard:
    """Checks email drafts for hallucinations and sensitive content."""

    def __init__(self):
        self._sensitive_patterns = self._compile_patterns()

    @staticmethod
    def _compile_patterns() -> List[tuple]:
        patterns = [
            (re.compile(r"(薪资|工资|年薪|月薪|底薪|奖金)\s*[:：]?\s*\d+", re.IGNORECASE), "salary_info"),
            (re.compile(r"(内部代号|代号|Code)\s*[:：]?\s*[A-Z]{2,}-\d+", re.IGNORECASE), "internal_code"),
            (re.compile(r"(机密|绝密|内部文件|CONFIDENTIAL|INTERNAL\s+ONLY)", re.IGNORECASE), "confidential_marker"),
            (re.compile(r"\b\d{15,18}[xX]?\b"), "id_number"),
            (re.compile(r"[¥￥]\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"), "financial_amount"),
        ]
        return patterns

    def check_sensitive(self, draft: str) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        for pattern, category in self._sensitive_patterns:
            for m in pattern.finditer(draft):
                if category == "financial_amount":
                    raw = m.group(1).replace(",", "")
                    try:
                        if float(raw) < 100000:
                            continue
                    except ValueError:
                        continue
                issues.append({
                    "category": category,
                    "matched_text": m.group()[:60],
                    "position": m.start(),
                })
        return issues

    async def check_hallucination(self, draft: str, original_email: dict) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        ref_text = " ".join([
            str(original_email.get("subject", "")),
            str(original_email.get("body", "")),
            str(original_email.get("sender", "")),
            " ".join(str(r) for r in (original_email.get("to") or [])),
            " ".join(str(r) for r in (original_email.get("cc") or [])),
        ]).lower()

        for m in _DATE_RE.finditer(draft):
            token = m.group().lower()
            if token not in ref_text:
                issues.append({"type": "unverified_date", "claim": m.group(), "severity": "warning"})

        draft_names = set(_CHINESE_NAME_RE.findall(draft))
        ref_names = set(_CHINESE_NAME_RE.findall(ref_text))
        common_words = {"您好", "你好", "谢谢", "感谢", "请问", "回复", "邮件", "附件", "发送",
                        "通知", "会议", "报告", "方案", "内容", "情况", "确认", "处理", "收到",
                        "了解", "安排", "尽快", "相关", "公司", "部门", "项目"}
        for name in draft_names - ref_names - common_words:
            issues.append({"type": "unverified_name", "claim": name, "severity": "warning"})

        draft_nums = set(_NUMBER_RE.findall(draft))
        trivial = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "100"}
        for num in draft_nums - trivial:
            if num not in ref_text:
                issues.append({"type": "unverified_number", "claim": num, "severity": "warning"})

        return issues

    async def run_all_checks(self, draft: str, original_email: dict) -> Dict[str, Any]:
        sensitive = self.check_sensitive(draft)
        hallucination = await self.check_hallucination(draft, original_email)

        all_issues = len(sensitive) + len(hallucination)
        passed = all_issues == 0

        summary_parts = []
        if sensitive:
            summary_parts.append(f"{len(sensitive)} 项敏感信息")
        if hallucination:
            summary_parts.append(f"{len(hallucination)} 项待核实内容")

        return {
            "passed": passed,
            "sensitive_issues": sensitive,
            "hallucination_issues": hallucination,
            "summary": "；".join(summary_parts) if summary_parts else "检查通过",
        }
