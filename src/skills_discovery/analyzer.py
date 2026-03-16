"""
Skill Discovery Analyzer — 分析历史邮件数据，发现可复用的处理模式。

数据流:
  1. 从 Qdrant 滚动查询所有唯一邮件
  2. 统计发件人、主题模式、回复率等维度
  3. 调用 LLM 进行深层模式挖掘
  4. 输出结构化的 Pattern 列表
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredPattern:
    """A discovered email routing pattern."""

    id: str
    name: str
    description: str
    trigger_type: str  # sender_match, subject_match, combined, to_match
    conditions: list[dict] = field(default_factory=list)
    reply_rate: float = 0.0
    sample_count: int = 0
    suggested_priority: str = "P2"
    suggested_need_reply: bool = True
    suggested_tone: str = ""
    example_subjects: list[str] = field(default_factory=list)
    example_senders: list[str] = field(default_factory=list)
    confidence: float = 0.0
    condition_logic: str = "and"  # 顶层条件逻辑: "and" | "or"


@dataclass
class EmailRecord:
    """Lightweight email record for analysis."""

    id: str
    subject: str
    sender: str
    to: list[str]
    cc: list[str]
    received_at: str
    message_type: str  # received, sent, draft
    source_folder: str = ""
    body_preview: str = ""
    in_reply_to: str = ""
    thread_id: str = ""


class EmailHistoryCollector:
    """Collects email history from Qdrant for analysis."""

    def __init__(self, qdrant_client, collection_name: str = "emails"):
        self.client = qdrant_client
        self.collection = collection_name

    def collect(self, limit: int = 5000) -> list[EmailRecord]:
        """Scroll through Qdrant and collect unique emails."""
        seen_ids: set[str] = set()
        records: list[EmailRecord] = []
        offset = None

        try:
            self.client.get_collection(self.collection)
        except Exception:
            logger.warning("Qdrant collection '%s' does not exist", self.collection)
            return []

        while len(records) < limit:
            batch_size = min(100, limit - len(records))
            points, next_offset = self.client.scroll(
                collection_name=self.collection,
                limit=batch_size,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            if not points:
                break

            for point in points:
                p = point.payload or {}
                eid = p.get("id", "")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)

                to_raw = p.get("to", [])
                cc_raw = p.get("cc", [])
                if isinstance(to_raw, str):
                    to_raw = [to_raw]
                if isinstance(cc_raw, str):
                    cc_raw = [cc_raw]

                body = p.get("body_preview", "") or p.get("body", "")
                if len(body) > 500:
                    body = body[:500]

                records.append(EmailRecord(
                    id=eid,
                    subject=p.get("subject", ""),
                    sender=p.get("sender", ""),
                    to=to_raw,
                    cc=cc_raw,
                    received_at=p.get("received_at", ""),
                    message_type=p.get("type", "received"),
                    source_folder=p.get("source_folder", ""),
                    body_preview=body,
                    in_reply_to=p.get("in_reply_to", ""),
                    thread_id=p.get("thread_id", ""),
                ))

            offset = next_offset
            if offset is None:
                break

        logger.info("Collected %d unique emails from Qdrant", len(records))
        return records


class PatternAnalyzer:
    """Analyzes email history to discover routing patterns."""

    def __init__(self, records: list[EmailRecord], my_email: str = ""):
        self.records = records
        self.my_email = my_email.lower().strip()
        self.received = [r for r in records if r.message_type != "sent"]
        self.sent = [r for r in records if r.message_type == "sent"]
        self._reply_map: dict[str, bool] = {}
        self._build_reply_map()
        # 如果 my_email 未提供，尝试从 sent 邮件推断（使用精确邮箱提取）
        if not self.my_email and self.sent:
            self.my_email = self._extract_email(self.sent[0].sender)

    def _extract_email(self, addr: str) -> str:
        """从地址字符串中提取邮箱。"""
        m = re.search(r'[\w.-]+@[\w.-]+', addr.lower())
        return m.group() if m else addr.lower().strip()

    def _build_reply_map(self):
        """Match sent replies to received emails by thread/subject."""
        sent_subjects: set[str] = set()
        sent_recipients: set[str] = set()

        for s in self.sent:
            normalized = re.sub(r"^(Re:\s*|Fw:\s*|转发:\s*|回复:\s*)+", "", s.subject, flags=re.IGNORECASE).strip().lower()
            sent_subjects.add(normalized)
            for addr in s.to:
                sent_recipients.add(addr.lower().strip())

        for r in self.received:
            normalized = re.sub(r"^(Re:\s*|Fw:\s*|转发:\s*|回复:\s*)+", "", r.subject, flags=re.IGNORECASE).strip().lower()
            sender_lower = r.sender.lower().strip()
            email_match = re.search(r'[\w.-]+@[\w.-]+', sender_lower)
            sender_email = email_match.group() if email_match else sender_lower

            replied = (
                normalized in sent_subjects
                or sender_email in sent_recipients
            )
            self._reply_map[r.id] = replied

    def compute_statistics(self) -> dict[str, Any]:
        """Compute basic statistics over the email history."""
        sender_counts: Counter = Counter()
        sender_replied: Counter = Counter()
        subject_words: Counter = Counter()

        for r in self.received:
            email_match = re.search(r'[\w.-]+@[\w.-]+', r.sender)
            sender_key = email_match.group() if email_match else r.sender
            sender_counts[sender_key] += 1
            if self._reply_map.get(r.id):
                sender_replied[sender_key] += 1

            words = re.findall(r'[\u4e00-\u9fff]+|[A-Za-z]+', r.subject)
            subject_words.update(w for w in words if len(w) > 1)

        return {
            "total_received": len(self.received),
            "total_sent": len(self.sent),
            "unique_senders": len(sender_counts),
            "top_senders": sender_counts.most_common(20),
            "sender_reply_rates": {
                s: (sender_replied[s] / c if c > 0 else 0)
                for s, c in sender_counts.most_common(20)
            },
            "top_subject_words": subject_words.most_common(30),
            "mailing_lists": self._analyze_mailing_lists(),
            "to_vs_cc_reply_rate": self._analyze_to_vs_cc(),
            "frequent_recipient_combos": self._analyze_recipient_combos(),
            "thread_stats": self._analyze_threads(),
        }

    def _analyze_mailing_lists(self) -> list[dict]:
        """识别邮件组地址及其回复率。"""
        list_patterns = re.compile(
            r'(^all[-_@]|[-_]team@|[-_]group@|[-_]list@|[-_]dept@'
            r'|^noreply@|^no[-_]reply@|^newsletter@|^announce[s]?@'
            r'|^notifications?@|^info@|^hr@|^finance@|^marketing@)',
            re.IGNORECASE,
        )
        addr_counts: Counter = Counter()
        addr_replied: Counter = Counter()

        for r in self.received:
            for addr_list in (r.to, r.cc):
                for addr in addr_list:
                    email_match = re.search(r'[\w.-]+@[\w.-]+', addr.lower())
                    if not email_match:
                        continue
                    clean = email_match.group()
                    if list_patterns.search(clean):
                        addr_counts[clean] += 1
                        if self._reply_map.get(r.id):
                            addr_replied[clean] += 1

        result = []
        for addr, count in addr_counts.most_common(20):
            if count >= 2:
                rate = addr_replied[addr] / count
                result.append({"address": addr, "count": count, "reply_rate": rate})
        return result

    def _analyze_to_vs_cc(self) -> dict:
        """分析我在 TO vs CC 中的回复率差异。"""
        to_count, to_replied = 0, 0
        cc_count, cc_replied = 0, 0

        for r in self.received:
            in_to = any(self._extract_email(addr) == self.my_email for addr in r.to) if self.my_email else False
            in_cc = any(self._extract_email(addr) == self.my_email for addr in r.cc) if self.my_email else False
            replied = self._reply_map.get(r.id, False)

            if in_to:
                to_count += 1
                if replied:
                    to_replied += 1
            elif in_cc:
                cc_count += 1
                if replied:
                    cc_replied += 1

        return {
            "to_count": to_count,
            "to_reply_rate": to_replied / to_count if to_count > 0 else 0.0,
            "cc_count": cc_count,
            "cc_reply_rate": cc_replied / cc_count if cc_count > 0 else 0.0,
        }

    def _analyze_recipient_combos(self) -> list[dict]:
        """发现频繁共现的收件人组合。"""
        combo_counts: Counter = Counter()
        combo_replied: Counter = Counter()

        for r in self.received:
            all_recipients: set[str] = set()
            for addr in r.to + r.cc:
                email_match = re.search(r'[\w.-]+@[\w.-]+', addr.lower())
                if email_match:
                    clean = email_match.group()
                    if clean != self.my_email:
                        all_recipients.add(clean)
            # 仅处理 2-4 人的收件人集合，超过 4 人直接跳过（避免截断带来的虚假匹配）
            if 2 <= len(all_recipients) <= 4:
                combo = tuple(sorted(all_recipients))
                combo_counts[combo] += 1
                if self._reply_map.get(r.id):
                    combo_replied[combo] += 1

        result = []
        for combo, count in combo_counts.most_common(10):
            if count >= 3:
                rate = combo_replied[combo] / count
                result.append({
                    "recipients": list(combo),
                    "count": count,
                    "reply_rate": rate,
                })
        return result

    def _analyze_threads(self) -> list[dict]:
        """按 thread_id 聚合，计算线程深度和用户参与度。"""
        threads: dict[str, list[EmailRecord]] = defaultdict(list)

        for r in self.records:
            tid = r.thread_id
            if not tid:
                continue
            threads[tid].append(r)

        result = []
        for tid, emails in threads.items():
            if len(emails) < 2:
                continue
            depth = len(emails)
            my_replies = sum(1 for e in emails if e.message_type == "sent")
            participation = my_replies / depth if depth > 0 else 0.0

            result.append({
                "thread_id": tid,
                "depth": depth,
                "my_replies": my_replies,
                "participation": participation,
                "subject": emails[0].subject,
                "senders": list({e.sender for e in emails if e.message_type != "sent"}),
            })

        # 按深度降序排列
        result.sort(key=lambda t: t["depth"], reverse=True)
        return result[:20]

    def build_llm_prompt(self, stats: dict) -> str:
        """构建多维度 LLM 分析 prompt。"""
        # --- 1. 发件人统计 ---
        sender_lines = []
        for sender, count in stats["top_senders"]:
            rate = stats["sender_reply_rates"].get(sender, 0)
            reply_label = f"{rate:.0%}" if count >= 2 else "N/A"
            sender_lines.append(f"  - {sender}: {count} 封, 回复率 {reply_label}")

        # --- 2. 收件人/抄送维度 ---
        recipient_section = ""
        mailing_lists = stats.get("mailing_lists", [])
        if mailing_lists:
            ml_lines = [
                f"  - {ml['address']}: {ml['count']} 封, 回复率 {ml['reply_rate']:.0%}"
                for ml in mailing_lists[:10]
            ]
            recipient_section += "## 邮件组地址统计\n" + "\n".join(ml_lines) + "\n\n"

        to_vs_cc = stats.get("to_vs_cc_reply_rate", {})
        if to_vs_cc.get("to_count", 0) > 0 or to_vs_cc.get("cc_count", 0) > 0:
            recipient_section += (
                f"## TO vs CC 收件人回复率\n"
                f"  - 我在 TO 中: {to_vs_cc.get('to_count', 0)} 封, "
                f"回复率 {to_vs_cc.get('to_reply_rate', 0):.0%}\n"
                f"  - 我在 CC 中: {to_vs_cc.get('cc_count', 0)} 封, "
                f"回复率 {to_vs_cc.get('cc_reply_rate', 0):.0%}\n\n"
            )

        combos = stats.get("frequent_recipient_combos", [])
        if combos:
            combo_lines = [
                f"  - {', '.join(c['recipients'])}: {c['count']} 封, 回复率 {c['reply_rate']:.0%}"
                for c in combos[:5]
            ]
            recipient_section += "## 高频收件人组合\n" + "\n".join(combo_lines) + "\n\n"

        # --- 3. 线程深度统计 ---
        thread_section = ""
        thread_stats = stats.get("thread_stats", [])
        deep_threads = [t for t in thread_stats if t["depth"] >= 2]
        if deep_threads:
            thread_lines = [
                f"  - 线程深度={t['depth']}, 我的回复={t['my_replies']}, "
                f"参与度={t['participation']:.0%}, 主题=\"{t['subject'][:40]}\""
                for t in deep_threads[:10]
            ]
            thread_section = "## 线程深度分析\n" + "\n".join(thread_lines) + "\n\n"

        # --- 4. 带正文样本的邮件列表 ---
        sample_emails = []
        for r in self.received[:80]:
            replied = "✅" if self._reply_map.get(r.id) else "❌"
            body_snippet = ""
            if r.body_preview:
                body_snippet = f', 正文样本="{r.body_preview[:100]}"'
            sample_emails.append(
                f"  - [{replied}] sender={r.sender}, subject=\"{r.subject[:50]}\", "
                f"to={','.join(r.to[:2])}{body_snippet}"
            )

        return f"""你是一个邮件路由模式分析专家。请根据以下多维度邮件历史数据，发现可以自动化处理的邮件路由规则。

## 统计概要
- 收到邮件: {stats['total_received']} 封
- 已发送回复: {stats['total_sent']} 封
- 不同发件人: {stats['unique_senders']} 个

## 高频发件人 (及回复率)
{chr(10).join(sender_lines)}

## 高频主题关键词
{', '.join(f'{w}({c})' for w, c in stats['top_subject_words'][:20])}

{recipient_section}{thread_section}## 邮件样本 (✅=已回复, ❌=未回复, 含正文样本)
{chr(10).join(sample_emails)}

## 任务
请识别 3-12 个有意义的邮件路由模式。每个模式应该是可以被自动化处理的规则链。

对每个模式，请提供:
1. **name**: 简短的模式名称 (中文)
2. **description**: 对该模式的描述
3. **trigger_type**: 触发类型，可以是 "sender_match" (发件人), "subject_match" (主题), "combined" (组合), "to_match" (收件人), "cc_match" (抄送), "recipient_role" (收件角色), "body_match" (正文), "thread_depth" (线程深度)
4. **condition_logic**: 条件组合方式，"and" (所有条件都满足) 或 "or" (任一条件满足)
5. **conditions**: 触发条件列表，每个条件格式为 {{"type": "sender_match|subject_match|to_match|cc_match|body_match|thread_depth", "operator": "in|contains|regex|gte", "value": "..."}}
6. **reply_rate**: 该模式对应邮件的回复率 (0.0-1.0)
7. **sample_count**: 匹配该模式的邮件数量
8. **suggested_priority**: 建议的优先级 (P0/P1/P2/P3)
9. **suggested_need_reply**: 是否需要回复 (true/false)
10. **suggested_tone**: 建议的回复语气 (可选)
11. **example_subjects**: 2-3 个匹配该模式的示例主题

请以 JSON 数组格式输出，不要包含其他内容。"""

    async def discover_with_llm(self) -> list[DiscoveredPattern]:
        """Use LLM to discover patterns from email history."""
        if len(self.received) < 3:
            logger.warning("邮件数量不足 (%d)，无法进行有效的模式发现", len(self.received))
            return self._discover_heuristic()

        stats = self.compute_statistics()
        prompt = self.build_llm_prompt(stats)

        try:
            from src.providers.factory import get_llm
            from langchain_core.messages import HumanMessage

            llm = get_llm(temperature=0)
            response = await llm.ainvoke([HumanMessage(content=prompt)])
            content = response.content

            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if not json_match:
                logger.warning("LLM 未返回有效 JSON，回退到启发式分析")
                return self._discover_heuristic()

            raw_patterns = json.loads(json_match.group())
            return self._parse_llm_patterns(raw_patterns)

        except Exception as e:
            logger.warning("LLM 分析失败 (%s)，回退到启发式分析", e)
            return self._discover_heuristic()

    def _parse_llm_patterns(self, raw: list[dict]) -> list[DiscoveredPattern]:
        patterns = []
        for i, item in enumerate(raw):
            conditions = item.get("conditions", [])
            for cond in conditions:
                if isinstance(cond.get("value"), list):
                    pass
                elif isinstance(cond.get("value"), str):
                    pass
                else:
                    cond["value"] = str(cond.get("value", ""))

            patterns.append(DiscoveredPattern(
                id=f"discovered_{i+1:03d}",
                name=item.get("name", f"Pattern {i+1}"),
                description=item.get("description", ""),
                trigger_type=item.get("trigger_type", "combined"),
                conditions=conditions,
                reply_rate=float(item.get("reply_rate", 0)),
                sample_count=int(item.get("sample_count", 0)),
                suggested_priority=item.get("suggested_priority", "P2"),
                suggested_need_reply=bool(item.get("suggested_need_reply", True)),
                suggested_tone=item.get("suggested_tone", ""),
                example_subjects=item.get("example_subjects", []),
                example_senders=item.get("example_senders", []),
                confidence=min(1.0, float(item.get("sample_count", 0)) / 10),
            ))
        return patterns

    def _discover_heuristic(self) -> list[DiscoveredPattern]:
        """Fallback: discover patterns using simple heuristics when LLM is unavailable."""
        patterns = []
        sender_counts: Counter = Counter()
        sender_replied: Counter = Counter()
        sender_subjects: dict[str, list[str]] = defaultdict(list)

        for r in self.received:
            email_match = re.search(r'[\w.-]+@[\w.-]+', r.sender)
            sender_key = email_match.group() if email_match else r.sender
            sender_counts[sender_key] += 1
            sender_subjects[sender_key].append(r.subject)
            if self._reply_map.get(r.id):
                sender_replied[sender_key] += 1

        idx = 0
        for sender, count in sender_counts.most_common(10):
            if count < 3:
                continue
            rate = sender_replied[sender] / count if count > 0 else 0
            subjects = sender_subjects[sender][:3]

            idx += 1
            if rate >= 0.6:
                priority, need_reply = "P1", True
            elif rate >= 0.3:
                priority, need_reply = "P2", True
            else:
                priority, need_reply = "P2", False

            patterns.append(DiscoveredPattern(
                id=f"discovered_{idx:03d}",
                name=f"{sender.split('@')[0]} 邮件处理",
                description=f"来自 {sender} 的邮件 ({count} 封, 回复率 {rate:.0%})",
                trigger_type="sender_match",
                conditions=[{
                    "type": "sender_match",
                    "operator": "contains",
                    "value": sender,
                }],
                reply_rate=rate,
                sample_count=count,
                suggested_priority=priority,
                suggested_need_reply=need_reply,
                example_subjects=subjects,
                example_senders=[sender],
                confidence=min(1.0, count / 10),
            ))

        return patterns
