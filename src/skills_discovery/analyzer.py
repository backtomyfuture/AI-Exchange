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


def strip_images_from_body(body: str) -> str:
    """移除 HTML body 中的 <img> 标签（含属性，支持自闭合和非自闭合）。"""
    return re.sub(r'<img[^>]*?/?>', '', body, flags=re.IGNORECASE)


def _normalize_mailbox(raw: str) -> str:
    """将 Mailbox(name='张霞', email_address='zhang-xia@...') 转为 '张霞 <zhang-xia@...>'。
    标准 RFC 格式（'Name <email>'）或纯邮箱格式直接返回。"""
    if not raw or not isinstance(raw, str):
        return raw or ""
    # 已经是标准格式
    if "@" in raw and "Mailbox(" not in raw:
        return raw
    # 解析 Mailbox(...) 格式
    name_m = re.search(r"name='([^']*)'", raw)
    email_m = re.search(r"email_address='([^']*)'", raw)
    if email_m:
        email = email_m.group(1)
        name = name_m.group(1) if name_m else ""
        return f"{name} <{email}>" if name else email
    return raw


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

                # 兼容两种数据源：
                # - Exchange源: to_recipients/cc_recipients (存储 Mailbox(...) 字符串列表)
                # - PST导入源: to/cc (存储 "Name <email>" 字符串列表)
                to_raw = p.get("to") or p.get("to_recipients") or []
                cc_raw = p.get("cc") or p.get("cc_recipients") or []
                if isinstance(to_raw, str):
                    to_raw = [to_raw]
                if isinstance(cc_raw, str):
                    cc_raw = [cc_raw]
                # 将 Mailbox(name='张霞', email_address='...') 转为标准 "Name <email>" 格式
                to_raw = [_normalize_mailbox(addr) for addr in to_raw]
                cc_raw = [_normalize_mailbox(addr) for addr in cc_raw]

                # 兼容两种邮件类型字段：
                # - PST源: type = "sent" / "received"
                # - Exchange源: 无 type 字段，通过 _parent_folder_name 或 source_folder 判断
                msg_type = p.get("type") or ""
                if not msg_type:
                    folder = p.get("_parent_folder_name", "") or p.get("source_folder", "")
                    folder_lower = folder.lower()
                    if any(kw in folder_lower for kw in ("sent", "已发送", "发件")):
                        msg_type = "sent"
                    else:
                        msg_type = "received"

                # sender 字段也可能是 Mailbox(...) 格式
                sender_raw = _normalize_mailbox(p.get("sender", ""))

                body = p.get("body_preview", "") or p.get("body", "")
                body = strip_images_from_body(body)
                if len(body) > 1000:
                    body = body[:1000]

                records.append(EmailRecord(
                    id=eid,
                    subject=p.get("subject", ""),
                    sender=sender_raw,
                    to=to_raw,
                    cc=cc_raw,
                    received_at=p.get("received_at", ""),
                    message_type=msg_type,
                    source_folder=p.get("source_folder", "") or p.get("_parent_folder_name", ""),
                    body_preview=body,
                    in_reply_to=p.get("in_reply_to", ""),
                    thread_id=p.get("thread_id", "") or p.get("conversation_id", ""),
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
            normalized = re.sub(r"^(Re:\s*|Fw:\s*|Fwd:\s*|转发:\s*|回复:\s*|答复:\s*)+", "", s.subject, flags=re.IGNORECASE).strip().lower()
            sent_subjects.add(normalized)
            for addr in s.to:
                sent_recipients.add(addr.lower().strip())

        for r in self.received:
            normalized = re.sub(r"^(Re:\s*|Fw:\s*|Fwd:\s*|转发:\s*|回复:\s*|答复:\s*)+", "", r.subject, flags=re.IGNORECASE).strip().lower()
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

    # 转发/呈阅检测关键词（类级别编译，避免重复编译）
    _FYI_SUBJECT_PATTERNS = re.compile(
        r'^(FW:|Fw:|Fwd:|转发[:：])|【呈阅|\[呈阅|呈阅示',
        re.IGNORECASE,
    )
    _FYI_BODY_KEYWORDS = re.compile(
        r'呈阅|请知|请悉|谨呈|敬请知悉|请阅|知悉',
    )

    def _detect_forward_fyi(self, record: EmailRecord) -> bool:
        """判断邮件是否为转发或呈阅类（不需要回复）。"""
        if self._FYI_SUBJECT_PATTERNS.search(record.subject):
            return True
        if record.body_preview and self._FYI_BODY_KEYWORDS.search(record.body_preview):
            return True
        return False

    def _analyze_group_received(self) -> list[dict]:
        """分析 to/cc 均不含 my_email 的邮件（群组成员收件）。

        返回按群组地址分组的统计，每项包含：
        - group_address: 群组/部门地址（或发件人，当 to 为空时）
        - count: 邮件数
        - reply_rate: 回复率
        - example_subjects: 示例主题
        """
        if not self.my_email:
            return []

        group_counts: Counter = Counter()
        group_replied: Counter = Counter()
        group_subjects: dict[str, list[str]] = defaultdict(list)

        for r in self.received:
            # 判断是否为群组收件：to 和 cc 中都没有 my_email
            in_to = any(self._extract_email(addr) == self.my_email for addr in r.to)
            in_cc = any(self._extract_email(addr) == self.my_email for addr in r.cc)
            if in_to or in_cc:
                continue  # 直接收件或 CC，不是群组

            # 确定分组 key：优先用 to 中第一个地址，否则用发件人
            if r.to:
                email_m = re.search(r'[\w.-]+@[\w.-]+', r.to[0].lower())
                group_key = email_m.group() if email_m else r.to[0].lower()
            else:
                email_m = re.search(r'[\w.-]+@[\w.-]+', r.sender.lower())
                group_key = email_m.group() if email_m else r.sender.lower()

            group_counts[group_key] += 1
            group_subjects[group_key].append(r.subject)
            if self._reply_map.get(r.id):
                group_replied[group_key] += 1

        result = []
        for addr, count in group_counts.most_common(20):
            if count < 3:
                continue
            rate = group_replied[addr] / count
            result.append({
                "group_address": addr,
                "count": count,
                "reply_rate": rate,
                "example_subjects": group_subjects[addr][:3],
            })
        return result

    def build_llm_prompt(self, stats: dict) -> str:
        """构建多维度 LLM 分析 prompt。"""
        # --- 1. 发件人统计 ---
        sender_lines = []
        for sender, count in stats["top_senders"]:
            rate = stats["sender_reply_rates"].get(sender, 0)
            reply_label = f"{rate:.0%}" if count >= 2 else "N/A"
            sender_lines.append(f"  - {sender}: {count} 封, 回复率 {reply_label}")

        # --- 0. 我的角色分布（基于 my_email）---
        role_section = ""
        if self.my_email:
            to_count_r = sum(
                1 for r in self.received
                if any(self._extract_email(a) == self.my_email for a in r.to)
            )
            cc_count_r = sum(
                1 for r in self.received
                if not any(self._extract_email(a) == self.my_email for a in r.to)
                and any(self._extract_email(a) == self.my_email for a in r.cc)
            )
            group_count_r = len(self.received) - to_count_r - cc_count_r
            fyi_count_r = sum(1 for r in self.received if self._detect_forward_fyi(r))
            role_section = (
                f"## 我的角色分布（my_email: {self.my_email}）\n"
                f"  - 直接收件（TO 含我）：{to_count_r} 封\n"
                f"  - 仅抄送（CC 含我）：{cc_count_r} 封\n"
                f"  - 群组成员（TO/CC 均无我）：{group_count_r} 封\n"
                f"  - 疑似转发/呈阅邮件：{fyi_count_r} 封\n\n"
            )

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

{role_section}{recipient_section}{thread_section}## 邮件样本 (✅=已回复, ❌=未回复, 含正文样本)
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
                condition_logic=item.get("condition_logic", "and"),
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
        """基于角色驱动的启发式规则发现。

        步骤优先级：
        1. 转发/呈阅检测（priority=80）
        2. 群组收件（to/cc 均无 my_email，priority=60）
        3. CC 抄送（priority=50）
        4. 直接收件 TO（发件人+to 组合，priority=40）
        5. 已知邮件组正则（priority=30）
        6. 线程深度（priority=20）
        """
        patterns = []
        idx = 0

        # ------------------------------------------------------------------ #
        # 步骤 1: 转发/呈阅模式                                                #
        # ------------------------------------------------------------------ #
        fyi_records = [r for r in self.received if self._detect_forward_fyi(r)]
        if len(fyi_records) >= 2:
            idx += 1
            rate = sum(1 for r in fyi_records if self._reply_map.get(r.id)) / len(fyi_records)
            patterns.append(DiscoveredPattern(
                id=f"discovered_{idx:03d}",
                name="转发与呈阅邮件",
                description=(
                    f"主题含转发标记或正文含呈阅/请知关键词的邮件 "
                    f"({len(fyi_records)} 封, 回复率 {rate:.0%})，通常不需要回复"
                ),
                trigger_type="combined",
                condition_logic="or",
                conditions=[
                    {
                        "type": "subject_match",
                        "operator": "regex",
                        "value": r"^(FW:|Fw:|Fwd:|转发[:：])|【呈阅|\[呈阅|呈阅示",
                    },
                    {
                        "type": "body_match",
                        "operator": "regex",
                        "value": "呈阅|请知|请悉|谨呈|敬请知悉|请阅|知悉",
                    },
                ],
                reply_rate=rate,
                sample_count=len(fyi_records),
                suggested_priority="P3",
                suggested_need_reply=False,
                example_subjects=[r.subject for r in fyi_records[:3]],
                confidence=min(1.0, len(fyi_records) / 10),
            ))

        # ------------------------------------------------------------------ #
        # 步骤 2: 群组收件模式（to/cc 均无 my_email）                          #
        # ------------------------------------------------------------------ #
        group_data = self._analyze_group_received()
        for grp in group_data:
            idx += 1
            rate = grp["reply_rate"]
            patterns.append(DiscoveredPattern(
                id=f"discovered_{idx:03d}",
                name=f"{grp['group_address'].split('@')[0]} 群组邮件",
                description=(
                    f"发送到 {grp['group_address']} 的邮件，我通过群组成员身份收到 "
                    f"({grp['count']} 封, 回复率 {rate:.0%})"
                ),
                trigger_type="to_match",
                conditions=[{
                    "type": "to_match",
                    "operator": "contains",
                    "value": grp["group_address"],
                }],
                reply_rate=rate,
                sample_count=grp["count"],
                suggested_priority="P3",
                suggested_need_reply=False,
                example_subjects=grp["example_subjects"],
                confidence=min(1.0, grp["count"] / 10),
            ))

        # ------------------------------------------------------------------ #
        # 步骤 3: CC 抄送模式                                                  #
        # ------------------------------------------------------------------ #
        to_vs_cc = self._analyze_to_vs_cc()
        cc_count = to_vs_cc.get("cc_count", 0)
        cc_rate = to_vs_cc.get("cc_reply_rate", 0.0)
        to_rate = to_vs_cc.get("to_reply_rate", 0.0)
        to_count = to_vs_cc.get("to_count", 0)
        if cc_count >= 3 and cc_rate < 0.3 and (not self.my_email or to_count == 0 or (to_rate - cc_rate) > 0.1):
            idx += 1
            patterns.append(DiscoveredPattern(
                id=f"discovered_{idx:03d}",
                name="抄送通知（我在 CC）",
                description=(
                    f"我仅在 CC 中的邮件 ({cc_count} 封, 回复率 {cc_rate:.0%})，通常不需要回复"
                ),
                trigger_type="recipient_role",
                conditions=[{
                    "type": "cc_match",
                    "operator": "contains",
                    "value": self.my_email or "$ME",
                }],
                reply_rate=cc_rate,
                sample_count=cc_count,
                suggested_priority="P3",
                suggested_need_reply=False,
                confidence=min(1.0, cc_count / 10),
            ))

        # ------------------------------------------------------------------ #
        # 步骤 4: 直接收件（TO 含 my_email）按发件人分组                       #
        # ------------------------------------------------------------------ #
        # 筛选出直接发给我（to 含 my_email）且不是转发/呈阅的邮件
        direct_records = []
        for r in self.received:
            if self._detect_forward_fyi(r):
                continue
            if self.my_email:
                in_to = any(self._extract_email(addr) == self.my_email for addr in r.to)
                if not in_to:
                    continue
            direct_records.append(r)

        direct_sender_counts: Counter = Counter()
        direct_sender_replied: Counter = Counter()
        direct_sender_subjects: dict[str, list[str]] = defaultdict(list)

        for r in direct_records:
            email_match = re.search(r'[\w.-]+@[\w.-]+', r.sender)
            sender_key = email_match.group() if email_match else r.sender
            direct_sender_counts[sender_key] += 1
            direct_sender_subjects[sender_key].append(r.subject)
            if self._reply_map.get(r.id):
                direct_sender_replied[sender_key] += 1

        for sender, count in direct_sender_counts.most_common(10):
            if count < 3:
                continue
            rate = direct_sender_replied[sender] / count if count > 0 else 0
            idx += 1

            if rate >= 0.6:
                priority, need_reply = "P1", True
            elif rate >= 0.3:
                priority, need_reply = "P2", True
            else:
                priority, need_reply = "P3", False

            # 有 my_email 时生成"发件人+TO"组合条件，更精准
            if self.my_email:
                conditions = [
                    {"type": "sender_match", "operator": "contains", "value": sender},
                    {"type": "to_match", "operator": "contains", "value": self.my_email},
                ]
                trigger_type = "combined"
            else:
                conditions = [{"type": "sender_match", "operator": "contains", "value": sender}]
                trigger_type = "sender_match"

            name = (f"{sender.split('@')[0]} 直接发给我" if self.my_email
                    else f"{sender.split('@')[0]} 邮件处理")
            desc = (f"来自 {sender} 直接发给我的邮件 ({count} 封, 回复率 {rate:.0%})" if self.my_email
                    else f"来自 {sender} 的邮件 ({count} 封, 回复率 {rate:.0%})")

            patterns.append(DiscoveredPattern(
                id=f"discovered_{idx:03d}",
                name=name,
                description=desc,
                trigger_type=trigger_type,
                condition_logic="and",
                conditions=conditions,
                reply_rate=rate,
                sample_count=count,
                suggested_priority=priority,
                suggested_need_reply=need_reply,
                example_subjects=direct_sender_subjects[sender][:3],
                example_senders=[sender],
                confidence=min(1.0, count / 10),
            ))

        # ------------------------------------------------------------------ #
        # 步骤 5: 已知邮件组正则（与步骤2互补，覆盖可见邮件列表地址）           #
        # ------------------------------------------------------------------ #
        mailing_lists = self._analyze_mailing_lists()
        for ml in mailing_lists:
            if ml["count"] < 3:
                continue
            # 避免与步骤2重复：若该地址已在群组规则中，跳过
            already_covered = any(
                any(
                    c.get("type") == "to_match" and c.get("value") == ml["address"]
                    for c in p.conditions
                )
                for p in patterns
            )
            if already_covered:
                continue
            idx += 1
            rate = ml["reply_rate"]
            need_reply = rate >= 0.3
            priority = "P2" if need_reply else "P3"
            patterns.append(DiscoveredPattern(
                id=f"discovered_{idx:03d}",
                name=f"{ml['address'].split('@')[0]} 邮件组",
                description=f"发送到 {ml['address']} 的系统邮件 ({ml['count']} 封, 回复率 {rate:.0%})",
                trigger_type="to_match",
                conditions=[{"type": "to_match", "operator": "contains", "value": ml["address"]}],
                reply_rate=rate,
                sample_count=ml["count"],
                suggested_priority=priority,
                suggested_need_reply=need_reply,
                confidence=min(1.0, ml["count"] / 10),
            ))

        # ------------------------------------------------------------------ #
        # 步骤 6: 线程深度模式（需要有效 thread_id）                           #
        # ------------------------------------------------------------------ #
        thread_stats = self._analyze_threads()
        high_depth = [t for t in thread_stats if t["depth"] >= 3 and t["participation"] >= 0.3]
        if len(high_depth) >= 2:
            avg_depth = sum(t["depth"] for t in high_depth) / len(high_depth)
            avg_participation = sum(t["participation"] for t in high_depth) / len(high_depth)
            idx += 1
            patterns.append(DiscoveredPattern(
                id=f"discovered_{idx:03d}",
                name="深度讨论线程",
                description=(
                    f"检测到 {len(high_depth)} 个高参与度讨论线程 "
                    f"(平均深度 {avg_depth:.1f}, 平均参与度 {avg_participation:.0%})"
                ),
                trigger_type="thread_depth",
                conditions=[{"type": "thread_depth", "operator": "gte", "value": "3"}],
                reply_rate=avg_participation,
                sample_count=len(high_depth),
                suggested_priority="P1",
                suggested_need_reply=True,
                confidence=min(1.0, len(high_depth) / 5),
                example_subjects=[t["subject"] for t in high_depth[:3]],
            ))

        return patterns
