# Skill Discovery 多维度增强 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 skill discovery 从单一发件人维度扩展为收件人/抄送 + 正文语义 + 线程深度三维度分析，并支持 AND/OR 组合条件。

**Architecture:** 在现有 `PatternAnalyzer` 基础上新增三个分析方法（收件人、正文语义、线程深度），增强 LLM prompt 和启发式路径，扩展 `DiscoveredPattern` 支持嵌套 AND/OR 条件，同步更新 generator 和 display 逻辑。运行时路由引擎（`tier1_reflex.py`）已支持 `to_match`/`body_match`/`cc_match` 条件类型，无需修改。

**Tech Stack:** Python 3.11+, dataclasses, LangChain LLM, PyYAML, pytest

**Design Doc:** `docs/plans/2026-03-16-skill-discovery-enhancement-design.md`

---

## Task 1: 扩展 EmailRecord 和 DiscoveredPattern 数据模型

**Files:**
- Modify: `src/skills_discovery/analyzer.py:23-56`
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试 — DiscoveredPattern 新增 condition_logic 字段**

在 `tests/unit/test_skill_discovery.py` 的 `TestPatternAnalyzer` 类之前添加：

```python
class TestDataModels:
    def test_discovered_pattern_has_condition_logic(self):
        pattern = DiscoveredPattern(
            id="test",
            name="test",
            description="test",
            trigger_type="combined",
            condition_logic="and",
        )
        assert pattern.condition_logic == "and"

    def test_discovered_pattern_default_logic_is_and(self):
        pattern = DiscoveredPattern(
            id="test",
            name="test",
            description="test",
            trigger_type="combined",
        )
        assert pattern.condition_logic == "and"

    def test_email_record_body_preview_default_empty(self):
        record = EmailRecord(
            id="test", subject="test", sender="a@b.com",
            to=[], cc=[], received_at="2024-01-01", message_type="received",
        )
        assert record.body_preview == ""
```

**Step 2: 运行测试验证失败**

Run: `cd /Users/jarod/Documents/AI-Exchange && .venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestDataModels -v`
Expected: FAIL — `DiscoveredPattern` 没有 `condition_logic` 字段

**Step 3: 实现 — 扩展数据模型**

修改 `src/skills_discovery/analyzer.py`：

`DiscoveredPattern` 新增字段：
```python
condition_logic: str = "and"  # 顶层条件逻辑: "and" | "or"
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestDataModels -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): 扩展 DiscoveredPattern 支持 condition_logic 字段"
```

---

## Task 2: 收件人/抄送分析 — 统计收集

**Files:**
- Modify: `src/skills_discovery/analyzer.py` — `compute_statistics()` 方法
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试 — compute_statistics 返回收件人维度**

```python
class TestRecipientAnalysis:
    def test_statistics_include_recipient_data(self):
        records = _make_records_with_recipients()
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()

        assert "mailing_lists" in stats
        assert "to_vs_cc_reply_rate" in stats
        assert "frequent_recipient_combos" in stats

    def test_mailing_list_detection(self):
        """邮件组地址（all-@, -team@, -group@）应被识别。"""
        records = [
            EmailRecord(
                id=f"r_{i}", subject=f"通知 #{i}",
                sender=f"sender{i}@corp.com",
                to=["all-staff@corp.com"], cc=[],
                received_at="2024-01-01", message_type="received",
            )
            for i in range(5)
        ] + [
            EmailRecord(
                id=f"s_{i}", subject=f"Re: 通知 #{i}",
                sender="me@corp.com",
                to=[f"sender{i}@corp.com"], cc=[],
                received_at="2024-01-01", message_type="sent",
            )
            for i in range(1)  # 只回复了1封，回复率低
        ]
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()

        assert len(stats["mailing_lists"]) > 0
        # all-staff@corp.com 应在列表中
        ml_addrs = [ml["address"] for ml in stats["mailing_lists"]]
        assert "all-staff@corp.com" in ml_addrs

    def test_to_vs_cc_reply_rate(self):
        """我在 TO 里的邮件 vs 我在 CC 里的邮件，回复率应不同。"""
        from src.skills_discovery.analyzer import PatternAnalyzer, EmailRecord
        records = [
            # 我在 TO 里 — 5封
            *[EmailRecord(
                id=f"to_{i}", subject=f"直接给你 #{i}",
                sender="boss@corp.com",
                to=["me@corp.com"], cc=[],
                received_at="2024-01-01", message_type="received",
            ) for i in range(5)],
            # 我在 CC 里 — 5封
            *[EmailRecord(
                id=f"cc_{i}", subject=f"抄送通知 #{i}",
                sender="colleague@corp.com",
                to=["other@corp.com"], cc=["me@corp.com"],
                received_at="2024-01-01", message_type="received",
            ) for i in range(5)],
            # 对"直接给你"的回复 — 3封
            *[EmailRecord(
                id=f"reply_{i}", subject=f"Re: 直接给你 #{i}",
                sender="me@corp.com",
                to=["boss@corp.com"], cc=[],
                received_at="2024-01-01", message_type="sent",
            ) for i in range(3)],
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()

        assert stats["to_vs_cc_reply_rate"]["to_reply_rate"] > stats["to_vs_cc_reply_rate"]["cc_reply_rate"]
```

辅助函数 `_make_records_with_recipients()`：

```python
def _make_records_with_recipients() -> list[EmailRecord]:
    """带有收件人/抄送的测试数据。"""
    records = []
    mailing_lists = ["all-staff@corp.com", "dev-team@corp.com"]
    for i in range(15):
        to_list = [mailing_lists[i % 2]] if i < 10 else ["me@corp.com"]
        cc_list = ["me@corp.com"] if i < 10 else []
        records.append(EmailRecord(
            id=f"recv_{i:04d}",
            subject=f"项目通知 #{i}",
            sender=f"user{i % 3}@corp.com",
            to=to_list, cc=cc_list,
            received_at=f"2024-01-{(i % 28) + 1:02d}T10:00:00",
            message_type="received",
        ))
    # 一些回复
    for i in range(3):
        records.append(EmailRecord(
            id=f"sent_{i:04d}",
            subject=f"Re: 项目通知 #{i}",
            sender="me@corp.com",
            to=[f"user{i}@corp.com"], cc=[],
            received_at=f"2024-01-{(i % 28) + 1:02d}T14:00:00",
            message_type="sent",
        ))
    return records
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestRecipientAnalysis -v`
Expected: FAIL — `compute_statistics()` 没有这些键；`PatternAnalyzer` 不接受 `my_email`

**Step 3: 实现 — PatternAnalyzer 增加 my_email 参数和收件人统计**

修改 `src/skills_discovery/analyzer.py` 的 `PatternAnalyzer.__init__`：

```python
def __init__(self, records: list[EmailRecord], my_email: str = ""):
    self.records = records
    self.my_email = my_email.lower().strip()
    self.received = [r for r in records if r.message_type != "sent"]
    self.sent = [r for r in records if r.message_type == "sent"]
    self._reply_map: dict[str, bool] = {}
    self._build_reply_map()
    # 如果 my_email 未提供，尝试从 sent 邮件推断
    if not self.my_email and self.sent:
        sender_match = re.search(r'[\w.-]+@[\w.-]+', self.sent[0].sender)
        if sender_match:
            self.my_email = sender_match.group().lower()
```

在 `compute_statistics()` 末尾新增收件人分析：

```python
# --- 收件人/抄送分析 ---
mailing_lists = self._analyze_mailing_lists()
to_vs_cc = self._analyze_to_vs_cc()
recipient_combos = self._analyze_recipient_combos()

return {
    # ... 现有字段 ...
    "mailing_lists": mailing_lists,
    "to_vs_cc_reply_rate": to_vs_cc,
    "frequent_recipient_combos": recipient_combos,
}
```

新增三个私有方法：

```python
def _analyze_mailing_lists(self) -> list[dict]:
    """识别邮件组地址及其回复率。"""
    list_patterns = re.compile(r'(all[-_]|[-_]team@|[-_]group@|[-_]list@|[-_]dept@)', re.IGNORECASE)
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
        in_to = any(self.my_email in addr.lower() for addr in r.to) if self.my_email else False
        in_cc = any(self.my_email in addr.lower() for addr in r.cc) if self.my_email else False
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
        "to_reply_rate": to_replied / to_count if to_count > 0 else 0,
        "cc_count": cc_count,
        "cc_reply_rate": cc_replied / cc_count if cc_count > 0 else 0,
    }

def _analyze_recipient_combos(self) -> list[dict]:
    """发现频繁共现的收件人组合。"""
    combo_counts: Counter = Counter()
    combo_replied: Counter = Counter()

    for r in self.received:
        all_recipients = set()
        for addr in r.to + r.cc:
            email_match = re.search(r'[\w.-]+@[\w.-]+', addr.lower())
            if email_match:
                clean = email_match.group()
                if clean != self.my_email:
                    all_recipients.add(clean)
        if len(all_recipients) >= 2:
            combo = tuple(sorted(all_recipients)[:4])  # 最多取4人避免过长
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
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestRecipientAnalysis -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): 新增收件人/抄送维度统计分析"
```

---

## Task 3: 线程深度 + 参与度分析

**Files:**
- Modify: `src/skills_discovery/analyzer.py` — 新增 `_analyze_threads()` 方法
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试**

```python
class TestThreadAnalysis:
    def test_statistics_include_thread_data(self):
        records = _make_records_with_threads()
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()

        assert "thread_stats" in stats
        assert len(stats["thread_stats"]) > 0

    def test_thread_depth_and_participation(self):
        """线程深度和参与度应被正确计算。"""
        records = [
            # 线程1：3轮，我回复了2次（参与度 2/3）
            EmailRecord(id="t1_1", subject="讨论A", sender="a@corp.com",
                        to=["me@corp.com"], cc=[], received_at="2024-01-01",
                        message_type="received", thread_id="thread_001"),
            EmailRecord(id="t1_2", subject="Re: 讨论A", sender="me@corp.com",
                        to=["a@corp.com"], cc=[], received_at="2024-01-01",
                        message_type="sent", thread_id="thread_001"),
            EmailRecord(id="t1_3", subject="Re: 讨论A", sender="a@corp.com",
                        to=["me@corp.com"], cc=[], received_at="2024-01-02",
                        message_type="received", thread_id="thread_001"),
            EmailRecord(id="t1_4", subject="Re: 讨论A", sender="me@corp.com",
                        to=["a@corp.com"], cc=[], received_at="2024-01-02",
                        message_type="sent", thread_id="thread_001"),
            # 线程2：只有1封，没有回复
            EmailRecord(id="t2_1", subject="通知B", sender="b@corp.com",
                        to=["me@corp.com"], cc=[], received_at="2024-01-01",
                        message_type="received", thread_id="thread_002"),
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()

        threads = stats["thread_stats"]
        # thread_001 应该深度=4，我的参与=2
        t1 = next((t for t in threads if t["thread_id"] == "thread_001"), None)
        assert t1 is not None
        assert t1["depth"] == 4
        assert t1["my_replies"] == 2
        assert t1["participation"] == pytest.approx(0.5)
```

辅助函数：

```python
def _make_records_with_threads() -> list[EmailRecord]:
    """带有线程的测试数据。"""
    records = []
    # 线程1: 深度4
    for i in range(4):
        is_sent = i % 2 == 1
        records.append(EmailRecord(
            id=f"thread1_{i}",
            subject="Re: 重要讨论" if i > 0 else "重要讨论",
            sender="me@corp.com" if is_sent else "partner@corp.com",
            to=["partner@corp.com"] if is_sent else ["me@corp.com"],
            cc=[], received_at=f"2024-01-0{i+1}T10:00:00",
            message_type="sent" if is_sent else "received",
            thread_id="thread_A",
        ))
    # 线程2: 深度2，我没参与
    for i in range(2):
        records.append(EmailRecord(
            id=f"thread2_{i}",
            subject="FYI" if i == 0 else "Re: FYI",
            sender="other@corp.com",
            to=["me@corp.com"], cc=[],
            received_at=f"2024-01-0{i+1}T10:00:00",
            message_type="received",
            thread_id="thread_B",
        ))
    return records
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestThreadAnalysis -v`
Expected: FAIL — `compute_statistics()` 没有 `thread_stats` 键

**Step 3: 实现 — 新增 `_analyze_threads()` 方法**

```python
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
        my_replies = sum(
            1 for e in emails
            if e.message_type == "sent"
        )
        participation = my_replies / depth if depth > 0 else 0

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
```

在 `compute_statistics()` 返回值中增加：

```python
"thread_stats": self._analyze_threads(),
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestThreadAnalysis -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): 新增线程深度和参与度分析"
```

---

## Task 4: 增强 LLM Prompt — 传入多维度数据

**Files:**
- Modify: `src/skills_discovery/analyzer.py` — `build_llm_prompt()` 方法
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试**

```python
class TestEnhancedLLMPrompt:
    def test_prompt_includes_recipient_data(self):
        records = _make_records_with_recipients()
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "收件人" in prompt or "邮件组" in prompt
        assert "TO" in prompt or "CC" in prompt

    def test_prompt_includes_thread_data(self):
        records = _make_records_with_threads()
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "线程" in prompt or "thread" in prompt.lower()

    def test_prompt_includes_body_samples(self):
        records = [
            EmailRecord(
                id=f"r_{i}", subject=f"审批请求 #{i}",
                sender="finance@corp.com",
                to=["me@corp.com"], cc=[],
                received_at="2024-01-01", message_type="received",
                body_preview="请审核附件中的合同并签字确认。" * 5,
            )
            for i in range(5)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "正文样本" in prompt or "邮件内容" in prompt

    def test_prompt_requests_condition_logic(self):
        """prompt 应要求 LLM 输出 condition_logic 字段。"""
        records = _make_records(n_received=10, n_sent=3)
        analyzer = PatternAnalyzer(records)
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)

        assert "condition_logic" in prompt or "AND" in prompt
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestEnhancedLLMPrompt -v`
Expected: FAIL — 当前 prompt 不包含这些内容

**Step 3: 实现 — 重写 `build_llm_prompt()`**

完全重写 `build_llm_prompt()` 方法，增加以下数据段：

1. **邮件组统计** — 从 `stats["mailing_lists"]` 构建
2. **TO vs CC 回复率差异** — 从 `stats["to_vs_cc_reply_rate"]` 构建
3. **高频收件人组合** — 从 `stats["frequent_recipient_combos"]` 构建
4. **正文语义样本** — 从 received 邮件中按发件人分组，每组取 3 封的 body_preview
5. **线程深度统计** — 从 `stats["thread_stats"]` 中取深度 ≥ 2 的线程
6. **条件逻辑要求** — 要求 LLM 输出 `condition_logic` 字段（`"and"` 或 `"or"`）

在样本邮件中增加 body_preview 前 200 字符。

LLM 输出格式要求新增：
- `condition_logic`: 条件组合方式 (`"and"` 或 `"or"`)
- `conditions` 支持类型增加 `cc_match`, `recipient_role`, `thread_depth`

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestEnhancedLLMPrompt -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): LLM prompt 增加收件人/正文/线程多维度数据"
```

---

## Task 5: 增强启发式路径 — 多维度发现

**Files:**
- Modify: `src/skills_discovery/analyzer.py` — `_discover_heuristic()` 方法
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试**

```python
class TestEnhancedHeuristic:
    def test_heuristic_discovers_mailing_list_patterns(self):
        """应能发现邮件组模式。"""
        records = [
            *[EmailRecord(
                id=f"ml_{i}", subject=f"全员通知 #{i}",
                sender=f"hr{i % 2}@corp.com",
                to=["all-staff@corp.com"], cc=["me@corp.com"],
                received_at="2024-01-01", message_type="received",
            ) for i in range(10)],
            # 只回复了1封
            EmailRecord(
                id="ml_reply", subject="Re: 全员通知 #0",
                sender="me@corp.com",
                to=["hr0@corp.com"], cc=[],
                received_at="2024-01-01", message_type="sent",
            ),
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        patterns = analyzer._discover_heuristic()

        # 应有 to_match 类型的 pattern
        to_patterns = [p for p in patterns if any(
            c.get("type") in ("to_match", "cc_match") for c in p.conditions
        )]
        assert len(to_patterns) > 0

    def test_heuristic_discovers_cc_only_pattern(self):
        """我只在 CC 里的邮件回复率低，应发现为不需要回复。"""
        records = [
            *[EmailRecord(
                id=f"cc_{i}", subject=f"FYI #{i}",
                sender="team@corp.com",
                to=["boss@corp.com"], cc=["me@corp.com"],
                received_at="2024-01-01", message_type="received",
            ) for i in range(8)],
        ]
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        patterns = analyzer._discover_heuristic()

        cc_patterns = [p for p in patterns if p.trigger_type == "recipient_role"]
        # 应发现 CC 模式且不需要回复
        if cc_patterns:
            assert not cc_patterns[0].suggested_need_reply

    def test_heuristic_discovers_thread_patterns(self):
        """高深度+高参与度的线程应生成模式。"""
        records = []
        # 创建一个深度6的线程，我回复了3次
        for i in range(6):
            is_sent = i % 2 == 1
            records.append(EmailRecord(
                id=f"deep_{i}", subject="Re: 紧急讨论",
                sender="me@corp.com" if is_sent else "lead@corp.com",
                to=["lead@corp.com"] if is_sent else ["me@corp.com"],
                cc=[], received_at=f"2024-01-0{i+1}T10:00:00",
                message_type="sent" if is_sent else "received",
                thread_id="deep_thread",
            ))
        analyzer = PatternAnalyzer(records, my_email="me@corp.com")
        patterns = analyzer._discover_heuristic()

        thread_patterns = [p for p in patterns if p.trigger_type == "thread_depth"]
        assert len(thread_patterns) > 0
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestEnhancedHeuristic -v`
Expected: FAIL — 启发式只生成 sender_match 模式

**Step 3: 实现 — 扩展 `_discover_heuristic()`**

在现有发件人分析之后，新增三段分析逻辑：

```python
def _discover_heuristic(self) -> list[DiscoveredPattern]:
    patterns = []
    idx = 0

    # --- 1. 发件人模式（现有逻辑，保持不变）---
    # ... 现有代码 ...

    # --- 2. 邮件组/收件人模式 ---
    mailing_lists = self._analyze_mailing_lists()
    for ml in mailing_lists:
        if ml["count"] < 3:
            continue
        idx += 1
        rate = ml["reply_rate"]
        need_reply = rate >= 0.3
        priority = "P2" if need_reply else "P3"
        patterns.append(DiscoveredPattern(
            id=f"discovered_{idx:03d}",
            name=f"{ml['address'].split('@')[0]} 邮件组",
            description=f"发送到 {ml['address']} 的邮件 ({ml['count']} 封, 回复率 {rate:.0%})",
            trigger_type="to_match",
            conditions=[{
                "type": "to_match",
                "operator": "contains",
                "value": ml["address"],
            }],
            reply_rate=rate,
            sample_count=ml["count"],
            suggested_priority=priority,
            suggested_need_reply=need_reply,
            confidence=min(1.0, ml["count"] / 10),
        ))

    # --- 3. CC 角色模式 ---
    to_vs_cc = self._analyze_to_vs_cc()
    if (to_vs_cc["cc_count"] >= 5
            and to_vs_cc["cc_reply_rate"] < 0.2
            and to_vs_cc["to_reply_rate"] - to_vs_cc["cc_reply_rate"] > 0.3):
        idx += 1
        patterns.append(DiscoveredPattern(
            id=f"discovered_{idx:03d}",
            name="CC 抄送通知",
            description=(
                f"我仅在 CC 中的邮件 ({to_vs_cc['cc_count']} 封, "
                f"回复率 {to_vs_cc['cc_reply_rate']:.0%}) 通常不需要回复"
            ),
            trigger_type="recipient_role",
            conditions=[{
                "type": "cc_match",
                "operator": "contains",
                "value": self.my_email or "$ME",
            }],
            reply_rate=to_vs_cc["cc_reply_rate"],
            sample_count=to_vs_cc["cc_count"],
            suggested_priority="P3",
            suggested_need_reply=False,
            confidence=min(1.0, to_vs_cc["cc_count"] / 10),
        ))

    # --- 4. 线程深度模式 ---
    thread_stats = self._analyze_threads()
    high_depth_threads = [t for t in thread_stats if t["depth"] >= 3 and t["participation"] >= 0.3]
    if len(high_depth_threads) >= 2:
        avg_depth = sum(t["depth"] for t in high_depth_threads) / len(high_depth_threads)
        avg_participation = sum(t["participation"] for t in high_depth_threads) / len(high_depth_threads)
        idx += 1
        patterns.append(DiscoveredPattern(
            id=f"discovered_{idx:03d}",
            name="深度讨论线程",
            description=(
                f"检测到 {len(high_depth_threads)} 个高参与度讨论线程 "
                f"(平均深度 {avg_depth:.1f}, 平均参与度 {avg_participation:.0%})"
            ),
            trigger_type="thread_depth",
            conditions=[{
                "type": "thread_depth",
                "operator": "gte",
                "value": "3",
            }],
            reply_rate=avg_participation,
            sample_count=len(high_depth_threads),
            suggested_priority="P1",
            suggested_need_reply=True,
            confidence=min(1.0, len(high_depth_threads) / 5),
            example_subjects=[t["subject"] for t in high_depth_threads[:3]],
        ))

    return patterns
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestEnhancedHeuristic -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): 启发式路径支持收件人/CC角色/线程深度模式发现"
```

---

## Task 6: LLM 返回结果解析增强

**Files:**
- Modify: `src/skills_discovery/analyzer.py` — `_parse_llm_patterns()` 方法
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试**

```python
class TestParseLLMEnhanced:
    def test_parse_condition_logic(self):
        records = _make_records()
        analyzer = PatternAnalyzer(records)

        raw = [{
            "name": "组合模式",
            "description": "发件人+主题组合",
            "trigger_type": "combined",
            "condition_logic": "and",
            "conditions": [
                {"type": "sender_match", "operator": "contains", "value": "finance@"},
                {"type": "subject_match", "operator": "regex", "value": "发票|报销"},
            ],
            "reply_rate": 0.9,
            "sample_count": 20,
            "suggested_priority": "P1",
            "suggested_need_reply": True,
        }]
        patterns = analyzer._parse_llm_patterns(raw)
        assert patterns[0].condition_logic == "and"

    def test_parse_or_logic(self):
        records = _make_records()
        analyzer = PatternAnalyzer(records)

        raw = [{
            "name": "OR 模式",
            "description": "主题或正文匹配",
            "trigger_type": "combined",
            "condition_logic": "or",
            "conditions": [
                {"type": "subject_match", "operator": "contains", "value": "urgent"},
                {"type": "body_match", "operator": "contains", "value": "紧急"},
            ],
            "reply_rate": 0.7,
            "sample_count": 10,
            "suggested_priority": "P0",
            "suggested_need_reply": True,
        }]
        patterns = analyzer._parse_llm_patterns(raw)
        assert patterns[0].condition_logic == "or"

    def test_parse_default_condition_logic_is_and(self):
        records = _make_records()
        analyzer = PatternAnalyzer(records)

        raw = [{
            "name": "无 logic 字段",
            "description": "测试",
            "trigger_type": "sender_match",
            "conditions": [{"type": "sender_match", "operator": "in", "value": ["a@b.com"]}],
            "reply_rate": 0.5,
            "sample_count": 5,
            "suggested_priority": "P2",
            "suggested_need_reply": True,
        }]
        patterns = analyzer._parse_llm_patterns(raw)
        assert patterns[0].condition_logic == "and"
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestParseLLMEnhanced -v`
Expected: FAIL — `_parse_llm_patterns` 没有解析 `condition_logic`

**Step 3: 实现**

在 `_parse_llm_patterns()` 中增加：

```python
condition_logic=item.get("condition_logic", "and"),
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestParseLLMEnhanced -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): LLM 解析支持 condition_logic 字段"
```

---

## Task 7: Generator 支持 AND/OR 条件和新类型

**Files:**
- Modify: `src/skills_discovery/generator.py`
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试**

```python
class TestGeneratorEnhanced:
    def test_manifest_with_condition_logic(self):
        pattern = DiscoveredPattern(
            id="test",
            name="Combined Skill",
            description="OR 组合",
            trigger_type="combined",
            condition_logic="or",
            conditions=[
                {"type": "subject_match", "operator": "contains", "value": "审批"},
                {"type": "body_match", "operator": "contains", "value": "请审核"},
            ],
            confidence=0.9,
        )
        manifest = generate_manifest(pattern, "skill_auto_combined")
        assert manifest["triggers"]["condition_logic"] == "or"

    def test_manifest_default_logic_not_emitted_if_and(self):
        """默认 and 时可以不输出 condition_logic 字段。"""
        pattern = DiscoveredPattern(
            id="test", name="Simple", description="简单",
            trigger_type="sender_match",
            conditions=[{"type": "sender_match", "operator": "in", "value": ["a@b.com"]}],
            confidence=0.9,
        )
        manifest = generate_manifest(pattern, "skill_auto_simple")
        # and 是默认值，可以省略也可以显式输出，都行
        logic = manifest["triggers"].get("condition_logic", "and")
        assert logic == "and"

    def test_handler_for_cc_match_pattern(self):
        pattern = DiscoveredPattern(
            id="test", name="CC 通知",
            description="CC 中不需要回复",
            trigger_type="recipient_role",
            conditions=[{"type": "cc_match", "operator": "contains", "value": "$ME"}],
            suggested_priority="P3",
            suggested_need_reply=False,
            reply_rate=0.1,
        )
        code = generate_handler(pattern)
        assert "class Skill(BaseSkill):" in code
        assert '"P3"' in code
        assert "False" in code

    def test_handler_for_thread_depth_pattern(self):
        pattern = DiscoveredPattern(
            id="test", name="深度讨论",
            description="高深度线程",
            trigger_type="thread_depth",
            conditions=[{"type": "thread_depth", "operator": "gte", "value": "3"}],
            suggested_priority="P1",
            suggested_need_reply=True,
            reply_rate=0.8,
        )
        code = generate_handler(pattern)
        assert "class Skill(BaseSkill):" in code
        assert '"P1"' in code

    def test_write_skill_with_or_logic(self, tmp_path: Path):
        pattern = DiscoveredPattern(
            id="test", name="OR Skill",
            description="OR 条件组合",
            trigger_type="combined",
            condition_logic="or",
            conditions=[
                {"type": "sender_match", "operator": "contains", "value": "boss@"},
                {"type": "subject_match", "operator": "regex", "value": "紧急"},
            ],
            suggested_priority="P0",
            suggested_need_reply=True,
            reply_rate=0.95,
            sample_count=20,
            confidence=0.95,
        )
        path = write_skill(pattern, registry_path=str(tmp_path))
        manifest = yaml.safe_load((Path(path) / "manifest.yaml").read_text())
        assert manifest["triggers"]["condition_logic"] == "or"
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestGeneratorEnhanced -v`
Expected: FAIL — `generate_manifest` 不输出 `condition_logic`

**Step 3: 实现**

修改 `generator.py` 的 `generate_manifest()`：

```python
def generate_manifest(pattern: DiscoveredPattern, skill_id: str) -> dict:
    # ... 现有逻辑 ...

    triggers = {
        "priority": trigger_priority,
        "conditions": conditions,
    }

    # 仅在非默认值时输出 condition_logic
    if hasattr(pattern, "condition_logic") and pattern.condition_logic != "and":
        triggers["condition_logic"] = pattern.condition_logic

    return {
        "id": skill_id,
        "name": pattern.name,
        "description": pattern.description,
        "version": "1.0.0",
        "execution_mode": "modifier",
        "triggers": triggers,
    }
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestGeneratorEnhanced -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/generator.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): generator 支持 condition_logic 和新条件类型"
```

---

## Task 8: body_preview 扩展到 1000 字符 + 跳过图片

**Files:**
- Modify: `src/skills_discovery/analyzer.py:104-106` — Qdrant collector 的 body 截取
- Modify: `scripts/discover_skills.py:188` — `_parsed_to_record` 的 body_preview 截取
- Test: `tests/unit/test_skill_discovery.py`

**Step 1: 写失败测试**

```python
class TestBodyPreviewEnhancement:
    def test_body_preview_1000_chars(self):
        """body_preview 应截取到 1000 字符。"""
        long_body = "这是正文内容" * 200  # 远超1000字符
        record = EmailRecord(
            id="test", subject="test", sender="a@b.com",
            to=[], cc=[], received_at="2024-01-01",
            message_type="received",
            body_preview=long_body[:1000],
        )
        assert len(record.body_preview) == 1000

    def test_body_preview_strips_img_tags(self):
        """body_preview 中的 <img> 标签应被移除。"""
        body_with_img = '正文开始<img src="logo.png" alt="Logo"/>中间内容<img src="sig.png"/>结尾'
        from src.skills_discovery.analyzer import strip_images_from_body
        cleaned = strip_images_from_body(body_with_img)
        assert "<img" not in cleaned
        assert "正文开始" in cleaned
        assert "中间内容" in cleaned
        assert "结尾" in cleaned
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestBodyPreviewEnhancement -v`
Expected: FAIL — `strip_images_from_body` 不存在

**Step 3: 实现**

在 `src/skills_discovery/analyzer.py` 顶部新增辅助函数：

```python
def strip_images_from_body(body: str) -> str:
    """移除 HTML 中的 <img> 标签。"""
    return re.sub(r'<img[^>]*/?>', '', body, flags=re.IGNORECASE)
```

修改 `EmailHistoryCollector.collect()` 中 body 截取：

```python
body = p.get("body_preview", "") or p.get("body", "")
body = strip_images_from_body(body)
if len(body) > 1000:
    body = body[:1000]
```

修改 `scripts/discover_skills.py` 的 `_parsed_to_record()`：

```python
def _parsed_to_record(parsed) -> EmailRecord:
    from src.skills_discovery.analyzer import strip_images_from_body
    body = parsed.body[:1000] if parsed.body else ""
    body = strip_images_from_body(body)
    return EmailRecord(
        # ...
        body_preview=body,
        # ...
    )
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestBodyPreviewEnhancement -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py scripts/discover_skills.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): body_preview 扩展到 1000 字符并跳过图片标签"
```

---

## Task 9: Tier1 路由引擎支持 cc_match 和 condition_logic

**Files:**
- Modify: `src/router/tier1_reflex.py`
- Test: `tests/unit/test_skill_discovery.py` (或新建 `tests/unit/test_tier1_enhanced.py`)

**Step 1: 写失败测试**

```python
# tests/unit/test_tier1_enhanced.py

from unittest.mock import MagicMock, patch
from src.router.tier1_reflex import Tier1ReflexRouter


class TestTier1CcMatch:
    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_cc_match_contains(self, mock_mgr):
        """cc_match 条件应匹配 CC 列表。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_cc_test",
            "conditions": [{"type": "cc_match", "operator": "contains", "value": "me@corp.com"}],
        }]
        router = Tier1ReflexRouter()
        email = {
            "subject": "FYI", "sender": "other@corp.com", "body": "",
            "to": ["boss@corp.com"], "cc": ["me@corp.com", "team@corp.com"],
        }
        matches = router.route(email)
        assert "skill_cc_test" in matches

    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_cc_match_no_match(self, mock_mgr):
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_cc_test",
            "conditions": [{"type": "cc_match", "operator": "contains", "value": "me@corp.com"}],
        }]
        router = Tier1ReflexRouter()
        email = {
            "subject": "Direct", "sender": "other@corp.com", "body": "",
            "to": ["me@corp.com"], "cc": [],
        }
        matches = router.route(email)
        assert "skill_cc_test" not in matches


class TestTier1ConditionLogic:
    @patch("src.router.tier1_reflex.get_skill_manager")
    def test_or_logic(self, mock_mgr):
        """condition_logic=or 时，任一条件匹配即可。"""
        mock_mgr.return_value.get_tier1_triggers.return_value = [{
            "skill_id": "skill_or_test",
            "condition_logic": "or",
            "conditions": [
                {"type": "sender_match", "operator": "contains", "value": "boss@"},
                {"type": "subject_match", "operator": "contains", "value": "紧急"},
            ],
        }]
        router = Tier1ReflexRouter()

        # 只匹配 subject
        email = {"subject": "紧急通知", "sender": "nobody@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_or_test" in router.route(email)

        # 只匹配 sender
        email2 = {"subject": "普通", "sender": "boss@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_or_test" in router.route(email2)

        # 都不匹配
        email3 = {"subject": "普通", "sender": "nobody@corp.com", "body": "", "to": [], "cc": []}
        assert "skill_or_test" not in router.route(email3)
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_tier1_enhanced.py -v`
Expected: FAIL — `tier1_reflex.py` 不支持 `cc_match` 和 `condition_logic`

**Step 3: 实现**

修改 `src/router/tier1_reflex.py` 的 `route()` 方法：

```python
def route(self, email: Dict[str, Any]) -> List[str]:
    matched_skills = []
    triggers = self.manager.get_tier1_triggers()

    to_list = email.get("to") or []
    cc_list = email.get("cc") or []
    subject = email.get("subject") or ""
    body = email.get("body") or ""
    sender = email.get("sender") or ""
    if isinstance(to_list, str):
        to_list = [to_list]
    if isinstance(cc_list, str):
        cc_list = [cc_list]

    for trigger in triggers:
        skill_id = trigger["skill_id"]
        conditions = trigger["conditions"]
        logic = trigger.get("condition_logic", "and")

        if logic == "or":
            is_match = any(
                self._check_condition(cond, subject, body, sender, to_list, cc_list)
                for cond in conditions
            )
        else:  # and
            is_match = all(
                self._check_condition(cond, subject, body, sender, to_list, cc_list)
                for cond in conditions
            )

        if is_match:
            logger.info(f"Tier 1 Match found: {skill_id}")
            matched_skills.append(skill_id)

    return matched_skills
```

在 `_check_condition` 中新增 `cc_list` 参数和 `cc_match` 类型：

```python
def _check_condition(self, cond: Dict, subject: str, body: str, sender: str,
                     to_list: List[str], cc_list: List[str]) -> bool:
    # ... 现有代码 ...

    elif c_type == "cc_match":
        if operator == "contains":
            return any(value.lower() in t.lower() for t in cc_list)
        elif operator == "eq":
            return any(value.lower() == t.lower() for t in cc_list)
        elif operator == "in":
            check_values = value if isinstance(value, list) else [value]
            return any(t.lower() in [v.lower() for v in check_values] for t in cc_list)
        return False

    # ... 其余现有代码 ...
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_tier1_enhanced.py -v`
Expected: PASS

**Step 5: 提交**

```bash
git add src/router/tier1_reflex.py tests/unit/test_tier1_enhanced.py
git commit -m "feat(router): Tier1 支持 cc_match 条件类型和 OR 条件逻辑"
```

---

## Task 10: display 展示增强 + discover_skills.py my_email 参数

**Files:**
- Modify: `scripts/discover_skills.py`
- Test: 手动验证（CLI 输出）

**Step 1: 写失败测试**

```python
# 在 tests/unit/test_skill_discovery.py 中

class TestDiscoverScriptIntegration:
    def test_parsed_to_record_body_1000(self):
        """_parsed_to_record 应截取 1000 字符。"""
        from scripts.discover_skills import _parsed_to_record
        from scripts.import_pst import ParsedEmail

        parsed = ParsedEmail(
            id="test", subject="test", sender="a@b.com",
            to=[], cc=[], body="x" * 2000,
            received_at="2024-01-01",
        )
        record = _parsed_to_record(parsed)
        assert len(record.body_preview) <= 1000

    def test_parsed_to_record_strips_img(self):
        from scripts.discover_skills import _parsed_to_record
        from scripts.import_pst import ParsedEmail

        parsed = ParsedEmail(
            id="test", subject="test", sender="a@b.com",
            to=[], cc=[], body='正文<img src="x.png"/>结尾',
            received_at="2024-01-01",
        )
        record = _parsed_to_record(parsed)
        assert "<img" not in record.body_preview
```

**Step 2: 运行测试验证失败**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestDiscoverScriptIntegration -v`
Expected: FAIL（body_preview 仍为 500 字符且包含 img 标签）

**Step 3: 实现**

修改 `scripts/discover_skills.py`：

1. `_parsed_to_record()` 使用 `strip_images_from_body` 并截取 1000 字符
2. `display_pattern()` 增强显示新的条件类型标签：
   - `cc_match` → "抄送含"
   - `recipient_role` → "收件角色"
   - `thread_depth` → "线程深度"
3. `run_discovery()` 增加 `my_email` 参数，传递给 `PatternAnalyzer`
4. CLI 增加 `--my-email` 参数

```python
# discover_skills.py CLI 新增参数
parser.add_argument(
    "--my-email",
    help="你的邮箱地址 (用于识别 TO/CC 角色，默认从已发送邮件推断)",
)
```

**Step 4: 运行测试验证通过**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestDiscoverScriptIntegration -v`
Expected: PASS

**Step 5: 提交**

```bash
git add scripts/discover_skills.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): discover_skills.py 支持 my_email 参数和增强展示"
```

---

## Task 11: 全量测试 + 回归验证

**Files:**
- Test: 所有测试文件

**Step 1: 运行全量单测**

Run: `.venv/bin/python -m pytest tests/unit/test_skill_discovery.py tests/unit/test_tier1_enhanced.py -v`
Expected: ALL PASS

**Step 2: 运行项目全量测试确保无回归**

Run: `.venv/bin/python -m pytest -q`
Expected: ALL PASS（已有的 `test_heuristic_discovery` 中 `assert p.trigger_type == "sender_match"` 需要更新为允许多种类型）

**Step 3: 如有回归，修复并提交**

可能需要修复：
- `TestPatternAnalyzer.test_heuristic_discovery` 中 `assert p.trigger_type == "sender_match"` 需要放宽为 `assert p.trigger_type in ("sender_match", "to_match", "recipient_role", "thread_depth")`

**Step 4: 最终提交**

```bash
git add -A
git commit -m "test: 修复回归测试，全量验证通过"
```

---

## 任务依赖图

```
Task 1 (数据模型) ──┐
                     ├── Task 4 (LLM Prompt)
Task 2 (收件人统计) ─┤
                     ├── Task 5 (启发式) ── Task 6 (LLM 解析)
Task 3 (线程分析) ──┘
                                              │
Task 7 (Generator) ──────────────────────────┘
                                              │
Task 8 (body_preview) ───────────────────────┘
                                              │
Task 9 (Tier1 路由) ─────────────────────────┘
                                              │
Task 10 (CLI + display) ────────────────────┘
                                              │
Task 11 (全量测试) ──────────────────────────┘
```

**可并行**: Task 1, 2, 3 可以并行。Task 7, 8, 9 可以并行。
