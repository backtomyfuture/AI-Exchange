# Skill Discovery 基于角色的规则发现 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 重构 `_discover_heuristic()` 为五步骤角色驱动分析，让发现的规则真正反映"我在这封邮件里是什么角色"，同时识别转发/呈阅类邮件。

**Architecture:** 在 `analyzer.py` 中新增 `_detect_forward_fyi()` 和 `_analyze_group_received()` 两个辅助方法，重构 `_discover_heuristic()` 按优先级顺序：转发/呈阅 → 群组收件 → CC 抄送 → 直接收件（TO）→ 已有的邮件组正则/线程深度。同步增强 `build_llm_prompt()` 补充"我的角色分布"和"转发检测"数据段。

**Tech Stack:** Python 3.11+, dataclasses, regex, pytest

**Design Doc:** `docs/plans/2026-03-17-skill-discovery-role-based-design.md`

---

## Task 1: 新增 `_detect_forward_fyi()` 方法及测试

**Files:**
- Modify: `src/skills_discovery/analyzer.py`（在 `_analyze_threads` 之后添加方法）
- Test: `tests/unit/test_skill_discovery.py`（追加 `TestForwardFyiDetection` 类）

**Step 1: 写失败测试**

在 `tests/unit/test_skill_discovery.py` 末尾追加：

```python
class TestForwardFyiDetection:
    """_detect_forward_fyi 方法测试。"""

    def _make_record(self, subject="", body="", **kwargs):
        return EmailRecord(
            id=kwargs.get("id", "test"),
            subject=subject,
            sender=kwargs.get("sender", "a@b.com"),
            to=kwargs.get("to", []),
            cc=kwargs.get("cc", []),
            received_at="2024-01-01",
            message_type="received",
            body_preview=body,
        )

    def test_fw_prefix_detected(self):
        r = self._make_record(subject="FW: 关于项目进展")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is True

    def test_fw_lowercase_detected(self):
        r = self._make_record(subject="Fw: 关于项目进展")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is True

    def test_fwd_prefix_detected(self):
        r = self._make_record(subject="Fwd: 关于项目进展")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is True

    def test_chinese_forward_prefix_detected(self):
        r = self._make_record(subject="转发: 关于项目进展")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is True

    def test_chengyue_in_subject_detected(self):
        r = self._make_record(subject="【呈阅示】关于新需求")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is True

    def test_chengyue_in_body_detected(self):
        r = self._make_record(subject="关于AI项目进展", body="谨呈领导审阅，请知。")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is True

    def test_qingzhi_in_body_detected(self):
        r = self._make_record(subject="工作汇报", body="敬请知悉，如有疑问请联系。")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is True

    def test_normal_email_not_detected(self):
        r = self._make_record(subject="关于项目进展的询问", body="您好，请问项目什么时候完成？")
        analyzer = PatternAnalyzer([r])
        assert analyzer._detect_forward_fyi(r) is False
```

**Step 2: 运行测试验证失败**

```bash
cd /Users/jarod/Documents/AI-Exchange
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestForwardFyiDetection -v
```
Expected: FAIL — `PatternAnalyzer` 没有 `_detect_forward_fyi` 方法

**Step 3: 实现 `_detect_forward_fyi()`**

在 `src/skills_discovery/analyzer.py` 的 `_analyze_threads` 方法之后添加：

```python
# 转发/呈阅检测关键词
_FYI_SUBJECT_PATTERNS = re.compile(
    r'^(FW:|Fw:|Fwd:|转发[:：])|【呈阅|[(\[]呈阅|呈阅示',
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
```

**Step 4: 运行测试验证通过**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestForwardFyiDetection -v
```
Expected: 8 passed

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): 新增转发/呈阅检测方法 _detect_forward_fyi"
```

---

## Task 2: 新增 `_analyze_group_received()` 方法及测试

**Files:**
- Modify: `src/skills_discovery/analyzer.py`
- Test: `tests/unit/test_skill_discovery.py`（追加 `TestGroupReceivedAnalysis` 类）

**Step 1: 写失败测试**

在 `TestForwardFyiDetection` 之后追加：

```python
class TestGroupReceivedAnalysis:
    """_analyze_group_received 方法测试。"""

    def _make_received(self, sender, to, cc, replied=False, subject="test"):
        r = EmailRecord(
            id=f"{sender}-{subject}",
            subject=subject,
            sender=sender,
            to=to,
            cc=cc,
            received_at="2024-01-01",
            message_type="received",
        )
        return r

    def test_identifies_group_emails(self):
        """to/cc 均不含 my_email 的邮件应被识别为群组收件。"""
        records = [
            self._make_received("a@b.com", ["group@b.com"], [], subject=f"s{i}")
            for i in range(4)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        result = analyzer._analyze_group_received()
        assert len(result) >= 1
        assert result[0]["group_address"] == "group@b.com"
        assert result[0]["count"] == 4

    def test_direct_email_not_group(self):
        """to 中含 my_email 的邮件不应被归入群组。"""
        records = [
            self._make_received("a@b.com", ["me@b.com"], [], subject=f"s{i}")
            for i in range(4)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        result = analyzer._analyze_group_received()
        assert len(result) == 0

    def test_cc_email_not_group(self):
        """cc 中含 my_email（to 不含）的邮件不应被归入群组。"""
        records = [
            self._make_received("a@b.com", ["other@b.com"], ["me@b.com"], subject=f"s{i}")
            for i in range(4)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        result = analyzer._analyze_group_received()
        assert len(result) == 0

    def test_minimum_count_threshold(self):
        """少于 3 封的群组地址不应出现在结果中。"""
        records = [
            self._make_received("a@b.com", ["group@b.com"], [], subject=f"s{i}")
            for i in range(2)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        result = analyzer._analyze_group_received()
        assert len(result) == 0

    def test_empty_to_fallback_to_sender(self):
        """to 和 cc 均为空时，应按发件人分组作为兜底。"""
        records = [
            self._make_received("sys@b.com", [], [], subject=f"s{i}")
            for i in range(3)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        result = analyzer._analyze_group_received()
        assert len(result) == 1
        assert "sys@b.com" in result[0]["group_address"]
```

**Step 2: 运行测试验证失败**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestGroupReceivedAnalysis -v
```
Expected: FAIL

**Step 3: 实现 `_analyze_group_received()`**

在 `_detect_forward_fyi` 之后添加：

```python
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
```

**Step 4: 运行测试验证通过**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestGroupReceivedAnalysis -v
```
Expected: 5 passed

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): 新增群组收件分析方法 _analyze_group_received"
```

---

## Task 3: 重构 `_discover_heuristic()` 为五步骤角色驱动逻辑

**Files:**
- Modify: `src/skills_discovery/analyzer.py`（替换 `_discover_heuristic` 实现）
- Test: `tests/unit/test_skill_discovery.py`（追加 `TestRoleBasedHeuristic` 类）

**Step 1: 写失败测试**

在 `TestGroupReceivedAnalysis` 之后追加：

```python
class TestRoleBasedHeuristic:
    """重构后的 _discover_heuristic 角色驱动逻辑测试。"""

    def _make_r(self, sender, to, cc, replied_subj_set=None, subject="test", body=""):
        return EmailRecord(
            id=f"{sender}-{subject}",
            subject=subject, sender=sender,
            to=to, cc=cc,
            received_at="2024-01-01",
            message_type="received",
            body_preview=body,
        )

    def test_forward_pattern_generated(self):
        """含转发前缀的邮件应生成 priority=P3 need_reply=False 的规则。"""
        records = [
            self._make_r("a@b.com", ["me@b.com"], [], subject=f"FW: 事项{i}")
            for i in range(4)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        patterns = analyzer._discover_heuristic()
        fyi = [p for p in patterns if p.trigger_type == "combined"
               and any(c.get("type") == "subject_match" for c in p.conditions)]
        assert len(fyi) >= 1
        assert fyi[0].suggested_need_reply is False
        assert fyi[0].suggested_priority == "P3"

    def test_group_pattern_generated(self):
        """群组收件邮件应生成 to_match 类型的 P3 规则。"""
        records = [
            self._make_r("a@b.com", ["dept@b.com"], [], subject=f"通知{i}")
            for i in range(4)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        patterns = analyzer._discover_heuristic()
        group_p = [p for p in patterns if p.trigger_type == "to_match"]
        assert len(group_p) >= 1
        assert group_p[0].suggested_need_reply is False
        assert group_p[0].suggested_priority == "P3"

    def test_direct_to_high_reply_rate(self):
        """直接收件且高回复率应生成 P1 need_reply=True 的规则。"""
        sent = [
            EmailRecord(
                id=f"sent-{i}", subject=f"回复: 工作{i}", sender="me@b.com",
                to=["boss@b.com"], cc=[], received_at="2024-01-01", message_type="sent",
            )
            for i in range(7)
        ]
        received = [
            self._make_r("boss@b.com", ["me@b.com"], [], subject=f"工作{i}")
            for i in range(7)
        ]
        analyzer = PatternAnalyzer(received + sent, my_email="me@b.com")
        patterns = analyzer._discover_heuristic()
        direct_p = [p for p in patterns
                    if any(c.get("type") == "sender_match" for c in p.conditions)
                    and any(c.get("type") == "to_match" for c in p.conditions)]
        assert len(direct_p) >= 1
        assert direct_p[0].suggested_need_reply is True
        assert direct_p[0].suggested_priority in ("P1", "P2")

    def test_cc_pattern_generated_when_enough_data(self):
        """CC 邮件足够多且回复率低时，应生成 CC 已阅规则。"""
        records = [
            self._make_r("a@b.com", ["other@b.com"], ["me@b.com"], subject=f"抄送{i}")
            for i in range(6)
        ]
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        patterns = analyzer._discover_heuristic()
        cc_p = [p for p in patterns if p.trigger_type == "recipient_role"]
        assert len(cc_p) >= 1
        assert cc_p[0].suggested_need_reply is False
```

**Step 2: 运行测试验证失败**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestRoleBasedHeuristic -v
```
Expected: FAIL（现有逻辑不区分角色）

**Step 3: 重构 `_discover_heuristic()`**

将 `src/skills_discovery/analyzer.py` 中的 `_discover_heuristic` 方法完整替换为：

```python
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
    if cc_count >= 3 and cc_rate < 0.3 and (not self.my_email or (to_rate - cc_rate) > 0.1):
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

    sender_counts: Counter = Counter()
    sender_replied: Counter = Counter()
    sender_subjects: dict[str, list[str]] = defaultdict(list)

    for r in direct_records:
        email_match = re.search(r'[\w.-]+@[\w.-]+', r.sender)
        sender_key = email_match.group() if email_match else r.sender
        sender_counts[sender_key] += 1
        sender_subjects[sender_key].append(r.subject)
        if self._reply_map.get(r.id):
            sender_replied[sender_key] += 1

    for sender, count in sender_counts.most_common(10):
        if count < 3:
            continue
        rate = sender_replied[sender] / count if count > 0 else 0
        idx += 1

        if rate >= 0.6:
            priority, need_reply = "P1", True
        elif rate >= 0.3:
            priority, need_reply = "P2", True
        else:
            priority, need_reply = "P3", False

        # 如果有 my_email，生成"发件人+TO"组合条件，更精准
        if self.my_email:
            conditions = [
                {"type": "sender_match", "operator": "contains", "value": sender},
                {"type": "to_match", "operator": "contains", "value": self.my_email},
            ]
            trigger_type = "combined"
        else:
            conditions = [{"type": "sender_match", "operator": "contains", "value": sender}]
            trigger_type = "sender_match"

        patterns.append(DiscoveredPattern(
            id=f"discovered_{idx:03d}",
            name=f"{sender.split('@')[0]} 直接发给我",
            description=f"来自 {sender} 直接发给我的邮件 ({count} 封, 回复率 {rate:.0%})",
            trigger_type=trigger_type,
            condition_logic="and",
            conditions=conditions,
            reply_rate=rate,
            sample_count=count,
            suggested_priority=priority,
            suggested_need_reply=need_reply,
            example_subjects=sender_subjects[sender][:3],
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
            any(c.get("value") == ml["address"] for c in p.conditions)
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
```

**Step 4: 运行测试验证通过**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestRoleBasedHeuristic -v
```
Expected: 4 passed

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): 重构 _discover_heuristic 为五步骤角色驱动分析"
```

---

## Task 4: 增强 `build_llm_prompt()` 补充角色分布和转发检测数据

**Files:**
- Modify: `src/skills_discovery/analyzer.py`（`build_llm_prompt` 方法）
- Test: `tests/unit/test_skill_discovery.py`（追加 `TestLLMPromptRoleSection` 类）

**Step 1: 写失败测试**

在 `TestRoleBasedHeuristic` 之后追加：

```python
class TestLLMPromptRoleSection:
    """build_llm_prompt 中角色分布段落的测试。"""

    def _make_records(self):
        received_direct = [
            EmailRecord(
                id=f"r{i}", subject="工作事项", sender="boss@b.com",
                to=["me@b.com"], cc=[], received_at="2024-01-01", message_type="received",
            ) for i in range(5)
        ]
        received_cc = [
            EmailRecord(
                id=f"c{i}", subject="会议通知", sender="hr@b.com",
                to=["team@b.com"], cc=["me@b.com"], received_at="2024-01-01", message_type="received",
            ) for i in range(3)
        ]
        received_group = [
            EmailRecord(
                id=f"g{i}", subject="系统通知", sender="sys@b.com",
                to=["dept@b.com"], cc=[], received_at="2024-01-01", message_type="received",
            ) for i in range(4)
        ]
        fw = [
            EmailRecord(
                id=f"fw{i}", subject=f"FW: 转发事项{i}", sender="a@b.com",
                to=["me@b.com"], cc=[], received_at="2024-01-01", message_type="received",
            ) for i in range(3)
        ]
        return received_direct + received_cc + received_group + fw

    def test_prompt_includes_role_distribution(self):
        records = self._make_records()
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)
        assert "我的角色分布" in prompt
        assert "直接收件" in prompt
        assert "群组成员" in prompt

    def test_prompt_includes_forward_count(self):
        records = self._make_records()
        analyzer = PatternAnalyzer(records, my_email="me@b.com")
        stats = analyzer.compute_statistics()
        prompt = analyzer.build_llm_prompt(stats)
        assert "转发" in prompt or "呈阅" in prompt
```

**Step 2: 运行测试验证失败**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestLLMPromptRoleSection -v
```
Expected: FAIL

**Step 3: 增强 `build_llm_prompt()`**

在 `build_llm_prompt` 方法中，找到 `# --- 2. 收件人/抄送维度 ---` 之前，插入新段落：

```python
# --- 0. 我的角色分布（新增）---
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
```

然后在 `return f"""...` 的 prompt 字符串中，在 `{recipient_section}` 之前插入 `{role_section}`：

```python
return f"""你是一个邮件路由模式分析专家。...

{role_section}{recipient_section}{thread_section}## 邮件样本 ...
```

**Step 4: 运行测试验证通过**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py::TestLLMPromptRoleSection -v
```
Expected: 2 passed

**Step 5: 提交**

```bash
git add src/skills_discovery/analyzer.py tests/unit/test_skill_discovery.py
git commit -m "feat(discovery): LLM prompt 新增角色分布和转发检测数据段"
```

---

## Task 5: 全量测试回归验证

**Files:**
- Test: 所有测试文件

**Step 1: 运行完整 skill discovery 测试**

```bash
.venv/bin/python -m pytest tests/unit/test_skill_discovery.py tests/unit/test_tier1_enhanced.py -v
```
Expected: ALL PASS（包含新增的 TestForwardFyiDetection、TestGroupReceivedAnalysis、TestRoleBasedHeuristic、TestLLMPromptRoleSection）

**Step 2: 运行全量回归测试**

```bash
.venv/bin/python -m pytest -q
```
Expected: 仅原有的 `test_pst_import::test_iter_from_pst_uses_pypff_when_available` 失败（环境缺少 pypff，与本次修改无关），其余全部通过

**Step 3: 手动冒烟验证（可选，需要有 Qdrant 数据）**

```bash
.venv/bin/python -c "
import asyncio, sys
sys.path.insert(0, '.')
from dotenv import load_dotenv; load_dotenv()
from scripts.discover_skills import collect_from_qdrant
from src.skills_discovery.analyzer import PatternAnalyzer

records = collect_from_qdrant(limit=500)
analyzer = PatternAnalyzer(records, my_email='q-fu@tianjin-air.com')
patterns = analyzer._discover_heuristic()
for p in patterns:
    print(f'[{p.suggested_priority}] {p.name} (trigger={p.trigger_type}, need_reply={p.suggested_need_reply})')
"
```
Expected: 能看到"群组邮件"、"转发与呈阅邮件"等规则，而非全部是 sender_match

**Step 4: 提交（如有修复）**

```bash
git add -A
git commit -m "test: 全量回归验证通过"
```

---

## 任务依赖图

```
Task 1 (_detect_forward_fyi) ──┐
                                ├── Task 3 (_discover_heuristic 重构)
Task 2 (_analyze_group_received)┘
                                     │
Task 4 (build_llm_prompt 增强) ──────┘
                                     │
Task 5 (全量回归验证) ───────────────┘
```
