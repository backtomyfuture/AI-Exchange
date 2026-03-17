# Skill Discovery 完整设计文档

**日期**: 2026-03-17
**状态**: 待实施
**作者**: Claude Code + 用户协作

---

## 1. 背景与目标

`scripts/discover_skills.py` 通过分析 Qdrant 中的历史邮件，自动生成 `skills_registry/` 下的处理规则（Skill）。

**当前问题**：启发式路径（`_discover_heuristic`）只按发件人分组，生成的规则全部是 `sender_match` 类型，无法反映以下业务现实：
- 群组邮件（to/cc 中都没有我的个人地址）→ 通常不需要回复
- 我只是被抄送 → 回复率远低于直接收件
- 转发/呈阅类邮件 → 无论发件人是谁，都不需要回复
- 我直接收到且经常回复的发件人 → 高优先级

**目标**：重构启发式分析，生成真正有业务意义的多维度规则。

---

## 2. 数据现状

Qdrant 中存在两种来源的邮件数据（已在上一轮修复中统一字段读取）：

| 字段 | Exchange 源 | PST 导入源 | 当前读取方式 |
|------|------------|-----------|------------|
| 收件人 | `to_recipients` | `to` | 兼容两者 ✅ |
| 抄送 | `cc_recipients` | `cc` | 兼容两者 ✅ |
| 邮件类型 | `_parent_folder_name` | `type` | 兼容两者 ✅ |
| 发件人格式 | `Mailbox(name=..., email_address=...)` | `Name <email>` | `_normalize_mailbox` ✅ |
| 线程 ID | 无（空字符串） | `thread_id`（空字符串） | 兼容，但数据为空 |

**关键约束**：
- Exchange 源的 `to` 字段存储的是**部门/群组地址**（如 `thxxcxb@hnair.com`），而非个人邮箱
- 因此"to/cc 中都没有 `my_email`"是判断群组成员收件的可靠依据
- 线程 ID 目前全部为空，线程深度分析暂时无数据

---

## 3. 完整规则发现逻辑

### 3.1 总体流程

```
收件邮件
    │
    ├─ 步骤1: 转发/呈阅检测（最高优先级）
    │      ├─ 命中 → 归入"转发呈阅"桶
    │      └─ 未命中 → 继续
    │
    ├─ 步骤2: 我的角色判断
    │      ├─ to/cc 均无 my_email → 归入"群组收件"桶（按群组地址分组）
    │      ├─ cc 含 my_email，to 不含 → 归入"CC 抄送"桶
    │      └─ to 含 my_email → 归入"直接收件"桶（按发件人分组）
    │
    └─ 步骤3: 对每个桶计算统计 → 生成 DiscoveredPattern
```

### 3.2 步骤1：转发/呈阅检测

**判断为转发/呈阅的条件（OR 逻辑，任一满足）：**

| 检测位置 | 匹配内容 |
|---------|---------|
| 主题前缀 | `FW:` / `Fw:` / `Fwd:` / `转发:` / `转发：` |
| 主题包含 | `【呈阅` / `[呈阅` / `呈阅示` |
| 正文包含 | `呈阅` / `请知` / `请悉` / `谨呈` / `敬请知悉` / `请阅` / `知悉` |

**生成规则**（合并所有此类邮件为一条规则）：
```yaml
name: 转发与呈阅邮件
trigger_type: combined
condition_logic: or
conditions:
  - type: subject_match
    operator: regex
    value: "^(FW:|Fw:|Fwd:|转发[:：])"
  - type: body_match
    operator: contains
    value: "呈阅|请知|请悉|谨呈|敬请知悉|请阅|知悉"
suggested_priority: P3
suggested_need_reply: false
priority: 80  # 最先匹配
```

### 3.3 步骤2a：群组收件模式

**判断条件**：`to` 和 `cc` 字段均不包含 `my_email`

**分组方式**：按 `to` 字段中的地址（群组/部门地址）分组。若 `to` 也为空，按发件人分组（兜底）。

**样例**：
- `to: thxxcxb@hnair.com`，我不在 to/cc → 归入 `thxxcxb@hnair.com` 桶
- `to: thxxyfzx@hnair.com`，我不在 to/cc → 归入 `thxxyfzx@hnair.com` 桶

**生成规则**（每个出现 ≥ 3 次的群组地址各生成一条）：
```yaml
name: 天航信息技术部群组邮件
trigger_type: to_match
conditions:
  - type: to_match
    operator: contains
    value: thxxcxb@hnair.com
suggested_priority: P3
suggested_need_reply: false
priority: 60
```

### 3.4 步骤2b：CC 抄送模式

**判断条件**：`cc` 含 `my_email`，`to` 不含 `my_email`

**分组方式**：
- 若整体 CC 回复率 < 20%（且样本 ≥ 3），生成一条全局"CC 已阅"规则
- 若某发件人的 CC 邮件出现 ≥ 3 次，可按发件人进一步细分

**生成规则**：
```yaml
name: 抄送通知（我在 CC）
trigger_type: recipient_role
conditions:
  - type: cc_match
    operator: contains
    value: q-fu@tianjin-air.com   # my_email
suggested_priority: P3
suggested_need_reply: false
priority: 50
```

### 3.5 步骤2c：直接收件模式（TO）

**判断条件**：`to` 含 `my_email`

**分组方式**：按发件人分组，计算回复率（已有逻辑，保留）

**优先级映射**：
| 回复率 | priority | need_reply |
|--------|----------|-----------|
| ≥ 60% | P1 | true |
| 30–60% | P2 | true |
| < 30% | P3 | false |

**生成规则（示例）**：
```yaml
name: lanjuan 直接发给我的邮件
trigger_type: combined
condition_logic: and
conditions:
  - type: sender_match
    operator: contains
    value: lanjuan@tianjin-air.com
  - type: to_match
    operator: contains
    value: q-fu@tianjin-air.com
suggested_priority: P1
suggested_need_reply: true
priority: 40
```

### 3.6 步骤3（现有逻辑，保留）：邮件组正则检测

针对 `to`/`cc` 中符合特定前缀的系统地址（`all-@`、`hr@`、`noreply@` 等），生成"已知邮件组"规则。与步骤2a的区别：
- 步骤2a 基于**我的角色**（to/cc 无我）→ 适用于任意群组地址
- 步骤3 基于**地址特征**（前缀正则）→ 覆盖我能显式收到的邮件列表

两者互补，优先级均为 P3。

### 3.7 步骤4（现有逻辑，保留）：线程深度模式

当 `thread_id` 有效且线程深度 ≥ 3、参与度 ≥ 30% 时，生成高参与度讨论规则（P1）。

当前数据中 `thread_id` 全部为空，此规则暂不触发。

---

## 4. LLM 路径增强

在现有 prompt 中补充以下数据段（`build_llm_prompt` 增强）：

```
## 我的角色分布（基于 my_email: {my_email}）
- 我在 TO 中（直接收件）：{to_count} 封，回复率 {to_reply_rate}
- 我在 CC 中（仅抄送）：{cc_count} 封，回复率 {cc_reply_rate}
- 我不在 TO/CC（群组成员）：{group_count} 封，回复率 {group_reply_rate}

## 疑似转发/呈阅邮件
- 主题含转发标记：{fw_count} 封
- 正文含呈阅关键词：{body_forward_count} 封
```

要求 LLM 在分析时：
1. 优先识别转发/呈阅模式
2. 将群组邮件识别为低优先级
3. 对直接收件且高回复率的发件人生成精准规则

---

## 5. 规则冲突与优先级

| 规则类型 | manifest priority | 说明 |
|---------|-----------------|------|
| 转发/呈阅 | 80 | 最先匹配，覆盖所有其他规则 |
| 群组邮件（by to_match） | 60 | 次优先 |
| CC 抄送 | 50 | — |
| 直接收件（by sender+to） | 40 | 精准规则 |
| 邮件组正则（现有） | 30 | 兜底 |

---

## 6. 修改范围

| 文件 | 修改内容 |
|------|---------|
| `src/skills_discovery/analyzer.py` | `_discover_heuristic()` 重构为 5 步骤；新增 `_detect_forward_fyi()`；`_analyze_group_received()`；增强 `build_llm_prompt()` |
| `scripts/discover_skills.py` | `display_pattern()` 新增"群组收件"标签 |
| `tests/unit/test_skill_discovery.py` | 新增对应测试类 |

---

## 7. 不修改范围

- `tier1_reflex.py`：已支持 `to_match`、`cc_match`、`body_match`、`condition_logic` OR/AND，无需改动
- `generator.py`：已支持所有条件类型的 manifest 生成，无需改动
- `collect_from_qdrant()`：字段兼容已在上轮修复，无需改动
- `_build_reply_map()`：回复率计算已修复，无需改动
