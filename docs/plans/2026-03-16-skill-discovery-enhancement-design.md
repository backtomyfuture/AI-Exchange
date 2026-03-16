# Skill Discovery 多维度增强设计

> 日期: 2026-03-16
> 状态: 已确认

## 1. 背景与目标

当前 `discover_skills.py` + `src/skills_discovery/analyzer.py` 仅基于**发件人**维度做模式发现，无法覆盖收件人/抄送、正文语义、线程深度等关键维度，导致遗漏大量可自动化的 skill。

**目标**: 将模式发现从单维度（发件人）扩展为多维度（收件人/抄送 > 正文语义 > 线程深度），并支持 AND/OR 组合条件。

## 2. 需求优先级

| 优先级 | 维度 | 说明 |
|:------|:-----|:----|
| P0 | 收件人/抄送分析 | 邮件组检测、TO vs CC 角色、收件人组合 |
| P1 | 正文语义分析 | LLM 语义归类（非关键词正则），body_preview 扩到 1000 字符，跳过图片 |
| P2 | 线程深度+参与度 | 线程轮数 + 用户回复次数，高参与度=重要讨论 |
| 不做 | 时间维度 | - |
| 不做 | 附件类型 | - |

## 3. 数据层变更

### 3.1 EmailRecord 扩展

- `body_preview`: 500 → 1000 字符
- 正文提取时跳过图片标签（`<img>` 等）

### 3.2 DiscoveredPattern 条件逻辑

conditions 支持 AND/OR 嵌套：

```python
{
    "logic": "and",  # 顶层逻辑关系
    "conditions": [
        {"type": "sender_match", "operator": "in", "value": [...]},
        {
            "logic": "or",  # 嵌套 OR
            "conditions": [
                {"type": "subject_match", "operator": "contains", "value": "审批"},
                {"type": "body_match", "operator": "contains", "value": "请审批"},
            ]
        }
    ]
}
```

### 3.3 新增 trigger_type 值

- `recipient_role` — 基于 TO/CC 角色
- `to_match` — 基于收件人/邮件组
- `body_match` — 基于正文语义
- `thread_depth` — 基于线程深度+参与度
- `combined` — 多维度组合

## 4. 分析维度设计

### 4.1 收件人/抄送分析 (P0)

**邮件组检测**:
- 统计 TO/CC 中出现的邮件组地址（如 `all-@`, `-team@`, `-group@`）
- 计算这些邮件的回复率
- 生成 `to_match` 条件

**TO vs CC 角色**:
- 区分"我在 TO 里"vs"我在 CC 里"的邮件
- 分别统计回复率差异
- 生成 `recipient_role` 条件（如 CC 中的邮件默认 P3 不回复）

**收件人组合**:
- 发现频繁共现的收件人组合
- 关联到特定业务流程（如某几个人同时出现 = 审批链）

### 4.2 正文语义分析 (P1)

- 将邮件按已有维度粗分组后，把每组 body_preview 样本交给 LLM 做语义归类
- LLM 输出语义标签（"催办"、"通知"、"审批请求"等）和对应的 `body_match` 条件
- 完全由 LLM 判断，不做关键词正则
- 正文提取跳过 `<img>` 标签内容

### 4.3 线程深度+参与度 (P2)

按 `thread_id` 聚合邮件，计算：
- 总轮数 (depth)
- 我的回复次数 (my_replies)
- 参与度 = my_replies / depth

模式发现逻辑：
- 高深度 (≥3) + 高参与度 (≥0.5) = 重要持续讨论 → 提升优先级
- 高深度 + 低参与度 = 可能被遗漏 → 提醒关注

## 5. LLM Prompt 增强

当前 prompt 仅传入发件人统计和主题关键词。增强数据输入：

1. 发件人统计（现有）
2. 收件人/抄送统计（新增）
   - 邮件组地址及其邮件量/回复率
   - TO vs CC 的回复率差异
   - 高频收件人组合
3. 正文语义样本（新增）
   - 每组 3-5 封邮件的 body_preview（1000 字符）
4. 线程深度统计（新增）
   - 高深度线程及用户参与度

LLM 输出要求：
- 发现 3-12 个模式
- 每个模式的 conditions 支持 AND/OR 逻辑
- 每个条件指定 `logic` 字段

## 6. 启发式路径增强

`_discover_heuristic()` 从纯发件人扩展为：
- 发件人频率 + 回复率（现有）
- CC vs TO 回复率差异 → 生成 `recipient_role` 模式
- 邮件组地址 → 生成 `to_match` 模式
- 线程深度阈值（≥3 轮 + 参与度 ≥0.5）→ 生成 `thread_depth` 模式

## 7. Skill 生成增强

### manifest.yaml

- `triggers.conditions` 支持嵌套 AND/OR 结构
- 新增条件类型：`to_match`, `cc_match`, `recipient_role`, `body_match`, `thread_depth`

### handler.py

- 生成的 handler 根据模式类型调整 classification 逻辑
- 线程深度模式：读取 state 中的 thread 信息做判断
- 收件人角色模式：读取 state 中的 TO/CC 信息做判断

## 8. 文件变更范围

| 文件 | 变更 |
|:-----|:-----|
| `src/skills_discovery/analyzer.py` | 核心：新增 3 个分析维度、扩展 LLM prompt、增强启发式、扩展数据模型 |
| `src/skills_discovery/generator.py` | 支持 AND/OR 条件生成 manifest 和 handler |
| `scripts/discover_skills.py` | body_preview 扩到 1000、display 支持新维度展示 |
| `scripts/import_pst.py` | body_preview 默认 1000（截取逻辑调整） |

## 9. 明确不做

- 不做时间维度分析
- 不做附件类型分析
- 不做正文图片处理（跳过图片）
- 不改 Qdrant 存储结构
- 不改现有 skill 的 handler 运行时逻辑
