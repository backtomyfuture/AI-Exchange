# 设计文档：减少飞书推送 —— 只推「需回复」+「值得阅读」的邮件

**日期**: 2026-06-29
**状态**: 已通过设计评审，待实现
**范围**: 收敛飞书只读卡片的推送条件，降低推送频率

---

## 1. 背景与问题

当前所有经处理的邮件几乎都会推送到飞书，使推送与「自己直接看邮箱」无异，失去了筛选价值。

根因在派发逻辑 [`_dispatch_notification`](../../../src/exchange_service.py)（约 158 行）：

```python
if classification.get("need_reply"):
    # 发审批卡片
    ...
if priority == "P1" or intent == "通知":
    # 发只读卡片  ← 问题所在
    ...
```

其中 `intent == "通知"` 是泄漏点：绝大多数信息类邮件都会被分类为「通知」，于是统统落入只读卡片推送（包括运维日志、广播、仅抄送的 FYI）。

---

## 2. 目标

非回复类邮件，只在「值得阅读」时推送只读卡片，其余静默归档：

- 需回复的邮件 → 不变，仍发审批卡片。
- 不需回复但重要的邮件（领导发来的、内容紧急的、直接发给我的）→ 推只读卡片。
- 其余（仅抄送的广播、营销、运维日志、例行 P2/P3 通知）→ 静默归档，不推送。

---

## 3. 新的派发规则

在 `_dispatch_notification` 中，当 `need_reply == False` 时，用以下规则替换现有的 `priority == "P1" or intent == "通知"`：

```
if intent == "垃圾邮件":        → 静默归档（硬性排除，最高优先级）
elif is_direct_to_me:          → 推送（我在 To 收件人里，不论优先级）
elif is_vip_sender:            → 推送（发件人在领导/VIP 名单）
elif priority in {"P0","P1"}:  → 推送（内容紧急/重要）
else:                          → 静默归档
```

判定顺序即上表先后；命中即停。

**净效果**：唯有同时满足「我不是直接收件人」+「非领导发件」+「非 P0/P1」的邮件才会被静默 —— 即仅抄送的广播、营销、运维交接日志、例行 P2/P3 通知。需回复路径完全不受影响。

### 决策表（验收口径）

| intent | is_direct_to_me | is_vip_sender | priority | 结果 |
|---|---|---|---|---|
| 垃圾邮件 | 任意 | 任意 | 任意 | 静默 |
| 非垃圾 | 是 | 任意 | 任意 | 推送 |
| 非垃圾 | 否 | 是 | 任意 | 推送 |
| 非垃圾 | 否 | 否 | P0/P1 | 推送 |
| 非垃圾 | 否 | 否 | P2/P3 | 静默 |

> 注：以上仅在 `need_reply == False` 分支生效。`need_reply == True` 始终发审批卡片，不进入本表。

---

## 4. 配套改动

### 4.1 `is_direct_to_me` 辅助函数（共享化）

现有逻辑在 `skills_registry/skill_vip_handling/handler.py::_is_direct_recipient`：用 `EXCHANGE_ACCOUNT_EMAIL` 比对 `email["to"]`（支持 str 或 list，大小写不敏感，子串匹配）。

- 抽取到共享工具（如 `src/utils/recipient.py` 或 `src/utils/email_helpers.py`），命名 `is_direct_recipient(email, me=None)`。
- VIP 技能改为调用共享实现，消除重复。
- 派发逻辑调用同一实现，保证语义一致。
- `me` 缺省从 `get_settings().EXCHANGE_ACCOUNT_EMAIL` 读取；为空时沿用现有「兜底视为直接收件人」行为。

### 4.2 `is_vip_sender` 辅助函数（领导名单单一数据源）

当前领导名单硬编码在 `skill_vip_handling/manifest.yaml` 的 `sender_match` 条件里（`lanjuan@tianjin-air.com`、`xt_zong@tianjin-air.com`）。

- 在 `src/config.py` 新增 `LEADER_SENDERS: List[str]`，默认值为上述两位，作为派发逻辑读取的单一数据源。
- 派发逻辑 `is_vip_sender(email)`：取 `email["sender"]`，小写后判断是否包含名单中任一地址。
- VIP 技能 manifest 保持现状（仍走 Tier1 反射触发，将这些发件人提升到 P0/P1）；本次不强行统一两处。已知重复在「未来工作」中记录，建议后续以 config 为准。

> 说明：因 VIP 技能本就把已知领导提升为 P0/P1，规则中的 `priority in {P0,P1}` 已能间接覆盖他们；新增显式 `is_vip_sender` 作为冗余兜底，直接落实用户「按领导发件人推送」的诉求，避免依赖技能是否触发。

### 4.3 分类器优先级评级标准

在 [categorizer.py](../../../src/nodes/categorizer.py)（约 85 行）的 system prompt 中补充优先级判定标准，使 `priority` 可信：

- **P0** = 领导发来 / 紧急，需立即处理
- **P1** = 重要，需关注
- **P2** = 一般事务
- **P3** = 通知 / 营销 / 无需关注

仅追加评级说明，不改变输出结构。

---

## 5. 测试

新增单元测试（如 `tests/unit/test_dispatch_filtering.py`），对第 3 节决策表逐行覆盖：

- 垃圾邮件 + 直接收件 → 静默（验证硬排除优先级）。
- 非垃圾 + 直接收件 + P3 → 推送。
- 非垃圾 + 仅抄送 + VIP 发件 → 推送。
- 非垃圾 + 仅抄送 + P1 → 推送。
- 非垃圾 + 仅抄送 + P2 → 静默。
- 需回复 → 仍发审批卡片（回归验证，确保未被改动）。

测试聚焦派发判定函数的「推送 / 静默」结论；卡片渲染与外发 IO 用 mock。

---

## 6. 不在本次范围内

- 不改审批卡片流程与渲染。
- 不改已有自动归档技能（营销 `skill_hnair_marketing`、安全平台 `skill_safety_platform_archive` 在更早环节已短路）。
- 不做 VIP/领导名单的管理界面，名单仍由配置驱动。
- 不统一 VIP 技能 manifest 与 config 的领导名单（记为未来工作）。

---

## 7. 影响文件

- `src/exchange_service.py` —— 改写 `_dispatch_notification` 只读分支判定。
- `src/config.py` —— 新增 `LEADER_SENDERS`。
- 新增 `src/utils/recipient.py`（或同义工具）—— `is_direct_recipient`、`is_vip_sender`。
- `skills_registry/skill_vip_handling/handler.py` —— 改用共享 `is_direct_recipient`。
- `src/nodes/categorizer.py` —— 优先级评级标准。
- `tests/unit/test_dispatch_filtering.py` —— 新增。
