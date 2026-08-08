# Tier 1 v1 迁移覆盖清单（31 条旧规则）

本文是 `docs/tier1-routing-design.md` §10 要求的迁移覆盖清单：31 条冻结候选
`skills_registry/` 规则逐一归类，且仅归入一类。分类法、必填字段定义见设计文档 §10。

切换前覆盖结论（design doc §10 的四选一 + 一个显式延后）：

- **已由新 Tier 1 覆盖**：CONSOLIDATE（11+1）、REWRITE（3）、SPLIT（2 条新规则）—— 见
  `tier1_rules/`，已通过 `compile_registry()` 编译和 fixture 回放验证。
- **已由现有 Tier 2 覆盖并经回放验证**：MOVE_TO_TIER2（6）—— 现有 Tier 2 投票机制
  （`TIER2_MIN_HITS=2`, `TIER2_MIN_RATIO=0.5`）已经存在，不需要新代码；
  `migration_status=already_supported`。业务 owner 复核后确认这 6 条也应直接退休
  （历史回复率 20%–50%、样本 4–6 封，不值得保留硬编码静默），已从
  `skills_registry/` 退休移除（审计记录见 Git 历史）。
- **明确退休且业务 owner 接受**：RETIRE（9）—— 已从 `skills_registry/` 退休移除
  （审计记录见 Git 历史）。
- 本次没有规则归入 `manual_review` 或 `explicitly_deferred`（不删除旧行为）类别。

新旧规则不存在双跑：`tier1_rules/` 尚未接入运行时（见下文"运行时集成"仍是独立的、
未决的后续阶段），本次迁移只对确认安全的旧规则做了实际下线（见 §RETIRE 组说明）。

## 迁移前发现的关键事实

这些事实在分类阶段验证，直接影响下面的归类结论，不是猜测：

1. **`card_type` 字段在现网从未被读取**——真正决定是否发卡的是
   `src/utils/notification_policy.decide_notification_kind()`，只看
   `need_reply`/`intent=="垃圾邮件"`/`is_direct_recipient`/`is_vip_sender`/`priority`。
2. **Tier 1 命中后 LLM categorizer 仍会跑**（`src/nodes/categorizer.py`，除非
   `action in [forward, transfer]`）：`merged_classification.update(current_classification)`
   —— 旧 Skill 显式设置过的字段覆盖 LLM，未设置的字段仍由 LLM 决定。因此几条旧规则
   （`skill_finance_invoice`/`skill_leadership_tone`/`skill_out_of_office`/
   `skill_project_tracker`）从未做出完整路由决策，只是给 LLM 递参考——这与新 Tier 1
   "命中即权威、不再问 LLM"的前提根本不兼容。
3. **真实的 last-write-wins 缺陷**：`skill_auto_lanjuan`（优先级 60）在
   `_apply_skills` 里比 `skill_vip_handling`（优先级 100）后执行并覆盖，导致
   `vip_handling` 更精细的"我是否为直接收件人"判断被清空。
4. **新 schema 的强制身份锚点**（`AnchorCondition.field` 只能是
   `sender.address`/`to.addresses`/`cc.addresses`，且不允许 `contains`/`regex`）使
   纯内容规则（无发件人/收件人限定）**结构上无法**表达为 Tier 1 v1 规则——
   `skill_ops_log_silent`、`skill_auto_1446` 属于此类，迁移分类阶段从计划中的
   REWRITE 改判为 RETIRE。
5. 部分"自动发现"群组规则历史回复率并非 0%（20%–50%，样本 4–6 封），不满足新模型
   `no_action` 的治理门槛（≥2 hard negative + owner + expires_at 只是形式要求，
   实质要求是"确定不需要回复"）——归入 MOVE_TO_TIER2 而非 CONSOLIDATE。

---

## RETIRE（9 条，已从 `skills_registry/` 退休移除）

| old_rule_id | business_purpose | final_decision 理由 |
|---|---|---|
| `skill_me_as_recipient` | "发给我(To)就强制 P1+需要回复+审批卡片" 的一刀切兜底规则 | 覆盖面等于几乎全部收件邮件，无区分度；且与几乎所有 read_only/no_action 规则存在潜在冲突。退休后交给 Tier2/3 按内容判断，业务 owner 已确认接受行为变化。 |
| `skill_auto_cc` | "我仅被抄送就强制 P3+静默" 的一刀切兜底规则（样本仅 5 封，回复率 0%） | 同上，方向相反；小样本不足以支撑永久静默判断，业务 owner 已确认接受。 |
| `skill_finance_invoice` | 发票/报销/合同关键词 → 只贴 `intent=审批` 标签，不决定 `need_reply`/`priority` | 半成品提示规则（见关键事实 #2），新模型没有"只设置部分字段、其余交给 LLM"的语义。业务 owner 已确认交给 LLM Tier3 判断财务类邮件。 |
| `skill_leadership_tone` | 检测汇报类邮件，修改 `system_prompt_modifier`（回复语气） | 根本不是路由决策，只改写 drafter 提示词；新 schema 的 `route` 模型放不下它。业务 owner 已确认直接退休，语气效果暂时消失，机制留待后续单独设计。 |
| `skill_out_of_office` | 检测休假关键词，修改 `system_prompt_modifier` | 同上，纯 prompt 修饰符，非路由决策。业务 owner 已确认直接退休。 |
| `skill_project_tracker` | 从主题提取 `P-XXXX` 项目号写入 `metadata`，不影响路由 | 纯 metadata 富化，非路由决策。业务 owner 已确认直接退休。 |
| `skill_auto_lanjuan` | "lanjuan 直接发给我" (4 封, 回复率 75%) → 强制 P1/需回复/审批 | 与 `skill_vip_handling` 重复且存在关键事实 #3 描述的 last-write-wins 缺陷：低优先级的它在 `vip_handling` 之后执行，清空了 `vip_handling` 对"是否为直接收件人"的更精细判断。已被 `T1-VIP-DIRECT-REPLY-001`/`T1-VIP-CC-ONLY-READONLY-001` 完整覆盖（见 SPLIT 组），退休是修复而非行为倒退。 |
| `skill_ops_log_silent` | 主题含"运维值班交接日志" → P3/只读，无发件人限定 (0 封回复样本记录) | 新 schema 强制要求身份锚点（关键事实 #4），纯主题正则无法表达为合法 Tier 1 v1 规则。退休，交给 Tier2/3 按内容判断；历史回复率为 0%，风险低。 |
| `skill_auto_1446` | 主题/正文含转发/呈阅关键词 (110 封, 回复率 5%) → P3/静默，无发件人限定 | 同上，结构上无法表达为合法 Tier 1 v1 规则（无身份锚点）。退休；110 封样本、5% 回复率下 LLM Tier3 独立判断的风险可接受。 |

**共同的 migration_status**: `explicitly_deferred` 不适用——这 9 条是**主动退休**
（业务 owner 已接受行为变化），不是"暂缓迁移、保留旧行为"。没有对应的新 Tier 1
规则，也不打算依赖 Tier 2（历史样本本身就弱或该规则本来就不是路由决策）。

---

## CONSOLIDATE → `T1-INTERNAL-DISTLIST-SILENCE-001`（11 条旧规则）

| old_rule_id | 收件地址 | 历史样本/回复率 |
|---|---|---|
| `skill_auto_gs4193` | gs4193@hnair.com | 3 封 / 0% |
| `skill_auto_hhlyrlzyb` | hhlyrlzyb@hnaaviation.com | 3 封 / 0% |
| `skill_auto_hhsc` | hhsc@hnair.com | 3 封 / 0% |
| `skill_auto_m_wu` | m.wu@tianjin-air.com | 8 封 / 0% |
| `skill_auto_shj_zhen` | shj-zhen@tianjin-air.com | 4 封 / 0% |
| `skill_auto_thitzb` | thitzb@hnair.com | 7 封 / 0% |
| `skill_auto_thxxcxb` | thxxcxb@hnair.com | 32 封 / 0% |
| `skill_auto_thxxyfzx` | thxxyfzx@hnair.com | 4 封 / 0% |
| `skill_auto_tjhkgh` | tjhkgh@tianjin-air.com | 5 封 / 0% |
| `skill_auto_yq_w` | yq.w@tianjin-air.com | 9 封 / 0% |
| `skill_auto_zhang_xia` | zhang-xia@tianjin-air.com | 44 封 / 0% |

- **business_purpose**：内部群组/分发列表地址，邮箱 owner 只是群组成员之一，历史 0%
  回复率。
- **owner**: `mailbox-owner`；**migration_status**: `pending_target_layer`
  （新规则已存在于 `tier1_rules/`，尚未接入运行时——见下文"运行时集成"）。
- **identity_anchor**: `to.addresses has_any [11 个地址]`（design doc §4.5 内联
  address group）。**content_conditions**: 无。
- **canonical_route**: `no_action`；**business_flow_id**:
  `internal-distribution-list-silence`；**authoritative_params**:
  `reason_code=internal_distribution_list`。
- **positive_samples / hard_negative_samples**: 见
  `tier1_rules/T1-INTERNAL-DISTLIST-SILENCE-001.yaml` 的
  `governance.positive_cases`/`negative_cases`（3 正例覆盖不同地址，2 反例含
  "地址只出现在 cc 不在 to"的边界情况）。
- **overlap_candidates**: 与 `T1-HNAIR-MARKETING-ARCHIVE-001`（共享
  `hhsc@hnair.com`，字段不同：一个是 sender 锚点一个是 to 锚点，编译期 warning，
  运行时按 §2.2 conflict 规则处理）；与 `T1-ZHANGXIA-FORWARD-READONLY-001`
  （共享 `zhang-xia@tianjin-air.com`，同理）。
- **failure_cost**: 低——错误静默的代价是漏看一封通常无需回复的群组邮件；
  `no_action` 的 governance 门槛（owner+expires_at+≥2 hard negative）已满足。
- **validity**: `effective_from=2025-01-01`, `expires_at=2026-06-01`（迁移时的
  placeholder 窗口，运行时集成前应重新评估）。
- **final_decision**: 11 条旧规则退休合并为 1 条新规则；旧 `skills_registry/` 目录
  **暂不物理下线**（见下文"为什么没有下线 CONSOLIDATE/REWRITE/SPLIT/MOVE_TO_TIER2
  源规则"）。

---

## CONSOLIDATE（吸收）→ `T1-SAFETY-PLATFORM-ARCHIVE-001`（1 条旧规则）

| old_rule_id | business_purpose |
|---|---|
| `skill_auto_hnasafety` | "hnasafety 直接发给我" (12 封, 回复率 0%)：`sender=hnasafety@hnaaviation.com AND to=q-fu@tianjin-air.com` |

- **final_decision**: 条件是 `skill_safety_platform_archive`（见 REWRITE 组）纯
  sender 锚点条件的严格子集，两者产生相同的 P3/静默结果，退休吸收进
  `T1-SAFETY-PLATFORM-ARCHIVE-001`，不单独迁移。
- **migration_status**: `pending_target_layer`。

---

## MOVE_TO_TIER2（6 条，已由现有 Tier 2 覆盖）

| old_rule_id | 收件地址 | 历史样本/回复率 |
|---|---|---|
| `skill_auto_guo_lei1` | guo-lei1@tianjin-air.com | 4 封 / 25% |
| `skill_auto_hyong_wang` | hyong-wang@tianjin-air.com | 5 封 / 20% |
| `skill_auto_jian_zhang` | jian.zhang@tianjin-air.com | 4 封 / 25% |
| `skill_auto_li_l3` | li_l3@tianjin-air.com | 4 封 / 25% |
| `skill_auto_liuliang1` | liuliang1@tianjin-air.com | 5 封 / 20% |
| `skill_auto_titong_chen` | titong-chen@tianjin-air.com | 6 封 / 50% |

- **business_purpose**：与 CONSOLIDATE 组同源（群组成员身份收件），但历史回复率
  20%–50%、样本 4–6 封，不满足"确定不需要回复"的 `no_action` 门槛。
- **owner**: `mailbox-owner`；**migration_status**: `already_supported`——现有
  Tier 2（`TIER2_MIN_HITS=2`, `TIER2_MIN_RATIO=0.5` 投票）已经能处理"有历史相关性
  但不绝对"的场景，不需要新代码，也不建 Tier 1 硬规则。
- **identity_anchor / content_conditions / canonical_route /
  business_flow_id / authoritative_params**: 不适用——不产生新 Tier 1 规则。
- **overlap_candidates**: 无（未进入 Tier 1 编译单元）。
- **failure_cost**: 中——错误强制静默的代价是漏看一封 20%-50% 概率需回复的邮件，
  这正是不敢做成硬性 `no_action` 的原因。
- **validity**: 不适用。
- **final_decision**: 旧 Tier 1 硬编码规则退休（不产生新 Tier 1 规则），落回 Tier 2
  历史投票 + Tier 3 LLM 兜底判断。业务 owner 复核后确认直接接受此风险，6 条旧
  `skills_registry/` 目录已退休移除（与 RETIRE 组相同，审计记录见 Git 历史）。

---

## REWRITE（3 条 → 3 条新规则）

| old_rule_id | 新规则 | canonical_route | business_flow_id | authoritative_params |
|---|---|---|---|---|
| `skill_safety_platform_archive` | `T1-SAFETY-PLATFORM-ARCHIVE-001` | `no_action` | `automated-system-notification-archive` | `reason_code=automated_system_notification` |
| `skill_hnair_marketing` | `T1-HNAIR-MARKETING-ARCHIVE-001` | `no_action` | `marketing-notification-archive` | `reason_code=marketing_spam` |
| `skill_zhangxia_forward` | `T1-ZHANGXIA-FORWARD-READONLY-001` | `read_only` | `forwarded-fyi-from-zhang-xia` | (无, `ReadOnlyParams` 无字段) |

- **owner**: `mailbox-owner`；**migration_status**: `pending_target_layer`。
- **identity_anchor**: 均为 `sender.address eq <固定地址>`。
- **content_conditions**: `skill_zhangxia_forward` 额外要求
  `subject regex 转发\|Fw:\|FW:`；其余两条无内容条件（sender 锚点即完整判定）。
- **positive_samples / hard_negative_samples**: 见对应 YAML 的
  `governance.positive_cases`/`negative_cases`。
- **overlap_candidates**: 见上文 CONSOLIDATE 组的 overlap_candidates（与
  `T1-INTERNAL-DISTLIST-SILENCE-001` 共享地址、不同字段的编译期 warning）。
- **failure_cost**: 低（安全平台/营销归档误判为静默的代价小；转发只读误判的代价
  是漏看一封知会邮件，reply 场景不受影响）。
- **validity**: `effective_from=2025-01-01`；无 `expires_at`（非 `no_action`，
  设计文档不强制）。
- **final_decision**: 1:1 改写，语义与旧规则一致（`card_type` 字段丢弃，因为现网
  从未读取，见关键事实 #1）。

---

## SPLIT → `T1-VIP-DIRECT-REPLY-001` + `T1-VIP-CC-ONLY-READONLY-001`（1 条旧规则）

| old_rule_id | business_purpose |
|---|---|
| `skill_vip_handling` | 检测 VIP 高管发件人，运行时按 `is_direct_recipient(email)` 分支：直接收件人→P0/需回复/审批；仅抄送→P1/只读 |

- **owner**: `mailbox-owner`；**migration_status**: `pending_target_layer`。
- **identity_anchor**（两条新规则共同）：
  `sender.address in [lanjuan@tianjin-air.com, xt_zong@tianjin-air.com]`。
- **content_conditions**: 用新增的 `$ME` 占位符表达原 handler.py 里的运行时分支
  （见 `src/router/tier1/dsl.py` 的 `ME_PLACEHOLDER`/`_resolve_me_value`，运行时从
  `EXCHANGE_ACCOUNT_EMAIL` 解析，未解析时整条规则返回 `UNKNOWN` → `manual_review`，
  不会静默选错分支）：
  - `T1-VIP-DIRECT-REPLY-001`: `to.addresses has_any ["$ME"]` →
    `canonical_route=reply`, `business_flow_id=vip-escalation`,
    `authoritative_params={reply_mode: sender_and_original_cc}`。
  - `T1-VIP-CC-ONLY-READONLY-001`: `not (to.addresses has_any ["$ME"])` →
    `canonical_route=read_only`, `business_flow_id=vip-fyi-only`。
- **positive_samples / hard_negative_samples**: 见对应 YAML；两条规则互斥性由
  `tests/unit/tier1/test_migrated_ruleset.py::test_vip_split_is_mutually_exclusive_never_a_runtime_conflict`
  验证（编译期只能给出"可能重叠"warning，运行时互斥性靠测试证明）。
- **overlap_candidates**: 两条新规则彼此共享锚点地址（预期内，见上）；已退休的
  `skill_auto_lanjuan` 是这两条规则共同覆盖并修复的旧规则（关键事实 #3）。
- **failure_cost**: 高（VIP 邮件误判为静默的代价高）——因此 `$ME` 未解析时的
  fail-closed（`manual_review`，不是默认某个分支）是刻意设计。
- **validity**: `effective_from=2025-01-01`；无 `expires_at`。
- **final_decision**: 拆分为两条规则；旧 `skill_auto_lanjuan` 退休（见 RETIRE 组）。

---

## 为什么没有下线 CONSOLIDATE/REWRITE/SPLIT/MOVE_TO_TIER2 的源规则

`tier1_rules/` 尚未接入运行时（`src/router/engine.py` 目前不读取它），这是独立的、
未决的后续阶段（31-规则迁移的原始指令范围内不包含运行时切换决策：迁移顺序、并行验证
策略、`EmailView` 投影接入点、artifact 存储/触发位置都还没有决定）。因此：

- **15 条**旧 `skills_registry/` 目录（11 条 CONSOLIDATE 源 + 3 条 REWRITE 源 +
  1 条 SPLIT 源）**保持原样、继续在旧引擎下运行**，行为不变——它们对应的新
  `tier1_rules/` 规则已编译验证但尚未接入运行时，提前移除旧目录会造成真实的覆盖
  缺口。
- 已退休下线的合计 **16 条**（退休目录已随清理物理删除，审计记录见 Git 历史）：
  - **9 条纯 RETIRE**（无替代规则、业务 owner 已确认接受 LLM 兜底）；
  - **2 条冗余重复**（`skill_auto_lanjuan`/`skill_auto_hnasafety`，退休后仍有未
    改动的姊妹规则 `skill_vip_handling`/`skill_safety_platform_archive` 继续覆盖
    同样的邮件，属于修复而非行为空白）；
  - **6 条 MOVE_TO_TIER2**（业务 owner 复核后确认直接接受"落回 Tier2/3 判断"的
    风险，不再保留旧的硬编码静默规则；这 6 条没有对应的新 Tier 1 规则，退休后不
    依赖运行时集成，可以立即下线，因此和另外 10 条一起提前处理）。
- 剩余 15 条源规则的实际下线（从 `skills_registry/` 移除）应该发生在运行时切换那
  一刻，不是提前发生。

## 下一步（未决，不在本次范围内）

- 运行时集成：`tier1_rules/` 接入 `src/router/engine.py`，决定 artifact 存储位置、
  编译触发时机（计划重启）、`EmailView` 投影的构造点、`me_email`/
  `internal_email_domains` 的 Settings 读取方式。
- 切换那一刻同步下线本文列出的 15 条仍在运行的旧规则源目录。
- `skill_leadership_tone`/`skill_out_of_office`（语气/请假状态修饰符）、
  `skill_project_tracker`（metadata 富化）需要的替代机制——本次明确退休，不是迁移。
