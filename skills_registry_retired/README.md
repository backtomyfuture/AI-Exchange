# 已退休的 Tier 1 legacy 规则

`SkillManager` 只加载 `skills_registry/` 下的目录；本目录里的 Skill **不会被生产
加载**，只保留 Git 历史用于审计。

移出这里的规则不是"暂缓迁移"（`explicitly_deferred`，保留旧行为），而是本次 Tier 1
v1 迁移过程中经过分析、业务 owner 已确认接受行为变化的**主动退休**。完整理由见
`docs/tier1-migration-inventory.md` 的 RETIRE 组和 CONSOLIDATE（吸收）组。

| 规则 | 退休原因（摘要） |
|---|---|
| `skill_me_as_recipient` | 一刀切"发给我就强制审批"，无区分度，交给 Tier2/3 |
| `skill_auto_cc` | 一刀切"我被抄送就强制静默"，样本小(5)，交给 Tier2/3 |
| `skill_finance_invoice` | 只贴 intent 标签、不决定路由的半成品提示规则 |
| `skill_leadership_tone` | 只改写 prompt 语气，不是路由决策 |
| `skill_out_of_office` | 只改写 prompt 语气，不是路由决策 |
| `skill_project_tracker` | 只做 metadata 富化，不是路由决策 |
| `skill_auto_lanjuan` | 与 `skill_vip_handling` 重复且存在 last-write-wins 缺陷；已被 `tier1_rules/T1-VIP-*` 覆盖 |
| `skill_ops_log_silent` | 无发件人/收件人限定的纯内容规则，新 schema 强制要求身份锚点，结构上无法迁移 |
| `skill_auto_1446` | 同上，纯内容规则，无法迁移 |
| `skill_auto_hnasafety` | 条件是 `skill_safety_platform_archive`（仍在 `skills_registry/`）的严格子集，退休吸收 |
| `skill_auto_guo_lei1` | 群组邮件规则，历史回复率 25%（4 封），业务确认不需要固定"不需回复"，退休交给 Tier2/3 |
| `skill_auto_hyong_wang` | 群组邮件规则，历史回复率 20%（5 封），同上 |
| `skill_auto_jian_zhang` | 群组邮件规则，历史回复率 25%（4 封），同上 |
| `skill_auto_li_l3` | 群组邮件规则，历史回复率 25%（4 封），同上 |
| `skill_auto_liuliang1` | 群组邮件规则，历史回复率 20%（5 封），同上 |
| `skill_auto_titong_chen` | 群组邮件规则，历史回复率 50%（6 封，接近对半），同上 |

不要从这里恢复到 `skills_registry/`：重新启用需要新的证据、冲突检查和有效期，
不是简单 `git mv` 回去（参见 `docs/tier1-routing-design.md` §5）。
