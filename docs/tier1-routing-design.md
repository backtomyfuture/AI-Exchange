# Tier 1 与安全交接设计 v2.0

本文是 Tier 1（`tier1_rules/` 声明式确定性路由）的权威设计规范。它取代此前分散在
`route`/`priority`/`card_type`/`action` 等松散字段上的隐性约定。实现（JSON Schema、
registry compiler、evaluator、fixture runner）必须以本文为准；本文与代码冲突时，先
更新本文再改代码。

## 1. 范围边界

这一版定义从 Intake Guard 到安全发送的完整控制面：Tier 0 入口判定、Tier 1–3 规范
路由、路由持久化、版本化 handoff profile、写作证据、草稿审批 revision 和执行门禁。
以下仍不在范围内：

- 风格记忆学习与 LLM 个性化训练；
- 允许规则动态加载任意 Python handler、URL 或脚本；
- 基于目录的通用身份解析器（`identity_id`）和外部域可信等级；
- 飞书卡片的权限受控下钻查看器；
- 无人工确认的自动发送。

旧的 `skills_registry` 可执行 handler 与兼容运行时已删除；迁移结论由 Git 历史保存。

## 2. 决策模型

### 2.1 三个独立维度

```
EvaluationOutcome: matched | abstain | conflict | error
CanonicalRoute:    reply | forward | read_only | no_action | manual_review
ExecutionState:    pending | preparing | awaiting_approval | awaiting_review
                    | approved | completed | failed | cancelled
```

`CanonicalRoute` 曾用名 `skip` 现改为 `no_action`，避免被误读为"跳过 Tier 1、继续下一层"。
`abstain` 只存在于单层求值过程中，不能进入最终 `RouteDecision` 或持久化决策表。

语义边界：

- `abstain`：Tier 1 无规则命中，`route=null`，继续 Tier 2。
- `matched`：唯一权威动作已确定（`route` 非 null），Tier 2/3 不得重新分类。
- `conflict`：多条规则命中且产生不同的权威动作，`route=manual_review`。
- `error`：匹配或规则执行过程本身失败（非业务判断失败），`route=manual_review`。
- 下游执行失败（例如草稿生成失败）只改变 `ExecutionState=failed`，绝不触发 Tier 2/3
  重新分类，也不改变已持久化的 `tier1_decision`。

### 2.2 `RouteDecision`（不可变）与 handoff（可变）

Tier 1、Tier 2、Tier 3 最终只产生一个 `RouteDecision`。历史表名
`tier1_decisions` 保存的是这个跨层规范决策，不表示它只可能来自 Tier 1。决策必须在
profile、RAG、模型、LangGraph checkpoint 和用户可见副作用之前持久化，并在写入和恢复
读取时重新验证 inbox lease、generation/fencing token、execution/authority epoch、
capability hash、lease session/owner 及邮件 identity/version。

路由与后续交接拆成两个独立对象，职责不混用：

```yaml
route_decision:                 # 不可变；先持久化，再创建 handoff
  decision_id: opaque-id
  outcome: matched | conflict | error
  route: reply | forward | read_only | no_action | manual_review
  decision_origin: rule_declared | runtime_conflict | runtime_indeterminate | runtime_error

  matched_rules:                 # []
    - rule_id: T1-001
      rule_version: 3
      matched_anchor_values: ["user@example.com"]

  candidate_actions:              # conflict 时保留全部竞争分支，不能只留一个
    - fingerprint: "sha256:aaa..."
      rule_ids: [T1-001, T1-003]
      route: reply
    - fingerprint: "sha256:bbb..."
      rule_ids: [T1-009]
      route: forward

  selected_action_fingerprint: "sha256:aaa..." | null
  business_flow_ids: [ticket_policy]   # 非权威审计标签，见 §3

  reason_codes: [SENDER_EXACT_MATCH, SUBJECT_CONTAINS]
  ruleset_revision: <git-derived revision id>
  git_commit: <sha>
  registry_artifact_digest: <sha256>
  fingerprint_version: 1 | 2
  handoff_profile_id: generic_reply_v1 | null
  engine_version: <string>
  normalizer_version: <string>
  parser_version: <string>
  message_snapshot_hash: <ContentStore sha256 引用>

handoff_run:                     # 可变；由 profile/证据/审批/发送流程维护
  state: planned | evidence_ready | manual_review | approval_pending
       | approved | executing | completed | failed
  decision_digest: <RouteDecision canonical digest>
  plan_digest: <HandoffPlan canonical digest>
  evidence_digest: <EvidencePack canonical digest> | null
```

不变量：

| outcome | route | candidate_actions | selected_action_fingerprint |
|---|---|---|---|
| `matched`（含规则主动声明的 `manual_review`） | 非空 | 恰一个 fingerprint | 非空 |
| `conflict` | `manual_review` | ≥2 个不同 fingerprint | `null` |
| `error` | `manual_review` | — | `null` |

规则主动声明 `manual_review` 时：`outcome=matched`, `decision_origin=rule_declared`,
`selected_action_fingerprint` 非空。系统合成的 `manual_review` 不使用
`system_isolated`（生产环境不存在"隔离坏规则、其余继续跑"的机制，见 §7）；改用
`decision_origin: runtime_conflict | runtime_indeterminate | runtime_error`。

### 2.3 route 专属交付

```
reply / forward: handoff planned → evidence_ready → approval_pending
                 → approved → executing → completed | failed
read_only:        只读通知交付，失败不改变 route
no_action:        审计完成，不产生用户可见交付
manual_review:    durable handoff disposition=manual_review，进入人工判断
```

`manual_review` 不复用 draft approval——二者的业务含义不同（人工判断 vs 人工审批一份
已生成的草稿/转发计划）。Graph 的 `next_step` 只是内部调度提示，用户可见交付只根据
immutable route 与 durable handoff disposition 决定。

崩溃恢复后必须读回同一个 `RouteDecision`，不允许用新的 enabled ruleset 对同一封已
决策的邮件重新分类。profile、证据或草稿失败只能把 durable handoff disposition 转为
`manual_review`，不能返回 Tier 2/3 重猜。

## 3. `decision.params`、handoff profile 与 `action_fingerprint`

```yaml
decision:
  route: <CanonicalRoute>
  business_flow_id: ticket_policy     # 非权威审计标签，不进入 fingerprint
  handoff_profile_id: generic_reply_v1 # reply/forward 的版本化写作合同
  params: <按 route 类型的 schema 校验>
```

`business_flow_id`（曾称 `workflow_id`）**只用于审计和分组**，不驱动任何执行分支，因此
不参与冲突判定。`handoff_profile_id` 决定版本化的写作合同，属于权威动作的一部分，
因此进入 v2 fingerprint。未显式声明 profile 的新 `reply`/`forward` 决策分别展开为
`generic_reply_v1`/`generic_forward_v1`；旧的 v1 历史标签仍按 v1 fingerprint 读取，
不会静默改写。

```
fingerprint_version = 1: sha256(canonical_json({ route, params }))
fingerprint_version = 2: sha256(canonical_json({ route, params, handoff_profile_id }))
```

Canonicalization 规则（编译期在原子激活流水线中统一执行,见 §7）：

1. 展开该 route 的全部默认值（缺省字段按 schema 默认值补齐后再算指纹）；
2. 字段名按固定顺序排序；
3. 地址统一标准化（大小写折叠、去除显示名）、去重、排序；
4. `null` 与"字段缺省"语义统一（视为同一个规范化值）；
5. 固定的 JSON 序列化格式（无多余空白，键顺序确定）；
6. 记录 `fingerprint_version`，未来算法变化时递增,不静默改变旧记录的含义。

### 3.1 Profile、计划与证据

`HandoffProfile` 是代码注册表中的只读、版本化合同，不能由 YAML 指向任意模块或网络
地址。它只声明允许的 evidence source、writer mode、固定 prompt modifier 或 fixed
draft。每次执行将 profile 展开成不可变 `HandoffPlan`，再由封闭的
`EvidenceAdapterRegistry` 生成 `EvidencePack`。当前注册源只有：

- `mail_thread`：同线程历史邮件；
- `semantic_history`：Qdrant 历史邮件；
- `exchange_contact`：使用现有 Exchange 凭据解析发件人目录身份。

每个外部证据源调用前都必须重新经过当前 lease/effect fence。required source 缺失或任一
授权失败会 fail closed 到 handoff `manual_review`；optional source 自身不可用可以省略，
但不得吞掉授权失败。EvidencePack 只能编码写作事实，不能编码或改写 route、profile、
收件人。

新增简单业务规则只需要新增 `tier1_rules/*.yaml`；新增写作合同需要在 profile 注册表中
加入新版本 ID；新增外部系统需要实现一个类型化、只读 adapter 并加入封闭 allowlist，
不能恢复每规则 `handler.py`。

### 3.2 各 route 的 params

| route | 必需 params | 可选/说明 |
|---|---|---|
| `reply` | 无 | 可选 `reply_mode: sender_only \| sender_and_original_cc`。不写时沿用现有默认行为（`draft_to=[原发件人]`, `draft_cc=原 Cc 列表`，即 `sender_and_original_cc`）。v1 不开放 `reply_all`，除非后续显式处理共享邮箱、代理发送、重复地址、Reply-To、外部收件人这些边界情况。 |
| `forward` | `fixed_recipients: List[str]`（精确地址，禁止通配符/域名级规则） | 可选 `cc`、`allow_recipient_edit`（人工可编辑，仍需审批）、`include_attachments`（显式布尔）。命中外部地址（判据见 §3.2）时必须显式声明 `governance.external_recipient_acknowledged: true`，否则该规则在原子激活阶段判定为不合法。 |
| `read_only` | 无 | — |
| `no_action` | `reason_code`（业务命名空间，见 §3.3） | `owner`/`validity`/`positive_cases`/`negative_cases` 不属于 params，见下。 |
| `manual_review`（规则主动声明） | `reason_code`（业务命名空间） | 与系统合成的 `manual_review` 共用下游卡片/审计路径，`decision_origin` 区分来源。 |

`no_action` 是误判成本最高的路由（重要邮件被静默压制），因此 schema 强制：
`route=no_action` 时规则顶层/`governance` 必须提供 `owner`、`validity.expires_at`、
至少 1 个 `positive_cases`、至少 2 个 `negative_cases`；这些字段不放进
`decision.params`，因为它们不是执行参数，是治理元数据。

### 3.3 外部收件人判据

新增静态配置 `INTERNAL_EMAIL_DOMAINS`（逗号分隔的内部域名列表，编译期读取，零网络
依赖）。`forward.fixed_recipients` 中任一地址的域名不在该列表内即视为外部收件人。
原子激活流水线校验：外部收件人存在但缺少
`governance.external_recipient_acknowledged: true` → 该规则判定 schema 不合法，
整个新 revision 拒绝激活。

不使用运行时飞书 `open_id` 解析（`extract_external_emails_from_recipients()`）做这个
判定：它只解析已经带 `open_id=` 前缀的运行时字符串,不适用于编译期的裸地址列表，且会让
registry 编译依赖一次实时飞书接口调用。

### 3.4 `reason_code` 命名空间

规则声明的业务 `reason_code`（`no_action`/`manual_review` 使用）与系统故障码
`safety.manual_review.MANUAL_REVIEW_CODES` 是两个独立命名空间。审计记录里通过
`decision_origin` 区分来源（`rule_declared` 用业务码，`runtime_error` 等用系统码），
不允许混用同一个枚举。

## 4. 匹配 DSL

### 4.1 `match.anchor` 与 `match.conditions`

```yaml
match:
  anchor:
    any:
      - field: sender.address
        op: eq
        value: user@example.com
  conditions:
    all:
      - field: subject
        op: contains
        value: "退票"
      - field: body.current_text
        op: contains_any
        values: ["手续费", "退改规定"]
```

- `anchor` 只能是精确枚举类型：标量字段（`sender.address`）用 `eq`/`in`；地址集合字段
  （`to.addresses`/`cc.addresses`）用 `has_any`/`has_all`（不用 `contains`，避免和字符
  串子串匹配混淆）。禁止 `contains`/`regex` 作为 anchor——弱锚点等于没有锚点。
- `match.conditions` 才允许 `contains`/`regex` 等内容型操作符。
- 一条内容驱动的规则必须同时有 `anchor`，不能只靠 subject/body 广谱短语。
- `body` 默认匹配 `body.current_text`（复用现有
  `src/utils/email_body_projection.py` 的 `ModelBodyProjection.current_text`，不
  新写"新增正文识别"算法）。需要匹配引用历史的规则必须显式声明
  `body.full_text` 并把 `governance.full_text_match_acknowledged: true` 设为
  机器可校验字段（不是文字说明）。

### 4.2 三态语义：`VALUE` / `EMPTY` / `UNKNOWN`

字段解析结果分三种状态，"缺失字段"不等于"无法确定"：

- `VALUE`：有正常值,正常参与 `eq/in/contains/has_any/has_all` 计算。
- `EMPTY`：已知为空或字段确实不存在（如 `cc=[]`、`current_text=""`），仍然正常参与
  计算，通常得到 `FALSE`，不是 `UNKNOWN`。
- `UNKNOWN`：真正无法确定（正文投影失败、地址解析失败、数据损坏）。

条件求值：

```
anchor = FALSE                        → NO_MATCH（不再算内容条件）
anchor = TRUE,  condition = UNKNOWN   → outcome=error → manual_review
anchor = UNKNOWN                      → outcome=error → manual_review
anchor = TRUE,  condition = FALSE     → NO_MATCH
anchor = TRUE,  condition = TRUE      → MATCH
```

`all`/`any`/`not` 使用标准三值逻辑：

```
NOT UNKNOWN = UNKNOWN
ALL: 任一 FALSE → FALSE；全部 TRUE → TRUE；其余 → UNKNOWN
ANY: 任一 TRUE  → TRUE； 全部 FALSE → FALSE；其余 → UNKNOWN
```

`not` 不能作为唯一顶层条件（防止用 `not sender=X` 构造出近似全量匹配的规则）。

### 4.3 地址来源冻结

Tier 1 anchor 使用的 `sender.address` 必须与 drafter/sender 用作回复目标的同一个
规范化 `sender` 字段一致（现有实现已经共用同一个字段，这里固化为不可违反的约束），
防止未来出现"Tier 1 按 From 命中，实际回复发到 Reply-To"的分裂。

### 4.4 正则安全

不采用"允许任意 Python 正则再检测危险模式"，改为只允许一个明确的安全子集：字符类、
锚点（`^`/`$`）、单个 token 上的有界量词、有限分支数的 alternation；禁止嵌套量词、
反向引用、lookahead/lookbehind、可能产生指数级回溯的组合。此外：

- pattern 长度上限、输入正文长度上限；
- 激活前强制 `compile` 自检；
- 单次规则求值的时间预算监控，超预算 → `outcome=error → manual_review`（不是让请求
  卡死）。

这一版不引入 `re2` 依赖（项目目前只有标准库 `re`），用上述静态限制代替。

### 4.5 地址/group 声明

v1 不引入身份解析器或目录集成（`identity_id` 不存在，见 §1）。规则粒度：一条规则 =
一个可独立治理的业务政策。允许 group：当多个地址的 owner/route/business_flow_id/
params/validity/风险完全相同时，可以在 `anchor.any` 里内联列出多个地址；编译后在
registry 里展开为逐地址的独立审计条目（`matched_anchor_values` 保留实际命中的地址，
不丢失粒度）。是否需要一个独立的、可跨规则复用的命名 address group 注册表,留给
第 9 节的 31 条旧规则复核阶段:如果发现真实的跨规则复用需求再引入，不预先建结构。

## 5. 生命周期

```yaml
status: enabled          # proposed | enabled | retired，只能手动编辑 YAML

validity:
  effective_from: "2026-08-01T00:00:00Z"
  expires_at: "2026-12-01T00:00:00Z"    # no_action 强制要求

owner: ticketing-team
```

- `status` 只能人工在 Git 里维护，系统从不代写；生产可用条件是
  `status == enabled AND effective_from <= decision_time < expires_at`。
- 到期只是运行时停止匹配该规则并产生持久化告警，不触碰 YAML/`status`——避免出现
  "YAML 说 enabled、运行时说 retired"两个事实源。
- 退休规则重新启用需要新的证据、冲突检查和有效期,不是简单改回 `enabled`。

## 6. 静态冲突检查：hard error / warning 分层

正则、`any`/`all`/`not` 的存在使静态分析器不可能完整证明"两条规则一定不重叠"。因此
"无法证明不重叠"不能等价于"冲突"：

**Hard error（阻止激活）**：
- 完全相同的规范化 `match`,但 fingerprint 不同；
- 相同精确 anchor + 相同 conditions，但 action 不同；
- 重复 `rule_id`；
- 同一 `rule_id`/`rule_version` 内容不一致；
- 永远不可能匹配的表达式；
- 非法 route params；
- 无法编译的正则。

**Warning（不阻止激活，仅记录）**：
- 两个正则可能重叠但无法证明；
- `contains` 与正则可能共同命中；
- 两个地址集合部分交叉；
- 一条规则可能是另一条的子集；
- `body.full_text` 规则可能因引用历史与其他规则共同命中。

Runtime conflict（§2.2 的 `candidate_actions` 多 fingerprint）才是最终权威处理路径；
warning 不代表问题已经被处理，只是不足以在编译期一律拒绝。

## 7. 生产激活：artifact 化，不是"进程拒绝启动"

```
raw YAML → compile/validate → immutable artifact（含 digest）→ atomic pointer switch
```

1. 任意 `enabled` 规则校验失败（schema → 地址标准化校验 → 可选命名 address group
   引用校验 → regex 编译 → route params 校验 → 重复 ID 校验 → §6 的静态 overlap/
   conflict 检查 → §8 的 fixture 回放）→ **新 artifact 编译/激活失败**，当前正在
   运行的旧 artifact 和进程**不受影响**；不做部分加载。
2. 只有当进程要加载"已经选定的"artifact、且该 artifact 本身加载或 digest 校验失败
   时，才 `fail hard`——这种情况应当极少见，因为被选中的 artifact 已经过完整校验。
3. 应用内部不做静默的 last-known-good 回退；回退与否是运维在编译阶段就该发现并处理
   的事，不是运行时行为。
4. 规则加载只在计划重启时发生，没有热加载。
5. 单封邮件在运行时遇到的解析异常是邮件级 fail-closed（`outcome=error →
   manual_review`），不是加载器悄悄删除某条规则。

"验证记录"就是规则 YAML 里的 `governance.positive_cases`/`negative_cases`
（引用 Git 版本化的 fixture），每次计划重启的原子激活流水线都会重新跑一遍;
"human review 通过"就是这条 YAML 改动本身经过正常的 Git/PR 审核。运行时逐邮件路由、
handoff、payload revision 和 approved envelope 使用 greenfield baseline 中的独立持久化表；
它们不是规则发布审批表。

## 8. Fixture 验证：两层

**规则级**：`positive_cases` → 该规则必须 `MATCH`；`negative_cases` → 必须
`NO_MATCH`。

**Ruleset 级回归语料**：用完整 `enabled` 集合执行,断言
`outcome`/`route`/`matched_rule_ids`/`selected_action_fingerprint`
（或 conflict 场景下的 `candidate_rule_ids`）。只有整体评估才能发现的问题——多规则
冲突、group 展开、`no_action` 对其他规则的遮蔽、regex/contains 共同命中、新规则对
旧规则的意外影响——都要在这一层覆盖。

最低要求：每条 `enabled` 规则 ≥1 positive case；内容型规则 ≥1 hard negative；
`no_action` ≥1 positive + ≥2 hard negative。任何 `match` 或 `decision` 字段变更都
必须触发 ruleset 回归语料全量重跑。

## 9. Schema 严格性

所有 manifest schema 使用 `additionalProperties: false`；YAML loader 拒绝重复
key、拒绝未知字段、拒绝旧字段（`need_reply`/`card_type`/`priority`/`action`/
`forward_to`/`tone_instruction` 等 `AutoOutcome` 遗留字段）静默通过——写错字段名
必须在激活阶段就报错，不能被默默忽略。

## 10. 31 条旧规则迁移

分类法：

```
KEEP | REWRITE | SPLIT | CONSOLIDATE | MOVE_TO_INTAKE_GUARD | MOVE_TO_TIER2
| MANUAL_ONLY | RETIRE
```

每条规则至少填写：

```
old_rule_id / business_purpose / owner / classification / migration_status
positive_samples / hard_negative_samples
identity_anchor / content_conditions
canonical_route / business_flow_id / authoritative_params
overlap_candidates / failure_cost / validity / final_decision
```

`migration_status: pending_target_layer | already_supported | explicitly_deferred`
是必填字段——`MOVE_TO_INTAKE_GUARD`/`MOVE_TO_TIER2` 只是迁移结论，不代表目标层已覆盖
该条规则；必须用当前 Intake Guard policy 或 Historical Route Consensus 回放验证。缺少这个字段容易出现
"旧规则退休了、文档说移到 Tier 2、但 Tier 2 这一版没改、实际产生无人处理的覆盖缺口"。

切换前必须产出覆盖清单，每条旧规则归入且仅归入一类：

- 已由新 Tier 1 覆盖；
- 已由现有 Tier 2 覆盖并经回放验证；
- 明确转 `manual_review`；
- 明确退休且业务 owner 接受；
- 暂缓迁移（`explicitly_deferred`，不得删除旧行为）。

## 11. Intake Guard、审批冻结与执行门禁

Intake Guard 在 durable inbox 归一化/去重后、Tier 1–3 前运行，只能返回：

- `pass`：允许进入路由和 Qdrant；
- `suppress`：高置信自动回复或明确邮件循环，审计后结束；
- `quarantine`：NDR、敏感度/保密标签、解析异常，等待人工释放。

Tier 0 `suppress` 表示系统入口不应处理；Tier 1 `no_action` 表示邮件已被业务政策识别但
无需业务动作，两者使用独立 durable audit。quarantine release 追加 `intake_releases`
记录并创建更高 execution epoch 的新 attempt，绝不覆盖旧 decision。营销邮件、newsletter
和 `List-Unsubscribe` 不属于 Tier 0 的模糊垃圾邮件识别。

Draft Approval 卡片绑定完整的 immutable payload revision：decision/plan/evidence digest、
draft、To/Cc 和 attachment manifest。任何编辑都追加新 revision，使旧卡片 action 失效。
durable approval 在一个事务中锁定 handoff、payload 与邮件审计状态，验证 revision/digest
后创建 append-only `ApprovedExecutionEnvelope`。sender 不读取 checkpoint 中可变 draft 或
收件人，只消费该 envelope，并在 Exchange effect 前经过确定性 `ExecutionGate` 与一次性
execution claim。门禁只能阻止并转人工，不能改写已批准内容或重新路由。

现有 reviewer 是 Draft Quality Gate：可根据原邮件和 EvidencePack 判断 ready/rewrite/
manual review，但不能改变 route/profile/recipients。人工审批后的 Execution Gate 完全确定
性运行，不是 Tier 4。

仍延后的事项：通用 `identity_id` 目录层、外部域可信等级、飞书权限受控下钻查看器、
跨规则复用的命名 address group，以及无人工确认的自动发送。
