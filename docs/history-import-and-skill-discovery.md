# 历史邮件导入与 Skill 候选发现

历史邮件是一次性初始化资料，承担两个用途：

1. 写入与在线检索相同的 Qdrant `emails` 集合，作为新邮件 RAG 的历史背景；
2. 离线发现可人工确认的 Tier 1 声明式路由候选。

这两个操作都必须由操作者手工发起。服务不会定时扫描 PST、Mbox、EML，也不会把发现结果自动变成生产规则。

## 1. 手工导入历史邮件

优先导入 Outlook PST。Mbox、EML 和 EML 目录仍可用于迁移或排查，但不会启动任何持续同步。

先预览，再进行一次正式导入：

```bash
.venv/bin/python scripts/import_pst.py archive.pst --dry-run
.venv/bin/python scripts/import_pst.py archive.pst
```

正式导入通过 `EmailProcessor` 写入默认的 `emails` 集合；在线 `retriever_node` 也读取这个集合。因此导入完成后，历史邮件会自然成为当前邮件拟稿的 RAG 背景，无需建立第二个“历史库”。

常用参数：

```text
SOURCE                    PST/Mbox/EML 文件或 EML 目录
--batch-size N            每批处理数量（默认 50）
--dry-run                 只预览，不写入 Qdrant
--source exchange         手工从 Exchange 拉取；不是后台同步任务
--folder NAME             Exchange 文件夹
--limit N                 Exchange 拉取上限
```

PST 解析优先使用 `libpff-python`；不可用时，脚本会回退到已安装的 `readpst`。大文件可调大 `--batch-size`，但建议仍先执行 `--dry-run`。

## 2. 发现候选，不直接启用规则

导入完成后，默认从共享 Qdrant 语料发现候选，并默认调用已配置的 LLM：

```bash
.venv/bin/python scripts/discover_skills.py
```

如需只用统计启发式：

```bash
.venv/bin/python scripts/discover_skills.py --no-llm
```

发现按时间而不是随机切分：最早 80% 的历史邮件用于发现，最新 20% 用于回放。每个候选都会显示：

- 完整的生产有效字段：目标 Skill ID、触发条件、条件逻辑、优先级、是否需要回复、语气、动作和固定收件人；
- 发现期样本量、回复率和置信度；
- 最新 20% 上的命中数、命中邮件观察回复率，以及少量主题/发件人示例；
- 不能提升的配置问题，例如运行时不支持的条件或无固定收件人的转发。

回放没有自动通过阈值。它是给操作者判断的证据，而不是自动授权。

发现会把候选及其有界回放快照写到本地忽略目录：

```text
artifacts/skill-discovery/review-<UTC 时间>.json
```

可用 `--review-output` 指定位置。脚本不会写入 `skills_registry/`，也不提供 `--auto-confirm`。

直接分析尚未导入的 PST 或 EML 只用于发现，**不会**把它们加入在线 RAG：

```bash
.venv/bin/python scripts/discover_skills.py --source pst --pst-path archive.pst
.venv/bin/python scripts/discover_skills.py --source eml --pst-path ./eml-archive
```

如果这些历史还需要供在线邮件检索，应先执行第 1 节的 `import_pst.py`。

## 3. 对话确认与提升

候选应由对话助手完整展示给操作者。操作者可以选择候选，并修改任何生产有效字段，包括触发条件和 `forward_to`。修改后的触发条件会在保存的最新 20% 回放快照上重新计算，再进入提升校验。

对话确认后，助手使用同一个发现工具的内部提升模式传入明确选择；没有“默认全选”。选择文件的最小形态如下（通常由对话助手生成）：

```json
{
  "selections": [
    {
      "candidate_id": "discovered_001",
      "overrides": {
        "skill_id": "skill_auto_finance_forward",
        "suggested_action": "forward",
        "suggested_forward_to": ["open_id=leader"],
        "suggested_need_reply": true
      }
    }
  ]
}
```

实现接口是：

```bash
.venv/bin/python scripts/discover_skills.py \
  --promote-review artifacts/skill-discovery/review-<UTC 时间>.json \
  --selections /path/to/confirmed-selections.json
```

这是同一发现工作流的确认阶段，不是给无人值守任务使用的独立批量提升机制。它会：

1. 校验所有被选候选和修改后的字段；
2. 在写入前检查所有目标 Skill ID；若任一目标已存在，整体停止，不覆盖、不合并；
3. 仅写入 `manifest.yaml`，不生成 `handler.py`；
4. 等待下一次计划服务重启加载规则，不热加载运行中的服务。

生成的规则使用通用 `AutoOutcomeSkill`。发现可以依据历史已发送邮件推测 `forward_to`，以尽量发现有价值的规则；但它始终只是候选，操作者必须在对话中确认或修改。提升后的 `forward` 必须有固定 `forward_to`，并只设置可编辑的转发收件人和审批草稿；真正的外发仍需经过飞书审批，绝不会由发现或路由规则直接发送。

## 4. 已知边界

- 运行时当前支持 `sender_match`、`subject_match`、`body_match`、`to_match`、`cc_match`，以及 `eq`、`contains`、`regex`、`in` 操作符。发现可以展示更多历史线索，但不支持的条件不能提升，避免生成永远不会命中的规则。
- 历史邮件导入并不自动产生 Tier 2 标签；Tier 2 仍只基于已记录的真实路由样本。
- 发现、回放和提升均不发送邮件；转发规则最多创建可编辑、待审批的计划。
