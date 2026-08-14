# 历史邮件导入与 Skill 候选发现

历史邮件是一次性初始化资料，承担两个用途：

1. 写入与在线检索相同的 Qdrant `emails` 集合，作为新邮件 RAG 的历史背景；
2. 离线发现可人工确认的 Tier 1 声明式路由候选。

这两个操作都必须由操作者手工发起。服务不会定时扫描 PST、Mbox、EML，也不会把发现结果自动变成生产规则。

## 1. 手工导入历史邮件

优先从 Exchange 回填服务器仍保留的历史，再用 Outlook PST 补充服务器不存在的邮件。Mbox、EML 和 EML 目录仍可用于迁移或排查，但不会启动任何持续同步。

先预览，再进行一次正式导入：

```bash
.venv/bin/python scripts/import_pst.py archive.pst --dry-run
.venv/bin/python scripts/import_pst.py archive.pst

# Exchange 全部业务邮件文件夹 + PST 补充；务必先 dry-run
.venv/bin/python scripts/import_pst.py \
  --source exchange --folder ALL --limit 0 --all-mail \
  --supplement archive.pst --dry-run
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
--all-mail                包含已读邮件；历史回填必须显式使用
--supplement PATH         Exchange 优先完成后，用 PST/Mbox/EML 补充缺失邮件
--route-evidence-folder   额外读取一个 Exchange 文件夹作为 route_decision 证据；
                          证据邮件本身不导入
--route-evidence-limit    route_decision 证据邮件上限；默认沿用 --limit
```

`--folder ALL` 会导入 Inbox、Sent Items 和非空的用户自建邮件文件夹，默认排除 Drafts、Outbox、Junk Email 和 Deleted Items。列表、详情或服务端总数不完整时，命令以失败退出，不把部分结果报告成完整导入。

同一次 `--source exchange --supplement ...` 运行总是先读取 Exchange。Internet Message-ID 相同的本地副本会跳过；缺少 Message-ID 时使用规范化邮件签名兜底。摘要分别显示空正文、重复、失败、成功邮件和 Qdrant 点数；历史导入会等待 Qdrant 确认写入，Qdrant/Embedding 返回零点时进程以非零状态退出。

### 历史 route_decision 证据

导入器不会猜测历史路由，只给主导入范围内的收件邮件写入可验证的
`route_decision`。已发送邮件可以通过 `--route-evidence-folder` 作为只读证据，但
不会被导入。当前只启用两条判定：

1. 已发送邮件的 `In-Reply-To` 与收件邮件的 `Internet Message-ID` 规范化后精确相等，
   收件邮件标记为 `reply`；
2. 已发送邮件明确包含转发标记，能够唯一对应原邮件，并且能取得至少一个实际 To
   地址，原邮件标记为 `forward`，`params.fixed_recipients` 保存实际地址。

转发正文缺少可验证的原发件人、原主题、原邮件标识或原文片段，收件人为空、对应
不唯一，以及其他情况均不写 `route_decision`。已写入的历史邮件仍可作为普通
`Historical RAG Context`，没有标签不等于导入失败，也不会自动进入 Tier 2 路由投票。

例如，只导入 100 封 Inbox、读取 Sent 作为证据：

```bash
.venv/bin/python scripts/import_pst.py \
  --source exchange --folder inbox --limit 100 --all-mail \
  --route-evidence-folder sent --route-evidence-limit 200 \
  --dry-run
```

PST 解析优先使用 `libpff-python`；不可用时，脚本会回退到已安装的 `readpst`。大文件可调大 `--batch-size`，但建议仍先执行 `--dry-run`。

### Docker 一次性任务

历史导入使用独立覆盖文件，不进入常驻应用进程。必须复用当前部署的 Compose project 和应用镜像，原始 PST 只读挂载，`HISTORY_IMPORT_WORKDIR` 是 `readpst` 的显式临时展开目录。

```bash
project_name="$(tr -d '\r\n' < secrets/compose_project_name)"
app_image="$(docker ps \
  --filter "label=com.docker.compose.project=$project_name" \
  --filter "label=com.docker.compose.service=ai-assistant-service" \
  --format '{{.Image}}' | head -n 1)"

export AI_EXCHANGE_IMAGE="$app_image"
export HISTORY_IMPORT_SOURCE="/absolute/path/archive.pst"
export HISTORY_IMPORT_WORKDIR="/absolute/path/history-import-work"

# 构建一次性镜像
docker compose -p "$project_name" \
  --env-file .env --env-file secrets/deployment.env \
  -f docker-compose.yml \
  -f docker-compose.history-import.yml \
  -f docker-compose.history-import.exchange-tls.yml \
  --profile history-import build history-import

# 默认命令是全量 Exchange + PST 补充的 dry-run
docker compose -p "$project_name" \
  --env-file .env --env-file secrets/deployment.env \
  -f docker-compose.yml \
  -f docker-compose.history-import.yml \
  -f docker-compose.history-import.exchange-tls.yml \
  --profile history-import run --rm history-import

# 核对 dry-run 后，正式写入 Qdrant
docker compose -p "$project_name" \
  --env-file .env --env-file secrets/deployment.env \
  -f docker-compose.yml \
  -f docker-compose.history-import.yml \
  -f docker-compose.history-import.exchange-tls.yml \
  --profile history-import run --rm history-import \
  --source exchange --folder ALL --limit 0 --all-mail \
  --supplement /imports/history.pst
```

若 Exchange 使用公共证书且不需要固定私网主机别名，省略 `docker-compose.history-import.exchange-tls.yml`。该任务只写共享 Qdrant `emails` 集合，不写 PostgreSQL Durable Inbox、Content Store，不触发分类、通知或已读操作。

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
