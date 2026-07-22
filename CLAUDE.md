# AI Email Assistant：当前架构与开发契约

本文只描述当前代码和必须保持的设计约束。历史实施计划、阶段报告和故障纪要由 Git
历史保存，不再追加到本文件。

## 1. 系统目标

AI-Exchange 接收 Exchange 邮件，生成结构化路由决策和回复草稿，并通过飞书完成
Human-in-the-Loop 审批。系统的核心价值包括：

- PostgreSQL Durable Inbox 保证收件事件先持久化、后处理；
- Tier 1/2/3 Router 在确定性、个性化历史和开放式推理之间分层决策；
- Qdrant 提供历史邮件检索、Tier 2 路由样本和离线 Skill Discovery 语料；
- LangGraph 与 Postgres Checkpointer 支持流程暂停、审批和恢复；
- 飞书审批是发送、附件上传和卡片副作用的授权入口。

## 2. 当前运行拓扑

生产只有一个 FastAPI 应用进程：

1. `POST /webhooks/exchange` 验证请求并写入 PostgreSQL Durable Inbox；
2. 同进程的 `DurableInboxWorker` 领取持久化事件；
3. `LegacyProcessingAdapter` 调用当前邮件处理实现；
4. LangGraph 执行分类、检索、草稿、审核和发送；
5. 飞书 WebSocket 接收审批动作并恢复对应 Checkpoint。

当前生产路径没有以下后台入口：

- 旧的内存 Webhook Queue/Worker；
- 无游标的 recent-mail polling；
- Daily Summary scheduler；
- SelfHealer 扫描循环。

不要重新引入这些入口。恢复工作由 Durable Inbox、启动恢复、显式运维命令和人工处理
承担。数据库中历史 `recovering` 状态的启动兼容仍需保留。

## 3. 关键深模块与 interface

### 3.1 Durable Ingestion

主要路径：

- `src/ingestion/webhook.py`：纯请求规范化与 receipt；
- `src/ingestion/repository.py`：Inbox 持久化和租约；
- `src/ingestion/worker.py`：领取、续租、重试和完成；
- `src/ingestion/runtime.py`：唯一运行时装配；
- `src/ingestion/legacy_adapter.py`：持久化 Worker 到邮件处理实现的 Adapter。

`src/ingestion/__init__.py` 不再做重型再导出。调用者必须从拥有类型或行为的 module
直接导入，避免导入 models 时加载 cold-start、sync 和 repository 实现。

Webhook 返回 202 只表示事件已进入 Inbox，不表示 Qdrant、草稿、飞书卡片或发送已经
成功。排障必须逐阶段验证。

### 3.2 Router 与 Skill

设计目标是：

1. Tier 1：YAML 硬规则，低延迟、可解释；
2. Tier 2：Qdrant 中带真实标签的历史样本；
3. Tier 3：只有 Tier 1/2 放弃或低置信度时才由 LLM 兜底；
4. 三层只能产生一个权威 `RouteDecision`，后续节点不得覆盖。

当前实现尚未完全达到这个目标：`RoutingEngine.execute_router()` 在 Tier 1 未命中后
会先执行 Tier 3；Tier 2 位于 Retriever，且只有 Categorizer 已判定
`need_reply=True` 后才能执行。修改路由时必须先修复这一执行顺序，并增加以下不变量
测试：

- Tier 1 命中时不调用 Embedding 或 LLM；
- Tier 2 高置信度时不调用 Tier 3；
- Tier 2 放弃时 Tier 3 只调用一次；
- 后续节点不得覆盖最终路由决策。

`skills_registry/` 是生产 Skill 注册表。示例、演示或含虚构收件人的 Skill 不得进入
该目录。

### 3.3 Qdrant、PST 与 Skill Discovery

Qdrant 是可重建的检索投影，不是权威业务存储。PostgreSQL 或原始邮件归档应保留
可追溯的事实。

目标数据职责应分离：

- 路由样本：每封邮件一个规范化决策标签，供 Tier 2 使用；
- 历史正文：可按段检索的文本，供草稿上下文使用；
- 风格/偏好：版本化用户画像或可检索偏好。

`scripts/import_pst.py` 负责 PST/Mbox/EML/Exchange 历史导入。PST 原始邮件只有进入
Qdrant 并不等于形成 Tier 2 数据；必须补充真实的回复行为、动作、意图、优先级、
标签来源、置信度和版本。

`scripts/discover_skills.py` 只应生成候选规则。目标生命周期是：

`proposed -> reviewed -> enabled -> retired`

候选规则必须通过时间切分验证和人工确认后，才能进入生产注册表。

### 3.4 Preference、Experience 与 Style Memory

三类记忆应保持不同职责：

- Routing Examples：邮件处理结果，属于 Router；
- Preference Memory：用户修改、拒绝、长度、语气和收件人偏好；
- Style Profile：从用户真实发件中提炼的版本化写作风格。

读取逻辑存在并不代表学习闭环已经运行。新增学习逻辑时必须有明确触发器、持久化
位置、数据版本、回滚方式和离线评估。

### 3.5 LangGraph 与 Checkpointer

`src/graph/builder.py` 定义当前工作流：

`categorizer -> retriever -> drafter -> reviewer -> sender/manual_review`

Reviewer 后通过 Postgres Checkpointer 暂停，飞书审批后恢复。完整邮件正文、附件字节和
草稿正文不得直接放入 Graph State；State 只保存 ContentRef、draft_id 和有界元数据。

Checkpointer 的迁移、运行时写入和清理使用不同数据库身份。不要让 migration 或
maintenance DSN 进入应用容器。

### 3.6 Provider

`src/providers/` 是模型选择 seam。业务 module 调用 `get_llm_for_role()` 或正式的
Provider interface，不得重新增加 `src.utils.llm_factory` 兼容壳。OAuth/Codex
订阅接入属于可选 Adapter，不应渗透 Router、Graph 或 Memory 的 interface。

## 4. 内容与副作用安全

- HTML 在解析和渲染前使用 `nh3` 净化；
- Base64、图片 data URI 和附件二进制不得进入分类器、Qdrant payload、Graph State
  或普通模型输入；
- 图片分析只在 `need_reply` 等业务条件满足后按需执行；
- 附件上传、审批卡片和发送必须经过显式授权；
- 日志不得包含密钥、邮件正文、真实标识符或异常原文，只记录有界
  `safe_code`、`stage` 和 `error_type`。

## 5. 配置与部署

本地和 Compose 共用最小 `.env`。复制 `.env.example` 后只填写其中的 17 个集成及
模型值：

```bash
cp .env.example .env
.venv/bin/python scripts/configure_deployment.py
.venv/bin/python scripts/deploy_system.py check
```

数据库角色、DSN、令牌、ContentStore key、运行限额和部署状态生成到忽略版本控制的
`secrets/`。测试需要隔离本地部署配置时使用 `PYTHON_DOTENV_DISABLED=1`。

常用命令：

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
source .venv/bin/activate && python -m src.main
curl http://localhost:8000/health
```

PostgreSQL 与 Qdrant：

```bash
docker compose up -d qdrant postgres
```

`langgraph-checkpoint-postgres==3.0.4` 的迁移 6–8 使用
`CREATE INDEX CONCURRENTLY`。数据库首次 bootstrap 必须使用隔离的 migration owner
secret，并遵循 `deploy/README.md` 的角色和所有权检查。

## 6. 修改与验证规则

- 修改整个项目之前先确认架构、可靠性、安全、测试和运维基线；
- 保留工作树中与当前任务无关的用户改动；
- 删除 module 前验证生产可达性、动态导入、运维入口、数据迁移职责和测试职责；
- 测试通过 module 的 interface 验证行为，不为已删除的兼容壳保留专属测试；
- Qdrant、邮件处理和审批变更必须使用现实 payload shape 做 E2E 验证；
- `ruff` 和全量测试通过不等于运行态正确，部署后还要验证 Compose、`/health`、
  `/ready`、日志和逐阶段处理结果。

## 7. 目录速查

- `src/ingestion/`：Durable Inbox、租约、策略和运行时；
- `src/router/`：Tiered Router；
- `skills_registry/`：生产 YAML/handler Skill；
- `src/skills_discovery/`：离线模式分析和候选规则；
- `src/memory/`：Preference、Experience、Style；
- `src/graph/`、`src/nodes/`：LangGraph 工作流；
- `src/providers/`：模型 Provider 和 OAuth Adapter；
- `src/db/`、`src/maintenance/`：数据库角色、Checkpoint 与维护；
- `src/storage/`：ContentStore；
- `scripts/`：部署、迁移、导入和显式运维入口；
- `deploy/README.md`：首次部署与生产门禁。
