# AI-Exchange 清理审计与绿地部署基线（2026-08）

## 已确认的部署决策

- 仅支持**全新空数据库**：不再提供历史 AI-Exchange 数据库的升级、回滚或兼容路径。
- 入站只保留 Exchange 轮询；HTTP Webhook、订阅、旧队列与其密钥均已移除。
- Tier1 v1 规则资产保留并随镜像交付，但未接入 `src/router/engine.py`，不会改变当前默认路由。
- 偏好/风格记忆学习默认关闭；只有显式设置 `MEMORY_LEARNING_ENABLED=true` 才允许写入学习结果。

## 已删除或收敛的内容

| 类别 | 已完成的清理 |
| --- | --- |
| 代码 | 删除未完成的 Webhook ingress、`cold_start.py`、`sync.py`、`command_receipts.py`，并移除所有运行时 import 与启动路径。 |
| 数据库 | 删除 8 个历史 Alembic revision，改为唯一不可降级的 `20260808_0001` polling 基线；旧表、列和 routine 会被 schema contract 拒绝。 |
| 测试 | 移除只覆盖已删 cold-start/sync/Webhook 架构的测试，替换为 polling 基线、无旧入口和单根 revision 的契约测试。 |
| 依赖 | 直接依赖从 `langchain` 收敛为实际使用的 `langchain-core`；移除未使用的直接 `tqdm`；测试卡脚本改用既有的 `httpx`。 |
| 构建 | 忽略 `*.egg-info/`，防止 `uv sync` 生成物污染工作树或进入镜像；镜像显式复制唯一运行时允许清单及保留的 `tier1_rules/`。 |
| 配置与文档 | 删除 Webhook/Shadow/Reconciliation 配置漂移，改写 README、部署说明、CI 名称和运行时 capability 命名为 polling 基线。 |

## 有意保留的资产

- `src/ingestion/legacy_adapter.py`：当前轮询事件仍通过它进入既有邮件处理链，属于活跃兼容边界，不能按名称删除。
- Tier1 v1：规则与编译器通过测试，但尚未激活；后续启用需要单独决定 artifact 存放、热加载/发布方式及与旧 `Tier1ReflexRouter` 的切换策略。
- daily digest、checkpoint 维护和数据库角色 preflight：均有当前调用路径，保留为运行时或安全边界，而不是历史兼容层。

## 空环境验证证据

已使用隔离 Compose 项目 `ai-exchange-schema-audit`，重复执行：构建 Python 3.12 镜像、启动空 PostgreSQL/Qdrant 卷、`database-provision`、`database-bootstrap`。

- `public.alembic_version = 20260808_0001`；
- capability stage 仅允许 `polling_ingestion`、`approval_send`、`graph_projection`；
- schema contract、角色 preflight 与 checkpoint bootstrap 均在 bootstrap 中完成；
- 全量测试：`5041 passed, 167 skipped`；Ruff 通过。

## 首次真实部署与旧资源回收

真实部署必须使用新的 Compose project name，保留现有 `secrets/`，按以下顺序完成：生成部署配置、创建空卷、provision、bootstrap、生成并初始化 polling manifest、启动服务并通过 `/ready`。只有该新项目通过 readiness 后，才可精确识别并删除旧 Compose 项目的容器和命名卷；不得删除 `secrets/` 或任何未确认归属的 Docker 资源。

外部 Exchange/Lark 实际效果仍取决于部署环境中已配置的集成凭据；健康检查、数据库基线、轮询授权和应用就绪需与外部副作用验证分开记录。
