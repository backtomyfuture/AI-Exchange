# AI Email Assistant (Enterprise Edition)

这是一个为企业环境设计的高级 AI 邮件处理系统。它利用 Gemini 3 Flash 进行智能推理，Qdrant 作为向量数据库（RAG），并使用 LangGraph 编排复杂的 Agent 工作流。系统通过飞书 (Lark) 与用户进行审批互动。

## 核心功能

1.  **自动化同步**: 自动轮询 Exchange 邮箱，抓取新邮件。
2.  **智能分类**: 使用 LLM 识别邮件意图，判断是否需要回复及其紧急程度。
3.  **多模态 RAG**: 支持提取邮件文本和图片描述，并存储在 Qdrant 中作为历史背景。
4.  **人机协作 (Human-in-the-Loop)**:
    -   系统生成回复草稿并通过飞书交互式卡片推送给用户。
    -   用户可以点击“通过”、“拒绝”或“修改”建议。
5.  **自动回复 & 归档**: 审批通过后，系统自动发送邮件并将回复内容索引回 Qdrant。

## 系统架构

-   **编排层**: `LangGraph` (基于 Postgres 的状态持久化)。
-   **向量数据库**: `Qdrant` (存储邮件嵌入向量，支持语义搜索)。
-   **推理引擎**: `Gemini 3 Flash` (通过 OpenAI Adapter 调用)。
-   **集成端口**:
    -   `Exchange API`: 自定义适配器，处理邮件收发和状态更新。
    -   `Lark (飞书)`: 采用 WebSocket (长连接) 监听回调，HTTP API 发送交互卡片。

## 分离式服务

项目在部署时分为两个核心容器服务：
-   **`exchange-service`**: 负责邮件同步循环、初步分类、RAG 检索和生成初始草稿并推送卡片。
-   **`lark-service`**: 运行 WebSocket 监听器，接收用户在飞书卡片上的操作，并根据反馈恢复/更新 LangGraph 状态机。

## 快速开始

### 1. 环境准备

安装依赖（MacOS 环境）:
```bash
python -m venv .venv
source .venv/bin/activate
uv sync --frozen
```

### 2. 配置环境变量

直接在本机运行 Python 时，复制 `.env.runtime.example` 到 `.env.runtime`；
使用生产 Compose 时，复制 `.env.example` 到 `.env`，并将 migration-owner
完整 DSN 单独写入 `MIGRATION_DATABASE_URL_FILE` 指向的 0600 文件；DSN 的
`options` 必须精确设置为 `-csearch_path=<目标 schema>`。不要显式把
`pg_catalog` 放到目标 schema 后面：当它未显式列出时，PostgreSQL 会先解析
系统目录，同时仍把未限定的新对象创建到目标 schema，从而避免同名类型劫持
DDL。不要把 admin 或 migration 凭据放入运行时配置。随后填入：
-   Exchange API 认证信息
-   飞书 App ID & Secret
-   Gemini API Key & Base URL
-   Postgres & Qdrant 连接信息

### 3. 运行系统

使用 Docker Compose 启动完整环境：
```bash
# 仅在独立 DBA checkpoint 已完成备份、角色创建、ownership 转移和权限复核，
# 且 disposable PostgreSQL 验证通过后执行；当前 live 数据库尚不满足该条件。
docker compose --profile migration run --rm database-bootstrap
docker compose up -d
```

数据库门禁会在任何 DDL、应用上下文或外部 worker 启动前验证 migration/runtime
均为独立 `LOGIN NOINHERIT` 非特权角色、双方没有任何授予型 membership，目标
database/schema/对象均由 migration role 持有，且除 migration owner 外无人拥有
目标 schema 的 `CREATE`。runtime 还必须没有 database `CREATE/TEMP` 和 schema
`CREATE`。当前 database、目标 schema 与目标对象的显式 ACL 只能出现 migration
和 runtime 两个角色；`PUBLIC` 或第三方角色不能读取目标数据，也不能借助其他用户
schema、large object、FDW/server、系统目录新增 ACL 或 `SECURITY DEFINER` 间接
提权。两个受管角色和当前 database 不允许任何 `ALTER ROLE/DATABASE SET` 覆盖，
会话必须保持触发器、row security 与 large-object 权限语义的安全默认值。

migration role 的默认权限必须显式撤销 `PUBLIC` 对新函数的 `EXECUTE` 和对新类型的
`USAGE`；当前 database 还必须撤销 `PUBLIC` 对 `lo_creat`、`lo_create` 与
`lo_from_bytea` 的 `EXECUTE`，使 runtime 不能在两次门禁之间创建 large object。
同一 PostgreSQL cluster 内所有其他可连接 database 也必须撤销 `PUBLIC` 的
`CONNECT/TEMPORARY`，确保 migration/runtime 两个受管角色对其他
`datallowconn` database 均无有效 `CONNECT`，再只向各自应用角色显式授权；新
Compose volume 会通过
init SQL 处理 `postgres/template1`，已有 volume 必须由 DBA 在 cutover checkpoint
中执行等价操作。
bootstrap 在 DDL 前后都会验证已知业务/Checkpointer 列确实绑定
`pg_catalog` 类型，并校验锁定的 `langgraph-checkpoint-postgres==3.0.4` migration
manifest；依赖内容漂移会在第一条 DDL 前失败。所有校验失败只会返回通用错误，
不会回退 admin 凭据或自动修改权限。

在 0003 建立精确清单前，目标 schema 不允许用户 trigger、非安全 view 标准
`_RETURN` 之外的 rewrite rule、`SECURITY DEFINER` routine 或启用的 event
trigger，也不允许 foreign key 或表继承/分区关系，避免内部 constraint trigger、
级联动作和子表权限绕过对象级 ACL。runtime 可访问的 ordinary view 必须设置
`security_invoker=true`；历史 owner-rights view 只有在 runtime 完全没有表级/列级
权限时才作为只读迁移桥保留，避免隐式执行路径获得 migration-owner 权限。

或者本地分进程启动：
-   运行主同步服务: `python -m src.exchange_service`
-   运行飞书监听服务: `python -m src.lark_service`

## 目录结构说明

-   `src/graph`: 定义 LangGraph 状态机和工作流逻辑。
-   `src/nodes`: 工作流中的各个功能节点（分类、检索、草稿、发送）。
-   `src/utils`: 核心组件（Exchange 客户端、飞书应用、数据库管理器、RAG 引擎）。
-   `src/scripts`: 辅助脚本（模型检查、Exchange API 测试等）。

## 运维建议

-   **监控日志**: 核心日志输出在控制台，可通过 Docker logs 查看。
-   **数据库管理**: Postgres 存储了所有的 LangGraph checkpoints，支持在服务重启后恢复处理中的任务。
-   **模型切换**: 推荐使用 Gemini 3 Flash 以获得最佳的性价比平衡。

---
**Last Updated**: 2026-01-30
