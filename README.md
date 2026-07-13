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
使用生产 Compose 时，复制 `.env.example` 到 `.env`。`.env` 只保存 Compose
控制面的普通配置和 runtime/admin 凭据；migration、checkpoint auditor 与
checkpoint execute 的完整 DSN 必须分别写入 `MIGRATION_DATABASE_URL_FILE`、
`CHECKPOINT_AUDITOR_DATABASE_URL_FILE` 和
`CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE` 指向的 0400/0600 文件，不能写回
`.env`。

migration DSN 的
`options` 必须精确设置为 `-csearch_path=<目标 schema>`。不要显式把
`pg_catalog` 放到目标 schema 后面：当它未显式列出时，PostgreSQL 会先解析
系统目录，同时仍把未限定的新对象创建到目标 schema，从而避免同名类型劫持
DDL。auditor 与 maintenance DSN 必须分别以各自角色登录，且 `options` 精确设置为
`-csearch_path=pg_catalog,<目标 schema>`；它们只能用于各自独立维护容器。四个
数据库身份必须互不相同。不要把 admin、migration、auditor 或 maintenance 凭据
放入运行时配置。随后填入：
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

常规 `docker compose up -d` 不会启动带 profile 的 bootstrap 或维护容器，也不会
挂载它们的私有文件。`database-bootstrap` 只接收 migration DSN，以及四个角色名
用于迁移和授权；它不接收 maintenance DSN/receipt key。`ai-assistant-service` 只接收
runtime 凭据和其余三个非秘密角色名，用于启动门禁；它不接收任何 migration 或
maintenance secret。

数据库门禁会在任何 DDL、应用上下文或外部 worker 启动前验证
migration/runtime/maintenance/auditor 均为独立 `LOGIN NOINHERIT` 非特权角色，四者没有
任何授予型 membership，目标 database/schema/对象均由 migration role 持有，且
除 migration owner 外无人拥有目标 schema 的 `CREATE`。runtime 还必须没有
database `CREATE/TEMP` 和 schema `CREATE`；maintenance 仅保留 checkpoint 清理
所需的精确只读/删除权限，不能访问 durable-ingestion 业务表。当前 database、目标
schema 与目标对象的显式 ACL 必须符合四角色精确清单；auditor 只允许目标 database
的非 grantable `CONNECT`、目标 schema 的非 grantable `USAGE`，以及
`alembic_version(version_num)`、`checkpoint_migrations(v)`、
`emails_log(id,status,updated_at)` 与三张 checkpoint 关系中计划扫描所需列的
非 grantable 直接列级 `SELECT`；未列出的现有列和未来新增列默认不可读，
auditor 不得拥有任何 membership。
`PUBLIC` 或其他角色不能读取目标数据，也不能借助其他用户 schema、large object、FDW/server、系统目录
新增 ACL 或 `SECURITY DEFINER` 间接提权。四个受管角色和当前 database 不允许
任何 `ALTER ROLE/DATABASE SET` 覆盖，
会话必须保持触发器、row security 与 large-object 权限语义的安全默认值。

migration role 的默认权限必须显式撤销 `PUBLIC` 对新函数的 `EXECUTE` 和对新类型的
`USAGE`；当前 database 还必须撤销 `PUBLIC` 对 `lo_creat`、`lo_create` 与
`lo_from_bytea` 的 `EXECUTE`，使 runtime 不能在两次门禁之间创建 large object。
同一 PostgreSQL cluster 内所有其他可连接 database 也必须撤销 `PUBLIC` 的
`CONNECT/TEMPORARY`，确保 migration/runtime/maintenance/auditor 四个受管角色对其他
`datallowconn` database 均无有效 `CONNECT`，再只向四个受管角色显式授权；新
Compose volume 会通过
init SQL 处理 `postgres/template1`，已有 volume 必须由 DBA 在 cutover checkpoint
中执行等价操作。
bootstrap 在 DDL 前后都会验证已知业务/Checkpointer 列确实绑定
`pg_catalog` 类型，并校验锁定的 `langgraph-checkpoint-postgres==3.0.4` migration
manifest；依赖内容漂移会在第一条 DDL 前失败。所有校验失败只会返回通用错误，
不会回退 admin 凭据或自动修改权限。

0003 之后只允许版本化访问清单中逐项声明的 foreign key、constraint/user trigger
及其函数；任何额外 trigger、rewrite rule、`SECURITY DEFINER` routine、启用的
event trigger 或继承/分区关系都会使门禁失败。runtime 可访问的 ordinary view
必须设置 `security_invoker=true`；历史 owner-rights view 只有在 runtime 完全
没有表级/列级权限时才作为只读迁移桥保留，避免隐式执行路径获得
migration-owner 权限。

### 4. 手工 checkpoint maintenance

维护 profile 不属于应用启动流程。先确认 live 服务仍未被授权迁移或切换；只有在
独立 DBA checkpoint 已核对备份、四角色 ACL、只读 auditor、当前 Alembic/Checkpointer revision
和 disposable PostgreSQL 证据后，才可在获批的目标库上执行以下命令。

先由 DBA 建立独立 auditor `LOGIN NOINHERIT`：不得拥有任何角色成员关系，只能直接
获得目标 database 的 `CONNECT`、目标 schema 的 `USAGE` 与上述精确列级 `SELECT`，
不得拥有对象、grant option 或任何写权限。plan 使用 auditor DSN，execute
使用 maintenance DSN；两者不能复用。准备三个权限精确为 0400 或 0600 的受控文件：
auditor DSN、maintenance DSN，以及单行 base64 编码的 Ed25519 原始公钥（解码后必须
恰好 32 bytes）。只读计划容器只挂载 auditor DSN；执行容器只持验证公钥，外部备份
系统独占私钥 seed，因此维护容器不能签发或伪造 v2 receipt。计划制品保存在独立
named volume 中，供后续精确确认使用：

```bash
docker compose --profile checkpoint-maintenance run --rm checkpoint-maintenance \
  plan --older-than-hours 24 --limit 100
```

删除不会自动发生。`--operator-attests-service-quiesced` 是操作员对“应用已经停写”的
显式声明，不是系统独立验证出的停写证明；执行前仍必须从编排平台或 DBA 侧核实所有
runtime 实例已经停止。`execute` 还必须同时满足：备份 receipt 已由外部流程签名、
`plan-id` 双重确认一致、批次上限已审批。receipt 文件必须是 0600 普通文件。执行
容器固定以 UID 0 运行，所以 bind mount 后该文件在容器内也必须由 UID 0 拥有；如
宿主文件不是 root:root，先复制到受控的 root-owned 目录并设为 0600。不要把
receipt、私钥、DSN 或邮件内容输出到终端/日志。

每个 runtime 实例使用双层数据库栅栏。第一层是独立 dedicated connection 持有同一
maintenance advisory key 的 session-level 共享锁，并由每次 checkpoint mutation 在执行
SQL 前精确核对该连接仍拥有锁；第二层由 LangGraph connection pool 的 configure callback
让每个 pool backend 在完整连接生命周期内持有同一共享锁。fenced saver 会先取得已配置的
pool connection，再核对 dedicated fence，最后才把 cursor 交给 `aput`、`aput_writes` 或
`adelete_thread`。因此 dedicated backend 若在核对后、写入前丢失，当前 pool connection
仍阻止 maintenance 独占锁；若 dedicated 与全部旧 pool backend 都丢失、maintenance 已完成，
恢复后的 pool backend 会重新取得共享锁，但 saver 会因 dedicated 核对失败而在任何
checkpoint SQL 前硬退出，不能在清理后重新写回。

共享锁连接或锁所有权失效会先关闭 Lark 异步 intake，再立即硬退出进程；正常停机则依次
停止调度与 Exchange Worker、收集在途 Lark 动作、关闭数据库连接池，最后才释放 dedicated
共享锁。维护进程取得独占锁后的固定 20 秒等待仍保留为 defense-in-depth，用于扩大故障检测
与运维响应裕量，但它不是互斥安全性的证明；正确性来自上述双层 session lock 和写前核对。
稳定期结束后会重新核对数据库身份、schema/checkpointer revision 与 plan 有效期，之后才
开放删除 session。该等待不能由环境变量缩短。`scripts/reprocess_email.py` 的三种恢复用法
也会先通过 runtime security 与四角色/schema/revision preflight，再启动 dedicated fence、
绑定 write guard 并使用同一 fenced saver；退出时先关闭 context/pool，之后才释放 dedicated
fence。context 关闭有固定 10 秒墙钟上限；超时或异常会直接硬退出且绝不释放 fence，避免
吞取消的连接 close 让脚本无限挂起或错误开放维护窗口。其他未绑定 write guard 的
`AppContext.setup_async()` 会在任何 I/O 前失败关闭。该栅栏仍不能识别旧版本、绕过协议的
外部客户端或非 checkpoint 写入，因此人工停写核验始终是必需条件。

```bash
docker compose --profile checkpoint-maintenance-execute run --rm \
  --volume /absolute/path/backup-receipt.json:/run/backup-receipt.json:ro \
  checkpoint-maintenance-execute execute \
  --plan-id <PLAN_ID> --confirm-plan-id <PLAN_ID> \
  --backup-id <BACKUP_ID> \
  --backup-receipt /run/backup-receipt.json \
  --operator-attests-service-quiesced --limit 100
```

这些命令仅描述手工维护入口，不构成当前生产激活许可；Phase 2 的 durable Inbox、
Shadow 和 Sync 开关仍保持 `false`，直到对应验收门禁和人工切换记录齐备。

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
