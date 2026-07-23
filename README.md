# AI Email Assistant (Enterprise Edition)

这是一个为企业环境设计的 AI 邮件处理系统。它通过可配置 Provider 调用模型，使用 Qdrant 提供历史检索，并由 LangGraph 编排可持久化的 Agent 工作流。系统通过飞书 (Lark) 与用户进行审批互动。

## 核心功能

1.  **可靠收件**: 以 Exchange Webhook 为主通道，先写入 PostgreSQL Durable Inbox，再异步处理；当前不启用轮询。
2.  **智能分类**: 使用 LLM 识别邮件意图，判断是否需要回复及其紧急程度。
3.  **历史检索**: 将经过内容投影的邮件文本写入 Qdrant；图片只按需分析，Base64、data URI 和附件字节不会进入向量库。
4.  **人机协作 (Human-in-the-Loop)**:
    -   系统生成回复草稿并通过飞书交互式卡片推送给用户。
    -   用户可以点击“通过”、“拒绝”或“修改”建议。
5.  **自动回复 & 归档**: 审批通过后，系统自动发送邮件并将回复内容索引回 Qdrant。

## 系统架构

-   **编排层**: `LangGraph` (基于 Postgres 的状态持久化)。
-   **向量数据库**: `Qdrant` (存储邮件嵌入向量，支持语义搜索)。
-   **推理引擎**: 通过 `src/providers/` 为不同角色选择模型和 Adapter。
-   **集成端口**:
    -   `Exchange API`: 自定义适配器，处理邮件收发和状态更新。
    -   `Lark (飞书)`: 采用 WebSocket (长连接) 监听回调，HTTP API 发送交互卡片。

## 单进程服务

当前精简上线形态只有一个 **`ai-assistant-service`**：FastAPI 接收 Exchange
Webhook，同一进程内的单消费者 Durable Inbox Worker 复用现有 LangGraph、
ContentStore 和飞书 WebSocket 完成处理与审批。Webhook 请求只负责可靠落库，业务
处理失败不会丢失已接收事件。该形态面向单 Exchange 账户、单飞书账户和全新初始化；
不包含 Shadow、Sync 轮询、历史数据迁移、多账户或多实例滚动切换。

## 快速开始

### 1. 环境准备

安装依赖（MacOS 环境）:
```bash
python -m venv .venv
source .venv/bin/activate
uv sync --frozen
```

### 2. 配置环境变量

复制模板并只填写其中列出的 17 项：

```bash
cp .env.example .env
# 编辑 .env 后生成内部配置
.venv/bin/python scripts/configure_deployment.py
.venv/bin/python scripts/deploy_system.py check
```

`.env` 只包含服务地址、Exchange、飞书、LLM 和 Embedding 接入参数。数据库四角色、
Metrics Token、ContentStore Key、迁移/维护 DSN、运行限额和部署状态均由配置工具生成到
忽略版本控制的 `secrets/`，权限固定为 `0600`。已有旧版 `.env` 执行同一命令会原地
迁移，并把正在使用的私网 TLS、归档目录和飞书云盘设置保存到自动管理的内部配置；
不会输出任何密钥值。

生产 preflight 仍会检查：

- Exchange HTTPS URL、API key、账户 ID，以及与服务端完全一致且至少 16 字节的
  Webhook secret；
- 飞书 `LARK_APP_ID`、至少 16 字节的 `LARK_APP_SECRET`/`LARK_ENCRYPT_KEY`、
  `LARK_CHAT_ID`，以及至少一个真实且非 `*` 的 `LARK_ALLOWED_OPEN_IDS`；
- 真实模型及 Embedding 凭据、地址和模型名；
- 非占位、无用户信息的真实 HTTPS `EXTERNAL_URL`。本机冒烟可以用 HTTP curl 访问
  `127.0.0.1`，但 Compose 内这个生产配置仍必须是未来受控的 HTTPS 回调 origin；尚未
  配置反向代理时，不能宣称外部 Webhook 已经上线。

Exchange 地址必须通过证书校验。若服务实际通过私网 IP 访问、证书却只覆盖 DNS
名称，部署工具会复用已迁移的内部 DNS/IP/CA 覆盖；不要通过关闭 TLS 校验解决证书
问题。新环境的特殊私网 TLS 初始化见 [`deploy/README.md`](deploy/README.md)。

反方向的 Exchange Webhook 入口同样有外部前置：本 Compose 只提供容器内 HTTP，
不包含公网 TLS 终止。正式环境必须由现有反向代理/入口提供真实可达的 HTTPS
`EXTERNAL_URL`，并把 `/webhooks/exchange` 路由到应用端口；本机直接访问 HTTP 只用于
隔离冒烟。当前项目验证完成后，再在 Exchange 扩展中以 `is_active=false` 创建/更新
单账户、仅 `NewMailEvent` 的订阅。两端必须使用完全相同、至少 16 字节的高熵
`EXCHANGE_WEBHOOK_SECRET`；先触发扩展的 TestEvent，不能只看扩展接口外层 HTTP 200，
还必须确认返回的目标 `data.status_code=200`，且 `data.response_body` 可解析为精确的
`{"status":"ok","test":true}`。这些条件全部满足后才把订阅设为 active，再用一封
真实测试邮件验证“Webhook 202 → Durable Inbox → 飞书卡片 → 用户点击 → 数据库状态变化”。

没有公网 DNS、但已有覆盖公司子域名的证书时，可启用
`docker-compose.webhook-tls.yml`。证书覆盖的内部主机名只需在 Exchange Gateway 的
`extra_hosts` 中固定解析到本机私网 IP，不必关闭证书校验。把完整证书链和私钥分别保存为
忽略版本控制且权限为 `0400` 或 `0600` 的 `secrets/webhook_tls_fullchain.pem` 与
`secrets/webhook_tls_key.pem`。部署工具会在两者同时存在时自动追加 TLS 覆盖并沿用同一
Compose project；只存在其中一个、文件为空、过大、权限过宽或为符号链接都会拒绝部署：

```bash
.venv/bin/python scripts/deploy_system.py check
.venv/bin/python scripts/deploy_system.py redeploy
```

默认入口是 `8443`，只暴露 `/webhooks/exchange`、`/ready` 和自身健康检查；正式验收必须
使用证书覆盖的主机名完成 TLS 校验，不能使用 `curl -k`。

真实 Inbox opaque ID 从 Exchange 的 `emails/folders/all` 接口读取，并写入本次 policy
manifest；不能用显示名 `INBOX` 代替。私网 TLS 场景可在镜像构建后，使用同一个
Compose project、TLS overlay 和应用镜像执行一个不传 `--build` 的 `--no-deps` one-shot；它从
容器环境读取 API key，不把密钥放入命令行，并利用 overlay 的 SNI/DNS/CA 配置：

```bash
umask 077
docker compose -p "$PROJECT_NAME" \
  -f docker-compose.yml -f docker-compose.exchange-tls.yml \
  run --rm --no-deps --entrypoint python ai-assistant-service -c '
import asyncio
from src.config import get_settings
from src.utils.exchange_api import ExchangeClient
async def main():
    settings = get_settings()
    client = ExchangeClient(settings)
    try:
        folders = await client.get_all_folders(force_refresh=True)
        expected = {name.strip().casefold()
                    for name in settings.EXCHANGE_FOLDERS_FULL.split(",")
                    if name.strip()}
        matches = [folder_id for folder_id, name in folders.items()
                   if name.strip().casefold() in expected]
        if len(matches) != 1:
            raise SystemExit("inbox_folder_not_unique")
        print(matches[0])
    finally:
        await client.close()
asyncio.run(main())
' > "/tmp/ai-exchange-inbox-$PROJECT_NAME"
test -s "/tmp/ai-exchange-inbox-$PROJECT_NAME"
```

输出文件只用于随后生成本次 manifest，不能提交。若 Exchange 域名本身正常解析且证书链
完整，则去掉 TLS overlay 参数。TestEvent 只证明签名和可达性，不会写 Durable Inbox，
因此不能替代后两项验收。

### 3. 运行系统

已有系统的完整重建和重启只需要：

```bash
.venv/bin/python scripts/deploy_system.py redeploy
```

命令会校验 17 项用户配置与所有内部 secret，构建一个新的统一应用镜像，在不删除
PostgreSQL、Qdrant 和 ContentStore 数据卷的前提下重建应用及可选 HTTPS 入口，并等待
`/ready` 同时返回 `status=ready` 与 `processing=active`。

以下内容仅适用于全新数据库的首次生产初始化；数据库 provisioning、migration 与
Durable Inbox policy 仍是独立的安全门禁，详见 [`deploy/README.md`](deploy/README.md)。

只从已经提交且干净的当前 HEAD 部署。先严格执行
[`deploy/README.md`](deploy/README.md) 第 1 节：完整测试、显式构建当前镜像并记录
image ID，不能复用旧镜像。为本次部署选择一个从未使用过的 Compose project name；
以下每条 Compose 命令必须使用同一个 project，避免意外复用默认项目的 PostgreSQL、
Qdrant 或 ContentStore volume。随后执行：

```bash
PROJECT_NAME="ai-exchange-phase4-20260717-1"

# 两条都必须为空；非空就换一个全新 project name，不删除或接管现有资源。
test -z "$(docker ps -aq --filter label=com.docker.compose.project="$PROJECT_NAME")"
test -z "$(docker volume ls -q --filter label=com.docker.compose.project="$PROJECT_NAME")"

# 只支持本 Compose 创建的全新、专用 PostgreSQL volume；禁止共享/外部集群。
# 集群除 POSTGRES_DB、postgres、template0、template1 外存在任何数据库都会拒绝。
docker compose -p "$PROJECT_NAME" up -d postgres qdrant

docker compose -p "$PROJECT_NAME" --profile database-provision run --rm database-provision
docker compose -p "$PROJECT_NAME" --profile migration run --rm database-bootstrap
```

从 Exchange 读取本账户真实 Inbox opaque folder ID 后，按
[`deploy/README.md`](deploy/README.md) 第 2–3 节生成
`deploy/generated-<BUILD_ID>/POLICY.json` 与 `CONTRACT.json`，先 dry-run，再把该
精确子目录只读挂载给 `ingestion-maintenance initialize`。不要挂载整个 `deploy/`，
不要手写 hash，也不要引用仓库中不存在的根级 `deploy/POLICY.json`。

初始化成功并配置真实 Exchange、飞书和模型凭据后，运行时会固定启用 Durable Inbox，
并保持 Shadow 与 Sync 关闭；不得对应用服务执行 `--scale`。启动和就绪验证由
`scripts/deploy_system.py redeploy` 完成。

常规 `docker compose -p "$PROJECT_NAME" up -d` 不会启动带 profile 的 bootstrap 或维护容器，也不会
挂载它们的私有文件。`database-provision` 是唯一接收 admin DSN 和四个角色密码
文件的容器，并且只接受空数据库；成功后可安全重试，但不能用于历史库改造。
`database-bootstrap` 只接收 migration DSN，以及四个角色名
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
`datallowconn` database 均无有效 `CONNECT`，再只向四个受管角色显式授权。新 Compose
volume 会通过 init SQL 处理 `postgres/template1`。provisioner 会拒绝额外 database、
业务对象和不完整的受管角色集合，但无法证明宿主 volume 从未被使用；全新 project/volume
仍由上述部署前检查保证。一次成功 provision 后可以在同一专用新集群中安全重试。
bootstrap 在 DDL 前后都会验证已知业务/Checkpointer 列确实绑定
`pg_catalog` 类型，并校验锁定的 `langgraph-checkpoint-postgres==3.0.4` migration
manifest；依赖内容漂移会在第一条 DDL 前失败。所有校验失败只会返回通用错误，
不会回退 admin 凭据或自动修改权限。

0003 之后只允许版本化访问清单中逐项声明的 foreign key、constraint/user trigger
及其函数；任何额外 trigger、rewrite rule、`SECURITY DEFINER` routine、启用的
event trigger 或继承/分区关系都会使门禁失败。runtime 可访问的 ordinary view
必须设置 `security_invoker=true`，避免隐式执行路径获得 migration-owner 权限；本次
greenfield 部署不创建或保留任何历史迁移桥。

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
docker compose -p "$PROJECT_NAME" --profile checkpoint-maintenance run --rm checkpoint-maintenance \
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
docker compose -p "$PROJECT_NAME" --profile checkpoint-maintenance-execute run --rm \
  --volume /absolute/path/backup-receipt.json:/run/backup-receipt.json:ro \
  checkpoint-maintenance-execute execute \
  --plan-id <PLAN_ID> --confirm-plan-id <PLAN_ID> \
  --backup-id <BACKUP_ID> \
  --backup-receipt /run/backup-receipt.json \
  --operator-attests-service-quiesced --limit 100
```

这些命令仅描述手工 checkpoint 维护入口。精简上线只允许显式打开
`DURABLE_INBOX_ENABLED=true`；`INGESTION_SHADOW_ENABLED` 与
`SYNC_RECONCILIATION_ENABLED` 必须保持 `false`。这不是原六阶段方案中的完整
cutover/production-ready 声明，而是面向可清空测试数据、单账户、单进程的新系统启动。

本地启动同一个应用：`python -m src.main`。旧的轮询/分进程入口不是当前部署拓扑。

## 目录结构说明

-   `src/graph`: 定义 LangGraph 状态机和工作流逻辑。
-   `src/nodes`: 工作流中的各个功能节点（分类、检索、草稿、发送）。
-   `src/ingestion`: Durable Inbox、租约、策略和唯一运行时。
-   `src/router` 与 `skills_registry`: Tiered Router 和生产 Skill。
-   `src/utils`: Exchange、飞书、数据库与检索实现。
-   `scripts`: 部署、维护、PST 导入和 Skill Discovery 入口。

## 运维建议

-   **监控日志**: 核心日志输出在控制台，可通过 Docker logs 查看。
-   **数据库管理**: Postgres 存储了所有的 LangGraph checkpoints，支持在服务重启后恢复处理中的任务。
-   **模型切换**: 在 Provider 配置中按角色选择模型，不让供应商细节进入业务 module。

---
**Last Updated**: 2026-07-22
