# AI Email Assistant (Enterprise Edition)

这是一个为企业环境设计的高级 AI 邮件处理系统。它利用 Gemini 3 Flash 进行智能推理，Qdrant 作为向量数据库（RAG），并使用 LangGraph 编排复杂的 Agent 工作流。系统通过飞书 (Lark) 与用户进行审批互动。

## 核心功能

1.  **可靠收件**: 以 Exchange Webhook 为主通道，先写入 PostgreSQL Durable Inbox，再异步处理；当前不启用轮询。
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

直接在本机运行 Python 时，复制 `.env.runtime.example` 到 `.env.runtime`；
使用生产 Compose 时，复制 `.env.example` 到 `.env`。`.env` 只保存 Compose
控制面的普通配置和 runtime/admin 凭据；migration、checkpoint auditor 与
checkpoint execute 的完整 DSN 必须分别写入 `MIGRATION_DATABASE_URL_FILE`、
`CHECKPOINT_AUDITOR_DATABASE_URL_FILE` 和
`CHECKPOINT_MAINTENANCE_DATABASE_URL_FILE` 指向的 0400/0600 文件，不能写回
`.env`。首次部署还要准备 `DATABASE_PROVISION_ADMIN_URL_FILE` 与四个
`POSTGRES_*_PASSWORD_FILE` 指向的 0400/0600 文件；这些文件只挂载到手动
provisioning 容器，不进入应用或 migration 容器。四个密码文件分别是对应角色的
唯一密码来源：runtime 文件必须与 `.env` 的 `POSTGRES_RUNTIME_PASSWORD` 一致，
migration、maintenance、auditor DSN 必须使用各自密码文件中的同一密码；admin URL
的密码必须与 `POSTGRES_ADMIN_PASSWORD` 一致。完整构建与初始化证据流程见
[`deploy/README.md`](deploy/README.md)。

migration DSN 的
`options` 必须精确设置为 `-csearch_path=<目标 schema>`。不要显式把
`pg_catalog` 放到目标 schema 后面：当它未显式列出时，PostgreSQL 会先解析
系统目录，同时仍把未限定的新对象创建到目标 schema，从而避免同名类型劫持
DDL。auditor 与 maintenance DSN 必须分别以各自角色登录，且 `options` 精确设置为
`-csearch_path=pg_catalog,<目标 schema>`；它们只能用于各自独立维护容器。四个
数据库身份必须互不相同。不要把 admin、migration、auditor 或 maintenance 凭据
放入运行时配置。随后完成生产 preflight：

- Exchange HTTPS URL、API key、账户 ID，以及与服务端完全一致且至少 16 字节的
  Webhook secret；
- 飞书 `LARK_APP_ID`、至少 16 字节的 `LARK_APP_SECRET`/`LARK_ENCRYPT_KEY`、
  `LARK_CHAT_ID`，以及至少一个真实且非 `*` 的 `LARK_ALLOWED_OPEN_IDS`；
- 至少 16 字节的 `METRICS_TOKEN`，和解码后严格 32 字节的 Base64
  `CONTENT_STORE_KEY`；
- 真实模型/Embedding 凭据与地址、四角色 PostgreSQL 配置，以及 Qdrant 配置；
- 非占位、无用户信息的真实 HTTPS `EXTERNAL_URL`。本机冒烟可以用 HTTP curl 访问
  `127.0.0.1`，但 Compose 内这个生产配置仍必须是未来受控的 HTTPS 回调 origin；尚未
  配置反向代理时，不能宣称外部 Webhook 已经上线。

Exchange 地址必须通过证书校验。若服务实际通过私网 IP 访问、证书却只覆盖 DNS
名称，不要把 `EXCHANGE_SSL_VERIFY` 设为 `false`。把 `EXCHANGE_API_URL` 改为证书
SAN 覆盖的主机名，并在 `.env` 设置 `EXCHANGE_TLS_HOSTNAME`、
`EXCHANGE_TLS_IP`、`EXCHANGE_CA_FILE_HOST`；最后一个变量指向只含证书/信任链、
不含私钥的可读 PEM 文件。如果服务端只发送叶子证书，单独挂载叶子证书不足以完成
链校验；应从证书 AIA 指向的官方 CA 来源取得并核验中间证书与根证书，组成私有临时
trust bundle，而不是关闭验证。此时所有 Compose 命令都追加 TLS 覆盖文件，例如：

```bash
docker compose -f docker-compose.yml -f docker-compose.exchange-tls.yml \
  config --quiet
```

覆盖层只给应用容器增加精确 DNS 到 IP 映射，并把 PEM 只读挂载到固定容器路径；
应用仍强制保持 TLS 验证。`EXCHANGE_TLS_HOSTNAME` 必须与 `EXCHANGE_API_URL` 的
host 完全一致且被证书覆盖。若 Exchange 地址本身已使用可解析且证书链完整的域名，
则不使用该覆盖层，并保持 `EXCHANGE_CA_FILE` 为空。

反方向的 Exchange Webhook 入口同样有外部前置：本 Compose 只提供容器内 HTTP，
不包含公网 TLS 终止。正式环境必须由现有反向代理/入口提供真实可达的 HTTPS
`EXTERNAL_URL`，并把 `/webhooks/exchange` 路由到应用端口；本机直接访问 HTTP 只用于
隔离冒烟。当前项目验证完成后，再在 Exchange 扩展中以 `is_active=false` 创建/更新
单账户、仅 `NewMailEvent` 的订阅。两端必须使用完全相同、至少 16 字节的高熵
`EXCHANGE_WEBHOOK_SECRET`；先触发扩展的 TestEvent，不能只看扩展接口外层 HTTP 200，
还必须确认返回的目标 `data.status_code=200`，且 `data.response_body` 可解析为精确的
`{"status":"ok","test":true}`。这些条件全部满足后才把订阅设为 active，再用一封
真实测试邮件验证“Webhook 202 → Durable Inbox → 飞书卡片 → 用户点击 → 数据库状态变化”。

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

初始化成功并配置真实 Exchange、飞书、模型与 ContentStore 凭据后，在 `.env` 中只把
`DURABLE_INBOX_ENABLED` 设为 `true`；Shadow 与 Sync 保持 `false`。
`INGESTION_INSTANCE_ID` 必须保持固定值 `ai-exchange-web`，且不得对应用服务执行
`--scale`。启动并验证：

```bash
docker compose -p "$PROJECT_NAME" up -d --no-build ai-assistant-service
.venv/bin/python - <<'PY'
import json
import os
import time
import urllib.request

url = f"http://127.0.0.1:{os.getenv('APP_PORT', '8000')}/ready"
deadline = time.monotonic() + 180
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            payload = json.load(response)
        if payload.get("status") == "ready" and payload.get("processing") == "active":
            break
    except Exception:
        pass
    time.sleep(2)
else:
    raise SystemExit("phase4_lite_readiness_timeout")
PY
```

响应必须同时包含 `"status":"ready"` 与 `"processing":"active"`；仅 HTTP 200 或
`processing=standby` 都不算成功。若上一节要求 Exchange TLS 覆盖层，上述每条 Compose
命令都使用 `-f docker-compose.yml -f docker-compose.exchange-tls.yml`。
若旧测试实例仍在并行运行，还必须先在新项目的 `.env` 选择未占用的 `APP_PORT`（隔离
冒烟建议 `18081`）；不要停止、删除或接管旧项目来释放端口。

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
-   `src/utils`: 核心组件（Exchange 客户端、飞书应用、数据库管理器、RAG 引擎）。
-   `src/scripts`: 辅助脚本（模型检查、Exchange API 测试等）。

## 运维建议

-   **监控日志**: 核心日志输出在控制台，可通过 Docker logs 查看。
-   **数据库管理**: Postgres 存储了所有的 LangGraph checkpoints，支持在服务重启后恢复处理中的任务。
-   **模型切换**: 推荐使用 Gemini 3 Flash 以获得最佳的性价比平衡。

---
**Last Updated**: 2026-07-17
