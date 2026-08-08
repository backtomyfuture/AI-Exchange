# AI Exchange

AI Exchange 是一个单账户、单进程的邮件助手：它通过 Exchange `sync_state` 轮询发现变化，先写入 PostgreSQL Durable Inbox，再由 LangGraph 处理、生成草稿并通过飞书审批卡片完成外部动作。Qdrant 只保存经过内容投影的检索数据。

## 当前支持边界

- 唯一入站方式是轮询；没有 HTTP Webhook 路由、Webhook 密钥或订阅。
- 数据库只支持**全新初始化**。不要把旧版本数据库、旧 Compose volume 或旧 Alembic 历史接入当前版本。
- 首次轮询建立 `sync_state` 基线并忽略已有历史；之后才处理增量变化。
- 标准部署固定为一个应用进程、一个 Exchange 账户和一个飞书账户；不要扩容应用服务。
- Tier1 v1 规则资产已保留，但尚未接入运行时，不能把它当作已启用功能。
- 偏好/风格记忆学习默认关闭。标准 Compose 显式设置 `MEMORY_LEARNING_ENABLED=false`；只有经过单独评审的显式配置才可开启。

## 本地开发

需要 Docker Desktop 或 OrbStack、Git 与 `uv`。macOS 可执行：

```bash
brew install uv
uv python install 3.12
uv venv --python 3.12 .venv
uv sync --frozen --group dev
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
```

Docker 镜像固定使用 Python 3.12。不要把本机已有的其他 Python 版本当作部署基线。

## 从空环境首次部署

以下流程会创建一个新的 Compose project 和新的命名卷。它不会升级、接管或删除任何旧部署。

1. 从模板建立用户配置，只填写 16 个集成/模型字段：

```bash
cp .env.example .env
# 编辑 .env；不要把生成的 secrets/ 提交到仓库
.venv/bin/python scripts/configure_deployment.py \
  --project-name ai-exchange-greenfield
```

HTTPS Exchange 的生产部署运行：

```bash
.venv/bin/python scripts/deploy_system.py check \
  --project-name ai-exchange-greenfield
```

本机或受控开发环境中的 HTTP/私网 Exchange 则改为运行：

```bash
.venv/bin/python scripts/deploy_system.py check \
  --development --project-name ai-exchange-greenfield
```

配置工具会将数据库角色、DSN、指标 token、内容加密密钥和 Compose project 名生成到受忽略的 `secrets/`；不会输出密钥。若配置了私网 Exchange TLS，请按 [`deploy/README.md`](deploy/README.md) 加上 TLS overlay。

2. 只在已提交且工作树干净的 revision 上发布。构建镜像并初始化全新 PostgreSQL：

```bash
PROJECT_NAME=ai-exchange-greenfield
COMPOSE=(docker compose --env-file .env --project-name "$PROJECT_NAME")
if [[ -f secrets/deployment.env ]]; then
  COMPOSE=(docker compose --env-file secrets/deployment.env --env-file .env --project-name "$PROJECT_NAME")
fi

"${COMPOSE[@]}" config --quiet
"${COMPOSE[@]}" build ai-assistant-service
"${COMPOSE[@]}" up -d postgres qdrant
"${COMPOSE[@]}" --profile database-provision run --rm database-provision
"${COMPOSE[@]}" --profile migration run --rm database-bootstrap
```

`database-provision` 和 `database-bootstrap` 只接受空应用数据库；失败时不要用旧库重试。换一个新的 project/volume，修复原因后从头执行。

3. 为这次构建生成发布证据和轮询初始化 manifest。发布证据的完整格式、镜像 digest 获取方法和 TLS 变体见 [`deploy/README.md`](deploy/README.md)。生成器只接受当前干净 Git HEAD、真实账户 ID 与 `INBOX` scope：

```bash
BUILD_ID=greenfield-$(git rev-parse --short HEAD)
OUTPUT_DIR="deploy/generated-$BUILD_ID"
ACCOUNT_ID=123456 # 替换为 .env 中的数字 Exchange 账户 ID

.venv/bin/python scripts/prepare_ingestion_manifests.py \
  --account-id "$ACCOUNT_ID" \
  --sync-folder INBOX \
  --build-id "$BUILD_ID" \
  --release-evidence-file /absolute/path/release-evidence.json \
  --output-dir "$OUTPUT_DIR"

"${COMPOSE[@]}" --profile ingestion-maintenance run --rm \
  --volume "$PWD/$OUTPUT_DIR:/run/manifest:ro" \
  ingestion-maintenance initialize \
  --account-id "$ACCOUNT_ID" \
  --policy-file /run/manifest/POLICY.json \
  --contract-file /run/manifest/CONTRACT.json \
  --actor <operator-id> \
  --reason initial-greenfield-deployment \
  --idempotency-key "$BUILD_ID"
```

可先在最后一条命令追加 `--dry-run` 检查 manifest；dry-run 不写数据库。生成目录包含发布证据派生信息，保持在忽略目录中，不要提交或复用到另一账户。

4. 启动并验证：

HTTPS Exchange 的生产部署运行：

```bash
.venv/bin/python scripts/deploy_system.py redeploy \
  --project-name "$PROJECT_NAME"
```

若当前 Exchange 地址是本机或非 HTTPS 地址，改为运行受控开发覆盖层：

```bash
.venv/bin/python scripts/deploy_system.py redeploy \
  --development --project-name "$PROJECT_NAME"
```

两种模式均用以下命令检查：

```bash
curl --fail http://127.0.0.1:8000/ready
```

`/ready` 必须同时报告 `status=ready` 与 `processing=active`。随后使用受保护的 `/queue`、`/metrics` 和一次真实的非破坏性邮件流验证轮询、Durable Inbox、分类和飞书卡片。健康接口本身不代表外部副作用已经验证。

## 切换与清理旧部署

新 project 完成上述启动与业务验证前，不要停止旧实例。两套实例不能同时对同一账户处于活跃轮询状态；切换窗口需要由操作员决定重复与遗漏风险。

确认新实例可用后，先停止旧实例，再仅针对已核对的旧 Compose project 执行 `docker compose --project-name <old-project> down --volumes`。这会删除该项目容器和命名卷；不要删除 `secrets/`，也不要对新 project 或任何共享 Docker 资源执行该命令。

## 常用运维入口

- `scripts/deploy_system.py check|redeploy`：验证配置、重建镜像并等待 `/ready`；本机/受控开发环境须显式加 `--development`，生产环境保持默认模式。
- `scripts/manage_ingestion.py status|pause|resume-ingress|requeue`：受限的运行时控制；每个写操作都需要 actor、reason 与 idempotency key。
- `scripts/checkpoint_cleanup.py`：独立的 checkpoint 维护流程，不属于应用启动流程。
- `src/router/tier1/`：已保留的未来 Tier1 v1 规则资产；未接入时不应修改默认路由。

详细的初始 manifest、私网 TLS 和 checkpoint 维护说明见 [`deploy/README.md`](deploy/README.md)。
