# 首次部署操作说明

此目录只保存操作文档。真实的发布证据、`POLICY.json`、`CONTRACT.json`、DSN 和密钥都属于本地受控数据，不可提交。当前系统只支持为**空数据库**创建 polling-only 基线；不提供历史数据库迁移或 Webhook 回滚路径。

## 发布前条件

1. 当前 Git HEAD 已提交、工作树干净，且已通过 `.venv/bin/ruff check src/ tests/` 与 `.venv/bin/python -m pytest -q`。
2. 已按根目录 [`README.md`](../README.md) 生成 `.env` 和 `secrets/`。首次部署使用新 Compose project；已知项目只有在明确授权永久清空后才能进入 `greenfield-reset`。
3. 已构建当前 HEAD 的 `ai-assistant-service` 镜像。不要复用未知镜像，也不要在未核对 Compose 标签时删除任何卷。

## 发布证据

`scripts/prepare_ingestion_manifests.py` 需要一份本地 JSON 证据。它不会自行执行或验证命令；操作员只能在检查和构建成功后记录结果。

```json
{
  "schema_version": 1,
  "build_id": "greenfield-<git-short-sha>",
  "source_revision": "<full-git-head-sha>",
  "checks": [
    {
      "name": "tests",
      "command": ".venv/bin/python -m pytest -q",
      "exit_code": 0
    },
    {
      "name": "lint",
      "command": ".venv/bin/ruff check src/ tests/",
      "exit_code": 0
    }
  ],
  "artifacts": [
    {
      "name": "ai-assistant-service",
      "sha256": "<64-lowercase-hex-image-id-without-sha256-prefix>"
    }
  ],
  "accepted_residual_risks": [
    "Unknown external-boundary outcomes are conservatively routed to manual review"
  ]
}
```

可用以下命令取得所需标识，写入位于仓库外或受忽略路径的 JSON：

```bash
git rev-parse HEAD
docker image inspect --format '{{.Id}}' ai-exchange:local | sed 's/^sha256://'
```

`source_revision` 必须精确等于当前 HEAD；`build_id` 必须精确等于 manifest 生成命令的值；所有 `checks[].exit_code` 必须为 `0`。生成器拒绝脏工作树、重复 JSON key、手写 manifest hash 或非 `INBOX` scope。

## 空库初始化顺序

以根 README 中的 `COMPOSE` 数组为准，依次执行：

```bash
"${COMPOSE[@]}" config --quiet
"${COMPOSE[@]}" build ai-assistant-service
"${COMPOSE[@]}" up -d postgres qdrant
"${COMPOSE[@]}" --profile database-provision run --rm database-provision
"${COMPOSE[@]}" --profile migration run --rm database-bootstrap
```

数据库容器启动不等于应用数据库可用。`database-provision` 创建隔离角色，`database-bootstrap` 应用唯一的 polling 基线并验证数据库契约。两者都失败关闭；不要在旧数据库上重试。

随后对同一账户、同一构建只生成一次 manifest。先用 `--dry-run` 审阅，再不带该参数初始化：

```bash
"${COMPOSE[@]}" --profile ingestion-maintenance run --rm \
  --volume "$PWD/$OUTPUT_DIR:/run/manifest:ro" \
  ingestion-maintenance initialize \
  --account-id "$ACCOUNT_ID" \
  --policy-file /run/manifest/POLICY.json \
  --contract-file /run/manifest/CONTRACT.json \
  --actor <operator-id> \
  --reason initial-greenfield-deployment \
  --idempotency-key "$BUILD_ID" \
  --dry-run
```

去掉 `--dry-run` 才会写入运行时授权。成功后再运行
`scripts/deploy_system.py redeploy --project-name "$PROJECT_NAME"`，并检查 `/ready`
的 `status=ready` 与 `processing=active`。如果 Exchange 指向本机或非 HTTPS 的受控开发
地址，必须显式改用
`scripts/deploy_system.py redeploy --development --project-name "$PROJECT_NAME"`；
该选项仍只使用主 `docker-compose.yml`，仅启用本地绑定和
`operations-console` profile，不降低生产 TLS 校验。

## 私网 Exchange TLS

若 Exchange API 使用私网 IP、但证书签发给 DNS 名称，保持
`EXCHANGE_SSL_VERIFY=true`。在 `.env` 的旧版迁移值或 `secrets/deployment.env` 中提供
完整的 `EXCHANGE_TLS_HOSTNAME`、`EXCHANGE_TLS_IP` 与 `EXCHANGE_CA_FILE_HOST` 三项。
主 `docker-compose.yml` 会直接加载 DNS、SNI 和只读 CA 挂载；不再需要额外的 Compose
TLS overlay。不要通过关闭证书校验、把密钥放入命令行或创建入站 Webhook 来绕开 TLS
问题。

## 后续控制与旧资源

运行时控制只能使用 `ingestion-maintenance` 的 `status`、`pause`、`resume-ingress` 和受审计的 `requeue` 子命令。它们不用于数据库升级。

在新 project 经过真实轮询、审批和外部动作验证后，才可停止旧 project。需要永久重建一个已知项目时，优先使用 `scripts/deploy_system.py greenfield-reset`：它先构建镜像并按 Compose project/service/volume 标签验证精确边界，再删除该项目的容器和命名卷。保留 `secrets/`，不要对共享卷、未核对资源或错误项目名使用 `down --volumes`。
