# Phase 4-Lite 初始化 manifest

本目录只保存操作说明；不要提交真实账户生成的 `POLICY.json`、`CONTRACT.json`、
release metadata、DSN 或其他凭据。生成器不接受手工输入的 manifest hash：schema 与
adapter hash 都绑定完整、干净的已提交 Git `HEAD` revision 和 tree，config hash 绑定
固定的单 Exchange/单飞书/单进程配置以及本次账户和 Inbox scope。

Release metadata 是操作员生成的精简发布记录，不是签名证明。生成器只校验其结构、
`source_revision`、`build_id`、自报退出码和 digest 格式；不会替操作员执行 `command`，
不会读取或验证 Docker image，也不会在运行时强绑定镜像。当前 Phase 4-Lite 的真实门禁
是按下述顺序先运行检查、显式构建当前 HEAD、记录 image ID，再初始化。镜像签名、运行时
attestation 和完整 cutover 属于已取消的 Phase 5/6，本阶段不增加这些系统。

Metadata 使用以下严格结构。`checks` 和 `artifacts` 均至少一项，所有 `exit_code` 必须
为 `0`；`source_revision` 必须等于当前 Git `HEAD`；`build_id` 必须与命令行一致。
`artifacts[].sha256` 保存不带 `sha256:` 前缀的 64 位小写 image ID；
`accepted_residual_risks` 必须至少记录当前 Exchange 扩展在“delivery 已落库但 Redis
入队失败”时缺少 pending scanner，以及外部边界未知结果会保守转人工复核这两项已知
风险；如现场还有其他已接受风险，也逐项追加。

```json
{
  "accepted_residual_risks": [
    "Exchange extension has no pending-delivery scanner after Redis enqueue failure",
    "Unknown external-boundary outcomes are conservatively routed to manual review"
  ],
  "artifacts": [
    {
      "name": "ai-assistant-service-image-id",
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  ],
  "build_id": "release-20260717-1",
  "checks": [
    {
      "command": ".venv/bin/python -m pytest -q",
      "exit_code": 0,
      "name": "full-test-suite"
    }
  ],
  "schema_version": 1,
  "source_revision": "0123456789abcdef0123456789abcdef01234567"
}
```

## 1. 检查并构建当前 HEAD

以下命令必须在仓库根目录执行。检查或构建失败就停止，不要把失败命令写成成功记录。
构建完成后不要再修改工作树；生成器会拒绝所有未忽略的 tracked、staged、untracked
变更，只允许 `.gitignore` 已忽略的本地制品。

```bash
BUILD_ID="release-20260717-1"
PROJECT_NAME="ai-exchange-phase4-20260717-1"
RELEASE_IMAGE="ai-exchange:phase4-lite-$BUILD_ID"

# PROJECT_NAME 是独立的 Compose-safe 小写 slug，不要直接拼接可能含点号/大写的 build id。
# 把 .env 的 AI_EXCHANGE_IMAGE 设置为 RELEASE_IMAGE 后，再验证解析结果。
CONFIGURED_IMAGE="$(docker compose -p "$PROJECT_NAME" config --format json | \
  .venv/bin/python -c 'import json,sys; print(json.load(sys.stdin)["services"]["ai-assistant-service"]["image"])')"
test "$CONFIGURED_IMAGE" = "$RELEASE_IMAGE"

test -z "$(git status --porcelain=v1 --untracked-files=all --ignore-submodules=none)"
HEAD_REVISION="$(git rev-parse --verify HEAD)"
.venv/bin/python -m pytest -q
.venv/bin/ruff check src/ tests/ scripts/prepare_ingestion_manifests.py
docker compose -p "$PROJECT_NAME" build --pull \
  database-provision database-bootstrap ingestion-maintenance ai-assistant-service

IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$RELEASE_IMAGE")"
printf 'HEAD=%s\nimage=%s\n' "$HEAD_REVISION" "$IMAGE_ID"
```

`RELEASE_IMAGE` 必须与 `.env` 中本次 `AI_EXCHANGE_IMAGE` 的精确 tag 一致；Compose 的全部
应用/one-shot 服务固定使用这一 tag。把实际执行的检查及退出码、`HEAD_REVISION`、
去掉 `sha256:` 前缀的 image ID 写入本地 release metadata JSON。保留这份文件用于
操作审计；不要把示例 digest 当成真实结果。

本页所有 Compose 命令必须沿用同一个 `PROJECT_NAME`。如果 Exchange 需要私网 TLS
覆盖，则每条命令还同时追加 `-f docker-compose.yml -f docker-compose.exchange-tls.yml`；
所有 secret file 变量必须是绝对路径，不能隐式落到仓库的 `secrets/` 默认路径。

## 2. 生成并本地校验

`--webhook-inbox-id` 必须是 Exchange Webhook 实际发送的 Inbox opaque ID，不能填写
`INBOX` 或占位符。输出目录必须尚不存在，生成后目录权限为 `0700`、两个文件权限为
`0600`，重复执行不会覆盖已有文件。

```bash
.venv/bin/python scripts/prepare_ingestion_manifests.py \
  --account-id <ACCOUNT_ID> \
  --webhook-inbox-id <REAL_OPAQUE_INBOX_ID> \
  --sync-folder Inbox \
  --build-id <BUILD_ID> \
  --release-evidence-file deploy/release-metadata-<BUILD_ID>.json \
  --output-dir deploy/generated-<BUILD_ID>
```

先执行无数据库写入的本地解析校验：

```bash
.venv/bin/python scripts/manage_ingestion.py initialize \
  --account-id <ACCOUNT_ID> \
  --policy-file deploy/generated-<BUILD_ID>/POLICY.json \
  --contract-file deploy/generated-<BUILD_ID>/CONTRACT.json \
  --actor <OPERATOR> --reason "fresh Phase4-Lite launch" \
  --idempotency-key <UNIQUE_KEY> --dry-run
```

## 3. 只读挂载并初始化

确保 Compose 所需的 ingestion-maintenance DSN secret 和四个角色名已经配置。只挂载本次
生成的子目录，不挂载整个 `deploy/`。先在同一个 maintenance image 中 dry-run，再删除
`--dry-run` 执行一次 greenfield 初始化：

```bash
GENERATED_DIR="$PWD/deploy/generated-<BUILD_ID>"

docker compose -p "$PROJECT_NAME" --profile ingestion-maintenance run --rm \
  --volume "$GENERATED_DIR:/run/ingestion-input:ro" \
  ingestion-maintenance initialize \
  --account-id <ACCOUNT_ID> \
  --policy-file /run/ingestion-input/POLICY.json \
  --contract-file /run/ingestion-input/CONTRACT.json \
  --actor <OPERATOR> --reason "fresh Phase4-Lite launch" \
  --idempotency-key <UNIQUE_KEY> --dry-run

docker compose -p "$PROJECT_NAME" --profile ingestion-maintenance run --rm \
  --volume "$GENERATED_DIR:/run/ingestion-input:ro" \
  ingestion-maintenance initialize \
  --account-id <ACCOUNT_ID> \
  --policy-file /run/ingestion-input/POLICY.json \
  --contract-file /run/ingestion-input/CONTRACT.json \
  --actor <OPERATOR> --reason "fresh Phase4-Lite launch" \
  --idempotency-key <UNIQUE_KEY>
```

`POLICY.json` 固定为一个 `INBOX` scope 和严格七项事件策略；Sync 的三项仅用于满足
统一策略契约，当前固定配置仍然关闭主动轮询与 Sync reconciliation。

## 4. 隔离运行验收

上线前使用一个从未使用过的 Compose project、全新 volume 和未占用端口。若旧实例仍在
运行，另加仅用于冒烟的本地 overlay，把端口覆盖为
`127.0.0.1:${APP_PORT:-18081}:8000`；不要使用会切换 development 或挂载源码的开发
overlay。所有命令固定同一组 `--env-file`、`-p` 和 `-f` 参数，所有 secret/CA 文件使用
绝对路径。启动必须带 `--no-build`，并按主 README 最多等待 180 秒，直到 `/ready` 同时
返回 `status=ready` 与 `processing=active`。随后比较运行容器的 `.Image` 和第 1 节记录的
`IMAGE_ID`，二者必须相同。

安全的 Worker 冒烟不使用未知 folder 的 `NewMailEvent`：该路径会在 intake 本地完成，
无法证明 Worker 领取。应使用真实 Inbox opaque ID、唯一虚构邮件 ID 和
`ModifiedEvent`，对完全相同的原始 JSON bytes 计算 HMAC，并向本机端口发送两次；两次都
必须返回 `202 {"status":"accepted"}`。通过 auditor DSN 做只读聚合验证：恰好一条
`event_inbox`，policy 为 `metadata_only`、状态为 `completed`、
`processing_started_at` 非空、`effect_started_at` 为空、lease 字段全部为空；对应
`emails` 只有一个 `ingested` metadata shell，且没有任何外部 effect 时间戳。这样覆盖
Webhook → Durable Inbox → Worker → 本地完成，同时不会读取 Exchange 详情、调用模型、
Qdrant 或发送飞书卡片。

本机签名请求不能证明公网回调。正式入口就绪后仍须按主 README 以 inactive 订阅完成
TestEvent 的嵌套目标状态/响应体验收，再启用订阅并发送真实邮件完成审批点击闭环。测试
结束只对隔离 project 使用同一参数执行 `down -v`，不得停止或删除旧项目。
