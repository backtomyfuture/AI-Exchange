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
export AI_EXCHANGE_IMAGE="$RELEASE_IMAGE"

# PROJECT_NAME 是独立的 Compose-safe 小写 slug，不要直接拼接可能含点号/大写的 build id。
# 镜像标签是本次部署进程的内部参数，不写入用户 `.env`。
CONFIGURED_IMAGE="$(docker compose -p "$PROJECT_NAME" config --format json | \
  .venv/bin/python -c 'import json,sys; print(json.load(sys.stdin)["services"]["ai-assistant-service"]["image"])')"
test "$CONFIGURED_IMAGE" = "$RELEASE_IMAGE"

test -z "$(git status --porcelain=v1 --untracked-files=all --ignore-submodules=none)"
HEAD_REVISION="$(git rev-parse --verify HEAD)"
.venv/bin/python -m pytest -q
.venv/bin/ruff check src/ tests/ scripts/prepare_ingestion_manifests.py
# 只构建一次规范镜像。多个服务共用同一 tag 时并发 build 会各自产生
# provenance 不同的 image，并以完成顺序覆盖 tag，破坏产物确定性。
docker compose -p "$PROJECT_NAME" build --pull ai-assistant-service

IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$RELEASE_IMAGE")"
printf 'HEAD=%s\nimage=%s\n' "$HEAD_REVISION" "$IMAGE_ID"
```

`RELEASE_IMAGE` 必须与当前 shell 的 `AI_EXCHANGE_IMAGE` 精确一致；Compose 的全部
应用/one-shot 服务固定复用这一个 tag。不要再单独或并发构建 one-shot 服务；后续
`run` 禁止传 `--build`，`up` 必须传 `--no-build`。把实际执行的检查及退出码、
`HEAD_REVISION`、
去掉 `sha256:` 前缀的 image ID 写入本地 release metadata JSON。保留这份文件用于
操作审计；不要把示例 digest 当成真实结果。

本页所有 Compose 命令必须沿用同一个 `PROJECT_NAME`。如果 Exchange 需要私网 TLS
覆盖，则每条命令同时使用自动生成的 `secrets/deployment.env` 和
`docker-compose.exchange-tls.yml`。常规重部署优先使用 `scripts/deploy_system.py`，避免
遗漏内部配置；用户 `.env` 始终只保留 16 项。

## 2. 生成并本地校验

`--webhook-inbox-id` 是冻结 policy schema 的兼容性元数据：传入本账户真实的 Inbox
opaque ID，不能填写 `INBOX` 或占位符。它不会创建 Webhook 订阅或 HTTP 路由；当前
邮件入口只会轮询 Gateway `INBOX`。输出目录必须尚不存在，生成后目录权限为 `0700`、
两个文件权限为 `0600`，重复执行不会覆盖已有文件。

```bash
.venv/bin/python scripts/prepare_ingestion_manifests.py \
  --account-id <ACCOUNT_ID> \
  --webhook-inbox-id <REAL_OPAQUE_INBOX_ID> \
  --sync-folder INBOX \
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

`POLICY.json` 固定为一个 `INBOX` scope 和严格七项事件策略；运行时只使用其中三项
Sync 策略，通过 `sync_state` 把邮件变化写入现有 Durable Inbox。Webhook 策略条目仅为
冻结 schema 兼容性保留，不会暴露入口；Sync reconciliation 仍保持关闭。

## 4. 隔离运行验收

上线前使用一个从未使用过的 Compose project、全新 volume 和未占用端口。若旧实例仍在
运行，另加仅用于冒烟的本地 overlay，把端口覆盖为
`127.0.0.1:${APP_PORT:-18081}:8000`；不要使用会切换 development 或挂载源码的开发
overlay。所有命令固定同一组 `--env-file`、`-p` 和 `-f` 参数，所有 secret/CA 文件使用
绝对路径。启动必须带 `--no-build`，并按主 README 最多等待 180 秒，直到 `/ready` 同时
返回 `status=ready` 与 `processing=active`。随后比较运行容器的 `.Image` 和第 1 节记录的
`IMAGE_ID`，二者必须相同。

`/ready` 首次返回 `processing=active` 表示新库已完成历史基线、游标已激活；在此之后
发送一封真实且可识别的测试邮件。等待最多两个轮询周期后，以 auditor DSN 只读确认该
邮件对应的 `event_inbox` 来源为 `sync`，并确认现有 Worker 已认领处理；随后按正常业务
链路检查飞书卡片和审批结果。该验收覆盖 `sync_state` → Durable Inbox → 既有 Worker →
既有邮件详情/图片按需处理/LLM/飞书，而不需要 HMAC、TestEvent 或 Webhook 订阅。

首次历史基线在后台执行：容器会先存活，`/ready` 在游标激活前保持未就绪。Gateway 会先
完成其内部 Exchange 分页，再返回这一轮的最终游标；部署工具和 Compose 健康检查为这个
过程预留 15 分钟。超过该时间应先检查 Gateway 连通性和基线进度，不要重置游标或反复
重建数据卷。

同一 Exchange 账户不能让旧、新两个应用同时处于活跃轮询状态。全新数据库的首次
`sync_state=null` 会建立并丢弃历史基线，因此切换前必须明确接受边界窗口的策略：先完成
新基线再停止旧实例会有短暂重复风险；先停止旧实例再建新基线则有遗漏风险。测试结束只对
隔离 project 使用同一参数执行 `down -v`，不得停止或删除旧项目。
